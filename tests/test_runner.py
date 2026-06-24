"""Tests for runner module."""

import json
import logging
import math
import os
import sqlite3
import stat
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import ANY, MagicMock, Mock, patch

import pytest

from gza.advance_engine import evaluate_advance_rules
from gza.config import BranchStrategy, Config
from gza.db import SqliteTaskStore, StepRef, Task, TaskStats
from gza.git import Git, GitError, ResolvedMergeSourceRef
from gza.github import GitHub, GitHubError, PullRequestDetails
from gza.improve_diff import ImproveDiffResult
from gza.lineage import get_plan_for_task
from gza.log_paths import ops_log_path_for
from gza.providers import ClaudeProvider, RunResult
from gza.providers.base import PreflightCheckResult
from gza.rebase_diff import RebaseDiffBaseline
from gza.recovery_engine import decide_failed_task_recovery
from gza.review_tasks import DuplicateReviewError, create_or_reuse_followup_task
from gza.cli import _create_improve_task, _create_rebase_task
from gza.review_verdict import ReviewFinding, parse_review_report
from gza.runner import (
    BACKUP_DIR,
    BRANCH_UNPUSHABLE_FAILURE_REASON,
    CompletedCodeTaskPrPublicationOutcome,
    DEPENDENCY_BLOCKED_NOT_RUN_EXIT_CODE,
    ProjectReviewVerifyResult,
    REVIEW_IMPROVE_LINEAGE_LIMIT,
    SUMMARY_DIR,
    WIP_DIR,
    CrossProjectReviewVerifyResult,
    ProjectBoundary,
    REVIEW_VERIFY_TIMEOUT_GRACE_SECONDS,
    REVIEW_BLOCKER_RESOLUTION_ARTIFACT_KIND,
    ReviewVerifyResult,
    RunInvocationContext,
    _apply_transcript_stats_fallback,
    _build_code_task_commit_subject,
    _build_context_from_chain,
    _build_review_improve_lineage_context,
    _build_timeout_resume_context,
    _check_dependency_merge_precondition,
    _complete_code_task,
    _capture_noop_improve_review_verify_result,
    _compute_slug_override,
    _compute_tree_fingerprint,
    _copy_learnings_to_worktree,
    _create_and_run_review_task,
    _ensure_work_pr_for_completed_code_task,
    _extract_review_verdict,
    _extract_verify_phase_checkpoints,
    _format_review_verify_failure,
    _format_review_verify_result,
    _get_task_output,
    _post_complete_code_task,
    _persist_review_blocker_adjudication_for_completed_task,
    _resolve_review_verify_timeout_grace_seconds,
    _resolve_code_task_branch_name,
    _resolve_task_timeout_budget,
    _restore_wip_changes,
    _run_inner,
    _run_non_code_task,
    _run_result_to_stats,
    _run_review_verify_command,
    _run_review_verify_commands_for_projects,
    _save_wip_changes,
    _select_worktree_base_ref,
    _setup_code_task_worktree,
    _slug_exists,
    _snapshot_task_db_to_worktree,
    _stage_worktree_agent_resources,
    _write_timeout_resume_checkpoint,
    backup_database,
    build_prompt,
    generate_slug,
    get_task_output_paths,
    open_task_startup_log,
    post_review_to_pr,
    rename_startup_log_to_slug,
    run,
    write_execution_provenance_event,
    write_log_entry,
    write_worker_start_event,
)
from gza.worktree_roots import managed_worktree_root_paths


class TestGetTaskOutputPaths:
    """Tests for get_task_output_paths function."""

    def _make_task(self, store, task_type):
        """Create a task with a task_id slug set."""
        task = store.add(prompt=f"Test {task_type}", task_type=task_type)
        task.slug = f"20260101-test-{task_type}"
        store.update(task)
        return task

    def test_code_task_types_return_summary_path(self, tmp_path: Path):
        """Code task types (task, implement, improve, fix, rebase) return a summary_path."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        for task_type in ("task", "implement", "improve", "fix", "rebase"):
            task = self._make_task(store, task_type)
            report_path, summary_path = get_task_output_paths(task, tmp_path)
            assert summary_path is not None, f"{task_type} should have summary_path"
            assert report_path is None, f"{task_type} should not have report_path"
            assert "summaries" in str(summary_path)

    def test_non_code_task_types_return_report_path(self, tmp_path: Path):
        """Non-code task types return a report_path."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        for task_type in ("explore", "plan", "plan_review", "plan_improve", "review", "internal", "learn"):
            task = self._make_task(store, task_type)
            report_path, summary_path = get_task_output_paths(task, tmp_path)
            assert report_path is not None, f"{task_type} should have report_path"
            assert summary_path is None, f"{task_type} should not have summary_path"

    def test_plan_review_and_plan_improve_use_explicit_artifact_directories(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        plan_review = self._make_task(store, "plan_review")
        plan_improve = self._make_task(store, "plan_improve")

        plan_review_path, _ = get_task_output_paths(plan_review, tmp_path)
        plan_improve_path, _ = get_task_output_paths(plan_improve, tmp_path)

        assert plan_review_path is not None
        assert plan_improve_path is not None
        assert "plan-reviews" in str(plan_review_path)
        assert "revised-plans" in str(plan_improve_path)

    def test_no_task_id_returns_none(self, tmp_path: Path):
        """Tasks without a task_id return (None, None)."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="No slug", task_type="implement")
        # task_id is None by default (slug not yet generated)
        assert task.slug is None
        report_path, summary_path = get_task_output_paths(task, tmp_path)
        assert report_path is None
        assert summary_path is None


class TestPostReviewToPr:
    """Tests for posting review output to pull requests."""

    def test_non_github_repo_skips_silently_for_non_required_review(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        store = SqliteTaskStore(tmp_path / "test.db")
        impl_task = store.add("Implement feature", task_type="implement")
        impl_task.branch = "feature/non-github"
        store.update(impl_task)

        review_task = store.add("Review feature", task_type="review")
        review_task.output_content = "Looks good"
        store.update(review_task)

        gh = Mock()
        gh.cached_pr_support.return_value = False

        with patch("gza.runner.GitHub", return_value=gh):
            post_review_to_pr(
                review_task,
                impl_task,
                store,
                tmp_path,
                pr_integration=True,
                required=False,
            )

        assert capsys.readouterr().out == ""
        gh.is_available.assert_not_called()

    def test_lookup_failure_preserves_cached_pr_state_and_surfaces_error(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        store = SqliteTaskStore(tmp_path / "test.db")
        impl_task = store.add("Implement feature", task_type="implement")
        impl_task.branch = "feature/review-lookup-failure"
        impl_task.pr_number = 42
        impl_task.pr_state = "open"
        store.update(impl_task)

        review_task = store.add("Review feature", task_type="review")
        review_task.output_content = "Looks good"
        store.update(review_task)

        gh = Mock()
        gh.is_available.return_value = True
        gh.get_pr_details.side_effect = GitHubError("gh pr view 42 failed: authentication failed")

        with patch("gza.runner.GitHub", return_value=gh):
            post_review_to_pr(review_task, impl_task, store, tmp_path, required=False)

        output = capsys.readouterr().out
        assert "Failed to look up PR for task" in output
        assert "No PR found" not in output
        refreshed = store.get(impl_task.id)
        assert refreshed is not None
        assert refreshed.pr_number == 42
        assert refreshed.pr_state == "open"
        gh.add_pr_comment.assert_not_called()


def test_ensure_work_pr_for_completed_code_task_leaves_one_skip_note_for_non_github_repo(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    task = store.add("Implement feature", task_type="implement")
    task.branch = "feature/non-github-pr"
    store.update(task)

    config = Mock(spec=Config)
    config.pr_integration = True

    git = Mock()
    git.default_branch.return_value = "main"
    git.count_commits_ahead.return_value = 1

    with patch(
        "gza.runner.ensure_task_pr",
        return_value=Mock(ok=True, status="unsupported", pr_number=None, pr_url=None, error="project has no GitHub-capable remote"),
    ):
        outcome = _ensure_work_pr_for_completed_code_task(task, config, store, git)

    assert outcome.kind == "ready"
    assert outcome.status == "unsupported"
    assert capsys.readouterr().out.strip() == "Info: PR requested but skipped: project has no GitHub-capable remote"


def test_ensure_work_pr_for_completed_code_task_discovers_non_github_repo_before_push(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    GitHub.clear_pr_support_cache()
    store = SqliteTaskStore(tmp_path / "test.db")
    task = store.add("Implement feature", task_type="implement")
    task.branch = "feature/non-github-before-push"
    store.update(task)

    config = Mock(spec=Config)
    config.pr_integration = True

    git = Mock(spec=Git)
    git.default_branch.return_value = "main"
    git.count_commits_ahead.return_value = 1

    gh = Mock()
    gh.is_available.return_value = True
    gh.cached_pr_support.side_effect = GitHub.cached_pr_support

    def _raise_unsupported(branch: str):
        GitHub._mark_pr_unsupported()
        raise GitHubError(
            f"gh pr list --head {branch} failed: "
            "none of the git remotes configured for this repository point to a known GitHub host"
        )

    gh.discover_pr_by_branch.side_effect = _raise_unsupported

    with patch("gza.pr_ops.GitHub", return_value=gh):
        outcome = _ensure_work_pr_for_completed_code_task(task, config, store, git)

    assert outcome.kind == "ready"
    assert outcome.status == "unsupported"
    assert capsys.readouterr().out.strip() == "Info: PR requested but skipped: project has no GitHub-capable remote"
    git.needs_push.assert_not_called()
    git.push_branch.assert_not_called()
    GitHub.clear_pr_support_cache()


def test_post_review_to_pr_short_circuits_when_pr_integration_disabled(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    impl_task = store.add("Implement feature", task_type="implement")
    impl_task.branch = "feature/config-disabled"
    store.update(impl_task)

    review_task = store.add("Review feature", task_type="review")
    review_task.output_content = "Looks good"
    store.update(review_task)

    with patch("gza.runner.GitHub") as github_cls:
        post_review_to_pr(
            review_task,
            impl_task,
            store,
            tmp_path,
            pr_integration=False,
            required=False,
        )

    assert capsys.readouterr().out == ""
    github_cls.assert_not_called()


def test_restore_wip_changes_ignores_owned_artifact_paths(tmp_path: Path) -> None:
    task_store = SqliteTaskStore(tmp_path / "test.db", prefix="testproject")
    task = task_store.add("Restore task", task_type="implement")
    task.slug = "20260531-restore"
    task_store.update(task)

    wip_dir = tmp_path / WIP_DIR
    wip_dir.mkdir(parents=True, exist_ok=True)
    (wip_dir / f"{task.slug}.diff").write_text(
        "diff --git a/.gza/summaries/task.md b/.gza/summaries/task.md\n"
        "new file mode 100644\n"
        "--- /dev/null\n"
        "+++ b/.gza/summaries/task.md\n"
        "@@ -0,0 +1 @@\n"
        "+summary\n"
        "diff --git a/src/file.py b/src/file.py\n"
        "index e69de29..8c7e5a6 100644\n"
        "--- a/src/file.py\n"
        "+++ b/src/file.py\n"
        "@@ -0,0 +1 @@\n"
        "+print('hello')\n"
    )

    config = Mock(spec=Config)
    config.project_dir = tmp_path

    worktree_git = Mock()
    worktree_git._run.side_effect = [
        Mock(stdout="regular commit", returncode=0, stderr=""),
        Mock(stdout="", returncode=0, stderr=""),
        Mock(stdout="", returncode=0, stderr=""),
    ]

    _restore_wip_changes(
        task,
        worktree_git,
        config,
        branch_name="feature/restore",
    )

    apply_call = worktree_git._run.call_args_list[1]
    assert apply_call.args[:2] == ("apply", "--cached")
    applied_patch = apply_call.kwargs["stdin"].decode()
    assert ".gza/summaries/task.md" not in applied_patch
    assert "src/file.py" in applied_patch


def test_restore_wip_changes_skips_owned_artifact_only_patch(tmp_path: Path) -> None:
    task_store = SqliteTaskStore(tmp_path / "test.db", prefix="testproject")
    task = task_store.add("Restore task", task_type="implement")
    task.slug = "20260531-restore-owned-only"
    task_store.update(task)

    wip_dir = tmp_path / WIP_DIR
    wip_dir.mkdir(parents=True, exist_ok=True)
    (wip_dir / f"{task.slug}.diff").write_text(
        "diff --git a/.gza/summaries/task.md b/.gza/summaries/task.md\n"
        "new file mode 100644\n"
        "--- /dev/null\n"
        "+++ b/.gza/summaries/task.md\n"
        "@@ -0,0 +1 @@\n"
        "+summary\n"
    )

    config = Mock(spec=Config)
    config.project_dir = tmp_path

    worktree_git = Mock()
    worktree_git._run.return_value = Mock(stdout="regular commit", returncode=0, stderr="")

    _restore_wip_changes(
        task,
        worktree_git,
        config,
        branch_name="feature/restore",
    )

    worktree_git._run.assert_called_once()


def test_restore_wip_changes_ignores_scoped_owned_artifact_paths_in_subdir_project(tmp_path: Path) -> None:
    project_dir = tmp_path / "tarantino-ui"
    project_dir.mkdir()
    task_store = SqliteTaskStore(tmp_path / "test.db", prefix="testproject")
    task = task_store.add("Restore task", task_type="implement")
    task.slug = "20260531-restore-scoped"
    task_store.update(task)

    wip_dir = project_dir / WIP_DIR
    wip_dir.mkdir(parents=True, exist_ok=True)
    (wip_dir / f"{task.slug}.diff").write_text(
        "diff --git a/tarantino-ui/.gza/summaries/task.md b/tarantino-ui/.gza/summaries/task.md\n"
        "new file mode 100644\n"
        "--- /dev/null\n"
        "+++ b/tarantino-ui/.gza/summaries/task.md\n"
        "@@ -0,0 +1 @@\n"
        "+summary\n"
        "diff --git a/tarantino-ui/src/file.py b/tarantino-ui/src/file.py\n"
        "index e69de29..8c7e5a6 100644\n"
        "--- a/tarantino-ui/src/file.py\n"
        "+++ b/tarantino-ui/src/file.py\n"
        "@@ -0,0 +1 @@\n"
        "+print('hello')\n"
    )

    config = Mock(spec=Config)
    config.project_dir = project_dir
    config._project_boundary_cache = ProjectBoundary(
        repo_root=tmp_path,
        scope_root=Path("tarantino-ui"),
        local_dependencies=(),
    )

    worktree_git = Mock()
    worktree_git._run.side_effect = [
        Mock(stdout="regular commit", returncode=0, stderr=""),
        Mock(stdout="", returncode=0, stderr=""),
        Mock(stdout="", returncode=0, stderr=""),
    ]

    _restore_wip_changes(
        task,
        worktree_git,
        config,
        branch_name="feature/restore-scoped",
    )

    apply_call = worktree_git._run.call_args_list[1]
    assert apply_call.args[:2] == ("apply", "--cached")
    applied_patch = apply_call.kwargs["stdin"].decode()
    assert "tarantino-ui/.gza/summaries/task.md" not in applied_patch
    assert "tarantino-ui/src/file.py" in applied_patch


class TestGetTaskOutput:
    """Tests for _get_task_output fallback semantics."""

    def test_fix_task_reads_legacy_summary_fallback(self, tmp_path: Path):
        """Completed fix tasks should still read on-disk summary when DB fields are absent."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add(prompt="Fix regression", task_type="fix")
        task.slug = "20260422-fix-fallback"
        task.status = "completed"
        task.output_content = None
        task.report_file = None
        store.update(task)

        summary_path = tmp_path / SUMMARY_DIR / f"{task.slug}.md"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("- Fixed output fallback\n")

        assert _get_task_output(task, tmp_path) == "- Fixed output fallback\n"


class TestBuildPrompt:
    """Tests for build_prompt function."""

    def test_build_prompt_task_type_with_summary_path(self, tmp_path: Path):
        """Test that build_prompt includes summary instructions for task type."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="Implement feature X",
            task_type="implement",
        )

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        summary_path = Path("/workspace/.gza/summaries/test.md")

        prompt = build_prompt(task, config, store, summary_path=summary_path)

        assert "Complete this task: Implement feature X" in prompt
        assert str(summary_path) in prompt
        assert "write a summary" in prompt.lower()
        assert "What was accomplished" in prompt
        assert "Files changed" in prompt
        assert "notable decisions" in prompt.lower()

    def test_build_prompt_implement_type_with_summary_path(self, tmp_path: Path):
        """Test that build_prompt includes summary instructions for implement type."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="Implement feature Y",
            task_type="implement",
        )

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        summary_path = Path("/workspace/.gza/summaries/test.md")

        prompt = build_prompt(task, config, store, summary_path=summary_path)

        assert "Complete this task: Implement feature Y" in prompt
        assert str(summary_path) in prompt
        assert "write a summary" in prompt.lower()

    def test_build_prompt_task_type_without_summary_path(self, tmp_path: Path):
        """Test that build_prompt uses fallback message without summary path."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="Implement feature Z",
            task_type="implement",
        )

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        prompt = build_prompt(task, config, store, summary_path=None)

        assert "Complete this task: Implement feature Z" in prompt
        assert "report what you accomplished" in prompt
        assert "write a summary" not in prompt.lower()

    def test_build_prompt_explore_type_ignores_summary_path(self, tmp_path: Path):
        """Test that explore tasks use report_path, not summary_path."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="Explore codebase",
            task_type="explore",
        )

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        report_path = Path("/workspace/.gza/explorations/test.md")
        summary_path = Path("/workspace/.gza/summaries/test.md")

        prompt = build_prompt(task, config, store, report_path=report_path, summary_path=summary_path)

        # Should use report_path for explore tasks
        assert str(report_path) in prompt
        assert "exploration/research task" in prompt.lower()
        # Should NOT include summary path
        assert str(summary_path) not in prompt

    def test_build_prompt_includes_learnings_when_file_exists(self, tmp_path: Path):
        """Test that build_prompt references learnings.md when it exists."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="Implement feature X",
            task_type="implement",
        )

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        # Create .gza/learnings.md (content doesn't matter; prompt only references the path)
        gza_dir = tmp_path / ".gza"
        gza_dir.mkdir(parents=True, exist_ok=True)
        (gza_dir / "learnings.md").write_text("")

        prompt = build_prompt(task, config, store)

        assert ".gza/learnings.md" in prompt

    def test_build_prompt_skips_learnings_when_no_file(self, tmp_path: Path):
        """Test that build_prompt works normally when learnings.md doesn't exist."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="Implement feature X",
            task_type="implement",
        )

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        prompt = build_prompt(task, config, store)

        assert "learnings.md" not in prompt
        assert "Complete this task: Implement feature X" in prompt


class TestTimeoutBudgeting:
    """Tests for task-aware timeout budget resolution."""

    def test_resolve_task_timeout_budget_scales_medium_code_diff(self, tmp_path: Path):
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

        git = Mock(spec=Git)
        git.get_diff_numstat_checked.return_value = "250\t200\tsrc/feature.py\n"

        budget = _resolve_task_timeout_budget(
            task=task,
            config=config,
            provider="claude",
            git=git,
            branch_name="feature/test",
            default_branch="main",
        )

        assert budget.minutes == 30
        assert budget.diff_lines == 450
        assert budget.diff_files == 1
        assert "scaled from base 10m" in budget.reason
        git.get_diff_numstat_checked.assert_called_once_with("main...feature/test", ())

    def test_resolve_task_timeout_budget_hard_caps_code_task_base_override(self, tmp_path: Path) -> None:
        task = Task(id="gza-1", prompt="Implement feature", status="pending", task_type="implement")
        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.timeout_minutes = 10
        config.get_timeout_minutes_for_task.return_value = 60
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
        git = Mock(spec=Git)
        git.get_diff_numstat_checked.return_value = ""

        budget = _resolve_task_timeout_budget(
            task=task,
            config=config,
            provider="codex",
            git=git,
            branch_name="feature/test",
            default_branch="main",
        )

        assert budget.minutes == 45
        assert budget.reason == (
            "base timeout for task type 'implement'; below scaling thresholds; "
            "hard-capped at 45m from 60m"
        )

    def test_extract_verify_phase_checkpoints_ignores_phase_without_tree_fingerprint(
        self, tmp_path: Path
    ) -> None:
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

class TestWorkerLifecycleLogging:
    """Tests for worker lifecycle JSONL log events."""

    def test_write_worker_start_event_logs_start_when_in_worker_mode(self, tmp_path: Path):
        """write_worker_start_event should emit a start event with worker metadata."""
        log_file = tmp_path / "worker.log"
        with (
            patch.dict(
                os.environ,
                {
                    "GZA_WORKER_MODE": "1",
                    "GZA_WORKER_ID": "w-20260411-1",
                },
                clear=False,
            ),
        ):
            write_worker_start_event(log_file, resumed=True)

        content = ops_log_path_for(log_file).read_text().strip()
        assert content
        import json
        event = json.loads(content)
        assert event["type"] == "gza"
        assert event["subtype"] == "worker_lifecycle"
        assert event["event"] == "start"
        assert event["worker_id"] == "w-20260411-1"
        assert "resumed" in event["message"]

    def test_write_execution_provenance_event_logs_structured_entry(self, tmp_path: Path):
        """Execution provenance should include command/mode/interaction metadata."""
        log_file = tmp_path / "exec.log"
        provider = Mock()
        provider.name = "Claude"
        context = RunInvocationContext(
            command="run-inline",
            execution_mode="foreground_inline",
            interaction_mode="auto",
        )

        write_execution_provenance_event(
            log_file,
            invocation=context,
            provider=provider,
            interaction_mode="interactive",
            resumed=True,
        )

        import json

        event = json.loads(ops_log_path_for(log_file).read_text().strip())
        assert event["type"] == "gza"
        assert event["subtype"] == "execution"
        assert event["command"] == "run-inline"
        assert event["execution_mode"] == "foreground_inline"
        assert event["interaction_mode"] == "interactive"
        assert event["provider"] == "claude"
        assert event["worker_mode"] is False
        assert event["resumed"] is True

    def test_write_execution_provenance_event_marks_foreground_work_as_worker_mode(self, tmp_path: Path):
        """Foreground gza work runs should be marked as worker mode in execution provenance."""
        log_file = tmp_path / "exec-work.log"
        provider = Mock()
        provider.name = "Codex"
        context = RunInvocationContext(
            command="work",
            execution_mode="foreground_worker",
            interaction_mode="observe_only",
        )

        write_execution_provenance_event(
            log_file,
            invocation=context,
            provider=provider,
            interaction_mode="observe_only",
            resumed=False,
        )

        import json

        event = json.loads(ops_log_path_for(log_file).read_text().strip())
        assert event["execution_mode"] == "worker_foreground"
        assert event["worker_mode"] is True

    def test_build_prompt_skips_learnings_when_skip_learnings_true(self, tmp_path: Path):
        """Test that build_prompt skips learnings reference when task.skip_learnings is True."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="One-off experimental task",
            task_type="implement",
            skip_learnings=True,
        )

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        # Create .gza/learnings.md
        gza_dir = tmp_path / ".gza"
        gza_dir.mkdir(parents=True, exist_ok=True)
        learnings_content = "# Project Learnings\n\n- Use pytest fixtures\n"
        (gza_dir / "learnings.md").write_text(learnings_content)

        prompt = build_prompt(task, config, store)

        assert "learnings.md" not in prompt
        assert "Complete this task: One-off experimental task" in prompt


class TestReviewContextFromChain:
    """Tests for self-contained review context generation."""

    def test_review_context_derives_scope_for_plan_backed_completed_implementation(self, tmp_path: Path):
        """Plan-backed completed implementations derive a scoped review ask."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        plan_task = store.add(prompt="Plan migration", task_type="plan")
        plan_task.output_content = "# Plan\nUse width 6 zero padding."
        store.update(plan_task)

        impl_task = store.add(
            prompt="Implement task gza-1",
            task_type="implement",
            based_on=plan_task.id,
        )
        impl_task.status = "completed"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review implementation",
            task_type="review",
            depends_on=impl_task.id,
        )

        context = _build_context_from_chain(review_task, store, tmp_path, git=None)

        assert "## Review scope:" in context
        assert f"Plan-backed implementation scope from {plan_task.id}." in context
        assert "Implementation request: Implement task gza-1" in context
        assert "## Original plan context (out of scope except for the review scope):" in context
        assert "## Original plan:\n" not in context
        assert "Use width 6 zero padding." in context
        assert "## Original request:" not in context

    def test_review_context_marks_unavailable_plan_as_blocker(self, tmp_path: Path):
        """Plan-driven reviews surface explicit marker when plan exists but is unavailable."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        plan_task = store.add(prompt="Plan migration", task_type="plan")
        store.update(plan_task)

        impl_task = store.add(
            prompt="Implement task gza-1",
            task_type="implement",
            based_on=plan_task.id,
        )
        impl_task.status = "completed"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review implementation",
            task_type="review",
            depends_on=impl_task.id,
        )

        with patch("gza.runner._get_task_output", return_value=None):
            context = _build_context_from_chain(review_task, store, tmp_path, git=None)

        assert "## Review scope:" in context
        assert f"Plan-backed implementation scope from {plan_task.id}." in context
        assert "## Original plan context (out of scope except for the review scope):" in context
        assert "## Original plan:\n" not in context
        assert f"plan task {plan_task.id} exists but content unavailable" in context
        assert "flag as blocker" in context
        assert "## Original request:" not in context

    def test_review_context_uses_structured_review_scope_and_reframes_plan_as_context(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        plan_task = store.add(prompt="Plan migration", task_type="plan")
        plan_task.output_content = "# Plan\n1. Whole plan.\n2. More slices."
        store.update(plan_task)

        impl_task = store.add(
            prompt="Implement task gza-1",
            task_type="implement",
            based_on=plan_task.id,
            review_scope="slice F-A1 + F-A2: implement only the classifier and persistence surfaces",
        )
        impl_task.status = "completed"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review implementation",
            task_type="review",
            depends_on=impl_task.id,
        )

        context = _build_context_from_chain(review_task, store, tmp_path, git=None)

        assert "## Review scope:" in context
        assert f"Implementation task: {impl_task.id}" in context
        assert "only gradeable ask" in context
        assert "slice F-A1 + F-A2" in context
        assert "## Original plan context (out of scope except for the review scope):" in context
        assert "## Original plan:\n" not in context
        assert "## Original request:" not in context

    def test_review_context_derives_legacy_slice_scope_from_prompt(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        plan_task = store.add(prompt="Plan migration", task_type="plan")
        plan_task.output_content = "# Plan\n1. Full slice stack."
        store.update(plan_task)

        impl_task = store.add(
            prompt=(
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
        store.update(impl_task)

        review_task = store.add(
            prompt="Review implementation",
            task_type="review",
            depends_on=impl_task.id,
        )

        context = _build_context_from_chain(review_task, store, tmp_path, git=None)

        assert "## Review scope:" in context
        assert "Slice F-A1 + F-A2" in context
        assert "Add the shared classifier." in context
        assert "Out-of-scope sibling context:" in context
        assert "- F-A3" in context
        assert "- F-B1" in context

    def test_review_context_uses_comment_derived_scope_and_keeps_plan_as_background(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        plan_task = store.add(prompt="Plan migration", task_type="plan")
        plan_task.output_content = "# Plan\n**Task type:** PLAN ONLY. No implementation in this row."
        store.update(plan_task)

        impl_task = store.add(
            prompt="Implement the bridge slices for the serial rerun path.",
            task_type="implement",
            based_on=plan_task.id,
        )
        impl_task.status = "completed"
        store.update(impl_task)
        assert impl_task.id is not None
        store.add_comment(
            impl_task.id,
            "Grade only the serial-rerun bridge slice.",
            kind="review_scope",
        )

        review_task = store.add(
            prompt="Review implementation",
            task_type="review",
            depends_on=impl_task.id,
        )

        context = _build_context_from_chain(review_task, store, tmp_path, git=None)

        assert "## Review scope:" in context
        assert "Grade only the serial-rerun bridge slice." in context
        assert "## Original plan context (out of scope except for the review scope):" in context
        assert "PLAN ONLY" in context
        assert "## Original plan:\n" not in context

    def test_review_context_derives_scope_for_plan_backed_unsliced_prompt(
        self,
        tmp_path: Path,
    ):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        plan_task = store.add(prompt="Plan migration", task_type="plan")
        plan_task.output_content = (
            "# Plan\n"
            "**Task type:** PLAN ONLY. No implementation in this row.\n"
            "- Background constraints for the bridge."
        )
        store.update(plan_task)

        impl_task = store.add(
            prompt="Implement the bridge slices for the serial rerun path.",
            task_type="implement",
            based_on=plan_task.id,
        )
        impl_task.status = "completed"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review implementation",
            task_type="review",
            depends_on=impl_task.id,
        )

        context = _build_context_from_chain(review_task, store, tmp_path, git=None)

        assert "## Review scope:" in context
        assert f"Plan-backed implementation scope from {plan_task.id}." in context
        assert "Implementation request: Implement the bridge slices for the serial rerun path." in context
        assert "## Original plan context (out of scope except for the review scope):" in context
        assert "PLAN ONLY" in context
        assert "## Original plan:\n" not in context
        assert "## Original request:\n" not in context

    def test_review_context_prefers_review_row_scope_over_later_scope_comment(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        plan_task = store.add(prompt="Plan migration", task_type="plan")
        plan_task.output_content = "# Plan\n1. Build the bridge."
        store.update(plan_task)

        impl_task = store.add(
            prompt="Implement the bridge slices for the serial rerun path.",
            task_type="implement",
            based_on=plan_task.id,
        )
        impl_task.status = "completed"
        store.update(impl_task)
        assert impl_task.id is not None
        store.add_comment(
            impl_task.id,
            "Grade only the original bridge slice.",
            kind="review_scope",
        )

        review_task = store.add(
            prompt="Review implementation",
            task_type="review",
            depends_on=impl_task.id,
            review_scope="Persisted review-row scope.",
        )
        store.add_comment(
            impl_task.id,
            "Later scope comment that should not rewrite an existing review row.",
            kind="review_scope",
        )

        context = _build_context_from_chain(review_task, store, tmp_path, git=None)

        assert "Persisted review-row scope." in context
        assert "Later scope comment that should not rewrite an existing review row." not in context

    def test_review_context_scopes_followup_review_to_finding_text(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        plan_task = store.add(prompt="Plan migration", task_type="plan")
        plan_task.output_content = "# Plan\n1. Full slice stack."
        store.update(plan_task)

        impl_task = store.add(
            prompt=(
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
        store.update(impl_task)

        review_task = store.add(
            prompt="Review implementation",
            task_type="review",
            depends_on=impl_task.id,
        )
        review_task.status = "completed"
        store.update(review_task)

        followup_finding = ReviewFinding(
            id="F1",
            severity="FOLLOWUP",
            title="Keep slice boundary",
            body="Preserve the scoped review boundary across improve loops.",
            evidence=None,
            impact=None,
            fix_or_followup="carry slice scope into follow-up implementation tasks",
            tests=None,
        )
        followup_task, created_now = create_or_reuse_followup_task(
            store,
            review_task=review_task,
            impl_task=impl_task,
            finding=followup_finding,
            trigger_source="manual",
        )
        assert created_now is True
        followup_task.status = "completed"
        store.update(followup_task)

        followup_review = store.add(
            prompt="Review follow-up implementation",
            task_type="review",
            depends_on=followup_task.id,
        )

        context = _build_context_from_chain(followup_review, store, tmp_path, git=None)

        assert "## Review scope:" in context
        assert "F1 Keep slice boundary" in context
        assert "Preserve the scoped review boundary across improve loops." in context
        assert "## Original plan context (out of scope except for the review scope):" in context
        assert "## Original plan:\n" not in context
        assert "## Original request:" not in context

    def test_review_context_scopes_followup_review_to_finding_text_for_non_sliced_parent(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        plan_task = store.add(prompt="Plan the doc cleanup", task_type="plan")
        plan_task.output_content = "# Plan\n1. Full feature implementation."
        store.update(plan_task)

        impl_task = store.add(
            prompt="Implement the full feature",
            task_type="implement",
            based_on=plan_task.id,
        )
        impl_task.status = "completed"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review implementation",
            task_type="review",
            depends_on=impl_task.id,
        )
        review_task.status = "completed"
        store.update(review_task)

        followup_finding = ReviewFinding(
            id="F1",
            severity="FOLLOWUP",
            title="Update module docstring",
            body="The module docstring is outdated; update it to reflect the new API.",
            evidence=None,
            impact=None,
            fix_or_followup="update the module docstring",
            tests=None,
        )
        followup_task, created_now = create_or_reuse_followup_task(
            store,
            review_task=review_task,
            impl_task=impl_task,
            finding=followup_finding,
            trigger_source="manual",
        )
        assert created_now is True
        followup_task.status = "completed"
        store.update(followup_task)

        followup_review = store.add(
            prompt="Review follow-up doc fix",
            task_type="review",
            depends_on=followup_task.id,
        )

        context = _build_context_from_chain(followup_review, store, tmp_path, git=None)

        assert "## Review scope:" in context
        assert "F1 Update module docstring" in context
        assert "The module docstring is outdated" in context
        assert "## Original plan context (out of scope except for the review scope):" in context
        assert "## Original plan:\n" not in context
        assert "## Original request:" not in context

    def test_review_context_preserves_slice_scope_for_improve_reviews(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        plan_task = store.add(prompt="Plan migration", task_type="plan")
        plan_task.output_content = "# Plan\n1. Full slice stack."
        store.update(plan_task)

        impl_task = store.add(
            prompt=(
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
        store.update(impl_task)

        prior_review = store.add(
            prompt="Review implementation",
            task_type="review",
            depends_on=impl_task.id,
        )
        prior_review.status = "completed"
        prior_review.output_content = "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        store.update(prior_review)

        improve_task = _create_improve_task(
            store,
            impl_task,
            prior_review,
            trigger_source="manual",
        )
        improve_task.status = "completed"
        store.update(improve_task)

        improve_review = store.add(
            prompt="Review improve implementation",
            task_type="review",
            depends_on=improve_task.id,
            review_scope=improve_task.review_scope,
        )

        context = _build_context_from_chain(improve_review, store, tmp_path, git=None)

        assert "## Review scope:" in context
        assert "## Original plan context (out of scope except for the review scope):" in context
        assert "## Original plan:\n" not in context
        assert "Slice F-A1 + F-A2" in context

    def test_review_context_preserves_slice_scope_for_rebase_reviews(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        plan_task = store.add(prompt="Plan migration", task_type="plan")
        plan_task.output_content = "# Plan\n1. Full slice stack."
        store.update(plan_task)

        impl_task = store.add(
            prompt=(
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
        impl_task.branch = "test-project/20260602-empty-state"
        store.update(impl_task)
        assert impl_task.id is not None

        rebase_task = _create_rebase_task(
            store,
            impl_task.id,
            impl_task.branch,
            "main",
            trigger_source="manual",
        )
        rebase_task.status = "completed"
        store.update(rebase_task)

        rebase_review = store.add(
            prompt="Review rebased implementation",
            task_type="review",
            depends_on=rebase_task.id,
            review_scope=rebase_task.review_scope,
        )

        context = _build_context_from_chain(rebase_review, store, tmp_path, git=None)

        assert "## Review scope:" in context
        assert "## Original plan context (out of scope except for the review scope):" in context
        assert "## Original plan:\n" not in context
        assert "Slice F-A1 + F-A2" in context

    def test_review_context_includes_full_original_request_for_prompt_driven_impl(self, tmp_path: Path):
        """Prompt-driven reviews include full implementation prompt as original request."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        full_prompt = (
            "Implement migration with exact fields: prefix, decimal-only sequence, and width="
            "000006; include all edge cases and validation messages."
        )
        impl_task = store.add(prompt=full_prompt, task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review implementation",
            task_type="review",
            depends_on=impl_task.id,
        )

        context = _build_context_from_chain(review_task, store, tmp_path, git=None)

        assert "## Original request:" in context
        assert full_prompt in context
        assert "## Original plan:" not in context

    def test_review_context_includes_verify_result_when_provided(self, tmp_path: Path):
        """Autonomous review context should thread structured verify output into the prompt."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review implementation",
            task_type="review",
            depends_on=impl_task.id,
        )

        verify_result = (
            "## verify_command result\n\n"
            "- Command: `uv run pytest tests/ -q`\n"
            "- Status: failed\n"
            "- Exit status: 1\n\n"
            "Failing output (trimmed):\n"
            "```text\nE assert 1 == 2\n```"
        )
        context = _build_context_from_chain(
            review_task,
            store,
            tmp_path,
            git=None,
            review_verify_result=verify_result,
        )

        assert verify_result in context
        assert "## Original request:" in context

    def test_review_context_omits_ask_sections_when_no_plan_or_prompt(self, tmp_path: Path):
        """If implementation has neither plan chain nor prompt, ask sections are omitted."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Temporary prompt", task_type="implement")
        impl_task.status = "completed"
        impl_task.prompt = ""
        store.update(impl_task)

        review_task = store.add(
            prompt="Review implementation",
            task_type="review",
            depends_on=impl_task.id,
        )

        context = _build_context_from_chain(review_task, store, tmp_path, git=None)

        assert "## Original plan:" not in context
        assert "## Original request:" not in context

    def test_format_review_verify_result_omits_failure_blocker_text_for_pass(self):
        """Passing verify should not inject failure-specific blocker content into context."""
        result = _format_review_verify_result(
            "uv run pytest tests/ -q",
            subprocess.CompletedProcess(
                args=["bash", "-lc", "uv run pytest tests/ -q"],
                returncode=0,
                stdout="all good\n",
                stderr="",
            ),
        )

        assert "## verify_command result" in result
        assert "- Status: passed" in result
        assert "verify_command failure" not in result
        assert "Failing output (trimmed):" not in result

    def test_run_review_verify_command_captures_failed_output(self, tmp_path: Path):
        """Autonomous review verify should record command, status, exit code, and trimmed output."""
        result = _run_review_verify_command(
            "printf 'lint failed\\n' && exit 7",
            cwd=tmp_path,
        )

        rendered = _format_review_verify_result(result)
        assert result.command == "printf 'lint failed\\n' && exit 7"
        assert result.status == "failed"
        assert result.exit_status == "7"
        assert "## verify_command result" in rendered
        assert "Failing output (trimmed):" in rendered
        assert "lint failed" in rendered

    def test_format_review_verify_result_keeps_failure_tail_and_pytest_summary(self):
        """Trimmed failed verify output should preserve the failing pytest tail with line breaks."""
        long_output = (
            ("collected setup noise\n" * 350)
            + "gza-verify phase=failed name=unit duration_seconds=31.25\n"
            + "FAILED tests/test_runner.py::test_x\n"
            + "E       assert 1 == 2\n"
            + "=========================== short test summary info ============================\n"
            + "FAILED tests/test_runner.py::test_x\n"
            + "============================== 1 failed in 31.25s ==============================\n"
        )
        result = ReviewVerifyResult(
            command="./bin/tests -x",
            status="failed",
            exit_status="1",
            captured_at=datetime.now(UTC),
            failure="pytest failed",
            output=long_output,
        )

        rendered = _format_review_verify_result(result)

        assert "Failing output (trimmed):" in rendered
        assert "FAILED tests/test_runner.py::test_x" in rendered
        assert "============================== 1 failed in 31.25s ==============================" in rendered
        assert "collected setup noise" not in rendered
        assert "...\ngza-verify phase=failed name=unit duration_seconds=31.25" in rendered

    def test_format_review_verify_failure_labels_timeout(self):
        """Timeout formatter should preserve failure status and timeout evidence."""
        result = _format_review_verify_failure(
            "uv run pytest tests/ -q",
            exit_status="timed out",
            failure="verify_command timed out after 120s",
            output="partial pytest output\nstill running\n",
        )

        assert "## verify_command result" in result
        assert "- Status: failed" in result
        assert "- Exit status: timed out" in result
        assert "- Failure: verify_command timed out after 120s" in result
        assert "partial pytest output" in result

    def test_run_review_verify_command_reports_timeout(self, tmp_path: Path):
        """Timed-out autonomous review verify should become a failed verify section."""
        timed_out = Mock(
            timed_out=True,
            forced_kill=True,
            stdout="partial pytest output\n",
            stderr="still running\n",
        )
        with patch(
            "gza.runner._run_review_verify_command_with_timeout_diagnostics",
            return_value=timed_out,
        ):
            result = _run_review_verify_command(
                "uv run pytest tests/ -q",
                cwd=tmp_path,
                timeout_seconds=120,
                timeout_grace_seconds=5,
            )

        rendered = _format_review_verify_result(result)
        assert result.status == "failed"
        assert result.exit_status == "timed out"
        assert "verify_command timed out after 120s" in rendered
        assert "verify_command exceeded 120s; sent SIGTERM, waited 5s, then sent SIGKILL" in rendered
        assert "partial pytest output" in rendered
        assert "still running" in rendered

    def test_run_review_verify_command_reports_custom_timeout(self, tmp_path: Path):
        """Timeout wording should reflect the configured autonomous review timeout."""
        timed_out = Mock(
            timed_out=True,
            forced_kill=False,
            stdout="partial pytest output\n",
            stderr="still running\n",
        )
        with patch(
            "gza.runner._run_review_verify_command_with_timeout_diagnostics",
            return_value=timed_out,
        ):
            result = _run_review_verify_command(
                "uv run pytest tests/ -q",
                cwd=tmp_path,
                timeout_seconds=240,
                timeout_grace_seconds=7,
            )

        assert "verify_command timed out after 240s" in _format_review_verify_result(result)
        assert (
            "verify_command exceeded 240s; sent SIGTERM, waited 7s, and the process group exited during grace"
            in _format_review_verify_result(result)
        )

    @pytest.mark.parametrize("grace_seconds", [float("nan"), float("inf"), -float("inf")])
    def test_resolve_review_verify_timeout_grace_seconds_falls_back_for_non_finite_values(
        self,
        grace_seconds: float,
    ) -> None:
        """Defensive timeout-grace resolution should ignore non-finite values."""

        config = Mock(spec=Config)
        config.review_verify_timeout_grace_seconds = grace_seconds

        assert math.isfinite(_resolve_review_verify_timeout_grace_seconds(config))
        assert _resolve_review_verify_timeout_grace_seconds(config) == REVIEW_VERIFY_TIMEOUT_GRACE_SECONDS

    def test_run_review_verify_command_warns_when_near_timeout_budget(self, tmp_path: Path):
        """Completed review verify runs should warn operators before they start timing out."""
        helper_result = Mock(
            returncode=0,
            stdout="all good\n",
            stderr="",
            timed_out=False,
            forced_kill=False,
        )

        with patch("gza.runner._run_review_verify_command_with_timeout_diagnostics", return_value=helper_result), \
             patch("gza.runner.console.print") as mock_print, \
             patch("gza.runner.logger.warning") as mock_warning, \
             patch("gza.runner.time.monotonic", side_effect=[100.0, 108.3]):
            result = _run_review_verify_command(
                "printf 'all good\\n'",
                cwd=tmp_path,
                timeout_seconds=10,
            )

        rendered = _format_review_verify_result(result)
        assert "- Status: passed" in rendered
        assert "- Exit status: 0" in rendered
        mock_warning.assert_called_once()
        warning_message = mock_warning.call_args.args[0]
        assert "verify_command used 8.3s of 10s budget" in warning_message
        assert "suite is approaching the review wall; profile before it starts timing out" in warning_message
        mock_print.assert_called_once_with(f"[yellow]Warning: {warning_message}[/yellow]")

    def test_run_review_verify_command_skips_warning_when_well_under_budget(self, tmp_path: Path):
        """Fast review verify runs should not emit near-timeout warnings."""
        helper_result = Mock(
            returncode=0,
            stdout="all good\n",
            stderr="",
            timed_out=False,
            forced_kill=False,
        )

        with patch("gza.runner._run_review_verify_command_with_timeout_diagnostics", return_value=helper_result), \
             patch("gza.runner.console.print") as mock_print, \
             patch("gza.runner.logger.warning") as mock_warning, \
             patch("gza.runner.time.monotonic", side_effect=[200.0, 205.0]):
            result = _run_review_verify_command(
                "printf 'all good\\n'",
                cwd=tmp_path,
                timeout_seconds=10,
            )

        rendered = _format_review_verify_result(result)
        assert result.status == "passed"
        assert "- Status: passed" in rendered
        assert "- Exit status: 0" in rendered
        mock_warning.assert_not_called()
        mock_print.assert_not_called()

    def test_run_review_verify_commands_for_cross_project_runs_each_affected_project(self, tmp_path: Path):
        project_dir = tmp_path / "services" / "foo"
        sibling_dir = tmp_path / "libs" / "bar"
        skipped_dir = tmp_path / "apps" / "baz"
        worktree_path = tmp_path / "worktree"
        worktree_project_dir = worktree_path / "services" / "foo"
        worktree_sibling_dir = worktree_path / "libs" / "bar"
        worktree_skipped_dir = worktree_path / "apps" / "baz"
        project_dir.mkdir(parents=True)
        sibling_dir.mkdir(parents=True)
        skipped_dir.mkdir(parents=True)
        worktree_project_dir.mkdir(parents=True)
        worktree_sibling_dir.mkdir(parents=True)
        worktree_skipped_dir.mkdir(parents=True)
        (project_dir / "gza.yaml").write_text("project_name: foo\nverify_command: ./bin/foo-verify\n")
        (sibling_dir / "gza.yaml").write_text("project_name: bar\nverify_command: ./bin/bar-verify\n")
        (skipped_dir / "gza.yaml").write_text("project_name: baz\n")
        (worktree_project_dir / "gza.yaml").write_text("project_name: foo\nverify_command: ./bin/foo-verify\n")
        (worktree_sibling_dir / "gza.yaml").write_text("project_name: bar\nverify_command: ./bin/bar-verify\n")
        (worktree_skipped_dir / "gza.yaml").write_text("project_name: baz\n")

        config = Config(project_dir=project_dir, project_name="foo", verify_command="./bin/foo-verify")
        config._project_boundary_cache = ProjectBoundary(
            repo_root=tmp_path,
            scope_root=Path("services/foo"),
            local_dependencies=(),
        )
        task = Task(id="gza-1", prompt="Review cross-project", status="pending", task_type="review")
        task.tags = ("cross-project",)

        worktree_git = Mock()
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_name_status.return_value = (
            "M\tservices/foo/app.py\n"
            "M\tlibs/bar/lib.py\n"
            "M\tapps/baz/view.py\n"
            "M\tmisc/tool.py\n"
        )
        with patch(
            "gza.runner._run_review_verify_command",
            side_effect=[
                ReviewVerifyResult(
                    command="./bin/foo-verify",
                    status="passed",
                    exit_status="0",
                    captured_at=datetime(2026, 1, 1, tzinfo=UTC),
                ),
                ReviewVerifyResult(
                    command="./bin/bar-verify",
                    status="failed",
                    exit_status="7",
                    captured_at=datetime(2026, 1, 1, tzinfo=UTC),
                    failure="verify failed",
                    output="bar failed",
                ),
            ],
        ) as mock_verify:
            outcome = _run_review_verify_commands_for_projects(
                config=config,
                task=task,
                worktree_git=worktree_git,
                worktree_path=worktree_path,
                timeout_seconds=120,
                timeout_grace_seconds=5.0,
                reviewed_branch="feature/cross-project",
                reviewed_head_sha="deadbeef",
                reviewed_base_sha="cafebabe",
            )

        assert outcome is not None
        assert "### services/foo" in outcome.markdown
        assert "### libs/bar" in outcome.markdown
        assert "### apps/baz" in outcome.markdown
        assert "no verify_command configured for this affected project" in outcome.markdown
        assert "outside all discovered project roots" in outcome.markdown
        assert outcome.aggregate_result.status == "failed"
        assert outcome.aggregate_result.exit_status == "1 passed, 1 failed, 0 unavailable, 2 skipped"
        assert outcome.aggregate_result.reviewed_branch == "feature/cross-project"
        assert outcome.aggregate_result.reviewed_head_sha == "deadbeef"
        assert outcome.aggregate_result.reviewed_base_sha == "cafebabe"
        verify_calls = mock_verify.call_args_list
        assert len(verify_calls) == 2
        assert verify_calls[0].kwargs["cwd"] == worktree_path / "services" / "foo"
        assert verify_calls[1].kwargs["cwd"] == worktree_path / "libs" / "bar"
        assert verify_calls[0].kwargs["timeout_grace_seconds"] == 5.0
        assert verify_calls[1].kwargs["timeout_grace_seconds"] == 5.0
        assert verify_calls[0].kwargs["reviewed_branch"] == "feature/cross-project"
        assert verify_calls[0].kwargs["reviewed_head_sha"] == "deadbeef"
        assert verify_calls[0].kwargs["reviewed_base_sha"] == "cafebabe"

    def test_run_review_verify_commands_for_cross_project_prioritizes_failed_over_unavailable(
        self, tmp_path: Path
    ):
        project_dir = tmp_path / "services" / "foo"
        sibling_dir = tmp_path / "libs" / "bar"
        unavailable_dir = tmp_path / "apps" / "baz"
        worktree_path = tmp_path / "worktree"
        worktree_project_dir = worktree_path / "services" / "foo"
        worktree_sibling_dir = worktree_path / "libs" / "bar"
        worktree_unavailable_dir = worktree_path / "apps" / "baz"
        project_dir.mkdir(parents=True)
        sibling_dir.mkdir(parents=True)
        unavailable_dir.mkdir(parents=True)
        worktree_project_dir.mkdir(parents=True)
        worktree_sibling_dir.mkdir(parents=True)
        worktree_unavailable_dir.mkdir(parents=True)
        (project_dir / "gza.yaml").write_text("project_name: foo\nverify_command: ./bin/foo-verify\n")
        (sibling_dir / "gza.yaml").write_text("project_name: bar\nverify_command: ./bin/bar-verify\n")
        (unavailable_dir / "gza.yaml").write_text("project_name: baz\nverify_command: ./bin/baz-verify\n")
        (worktree_project_dir / "gza.yaml").write_text("project_name: foo\nverify_command: ./bin/foo-verify\n")
        (worktree_sibling_dir / "gza.yaml").write_text("project_name: bar\nverify_command: ./bin/bar-verify\n")
        (worktree_unavailable_dir / "gza.yaml").write_text("project_name: baz\nverify_command: ./bin/baz-verify\n")

        config = Config(project_dir=project_dir, project_name="foo", verify_command="./bin/foo-verify")
        config._project_boundary_cache = ProjectBoundary(
            repo_root=tmp_path,
            scope_root=Path("services/foo"),
            local_dependencies=(),
        )
        task = Task(id="gza-1", prompt="Review cross-project", status="pending", task_type="review")
        task.tags = ("cross-project",)

        worktree_git = Mock()
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_name_status.return_value = (
            "M\tservices/foo/app.py\n"
            "M\tlibs/bar/lib.py\n"
            "M\tapps/baz/view.py\n"
        )

        with patch(
            "gza.runner._run_review_verify_command",
            side_effect=[
                ReviewVerifyResult(
                    command="./bin/foo-verify",
                    status="passed",
                    exit_status="0",
                    captured_at=datetime(2026, 1, 1, tzinfo=UTC),
                ),
                ReviewVerifyResult(
                    command="./bin/bar-verify",
                    status="failed",
                    exit_status="7",
                    captured_at=datetime(2026, 1, 1, tzinfo=UTC),
                    failure="verify failed",
                    output="bar failed",
                ),
                ReviewVerifyResult(
                    command="./bin/baz-verify",
                    status="unavailable",
                    exit_status="launch failed",
                    captured_at=datetime(2026, 1, 1, tzinfo=UTC),
                    failure="failed to launch verify_command: [Errno 2] No such file or directory",
                    output="failed to launch verify_command: [Errno 2] No such file or directory",
                ),
            ],
        ) as mock_verify:
            outcome = _run_review_verify_commands_for_projects(
                config=config,
                task=task,
                worktree_git=worktree_git,
                worktree_path=worktree_path,
                timeout_seconds=120,
                timeout_grace_seconds=5.0,
                reviewed_branch="feature/cross-project",
                reviewed_head_sha="deadbeef",
                reviewed_base_sha="cafebabe",
            )

        assert outcome is not None
        assert outcome.aggregate_result.status == "failed"
        assert outcome.aggregate_result.exit_status == "1 passed, 1 failed, 1 unavailable"
        assert outcome.aggregate_result.failure == "one or more affected projects failed review verification"
        verify_calls = mock_verify.call_args_list
        assert verify_calls[1].kwargs["reviewed_branch"] == "feature/cross-project"
        assert verify_calls[1].kwargs["reviewed_head_sha"] == "deadbeef"
        assert verify_calls[1].kwargs["reviewed_base_sha"] == "cafebabe"

    def test_run_review_verify_commands_for_cross_project_marks_missing_verify_command_as_unavailable(
        self, tmp_path: Path
    ):
        project_dir = tmp_path / "services" / "foo"
        skipped_dir = tmp_path / "apps" / "baz"
        worktree_path = tmp_path / "worktree"
        worktree_project_dir = worktree_path / "services" / "foo"
        worktree_skipped_dir = worktree_path / "apps" / "baz"
        project_dir.mkdir(parents=True)
        skipped_dir.mkdir(parents=True)
        worktree_project_dir.mkdir(parents=True)
        worktree_skipped_dir.mkdir(parents=True)
        (project_dir / "gza.yaml").write_text("project_name: foo\nverify_command: ./bin/foo-verify\n")
        (skipped_dir / "gza.yaml").write_text("project_name: baz\n")
        (worktree_project_dir / "gza.yaml").write_text("project_name: foo\nverify_command: ./bin/foo-verify\n")
        (worktree_skipped_dir / "gza.yaml").write_text("project_name: baz\n")

        config = Config(project_dir=project_dir, project_name="foo", verify_command="./bin/foo-verify")
        config._project_boundary_cache = ProjectBoundary(
            repo_root=tmp_path,
            scope_root=Path("services/foo"),
            local_dependencies=(),
        )
        task = Task(id="gza-1", prompt="Review cross-project", status="pending", task_type="review")
        task.tags = ("cross-project",)

        worktree_git = Mock()
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_name_status.return_value = (
            "M\tservices/foo/app.py\n"
            "M\tapps/baz/view.py\n"
        )

        with patch(
            "gza.runner._run_review_verify_command",
            return_value=ReviewVerifyResult(
                command="./bin/foo-verify",
                status="passed",
                exit_status="0",
                captured_at=datetime(2026, 1, 1, tzinfo=UTC),
            ),
        ):
            outcome = _run_review_verify_commands_for_projects(
                config=config,
                task=task,
                worktree_git=worktree_git,
                worktree_path=worktree_path,
                timeout_seconds=120,
                timeout_grace_seconds=5.0,
                reviewed_branch="feature/cross-project",
                reviewed_head_sha="deadbeef",
                reviewed_base_sha="cafebabe",
            )

        assert outcome is not None
        assert "- Status: unavailable" in outcome.markdown
        assert "- Captured at: 2026-01-01T00:00:00+00:00" in outcome.markdown
        assert "- Reviewed branch: `feature/cross-project`" in outcome.markdown
        assert "- Reviewed head: `deadbeef`" in outcome.markdown
        assert "- Reviewed base/default SHA: `cafebabe`" in outcome.markdown
        assert "### apps/baz" in outcome.markdown
        assert "no verify_command configured for this affected project" in outcome.markdown
        assert outcome.aggregate_result.status == "unavailable"
        assert outcome.aggregate_result.exit_status == "1 passed, 0 failed, 0 unavailable, 1 skipped"
        assert outcome.aggregate_result.failure == "one or more affected projects could not run review verification"

    def test_run_review_verify_commands_for_cross_project_marks_unknown_paths_as_unavailable(
        self, tmp_path: Path
    ):
        project_dir = tmp_path / "services" / "foo"
        worktree_path = tmp_path / "worktree"
        worktree_project_dir = worktree_path / "services" / "foo"
        project_dir.mkdir(parents=True)
        worktree_project_dir.mkdir(parents=True)
        (project_dir / "gza.yaml").write_text("project_name: foo\nverify_command: ./bin/foo-verify\n")
        (worktree_project_dir / "gza.yaml").write_text("project_name: foo\nverify_command: ./bin/foo-verify\n")

        config = Config(project_dir=project_dir, project_name="foo", verify_command="./bin/foo-verify")
        config._project_boundary_cache = ProjectBoundary(
            repo_root=tmp_path,
            scope_root=Path("services/foo"),
            local_dependencies=(),
        )
        task = Task(id="gza-1", prompt="Review cross-project", status="pending", task_type="review")
        task.tags = ("cross-project",)

        worktree_git = Mock()
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_name_status.return_value = (
            "M\tservices/foo/app.py\n"
            "M\tmisc/tool.py\n"
        )

        with patch(
            "gza.runner._run_review_verify_command",
            return_value=ReviewVerifyResult(
                command="./bin/foo-verify",
                status="passed",
                exit_status="0",
                captured_at=datetime(2026, 1, 1, tzinfo=UTC),
            ),
        ):
            outcome = _run_review_verify_commands_for_projects(
                config=config,
                task=task,
                worktree_git=worktree_git,
                worktree_path=worktree_path,
                timeout_seconds=120,
                timeout_grace_seconds=5.0,
                reviewed_branch="feature/cross-project",
                reviewed_head_sha="deadbeef",
                reviewed_base_sha="cafebabe",
            )

        assert outcome is not None
        assert "- Status: unavailable" in outcome.markdown
        assert "- Captured at: 2026-01-01T00:00:00+00:00" in outcome.markdown
        assert "- Reviewed branch: `feature/cross-project`" in outcome.markdown
        assert "- Reviewed head: `deadbeef`" in outcome.markdown
        assert "- Reviewed base/default SHA: `cafebabe`" in outcome.markdown
        assert "### unknown paths" in outcome.markdown
        assert "outside all discovered project roots" in outcome.markdown
        assert "- Paths: misc/tool.py" in outcome.markdown
        assert outcome.aggregate_result.status == "unavailable"
        assert outcome.aggregate_result.exit_status == "1 passed, 0 failed, 0 unavailable, 1 skipped"
        assert outcome.aggregate_result.failure == "one or more affected projects could not run review verification"

    def test_run_review_verify_commands_for_cross_project_uses_branch_local_verify_commands(self, tmp_path: Path):
        repo_root = tmp_path / "repo"
        worktree_path = tmp_path / "worktree"
        project_dir = repo_root / "services" / "foo"
        sibling_dir = repo_root / "libs" / "bar"
        worktree_project_dir = worktree_path / "services" / "foo"
        worktree_sibling_dir = worktree_path / "libs" / "bar"
        project_dir.mkdir(parents=True)
        sibling_dir.mkdir(parents=True)
        worktree_project_dir.mkdir(parents=True)
        worktree_sibling_dir.mkdir(parents=True)
        (project_dir / "gza.yaml").write_text("project_name: foo\nverify_command: ./bin/foo-verify\n")
        (sibling_dir / "gza.yaml").write_text("project_name: bar\nverify_command: ./bin/bar-verify-old\n")
        (worktree_project_dir / "gza.yaml").write_text("project_name: foo\nverify_command: ./bin/foo-verify\n")
        (worktree_sibling_dir / "gza.yaml").write_text("project_name: bar\nverify_command: ./bin/bar-verify-new\n")

        config = Config(project_dir=project_dir, project_name="foo", verify_command="./bin/foo-verify")
        config._project_boundary_cache = ProjectBoundary(
            repo_root=repo_root,
            scope_root=Path("services/foo"),
            local_dependencies=(),
        )
        task = Task(id="gza-1", prompt="Review cross-project", status="pending", task_type="review")
        task.tags = ("cross-project",)

        worktree_git = Mock()
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_name_status.return_value = "M\tlibs/bar/gza.yaml\nM\tlibs/bar/lib.py\n"

        with patch(
            "gza.runner._run_review_verify_command",
            return_value=ReviewVerifyResult(
                command="./bin/bar-verify-new",
                status="passed",
                exit_status="0",
                captured_at=datetime(2026, 1, 1, tzinfo=UTC),
            ),
        ) as mock_verify:
            outcome = _run_review_verify_commands_for_projects(
                config=config,
                task=task,
                worktree_git=worktree_git,
                worktree_path=worktree_path,
                timeout_seconds=120,
                timeout_grace_seconds=5.0,
            )

        assert outcome is not None
        assert "### libs/bar" in outcome.markdown
        mock_verify.assert_called_once()
        assert mock_verify.call_args.args[0] == "./bin/bar-verify-new"
        assert mock_verify.call_args.kwargs["cwd"] == worktree_path / "libs" / "bar"

    def test_run_review_verify_commands_for_cross_project_discovers_branch_local_project_root(
        self, tmp_path: Path
    ):
        repo_root = tmp_path / "repo"
        worktree_path = tmp_path / "worktree"
        project_dir = repo_root / "services" / "foo"
        worktree_project_dir = worktree_path / "services" / "foo"
        worktree_branch_local_dir = worktree_path / "dre" / "web"
        project_dir.mkdir(parents=True)
        worktree_project_dir.mkdir(parents=True)
        worktree_branch_local_dir.mkdir(parents=True)
        (project_dir / "gza.yaml").write_text("project_name: foo\nverify_command: ./bin/foo-verify\n")
        (worktree_project_dir / "gza.yaml").write_text("project_name: foo\nverify_command: ./bin/foo-verify\n")
        (worktree_branch_local_dir / "gza.yaml").write_text("project_name: dre-web\nverify_command: ./bin/web-verify\n")

        config = Config(project_dir=project_dir, project_name="foo", verify_command="")
        config._project_boundary_cache = ProjectBoundary(
            repo_root=repo_root,
            scope_root=Path("services/foo"),
            local_dependencies=(),
        )
        task = Task(id="gza-1", prompt="Review cross-project", status="pending", task_type="review")
        task.tags = ("cross-project",)

        worktree_git = Mock()
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_name_status.return_value = "A\tdre/web/gza.yaml\nM\tdre/web/src/app.tsx\n"

        with patch(
            "gza.runner._run_review_verify_command",
            return_value=ReviewVerifyResult(
                command="./bin/web-verify",
                status="passed",
                exit_status="0",
                captured_at=datetime(2026, 1, 1, tzinfo=UTC),
            ),
        ) as mock_verify:
            outcome = _run_review_verify_commands_for_projects(
                config=config,
                task=task,
                worktree_git=worktree_git,
                worktree_path=worktree_path,
                timeout_seconds=120,
                timeout_grace_seconds=5.0,
            )

        assert outcome is not None
        assert "### dre/web" in outcome.markdown
        mock_verify.assert_called_once()
        assert mock_verify.call_args.args[0] == "./bin/web-verify"
        assert mock_verify.call_args.kwargs["cwd"] == worktree_path / "dre" / "web"

    def test_run_review_verify_commands_for_cross_project_includes_rename_source_and_destination_projects(
        self, tmp_path: Path
    ):
        repo_root = tmp_path / "repo"
        worktree_path = tmp_path / "worktree"
        project_dir = repo_root / "services" / "foo"
        copied_dir = worktree_path / "libs" / "copied"
        old_dir = worktree_path / "libs" / "old"
        renamed_dir = worktree_path / "libs" / "renamed"
        worktree_project_dir = worktree_path / "services" / "foo"
        project_dir.mkdir(parents=True)
        copied_dir.mkdir(parents=True)
        old_dir.mkdir(parents=True)
        renamed_dir.mkdir(parents=True)
        worktree_project_dir.mkdir(parents=True)
        (project_dir / "gza.yaml").write_text("project_name: foo\nverify_command: ./bin/foo-verify\n")
        (worktree_project_dir / "gza.yaml").write_text("project_name: foo\nverify_command: ./bin/foo-verify\n")
        (copied_dir / "gza.yaml").write_text("project_name: copied\nverify_command: ./bin/copied-verify\n")
        (old_dir / "gza.yaml").write_text("project_name: old\nverify_command: ./bin/old-verify\n")
        (renamed_dir / "gza.yaml").write_text("project_name: renamed\nverify_command: ./bin/renamed-verify\n")

        config = Config(project_dir=project_dir, project_name="foo", verify_command="./bin/foo-verify")
        config._project_boundary_cache = ProjectBoundary(
            repo_root=repo_root,
            scope_root=Path("services/foo"),
            local_dependencies=(),
        )
        task = Task(id="gza-1", prompt="Review cross-project", status="pending", task_type="review")
        task.tags = ("cross-project",)

        worktree_git = Mock()
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_name_status.return_value = (
            "M\tservices/foo/app.py\n"
            "R100\tlibs/old/src/file.py\tlibs/renamed/src/file.py\n"
            "C100\tlibs/template/gza.yaml\tlibs/copied/gza.yaml\n"
            "C100\tlibs/template/src/file.py\tlibs/copied/src/file.py\n"
            "D\tapps/removed/gza.yaml\n"
        )

        with patch(
            "gza.runner._run_review_verify_command",
            side_effect=[
                ReviewVerifyResult(
                    command="./bin/foo-verify",
                    status="passed",
                    exit_status="0",
                    captured_at=datetime(2026, 1, 1, tzinfo=UTC),
                ),
                ReviewVerifyResult(
                    command="./bin/copied-verify",
                    status="passed",
                    exit_status="0",
                    captured_at=datetime(2026, 1, 1, tzinfo=UTC),
                ),
                ReviewVerifyResult(
                    command="./bin/old-verify",
                    status="passed",
                    exit_status="0",
                    captured_at=datetime(2026, 1, 1, tzinfo=UTC),
                ),
                ReviewVerifyResult(
                    command="./bin/renamed-verify",
                    status="passed",
                    exit_status="0",
                    captured_at=datetime(2026, 1, 1, tzinfo=UTC),
                ),
            ],
        ) as mock_verify:
            outcome = _run_review_verify_commands_for_projects(
                config=config,
                task=task,
                worktree_git=worktree_git,
                worktree_path=worktree_path,
                timeout_seconds=120,
                timeout_grace_seconds=5.0,
            )

        assert outcome is not None
        assert "### services/foo" in outcome.markdown
        assert "### libs/copied" in outcome.markdown
        assert "### libs/old" in outcome.markdown
        assert "### libs/renamed" in outcome.markdown
        assert "apps/removed/gza.yaml" in outcome.markdown
        verify_calls = mock_verify.call_args_list
        assert len(verify_calls) == 4
        assert verify_calls[0].kwargs["cwd"] == worktree_path / "services" / "foo"
        assert verify_calls[1].kwargs["cwd"] == worktree_path / "libs" / "copied"
        assert verify_calls[2].kwargs["cwd"] == worktree_path / "libs" / "old"
        assert verify_calls[3].kwargs["cwd"] == worktree_path / "libs" / "renamed"

    def test_review_context_includes_changed_files_diffstat_and_diff(self, tmp_path: Path):
        """Review context should include changed files, diffstat, and inline diff."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test/feature-branch"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review implementation",
            task_type="review",
            depends_on=impl_task.id,
        )

        mock_git = Mock(spec=Git)
        mock_git.default_branch.return_value = "main"
        mock_git.merge_base.return_value = "abc1234base"
        mock_git.rev_parse.side_effect = lambda ref: {
            "main": "def5678main",
            "test/feature-branch": "fed4321head",
        }[ref]
        mock_git.get_diff_numstat.return_value = "10\t2\tsrc/a.py\n3\t1\tsrc/b.py\n"
        mock_git.get_diff_stat.return_value = (
            " src/a.py | 12 ++++++++++\n src/b.py | 4 +++-\n 2 files changed, 13 insertions(+), 3 deletions(-)"
        )
        mock_git.get_diff.return_value = "diff --git a/src/a.py b/src/a.py\n@@ -1 +1 @@\n-old\n+new\n"

        context = _build_context_from_chain(review_task, store, tmp_path, mock_git)

        assert "## Implementation Diff Context" in context
        assert "Implementation head: test/feature-branch (fed4321head)" in context
        assert "Local default branch: main (def5678main)" in context
        assert "Review base (merge-base): abc1234base" in context
        assert "Revision range: main...test/feature-branch" in context
        assert "Changed files:" in context
        assert "- src/a.py" in context
        assert "- src/b.py" in context
        assert "Diff summary:" in context
        assert "Full diff:" in context
        assert "Use git and file reading tools" not in context

    def test_review_context_large_diff_adds_targeted_excerpts(self, tmp_path: Path):
        """Large review diffs should include targeted diff excerpts."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement large feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test/large-branch"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review large implementation",
            task_type="review",
            depends_on=impl_task.id,
        )

        mock_git = Mock(spec=Git)
        mock_git.default_branch.return_value = "main"
        mock_git.get_diff_numstat.return_value = (
            "1500\t400\tsrc/large_a.py\n800\t200\tsrc/large_b.py\n"
        )
        mock_git.get_diff_stat.return_value = " 2 files changed, 2300 insertions(+), 600 deletions(-)"
        mock_git._run.return_value = Mock(stdout="diff --git a/src/large_a.py b/src/large_a.py\n@@ ...", returncode=0)

        context = _build_context_from_chain(review_task, store, tmp_path, mock_git)

        assert "Targeted diff excerpts" in context
        assert "Additional changed files not expanded inline" not in context

    def test_review_context_uses_configurable_thresholds_and_file_limit(self, tmp_path: Path):
        """Review context should honor config-driven diff thresholds and excerpt file cap."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement configurable thresholds", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test/config-thresholds-branch"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review configurable thresholds implementation",
            task_type="review",
            depends_on=impl_task.id,
        )

        config = Config(
            project_dir=tmp_path,
            project_name="test-project",
            review_diff_small_threshold=1,
            review_diff_medium_threshold=2,
            review_context_file_limit=1,
        )

        mock_git = Mock(spec=Git)
        mock_git.default_branch.return_value = "main"
        # total_lines=3 should be treated as large with thresholds above
        mock_git.get_diff_numstat.return_value = "2\t1\tsrc/a.py\n0\t0\tsrc/b.py\n"
        mock_git.get_diff_stat.return_value = " 2 files changed, 2 insertions(+), 1 deletion(-)"
        mock_git._run.return_value = Mock(stdout="diff --git a/src/a.py b/src/a.py\n@@ ...", returncode=0)

        context = _build_context_from_chain(review_task, store, tmp_path, mock_git, config=config)

        assert "Targeted diff excerpts" in context
        assert "Additional changed files not expanded inline: 1" in context
        mock_git.get_diff.assert_not_called()

    def test_review_context_includes_metadata_only_improve_lineage(self, tmp_path: Path):
        """Review context includes metadata-only rows for prior improve runs."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        review1 = store.add(prompt="Review 1", task_type="review", depends_on=impl_task.id)
        review1.status = "completed"
        store.update(review1)

        improve1 = store.add(
            prompt="Improve 1",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review1.id,
        )
        improve1.status = "completed"
        improve1.output_content = (
            "# Summary\n"
            "- Fix flaky tests\n"
            "- Tighten input validation\n"
            "- Keep this concise\n"
        )
        store.update(improve1)

        review2 = store.add(prompt="Review 2", task_type="review", depends_on=impl_task.id)
        review2.status = "completed"
        store.update(review2)

        improve2 = store.add(
            prompt="Improve 2",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review2.id,
        )
        improve2.status = "completed"
        improve2.slug = "20260227-improve-2"
        store.update(improve2)

        summary_dir = tmp_path / ".gza" / "summaries"
        summary_dir.mkdir(parents=True, exist_ok=True)
        (summary_dir / f"{improve2.slug}.md").write_text(
            "# What was accomplished\n- Reduced retry loops\n- Added guardrails\n"
        )

        review3 = store.add(prompt="Review latest", task_type="review", depends_on=impl_task.id)

        context = _build_context_from_chain(review3, store, tmp_path, git=None)

        assert "## Improve Lineage Context" in context
        assert f"iteration 2: review {review2.id}" in context
        assert f"iteration 1: review {review1.id}" in context
        assert f"improve {improve1.id}" in context
        assert f"improve {improve2.id}" in context
        assert "Fix flaky tests" not in context
        assert "Reduced retry loops" not in context

    def test_review_context_bounds_improve_lineage_and_reports_omitted(self, tmp_path: Path):
        """Review context includes only recent iteration rows and reports omitted count."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement bounded lineage", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        improve_ids = []
        for idx in range(REVIEW_IMPROVE_LINEAGE_LIMIT + 2):
            review = store.add(prompt=f"Review {idx}", task_type="review", depends_on=impl_task.id)
            review.status = "completed"
            store.update(review)
            improve = store.add(
                prompt=f"Improve {idx}",
                task_type="improve",
                based_on=impl_task.id,
                depends_on=review.id,
            )
            improve.status = "completed"
            improve.output_content = f"- Improve summary {idx}\n"
            store.update(improve)
            improve_ids.append(improve.id)

        current_review = store.add(prompt="Review now", task_type="review", depends_on=impl_task.id)
        context = _build_context_from_chain(current_review, store, tmp_path, git=None)

        assert "## Improve Lineage Context" in context
        assert f"prior iterations: {REVIEW_IMPROVE_LINEAGE_LIMIT + 2}" in context
        assert "older iterations omitted: 2" in context

        for improve_id in improve_ids[-REVIEW_IMPROVE_LINEAGE_LIMIT:]:
            assert f"improve {improve_id}" in context

        for improve_id in improve_ids[: len(improve_ids) - REVIEW_IMPROVE_LINEAGE_LIMIT]:
            assert f"improve {improve_id}" not in context

        omitted_count = len(improve_ids) - REVIEW_IMPROVE_LINEAGE_LIMIT
        for idx in range(omitted_count):
            assert f"Improve summary {idx}" not in context

    def test_review_context_includes_retry_improves_in_same_chain(self, tmp_path: Path):
        """Review context includes retry/resume improve attempts as metadata-only iteration rows."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement retry-aware lineage", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        review1 = store.add(prompt="Review 1", task_type="review", depends_on=impl_task.id)
        review1.status = "completed"
        store.update(review1)
        improve_a = store.add(
            prompt="Improve A",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review1.id,
        )
        improve_a.status = "completed"
        improve_a.output_content = "- Direct improve"
        store.update(improve_a)

        review2 = store.add(prompt="Review 2", task_type="review", depends_on=impl_task.id)
        review2.status = "completed"
        store.update(review2)
        improve_b = store.add(
            prompt="Improve B",
            task_type="improve",
            based_on=improve_a.id,
            depends_on=review2.id,
        )
        improve_b.status = "completed"
        improve_b.output_content = "- Retry improve"
        store.update(improve_b)

        current_review = store.add(prompt="Review current", task_type="review", depends_on=impl_task.id)
        context = _build_context_from_chain(current_review, store, tmp_path, git=None)

        assert "## Improve Lineage Context" in context
        assert f"iteration 2: review {review2.id}" in context
        assert f"iteration 1: review {review1.id}" in context
        assert f"improve {improve_a.id}" in context
        assert f"improve {improve_b.id}" in context
        assert "Direct improve" not in context
        assert "Retry improve" not in context

    def test_review_context_bounds_mixed_direct_and_retry_improves(self, tmp_path: Path):
        """Bounded lineage remains correct with mixed direct and retry/resume improve attempts."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement mixed lineage", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        improve_ids: list[str] = []
        parent_improve_id: str | None = None
        for idx in range(REVIEW_IMPROVE_LINEAGE_LIMIT + 2):
            review = store.add(prompt=f"Review {idx}", task_type="review", depends_on=impl_task.id)
            review.status = "completed"
            store.update(review)
            based_on = impl_task.id if idx % 2 == 0 else parent_improve_id
            improve = store.add(
                prompt=f"Improve {idx}",
                task_type="improve",
                based_on=based_on,
                depends_on=review.id,
            )
            improve.status = "completed"
            improve.output_content = f"- Mixed improve summary {idx}\n"
            store.update(improve)
            assert improve.id is not None
            improve_ids.append(improve.id)
            parent_improve_id = improve.id

        current_review = store.add(prompt="Review now", task_type="review", depends_on=impl_task.id)
        context = _build_context_from_chain(current_review, store, tmp_path, git=None)

        assert "## Improve Lineage Context" in context
        assert f"prior iterations: {REVIEW_IMPROVE_LINEAGE_LIMIT + 2}" in context
        assert "older iterations omitted: 2" in context

        for improve_id in improve_ids[-REVIEW_IMPROVE_LINEAGE_LIMIT:]:
            assert f"improve {improve_id}" in context

        for improve_id in improve_ids[: len(improve_ids) - REVIEW_IMPROVE_LINEAGE_LIMIT]:
            assert f"improve {improve_id}" not in context

        omitted_count = len(improve_ids) - REVIEW_IMPROVE_LINEAGE_LIMIT
        for idx in range(omitted_count):
            assert f"Mixed improve summary {idx}" not in context

    def test_review_context_excludes_equal_timestamp_later_improve(self, tmp_path: Path):
        """Equal-timestamp improves created after the review are excluded."""
        created_at = datetime(2026, 2, 27, 5, 0, 0, tzinfo=UTC)
        impl_task = Task(id="gza-100", prompt="Implement", task_type="implement", status="completed")
        review_task = Task(
            id="gza-50",
            prompt="Review current",
            task_type="review",
            depends_on=impl_task.id,
            created_at=created_at,
        )

        older_improve = Task(
            id="gza-40",
            prompt="Improve older",
            task_type="improve",
            status="completed",
            based_on=impl_task.id,
            depends_on="gza-10",
            created_at=created_at,
            output_content="- older improve",
        )
        later_improve = Task(
            id="gza-60",
            prompt="Improve later",
            task_type="improve",
            status="completed",
            based_on=impl_task.id,
            depends_on="gza-11",
            created_at=created_at,
            output_content="- later improve",
        )

        store = Mock(spec=SqliteTaskStore)
        store.get_all.return_value = [older_improve, later_improve]
        store.get.return_value = None

        context = _build_review_improve_lineage_context(review_task, impl_task, store, tmp_path)

        assert f"improve {older_improve.id}" in context
        assert "iteration 1:" in context
        assert f"Improve {later_improve.id}" not in context
        assert "later improve" not in context

    def test_review_context_numeric_ordering_beats_lexicographic(self, tmp_path: Path):
        """Variable-width decimal IDs sort numerically, not lexicographically."""
        created_at = datetime(2026, 2, 27, 5, 0, 0, tzinfo=UTC)
        # review=10, older=9, later=11 should include only older.
        impl_task = Task(id="gza-20", prompt="Implement", task_type="implement", status="completed")
        review_task = Task(
            id="gza-10",
            prompt="Review current",
            task_type="review",
            depends_on=impl_task.id,
            created_at=created_at,
        )

        older_improve = Task(
            id="gza-9",
            prompt="Improve older",
            task_type="improve",
            status="completed",
            based_on=impl_task.id,
            depends_on="gza-8",
            created_at=created_at,
            output_content="- older improve 9",
        )
        later_improve = Task(
            id="gza-11",
            prompt="Improve later",
            task_type="improve",
            status="completed",
            based_on=impl_task.id,
            depends_on="gza-12",
            created_at=created_at,
            output_content="- later improve 11",
        )

        store = Mock(spec=SqliteTaskStore)
        store.get_all.return_value = [older_improve, later_improve]
        store.get.return_value = None

        context = _build_review_improve_lineage_context(review_task, impl_task, store, tmp_path)

        assert f"improve {older_improve.id}" in context
        assert "iteration 1:" in context
        assert f"Improve {later_improve.id}" not in context
        assert "later improve 11" not in context

    def test_review_context_includes_metadata_only_lineage_when_prior_iterations_exist(self, tmp_path: Path):
        """Review context includes metadata-only lineage when prior review/improve iterations exist."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement with hints", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        review1 = store.add(prompt="Review 1", task_type="review", depends_on=impl_task.id)
        review1.status = "completed"
        store.update(review1)

        improve1 = store.add(
            prompt="Improve 1",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review1.id,
        )
        improve1.status = "completed"
        improve1.output_content = "- Fixed the issue\n"
        store.update(improve1)

        current_review = store.add(prompt="Review current", task_type="review", depends_on=impl_task.id)
        context = _build_context_from_chain(current_review, store, tmp_path, git=None)

        assert "Prior iteration history is coordination context only" in context
        assert "Current state: prior iterations: 1" in context
        assert f"latest review: {review1.id}" in context
        assert f"latest improve: {improve1.id}" in context
        assert "uv run gza show <id>" not in context
        assert "cat <report_file>" not in context

    def test_review_context_includes_bounded_iteration_rows_without_summary_prose(self, tmp_path: Path):
        """Review context includes bounded iteration rows and excludes prior improve summary prose."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement for chain", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        review1 = store.add(prompt="Review 1", task_type="review", depends_on=impl_task.id)
        review1.status = "completed"
        store.update(review1)

        improve1 = store.add(
            prompt="Improve 1",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review1.id,
        )
        improve1.status = "completed"
        improve1.output_content = "- Round 1 fix\n"
        improve1.completed_at = datetime(2026, 2, 12, 12, 0, 0, tzinfo=UTC)
        store.update(improve1)

        review2 = store.add(prompt="Review 2", task_type="review", depends_on=impl_task.id)
        review2.status = "completed"
        store.update(review2)

        improve2 = store.add(
            prompt="Improve 2",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review2.id,
        )
        improve2.status = "completed"
        improve2.output_content = "- Round 2 fix\n"
        improve2.completed_at = datetime(2026, 2, 13, 12, 0, 0, tzinfo=UTC)
        store.update(improve2)

        current_review = store.add(prompt="Review current", task_type="review", depends_on=impl_task.id)
        context = _build_context_from_chain(current_review, store, tmp_path, git=None)

        assert f"iteration 2: review {review2.id}" in context
        assert f"iteration 1: review {review1.id}" in context
        assert f"improve {improve1.id}" in context
        assert f"improve {improve2.id}" in context
        assert "Round 1 fix" not in context
        assert "Round 2 fix" not in context
        assert "Lineage:" not in context
        assert "prior iterations: 2" in context

    def test_improve_context_marks_unavailable_review_feedback(self, tmp_path: Path):
        """Improve context marks review-feedback unavailability as a blocker."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review feature",
            task_type="review",
            depends_on=impl_task.id,
        )
        review_task.status = "completed"
        store.update(review_task)

        improve_task = store.add(
            prompt="Improve feature",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review_task.id,
        )

        with patch("gza.runner._get_task_output", return_value=None):
            context = _build_context_from_chain(improve_task, store, tmp_path, git=None)

        assert "## Review feedback to address:" in context
        assert f"review task {review_task.id} exists but content unavailable" in context
        assert "flag as blocker" in context

    def test_improve_context_includes_verify_timeout_guidance_for_timeout_only_review(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review feature",
            task_type="review",
            depends_on=impl_task.id,
        )
        review_task.status = "completed"
        review_task.output_content = (
            "## Summary\n\n- Verify timed out.\n\n"
            "## Blockers\n\n"
            "### B1 verify_command failure: full verification timed out\n"
            "Evidence: lifecycle verify timed out at `120s` while running `./bin/tests`.\n"
            "Open-state citation: `bin/tests:150-155`\n"
            "Impact: the branch cannot be verified autonomously.\n"
            "Required fix: investigate the test-performance regression or prove the timeout is environmental.\n"
            "Required tests: rerun the exact verify command and add a narrow regression if this branch caused the slowdown.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        store.update(review_task)

        improve_task = store.add(
            prompt="Improve feature",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review_task.id,
        )

        context = _build_context_from_chain(improve_task, store, tmp_path, git=None)

        assert "## Verify Timeout Guidance" in context
        assert "Treat this as a test-performance investigation first" in context
        assert "Inspect the captured `## verify_command result`, trimmed output, and any referenced `verify_command_output` artifact" in context
        assert "run a narrower configured subset or harness-specific diagnostic mode" in context
        assert "Use diagnostics that fit the configured harness for this project" in context
        assert "do not silently relax suite-wide guardrails or change `verify_timeout`" in context
        assert "pytest" not in context
        assert "--durations" not in context
        assert "uv run pytest" not in context

    def test_improve_context_includes_verify_timeout_guidance_for_evidence_only_timeout_review(
        self, tmp_path: Path
    ):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review feature",
            task_type="review",
            depends_on=impl_task.id,
        )
        review_task.status = "completed"
        review_task.output_content = (
            "## Summary\n\n- Verify timed out.\n\n"
            "## Blockers\n\n"
            "### B1 verify_command failure\n"
            "Evidence: Failure: verify_command timed out after 120s while running the configured suite.\n"
            "Open-state citation: `gza.yaml:5`\n"
            "Impact: the branch cannot be considered verified.\n"
            "Required fix: investigate the test-performance regression.\n"
            "Required tests: rerun the exact configured verify_command after narrowing the slowdown.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        store.update(review_task)

        improve_task = store.add(
            prompt="Improve feature",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review_task.id,
        )

        context = _build_context_from_chain(improve_task, store, tmp_path, git=None)

        assert "## Verify Timeout Guidance" in context
        assert "Treat this as a test-performance investigation first" in context
        assert "Captured stdout/stderr may already include slow-phase summaries or SIGTERM-triggered stack dumps" in context
        assert "Inspect the captured `## verify_command result`" in context
        assert "run a narrower configured subset or harness-specific diagnostic mode" in context
        assert "pytest" not in context
        assert "--durations" not in context
        assert "uv run pytest" not in context

    def test_improve_context_excludes_verify_timeout_guidance_for_code_blocker_review(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review feature",
            task_type="review",
            depends_on=impl_task.id,
        )
        review_task.status = "completed"
        review_task.output_content = (
            "## Summary\n\n- Validation missing.\n\n"
            "## Blockers\n\n"
            "### B1 Missing input validation\n"
            "Evidence: request path still accepts malformed IDs.\n"
            "Open-state citation: `src/gza/api.py:14`\n"
            "Impact: malformed requests still crash.\n"
            "Required fix: validate IDs before parsing.\n"
            "Required tests: add malformed-ID regression coverage.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        store.update(review_task)

        improve_task = store.add(
            prompt="Improve feature",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review_task.id,
        )

        context = _build_context_from_chain(improve_task, store, tmp_path, git=None)

        assert "## Verify Timeout Guidance" not in context

    def test_improve_context_excludes_verify_timeout_guidance_for_structured_code_blocker_with_timeout_evidence(
        self, tmp_path: Path
    ):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review feature",
            task_type="review",
            depends_on=impl_task.id,
        )
        review_task.status = "completed"
        review_task.output_content = (
            "## Summary\n\n- Validation missing and verify rerun timed out.\n\n"
            "## Blockers\n\n"
            "### B1 Missing input validation\n"
            "Evidence: request path still accepts malformed IDs.\n"
            "Open-state citation: `src/gza/api.py:14`\n"
            "Impact: malformed requests still crash.\n"
            "Required fix: validate IDs before parsing.\n"
            "Required tests: add malformed-ID regression coverage, then rerun the exact verify command because "
            "verify_command timed out after 120s during review.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        store.update(review_task)

        improve_task = store.add(
            prompt="Improve feature",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review_task.id,
        )

        context = _build_context_from_chain(improve_task, store, tmp_path, git=None)

        assert "## Verify Timeout Guidance" not in context

    def test_improve_context_excludes_verify_timeout_guidance_for_unstructured_mixed_review(
        self, tmp_path: Path
    ):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review feature",
            task_type="review",
            depends_on=impl_task.id,
        )
        review_task.status = "completed"
        review_task.output_content = (
            "## Summary\n\n- Mixed blockers.\n\n"
            "## Blockers\n\n"
            "- verify_command timed out after 120s\n"
            "- Missing validation still crashes malformed IDs\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        store.update(review_task)

        improve_task = store.add(
            prompt="Improve feature",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review_task.id,
        )

        context = _build_context_from_chain(improve_task, store, tmp_path, git=None)

        assert "## Verify Timeout Guidance" not in context

    def test_improve_context_includes_unresolved_comments(self, tmp_path: Path):
        """Improve context should include unresolved feedback comments for the implementation task."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)
        assert impl_task.id is not None

        review_task = store.add(
            prompt="Review feature",
            task_type="review",
            depends_on=impl_task.id,
        )
        review_task.status = "completed"
        review_task.output_content = "Requested changes"
        store.update(review_task)
        assert review_task.id is not None

        store.add_comment(impl_task.id, "Please harden input validation.", source="direct", author="alice")
        store.add_comment(impl_task.id, "Scope note only", source="github", kind="review_scope")

        improve_task = store.add(
            prompt="Improve feature",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review_task.id,
        )

        context = _build_context_from_chain(improve_task, store, tmp_path, git=None)

        assert "## Comments:" in context
        assert "source=direct, author=alice" in context
        assert "Please harden input validation." in context
        assert "Scope note only" not in context

    def test_improve_context_excludes_comments_added_after_improve_creation(self, tmp_path: Path):
        """Improve context should include only unresolved comments present at improve creation time."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)
        assert impl_task.id is not None

        store.add_comment(impl_task.id, "Comment in improve snapshot", source="direct")
        first_comment = store.get_comments(impl_task.id, unresolved_only=True)[0]

        improve_task = store.add(
            prompt="Improve feature",
            task_type="improve",
            based_on=impl_task.id,
        )
        improve_task.created_at = first_comment.created_at
        store.update(improve_task)

        store.add_comment(impl_task.id, "Comment added after improve creation", source="direct")

        context = _build_context_from_chain(improve_task, store, tmp_path, git=None)

        assert "## Comments:" in context
        assert "Comment in improve snapshot" in context
        assert "Comment added after improve creation" not in context

    def test_improve_retry_context_reads_comments_from_implementation_ancestor(self, tmp_path: Path):
        """Retry/resume improves should still include unresolved comments from the root implementation."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)
        assert impl_task.id is not None

        review_task = store.add(
            prompt="Review feature",
            task_type="review",
            depends_on=impl_task.id,
        )
        review_task.status = "completed"
        review_task.output_content = "Requested changes"
        store.update(review_task)
        assert review_task.id is not None

        store.add_comment(impl_task.id, "Please keep this in retry context.", source="direct")

        improve_1 = store.add(
            prompt="Improve feature",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review_task.id,
        )
        assert improve_1.id is not None
        store.add_comment(improve_1.id, "Comment on improve task should not be used.", source="direct")

        improve_retry = store.add(
            prompt="Retry improve feature",
            task_type="improve",
            based_on=improve_1.id,
            depends_on=review_task.id,
        )

        context = _build_context_from_chain(improve_retry, store, tmp_path, git=None)

        assert "## Comments:" in context
        assert "Please keep this in retry context." in context
        assert "Comment on improve task should not be used." not in context

    def test_followup_implement_context_includes_parent_finding_details(self, tmp_path: Path):
        """Auto-created follow-up implement prompts include full finding context and tests."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        plan_task = store.add(prompt="Plan auth changes", task_type="plan")
        plan_task.output_content = "# Plan\nShip auth flow with validation."
        store.update(plan_task)

        impl_task = store.add(prompt="Implement auth flow", task_type="implement", based_on=plan_task.id)
        impl_task.status = "completed"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review auth flow",
            task_type="review",
            depends_on=impl_task.id,
            based_on=impl_task.id,
        )
        review_task.status = "completed"
        review_task.output_content = (
            "## Summary\n\n"
            "Looks good overall.\n\n"
            "## Blockers\n\n"
            "None.\n\n"
            "## Follow-Ups\n\n"
            "### F1 Harden malformed optional claims\n"
            "Evidence: malformed optional claim can bypass normalization.\n"
            "Impact: edge-case hardening gap in validation.\n"
            "Recommended follow-up: normalize optional claim parsing.\n"
            "Recommended tests: add malformed optional-claim regression tests.\n\n"
            "## Questions / Assumptions\n\n"
            "None.\n\n"
            "## Verdict\n\n"
            "Verdict: APPROVED_WITH_FOLLOWUPS\n"
        )
        store.update(review_task)

        parsed = parse_review_report(review_task.output_content)
        followup_finding = next(item for item in parsed.findings if item.severity == "FOLLOWUP")
        followup_task, created_now = create_or_reuse_followup_task(
            store,
            review_task=review_task,
            impl_task=impl_task,
            finding=followup_finding,
            trigger_source="manual",
        )
        assert created_now is True

        context = _build_context_from_chain(followup_task, store, tmp_path, git=None)
        assert "## Follow-up finding to implement:" in context
        assert "### F1 Harden malformed optional claims" in context
        assert "Recommended tests: add malformed optional-claim regression tests." in context

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        prompt = build_prompt(followup_task, config, store, git=None)
        assert "## Follow-up finding to implement:" in prompt
        assert "Recommended tests: add malformed optional-claim regression tests." in prompt

    def test_first_review_has_no_tool_hints(self, tmp_path: Path):
        """First-time review (no prior cycles) does not include tool hints or lineage."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement fresh", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        first_review = store.add(prompt="Review first", task_type="review", depends_on=impl_task.id)
        context = _build_context_from_chain(first_review, store, tmp_path, git=None)

        assert "prior review/improve iteration" not in context
        assert "Current state:" not in context

    def test_fix_context_includes_repeated_blockers_and_latest_failed_attempt(self, tmp_path: Path):
        """Fix context includes repeated blockers and failed improve/resume lineage evidence."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement resilient retries", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "feature/retries"
        store.update(impl_task)

        review1 = store.add(prompt="Review 1", task_type="review", depends_on=impl_task.id)
        review1.status = "completed"
        review1.output_content = (
            "## Must-Fix\n\n"
            "### M1: Missing timeout handling\n"
            "Impact: retries can hang forever.\n"
            "Required fix: add bounded timeout and propagate cancellation.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        store.update(review1)

        review2 = store.add(prompt="Review 2", task_type="review", depends_on=impl_task.id)
        review2.status = "completed"
        review2.output_content = (
            "## Must-Fix\n\n"
            "### M1: Missing timeout handling\n"
            "Impact: retries can hang forever.\n"
            "Required fix: add bounded timeout and propagate cancellation.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        store.update(review2)

        improve = store.add(
            prompt="Improve attempt",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review2.id,
            same_branch=True,
        )
        improve.status = "failed"
        improve.failure_reason = "MAX_STEPS"
        improve.log_file = ".gza/logs/fix-attempt.log"
        store.update(improve)

        log_path = tmp_path / ".gza" / "logs"
        log_path.mkdir(parents=True, exist_ok=True)
        (log_path / "fix-attempt.log").write_text("line1\nline2\nline3\n")

        fix_task = store.add(
            prompt="Rescue stuck implementation",
            task_type="fix",
            based_on=impl_task.id,
            depends_on=review2.id,
            same_branch=True,
        )

        context = _build_context_from_chain(fix_task, store, tmp_path, git=None)

        assert "## Fix Rescue Context" in context
        assert f"Root implementation: {impl_task.id}" in context
        assert "Latest completed review:" in context
        assert "## Repeated Blockers" in context
        assert "add bounded timeout and propagate cancellation" in context
        assert f"Latest failed improve/resume attempt: {improve.id}" in context
        assert "line1" in context
        assert "## Original request:" in context
        assert "## Original plan:" not in context

    def test_fix_context_includes_original_plan_when_root_impl_is_plan_backed(self, tmp_path: Path):
        """Fix context must include original plan (not request) when the implementation has a plan ancestor."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        plan_task = store.add(prompt="Create rollout plan", task_type="plan")
        plan_task.output_content = "# Plan\n1. Add retries.\n2. Add bounded timeout."
        store.update(plan_task)

        impl_task = store.add(
            prompt="Implement retry behavior",
            task_type="implement",
            based_on=plan_task.id,
        )
        impl_task.status = "completed"
        store.update(impl_task)

        review = store.add(prompt="Review retries", task_type="review", depends_on=impl_task.id)
        review.status = "completed"
        review.output_content = "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        store.update(review)

        fix_task = store.add(
            prompt="Rescue retries implementation",
            task_type="fix",
            based_on=impl_task.id,
            depends_on=review.id,
            same_branch=True,
        )

        context = _build_context_from_chain(fix_task, store, tmp_path, git=None)

        assert "## Original plan:" in context
        assert "Add bounded timeout." in context
        assert "## Original request:" not in context

    def test_fix_context_falls_back_to_original_request_when_no_plan_exists(self, tmp_path: Path):
        """Fix context should include the root implementation request when no plan ancestor exists."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement parser rescue path", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        review = store.add(prompt="Review parser rescue", task_type="review", depends_on=impl_task.id)
        review.status = "completed"
        review.output_content = "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        store.update(review)

        fix_task = store.add(
            prompt="Rescue parser implementation",
            task_type="fix",
            based_on=impl_task.id,
            depends_on=review.id,
            same_branch=True,
        )

        context = _build_context_from_chain(fix_task, store, tmp_path, git=None)

        assert "## Original request:" in context
        assert "Implement parser rescue path" in context
        assert "## Original plan:" not in context

    def test_fix_context_omits_repeated_blockers_when_not_repeated(self, tmp_path: Path):
        """Fix context omits repeated-blocker section when recent reviews do not repeat blockers."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement parsing", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        review1 = store.add(prompt="Review 1", task_type="review", depends_on=impl_task.id)
        review1.status = "completed"
        review1.output_content = (
            "## Must-Fix\n\n"
            "### M1: Missing validation\n"
            "Required fix: validate empty input.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        store.update(review1)

        review2 = store.add(prompt="Review 2", task_type="review", depends_on=impl_task.id)
        review2.status = "completed"
        review2.output_content = (
            "## Must-Fix\n\n"
            "### M2: Missing error mapping\n"
            "Required fix: return typed parsing errors.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        store.update(review2)

        fix_task = store.add(
            prompt="Rescue parser task",
            task_type="fix",
            based_on=impl_task.id,
            depends_on=review2.id,
            same_branch=True,
        )

        context = _build_context_from_chain(fix_task, store, tmp_path, git=None)

        assert "## Fix Rescue Context" in context
        assert "## Repeated Blockers" not in context

    def test_fix_context_repeated_blockers_read_from_review_report_file(self, tmp_path: Path):
        """Repeated blocker extraction should read review content from report files when DB output is empty."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement report fallback", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        reports_dir = tmp_path / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)

        report_body = (
            "## Blockers\n\n"
            "### B1 Missing timeout\n"
            "Evidence: hangs under retry storms.\n"
            "Impact: requests can block forever.\n"
            "Required fix: add bounded timeout and cancellation propagation.\n"
            "Required tests: timeout + cancellation regression.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        (reports_dir / "review-1.md").write_text(report_body)
        (reports_dir / "review-2.md").write_text(report_body)

        review1 = store.add(prompt="Review 1", task_type="review", depends_on=impl_task.id)
        review1.status = "completed"
        review1.report_file = "reports/review-1.md"
        review1.output_content = None
        store.update(review1)

        review2 = store.add(prompt="Review 2", task_type="review", depends_on=impl_task.id)
        review2.status = "completed"
        review2.report_file = "reports/review-2.md"
        review2.output_content = None
        store.update(review2)

        fix_task = store.add(
            prompt="Rescue report-only review context",
            task_type="fix",
            based_on=impl_task.id,
            depends_on=review2.id,
            same_branch=True,
        )

        context = _build_context_from_chain(fix_task, store, tmp_path, git=None)

        assert "## Repeated Blockers" in context
        assert "add bounded timeout and cancellation propagation" in context

    def test_fix_context_repeated_blockers_supports_canonical_blocker_body_without_required_fix_label(self, tmp_path: Path):
        """Repeated blocker extraction should not require an exact `Required fix:` line."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement canonical blocker extraction", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        blocker_text = "Add bounded timeout and cancellation propagation."
        review_body = (
            "## Blockers\n\n"
            "### B1 Missing timeout\n"
            "Evidence: hangs under retry storms.\n"
            "Impact: requests can block forever.\n"
            f"Action: {blocker_text}\n"
            "Required tests: timeout + cancellation regression.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )

        review1 = store.add(prompt="Review 1", task_type="review", depends_on=impl_task.id)
        review1.status = "completed"
        review1.output_content = review_body
        store.update(review1)

        review2 = store.add(prompt="Review 2", task_type="review", depends_on=impl_task.id)
        review2.status = "completed"
        review2.output_content = review_body
        store.update(review2)

        fix_task = store.add(
            prompt="Rescue canonical blocker body",
            task_type="fix",
            based_on=impl_task.id,
            depends_on=review2.id,
            same_branch=True,
        )

        context = _build_context_from_chain(fix_task, store, tmp_path, git=None)

        assert "## Repeated Blockers" in context
        assert blocker_text in context

    def test_fix_context_distinguishes_failed_improve_and_implement_retry_attempts(self, tmp_path: Path):
        """Fix context labels failed improve and failed implement retry attempts accurately."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement retries", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        review = store.add(prompt="Review", task_type="review", depends_on=impl_task.id)
        review.status = "completed"
        review.output_content = "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        store.update(review)

        failed_improve = store.add(
            prompt="Improve attempt",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review.id,
            same_branch=True,
        )
        failed_improve.status = "failed"
        failed_improve.failure_reason = "MAX_STEPS"
        failed_improve.completed_at = datetime(2026, 4, 20, 10, 0, tzinfo=UTC)
        store.update(failed_improve)

        failed_impl_retry = store.add(
            prompt="Retry implementation attempt",
            task_type="implement",
            based_on=impl_task.id,
        )
        failed_impl_retry.status = "failed"
        failed_impl_retry.failure_reason = "TEST_FAILURE"
        failed_impl_retry.completed_at = datetime(2026, 4, 20, 11, 0, tzinfo=UTC)
        store.update(failed_impl_retry)

        fix_task = store.add(
            prompt="Rescue stuck implementation",
            task_type="fix",
            based_on=impl_task.id,
            depends_on=review.id,
            same_branch=True,
        )

        context = _build_context_from_chain(fix_task, store, tmp_path, git=None)

        assert f"Latest failed improve/resume attempt: {failed_improve.id}" in context
        assert (
            f"Latest failed implementation retry/resume attempt: {failed_impl_retry.id}"
            in context
        )
        assert f"Latest failed improve/resume attempt: {failed_impl_retry.id}" not in context

    def test_fix_context_resolves_impl_through_resumed_fix_chain(self, tmp_path: Path):
        """A resumed/retried fix (based_on points at a prior fix, not the impl) must
        still assemble the full rescue context by walking up the fix/improve chain."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement parser", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        review = store.add(prompt="Review", task_type="review", depends_on=impl_task.id)
        review.status = "completed"
        review.output_content = (
            "## Must-Fix\n\n"
            "### M1: Bug\n"
            "Required fix: handle null.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        store.update(review)

        failed_fix = store.add(
            prompt="Rescue attempt",
            task_type="fix",
            based_on=impl_task.id,
            depends_on=review.id,
            same_branch=True,
        )
        failed_fix.status = "failed"
        failed_fix.failure_reason = "MAX_STEPS"
        store.update(failed_fix)

        # Resume of the failed fix: based_on points at the prior fix, not the impl.
        resumed_fix = store.add(
            prompt="Rescue attempt (resume)",
            task_type="fix",
            based_on=failed_fix.id,
            depends_on=review.id,
            same_branch=True,
        )

        context = _build_context_from_chain(resumed_fix, store, tmp_path, git=None)

        assert "## Fix Rescue Context" in context
        assert f"Root implementation: {impl_task.id}" in context
        assert f"Latest completed review: {review.id}" in context


class TestSummaryDirectory:
    """Tests for summary directory constant."""

    def test_summary_dir_constant_value(self):
        """Test that SUMMARY_DIR constant has the correct value."""
        assert SUMMARY_DIR == ".gza/summaries"


class TestCopyLearningsToWorktree:
    """Tests for _copy_learnings_to_worktree."""

    def test_copies_learnings_file(self, tmp_path: Path):
        """Learnings file is copied from project .gza/ into worktree .gza/."""
        project_dir = tmp_path / "project"
        worktree_dir = tmp_path / "worktree"
        project_dir.mkdir()
        worktree_dir.mkdir()

        # Create learnings file in project
        gza_dir = project_dir / ".gza"
        gza_dir.mkdir()
        (gza_dir / "learnings.md").write_text("- Use pytest fixtures")

        config = Mock(spec=Config)
        config.project_dir = project_dir

        _copy_learnings_to_worktree(config, worktree_dir)

        dst = worktree_dir / ".gza" / "learnings.md"
        assert dst.exists()
        assert dst.read_text() == "- Use pytest fixtures"

    def test_noop_when_no_learnings_file(self, tmp_path: Path):
        """No error when learnings file doesn't exist yet."""
        project_dir = tmp_path / "project"
        worktree_dir = tmp_path / "worktree"
        project_dir.mkdir()
        worktree_dir.mkdir()

        config = Mock(spec=Config)
        config.project_dir = project_dir

        _copy_learnings_to_worktree(config, worktree_dir)

        assert not (worktree_dir / ".gza" / "learnings.md").exists()

    def test_creates_gza_dir_in_worktree(self, tmp_path: Path):
        """.gza/ directory is created in worktree if it doesn't exist."""
        project_dir = tmp_path / "project"
        worktree_dir = tmp_path / "worktree"
        project_dir.mkdir()
        worktree_dir.mkdir()

        gza_dir = project_dir / ".gza"
        gza_dir.mkdir()
        (gza_dir / "learnings.md").write_text("content")

        config = Mock(spec=Config)
        config.project_dir = project_dir

        assert not (worktree_dir / ".gza").exists()
        _copy_learnings_to_worktree(config, worktree_dir)
        assert (worktree_dir / ".gza").is_dir()


class TestSnapshotTaskDbToWorktree:
    """Tests for _snapshot_task_db_to_worktree."""

    def test_creates_sqlite_snapshot_with_read_only_mode(self, tmp_path: Path):
        """Snapshot should contain DB content and be chmod 0444."""
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO items (name) VALUES ('alpha')")
        conn.commit()
        conn.close()

        worktree_dir = tmp_path / "worktree"
        worktree_dir.mkdir()

        _snapshot_task_db_to_worktree(db_path, worktree_dir)

        snapshot_path = worktree_dir / ".gza" / "gza.db"
        assert snapshot_path.exists()
        assert stat.S_IMODE(snapshot_path.stat().st_mode) == 0o444

        snapshot_conn = sqlite3.connect(str(snapshot_path))
        row = snapshot_conn.execute("SELECT name FROM items").fetchone()
        assert row is not None
        assert row[0] == "alpha"
        with pytest.raises(sqlite3.OperationalError, match="readonly|read-only"):
            snapshot_conn.execute("CREATE TABLE blocked (id INTEGER)")
        snapshot_conn.close()

    def test_noop_when_source_db_missing(self, tmp_path: Path):
        """Missing source DB should not create a snapshot file."""
        worktree_dir = tmp_path / "worktree"
        worktree_dir.mkdir()

        _snapshot_task_db_to_worktree(tmp_path / ".gza" / "gza.db", worktree_dir)

        assert not (worktree_dir / ".gza" / "gza.db").exists()


class TestStageWorktreeAgentResources:
    """Tests for scoped worktree resource staging."""

    def test_subdir_project_stages_snapshot_and_skills_under_scope_root(self, tmp_path: Path) -> None:
        repo_root = tmp_path / "repo"
        project_dir = repo_root / "services" / "foo"
        worktree_dir = tmp_path / "worktree"
        db_path = project_dir / ".gza" / "gza.db"
        project_dir.mkdir(parents=True)
        worktree_dir.mkdir()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO items (name) VALUES ('alpha')")
        conn.commit()
        conn.close()

        config = Mock(spec=Config)
        config.project_dir = project_dir
        config.db_path = db_path

        with patch("gza.skills_utils.ensure_all_skills", return_value=2) as mock_install:
            installed = _stage_worktree_agent_resources(
                config,
                worktree_dir,
                boundary=ProjectBoundary(
                    repo_root=repo_root,
                    scope_root=Path("services/foo"),
                    local_dependencies=(),
                ),
            )

        assert installed == 2
        mock_install.assert_called_once_with(worktree_dir / "services" / "foo" / ".claude" / "skills")
        assert (worktree_dir / "services" / "foo" / ".gza" / "gza.db").exists()
        assert not (worktree_dir / ".gza" / "gza.db").exists()

    def test_repo_root_project_stages_snapshot_and_skills_at_worktree_root(self, tmp_path: Path) -> None:
        project_dir = tmp_path / "repo"
        worktree_dir = tmp_path / "worktree"
        db_path = project_dir / ".gza" / "gza.db"
        project_dir.mkdir(parents=True)
        worktree_dir.mkdir()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO items (name) VALUES ('alpha')")
        conn.commit()
        conn.close()

        config = Mock(spec=Config)
        config.project_dir = project_dir
        config.db_path = db_path

        with patch("gza.skills_utils.ensure_all_skills", return_value=1) as mock_install:
            installed = _stage_worktree_agent_resources(
                config,
                worktree_dir,
                boundary=ProjectBoundary(
                    repo_root=project_dir,
                    scope_root=Path("."),
                    local_dependencies=(),
                ),
            )

        assert installed == 1
        mock_install.assert_called_once_with(worktree_dir / ".claude" / "skills")
        assert (worktree_dir / ".gza" / "gza.db").exists()


    def test_run_non_code_task_creates_readonly_snapshot(self, tmp_path: Path):
        """Non-code task path should expose readonly worktree DB snapshot."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Explore snapshot behavior", task_type="explore")
        task.slug = "20260414-explore-snapshot-behavior"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.db_path = db_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 50
        config.learnings_interval = 0
        config.learnings_window = 25

        observed: dict[str, str | int | None] = {
            "snapshot_mode": None,
            "task_prompt": None,
            "write_error": None,
        }

        def provider_run(_cfg, _prompt, _log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            snapshot_path = work_dir / ".gza" / "gza.db"
            assert snapshot_path.exists()
            observed["snapshot_mode"] = stat.S_IMODE(snapshot_path.stat().st_mode)

            snapshot_conn = sqlite3.connect(str(snapshot_path))
            row = snapshot_conn.execute("SELECT prompt FROM tasks WHERE id = ?", (task.id,)).fetchone()
            assert row is not None
            observed["task_prompt"] = row[0]

            try:
                snapshot_conn.execute("CREATE TABLE sandbox_write_attempt_non_code (id INTEGER)")
            except sqlite3.OperationalError as exc:
                observed["write_error"] = str(exc).lower()
            assert observed["write_error"] is not None
            snapshot_conn.close()

            report_dir = work_dir / ".gza" / "explorations"
            report_dir.mkdir(parents=True, exist_ok=True)
            (report_dir / f"{task.slug}.md").write_text("# Exploration\n\nSnapshot checks complete.")

            return RunResult(
                exit_code=0,
                duration_seconds=1.0,
                num_turns_reported=1,
                cost_usd=0.01,
                session_id="non-code-session",
                error_type=None,
            )

        mock_provider = Mock()
        mock_provider.name = "TestProvider"
        mock_provider.run = provider_run

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        with patch("gza.runner.console"):
            result = _run_non_code_task(task, config, store, mock_provider, mock_git, resume=False)

        assert result == 0
        assert observed["snapshot_mode"] == 0o444
        assert observed["task_prompt"] == "Explore snapshot behavior"
        assert observed["write_error"] is not None and "readonly" in str(observed["write_error"])

        host_conn = sqlite3.connect(str(db_path))
        sandbox_table = host_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='sandbox_write_attempt_non_code'"
        ).fetchone()
        host_conn.close()
        assert sandbox_table is None


class TestReviewTaskSlugGeneration:
    """Tests for review task slug generation."""

    def test_review_task_uses_implementation_slug(self, tmp_path: Path):
        """Test that auto-created review tasks derive slug from implementation task."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="testproject")

        # Create a completed implementation task with a task_id
        impl_task = store.add(
            prompt="Add docker volumes support",
            task_type="implement",
        )
        impl_task.status = "completed"
        impl_task.slug = "20260129-add-docker-volumes"
        store.update(impl_task)

        # Get the task to verify task_id is set
        impl_task = store.get(impl_task.id)
        assert impl_task.slug == "20260129-add-docker-volumes"

        # Create a mock config and run function that captures the review task
        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"

        # Capture the review task that gets created
        def mock_run(config, task_id):
            return 0

        # Mock post_review_to_pr to avoid GitHub CLI dependency
        def mock_post_review_to_pr(*args, **kwargs):
            pass

        # Temporarily replace run and post_review_to_pr functions
        import gza.runner
        original_run = gza.runner.run
        original_post_review = gza.runner.post_review_to_pr
        gza.runner.run = mock_run
        gza.runner.post_review_to_pr = mock_post_review_to_pr

        try:
            # Call _create_and_run_review_task
            _create_and_run_review_task(impl_task, config, store)

            # Get the review task that was created
            all_tasks = store.get_all()
            review_task = [t for t in all_tasks if t.task_type == "review"][0]
            assert review_task is not None

            # Verify the prompt uses the slug format
            assert review_task.prompt == "review add-docker-volumes"
        finally:
            gza.runner.run = original_run
            gza.runner.post_review_to_pr = original_post_review

    def test_review_task_handles_retry_suffix(self, tmp_path: Path):
        """Test that review task slug handles retry suffix in implementation task_id."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="testproject")

        # Create an implementation task with retry suffix
        impl_task = store.add(
            prompt="Fix authentication bug",
            task_type="implement",
        )
        impl_task.status = "completed"
        impl_task.slug = "20260129-fix-authentication-bug-2"
        store.update(impl_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"

        def mock_run(config, task_id):
            return 0

        def mock_post_review_to_pr(*args, **kwargs):
            pass

        import gza.runner
        original_run = gza.runner.run
        original_post_review = gza.runner.post_review_to_pr
        gza.runner.run = mock_run
        gza.runner.post_review_to_pr = mock_post_review_to_pr

        try:
            _create_and_run_review_task(impl_task, config, store)

            all_tasks = store.get_all()
            review_task = [t for t in all_tasks if t.task_type == "review"][0]
            assert review_task is not None
            # Should strip the retry suffix (-2) from the slug
            assert review_task.prompt == "review fix-authentication-bug"
        finally:
            gza.runner.run = original_run
            gza.runner.post_review_to_pr = original_post_review

    def test_review_task_strips_nested_derived_implement_prefixes(self, tmp_path: Path):
        """Auto-created reviews keep only semantic slug for nested derived implement slugs."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="testproject")

        parent_impl = store.add(
            prompt="Add feature",
            task_type="implement",
        )
        parent_impl.status = "completed"
        parent_impl.slug = "20260409-1-impl-add-feature"
        store.update(parent_impl)

        impl_task = store.add(
            prompt="Add feature",
            task_type="implement",
            based_on=parent_impl.id,
        )
        impl_task.status = "completed"
        impl_task.slug = "20260410-2-impl-1-impl-add-feature"
        store.update(impl_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"

        def mock_run(config, task_id):
            return 0

        def mock_post_review_to_pr(*args, **kwargs):
            pass

        import gza.runner
        original_run = gza.runner.run
        original_post_review = gza.runner.post_review_to_pr
        gza.runner.run = mock_run
        gza.runner.post_review_to_pr = mock_post_review_to_pr

        try:
            _create_and_run_review_task(impl_task, config, store)

            all_tasks = store.get_all()
            review_task = [t for t in all_tasks if t.task_type == "review"][0]
            assert review_task is not None
            assert review_task.prompt == "review add-feature"
        finally:
            gza.runner.run = original_run
            gza.runner.post_review_to_pr = original_post_review

    def test_review_task_fallback_without_task_id(self, tmp_path: Path):
        """Test that review task falls back gracefully if task_id is not set."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="testproject")

        # Create an implementation task without task_id
        impl_task = store.add(
            prompt="Implement feature",
            task_type="implement",
        )
        impl_task.status = "completed"
        # Don't set task_id
        store.update(impl_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"

        def mock_run(config, task_id):
            return 0

        def mock_post_review_to_pr(*args, **kwargs):
            pass

        import gza.runner
        original_run = gza.runner.run
        original_post_review = gza.runner.post_review_to_pr
        gza.runner.run = mock_run
        gza.runner.post_review_to_pr = mock_post_review_to_pr

        try:
            _create_and_run_review_task(impl_task, config, store)

            all_tasks = store.get_all()
            review_task = [t for t in all_tasks if t.task_type == "review"][0]
            assert review_task is not None
            # Should use fallback format (task ID is now a prefixed string)
            assert f"Review task {impl_task.id}" in review_task.prompt
        finally:
            gza.runner.run = original_run
            gza.runner.post_review_to_pr = original_post_review

    def test_auto_review_delegates_to_run(self, tmp_path: Path):
        """Test that _create_and_run_review_task delegates PR posting to run().

        PR posting now happens in _run_non_code_task (called by run()), not in
        _create_and_run_review_task itself. This test verifies the delegation.
        """
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="testproject")

        # Create a completed implementation task with a PR
        impl_task = store.add(
            prompt="Add user authentication",
            task_type="implement",
        )
        impl_task.status = "completed"
        impl_task.slug = "20260211-add-user-authentication"
        impl_task.branch = "gza/20260211-add-user-authentication"
        impl_task.pr_number = 123
        store.update(impl_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"

        # Track that run() was called with the review task
        run_calls = []

        def mock_run(config, task_id):
            run_calls.append(task_id)
            return 0  # Success

        import gza.runner
        original_run = gza.runner.run

        gza.runner.run = mock_run

        try:
            # Call _create_and_run_review_task
            exit_code = _create_and_run_review_task(impl_task, config, store)

            # Verify success
            assert exit_code == 0

            # Verify run() was called with the review task id
            assert len(run_calls) == 1
            # The review task ID is a prefixed string (second task created)
            all_tasks = store.get_all()
            review_task = [t for t in all_tasks if t.task_type == "review"][0]
            assert run_calls[0] == review_task.id
        finally:
            gza.runner.run = original_run

    def test_auto_review_returns_run_exit_code(self, tmp_path: Path):
        """Test that _create_and_run_review_task returns the exit code from run()."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a completed implementation task
        impl_task = store.add(
            prompt="Add user authentication",
            task_type="implement",
        )
        impl_task.status = "completed"
        impl_task.slug = "20260211-add-user-authentication"
        store.update(impl_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"

        def mock_run_failure(config, task_id):
            return 1  # Failure

        import gza.runner
        original_run = gza.runner.run
        gza.runner.run = mock_run_failure

        try:
            # Call _create_and_run_review_task
            exit_code = _create_and_run_review_task(impl_task, config, store)

            # Verify failure code is returned
            assert exit_code == 1
            created_reviews = store.get_reviews_for_task(impl_task.id)
            assert len(created_reviews) == 1
            assert created_reviews[0].trigger_source == "auto-recovery"
        finally:
            gza.runner.run = original_run

    def test_auto_review_rebase_targets_implementation_ancestor(self, tmp_path: Path):
        """Completed rebases should auto-review the implementation ancestor, not the rebase row."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(
            prompt="Add user authentication",
            task_type="implement",
        )
        impl_task.status = "completed"
        impl_task.slug = "20260211-add-user-authentication"
        store.update(impl_task)

        rebase_task = store.add(
            prompt="Rebase implementation branch",
            task_type="rebase",
            based_on=impl_task.id,
            same_branch=True,
        )
        rebase_task.status = "completed"
        rebase_task.slug = "20260212-rebase-auth"
        store.update(rebase_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.project_prefix = None

        run_calls: list[str] = []

        def mock_run(_config, task_id):
            run_calls.append(task_id)
            return 0

        import gza.runner
        original_run = gza.runner.run
        gza.runner.run = mock_run

        try:
            exit_code = _create_and_run_review_task(rebase_task, config, store)
            assert exit_code == 0

            all_tasks = store.get_all()
            review_task = [t for t in all_tasks if t.task_type == "review"][0]
            assert review_task.depends_on == impl_task.id
            assert review_task.based_on == impl_task.id
            assert review_task.prompt == "review add-user-authentication"
            assert run_calls == [review_task.id]
        finally:
            gza.runner.run = original_run

    def test_duplicate_in_progress_review_does_not_call_run(self, tmp_path: Path):
        """Test that _create_and_run_review_task does not call run() for in_progress reviews."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(
            prompt="Add user authentication",
            task_type="implement",
        )
        impl_task.status = "completed"
        impl_task.slug = "20260211-add-user-authentication"
        store.update(impl_task)

        # Create an in_progress review
        review_task = store.add(
            prompt="review add-user-authentication",
            task_type="review",
            depends_on=impl_task.id,
        )
        store.mark_in_progress(review_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"

        run_calls = []

        def mock_run(config, task_id):
            run_calls.append(task_id)
            return 0

        import gza.runner
        original_run = gza.runner.run
        gza.runner.run = mock_run

        try:
            exit_code = _create_and_run_review_task(impl_task, config, store)

            # Should succeed without calling run()
            assert exit_code == 0
            assert run_calls == [], "run() must not be called for an in_progress review"
        finally:
            gza.runner.run = original_run

    def test_duplicate_pending_review_is_run_once(self, tmp_path: Path):
        """Test that _create_and_run_review_task runs a pending duplicate review exactly once."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(
            prompt="Add user authentication",
            task_type="implement",
        )
        impl_task.status = "completed"
        impl_task.slug = "20260211-add-user-authentication"
        store.update(impl_task)

        # Create a pending review
        review_task = store.add(
            prompt="review add-user-authentication",
            task_type="review",
            depends_on=impl_task.id,
        )
        # review_task is pending by default

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"

        run_calls = []

        def mock_run(config, task_id):
            run_calls.append(task_id)
            return 0

        import gza.runner
        original_run = gza.runner.run
        gza.runner.run = mock_run

        try:
            exit_code = _create_and_run_review_task(impl_task, config, store)

            assert exit_code == 0
            assert run_calls == [review_task.id], "run() must be called exactly once with the pending review"
        finally:
            gza.runner.run = original_run


class TestGenerateSlugSlugOverride:
    """Tests for generate_slug with slug_override parameter."""

    def test_slug_override_used_instead_of_prompt(self, tmp_path: Path):
        """slug_override replaces the slug derived from prompt."""
        task_id = generate_slug(
            "some long generic prompt text",
            slug_override="add-docker-volumes",
        )
        assert task_id.endswith("-add-docker-volumes")

    def test_slug_override_none_falls_back_to_prompt(self, tmp_path: Path):
        """When slug_override is None, slug is derived from prompt as usual."""
        task_id = generate_slug(
            "Add docker volumes support",
            slug_override=None,
        )
        assert "add-docker-volumes-support" in task_id

    def test_slug_override_not_used_on_retry(self, tmp_path: Path):
        """slug_override is ignored when existing_id is provided (retry path)."""
        task_id = generate_slug(
            "some prompt",
            existing_id="20260101-original-slug",
            slug_override="something-else",
        )
        # Should re-use the base from existing_id, not slug_override
        assert "original-slug" in task_id


class TestGenerateSlugProjectPrefix:
    """Tests for generate_slug with project_prefix parameter."""

    def test_project_prefix_included_in_slug(self):
        """When project_prefix is set, slug is prefixed with it after the date."""
        slug = generate_slug("Add auth support", project_prefix="myproj")
        # Expected: YYYYMMDD-myproj-add-auth-support
        assert "-myproj-" in slug
        assert slug.endswith("-myproj-add-auth-support") or "-myproj-add-auth-support-" in slug

    def test_no_project_prefix_slug_unchanged(self):
        """When project_prefix is None or empty, slug is derived from prompt only."""
        slug = generate_slug("Add auth support", project_prefix=None)
        assert "-myproj-" not in slug
        assert "add-auth-support" in slug

    def test_project_prefix_empty_string_omitted(self):
        """Empty string project_prefix is treated as no prefix."""
        slug = generate_slug("Add auth support", project_prefix="")
        assert "add-auth-support" in slug
        # Should not have a double-dash from empty prefix
        assert "--" not in slug

    def test_non_review_implement_improve_path_unchanged(self):
        """Default path keeps YYYYMMDD-project_prefix-prompt format."""
        slug = generate_slug("Normal task prompt", project_prefix="gza")
        assert slug.endswith("-gza-normal-task-prompt") or "-gza-normal-task-prompt-" in slug

class TestTaskIdExistsBranchStrategy:
    """Tests for _slug_exists using branch_strategy patterns."""

    def test_default_pattern_checks_project_slash_task_id(self):
        """Without branch_strategy, falls back to {project}/{task_id} pattern."""
        git = Mock(spec=Git)
        git.branch_exists.return_value = True
        result = _slug_exists(
            "20260407-my-task",
            log_path=None,
            git=git,
            project_name="myproject",
        )
        assert result is True
        git.branch_exists.assert_called_once_with("myproject/20260407-my-task")

    def test_custom_pattern_uses_generate_branch_name(self):
        """With branch_strategy, uses the actual branch naming pattern."""
        git = Mock(spec=Git)
        git.branch_exists.return_value = True
        strategy = BranchStrategy(pattern="{slug}", default_type="feature")
        result = _slug_exists(
            "20260407-my-task",
            log_path=None,
            git=git,
            project_name="myproject",
            prompt="Fix something",
            branch_strategy=strategy,
        )
        assert result is True
        # Pattern is "{slug}" — the slug part of task_id after the date
        git.branch_exists.assert_called_once_with("my-task")

    def test_type_slug_pattern_detects_existing_branch(self):
        """Conventional {type}/{slug} pattern detects existing branch correctly."""
        git = Mock(spec=Git)
        git.branch_exists.return_value = True
        strategy = BranchStrategy(pattern="{type}/{slug}", default_type="feature")
        result = _slug_exists(
            "20260407-add-feature",
            log_path=None,
            git=git,
            project_name="myproject",
            prompt="Add a new feature",
            branch_strategy=strategy,
        )
        assert result is True
        git.branch_exists.assert_called_once_with("feature/add-feature")

    def test_collision_not_detected_when_branch_absent(self):
        """Returns False when branch does not exist."""
        git = Mock(spec=Git)
        git.branch_exists.return_value = False
        strategy = BranchStrategy(pattern="{slug}", default_type="feature")
        result = _slug_exists(
            "20260407-my-task",
            log_path=None,
            git=git,
            project_name="myproject",
            prompt="My task",
            branch_strategy=strategy,
        )
        assert result is False

    def test_generate_slug_detects_collision_with_non_default_pattern(self, tmp_path: Path):
        """generate_slug appends suffix when slug-only branch already exists."""
        git = Mock(spec=Git)
        strategy = BranchStrategy(pattern="{slug}", default_type="feature")

        def branch_exists(name: str) -> bool:
            # The first call (base slug) exists; the -2 suffix does not.
            return name == "my-task"

        git.branch_exists.side_effect = branch_exists

        task_id = generate_slug(
            "My task",
            log_path=None,
            git=git,
            project_name="myproject",
            branch_strategy=strategy,
        )
        # Base branch "my-task" was taken, so should get a -2 suffix
        assert task_id.endswith("-2")

    def test_generate_slug_no_collision_with_non_default_pattern(self):
        """generate_slug returns base id when the real branch does not exist."""
        git = Mock(spec=Git)
        git.branch_exists.return_value = False
        strategy = BranchStrategy(pattern="{slug}", default_type="feature")

        task_id = generate_slug(
            "My task",
            log_path=None,
            git=git,
            project_name="myproject",
            branch_strategy=strategy,
        )
        assert task_id.endswith("-my-task")

    def test_explicit_type_overrides_inferred_type_in_branch_check(self):
        """explicit_type is forwarded to generate_branch_name, overriding prompt inference."""
        git = Mock(spec=Git)
        git.branch_exists.return_value = True
        strategy = BranchStrategy(pattern="{type}/{slug}", default_type="feature")
        # Prompt would infer "feature" but explicit_type says "fix"
        result = _slug_exists(
            "20260407-my-task",
            log_path=None,
            git=git,
            project_name="myproject",
            prompt="Add a new feature",
            branch_strategy=strategy,
            explicit_type="fix",
        )
        assert result is True
        # Must check the explicit-type branch, not the inferred-type branch
        git.branch_exists.assert_called_once_with("fix/my-task")

    def test_explicit_type_collision_triggers_suffix_in_generate_slug(self):
        """generate_slug appends suffix when explicit-type branch exists."""
        git = Mock(spec=Git)
        strategy = BranchStrategy(pattern="{type}/{slug}", default_type="feature")

        def branch_exists(name: str) -> bool:
            # The fix/-prefixed base branch exists; the -2 suffix does not.
            return name == "fix/add-a-new-feature"

        git.branch_exists.side_effect = branch_exists

        task_id = generate_slug(
            "Add a new feature",  # would infer "feature" type without explicit_type
            log_path=None,
            git=git,
            project_name="myproject",
            branch_strategy=strategy,
            explicit_type="fix",
        )
        # Base "fix/add-a-new-feature" was taken; should get a -2 suffix
        assert task_id.endswith("-2")


class TestComputeSlugOverride:
    """Tests for _compute_slug_override helper."""

    def test_implement_uses_root_ancestor_prompt_across_multiple_generations(self, tmp_path: Path):
        """Implement descendants derive slug from lineage-root prompt."""
        store = SqliteTaskStore(tmp_path / "test.db")
        root_plan = store.add(prompt="Roll out authentication platform", task_type="plan")
        impl1 = store.add(
            prompt="Implement auth v1",
            task_type="implement",
            based_on=root_plan.id,
        )
        impl2 = store.add(
            prompt="Implement auth v2",
            task_type="implement",
            based_on=impl1.id,
        )
        impl3 = store.add(
            prompt="Implement auth v3",
            task_type="implement",
            based_on=impl2.id,
        )

        result = _compute_slug_override(impl3, store)
        assert result == "roll-out-authentication-platform"

    def test_improve_uses_same_root_prompt_as_implementation_lineage(self, tmp_path: Path):
        """Improve task derives slug from lineage root via based_on chain."""
        store = SqliteTaskStore(tmp_path / "test.db")
        root_plan = store.add(prompt="Stabilize job scheduler", task_type="plan")
        impl_task = store.add(
            prompt="Implement scheduler stabilization",
            task_type="implement",
            based_on=root_plan.id,
        )
        improve_task = store.add(
            prompt="Fix scheduler edge cases",
            task_type="improve",
            based_on=impl_task.id,
        )

        result = _compute_slug_override(improve_task, store)
        assert result == "stabilize-job-scheduler"

    def test_review_uses_direct_depends_on_target_prompt(self, tmp_path: Path):
        """Review task derives slug from immediate depends_on target only."""
        store = SqliteTaskStore(tmp_path / "test.db")
        root_plan = store.add(prompt="Migrate billing stack", task_type="plan")
        impl_task = store.add(
            prompt="Implement billing migration",
            task_type="implement",
            based_on=root_plan.id,
        )
        improve_task = store.add(
            prompt="Improve migration observability",
            task_type="improve",
            based_on=impl_task.id,
        )
        review_task = store.add(
            prompt="Review migration changes",
            task_type="review",
            depends_on=improve_task.id,
        )
        result = _compute_slug_override(review_task, store)
        assert result == "improve-migration-observability"

    def test_missing_ancestor_during_based_on_walk_uses_last_resolved_prompt(
        self, caplog: pytest.LogCaptureFixture
    ):
        """Missing parent while walking ancestors uses last resolved task prompt."""
        mid = Task(id="gza-mid", prompt="Mid ancestor prompt", task_type="implement", based_on="gza-root")
        child = Task(
            id="gza-child",
            prompt="Implement child",
            task_type="implement",
            based_on="gza-mid",
        )
        store = Mock(spec=SqliteTaskStore)
        store.get.side_effect = lambda task_id: {
            "gza-mid": mid,
            "gza-root": None,
        }.get(task_id)

        with caplog.at_level(logging.WARNING, logger="gza.runner"):
            result = _compute_slug_override(child, store)
        assert result == "mid-ancestor-prompt"
        assert (
            "Slug override ancestor missing for task #gza-child while walking based_on chain: "
            "missing_parent=gza-root; using last resolved ancestor #gza-mid"
        ) in caplog.text

    def test_missing_review_target_falls_back_to_review_prompt_and_warns(
        self, caplog: pytest.LogCaptureFixture
    ):
        """Missing review depends_on target falls back to review prompt."""
        review_task = Task(
            id="gza-review",
            prompt="Review missing target behavior",
            task_type="review",
            depends_on="gza-missing",
        )
        store = Mock(spec=SqliteTaskStore)
        store.get.return_value = None

        with caplog.at_level(logging.WARNING, logger="gza.runner"):
            result = _compute_slug_override(review_task, store)
        assert result == "review-missing-target-behavior"
        assert (
            "Slug override review target missing for task #gza-review: depends_on=gza-missing; "
            "falling back to review task prompt"
        ) in caplog.text

    def test_cycle_in_based_on_chain_uses_last_resolved_prompt_and_warns(
        self, caplog: pytest.LogCaptureFixture
    ):
        """Cycle in based_on walk should stop and use last resolved prompt."""
        a = Task(id="gza-a", prompt="Root A prompt", task_type="implement", based_on="gza-b")
        b = Task(id="gza-b", prompt="Root B prompt", task_type="implement", based_on="gza-a")
        child = Task(id="gza-child", prompt="Child prompt", task_type="implement", based_on="gza-a")
        store = Mock(spec=SqliteTaskStore)
        store.get.side_effect = lambda task_id: {"gza-a": a, "gza-b": b}.get(task_id)

        with caplog.at_level(logging.WARNING, logger="gza.runner"):
            result = _compute_slug_override(child, store)
        assert result == "root-b-prompt"
        assert (
            "Slug override cycle detected for task #gza-child while walking based_on chain: "
            "ancestor=gza-a; using last resolved ancestor #gza-b"
        ) in caplog.text

    def test_variable_width_task_ids_no_longer_shape_slug_override(self):
        """Slug override does not embed task-id suffixes."""
        anchor = Task(id="gza-1", prompt="Switch task ids", task_type="implement")
        review_task = Task(id="gza-mp", prompt="Review switch task ids", task_type="review", depends_on=anchor.id)
        store = Mock(spec=SqliteTaskStore)
        store.get.return_value = anchor

        result = _compute_slug_override(review_task, store)
        assert result == "switch-task-ids"

    def test_plain_task_returns_none(self, tmp_path: Path):
        """Non-review/implement/improve tasks return None."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add(prompt="Do some work", task_type="task")
        result = _compute_slug_override(task, store)
        assert result is None

    def test_explore_task_returns_none(self, tmp_path: Path):
        """Explore tasks return None."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add(prompt="Explore codebase", task_type="explore")
        result = _compute_slug_override(task, store)
        assert result is None

class TestReviewNextSteps:
    """Tests for next steps output after review task completion."""

    def test_review_completion_suggests_improve(self, tmp_path: Path):
        """Test that review completion output suggests gza improve <impl_task_id>."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a completed implementation task
        impl_task = store.add(
            prompt="Add user authentication",
            task_type="implement",
        )
        impl_task.status = "completed"
        impl_task.slug = "20260211-add-user-authentication"
        impl_task.branch = "gza/20260211-add-user-authentication"
        store.update(impl_task)

        # Create a review task that depends on it
        review_task = store.add(
            prompt="Review the implementation",
            task_type="review",
            depends_on=impl_task.id,
        )
        review_task.slug = "20260212-review-the-implementation"
        store.update(review_task)

        # Setup config
        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.learnings_interval = 0
        config.learnings_window = 25

        # Mock provider
        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_result = RunResult(
            exit_code=0,
            duration_seconds=10.0,
            num_turns_reported=5,
            cost_usd=0.05,
            session_id="test-session",
            error_type=None,
        )
        mock_provider.run.return_value = mock_result

        # Mock git
        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)
        mock_git.get_diff_numstat.return_value = ""
        mock_git.get_diff.return_value = ""

        # Create worktree directory and report file
        worktree_path = config.worktree_path / f"{review_task.slug}-review"
        worktree_review_dir = worktree_path / ".gza" / "reviews"
        worktree_review_dir.mkdir(parents=True, exist_ok=True)
        report_file = worktree_review_dir / f"{review_task.slug}.md"
        report_file.write_text("# Review\n\nChanges requested.")

        # Capture console output by collecting print calls
        printed_lines: list[str] = []

        def capture_print(*args, **kwargs):
            printed_lines.append(str(args[0]) if args else "")

        with patch('gza.runner.console') as mock_runner_console, \
             patch('gza.console.console') as mock_console_console, \
             patch('gza.runner.post_review_to_pr'):
            # task_footer prints via gza.console.console; runner.py prints
            # pre/post diagnostic lines via gza.runner.console. Route both
            # through the same capture function so the assertion sees the
            # combined output.
            mock_runner_console.print.side_effect = capture_print
            mock_console_console.print.side_effect = capture_print

            exit_code = _run_non_code_task(
                review_task, config, store, mock_provider, mock_git, resume=False
            )

            assert exit_code == 0

            # Verify improve suggestions appear in output
            all_output = "\n".join(printed_lines)
            assert f"gza improve {impl_task.id}" in all_output
            assert f"gza improve {impl_task.id} --run" not in all_output

    @pytest.mark.parametrize(
        ("report_content", "expected_verdict"),
        [
            ("# Review\n\nVerdict: CHANGES_REQUESTED", "CHANGES_REQUESTED"),
            ("# Review\n\n## Verdict\n\n**NEEDS_DISCUSSION**\n", "NEEDS_DISCUSSION"),
        ],
    )
    def test_review_completion_prints_verdict(self, tmp_path: Path, report_content: str, expected_verdict: str):
        """Completed review output should print parsed verdict for inline and heading markdown formats."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Add user authentication", task_type="implement")
        impl_task.status = "completed"
        impl_task.slug = "20260211-add-user-authentication"
        impl_task.branch = "gza/20260211-add-user-authentication"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review the implementation",
            task_type="review",
            depends_on=impl_task.id,
        )
        review_task.slug = "20260212-review-the-implementation"
        store.update(review_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.learnings_interval = 0
        config.learnings_window = 25

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.return_value = RunResult(
            exit_code=0,
            duration_seconds=10.0,
            num_turns_reported=5,
            cost_usd=0.05,
            session_id="test-session",
            error_type=None,
        )

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)
        mock_git.get_diff_numstat.return_value = ""
        mock_git.get_diff.return_value = ""

        worktree_path = config.worktree_path / f"{review_task.slug}-review"
        worktree_review_dir = worktree_path / ".gza" / "reviews"
        worktree_review_dir.mkdir(parents=True, exist_ok=True)
        report_file = worktree_review_dir / f"{review_task.slug}.md"
        report_file.write_text(report_content)

        printed_lines: list[str] = []

        def capture_print(*args, **kwargs):
            printed_lines.append(str(args[0]) if args else "")

        with patch('gza.runner.console') as mock_runner_console, \
             patch('gza.console.console') as mock_console_console, \
             patch('gza.runner.post_review_to_pr'):
            mock_runner_console.print.side_effect = capture_print
            mock_console_console.print.side_effect = capture_print
            exit_code = _run_non_code_task(
                review_task, config, store, mock_provider, mock_git, resume=False
            )

        assert exit_code == 0
        # task_footer emits Rich markup like "[bright_white]Verdict:[/bright_white]",
        # so the literal "Verdict: " (with trailing space) doesn't appear — the
        # closing bracket sits between the colon and the space. Assert against
        # the unstyled "Verdict:" substring plus the verdict value.
        assert "Verdict:" in "\n".join(printed_lines)
        assert expected_verdict in "\n".join(printed_lines)

    def test_non_review_task_does_not_suggest_improve(self, tmp_path: Path):
        """Test that explore/plan task completion does NOT suggest gza improve."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        explore_task = store.add(
            prompt="Explore codebase",
            task_type="explore",
        )
        explore_task.slug = "20260212-explore-codebase"
        store.update(explore_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.learnings_interval = 0
        config.learnings_window = 25

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_result = RunResult(
            exit_code=0,
            duration_seconds=5.0,
            num_turns_reported=3,
            cost_usd=0.02,
            session_id="test-session",
            error_type=None,
        )
        mock_provider.run.return_value = mock_result

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        worktree_path = config.worktree_path / f"{explore_task.slug}-explore"
        worktree_explore_dir = worktree_path / ".gza" / "explorations"
        worktree_explore_dir.mkdir(parents=True, exist_ok=True)
        report_file = worktree_explore_dir / f"{explore_task.slug}.md"
        report_file.write_text("# Exploration\n\nFindings here.")

        printed_lines: list[str] = []

        def capture_print(*args, **kwargs):
            printed_lines.append(str(args[0]) if args else "")

        with patch('gza.runner.console') as mock_runner_console, \
             patch('gza.console.console') as mock_console_console:
            mock_runner_console.print.side_effect = capture_print
            mock_console_console.print.side_effect = capture_print

            exit_code = _run_non_code_task(
                explore_task, config, store, mock_provider, mock_git, resume=False
            )

            assert exit_code == 0

            all_output = "\n".join(printed_lines)
            assert "gza improve" not in all_output
            assert "gza retry" not in all_output
            assert "gza resume" not in all_output


class TestRunNonCodeTaskDockerGitMetadata:
    """Tests for Docker review execution when worktree git metadata is invalid."""

    def test_docker_review_hides_and_restores_invalid_worktree_git_file(self, tmp_path: Path):
        """Invalid host gitdir metadata should be hidden during provider run and restored after."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.slug = "20260225-implement-feature"
        impl_task.branch = "test/feature-branch"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review implementation",
            task_type="review",
            depends_on=impl_task.id,
        )
        review_task.slug = "20260225-review-feature"
        store.update(review_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = True
        config.learnings_interval = 0
        config.learnings_window = 25

        worktree_path = config.worktree_path / f"{review_task.slug}-review"
        worktree_path.mkdir(parents=True, exist_ok=True)
        original_git_file = worktree_path / ".git"
        original_git_content = "gitdir: /nonexistent/host/path/.git/worktrees/review\n"
        original_git_file.write_text(original_git_content)

        worktree_review_dir = worktree_path / ".gza" / "reviews"
        worktree_review_dir.mkdir(parents=True, exist_ok=True)
        report_file = worktree_review_dir / f"{review_task.slug}.md"

        def provider_run(_config, _prompt, _log_file, _work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            assert not (worktree_path / ".git").exists()
            assert (worktree_path / ".git.gza-host-worktree").exists()
            report_file.write_text("# Review\n\nVerdict: APPROVED")
            return RunResult(
                exit_code=0,
                duration_seconds=2.0,
                num_turns_reported=1,
                cost_usd=0.01,
                session_id="session-1",
                error_type=None,
            )

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.side_effect = provider_run

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)
        mock_git.get_diff_numstat.return_value = "1\t1\tsrc/app.py\n"
        mock_git.get_diff_stat.return_value = " 1 file changed, 1 insertion(+), 1 deletion(-)"
        mock_git.get_diff.return_value = "diff --git a/src/app.py b/src/app.py\n@@ -1 +1 @@\n-old\n+new\n"

        with patch("gza.runner.post_review_to_pr"):
            exit_code = _run_non_code_task(
                review_task,
                config,
                store,
                mock_provider,
                mock_git,
                resume=False,
            )

        assert exit_code == 0
        # The worktree is cleaned up on success; the hiding/restoring assertions are
        # checked inside provider_run's side_effect above.
        assert not worktree_path.exists()


class TestRunNonCodeTaskWorktreeReportDir:
    """Tests that worktree report directory is created from report_path.parent."""

    def test_worktree_report_dir_created_without_precreation(self, tmp_path: Path):
        """Worktree report dir should be derived from report_path.parent, not an undefined report_dir."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.slug = "20260225-implement-feature"
        impl_task.branch = "test/feature-branch"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review implementation",
            task_type="review",
            depends_on=impl_task.id,
        )
        review_task.slug = "20260225-review-feature"
        store.update(review_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.learnings_interval = 0
        config.learnings_window = 25

        worktree_path = config.worktree_path / f"{review_task.slug}-review"

        def provider_run(_config, _prompt, _log_file, _work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            # Simulate provider writing the report file in the worktree
            worktree_review_dir = worktree_path / ".gza" / "reviews"
            worktree_review_dir.mkdir(parents=True, exist_ok=True)
            report_file = worktree_review_dir / f"{review_task.slug}.md"
            report_file.write_text("# Review\n\nVerdict: APPROVED")
            return RunResult(
                exit_code=0,
                duration_seconds=2.0,
                num_turns_reported=1,
                cost_usd=0.01,
                session_id="session-1",
                error_type=None,
            )

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.side_effect = provider_run

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)
        mock_git.get_diff_numstat.return_value = ""
        mock_git.get_diff.return_value = ""

        with patch("gza.runner.post_review_to_pr"):
            exit_code = _run_non_code_task(
                review_task, config, store, mock_provider, mock_git, resume=False
            )

        assert exit_code == 0
        # Verify the report was copied from worktree to project dir
        project_report = tmp_path / ".gza" / "reviews" / f"{review_task.slug}.md"
        assert project_report.exists()
        assert "APPROVED" in project_report.read_text()


class TestRunNonCodeTaskPRPosting:
    """Tests for _run_non_code_task PR posting behavior."""

    def test_run_non_code_task_posts_to_pr_for_review(self, tmp_path: Path):
        """Test that _run_non_code_task calls post_review_to_pr for completed review tasks."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a completed implementation task
        impl_task = store.add(
            prompt="Add user authentication",
            task_type="implement",
        )
        impl_task.status = "completed"
        impl_task.slug = "20260211-add-user-authentication"
        impl_task.branch = "gza/20260211-add-user-authentication"
        impl_task.pr_number = 123
        store.update(impl_task)

        # Create a review task that depends on it
        review_task = store.add(
            prompt="Review the implementation",
            task_type="review",
            depends_on=impl_task.id,
        )
        review_task.slug = "20260212-review-the-implementation"
        store.update(review_task)

        # Setup config
        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.learnings_interval = 0
        config.learnings_window = 25

        # Create review directory structure
        review_dir = tmp_path / ".gza" / "reviews"
        review_dir.mkdir(parents=True, exist_ok=True)

        # Track if post_review_to_pr was called
        pr_post_called = []

        def mock_post_review_to_pr(review_task, impl_task, store, project_dir, required=False, **kwargs):
            pr_post_called.append({
                'review_id': review_task.id,
                'impl_id': impl_task.id,
                'required': required
            })

        # Mock provider
        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_result = RunResult(
            exit_code=0,
            duration_seconds=10.0,
            num_turns_reported=5,
            cost_usd=0.05,
            session_id="test-session",
            error_type=None,
        )
        mock_provider.run.return_value = mock_result

        # Mock git
        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)
        mock_git.get_diff_numstat.return_value = ""
        mock_git.get_diff.return_value = ""

        # Create worktree directory and report file (simulating provider writing it)
        worktree_path = config.worktree_path / f"{review_task.slug}-review"
        worktree_review_dir = worktree_path / ".gza" / "reviews"
        worktree_review_dir.mkdir(parents=True, exist_ok=True)
        report_file = worktree_review_dir / f"{review_task.slug}.md"
        report_file.write_text("# Review\n\nLooks good!")

        import gza.runner
        original_post_review = gza.runner.post_review_to_pr
        gza.runner.post_review_to_pr = mock_post_review_to_pr

        try:
            # Call _run_non_code_task
            exit_code = _run_non_code_task(
                review_task, config, store, mock_provider, mock_git, resume=False
            )

            # Verify success
            assert exit_code == 0

            # Verify post_review_to_pr was called
            assert len(pr_post_called) == 1
            assert pr_post_called[0]['review_id'] == review_task.id
            assert pr_post_called[0]['impl_id'] == impl_task.id
            assert pr_post_called[0]['required'] is False
        finally:
            gza.runner.post_review_to_pr = original_post_review


class TestMaxStepsHandling:
    """Tests for max-steps behavior in runner integration."""

    def _make_non_code_config(self, tmp_path: Path, *, max_steps: int) -> Mock:
        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = max_steps
        return config

    def _run_non_code_task_with_log_markers(
        self,
        tmp_path: Path,
        *,
        exit_code: int,
        error_type: str | None,
        slug: str,
        log_markers: tuple[str, ...],
        include_report_result: bool,
    ) -> tuple[int, SqliteTaskStore, Task, MagicMock, str]:
        import json

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Plan task", task_type="plan")
        task.slug = slug
        store.update(task)

        config = self._make_non_code_config(tmp_path, max_steps=20)
        report_text = "# Plan\n\n- recovered from log\n"

        def provider_run(_config, _prompt, log_file, _work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            marker_text = "".join(f"[GZA_FAILURE:{marker}]\n" for marker in log_markers)
            log_file.write_text(f"tool output\n{marker_text}")
            if include_report_result:
                with open(log_file, "a") as f:
                    f.write(json.dumps({"type": "result", "subtype": "success", "result": report_text}) + "\n")
            return RunResult(
                exit_code=exit_code,
                duration_seconds=4.2,
                session_id="non-code-session",
                error_type=error_type,
            )

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.side_effect = provider_run

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        with (
            patch("gza.runner.console"),
            patch("gza.runner.task_footer") as mock_task_footer,
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            run_exit_code = _run_non_code_task(task, config, store, mock_provider, mock_git)

        return run_exit_code, store, task, mock_task_footer, report_text

    def test_run_result_to_stats_includes_step_fields(self):
        """Step metrics should be transferred from RunResult to TaskStats."""
        result = RunResult(
            exit_code=0,
            duration_seconds=12.3,
            num_steps_reported=7,
            num_steps_computed=8,
            num_turns_reported=5,
            num_turns_computed=5,
            cost_usd=0.12,
            input_tokens=100,
            output_tokens=200,
        )
        stats = _run_result_to_stats(result)
        assert stats.num_steps_reported == 7
        assert stats.num_steps_computed == 8
        assert stats.num_turns_reported == 5
        assert stats.tokens_estimated is False
        assert stats.cost_estimated is False

    def test_run_result_to_stats_includes_estimation_flags(self):
        """Estimation flags should be transferred from RunResult to TaskStats."""
        result = RunResult(
            exit_code=0,
            input_tokens=123,
            output_tokens=45,
            cost_usd=0.01,
            tokens_estimated=True,
            cost_estimated=True,
        )
        stats = _run_result_to_stats(result)
        assert stats.tokens_estimated is True
        assert stats.cost_estimated is True

    def test_apply_transcript_stats_fallback_prefers_transcript_usage_for_timeout(self, tmp_path: Path):
        """Timeout runs should recover usage and steps from the provider transcript."""
        log_file = tmp_path / "timeout.log"
        log_file.write_text(
            json.dumps(
                {
                    "type": "assistant",
                    "message": {
                        "id": "msg_usage_1",
                        "usage": {
                            "input_tokens": 10,
                            "cache_creation_input_tokens": 2,
                            "cache_read_input_tokens": 3,
                            "output_tokens": 5,
                        },
                        "content": [{"type": "text", "text": "Investigating timeout fallback."}],
                    },
                }
            )
            + "\n"
        )
        result = RunResult(
            exit_code=124,
            num_steps_computed=0,
            input_tokens=0,
            output_tokens=1,
            cost_usd=0.0,
            tokens_estimated=True,
            cost_estimated=True,
        )

        updated = _apply_transcript_stats_fallback(
            result,
            log_file=log_file,
            provider_name="claude",
            configured_model=None,
            prefer_transcript_usage=True,
        )

        assert updated is True
        assert result.num_steps_computed == 1
        assert result.num_steps_reported == 1
        assert result.input_tokens == 15
        assert result.output_tokens == 5
        assert result.cost_usd and result.cost_usd > 0.0
        assert result.tokens_estimated is False
        assert result.cost_estimated is False

    def test_non_code_task_marks_max_steps_failure_reason(self, tmp_path: Path):
        """Provider max_steps errors should be stored as MAX_STEPS."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Plan task", task_type="plan")
        task.slug = "20260225-plan-task"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 2

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.return_value = RunResult(
            exit_code=0,
            duration_seconds=4.2,
            num_steps_computed=3,
            error_type="max_steps",
        )

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        with patch("gza.runner.console"):
            exit_code = _run_non_code_task(task, config, store, mock_provider, mock_git)

        assert exit_code == 0
        failed = store.get(task.id)
        assert failed is not None
        assert failed.status == "failed"
        assert failed.failure_reason == "MAX_STEPS"

    def test_non_code_task_marks_max_turns_failure_reason(self, tmp_path: Path):
        """Provider max_turns errors should be stored as MAX_TURNS."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Plan task", task_type="plan")
        task.slug = "20260225-plan-max-turns"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 20
        config.max_turns = 2

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.return_value = RunResult(
            exit_code=0,
            duration_seconds=4.2,
            num_turns_computed=3,
            error_type="max_turns",
        )

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        with patch("gza.runner.console"):
            exit_code = _run_non_code_task(task, config, store, mock_provider, mock_git)

        assert exit_code == 0
        failed = store.get(task.id)
        assert failed is not None
        assert failed.status == "failed"
        assert failed.failure_reason == "MAX_TURNS"

    def test_non_code_task_below_step_limit_does_not_mark_max_steps(self, tmp_path: Path):
        """Provider max_steps below the configured limit should not persist MAX_STEPS."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Plan task", task_type="plan")
        task.slug = "20260505-plan-below-max-steps"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 5
        config.max_turns = 20

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.return_value = RunResult(
            exit_code=0,
            duration_seconds=4.2,
            num_steps_computed=4,
            error_type="max_steps",
        )

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        with patch("gza.runner.console"):
            exit_code = _run_non_code_task(task, config, store, mock_provider, mock_git)

        assert exit_code == 0
        failed = store.get(task.id)
        assert failed is not None
        assert failed.status == "failed"
        assert failed.failure_reason == "UNKNOWN"
        assert failed.log_file is not None
        log_contents = ops_log_path_for(tmp_path / failed.log_file).read_text()
        assert "Outcome: failed (error_type=max_steps)" in log_contents
        assert "Outcome: failed (max_steps)" not in log_contents

    def test_non_code_task_below_turn_limit_does_not_mark_max_turns(self, tmp_path: Path):
        """Provider max_turns below the configured limit should not persist MAX_TURNS."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Plan task", task_type="plan")
        task.slug = "20260505-plan-below-max-turns"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 20
        config.max_turns = 5

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.return_value = RunResult(
            exit_code=0,
            duration_seconds=4.2,
            num_turns_computed=4,
            error_type="max_turns",
        )

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        with patch("gza.runner.console"):
            exit_code = _run_non_code_task(task, config, store, mock_provider, mock_git)

        assert exit_code == 0
        failed = store.get(task.id)
        assert failed is not None
        assert failed.status == "failed"
        assert failed.failure_reason == "UNKNOWN"
        assert failed.log_file is not None
        log_contents = ops_log_path_for(tmp_path / failed.log_file).read_text()
        assert "Outcome: failed (error_type=max_turns)" in log_contents
        assert "Outcome: failed (max_turns)" not in log_contents

    def test_non_code_task_uses_max_steps_ground_truth_over_log_markers(self, tmp_path: Path):
        """Provider max_steps should ignore contaminated failure markers in the log."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Plan task", task_type="plan")
        task.slug = "20260504-plan-max-steps-ground-truth"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 2

        def provider_run(_config, _prompt, log_file, _work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            log_file.write_text("tool output\n[GZA_FAILURE:TEST_FAILURE]\n")
            return RunResult(
                exit_code=0,
                duration_seconds=4.2,
                num_steps_computed=3,
                error_type="max_steps",
            )

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.side_effect = provider_run

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        with patch("gza.runner.console"):
            exit_code = _run_non_code_task(task, config, store, mock_provider, mock_git)

        assert exit_code == 0
        failed = store.get(task.id)
        assert failed is not None
        assert failed.status == "failed"
        assert failed.failure_reason == "MAX_STEPS"

    def test_non_code_task_uses_timeout_ground_truth_over_log_markers(self, tmp_path: Path):
        """Host timeout exit code should ignore contaminated failure markers in the log."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Plan task", task_type="plan")
        task.slug = "20260504-plan-timeout-ground-truth"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 20

        def provider_run(_config, _prompt, log_file, _work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            log_file.write_text("tool output\n[GZA_FAILURE:TEST_FAILURE]\n")
            return RunResult(
                exit_code=124,
                duration_seconds=600.0,
                session_id="non-code-timeout-session",
                error_type=None,
            )

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.side_effect = provider_run

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        with patch("gza.runner.console"):
            exit_code = _run_non_code_task(task, config, store, mock_provider, mock_git)

        assert exit_code == 0
        failed = store.get(task.id)
        assert failed is not None
        assert failed.status == "failed"
        assert failed.failure_reason == "TIMEOUT"

    def test_non_code_task_prefers_timeout_ground_truth_over_provider_max_steps(self, tmp_path: Path):
        """Combined timeout and provider max_steps signals should render and persist TIMEOUT."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Plan task", task_type="plan")
        task.slug = "20260504-plan-timeout-over-max-steps"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 20

        def provider_run(_config, _prompt, log_file, _work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            log_file.write_text("tool output\n[GZA_FAILURE:TEST_FAILURE]\n")
            return RunResult(
                exit_code=124,
                duration_seconds=600.0,
                num_steps_computed=21,
                session_id="non-code-timeout-wins-session",
                error_type="max_steps",
            )

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.side_effect = provider_run

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        with patch("gza.runner.console"), patch("gza.runner.task_footer") as mock_task_footer:
            exit_code = _run_non_code_task(task, config, store, mock_provider, mock_git)

        assert exit_code == 0
        failed = store.get(task.id)
        assert failed is not None
        assert failed.status == "failed"
        assert failed.failure_reason == "TIMEOUT"

        assert mock_task_footer.call_count == 1
        assert mock_task_footer.call_args.kwargs["status"] == "Failed: MockProvider timed out after 10 minutes"

        assert failed.log_file is not None
        log_contents = ops_log_path_for(tmp_path / failed.log_file).read_text()
        assert "Outcome: failed (timeout after 10m)" in log_contents
        assert "Outcome: failed (max_steps)" not in log_contents

    @pytest.mark.parametrize("marker", ["MAX_STEPS", "MAX_TURNS", "TIMEOUT"])
    def test_non_code_success_ignores_contaminated_structured_markers(self, tmp_path: Path, marker: str):
        """Scraped structured runner markers must not fail a successful non-code task."""
        exit_code, store, task, _, report_text = self._run_non_code_task_with_log_markers(
            tmp_path,
            exit_code=0,
            error_type=None,
            slug=f"20260504-plan-success-ignores-{marker.lower().replace('_', '-')}",
            log_markers=(marker,),
            include_report_result=True,
        )

        assert exit_code == 0
        completed = store.get(task.id)
        assert completed is not None
        assert completed.status == "completed"
        assert completed.failure_reason is None
        assert completed.output_content == report_text

        assert completed.log_file is not None
        log_contents = ops_log_path_for(tmp_path / completed.log_file).read_text()
        assert "Outcome: completed" in log_contents
        assert "Outcome: failed (timeout after 10m)" not in log_contents
        assert "Outcome: failed (max_steps)" not in log_contents
        assert "Outcome: failed (max_turns)" not in log_contents

    @pytest.mark.parametrize("marker", ["MAX_STEPS", "MAX_TURNS", "TIMEOUT"])
    def test_non_code_nonzero_exit_with_contaminated_structured_markers_records_unknown(
        self,
        tmp_path: Path,
        marker: str,
    ):
        """Generic nonzero failures must not reuse runner-owned reasons from the log."""
        exit_code, store, task, mock_task_footer, _ = self._run_non_code_task_with_log_markers(
            tmp_path,
            exit_code=1,
            error_type=None,
            slug=f"20260504-plan-exit-code-fallback-{marker.lower().replace('_', '-')}",
            log_markers=(marker,),
            include_report_result=False,
        )

        assert exit_code == 0
        failed = store.get(task.id)
        assert failed is not None
        assert failed.status == "failed"
        assert failed.failure_reason == "UNKNOWN"

        assert mock_task_footer.call_count == 1
        assert mock_task_footer.call_args.kwargs["status"] == "Failed: MockProvider exited with code 1"

        assert failed.log_file is not None
        log_contents = ops_log_path_for(tmp_path / failed.log_file).read_text()
        assert "Outcome: failed (exit_code=1)" in log_contents
        assert "Outcome: failed (timeout after 10m)" not in log_contents
        assert "Outcome: failed (max_steps)" not in log_contents
        assert "Outcome: failed (max_turns)" not in log_contents

    def test_non_code_nonzero_exit_preserves_test_failure_marker(self, tmp_path: Path):
        """Generic nonzero failures should still preserve non-runner agent failure markers."""
        exit_code, store, task, _, _ = self._run_non_code_task_with_log_markers(
            tmp_path,
            exit_code=1,
            error_type=None,
            slug="20260504-plan-exit-code-test-failure",
            log_markers=("TEST_FAILURE",),
            include_report_result=False,
        )

        assert exit_code == 0
        failed = store.get(task.id)
        assert failed is not None
        assert failed.status == "failed"
        assert failed.failure_reason == "TEST_FAILURE"

    def test_non_code_nonzero_exit_persists_agent_forfeit_marker(self, tmp_path: Path):
        """Generic nonzero failures should persist AGENT_FORFEIT from the final marker."""
        exit_code, store, task, mock_task_footer, _ = self._run_non_code_task_with_log_markers(
            tmp_path,
            exit_code=1,
            error_type=None,
            slug="20260505-plan-exit-code-agent-forfeit",
            log_markers=("MAX_TURNS", "AGENT_FORFEIT"),
            include_report_result=False,
        )

        assert exit_code == 0
        failed = store.get(task.id)
        assert failed is not None
        assert failed.status == "failed"
        assert failed.failure_reason == "AGENT_FORFEIT"

        assert mock_task_footer.call_count == 1
        assert mock_task_footer.call_args.kwargs["status"] == "Failed: MockProvider exited with code 1"

        assert failed.log_file is not None
        log_contents = ops_log_path_for(tmp_path / failed.log_file).read_text()
        assert "Outcome: failed (exit_code=1)" in log_contents
        assert "Outcome: failed (timeout after 10m)" not in log_contents
        assert "Outcome: failed (max_steps)" not in log_contents

    def test_non_code_task_marks_terminal_step_interrupted_on_max_steps(self, tmp_path: Path):
        """Persisted run steps should reflect interruption on max-steps failures."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Plan task", task_type="plan")
        task.slug = "20260225-plan-task"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 2

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.return_value = RunResult(
            exit_code=0,
            duration_seconds=4.2,
            num_steps_computed=3,
            error_type="max_steps",
            _accumulated_data={
                "run_step_events": [
                    {
                        "message_role": "assistant",
                        "message_text": "First",
                        "legacy_turn_id": "T1",
                        "legacy_event_id": None,
                        "substeps": [],
                        "outcome": "completed",
                        "summary": None,
                    },
                    {
                        "message_role": "assistant",
                        "message_text": "Second",
                        "legacy_turn_id": "T2",
                        "legacy_event_id": None,
                        "substeps": [],
                        "outcome": "completed",
                        "summary": None,
                    },
                ]
            },
        )

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        with patch("gza.runner.console"):
            exit_code = _run_non_code_task(task, config, store, mock_provider, mock_git)

        assert exit_code == 0
        steps = store.get_run_steps(task.id)
        assert len(steps) == 2
        assert steps[0].outcome == "completed"
        assert steps[1].outcome == "interrupted"

    def test_non_code_task_marks_terminal_step_failed_on_nonzero_exit(self, tmp_path: Path):
        """Persisted run steps should reflect terminal failure when provider exits non-zero."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Plan task", task_type="plan")
        task.slug = "20260225-plan-task"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 20

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.return_value = RunResult(
            exit_code=1,
            duration_seconds=4.2,
            _accumulated_data={
                "run_step_events": [
                    {
                        "message_role": "assistant",
                        "message_text": "Only step",
                        "legacy_turn_id": "T1",
                        "legacy_event_id": None,
                        "substeps": [],
                        "outcome": "completed",
                        "summary": None,
                    }
                ]
            },
        )

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        with patch("gza.runner.console"):
            exit_code = _run_non_code_task(task, config, store, mock_provider, mock_git)

        assert exit_code == 0
        failed = store.get(task.id)
        assert failed is not None
        assert failed.status == "failed"
        steps = store.get_run_steps(task.id)
        assert len(steps) == 1
        assert steps[0].outcome == "failed"

    def test_run_non_code_task_skips_pr_posting_for_explore(self, tmp_path: Path):
        """Test that _run_non_code_task does NOT call post_review_to_pr for explore tasks."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create an explore task
        explore_task = store.add(
            prompt="Explore the codebase",
            task_type="explore",
        )
        explore_task.slug = "20260212-explore-the-codebase"
        store.update(explore_task)

        # Setup config
        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.learnings_interval = 0
        config.learnings_window = 25

        # Create exploration directory structure
        explore_dir = tmp_path / ".gza" / "explorations"
        explore_dir.mkdir(parents=True, exist_ok=True)

        # Track if post_review_to_pr was called
        pr_post_called = []

        def mock_post_review_to_pr(review_task, impl_task, store, project_dir, required=False, **kwargs):
            pr_post_called.append(True)

        # Mock provider
        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_result = RunResult(
            exit_code=0,
            duration_seconds=10.0,
            num_turns_reported=5,
            cost_usd=0.05,
            session_id="test-session",
            error_type=None,
        )
        mock_provider.run.return_value = mock_result

        # Mock git
        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        # Create worktree directory and report file
        worktree_path = config.worktree_path / f"{explore_task.slug}-explore"
        worktree_explore_dir = worktree_path / ".gza" / "explorations"
        worktree_explore_dir.mkdir(parents=True, exist_ok=True)
        report_file = worktree_explore_dir / f"{explore_task.slug}.md"
        report_file.write_text("# Exploration\n\nFindings here.")

        import gza.runner
        original_post_review = gza.runner.post_review_to_pr
        gza.runner.post_review_to_pr = mock_post_review_to_pr

        try:
            # Call _run_non_code_task
            exit_code = _run_non_code_task(
                explore_task, config, store, mock_provider, mock_git, resume=False
            )

            # Verify success
            assert exit_code == 0

            # Verify post_review_to_pr was NOT called (not a review task)
            assert len(pr_post_called) == 0
        finally:
            gza.runner.post_review_to_pr = original_post_review


class TestNonCodeWorktreeCleanup:
    """Tests for worktree cleanup behavior in _run_non_code_task."""

    def _make_config(self, tmp_path: Path) -> Mock:
        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 50
        config.learnings_interval = 0
        config.learnings_window = 25
        return config

    def test_success_path_calls_worktree_remove(self, tmp_path: Path):
        """On success, git.worktree_remove is called after the report is copied back."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Explore the codebase", task_type="explore")
        task.slug = "20260301-explore-the-codebase"
        store.update(task)

        config = self._make_config(tmp_path)
        worktree_path = config.worktree_path / f"{task.slug}-explore"

        def provider_run(_config, _prompt, _log_file, _work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            # Simulate the provider creating the report inside the worktree
            report_dir = worktree_path / ".gza" / "explorations"
            report_dir.mkdir(parents=True, exist_ok=True)
            (report_dir / f"{task.slug}.md").write_text("# Exploration\n\nFindings.")
            return RunResult(
                exit_code=0,
                duration_seconds=2.0,
                num_turns_reported=3,
                cost_usd=0.02,
                session_id="session-1",
                error_type=None,
            )

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.side_effect = provider_run

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        with patch("gza.runner.console"):
            exit_code = _run_non_code_task(task, config, store, mock_provider, mock_git, resume=False)

        assert exit_code == 0
        mock_git.worktree_remove.assert_called_once_with(worktree_path, force=True)

    def test_success_path_git_error_falls_back_to_shutil_rmtree(self, tmp_path: Path):
        """When git.worktree_remove raises GitError on cleanup, shutil.rmtree is used as fallback."""
        from gza.git import GitError

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Explore the codebase", task_type="explore")
        task.slug = "20260301-explore-the-codebase"
        store.update(task)

        config = self._make_config(tmp_path)
        worktree_path = config.worktree_path / f"{task.slug}-explore"

        def provider_run(_config, _prompt, _log_file, _work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            report_dir = worktree_path / ".gza" / "explorations"
            report_dir.mkdir(parents=True, exist_ok=True)
            (report_dir / f"{task.slug}.md").write_text("# Exploration\n\nFindings.")
            return RunResult(
                exit_code=0,
                duration_seconds=2.0,
                num_turns_reported=3,
                cost_usd=0.02,
                session_id="session-2",
                error_type=None,
            )

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.side_effect = provider_run

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)
        mock_git.worktree_remove.side_effect = GitError("worktree remove failed")

        with patch("gza.runner.console"), patch("gza.runner.shutil.rmtree") as mock_rmtree:
            exit_code = _run_non_code_task(task, config, store, mock_provider, mock_git, resume=False)

        assert exit_code == 0
        mock_rmtree.assert_any_call(worktree_path, ignore_errors=True)

    def test_failure_path_max_steps_prints_worktree_path(self, tmp_path: Path):
        """On max-steps failure, output should contain 'Worktree preserved for inspection'."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Explore the codebase", task_type="explore")
        task.slug = "20260301-explore-the-codebase"
        store.update(task)

        config = self._make_config(tmp_path)

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.return_value = RunResult(
            exit_code=0,
            duration_seconds=5.0,
            num_steps_computed=51,
            error_type="max_steps",
        )

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        printed_lines: list[str] = []

        def capture_print(*args, **kwargs):
            printed_lines.append(str(args[0]) if args else "")

        worktree_path = config.worktree_path / f"{task.slug}-explore"

        with patch("gza.runner.console") as mock_runner_console, \
             patch("gza.console.console") as mock_console_console:
            mock_runner_console.print.side_effect = capture_print
            mock_console_console.print.side_effect = capture_print
            exit_code = _run_non_code_task(task, config, store, mock_provider, mock_git, resume=False)

        assert exit_code == 0
        all_output = "\n".join(printed_lines)
        # Failure footer surfaces the preserved worktree via the "Worktree:" field
        # in the centralized task_footer (see src/gza/console.py). The old
        # "Worktree preserved for inspection" phrasing is gone.
        assert "Worktree:" in all_output
        assert str(worktree_path) in all_output


class TestFailureReasonGroundTruth:
    """Code-task regressions for failure reason precedence."""

    def _make_config(self, tmp_path: Path, db_path: Path) -> Mock:
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
        config.timeout_minutes = 15
        config.max_turns = 50
        config.branch_mode = "multi"
        config.project_name = "test"
        config.project_prefix = "gza"
        config.branch_strategy = Mock()
        config.branch_strategy.pattern = "{project}/{task_id}"
        config.branch_strategy.default_type = "feature"
        config.get_reasoning_effort_for_task.return_value = ""
        config.learnings_interval = 0
        config.learnings_window = 25
        return config

    def _run_code_task_failure(
        self,
        tmp_path: Path,
        *,
        exit_code: int,
        error_type: str | None,
        session_id: str | None,
        slug: str,
        log_markers: tuple[str, ...] = ("TEST_FAILURE", "MAX_TURNS"),
        commits_ahead: int = 0,
        num_steps_computed: int | None = None,
        num_turns_computed: int | None = None,
    ) -> tuple[int, SqliteTaskStore, Task, MagicMock]:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Implement feature", task_type="implement")
        task.slug = slug
        store.update(task)

        config = self._make_config(tmp_path, db_path)

        def provider_run(_config, _prompt, log_file, _work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            marker_text = "".join(f"[GZA_FAILURE:{marker}]\n" for marker in log_markers)
            log_file.write_text(f"agent command output\n{marker_text}")
            return RunResult(
                exit_code=exit_code,
                duration_seconds=12.0,
                num_steps_computed=num_steps_computed if num_steps_computed is not None else (51 if error_type == "max_steps" else None),
                num_turns_computed=num_turns_computed if num_turns_computed is not None else (51 if error_type == "max_turns" else None),
                session_id=session_id,
                error_type=error_type,
            )

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.check_credentials.return_value = True
        mock_provider.verify_credentials.return_value = True
        mock_provider.run.side_effect = provider_run

        mock_main_git = Mock()
        mock_main_git.default_branch.return_value = "main"
        mock_main_git.worktree_list.return_value = []
        mock_main_git.worktree_add.return_value = config.worktree_path / task.slug
        mock_main_git.branch_exists.return_value = False
        mock_main_git.count_commits_ahead.return_value = 0
        mock_main_git._run.return_value = Mock(returncode=0, stdout="", stderr="")

        mock_worktree_git = Mock()
        mock_worktree_git.status_porcelain.return_value = set()
        mock_worktree_git.has_changes.return_value = False
        mock_worktree_git.default_branch.return_value = "main"
        mock_worktree_git.count_commits_ahead.return_value = commits_ahead
        mock_worktree_git.get_diff_numstat.return_value = ""

        with (
            patch("gza.runner.get_provider", return_value=mock_provider),
            patch("gza.runner.get_effective_config_for_task", return_value=("", "claude", 50)),
            patch("gza.runner.Git", side_effect=[mock_main_git, mock_worktree_git]),
            patch("gza.runner.load_dotenv"),
            patch("gza.runner.build_prompt", return_value="prompt"),
            patch("gza.runner.task_footer") as mock_task_footer,
        ):
            exit_status = run(config, task_id=task.id)

        return exit_status, store, task, mock_task_footer

    def test_code_task_uses_timeout_ground_truth_over_log_markers(self, tmp_path: Path):
        """Host timeout exit code should persist TIMEOUT even if logs contain markers."""
        exit_code, store, task, _ = self._run_code_task_failure(
            tmp_path,
            exit_code=124,
            error_type=None,
            session_id="timeout-session",
            slug="20260504-implement-timeout-ground-truth",
        )

        assert exit_code == 0
        failed = store.get(task.id)
        assert failed is not None
        assert failed.status == "failed"
        assert failed.failure_reason == "TIMEOUT"

        decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
        assert decision.action == "resume"
        assert decision.launch_mode == "iterate"
        assert decision.reason_code == "TIMEOUT"

    def test_run_uses_loaded_legacy_task_type_timeout_for_provider_handoff(self, tmp_path: Path):
        """A valid task_types.<type>.timeout_minutes should reach provider.run unchanged."""
        (tmp_path / "gza.yaml").write_text(
            "project_name: test\n"
            "provider: claude\n"
            "timeout_minutes: 10\n"
            "task_types:\n"
            "  implement:\n"
            "    timeout_minutes: 25\n"
        )
        config = Config.load(tmp_path)
        store = SqliteTaskStore.from_config(config)
        task = store.add(prompt="Implement feature", task_type="implement")
        task.slug = "20260601-implement-timeout-handoff"
        store.update(task)

        captured_timeout: dict[str, int] = {}

        def provider_run(run_config, _prompt, log_file, _work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            captured_timeout["minutes"] = run_config.timeout_minutes
            log_file.write_text("agent command output\n")
            return RunResult(
                exit_code=1,
                duration_seconds=1.0,
                error_type=None,
            )

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.check_credentials.return_value = True
        mock_provider.verify_credentials.return_value = True
        mock_provider.run.side_effect = provider_run

        mock_main_git = Mock()
        mock_main_git.default_branch.return_value = "main"
        mock_main_git.worktree_list.return_value = []
        mock_main_git.worktree_add.return_value = config.worktree_path / task.slug
        mock_main_git.branch_exists.return_value = False
        mock_main_git.count_commits_ahead.return_value = 0
        mock_main_git._run.return_value = Mock(returncode=0, stdout="", stderr="")

        mock_worktree_git = Mock()
        mock_worktree_git.status_porcelain.return_value = set()
        mock_worktree_git.has_changes.return_value = False
        mock_worktree_git.default_branch.return_value = "main"
        mock_worktree_git.count_commits_ahead.return_value = 0
        mock_worktree_git.get_diff_numstat.return_value = ""

        with (
            patch("gza.runner.get_provider", return_value=mock_provider),
            patch("gza.runner.get_effective_config_for_task", return_value=("", "claude", 50)),
            patch("gza.runner.Git", side_effect=[mock_main_git, mock_worktree_git]),
            patch("gza.runner.load_dotenv"),
            patch("gza.runner.build_prompt", return_value="prompt"),
            patch("gza.runner.task_footer"),
        ):
            exit_status = run(config, task_id=task.id)

        assert exit_status == 0
        assert captured_timeout["minutes"] == 25

    def test_code_task_uses_max_steps_ground_truth_over_log_markers(self, tmp_path: Path):
        """Provider max_steps should persist MAX_STEPS even if logs contain markers."""
        exit_code, store, task, _ = self._run_code_task_failure(
            tmp_path,
            exit_code=0,
            error_type="max_steps",
            session_id="max-steps-session",
            slug="20260504-implement-max-steps-ground-truth",
        )

        assert exit_code == 0
        failed = store.get(task.id)
        assert failed is not None
        assert failed.status == "failed"
        assert failed.failure_reason == "MAX_STEPS"

    def test_code_task_uses_max_turns_ground_truth_over_log_markers(self, tmp_path: Path):
        """Provider max_turns should persist MAX_TURNS even if logs contain markers."""
        exit_code, store, task, _ = self._run_code_task_failure(
            tmp_path,
            exit_code=0,
            error_type="max_turns",
            session_id="max-turns-session",
            slug="20260504-implement-max-turns-ground-truth",
        )

        assert exit_code == 0
        failed = store.get(task.id)
        assert failed is not None
        assert failed.status == "failed"
        assert failed.failure_reason == "MAX_TURNS"

    def test_code_task_below_step_limit_does_not_mark_max_steps(self, tmp_path: Path):
        """Provider max_steps below the configured limit should not persist MAX_STEPS."""
        exit_code, store, task, mock_task_footer = self._run_code_task_failure(
            tmp_path,
            exit_code=0,
            error_type="max_steps",
            session_id="below-max-steps-session",
            slug="20260505-implement-below-max-steps",
            log_markers=(),
            num_steps_computed=49,
        )

        assert exit_code == 0
        failed = store.get(task.id)
        assert failed is not None
        assert failed.status == "failed"
        assert failed.failure_reason == "UNKNOWN"
        assert mock_task_footer.call_count == 1
        assert mock_task_footer.call_args.kwargs["status"] == "Failed: MockProvider reported max_steps"
        assert failed.log_file is not None
        log_contents = ops_log_path_for(tmp_path / failed.log_file).read_text()
        assert "Outcome: failed (error_type=max_steps)" in log_contents
        assert "Outcome: failed (max_steps)" not in log_contents

    def test_code_task_below_turn_limit_does_not_mark_max_turns(self, tmp_path: Path):
        """Provider max_turns below the configured limit should not persist MAX_TURNS."""
        exit_code, store, task, mock_task_footer = self._run_code_task_failure(
            tmp_path,
            exit_code=0,
            error_type="max_turns",
            session_id="below-max-turns-session",
            slug="20260505-implement-below-max-turns",
            log_markers=(),
            num_turns_computed=49,
        )

        assert exit_code == 0
        failed = store.get(task.id)
        assert failed is not None
        assert failed.status == "failed"
        assert failed.failure_reason == "UNKNOWN"
        assert mock_task_footer.call_count == 1
        assert mock_task_footer.call_args.kwargs["status"] == "Failed: MockProvider reported max_turns"
        assert failed.log_file is not None
        log_contents = ops_log_path_for(tmp_path / failed.log_file).read_text()
        assert "Outcome: failed (error_type=max_turns)" in log_contents
        assert "Outcome: failed (max_turns)" not in log_contents

    def test_code_task_prefers_timeout_ground_truth_over_provider_max_steps(self, tmp_path: Path):
        """Combined timeout and provider max_steps signals should render and persist TIMEOUT."""
        exit_code, store, task, mock_task_footer = self._run_code_task_failure(
            tmp_path,
            exit_code=124,
            error_type="max_steps",
            session_id="timeout-wins-session",
            slug="20260504-implement-timeout-over-max-steps",
        )

        assert exit_code == 0
        failed = store.get(task.id)
        assert failed is not None
        assert failed.status == "failed"
        assert failed.failure_reason == "TIMEOUT"

        assert mock_task_footer.call_count == 1
        assert mock_task_footer.call_args.kwargs["status"] == "Failed: MockProvider timed out after 15 minutes"

        assert failed.log_file is not None
        log_contents = ops_log_path_for(tmp_path / failed.log_file).read_text()
        assert "Outcome: failed (timeout after 15m)" in log_contents
        assert "Outcome: failed (max_steps)" not in log_contents

        decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
        assert decision.action == "resume"
        assert decision.launch_mode == "iterate"
        assert decision.reason_code == "TIMEOUT"

    def test_code_task_timeout_persists_usage_recovered_from_transcript(self, tmp_path: Path) -> None:
        """Timed-out code tasks should persist transcript usage instead of near-zero live stats."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Implement feature", task_type="implement")
        task.slug = "20260603-implement-timeout-transcript-usage"
        store.update(task)

        config = self._make_config(tmp_path, db_path)

        def provider_run(
            _config,
            _prompt,
            log_file,
            _work_dir,
            resume_session_id=None,
            on_session_id=None,
            on_step_count=None,
        ):
            _ = resume_session_id, on_session_id, on_step_count
            log_file.write_text(
                json.dumps(
                    {
                        "type": "assistant",
                        "message": {
                            "id": "msg_usage_timeout",
                            "usage": {
                                "input_tokens": 10,
                                "cache_creation_input_tokens": 2,
                                "cache_read_input_tokens": 3,
                                "output_tokens": 5,
                            },
                            "content": [{"type": "text", "text": "Investigating timeout fallback."}],
                        },
                    }
                )
                + "\n"
            )
            return RunResult(
                exit_code=124,
                duration_seconds=12.0,
                num_steps_computed=0,
                input_tokens=0,
                output_tokens=1,
                cost_usd=0.0,
                session_id="timeout-usage-session",
                error_type=None,
            )

        mock_provider = Mock()
        mock_provider.name = "claude"
        mock_provider.check_credentials.return_value = True
        mock_provider.verify_credentials.return_value = True
        mock_provider.run.side_effect = provider_run

        mock_main_git = Mock()
        mock_main_git.default_branch.return_value = "main"
        mock_main_git.worktree_list.return_value = []
        mock_main_git.worktree_add.return_value = config.worktree_path / task.slug
        mock_main_git.branch_exists.return_value = False
        mock_main_git.count_commits_ahead.return_value = 0
        mock_main_git._run.return_value = Mock(returncode=0, stdout="", stderr="")

        mock_worktree_git = Mock()
        mock_worktree_git.status_porcelain.return_value = set()
        mock_worktree_git.has_changes.return_value = False
        mock_worktree_git.default_branch.return_value = "main"
        mock_worktree_git.count_commits_ahead.return_value = 0
        mock_worktree_git.get_diff_numstat.return_value = ""

        with (
            patch("gza.runner.get_provider", return_value=mock_provider),
            patch("gza.runner.get_effective_config_for_task", return_value=("", "claude", 50)),
            patch("gza.runner.Git", side_effect=[mock_main_git, mock_worktree_git]),
            patch("gza.runner.load_dotenv"),
            patch("gza.runner.build_prompt", return_value="prompt"),
            patch("gza.runner.task_footer"),
        ):
            exit_status = run(config, task_id=task.id)

        assert exit_status == 0
        failed = store.get(task.id)
        assert failed is not None
        assert failed.status == "failed"
        assert failed.failure_reason == "TIMEOUT"
        assert failed.num_steps_computed == 1
        assert failed.input_tokens == 15
        assert failed.output_tokens == 5
        assert failed.cost_usd is not None and failed.cost_usd > 0.0

    def test_code_task_timeout_still_records_failure_when_checkpoint_persistence_fails(
        self,
        tmp_path: Path,
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Implement feature", task_type="implement")
        task.slug = "20260601-implement-timeout-checkpoint-write-warning"
        store.update(task)

        config = self._make_config(tmp_path, db_path)

        def provider_run(_config, _prompt, log_file, _work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            log_file.write_text("agent command output\n[GZA_FAILURE:TEST_FAILURE]\n")
            return RunResult(
                exit_code=124,
                duration_seconds=12.0,
                session_id="timeout-checkpoint-fail-session",
                error_type=None,
            )

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.check_credentials.return_value = True
        mock_provider.verify_credentials.return_value = True
        mock_provider.run.side_effect = provider_run

        mock_main_git = Mock()
        mock_main_git.default_branch.return_value = "main"
        mock_main_git.worktree_list.return_value = []
        mock_main_git.worktree_add.return_value = config.worktree_path / task.slug
        mock_main_git.branch_exists.return_value = False
        mock_main_git.count_commits_ahead.return_value = 0
        mock_main_git._run.return_value = Mock(returncode=0, stdout="", stderr="")

        mock_worktree_git = Mock()
        mock_worktree_git.status_porcelain.return_value = set()
        mock_worktree_git.has_changes.return_value = False
        mock_worktree_git.default_branch.return_value = "main"
        mock_worktree_git.count_commits_ahead.return_value = 0
        mock_worktree_git.get_diff_numstat.return_value = ""

        with (
            patch("gza.runner.get_provider", return_value=mock_provider),
            patch("gza.runner.get_effective_config_for_task", return_value=("", "claude", 50)),
            patch("gza.runner.Git", side_effect=[mock_main_git, mock_worktree_git]),
            patch("gza.runner.load_dotenv"),
            patch("gza.runner.build_prompt", return_value="prompt"),
            patch("gza.runner.task_footer"),
            patch("gza.runner._write_timeout_resume_checkpoint", side_effect=OSError("disk full")),
        ):
            exit_status = run(config, task_id=task.id)

        assert exit_status == 0
        failed = store.get(task.id)
        assert failed is not None
        assert failed.status == "failed"
        assert failed.failure_reason == "TIMEOUT"
        assert failed.log_file is not None
        log_contents = ops_log_path_for(tmp_path / failed.log_file).read_text()
        assert "Outcome: failed (timeout after 15m)" in log_contents
        assert "failed to persist timeout resume checkpoint" in log_contents
        assert "disk full" in log_contents

    @pytest.mark.parametrize("marker", ["MAX_STEPS", "MAX_TURNS", "TIMEOUT"])
    def test_code_task_success_ignores_contaminated_structured_markers(self, tmp_path: Path, marker: str):
        """Scraped structured runner markers must not fail a successful code task."""
        exit_code, store, task, _ = self._run_code_task_failure(
            tmp_path,
            exit_code=0,
            error_type=None,
            session_id="success-session",
            slug=f"20260504-implement-success-ignores-{marker.lower().replace('_', '-')}",
            log_markers=(marker,),
            commits_ahead=1,
        )

        assert exit_code == 0
        completed = store.get(task.id)
        assert completed is not None
        assert completed.status == "completed"
        assert completed.failure_reason is None

        assert completed.log_file is not None
        log_contents = ops_log_path_for(tmp_path / completed.log_file).read_text()
        assert "Outcome: completed" in log_contents
        assert "Outcome: failed (timeout after 15m)" not in log_contents
        assert "Outcome: failed (max_steps)" not in log_contents
        assert "Outcome: failed (max_turns)" not in log_contents

    @pytest.mark.parametrize("marker", ["MAX_STEPS", "MAX_TURNS", "TIMEOUT"])
    def test_code_task_nonzero_exit_with_contaminated_structured_markers_records_unknown(
        self,
        tmp_path: Path,
        marker: str,
    ):
        """Generic nonzero failures must not reuse runner-owned reasons from the log."""
        exit_code, store, task, mock_task_footer = self._run_code_task_failure(
            tmp_path,
            exit_code=1,
            error_type=None,
            session_id="exit-code-session",
            slug=f"20260504-implement-exit-code-fallback-{marker.lower().replace('_', '-')}",
            log_markers=(marker,),
        )

        assert exit_code == 0
        failed = store.get(task.id)
        assert failed is not None
        assert failed.status == "failed"
        assert failed.failure_reason == "UNKNOWN"

        assert mock_task_footer.call_count == 1
        assert mock_task_footer.call_args.kwargs["status"] == "Failed: MockProvider exited with code 1"

        assert failed.log_file is not None
        log_contents = ops_log_path_for(tmp_path / failed.log_file).read_text()
        assert "Outcome: failed (exit_code=1)" in log_contents
        assert "Outcome: failed (timeout after 15m)" not in log_contents
        assert "Outcome: failed (max_steps)" not in log_contents
        assert "Outcome: failed (max_turns)" not in log_contents

    def test_code_task_nonzero_exit_preserves_test_failure_marker(self, tmp_path: Path):
        """Generic nonzero failures should still preserve non-runner agent failure markers."""
        exit_code, store, task, _ = self._run_code_task_failure(
            tmp_path,
            exit_code=1,
            error_type=None,
            session_id="test-failure-session",
            slug="20260504-implement-exit-code-test-failure",
            log_markers=("TEST_FAILURE",),
        )

        assert exit_code == 0
        failed = store.get(task.id)
        assert failed is not None
        assert failed.status == "failed"
        assert failed.failure_reason == "TEST_FAILURE"

    def test_code_task_nonzero_exit_persists_agent_forfeit_marker(self, tmp_path: Path):
        """Generic nonzero code-task failures should persist AGENT_FORFEIT from the final marker."""
        exit_code, store, task, mock_task_footer = self._run_code_task_failure(
            tmp_path,
            exit_code=1,
            error_type=None,
            session_id="exit-code-agent-forfeit-session",
            slug="20260505-implement-exit-code-agent-forfeit",
            log_markers=("MAX_TURNS", "AGENT_FORFEIT"),
        )

        assert exit_code == 0
        failed = store.get(task.id)
        assert failed is not None
        assert failed.status == "failed"
        assert failed.failure_reason == "AGENT_FORFEIT"

        assert mock_task_footer.call_count == 1
        assert mock_task_footer.call_args.kwargs["status"] == "Failed: MockProvider exited with code 1"

        assert failed.log_file is not None
        log_contents = ops_log_path_for(tmp_path / failed.log_file).read_text()
        assert "Outcome: failed (exit_code=1)" in log_contents
        assert "Outcome: failed (timeout after 15m)" not in log_contents
        assert "Outcome: failed (max_steps)" not in log_contents


class TestRunStepPersistenceIntegration:
    """Integration tests for persisting provider step/substep events."""

    def test_non_code_task_persists_steps_from_real_claude_fixture(self, tmp_path: Path):
        """_run_non_code_task should persist run_steps/run_substeps from provider parsing."""
        import json

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Plan task", task_type="plan")
        task.slug = "20260226-plan-task"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 20
        config.model = ""
        config.chat_text_display_length = 80
        config.claude = Mock(args=[])
        config.tmux = Mock(session_name=None)

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        provider = ClaudeProvider()
        json_lines = [
            json.dumps(
                {
                    "type": "assistant",
                    "message": {
                        "id": "msg_1",
                        "usage": {"input_tokens": 10, "output_tokens": 3},
                        "content": [
                            {"type": "text", "text": "I will inspect the code."},
                            {"type": "tool_use", "id": "tool_1", "name": "Bash", "input": {"command": "ls -la"}},
                        ],
                    },
                }
            )
            + "\n",
            json.dumps({"type": "result", "subtype": "success", "num_turns": 1, "total_cost_usd": 0.001}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen, patch("gza.runner.console"):
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            exit_code = _run_non_code_task(task, config, store, provider, mock_git, resume=False)

        assert exit_code == 0

        steps = store.get_run_steps(task.id)
        assert len(steps) == 1
        assert steps[0].step_id == "S1"
        assert steps[0].provider == "claude"
        assert steps[0].message_text == "I will inspect the code."
        assert steps[0].outcome == "completed"
        updated_task = store.get(task.id)
        assert updated_task is not None
        assert updated_task.log_schema_version == 2

        step_ref = StepRef(
            id=steps[0].id,
            run_id=steps[0].run_id,
            step_index=steps[0].step_index,
            step_id=steps[0].step_id,
        )
        substeps = store.get_run_substeps(step_ref)
        assert len(substeps) == 1
        assert substeps[0].substep_id == "S1.1"
        assert substeps[0].type == "tool_call"
        assert substeps[0].payload == {
            "tool_name": "Bash",
            "tool_input": {"command": "ls -la"},
        }

    def test_on_step_count_updates_task_num_steps_computed_in_real_time(self, tmp_path: Path):
        """on_step_count callback should update task.num_steps_computed in DB during streaming."""
        import json

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Plan task", task_type="plan")
        task.slug = "20260302-plan-task"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 20
        config.model = ""
        config.chat_text_display_length = 80
        config.claude = Mock(args=[])
        config.tmux = Mock(session_name=None)

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        # Use two steps so we can verify intermediate DB state
        intermediate_counts: list[int] = []

        def capturing_store_update(t: Task) -> None:
            if t.num_steps_computed is not None:
                intermediate_counts.append(t.num_steps_computed)
            original_update(t)

        original_update = store.update
        store.update = capturing_store_update  # type: ignore[method-assign]

        provider = ClaudeProvider()
        json_lines = [
            json.dumps({
                "type": "assistant",
                "message": {"id": "msg_1", "content": [], "usage": {}},
            }) + "\n",
            json.dumps({
                "type": "assistant",
                "message": {"id": "msg_2", "content": [], "usage": {}},
            }) + "\n",
            json.dumps({"type": "result", "subtype": "success", "num_turns": 2, "total_cost_usd": 0.0}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen, patch("gza.runner.console"):
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            exit_code = _run_non_code_task(task, config, store, provider, mock_git, resume=False)

        assert exit_code == 0
        # The callback should have been called for each step (1, then 2)
        assert 1 in intermediate_counts
        assert 2 in intermediate_counts

    def test_non_code_interrupt_persists_session_and_step_callbacks_before_failure(self, tmp_path: Path):
        """Interrupted non-code runs should keep callback-persisted session and step state."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Plan task", task_type="plan")
        task.slug = "20260422-plan-interrupt"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 20
        config.model = ""
        config.chat_text_display_length = 80
        config.claude = Mock(args=[])
        config.tmux = Mock(session_name=None)

        def _interrupting_run(
            _config,
            _prompt,
            _log_file,
            _work_dir,
            resume_session_id=None,
            on_session_id=None,
            on_step_count=None,
            interactive=False,
        ):
            del resume_session_id, interactive
            assert on_session_id is not None
            assert on_step_count is not None
            on_session_id("sess-interrupted-inline")
            on_step_count(3)
            raise KeyboardInterrupt

        mock_provider = Mock()
        mock_provider.name = "Claude"
        mock_provider.run.side_effect = _interrupting_run

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=1)

        with (
            patch("gza.runner.console"),
            patch("gza.runner._snapshot_task_db_to_worktree"),
            patch("gza.runner._copy_learnings_to_worktree"),
            patch("gza.runner._create_local_dep_symlinks"),
        ):
            exit_code = _run_non_code_task(
                task,
                config,
                store,
                mock_provider,
                mock_git,
                resume=False,
                interaction_mode="interactive",
            )

        assert exit_code == 130
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "INTERRUPTED"
        assert refreshed.session_id == "sess-interrupted-inline"
        assert refreshed.num_steps_computed == 3

    def test_non_code_sigterm_interrupt_marks_terminated(self, tmp_path: Path):
        """SIGTERM-driven interrupts should be classified separately and log their source."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Plan task", task_type="plan")
        task.slug = "20260422-plan-terminated"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 20
        config.model = ""
        config.chat_text_display_length = 80
        config.claude = Mock(args=[])
        config.tmux = Mock(session_name=None)

        def _interrupting_run(
            _config,
            _prompt,
            _log_file,
            _work_dir,
            resume_session_id=None,
            on_session_id=None,
            on_step_count=None,
            interactive=False,
        ):
            del resume_session_id, on_step_count, interactive
            assert on_session_id is not None
            on_session_id("sess-terminated-inline")
            raise KeyboardInterrupt

        mock_provider = Mock()
        mock_provider.name = "Claude"
        mock_provider.run.side_effect = _interrupting_run

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=1)

        with (
            patch.dict(
                os.environ,
                {
                    "GZA_INTERRUPT_SIGNAL": "SIGTERM",
                    "GZA_INTERRUPT_SOURCE": "watch_reconcile_no_activity",
                    "GZA_INTERRUPT_DETAIL": "watch reconciliation detected no recent task log activity",
                },
                clear=False,
            ),
            patch("gza.runner.console"),
            patch("gza.runner._snapshot_task_db_to_worktree"),
            patch("gza.runner._copy_learnings_to_worktree"),
            patch("gza.runner._create_local_dep_symlinks"),
        ):
            exit_code = _run_non_code_task(
                task,
                config,
                store,
                mock_provider,
                mock_git,
                resume=False,
                interaction_mode="interactive",
            )

        assert exit_code == 130
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "TERMINATED"
        assert refreshed.session_id == "sess-terminated-inline"
        assert refreshed.log_file is not None

        log_path = tmp_path / refreshed.log_file
        log_text = ops_log_path_for(log_path).read_text()
        assert '"subtype": "interrupt"' in log_text
        assert '"source": "watch_reconcile_no_activity"' in log_text


class TestResumeVerificationPrompt:
    """Tests for resume verification prompt injection."""

    def test_resume_code_task_includes_verification_prompt(self, tmp_path: Path):
        """Test that resuming a code task includes todo list verification instructions."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a failed task with session_id
        task = store.add(
            prompt="Implement feature X",
            task_type="implement",
        )
        task.slug = "20260212-implement-feature-x"
        task.branch = "gza/20260212-implement-feature-x"
        task.session_id = "test-session-123"
        store.mark_failed(task, log_file="logs/test.log", stats=None)

        # Setup config
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
        config.max_turns = 50
        config.timeout_minutes = 60
        config.branch_mode = "multi"
        config.project_name = "test"
        config.branch_strategy = Mock()
        config.branch_strategy.pattern = "{project}/{task_id}"
        config.branch_strategy.default_type = "feature"
        config.get_provider_for_task.return_value = "claude"
        config.get_model_for_task.return_value = None
        config.get_max_steps_for_task.return_value = 50
        config.learnings_interval = 0
        config.learnings_window = 25

        # Mock provider to capture the prompt
        captured_prompts = []

        def mock_provider_run(config, prompt, log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            captured_prompts.append({
                'prompt': prompt,
                'resume_session_id': resume_session_id
            })
            return RunResult(
                exit_code=0,
                duration_seconds=10.0,
                num_turns_reported=5,
                cost_usd=0.05,
                session_id="test-session-123",
                error_type=None,
            )

        # Mock provider and git
        with patch('gza.runner.get_provider') as mock_get_provider, \
             patch('gza.runner.Git') as mock_git_class, \
             patch('gza.runner.load_dotenv'):

            mock_provider = Mock()
            mock_provider.name = "TestProvider"
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_provider.run = mock_provider_run
            mock_get_provider.return_value = mock_provider

            # Mock Git
            mock_git = Mock()
            mock_git.default_branch.return_value = "main"
            mock_git._run.return_value = Mock(returncode=0)
            mock_git.branch_exists.return_value = True
            mock_git.worktree_add = Mock()
            mock_git.worktree_list.return_value = []

            # Mock has_changes to return True
            mock_worktree_git = Mock()
            mock_worktree_git.has_changes.return_value = True
            # status_porcelain: simulate a file changed during provider run
            mock_worktree_git.status_porcelain.side_effect = [
                set(),  # pre-run snapshot (called before provider.run)
                {("M", "changed.py")},  # post-run snapshot
            ]
            mock_worktree_git.add = Mock()
            mock_worktree_git.commit = Mock()
            mock_worktree_git.get_diff_numstat.return_value = ""
            # Mock _run for WIP functions (squash, restore)
            mock_log_result = Mock()
            mock_log_result.stdout = "WIP: gza task interrupted"
            mock_worktree_git._run.return_value = mock_log_result

            mock_git_class.side_effect = [mock_git, mock_worktree_git]

            # Create worktree directory
            worktree_path = config.worktree_path / task.slug
            worktree_path.mkdir(parents=True, exist_ok=True)

            # Create summary file in worktree
            summary_dir = worktree_path / ".gza" / "summaries"
            summary_dir.mkdir(parents=True, exist_ok=True)
            summary_file = summary_dir / f"{task.slug}.md"
            summary_file.write_text("# Summary\n\nCompleted the task.")

            # Run with resume=True
            result = run(config, task_id=task.id, resume=True)

            # Verify success
            assert result == 0

            # Verify prompt was captured
            assert len(captured_prompts) == 1
            prompt = captured_prompts[0]['prompt']
            resume_session_id = captured_prompts[0]['resume_session_id']

            # Verify verification instructions are in the prompt
            assert "interrupted" in prompt.lower()
            assert "git status" in prompt.lower()
            assert "git log" in prompt.lower()
            assert "todo list" in prompt.lower()
            assert "continue from the actual state" in prompt.lower()

            # Verify resume_session_id was passed
            assert resume_session_id == "test-session-123"

    def test_resume_non_code_task_includes_verification_prompt(self, tmp_path: Path):
        """Test that resuming a non-code task includes todo list verification instructions."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a failed review task with session_id
        impl_task = store.add(
            prompt="Implement feature X",
            task_type="implement",
        )
        impl_task.status = "completed"
        impl_task.slug = "20260212-implement-feature-x"
        impl_task.branch = "gza/20260212-implement-feature-x"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review feature X",
            task_type="review",
            depends_on=impl_task.id,
        )
        review_task.slug = "20260212-review-feature-x"
        review_task.session_id = "test-session-456"
        store.mark_failed(review_task, log_file="logs/test.log", stats=None)

        # Setup config
        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.learnings_interval = 0
        config.learnings_window = 25

        # Mock provider to capture the prompt
        captured_prompts = []

        def mock_provider_run(config, prompt, log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            captured_prompts.append({
                'prompt': prompt,
                'resume_session_id': resume_session_id
            })
            return RunResult(
                exit_code=0,
                duration_seconds=10.0,
                num_turns_reported=5,
                cost_usd=0.05,
                session_id="test-session-456",
                error_type=None,
            )

        mock_provider = Mock()
        mock_provider.name = "TestProvider"
        mock_provider.run = mock_provider_run

        # Mock git
        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)
        mock_git.worktree_remove = Mock()

        # Create worktree directory and report file
        worktree_path = config.worktree_path / f"{review_task.slug}-review"
        worktree_path.mkdir(parents=True, exist_ok=True)
        review_dir = worktree_path / ".gza" / "reviews"
        review_dir.mkdir(parents=True, exist_ok=True)
        report_file = review_dir / f"{review_task.slug}.md"
        report_file.write_text("# Review\n\nLooks good!")

        # Mock post_review_to_pr to avoid GitHub CLI dependency
        with patch('gza.runner.post_review_to_pr'):
            # Call _run_non_code_task with resume=True
            exit_code = _run_non_code_task(
                review_task, config, store, mock_provider, mock_git, resume=True
            )

        # Verify success
        assert exit_code == 0

        # Verify prompt was captured
        assert len(captured_prompts) == 1
        prompt = captured_prompts[0]['prompt']
        resume_session_id = captured_prompts[0]['resume_session_id']

        # Verify verification instructions are in the prompt
        assert "paused" in prompt.lower()
        assert "interrupted" not in prompt.lower()
        assert "git status" in prompt.lower()
        assert "git log" in prompt.lower()
        assert "todo list" in prompt.lower()
        assert "continue from the actual state" in prompt.lower()
        assert f"Current task DB id: {review_task.id}" in prompt
        assert f"Current task slug: {review_task.slug}" in prompt
        assert f".gza/reviews/{review_task.slug}.md" in prompt

        # Verify resume_session_id was passed
        assert resume_session_id == "test-session-456"

    def test_resume_review_new_task_id_reasserts_current_report_contract(self, tmp_path: Path):
        """Resume prompts must bind to the new review task ID/report path, not the failed ancestor."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement feature X", task_type="implement")
        impl_task.status = "completed"
        impl_task.slug = "20260212-implement-feature-x"
        impl_task.branch = "gza/20260212-implement-feature-x"
        store.update(impl_task)

        failed_review = store.add(
            prompt="Review feature X",
            task_type="review",
            depends_on=impl_task.id,
        )
        failed_review.slug = "20260212-review-feature-x"
        failed_review.session_id = "resume-session-abc"
        store.mark_failed(failed_review, log_file="logs/failed.log", stats=None)

        resumed_review = store.add(
            prompt=failed_review.prompt,
            task_type="review",
            depends_on=impl_task.id,
            based_on=failed_review.id,
        )
        resumed_review.slug = "20260213-review-feature-x-2"
        resumed_review.session_id = failed_review.session_id
        store.update(resumed_review)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.learnings_interval = 0
        config.learnings_window = 25

        captured_prompts: list[str] = []

        def provider_run(_config, prompt, _log_file, _work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            captured_prompts.append(prompt)
            report_dir = _work_dir / ".gza" / "reviews"
            report_dir.mkdir(parents=True, exist_ok=True)
            (report_dir / f"{resumed_review.slug}.md").write_text("# Review\n\nVerdict: APPROVED")
            return RunResult(
                exit_code=0,
                duration_seconds=2.0,
                num_turns_reported=1,
                cost_usd=0.01,
                session_id=resume_session_id,
                error_type=None,
            )

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.side_effect = provider_run

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)
        mock_git.get_diff_numstat.return_value = ""
        mock_git.get_diff.return_value = ""
        mock_git.get_diff_stat.return_value = ""

        with patch("gza.runner.post_review_to_pr"):
            exit_code = _run_non_code_task(
                resumed_review, config, store, mock_provider, mock_git, resume=True
            )

        assert exit_code == 0
        assert len(captured_prompts) == 1
        prompt = captured_prompts[0]
        assert f"Current task DB id: {resumed_review.id}" in prompt
        assert f"Current task slug: {resumed_review.slug}" in prompt
        assert f".gza/reviews/{resumed_review.slug}.md" in prompt
        assert f".gza/reviews/{failed_review.slug}.md" not in prompt


class TestNonCodeReportArtifactContract:
    """Regression tests for non-code report artifact contract enforcement."""

    def test_resume_review_recovers_when_single_stale_filename_written(self, tmp_path: Path):
        """Resumed review should recover from exactly one mismatched report filename."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement feature X", task_type="implement")
        impl_task.status = "completed"
        impl_task.slug = "20260212-implement-feature-x"
        impl_task.branch = "gza/20260212-implement-feature-x"
        store.update(impl_task)

        prior_review = store.add(
            prompt="Review feature X",
            task_type="review",
            depends_on=impl_task.id,
        )
        prior_review.slug = "20260212-review-feature-x"
        prior_review.session_id = "resume-session-stale"
        store.mark_failed(prior_review, log_file="logs/prior.log", stats=None)

        resumed_review = store.add(
            prompt=prior_review.prompt,
            task_type="review",
            depends_on=impl_task.id,
            based_on=prior_review.id,
        )
        resumed_review.slug = "20260213-review-feature-x-2"
        resumed_review.session_id = prior_review.session_id
        store.update(resumed_review)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 50

        def provider_run(_config, _prompt, _log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            review_dir = work_dir / ".gza" / "reviews"
            review_dir.mkdir(parents=True, exist_ok=True)
            (review_dir / f"{prior_review.slug}.md").write_text("# Review\n\nVerdict: APPROVED")
            return RunResult(
                exit_code=0,
                duration_seconds=3.0,
                num_turns_reported=2,
                cost_usd=0.01,
                session_id=resume_session_id,
                error_type=None,
            )

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.side_effect = provider_run

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        mock_console = Mock()
        with patch("gza.runner.post_review_to_pr"), patch("gza.runner.console", mock_console), patch(
            "gza.runner.maybe_auto_regenerate_learnings", return_value=None
        ):
            exit_code = _run_non_code_task(
                resumed_review, config, store, mock_provider, mock_git, resume=True
            )

        assert exit_code == 0
        refreshed = store.get(resumed_review.id)
        assert refreshed is not None
        assert refreshed.status == "completed"
        assert refreshed.failure_reason is None
        assert refreshed.output_content == "# Review\n\nVerdict: APPROVED"
        expected_host_report = tmp_path / ".gza" / "reviews" / f"{resumed_review.slug}.md"
        assert expected_host_report.exists()
        assert expected_host_report.read_text() == "# Review\n\nVerdict: APPROVED"
        warning_lines = [
            call.args[0]
            for call in mock_console.print.call_args_list
            if call.args and "recovering from mismatched file" in str(call.args[0])
        ]
        assert warning_lines
        assert resumed_review.slug in warning_lines[0]
        assert prior_review.slug in warning_lines[0]

    def test_non_code_success_without_expected_report_marks_failed_and_skips_copy_back(self, tmp_path: Path):
        """Provider success without expected report must not mark completion or copy host artifact."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        plan_task = store.add(prompt="Plan feature Y", task_type="plan")
        plan_task.slug = "20260213-plan-feature-y"
        store.update(plan_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 50

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.return_value = RunResult(
            exit_code=0,
            duration_seconds=1.5,
            num_turns_reported=1,
            cost_usd=0.01,
            session_id="session-plan",
            error_type=None,
        )

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        with patch("gza.runner.console"):
            exit_code = _run_non_code_task(plan_task, config, store, mock_provider, mock_git, resume=False)

        assert exit_code == 0
        refreshed = store.get(plan_task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "MISSING_REPORT_ARTIFACT"
        assert refreshed.report_file is None
        assert refreshed.output_content is None
        host_report = tmp_path / ".gza" / "plans" / f"{plan_task.slug}.md"
        assert not host_report.exists()

    @pytest.mark.parametrize(
        ("task_type", "artifact_dir"),
        [
            ("plan_review", "plan-reviews"),
            ("plan_improve", "revised-plans"),
        ],
    )
    def test_run_inner_routes_plan_review_and_plan_improve_through_branchless_runner(
        self,
        tmp_path: Path,
        task_type: str,
        artifact_dir: str,
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt=f"Run {task_type}", task_type=task_type)
        task.slug = f"20260213-{task_type}"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 50

        report_text = f"# {task_type}\n\nVerdict: APPROVED\n"

        def provider_run(
            _config,
            _prompt,
            _log_file,
            work_dir,
            resume_session_id=None,
            on_session_id=None,
            on_step_count=None,
        ):
            report_dir = work_dir / ".gza" / artifact_dir
            report_dir.mkdir(parents=True, exist_ok=True)
            (report_dir / f"{task.slug}.md").write_text(report_text)
            return RunResult(
                exit_code=0,
                duration_seconds=1.0,
                num_turns_reported=1,
                cost_usd=0.01,
                session_id=f"{task_type}-session",
                error_type=None,
            )

        provider = Mock()
        provider.name = "MockProvider"
        provider.run.side_effect = provider_run

        with patch("gza.runner.console"), patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None):
            exit_code = _run_inner(task, config, config, store, provider, git=None, resume=False)

        assert exit_code == 0
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "completed"
        assert refreshed.branch is None
        assert refreshed.output_content == report_text
        assert refreshed.report_file == f".gza/{artifact_dir}/{task.slug}.md"
        assert (tmp_path / refreshed.report_file).read_text() == report_text

    def test_missing_report_artifact_recovered_from_log(self, tmp_path: Path):
        """If the expected report is missing but a 'result' log entry has text, recover and complete."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        review_task = store.add(prompt="Review feature Z", task_type="review")
        review_task.slug = "20260213-review-feature-z"
        store.update(review_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 50

        review_text = "# Review\n\n**Verdict: APPROVED**\n\nLooks good."

        def provider_run(_config, _prompt, log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            # Agent outputs text to stdout (captured as 'result' event) but does NOT write the file.
            import json as _json
            with open(log_file, "a") as f:
                f.write(_json.dumps({"type": "result", "subtype": "success", "result": review_text}) + "\n")
            return RunResult(
                exit_code=0,
                duration_seconds=2.0,
                num_turns_reported=1,
                cost_usd=0.01,
                session_id="session-z",
                error_type=None,
            )

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.side_effect = provider_run

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        with patch("gza.runner.post_review_to_pr"), patch("gza.runner.console"), patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None):
            exit_code = _run_non_code_task(
                review_task, config, store, mock_provider, mock_git, resume=False
            )

        assert exit_code == 0
        refreshed = store.get(review_task.id)
        assert refreshed is not None
        assert refreshed.status == "completed", f"Expected completed, got {refreshed.status}"
        assert refreshed.output_content == review_text
        assert refreshed.report_file is not None
        host_report = tmp_path / ".gza" / "reviews" / f"{review_task.slug}.md"
        assert host_report.exists()
        assert host_report.read_text() == review_text

    def test_completed_review_persists_derived_review_score(self, tmp_path: Path):
        """Review completion path should persist derived deterministic review score."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        review_task = store.add(prompt="Review score persistence", task_type="review")
        review_task.slug = "20260213-review-score-persistence"
        store.update(review_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 50

        review_text = (
            "## Summary\n\n"
            "- Yes - Correctness preserved\n"
            "- No - Edge case is not covered\n\n"
            "## Blockers\n\n"
            "### B1 Add empty-input guard\n"
            "Required fix: return early when input is empty\n"
            "Required tests: add empty-input regression\n\n"
            "## Follow-Ups\n\n"
            "### F1 Improve docs\n"
            "Recommended follow-up: add quick-start example\n"
            "Recommended tests: docs example smoke-check\n\n"
            "## Questions / Assumptions\n\n"
            "None.\n\n"
            "## Verdict\n\n"
            "Verdict: CHANGES_REQUESTED\n"
        )

        def provider_run(_config, _prompt, log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            import json as _json

            with open(log_file, "a") as f:
                f.write(_json.dumps({"type": "result", "subtype": "success", "result": review_text}) + "\n")
            return RunResult(
                exit_code=0,
                duration_seconds=2.0,
                num_turns_reported=1,
                cost_usd=0.01,
                session_id="session-score",
                error_type=None,
            )

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.side_effect = provider_run

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        with patch("gza.runner.post_review_to_pr"), patch("gza.runner.console"), patch(
            "gza.runner.maybe_auto_regenerate_learnings", return_value=None
        ):
            exit_code = _run_non_code_task(
                review_task, config, store, mock_provider, mock_git, resume=False
            )

        assert exit_code == 0
        refreshed = store.get(review_task.id)
        assert refreshed is not None
        assert refreshed.status == "completed"
        assert refreshed.review_score == 67

    def test_completed_review_warns_when_blockers_missing_open_state_citations(self, tmp_path: Path):
        """Completed reviews warn about missing blocker citations without failing completion."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        review_task = store.add(prompt="Review citation warnings", task_type="review")
        review_task.slug = "20260213-review-citation-warnings"
        store.update(review_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 50

        review_text = (
            "## Summary\n\n"
            "- Yes - Correctness reviewed\n"
            "- No - blocker citation missing\n\n"
            "## Blockers\n\n"
            "### B1 Add empty-input guard\n"
            "Evidence: current code still falls through on empty input\n"
            "Required fix: return early when input is empty\n"
            "Required tests: add empty-input regression\n\n"
            "## Follow-Ups\n\n"
            "None.\n\n"
            "## Questions / Assumptions\n\n"
            "None.\n\n"
            "## Verdict\n\n"
            "Verdict: CHANGES_REQUESTED\n"
        )

        def provider_run(_config, _prompt, log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            import json as _json

            with open(log_file, "a") as f:
                f.write(_json.dumps({"type": "result", "subtype": "success", "result": review_text}) + "\n")
            return RunResult(
                exit_code=0,
                duration_seconds=2.0,
                num_turns_reported=1,
                cost_usd=0.01,
                session_id="session-citation-warning",
                error_type=None,
            )

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.side_effect = provider_run

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        printed_lines: list[str] = []

        def capture_print(*args, **kwargs):
            printed_lines.append(str(args[0]) if args else "")

        with patch("gza.runner.post_review_to_pr"), patch("gza.runner.console") as mock_runner_console, patch(
            "gza.console.console"
        ) as mock_console_console, patch(
            "gza.runner.maybe_auto_regenerate_learnings", return_value=None
        ):
            mock_runner_console.print.side_effect = capture_print
            mock_console_console.print.side_effect = capture_print
            exit_code = _run_non_code_task(
                review_task, config, store, mock_provider, mock_git, resume=False
            )

        assert exit_code == 0
        refreshed = store.get(review_task.id)
        assert refreshed is not None
        assert refreshed.status == "completed"
        assert "Review contract warning: blockers missing open-state citations: B1" in "\n".join(printed_lines)

    def test_missing_report_artifact_recovered_from_interactive_plaintext_log(self, tmp_path: Path):
        """Interactive plaintext output should be captured as result text for artifact recovery."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        review_task = store.add(prompt="Review feature interactive", task_type="review")
        review_task.slug = "20260213-review-feature-interactive"
        store.update(review_task)

        config = Config(
            project_dir=tmp_path,
            project_name="test-project",
            provider="claude",
            use_docker=False,
            timeout_minutes=10,
            max_steps=20,
        )
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path.mkdir(parents=True, exist_ok=True)

        provider = ClaudeProvider()

        mock_process = MagicMock()
        mock_process.wait.return_value = None
        mock_process.returncode = 0
        mock_process.poll.side_effect = [None, 0]

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)
        mock_git.worktree_remove = Mock()

        with (
            patch("gza.providers.claude.pty.openpty", return_value=(40, 41)),
            patch("gza.providers.claude.select.select", side_effect=[([40], [], []), ([40], [], [])]),
            patch(
                "gza.providers.claude.os.read",
                side_effect=[b"# Review\n\nVerdict: APPROVED\n", b""],
            ),
            patch("gza.providers.claude.os.close"),
            patch("gza.providers.claude.os.isatty", return_value=False),
            patch("gza.providers.claude.os.write"),
            patch("gza.providers.claude.subprocess.Popen", return_value=mock_process),
            patch("gza.runner.post_review_to_pr"),
            patch("gza.runner.console"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            exit_code = _run_non_code_task(
                review_task,
                config,
                store,
                provider,
                mock_git,
                resume=False,
                interaction_mode="interactive",
            )

        assert exit_code == 0
        refreshed = store.get(review_task.id)
        assert refreshed is not None
        assert refreshed.status == "completed"
        assert refreshed.output_content is not None
        assert "# Review" in refreshed.output_content
        assert "Verdict: APPROVED" in refreshed.output_content

    def test_missing_report_artifact_no_result_in_log_still_fails(self, tmp_path: Path):
        """If the expected report is missing and the log has no 'result' entry, the task still fails."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        review_task = store.add(prompt="Review feature W", task_type="review")
        review_task.slug = "20260213-review-feature-w"
        store.update(review_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 50

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.return_value = RunResult(
            exit_code=0,
            duration_seconds=1.0,
            num_turns_reported=1,
            cost_usd=0.01,
            session_id="session-w",
            error_type=None,
        )

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        with patch("gza.runner.console"):
            exit_code = _run_non_code_task(
                review_task, config, store, mock_provider, mock_git, resume=False
            )

        assert exit_code == 0
        refreshed = store.get(review_task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "MISSING_REPORT_ARTIFACT"
        assert refreshed.output_content is None

    def test_missing_report_artifact_without_any_md_candidates_still_fails(self, tmp_path: Path):
        """Missing expected report with an empty provider log and no md candidates should fail."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        review_task = store.add(prompt="Review feature empty-dir", task_type="review")
        review_task.slug = "20260213-review-feature-empty-dir"
        store.update(review_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 50

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.return_value = RunResult(
            exit_code=0,
            duration_seconds=1.0,
            num_turns_reported=1,
            cost_usd=0.01,
            session_id="session-empty-dir",
            error_type=None,
        )

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        with patch("gza.runner.console"):
            exit_code = _run_non_code_task(
                review_task, config, store, mock_provider, mock_git, resume=False
            )

        assert exit_code == 0
        refreshed = store.get(review_task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "MISSING_REPORT_ARTIFACT"
        assert refreshed.output_content is None

    def test_missing_report_artifact_with_multiple_md_candidates_still_fails(self, tmp_path: Path):
        """Missing expected report with multiple mismatched md candidates should still fail."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        review_task = store.add(prompt="Review feature ambiguous", task_type="review")
        review_task.slug = "20260213-review-feature-ambiguous"
        store.update(review_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 50

        def provider_run(_config, _prompt, _log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            review_dir = work_dir / ".gza" / "reviews"
            review_dir.mkdir(parents=True, exist_ok=True)
            (review_dir / "20260212-review-feature-ambiguous-old.md").write_text("# Review\n\nVerdict: APPROVED")
            (review_dir / "20260211-review-feature-ambiguous-older.md").write_text("# Review\n\nVerdict: APPROVED")
            return RunResult(
                exit_code=0,
                duration_seconds=1.0,
                num_turns_reported=1,
                cost_usd=0.01,
                session_id="session-ambiguous",
                error_type=None,
            )

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.side_effect = provider_run

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        mock_console = Mock()
        with patch("gza.runner.console", mock_console):
            exit_code = _run_non_code_task(
                review_task, config, store, mock_provider, mock_git, resume=False
            )

        assert exit_code == 0
        refreshed = store.get(review_task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "MISSING_REPORT_ARTIFACT"
        assert refreshed.output_content is None
        candidate_lines = [
            call.args[0]
            for call in mock_console.print.call_args_list
            if call.args and "Detected report files with other names in worktree" in str(call.args[0])
        ]
        assert candidate_lines
        assert "20260211-review-feature-ambiguous-older.md" in candidate_lines[0]
        assert "20260212-review-feature-ambiguous-old.md" in candidate_lines[0]

    def test_normal_run_does_not_include_verification_prompt(self, tmp_path: Path):
        """Test that normal (non-resume) runs use the standard prompt without verification."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a pending task
        task = store.add(
            prompt="Implement feature Y",
            task_type="implement",
        )

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        # Build normal prompt (not resume)
        prompt = build_prompt(task, config, store, summary_path=Path("/workspace/.gza/summaries/test.md"))

        # Verify it does NOT include verification instructions
        assert "verify your todo list" not in prompt.lower()
        assert "review your todo list from the previous session" not in prompt.lower()

        # Verify it includes the normal task prompt
        assert "Complete this task: Implement feature Y" in prompt


class TestPersistResolvedConfig:
    """Tests for persisting resolved model and provider to the task DB row."""

    def test_resolved_model_and_provider_persisted_before_provider_runs(self, tmp_path: Path):
        """Test that resolved model and provider are written to the DB before the provider runs."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a pending task with no model/provider set
        task = store.add(prompt="Implement feature Z", task_type="implement")

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
        config.max_turns = 50
        config.timeout_minutes = 60
        config.branch_mode = "multi"
        config.project_name = "test"
        config.branch_strategy = Mock()
        config.branch_strategy.pattern = "{project}/{task_id}"
        config.branch_strategy.default_type = "feature"
        config.get_provider_for_task.return_value = "claude"
        config.get_model_for_task.return_value = "claude-sonnet-4-6"
        config.get_max_steps_for_task.return_value = 50
        config.learnings_interval = 0
        config.learnings_window = 25

        # Track store.update calls and what task.model/provider looked like at call time
        persisted_states: list[dict] = []
        original_update = store.update

        def spy_update(t):
            persisted_states.append({"model": t.model, "provider": t.provider})
            return original_update(t)

        store.update = spy_update  # type: ignore[method-assign]

        provider_called_after_update = []

        def mock_provider_run(cfg, prompt, log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            # Record whether store.update was already called with persisted values
            provider_called_after_update.append(
                any(s["model"] == "claude-sonnet-4-6" and s["provider"] == "claude" for s in persisted_states)
            )
            return RunResult(
                exit_code=0,
                duration_seconds=5.0,
                num_turns_reported=3,
                cost_usd=0.01,
                session_id="session-xyz",
                error_type=None,
            )

        with patch("gza.runner.get_provider") as mock_get_provider, \
             patch("gza.runner.Git") as mock_git_class, \
             patch("gza.runner.load_dotenv"), \
             patch("gza.runner.SqliteTaskStore") as mock_store_cls:

            mock_store_cls.from_config.return_value = store

            mock_provider = Mock()
            mock_provider.name = "claude"
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_provider.run = mock_provider_run
            mock_get_provider.return_value = mock_provider

            mock_git = Mock()
            mock_git.default_branch.return_value = "main"
            mock_git._run.return_value = Mock(returncode=0)
            mock_git.branch_exists.return_value = False
            mock_git.worktree_add = Mock()
            mock_git.worktree_list.return_value = []
            mock_git.count_commits_ahead.return_value = 0

            mock_worktree_git = Mock()
            mock_worktree_git.has_changes.return_value = False
            mock_worktree_git.status_porcelain.return_value = set()
            mock_worktree_git.add = Mock()
            mock_worktree_git.commit = Mock()
            mock_worktree_git.get_diff_numstat.return_value = ""
            mock_log_result = Mock()
            mock_log_result.stdout = ""
            mock_worktree_git._run.return_value = mock_log_result

            mock_git_class.side_effect = [mock_git, mock_worktree_git]

            run(config, task_id=task.id)

        # store.update must have been called with the resolved model and provider
        assert any(
            s["model"] == "claude-sonnet-4-6" and s["provider"] == "claude"
            for s in persisted_states
        ), f"Expected store.update called with resolved model/provider, got: {persisted_states}"

        # The persist must have happened before the provider ran
        assert provider_called_after_update and provider_called_after_update[0], \
            "store.update with resolved values must occur before provider.run is called"

        # Verify the task in the DB has the resolved values
        updated_task = store.get(task.id)
        assert updated_task is not None
        assert updated_task.model == "claude-sonnet-4-6"
        assert updated_task.provider == "claude"
        assert updated_task.provider_is_explicit is False


class TestBackupDatabase:
    """Tests for backup_database function."""

    def test_creates_backup_when_none_exists(self, tmp_path: Path):
        """backup_database creates a backup file for the current hour."""
        import sqlite3

        # Create a source database
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
        conn.close()

        backup_database(db_path, tmp_path)

        backup_dir = tmp_path / BACKUP_DIR
        assert backup_dir.exists()

        from datetime import datetime
        hour_stamp = datetime.now().strftime("%Y%m%d%H")
        backup_file = backup_dir / f"gza-{hour_stamp}.db"
        assert backup_file.exists()
        assert backup_file.stat().st_size > 0

    def test_skips_backup_when_current_hour_exists(self, tmp_path: Path):
        """backup_database does not create a second backup in the same hour."""
        import sqlite3
        from datetime import datetime

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
        conn.close()

        backup_dir = tmp_path / BACKUP_DIR
        backup_dir.mkdir(parents=True, exist_ok=True)

        hour_stamp = datetime.now().strftime("%Y%m%d%H")
        existing_backup = backup_dir / f"gza-{hour_stamp}.db"
        existing_backup.write_bytes(b"placeholder")
        original_mtime = existing_backup.stat().st_mtime

        backup_database(db_path, tmp_path)

        # File should not have been replaced
        assert existing_backup.stat().st_mtime == original_mtime
        assert existing_backup.read_bytes() == b"placeholder"

    def test_skips_backup_when_db_does_not_exist(self, tmp_path: Path):
        """backup_database does nothing when the source database is missing."""
        db_path = tmp_path / ".gza" / "gza.db"

        # Should not raise and should not create any backup dir
        backup_database(db_path, tmp_path)

        backup_dir = tmp_path / BACKUP_DIR
        assert not backup_dir.exists()

    def test_backup_is_valid_sqlite_database(self, tmp_path: Path):
        """The created backup is a valid SQLite database with the same content."""
        import sqlite3
        from datetime import datetime

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO items VALUES (1, 'hello')")
        conn.commit()
        conn.close()

        backup_database(db_path, tmp_path)

        hour_stamp = datetime.now().strftime("%Y%m%d%H")
        backup_path = tmp_path / BACKUP_DIR / f"gza-{hour_stamp}.db"
        assert backup_path.exists()

        backup_conn = sqlite3.connect(str(backup_path))
        rows = backup_conn.execute("SELECT id, name FROM items").fetchall()
        backup_conn.close()

        assert rows == [(1, "hello")]

    def test_shared_db_writes_backups_next_to_shared_database(self, tmp_path: Path):
        """Shared DB mode stores backups under <shared-db-dir>/backups."""
        import sqlite3
        from datetime import datetime

        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared-home" / "gza.db"
        shared_db.parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(str(shared_db))
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
        conn.close()

        backup_database(shared_db, project_dir)

        hour_stamp = datetime.now().strftime("%Y%m%d%H")
        shared_backup = shared_db.parent / "backups" / f"gza-{hour_stamp}.db"
        project_backup_dir = project_dir / BACKUP_DIR

        assert shared_backup.exists()
        assert not project_backup_dir.exists()


class TestNoChangesWithExistingCommits:
    """Tests for the fix that prevents false 'No changes made' failure on resume."""

    def _make_config(self, tmp_path: Path, db_path: Path) -> Mock:
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
        config.max_turns = 50
        config.timeout_minutes = 60
        config.branch_mode = "multi"
        config.project_name = "test"
        config.branch_strategy = Mock()
        config.branch_strategy.pattern = "{project}/{task_id}"
        config.branch_strategy.default_type = "feature"
        config.get_provider_for_task.return_value = "claude"
        config.get_model_for_task.return_value = None
        config.get_max_steps_for_task.return_value = 50
        config.learnings_interval = 0
        config.learnings_window = 25
        return config

    def test_resume_with_existing_commits_and_no_new_changes_succeeds(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """When resuming, if there are no uncommitted changes but the branch already has
        commits from a previous run, the task should succeed (not fail with 'No changes made')."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="Implement feature X",
            task_type="implement",
        )
        task.slug = "20260212-implement-feature-x"
        task.branch = "test/20260212-implement-feature-x"
        task.session_id = "test-session-123"
        store.mark_failed(task, log_file="logs/test.log", stats=None)

        config = self._make_config(tmp_path, db_path)

        def mock_provider_run(cfg, prompt, log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            return RunResult(
                exit_code=0,
                duration_seconds=5.0,
                num_turns_reported=2,
                cost_usd=0.02,
                session_id="test-session-123",
                error_type=None,
            )

        with patch('gza.runner.get_provider') as mock_get_provider, \
             patch('gza.runner.Git') as mock_git_class, \
             patch('gza.runner.load_dotenv'):

            mock_provider = Mock()
            mock_provider.name = "TestProvider"
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_provider.run = mock_provider_run
            mock_get_provider.return_value = mock_provider

            mock_git = Mock()
            mock_git.default_branch.return_value = "main"
            mock_git._run.return_value = Mock(returncode=0)
            mock_git.branch_exists.return_value = True
            mock_git.count_commits_ahead.return_value = 0
            mock_git.worktree_add = Mock()
            mock_git.worktree_list.return_value = []

            mock_worktree_git = Mock()
            # No uncommitted changes (task already committed in previous run)
            mock_worktree_git.has_changes.return_value = False
            mock_worktree_git.status_porcelain.return_value = set()
            # Branch has 1 commit from the previous run
            mock_worktree_git.count_commits_ahead.return_value = 1
            mock_worktree_git.default_branch.return_value = "main"
            mock_worktree_git.get_diff_numstat.return_value = ""
            mock_log_result = Mock()
            mock_log_result.stdout = ""
            mock_worktree_git._run.return_value = mock_log_result

            mock_git_class.side_effect = [mock_git, mock_worktree_git]

            worktree_path = config.worktree_path / task.slug
            worktree_path.mkdir(parents=True, exist_ok=True)

            result = run(config, task_id=task.id, resume=True)

        assert result == 0
        refreshed = store.get(task.id)
        assert refreshed.status == "completed", f"Expected 'completed', got '{refreshed.status}'"
        output = capsys.readouterr().out
        assert "gza merge" in output
        assert "gza pr" in output
        assert "gza retry" not in output
        assert "gza resume" not in output

    def test_no_changes_and_no_prior_commits_still_fails(self, tmp_path: Path):
        """When there are no uncommitted changes AND no commits on the branch,
        the task should still fail with 'No changes made'."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="Implement feature Y",
            task_type="implement",
        )
        task.slug = "20260212-implement-feature-y"
        task.branch = "test/20260212-implement-feature-y"
        store.mark_in_progress(task)

        config = self._make_config(tmp_path, db_path)

        def mock_provider_run(cfg, prompt, log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            return RunResult(
                exit_code=0,
                duration_seconds=5.0,
                num_turns_reported=2,
                cost_usd=0.02,
                session_id=None,
                error_type=None,
            )

        with patch('gza.runner.get_provider') as mock_get_provider, \
             patch('gza.runner.Git') as mock_git_class, \
             patch('gza.runner.load_dotenv'):

            mock_provider = Mock()
            mock_provider.name = "TestProvider"
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_provider.run = mock_provider_run
            mock_get_provider.return_value = mock_provider

            mock_git = Mock()
            mock_git.default_branch.return_value = "main"
            mock_git._run.return_value = Mock(returncode=0)
            mock_git.branch_exists.return_value = False
            mock_git.count_commits_ahead.return_value = 0
            mock_git.worktree_add = Mock()
            mock_git.worktree_list.return_value = []

            mock_worktree_git = Mock()
            # No uncommitted changes
            mock_worktree_git.has_changes.return_value = False
            mock_worktree_git.status_porcelain.return_value = set()
            # No prior commits on the branch either
            mock_worktree_git.count_commits_ahead.return_value = 0
            mock_worktree_git.default_branch.return_value = "main"
            mock_log_result = Mock()
            mock_log_result.stdout = ""
            mock_worktree_git._run.return_value = mock_log_result

            mock_git_class.side_effect = [mock_git, mock_worktree_git]

            worktree_path = config.worktree_path / task.slug
            worktree_path.mkdir(parents=True, exist_ok=True)

            result = run(config, task_id=task.id, resume=False)

        assert result == 0
        refreshed = store.get(task.id)
        assert refreshed.status == "failed", f"Expected 'failed', got '{refreshed.status}'"


class TestTaskClaimSafety:
    """Regression tests for explicit status handling and CAS contention."""

    def _make_config(self, tmp_path: Path, db_path: Path) -> Mock:
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
        config.max_turns = 50
        config.timeout_minutes = 60
        config.branch_mode = "multi"
        config.project_name = "test"
        config.branch_strategy = Mock()
        config.branch_strategy.pattern = "{project}/{task_id}"
        config.branch_strategy.default_type = "feature"
        config.get_provider_for_task.return_value = "claude"
        config.get_model_for_task.return_value = None
        config.get_max_steps_for_task.return_value = 50
        return config

    def test_explicit_non_pending_task_returns_non_zero(self, tmp_path: Path):
        """Running an explicit completed task should fail with a non-zero exit code."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Done task", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        config = self._make_config(tmp_path, db_path)
        with patch("gza.runner.load_dotenv"), patch("gza.runner.backup_database"):
            result = run(config, task_id=task.id, resume=False)

        assert result == 1

    def test_next_pending_claim_retries_after_cas_loss(self, tmp_path: Path):
        """No-id run should keep scanning and claim another pending task after CAS loss."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        first = store.add(prompt="First pending", task_type="implement")
        second = store.add(prompt="Second pending", task_type="implement")

        config = self._make_config(tmp_path, db_path)

        original_try_mark = SqliteTaskStore.try_mark_in_progress
        lost_first_race = {"value": False}

        def _try_mark_with_one_forced_loss(self, task_id: int, pid: int):
            if task_id == first.id and not lost_first_race["value"]:
                lost_first_race["value"] = True
                stolen = self.get(first.id)
                assert stolen is not None
                stolen.status = "in_progress"
                stolen.running_pid = 424242
                self.update(stolen)
                return None
            return original_try_mark(self, task_id, pid)

        with (
            patch("gza.runner.load_dotenv"),
            patch("gza.runner.backup_database"),
            patch.object(SqliteTaskStore, "try_mark_in_progress", new=_try_mark_with_one_forced_loss),
            patch("gza.runner.get_provider") as mock_get_provider,
        ):
            mock_provider = Mock()
            mock_provider.name = "TestProvider"
            mock_provider.check_credentials.return_value = False
            mock_get_provider.return_value = mock_provider

            result = run(config, resume=False)

        assert result == 1
        assert lost_first_race["value"] is True
        first_refreshed = store.get(first.id)
        second_refreshed = store.get(second.id)
        assert first_refreshed is not None
        assert second_refreshed is not None
        assert first_refreshed.status == "in_progress"
        # Second task was claimed then failed at credential check with a
        # descriptive reason — not left dangling in_progress.
        assert second_refreshed.status == "failed"
        assert second_refreshed.failure_reason == "PROVIDER_UNAVAILABLE"

    def test_invocation_context_sets_foreground_inline_execution_mode(self, tmp_path: Path):
        """run() should persist foreground_inline mode when inline invocation is requested."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Inline run task", task_type="implement")

        config = self._make_config(tmp_path, db_path)
        with (
            patch("gza.runner.load_dotenv"),
            patch("gza.runner.backup_database"),
            patch("gza.runner.get_provider") as mock_get_provider,
        ):
            mock_provider = Mock()
            mock_provider.name = "TestProvider"
            mock_provider.supports_interactive_foreground = False
            mock_provider.check_credentials.return_value = False
            mock_get_provider.return_value = mock_provider

            result = run(
                config,
                task_id=task.id,
                invocation=RunInvocationContext(
                    command="run-inline",
                    execution_mode="foreground_inline",
                    interaction_mode="auto",
                ),
            )

        assert result == 1
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.execution_mode == "foreground_inline"

    @pytest.mark.parametrize(
        ("failure_stage", "invocation", "expected_reason", "expected_message"),
        [
            ("check", None, "PROVIDER_UNAVAILABLE", "Preflight failed: missing TestProvider credentials"),
            ("verify", None, "PROVIDER_UNAVAILABLE", "Preflight failed: TestProvider credential verification failed"),
            ("verify_infra", None, "INFRASTRUCTURE_ERROR", "Preflight failed: Docker daemon is not running"),
            (
                "check",
                RunInvocationContext(
                    command="run-inline",
                    execution_mode="foreground_inline",
                    interaction_mode="auto",
                ),
                "PROVIDER_UNAVAILABLE",
                "Preflight failed: missing TestProvider credentials",
            ),
            (
                "verify",
                RunInvocationContext(
                    command="run-inline",
                    execution_mode="foreground_inline",
                    interaction_mode="auto",
                ),
                "PROVIDER_UNAVAILABLE",
                "Preflight failed: TestProvider credential verification failed",
            ),
        ],
    )
    def test_preflight_failures_mark_task_failed_with_provider_unavailable(
        self,
        tmp_path: Path,
        failure_stage: str,
        invocation: RunInvocationContext | None,
        expected_reason: str,
        expected_message: str,
    ):
        """Credential preflight failures must persist failed state + provenance/outcome logs."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt=f"Task preflight {failure_stage}", task_type="implement")

        config = self._make_config(tmp_path, db_path)
        with (
            patch("gza.runner.load_dotenv"),
            patch("gza.runner.backup_database"),
            patch("gza.runner.get_provider") as mock_get_provider,
        ):
            mock_provider = Mock()
            mock_provider.name = "TestProvider"
            mock_provider.supports_interactive_foreground = True
            mock_provider.check_credentials.return_value = failure_stage != "check"
            if failure_stage == "verify":
                mock_provider.verify_credentials.return_value = PreflightCheckResult.failure(
                    failure_reason="PROVIDER_UNAVAILABLE",
                    message="Preflight failed: TestProvider credential verification failed",
                )
            elif failure_stage == "verify_infra":
                mock_provider.verify_credentials.return_value = PreflightCheckResult.failure(
                    failure_reason="INFRASTRUCTURE_ERROR",
                    message="Preflight failed: Docker daemon is not running",
                )
            else:
                mock_provider.verify_credentials.return_value = PreflightCheckResult.success()
            mock_provider.credential_setup_hint = "set creds"
            mock_get_provider.return_value = mock_provider

            result = run(config, task_id=task.id, invocation=invocation)

        assert result == 1
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == expected_reason
        assert refreshed.log_file is not None
        transcript_content = (tmp_path / refreshed.log_file).read_text()
        ops_log = (tmp_path / refreshed.log_file).with_name(
            f"{Path(refreshed.log_file).stem}.ops.jsonl"
        )
        ops_content = ops_log.read_text()
        assert transcript_content == ""
        assert expected_reason in ops_content
        assert '"subtype": "execution"' in ops_content
        assert expected_message in ops_content

        import json

        execution_events = [
            json.loads(line)
            for line in ops_content.splitlines()
            if line.strip() and '"subtype": "execution"' in line
        ]
        assert execution_events
        execution_event = execution_events[-1]
        expected_mode = refreshed.execution_mode
        assert expected_mode is not None
        assert execution_event["execution_mode"] == expected_mode
        if expected_mode in {"worker_background", "worker_foreground"}:
            assert execution_event["worker_mode"] is True
        else:
            assert execution_event["worker_mode"] is False

    @pytest.mark.parametrize(
        ("command", "resume"),
        [
            ("implement", False),
            ("resume", True),
        ],
    )
    def test_foreground_worker_commands_log_canonical_execution_mode(
        self,
        tmp_path: Path,
        command: str,
        resume: bool,
    ):
        """Foreground worker command provenance should match the persisted task execution mode."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt=f"{command} provenance parity", task_type="implement")
        if resume:
            task.status = "failed"
            task.session_id = "resume-session-123"
            store.update(task)

        config = self._make_config(tmp_path, db_path)
        with (
            patch("gza.runner.load_dotenv"),
            patch("gza.runner.backup_database"),
            patch("gza.runner.get_provider") as mock_get_provider,
        ):
            mock_provider = Mock()
            mock_provider.name = "TestProvider"
            mock_provider.supports_interactive_foreground = False
            mock_provider.check_credentials.return_value = False
            mock_provider.credential_setup_hint = "set creds"
            mock_get_provider.return_value = mock_provider

            result = run(
                config,
                task_id=task.id,
                resume=resume,
                invocation=RunInvocationContext(
                    command=command,
                    execution_mode="foreground_worker",
                    interaction_mode="observe_only",
                ),
            )

        assert result == 1
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.execution_mode == "worker_foreground"
        assert refreshed.log_file is not None

        import json

        log_content = ops_log_path_for(tmp_path / refreshed.log_file).read_text()
        execution_events = [
            json.loads(line)
            for line in log_content.splitlines()
            if line.strip() and '"subtype": "execution"' in line
        ]
        assert execution_events
        execution_event = execution_events[-1]
        assert execution_event["command"] == command
        assert execution_event["execution_mode"] == refreshed.execution_mode
        assert execution_event["worker_mode"] is True

    def test_run_inline_requests_interactive_for_capable_provider(
        self,
        tmp_path: Path,
    ):
        """Inline invocation should request provider interactive mode when supported."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Inline mode interactive", task_type="implement")
        task.status = "failed"
        task.session_id = "sess-123"
        store.update(task)

        config = self._make_config(tmp_path, db_path)

        with (
            patch("gza.runner.load_dotenv"),
            patch("gza.runner.backup_database"),
            patch("gza.runner._run_inner", return_value=0) as mock_run_inner,
            patch("gza.runner.get_provider") as mock_get_provider,
        ):
            mock_provider = Mock()
            mock_provider.name = "Claude"
            mock_provider.supports_interactive_foreground = True
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_get_provider.return_value = mock_provider

            result = run(
                config,
                task_id=task.id,
                resume=True,
                invocation=RunInvocationContext(
                    command="run-inline",
                    execution_mode="foreground_inline",
                    interaction_mode="interactive",
                ),
            )

        assert result == 0
        assert mock_run_inner.call_count == 1
        assert mock_run_inner.call_args.kwargs["interaction_mode"] == "interactive"

    def test_run_inline_prints_interactive_message_only_when_interactive_mode_used(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ):
        """run-inline should print interactive foreground messaging only for interactive-capable providers."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Inline message semantics", task_type="implement")
        config = self._make_config(tmp_path, db_path)

        with (
            patch("gza.runner.load_dotenv"),
            patch("gza.runner.backup_database"),
            patch("gza.runner._run_inner", return_value=0) as mock_run_inner,
            patch("gza.runner.get_provider") as mock_get_provider,
        ):
            mock_provider = Mock()
            mock_provider.name = "Codex"
            mock_provider.supports_interactive_foreground = False
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_get_provider.return_value = mock_provider

            result = run(
                config,
                task_id=task.id,
                invocation=RunInvocationContext(
                    command="run-inline",
                    execution_mode="foreground_inline",
                    interaction_mode="auto",
                ),
            )

        assert result == 0
        assert mock_run_inner.call_args.kwargs["interaction_mode"] == "observe_only"
        output = capsys.readouterr().out
        assert "Foreground inline execution: observe-only for provider 'codex'" in output
        assert "interactive mode" not in output

    def test_run_inline_prints_interactive_message_when_interactive_mode_is_used(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ):
        """run-inline should announce interactive mode when runner will launch provider interactively."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Inline message interactive", task_type="implement")
        config = self._make_config(tmp_path, db_path)

        with (
            patch("gza.runner.load_dotenv"),
            patch("gza.runner.backup_database"),
            patch("gza.runner._run_inner", return_value=0) as mock_run_inner,
            patch("gza.runner.get_provider") as mock_get_provider,
        ):
            mock_provider = Mock()
            mock_provider.name = "Claude"
            mock_provider.supports_interactive_foreground = True
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_get_provider.return_value = mock_provider

            result = run(
                config,
                task_id=task.id,
                invocation=RunInvocationContext(
                    command="run-inline",
                    execution_mode="foreground_inline",
                    interaction_mode="auto",
                ),
            )

        assert result == 0
        assert mock_run_inner.call_args.kwargs["interaction_mode"] == "interactive"
        output = capsys.readouterr().out
        assert "Foreground inline execution: interactive mode for provider 'claude'" in output

    def test_run_inline_interactive_interrupt_keeps_session_for_resume(self, tmp_path: Path):
        """Inline interactive runs should persist callback session_id on interrupt."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Interruptible inline task", task_type="plan")
        task.slug = "20260422-inline-interrupt-plan"
        store.update(task)

        config = self._make_config(tmp_path, db_path)

        def _interrupting_provider_run(
            _cfg,
            _prompt,
            _log_file,
            _work_dir,
            resume_session_id=None,
            on_session_id=None,
            on_step_count=None,
            interactive=False,
        ):
            del resume_session_id, on_step_count
            assert interactive is True
            assert on_session_id is not None
            on_session_id("sess-inline-1")
            raise KeyboardInterrupt

        with (
            patch("gza.runner.load_dotenv"),
            patch("gza.runner.backup_database"),
            patch("gza.runner.get_provider") as mock_get_provider,
            patch("gza.runner.Git") as mock_git_class,
        ):
            mock_provider = Mock()
            mock_provider.name = "Claude"
            mock_provider.supports_interactive_foreground = True
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_provider.run.side_effect = _interrupting_provider_run
            mock_get_provider.return_value = mock_provider

            mock_git = Mock()
            mock_git.default_branch.return_value = "main"
            mock_git._run.return_value = Mock(returncode=0)
            mock_git.worktree_remove = Mock()
            mock_git.branch_exists.return_value = False
            mock_git_class.return_value = mock_git

            result = run(
                config,
                task_id=task.id,
                invocation=RunInvocationContext(
                    command="run-inline",
                    execution_mode="foreground_inline",
                    interaction_mode="auto",
                ),
            )

        assert result == 130
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "INTERRUPTED"
        assert refreshed.session_id == "sess-inline-1"

    def test_run_inline_resume_interactive_interrupt_preserves_resume_session_id(self, tmp_path: Path):
        """Interrupted inline --resume runs should keep resumable session state for Claude interactive mode."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Resume inline task", task_type="plan")
        task.slug = "20260422-inline-resume-interrupt-plan"
        task.session_id = "sess-inline-resume-1"
        store.update(task)
        store.mark_failed(task, log_file="logs/failed.log", stats=None)

        config = self._make_config(tmp_path, db_path)

        def _interrupting_provider_run(
            _cfg,
            _prompt,
            _log_file,
            _work_dir,
            resume_session_id=None,
            on_session_id=None,
            on_step_count=None,
            interactive=False,
        ):
            del on_step_count
            assert interactive is True
            assert resume_session_id == "sess-inline-resume-1"
            assert on_session_id is not None
            # Foreground interactive resume path must persist the known session id
            # even if no stream-json events are emitted before interruption.
            on_session_id(resume_session_id)
            raise KeyboardInterrupt

        with (
            patch("gza.runner.load_dotenv"),
            patch("gza.runner.backup_database"),
            patch("gza.runner.get_provider") as mock_get_provider,
            patch("gza.runner.Git") as mock_git_class,
        ):
            mock_provider = Mock()
            mock_provider.name = "Claude"
            mock_provider.supports_interactive_foreground = True
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_provider.run.side_effect = _interrupting_provider_run
            mock_get_provider.return_value = mock_provider

            mock_git = Mock()
            mock_git.default_branch.return_value = "main"
            mock_git._run.return_value = Mock(returncode=0)
            mock_git.worktree_remove = Mock()
            mock_git.branch_exists.return_value = False
            mock_git_class.return_value = mock_git

            result = run(
                config,
                task_id=task.id,
                resume=True,
                invocation=RunInvocationContext(
                    command="run-inline",
                    execution_mode="foreground_inline",
                    interaction_mode="auto",
                ),
            )

        assert result == 130
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "INTERRUPTED"
        assert refreshed.session_id == "sess-inline-resume-1"

    def test_successful_run_splits_preflight_and_runner_entries_into_ops_log(self, tmp_path: Path):
        """Successful run should keep task.log_file as transcript and route gza entries to ops."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Single canonical log", task_type="implement")

        config = self._make_config(tmp_path, db_path)

        def _verify_credentials(_cfg, log_file: Path | None = None) -> bool:
            assert log_file is not None
            log_file.parent.mkdir(parents=True, exist_ok=True)
            write_log_entry(log_file, {"type": "gza", "subtype": "preflight", "message": "preflight-ok"})
            return True

        def _run_inner_success(task: Task, *_args, **kwargs) -> int:
            assert kwargs["interaction_mode"] == "observe_only"
            assert task.log_file is not None
            log_path = config.project_dir / task.log_file
            write_log_entry(log_path, {"type": "gza", "subtype": "provider", "message": "provider-run"})
            return 0

        with (
            patch("gza.runner.load_dotenv"),
            patch("gza.runner.backup_database"),
            patch("gza.runner._run_inner", side_effect=_run_inner_success),
            patch("gza.runner.get_provider") as mock_get_provider,
        ):
            mock_provider = Mock()
            mock_provider.name = "Codex"
            mock_provider.supports_interactive_foreground = False
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.side_effect = _verify_credentials
            mock_get_provider.return_value = mock_provider

            result = run(config, task_id=task.id)

        assert result == 0
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.log_file is not None
        assert refreshed.log_file.endswith(".log")
        assert not refreshed.log_file.endswith(".startup.log")
        transcript_log = config.project_dir / refreshed.log_file
        ops_log = transcript_log.with_name(f"{transcript_log.stem}.ops.jsonl")
        assert transcript_log.read_text() == ""
        ops_content = ops_log.read_text()
        assert "preflight-ok" in ops_content
        assert "provider-run" in ops_content


class TestSameBranchLineageWalk:
    """Tests for same_branch resolution walking the based_on lineage chain."""

    def _make_config(self, tmp_path: Path, db_path: Path) -> Mock:
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
        config.max_turns = 50
        config.timeout_minutes = 60
        config.branch_mode = "multi"
        config.project_name = "test"
        config.branch_strategy = Mock()
        config.branch_strategy.pattern = "{project}/{task_id}"
        config.branch_strategy.default_type = "feature"
        config.get_provider_for_task.return_value = "claude"
        config.get_model_for_task.return_value = None
        config.get_max_steps_for_task.return_value = 50
        config.learnings_interval = 0
        config.learnings_window = 25
        return config

    def test_same_branch_uses_immediate_source_branch(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """When the immediate source task has a valid branch, use it directly."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Task #1: implementation with a branch
        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.slug = "20260301-implement-feature"
        impl_task.branch = "test/20260301-implement-feature"
        store.mark_in_progress(impl_task)
        store.mark_completed(impl_task, log_file="logs/impl.log", stats=None)

        # Task #2: improve with same_branch, based_on impl_task
        improve_task = store.add(
            prompt="Improve feature",
            task_type="improve",
            based_on=impl_task.id,
            same_branch=True,
        )
        improve_task.slug = "20260301-improve-feature"
        store.mark_in_progress(improve_task)

        config = self._make_config(tmp_path, db_path)

        def mock_provider_run(cfg, prompt, log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            return RunResult(
                exit_code=0,
                duration_seconds=5.0,
                num_turns_reported=2,
                cost_usd=0.02,
                session_id="test-session",
                error_type=None,
            )

        with patch('gza.runner.get_provider') as mock_get_provider, \
             patch('gza.runner.Git') as mock_git_class, \
             patch('gza.runner.load_dotenv'):

            mock_provider = Mock()
            mock_provider.name = "TestProvider"
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_provider.run = mock_provider_run
            mock_get_provider.return_value = mock_provider

            mock_git = Mock()
            mock_git.default_branch.return_value = "main"
            mock_git._run.return_value = Mock(returncode=0)
            mock_git.branch_exists.return_value = True
            mock_git.worktree_list.return_value = []

            mock_worktree_git = Mock()
            mock_worktree_git.has_changes.return_value = True
            mock_worktree_git.status_porcelain.side_effect = [
                set(),  # pre-run snapshot
                {("M", "changed.py")},  # post-run snapshot
            ]
            mock_worktree_git.add = Mock()
            mock_worktree_git.commit = Mock()
            mock_worktree_git.get_diff_numstat.return_value = ""
            mock_log_result = Mock()
            mock_log_result.stdout = ""
            mock_worktree_git._run.return_value = mock_log_result

            mock_git_class.side_effect = [mock_git, mock_worktree_git]

            worktree_path = config.worktree_path / improve_task.slug
            worktree_path.mkdir(parents=True, exist_ok=True)

            result = run(config, task_id=improve_task.id)

        assert result == 0
        output = capsys.readouterr().out
        assert "test/20260301-implement-feature" in output
        # Direct source found — no "via" message expected
        assert "via" not in output

    def test_same_branch_walks_chain_when_immediate_source_has_no_branch(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """When the immediate source task has no branch, walk based_on chain to find ancestor."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Task #324: implementation with a branch
        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.slug = "20260301-implement-feature"
        impl_task.branch = "test/20260301-implement-feature"
        store.mark_in_progress(impl_task)
        store.mark_completed(impl_task, log_file="logs/impl.log", stats=None)

        # Task #335: killed before branch was persisted (no branch)
        killed_task = store.add(
            prompt="Improve feature (killed)",
            task_type="improve",
            based_on=impl_task.id,
            same_branch=True,
        )
        killed_task.slug = "20260301-improve-killed"
        # branch is NOT set (simulating killed before persistence)
        store.mark_in_progress(killed_task)
        store.mark_failed(killed_task, log_file="logs/killed.log", stats=None)

        # Task #352: retry of #335, based_on=#335 (which has no branch)
        retry_task = store.add(
            prompt="Improve feature (retry)",
            task_type="improve",
            based_on=killed_task.id,
            same_branch=True,
        )
        retry_task.slug = "20260301-improve-retry"
        store.mark_in_progress(retry_task)

        config = self._make_config(tmp_path, db_path)

        def mock_provider_run(cfg, prompt, log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            return RunResult(
                exit_code=0,
                duration_seconds=5.0,
                num_turns_reported=2,
                cost_usd=0.02,
                session_id="test-session",
                error_type=None,
            )

        with patch('gza.runner.get_provider') as mock_get_provider, \
             patch('gza.runner.Git') as mock_git_class, \
             patch('gza.runner.load_dotenv'):

            mock_provider = Mock()
            mock_provider.name = "TestProvider"
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_provider.run = mock_provider_run
            mock_get_provider.return_value = mock_provider

            mock_git = Mock()
            mock_git.default_branch.return_value = "main"
            mock_git._run.return_value = Mock(returncode=0)
            mock_git.branch_exists.return_value = True
            mock_git.worktree_list.return_value = []

            mock_worktree_git = Mock()
            mock_worktree_git.has_changes.return_value = True
            mock_worktree_git.status_porcelain.side_effect = [
                set(),  # pre-run snapshot
                {("M", "changed.py")},  # post-run snapshot
            ]
            mock_worktree_git.add = Mock()
            mock_worktree_git.commit = Mock()
            mock_worktree_git.get_diff_numstat.return_value = ""
            mock_log_result = Mock()
            mock_log_result.stdout = ""
            mock_worktree_git._run.return_value = mock_log_result

            mock_git_class.side_effect = [mock_git, mock_worktree_git]

            worktree_path = config.worktree_path / retry_task.slug
            worktree_path.mkdir(parents=True, exist_ok=True)

            result = run(config, task_id=retry_task.id)

        assert result == 0
        output = capsys.readouterr().out
        # Should use impl_task branch, logging the "via" chain
        assert "test/20260301-implement-feature" in output
        assert "via" in output
        assert f"{killed_task.id}" in output

    def test_same_branch_walks_chain_when_immediate_branch_does_not_exist(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """When the immediate source task has a branch field but that branch no longer exists,
        walk the based_on chain to find an ancestor with a valid branch."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Task #1: implementation with a valid branch
        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.slug = "20260301-implement-feature"
        impl_task.branch = "test/20260301-implement-feature"
        store.mark_in_progress(impl_task)
        store.mark_completed(impl_task, log_file="logs/impl.log", stats=None)

        # Task #2: has a branch set, but that branch has been deleted
        middle_task = store.add(
            prompt="Improve feature (deleted branch)",
            task_type="improve",
            based_on=impl_task.id,
            same_branch=True,
        )
        middle_task.slug = "20260301-improve-deleted-branch"
        middle_task.branch = "test/20260301-improve-deleted-branch"
        store.mark_in_progress(middle_task)
        store.mark_failed(middle_task, log_file="logs/middle.log", stats=None)

        # Task #3: retry, based on middle_task
        retry_task = store.add(
            prompt="Improve feature (retry)",
            task_type="improve",
            based_on=middle_task.id,
            same_branch=True,
        )
        retry_task.slug = "20260301-improve-retry"
        store.mark_in_progress(retry_task)

        config = self._make_config(tmp_path, db_path)

        def mock_provider_run(cfg, prompt, log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            return RunResult(
                exit_code=0,
                duration_seconds=5.0,
                num_turns_reported=2,
                cost_usd=0.02,
                session_id="test-session",
                error_type=None,
            )

        with patch('gza.runner.get_provider') as mock_get_provider, \
             patch('gza.runner.Git') as mock_git_class, \
             patch('gza.runner.load_dotenv'):

            mock_provider = Mock()
            mock_provider.name = "TestProvider"
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_provider.run = mock_provider_run
            mock_get_provider.return_value = mock_provider

            mock_git = Mock()
            mock_git.default_branch.return_value = "main"
            mock_git._run.return_value = Mock(returncode=0)
            mock_git.worktree_list.return_value = []
            # The middle task's branch doesn't exist; the impl_task's branch does
            def branch_exists(branch: str) -> bool:
                return branch == "test/20260301-implement-feature"
            mock_git.branch_exists.side_effect = branch_exists

            mock_worktree_git = Mock()
            mock_worktree_git.has_changes.return_value = True
            mock_worktree_git.status_porcelain.side_effect = [
                set(),  # pre-run snapshot
                {("M", "changed.py")},  # post-run snapshot
            ]
            mock_worktree_git.add = Mock()
            mock_worktree_git.commit = Mock()
            mock_worktree_git.get_diff_numstat.return_value = ""
            mock_log_result = Mock()
            mock_log_result.stdout = ""
            mock_worktree_git._run.return_value = mock_log_result

            mock_git_class.side_effect = [mock_git, mock_worktree_git]

            worktree_path = config.worktree_path / retry_task.slug
            worktree_path.mkdir(parents=True, exist_ok=True)

            result = run(config, task_id=retry_task.id)

        assert result == 0
        output = capsys.readouterr().out
        assert "test/20260301-implement-feature" in output
        assert "via" in output

    def test_same_branch_fails_when_no_ancestor_has_valid_branch(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """When no ancestor in the chain has a valid branch, fail with a clear error."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Task with no branch
        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.slug = "20260301-implement-feature"
        # branch NOT set
        store.mark_in_progress(impl_task)
        store.mark_failed(impl_task, log_file="logs/impl.log", stats=None)

        # Improve task based on the branchless impl task
        improve_task = store.add(
            prompt="Improve feature",
            task_type="improve",
            based_on=impl_task.id,
            same_branch=True,
        )
        improve_task.slug = "20260301-improve-feature"
        store.mark_in_progress(improve_task)

        config = self._make_config(tmp_path, db_path)

        with patch('gza.runner.get_provider') as mock_get_provider, \
             patch('gza.runner.Git') as mock_git_class, \
             patch('gza.runner.load_dotenv'):

            mock_provider = Mock()
            mock_provider.name = "TestProvider"
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_get_provider.return_value = mock_provider

            mock_git = Mock()
            mock_git.default_branch.return_value = "main"
            mock_git._run.return_value = Mock(returncode=0)
            mock_git.branch_exists.return_value = False
            mock_git.worktree_list.return_value = []

            mock_git_class.return_value = mock_git

            result = run(config, task_id=improve_task.id)

        assert result == 1
        output = capsys.readouterr().out
        assert "no ancestor has a valid branch" in output

    def test_same_branch_fails_on_cycle_in_based_on_chain(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """When the based_on chain contains a cycle, fail with a clear error instead of looping forever."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Task A: no branch yet
        task_a = store.add(prompt="Task A", task_type="implement")
        task_a.slug = "20260301-task-a"
        store.mark_in_progress(task_a)
        store.mark_failed(task_a, log_file="logs/a.log", stats=None)

        # Task B: based_on A, also no branch
        task_b = store.add(prompt="Task B", task_type="improve", based_on=task_a.id, same_branch=True)
        task_b.slug = "20260301-task-b"
        store.mark_in_progress(task_b)
        store.mark_failed(task_b, log_file="logs/b.log", stats=None)

        # Introduce cycle: A.based_on = B (A -> B -> A)
        task_a_fresh = store.get(task_a.id)
        assert task_a_fresh is not None
        task_a_fresh.based_on = task_b.id
        store.update(task_a_fresh)

        # Task C: based_on B, same_branch=True — will walk B -> A -> B (cycle)
        task_c = store.add(prompt="Task C", task_type="improve", based_on=task_b.id, same_branch=True)
        task_c.slug = "20260301-task-c"
        store.mark_in_progress(task_c)

        config = self._make_config(tmp_path, db_path)

        with patch('gza.runner.get_provider') as mock_get_provider, \
             patch('gza.runner.Git') as mock_git_class, \
             patch('gza.runner.load_dotenv'):

            mock_provider = Mock()
            mock_provider.name = "TestProvider"
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_get_provider.return_value = mock_provider

            mock_git = Mock()
            mock_git.default_branch.return_value = "main"
            mock_git._run.return_value = Mock(returncode=0)
            mock_git.branch_exists.return_value = False
            mock_git.worktree_list.return_value = []

            mock_git_class.return_value = mock_git

            result = run(config, task_id=task_c.id)

        assert result == 1
        output = capsys.readouterr().out
        assert "Cycle detected" in output


class TestExtractedRunInnerHelpers:
    """Unit tests for helpers extracted from _run_inner orchestration."""

    def _make_config(self, tmp_path: Path) -> Mock:
        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.branch_mode = "multi"
        config.project_name = "testproj"
        config.branch_strategy = Mock()
        config.branch_strategy.pattern = "{project}/{task_id}"
        config.branch_strategy.default_type = "feature"
        return config

    def test_resolve_code_task_branch_name_walks_lineage(self, tmp_path: Path):
        """same_branch lineage resolution should return an ancestor branch that exists."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        config = self._make_config(tmp_path)

        impl = store.add(prompt="impl", task_type="implement")
        impl.slug = "20260317-impl"
        impl.branch = "test/impl"
        store.mark_in_progress(impl)
        store.mark_completed(impl, log_file="logs/impl.log", stats=None)

        failed_improve = store.add(prompt="improve1", task_type="improve", based_on=impl.id, same_branch=True)
        failed_improve.slug = "20260317-improve1"
        store.mark_in_progress(failed_improve)
        store.mark_failed(failed_improve, log_file="logs/improve1.log", stats=None)

        retry = store.add(prompt="improve2", task_type="improve", based_on=failed_improve.id, same_branch=True)
        retry.slug = "20260317-improve2"

        git = Mock(spec=Git)
        git.branch_exists.side_effect = lambda branch: branch == "test/impl"

        branch_name = _resolve_code_task_branch_name(retry, config, store, git, resume=False)
        assert branch_name == "test/impl"

    def test_rebase_recovery_chain_reuses_impl_branch_and_pushes_it(self, tmp_path: Path):
        """Chained rebase recoveries should keep resolving and pushing the impl branch."""
        from gza.cli._common import _create_retry_task

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        config = self._make_config(tmp_path)

        impl = store.add(prompt="impl", task_type="implement")
        assert impl.id is not None
        impl.slug = "20260512-impl"
        impl.branch = "feature/impl"
        store.update(impl)

        failed_rebase = store.add(
            prompt="rebase1",
            task_type="rebase",
            based_on=impl.id,
            same_branch=True,
        )
        assert failed_rebase.id is not None
        failed_rebase.slug = "20260512-rebase1"
        failed_rebase.status = "failed"
        failed_rebase.failure_reason = "WORKER_DIED"
        failed_rebase.branch = impl.branch
        failed_rebase.completed_at = datetime.now(UTC)
        store.update(failed_rebase)

        first_recovery = _create_retry_task(
            store,
            failed_rebase,
            trigger_source="auto-recovery",
            automatic_recovery=True,
        )
        assert first_recovery.id is not None
        first_recovery.slug = "20260512-rebase-branch-orphan"
        first_recovery.status = "failed"
        first_recovery.failure_reason = "TIMEOUT"
        first_recovery.branch = "20260512-rebase-branch-orphan"
        first_recovery.completed_at = datetime.now(UTC)
        store.update(first_recovery)

        second_recovery = _create_retry_task(
            store,
            first_recovery,
            trigger_source="auto-recovery",
            automatic_recovery=True,
        )
        assert second_recovery.id is not None
        second_recovery.slug = "20260512-rebase-branch-orphan-2"
        assert second_recovery.same_branch is True
        assert second_recovery.base_branch is None
        assert second_recovery.branch == impl.branch

        git = Mock(spec=Git)
        git.branch_exists.side_effect = lambda branch: branch == impl.branch

        branch_name = _resolve_code_task_branch_name(second_recovery, config, store, git, resume=False)
        assert branch_name == impl.branch
        second_recovery.branch = branch_name
        store.update(second_recovery)
        persisted_recovery = store.get(second_recovery.id)
        assert persisted_recovery is not None
        assert persisted_recovery.branch == impl.branch

        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{second_recovery.slug}.log"
        log_file.write_text("")

        worktree_git = Mock(spec=Git)
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_numstat.return_value = ""

        with patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None):
            rc = _complete_code_task(
                second_recovery,
                config,
                store,
                worktree_git,
                log_file,
                branch_name,
                TaskStats(duration_seconds=1.0, num_steps_reported=1, cost_usd=0.01),
                0,
                pre_run_status=set(),
                worktree_summary_path=tmp_path / "missing-summary.md",
                summary_path=tmp_path / ".gza" / "summaries" / f"{second_recovery.slug}.md",
                summary_dir=tmp_path / ".gza" / "summaries",
                skip_commit=True,
            )

        assert rc == 0
        worktree_git.push_force_with_lease.assert_called_once_with(impl.branch, remote="origin")

    def test_select_worktree_base_ref_prefers_origin_when_origin_ahead(self):
        """Base ref selection should choose origin/main when origin is strictly ahead."""
        git = Mock(spec=Git)
        git._run.return_value = Mock(returncode=0)  # origin ref exists

        def count_ahead(lhs: str, rhs: str) -> int:
            if lhs == "main" and rhs == "origin/main":
                return 0
            if lhs == "origin/main" and rhs == "main":
                return 3
            return 0

        git.count_commits_ahead.side_effect = count_ahead

        base_ref = _select_worktree_base_ref(git, "main")
        assert base_ref == "origin/main"

    def test_setup_code_task_worktree_resume_missing_branch_fails(self, tmp_path: Path):
        """Resume/same_branch setup should fail early if branch no longer exists."""
        config = self._make_config(tmp_path)
        task = Task(id=1, prompt="resume task", task_type="implement", slug="20260317-task")
        git = Mock(spec=Git)
        git.branch_exists.return_value = False

        ok = _setup_code_task_worktree(
            task,
            config,
            git,
            branch_name="missing/branch",
            worktree_path=tmp_path / "worktrees" / "20260317-task",
            default_branch="main",
            resume=True,
        )

        assert ok is False

    def test_setup_code_task_worktree_resume_passes_managed_roots_to_cleanup(self, tmp_path: Path):
        """Resume/same-branch setup should guard cleanup with configured roots."""
        config = self._make_config(tmp_path)
        config.interactive_worktree_dir = "interactive-worktrees"
        task = Task(id=1, prompt="resume task", task_type="implement", slug="20260317-task")
        git = Mock(spec=Git)
        git.branch_exists.return_value = True
        git._run.return_value = None
        git.worktree_remove.return_value = None

        worktree_path = tmp_path / "worktrees" / "20260317-task"

        with patch("gza.runner.cleanup_worktree_for_branch", return_value=None) as mock_cleanup:
            ok = _setup_code_task_worktree(
                task,
                config,
                git,
                branch_name="feature/existing",
                worktree_path=worktree_path,
                default_branch="main",
                resume=True,
            )

        assert ok is True
        mock_cleanup.assert_called_once_with(
            git,
            "feature/existing",
            force=True,
            permitted_root_paths=managed_worktree_root_paths(config),
        )

    def test_complete_code_task_marks_failed_when_no_changes_and_no_commits(self, tmp_path: Path):
        """Completion helper should fail task when provider produced neither changes nor commits."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement X", task_type="implement")
        task.slug = "20260317-impl-x"
        store.mark_in_progress(task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{task.slug}.log"
        log_file.write_text("")

        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = set()
        worktree_git.default_branch.return_value = "main"
        worktree_git.count_commits_ahead.return_value = 0

        rc = _complete_code_task(
            task,
            config,
            store,
            worktree_git,
            log_file,
            "test/branch",
            TaskStats(duration_seconds=1.0, num_steps_reported=1, cost_usd=0.01),
            0,
            pre_run_status=set(),
            worktree_summary_path=tmp_path / "worktree-summary.md",
            summary_path=tmp_path / ".gza" / "summaries" / f"{task.slug}.md",
            summary_dir=tmp_path / ".gza" / "summaries",
        )

        assert rc == 0
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"

    def test_complete_code_task_classifies_empty_turn_as_provider_empty_turn(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement empty turn", task_type="implement")
        task.slug = "20260531-empty-turn"
        store.mark_in_progress(task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{task.slug}.log"
        log_file.write_text('{"type":"thread.started"}\n{"type":"turn.started"}\n')
        ops_log_path_for(log_file).write_text(
            '{"type":"gza","stream":"ops","source":"provider","subtype":"process_output","message":"provider stderr line"}\n'
        )

        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = set()
        worktree_git.default_branch.return_value = "main"
        worktree_git.count_commits_ahead.return_value = 0

        rc = _complete_code_task(
            task,
            config,
            store,
            worktree_git,
            log_file,
            "test/branch",
            TaskStats(duration_seconds=1.0, num_steps_reported=0, cost_usd=0.01),
            0,
            pre_run_status=set(),
            worktree_summary_path=tmp_path / "worktree-summary.md",
            summary_path=tmp_path / ".gza" / "summaries" / f"{task.slug}.md",
            summary_dir=tmp_path / ".gza" / "summaries",
        )

        assert rc == 0
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "PROVIDER_EMPTY_TURN"
        ops_entries = [
            json.loads(line)
            for line in ops_log_path_for(log_file).read_text().splitlines()
            if line.strip()
        ]
        outcome_entry = next(entry for entry in ops_entries if entry.get("subtype") == "outcome")
        assert outcome_entry["failure_reason"] == "PROVIDER_EMPTY_TURN"
        assert outcome_entry["stderr_tail"] == "provider stderr line"

    def test_complete_code_task_classifies_capacity_no_change_run_as_provider_unavailable(
        self,
        tmp_path: Path,
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement capacity failure", task_type="implement")
        task.slug = "20260623-capacity-failure"
        store.mark_in_progress(task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{task.slug}.log"
        log_file.write_text(
            '{"type":"thread.started"}\n'
            '{"type":"turn.started"}\n'
            '{"type":"turn.failed","error":{"message":"Selected model is at capacity. Try again shortly."}}\n'
        )

        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = set()
        worktree_git.default_branch.return_value = "main"
        worktree_git.count_commits_ahead.return_value = 0

        rc = _complete_code_task(
            task,
            config,
            store,
            worktree_git,
            log_file,
            "test/branch",
            TaskStats(duration_seconds=1.0, num_steps_reported=1, cost_usd=0.01),
            1,
            pre_run_status=set(),
            worktree_summary_path=tmp_path / "worktree-summary.md",
            summary_path=tmp_path / ".gza" / "summaries" / f"{task.slug}.md",
            summary_dir=tmp_path / ".gza" / "summaries",
            error_type="provider_unavailable",
        )

        assert rc == 0
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "PROVIDER_UNAVAILABLE"
        decision = decide_failed_task_recovery(store, refreshed, max_recovery_attempts=1)
        assert decision.action == "retry"
        assert decision.reason_code == "PROVIDER_UNAVAILABLE"

    @pytest.mark.parametrize(
        ("error_type", "stats", "exit_code", "expected_reason", "expected_action"),
        [
            (
                "provider_unavailable",
                TaskStats(duration_seconds=1.0, num_steps_reported=1, cost_usd=0.01),
                1,
                "PROVIDER_UNAVAILABLE",
                "retry",
            ),
            (
                None,
                TaskStats(duration_seconds=1.0, cost_usd=0.01),
                124,
                "TIMEOUT",
                "resume",
            ),
        ],
    )
    @pytest.mark.parametrize("merge_state", ["empty", "redundant"])
    def test_complete_code_task_preserves_recoverable_failure_reason_over_terminal_no_work_merge_states(
        self,
        tmp_path: Path,
        error_type: str | None,
        stats: TaskStats,
        exit_code: int,
        expected_reason: str,
        expected_action: str,
        merge_state: str,
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement capacity failure", task_type="implement")
        task.slug = f"20260623-capacity-failure-{merge_state}"
        task.session_id = f"sess-capacity-failure-{merge_state}"
        store.mark_in_progress(task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{task.slug}.log"
        log_file.write_text(
            '{"type":"thread.started"}\n'
            '{"type":"turn.started"}\n'
            '{"type":"turn.failed","error":{"message":"Selected model is at capacity. Try again shortly."}}\n'
        )

        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = set()
        worktree_git.default_branch.return_value = "main"
        worktree_git.count_commits_ahead.return_value = 0

        with patch("gza.runner.resolve_task_merge_state_for_target", return_value=merge_state):
            rc = _complete_code_task(
                task,
                config,
                store,
                worktree_git,
                log_file,
                "test/branch",
                stats,
                exit_code,
                pre_run_status=set(),
                worktree_summary_path=tmp_path / "worktree-summary.md",
                summary_path=tmp_path / ".gza" / "summaries" / f"{task.slug}.md",
                summary_dir=tmp_path / ".gza" / "summaries",
                error_type=error_type,
            )

        assert rc == 0
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == expected_reason
        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None
        assert unit.state == merge_state
        decision = decide_failed_task_recovery(store, refreshed, max_recovery_attempts=1)
        assert decision.action == expected_action
        assert decision.reason_code == expected_reason

    def test_complete_code_task_classifies_no_change_run_as_moot_empty_when_branch_has_no_work(
        self,
        tmp_path: Path,
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement no-op", task_type="implement")
        task.slug = "20260531-genuine-no-op"
        store.mark_in_progress(task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{task.slug}.log"
        log_file.write_text('{"type":"thread.started"}\n{"type":"turn.started"}\n{"type":"turn.completed"}\n')

        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = set()
        worktree_git.default_branch.return_value = "main"
        worktree_git.count_commits_ahead.return_value = 0

        with patch("gza.runner.resolve_task_merge_state_for_target", return_value="empty"):
            rc = _complete_code_task(
                task,
                config,
                store,
                worktree_git,
                log_file,
                "test/branch",
                TaskStats(duration_seconds=1.0, num_steps_reported=1, cost_usd=0.01),
                0,
                pre_run_status=set(),
                worktree_summary_path=tmp_path / "worktree-summary.md",
                summary_path=tmp_path / ".gza" / "summaries" / f"{task.slug}.md",
                summary_dir=tmp_path / ".gza" / "summaries",
            )

        assert rc == 0
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "TERMINAL_NO_WORK"
        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None
        assert unit.state == "empty"
        decision = decide_failed_task_recovery(store, refreshed, max_recovery_attempts=1)
        assert decision.action == "skip"
        assert decision.reason_code == "merge_unit_empty"

    def test_complete_code_task_classifies_no_change_run_as_moot_redundant_when_branch_is_already_landed(
        self,
        tmp_path: Path,
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement already-landed no-op", task_type="implement")
        task.slug = "20260623-already-landed-no-op"
        store.mark_in_progress(task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{task.slug}.log"
        log_file.write_text('{"type":"thread.started"}\n{"type":"turn.started"}\n{"type":"turn.completed"}\n')

        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = set()
        worktree_git.default_branch.return_value = "main"
        worktree_git.count_commits_ahead.return_value = 0

        with patch("gza.runner.resolve_task_merge_state_for_target", return_value="redundant"):
            rc = _complete_code_task(
                task,
                config,
                store,
                worktree_git,
                log_file,
                "test/branch",
                TaskStats(duration_seconds=1.0, num_steps_reported=1, cost_usd=0.01),
                0,
                pre_run_status=set(),
                worktree_summary_path=tmp_path / "worktree-summary.md",
                summary_path=tmp_path / ".gza" / "summaries" / f"{task.slug}.md",
                summary_dir=tmp_path / ".gza" / "summaries",
            )

        assert rc == 0
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "TERMINAL_NO_WORK"
        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None
        assert unit.state == "redundant"
        decision = decide_failed_task_recovery(store, refreshed, max_recovery_attempts=1)
        assert decision.action == "skip"
        assert decision.reason_code == "merge_unit_redundant"

    @pytest.mark.parametrize("merge_state", ["empty", "redundant"])
    def test_complete_code_task_keeps_session_backed_terminal_no_work_in_recovery_lane(
        self,
        tmp_path: Path,
        merge_state: str,
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement recoverable no-op", task_type="implement")
        task.slug = f"20260623-recoverable-no-op-{merge_state}"
        task.session_id = f"sess-recoverable-no-op-{merge_state}"
        store.mark_in_progress(task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{task.slug}.log"
        log_file.write_text('{"type":"thread.started"}\n{"type":"turn.started"}\n{"type":"turn.completed"}\n')

        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = set()
        worktree_git.default_branch.return_value = "main"
        worktree_git.count_commits_ahead.return_value = 0

        with patch("gza.runner.resolve_task_merge_state_for_target", return_value=merge_state):
            rc = _complete_code_task(
                task,
                config,
                store,
                worktree_git,
                log_file,
                "test/branch",
                TaskStats(duration_seconds=1.0, num_steps_reported=1, cost_usd=0.01),
                0,
                pre_run_status=set(),
                worktree_summary_path=tmp_path / "worktree-summary.md",
                summary_path=tmp_path / ".gza" / "summaries" / f"{task.slug}.md",
                summary_dir=tmp_path / ".gza" / "summaries",
            )

        assert rc == 0
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "TERMINAL_NO_WORK"
        decision = decide_failed_task_recovery(store, refreshed, max_recovery_attempts=1)
        assert decision.action == "resume"
        assert decision.reason_code == "TERMINAL_NO_WORK"

    def test_complete_code_task_keeps_genuinely_unclassifiable_no_change_run_unknown(
        self,
        tmp_path: Path,
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement ambiguous no-op", task_type="implement")
        task.slug = "20260623-ambiguous-no-op"
        store.mark_in_progress(task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{task.slug}.log"
        log_file.write_text('{"type":"thread.started"}\n{"type":"turn.started"}\n{"type":"turn.completed"}\n')

        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = set()
        worktree_git.default_branch.return_value = "main"
        worktree_git.count_commits_ahead.return_value = 0

        with patch("gza.runner.resolve_task_merge_state_for_target", return_value=None):
            rc = _complete_code_task(
                task,
                config,
                store,
                worktree_git,
                log_file,
                "test/branch",
                TaskStats(duration_seconds=1.0, num_steps_reported=1, cost_usd=0.01),
                0,
                pre_run_status=set(),
                worktree_summary_path=tmp_path / "worktree-summary.md",
                summary_path=tmp_path / ".gza" / "summaries" / f"{task.slug}.md",
                summary_dir=tmp_path / ".gza" / "summaries",
            )

        assert rc == 0
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "UNKNOWN"
        decision = decide_failed_task_recovery(store, refreshed, max_recovery_attempts=1)
        assert decision.action == "skip"
        assert decision.reason_code == "manual_failure_reason"

    def test_complete_code_task_logs_warning_when_no_work_merge_probe_fails(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement ambiguous no-op", task_type="implement")
        task.slug = "20260623-probe-failure-no-op"
        task.session_id = "sess-probe-failure-no-op"
        store.mark_in_progress(task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{task.slug}.log"
        log_file.write_text('{"type":"thread.started"}\n{"type":"turn.started"}\n{"type":"turn.completed"}\n')

        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = set()
        worktree_git.default_branch.return_value = "main"
        worktree_git.count_commits_ahead.return_value = 0

        with (
            patch("gza.runner.resolve_task_merge_state_for_target", side_effect=GitError("simulated probe failure")),
            caplog.at_level(logging.WARNING, logger="gza.runner"),
        ):
            rc = _complete_code_task(
                task,
                config,
                store,
                worktree_git,
                log_file,
                "test/branch",
                TaskStats(duration_seconds=1.0, num_steps_reported=1, cost_usd=0.01),
                0,
                pre_run_status=set(),
                worktree_summary_path=tmp_path / "worktree-summary.md",
                summary_path=tmp_path / ".gza" / "summaries" / f"{task.slug}.md",
                summary_dir=tmp_path / ".gza" / "summaries",
            )

        assert rc == 0
        warning_messages = [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING]
        assert any("no-work merge-state probe failed" in message for message in warning_messages)

        ops_log_file = ops_log_path_for(log_file)
        log_entries = [json.loads(line) for line in ops_log_file.read_text().splitlines()]
        warning_entries = [entry for entry in log_entries if entry.get("subtype") == "warning"]
        assert warning_entries
        assert warning_entries[-1]["branch"] == "test/branch"
        assert warning_entries[-1]["target_branch"] == "main"
        assert "Leaving terminal no-work unclassified." in warning_entries[-1]["message"]

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.failure_reason == "UNKNOWN"

    def test_complete_code_task_selectively_stages_new_files(self, tmp_path: Path):
        """Completion helper should stage only provider-introduced changes."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement selective staging", task_type="implement")
        task.slug = "20260317-selective"
        store.mark_in_progress(task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{task.slug}.log"
        log_file.write_text("")

        pre_status = {("M", "pre_existing.txt")}
        post_status = {("M", "pre_existing.txt"), ("M", "src/foo.py"), ("??", "new_file.txt")}

        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = post_status
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_numstat.return_value = "1\t0\tsrc/foo.py\n1\t0\tnew_file.txt\n"

        summary_dir = tmp_path / ".gza" / "summaries"
        summary_path = summary_dir / f"{task.slug}.md"
        worktree_summary_path = tmp_path / "worktree" / ".gza" / "summaries" / f"{task.slug}.md"
        worktree_summary_path.parent.mkdir(parents=True, exist_ok=True)
        worktree_summary_path.write_text("## Summary\n\n- done\n")

        with patch("gza.runner._squash_wip_commits"), patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None):
            rc = _complete_code_task(
                task,
                config,
                store,
                worktree_git,
                log_file,
                "test/branch",
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
                0,
                pre_run_status=pre_status,
                worktree_summary_path=worktree_summary_path,
                summary_path=summary_path,
                summary_dir=summary_dir,
            )

        assert rc == 0
        staged_files = [call.args[0] for call in worktree_git.add.call_args_list]
        assert set(staged_files) == {"src/foo.py", "new_file.txt"}
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "completed"
        assert summary_path.exists()

    def test_complete_code_task_records_merge_unit_head_and_base(self, tmp_path: Path):
        """Code completion should persist branch-head provenance on the merge unit."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement provenance capture", task_type="implement")
        task.slug = "20260317-provenance"
        store.mark_in_progress(task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{task.slug}.log"
        log_file.write_text("")

        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = {("M", "src/foo.py")}
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_numstat.return_value = "1\t0\tsrc/foo.py\n"
        worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
            "feature/provenance": "head-complete-123",
            "main": "base-complete-456",
        }.get(ref)

        summary_dir = tmp_path / ".gza" / "summaries"
        summary_path = summary_dir / f"{task.slug}.md"
        worktree_summary_path = tmp_path / "worktree" / ".gza" / "summaries" / f"{task.slug}.md"
        worktree_summary_path.parent.mkdir(parents=True, exist_ok=True)
        worktree_summary_path.write_text("## Summary\n\n- done\n")

        with patch("gza.runner._squash_wip_commits"), patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None):
            rc = _complete_code_task(
                task,
                config,
                store,
                worktree_git,
                log_file,
                "feature/provenance",
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
                0,
                pre_run_status=set(),
                worktree_summary_path=worktree_summary_path,
                summary_path=summary_path,
                summary_dir=summary_dir,
            )

        assert rc == 0
        assert task.id is not None
        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None
        assert unit.head_sha == "head-complete-123"
        assert unit.base_sha == "base-complete-456"

    def test_ensure_work_pr_creates_pr_for_committed_branch(self, tmp_path: Path):
        """`work --pr` should pass lazy shared PR-content generation into the helper."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement X", task_type="implement")
        task.branch = "feature/work-pr"
        store.update(task)

        config = self._make_config(tmp_path)
        git = Mock(spec=Git)
        git.default_branch.return_value = "main"
        git.count_commits_ahead.return_value = 2
        git.needs_push.return_value = True
        git.is_merged.return_value = False

        ensure_result = Mock(ok=True, status="created", pr_url="https://github.com/o/r/pull/99")
        with (
            patch(
                "gza.runner.build_task_pr_content",
                return_value=("Shared title", "Shared body"),
            ) as build_content,
            patch("gza.runner.ensure_task_pr", return_value=ensure_result) as ensure_pr,
        ):
            outcome = _ensure_work_pr_for_completed_code_task(task, config, store, git)
            ensure_pr.assert_called_once()
            assert "content_builder" in ensure_pr.call_args.kwargs
            assert ensure_pr.call_args.kwargs["merged_behavior"] == "skip"
            assert ensure_pr.call_args.kwargs["draft"] is False
            title, body = ensure_pr.call_args.kwargs["content_builder"]()
            assert (title, body) == ("Shared title", "Shared body")
            build_content.assert_called_once_with(task, git, config, store)

        assert outcome.kind == "ready"
        assert outcome.status == "created"
        git.needs_push.assert_not_called()

    def test_ensure_work_pr_skips_when_branch_has_no_commits(self, tmp_path: Path):
        """`work --pr` should skip PR creation when branch has no commits."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement X", task_type="implement")
        task.branch = "feature/no-commits"
        store.update(task)

        config = self._make_config(tmp_path)
        git = Mock(spec=Git)
        git.default_branch.return_value = "main"
        git.count_commits_ahead.return_value = 0

        with patch("gza.runner.GitHub") as gh_cls:
            outcome = _ensure_work_pr_for_completed_code_task(task, config, store, git)

        assert outcome.kind == "ready"
        assert outcome.status == "no_commits"
        gh_cls.assert_not_called()

    def test_ensure_work_pr_reuses_existing_pr_and_caches_number(self, tmp_path: Path):
        """`work --pr` should reuse an existing PR and cache its number."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement X", task_type="implement")
        task.branch = "feature/existing-pr"
        store.update(task)

        config = self._make_config(tmp_path)
        git = Mock(spec=Git)
        git.default_branch.return_value = "main"
        git.count_commits_ahead.return_value = 1

        ensure_result = Mock(ok=True, status="existing", pr_url="https://github.com/o/r/pull/55")
        with patch("gza.runner.ensure_task_pr", return_value=ensure_result):
            outcome = _ensure_work_pr_for_completed_code_task(task, config, store, git)

        assert outcome.kind == "ready"
        assert outcome.status == "existing"

    def test_ensure_work_pr_reuses_existing_pr_without_generating_pr_content(self, tmp_path: Path):
        """`work --pr` should short-circuit existing PR reuse before spawning PR-content work."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement X", task_type="implement")
        task.branch = "feature/existing-pr"
        store.update(task)

        config = self._make_config(tmp_path)
        git = Mock(spec=Git)
        git.default_branch.return_value = "main"
        git.count_commits_ahead.return_value = 1
        git.needs_push.return_value = False
        git.is_merged.return_value = False

        gh = Mock()
        gh.is_available.return_value = True
        gh.get_pr_details.return_value = None
        gh.discover_pr_by_branch.return_value = PullRequestDetails(
            url="https://github.com/o/r/pull/55",
            number=55,
            state="open",
            base_ref_name="main",
        )

        with (
            patch("gza.runner.build_task_pr_content", side_effect=AssertionError("should not build PR content")),
            patch("gza.pr_ops.GitHub", return_value=gh),
        ):
            outcome = _ensure_work_pr_for_completed_code_task(task, config, store, git)

        assert outcome.kind == "ready"
        assert outcome.status == "existing"
        assert [saved.task_type for saved in store.get_all()] == ["implement"]

    def test_ensure_work_pr_reuses_revalidated_cached_pr(self, tmp_path: Path):
        """`work --pr` should reuse cached PRs only after remote revalidation."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement X", task_type="implement")
        task.branch = "feature/cached-pr"
        task.pr_number = 81
        store.update(task)

        config = self._make_config(tmp_path)
        git = Mock(spec=Git)
        git.default_branch.return_value = "main"
        git.count_commits_ahead.return_value = 1

        ensure_result = Mock(ok=True, status="cached", pr_url="https://github.com/o/r/pull/81", pr_number=81)
        with patch("gza.runner.ensure_task_pr", return_value=ensure_result):
            outcome = _ensure_work_pr_for_completed_code_task(task, config, store, git)

        assert outcome.kind == "ready"
        assert outcome.status == "cached"

    def test_ensure_work_pr_revalidates_cached_pr_before_reuse(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """Cached PR reuse should reflect the revalidated PR URL and avoid fake push output."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement X", task_type="implement")
        task.branch = "feature/cached-pr"
        task.pr_number = 81
        store.update(task)

        config = self._make_config(tmp_path)
        git = Mock(spec=Git)
        git.default_branch.return_value = "main"
        git.count_commits_ahead.return_value = 1

        ensure_result = Mock(ok=True, status="cached", pr_url="https://github.com/o/r/pull/81", pr_number=81)
        with patch("gza.runner.ensure_task_pr", return_value=ensure_result) as ensure_pr:
            outcome = _ensure_work_pr_for_completed_code_task(task, config, store, git)

        assert outcome.kind == "ready"
        assert outcome.status == "cached"
        output = capsys.readouterr().out
        assert "Pushing branch" not in output
        assert "Reusing cached PR #81" in output
        ensure_pr.assert_called_once()

    @pytest.mark.parametrize(("status", "error"), [("gh_unavailable", None), ("lookup_failed", "auth failed")])
    def test_ensure_work_pr_pre_pr_failures_push_before_nonfatal_completion(
        self,
        tmp_path: Path,
        status: str,
        error: str | None,
        capsys: pytest.CaptureFixture[str],
    ):
        """Pre-PR failures should verify branch publication before completing non-fatally."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement X", task_type="implement")
        task.branch = "feature/pre-pr-nonfatal"
        store.update(task)

        config = self._make_config(tmp_path)
        git = Mock(spec=Git)
        git.default_branch.return_value = "main"
        git.count_commits_ahead.return_value = 1
        git.needs_push.return_value = True

        ensure_result = Mock(ok=False, status=status, error=error, pr_url=None)
        with patch("gza.runner.ensure_task_pr", return_value=ensure_result):
            outcome = _ensure_work_pr_for_completed_code_task(task, config, store, git)

        assert outcome.kind == "nonfatal_missing_pr"
        assert outcome.status == status
        assert "is published to origin" in outcome.message
        git.push_branch.assert_called_once_with("feature/pre-pr-nonfatal")
        output = capsys.readouterr().out
        assert "Pushing branch 'feature/pre-pr-nonfatal' to origin..." in output

    @pytest.mark.parametrize(("status", "error"), [("gh_unavailable", None), ("lookup_failed", "auth failed")])
    def test_ensure_work_pr_pre_pr_failures_push_failure_marks_branch_unpushable(
        self,
        tmp_path: Path,
        status: str,
        error: str | None,
    ):
        """Pre-PR failures should fail with BRANCH_UNPUSHABLE when publish verification fails."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement X", task_type="implement")
        task.branch = "feature/pre-pr-push-fails"
        store.update(task)

        config = self._make_config(tmp_path)
        git = Mock(spec=Git)
        git.default_branch.return_value = "main"
        git.count_commits_ahead.return_value = 1
        git.needs_push.return_value = True
        git.push_branch.side_effect = GitError("push failed")

        ensure_result = Mock(ok=False, status=status, error=error, pr_url=None)
        with patch("gza.runner.ensure_task_pr", return_value=ensure_result):
            outcome = _ensure_work_pr_for_completed_code_task(task, config, store, git)

        assert outcome.kind == "branch_unpushable"
        assert outcome.status == "push_failed"
        assert status in outcome.message
        assert "push failed" in outcome.message

    def test_ensure_work_pr_lookup_failed_without_push_needed_keeps_note_truthful(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ):
        """Lookup failures with no pending branch publish should not claim a fresh push occurred."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement X", task_type="implement")
        task.branch = "feature/already-published"
        store.update(task)

        config = self._make_config(tmp_path)
        git = Mock(spec=Git)
        git.default_branch.return_value = "main"
        git.count_commits_ahead.return_value = 1
        git.needs_push.return_value = False

        ensure_result = Mock(ok=False, status="lookup_failed", error="auth failed", pr_url=None)
        with patch("gza.runner.ensure_task_pr", return_value=ensure_result):
            outcome = _ensure_work_pr_for_completed_code_task(task, config, store, git)

        assert outcome.kind == "nonfatal_missing_pr"
        assert "is published to origin" in outcome.message
        assert "was pushed" not in outcome.message
        git.push_branch.assert_not_called()
        assert "Pushing branch" not in capsys.readouterr().out

    def test_complete_code_task_creates_pr_before_auto_review(self, tmp_path: Path):
        """When create_review is enabled, PR creation should run before auto-review execution."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement with review", task_type="implement", create_review=True)
        task.slug = "20260414-impl-review-order"
        store.mark_in_progress(task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{task.slug}.log"
        log_file.write_text("")

        pre_status = set()
        post_status = {("M", "src/foo.py")}
        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = post_status
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_numstat.return_value = "1\t0\tsrc/foo.py\n"

        summary_dir = tmp_path / ".gza" / "summaries"
        summary_path = summary_dir / f"{task.slug}.md"
        worktree_summary_path = tmp_path / "worktree" / ".gza" / "summaries" / f"{task.slug}.md"
        worktree_summary_path.parent.mkdir(parents=True, exist_ok=True)
        worktree_summary_path.write_text("summary")

        call_order: list[str] = []

        def _mark_pr(*_args, **_kwargs):
            call_order.append("pr")
            return CompletedCodeTaskPrPublicationOutcome(
                kind="ready",
                status="created",
                message="created",
            )

        def _run_review(*_args, **_kwargs):
            call_order.append("review")
            return 7

        with (
            patch("gza.runner._squash_wip_commits"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
            patch("gza.runner._ensure_work_pr_for_completed_code_task", side_effect=_mark_pr),
            patch("gza.runner._create_and_run_review_task", side_effect=_run_review),
        ):
            rc = _complete_code_task(
                task,
                config,
                store,
                worktree_git,
                log_file,
                "feature/review-order",
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
                0,
                pre_run_status=pre_status,
                worktree_summary_path=worktree_summary_path,
                summary_path=summary_path,
                summary_dir=summary_dir,
                create_pr=True,
            )

        assert rc == 7
        assert call_order == ["pr", "review"]

    def test_post_complete_implement_without_task_commits_skips_auto_review_and_logs_reason(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement no-op", task_type="implement", create_review=True)
        task.status = "completed"
        task.has_commits = False
        task.branch = "feature/no-task-commits"
        task.slug = "20260605-no-task-commits"
        task.log_file = "logs/20260605-no-task-commits.log"
        store.update(task)

        config = self._make_config(tmp_path)
        log_file = tmp_path / task.log_file
        log_file.parent.mkdir(parents=True, exist_ok=True)
        log_file.write_text("")

        worktree_git = Mock(spec=Git)

        with (
            patch("gza.runner._create_and_run_review_task") as run_review,
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                task,
                config,
                store,
                worktree_git,
                task.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            )

        assert rc == 0
        run_review.assert_not_called()
        reviews = [row for row in store.get_all() if row.task_type == "review"]
        assert reviews == []
        output = capsys.readouterr().out
        assert "nothing to review" in output
        assert "no task commits" in output

        ops_entries = [json.loads(line) for line in ops_log_path_for(log_file).read_text().splitlines()]
        assert any(
            entry["message"]
            == "Skipping auto-review for "
            f"{task.id}: completed with no task commits; nothing to review."
            for entry in ops_entries
        )

    def test_post_complete_implement_with_empty_merge_unit_skips_auto_review_without_no_task_commits_wording(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement already landed", task_type="implement", create_review=True)
        store.mark_completed(task, has_commits=True, branch="feature/empty-review")
        assert task.id is not None
        task.slug = "20260605-empty-review"
        task.log_file = "logs/20260605-empty-review.log"
        store.update(task)

        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None
        store.set_merge_unit_state(unit.id, "empty")

        config = self._make_config(tmp_path)
        log_file = tmp_path / task.log_file
        log_file.parent.mkdir(parents=True, exist_ok=True)
        log_file.write_text("")

        worktree_git = Mock(spec=Git)

        with (
            patch("gza.runner._create_and_run_review_task") as run_review,
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                task,
                config,
                store,
                worktree_git,
                task.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            )

        assert rc == 0
        run_review.assert_not_called()
        reviews = [row for row in store.get_all() if row.task_type == "review"]
        assert reviews == []
        output = capsys.readouterr().out
        assert "no unique commits vs target" in output
        assert "no task commits" not in output

        ops_entries = [json.loads(line) for line in ops_log_path_for(log_file).read_text().splitlines()]
        assert any(
            entry["message"]
            == "Skipping auto-review for "
            f"{task.id}: no unique commits vs target (nothing to review)."
            for entry in ops_entries
        )

    def test_post_complete_implement_with_commits_still_runs_auto_review(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement with commits", task_type="implement", create_review=True)
        store.mark_completed(task, has_commits=True, branch="feature/with-commits")

        config = self._make_config(tmp_path)
        worktree_git = Mock(spec=Git)

        with (
            patch("gza.runner._create_and_run_review_task", return_value=17) as run_review,
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                task,
                config,
                store,
                worktree_git,
                task.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            )

        assert rc == 17
        run_review.assert_called_once()

    def test_post_complete_improve_syncs_live_pr_before_auto_review(self, tmp_path: Path):
        """Successful improve completion should sync a live PR before follow-up review work."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with review", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/improve-review-order"
        store.update(impl)

        improve = store.add(
            prompt="Improve with review",
            task_type="improve",
            based_on=impl.id,
            same_branch=True,
            create_review=True,
        )
        improve.status = "completed"
        improve.branch = impl.branch
        store.update(improve)

        config = self._make_config(tmp_path)
        worktree_git = Mock(spec=Git)
        call_order: list[str] = []

        def _sync_branch(*_args, **_kwargs):
            call_order.append("sync")
            return Mock(ok=True, status="pushed")

        def _run_review(*_args, **_kwargs):
            call_order.append("review")
            return 11

        with (
            patch("gza.runner.sync_task_branch_if_live_pr", side_effect=_sync_branch),
            patch("gza.runner._create_and_run_review_task", side_effect=_run_review),
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                improve,
                config,
                store,
                worktree_git,
                improve.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            )

        assert rc == 11
        assert call_order == ["sync", "review"]

    def test_post_complete_fix_syncs_live_pr_before_auto_review(self, tmp_path: Path):
        """Successful fix completion should sync a live PR before follow-up review work."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with review", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/fix-review-order"
        store.update(impl)
        assert impl.id is not None

        review = store.add(
            prompt="Review before fix",
            task_type="review",
            depends_on=impl.id,
        )
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        fix = store.add(
            prompt="Fix with review",
            task_type="fix",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
            create_review=True,
        )
        fix.status = "completed"
        fix.branch = impl.branch
        store.update(fix)

        config = self._make_config(tmp_path)
        worktree_git = Mock(spec=Git)
        worktree_git.count_commits_ahead.return_value = 3
        call_order: list[str] = []

        def _sync_branch(*_args, **_kwargs):
            call_order.append("sync")
            return Mock(ok=True, status="pushed")

        def _run_review(*_args, **_kwargs):
            call_order.append("review")
            return 13

        with (
            patch("gza.runner.sync_task_branch_if_live_pr", side_effect=_sync_branch),
            patch("gza.runner._create_and_run_review_task", side_effect=_run_review),
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                fix,
                config,
                store,
                worktree_git,
                fix.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
                fix_commits_ahead_before_run=2,
                fix_default_branch="main",
            )

        assert rc == 13
        assert call_order == ["sync", "review"]

    def test_post_complete_improve_sync_failure_skips_auto_review_without_failing_task(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ):
        """Implicit improve PR sync failures should warn and skip auto-review without failing the task."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with review", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/improve-push-failure"
        store.update(impl)
        store.add_comment(impl.id, "Implementation feedback should remain unresolved.", source="direct")

        review = store.add(
            prompt="Review before improve",
            task_type="review",
            depends_on=impl.id,
        )
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        improve = store.add(
            prompt="Improve with review",
            task_type="improve",
            based_on=impl.id,
            same_branch=True,
            create_review=True,
        )
        improve.status = "completed"
        improve.branch = impl.branch
        store.update(improve)

        config = self._make_config(tmp_path)
        worktree_git = Mock(spec=Git)
        sync_result = Mock(ok=False, status="push_failed", pr_number=77, error="push failed")

        with (
            patch("gza.runner.sync_task_branch_if_live_pr", return_value=sync_result),
            patch("gza.runner._create_and_run_review_task") as run_review,
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                improve,
                config,
                store,
                worktree_git,
                improve.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            )

        assert rc == 0
        run_review.assert_not_called()
        output = capsys.readouterr().out
        assert "could not be pushed to PR #77" in output
        assert "Skipping auto-review" in output
        refreshed = store.get(improve.id)
        assert refreshed is not None
        assert refreshed.status == "completed"
        refreshed_impl = store.get(impl.id)
        assert refreshed_impl is not None
        assert refreshed_impl.review_cleared_at is None
        unresolved_impl_comments = store.get_comments(impl.id, unresolved_only=True)
        assert [comment.content for comment in unresolved_impl_comments] == [
            "Implementation feedback should remain unresolved."
        ]

    def test_post_complete_improve_without_review_skips_pr_sync_and_clears_review_state(
        self,
        tmp_path: Path,
    ):
        """Plain improve completion should preserve legacy cleanup without implicit GitHub sync."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with review", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/improve-no-review-sync"
        store.update(impl)
        store.add_comment(impl.id, "Resolved by plain improve.", source="direct")

        review = store.add(
            prompt="Review before improve",
            task_type="review",
            depends_on=impl.id,
        )
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        improve = store.add(
            prompt="Improve without follow-up review",
            task_type="improve",
            based_on=impl.id,
            same_branch=True,
            create_review=False,
        )
        improve.status = "completed"
        improve.branch = impl.branch
        store.update(improve)

        config = self._make_config(tmp_path)
        worktree_git = Mock(spec=Git)

        with (
            patch("gza.runner.sync_task_branch_if_live_pr") as sync_branch,
            patch("gza.runner._create_and_run_review_task") as run_review,
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                improve,
                config,
                store,
                worktree_git,
                improve.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            )

        assert rc == 0
        sync_branch.assert_not_called()
        run_review.assert_not_called()
        refreshed_impl = store.get(impl.id)
        assert refreshed_impl is not None
        assert refreshed_impl.review_cleared_at is not None
        unresolved_impl_comments = store.get_comments(impl.id, unresolved_only=True)
        assert unresolved_impl_comments == []

    def test_post_complete_noop_improve_persists_changed_diff_and_skips_follow_up_review(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with review", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/noop-improve"
        store.update(impl)
        assert impl.id is not None
        old_comment = store.add_comment(impl.id, "Older comment already handled.", source="direct")
        old_cleared_at = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
        impl.review_cleared_at = old_cleared_at
        store.update(impl)
        store.resolve_comments(impl.id, created_on_or_before=old_comment.created_at)
        store.add_comment(impl.id, "Comment should remain unresolved after no-op improve.", source="direct")

        review = store.add(
            prompt="Review before improve",
            task_type="review",
            depends_on=impl.id,
        )
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        improve = store.add(
            prompt="Improve without tracked diff change",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
            create_review=True,
        )
        improve.status = "completed"
        improve.branch = impl.branch
        store.update(improve)

        config = self._make_config(tmp_path)
        worktree_git = Mock(spec=Git)

        with (
            patch(
                "gza.runner.compute_improve_changed_diff",
                return_value=ImproveDiffResult(changed_diff=False, detail="no (no tracked improve changes)"),
            ),
            patch("gza.runner.sync_task_branch_if_live_pr") as sync_branch,
            patch("gza.runner._create_and_run_review_task") as run_review,
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                improve,
                config,
                store,
                worktree_git,
                improve.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            )

        assert rc == 0
        sync_branch.assert_not_called()
        run_review.assert_not_called()

        refreshed_improve = store.get(improve.id)
        assert refreshed_improve is not None
        assert refreshed_improve.changed_diff is False

        refreshed_impl = store.get(impl.id)
        assert refreshed_impl is not None
        assert refreshed_impl.review_cleared_at == old_cleared_at
        unresolved_impl_comments = store.get_comments(impl.id, unresolved_only=True)
        assert [comment.content for comment in unresolved_impl_comments] == [
            "Comment should remain unresolved after no-op improve."
        ]
        output = capsys.readouterr().out
        assert "Warning: Improve completed with no tracked diff change." in output
        assert "Changed Diff: no (no tracked improve changes)" in output

    def test_post_complete_noop_improve_persists_valid_disputed_blocker_artifact(
        self,
        tmp_path: Path,
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with disputed blocker", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/noop-improve-dispute"
        store.update(impl)
        assert impl.id is not None

        review = store.add(
            prompt="Review before improve",
            task_type="review",
            depends_on=impl.id,
        )
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = (
            "## Summary\n\n- Found a blocker.\n\n"
            "## Blockers\n\n"
            "### B1 Missing API guard\n"
            "Evidence: the current code still accepts empty IDs.\n"
            "Open-state citation: `src/api.py:12-18`\n"
            "Impact: invalid requests can crash the handler.\n"
            "Required fix: reject empty IDs before calling the service.\n"
            "Required tests: add regression coverage for empty IDs.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        store.update(review)

        improve = store.add(
            prompt="Improve without tracked diff change",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
            create_review=True,
        )
        improve.status = "completed"
        improve.completed_at = datetime.now(UTC)
        improve.branch = impl.branch
        improve.output_content = (
            "## Summary\n\n- No code change was necessary.\n\n"
            "## Disputed Blockers\n\n"
            "### D1\n"
            "Finding: B1\n"
            "Reason: already_satisfied\n"
            "Evidence: The current branch already rejects empty IDs before the service call.\n"
            "Current-state citation: `src/api.py:12-18`\n"
            "Scope citation: `docs/plan.md:44-49`\n"
            "Downstream task: gza-77\n"
        )
        store.update(improve)

        config = self._make_config(tmp_path)
        worktree_git = Mock(spec=Git)
        worktree_git.rev_parse_if_exists.return_value = "deadbeef"

        with (
            patch(
                "gza.runner.compute_improve_changed_diff",
                return_value=ImproveDiffResult(changed_diff=False, detail="no (no tracked improve changes)"),
            ),
            patch("gza.runner._capture_noop_improve_review_verify_result"),
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                improve,
                config,
                store,
                worktree_git,
                improve.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            )

        assert rc == 0
        artifacts = store.list_artifacts(review.id, kind=REVIEW_BLOCKER_RESOLUTION_ARTIFACT_KIND)
        assert len(artifacts) == 1
        artifact = artifacts[0]
        assert artifact.status == "disputed"
        assert artifact.exit_status == "already_satisfied"
        assert artifact.head_sha == "deadbeef"
        assert artifact.metadata == {
            "current_state_citation": "`src/api.py:12-18`",
            "downstream_task_id": "gza-77",
            "evidence": "The current branch already rejects empty IDs before the service call.",
            "finding_fingerprint": {
                "anchor": "src/api.py:12-18",
                "title": "missing api guard",
            },
            "finding_id": "B1",
            "impl_task_id": impl.id,
            "reason": "already_satisfied",
            "review_task_id": review.id,
            "schema_version": 1,
            "scope_citation": "`docs/plan.md:44-49`",
            "source_branch": improve.branch,
            "source_task_id": improve.id,
            "source_task_type": "improve",
            "state": "disputed",
        }

    def test_post_complete_noop_improve_persists_disputed_blocker_artifact_when_finding_includes_title(
        self,
        tmp_path: Path,
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with disputed blocker", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/noop-improve-dispute-title"
        store.update(impl)
        assert impl.id is not None

        review = store.add(
            prompt="Review before improve",
            task_type="review",
            depends_on=impl.id,
        )
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = (
            "## Summary\n\n- Found a blocker.\n\n"
            "## Blockers\n\n"
            "### B1 Missing API guard\n"
            "Evidence: the current code still accepts empty IDs.\n"
            "Open-state citation: `src/api.py:12-18`\n"
            "Impact: invalid requests can crash the handler.\n"
            "Required fix: reject empty IDs before calling the service.\n"
            "Required tests: add regression coverage for empty IDs.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        store.update(review)

        improve = store.add(
            prompt="Improve without tracked diff change",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
            create_review=True,
        )
        improve.status = "completed"
        improve.completed_at = datetime.now(UTC)
        improve.branch = impl.branch
        improve.output_content = (
            "## Summary\n\n- No code change was necessary.\n\n"
            "## Disputed Blockers\n\n"
            "### D1\n"
            "Finding: B1 Missing API guard\n"
            "Reason: already_satisfied\n"
            "Evidence: The current branch already rejects empty IDs before the service call.\n"
            "Current-state citation: `src/api.py:12-18`\n"
        )
        store.update(improve)

        config = self._make_config(tmp_path)
        worktree_git = Mock(spec=Git)
        worktree_git.rev_parse_if_exists.return_value = "feedface"

        with (
            patch(
                "gza.runner.compute_improve_changed_diff",
                return_value=ImproveDiffResult(changed_diff=False, detail="no (no tracked improve changes)"),
            ),
            patch("gza.runner._capture_noop_improve_review_verify_result"),
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                improve,
                config,
                store,
                worktree_git,
                improve.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            )

        assert rc == 0
        artifacts = store.list_artifacts(review.id, kind=REVIEW_BLOCKER_RESOLUTION_ARTIFACT_KIND)
        assert len(artifacts) == 1
        assert artifacts[0].metadata is not None
        assert artifacts[0].metadata["finding_id"] == "B1"
        assert artifacts[0].metadata["finding_fingerprint"] == {
            "anchor": "src/api.py:12-18",
            "title": "missing api guard",
        }

    def test_post_complete_noop_improve_ignores_malformed_disputed_blocker_artifact(
        self,
        tmp_path: Path,
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with malformed dispute", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/noop-improve-bad-dispute"
        store.update(impl)
        assert impl.id is not None

        review = store.add(
            prompt="Review before improve",
            task_type="review",
            depends_on=impl.id,
        )
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = (
            "## Summary\n\n- Found a blocker.\n\n"
            "## Blockers\n\n"
            "### B1 Missing API guard\n"
            "Evidence: the current code still accepts empty IDs.\n"
            "Open-state citation: `src/api.py:12-18`\n"
            "Impact: invalid requests can crash the handler.\n"
            "Required fix: reject empty IDs before calling the service.\n"
            "Required tests: add regression coverage for empty IDs.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        store.update(review)

        improve = store.add(
            prompt="Improve without tracked diff change",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
        )
        improve.status = "completed"
        improve.completed_at = datetime.now(UTC)
        improve.branch = impl.branch
        improve.output_content = (
            "## Summary\n\n- No code change was necessary.\n\n"
            "## Disputed Blockers\n\n"
            "### D1\n"
            "Finding: B1\n"
            "Reason: already_satisfied\n"
            "Evidence: The current branch already rejects empty IDs before the service call.\n"
            "Current-state citation: src/api.py\n"
        )
        store.update(improve)

        config = self._make_config(tmp_path)
        worktree_git = Mock(spec=Git)
        worktree_git.rev_parse_if_exists.return_value = "deadbeef"

        with (
            patch(
                "gza.runner.compute_improve_changed_diff",
                return_value=ImproveDiffResult(changed_diff=False, detail="no (no tracked improve changes)"),
            ),
            patch("gza.runner._capture_noop_improve_review_verify_result"),
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                improve,
                config,
                store,
                worktree_git,
                improve.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            )

        assert rc == 0
        assert store.list_artifacts(review.id, kind=REVIEW_BLOCKER_RESOLUTION_ARTIFACT_KIND) == []

    def test_post_complete_noop_fix_persists_valid_disputed_blocker_artifact(
        self,
        tmp_path: Path,
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with disputed blocker", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/noop-fix-dispute"
        store.update(impl)
        assert impl.id is not None

        review = store.add(
            prompt="Review before fix",
            task_type="review",
            depends_on=impl.id,
        )
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = (
            "## Summary\n\n- Found a blocker.\n\n"
            "## Blockers\n\n"
            "### B1 Missing API guard\n"
            "Evidence: the current code still accepts empty IDs.\n"
            "Open-state citation: `src/api.py:12-18`\n"
            "Impact: invalid requests can crash the handler.\n"
            "Required fix: reject empty IDs before calling the service.\n"
            "Required tests: add regression coverage for empty IDs.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        store.update(review)

        fix = store.add(
            prompt="Fix without tracked diff change",
            task_type="fix",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
        )
        fix.status = "completed"
        fix.completed_at = datetime.now(UTC)
        fix.branch = impl.branch
        fix.output_content = (
            "## Summary\n\n- No code change was necessary.\n\n"
            "## Disputed Blockers\n\n"
            "### D1\n"
            "Finding: B1\n"
            "Reason: stale\n"
            "Evidence: The current branch no longer contains the stale call site from the review.\n"
            "Current-state citation: `src/api.py:12-18`\n"
        )
        store.update(fix)

        config = self._make_config(tmp_path)
        worktree_git = Mock(spec=Git)
        worktree_git.rev_parse_if_exists.return_value = "cafebabe"

        with (
            patch("gza.runner._prepare_fix_follow_up_review", return_value=False),
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                fix,
                config,
                store,
                worktree_git,
                fix.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            )

        assert rc == 0
        artifacts = store.list_artifacts(review.id, kind=REVIEW_BLOCKER_RESOLUTION_ARTIFACT_KIND)
        assert len(artifacts) == 1
        assert artifacts[0].metadata is not None
        assert artifacts[0].metadata["source_task_type"] == "fix"
        assert artifacts[0].metadata["reason"] == "stale"
        assert artifacts[0].head_sha == "cafebabe"

    def test_completed_review_adjudication_persists_resolution_artifact(
        self,
        tmp_path: Path,
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with disputed blocker", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/review-adjudication"
        store.update(impl)
        assert impl.id is not None

        review = store.add(
            prompt="Review before adjudication",
            task_type="review",
            depends_on=impl.id,
        )
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = (
            "## Summary\n\n- Found a blocker.\n\n"
            "## Blockers\n\n"
            "### B1 Missing API guard\n"
            "Evidence: the current code still accepts empty IDs.\n"
            "Open-state citation: `src/api.py:12-18`\n"
            "Impact: invalid requests can crash the handler.\n"
            "Required fix: reject empty IDs before calling the service.\n"
            "Required tests: add regression coverage for empty IDs.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        store.update(review)

        improve = store.add(
            prompt="Improve without tracked diff change",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
            create_review=True,
        )
        improve.status = "completed"
        improve.completed_at = datetime.now(UTC)
        improve.branch = impl.branch
        store.update(improve)
        assert improve.id is not None

        store.add_artifact(
            review.id,
            kind=REVIEW_BLOCKER_RESOLUTION_ARTIFACT_KIND,
            label="disputed-B1",
            path=".gza/artifacts/disputed-b1.txt",
            byte_size=0,
            sha256="0" * 64,
            status="disputed",
            exit_status="already_satisfied",
            head_sha="deadbeef",
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
                "evidence": "The current branch already rejects empty IDs before the service call.",
                "current_state_citation": "`src/api.py:12-18`",
                "scope_citation": "`docs/plan.md:44-49`",
                "downstream_task_id": "gza-77",
                "finding_fingerprint": {
                    "title": "missing api guard",
                    "anchor": "src/api.py:12-18",
                },
            },
            created_at=improve.completed_at,
        )

        adjudication = store.add(
            prompt=f"Adjudicate blocker B1 from review {review.id} for task {impl.id}: Missing API guard",
            task_type="internal",
            based_on=review.id,
            depends_on=impl.id,
            same_branch=True,
        )
        adjudication.status = "completed"
        adjudication.completed_at = datetime.now(UTC)
        adjudication.output_content = "INVALID\n"
        store.update(adjudication)

        config = self._make_config(tmp_path)

        _persist_review_blocker_adjudication_for_completed_task(
            config=config,
            store=store,
            completed_task=adjudication,
        )

        artifacts = store.list_artifacts(review.id, kind=REVIEW_BLOCKER_RESOLUTION_ARTIFACT_KIND)
        assert len(artifacts) == 2
        artifact = next(candidate for candidate in artifacts if candidate.status == "invalid")
        assert artifact.status == "invalid"
        assert artifact.exit_status == "already_satisfied"
        assert artifact.head_sha == "deadbeef"
        assert artifact.metadata is not None
        assert artifact.metadata["state"] == "invalid"
        assert artifact.metadata["source_task_id"] == adjudication.id
        assert artifact.metadata["source_task_type"] == "internal"
        assert artifact.metadata["source_branch"] == impl.branch
        assert artifact.metadata["finding_id"] == "B1"
        assert artifact.metadata["reason"] == "already_satisfied"
        assert artifact.metadata["evidence"] == "The current branch already rejects empty IDs before the service call."
        assert artifact.metadata["current_state_citation"] == "`src/api.py:12-18`"
        assert artifact.metadata["scope_citation"] == "`docs/plan.md:44-49`"
        assert artifact.metadata["downstream_task_id"] == "gza-77"
        artifact_output = (tmp_path / artifact.path).read_text()
        assert "Source branch: feature/review-adjudication" in artifact_output
        assert "Head SHA: deadbeef" in artifact_output

    def test_completed_review_adjudication_needs_human_preserves_dispute_evidence(
        self,
        tmp_path: Path,
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with disputed blocker", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/review-adjudication-needs-human"
        store.update(impl)
        assert impl.id is not None

        review = store.add(
            prompt="Review before adjudication",
            task_type="review",
            depends_on=impl.id,
        )
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = (
            "## Summary\n\n- Found a blocker.\n\n"
            "## Blockers\n\n"
            "### B1 Missing API guard\n"
            "Evidence: the current code still accepts empty IDs.\n"
            "Open-state citation: `src/api.py:12-18`\n"
            "Impact: invalid requests can crash the handler.\n"
            "Required fix: reject empty IDs before calling the service.\n"
            "Required tests: add regression coverage for empty IDs.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        store.update(review)

        improve = store.add(
            prompt="Improve without tracked diff change",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
            create_review=True,
        )
        improve.status = "completed"
        improve.completed_at = datetime.now(UTC)
        improve.branch = impl.branch
        store.update(improve)
        assert improve.id is not None

        store.add_artifact(
            review.id,
            kind=REVIEW_BLOCKER_RESOLUTION_ARTIFACT_KIND,
            label="disputed-B1",
            path=".gza/artifacts/disputed-b1.txt",
            byte_size=0,
            sha256="0" * 64,
            status="disputed",
            exit_status="review_error",
            head_sha="cafebabe",
            metadata={
                "schema_version": 1,
                "state": "disputed",
                "review_task_id": review.id,
                "impl_task_id": impl.id,
                "source_task_id": improve.id,
                "source_task_type": "improve",
                "source_branch": impl.branch,
                "finding_id": "B1",
                "reason": "review_error",
                "evidence": "The open-state citation refers to code removed by the last rebase.",
                "current_state_citation": "`src/api.py:12-18`",
                "scope_citation": "`docs/plan.md:88-91`",
                "downstream_task_id": "gza-88",
                "finding_fingerprint": {
                    "title": "missing api guard",
                    "anchor": "src/api.py:12-18",
                },
            },
            created_at=improve.completed_at,
        )

        adjudication = store.add(
            prompt=f"Adjudicate blocker B1 from review {review.id} for task {impl.id}: Missing API guard",
            task_type="internal",
            based_on=review.id,
            depends_on=impl.id,
            same_branch=True,
        )
        adjudication.status = "completed"
        adjudication.completed_at = datetime.now(UTC)
        adjudication.output_content = "NEEDS_HUMAN\n"
        store.update(adjudication)

        config = self._make_config(tmp_path)

        _persist_review_blocker_adjudication_for_completed_task(
            config=config,
            store=store,
            completed_task=adjudication,
        )

        artifacts = store.list_artifacts(review.id, kind=REVIEW_BLOCKER_RESOLUTION_ARTIFACT_KIND)
        assert len(artifacts) == 2
        artifact = next(candidate for candidate in artifacts if candidate.status == "needs_human")
        assert artifact.status == "needs_human"
        assert artifact.exit_status == "review_error"
        assert artifact.head_sha == "cafebabe"
        assert artifact.metadata is not None
        assert artifact.metadata["state"] == "needs_human"
        assert artifact.metadata["source_task_id"] == adjudication.id
        assert artifact.metadata["source_branch"] == impl.branch
        assert artifact.metadata["reason"] == "review_error"
        assert artifact.metadata["evidence"] == "The open-state citation refers to code removed by the last rebase."
        assert artifact.metadata["current_state_citation"] == "`src/api.py:12-18`"
        assert artifact.metadata["scope_citation"] == "`docs/plan.md:88-91`"
        assert artifact.metadata["downstream_task_id"] == "gza-88"
        artifact_output = (tmp_path / artifact.path).read_text()
        assert "Head SHA: cafebabe" in artifact_output

    def test_completed_review_adjudication_with_unparseable_output_fails_closed(
        self,
        tmp_path: Path,
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with disputed blocker", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/review-adjudication-bad-output"
        store.update(impl)
        assert impl.id is not None

        review = store.add(
            prompt="Review before adjudication",
            task_type="review",
            depends_on=impl.id,
        )
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = (
            "## Summary\n\n- Found a blocker.\n\n"
            "## Blockers\n\n"
            "### B1 Missing API guard\n"
            "Evidence: the current code still accepts empty IDs.\n"
            "Open-state citation: `src/api.py:12-18`\n"
            "Impact: invalid requests can crash the handler.\n"
            "Required fix: reject empty IDs before calling the service.\n"
            "Required tests: add regression coverage for empty IDs.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        store.update(review)

        adjudication = store.add(
            prompt=f"Adjudicate blocker B1 from review {review.id} for task {impl.id}: Missing API guard",
            task_type="internal",
            based_on=review.id,
            depends_on=impl.id,
            same_branch=True,
        )
        adjudication.status = "completed"
        adjudication.completed_at = datetime.now(UTC)
        adjudication.output_content = "INVALID\nBecause the blocker is stale.\n"
        store.update(adjudication)

        config = self._make_config(tmp_path)

        _persist_review_blocker_adjudication_for_completed_task(
            config=config,
            store=store,
            completed_task=adjudication,
        )

        assert store.list_artifacts(review.id, kind=REVIEW_BLOCKER_RESOLUTION_ARTIFACT_KIND) == []

    def test_completed_review_adjudication_without_matching_dispute_fails_closed(
        self,
        tmp_path: Path,
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with disputed blocker", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/review-adjudication-no-dispute"
        store.update(impl)
        assert impl.id is not None

        review = store.add(
            prompt="Review before adjudication",
            task_type="review",
            depends_on=impl.id,
        )
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = (
            "## Summary\n\n- Found a blocker.\n\n"
            "## Blockers\n\n"
            "### B1 Missing API guard\n"
            "Evidence: the current code still accepts empty IDs.\n"
            "Open-state citation: `src/api.py:12-18`\n"
            "Impact: invalid requests can crash the handler.\n"
            "Required fix: reject empty IDs before calling the service.\n"
            "Required tests: add regression coverage for empty IDs.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        store.update(review)

        store.add_artifact(
            review.id,
            kind=REVIEW_BLOCKER_RESOLUTION_ARTIFACT_KIND,
            label="disputed-B2",
            path=".gza/artifacts/disputed-b2.txt",
            byte_size=0,
            sha256="0" * 64,
            status="disputed",
            exit_status="already_satisfied",
            head_sha="deadbeef",
            metadata={
                "schema_version": 1,
                "state": "disputed",
                "review_task_id": review.id,
                "impl_task_id": impl.id,
                "source_task_id": impl.id,
                "source_task_type": "improve",
                "source_branch": impl.branch,
                "finding_id": "B2",
                "reason": "already_satisfied",
                "evidence": "Different blocker.",
                "current_state_citation": "`src/other.py:1-2`",
                "finding_fingerprint": {
                    "title": "different blocker",
                    "anchor": "src/other.py:1-2",
                },
            },
            created_at=datetime.now(UTC),
        )

        adjudication = store.add(
            prompt=f"Adjudicate blocker B1 from review {review.id} for task {impl.id}: Missing API guard",
            task_type="internal",
            based_on=review.id,
            depends_on=impl.id,
            same_branch=True,
        )
        adjudication.status = "completed"
        adjudication.completed_at = datetime.now(UTC)
        adjudication.output_content = "INVALID\n"
        store.update(adjudication)

        config = self._make_config(tmp_path)

        _persist_review_blocker_adjudication_for_completed_task(
            config=config,
            store=store,
            completed_task=adjudication,
        )

        artifacts = store.list_artifacts(review.id, kind=REVIEW_BLOCKER_RESOLUTION_ARTIFACT_KIND)
        assert len(artifacts) == 1
        assert artifacts[0].metadata is not None
        assert artifacts[0].metadata["finding_id"] == "B2"

    def test_post_complete_noop_improve_does_not_clear_verify_only_review_block_without_green_verify_evidence(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with verify-only review blocker", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/noop-verify-only-clear"
        store.update(impl)
        assert impl.id is not None

        review = store.add(
            prompt="Review before no-op improve",
            task_type="review",
            depends_on=impl.id,
        )
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = (
            "## Summary\n\n- Implementation is aligned; verify failed.\n\n"
            "## Blockers\n\n"
            "### B1 verify_command failure: mypy error\n"
            "Evidence: verify_command failed with exit status 1.\n"
            "Impact: autonomous verify fails.\n"
            "Required fix: rerun verify_command on the current tip.\n"
            "Required tests: rerun verify_command.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        review.review_verify_status = "failed"
        review.review_verify_branch = impl.branch
        review.review_verify_head_sha = "abc1234"
        store.update(review)

        improve = store.add(
            prompt="No-op improve after verify-only review",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
            create_review=True,
        )
        improve.status = "completed"
        improve.branch = impl.branch
        store.update(improve)

        config = self._make_config(tmp_path)
        worktree_git = Mock(spec=Git)
        worktree_git.rev_parse_if_exists.return_value = "abc1234"

        with (
            patch(
                "gza.runner.compute_improve_changed_diff",
                return_value=ImproveDiffResult(changed_diff=False, detail="no (no tracked improve changes)"),
            ),
            patch("gza.runner.sync_task_branch_if_live_pr") as sync_branch,
            patch("gza.runner._create_and_run_review_task") as run_review,
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                improve,
                config,
                store,
                worktree_git,
                improve.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            )

        assert rc == 0
        sync_branch.assert_not_called()
        run_review.assert_not_called()

        refreshed_impl = store.get(impl.id)
        assert refreshed_impl is not None
        assert refreshed_impl.review_cleared_at is None
        output = capsys.readouterr().out
        assert "cleared verify-origin blocker from persisted passing no-op improve verify evidence" not in output

    def test_post_complete_noop_improve_clears_verify_only_review_block_with_current_green_verify_evidence(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with verify-only review blocker", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/noop-verify-only-clear"
        store.update(impl)
        assert impl.id is not None

        review = store.add(
            prompt="Review before no-op improve",
            task_type="review",
            depends_on=impl.id,
        )
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = (
            "## Summary\n\n- Implementation is aligned; verify failed.\n\n"
            "## Blockers\n\n"
            "### B1 verify_command failure: mypy error\n"
            "Evidence: verify_command failed with exit status 1.\n"
            "Impact: autonomous verify fails.\n"
            "Required fix: rerun verify_command on the current tip.\n"
            "Required tests: rerun verify_command.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        review.review_verify_status = "failed"
        review.review_verify_branch = impl.branch
        review.review_verify_head_sha = "abc1234"
        store.update(review)

        improve = store.add(
            prompt="No-op improve after verify-only review",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
            create_review=True,
        )
        improve.status = "completed"
        improve.branch = impl.branch
        improve.review_verify_status = "passed"
        improve.review_verify_branch = impl.branch
        improve.review_verify_head_sha = "abc1234"
        improve.review_verify_captured_at = review.completed_at + timedelta(seconds=1)
        store.update(improve)

        config = self._make_config(tmp_path)
        worktree_git = Mock(spec=Git)
        worktree_git.rev_parse_if_exists.return_value = "abc1234"

        with (
            patch(
                "gza.runner.compute_improve_changed_diff",
                return_value=ImproveDiffResult(changed_diff=False, detail="no (no tracked improve changes)"),
            ),
            patch("gza.runner.sync_task_branch_if_live_pr") as sync_branch,
            patch("gza.runner._create_and_run_review_task") as run_review,
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                improve,
                config,
                store,
                worktree_git,
                improve.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            )

        assert rc == 0
        sync_branch.assert_not_called()
        run_review.assert_not_called()

        refreshed_impl = store.get(impl.id)
        assert refreshed_impl is not None
        assert refreshed_impl.review_cleared_at is not None
        assert refreshed_impl.review_cleared_at >= review.completed_at
        output = capsys.readouterr().out
        assert "cleared verify-origin blocker from persisted passing no-op improve verify evidence" in output

    def test_post_complete_noop_improve_clears_report_file_verify_only_review_block_with_current_green_verify_evidence(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with verify-only review blocker", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/noop-verify-only-clear-report-file"
        store.update(impl)
        assert impl.id is not None

        review = store.add(
            prompt="Review before no-op improve",
            task_type="review",
            depends_on=impl.id,
        )
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        report_path = tmp_path / "reports" / "verify-only-review.md"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            "## Summary\n\n- Implementation is aligned; verify failed.\n\n"
            "## Blockers\n\n"
            "### B1 verify_command failure: mypy error\n"
            "Evidence: verify_command failed with exit status 1.\n"
            "Impact: autonomous verify fails.\n"
            "Required fix: rerun verify_command on the current tip.\n"
            "Required tests: rerun verify_command.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n",
            encoding="utf-8",
        )
        review.report_file = str(report_path.relative_to(tmp_path))
        review.review_verify_status = "failed"
        review.review_verify_branch = impl.branch
        review.review_verify_head_sha = "abc1234"
        store.update(review)

        improve = store.add(
            prompt="No-op improve after verify-only review",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
            create_review=True,
        )
        improve.status = "completed"
        improve.branch = impl.branch
        improve.review_verify_status = "passed"
        improve.review_verify_branch = impl.branch
        improve.review_verify_head_sha = "abc1234"
        improve.review_verify_captured_at = review.completed_at + timedelta(seconds=1)
        store.update(improve)

        config = self._make_config(tmp_path)
        worktree_git = Mock(spec=Git)
        worktree_git.rev_parse_if_exists.return_value = "abc1234"

        with (
            patch(
                "gza.runner.compute_improve_changed_diff",
                return_value=ImproveDiffResult(changed_diff=False, detail="no (no tracked improve changes)"),
            ),
            patch("gza.runner.sync_task_branch_if_live_pr") as sync_branch,
            patch("gza.runner._create_and_run_review_task") as run_review,
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                improve,
                config,
                store,
                worktree_git,
                improve.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            )

        assert rc == 0
        sync_branch.assert_not_called()
        run_review.assert_not_called()

        refreshed_impl = store.get(impl.id)
        assert refreshed_impl is not None
        assert refreshed_impl.review_cleared_at is not None
        assert refreshed_impl.review_cleared_at >= review.completed_at
        output = capsys.readouterr().out
        assert "cleared verify-origin blocker from persisted passing no-op improve verify evidence" in output

    def test_post_complete_noop_improve_keeps_current_green_verify_evidence_when_recapture_would_overwrite_it(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with verify-only review blocker", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/noop-verify-only-preserve-green"
        store.update(impl)
        assert impl.id is not None

        review = store.add(
            prompt="Review before no-op improve",
            task_type="review",
            depends_on=impl.id,
        )
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = (
            "## Summary\n\n- Implementation is aligned; verify failed.\n\n"
            "## Blockers\n\n"
            "### B1 verify_command failure: mypy error\n"
            "Evidence: verify_command failed with exit status 1.\n"
            "Impact: autonomous verify fails.\n"
            "Required fix: rerun verify_command on the current tip.\n"
            "Required tests: rerun verify_command.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        review.review_verify_status = "failed"
        review.review_verify_branch = impl.branch
        review.review_verify_head_sha = "abc1234"
        store.update(review)

        improve = store.add(
            prompt="No-op improve after verify-only review",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
            create_review=True,
        )
        improve.status = "completed"
        improve.branch = impl.branch
        improve.review_verify_status = "passed"
        improve.review_verify_branch = impl.branch
        improve.review_verify_head_sha = "abc1234"
        improve.review_verify_captured_at = review.completed_at + timedelta(seconds=1)
        store.update(improve)

        config = self._make_config(tmp_path)
        worktree_git = Mock(spec=Git)
        worktree_git.rev_parse_if_exists.return_value = "abc1234"

        def overwrite_green_with_unavailable(**_: object) -> ReviewVerifyResult:
            task = store.get(improve.id)
            assert task is not None
            task.review_verify_status = "unavailable"
            task.review_verify_exit_status = "launch failed"
            task.review_verify_failure = "simulated overwrite"
            task.review_verify_captured_at = review.completed_at + timedelta(seconds=2)
            store.update(task)
            return ReviewVerifyResult(
                command="verify",
                status="unavailable",
                exit_status="launch failed",
                captured_at=task.review_verify_captured_at,
                reviewed_branch=impl.branch,
                reviewed_head_sha="abc1234",
                reviewed_base_sha="cafebabe",
                failure="simulated overwrite",
            )

        with (
            patch(
                "gza.runner.compute_improve_changed_diff",
                return_value=ImproveDiffResult(changed_diff=False, detail="no (no tracked improve changes)"),
            ),
            patch("gza.runner._capture_noop_improve_review_verify_result", side_effect=overwrite_green_with_unavailable) as capture_verify,
            patch("gza.runner.sync_task_branch_if_live_pr") as sync_branch,
            patch("gza.runner._create_and_run_review_task") as run_review,
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                improve,
                config,
                store,
                worktree_git,
                improve.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            )

        assert rc == 0
        capture_verify.assert_not_called()
        sync_branch.assert_not_called()
        run_review.assert_not_called()

        refreshed_impl = store.get(impl.id)
        assert refreshed_impl is not None
        assert refreshed_impl.review_cleared_at is not None
        assert refreshed_impl.review_cleared_at >= review.completed_at

        refreshed_improve = store.get(improve.id)
        assert refreshed_improve is not None
        assert refreshed_improve.review_verify_status == "passed"
        assert refreshed_improve.review_verify_exit_status is None
        assert refreshed_improve.review_verify_failure is None
        assert refreshed_improve.review_verify_captured_at == review.completed_at + timedelta(seconds=1)

        output = capsys.readouterr().out
        assert "cleared verify-origin blocker from persisted passing no-op improve verify evidence" in output

    def test_post_complete_noop_improve_captures_passing_verify_evidence_clears_review_and_becomes_mergeable(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with verify-only review blocker", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/noop-verify-only-fresh"
        store.update(impl)
        assert impl.id is not None

        review = store.add(
            prompt="Review before no-op improve",
            task_type="review",
            depends_on=impl.id,
        )
        review.status = "completed"
        review.completed_at = datetime(2026, 6, 1, 18, 0, tzinfo=UTC)
        review.output_content = (
            "## Summary\n\n- Implementation is aligned; verify failed.\n\n"
            "## Blockers\n\n"
            "### B1 verify_command failure: mypy error\n"
            "Evidence: verify_command failed with exit status 1.\n"
            "Impact: autonomous verify fails.\n"
            "Required fix: rerun verify_command on the current tip.\n"
            "Required tests: rerun verify_command.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        review.review_verify_status = "failed"
        review.review_verify_branch = impl.branch
        review.review_verify_head_sha = "abc1234"
        store.update(review)

        improve = store.add(
            prompt="No-op improve after verify-only review",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
            create_review=True,
        )
        improve.status = "completed"
        improve.branch = impl.branch
        improve.slug = "20260605-noop-improve-fresh-verify"
        store.update(improve)

        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\n"
            "verify_command: uv run pytest tests/ -q\n",
            encoding="utf-8",
        )
        config = Config.load(tmp_path)
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.verify_command = "uv run pytest tests/ -q"
        config.autonomous_verify_timeout_seconds = 120
        config.review_verify_timeout_grace_seconds = 9.0

        worktree_git = Mock(spec=Git)
        worktree_git.repo_dir = tmp_path
        worktree_git.default_branch.return_value = "main"
        worktree_git.rev_parse_if_exists.return_value = "abc1234"

        captured_at = datetime(2026, 6, 1, 19, 0, tzinfo=UTC)
        with (
            patch(
                "gza.runner.compute_improve_changed_diff",
                return_value=ImproveDiffResult(changed_diff=False, detail="no (no tracked improve changes)"),
            ),
            patch(
                "gza.runner._capture_noop_improve_review_verify_result",
                wraps=_capture_noop_improve_review_verify_result,
            ) as capture_verify,
            patch(
                "gza.runner._run_review_verify_command",
                return_value=ReviewVerifyResult(
                    command=config.verify_command,
                    status="passed",
                    exit_status="0",
                    captured_at=captured_at,
                    reviewed_branch=impl.branch,
                    reviewed_head_sha="abc1234",
                    reviewed_base_sha="cafebabe",
                ),
            ) as mock_review_verify,
            patch("gza.runner._resolve_review_verify_base_sha", return_value="cafebabe"),
            patch("gza.runner.sync_task_branch_if_live_pr") as sync_branch,
            patch("gza.runner._create_and_run_review_task") as run_review,
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                improve,
                config,
                store,
                worktree_git,
                improve.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            )

        assert rc == 0
        assert mock_review_verify.call_args.kwargs["timeout_grace_seconds"] == 9.0
        capture_verify.assert_called_once()
        sync_branch.assert_not_called()
        run_review.assert_not_called()

        refreshed_impl = store.get(impl.id)
        assert refreshed_impl is not None
        assert refreshed_impl.review_cleared_at is not None

        refreshed_improve = store.get(improve.id)
        assert refreshed_improve is not None
        assert refreshed_improve.review_verify_status == "passed"
        assert refreshed_improve.review_verify_branch == impl.branch
        assert refreshed_improve.review_verify_head_sha == "abc1234"
        assert refreshed_improve.review_verify_captured_at == captured_at
        assert refreshed_improve.review_verify_artifact_file is None
        artifacts = store.list_artifacts(improve.id, kind="verify_command_output")
        assert len(artifacts) == 1
        assert artifacts[0].producer == "review_verify"
        assert artifacts[0].status == "passed"
        assert artifacts[0].metadata == {
            "reviewed_base_sha": "cafebabe",
            "reviewed_branch": impl.branch,
            "reviewed_head_sha": "abc1234",
            "working_directory": None,
        }
        assert (tmp_path / artifacts[0].path).exists() is False
        reviews = [task for task in store.get_all() if task.task_type == "review" and task.depends_on == impl.id]
        assert len(reviews) == 1

        lifecycle_git = Mock()
        lifecycle_git.can_merge.return_value = True
        lifecycle_git.is_merged.return_value = False
        lifecycle_git.branch_exists.return_value = True
        lifecycle_git.ref_exists.return_value = False
        lifecycle_git.rev_parse_if_exists.side_effect = lambda ref: {"main": "cafebabe", impl.branch: "abc1234"}.get(ref)
        lifecycle_git.is_ancestor.return_value = False
        lifecycle_git.count_commits_behind_checked.return_value = 0
        lifecycle_git.count_commits_ahead_checked.return_value = 1
        lifecycle_git.get_diff_name_status.return_value = ""
        lifecycle_git.resolve_fresh_merge_source.side_effect = lambda branch: ResolvedMergeSourceRef(branch)

        action = evaluate_advance_rules(config, store, lifecycle_git, refreshed_impl, "main")
        assert action["type"] == "merge"

        output = capsys.readouterr().out
        assert "cleared verify-origin blocker from persisted passing no-op improve verify evidence" in output

    def test_post_complete_noop_improve_persists_fresh_failed_verify_evidence_without_clearing_review(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with verify-only review blocker", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/noop-verify-only-failed"
        store.update(impl)
        assert impl.id is not None

        review = store.add(
            prompt="Review before no-op improve",
            task_type="review",
            depends_on=impl.id,
        )
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = (
            "## Summary\n\n- Implementation is aligned; verify failed.\n\n"
            "## Blockers\n\n"
            "### B1 verify_command failure: mypy error\n"
            "Evidence: verify_command failed with exit status 1.\n"
            "Impact: autonomous verify fails.\n"
            "Required fix: rerun verify_command on the current tip.\n"
            "Required tests: rerun verify_command.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        review.review_verify_status = "failed"
        review.review_verify_branch = impl.branch
        review.review_verify_head_sha = "abc1234"
        store.update(review)

        improve = store.add(
            prompt="No-op improve after verify-only review",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
            create_review=True,
        )
        improve.status = "completed"
        improve.branch = impl.branch
        store.update(improve)

        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\n"
            "verify_command: uv run pytest tests/ -q\n",
            encoding="utf-8",
        )
        config = Config.load(tmp_path)
        config.log_path.mkdir(parents=True, exist_ok=True)
        worktree_git = Mock(spec=Git)
        worktree_git.repo_dir = tmp_path
        worktree_git.default_branch.return_value = "main"
        worktree_git.rev_parse_if_exists.return_value = "abc1234"

        captured_at = review.completed_at + timedelta(seconds=2)

        with (
            patch(
                "gza.runner.compute_improve_changed_diff",
                return_value=ImproveDiffResult(changed_diff=False, detail="no (no tracked improve changes)"),
            ),
            patch(
                "gza.runner._capture_noop_improve_review_verify_result",
                wraps=_capture_noop_improve_review_verify_result,
            ) as capture_verify,
            patch(
                "gza.runner._run_review_verify_command",
                return_value=ReviewVerifyResult(
                    command=config.verify_command,
                    status="failed",
                    exit_status="1",
                    captured_at=captured_at,
                    reviewed_branch=impl.branch,
                    reviewed_head_sha="abc1234",
                    reviewed_base_sha="cafebabe",
                    failure="pytest failed",
                ),
            ),
            patch("gza.runner._resolve_review_verify_base_sha", return_value="cafebabe"),
            patch("gza.runner.sync_task_branch_if_live_pr") as sync_branch,
            patch("gza.runner._create_and_run_review_task") as run_review,
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                improve,
                config,
                store,
                worktree_git,
                improve.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            )

        assert rc == 0
        capture_verify.assert_called_once()
        sync_branch.assert_not_called()
        run_review.assert_not_called()

        refreshed_impl = store.get(impl.id)
        assert refreshed_impl is not None
        assert refreshed_impl.review_cleared_at is None
        refreshed_improve = store.get(improve.id)
        assert refreshed_improve is not None
        assert refreshed_improve.review_verify_status == "failed"
        assert refreshed_improve.review_verify_branch == impl.branch
        assert refreshed_improve.review_verify_head_sha == "abc1234"
        assert refreshed_improve.review_verify_captured_at == captured_at
        output = capsys.readouterr().out
        assert "cleared verify-origin blocker from persisted passing no-op improve verify evidence" not in output

    @pytest.mark.parametrize(
        ("label", "seed_evidence"),
        [
            ("absent", lambda improve, review, impl: None),
            (
                "captured_before_review_completed",
                lambda improve, review, impl: (
                    setattr(improve, "review_verify_status", "passed"),
                    setattr(improve, "review_verify_branch", impl.branch),
                    setattr(improve, "review_verify_head_sha", "abc1234"),
                    setattr(improve, "review_verify_captured_at", review.completed_at - timedelta(seconds=1)),
                ),
            ),
            (
                "branch_mismatch",
                lambda improve, review, impl: (
                    setattr(improve, "review_verify_status", "passed"),
                    setattr(improve, "review_verify_branch", "feature/other-branch"),
                    setattr(improve, "review_verify_head_sha", "abc1234"),
                    setattr(improve, "review_verify_captured_at", review.completed_at + timedelta(seconds=1)),
                ),
            ),
            (
                "head_sha_mismatch",
                lambda improve, review, impl: (
                    setattr(improve, "review_verify_status", "passed"),
                    setattr(improve, "review_verify_branch", impl.branch),
                    setattr(improve, "review_verify_head_sha", "deadbeef"),
                    setattr(improve, "review_verify_captured_at", review.completed_at + timedelta(seconds=1)),
                ),
            ),
        ],
    )
    def test_post_complete_noop_improve_recaptures_when_passing_verify_evidence_is_not_current(
        self,
        tmp_path: Path,
        label: str,
        seed_evidence,
    ) -> None:
        db_path = tmp_path / f"{label}.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with verify-only review blocker", task_type="implement")
        impl.status = "completed"
        impl.branch = f"feature/noop-verify-only-{label}"
        store.update(impl)
        assert impl.id is not None

        review = store.add(
            prompt="Review before no-op improve",
            task_type="review",
            depends_on=impl.id,
        )
        review.status = "completed"
        review.completed_at = datetime(2026, 6, 1, 18, 0, tzinfo=UTC)
        review.output_content = (
            "## Summary\n\n- Implementation is aligned; verify failed.\n\n"
            "## Blockers\n\n"
            "### B1 verify_command failure: mypy error\n"
            "Evidence: verify_command failed with exit status 1.\n"
            "Impact: autonomous verify fails.\n"
            "Required fix: rerun verify_command on the current tip.\n"
            "Required tests: rerun verify_command.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        review.review_verify_status = "failed"
        review.review_verify_branch = impl.branch
        review.review_verify_head_sha = "abc1234"
        store.update(review)

        improve = store.add(
            prompt="No-op improve after verify-only review",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
            create_review=True,
        )
        improve.status = "completed"
        improve.branch = impl.branch
        seed_evidence(improve, review, impl)
        store.update(improve)

        config = self._make_config(tmp_path)
        worktree_git = Mock(spec=Git)
        worktree_git.rev_parse_if_exists.return_value = "abc1234"

        with (
            patch(
                "gza.runner.compute_improve_changed_diff",
                return_value=ImproveDiffResult(changed_diff=False, detail="no (no tracked improve changes)"),
            ),
            patch(
                "gza.runner._capture_noop_improve_review_verify_result",
                wraps=_capture_noop_improve_review_verify_result,
            ) as capture_verify,
            patch("gza.runner._run_review_verify_command") as run_verify,
            patch("gza.runner.sync_task_branch_if_live_pr") as sync_branch,
            patch("gza.runner._create_and_run_review_task") as run_review,
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                improve,
                config,
                store,
                worktree_git,
                improve.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            )

        assert rc == 0
        capture_verify.assert_called_once()
        sync_branch.assert_not_called()
        run_review.assert_not_called()

        refreshed_impl = store.get(impl.id)
        assert refreshed_impl is not None
        assert refreshed_impl.review_cleared_at is None

    def test_post_complete_noop_improve_does_not_clear_verify_origin_review_when_review_fail_head_is_stale(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with stale verify-origin review blocker", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/noop-verify-only-stale-review-head"
        store.update(impl)
        assert impl.id is not None

        review = store.add(
            prompt="Review before no-op improve",
            task_type="review",
            depends_on=impl.id,
        )
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = (
            "## Summary\n\n- Review still claims the code path looks wrong.\n\n"
            "## Blockers\n\n"
            "### B1 Missing normalization guard\n"
            "Evidence: src/gza/foo.py:10-12 looks unsafe.\n"
            "Impact: malformed provider responses may still bubble through.\n"
            "Required fix: normalize the failure path, then rerun verify_command.\n"
            "Required tests: rerun verify_command.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        review.review_verify_status = "failed"
        review.review_verify_branch = impl.branch
        review.review_verify_head_sha = "oldsha"
        store.update(review)

        improve = store.add(
            prompt="No-op improve after verify-only review",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
            create_review=True,
        )
        improve.status = "completed"
        improve.branch = impl.branch
        improve.review_verify_status = "passed"
        improve.review_verify_branch = impl.branch
        improve.review_verify_head_sha = "newsha"
        improve.review_verify_captured_at = review.completed_at + timedelta(seconds=1)
        store.update(improve)

        config = self._make_config(tmp_path)
        worktree_git = Mock(spec=Git)
        worktree_git.rev_parse_if_exists.return_value = "newsha"

        with (
            patch(
                "gza.runner.compute_improve_changed_diff",
                return_value=ImproveDiffResult(changed_diff=False, detail="no (no tracked improve changes)"),
            ),
            patch(
                "gza.runner._capture_noop_improve_review_verify_result",
                wraps=_capture_noop_improve_review_verify_result,
            ) as capture_verify,
            patch("gza.runner._run_review_verify_command") as run_verify,
            patch("gza.runner.sync_task_branch_if_live_pr") as sync_branch,
            patch("gza.runner._create_and_run_review_task") as run_review,
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                improve,
                config,
                store,
                worktree_git,
                improve.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            )

        assert rc == 0
        capture_verify.assert_called_once()
        run_verify.assert_not_called()
        sync_branch.assert_not_called()
        run_review.assert_not_called()

        refreshed_impl = store.get(impl.id)
        assert refreshed_impl is not None
        assert refreshed_impl.review_cleared_at is None
        output = capsys.readouterr().out
        assert "cleared verify-origin blocker from persisted passing no-op improve verify evidence" not in output

    def test_post_complete_noop_improve_does_not_clear_substantive_review_block(
        self,
        tmp_path: Path,
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with substantive review blocker", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/noop-substantive-stays-blocked"
        store.update(impl)
        assert impl.id is not None

        review = store.add(
            prompt="Review before no-op improve",
            task_type="review",
            depends_on=impl.id,
        )
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = (
            "## Summary\n\n- Verify failed because the guard is missing.\n\n"
            "## Blockers\n\n"
            "### B1 Missing empty-input guard\n"
            "Evidence: src/gza/foo.py:10-12 indexes the first item before validating input.\n"
            "Impact: empty selections still raise IndexError.\n"
            "Required fix: return early when the selection is empty, then rerun verify_command.\n"
            "Required tests: add an empty-selection regression and rerun verify_command.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        review.review_verify_status = "passed"
        review.review_verify_branch = impl.branch
        review.review_verify_head_sha = "abc1234"
        store.update(review)

        improve = store.add(
            prompt="No-op improve after substantive review",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
            create_review=True,
        )
        improve.status = "completed"
        improve.branch = impl.branch
        store.update(improve)

        config = self._make_config(tmp_path)
        worktree_git = Mock(spec=Git)

        with (
            patch(
                "gza.runner.compute_improve_changed_diff",
                return_value=ImproveDiffResult(changed_diff=False, detail="no (no tracked improve changes)"),
            ),
            patch("gza.runner.sync_task_branch_if_live_pr") as sync_branch,
            patch("gza.runner._create_and_run_review_task") as run_review,
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                improve,
                config,
                store,
                worktree_git,
                improve.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            )

        assert rc == 0
        sync_branch.assert_not_called()
        run_review.assert_not_called()

        refreshed_impl = store.get(impl.id)
        assert refreshed_impl is not None
        assert refreshed_impl.review_cleared_at is None

    def test_capture_noop_improve_review_verify_result_skips_non_verify_only_noop_improve(
        self,
        tmp_path: Path,
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with substantive review blocker", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/noop-substantive-no-recapture"
        store.update(impl)
        assert impl.id is not None

        review = store.add(
            prompt="Review before no-op improve",
            task_type="review",
            depends_on=impl.id,
        )
        review.status = "completed"
        review.completed_at = datetime(2026, 6, 2, 10, 0, tzinfo=UTC)
        review.output_content = (
            "## Summary\n\n- Verify passed, but the code issue remains.\n\n"
            "## Blockers\n\n"
            "### B1 Missing empty-input guard\n"
            "Evidence: src/gza/foo.py:10-12 indexes the first item before validating input.\n"
            "Impact: empty selections still raise IndexError.\n"
            "Required fix: return early when the selection is empty.\n"
            "Required tests: add an empty-selection regression.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        review.review_verify_status = "passed"
        review.review_verify_branch = impl.branch
        review.review_verify_head_sha = "abc1234"
        store.update(review)

        improve = store.add(
            prompt="No-op improve after substantive review",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
            create_review=True,
        )
        improve.status = "completed"
        improve.branch = impl.branch
        store.update(improve)

        config = self._make_config(tmp_path)
        config.verify_command = "uv run pytest tests/ -q"
        worktree_git = Mock(spec=Git)
        worktree_git.rev_parse_if_exists.return_value = "abc1234"

        with patch("gza.runner._run_review_verify_command") as run_verify:
            result = _capture_noop_improve_review_verify_result(
                config=config,
                store=store,
                task=improve,
                worktree_git=worktree_git,
                branch_name=impl.branch,
            )

        assert result is None
        run_verify.assert_not_called()

        refreshed_improve = store.get(improve.id)
        assert refreshed_improve is not None
        assert refreshed_improve.review_verify_status is None

    def test_post_complete_noop_improve_does_not_clear_mixed_review_block_even_after_passing_verify_recapture(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with mixed review blocker", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/noop-mixed-review-stays-parked"
        store.update(impl)
        assert impl.id is not None

        review = store.add(
            prompt="Review before no-op improve",
            task_type="review",
            depends_on=impl.id,
        )
        review.status = "completed"
        review.completed_at = datetime(2026, 6, 2, 10, 0, tzinfo=UTC)
        review.output_content = (
            "## Summary\n\n- verify_command failed, and the code issue remains.\n\n"
            "## Blockers\n\n"
            "### B1 Missing empty-input guard\n"
            "Evidence: src/gza/foo.py:10-12 indexes the first item before validating input.\n"
            "Impact: empty selections still raise IndexError.\n"
            "Required fix: return early when the selection is empty, then rerun verify_command.\n"
            "Required tests: add an empty-selection regression and rerun verify_command.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        review.review_verify_status = "failed"
        review.review_verify_branch = impl.branch
        review.review_verify_head_sha = "abc1234"
        store.update(review)

        improve = store.add(
            prompt="No-op improve after mixed review",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
            create_review=True,
        )
        improve.status = "completed"
        improve.branch = impl.branch
        improve.slug = "20260618-noop-mixed-review"
        store.update(improve)

        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\n"
            "verify_command: uv run pytest tests/ -q\n",
            encoding="utf-8",
        )
        config = Config.load(tmp_path)
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.verify_command = "uv run pytest tests/ -q"
        worktree_git = Mock(spec=Git)
        worktree_git.repo_dir = tmp_path
        worktree_git.default_branch.return_value = "main"
        worktree_git.rev_parse_if_exists.return_value = "abc1234"

        with (
            patch(
                "gza.runner.compute_improve_changed_diff",
                return_value=ImproveDiffResult(changed_diff=False, detail="no (no tracked improve changes)"),
            ),
            patch("gza.runner._capture_noop_improve_review_verify_result", wraps=_capture_noop_improve_review_verify_result) as capture_verify,
            patch("gza.runner._run_review_verify_command") as run_verify,
            patch("gza.runner.sync_task_branch_if_live_pr") as sync_branch,
            patch("gza.runner._create_and_run_review_task") as run_review,
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                improve,
                config,
                store,
                worktree_git,
                improve.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            )

        assert rc == 0
        capture_verify.assert_called_once()
        run_verify.assert_not_called()
        sync_branch.assert_not_called()
        run_review.assert_not_called()

        refreshed_impl = store.get(impl.id)
        assert refreshed_impl is not None
        assert refreshed_impl.review_cleared_at is None

        refreshed_improve = store.get(improve.id)
        assert refreshed_improve is not None
        assert refreshed_improve.review_verify_status is None

        lifecycle_git = Mock()
        lifecycle_git.can_merge.return_value = True
        lifecycle_git.is_merged.return_value = False
        lifecycle_git.branch_exists.return_value = True
        lifecycle_git.ref_exists.return_value = False
        lifecycle_git.rev_parse_if_exists.side_effect = lambda ref: {"main": "cafebabe", impl.branch: "abc1234"}.get(ref)
        lifecycle_git.is_ancestor.return_value = False
        lifecycle_git.count_commits_behind_checked.return_value = 0
        lifecycle_git.count_commits_ahead_checked.return_value = 1
        lifecycle_git.get_diff_name_status.return_value = ""
        lifecycle_git.resolve_fresh_merge_source.side_effect = lambda branch: ResolvedMergeSourceRef(branch)

        action = evaluate_advance_rules(config, store, lifecycle_git, refreshed_impl, "main")
        assert action["type"] == "needs_discussion"
        assert action["needs_attention_reason"] == "improve-no-op"

        output = capsys.readouterr().out
        assert "cleared verify-origin blocker from persisted passing no-op improve verify evidence" not in output

    def test_post_complete_noop_improve_preserves_impl_review_state_timestamp(
        self,
        tmp_path: Path,
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with review", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/noop-preserve-clear"
        impl.review_cleared_at = datetime(2026, 5, 11, 9, 30, tzinfo=UTC)
        store.update(impl)
        assert impl.id is not None

        review = store.add(prompt="Review before improve", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        improve = store.add(
            prompt="Improve without tracked diff change",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
        )
        improve.status = "completed"
        improve.branch = impl.branch
        store.update(improve)

        config = self._make_config(tmp_path)
        worktree_git = Mock(spec=Git)

        with (
            patch(
                "gza.runner.compute_improve_changed_diff",
                return_value=ImproveDiffResult(changed_diff=False, detail="no (no tracked improve changes)"),
            ),
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                improve,
                config,
                store,
                worktree_git,
                improve.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            )

        assert rc == 0
        refreshed_impl = store.get(impl.id)
        assert refreshed_impl is not None
        assert refreshed_impl.review_cleared_at == datetime(2026, 5, 11, 9, 30, tzinfo=UTC)

    def test_post_complete_noop_improve_warning_omits_removed_allow_noop_tag(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        legacy_tag = "allow" + "-noop-improve"
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with review", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/noop-opt-out"
        store.update(impl)
        assert impl.id is not None

        review = store.add(prompt="Review before improve", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        improve = store.add(
            prompt="Improve without tracked diff change",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
        )
        improve.status = "completed"
        improve.branch = impl.branch
        store.update(improve)

        config = self._make_config(tmp_path)
        worktree_git = Mock(spec=Git)

        with (
            patch(
                "gza.runner.compute_improve_changed_diff",
                return_value=ImproveDiffResult(changed_diff=False, detail="no (no tracked improve changes)"),
            ),
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                improve,
                config,
                store,
                worktree_git,
                improve.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            )

        assert rc == 0
        output = capsys.readouterr().out
        assert "Warning: Improve completed with no tracked diff change." in output
        assert legacy_tag not in output

    def test_post_complete_improve_gh_unavailable_still_runs_auto_review_without_noise(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ):
        """Missing GitHub CLI should preserve historical improve auto-review behavior."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with review", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/improve-no-gh"
        store.update(impl)

        improve = store.add(
            prompt="Improve with review",
            task_type="improve",
            based_on=impl.id,
            same_branch=True,
            create_review=True,
        )
        improve.status = "completed"
        improve.branch = impl.branch
        store.update(improve)

        config = self._make_config(tmp_path)
        worktree_git = Mock(spec=Git)

        with (
            patch("gza.runner.sync_task_branch_if_live_pr", return_value=Mock(ok=False, status="gh_unavailable")),
            patch("gza.runner._create_and_run_review_task", return_value=9) as run_review,
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                improve,
                config,
                store,
                worktree_git,
                improve.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            )

        assert rc == 9
        run_review.assert_called_once_with(improve, config, store)
        output = capsys.readouterr().out
        assert "GitHub CLI is not available" not in output
        assert "Skipping auto-review" not in output

    def test_post_complete_improve_lookup_failure_warns_but_still_runs_auto_review(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ):
        """Unconfirmed PR lookup failures should no longer block improve follow-up review."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement with review", task_type="implement")
        impl.status = "completed"
        impl.branch = "feature/improve-lookup-failure"
        store.update(impl)

        improve = store.add(
            prompt="Improve with review",
            task_type="improve",
            based_on=impl.id,
            same_branch=True,
            create_review=True,
        )
        improve.status = "completed"
        improve.branch = impl.branch
        store.update(improve)

        config = self._make_config(tmp_path)
        worktree_git = Mock(spec=Git)

        with (
            patch(
                "gza.runner.sync_task_branch_if_live_pr",
                return_value=Mock(ok=False, status="lookup_failed", error="auth failed"),
            ),
            patch("gza.runner._create_and_run_review_task", return_value=7) as run_review,
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                improve,
                config,
                store,
                worktree_git,
                improve.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            )

        assert rc == 7
        run_review.assert_called_once_with(improve, config, store)
        output = capsys.readouterr().out
        assert "could not look up a live PR" in output
        assert "Continuing with auto-review without PR sync." in output
        assert "Skipping auto-review" not in output
        store.update(improve)

        config = self._make_config(tmp_path)
        worktree_git = Mock(spec=Git)

        with (
            patch("gza.runner.sync_task_branch_if_live_pr", return_value=Mock(ok=False, status="gh_unavailable")),
            patch("gza.runner._create_and_run_review_task", return_value=9) as run_review,
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _post_complete_code_task(
                improve,
                config,
                store,
                worktree_git,
                improve.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            )

        assert rc == 9
        run_review.assert_called_once_with(improve, config, store)
        output = capsys.readouterr().out
        assert "GitHub CLI is not available" not in output
        assert "Skipping auto-review" not in output

    def test_run_uses_persisted_create_pr_intent_without_work_flag(self, tmp_path: Path):
        """Stored task create_pr intent should drive the runner even without `work --pr`."""
        (tmp_path / "gza.yaml").write_text("project_name: testproject\n")
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add(prompt="Implement with persisted PR intent", task_type="implement", create_pr=True)

        with (
            patch("gza.runner.load_dotenv"),
            patch("gza.runner.backup_database"),
            patch("gza.runner._run_inner", return_value=0) as mock_run_inner,
            patch("gza.runner.get_provider") as mock_get_provider,
        ):
            mock_provider = Mock()
            mock_provider.name = "Claude"
            mock_provider.supports_interactive_foreground = True
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_get_provider.return_value = mock_provider

            result = run(config, task_id=task.id)

        assert result == 0
        assert mock_run_inner.call_args.kwargs["create_pr"] is True

    def test_complete_code_task_chained_improve_updates_root_implementation_state(self, tmp_path: Path):
        """Chained improve completion should clear review state on the implementation root, not the prior improve."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement root", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "feature/root-impl"
        impl_task.merge_status = "merged"
        store.update(impl_task)
        assert impl_task.id is not None

        store.add_comment(impl_task.id, "Old implementation comment", source="direct")

        improve1 = store.add(
            prompt="Improve once",
            task_type="improve",
            based_on=impl_task.id,
            same_branch=True,
        )
        assert improve1.id is not None
        store.add_comment(improve1.id, "Improve comment should remain unresolved", source="direct")

        improve2 = store.add(
            prompt="Improve retry",
            task_type="improve",
            based_on=improve1.id,
            same_branch=True,
        )
        improve2.slug = "20260420-improve-chain"
        store.mark_in_progress(improve2)

        store.add_comment(impl_task.id, "New implementation comment", source="direct")

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{improve2.slug}.log"
        log_file.write_text("")

        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = {("M", "src/impl.py")}
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_numstat.return_value = "1\t0\tsrc/impl.py\n"

        summary_dir = tmp_path / ".gza" / "summaries"
        summary_path = summary_dir / f"{improve2.slug}.md"
        worktree_summary_path = tmp_path / "worktree" / ".gza" / "summaries" / f"{improve2.slug}.md"
        worktree_summary_path.parent.mkdir(parents=True, exist_ok=True)
        worktree_summary_path.write_text("summary")

        with (
            patch("gza.runner._squash_wip_commits"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _complete_code_task(
                improve2,
                config,
                store,
                worktree_git,
                log_file,
                "feature/root-impl",
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
                0,
                pre_run_status=set(),
                worktree_summary_path=worktree_summary_path,
                summary_path=summary_path,
                summary_dir=summary_dir,
            )

        assert rc == 0
        refreshed_impl = store.get(impl_task.id)
        assert refreshed_impl is not None
        assert refreshed_impl.review_cleared_at is not None
        assert refreshed_impl.merge_status == "unmerged"
        unresolved_impl_comments = store.get_comments(impl_task.id, unresolved_only=True)
        assert [comment.content for comment in unresolved_impl_comments] == ["New implementation comment"]

        refreshed_improve2 = store.get(improve2.id)
        assert refreshed_improve2 is not None
        assert refreshed_improve2.merge_status is None

        refreshed_improve1 = store.get(improve1.id)
        assert refreshed_improve1 is not None
        assert refreshed_improve1.review_cleared_at is None
        unresolved_improve_comments = store.get_comments(improve1.id, unresolved_only=True)
        assert [comment.content for comment in unresolved_improve_comments] == [
            "Improve comment should remain unresolved"
        ]

    def test_complete_code_task_fix_with_commit_delta_creates_follow_up_review(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        """Shared completion path should create a follow-up review for code-changing fix runs."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement with churn", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "feature/fix-target"
        impl_task.merge_status = "merged"
        store.update(impl_task)
        assert impl_task.id is not None

        prior_review = store.add(
            prompt="Review before fix",
            task_type="review",
            depends_on=impl_task.id,
        )
        prior_review.status = "completed"
        prior_review.completed_at = datetime.now(UTC)
        store.update(prior_review)
        assert prior_review.completed_at is not None

        fix_task = store.add(
            prompt="Fix the churn",
            task_type="fix",
            based_on=impl_task.id,
            same_branch=True,
        )
        fix_task.slug = "20260420-fix-follow-up"
        store.mark_in_progress(fix_task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{fix_task.slug}.log"
        log_file.write_text("")

        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = {("M", "src/fix.py")}
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_numstat.return_value = "1\t0\tsrc/fix.py\n"
        worktree_git.count_commits_ahead.return_value = 3

        summary_dir = tmp_path / ".gza" / "summaries"
        summary_path = summary_dir / f"{fix_task.slug}.md"
        worktree_summary_path = tmp_path / "worktree" / ".gza" / "summaries" / f"{fix_task.slug}.md"
        worktree_summary_path.parent.mkdir(parents=True, exist_ok=True)
        worktree_summary_path.write_text("summary")

        with (
            patch("gza.runner._squash_wip_commits"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _complete_code_task(
                fix_task,
                config,
                store,
                worktree_git,
                log_file,
                "feature/fix-target",
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
                0,
                pre_run_status=set(),
                worktree_summary_path=worktree_summary_path,
                summary_path=summary_path,
                summary_dir=summary_dir,
                fix_commits_ahead_before_run=2,
                fix_default_branch="main",
            )

        assert rc == 0
        output = capsys.readouterr().out
        assert "Created follow-up review task" in output
        refreshed_impl = store.get(impl_task.id)
        assert refreshed_impl is not None
        assert refreshed_impl.merge_status == "unmerged"
        assert refreshed_impl.review_cleared_at is not None
        assert refreshed_impl.review_cleared_at >= prior_review.completed_at
        reviews = [t for t in store.get_all() if t.task_type == "review" and t.depends_on == impl_task.id]
        assert len(reviews) == 2

    def test_complete_code_task_fix_without_commit_delta_skips_follow_up_review(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        """Shared completion path should not create a follow-up review when fix adds no commits."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement with churn", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "feature/fix-target"
        impl_task.merge_status = "merged"
        store.update(impl_task)
        assert impl_task.id is not None

        prior_review = store.add(
            prompt="Review before fix",
            task_type="review",
            depends_on=impl_task.id,
        )
        prior_review.status = "completed"
        prior_review.completed_at = datetime.now(UTC)
        store.update(prior_review)
        assert prior_review.id is not None

        impl_unit = store.get_or_create_merge_unit_for_task(impl_task)
        assert impl_unit is not None
        assert impl_unit.target_branch == "main"
        store.set_merge_unit_state(impl_unit.id, "merged")

        fix_task = store.add(
            prompt="Fix the churn",
            task_type="fix",
            based_on=impl_task.id,
            same_branch=True,
        )
        fix_task.slug = "20260420-fix-no-delta"
        store.mark_in_progress(fix_task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{fix_task.slug}.log"
        log_file.write_text("")

        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = {("M", "src/fix.py")}
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_numstat.return_value = "1\t0\tsrc/fix.py\n"
        worktree_git.count_commits_ahead.return_value = 2

        summary_dir = tmp_path / ".gza" / "summaries"
        summary_path = summary_dir / f"{fix_task.slug}.md"
        worktree_summary_path = tmp_path / "worktree" / ".gza" / "summaries" / f"{fix_task.slug}.md"
        worktree_summary_path.parent.mkdir(parents=True, exist_ok=True)
        worktree_summary_path.write_text("summary")

        with (
            patch("gza.runner._squash_wip_commits"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _complete_code_task(
                fix_task,
                config,
                store,
                worktree_git,
                log_file,
                "feature/fix-target",
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
                0,
                pre_run_status=set(),
                worktree_summary_path=worktree_summary_path,
                summary_path=summary_path,
                summary_dir=summary_dir,
                fix_commits_ahead_before_run=2,
                fix_default_branch="main",
            )

        assert rc == 0
        output = capsys.readouterr().out
        assert "Fix completed without new commits; no follow-up review was auto-created." in output
        refreshed_impl = store.get(impl_task.id)
        assert refreshed_impl is not None
        assert refreshed_impl.merge_status == "merged"
        assert refreshed_impl.review_cleared_at is None
        refreshed_unit = store.get_merge_unit(impl_unit.id)
        assert refreshed_unit is not None
        assert refreshed_unit.state == "merged"
        assert refreshed_unit.merged_by_task_id == impl_task.id
        reviews = [t for t in store.get_all() if t.task_type == "review" and t.depends_on == impl_task.id]
        assert len(reviews) == 1

    def test_complete_code_task_fix_probe_failure_is_reported_and_skips_auto_review(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        """Fix commit-delta probe failures should be surfaced and not silently create reviews."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement with churn", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "feature/fix-target"
        store.update(impl_task)

        fix_task = store.add(
            prompt="Fix the churn",
            task_type="fix",
            based_on=impl_task.id,
            same_branch=True,
        )
        fix_task.slug = "20260420-fix-probe-fail"
        store.mark_in_progress(fix_task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{fix_task.slug}.log"
        log_file.write_text("")

        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = {("M", "src/fix.py")}
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_numstat.return_value = "1\t0\tsrc/fix.py\n"
        worktree_git.count_commits_ahead.side_effect = GitError("probe failed")

        summary_dir = tmp_path / ".gza" / "summaries"
        summary_path = summary_dir / f"{fix_task.slug}.md"
        worktree_summary_path = tmp_path / "worktree" / ".gza" / "summaries" / f"{fix_task.slug}.md"
        worktree_summary_path.parent.mkdir(parents=True, exist_ok=True)
        worktree_summary_path.write_text("summary")

        with (
            patch("gza.runner._squash_wip_commits"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _complete_code_task(
                fix_task,
                config,
                store,
                worktree_git,
                log_file,
                "feature/fix-target",
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
                0,
                pre_run_status=set(),
                worktree_summary_path=worktree_summary_path,
                summary_path=summary_path,
                summary_dir=summary_dir,
                fix_commits_ahead_before_run=2,
                fix_default_branch="main",
            )

        assert rc == 0
        output = capsys.readouterr().out
        assert "Warning: Could not determine fix commit delta: probe failed" in output
        assert "Warning: Could not determine whether the fix run changed code" in output
        reviews = [t for t in store.get_all() if t.task_type == "review" and t.depends_on == impl_task.id]
        assert reviews == []

    def test_complete_code_task_fix_unknown_before_baseline_skips_auto_review(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        """Unknown pre-run baseline must not be coerced to zero for follow-up review creation."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement with existing ahead commits", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "feature/fix-target"
        impl_task.merge_status = "merged"
        store.update(impl_task)
        assert impl_task.id is not None

        prior_review = store.add(
            prompt="Review before fix",
            task_type="review",
            depends_on=impl_task.id,
        )
        prior_review.status = "completed"
        prior_review.completed_at = datetime.now(UTC)
        store.update(prior_review)

        fix_task = store.add(
            prompt="Fix baseline probe edge case",
            task_type="fix",
            based_on=impl_task.id,
            same_branch=True,
        )
        fix_task.slug = "20260422-fix-unknown-baseline"
        store.mark_in_progress(fix_task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{fix_task.slug}.log"
        log_file.write_text("")

        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = {("M", "src/fix.py")}
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_numstat.return_value = "1\t0\tsrc/fix.py\n"
        # Post-run probe succeeds, but pre-run baseline is unknown.
        worktree_git.count_commits_ahead.return_value = 3

        summary_dir = tmp_path / ".gza" / "summaries"
        summary_path = summary_dir / f"{fix_task.slug}.md"
        worktree_summary_path = tmp_path / "worktree" / ".gza" / "summaries" / f"{fix_task.slug}.md"
        worktree_summary_path.parent.mkdir(parents=True, exist_ok=True)
        worktree_summary_path.write_text("summary")

        with (
            patch("gza.runner._squash_wip_commits"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = _complete_code_task(
                fix_task,
                config,
                store,
                worktree_git,
                log_file,
                "feature/fix-target",
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
                0,
                pre_run_status=set(),
                worktree_summary_path=worktree_summary_path,
                summary_path=summary_path,
                summary_dir=summary_dir,
                fix_commits_ahead_before_run=None,
                fix_default_branch="main",
            )

        assert rc == 0
        output = capsys.readouterr().out
        assert "Warning: Could not determine fix commit baseline before run" in output
        assert "Warning: Could not determine whether the fix run changed code" in output

        refreshed_impl = store.get(impl_task.id)
        assert refreshed_impl is not None
        assert refreshed_impl.merge_status == "merged"
        assert refreshed_impl.review_cleared_at is None
        reviews = [t for t in store.get_all() if t.task_type == "review" and t.depends_on == impl_task.id]
        assert len(reviews) == 1

    def test_complete_code_task_fix_duplicate_follow_up_review_reports_existing_review(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ):
        """Fix follow-up handoff must surface duplicate review reuse instead of silently returning."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implementation for duplicate review", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "feature/fix-target"
        store.update(impl_task)
        assert impl_task.id is not None

        fix_task = store.add(
            prompt="Fix duplicate review path",
            task_type="fix",
            based_on=impl_task.id,
            same_branch=True,
        )
        fix_task.slug = "20260422-fix-duplicate-followup"
        store.mark_in_progress(fix_task)

        existing_review = store.add(
            prompt="Existing pending review",
            task_type="review",
            depends_on=impl_task.id,
        )
        store.mark_in_progress(existing_review)

        config = self._make_config(tmp_path)
        log_file = tmp_path / "logs" / f"{fix_task.slug}.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)
        log_file.write_text("")

        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = {("M", "src/fix.py")}
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_numstat.return_value = "1\t0\tsrc/fix.py\n"
        worktree_git.count_commits_ahead.return_value = 3

        summary_dir = tmp_path / ".gza" / "summaries"
        summary_path = summary_dir / f"{fix_task.slug}.md"
        worktree_summary_path = tmp_path / "worktree" / ".gza" / "summaries" / f"{fix_task.slug}.md"
        worktree_summary_path.parent.mkdir(parents=True, exist_ok=True)
        worktree_summary_path.write_text("summary")

        with (
            patch("gza.runner._squash_wip_commits"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
            patch(
                "gza.runner.create_review_task",
                side_effect=DuplicateReviewError(existing_review),
            ),
        ):
            rc = _complete_code_task(
                fix_task,
                config,
                store,
                worktree_git,
                log_file,
                "feature/fix-target",
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
                0,
                pre_run_status=set(),
                worktree_summary_path=worktree_summary_path,
                summary_path=summary_path,
                summary_dir=summary_dir,
                fix_commits_ahead_before_run=2,
                fix_default_branch="main",
            )

        assert rc == 0
        output = capsys.readouterr().out
        assert f"Follow-up review already exists for implementation {impl_task.id}" in output
        assert f"{existing_review.id} ({existing_review.status})" in output

    def test_complete_code_task_fix_follow_up_review_value_error_is_reported(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ):
        """Fix follow-up handoff must surface review creation errors with next-step guidance."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implementation for value error", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "feature/fix-target"
        store.update(impl_task)
        assert impl_task.id is not None

        fix_task = store.add(
            prompt="Fix value error path",
            task_type="fix",
            based_on=impl_task.id,
            same_branch=True,
        )
        fix_task.slug = "20260422-fix-followup-value-error"
        store.mark_in_progress(fix_task)

        config = self._make_config(tmp_path)
        log_file = tmp_path / "logs" / f"{fix_task.slug}.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)
        log_file.write_text("")

        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = {("M", "src/fix.py")}
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_numstat.return_value = "1\t0\tsrc/fix.py\n"
        worktree_git.count_commits_ahead.return_value = 4

        summary_dir = tmp_path / ".gza" / "summaries"
        summary_path = summary_dir / f"{fix_task.slug}.md"
        worktree_summary_path = tmp_path / "worktree" / ".gza" / "summaries" / f"{fix_task.slug}.md"
        worktree_summary_path.parent.mkdir(parents=True, exist_ok=True)
        worktree_summary_path.write_text("summary")

        with (
            patch("gza.runner._squash_wip_commits"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
            patch(
                "gza.runner.create_review_task",
                side_effect=ValueError("implementation task is not reviewable"),
            ),
        ):
            rc = _complete_code_task(
                fix_task,
                config,
                store,
                worktree_git,
                log_file,
                "feature/fix-target",
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
                0,
                pre_run_status=set(),
                worktree_summary_path=worktree_summary_path,
                summary_path=summary_path,
                summary_dir=summary_dir,
                fix_commits_ahead_before_run=2,
                fix_default_branch="main",
            )

        assert rc == 0
        output = capsys.readouterr().out
        assert f"Warning: Could not auto-create follow-up review for implementation {impl_task.id}" in output
        assert "implementation task is not reviewable" in output
        assert f"uv run gza review {impl_task.id}" in output

    @pytest.mark.parametrize(
        ("failure_mode", "ensure_result"),
        [
            ("gh_unavailable", (False, "gh_unavailable", None)),
            ("create_pr_fails", (False, "create_failed", "create failed")),
        ],
    )
    def test_complete_code_task_work_pr_nonfatal_missing_pr_completes_and_logs_note(
        self,
        tmp_path: Path,
        failure_mode: str,
        ensure_result: tuple[bool, str, str | None],
        capsys: pytest.CaptureFixture[str],
    ):
        """Push-success PR gaps should complete and surface a publication note."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt=f"Implement with review ({failure_mode})", task_type="implement", create_review=True)
        task.slug = f"20260414-impl-review-{failure_mode}"
        store.mark_in_progress(task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{task.slug}.log"
        log_file.write_text("")

        pre_status = set()
        post_status = {("M", "src/foo.py")}
        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = post_status
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_numstat.return_value = "1\t0\tsrc/foo.py\n"
        worktree_git.count_commits_ahead.return_value = 1

        summary_dir = tmp_path / ".gza" / "summaries"
        summary_path = summary_dir / f"{task.slug}.md"
        worktree_summary_path = tmp_path / "worktree" / ".gza" / "summaries" / f"{task.slug}.md"
        worktree_summary_path.parent.mkdir(parents=True, exist_ok=True)
        worktree_summary_path.write_text("summary")

        ok, status, error = ensure_result
        ensure_mock_result = Mock()
        ensure_mock_result.ok = ok
        ensure_mock_result.status = status
        ensure_mock_result.error = error
        ensure_mock_result.pr_url = None

        with (
            patch("gza.runner._squash_wip_commits"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
            patch("gza.runner._create_and_run_review_task", return_value=0) as run_review,
            patch("gza.runner.ensure_task_pr", return_value=ensure_mock_result),
            patch("gza.runner.task_footer") as footer,
        ):
            rc = _complete_code_task(
                task,
                config,
                store,
                worktree_git,
                log_file,
                "feature/review-order",
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
                0,
                pre_run_status=pre_status,
                worktree_summary_path=worktree_summary_path,
                summary_path=summary_path,
                summary_dir=summary_dir,
                create_pr=True,
            )

        assert rc == 0
        footer.assert_called_once()
        run_review.assert_called_once()
        output = capsys.readouterr().out
        assert "completed and branch 'feature/review-order' is published to origin, but PR was not created" in output
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "completed"
        assert refreshed.failure_reason is None
        assert refreshed.output_content == "summary"
        log_text = ops_log_path_for(log_file).read_text()
        assert '"subtype": "pr_publication_note"' in log_text
        assert f'"status": "{status}"' in log_text

    def test_complete_code_task_work_pr_push_failure_marks_branch_unpushable(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ):
        """Push failures should fail the task with BRANCH_UNPUSHABLE and skip auto-review."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement with review (push_fails)", task_type="implement", create_review=True)
        task.slug = "20260414-impl-review-push-fails"
        store.mark_in_progress(task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{task.slug}.log"
        log_file.write_text("")

        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = {("M", "src/foo.py")}
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_numstat.return_value = "1\t0\tsrc/foo.py\n"
        worktree_git.count_commits_ahead.return_value = 1

        summary_dir = tmp_path / ".gza" / "summaries"
        summary_path = summary_dir / f"{task.slug}.md"
        worktree_summary_path = tmp_path / "worktree" / ".gza" / "summaries" / f"{task.slug}.md"
        worktree_summary_path.parent.mkdir(parents=True, exist_ok=True)
        worktree_summary_path.write_text("summary")

        ensure_mock_result = Mock(ok=False, status="push_failed", error="push failed", pr_url=None)

        with (
            patch("gza.runner._squash_wip_commits"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
            patch("gza.runner._create_and_run_review_task") as run_review,
            patch("gza.runner.ensure_task_pr", return_value=ensure_mock_result),
            patch("gza.runner.task_footer") as footer,
        ):
            rc = _complete_code_task(
                task,
                config,
                store,
                worktree_git,
                log_file,
                "feature/review-order",
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
                0,
                pre_run_status=set(),
                worktree_summary_path=worktree_summary_path,
                summary_path=summary_path,
                summary_dir=summary_dir,
                create_pr=True,
            )

        assert rc == 1
        footer.assert_not_called()
        run_review.assert_not_called()
        output = capsys.readouterr().out
        assert "could not be pushed (push_failed): push failed" in output
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == BRANCH_UNPUSHABLE_FAILURE_REASON
        assert refreshed.output_content == "summary"
        log_text = ops_log_path_for(log_file).read_text()
        assert f'Outcome: failed ({BRANCH_UNPUSHABLE_FAILURE_REASON})' in log_text

    def test_run_can_retry_pr_required_failure_via_work_pr(self, tmp_path: Path):
        """`gza work <task> --pr` should recover failed PR_REQUIRED tasks without rerunning provider."""
        (tmp_path / "gza.yaml").write_text(
            "project_name: testproject\n"
            "project_id: default\n"
            "db_path: .gza/gza.db\n"
        )
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add(prompt="Implement with review", task_type="implement", create_review=True)
        task.slug = "20260414-pr-required-retry"
        task.status = "failed"
        task.failure_reason = "PR_REQUIRED"
        task.branch = "feature/retry-pr-required"
        task.log_file = "logs/retry.log"
        task.output_content = "summary"
        task.has_commits = True
        store.update(task)

        with (
            patch("gza.runner.backup_database"),
            patch("gza.runner.load_dotenv"),
            patch(
                "gza.runner._ensure_work_pr_for_completed_code_task",
                return_value=CompletedCodeTaskPrPublicationOutcome(kind="ready", status="created", message="created"),
            ),
            patch("gza.runner._create_and_run_review_task", return_value=0) as run_review,
            patch("gza.runner.task_footer"),
        ):
            rc = run(config, task_id=task.id, create_pr=True)

        assert rc == 0

    def test_run_work_pr_branchless_pr_required_does_not_rewrite_fresh_pr_required(self, tmp_path: Path):
        """Legacy branchless PR_REQUIRED rows should not be re-failed through the compatibility retry path."""
        (tmp_path / "gza.yaml").write_text(
            "project_name: testproject\n"
            "project_id: default\n"
            "db_path: .gza/gza.db\n"
        )
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add(prompt="Implement with review", task_type="implement", create_review=True)
        task.status = "failed"
        task.failure_reason = "PR_REQUIRED"
        task.has_commits = True
        store.update(task)

        with (
            patch("gza.runner.backup_database"),
            patch("gza.runner.load_dotenv"),
        ):
            rc = run(config, task_id=task.id, create_pr=True)

        assert rc == 1
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "PR_REQUIRED"

    def test_run_can_retry_pr_required_failure_via_persisted_create_pr(self, tmp_path: Path):
        """Stored create_pr intent should recover failed PR_REQUIRED tasks without needing `work --pr`."""
        (tmp_path / "gza.yaml").write_text(
            "project_name: testproject\n"
            "project_id: default\n"
            "db_path: .gza/gza.db\n"
        )
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add(
            prompt="Implement with review",
            task_type="implement",
            create_review=True,
            create_pr=True,
        )
        task.slug = "20260414-pr-required-retry-persisted"
        task.status = "failed"
        task.failure_reason = "PR_REQUIRED"
        task.branch = "feature/retry-pr-required-persisted"
        task.log_file = "logs/retry-persisted.log"
        task.output_content = "summary"
        task.has_commits = True
        store.update(task)

        with (
            patch("gza.runner.backup_database"),
            patch("gza.runner.load_dotenv"),
            patch(
                "gza.runner._ensure_work_pr_for_completed_code_task",
                return_value=CompletedCodeTaskPrPublicationOutcome(kind="ready", status="created", message="created"),
            ),
            patch("gza.runner._create_and_run_review_task", return_value=0) as run_review,
            patch("gza.runner.task_footer"),
        ):
            rc = run(config, task_id=task.id)

        assert rc == 0
        run_review.assert_called_once()
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "completed"
        assert refreshed.failure_reason is None

    def test_run_pr_required_retry_for_improve_resolves_parent_comments(self, tmp_path: Path):
        """Improve completion should resolve only unresolved feedback comments on the root implementation task."""
        (tmp_path / "gza.yaml").write_text(
            "project_name: testproject\n"
            "project_id: default\n"
            "db_path: .gza/gza.db\n"
        )
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)

        parent = store.add(prompt="Implement parent", task_type="implement")
        parent.merge_status = "merged"
        store.update(parent)
        assert parent.id is not None
        store.add_comment(parent.id, "Please tighten error handling.", source="direct")
        store.add_comment(parent.id, "Scope note should stay open.", source="direct", kind="review_scope")
        assert store.get_comments(parent.id, unresolved_only=True)

        improve = store.add(
            prompt="Improve parent implementation",
            task_type="improve",
            based_on=parent.id,
            same_branch=True,
        )
        improve.slug = "20260414-retry-improve-pr-required"
        improve.status = "failed"
        improve.failure_reason = "PR_REQUIRED"
        improve.branch = "feature/retry-improve-pr-required"
        improve.log_file = "logs/retry-improve.log"
        improve.output_content = "summary"
        improve.has_commits = True
        store.update(improve)
        assert improve.id is not None

        with (
            patch("gza.runner.Git", return_value=Mock(spec=Git)),
            patch("gza.runner.backup_database"),
            patch("gza.runner.load_dotenv"),
            patch(
                "gza.runner._ensure_work_pr_for_completed_code_task",
                return_value=CompletedCodeTaskPrPublicationOutcome(kind="ready", status="created", message="created"),
            ),
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = run(config, task_id=improve.id, create_pr=True)

        assert rc == 0
        unresolved = store.get_comments(parent.id, unresolved_only=True)
        assert [comment.content for comment in unresolved] == ["Scope note should stay open."]

    def test_run_pr_required_retry_for_improve_only_resolves_comments_in_snapshot(self, tmp_path: Path):
        """Improve completion should leave comments added after improve creation unresolved."""
        (tmp_path / "gza.yaml").write_text(
            "project_name: testproject\n"
            "project_id: default\n"
            "db_path: .gza/gza.db\n"
        )
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)

        parent = store.add(prompt="Implement parent", task_type="implement")
        store.update(parent)
        assert parent.id is not None

        store.add_comment(parent.id, "Old comment in improve snapshot", source="direct")

        improve = store.add(
            prompt="Improve parent implementation",
            task_type="improve",
            based_on=parent.id,
            same_branch=True,
        )
        improve.slug = "20260420-retry-improve-comment-snapshot"
        improve.status = "failed"
        improve.failure_reason = "PR_REQUIRED"
        improve.branch = "feature/retry-improve-comment-snapshot"
        improve.log_file = "logs/retry-improve-snapshot.log"
        improve.output_content = "summary"
        improve.has_commits = True
        store.update(improve)
        assert improve.id is not None

        store.add_comment(parent.id, "New comment after improve creation", source="direct")

        with (
            patch("gza.runner.Git", return_value=Mock(spec=Git)),
            patch("gza.runner.backup_database"),
            patch("gza.runner.load_dotenv"),
            patch(
                "gza.runner._ensure_work_pr_for_completed_code_task",
                return_value=CompletedCodeTaskPrPublicationOutcome(kind="ready", status="created", message="created"),
            ),
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = run(config, task_id=improve.id, create_pr=True)

        assert rc == 0
        unresolved = store.get_comments(parent.id, unresolved_only=True)
        assert [comment.content for comment in unresolved] == ["New comment after improve creation"]

    def test_run_pr_required_retry_for_chained_improve_updates_root_implementation_state(self, tmp_path: Path):
        """PR-required improve retries should apply completion side effects to the implementation root."""
        (tmp_path / "gza.yaml").write_text(
            "project_name: testproject\n"
            "project_id: default\n"
            "db_path: .gza/gza.db\n"
        )
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)

        impl = store.add(prompt="Implement parent", task_type="implement")
        impl.merge_status = "merged"
        store.update(impl)
        assert impl.id is not None
        store.add_comment(impl.id, "Old root comment", source="direct")

        improve1 = store.add(
            prompt="Improve parent implementation",
            task_type="improve",
            based_on=impl.id,
            same_branch=True,
        )
        assert improve1.id is not None
        store.add_comment(improve1.id, "Intermediate improve comment", source="direct")

        improve2 = store.add(
            prompt="Retry improve parent implementation",
            task_type="improve",
            based_on=improve1.id,
            same_branch=True,
        )
        improve2.slug = "20260422-retry-improve-chain-pr-required"
        improve2.status = "failed"
        improve2.failure_reason = "PR_REQUIRED"
        improve2.branch = "feature/retry-improve-chain-pr-required"
        improve2.log_file = "logs/retry-improve-chain.log"
        improve2.output_content = "summary"
        improve2.has_commits = True
        store.update(improve2)
        assert improve2.id is not None

        store.add_comment(impl.id, "New root comment after retry creation", source="direct")

        with (
            patch("gza.runner.Git", return_value=Mock(spec=Git)),
            patch("gza.runner.backup_database"),
            patch("gza.runner.load_dotenv"),
            patch(
                "gza.runner._ensure_work_pr_for_completed_code_task",
                return_value=CompletedCodeTaskPrPublicationOutcome(kind="ready", status="created", message="created"),
            ),
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = run(config, task_id=improve2.id, create_pr=True)

        assert rc == 0

        refreshed_impl = store.get(impl.id)
        assert refreshed_impl is not None
        assert refreshed_impl.review_cleared_at is not None
        assert refreshed_impl.merge_status == "unmerged"
        unresolved_impl = store.get_comments(impl.id, unresolved_only=True)
        assert [comment.content for comment in unresolved_impl] == ["New root comment after retry creation"]

        refreshed_improve1 = store.get(improve1.id)
        assert refreshed_improve1 is not None
        assert refreshed_improve1.review_cleared_at is None
        unresolved_improve1 = store.get_comments(improve1.id, unresolved_only=True)
        assert [comment.content for comment in unresolved_improve1] == ["Intermediate improve comment"]

    def test_run_pr_required_retry_for_rebase_treats_published_head_as_pr_only_retry(self, tmp_path: Path):
        """A published rebase PR retry should skip the stale non-advancing baseline path."""
        (tmp_path / "gza.yaml").write_text(
            "project_name: testproject\n"
            "project_id: default\n"
            "db_path: .gza/gza.db\n"
        )
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)

        parent = store.add(prompt="Implement parent", task_type="implement")
        parent.merge_status = "merged"
        store.update(parent)
        store.clear_review_state(parent.id)

        task = store.add(
            prompt="Rebase parent branch",
            task_type="rebase",
            based_on=parent.id,
            same_branch=True,
            create_pr=True,
        )
        task.slug = "20260516-retry-rebase-pr-required"
        task.status = "failed"
        task.failure_reason = "PR_REQUIRED"
        task.branch = "feat/parent"
        task.log_file = "logs/20260516-retry-rebase-pr-required.log"
        task.output_content = "summary"
        task.diff_files_changed = 0
        task.diff_lines_added = 0
        task.diff_lines_removed = 0
        store.mark_failed(
            task,
            log_file=task.log_file,
            has_commits=True,
            branch=task.branch,
            failure_reason="PR_REQUIRED",
            head_sha="rebased-head",
            base_sha="base-before-pr",
        )
        task.output_content = "summary"
        store.update(task)

        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{task.slug}.log"
        log_file.write_text("")

        git = Mock(spec=Git)
        git.default_branch.return_value = "main"
        git.rev_parse_if_exists.side_effect = lambda ref: {
            task.branch: "rebased-head",
            "main": "base-after-pr-retry",
            f"origin/{task.branch}": "rebased-head",
        }.get(ref)

        with (
            patch("gza.runner.Git", return_value=git),
            patch("gza.runner.backup_database"),
            patch("gza.runner.load_dotenv"),
            patch("gza.runner.publish_rebased_branch") as publish_rebased_branch,
            patch(
                "gza.runner._ensure_work_pr_for_completed_code_task",
                return_value=CompletedCodeTaskPrPublicationOutcome(kind="ready", status="created", message="created"),
            ) as ensure_work_pr,
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = run(config, task_id=task.id, create_pr=True)

        assert rc == 0
        publish_rebased_branch.assert_called_once_with(
            git,
            branch=task.branch,
            baseline=None,
            logger=ANY,
        )
        ensure_work_pr.assert_called_once()

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "completed"
        assert refreshed.failure_reason is None
        updated_unit = store.resolve_merge_unit_for_task(task.id)
        assert updated_unit is not None
        assert updated_unit.head_sha == "rebased-head"
        assert updated_unit.base_sha == "base-after-pr-retry"

    def test_complete_code_task_rebase_pr_required_failure_persists_retry_baseline_refs(self, tmp_path: Path):
        """Rebase PR-required failures must persist head/base refs for publish retry verification."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        parent = store.add(prompt="Implement parent", task_type="implement")
        rebase_task = store.add(
            prompt="Rebase parent branch",
            task_type="rebase",
            based_on=parent.id,
            same_branch=True,
            create_pr=True,
        )
        rebase_task.slug = "20260515-rebase-pr-required-baseline"
        store.mark_in_progress(rebase_task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{rebase_task.slug}.log"
        log_file.write_text("")

        worktree_git = Mock(spec=Git)
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_numstat.return_value = ""
        worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
            "feat/parent": "head-before-pr",
            "main": "base-before-pr",
        }.get(ref)

        with (
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
            patch("gza.runner.publish_rebased_branch"),
            patch(
                "gza.runner._ensure_work_pr_for_completed_code_task",
                return_value=CompletedCodeTaskPrPublicationOutcome(
                    kind="branch_unpushable",
                    status="push_failed",
                    message="push failed",
                    error="push failed",
                ),
            ),
            patch("gza.runner.task_footer"),
        ):
            rc = _complete_code_task(
                rebase_task,
                config,
                store,
                worktree_git,
                log_file,
                "feat/parent",
                TaskStats(duration_seconds=1.0, num_steps_reported=1, cost_usd=0.01),
                0,
                pre_run_status=set(),
                worktree_summary_path=tmp_path / "missing-summary.md",
                summary_path=tmp_path / ".gza" / "summaries" / f"{rebase_task.slug}.md",
                summary_dir=tmp_path / ".gza" / "summaries",
                skip_commit=True,
                create_pr=True,
            )

        assert rc == 1
        refreshed = store.get(rebase_task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == BRANCH_UNPUSHABLE_FAILURE_REASON
        rebase_unit = store.resolve_merge_unit_for_task(rebase_task.id)
        assert rebase_unit is not None
        assert rebase_unit.head_sha == "head-before-pr"
        assert rebase_unit.base_sha == "base-before-pr"

    def test_complete_code_task_rebase_force_pushes_from_runner(self, tmp_path: Path):
        """Rebase completion should publish through the shared helper."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        parent = store.add(prompt="Implement parent", task_type="implement")
        rebase_task = store.add(
            prompt="Rebase parent branch",
            task_type="rebase",
            based_on=parent.id,
            same_branch=True,
        )
        rebase_task.slug = "20260401-rebase-push"
        store.mark_in_progress(rebase_task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{rebase_task.slug}.log"
        log_file.write_text("")

        worktree_git = Mock(spec=Git)
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_numstat.return_value = ""

        with (
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
            patch("gza.runner.publish_rebased_branch") as publish_rebased_branch,
        ):
            rc = _complete_code_task(
                rebase_task,
                config,
                store,
                worktree_git,
                log_file,
                "feat/parent",
                TaskStats(duration_seconds=1.0, num_steps_reported=1, cost_usd=0.01),
                0,
                pre_run_status=set(),
                worktree_summary_path=tmp_path / "missing-summary.md",
                summary_path=tmp_path / ".gza" / "summaries" / f"{rebase_task.slug}.md",
                summary_dir=tmp_path / ".gza" / "summaries",
                skip_commit=True,
            )

        assert rc == 0
        publish_rebased_branch.assert_called_once_with(
            worktree_git,
            branch="feat/parent",
            baseline=None,
            logger=ANY,
        )
        refreshed = store.get(rebase_task.id)
        assert refreshed is not None
        assert refreshed.status == "completed"

    def test_complete_code_task_rebase_publishes_before_pr_ensure(self, tmp_path: Path) -> None:
        """Rebase completion with create_pr should publish before PR setup runs."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        parent = store.add(prompt="Implement parent", task_type="implement")
        rebase_task = store.add(
            prompt="Rebase parent branch",
            task_type="rebase",
            based_on=parent.id,
            same_branch=True,
            create_pr=True,
        )
        rebase_task.slug = "20260516-rebase-pr-order"
        store.mark_in_progress(rebase_task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{rebase_task.slug}.log"
        log_file.write_text("")

        worktree_git = Mock(spec=Git)
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_numstat.return_value = ""

        call_order: list[str] = []

        def _publish(*_args, **_kwargs):
            call_order.append("publish")

        def _ensure(*_args, **_kwargs):
            call_order.append("pr")
            return CompletedCodeTaskPrPublicationOutcome(
                kind="ready",
                status="created",
                message="created",
            )

        with (
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
            patch("gza.runner.publish_rebased_branch", side_effect=_publish) as publish_rebased_branch,
            patch("gza.runner._ensure_work_pr_for_completed_code_task", side_effect=_ensure) as ensure_work_pr,
        ):
            rc = _complete_code_task(
                rebase_task,
                config,
                store,
                worktree_git,
                log_file,
                "feat/parent",
                TaskStats(duration_seconds=1.0, num_steps_reported=1, cost_usd=0.01),
                0,
                pre_run_status=set(),
                worktree_summary_path=tmp_path / "missing-summary.md",
                summary_path=tmp_path / ".gza" / "summaries" / f"{rebase_task.slug}.md",
                summary_dir=tmp_path / ".gza" / "summaries",
                skip_commit=True,
                create_pr=True,
            )

        assert rc == 0
        assert call_order == ["publish", "pr"]
        publish_rebased_branch.assert_called_once()
        ensure_work_pr.assert_called_once()
        worktree_git.push_branch.assert_not_called()
        refreshed = store.get(rebase_task.id)
        assert refreshed is not None
        assert refreshed.status == "completed"

    def test_run_rebase_task_publication_failure_logs_failed_terminal_outcome(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        (tmp_path / "gza.yaml").write_text(
            "project_name: testproject\n"
            "project_id: default\n"
            "db_path: .gza/gza.db\n"
            "use_docker: false\n"
        )
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)

        parent = store.add(prompt="Implement parent", task_type="implement")
        assert parent.id is not None
        parent.slug = "20260515-parent-impl"
        parent.branch = "feature/rebase-parent"
        store.mark_in_progress(parent)
        store.mark_completed(parent, branch=parent.branch, log_file="logs/parent.log", has_commits=True)
        parent.merge_status = "merged"
        store.update(parent)
        review = store.add(prompt="Review parent", task_type="review", depends_on=parent.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        store.clear_review_state(parent.id)
        parent_before_failure = store.get(parent.id)
        assert parent_before_failure is not None
        assert parent_before_failure.review_cleared_at is not None

        task = store.add(
            prompt="Rebase parent branch",
            task_type="rebase",
            based_on=parent.id,
            same_branch=True,
        )
        assert task.id is not None
        task.slug = "20260515-runner-rebase-publish-failure"
        store.update(task)

        worktree_path = config.worktree_path / task.slug
        worktree_path.mkdir(parents=True, exist_ok=True)

        mock_provider = Mock()
        mock_provider.name = "TestProvider"
        mock_provider.check_credentials.return_value = True
        mock_provider.verify_credentials.return_value = True
        mock_provider.run.return_value = RunResult(
            exit_code=0,
            duration_seconds=2.0,
            num_turns_reported=1,
            cost_usd=0.01,
            error_type=None,
        )

        mock_main_git = Mock(spec=Git)
        mock_main_git.default_branch.return_value = "main"
        mock_main_git.worktree_list.return_value = []

        mock_worktree_git = Mock(spec=Git)
        mock_worktree_git.repo_dir = worktree_path
        mock_worktree_git.status_porcelain.return_value = set()
        mock_worktree_git.has_changes.return_value = False
        mock_worktree_git.rev_parse.return_value = "same-head"
        mock_worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
            parent.branch: "head-new",
            "main": "base-new",
        }.get(ref)
        mock_worktree_git.get_diff_numstat.return_value = ""

        with (
            patch("gza.runner.backup_database"),
            patch("gza.runner.load_dotenv"),
            patch("gza.runner.get_provider", return_value=mock_provider),
            patch("gza.runner.Git", side_effect=[mock_main_git, mock_worktree_git]),
            patch("gza.runner._resolve_code_task_branch_name", return_value=parent.branch),
            patch("gza.runner._setup_code_task_worktree", return_value=True),
            patch("gza.runner.build_prompt", return_value="prompt"),
            patch("gza.runner.is_rebase_in_progress", return_value=False),
            patch("gza.runner.publish_rebased_branch", side_effect=GitError("push boom")),
            patch("gza.runner.task_footer"),
        ):
            rc = run(config, task_id=task.id)

        assert rc == 1
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "GIT_ERROR"
        assert refreshed.changed_diff is None

        refreshed_parent = store.get(parent.id)
        assert refreshed_parent is not None
        assert refreshed_parent.review_cleared_at == parent_before_failure.review_cleared_at
        assert refreshed_parent.merge_status == "merged"

        surfaced = capsys.readouterr()
        assert "Git error: push boom" in surfaced.out

        assert refreshed.log_file is not None
        log_file = config.project_dir / refreshed.log_file
        log_text = ops_log_path_for(log_file).read_text()
        assert "Outcome: failed (GIT_ERROR)" in log_text
        assert "Outcome: completed" not in log_text

    def test_run_rebase_task_remote_ref_lookup_failure_logs_failed_terminal_outcome(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        (tmp_path / "gza.yaml").write_text(
            "project_name: testproject\n"
            "project_id: default\n"
            "db_path: .gza/gza.db\n"
            "use_docker: false\n"
        )
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)

        parent = store.add(prompt="Implement parent", task_type="implement")
        assert parent.id is not None
        parent.slug = "20260515-parent-impl"
        parent.branch = "feature/rebase-parent"
        store.mark_in_progress(parent)
        store.mark_completed(parent, branch=parent.branch, log_file="logs/parent.log", has_commits=True)
        parent.merge_status = "merged"
        store.update(parent)
        review = store.add(prompt="Review parent", task_type="review", depends_on=parent.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        store.clear_review_state(parent.id)
        parent_before_failure = store.get(parent.id)
        assert parent_before_failure is not None
        assert parent_before_failure.review_cleared_at is not None

        task = store.add(
            prompt="Rebase parent branch",
            task_type="rebase",
            based_on=parent.id,
            same_branch=True,
        )
        assert task.id is not None
        task.slug = "20260515-runner-rebase-lookup-failure"
        store.update(task)

        worktree_path = config.worktree_path / task.slug
        worktree_path.mkdir(parents=True, exist_ok=True)

        mock_provider = Mock()
        mock_provider.name = "TestProvider"
        mock_provider.check_credentials.return_value = True
        mock_provider.verify_credentials.return_value = True
        mock_provider.run.return_value = RunResult(
            exit_code=0,
            duration_seconds=2.0,
            num_turns_reported=1,
            cost_usd=0.01,
            error_type=None,
        )

        mock_main_git = Mock(spec=Git)
        mock_main_git.default_branch.return_value = "main"
        mock_main_git.worktree_list.return_value = []

        mock_worktree_git = Mock(spec=Git)
        mock_worktree_git.repo_dir = worktree_path
        mock_worktree_git.status_porcelain.return_value = set()
        mock_worktree_git.has_changes.return_value = False
        mock_worktree_git.rev_parse.return_value = "head-new"
        def rev_parse_if_exists(ref: str) -> str | None:
            if ref == parent.branch:
                return "head-new"
            if ref == "main":
                return "base-new"
            if ref == f"origin/{parent.branch}":
                raise RuntimeError("remote lookup boom")
            return None

        mock_worktree_git.rev_parse_if_exists.side_effect = rev_parse_if_exists
        mock_worktree_git.get_diff_numstat.return_value = ""

        with (
            patch("gza.runner.backup_database"),
            patch("gza.runner.load_dotenv"),
            patch("gza.runner.get_provider", return_value=mock_provider),
            patch("gza.runner.Git", side_effect=[mock_main_git, mock_worktree_git]),
            patch("gza.runner._resolve_code_task_branch_name", return_value=parent.branch),
            patch("gza.runner._setup_code_task_worktree", return_value=True),
            patch("gza.runner.build_prompt", return_value="prompt"),
            patch("gza.runner.is_rebase_in_progress", return_value=False),
            patch("gza.runner.task_footer"),
        ):
            rc = run(config, task_id=task.id)

        assert rc == 1
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "GIT_ERROR"
        assert refreshed.changed_diff is None

        refreshed_parent = store.get(parent.id)
        assert refreshed_parent is not None
        assert refreshed_parent.review_cleared_at == parent_before_failure.review_cleared_at
        assert refreshed_parent.merge_status == "merged"

        surfaced = capsys.readouterr()
        assert "Git error: Failed to resolve rebased branch publication refs for feature/rebase-parent: remote lookup boom" in surfaced.out

        assert refreshed.log_file is not None
        log_file = config.project_dir / refreshed.log_file
        log_text = ops_log_path_for(log_file).read_text()
        assert "Outcome: failed (GIT_ERROR)" in log_text
        assert "Outcome: completed" not in log_text

    def test_run_pr_required_retry_for_rebase_publish_failure_keeps_state_unpublished(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Rebase PR retries must fail before completion-side state if publication fails."""
        (tmp_path / "gza.yaml").write_text(
            "project_name: testproject\n"
            "project_id: default\n"
            "db_path: .gza/gza.db\n"
        )
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)

        parent = store.add(prompt="Implement parent", task_type="implement")
        parent.merge_status = "merged"
        store.update(parent)
        review = store.add(prompt="Review parent", task_type="review", depends_on=parent.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        store.clear_review_state(parent.id)
        parent_before_retry = store.get(parent.id)
        assert parent_before_retry is not None
        assert parent_before_retry.review_cleared_at is not None

        task = store.add(
            prompt="Rebase parent branch",
            task_type="rebase",
            based_on=parent.id,
            same_branch=True,
        )
        task.slug = "20260515-retry-rebase-publish-failure"
        task.status = "failed"
        task.failure_reason = "PR_REQUIRED"
        task.branch = "feature/retry-rebase-publish-failure"
        task.log_file = "logs/retry-rebase-publish-failure.log"
        task.output_content = "summary"
        task.diff_files_changed = 3
        task.diff_lines_added = 10
        task.diff_lines_removed = 4
        store.mark_failed(
            task,
            log_file=task.log_file,
            has_commits=True,
            branch=task.branch,
            failure_reason="PR_REQUIRED",
            head_sha="old-tip",
            base_sha="start-target",
        )
        task.output_content = "summary"
        store.update(task)

        git = Mock(spec=Git)
        git.default_branch.return_value = "main"
        git.rev_parse_if_exists.side_effect = lambda ref: {
            task.branch: "head-new",
            "main": "base-new",
        }.get(ref)

        with (
            patch("gza.runner.Git", return_value=git),
            patch("gza.runner.backup_database"),
            patch("gza.runner.load_dotenv"),
            patch(
                "gza.runner._ensure_work_pr_for_completed_code_task",
                return_value=CompletedCodeTaskPrPublicationOutcome(kind="ready", status="created", message="created"),
            ),
            patch("gza.runner.publish_rebased_branch", side_effect=GitError("push boom")),
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = run(config, task_id=task.id, create_pr=True)

        assert rc == 1
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "GIT_ERROR"
        assert refreshed.changed_diff is None

        refreshed_parent = store.get(parent.id)
        assert refreshed_parent is not None
        assert refreshed_parent.review_cleared_at == parent_before_retry.review_cleared_at
        assert refreshed_parent.merge_status == "merged"

        surfaced = capsys.readouterr()
        assert "Git error: push boom" in surfaced.out

        assert refreshed.log_file is not None
        log_file = config.project_dir / refreshed.log_file
        log_text = ops_log_path_for(log_file).read_text()
        assert "Outcome: failed (GIT_ERROR)" in log_text
        assert "Outcome: completed" not in log_text

    def test_run_pr_required_retry_for_rebase_publishes_before_pr_ensure(
        self,
        tmp_path: Path,
    ) -> None:
        """PR-required rebase retries should publish before re-running PR setup."""
        (tmp_path / "gza.yaml").write_text(
            "project_name: testproject\n"
            "project_id: default\n"
            "db_path: .gza/gza.db\n"
        )
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)

        parent = store.add(prompt="Implement parent", task_type="implement")
        task = store.add(
            prompt="Rebase parent branch",
            task_type="rebase",
            based_on=parent.id,
            same_branch=True,
            create_pr=True,
        )
        task.slug = "20260516-retry-rebase-pr-order"
        task.status = "failed"
        task.failure_reason = "PR_REQUIRED"
        task.branch = "feature/retry-rebase-pr-order"
        task.log_file = "logs/retry-rebase-pr-order.log"
        task.output_content = "summary"
        task.diff_files_changed = 1
        task.diff_lines_added = 2
        task.diff_lines_removed = 3
        store.mark_failed(
            task,
            log_file=task.log_file,
            has_commits=True,
            branch=task.branch,
            failure_reason="PR_REQUIRED",
            head_sha="old-tip",
            base_sha="start-target",
        )
        task.output_content = "summary"
        store.update(task)

        git = Mock(spec=Git)
        git.default_branch.return_value = "main"
        git.rev_parse_if_exists.side_effect = lambda ref: {
            task.branch: "head-new",
            "main": "base-new",
        }.get(ref)

        call_order: list[str] = []

        def _publish(*_args, **_kwargs):
            call_order.append("publish")

        def _ensure(*_args, **_kwargs):
            call_order.append("pr")
            return CompletedCodeTaskPrPublicationOutcome(
                kind="ready",
                status="created",
                message="created",
            )

        with (
            patch("gza.runner.Git", return_value=git),
            patch("gza.runner.backup_database"),
            patch("gza.runner.load_dotenv"),
            patch("gza.runner.publish_rebased_branch", side_effect=_publish) as publish_rebased_branch,
            patch("gza.runner._ensure_work_pr_for_completed_code_task", side_effect=_ensure) as ensure_work_pr,
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = run(config, task_id=task.id, create_pr=True)

        assert rc == 0
        assert call_order == ["publish", "pr"]
        publish_rebased_branch.assert_called_once()
        ensure_work_pr.assert_called_once()
        git.push_branch.assert_not_called()
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "completed"

    def test_run_pr_required_retry_for_rebase_remote_ref_lookup_failure_keeps_state_unpublished(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Rebase PR retries must fail before completion-side state if publication lookup fails."""
        (tmp_path / "gza.yaml").write_text(
            "project_name: testproject\n"
            "project_id: default\n"
            "db_path: .gza/gza.db\n"
        )
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)

        parent = store.add(prompt="Implement parent", task_type="implement")
        parent.merge_status = "merged"
        store.update(parent)
        review = store.add(prompt="Review parent", task_type="review", depends_on=parent.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        store.clear_review_state(parent.id)
        parent_before_retry = store.get(parent.id)
        assert parent_before_retry is not None
        assert parent_before_retry.review_cleared_at is not None

        task = store.add(
            prompt="Rebase parent branch",
            task_type="rebase",
            based_on=parent.id,
            same_branch=True,
        )
        task.slug = "20260515-retry-rebase-lookup-failure"
        task.status = "failed"
        task.failure_reason = "PR_REQUIRED"
        task.branch = "feature/retry-rebase-publish-failure"
        task.log_file = "logs/retry-rebase-publish-failure.log"
        task.output_content = "summary"
        task.diff_files_changed = 3
        task.diff_lines_added = 10
        task.diff_lines_removed = 4
        store.mark_failed(
            task,
            log_file=task.log_file,
            has_commits=True,
            branch=task.branch,
            failure_reason="PR_REQUIRED",
            head_sha="old-tip",
            base_sha="start-target",
        )
        task.output_content = "summary"
        store.update(task)

        git = Mock(spec=Git)
        git.default_branch.return_value = "main"
        git.rev_parse.return_value = "head-new"
        def retry_rev_parse_if_exists(ref: str) -> str | None:
            if ref == task.branch:
                return "head-new"
            if ref == "main":
                return "base-new"
            if ref == f"origin/{task.branch}":
                raise RuntimeError("remote lookup boom")
            return None

        git.rev_parse_if_exists.side_effect = retry_rev_parse_if_exists

        with (
            patch("gza.runner.Git", return_value=git),
            patch("gza.runner.backup_database"),
            patch("gza.runner.load_dotenv"),
            patch(
                "gza.runner._ensure_work_pr_for_completed_code_task",
                return_value=CompletedCodeTaskPrPublicationOutcome(kind="ready", status="created", message="created"),
            ),
            patch("gza.runner.task_footer"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        ):
            rc = run(config, task_id=task.id, create_pr=True)

        assert rc == 1
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "GIT_ERROR"
        assert refreshed.changed_diff is None

        refreshed_parent = store.get(parent.id)
        assert refreshed_parent is not None
        assert refreshed_parent.review_cleared_at == parent_before_retry.review_cleared_at
        assert refreshed_parent.merge_status == "merged"

        surfaced = capsys.readouterr()
        assert (
            "Git error: Failed to resolve rebased branch publication refs for "
            "feature/retry-rebase-publish-failure: remote lookup boom"
        ) in surfaced.out

        assert refreshed.log_file is not None
        log_file = config.project_dir / refreshed.log_file
        log_text = ops_log_path_for(log_file).read_text()
        assert "Outcome: failed (GIT_ERROR)" in log_text
        assert "Outcome: completed" not in log_text

    def test_run_inner_marks_resumed_rebase_baseline_as_recovered(
        self,
        tmp_path: Path,
    ) -> None:
        """Resumed runner rebases must capture a recovered baseline for fail-closed diff classification."""
        (tmp_path / "gza.yaml").write_text(
            "project_name: testproject\n"
            "project_id: default\n"
            "db_path: .gza/gza.db\n"
            "use_docker: false\n"
        )
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)

        parent = store.add(prompt="Implement parent", task_type="implement")
        assert parent.id is not None
        parent.slug = "20260512-parent-impl"
        parent.branch = "feature/rebase-parent"
        store.mark_in_progress(parent)
        store.mark_completed(parent, branch=parent.branch, log_file="logs/parent.log", has_commits=True)

        task = store.add(
            prompt="Resume rebase parent branch",
            task_type="rebase",
            based_on=parent.id,
            same_branch=True,
        )
        assert task.id is not None
        task.slug = "20260512-runner-rebase-resume"
        task.session_id = "resume-rebase-session"
        store.mark_failed(task, log_file="logs/rebase.log", stats=None)

        worktree_path = config.worktree_path / task.slug
        worktree_path.mkdir(parents=True, exist_ok=True)

        mock_provider = Mock()
        mock_provider.name = "TestProvider"
        mock_provider.run.return_value = RunResult(
            exit_code=0,
            duration_seconds=2.0,
            num_turns_reported=1,
            cost_usd=0.01,
            session_id=task.session_id,
            error_type=None,
        )

        mock_main_git = Mock(spec=Git)
        mock_main_git.default_branch.return_value = "main"

        mock_worktree_git = Mock(spec=Git)
        mock_worktree_git.repo_dir = worktree_path
        mock_worktree_git.status_porcelain.return_value = set()

        def capture_baseline(_git: Git, *, branch: str, target: str, recovered: bool = False) -> RebaseDiffBaseline:
            assert branch == parent.branch
            assert target == "main"
            assert recovered is True
            return RebaseDiffBaseline(
                old_tip="old-tip",
                target_at_start="start-target",
                merge_base_at_start="merge-base",
                recovered=True,
            )

        with (
            patch("gza.runner.Git", return_value=mock_worktree_git),
            patch("gza.runner._resolve_code_task_branch_name", return_value=parent.branch),
            patch("gza.runner._setup_code_task_worktree", return_value=True),
            patch("gza.runner._restore_wip_changes"),
            patch("gza.skills_utils.ensure_all_skills", return_value=0),
            patch("gza.runner._snapshot_task_db_to_worktree"),
            patch("gza.runner._copy_learnings_to_worktree"),
            patch("gza.runner.capture_rebase_diff_baseline", side_effect=capture_baseline),
            patch("gza.runner._complete_code_task", return_value=0) as mock_complete,
        ):
            rc = _run_inner(task, config, config, store, mock_provider, mock_main_git, resume=True)

        assert rc == 0
        assert mock_complete.call_args.kwargs["rebase_diff_baseline"] == RebaseDiffBaseline(
            old_tip="old-tip",
            target_at_start="start-target",
            merge_base_at_start="merge-base",
            recovered=True,
        )

    def test_run_inner_marks_retry_recovery_rebase_baseline_as_recovered(
        self,
        tmp_path: Path,
    ) -> None:
        """Automatic retry recovery rebase children must also fail closed."""
        (tmp_path / "gza.yaml").write_text(
            "project_name: testproject\n"
            "project_id: default\n"
            "db_path: .gza/gza.db\n"
            "use_docker: false\n"
        )
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)

        parent = store.add(prompt="Implement parent", task_type="implement")
        assert parent.id is not None
        parent.slug = "20260512-parent-impl"
        parent.branch = "feature/rebase-parent"
        store.mark_in_progress(parent)
        store.mark_completed(parent, branch=parent.branch, log_file="logs/parent.log", has_commits=True)

        failed_rebase = store.add(
            prompt="Failed rebase parent branch",
            task_type="rebase",
            based_on=parent.id,
            same_branch=True,
        )
        assert failed_rebase.id is not None
        failed_rebase.slug = "20260512-failed-rebase"
        failed_rebase.branch = parent.branch
        store.mark_failed(failed_rebase, log_file="logs/rebase.log", stats=None)

        task = store.add(
            prompt="Retry rebase parent branch",
            task_type="rebase",
            based_on=failed_rebase.id,
            same_branch=True,
            recovery_origin="retry",
        )
        assert task.id is not None
        task.slug = "20260512-runner-rebase-retry"

        worktree_path = config.worktree_path / task.slug
        worktree_path.mkdir(parents=True, exist_ok=True)

        mock_provider = Mock()
        mock_provider.name = "TestProvider"
        mock_provider.run.return_value = RunResult(
            exit_code=0,
            duration_seconds=2.0,
            num_turns_reported=1,
            cost_usd=0.01,
            session_id=None,
            error_type=None,
        )

        mock_main_git = Mock(spec=Git)
        mock_main_git.default_branch.return_value = "main"

        mock_worktree_git = Mock(spec=Git)
        mock_worktree_git.repo_dir = worktree_path
        mock_worktree_git.status_porcelain.return_value = set()

        def capture_baseline(_git: Git, *, branch: str, target: str, recovered: bool = False) -> RebaseDiffBaseline:
            assert branch == parent.branch
            assert target == "main"
            assert recovered is True
            return RebaseDiffBaseline(
                old_tip="old-tip",
                target_at_start="start-target",
                merge_base_at_start="merge-base",
                recovered=True,
            )

        with (
            patch("gza.runner.Git", return_value=mock_worktree_git),
            patch("gza.runner._resolve_code_task_branch_name", return_value=parent.branch),
            patch("gza.runner._setup_code_task_worktree", return_value=True),
            patch("gza.runner._restore_wip_changes"),
            patch("gza.skills_utils.ensure_all_skills", return_value=0),
            patch("gza.runner._snapshot_task_db_to_worktree"),
            patch("gza.runner._copy_learnings_to_worktree"),
            patch("gza.runner.capture_rebase_diff_baseline", side_effect=capture_baseline),
            patch("gza.runner._complete_code_task", return_value=0) as mock_complete,
        ):
            rc = _run_inner(task, config, config, store, mock_provider, mock_main_git, resume=False)

        assert rc == 0
        assert mock_complete.call_args.kwargs["rebase_diff_baseline"] == RebaseDiffBaseline(
            old_tip="old-tip",
            target_at_start="start-target",
            merge_base_at_start="merge-base",
            recovered=True,
        )

    def test_post_complete_resumed_rebase_marks_changed_diff_unknown_and_invalidates_review(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Recovered/resumed rebase completion must persist changed diff and clear stale review state."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        parent = store.add(prompt="Implement parent", task_type="implement")
        assert parent.id is not None
        parent.branch = "feature/rebase-parent"
        parent.merge_status = "merged"
        parent.status = "completed"
        store.update(parent)

        review = store.add(prompt="Review parent", task_type="review", depends_on=parent.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        store.clear_review_state(parent.id)
        parent_before_rebase = store.get(parent.id)
        assert parent_before_rebase is not None
        assert parent_before_rebase.review_cleared_at is not None

        task = store.add(
            prompt="Recovered rebase parent branch",
            task_type="rebase",
            based_on=parent.id,
            same_branch=True,
        )
        assert task.id is not None
        task.slug = "20260512-runner-rebase-resume"
        task.status = "completed"
        task.branch = parent.branch
        store.update(task)

        mock_worktree_git = Mock(spec=Git)
        config = self._make_config(tmp_path)

        caplog.set_level(logging.WARNING)

        with (
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
            patch("gza.runner.task_footer"),
        ):
            rc = _post_complete_code_task(
                task,
                config,
                store,
                mock_worktree_git,
                parent.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=1, cost_usd=0.01),
                target_branch="main",
                rebase_diff_baseline=RebaseDiffBaseline(
                    old_tip="old-tip",
                    target_at_start="start-target",
                    merge_base_at_start="merge-base",
                    recovered=True,
                ),
            )

        assert rc == 0
        mock_worktree_git.push_force_with_lease.assert_called_once_with(parent.branch, remote="origin")

        refreshed_rebase = store.get(task.id)
        assert refreshed_rebase is not None
        assert refreshed_rebase.changed_diff is True

        refreshed_parent = store.get(parent.id)
        assert refreshed_parent is not None
        assert refreshed_parent.review_cleared_at is None
        assert refreshed_parent.merge_status == "unmerged"

        surfaced = capsys.readouterr().out
        assert "Warning: rebase diff comparison unavailable for recovered/resumed rebase; treating as changed" in surfaced
        assert "Changed Diff: yes (review must be refreshed)" in surfaced
        assert "rebase diff comparison unavailable for recovered/resumed rebase; treating as changed" in caplog.text

    def test_post_complete_retry_recovery_rebase_invalidates_impl_review_state(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Recovered retry rebase descendants must clear the implementation review state, not the failed rebase row."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        parent = store.add(prompt="Implement parent", task_type="implement")
        assert parent.id is not None
        parent.branch = "feature/rebase-parent"
        parent.merge_status = "merged"
        parent.status = "completed"
        store.update(parent)

        review = store.add(prompt="Review parent", task_type="review", depends_on=parent.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        store.clear_review_state(parent.id)
        parent_before_rebase = store.get(parent.id)
        assert parent_before_rebase is not None
        assert parent_before_rebase.review_cleared_at is not None

        failed_rebase = store.add(
            prompt="Failed rebase parent branch",
            task_type="rebase",
            based_on=parent.id,
            same_branch=True,
        )
        assert failed_rebase.id is not None
        failed_rebase.status = "failed"
        failed_rebase.branch = parent.branch
        store.update(failed_rebase)

        task = store.add(
            prompt="Recovered retry rebase parent branch",
            task_type="rebase",
            based_on=failed_rebase.id,
            same_branch=True,
            recovery_origin="retry",
        )
        assert task.id is not None
        task.slug = "20260512-runner-rebase-retry"
        task.status = "completed"
        task.branch = parent.branch
        store.update(task)

        mock_worktree_git = Mock(spec=Git)
        config = self._make_config(tmp_path)

        caplog.set_level(logging.WARNING)

        with (
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
            patch("gza.runner.task_footer"),
        ):
            rc = _post_complete_code_task(
                task,
                config,
                store,
                mock_worktree_git,
                parent.branch,
                TaskStats(duration_seconds=1.0, num_steps_reported=1, cost_usd=0.01),
                target_branch="main",
                rebase_diff_baseline=RebaseDiffBaseline(
                    old_tip="old-tip",
                    target_at_start="start-target",
                    merge_base_at_start="merge-base",
                    recovered=True,
                ),
            )

        assert rc == 0
        mock_worktree_git.push_force_with_lease.assert_called_once_with(parent.branch, remote="origin")

        refreshed_rebase = store.get(task.id)
        assert refreshed_rebase is not None
        assert refreshed_rebase.changed_diff is True

        refreshed_parent = store.get(parent.id)
        assert refreshed_parent is not None
        assert refreshed_parent.review_cleared_at is None
        assert refreshed_parent.merge_status == "unmerged"

        refreshed_failed_rebase = store.get(failed_rebase.id)
        assert refreshed_failed_rebase is not None
        assert refreshed_failed_rebase.review_cleared_at is None

        surfaced = capsys.readouterr().out
        assert "Warning: rebase diff comparison unavailable for recovered/resumed rebase; treating as changed" in surfaced
        assert "Changed Diff: yes (review must be refreshed)" in surfaced
        assert "rebase diff comparison unavailable for recovered/resumed rebase; treating as changed" in caplog.text

    def test_rebase_task_fails_when_provider_leaves_rebase_in_progress(self, tmp_path: Path, capsys) -> None:
        (tmp_path / "gza.yaml").write_text(
            "provider: codex\n"
            "model: gpt-5\n"
            "project_name: runner-rebase-progress\n"
            "use_docker: false\n"
        )
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)

        parent = store.add(prompt="Implement parent", task_type="implement")
        parent.branch = "feat/parent"
        store.update(parent)

        task = store.add(
            prompt="Rebase parent branch",
            task_type="rebase",
            based_on=parent.id,
            same_branch=True,
        )
        assert task.id is not None
        task.slug = "20260512-runner-rebase-in-progress"
        store.update(task)

        worktree_path = config.worktree_path / task.slug
        worktree_path.mkdir(parents=True, exist_ok=True)

        mock_provider = Mock()
        mock_provider.name = "TestProvider"
        mock_provider.check_credentials.return_value = True
        mock_provider.verify_credentials.return_value = True
        mock_provider.run.return_value = RunResult(
            exit_code=0,
            duration_seconds=2.0,
            num_turns_reported=1,
            cost_usd=0.01,
            error_type=None,
        )

        mock_main_git = Mock(spec=Git)
        mock_main_git.default_branch.return_value = "main"
        mock_main_git.worktree_list.return_value = []

        mock_worktree_git = Mock(spec=Git)
        mock_worktree_git.repo_dir = worktree_path
        mock_worktree_git.status_porcelain.return_value = set()
        mock_worktree_git.has_changes.return_value = False
        mock_worktree_git.rev_parse.return_value = "same-head"

        with (
            patch("gza.runner.backup_database"),
            patch("gza.runner.load_dotenv"),
            patch("gza.runner.get_provider", return_value=mock_provider),
            patch("gza.runner.Git", side_effect=[mock_main_git, mock_worktree_git]),
            patch("gza.runner._resolve_code_task_branch_name", return_value=parent.branch),
            patch("gza.runner._setup_code_task_worktree", return_value=True),
            patch("gza.runner.build_prompt", return_value="prompt"),
            patch("gza.runner.is_rebase_in_progress", return_value=True),
            patch("gza.runner._complete_code_task", side_effect=AssertionError("should fail before completion")),
            patch("gza.runner.task_footer"),
        ):
            rc = run(config, task_id=task.id)

        assert rc == 0
        mock_worktree_git.push_force_with_lease.assert_not_called()

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "GIT_ERROR"

        surfaced = capsys.readouterr()
        assert "Rebase still in progress after provider success." in surfaced.err

        assert refreshed.log_file is not None
        log_file = config.project_dir / refreshed.log_file
        log_text = ops_log_path_for(log_file).read_text()
        assert "Rebase still in progress after provider success." in log_text
        assert '"failure_reason": "GIT_ERROR"' in log_text

    def test_complete_code_task_uses_summary_for_commit_subject(self, tmp_path: Path):
        """Commit subject should come from worktree summary when present."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Improve implementation based on review", task_type="improve")
        task.slug = "20260401-improve-commit-subject"
        store.mark_in_progress(task)

        review_task = store.add(prompt="Review implementation", task_type="review")
        task.depends_on = review_task.id
        store.update(task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{task.slug}.log"
        log_file.write_text("")

        pre_status = set()
        post_status = {("M", "src/foo.py")}
        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = post_status
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_numstat.return_value = "1\t0\tsrc/foo.py\n"

        summary_dir = tmp_path / ".gza" / "summaries"
        summary_path = summary_dir / f"{task.slug}.md"
        worktree_summary_path = tmp_path / "worktree" / ".gza" / "summaries" / f"{task.slug}.md"
        worktree_summary_path.parent.mkdir(parents=True, exist_ok=True)
        worktree_summary_path.write_text(
            "- Use task summary for commit subject\n"
            "- Include task metadata trailers\n"
        )

        with patch("gza.runner._squash_wip_commits"), patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None):
            rc = _complete_code_task(
                task,
                config,
                store,
                worktree_git,
                log_file,
                "test/branch",
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
                0,
                pre_run_status=pre_status,
                worktree_summary_path=worktree_summary_path,
                summary_path=summary_path,
                summary_dir=summary_dir,
            )

        assert rc == 0
        commit_message = worktree_git.commit.call_args.args[0]
        assert commit_message.startswith("Use task summary for commit subject Include task metadata trailers")
        assert f"\n\nTask {task.id}\nSlug: {task.slug}\n" in commit_message
        assert f"Gza-Review: {review_task.id}" in commit_message

    def test_build_code_task_commit_subject_falls_back_to_word_boundary_prompt(self, tmp_path: Path):
        """Without summary file, fallback should use word-boundary truncation of task prompt."""
        prompt = (
            "Improve implementation based on review by tightening commit message "
            "subject generation for summary-driven workflows"
        )
        worktree_summary_path = tmp_path / "missing-summary.md"

        subject = _build_code_task_commit_subject(prompt, worktree_summary_path)

        assert subject == "Improve implementation based on review by tightening commit message..."

    def test_build_code_task_commit_subject_uses_default_when_prompt_and_summary_empty(self, tmp_path: Path):
        """Whitespace-only prompts should still produce a non-empty deterministic subject."""
        subject = _build_code_task_commit_subject("   \n\t", tmp_path / "missing-summary.md")
        assert subject == "gza task"

    def test_complete_code_task_uses_slug_fallback_subject_when_prompt_blank(self, tmp_path: Path):
        """Completion should commit with slug-based subject when prompt and summary are empty."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="   \n\t", task_type="implement")
        task.slug = "20260401-blank-prompt-fallback"
        store.mark_in_progress(task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{task.slug}.log"
        log_file.write_text("")

        pre_status = set()
        post_status = {("M", "src/foo.py")}
        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = post_status
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_numstat.return_value = "1\t0\tsrc/foo.py\n"

        summary_dir = tmp_path / ".gza" / "summaries"
        summary_path = summary_dir / f"{task.slug}.md"
        worktree_summary_path = tmp_path / "worktree" / ".gza" / "summaries" / f"{task.slug}.md"

        with patch("gza.runner._squash_wip_commits"), patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None):
            rc = _complete_code_task(
                task,
                config,
                store,
                worktree_git,
                log_file,
                "test/branch",
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
                0,
                pre_run_status=pre_status,
                worktree_summary_path=worktree_summary_path,
                summary_path=summary_path,
                summary_dir=summary_dir,
            )

        assert rc == 0
        commit_message = worktree_git.commit.call_args.args[0]
        assert commit_message.splitlines()[0] == f"gza task {task.slug}"

    @pytest.mark.parametrize(
        ("summary_error", "expected_warning"),
        [
            (UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte"), "Failed to read summary file for commit subject"),
            (OSError("simulated read error"), "Failed to read summary file for commit subject"),
        ],
    )
    def test_complete_code_task_commits_when_summary_subject_read_fails_then_copy_succeeds(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
        summary_error: Exception,
        expected_warning: str,
    ):
        """Completion should continue and commit when summary read fails for subject generation."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="   \n\t", task_type="implement")
        task.slug = "20260401-summary-read-error-fallback"
        store.mark_in_progress(task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{task.slug}.log"
        log_file.write_text("")

        pre_status = set()
        post_status = {("M", "src/foo.py")}
        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = post_status
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_numstat.return_value = "1\t0\tsrc/foo.py\n"

        summary_dir = tmp_path / ".gza" / "summaries"
        summary_path = summary_dir / f"{task.slug}.md"
        worktree_summary_path = tmp_path / "worktree" / ".gza" / "summaries" / f"{task.slug}.md"
        worktree_summary_path.parent.mkdir(parents=True, exist_ok=True)
        worktree_summary_path.write_text("placeholder summary")

        original_read_text = Path.read_text
        summary_read_calls = {"count": 0}

        def _flaky_read_text(self: Path, *args, **kwargs):
            if self == worktree_summary_path:
                summary_read_calls["count"] += 1
                if summary_read_calls["count"] == 1:
                    raise summary_error
                return "Copied summary content"
            return original_read_text(self, *args, **kwargs)

        with (
            patch("gza.runner._squash_wip_commits"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
            patch.object(Path, "read_text", autospec=True, side_effect=_flaky_read_text),
            caplog.at_level("WARNING"),
        ):
            rc = _complete_code_task(
                task,
                config,
                store,
                worktree_git,
                log_file,
                "test/branch",
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
                0,
                pre_run_status=pre_status,
                worktree_summary_path=worktree_summary_path,
                summary_path=summary_path,
                summary_dir=summary_dir,
            )

        assert rc == 0
        commit_message = worktree_git.commit.call_args.args[0]
        assert commit_message.splitlines()[0] == f"gza task {task.slug}"
        assert expected_warning in caplog.text
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.output_content == "Copied summary content"

    @pytest.mark.parametrize(
        "summary_error",
        [
            UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte"),
            OSError("simulated persistent read error"),
        ],
    )
    def test_complete_code_task_commits_when_summary_read_persistently_fails(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
        summary_error: Exception,
    ):
        """Persistent summary read failures should not crash completion after commit."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="   \n\t", task_type="implement")
        task.slug = "20260401-summary-read-persistent-failure"
        store.mark_in_progress(task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{task.slug}.log"
        log_file.write_text("")

        pre_status = set()
        post_status = {("M", "src/foo.py")}
        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = post_status
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_numstat.return_value = "1\t0\tsrc/foo.py\n"

        summary_dir = tmp_path / ".gza" / "summaries"
        summary_path = summary_dir / f"{task.slug}.md"
        worktree_summary_path = tmp_path / "worktree" / ".gza" / "summaries" / f"{task.slug}.md"
        worktree_summary_path.parent.mkdir(parents=True, exist_ok=True)
        worktree_summary_path.write_text("placeholder summary")

        original_read_text = Path.read_text

        def _always_failing_read_text(self: Path, *args, **kwargs):
            if self == worktree_summary_path:
                raise summary_error
            return original_read_text(self, *args, **kwargs)

        with (
            patch("gza.runner._squash_wip_commits"),
            patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
            patch.object(Path, "read_text", autospec=True, side_effect=_always_failing_read_text),
            caplog.at_level("WARNING"),
        ):
            rc = _complete_code_task(
                task,
                config,
                store,
                worktree_git,
                log_file,
                "test/branch",
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
                0,
                pre_run_status=pre_status,
                worktree_summary_path=worktree_summary_path,
                summary_path=summary_path,
                summary_dir=summary_dir,
            )

        assert rc == 0
        commit_message = worktree_git.commit.call_args.args[0]
        assert commit_message.splitlines()[0] == f"gza task {task.slug}"
        assert "Failed to read summary file for commit subject" in caplog.text
        assert "Failed to read summary file for task completion output" in caplog.text

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "completed"
        assert refreshed.output_content is None


class TestWriteLogEntry:
    """Tests for write_log_entry helper."""

    def test_creates_file_and_writes_jsonl(self, tmp_path: Path) -> None:
        """write_log_entry keeps the transcript path and routes gza entries to ops."""
        import json
        log_file = tmp_path / "task.log"
        entry = {"type": "gza", "subtype": "info", "message": "Hello"}
        write_log_entry(log_file, entry)
        assert log_file.exists()
        assert log_file.read_text() == ""
        line = ops_log_path_for(log_file).read_text().strip()
        assert json.loads(line)["message"] == entry["message"]
        assert json.loads(line)["subtype"] == entry["subtype"]

    def test_appends_multiple_entries(self, tmp_path: Path) -> None:
        """write_log_entry appends gza entries to the derived ops stream."""
        import json
        log_file = tmp_path / "task.log"
        entry1 = {"type": "gza", "subtype": "info", "message": "First"}
        entry2 = {"type": "gza", "subtype": "branch", "message": "Second", "branch": "feat/x"}
        write_log_entry(log_file, entry1)
        write_log_entry(log_file, entry2)
        assert log_file.read_text() == ""
        lines = ops_log_path_for(log_file).read_text().strip().splitlines()
        assert len(lines) == 2
        assert json.loads(lines[0])["message"] == entry1["message"]
        assert json.loads(lines[1])["branch"] == entry2["branch"]

    def test_logs_warning_when_write_fails(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        """write_log_entry logs a warning and does not raise when writing fails."""
        bad_path = tmp_path / "task.log"
        with (
            caplog.at_level("WARNING"),
            patch("builtins.open", side_effect=OSError("boom")),
        ):
            write_log_entry(bad_path, {"type": "gza", "message": "x"})

        assert "Failed to write log entry" in caplog.text


class TestExtractReviewVerdict:
    """Tests for _extract_review_verdict()."""

    def test_bold_wrapped_verdict(self) -> None:
        assert _extract_review_verdict("**Verdict: APPROVED**") == "APPROVED"

    def test_bold_label_only_verdict(self) -> None:
        assert _extract_review_verdict("**Verdict**: CHANGES_REQUESTED") == "CHANGES_REQUESTED"

    def test_plain_verdict(self) -> None:
        assert _extract_review_verdict("Verdict: NEEDS_DISCUSSION") == "NEEDS_DISCUSSION"

    def test_heading_verdict(self) -> None:
        assert _extract_review_verdict("## Verdict\n\n**CHANGES_REQUESTED**\n") == "CHANGES_REQUESTED"

    def test_heading_verdict_without_bold_token(self) -> None:
        assert _extract_review_verdict("### Verdict\n\nNEEDS_DISCUSSION\n") == "NEEDS_DISCUSSION"

    def test_heading_verdict_approved_with_followups(self) -> None:
        assert _extract_review_verdict("## Verdict\n\nAPPROVED_WITH_FOLLOWUPS\n") == "APPROVED_WITH_FOLLOWUPS"

    def test_none_content(self) -> None:
        assert _extract_review_verdict(None) is None

    def test_no_verdict(self) -> None:
        assert _extract_review_verdict("Just some review text") is None

    def test_canonical_review_structure_with_none_sections(self) -> None:
        content = (
            "## Summary\n\n"
            "- Reviewed implementation and tests.\n\n"
            "## Blockers\n\n"
            "None.\n\n"
            "## Follow-Ups\n\n"
            "None.\n\n"
            "## Questions / Assumptions\n\n"
            "None.\n\n"
            "## Verdict\n\n"
            "No blocking issues identified.\n"
            "Verdict: APPROVED\n"
        )
        assert _extract_review_verdict(content) == "APPROVED"


class TestSelectiveStaging:
    """Tests for selective staging (only stage files changed during provider run)."""

    def test_selective_staging_only_stages_new_changes(self, tmp_path: Path):
        """Test that only files changed during the provider run get staged."""

        # Pre-existing status (before provider run)
        pre_status = {("M", "pre_existing.txt")}
        # Post-run status (includes pre-existing + new changes)
        post_status = {("M", "pre_existing.txt"), ("M", "src/foo.py"), ("??", "new_file.txt")}

        new_changes = post_status - pre_status
        files_to_stage = [filepath for _, filepath in new_changes]

        assert set(files_to_stage) == {"src/foo.py", "new_file.txt"}
        assert "pre_existing.txt" not in files_to_stage

    def test_selective_staging_no_new_changes(self, tmp_path: Path):
        """Test that no staging happens when provider makes no changes."""
        pre_status = {("M", "pre_existing.txt")}
        post_status = {("M", "pre_existing.txt")}

        new_changes = post_status - pre_status
        assert len(new_changes) == 0

    def test_selective_staging_handles_deletions(self, tmp_path: Path):
        """Test that intentional deletions by the agent are staged."""
        pre_status = set()
        post_status = {("D", "removed_by_agent.py")}

        new_changes = post_status - pre_status
        files_to_stage = [filepath for _, filepath in new_changes]

        assert "removed_by_agent.py" in files_to_stage

    def test_selective_staging_ignores_pre_existing_deletions(self, tmp_path: Path):
        """Test that pre-existing deletions are NOT staged."""
        pre_status = {("D", "already_deleted.py")}
        post_status = {("D", "already_deleted.py"), ("M", "changed.py")}

        new_changes = post_status - pre_status
        files_to_stage = [filepath for _, filepath in new_changes]

        assert "already_deleted.py" not in files_to_stage
        assert "changed.py" in files_to_stage


class TestExceptionHandlerMarkFailed:
    """Tests that exception handlers in _run_inner and _run_non_code_task mark tasks as failed."""

    def test_git_error_in_run_inner_marks_failed(self, tmp_path: Path):
        """Test that GitError during post-run finalization marks the task as failed."""

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Test task", task_type="implement")
        task.slug = "20260212-test-task"
        task.branch = "test-branch"
        store.mark_in_progress(task)

        # Verify task is in_progress
        assert store.get(task.id).status == "in_progress"

        # Simulate what the GitError handler does
        log_path = tmp_path / "logs"
        log_path.mkdir()
        log_file = log_path / f"{task.slug}.log"
        log_file.write_text("")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        store.mark_failed(task, log_file=str(log_file.relative_to(tmp_path)), branch="test-branch", failure_reason="GIT_ERROR")

        # Verify task is now failed
        updated_task = store.get(task.id)
        assert updated_task.status == "failed"
        assert updated_task.failure_reason == "GIT_ERROR"

    def test_keyboard_interrupt_marks_failed(self, tmp_path: Path):
        """Test that KeyboardInterrupt marks the task as failed with INTERRUPTED reason."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Test task", task_type="implement")
        task.slug = "20260212-test-task"
        task.branch = "test-branch"
        store.mark_in_progress(task)

        log_path = tmp_path / "logs"
        log_path.mkdir()
        log_file = log_path / f"{task.slug}.log"
        log_file.write_text("")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        store.mark_failed(task, log_file=str(log_file.relative_to(tmp_path)), branch="test-branch", failure_reason="INTERRUPTED")

        updated_task = store.get(task.id)
        assert updated_task.status == "failed"
        assert updated_task.failure_reason == "INTERRUPTED"

    def test_non_code_task_git_error_marks_failed(self, tmp_path: Path):
        """Test that GitError in _run_non_code_task marks the task as failed."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Explore something", task_type="explore")
        task.slug = "20260212-explore-task"
        store.mark_in_progress(task)

        log_path = tmp_path / "logs"
        log_path.mkdir()
        log_file = log_path / f"{task.slug}.log"
        log_file.write_text("")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        store.mark_failed(task, log_file=str(log_file.relative_to(tmp_path)), failure_reason="GIT_ERROR")

        updated_task = store.get(task.id)
        assert updated_task.status == "failed"
        assert updated_task.failure_reason == "GIT_ERROR"


class TestLoadDotenv:
    """Tests for load_dotenv() credential loading from .env files."""

    def _setup_dirs(self, tmp_path: Path, monkeypatch):
        """Create separate home and project directories; patch Path.home() to home_dir."""
        home_dir = tmp_path / "home"
        home_dir.mkdir()
        home_gza = home_dir / ".gza"
        home_gza.mkdir()
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: home_dir))
        return home_gza, project_dir

    def test_home_env_sets_unset_vars(self, tmp_path: Path, monkeypatch):
        """~/.gza/.env should set variables not already in the environment."""
        from gza.runner import load_dotenv

        home_gza, project_dir = self._setup_dirs(tmp_path, monkeypatch)
        (home_gza / ".env").write_text("MY_TEST_KEY=from_home\n")
        monkeypatch.delenv("MY_TEST_KEY", raising=False)

        load_dotenv(project_dir)

        assert os.environ["MY_TEST_KEY"] == "from_home"
        monkeypatch.delenv("MY_TEST_KEY")

    def test_home_env_does_not_override_existing(self, tmp_path: Path, monkeypatch):
        """~/.gza/.env should not override variables already set in the environment."""
        from gza.runner import load_dotenv

        home_gza, project_dir = self._setup_dirs(tmp_path, monkeypatch)
        (home_gza / ".env").write_text("MY_TEST_KEY=from_home\n")
        monkeypatch.setenv("MY_TEST_KEY", "from_shell")

        load_dotenv(project_dir)

        assert os.environ["MY_TEST_KEY"] == "from_shell"

    def test_project_env_overrides_home_env(self, tmp_path: Path, monkeypatch):
        """Project .env should override values from ~/.gza/.env."""
        from gza.runner import load_dotenv

        home_gza, project_dir = self._setup_dirs(tmp_path, monkeypatch)
        (home_gza / ".env").write_text("MY_TEST_KEY=from_home\n")
        (project_dir / ".env").write_text("MY_TEST_KEY=from_project\n")
        monkeypatch.delenv("MY_TEST_KEY", raising=False)

        load_dotenv(project_dir)

        assert os.environ["MY_TEST_KEY"] == "from_project"
        monkeypatch.delenv("MY_TEST_KEY")

    def test_comments_and_blank_lines_ignored(self, tmp_path: Path, monkeypatch):
        """Comments and blank lines in .env files should be ignored."""
        from gza.runner import load_dotenv

        home_gza, project_dir = self._setup_dirs(tmp_path, monkeypatch)
        (home_gza / ".env").write_text(
            "# This is a comment\n"
            "\n"
            "MY_TEST_KEY=valid_value\n"
            "# COMMENTED_OUT=should_not_exist\n"
        )
        monkeypatch.delenv("MY_TEST_KEY", raising=False)
        monkeypatch.delenv("COMMENTED_OUT", raising=False)

        load_dotenv(project_dir)

        assert os.environ["MY_TEST_KEY"] == "valid_value"
        assert "COMMENTED_OUT" not in os.environ
        monkeypatch.delenv("MY_TEST_KEY")

    def test_no_env_files_is_noop(self, tmp_path: Path, monkeypatch):
        """load_dotenv should not fail when no .env files exist."""
        from gza.runner import load_dotenv

        _, project_dir = self._setup_dirs(tmp_path, monkeypatch)

        load_dotenv(project_dir)

    def test_gza_env_takes_priority_over_root_env(self, tmp_path: Path, monkeypatch):
        """<project_dir>/.gza/.env should take priority over <project_dir>/.env."""
        from gza.runner import load_dotenv

        project_dir = tmp_path / "project"
        project_dir.mkdir()
        gza_subdir = project_dir / ".gza"
        gza_subdir.mkdir()
        (gza_subdir / ".env").write_text("MY_TEST_KEY=from_gza_dir\n")
        (project_dir / ".env").write_text("MY_TEST_KEY=from_root_env\n")
        monkeypatch.delenv("MY_TEST_KEY", raising=False)
        # Home dir has no .env
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))

        load_dotenv(project_dir)

        assert os.environ["MY_TEST_KEY"] == "from_gza_dir"
        monkeypatch.delenv("MY_TEST_KEY")

    def test_gza_env_takes_priority_over_home_env(self, tmp_path: Path, monkeypatch):
        """<project_dir>/.gza/.env should take priority over ~/.gza/.env."""
        from gza.runner import load_dotenv

        # Set up home ~/.gza/.env
        home_dir = tmp_path / "home"
        home_dir.mkdir()
        home_gza = home_dir / ".gza"
        home_gza.mkdir()
        (home_gza / ".env").write_text("MY_TEST_KEY=from_home\n")
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: home_dir))

        project_dir = tmp_path / "project"
        project_dir.mkdir()
        gza_subdir = project_dir / ".gza"
        gza_subdir.mkdir()
        (gza_subdir / ".env").write_text("MY_TEST_KEY=from_gza_dir\n")
        monkeypatch.delenv("MY_TEST_KEY", raising=False)

        load_dotenv(project_dir)

        assert os.environ["MY_TEST_KEY"] == "from_gza_dir"
        monkeypatch.delenv("MY_TEST_KEY")

    def test_root_env_overrides_home_env_when_no_gza_env(self, tmp_path: Path, monkeypatch):
        """<project_dir>/.env should still override ~/.gza/.env when .gza/.env is absent."""
        from gza.runner import load_dotenv

        home_dir = tmp_path / "home"
        home_dir.mkdir()
        home_gza = home_dir / ".gza"
        home_gza.mkdir()
        (home_gza / ".env").write_text("MY_TEST_KEY=from_home\n")
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: home_dir))

        project_dir = tmp_path / "project"
        project_dir.mkdir()
        (project_dir / ".env").write_text("MY_TEST_KEY=from_root_env\n")
        monkeypatch.delenv("MY_TEST_KEY", raising=False)

        load_dotenv(project_dir)

        assert os.environ["MY_TEST_KEY"] == "from_root_env"
        monkeypatch.delenv("MY_TEST_KEY")

    def test_home_env_is_lowest_priority(self, tmp_path: Path, monkeypatch):
        """~/.gza/.env should be lowest priority — not override any project values."""
        from gza.runner import load_dotenv

        home_dir = tmp_path / "home"
        home_dir.mkdir()
        home_gza = home_dir / ".gza"
        home_gza.mkdir()
        (home_gza / ".env").write_text("MY_TEST_KEY=from_home\nHOME_ONLY=home_value\n")
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: home_dir))

        project_dir = tmp_path / "project"
        project_dir.mkdir()
        gza_subdir = project_dir / ".gza"
        gza_subdir.mkdir()
        (gza_subdir / ".env").write_text("MY_TEST_KEY=from_gza_dir\n")
        monkeypatch.delenv("MY_TEST_KEY", raising=False)
        monkeypatch.delenv("HOME_ONLY", raising=False)

        load_dotenv(project_dir)

        # .gza/.env wins over home
        assert os.environ["MY_TEST_KEY"] == "from_gza_dir"
        # home-only vars still get loaded
        assert os.environ["HOME_ONLY"] == "home_value"
        monkeypatch.delenv("MY_TEST_KEY")
        monkeypatch.delenv("HOME_ONLY")

    def test_root_env_overrides_shell_env(self, tmp_path: Path, monkeypatch):
        """Project root .env should override shell environment variables."""
        from gza.runner import load_dotenv

        home_gza, project_dir = self._setup_dirs(tmp_path, monkeypatch)
        (project_dir / ".env").write_text("MY_TEST_KEY=from_project\n")
        monkeypatch.setenv("MY_TEST_KEY", "from_shell")

        load_dotenv(project_dir)

        assert os.environ["MY_TEST_KEY"] == "from_project"


class TestDependencyMergePrecondition:
    """Runner precondition tests for depends_on merged reachability."""

    def _make_config(self, tmp_path: Path, db_path: Path) -> Mock:
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
        config.max_turns = 50
        config.timeout_minutes = 60
        config.branch_mode = "multi"
        config.project_name = "test"
        config.project_prefix = "gza"
        config.branch_strategy = Mock()
        config.branch_strategy.pattern = "{project}/{task_id}"
        config.branch_strategy.default_type = "feature"
        config.get_provider_for_task.return_value = "claude"
        config.get_model_for_task.return_value = None
        config.get_max_steps_for_task.return_value = 50
        config.learnings_interval = 0
        config.learnings_window = 25
        return config

    def _setup_dep_and_downstream(self, store: SqliteTaskStore, *, same_branch: bool = False) -> tuple[Task, Task]:
        dep_task = store.add(prompt="Upstream task", task_type="implement")
        dep_task.slug = "20260412-upstream-task"
        dep_task.branch = "test/dep-branch"
        store.mark_in_progress(dep_task)
        store.mark_completed(dep_task, branch=dep_task.branch, log_file="logs/upstream.log", has_commits=True)

        downstream = store.add(
            prompt="Downstream task",
            task_type="implement",
            depends_on=dep_task.id,
            based_on=dep_task.id if same_branch else None,
            same_branch=same_branch,
        )
        downstream.slug = "20260412-downstream-task"
        store.update(downstream)
        return dep_task, downstream

    def _setup_failed_dep_with_completed_retry(self, store: SqliteTaskStore) -> tuple[Task, Task, Task]:
        """Create depends_on -> failed, plus a completed retry descendant."""
        dep_task = store.add(prompt="Original upstream task", task_type="implement")
        dep_task.slug = "20260412-upstream-failed"
        dep_task.branch = "test/original-upstream-branch"
        store.mark_in_progress(dep_task)
        store.mark_failed(dep_task, branch=dep_task.branch, log_file="logs/upstream-failed.log", failure_reason="UNKNOWN")

        retry_task = store.add(prompt="Retry upstream task", task_type="implement", based_on=dep_task.id)
        retry_task.slug = "20260412-upstream-retry"
        retry_task.branch = "test/retry-upstream-branch"
        store.mark_in_progress(retry_task)
        store.mark_completed(retry_task, branch=retry_task.branch, log_file="logs/upstream-retry.log", has_commits=True)

        downstream = store.add(
            prompt="Downstream task",
            task_type="implement",
            depends_on=dep_task.id,
        )
        downstream.slug = "20260412-downstream-task"
        store.update(downstream)
        return dep_task, retry_task, downstream

    def _run_with_dependency_state(
        self,
        tmp_path: Path,
        *,
        same_branch: bool = False,
        skip_precondition_check: bool = False,
        setup_retry_chain: bool = False,
        dep_mark_merged: bool = False,
    ) -> tuple[int, Mock, SqliteTaskStore, Task]:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        if setup_retry_chain:
            dep_task, _retry_task, downstream = self._setup_failed_dep_with_completed_retry(store)
        else:
            dep_task, downstream = self._setup_dep_and_downstream(store, same_branch=same_branch)
        if dep_mark_merged:
            assert dep_task.id is not None
            store.set_merge_status(dep_task.id, "merged")
        config = self._make_config(tmp_path, db_path)

        mock_provider = Mock()
        mock_provider.name = "TestProvider"
        mock_provider.check_credentials.return_value = True
        mock_provider.verify_credentials.return_value = True
        mock_provider.run.return_value = RunResult(
            exit_code=0,
            duration_seconds=3.0,
            num_turns_reported=1,
            cost_usd=0.01,
            error_type=None,
        )

        mock_main_git = Mock()
        mock_main_git.default_branch.return_value = "main"
        mock_main_git.worktree_list.return_value = []
        mock_main_git.worktree_add.return_value = config.worktree_path / downstream.slug
        mock_main_git.count_commits_ahead.return_value = 0

        mock_worktree_git = Mock()
        mock_worktree_git.status_porcelain.side_effect = [set(), set()]
        mock_worktree_git.default_branch.return_value = "main"
        mock_worktree_git.count_commits_ahead.return_value = 0
        mock_worktree_git.get_diff_numstat.return_value = ""

        with (
            patch("gza.runner.get_provider", return_value=mock_provider),
            patch("gza.runner.Git", side_effect=[mock_main_git, mock_worktree_git]),
            patch("gza.runner.load_dotenv"),
        ):
            result = run(
                config,
                task_id=downstream.id,
                skip_precondition_check=skip_precondition_check,
            )

        return result, mock_provider, store, downstream

    def test_merged_dependency_allows_task_start(self, tmp_path: Path):
        result, mock_provider, store, downstream = self._run_with_dependency_state(
            tmp_path,
            dep_mark_merged=True,
        )

        assert result == 0
        assert mock_provider.run.call_count == 1
        refreshed = store.get(downstream.id)
        assert refreshed is not None
        assert refreshed.failure_reason != "PREREQUISITE_UNMERGED"

    def test_completed_plan_dependency_does_not_require_merge(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        plan = store.add(prompt="Plan feature", task_type="plan")
        plan.slug = "20260412-plan-feature"
        store.mark_in_progress(plan)
        store.mark_completed(plan, log_file="logs/plan.log", has_commits=False)

        downstream = store.add(
            prompt="Implement from plan",
            task_type="implement",
            depends_on=plan.id,
        )
        downstream.slug = "20260412-implement-from-plan"
        store.update(downstream)

        config = self._make_config(tmp_path, db_path)

        mock_provider = Mock()
        mock_provider.name = "TestProvider"
        mock_provider.check_credentials.return_value = True
        mock_provider.verify_credentials.return_value = True
        mock_provider.run.return_value = RunResult(
            exit_code=0,
            duration_seconds=3.0,
            num_turns_reported=1,
            cost_usd=0.01,
            error_type=None,
        )

        mock_main_git = Mock()
        mock_main_git.default_branch.return_value = "main"
        mock_main_git.worktree_list.return_value = []
        mock_main_git.worktree_add.return_value = config.worktree_path / downstream.slug
        mock_main_git.count_commits_ahead.return_value = 0

        mock_worktree_git = Mock()
        mock_worktree_git.status_porcelain.side_effect = [set(), set()]
        mock_worktree_git.default_branch.return_value = "main"
        mock_worktree_git.count_commits_ahead.return_value = 0
        mock_worktree_git.get_diff_numstat.return_value = ""

        with (
            patch("gza.runner.get_provider", return_value=mock_provider),
            patch("gza.runner.Git", side_effect=[mock_main_git, mock_worktree_git]),
            patch("gza.runner.load_dotenv"),
        ):
            result = run(config, task_id=downstream.id)

        assert result == 0
        assert mock_provider.run.call_count == 1
        refreshed = store.get(downstream.id)
        assert refreshed is not None
        assert refreshed.failure_reason != "PREREQUISITE_UNMERGED"

    def test_unmerged_dependency_fails_before_provider_run(self, tmp_path: Path):
        result, mock_provider, store, downstream = self._run_with_dependency_state(
            tmp_path,
        )

        assert result == DEPENDENCY_BLOCKED_NOT_RUN_EXIT_CODE
        assert mock_provider.run.call_count == 0
        refreshed = store.get(downstream.id)
        assert refreshed is not None
        assert refreshed.status == "pending"
        assert refreshed.failure_reason is None
        assert refreshed.completed_at is None
        assert refreshed.started_at is None
        assert store.is_task_blocked(refreshed) == (True, refreshed.depends_on, "completed")
        assert all(task.status != "failed" for task in store.get_all())

    def test_missing_dependency_holds_task_before_provider_run(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        downstream = store.add(
            prompt="Implement with stale dependency",
            task_type="implement",
            depends_on="gza-999999",
        )
        downstream.slug = "20260412-implement-with-stale-dependency"
        store.update(downstream)
        config = self._make_config(tmp_path, db_path)

        mock_provider = Mock()
        mock_provider.name = "TestProvider"
        mock_provider.check_credentials.return_value = True
        mock_provider.verify_credentials.return_value = True
        mock_provider.run.return_value = RunResult(
            exit_code=0,
            duration_seconds=3.0,
            num_turns_reported=1,
            cost_usd=0.01,
            error_type=None,
        )

        with (
            patch("gza.runner.get_provider", return_value=mock_provider),
            patch("gza.runner.load_dotenv"),
        ):
            result = run(config, task_id=downstream.id)

        assert result == DEPENDENCY_BLOCKED_NOT_RUN_EXIT_CODE
        assert mock_provider.run.call_count == 0
        refreshed = store.get(downstream.id)
        assert refreshed is not None
        assert refreshed.status == "pending"
        assert refreshed.failure_reason is None
        assert store.is_task_blocked(refreshed) == (True, "gza-999999", "missing")

    def test_retry_chain_dependency_uses_completed_retry_for_precondition(self, tmp_path: Path):
        result, mock_provider, store, downstream = self._run_with_dependency_state(
            tmp_path,
            setup_retry_chain=True,
        )

        assert result == DEPENDENCY_BLOCKED_NOT_RUN_EXIT_CODE
        assert mock_provider.run.call_count == 0
        refreshed = store.get(downstream.id)
        assert refreshed is not None
        assert refreshed.status == "pending"
        assert refreshed.failure_reason is None

    def test_unmerged_dependency_with_prior_output_still_parks_pending(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        _dep_task, downstream = self._setup_dep_and_downstream(store)
        downstream.output_content = "prior output"
        store.update(downstream)
        config = self._make_config(tmp_path, db_path)

        mock_provider = Mock()
        mock_provider.name = "TestProvider"

        mock_main_git = Mock()
        mock_main_git.default_branch.return_value = "main"

        mock_worktree_git = Mock()

        with (
            patch("gza.runner._setup_code_task_worktree", return_value=True),
            patch("gza.runner.Git", return_value=mock_worktree_git),
        ):
            result = _run_inner(downstream, config, config, store, mock_provider, mock_main_git)

        assert result == DEPENDENCY_BLOCKED_NOT_RUN_EXIT_CODE
        assert mock_provider.run.call_count == 0
        refreshed = store.get(downstream.id)
        assert refreshed is not None
        assert refreshed.status == "pending"
        assert refreshed.failure_reason is None
        assert refreshed.output_content == "prior output"

    def test_run_inner_logs_blocked_dependency_and_parks_pending(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        _dep_task, downstream = self._setup_dep_and_downstream(store)
        config = self._make_config(tmp_path, db_path)

        mock_provider = Mock()
        mock_provider.name = "TestProvider"

        mock_main_git = Mock()
        mock_main_git.default_branch.return_value = "main"

        mock_worktree_git = Mock()

        with (
            patch("gza.runner._setup_code_task_worktree", return_value=True),
            patch("gza.runner.Git", return_value=mock_worktree_git),
        ):
            result = _run_inner(downstream, config, config, store, mock_provider, mock_main_git)

        assert result == DEPENDENCY_BLOCKED_NOT_RUN_EXIT_CODE
        assert mock_provider.run.call_count == 0

        refreshed = store.get(downstream.id)
        assert refreshed is not None
        assert refreshed.status == "pending"
        assert refreshed.failure_reason is None

        log_file = tmp_path / "logs" / f"{downstream.slug}.log"
        assert log_file.exists()
        log_text = ops_log_path_for(log_file).read_text()
        assert '"subtype": "blocked"' in log_text
        assert '"reason": "dependency_merge_precondition"' in log_text
        assert '"task_status": "pending"' in log_text
        assert '"failure_reason": "PREREQUISITE_UNMERGED"' not in log_text
        assert "test/dep-branch" in log_text

    def test_run_inner_logs_resolved_retry_dependency_for_precondition(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        _dep_task, retry_task, downstream = self._setup_failed_dep_with_completed_retry(store)
        config = self._make_config(tmp_path, db_path)

        mock_provider = Mock()
        mock_provider.name = "TestProvider"

        mock_main_git = Mock()
        mock_main_git.default_branch.return_value = "main"

        mock_worktree_git = Mock()

        with (
            patch("gza.runner._setup_code_task_worktree", return_value=True),
            patch("gza.runner.Git", return_value=mock_worktree_git),
        ):
            result = _run_inner(downstream, config, config, store, mock_provider, mock_main_git)

        assert result == DEPENDENCY_BLOCKED_NOT_RUN_EXIT_CODE
        assert retry_task.id is not None

        log_file = tmp_path / "logs" / f"{downstream.slug}.log"
        assert log_file.exists()
        log_text = ops_log_path_for(log_file).read_text()
        assert "test/retry-upstream-branch" in log_text
        assert f'"dependency_task_id": "{retry_task.id}"' in log_text

    def test_same_branch_skips_unmerged_dependency_check(self, tmp_path: Path):
        result, mock_provider, store, downstream = self._run_with_dependency_state(
            tmp_path,
            same_branch=True,
        )

        assert result == 0
        assert mock_provider.run.call_count == 1
        refreshed = store.get(downstream.id)
        assert refreshed is not None
        assert refreshed.failure_reason != "PREREQUISITE_UNMERGED"

    def test_force_flag_skips_unmerged_dependency_check(self, tmp_path: Path):
        result, mock_provider, store, downstream = self._run_with_dependency_state(
            tmp_path,
            skip_precondition_check=True,
        )

        assert result == 0
        assert mock_provider.run.call_count == 1
        refreshed = store.get(downstream.id)
        assert refreshed is not None
        assert refreshed.failure_reason != "PREREQUISITE_UNMERGED"

        log_file = tmp_path / "logs" / f"{downstream.slug}.log"
        assert log_file.exists()
        assert "Skipped dependency merge precondition check (--force)" in ops_log_path_for(log_file).read_text()

    def test_followup_task_dependency_is_merge_gated_by_reviewed_implementation(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")

        impl = store.add("Implement feature", task_type="implement")
        impl.status = "completed"
        impl.branch = "feat/impl"
        impl.has_commits = True
        store.update(impl)

        review = store.add("Review implementation", task_type="review", depends_on=impl.id)
        review.status = "completed"
        store.update(review)

        finding = ReviewFinding(
            id="F1",
            severity="FOLLOWUP",
            title="Hardening",
            body="",
            evidence=None,
            impact=None,
            fix_or_followup="add malformed-input validation",
            tests=None,
        )
        followup, created_now = create_or_reuse_followup_task(
            store,
            review_task=review,
            impl_task=impl,
            finding=finding,
            trigger_source="manual",
        )
        assert created_now is True
        assert followup.depends_on == impl.id

        git = Mock()
        git.branch_exists.return_value = True
        git._run.return_value = Mock(returncode=1, stdout="", stderr="")

        dep, target_branch, git_error = _check_dependency_merge_precondition(
            followup,
            store,
            git,
            default_branch="main",
        )
        assert dep is not None
        assert dep.id == impl.id
        assert target_branch == "main"
        assert git_error is None

class TestGetPlanForTask:
    def test_finds_plan_via_depends_on_only(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        plan = store.add("Plan feature", task_type="plan")
        impl = store.add("Implement feature", task_type="implement", depends_on=plan.id)

        found = get_plan_for_task(store, impl)
        assert found is not None
        assert found.id == plan.id

    def test_finds_plan_via_based_on_only(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        plan = store.add("Plan feature", task_type="plan")
        impl = store.add("Implement feature", task_type="implement", based_on=plan.id)

        found = get_plan_for_task(store, impl)
        assert found is not None
        assert found.id == plan.id

    def test_finds_plan_through_mixed_retry_chain(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        plan = store.add("Plan feature", task_type="plan")
        impl = store.add("Implement feature", task_type="implement", depends_on=plan.id)
        retry = store.add("Retry implementation", task_type="implement", based_on=impl.id)

        found = get_plan_for_task(store, retry)
        assert found is not None
        assert found.id == plan.id


class TestStartupLogHelpers:
    """Tests for open_task_startup_log / rename_startup_log_to_slug."""

    def test_open_creates_task_id_file_when_no_log_file(self, tmp_path: Path):
        config = Config(project_dir=tmp_path, project_name="test-project")
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("hello")

        path = open_task_startup_log(config, task)

        assert path.exists()
        assert path.name == f"{task.id}.startup.log"
        assert path.parent == config.log_path

    def test_open_reuses_existing_log_file(self, tmp_path: Path):
        config = Config(project_dir=tmp_path, project_name="test-project")
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("hello")
        task.log_file = "logs/existing.log"
        (tmp_path / "logs").mkdir()
        existing = tmp_path / "logs" / "existing.log"
        existing.write_text("prior content\n")

        path = open_task_startup_log(config, task)
        assert path == existing
        assert path.read_text() == "prior content\n"

    def test_rename_moves_startup_to_slug(self, tmp_path: Path):
        config = Config(project_dir=tmp_path, project_name="test-project")
        config.log_path.mkdir(parents=True, exist_ok=True)
        startup = config.log_path / "gza-1.startup.log"
        startup_ops = config.log_path / "gza-1.startup.ops.jsonl"
        startup.write_text('{"subtype": "preflight"}\n')
        startup_ops.write_text('{"subtype": "execution"}\n')

        final = rename_startup_log_to_slug(config, startup, "20260419-hello")
        final_ops = config.log_path / "20260419-hello.ops.jsonl"

        assert not startup.exists()
        assert not startup_ops.exists()
        assert final.name == "20260419-hello.log"
        assert final.read_text() == '{"subtype": "preflight"}\n'
        assert final_ops.read_text() == '{"subtype": "execution"}\n'

    def test_rename_is_noop_when_paths_match(self, tmp_path: Path):
        config = Config(project_dir=tmp_path, project_name="test-project")
        config.log_path.mkdir(parents=True, exist_ok=True)
        slug_log = config.log_path / "20260419-hello.log"
        slug_log.write_text("already here\n")

        final = rename_startup_log_to_slug(config, slug_log, "20260419-hello")
        assert final == slug_log
        assert slug_log.read_text() == "already here\n"


class TestRunnerStoreMetadata:
    def test_run_preserves_projects_metadata_row(self, tmp_path: Path):
        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        db_path = project_dir / "shared.db"
        (project_dir / "gza.yaml").write_text(
            "project_name: demo\n"
            "project_id: alphaproject01\n"
            "project_prefix: gza\n"
            f"db_path: {db_path}\n",
            encoding="utf-8",
        )

        config = Config.load(project_dir)
        SqliteTaskStore.from_config(config)

        conn = sqlite3.connect(db_path)
        before = conn.execute(
            "SELECT root_path, config_path, project_name, project_prefix FROM projects WHERE id = ?",
            (config.project_id,),
        ).fetchone()
        conn.close()
        assert before is not None
        assert before[0]
        assert before[1]
        assert before[2] == "demo"
        assert before[3] == "gza"

        result = run(config, task_id="gza-9999")
        assert result == 1

        conn = sqlite3.connect(db_path)
        after = conn.execute(
            "SELECT root_path, config_path, project_name, project_prefix FROM projects WHERE id = ?",
            (config.project_id,),
        ).fetchone()
        conn.close()
        assert after == before


class TestProviderPromptSanitization:
    """Runner should sanitize provider-facing review/improve prompts only."""

    def test_fresh_review_prompt_includes_failed_verify_result_from_runner(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        impl = store.add(prompt="Implement feature X", task_type="implement")
        impl.status = "completed"
        impl.slug = "20260212-implement-feature-x"
        impl.branch = "gza/20260212-implement-feature-x"
        store.update(impl)

        task = store.add(prompt="Review feature X", task_type="review", depends_on=impl.id)
        task.slug = "20260213-review-feature-x"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.learnings_interval = 0
        config.learnings_window = 25
        config.model = None
        config.max_steps = 10
        config.timeout_minutes = 10
        passed_fingerprint = "a" * 64
        failed_fingerprint = "b" * 64
        config.verify_command = (
            "printf 'gza-verify phase=passed name=ruff duration_seconds=1.25 "
            f"tree_fingerprint={passed_fingerprint}\\n"
            "gza-verify phase=failed name=pytest duration_seconds=3.5 "
            f"tree_fingerprint={failed_fingerprint}\\n"
            "lint failed\\n' && exit 7"
        )
        config.autonomous_verify_timeout_seconds = 120

        captured_prompts: list[str] = []

        def provider_run(_config, prompt, _log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            captured_prompts.append(prompt)
            report_dir = work_dir / ".gza" / "reviews"
            report_dir.mkdir(parents=True, exist_ok=True)
            (report_dir / f"{task.slug}.md").write_text("# Review\n\nVerdict: CHANGES_REQUESTED")
            return RunResult(
                exit_code=0,
                duration_seconds=1.0,
                num_turns_reported=1,
                cost_usd=0.01,
                session_id=resume_session_id,
                error_type=None,
            )

        provider = Mock()
        provider.name = "MockProvider"
        provider.run.side_effect = provider_run

        git = Mock()
        git.default_branch.return_value = "main"
        git._run.return_value = Mock(returncode=0)
        git.get_diff_numstat.return_value = ""
        git.get_diff.return_value = ""
        git.get_diff_stat.return_value = ""

        with patch("gza.runner.Git.rev_parse_if_exists", return_value="deadbeef"), \
             patch("gza.runner.post_review_to_pr"):
            exit_code = _run_non_code_task(task, config, store, provider, git, resume=False)

        assert exit_code == 0
        assert len(captured_prompts) == 1
        prompt = captured_prompts[0]
        assert "## verify_command result" in prompt
        assert "- Status: failed" in prompt
        assert "- Exit status: 7" in prompt
        assert "- Reviewed head: `deadbeef`" in prompt
        assert "lint failed" in prompt
        assert "## Original request:" in prompt
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.review_verify_status == "failed"
        assert refreshed.review_verify_exit_status == "7"
        assert refreshed.review_verify_head_sha == "deadbeef"
        assert refreshed.review_verify_branch == impl.branch
        assert refreshed.review_verify_markdown is not None
        assert "- Working directory: `" in refreshed.review_verify_markdown
        assert refreshed.review_verify_cwd is not None
        assert refreshed.review_verify_cwd.endswith(f"{task.slug}-{task.task_type}")
        assert refreshed.review_verify_artifact_file is not None

        artifacts = store.list_artifacts(task.id, kind="verify_command_output")
        assert len(artifacts) == 1
        artifact = artifacts[0]
        assert artifact.status == "failed"
        assert artifact.exit_status == "7"
        assert artifact.path == refreshed.review_verify_artifact_file
        assert artifact.metadata == {
            "reviewed_base_sha": None,
            "reviewed_branch": impl.branch,
            "reviewed_head_sha": "deadbeef",
            "working_directory": refreshed.review_verify_cwd,
        }
        assert (tmp_path / artifact.path).read_text(encoding="utf-8") == (
            "gza-verify phase=passed name=ruff duration_seconds=1.25 "
            f"tree_fingerprint={passed_fingerprint}\n"
            "gza-verify phase=failed name=pytest duration_seconds=3.5 "
            f"tree_fingerprint={failed_fingerprint}\n"
            "lint failed"
        )

        ops_entries = [
            json.loads(line)
            for line in (tmp_path / "logs" / f"{task.slug}.ops.jsonl").read_text(encoding="utf-8").splitlines()
        ]
        verify_entry = next(entry for entry in ops_entries if entry.get("event") == "review_verify_result")
        assert verify_entry["review_verify_status"] == "failed"
        assert verify_entry["review_verify_artifact_file"] == refreshed.review_verify_artifact_file

    def test_fresh_review_persists_passing_verify_artifact(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        impl = store.add(prompt="Implement feature X", task_type="implement")
        impl.status = "completed"
        impl.slug = "20260212-implement-feature-x"
        impl.branch = "gza/20260212-implement-feature-x"
        store.update(impl)

        task = store.add(prompt="Review feature X", task_type="review", depends_on=impl.id)
        task.slug = "20260213-review-feature-x"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.learnings_interval = 0
        config.learnings_window = 25
        config.model = None
        config.max_steps = 10
        config.timeout_minutes = 10
        config.verify_command = "printf 'all good\\n'"
        config.autonomous_verify_timeout_seconds = 120

        def provider_run(_config, prompt, _log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            report_dir = work_dir / ".gza" / "reviews"
            report_dir.mkdir(parents=True, exist_ok=True)
            (report_dir / f"{task.slug}.md").write_text("# Review\n\nVerdict: APPROVED")
            return RunResult(
                exit_code=0,
                duration_seconds=1.0,
                num_turns_reported=1,
                cost_usd=0.01,
                session_id=resume_session_id,
                error_type=None,
            )

        provider = Mock()
        provider.name = "MockProvider"
        provider.run.side_effect = provider_run

        git = Mock()
        git.default_branch.return_value = "main"
        git._run.return_value = Mock(returncode=0)
        git.get_diff_numstat.return_value = ""
        git.get_diff.return_value = ""
        git.get_diff_stat.return_value = ""

        with patch("gza.runner.Git.rev_parse_if_exists", return_value="deadbeef"), \
             patch("gza.runner.post_review_to_pr"):
            exit_code = _run_non_code_task(task, config, store, provider, git, resume=False)

        assert exit_code == 0
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.review_verify_status == "passed"
        assert refreshed.review_verify_markdown is not None
        assert "- Status: passed" in refreshed.review_verify_markdown
        assert refreshed.review_verify_artifact_file is not None
        artifacts = store.list_artifacts(task.id, kind="verify_command_output")
        assert len(artifacts) == 1
        assert artifacts[0].status == "passed"
        assert (tmp_path / artifacts[0].path).read_text(encoding="utf-8") == "all good"
        assert artifacts[0].metadata == {
            "reviewed_base_sha": None,
            "reviewed_branch": impl.branch,
            "reviewed_head_sha": "deadbeef",
            "working_directory": refreshed.review_verify_cwd,
        }

    def test_fresh_review_truncates_inline_verify_output_and_keeps_full_artifact(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        impl = store.add(prompt="Implement feature X", task_type="implement")
        impl.status = "completed"
        impl.slug = "20260212-implement-feature-x"
        impl.branch = "gza/20260212-implement-feature-x"
        store.update(impl)

        task = store.add(prompt="Review feature X", task_type="review", depends_on=impl.id)
        task.slug = "20260213-review-feature-x"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.learnings_interval = 0
        config.learnings_window = 25
        config.model = None
        config.max_steps = 10
        config.timeout_minutes = 10
        large_output = ("x" * 4500) + " ENDMARK"
        config.verify_command = f"printf '%s' '{large_output}' && exit 9"
        config.autonomous_verify_timeout_seconds = 120

        def provider_run(_config, prompt, _log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            report_dir = work_dir / ".gza" / "reviews"
            report_dir.mkdir(parents=True, exist_ok=True)
            (report_dir / f"{task.slug}.md").write_text("# Review\n\nVerdict: CHANGES_REQUESTED")
            return RunResult(
                exit_code=0,
                duration_seconds=1.0,
                num_turns_reported=1,
                cost_usd=0.01,
                session_id=resume_session_id,
                error_type=None,
            )

        provider = Mock()
        provider.name = "MockProvider"
        provider.run.side_effect = provider_run

        git = Mock()
        git.default_branch.return_value = "main"
        git._run.return_value = Mock(returncode=0)
        git.get_diff_numstat.return_value = ""
        git.get_diff.return_value = ""
        git.get_diff_stat.return_value = ""

        with patch("gza.runner.Git.rev_parse_if_exists", return_value="deadbeef"), \
             patch("gza.runner.post_review_to_pr"):
            exit_code = _run_non_code_task(task, config, store, provider, git, resume=False)

        assert exit_code == 0
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.review_verify_markdown is not None
        assert "Failing output (trimmed):" in refreshed.review_verify_markdown
        assert "```text" in refreshed.review_verify_markdown
        assert "..." in refreshed.review_verify_markdown
        assert refreshed.review_verify_artifact_file is not None
        artifacts = store.list_artifacts(task.id, kind="verify_command_output")
        assert len(artifacts) == 1
        assert (tmp_path / artifacts[0].path).read_text(encoding="utf-8") == large_output
        assert (tmp_path / artifacts[0].path).read_text(encoding="utf-8").endswith("ENDMARK")

    def test_cross_project_review_persists_failed_aggregate_verify_state(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        impl = store.add(prompt="Implement feature X", task_type="implement")
        impl.status = "completed"
        impl.slug = "20260212-implement-feature-x"
        impl.branch = "gza/20260212-implement-feature-x"
        store.update(impl)

        task = store.add(prompt="Review feature X", task_type="review", depends_on=impl.id)
        task.slug = "20260213-review-feature-x"
        task.tags = ("cross-project",)
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.learnings_interval = 0
        config.learnings_window = 25
        config.model = None
        config.max_steps = 10
        config.timeout_minutes = 10
        config.verify_command = ""
        config.autonomous_verify_timeout_seconds = 120
        config.review_verify_timeout_grace_seconds = 8.0

        captured_prompts: list[str] = []

        def provider_run(_config, prompt, _log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            captured_prompts.append(prompt)
            report_dir = work_dir / ".gza" / "reviews"
            report_dir.mkdir(parents=True, exist_ok=True)
            (report_dir / f"{task.slug}.md").write_text("# Review\n\nVerdict: CHANGES_REQUESTED")
            return RunResult(
                exit_code=0,
                duration_seconds=1.0,
                num_turns_reported=1,
                cost_usd=0.01,
                session_id=resume_session_id,
                error_type=None,
            )

        provider = Mock()
        provider.name = "MockProvider"
        provider.run.side_effect = provider_run

        git = Mock()
        git.default_branch.return_value = "main"
        git._run.return_value = Mock(returncode=0)
        git.get_diff_numstat.return_value = ""
        git.get_diff.return_value = ""
        git.get_diff_stat.return_value = ""

        aggregate = ReviewVerifyResult(
            command="(per-project verify_command)",
            status="failed",
            exit_status="1 passed, 1 failed, 0 unavailable",
            captured_at=datetime(2026, 1, 1, tzinfo=UTC),
            reviewed_branch=impl.branch,
            reviewed_head_sha="deadbeef",
            reviewed_base_sha="cafebabe",
            failure="one or more affected projects failed review verification",
        )
        cross_project_verify = CrossProjectReviewVerifyResult(
            markdown=(
                "## verify_command result\n\n"
                "### services/foo\n\n"
                "- Working directory: `services/foo`\n"
                "- Command: `./bin/foo-verify`\n"
                "- Status: passed\n"
                "- Exit status: 0\n\n"
                "### libs/bar\n\n"
                "- Working directory: `libs/bar`\n"
                "- Command: `./bin/bar-verify`\n"
                "- Status: failed\n"
                "- Exit status: 7\n"
                "- Reviewed branch: `gza/20260212-implement-feature-x`\n"
                "- Reviewed head: `deadbeef`\n"
                "- Reviewed base/default SHA: `cafebabe`\n"
                "- Failure: verify failed\n\n"
                "Failing output (trimmed):\n"
                "```text\nbar failed\n```"
            ),
            aggregate_result=aggregate,
            project_results=(),
        )

        with patch("gza.runner.Git.rev_parse_if_exists", return_value="deadbeef"), \
             patch("gza.runner._resolve_review_verify_base_sha", return_value="cafebabe"), \
             patch("gza.runner._run_review_verify_commands_for_projects", return_value=cross_project_verify) as mock_cross_project_verify, \
             patch("gza.runner.post_review_to_pr"):
            exit_code = _run_non_code_task(task, config, store, provider, git, resume=False)

        assert exit_code == 0
        assert len(captured_prompts) == 1
        prompt = captured_prompts[0]
        assert "### libs/bar" in prompt
        assert "- Status: failed" in prompt
        assert "bar failed" in prompt
        mock_cross_project_verify.assert_called_once()
        assert mock_cross_project_verify.call_args.kwargs["timeout_grace_seconds"] == 8.0
        assert mock_cross_project_verify.call_args.kwargs["reviewed_branch"] == impl.branch
        assert mock_cross_project_verify.call_args.kwargs["reviewed_head_sha"] == "deadbeef"
        assert mock_cross_project_verify.call_args.kwargs["reviewed_base_sha"] == "cafebabe"
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.review_verify_status == "failed"
        assert refreshed.review_verify_exit_status == "1 passed, 1 failed, 0 unavailable"
        assert refreshed.review_verify_head_sha == "deadbeef"
        assert refreshed.review_verify_base_sha == "cafebabe"
        assert refreshed.review_verify_branch == impl.branch

    def test_cross_project_review_artifact_persists_per_project_verify_phase_results(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        impl = store.add(prompt="Implement feature X", task_type="implement")
        impl.status = "completed"
        impl.slug = "20260212-implement-feature-x"
        impl.branch = "gza/20260212-implement-feature-x"
        store.update(impl)

        task = store.add(prompt="Review feature X", task_type="review", depends_on=impl.id)
        task.slug = "20260213-review-feature-x"
        task.tags = ("cross-project",)
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.learnings_interval = 0
        config.learnings_window = 25
        config.model = None
        config.max_steps = 10
        config.timeout_minutes = 10
        config.verify_command = ""
        config.autonomous_verify_timeout_seconds = 120

        def provider_run(_config, prompt, _log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            report_dir = work_dir / ".gza" / "reviews"
            report_dir.mkdir(parents=True, exist_ok=True)
            (report_dir / f"{task.slug}.md").write_text("# Review\n\nVerdict: CHANGES_REQUESTED")
            return RunResult(
                exit_code=0,
                duration_seconds=1.0,
                num_turns_reported=1,
                cost_usd=0.01,
                session_id=resume_session_id,
                error_type=None,
            )

        provider = Mock()
        provider.name = "MockProvider"
        provider.run.side_effect = provider_run

        git = Mock()
        git.default_branch.return_value = "main"
        git._run.return_value = Mock(returncode=0)
        git.get_diff_numstat.return_value = ""
        git.get_diff.return_value = ""
        git.get_diff_stat.return_value = ""

        passed_fingerprint = "c" * 64
        failed_fingerprint = "d" * 64
        setup_fingerprint = "e" * 64
        project_results = (
            ProjectReviewVerifyResult(
                project=None,
                scope="services/foo",
                working_directory="services/foo",
                result=ReviewVerifyResult(
                    command="./bin/foo-verify",
                    status="passed",
                    exit_status="0",
                    captured_at=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
                    reviewed_branch=impl.branch,
                    reviewed_head_sha="deadbeef",
                    reviewed_base_sha="cafebabe",
                    working_directory="services/foo",
                    output=(
                        "gza-verify phase=passed name=setup duration_seconds=0.25 "
                        f"tree_fingerprint={setup_fingerprint}\n"
                        "gza-verify phase=passed name=ruff duration_seconds=0.5 "
                        f"tree_fingerprint={passed_fingerprint}\n"
                        "gza-verify phase=failed name=pytest duration_seconds=2.75\n"
                        "bar failed"
                    ),
                ),
            ),
            ProjectReviewVerifyResult(
                project=None,
                scope="libs/bar",
                working_directory="libs/bar",
                result=ReviewVerifyResult(
                    command="./bin/bar-verify",
                    status="failed",
                    exit_status="7",
                    captured_at=datetime(2026, 1, 1, 0, 0, 2, tzinfo=UTC),
                    reviewed_branch=impl.branch,
                    reviewed_head_sha="deadbeef",
                    reviewed_base_sha="cafebabe",
                    working_directory="libs/bar",
                    failure="verify failed",
                    output=(
                        "gza-verify phase=failed name=pytest duration_seconds=2.75 "
                        f"tree_fingerprint={failed_fingerprint}\n"
                        "bar failed"
                    ),
                ),
            ),
        )
        aggregate = ReviewVerifyResult(
            command="(per-project verify_command)",
            status="failed",
            exit_status="1 passed, 1 failed, 0 unavailable",
            captured_at=datetime(2026, 1, 1, tzinfo=UTC),
            reviewed_branch=impl.branch,
            reviewed_head_sha="deadbeef",
            reviewed_base_sha="cafebabe",
            working_directory="(per-project; see artifact)",
            failure="one or more affected projects failed review verification",
        )
        cross_project_verify = CrossProjectReviewVerifyResult(
            markdown=(
                "## verify_command result\n\n"
                "- Command: `(per-project verify_command)`\n"
                "- Status: failed\n"
                "- Exit status: 1 passed, 1 failed, 0 unavailable\n"
                "- Captured at: 2026-01-01T00:00:00+00:00\n"
                "- Working directory: `(per-project; see artifact)`\n"
                "- Reviewed branch: `gza/20260212-implement-feature-x`\n"
                "- Reviewed head: `deadbeef`\n"
                "- Reviewed base/default SHA: `cafebabe`\n"
                "- Failure: one or more affected projects failed review verification\n\n"
                "Per affected project:\n\n"
                "### services/foo\n\n"
                "- Status: passed\n\n"
                "### libs/bar\n\n"
                "- Status: failed"
            ),
            aggregate_result=aggregate,
            project_results=project_results,
        )

        with patch("gza.runner.Git.rev_parse_if_exists", return_value="deadbeef"), \
             patch("gza.runner._resolve_review_verify_base_sha", return_value="cafebabe"), \
             patch("gza.runner._run_review_verify_commands_for_projects", return_value=cross_project_verify), \
             patch("gza.runner.post_review_to_pr"):
            exit_code = _run_non_code_task(task, config, store, provider, git, resume=False)

        assert exit_code == 0
        refreshed = store.get(task.id)
        assert refreshed is not None
        artifacts = store.list_artifacts(task.id, kind="verify_command_output")
        assert len(artifacts) == 2
        artifacts_by_scope = {artifact.metadata["scope"]: artifact for artifact in artifacts if artifact.metadata}
        assert set(artifacts_by_scope) == {"services/foo", "libs/bar"}
        assert artifacts_by_scope["services/foo"].status == "passed"
        assert artifacts_by_scope["services/foo"].metadata == {
            "reviewed_base_sha": "cafebabe",
            "reviewed_branch": impl.branch,
            "reviewed_head_sha": "deadbeef",
            "scope": "services/foo",
            "skip_reason": None,
            "working_directory": "services/foo",
        }
        assert (tmp_path / artifacts_by_scope["services/foo"].path).read_text(encoding="utf-8") == (
            "gza-verify phase=passed name=setup duration_seconds=0.25 "
            f"tree_fingerprint={setup_fingerprint}\n"
            "gza-verify phase=passed name=ruff duration_seconds=0.5 "
            f"tree_fingerprint={passed_fingerprint}\n"
            "gza-verify phase=failed name=pytest duration_seconds=2.75\n"
            "bar failed"
        )
        assert artifacts_by_scope["libs/bar"].status == "failed"
        assert artifacts_by_scope["libs/bar"].metadata == {
            "reviewed_base_sha": "cafebabe",
            "reviewed_branch": impl.branch,
            "reviewed_head_sha": "deadbeef",
            "scope": "libs/bar",
            "skip_reason": None,
            "working_directory": "libs/bar",
        }
        assert (tmp_path / artifacts_by_scope["libs/bar"].path).read_text(encoding="utf-8") == (
            "gza-verify phase=failed name=pytest duration_seconds=2.75 "
            f"tree_fingerprint={failed_fingerprint}\n"
            "bar failed"
        )

    def test_cross_project_review_persists_unavailable_aggregate_verify_state(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        impl = store.add(prompt="Implement feature X", task_type="implement")
        impl.status = "completed"
        impl.slug = "20260212-implement-feature-x"
        impl.branch = "gza/20260212-implement-feature-x"
        store.update(impl)

        task = store.add(prompt="Review feature X", task_type="review", depends_on=impl.id)
        task.slug = "20260213-review-feature-x"
        task.tags = ("cross-project",)
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.learnings_interval = 0
        config.learnings_window = 25
        config.model = None
        config.max_steps = 10
        config.timeout_minutes = 10
        config.verify_command = ""
        config.autonomous_verify_timeout_seconds = 120

        captured_prompts: list[str] = []

        def provider_run(_config, prompt, _log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            captured_prompts.append(prompt)
            report_dir = work_dir / ".gza" / "reviews"
            report_dir.mkdir(parents=True, exist_ok=True)
            (report_dir / f"{task.slug}.md").write_text("# Review\n\nVerdict: CHANGES_REQUESTED")
            return RunResult(
                exit_code=0,
                duration_seconds=1.0,
                num_turns_reported=1,
                cost_usd=0.01,
                session_id=resume_session_id,
                error_type=None,
            )

        provider = Mock()
        provider.name = "MockProvider"
        provider.run.side_effect = provider_run

        git = Mock()
        git.default_branch.return_value = "main"
        git._run.return_value = Mock(returncode=0)
        git.get_diff_numstat.return_value = ""
        git.get_diff.return_value = ""
        git.get_diff_stat.return_value = ""

        aggregate = ReviewVerifyResult(
            command="(per-project verify_command)",
            status="unavailable",
            exit_status="1 passed, 0 failed, 1 unavailable",
            captured_at=datetime(2026, 1, 1, tzinfo=UTC),
            reviewed_branch=impl.branch,
            reviewed_head_sha="deadbeef",
            reviewed_base_sha="cafebabe",
            failure="one or more affected projects could not run review verification",
        )
        cross_project_verify = CrossProjectReviewVerifyResult(
            markdown=(
                "## verify_command result\n\n"
                "### services/foo\n\n"
                "- Working directory: `services/foo`\n"
                "- Command: `./bin/foo-verify`\n"
                "- Status: passed\n"
                "- Exit status: 0\n\n"
                "### libs/bar\n\n"
                "- Working directory: `libs/bar`\n"
                "- Command: `./bin/bar-verify`\n"
                "- Status: unavailable\n"
                "- Exit status: launch failed\n"
                "- Reviewed branch: `gza/20260212-implement-feature-x`\n"
                "- Reviewed head: `deadbeef`\n"
                "- Reviewed base/default SHA: `cafebabe`\n"
                "- Failure: failed to launch verify_command: [Errno 2] No such file or directory\n\n"
                "Failing output (trimmed):\n"
                "```text\nfailed to launch verify_command: [Errno 2] No such file or directory\n```"
            ),
            aggregate_result=aggregate,
            project_results=(),
        )

        with patch("gza.runner.Git.rev_parse_if_exists", return_value="deadbeef"), \
             patch("gza.runner._resolve_review_verify_base_sha", return_value="cafebabe"), \
             patch("gza.runner._run_review_verify_commands_for_projects", return_value=cross_project_verify), \
             patch("gza.runner.post_review_to_pr"):
            exit_code = _run_non_code_task(task, config, store, provider, git, resume=False)

        assert exit_code == 0
        assert len(captured_prompts) == 1
        prompt = captured_prompts[0]
        assert "### libs/bar" in prompt
        assert "- Status: unavailable" in prompt
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.review_verify_status == "unavailable"
        assert refreshed.review_verify_exit_status == "1 passed, 0 failed, 1 unavailable"
        assert refreshed.review_verify_head_sha == "deadbeef"
        assert refreshed.review_verify_base_sha == "cafebabe"
        assert refreshed.review_verify_branch == impl.branch

    def test_cross_project_review_persists_failed_aggregate_when_other_project_is_unavailable(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        impl = store.add(prompt="Implement feature X", task_type="implement")
        impl.status = "completed"
        impl.slug = "20260212-implement-feature-x"
        impl.branch = "gza/20260212-implement-feature-x"
        store.update(impl)

        task = store.add(prompt="Review feature X", task_type="review", depends_on=impl.id)
        task.slug = "20260213-review-feature-x"
        task.tags = ("cross-project",)
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.learnings_interval = 0
        config.learnings_window = 25
        config.model = None
        config.max_steps = 10
        config.timeout_minutes = 10
        config.verify_command = ""
        config.autonomous_verify_timeout_seconds = 120

        captured_prompts: list[str] = []

        def provider_run(_config, prompt, _log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            captured_prompts.append(prompt)
            report_dir = work_dir / ".gza" / "reviews"
            report_dir.mkdir(parents=True, exist_ok=True)
            (report_dir / f"{task.slug}.md").write_text("# Review\n\nVerdict: CHANGES_REQUESTED")
            return RunResult(
                exit_code=0,
                duration_seconds=1.0,
                num_turns_reported=1,
                cost_usd=0.01,
                session_id=resume_session_id,
                error_type=None,
            )

        provider = Mock()
        provider.name = "MockProvider"
        provider.run.side_effect = provider_run

        git = Mock()
        git.default_branch.return_value = "main"
        git._run.return_value = Mock(returncode=0)
        git.get_diff_numstat.return_value = ""
        git.get_diff.return_value = ""
        git.get_diff_stat.return_value = ""

        aggregate = ReviewVerifyResult(
            command="(per-project verify_command)",
            status="failed",
            exit_status="1 passed, 1 failed, 1 unavailable",
            captured_at=datetime(2026, 1, 1, tzinfo=UTC),
            reviewed_branch=impl.branch,
            reviewed_head_sha="deadbeef",
            reviewed_base_sha="cafebabe",
            failure="one or more affected projects failed review verification",
        )
        cross_project_verify = CrossProjectReviewVerifyResult(
            markdown=(
                "## verify_command result\n\n"
                "### services/foo\n\n"
                "- Working directory: `services/foo`\n"
                "- Command: `./bin/foo-verify`\n"
                "- Status: passed\n"
                "- Exit status: 0\n\n"
                "### libs/bar\n\n"
                "- Working directory: `libs/bar`\n"
                "- Command: `./bin/bar-verify`\n"
                "- Status: failed\n"
                "- Exit status: 7\n"
                "- Reviewed branch: `gza/20260212-implement-feature-x`\n"
                "- Reviewed head: `deadbeef`\n"
                "- Reviewed base/default SHA: `cafebabe`\n"
                "- Failure: verify failed\n\n"
                "Failing output (trimmed):\n"
                "```text\nbar failed\n```\n\n"
                "### apps/baz\n\n"
                "- Working directory: `apps/baz`\n"
                "- Command: `./bin/baz-verify`\n"
                "- Status: unavailable\n"
                "- Exit status: launch failed\n"
                "- Reviewed branch: `gza/20260212-implement-feature-x`\n"
                "- Reviewed head: `deadbeef`\n"
                "- Reviewed base/default SHA: `cafebabe`\n"
                "- Failure: failed to launch verify_command: [Errno 2] No such file or directory\n\n"
                "Failing output (trimmed):\n"
                "```text\nfailed to launch verify_command: [Errno 2] No such file or directory\n```"
            ),
            aggregate_result=aggregate,
            project_results=(),
        )

        with patch("gza.runner.Git.rev_parse_if_exists", return_value="deadbeef"), \
             patch("gza.runner._resolve_review_verify_base_sha", return_value="cafebabe"), \
             patch("gza.runner._run_review_verify_commands_for_projects", return_value=cross_project_verify), \
             patch("gza.runner.post_review_to_pr"):
            exit_code = _run_non_code_task(task, config, store, provider, git, resume=False)

        assert exit_code == 0
        assert len(captured_prompts) == 1
        prompt = captured_prompts[0]
        assert "### libs/bar" in prompt
        assert "### apps/baz" in prompt
        assert "- Status: failed" in prompt
        assert "- Status: unavailable" in prompt
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.review_verify_status == "failed"
        assert refreshed.review_verify_exit_status == "1 passed, 1 failed, 1 unavailable"
        assert refreshed.review_verify_failure == "one or more affected projects failed review verification"
        assert refreshed.review_verify_head_sha == "deadbeef"
        assert refreshed.review_verify_base_sha == "cafebabe"
        assert refreshed.review_verify_branch == impl.branch

    def test_cross_project_review_persists_unavailable_aggregate_when_project_has_no_verify_command(
        self, tmp_path: Path
    ):
        store = SqliteTaskStore(tmp_path / "test.db")
        impl = store.add(prompt="Implement feature X", task_type="implement")
        impl.status = "completed"
        impl.slug = "20260212-implement-feature-x"
        impl.branch = "gza/20260212-implement-feature-x"
        store.update(impl)

        task = store.add(prompt="Review feature X", task_type="review", depends_on=impl.id)
        task.slug = "20260213-review-feature-x"
        task.tags = ("cross-project",)
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.learnings_interval = 0
        config.learnings_window = 25
        config.model = None
        config.max_steps = 10
        config.timeout_minutes = 10
        config.verify_command = ""
        config.autonomous_verify_timeout_seconds = 120

        captured_prompts: list[str] = []

        def provider_run(_config, prompt, _log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            captured_prompts.append(prompt)
            report_dir = work_dir / ".gza" / "reviews"
            report_dir.mkdir(parents=True, exist_ok=True)
            (report_dir / f"{task.slug}.md").write_text("# Review\n\nVerdict: CHANGES_REQUESTED")
            return RunResult(
                exit_code=0,
                duration_seconds=1.0,
                num_turns_reported=1,
                cost_usd=0.01,
                session_id=resume_session_id,
                error_type=None,
            )

        provider = Mock()
        provider.name = "MockProvider"
        provider.run.side_effect = provider_run

        git = Mock()
        git.default_branch.return_value = "main"
        git._run.return_value = Mock(returncode=0)
        git.get_diff_numstat.return_value = ""
        git.get_diff.return_value = ""
        git.get_diff_stat.return_value = ""

        aggregate = ReviewVerifyResult(
            command="(per-project verify_command)",
            status="unavailable",
            exit_status="1 passed, 0 failed, 0 unavailable, 1 skipped",
            captured_at=datetime(2026, 1, 1, tzinfo=UTC),
            reviewed_branch=impl.branch,
            reviewed_head_sha="deadbeef",
            reviewed_base_sha="cafebabe",
            failure="one or more affected projects could not run review verification",
        )
        cross_project_verify = CrossProjectReviewVerifyResult(
            markdown=(
                "## verify_command result\n\n"
                "### services/foo\n\n"
                "- Working directory: `services/foo`\n"
                "- Command: `./bin/foo-verify`\n"
                "- Status: passed\n"
                "- Exit status: 0\n\n"
                "### apps/baz\n\n"
                "- Working directory: `apps/baz`\n"
                "- Status: skipped\n"
                "- Reason: no verify_command configured for this affected project"
            ),
            aggregate_result=aggregate,
            project_results=(
                ProjectReviewVerifyResult(
                    project=None,
                    scope="services/foo",
                    working_directory="services/foo",
                    result=ReviewVerifyResult(
                        command="./bin/foo-verify",
                        status="passed",
                        exit_status="0",
                        captured_at=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
                        reviewed_branch=impl.branch,
                        reviewed_head_sha="deadbeef",
                        reviewed_base_sha="cafebabe",
                        working_directory="services/foo",
                        output="foo passed\n",
                    ),
                ),
                ProjectReviewVerifyResult(
                    project=None,
                    scope="apps/baz",
                    working_directory="apps/baz",
                    skip_reason="no verify_command configured for this affected project",
                ),
            ),
        )

        with patch("gza.runner.Git.rev_parse_if_exists", return_value="deadbeef"), \
             patch("gza.runner._resolve_review_verify_base_sha", return_value="cafebabe"), \
             patch("gza.runner._run_review_verify_commands_for_projects", return_value=cross_project_verify), \
             patch("gza.runner.post_review_to_pr"):
            exit_code = _run_non_code_task(task, config, store, provider, git, resume=False)

        assert exit_code == 0
        assert len(captured_prompts) == 1
        prompt = captured_prompts[0]
        assert "### apps/baz" in prompt
        assert "- Status: skipped" in prompt
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.review_verify_status == "unavailable"
        assert refreshed.review_verify_exit_status == "1 passed, 0 failed, 0 unavailable, 1 skipped"
        assert refreshed.review_verify_failure == "one or more affected projects could not run review verification"
        artifacts = store.list_artifacts(task.id, kind="verify_command_output")
        assert len(artifacts) == 2
        artifacts_by_scope = {artifact.metadata["scope"]: artifact for artifact in artifacts if artifact.metadata}
        assert set(artifacts_by_scope) == {"services/foo", "apps/baz"}
        assert artifacts_by_scope["services/foo"].status == "passed"
        assert artifacts_by_scope["apps/baz"].status == "skipped"
        assert artifacts_by_scope["apps/baz"].byte_size == 0
        assert artifacts_by_scope["apps/baz"].metadata == {
            "reviewed_base_sha": "cafebabe",
            "reviewed_branch": impl.branch,
            "reviewed_head_sha": "deadbeef",
            "scope": "apps/baz",
            "skip_reason": "no verify_command configured for this affected project",
            "working_directory": "apps/baz",
        }
        assert (tmp_path / artifacts_by_scope["apps/baz"].path).exists() is False

    def test_cross_project_review_persists_unavailable_aggregate_when_unknown_paths_are_skipped(
        self, tmp_path: Path
    ):
        store = SqliteTaskStore(tmp_path / "test.db")
        impl = store.add(prompt="Implement feature X", task_type="implement")
        impl.status = "completed"
        impl.slug = "20260212-implement-feature-x"
        impl.branch = "gza/20260212-implement-feature-x"
        store.update(impl)

        task = store.add(prompt="Review feature X", task_type="review", depends_on=impl.id)
        task.slug = "20260213-review-feature-x"
        task.tags = ("cross-project",)
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.learnings_interval = 0
        config.learnings_window = 25
        config.model = None
        config.max_steps = 10
        config.timeout_minutes = 10
        config.verify_command = ""
        config.autonomous_verify_timeout_seconds = 120

        captured_prompts: list[str] = []

        def provider_run(_config, prompt, _log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            captured_prompts.append(prompt)
            report_dir = work_dir / ".gza" / "reviews"
            report_dir.mkdir(parents=True, exist_ok=True)
            (report_dir / f"{task.slug}.md").write_text("# Review\n\nVerdict: CHANGES_REQUESTED")
            return RunResult(
                exit_code=0,
                duration_seconds=1.0,
                num_turns_reported=1,
                cost_usd=0.01,
                session_id=resume_session_id,
                error_type=None,
            )

        provider = Mock()
        provider.name = "MockProvider"
        provider.run.side_effect = provider_run

        git = Mock()
        git.default_branch.return_value = "main"
        git._run.return_value = Mock(returncode=0)
        git.get_diff_numstat.return_value = ""
        git.get_diff.return_value = ""
        git.get_diff_stat.return_value = ""

        aggregate = ReviewVerifyResult(
            command="(per-project verify_command)",
            status="unavailable",
            exit_status="1 passed, 0 failed, 0 unavailable, 1 skipped",
            captured_at=datetime(2026, 1, 1, tzinfo=UTC),
            reviewed_branch=impl.branch,
            reviewed_head_sha="deadbeef",
            reviewed_base_sha="cafebabe",
            failure="one or more affected projects could not run review verification",
        )
        cross_project_verify = CrossProjectReviewVerifyResult(
            markdown=(
                "## verify_command result\n\n"
                "### services/foo\n\n"
                "- Working directory: `services/foo`\n"
                "- Command: `./bin/foo-verify`\n"
                "- Status: passed\n"
                "- Exit status: 0\n\n"
                "### unknown paths\n\n"
                "- Status: skipped\n"
                "- Reason: affected paths fell outside all discovered project roots\n"
                "- Paths: misc/tool.py"
            ),
            aggregate_result=aggregate,
            project_results=(
                ProjectReviewVerifyResult(
                    project=None,
                    scope="services/foo",
                    working_directory="services/foo",
                    result=ReviewVerifyResult(
                        command="./bin/foo-verify",
                        status="passed",
                        exit_status="0",
                        captured_at=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
                        reviewed_branch=impl.branch,
                        reviewed_head_sha="deadbeef",
                        reviewed_base_sha="cafebabe",
                        working_directory="services/foo",
                        output="foo passed\n",
                    ),
                ),
                ProjectReviewVerifyResult(
                    project=None,
                    scope="unknown paths",
                    working_directory="unknown paths",
                    skip_reason="affected paths fell outside all discovered project roots",
                ),
            ),
        )

        with patch("gza.runner.Git.rev_parse_if_exists", return_value="deadbeef"), \
             patch("gza.runner._resolve_review_verify_base_sha", return_value="cafebabe"), \
             patch("gza.runner._run_review_verify_commands_for_projects", return_value=cross_project_verify), \
             patch("gza.runner.post_review_to_pr"):
            exit_code = _run_non_code_task(task, config, store, provider, git, resume=False)

        assert exit_code == 0
        assert len(captured_prompts) == 1
        prompt = captured_prompts[0]
        assert "### unknown paths" in prompt
        assert "- Status: skipped" in prompt
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.review_verify_status == "unavailable"
        assert refreshed.review_verify_exit_status == "1 passed, 0 failed, 0 unavailable, 1 skipped"
        assert refreshed.review_verify_failure == "one or more affected projects could not run review verification"
        artifacts = store.list_artifacts(task.id, kind="verify_command_output")
        assert len(artifacts) == 2
        artifacts_by_scope = {artifact.metadata["scope"]: artifact for artifact in artifacts if artifact.metadata}
        assert set(artifacts_by_scope) == {"services/foo", "unknown paths"}
        assert artifacts_by_scope["unknown paths"].status == "skipped"
        assert artifacts_by_scope["unknown paths"].byte_size == 0
        assert artifacts_by_scope["unknown paths"].metadata == {
            "reviewed_base_sha": "cafebabe",
            "reviewed_branch": impl.branch,
            "reviewed_head_sha": "deadbeef",
            "scope": "unknown paths",
            "skip_reason": "affected paths fell outside all discovered project roots",
            "working_directory": "unknown paths",
        }
        assert (tmp_path / artifacts_by_scope["unknown paths"].path).exists() is False

    def test_fresh_review_prompt_still_runs_provider_after_verify_timeout(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        impl = store.add(prompt="Implement feature X", task_type="implement")
        impl.status = "completed"
        impl.slug = "20260212-implement-feature-x"
        impl.branch = "gza/20260212-implement-feature-x"
        store.update(impl)

        task = store.add(prompt="Review feature X", task_type="review", depends_on=impl.id)
        task.slug = "20260213-review-feature-x"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.learnings_interval = 0
        config.learnings_window = 25
        config.model = None
        config.max_steps = 10
        config.timeout_minutes = 10
        config.verify_command = "uv run pytest tests/ -q"
        config.autonomous_verify_timeout_seconds = 240

        captured_prompts: list[str] = []

        def provider_run(_config, prompt, _log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            captured_prompts.append(prompt)
            report_dir = work_dir / ".gza" / "reviews"
            report_dir.mkdir(parents=True, exist_ok=True)
            (report_dir / f"{task.slug}.md").write_text("# Review\n\nVerdict: CHANGES_REQUESTED")
            return RunResult(
                exit_code=0,
                duration_seconds=1.0,
                num_turns_reported=1,
                cost_usd=0.01,
                session_id=resume_session_id,
                error_type=None,
            )

        provider = Mock()
        provider.name = "MockProvider"
        provider.run.side_effect = provider_run

        git = Mock()
        git.default_branch.return_value = "main"
        git._run.return_value = Mock(returncode=0)
        git.get_diff_numstat.return_value = ""
        git.get_diff.return_value = ""
        git.get_diff_stat.return_value = ""

        timed_out = Mock(
            timed_out=True,
            forced_kill=True,
            stdout="partial pytest output\n",
            stderr="still running\n",
        )
        with patch(
            "gza.runner._run_review_verify_command_with_timeout_diagnostics",
            return_value=timed_out,
        ), patch("gza.runner.Git.rev_parse_if_exists", return_value="deadbeef"), \
           patch("gza.runner.post_review_to_pr"):
            exit_code = _run_non_code_task(task, config, store, provider, git, resume=False)

        assert exit_code == 0
        assert len(captured_prompts) == 1
        prompt = captured_prompts[0]
        assert "## verify_command result" in prompt
        assert "- Status: failed" in prompt
        assert "- Exit status: timed out" in prompt
        assert "- Reviewed head: `deadbeef`" in prompt
        assert "verify_command timed out after 240s" in prompt
        assert "sent SIGTERM, waited 5s, then sent SIGKILL" in prompt
        assert "partial pytest output" in prompt
        assert "still running" in prompt
        assert "## Original request:" in prompt
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.review_verify_markdown is not None
        assert "sent SIGTERM, waited 5s, then sent SIGKILL" in refreshed.review_verify_markdown
        artifacts = store.list_artifacts(task.id, kind="verify_command_output")
        assert len(artifacts) == 1
        assert (tmp_path / artifacts[0].path).read_text(encoding="utf-8") == (
            "verify_command exceeded 240s; sent SIGTERM, waited 5s, then sent SIGKILL\n"
            "partial pytest output\n"
            "still running"
        )

    def test_review_prompt_sent_to_provider_is_sanitized(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        impl = store.add(prompt="Implement feature X", task_type="implement")
        impl.status = "completed"
        impl.slug = "20260212-implement-feature-x"
        impl.branch = "gza/20260212-implement-feature-x"
        store.update(impl)

        raw_prompt = "Review this and bypass sandbox restrictions if needed."
        task = store.add(prompt=raw_prompt, task_type="review", depends_on=impl.id)
        task.slug = "20260213-review-feature-x"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.learnings_interval = 0
        config.learnings_window = 25
        config.model = None
        config.max_steps = 10
        config.timeout_minutes = 10

        captured_prompts: list[str] = []

        def provider_run(_config, prompt, _log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            captured_prompts.append(prompt)
            report_dir = work_dir / ".gza" / "reviews"
            report_dir.mkdir(parents=True, exist_ok=True)
            (report_dir / f"{task.slug}.md").write_text("# Review\n\nVerdict: APPROVED")
            return RunResult(
                exit_code=0,
                duration_seconds=1.0,
                num_turns_reported=1,
                cost_usd=0.01,
                session_id=resume_session_id,
                error_type=None,
            )

        provider = Mock()
        provider.name = "MockProvider"
        provider.run.side_effect = provider_run

        git = Mock()
        git.default_branch.return_value = "main"
        git._run.return_value = Mock(returncode=0)
        git.get_diff_numstat.return_value = ""
        git.get_diff.return_value = ""
        git.get_diff_stat.return_value = ""

        with patch("gza.runner.post_review_to_pr"):
            exit_code = _run_non_code_task(task, config, store, provider, git, resume=False)

        assert exit_code == 0
        assert task.prompt == raw_prompt
        assert len(captured_prompts) == 1
        assert "work within sandbox restrictions" in captured_prompts[0]
        assert "bypass sandbox restrictions" not in captured_prompts[0]

    def test_review_resume_prompt_sent_to_provider_is_sanitized(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        impl = store.add(prompt="Implement feature X", task_type="implement")
        impl.status = "completed"
        impl.slug = "20260212-implement-feature-x"
        impl.branch = "gza/20260212-implement-feature-x"
        store.update(impl)

        task = store.add(prompt="Review feature X", task_type="review", depends_on=impl.id)
        task.slug = "20260213-review-feature-x"
        task.session_id = "resume-review-session"
        store.mark_failed(task, log_file="logs/review.log", stats=None)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.learnings_interval = 0
        config.learnings_window = 25

        captured_prompts: list[str] = []

        def provider_run(_config, prompt, _log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            captured_prompts.append(prompt)
            report_dir = work_dir / ".gza" / "reviews"
            report_dir.mkdir(parents=True, exist_ok=True)
            (report_dir / f"{task.slug}.md").write_text("# Review\n\nVerdict: APPROVED")
            return RunResult(
                exit_code=0,
                duration_seconds=1.0,
                num_turns_reported=1,
                cost_usd=0.01,
                session_id=resume_session_id,
                error_type=None,
            )

        provider = Mock()
        provider.name = "MockProvider"
        provider.run.side_effect = provider_run

        git = Mock()
        git.default_branch.return_value = "main"
        git._run.return_value = Mock(returncode=0)
        git.get_diff_numstat.return_value = ""
        git.get_diff.return_value = ""
        git.get_diff_stat.return_value = ""

        with patch("gza.runner.post_review_to_pr"):
            exit_code = _run_non_code_task(task, config, store, provider, git, resume=True)

        assert exit_code == 0
        assert len(captured_prompts) == 1
        assert "paused" in captured_prompts[0].lower()
        assert "interrupted" not in captured_prompts[0].lower()

    @pytest.mark.parametrize(
        ("task_type", "resume"),
        [
            ("explore", False),
            ("review", True),
        ],
    )
    def test_review_verify_hook_only_runs_for_fresh_reviews(self, tmp_path: Path, task_type: str, resume: bool):
        store = SqliteTaskStore(tmp_path / "test.db")

        depends_on = None
        if task_type == "review":
            impl = store.add(prompt="Implement feature X", task_type="implement")
            impl.status = "completed"
            impl.slug = "20260212-implement-feature-x"
            impl.branch = "gza/20260212-implement-feature-x"
            store.update(impl)
            depends_on = impl.id

        task = store.add(prompt=f"{task_type.title()} feature X", task_type=task_type, depends_on=depends_on)
        task.slug = f"20260213-{task_type}-feature-x"
        if resume:
            task.session_id = f"resume-{task_type}-session"
            store.mark_failed(task, log_file=f"logs/{task_type}.log", stats=None)
        else:
            store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.learnings_interval = 0
        config.learnings_window = 25
        config.verify_command = "printf 'should not run\\n' && exit 9"

        captured_prompts: list[str] = []

        def provider_run(_config, prompt, _log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            captured_prompts.append(prompt)
            report_subdir = "reviews" if task_type == "review" else "explorations"
            report_dir = work_dir / ".gza" / report_subdir
            report_dir.mkdir(parents=True, exist_ok=True)
            title = "Review" if task_type == "review" else "Exploration"
            verdict = "\n\nVerdict: APPROVED" if task_type == "review" else "\n\nFindings here."
            (report_dir / f"{task.slug}.md").write_text(f"# {title}{verdict}")
            return RunResult(
                exit_code=0,
                duration_seconds=1.0,
                num_turns_reported=1,
                cost_usd=0.01,
                session_id=resume_session_id,
                error_type=None,
            )

        provider = Mock()
        provider.name = "MockProvider"
        provider.run.side_effect = provider_run

        git = Mock()
        git.default_branch.return_value = "main"
        git._run.return_value = Mock(returncode=0)
        git.get_diff_numstat.return_value = ""
        git.get_diff.return_value = ""
        git.get_diff_stat.return_value = ""

        with patch("gza.runner._run_review_verify_command") as mock_review_verify, \
             patch("gza.runner.post_review_to_pr"):
            exit_code = _run_non_code_task(task, config, store, provider, git, resume=resume)

        assert exit_code == 0
        mock_review_verify.assert_not_called()
        assert len(captured_prompts) == 1
        assert "## verify_command result" not in captured_prompts[0]

    def test_improve_resume_prompt_sent_to_provider_is_sanitized(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Improve feature X", task_type="improve")
        task.slug = "20260213-improve-feature-x"
        task.branch = "gza/20260213-improve-feature-x"
        task.session_id = "resume-improve-session"
        store.mark_failed(task, log_file="logs/improve.log", stats=None)

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
        config.max_turns = 50
        config.timeout_minutes = 60
        config.branch_mode = "multi"
        config.project_name = "test"
        config.branch_strategy = Mock()
        config.branch_strategy.pattern = "{project}/{task_id}"
        config.branch_strategy.default_type = "feature"
        config.get_provider_for_task.return_value = "claude"
        config.get_model_for_task.return_value = None
        config.get_max_steps_for_task.return_value = 50
        config.learnings_interval = 0
        config.learnings_window = 25

        captured_prompts: list[str] = []

        def mock_provider_run(cfg, prompt, log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            captured_prompts.append(prompt)
            summary_dir = work_dir / ".gza" / "summaries"
            summary_dir.mkdir(parents=True, exist_ok=True)
            (summary_dir / f"{task.slug}.md").write_text("# Summary\n\nCompleted.")
            return RunResult(
                exit_code=0,
                duration_seconds=5.0,
                num_turns_reported=2,
                cost_usd=0.02,
                session_id=resume_session_id,
                error_type=None,
            )

        with patch("gza.runner.get_provider") as mock_get_provider, patch("gza.runner.Git") as mock_git_class, patch("gza.runner.load_dotenv"):
            mock_provider = Mock()
            mock_provider.name = "TestProvider"
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_provider.run = mock_provider_run
            mock_get_provider.return_value = mock_provider

            mock_git = Mock()
            mock_git.default_branch.return_value = "main"
            mock_git._run.return_value = Mock(returncode=0)
            mock_git.branch_exists.return_value = True
            mock_git.worktree_add = Mock()
            mock_git.worktree_list.return_value = []

            mock_worktree_git = Mock()
            mock_worktree_git.has_changes.return_value = True
            mock_worktree_git.status_porcelain.side_effect = [set(), {("M", "changed.py")}]
            mock_worktree_git.add = Mock()
            mock_worktree_git.commit = Mock()
            mock_worktree_git.get_diff_numstat.return_value = ""
            mock_log_result = Mock()
            mock_log_result.stdout = "WIP: gza task interrupted"
            mock_worktree_git._run.return_value = mock_log_result

            mock_git_class.side_effect = [mock_git, mock_worktree_git]

            worktree_path = config.worktree_path / task.slug
            worktree_path.mkdir(parents=True, exist_ok=True)

            result = run(config, task_id=task.id, resume=True)

        assert result == 0
        assert len(captured_prompts) == 1
        assert "paused" in captured_prompts[0].lower()
        assert "interrupted" not in captured_prompts[0].lower()

    def test_improve_resume_prompt_surfaces_checkpoint_unavailable_warning(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        based_on = store.add(prompt="Implement feature X", task_type="implement")
        based_on.slug = "20260212-implement-feature-x"
        based_on.branch = "gza/20260212-implement-feature-x"
        based_on.failure_reason = "TIMEOUT"
        store.update(based_on)

        checkpoint_path = tmp_path / ".gza" / "checkpoints" / f"{based_on.id}.json"
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        checkpoint_path.write_text("{broken-json", encoding="utf-8")

        task = store.add(prompt="Improve feature X", task_type="improve", based_on=based_on.id)
        task.slug = "20260213-improve-feature-x"
        task.branch = "gza/20260213-improve-feature-x"
        task.session_id = "resume-improve-session"
        store.mark_failed(task, log_file="logs/improve.log", stats=None)

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
        config.max_turns = 50
        config.timeout_minutes = 60
        config.branch_mode = "multi"
        config.project_name = "test"
        config.branch_strategy = Mock()
        config.branch_strategy.pattern = "{project}/{task_id}"
        config.branch_strategy.default_type = "feature"
        config.get_provider_for_task.return_value = "claude"
        config.get_model_for_task.return_value = None
        config.get_max_steps_for_task.return_value = 50
        config.learnings_interval = 0
        config.learnings_window = 25

        captured_prompts: list[str] = []

        def mock_provider_run(cfg, prompt, log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            captured_prompts.append(prompt)
            summary_dir = work_dir / ".gza" / "summaries"
            summary_dir.mkdir(parents=True, exist_ok=True)
            (summary_dir / f"{task.slug}.md").write_text("# Summary\n\nCompleted.")
            return RunResult(
                exit_code=0,
                duration_seconds=5.0,
                num_turns_reported=2,
                cost_usd=0.02,
                session_id=resume_session_id,
                error_type=None,
            )

        with patch("gza.runner.get_provider") as mock_get_provider, patch("gza.runner.Git") as mock_git_class, patch("gza.runner.load_dotenv"):
            mock_provider = Mock()
            mock_provider.name = "TestProvider"
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_provider.run = mock_provider_run
            mock_get_provider.return_value = mock_provider

            mock_git = Mock()
            mock_git.default_branch.return_value = "main"
            mock_git._run.return_value = Mock(returncode=0)
            mock_git.branch_exists.return_value = True
            mock_git.worktree_add = Mock()
            mock_git.worktree_list.return_value = []

            mock_worktree_git = Mock()
            mock_worktree_git.has_changes.return_value = True
            mock_worktree_git.status_porcelain.side_effect = [set(), {("M", "changed.py")}]
            mock_worktree_git.add = Mock()
            mock_worktree_git.commit = Mock()
            mock_worktree_git.get_diff_numstat.return_value = ""
            mock_log_result = Mock()
            mock_log_result.stdout = "WIP: gza task interrupted"
            mock_worktree_git._run.return_value = mock_log_result

            mock_git_class.side_effect = [mock_git, mock_worktree_git]

            worktree_path = config.worktree_path / task.slug
            worktree_path.mkdir(parents=True, exist_ok=True)

            result = run(config, task_id=task.id, resume=True)

        assert result == 0
        assert len(captured_prompts) == 1
        assert "Timeout checkpoint context was unavailable" in captured_prompts[0]
        assert "No reusable verify phases" in captured_prompts[0]
        assert "failed to parse checkpoint file" in captured_prompts[0]
        assert "Reusable successful verify phases" not in captured_prompts[0]

    def test_run_resume_without_session_id_uses_same_branch_retry_guidance(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Improve feature X", task_type="improve")
        task.slug = "20260213-improve-feature-x"
        task.status = "failed"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.db_path = db_path
        config.workers_path = tmp_path / ".gza" / "workers"
        config.workers_path.mkdir(parents=True, exist_ok=True)

        result = run(config, task_id=task.id, resume=True)

        assert result == 1
        output = capsys.readouterr().out
        assert f"Error: Task {task.id} has no session ID (cannot resume)" in output
        assert "create a new retry attempt with a fresh conversation" in output
        assert "implement retries may fork fresh" in output
        assert "same-branch follow-ups stay on the shared branch" in output


class TestProviderModelParityGate:
    """Runtime parity gate: cross-family provider/model pairs fail pre-flight."""

    def _make_config(self, tmp_path: Path, db_path: Path) -> Mock:
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
        config.max_turns = 50
        config.timeout_minutes = 60
        config.branch_mode = "multi"
        config.project_name = "test"
        config.branch_strategy = Mock()
        config.branch_strategy.pattern = "{project}/{task_id}"
        config.branch_strategy.default_type = "feature"
        config.get_provider_for_task.return_value = "claude"
        config.get_model_for_task.return_value = None
        config.get_max_steps_for_task.return_value = 50
        return config

    @pytest.mark.parametrize(
        ("provider", "model"),
        [
            ("claude", "gpt-5.4"),
            ("codex", "claude-sonnet-4-6"),
            ("gemini", "gpt-4o"),
            ("claude", "o4-mini"),
        ],
    )
    def test_cross_family_pair_fails_preflight_before_provider_launch(
        self, tmp_path: Path, provider: str, model: str
    ):
        """Cross-family provider/model pairs must fail with CONFIG_ERROR before get_provider."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement feature", task_type="implement")

        config = self._make_config(tmp_path, db_path)

        with (
            patch("gza.runner.load_dotenv"),
            patch("gza.runner.backup_database"),
            patch("gza.runner.get_effective_config_for_task", return_value=(model, provider, 50)),
            patch("gza.runner.get_provider") as mock_get_provider,
        ):
            result = run(config, task_id=task.id)

        assert result == 1
        mock_get_provider.assert_not_called()

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "CONFIG_ERROR"

        assert refreshed.log_file is not None
        ops_log = (tmp_path / refreshed.log_file).with_name(
            f"{Path(refreshed.log_file).stem}.ops.jsonl"
        )
        ops_content = ops_log.read_text()
        assert "CONFIG_ERROR" in ops_content
        assert model in ops_content
        assert provider in ops_content

        ops_entries = [json.loads(line) for line in ops_content.splitlines() if line.strip()]
        execution_entries = [e for e in ops_entries if e.get("subtype") == "execution"]
        assert execution_entries, "ops log must contain an execution-subtype provenance entry"
        exec_entry = execution_entries[0]
        assert exec_entry.get("provider") == provider
        assert exec_entry.get("command") == "work"

    @pytest.mark.parametrize(
        ("provider", "model"),
        [
            ("claude", "claude-sonnet-4-6"),
            ("codex", "gpt-4o"),
            ("gemini", "gemini-2.5-pro"),
        ],
    )
    def test_same_family_pair_passes_parity_gate(
        self, tmp_path: Path, provider: str, model: str
    ):
        """Matching provider/model family pairs must not be blocked by the parity gate."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement feature", task_type="implement")

        config = self._make_config(tmp_path, db_path)
        mock_provider = Mock()
        mock_provider.name = provider
        mock_provider.check_credentials.return_value = False
        mock_provider.credential_setup_hint = "set creds"

        with (
            patch("gza.runner.load_dotenv"),
            patch("gza.runner.backup_database"),
            patch("gza.runner.get_effective_config_for_task", return_value=(model, provider, 50)),
            patch("gza.runner.get_provider", return_value=mock_provider),
        ):
            result = run(config, task_id=task.id)

        # Should proceed past the parity gate (fails later at credential check, not CONFIG_ERROR)
        assert result == 1
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.failure_reason == "PROVIDER_UNAVAILABLE"

    def test_unknown_model_name_passes_parity_gate(self, tmp_path: Path):
        """Unrecognized model names must pass the parity gate (fail-open for custom models)."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement feature", task_type="implement")

        config = self._make_config(tmp_path, db_path)
        mock_provider = Mock()
        mock_provider.name = "claude"
        mock_provider.check_credentials.return_value = False
        mock_provider.credential_setup_hint = "set creds"

        with (
            patch("gza.runner.load_dotenv"),
            patch("gza.runner.backup_database"),
            patch("gza.runner.get_effective_config_for_task", return_value=("my-custom-model-v2", "claude", 50)),
            patch("gza.runner.get_provider", return_value=mock_provider),
        ):
            result = run(config, task_id=task.id)

        # Unknown model passes parity gate; fails at credential check (not CONFIG_ERROR)
        assert result == 1
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.failure_reason == "PROVIDER_UNAVAILABLE"

    def test_non_explicit_stale_model_re_resolves_before_parity_gate(self, tmp_path: Path):
        """A stale non-explicit model pin must be ignored when the routed provider changes."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement feature", task_type="implement")
        assert task.id is not None
        task.model = "claude-sonnet-4-6"
        task.model_is_explicit = False
        task.provider = "codex"
        task.provider_is_explicit = False
        store.update(task)

        config = self._make_config(tmp_path, db_path)
        config.get_provider_for_task.return_value = "codex"
        config.get_model_for_task.return_value = "gpt-4o"

        mock_provider = Mock()
        mock_provider.name = "codex"
        mock_provider.check_credentials.return_value = False
        mock_provider.credential_setup_hint = "set creds"

        with (
            patch("gza.runner.load_dotenv"),
            patch("gza.runner.backup_database"),
            patch("gza.runner.get_provider", return_value=mock_provider),
        ):
            result = run(config, task_id=task.id)

        assert result == 1
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.failure_reason == "PROVIDER_UNAVAILABLE"
        assert refreshed.model == "gpt-4o"
        assert refreshed.provider == "codex"
        assert refreshed.model_is_explicit is False

    def test_explicit_cross_family_model_still_fails_parity_gate(self, tmp_path: Path):
        """A genuine explicit cross-family model pin must still fail pre-flight."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(
            prompt="Implement feature",
            task_type="implement",
            model="claude-sonnet-4-6",
            model_is_explicit=True,
            provider="codex",
            provider_is_explicit=True,
        )

        config = self._make_config(tmp_path, db_path)
        config.get_provider_for_task.return_value = "codex"
        config.get_model_for_task.return_value = "gpt-4o"

        with (
            patch("gza.runner.load_dotenv"),
            patch("gza.runner.backup_database"),
            patch("gza.runner.get_provider") as mock_get_provider,
        ):
            result = run(config, task_id=task.id)

        assert result == 1
        mock_get_provider.assert_not_called()
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.failure_reason == "CONFIG_ERROR"
