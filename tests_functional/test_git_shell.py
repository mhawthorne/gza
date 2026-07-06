"""Functional regressions for CLI subprocess and real git shell-command flows."""

import argparse
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from gza import advance_engine as advance_engine_module
from gza.advance_engine import evaluate_advance_rules, resolve_advance_context
from gza.cli._common import run_with_recovery
from gza.cli.advance_engine import determine_next_action
from gza.cli.advance_executor import (
    AdvanceActionExecutionContext,
    execute_advance_action,
    resolve_execution_needs_attention,
)
from gza.cli.git_ops import _merge_single_task, _run_task_backed_rebase
from gza.config import Config
from gza.db import SCHEMA_VERSION, SqliteTaskStore, TaskStats, check_migration_status
from gza.git import Git, GitError, active_worktree_path_for_branch, cleanup_worktree_for_branch
from gza.github import PullRequest
from gza.log_paths import ops_log_path_for
from gza.recovery_engine import FailedRecoveryDecision
from gza.review_verdict import ParsedReviewReport
from gza.runner import WIP_DIR, _complete_code_task, _restore_wip_changes, _save_wip_changes, _squash_wip_commits
from tests.cli.conftest import make_store, setup_config
from tests.test_advance_engine import _make_store
from tests.test_db import _make_v24_db
from tests_functional.git_helpers import init_repo_with_remote_tracking_only_feature
from tests_functional.helpers.cli import run_gza_subprocess


class _FakeAvailableGitHub:
    def __init__(self, create_calls: list[tuple[str, str, str, str, bool]]) -> None:
        self._create_calls = create_calls

    @classmethod
    def cached_pr_support(cls) -> bool | None:
        return None

    def is_available(self) -> bool:
        return True

    def get_pr_details(self, _number: int):
        return None

    def discover_pr_by_branch(self, _branch: str):
        return None

    def create_pr(self, head: str, base: str, title: str, body: str, draft: bool = False) -> PullRequest:
        self._create_calls.append((head, base, title, body, draft))
        return PullRequest(url=f"https://example.test/{head}", number=len(self._create_calls))


def _init_repo_with_origin(tmp_path: Path) -> tuple[Config, SqliteTaskStore, Git, Path]:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    git = Git(tmp_path)
    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    (tmp_path / "README.md").write_text("initial\n", encoding="utf-8")
    git._run("add", "README.md")
    git._run("commit", "-m", "Initial commit")

    remote_dir = tmp_path / "origin.git"
    git._run("init", "--bare", str(remote_dir))
    git._run("remote", "add", "origin", str(remote_dir))
    git._run("push", "-u", "origin", "main")
    return config, store, git, remote_dir


def _prepare_stale_wip_branch_publication_failure(
    tmp_path: Path,
    *,
    branch: str,
    prompt: str,
    slug: str,
) -> tuple[Config, SqliteTaskStore, Git, object, Path, str, list[tuple[str, str, str, str, bool]]]:
    config, store, git, remote_dir = _init_repo_with_origin(tmp_path)
    git._run("checkout", "-b", branch)
    git._run("push", "-u", "origin", branch)

    (tmp_path / "feature.txt").write_text("local final\n", encoding="utf-8")
    git._run("add", "feature.txt")
    git._run("commit", "-m", "Feature final")
    local_tip = git.rev_parse("HEAD")

    remote_checkout = tmp_path / f"{branch.replace('/', '-')}-remote"
    git._run("clone", str(remote_dir), str(remote_checkout))
    remote_git = Git(remote_checkout)
    remote_git._run("config", "user.name", "Remote User")
    remote_git._run("config", "user.email", "remote@example.com")
    remote_git._run("checkout", branch)
    (remote_checkout / "wip.txt").write_text("stale savepoint\n", encoding="utf-8")
    remote_git._run("add", "wip.txt")
    remote_git._run("commit", "-m", "WIP: gza task interrupted")
    remote_git._run("push", "origin", branch)

    git.fetch("origin")
    assert git.count_commits_ahead(branch, f"origin/{branch}") == 1
    assert git.count_commits_ahead(f"origin/{branch}", branch) == 1

    task = store.add(prompt, task_type="implement", create_pr=True)
    assert task.id is not None
    task.slug = slug
    store.mark_in_progress(task)
    task = store.get(task.id)
    assert task is not None
    task.slug = slug
    store.update(task)

    log_file = tmp_path / "logs" / f"{slug}.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    log_file.write_text("", encoding="utf-8")
    summary_dir = tmp_path / ".gza" / "summaries"
    summary_path = summary_dir / f"{slug}.md"
    worktree_summary_path = tmp_path / ".gza" / "worktree-summaries" / f"{slug}.md"
    worktree_summary_path.parent.mkdir(parents=True, exist_ok=True)
    worktree_summary_path.write_text("summary\n", encoding="utf-8")

    create_pr_calls: list[tuple[str, str, str, str, bool]] = []
    with (
        patch("gza.pr_ops.GitHub", side_effect=lambda: _FakeAvailableGitHub(create_pr_calls)),
        patch("gza.runner.build_task_pr_content", return_value=("Test PR", "PR body")),
        patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
    ):
        rc = _complete_code_task(
            task,
            config,
            store,
            git,
            log_file,
            branch,
            TaskStats(duration_seconds=1.0, num_steps_reported=1, cost_usd=0.01),
            0,
            pre_run_status=set(),
            worktree_summary_path=worktree_summary_path,
            summary_path=summary_path,
            summary_dir=summary_dir,
            skip_commit=True,
            create_pr=True,
        )

    assert rc == 1
    failed_task = store.get(task.id)
    assert failed_task is not None
    assert failed_task.status == "failed"
    assert failed_task.failure_reason == "BRANCH_UNPUSHABLE"
    assert failed_task.output_content == "summary\n"
    assert create_pr_calls == []
    return config, store, git, failed_task, log_file, local_tip, create_pr_calls


def test_v24_to_v27_chains_via_gza_migrate(tmp_path: Path) -> None:
    db_path = tmp_path / ".gza" / "gza.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    (tmp_path / "gza.yaml").write_text(
        "project_name: gza\n"
        f"db_path: {db_path}\n",
        encoding="utf-8",
    )
    _make_v24_db(db_path)

    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO tasks (id, prompt, created_at) VALUES (1, 'parent', '2024-01-01T00:00:00+00:00')"
    )
    conn.execute(
        "INSERT INTO tasks (id, prompt, based_on, depends_on, created_at) VALUES (2, 'child', 1, 1, '2024-01-01T00:00:00+00:00')"
    )
    conn.commit()
    conn.close()

    result = run_gza_subprocess("migrate", "--yes", "--project", str(tmp_path), cwd=tmp_path)
    assert result.returncode == 0, result.stderr

    status = check_migration_status(db_path)
    assert status["current_version"] == 27
    assert status["pending_manual"] == []
    assert status["pending_auto"] == list(range(28, SCHEMA_VERSION + 1))


def test_squash_merge_reconciles_origin_branch_and_keeps_advance_planning_clean(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")
    git = Git(tmp_path)

    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    (tmp_path / "file.txt").write_text("initial\n")
    git._run("add", "file.txt")
    git._run("commit", "-m", "Initial commit")

    remote_dir = tmp_path / "origin.git"
    git._run("init", "--bare", str(remote_dir))
    git._run("remote", "add", "origin", str(remote_dir))
    git._run("push", "-u", "origin", "main")

    branch = "feature/squash-reconcile"
    git._run("checkout", "-b", branch)
    (tmp_path / "file.txt").write_text("initial\nfeature one\n")
    git._run("add", "file.txt")
    git._run("commit", "-m", "Feature one")
    (tmp_path / "file.txt").write_text("initial\nfeature one\nfeature two\n")
    git._run("add", "file.txt")
    git._run("commit", "-m", "Feature two")
    git._run("push", "-u", "origin", branch)
    git._run("checkout", "main")
    git.fetch("origin")

    task = store.add("Implement squash reconcile", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = branch
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    args = argparse.Namespace(
        rebase=False,
        squash=True,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
    )

    result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 0
    squash_oid = git.rev_parse("HEAD")
    assert git.rev_parse(f"refs/heads/{branch}") == squash_oid
    assert git.rev_parse(f"refs/remotes/origin/{branch}") == squash_oid
    assert git.rev_parse(f"refs/remotes/origin/{branch}") == git.rev_parse(f"refs/heads/{branch}")
    assert git.resolve_fresh_merge_source(branch).warning is None

    refreshed = store.get(task.id)
    assert refreshed is not None
    refreshed.merge_status = "unmerged"
    store.update(refreshed)

    ctx = resolve_advance_context(config, store, git, refreshed, "main")
    assert ctx.merge_source_warning is None

    action = evaluate_advance_rules(config, store, git, refreshed, "main")
    assert action.get("needs_attention_reason") != "merge-source-needs-manual-resolution"


def test_run_task_backed_rebase_clean_rebase_updates_origin_and_clears_merge_source_divergence(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    git = Git(tmp_path)

    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    (tmp_path / "base.txt").write_text("base\n")
    git._run("add", "base.txt")
    git._run("commit", "-m", "Initial commit")

    remote_dir = tmp_path / "origin.git"
    git._run("init", "--bare", str(remote_dir))
    git._run("remote", "add", "origin", str(remote_dir))
    git._run("push", "-u", "origin", "main")

    branch = "feature/rebase-publish"
    git._run("checkout", "-b", branch)
    (tmp_path / "feature.txt").write_text("feature\n")
    git._run("add", "feature.txt")
    git._run("commit", "-m", "Feature commit")
    git._run("push", "-u", "origin", branch)
    original_remote_sha = git.rev_parse("HEAD")

    git._run("checkout", "main")
    (tmp_path / "base.txt").write_text("base\nmain update\n")
    git._run("add", "base.txt")
    git._run("commit", "-m", "Main update")

    parent = store.add("Implement feature", task_type="implement")
    assert parent.id is not None
    parent.status = "completed"
    parent.completed_at = datetime.now(UTC)
    parent.branch = branch
    parent.merge_status = "unmerged"
    parent.has_commits = True
    store.update(parent)

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    assert rebase_task.id is not None
    rebase_task.branch = branch
    store.update(rebase_task)

    rc = _run_task_backed_rebase(
        config=config,
        store=store,
        rebase_task=rebase_task,
        branch=branch,
        target_branch="main",
    )

    assert rc == 0
    rebased_sha = git.rev_parse(branch)
    assert rebased_sha != original_remote_sha
    git.fetch("origin")
    assert git.rev_parse(f"origin/{branch}") == rebased_sha
    assert git.resolve_fresh_merge_source(branch).warning is None


def test_real_git_stale_wip_publication_reconcile_retries_pr_and_reaches_merge_gate(
    tmp_path: Path,
) -> None:
    from gza.cli.git_ops import _reconcile_diverged_branch_with_origin

    (
        config,
        store,
        git,
        failed_task,
        log_file,
        local_tip,
        create_pr_calls,
    ) = _prepare_stale_wip_branch_publication_failure(
        tmp_path,
        branch="feature/stale-wip-e2e",
        prompt="Implement stale WIP reconcile",
        slug="20260706-stale-wip-e2e",
    )

    git._run("checkout", "main")

    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=1,
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
        reconcile_diverged_branch=lambda current_task: _reconcile_diverged_branch_with_origin(
            config,
            git,
            current_task,
            target_branch="main",
        ),
        config=config,
        git=git,
    )

    with (
        patch("gza.pr_ops.GitHub", side_effect=lambda: _FakeAvailableGitHub(create_pr_calls)),
        patch("gza.runner.build_task_pr_content", return_value=("Test PR", "PR body")),
        patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
    ):
        result = execute_advance_action(
            task=failed_task,
            action={
                "type": "reconcile_branch_divergence",
                "decision": FailedRecoveryDecision(
                    task_id=failed_task.id,
                    action="reconcile",
                    reason_code="BRANCH_UNPUSHABLE",
                    reason_text="branch publication failed; reconcile local/origin refs",
                    launch_mode="none",
                    attempt_index=1,
                    attempt_limit=2,
                ),
            },
            context=context,
        )

    assert result.status == "success"
    assert "force-with-lease" in result.message
    refreshed = store.get(failed_task.id)
    assert refreshed is not None
    assert refreshed.status == "completed"
    assert refreshed.failure_reason is None
    assert create_pr_calls == [("feature/stale-wip-e2e", "main", "Test PR", "PR body", False)]
    assert git.rev_parse("feature/stale-wip-e2e") == local_tip
    git.fetch("origin")
    assert git.rev_parse("origin/feature/stale-wip-e2e") == local_tip

    config.require_review_before_merge = False
    config.advance_create_reviews = False
    next_action = determine_next_action(
        config,
        store,
        git,
        refreshed,
        "main",
        selected_for_merge=True,
    )
    assert next_action["type"] == "merge"
    assert "failed (BRANCH_UNPUSHABLE)" in ops_log_path_for(log_file).read_text()


def test_real_git_run_with_recovery_stale_wip_reconcile_completes_without_manual_intervention(
    tmp_path: Path,
) -> None:
    (
        config,
        store,
        git,
        failed_task,
        _log_file,
        local_tip,
        create_pr_calls,
    ) = _prepare_stale_wip_branch_publication_failure(
        tmp_path,
        branch="feature/run-with-recovery-stale-wip",
        prompt="Recover stale WIP divergence",
        slug="20260706-run-with-recovery-stale-wip",
    )

    git._run("checkout", "main")
    terminal_skip_calls: list[str] = []
    run_calls: list[str] = []

    def _run_task(current_task, _resume):
        run_calls.append(current_task.id or "")
        return 1

    with (
        patch("gza.pr_ops.GitHub", side_effect=lambda: _FakeAvailableGitHub(create_pr_calls)),
        patch("gza.runner.build_task_pr_content", return_value=("Test PR", "PR body")),
        patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
    ):
        final_task, rc = run_with_recovery(
            config,
            store,
            failed_task,
            run_task=_run_task,
            max_resume_attempts=2,
            on_terminal_skip=lambda failed, _decision, _rc: terminal_skip_calls.append(failed.id or ""),
        )

    assert rc == 0
    assert run_calls == [failed_task.id]
    assert terminal_skip_calls == []
    assert final_task.status == "completed"
    assert final_task.failure_reason is None
    assert create_pr_calls == [("feature/run-with-recovery-stale-wip", "main", "Test PR", "PR body", False)]
    assert git.rev_parse("feature/run-with-recovery-stale-wip") == local_tip
    git.fetch("origin")
    assert git.rev_parse("origin/feature/run-with-recovery-stale-wip") == local_tip


def test_real_git_non_benign_remote_divergence_parks_manual_resolution_without_pr_required(
    tmp_path: Path,
) -> None:
    from gza.cli.git_ops import _reconcile_diverged_branch_with_origin

    config, store, git, remote_dir = _init_repo_with_origin(tmp_path)
    branch = "feature/non-benign-conflict"

    (tmp_path / "conflict.txt").write_text("base\n", encoding="utf-8")
    git._run("add", "conflict.txt")
    git._run("commit", "-m", "Add conflict base")
    git._run("push", "origin", "main")

    git._run("checkout", "-b", branch)
    git._run("push", "-u", "origin", branch)
    (tmp_path / "conflict.txt").write_text("local branch change\n", encoding="utf-8")
    git._run("add", "conflict.txt")
    git._run("commit", "-m", "Local branch change")

    remote_checkout = tmp_path / "remote-non-benign"
    git._run("clone", str(remote_dir), str(remote_checkout))
    remote_git = Git(remote_checkout)
    remote_git._run("config", "user.name", "Remote User")
    remote_git._run("config", "user.email", "remote@example.com")
    remote_git._run("checkout", branch)
    (remote_checkout / "conflict.txt").write_text("remote branch change\n", encoding="utf-8")
    remote_git._run("add", "conflict.txt")
    remote_git._run("commit", "-m", "Remote branch change")
    remote_git._run("push", "origin", branch)

    git._run("checkout", "main")
    (tmp_path / "conflict.txt").write_text("main branch change\n", encoding="utf-8")
    git._run("add", "conflict.txt")
    git._run("commit", "-m", "Main branch change")
    git.fetch("origin")
    assert git.count_commits_ahead(branch, f"origin/{branch}") == 1
    assert git.count_commits_ahead(f"origin/{branch}", branch) == 1

    task = store.add("Recover genuine remote conflict", task_type="implement")
    assert task.id is not None
    task.status = "failed"
    task.failure_reason = "BRANCH_UNPUSHABLE"
    task.branch = branch
    task.has_commits = True
    task.completed_at = datetime.now(UTC)
    store.update(task)

    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=1,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("unused"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        create_targeted_rebase_task=lambda _task, _target: pytest.fail("unused"),
        spawn_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        reconcile_diverged_branch=lambda current_task: _reconcile_diverged_branch_with_origin(
            config,
            git,
            current_task,
            target_branch="main",
        ),
        config=config,
        git=git,
    )

    result = execute_advance_action(
        task=task,
        action={"type": "reconcile_branch_divergence"},
        context=context,
    )

    assert result.status == "skip"
    assert result.attention_reason == "reconcile-needs-manual-resolution"
    assert "PR_REQUIRED" not in result.message
    assert "hit conflicts" in result.message
    attention = resolve_execution_needs_attention(task, result)
    assert attention is not None
    assert attention.action["needs_attention_reason"] == "reconcile-needs-manual-resolution"


def test_squash_merge_without_remote_tracking_ref_stays_local_only(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")
    git = Git(tmp_path)

    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    (tmp_path / "file.txt").write_text("initial\n")
    git._run("add", "file.txt")
    git._run("commit", "-m", "Initial commit")

    remote_dir = tmp_path / "origin.git"
    git._run("init", "--bare", str(remote_dir))
    git._run("remote", "add", "origin", str(remote_dir))
    git._run("push", "-u", "origin", "main")

    branch = "feature/local-only-squash"
    git._run("checkout", "-b", branch)
    (tmp_path / "feature.txt").write_text("feature\n")
    git._run("add", "feature.txt")
    git._run("commit", "-m", "Feature")
    git._run("checkout", "main")

    task = store.add("Implement local only squash", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = branch
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    args = argparse.Namespace(
        rebase=False,
        squash=True,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
    )

    result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 0
    assert git.rev_parse_if_exists(f"refs/remotes/origin/{branch}") is None


def test_resolve_context_prefers_local_branch_when_origin_is_stale(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    git = Git(tmp_path)
    branch = "feat/local-ahead"

    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    (tmp_path / "base.txt").write_text("base\n")
    git._run("add", "base.txt")
    git._run("commit", "-m", "Initial commit")

    git._run("checkout", "-b", branch)
    (tmp_path / "feature.txt").write_text("remote tip\n")
    git._run("add", "feature.txt")
    git._run("commit", "-m", "Remote tip")
    remote_sha = git.rev_parse("HEAD")

    (tmp_path / "feature.txt").write_text("remote tip\nlocal tip\n")
    git._run("add", "feature.txt")
    git._run("commit", "-m", "Local tip")
    git._run("update-ref", f"refs/remotes/origin/{branch}", remote_sha)
    git._run("checkout", "main")

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = branch
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    ctx = resolve_advance_context(config, store, git, impl, "main")

    assert ctx.merge_source_ref == branch
    assert ctx.merge_source_warning is None
    assert ctx.can_merge is True


def test_real_git_remote_tracking_ref_unblocks_failed_rebase_after_later_approved_review(
    tmp_path: Path, monkeypatch
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    branch = "feat/remote-only-mergeable"
    git = init_repo_with_remote_tracking_only_feature(tmp_path, branch)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    impl.branch = branch
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review = store.add("Review", task_type="review", depends_on=impl.id)
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    review.report_file = "reviews/fake.md"
    store.update(review)

    failed_rebase = store.add("Failed rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    failed_rebase.status = "failed"
    failed_rebase.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    failed_rebase.branch = branch
    failed_rebase.failure_reason = "MERGE_CONFLICT"
    store.update(failed_rebase)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda project_dir, r: ParsedReviewReport(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    assert git.branch_exists(branch) is False
    assert git.ref_exists(f"origin/{branch}") is True

    ctx = resolve_advance_context(config, store, git, impl, "main")
    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert ctx.can_merge is True
    assert action["type"] == "merge"
    assert action["description"] == "Merge (review APPROVED)"


def test_cleanup_worktree_for_branch_refuses_foreign_live_worktree_in_real_repo(tmp_path: Path) -> None:
    """Real git worktrees outside managed roots must remain untouched."""
    git = Git(tmp_path)
    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    (tmp_path / "README.md").write_text("initial\n")
    git._run("add", "README.md")
    git._run("commit", "-m", "Initial commit")

    branch = "feature/foreign-worktree"
    git._run("checkout", "-b", branch)
    (tmp_path / "feature.txt").write_text("feature\n")
    git._run("add", "feature.txt")
    git._run("commit", "-m", "Add feature")
    git._run("checkout", "main")

    managed_root = tmp_path / ".gza-managed"
    foreign_path = tmp_path / "user-worktrees" / "foreign-feature"
    foreign_path.parent.mkdir(parents=True, exist_ok=True)
    git._run("worktree", "add", str(foreign_path), branch)
    sentinel = foreign_path / "sentinel.txt"
    sentinel.write_text("leave me alone\n")
    registrations_before = {p.name for p in (tmp_path / ".git" / "worktrees").iterdir() if p.is_dir()}

    with pytest.raises(GitError, match="Refusing to remove worktree for branch 'feature/foreign-worktree'"):
        cleanup_worktree_for_branch(
            git,
            branch,
            force=True,
            permitted_root_paths=[managed_root],
        )

    assert sentinel.exists()
    registrations_after = {p.name for p in (tmp_path / ".git" / "worktrees").iterdir() if p.is_dir()}
    assert registrations_after == registrations_before
    active_path = active_worktree_path_for_branch(git, branch)
    assert active_path == foreign_path.resolve(strict=False)
    assert Git(foreign_path).current_branch() == branch


def test_is_ancestor_with_real_repo(tmp_path: Path) -> None:
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    git = Git(repo_dir)
    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    (repo_dir / "file.txt").write_text("base\n")
    git._run("add", "file.txt")
    git._run("commit", "-m", "base")
    base_sha = git.rev_parse("HEAD")
    git._run("checkout", "-b", "feature/demo")
    (repo_dir / "file.txt").write_text("base\nfeature\n")
    git._run("add", "file.txt")
    git._run("commit", "-m", "feature")

    assert git.is_ancestor(base_sha, "feature/demo") is True
    assert git.is_ancestor("feature/demo", "main") is False


def test_reverse_check_patch_file_result_accepts_selected_subset_already_on_base(tmp_path: Path) -> None:
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    git = Git(repo_dir)

    git._run("init", "-b", "main")
    git._run("config", "user.email", "test@example.com")
    git._run("config", "user.name", "Test User")

    source_file = repo_dir / "src" / "file.py"
    source_file.parent.mkdir(parents=True, exist_ok=True)
    source_file.write_text("print('anchor')\n")
    git._run("add", "src/file.py")
    git._run("commit", "-m", "base")

    git._run("checkout", "-b", "feature/source")
    source_file.write_text("print('line a')\nprint('anchor')\n")
    git._run("add", "src/file.py")
    git._run("commit", "-m", "add line a")

    patch_text = git.get_diff_patch_for_paths("main...feature/source", ("src/file.py",), binary=True)
    patch_file = repo_dir / "selected.patch"
    patch_file.write_text(patch_text)
    assert "+print('line a')" in patch_text
    assert "+print('line b')" not in patch_text

    git._run("checkout", "main")
    source_file.write_text("print('line a')\nprint('anchor')\nprint('line b')\n")
    git._run("add", "src/file.py")
    git._run("commit", "-m", "add line a and later line b on main")
    current_base_delta = git.get_diff_patch_for_paths("main..feature/source", ("src/file.py",), binary=True)

    result = git.reverse_check_patch_file_result(patch_file)

    assert current_base_delta.strip()
    assert result.returncode == 0
    assert source_file.read_text() == "print('line a')\nprint('anchor')\nprint('line b')\n"


def test_plan_extraction_commit_source_uses_commit_subject_and_provenance(tmp_path: Path) -> None:
    git = Git(tmp_path)
    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")

    (tmp_path / "src").mkdir(exist_ok=True)
    (tmp_path / "src" / "agent_sessions.py").write_text("persisted = False\n")
    git._run("add", "src/agent_sessions.py")
    git._run("commit", "-m", "Improve agent session persistence")

    from gza.extractions import normalize_selected_paths, plan_extraction, resolve_source_selection

    source = resolve_source_selection(
        store,
        git,
        source_task_id=None,
        source_branch=None,
        source_commits=("HEAD",),
        base_branch_override=None,
    )
    draft = plan_extraction(
        git,
        source,
        normalize_selected_paths(["src/agent_sessions.py"]),
        operator_prompt=None,
    )

    assert draft.prompt.startswith("Carry over: Improve agent session persistence\n")
    assert "Source: commit " in draft.prompt
    assert "Source commit subjects:" in draft.prompt


def test_save_wip_changes_creates_commit_and_diff(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")
    task = store.add(prompt="Test task", task_type="implement")
    task.slug = "20260212-test-task"

    worktree_path = tmp_path / "worktree"
    worktree_path.mkdir()
    (worktree_path / "test.txt").write_text("test content")

    git = Git(worktree_path)
    git._run("init")
    git._run("config", "user.email", "test@example.com")
    git._run("config", "user.name", "Test User")

    config = Mock(spec=Config)
    config.project_dir = tmp_path

    _save_wip_changes(task, git, config, "test-branch")

    log = git._run("log", "-1", "--pretty=%s").stdout.strip()
    assert log == "WIP: gza task interrupted"

    wip_file = tmp_path / WIP_DIR / "20260212-test-task.diff"
    assert wip_file.exists()
    assert "test.txt" in wip_file.read_text()


def test_save_wip_changes_with_no_changes(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")
    task = store.add(prompt="Test task", task_type="implement")
    task.slug = "20260212-test-task"

    worktree_path = tmp_path / "worktree"
    worktree_path.mkdir()
    git = Git(worktree_path)
    git._run("init")
    git._run("config", "user.email", "test@example.com")
    git._run("config", "user.name", "Test User")
    (worktree_path / "initial.txt").write_text("initial")
    git.add(".")
    git.commit("Initial commit")

    config = Mock(spec=Config)
    config.project_dir = tmp_path

    _save_wip_changes(task, git, config, "test-branch")

    log = git._run("log", "-1", "--pretty=%s").stdout.strip()
    assert log == "Initial commit"
    assert not (tmp_path / WIP_DIR / "20260212-test-task.diff").exists()


def test_restore_wip_changes_finds_wip_commit(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")
    task = store.add(prompt="Test task", task_type="implement")
    task.slug = "20260212-test-task"

    worktree_path = tmp_path / "worktree"
    worktree_path.mkdir()
    git = Git(worktree_path)
    git._run("init")
    git._run("config", "user.email", "test@example.com")
    git._run("config", "user.name", "Test User")
    (worktree_path / "test.txt").write_text("test")
    git.add(".")
    git.commit("WIP: gza task interrupted\n\nTask ID: 20260212-test-task")

    config = Mock(spec=Config)
    config.project_dir = tmp_path

    _restore_wip_changes(task, git, config, "test-branch")

    log = git._run("log", "-1", "--pretty=%s").stdout.strip()
    assert log == "WIP: gza task interrupted"


def test_restore_wip_changes_applies_diff_backup(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")
    task = store.add(prompt="Test task", task_type="implement")
    task.slug = "20260212-test-task"

    worktree_path = tmp_path / "worktree"
    worktree_path.mkdir()
    git = Git(worktree_path)
    git._run("init")
    git._run("config", "user.email", "test@example.com")
    git._run("config", "user.name", "Test User")
    (worktree_path / "initial.txt").write_text("initial")
    git.add(".")
    git.commit("Initial commit")

    wip_dir = tmp_path / WIP_DIR
    wip_dir.mkdir(parents=True)
    wip_file = wip_dir / "20260212-test-task.diff"
    wip_file.write_text(
        "diff --git a/test.txt b/test.txt\n"
        "new file mode 100644\n"
        "index 0000000..9daeafb\n"
        "--- /dev/null\n"
        "+++ b/test.txt\n"
        "@@ -0,0 +1 @@\n"
        "+test\n"
    )

    config = Mock(spec=Config)
    config.project_dir = tmp_path

    _restore_wip_changes(task, git, config, "test-branch")

    log = git._run("log", "-1", "--pretty=%s").stdout.strip()
    assert log == "WIP: restored from diff"


def test_squash_wip_commits(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")
    task = store.add(prompt="Test task", task_type="implement")
    task.slug = "20260212-test-task"

    worktree_path = tmp_path / "worktree"
    worktree_path.mkdir()
    git = Git(worktree_path)
    git._run("init")
    git._run("config", "user.email", "test@example.com")
    git._run("config", "user.name", "Test User")

    (worktree_path / "initial.txt").write_text("initial")
    git.add(".")
    git.commit("Initial commit")

    (worktree_path / "wip1.txt").write_text("wip1")
    git.add(".")
    git.commit("WIP: first attempt")

    (worktree_path / "wip2.txt").write_text("wip2")
    git.add(".")
    git.commit("WIP: second attempt")

    log_before = git._run("log", "--oneline").stdout.strip().split("\n")
    assert len(log_before) == 3

    _squash_wip_commits(git, task)

    log_after = git._run("log", "--oneline").stdout.strip().split("\n")
    assert len(log_after) == 1
    assert log_after[0].endswith("Initial commit")
    assert git.has_changes(".", include_untracked=False)


def test_squash_wip_commits_with_no_wip_commits(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")
    task = store.add(prompt="Test task", task_type="implement")
    task.slug = "20260212-test-task"

    worktree_path = tmp_path / "worktree"
    worktree_path.mkdir()
    git = Git(worktree_path)
    git._run("init")
    git._run("config", "user.email", "test@example.com")
    git._run("config", "user.name", "Test User")

    (worktree_path / "test.txt").write_text("test")
    git.add(".")
    git.commit("Normal commit")

    _squash_wip_commits(git, task)

    log = git._run("log", "-1", "--pretty=%s").stdout.strip()
    assert log == "Normal commit"
