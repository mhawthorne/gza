"""Functional regressions for CLI subprocess and real git shell-command flows."""

import argparse
from datetime import UTC, datetime
from pathlib import Path

from gza import advance_engine as advance_engine_module
from gza.advance_engine import evaluate_advance_rules, resolve_advance_context
from gza.cli.git_ops import _merge_single_task
from gza.config import Config
from gza.db import check_migration_status
from gza.git import Git
from gza.review_verdict import ParsedReviewReport
from tests.cli.conftest import make_store, setup_config
from tests.helpers.cli import run_gza_subprocess
from tests.test_advance_engine import _init_repo_with_remote_tracking_only_feature, _make_store
from tests.test_db import _make_v24_db


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
    assert status["pending_auto"] == [28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44]


def test_squash_merge_reconciles_origin_branch_and_keeps_advance_planning_clean(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
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


def test_squash_merge_without_remote_tracking_ref_stays_local_only(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
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
    git = _init_repo_with_remote_tracking_only_feature(tmp_path, branch)

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
