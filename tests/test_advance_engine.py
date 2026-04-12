"""Unit tests for the declarative advance rule engine."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from gza.advance_engine import (
    WORKER_CONSUMING_ACTIONS,
    evaluate_advance_rules,
    resolve_advance_context,
)
from gza.config import Config
from gza.db import SqliteTaskStore


class _FakeGit:
    def __init__(self, can_merge: bool = True):
        self._can_merge = can_merge

    def can_merge(self, source_branch: str, target_branch: str) -> bool:
        return self._can_merge


def _make_store(tmp_path: Path) -> SqliteTaskStore:
    (tmp_path / "gza.yaml").write_text("project_name: test-project\n")
    config = Config.load(tmp_path)
    db_path = tmp_path / ".gza" / "gza.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return SqliteTaskStore(db_path, prefix=config.project_prefix)


def test_resolve_context_includes_resume_state_for_test_failure(tmp_path: Path):
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    failed = store.add("Fix failing tests", task_type="implement")
    failed.status = "failed"
    failed.failure_reason = "TEST_FAILURE"
    failed.session_id = "sess-1"
    failed.completed_at = datetime.now(UTC)
    failed.branch = "feat/test-failure"
    store.update(failed)

    ctx = resolve_advance_context(
        config,
        store,
        _FakeGit(can_merge=True),
        failed,
        "main",
    )

    assert ctx.is_resumable_failed_task is True
    assert ctx.failure_reason == "TEST_FAILURE"


def test_rule_ordering_prefers_conflict_before_review_actions(tmp_path: Path):
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    task = store.add("Implement feature", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feat/conflict"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id)
    review.status = "pending"
    store.update(review)

    action = evaluate_advance_rules(
        config,
        store,
        _FakeGit(can_merge=False),
        task,
        "main",
    )

    assert action["type"] == "needs_rebase"


def test_worker_action_taxonomy_covers_batch_accounting_actions() -> None:
    assert "needs_rebase" in WORKER_CONSUMING_ACTIONS
    assert "create_implement" in WORKER_CONSUMING_ACTIONS


def test_evaluate_prefers_in_progress_review_over_pending_sibling(tmp_path: Path):
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    task = store.add("Implement feature", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feat/review-priority"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    pending = store.add(f"Review {task.id} pending", task_type="review", depends_on=task.id)
    pending.status = "pending"
    store.update(pending)

    in_progress = store.add(f"Review {task.id} active", task_type="review", depends_on=task.id)
    in_progress.status = "in_progress"
    store.update(in_progress)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), task, "main")
    assert action["type"] == "wait_review"
    assert action["review_task"].id == in_progress.id


def test_evaluate_runs_pending_review_when_no_in_progress_exists(tmp_path: Path):
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    task = store.add("Implement feature", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feat/review-pending"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    pending = store.add(f"Review {task.id} pending", task_type="review", depends_on=task.id)
    pending.status = "pending"
    store.update(pending)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), task, "main")
    assert action["type"] == "run_review"
    assert action["review_task"].id == pending.id
