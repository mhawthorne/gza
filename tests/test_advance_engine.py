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
from gza.review_verdict import ParsedReviewReport


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


def test_resolve_context_excludes_resume_state_for_test_failure(tmp_path: Path):
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

    assert ctx.is_resumable_failed_task is False
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


def test_evaluate_returns_skip_when_resume_budget_exhausted(tmp_path: Path):
    (tmp_path / "gza.yaml").write_text("project_name: test-project\nmax_resume_attempts: 1\n")
    config = Config.load(tmp_path)
    db_path = tmp_path / ".gza" / "gza.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    store = SqliteTaskStore(db_path, prefix=config.project_prefix)

    original = store.add("Implement", task_type="implement")
    original.status = "failed"
    original.failure_reason = "MAX_STEPS"
    original.session_id = "sess-1"
    original.completed_at = datetime.now(UTC)
    original.branch = "feat/original"
    store.update(original)

    resumed = store.add("Implement resume", task_type="implement")
    resumed.status = "failed"
    resumed.failure_reason = "MAX_STEPS"
    resumed.session_id = "sess-2"
    resumed.based_on = original.id
    resumed.completed_at = datetime.now(UTC)
    resumed.branch = "feat/resume"
    store.update(resumed)

    action = evaluate_advance_rules(
        config,
        store,
        _FakeGit(can_merge=True),
        resumed,
        "main",
    )

    assert action["type"] == "skip"
    assert action["description"] == "SKIP: max resume attempts (1) reached"


def test_completed_fix_after_changes_requested_requires_fresh_review(tmp_path: Path, monkeypatch):
    """A completed code-changing fix must stale the prior review so advance creates a new one,
    instead of looping back on the old review's CHANGES_REQUESTED verdict."""
    from datetime import timedelta

    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    impl.status = "completed"
    review_time = datetime.now(UTC) - timedelta(hours=2)
    impl.completed_at = review_time - timedelta(hours=1)
    impl.branch = "feat/fix-supersedes-review"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review = store.add(f"Review {impl.id}", task_type="review", depends_on=impl.id)
    review.status = "completed"
    review.completed_at = review_time
    review.report_file = "reviews/fake.md"
    store.update(review)

    # The fix completed with code changes AFTER the review and marked review_cleared_at.
    fix = store.add(f"Fix {impl.id}", task_type="fix", based_on=impl.id, depends_on=review.id, same_branch=True)
    fix.status = "completed"
    fix.completed_at = review_time + timedelta(hours=1)
    fix.has_commits = True
    store.update(fix)

    store.clear_review_state(impl.id)
    impl = store.get(impl.id)
    assert impl is not None

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda project_dir, r: ParsedReviewReport(
            verdict="CHANGES_REQUESTED",
            findings=(),
            format_version="legacy",
        ),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")
    assert action["type"] == "create_review", action


def test_unmerged_view_shows_fix_after_review_as_stale(tmp_path: Path):
    """After a code-changing fix completes, the unmerged classifier should treat the
    prior review as stale and name the fix as the cause."""
    from datetime import timedelta

    from gza.query import get_code_changing_descendants_for_root

    store = _make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC) - timedelta(hours=3)
    impl.branch = "feat/unmerged-stale"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review = store.add(f"Review {impl.id}", task_type="review", depends_on=impl.id)
    review.status = "completed"
    review.completed_at = datetime.now(UTC) - timedelta(hours=2)
    store.update(review)

    fix = store.add(f"Fix {impl.id}", task_type="fix", based_on=impl.id, depends_on=review.id, same_branch=True)
    fix.status = "completed"
    fix.completed_at = datetime.now(UTC) - timedelta(hours=1)
    fix.has_commits = True
    store.update(fix)

    descendants = get_code_changing_descendants_for_root(store, impl)
    assert fix.id in {t.id for t in descendants}

    latest = max(descendants, key=lambda t: t.completed_at or datetime.min)
    assert latest.id == fix.id
    assert review.completed_at is not None
    assert latest.completed_at is not None
    assert latest.completed_at > review.completed_at


def test_approved_with_followups_returns_merge_with_followups(tmp_path: Path, monkeypatch):
    from gza import advance_engine as advance_engine_module
    from gza.review_verdict import ParsedReviewReport, ReviewFinding

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    task = store.add("Implement feature", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feat/followups"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id)
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    store.update(review)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda project_dir, r: ParsedReviewReport(
            verdict="APPROVED_WITH_FOLLOWUPS",
            findings=(
                ReviewFinding(
                    id="F1",
                    severity="FOLLOWUP",
                    title="title",
                    body="body",
                    evidence=None,
                    impact=None,
                    fix_or_followup="add check",
                    tests=None,
                ),
            ),
            format_version="v2",
        ),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), task, "main")
    assert action["type"] == "merge_with_followups"
    assert len(action["followup_findings"]) == 1


def test_approved_with_followups_without_followup_findings_needs_discussion(tmp_path: Path, monkeypatch):
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    task = store.add("Implement feature", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feat/no-followups"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id)
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    store.update(review)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda project_dir, r: ParsedReviewReport(
            verdict="APPROVED_WITH_FOLLOWUPS",
            findings=(),
            format_version="v2",
        ),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), task, "main")
    assert action["type"] == "needs_discussion"


def test_approved_with_newer_unresolved_comment_prefers_run_improve(tmp_path: Path, monkeypatch):
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    task = store.add("Implement feature", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feat/approved-new-comments"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)
    assert task.id is not None

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id)
    review.status = "completed"
    review.completed_at = datetime(2026, 1, 1, tzinfo=UTC)
    store.update(review)
    assert review.id is not None

    pending_improve = store.add(
        "Pending improve for fresh comments",
        task_type="improve",
        based_on=task.id,
        depends_on=review.id,
    )
    pending_improve.status = "pending"
    pending_improve.created_at = datetime(2026, 1, 2, tzinfo=UTC)
    store.update(pending_improve)

    store.add_comment(task.id, "New unresolved feedback after approval.")

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda project_dir, r: ParsedReviewReport(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), task, "main")
    assert action["type"] == "run_improve"
    assert action["improve_task"].id == pending_improve.id


def test_approved_with_followups_and_newer_unresolved_comment_creates_improve(tmp_path: Path, monkeypatch):
    from gza import advance_engine as advance_engine_module
    from gza.review_verdict import ReviewFinding

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    task = store.add("Implement feature", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feat/approved-followups-new-comments"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)
    assert task.id is not None

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id)
    review.status = "completed"
    review.completed_at = datetime(2026, 1, 1, tzinfo=UTC)
    store.update(review)

    store.add_comment(task.id, "Need one more tweak from operator feedback.")

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda project_dir, r: ParsedReviewReport(
            verdict="APPROVED_WITH_FOLLOWUPS",
            findings=(
                ReviewFinding(
                    id="F1",
                    severity="FOLLOWUP",
                    title="Follow-up",
                    body="Body",
                    evidence=None,
                    impact=None,
                    fix_or_followup="Create follow-up task",
                    tests=None,
                ),
            ),
            format_version="v2",
        ),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), task, "main")
    assert action["type"] == "improve"
    assert action["review_task"].id == review.id
