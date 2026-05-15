"""Unit tests for the declarative advance rule engine."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from gza.advance_engine import (
    WORKER_CONSUMING_ACTIONS,
    classify_advance_action,
    evaluate_advance_rules,
    failed_recovery_decision_to_action,
    resolve_advance_context,
    resolve_closing_review_action,
)
from gza.config import Config
from gza.db import SqliteTaskStore, Task as DbTask
from gza.git import Git
from gza.lineage_query import LineageOwnerQuery, query_lineage_owner_rows
from gza.recovery_engine import decide_failed_task_recovery
from gza.review_verdict import ParsedReviewReport


class _FakeGit:
    def __init__(
        self,
        can_merge: bool = True,
        *,
        can_merge_by_ref: dict[tuple[str, str], bool] | None = None,
        is_merged_by_ref: dict[tuple[str, str], bool] | None = None,
        existing_branches: set[str] | None = None,
        existing_refs: set[str] | None = None,
        ref_shas: dict[str, str | None] | None = None,
        ancestor_pairs: dict[tuple[str, str], bool] | None = None,
        merge_source_result: tuple[str | None, str | None] | None = None,
        legacy_merge_source_ref: str | None = None,
    ):
        self._can_merge = can_merge
        self._can_merge_by_ref = can_merge_by_ref or {}
        self._is_merged_by_ref = is_merged_by_ref or {}
        self._existing_branches = existing_branches or set()
        self._existing_refs = existing_refs or set()
        self._ref_shas = ref_shas or {}
        self._ancestor_pairs = ancestor_pairs or {}
        self._merge_source_result = merge_source_result
        self._legacy_merge_source_ref = legacy_merge_source_ref
        self.rev_parse_calls: list[str] = []
        self.is_ancestor_calls: list[tuple[str, str]] = []

    def can_merge(self, source_branch: str, target_branch: str) -> bool:
        return self._can_merge_by_ref.get((source_branch, target_branch), self._can_merge)

    def is_merged(self, source_branch: str, target_branch: str) -> bool:
        return self._is_merged_by_ref.get((source_branch, target_branch), False)

    def branch_exists(self, branch: str) -> bool:
        return branch in self._existing_branches

    def ref_exists(self, ref: str) -> bool:
        return ref in self._existing_refs

    def rev_parse_if_exists(self, ref: str) -> str | None:
        self.rev_parse_calls.append(ref)
        return self._ref_shas.get(ref)

    def is_ancestor(self, ancestor: str, descendant: str) -> bool:
        self.is_ancestor_calls.append((ancestor, descendant))
        return self._ancestor_pairs.get((ancestor, descendant), False)

    def resolve_fresh_merge_source(self, branch: str):
        from gza.git import ResolvedMergeSourceRef

        if self._merge_source_result is not None:
            ref, warning = self._merge_source_result
            return ResolvedMergeSourceRef(ref, warning)
        remote_ref = f"origin/{branch}"
        if remote_ref in self._existing_refs:
            return ResolvedMergeSourceRef(remote_ref)
        if branch in self._existing_branches:
            return ResolvedMergeSourceRef(branch)
        return ResolvedMergeSourceRef(branch)

    def resolve_merge_source_ref(self, branch: str) -> str | None:
        if self._legacy_merge_source_ref is not None:
            return self._legacy_merge_source_ref
        if branch in self._existing_branches:
            return branch
        remote_ref = f"origin/{branch}"
        if remote_ref in self._existing_refs:
            return remote_ref
        return None


def _make_store(tmp_path: Path) -> SqliteTaskStore:
    (tmp_path / "gza.yaml").write_text("project_name: test-project\n")
    config = Config.load(tmp_path)
    db_path = tmp_path / ".gza" / "gza.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return SqliteTaskStore(db_path, prefix=config.project_prefix)


def _init_repo_with_remote_tracking_only_feature(tmp_path: Path, branch: str) -> Git:
    git = Git(tmp_path)
    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    (tmp_path / "base.txt").write_text("base\n")
    git._run("add", "base.txt")
    git._run("commit", "-m", "Initial commit")

    git._run("checkout", "-b", branch)
    feature_file = Path(branch.replace("/", "_") + ".txt")
    (tmp_path / feature_file).write_text("feature\n")
    git._run("add", str(feature_file))
    git._run("commit", "-m", "Feature commit")
    feature_sha = git.rev_parse("HEAD")
    git._run("checkout", "main")
    git._run("update-ref", f"refs/remotes/origin/{branch}", feature_sha)
    git._run("branch", "-D", branch)
    return git


def _make_completed_impl_with_failed_rebase(
    store: SqliteTaskStore,
    *,
    branch: str,
    failure_reason: str = "MERGE_CONFLICT",
) -> tuple[DbTask, DbTask]:
    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 14, 9, 0, tzinfo=UTC)
    impl.branch = branch
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)
    store.get_or_create_merge_unit_for_task(impl)

    failed_rebase = store.add(
        f"Failed rebase for {impl.id}",
        task_type="rebase",
        based_on=impl.id,
        same_branch=True,
    )
    assert failed_rebase.id is not None
    failed_rebase.status = "failed"
    failed_rebase.failure_reason = failure_reason
    failed_rebase.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
    failed_rebase.branch = branch
    store.update(failed_rebase)
    return impl, failed_rebase


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


def test_completed_explore_without_followup_needs_discussion(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    explore = store.add("Explore ingestion options", task_type="explore")
    explore.status = "completed"
    explore.completed_at = datetime.now(UTC)
    store.update(explore)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), explore, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "explore-needs-follow-up-decision"
    assert "completed explore has no plan or implement follow-up" in action["description"]


def test_completed_explore_with_only_dropped_plan_descendant_still_needs_discussion(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    explore = store.add("Explore ingestion options", task_type="explore")
    explore.status = "completed"
    explore.completed_at = datetime.now(UTC)
    store.update(explore)

    dropped_plan = store.add("Plan ingestion options", task_type="plan", based_on=explore.id)
    dropped_plan.status = "dropped"
    dropped_plan.completed_at = datetime.now(UTC)
    store.update(dropped_plan)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), explore, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "explore-needs-follow-up-decision"
    assert "completed explore has no plan or implement follow-up" in action["description"]


def test_branch_bearing_completed_explore_with_pending_plan_descendant_does_not_need_discussion(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    explore = store.add("Explore ingestion options", task_type="explore")
    explore.status = "completed"
    explore.completed_at = datetime.now(UTC)
    explore.branch = "feat/explore-ingestion"
    store.update(explore)

    pending_plan = store.add("Plan ingestion options", task_type="plan", based_on=explore.id)
    pending_plan.status = "pending"
    store.update(pending_plan)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), explore, "main")

    assert action["type"] == "merge"
    assert action["description"] == "Merge task (no review yet)"
    assert action.get("needs_attention_reason") is None


def test_branch_bearing_completed_explore_with_pending_implement_descendant_does_not_need_discussion(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    explore = store.add("Explore ingestion options", task_type="explore")
    explore.status = "completed"
    explore.completed_at = datetime.now(UTC)
    explore.branch = "feat/explore-ingestion"
    store.update(explore)

    plan = store.add("Plan ingestion options", task_type="plan", based_on=explore.id)
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    pending_implement = store.add("Implement ingestion options", task_type="implement", based_on=plan.id)
    pending_implement.status = "pending"
    store.update(pending_implement)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), explore, "main")

    assert action["type"] == "merge"
    assert action["description"] == "Merge task (no review yet)"
    assert action.get("needs_attention_reason") is None


def test_completed_plan_with_only_dropped_implement_descendant_still_needs_implement(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan ingestion options", task_type="plan")
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    dropped_implement = store.add("Implement ingestion options", task_type="implement", based_on=plan.id)
    dropped_implement.status = "dropped"
    dropped_implement.completed_at = datetime.now(UTC)
    store.update(dropped_implement)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == "create_implement"
    assert action["description"] == "Create and start implement task"


def test_pending_branchless_plan_without_implement_descendant_uses_no_branch_skip(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan ingestion options", task_type="plan")
    plan.status = "pending"
    store.update(plan)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == "skip"
    assert action["description"] == "SKIP: pending plan task has no branch; no merge action available"


def test_pending_branchless_plan_with_implement_descendant_still_uses_no_branch_skip(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan ingestion options", task_type="plan")
    plan.status = "pending"
    store.update(plan)

    implement = store.add("Implement ingestion options", task_type="implement", based_on=plan.id)
    implement.status = "pending"
    store.update(implement)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == "skip"
    assert action["description"] == "SKIP: pending plan task has no branch; no merge action available"
    assert "implement task already exists" not in action["description"]


def test_completed_no_branch_task_uses_shape_specific_skip_message(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    review = store.add("Review architecture notes", task_type="review")
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    store.update(review)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), review, "main")

    assert action["type"] == "skip"
    assert action["description"] == "SKIP: completed review task has no branch; no mergeable commits found"


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


def test_rebase_after_review_with_unchanged_diff_preserves_approved_review(tmp_path: Path, monkeypatch) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    task = store.add("Implement feature", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    task.branch = "feature/rebase-preserved"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", based_on=task.id, depends_on=task.id)
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    store.update(review)

    rebase = store.add(f"Rebase {task.id}", task_type="rebase", based_on=task.id, same_branch=True)
    rebase.status = "completed"
    rebase.completed_at = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    rebase.branch = task.branch
    rebase.changed_diff = False
    store.update(rebase)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda project_dir, r: ParsedReviewReport(verdict="APPROVED", findings=(), format_version="legacy"),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), task, "main")
    assert action["type"] == "merge"
    assert action["description"] == f"Merge (review APPROVED, preserved across rebase {rebase.id})"


def test_rebase_after_review_with_changed_diff_requires_fresh_review(tmp_path: Path, monkeypatch) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    task = store.add("Implement feature", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    task.branch = "feature/rebase-changed"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", based_on=task.id, depends_on=task.id)
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    store.update(review)

    rebase = store.add(f"Rebase {task.id}", task_type="rebase", based_on=task.id, same_branch=True)
    rebase.status = "completed"
    rebase.completed_at = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    rebase.branch = task.branch
    rebase.changed_diff = True
    store.update(rebase)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda project_dir, r: ParsedReviewReport(verdict="APPROVED", findings=(), format_version="legacy"),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), task, "main")
    assert action["type"] == "create_review"
    assert action["description"] == f"Create review (rebase {rebase.id} changed diff)"


def test_rebase_after_review_with_unknown_diff_requires_fresh_review(tmp_path: Path, monkeypatch) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    task = store.add("Implement feature", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    task.branch = "feature/rebase-unknown"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", based_on=task.id, depends_on=task.id)
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    store.update(review)

    rebase = store.add(f"Rebase {task.id}", task_type="rebase", based_on=task.id, same_branch=True)
    rebase.status = "completed"
    rebase.completed_at = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    rebase.branch = task.branch
    rebase.changed_diff = None
    store.update(rebase)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda project_dir, r: ParsedReviewReport(verdict="APPROVED", findings=(), format_version="legacy"),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), task, "main")
    assert action["type"] == "create_review"
    assert action["description"] == f"Create review (rebase {rebase.id} change unknown)"


def test_evaluate_resumes_timeout_retry_descendant_once(tmp_path: Path):
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

    assert action["type"] == "resume"
    assert action["description"] == "Resume failed task (MAX_STEPS)"


def test_actionable_failed_recovery_actions_are_not_needs_attention(tmp_path: Path) -> None:
    store = _make_store(tmp_path)

    failed = store.add("Implement feature", task_type="implement")
    failed.status = "failed"
    failed.failure_reason = "MAX_STEPS"
    failed.session_id = "sess-1"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    action = failed_recovery_decision_to_action(failed, decision)

    assert decision.action == "resume"
    assert classify_advance_action(action) == "actionable"


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


def test_completed_improve_without_review_clear_creates_closing_review(
    tmp_path: Path,
    monkeypatch,
):
    """A completed improve newer than the latest review must create a closing review."""
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    impl.status = "completed"
    review_time = datetime.now(UTC) - timedelta(hours=2)
    impl.completed_at = review_time - timedelta(hours=1)
    impl.branch = "feat/improve-publish-blocked"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review = store.add(f"Review {impl.id}", task_type="review", depends_on=impl.id)
    review.status = "completed"
    review.completed_at = review_time
    review.report_file = "reviews/fake.md"
    store.update(review)

    improve = store.add(
        f"Improve {impl.id}",
        task_type="improve",
        based_on=impl.id,
        depends_on=review.id,
        same_branch=True,
    )
    improve.status = "completed"
    improve.completed_at = review_time + timedelta(hours=1)
    improve.branch = impl.branch
    improve.has_commits = True
    store.update(improve)

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
    assert action["description"] == "Create closing review (code changed since the last review)"


def test_completed_orphan_rebase_does_not_invalidate_review_on_impl_branch(tmp_path: Path, monkeypatch) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    impl.branch = "feat/canonical"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    store.update(review)

    orphan_rebase = store.add("Rebase orphan", task_type="rebase", based_on=impl.id, same_branch=True)
    assert orphan_rebase.id is not None
    orphan_rebase.status = "completed"
    orphan_rebase.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    orphan_rebase.branch = "feat/orphan"
    orphan_rebase.has_commits = True
    store.update(orphan_rebase)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda project_dir, r: ParsedReviewReport(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")
    assert action["type"] == "merge"
    assert action["description"] == "Merge (review APPROVED)"


def test_failed_rebase_is_ignored_after_later_approved_review(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    impl.branch = "feat/mergeable-origin-tip"
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
    failed_rebase.branch = impl.branch
    failed_rebase.failure_reason = "WORKER_DIED"
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

    action = evaluate_advance_rules(
        config,
        store,
        _FakeGit(
            can_merge=False,
            can_merge_by_ref={("origin/feat/mergeable-origin-tip", "main"): True},
            existing_refs={"origin/feat/mergeable-origin-tip"},
        ),
        impl,
        "main",
    )

    assert action["type"] == "merge"
    assert action["description"] == "Merge (review APPROVED)"


def test_failed_rebase_still_blocks_when_current_tip_needs_rebase(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feat/still-conflicts"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    failed_rebase = store.add("Failed rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    failed_rebase.status = "failed"
    failed_rebase.completed_at = datetime.now(UTC)
    failed_rebase.branch = impl.branch
    failed_rebase.failure_reason = "MERGE_CONFLICT"
    store.update(failed_rebase)

    action = evaluate_advance_rules(
        config,
        store,
        _FakeGit(
            can_merge=False,
            can_merge_by_ref={("origin/feat/still-conflicts", "main"): False},
            existing_refs={"origin/feat/still-conflicts"},
        ),
        impl,
        "main",
    )

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "rebase-failed-needs-manual-resolution"


def test_failed_rebase_without_review_still_requires_manual_resolution(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feat/no-review-failed-rebase"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    failed_rebase = store.add("Failed rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    failed_rebase.status = "failed"
    failed_rebase.completed_at = datetime.now(UTC)
    failed_rebase.branch = impl.branch
    failed_rebase.failure_reason = "MERGE_CONFLICT"
    store.update(failed_rebase)

    action = evaluate_advance_rules(
        config,
        store,
        _FakeGit(
            can_merge=True,
            can_merge_by_ref={("origin/feat/no-review-failed-rebase", "main"): True},
            existing_refs={"origin/feat/no-review-failed-rebase"},
        ),
        impl,
        "main",
    )

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "rebase-failed-needs-manual-resolution"
    assert "failed, needs manual resolution" in action["description"]


def test_failed_rebase_clears_when_merge_unit_is_merged(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    impl, _failed_rebase = _make_completed_impl_with_failed_rebase(
        store,
        branch="feature/merge-unit-merged",
    )
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "merged", merged_by_task_id=impl.id)

    git = _FakeGit(
        can_merge=False,
        ref_shas={
            impl.branch: "branch-sha",
            "main": "target-sha",
        },
        ancestor_pairs={("main", impl.branch): False},
    )

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "skip"
    assert action["description"] == "SKIP: target implementation already merged (merge-unit-merged)"
    assert git.rev_parse_calls == []
    assert git.is_ancestor_calls == []


def test_failed_rebase_clears_and_marks_merged_when_branch_tip_equals_target_tip(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    impl, _failed_rebase = _make_completed_impl_with_failed_rebase(
        store,
        branch="feature/branch-equals-target",
    )

    git = _FakeGit(
        can_merge=False,
        ref_shas={
            impl.branch: "same-sha",
            "main": "same-sha",
        },
    )

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "skip"
    assert action["description"] == (
        "SKIP: target implementation already merged (branch-tip-equals-target-tip)"
    )
    refreshed_unit = store.resolve_merge_unit_for_task(impl.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=False),
        config=config,
        git=git,
        target_branch="main",
    )
    assert all(row.owner_task.id != impl.id for row in rows)


def test_failed_rebase_does_not_persist_merged_from_stale_local_tip_when_origin_is_fresher(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    impl, _failed_rebase = _make_completed_impl_with_failed_rebase(
        store,
        branch="feature/stale-local-tip",
    )

    git = _FakeGit(
        can_merge=False,
        can_merge_by_ref={("origin/feature/stale-local-tip", "main"): False},
        ref_shas={
            impl.branch: "target-sha",
            "origin/feature/stale-local-tip": "remote-sha",
            "main": "target-sha",
        },
        ancestor_pairs={("main", "origin/feature/stale-local-tip"): False},
        merge_source_result=("origin/feature/stale-local-tip", None),
    )

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "rebase-failed-needs-manual-resolution"
    assert "target implementation already merged" not in action["description"]

    refreshed_unit = store.resolve_merge_unit_for_task(impl.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"

    ctx = resolve_advance_context(config, store, git, impl, "main")
    assert ctx.merge_source_ref == "origin/feature/stale-local-tip"
    assert ctx.post_merge_rebase_state is not None
    assert ctx.post_merge_rebase_state.already_merged is False
    assert ctx.post_merge_rebase_state.reason is None


def test_failed_rebase_clears_when_branch_contains_current_target_tip(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    impl, _failed_rebase = _make_completed_impl_with_failed_rebase(
        store,
        branch="feature/contains-target",
    )

    git = _FakeGit(
        can_merge=True,
        ref_shas={
            impl.branch: "branch-sha",
            "main": "target-sha",
        },
        ancestor_pairs={("main", impl.branch): True},
    )

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "create_review"
    refreshed_unit = store.resolve_merge_unit_for_task(impl.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"


def test_failed_rebase_resolution_precedence_merge_unit_wins(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    impl, _failed_rebase = _make_completed_impl_with_failed_rebase(
        store,
        branch="feature/precedence-merge-unit",
    )
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "merged", merged_by_task_id=impl.id)

    git = _FakeGit(can_merge=False)
    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "skip"
    assert action["description"] == "SKIP: target implementation already merged (merge-unit-merged)"
    assert git.rev_parse_calls == []
    assert git.is_ancestor_calls == []


def test_conflict_needs_rebase_not_emitted_when_target_already_merged(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 14, 9, 0, tzinfo=UTC)
    impl.branch = "feature/conflict-already-merged"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)
    unit = store.get_or_create_merge_unit_for_task(impl)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "merged", merged_by_task_id=impl.id)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=False), impl, "main")

    assert action["type"] == "skip"
    assert action["description"] == "SKIP: target implementation already merged (merge-unit-merged)"


def test_already_merged_branch_skips_post_rebase_review_and_rebase_actions(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 14, 4, 0, tzinfo=UTC)
    impl.branch = "feat/already-merged-after-rebase"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)
    store.get_or_create_merge_unit_for_task(impl)

    review = store.add("Initial review", task_type="review", depends_on=impl.id)
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 14, 5, 0, tzinfo=UTC)
    review.report_file = "reviews/fake.md"
    store.update(review)

    rebase = store.add("Completed rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    rebase.status = "completed"
    rebase.completed_at = datetime(2026, 5, 14, 6, 0, tzinfo=UTC)
    rebase.branch = impl.branch
    rebase.has_commits = True
    rebase.changed_diff = True
    store.update(rebase)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review_task: ParsedReviewReport(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    action = evaluate_advance_rules(
        config,
        store,
        _FakeGit(
            can_merge=False,
            is_merged_by_ref={("origin/feat/already-merged-after-rebase", "main"): True},
            existing_refs={"origin/feat/already-merged-after-rebase"},
        ),
        impl,
        "main",
    )

    assert action["type"] == "skip"
    assert action["description"] == "SKIP: already merged into target branch"


def test_already_merged_branch_prefers_fresh_remote_over_stale_legacy_local_ref(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 14, 4, 0, tzinfo=UTC)
    impl.branch = "feat/stale-local-fresh-remote"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)
    store.get_or_create_merge_unit_for_task(impl)

    review = store.add("Initial review", task_type="review", depends_on=impl.id)
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 14, 5, 0, tzinfo=UTC)
    review.report_file = "reviews/fake.md"
    store.update(review)

    rebase = store.add("Completed rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    rebase.status = "completed"
    rebase.completed_at = datetime(2026, 5, 14, 6, 0, tzinfo=UTC)
    rebase.branch = impl.branch
    rebase.has_commits = True
    rebase.changed_diff = True
    store.update(rebase)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review_task: ParsedReviewReport(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    action = evaluate_advance_rules(
        config,
        store,
        _FakeGit(
            can_merge=False,
            is_merged_by_ref={
                ("origin/feat/stale-local-fresh-remote", "main"): True,
                ("feat/stale-local-fresh-remote", "main"): False,
            },
            existing_branches={"feat/stale-local-fresh-remote"},
            existing_refs={"origin/feat/stale-local-fresh-remote"},
            merge_source_result=("origin/feat/stale-local-fresh-remote", None),
            legacy_merge_source_ref="feat/stale-local-fresh-remote",
        ),
        impl,
        "main",
    )

    assert action["type"] == "skip"
    assert action["description"] == "SKIP: already merged into target branch"


def test_failed_rebase_is_superseded_by_later_completed_same_branch_rebase(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    impl.branch = "feat/rebase-supersedes-failure"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review = store.add("Initial review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    review.report_file = "reviews/fake.md"
    store.update(review)

    failed_rebase = store.add("Failed rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    assert failed_rebase.id is not None
    failed_rebase.status = "failed"
    failed_rebase.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    failed_rebase.branch = impl.branch
    failed_rebase.failure_reason = "MERGE_CONFLICT"
    store.update(failed_rebase)

    completed_rebase = store.add(
        "Completed recovery rebase",
        task_type="rebase",
        based_on=impl.id,
        same_branch=True,
    )
    assert completed_rebase.id is not None
    completed_rebase.status = "completed"
    completed_rebase.completed_at = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    completed_rebase.branch = impl.branch
    completed_rebase.has_commits = True
    store.update(completed_rebase)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda project_dir, r: ParsedReviewReport(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    action = evaluate_advance_rules(
        config,
        store,
        _FakeGit(
            can_merge=True,
            can_merge_by_ref={("origin/feat/rebase-supersedes-failure", "main"): True},
            existing_refs={"origin/feat/rebase-supersedes-failure"},
        ),
        impl,
        "main",
    )

    assert action["type"] == "create_review"
    assert action["description"].startswith("Create review (rebase ")
    assert action["description"].endswith(" change unknown)")


def test_failed_rebase_with_only_older_review_still_requires_manual_resolution(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    impl.branch = "feat/failed-rebase-still-blocked"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review = store.add("Initial review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    review.report_file = "reviews/fake.md"
    store.update(review)

    failed_rebase = store.add("Failed rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    assert failed_rebase.id is not None
    failed_rebase.status = "failed"
    failed_rebase.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    failed_rebase.branch = impl.branch
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

    action = evaluate_advance_rules(
        config,
        store,
        _FakeGit(
            can_merge=True,
            can_merge_by_ref={("origin/feat/failed-rebase-still-blocked", "main"): True},
            existing_refs={"origin/feat/failed-rebase-still-blocked"},
        ),
        impl,
        "main",
    )

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "rebase-failed-needs-manual-resolution"


def test_failed_rebase_is_superseded_by_later_review_clear_event(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    impl.branch = "feat/failed-rebase-cleared-late"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review = store.add("Initial review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    review.report_file = "reviews/fake.md"
    store.update(review)

    failed_rebase = store.add("Failed rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    assert failed_rebase.id is not None
    failed_rebase.status = "failed"
    failed_rebase.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    failed_rebase.branch = impl.branch
    failed_rebase.failure_reason = "MERGE_CONFLICT"
    store.update(failed_rebase)

    impl.review_cleared_at = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    store.update(impl)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda project_dir, r: ParsedReviewReport(
            verdict="CHANGES_REQUESTED",
            findings=(),
            format_version="legacy",
        ),
    )

    action = evaluate_advance_rules(
        config,
        store,
        _FakeGit(
            can_merge=True,
            can_merge_by_ref={("origin/feat/failed-rebase-cleared-late", "main"): True},
            existing_refs={"origin/feat/failed-rebase-cleared-late"},
        ),
        impl,
        "main",
    )

    assert action["type"] == "merge"
    assert action["description"] == "Merge (previous review addressed)"


def test_failed_rebase_with_older_review_clear_event_still_requires_manual_resolution(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    impl.branch = "feat/failed-rebase-cleared-too-early"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review = store.add("Initial review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    review.report_file = "reviews/fake.md"
    store.update(review)

    failed_rebase = store.add("Failed rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    assert failed_rebase.id is not None
    failed_rebase.status = "failed"
    failed_rebase.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    failed_rebase.branch = impl.branch
    failed_rebase.failure_reason = "MERGE_CONFLICT"
    store.update(failed_rebase)

    impl.review_cleared_at = datetime(2026, 5, 10, 10, 30, tzinfo=UTC)
    store.update(impl)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda project_dir, r: ParsedReviewReport(
            verdict="CHANGES_REQUESTED",
            findings=(),
            format_version="legacy",
        ),
    )

    action = evaluate_advance_rules(
        config,
        store,
        _FakeGit(
            can_merge=True,
            can_merge_by_ref={("origin/feat/failed-rebase-cleared-too-early", "main"): True},
            existing_refs={"origin/feat/failed-rebase-cleared-too-early"},
        ),
        impl,
        "main",
    )

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "rebase-failed-needs-manual-resolution"


def test_can_merge_prefers_origin_ref_when_available_across_worktrees(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feat/worktree-stable"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    git_without_local_branch = _FakeGit(
        can_merge=False,
        can_merge_by_ref={("origin/feat/worktree-stable", "main"): True},
        existing_refs={"origin/feat/worktree-stable"},
    )
    git_with_stale_local_branch = _FakeGit(
        can_merge=False,
        can_merge_by_ref={
            ("feat/worktree-stable", "main"): False,
            ("origin/feat/worktree-stable", "main"): True,
        },
        existing_branches={"feat/worktree-stable"},
        existing_refs={"origin/feat/worktree-stable"},
    )

    ctx_without_local_branch = resolve_advance_context(
        config,
        store,
        git_without_local_branch,
        impl,
        "main",
    )
    ctx_with_stale_local_branch = resolve_advance_context(
        config,
        store,
        git_with_stale_local_branch,
        impl,
        "main",
    )

    assert ctx_without_local_branch.can_merge is True
    assert ctx_with_stale_local_branch.can_merge is True


@pytest.mark.functional
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


def test_diverged_local_and_origin_need_manual_resolution(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feat/diverged"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    action = evaluate_advance_rules(
        config,
        store,
        _FakeGit(
            merge_source_result=(
                None,
                "Local branch 'feat/diverged' and remote-tracking ref 'origin/feat/diverged' diverged. "
                "Push, fetch, or reconcile them before advancing or merging.",
            )
        ),
        impl,
        "main",
    )

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "merge-source-needs-manual-resolution"
    assert "diverged" in action["description"]


def test_diverged_local_and_origin_fail_closed_even_when_local_tip_matches_target(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    impl, _failed_rebase = _make_completed_impl_with_failed_rebase(
        store,
        branch="feature/diverged-equals-target-local",
    )

    git = _FakeGit(
        can_merge=False,
        ref_shas={
            impl.branch: "target-sha",
            "main": "target-sha",
        },
        merge_source_result=(
            None,
            (
                "Local branch 'feature/diverged-equals-target-local' and remote-tracking ref "
                "'origin/feature/diverged-equals-target-local' diverged. Push, fetch, or "
                "reconcile them before advancing or merging."
            ),
        ),
    )

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "merge-source-needs-manual-resolution"
    assert "diverged" in action["description"]
    assert "target implementation already merged" not in action["description"]

    refreshed_unit = store.resolve_merge_unit_for_task(impl.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"

    ctx = resolve_advance_context(config, store, git, impl, "main")
    assert ctx.post_merge_rebase_state is not None
    assert ctx.post_merge_rebase_state.already_merged is False
    assert ctx.post_merge_rebase_state.warning is not None
    assert "diverged" in ctx.post_merge_rebase_state.warning


@pytest.mark.functional
def test_real_git_remote_tracking_ref_unblocks_failed_rebase_after_later_approved_review(
    tmp_path: Path, monkeypatch
) -> None:
    from gza import advance_engine as advance_engine_module

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


@pytest.mark.parametrize(
    ("closing_review_status", "expected_action_type"),
    [
        ("pending", "run_review"),
        ("in_progress", "wait_review"),
        ("completed", None),
        ("failed", None),
    ],
)
def test_closing_review_invariant_does_not_create_duplicate_review(
    tmp_path: Path,
    closing_review_status: str,
    expected_action_type: str | None,
) -> None:
    store = _make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 1, 1, tzinfo=UTC)
    impl.branch = "feat/closing-review-dedupe"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    stale_review = store.add("Old review", task_type="review", depends_on=impl.id)
    assert stale_review.id is not None
    stale_review.status = "completed"
    stale_review.completed_at = datetime(2026, 1, 2, tzinfo=UTC)
    store.update(stale_review)

    improve = store.add(
        "Improve feature",
        task_type="improve",
        based_on=impl.id,
        depends_on=stale_review.id,
        same_branch=True,
    )
    assert improve.id is not None
    improve.status = "completed"
    improve.completed_at = datetime(2026, 1, 3, tzinfo=UTC)
    store.update(improve)

    impl.review_cleared_at = datetime(2026, 1, 3, tzinfo=UTC)
    store.update(impl)

    closing_review = store.add("Closing review", task_type="review", depends_on=impl.id)
    assert closing_review.id is not None
    closing_review.status = closing_review_status
    closing_review.created_at = datetime(2026, 1, 4, tzinfo=UTC)
    if closing_review_status in {"completed", "failed"}:
        closing_review.completed_at = datetime(2026, 1, 4, tzinfo=UTC)
    store.update(closing_review)

    action = resolve_closing_review_action(
        task=impl,
        reviews=store.get_reviews_for_task(impl.id),
        latest_completed_review=stale_review,
        latest_completed_code_change=improve,
    )

    if expected_action_type is None:
        assert action is None
    else:
        assert action is not None
        assert action["type"] == expected_action_type
        assert action.get("review_task").id == closing_review.id


def test_failed_improve_does_not_require_closing_review(tmp_path: Path) -> None:
    store = _make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 1, 1, tzinfo=UTC)
    impl.branch = "feat/failed-improve-no-closing-review"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 1, 2, tzinfo=UTC)
    store.update(review)

    failed_improve = store.add(
        "Failed improve",
        task_type="improve",
        based_on=impl.id,
        depends_on=review.id,
        same_branch=True,
    )
    assert failed_improve.id is not None
    failed_improve.status = "failed"
    failed_improve.completed_at = datetime(2026, 1, 3, tzinfo=UTC)
    store.update(failed_improve)

    action = resolve_closing_review_action(
        task=impl,
        reviews=store.get_reviews_for_task(impl.id),
        latest_completed_review=review,
        latest_completed_code_change=impl,
    )

    assert action is None


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
