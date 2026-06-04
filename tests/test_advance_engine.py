"""Unit tests for the declarative advance rule engine."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from gza.advance_engine import (
    ADVANCE_RULES,
    WORKER_CONSUMING_ACTIONS,
    _resolve_and_persist_post_merge_rebase_state,
    classify_advance_action,
    evaluate_advance_rules,
    failed_recovery_decision_to_action,
    require_needs_attention_subject,
    resolve_advance_context,
    resolve_closing_review_action,
    resolve_subject_task,
)
from gza.config import Config
from gza.db import SqliteTaskStore, Task as DbTask
from gza.git import GitError
from gza.lineage_query import LineageOwnerQuery, query_lineage_owner_rows
from gza.recovery_engine import FailedRecoveryDecision, decide_failed_task_recovery
from gza.review_verdict import ParsedReviewReport, ReviewFinding


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
        ahead_count: int | None = None,
        behind_count: int | None = None,
        behind_count_error: Exception | None = None,
        name_status_by_range: dict[str, str] | None = None,
        name_status_error_by_range: dict[str, Exception] | None = None,
        resolve_fresh_merge_source_ref_error: Exception | None = None,
        rev_parse_errors: dict[str, Exception] | None = None,
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
        self._ahead_count = ahead_count
        self._behind_count = behind_count
        self._behind_count_error = behind_count_error
        self._name_status_by_range = name_status_by_range or {}
        self._name_status_error_by_range = name_status_error_by_range or {}
        self._resolve_fresh_merge_source_ref_error = resolve_fresh_merge_source_ref_error
        self._rev_parse_errors = rev_parse_errors or {}
        self.rev_parse_calls: list[str] = []
        self.is_ancestor_calls: list[tuple[str, str]] = []
        self.behind_calls: list[tuple[str, str]] = []
        self.name_status_calls: list[str] = []

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
        error = self._rev_parse_errors.get(ref)
        if error is not None:
            raise error
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

    def resolve_fresh_merge_source_ref(self, branch: str) -> str | None:
        if self._resolve_fresh_merge_source_ref_error is not None:
            raise self._resolve_fresh_merge_source_ref_error
        return self.resolve_fresh_merge_source(branch).ref

    def count_commits_behind(self, source_ref: str, target_ref: str) -> int | None:
        self.behind_calls.append((source_ref, target_ref))
        if self._behind_count_error is not None:
            raise self._behind_count_error
        return self._behind_count

    def count_commits_ahead_checked(self, source_ref: str, target_ref: str) -> int | None:
        return self._ahead_count

    def get_diff_name_status(
        self,
        revision_range: str,
        paths: tuple[str, ...] | list[str] = (),
        *,
        check: bool = False,
    ) -> str:
        self.name_status_calls.append(revision_range)
        error = self._name_status_error_by_range.get(revision_range)
        if error is not None:
            raise error
        return self._name_status_by_range.get(revision_range, "")


def _make_store(tmp_path: Path) -> SqliteTaskStore:
    (tmp_path / "gza.yaml").write_text("project_name: test-project\n")
    config = Config.load(tmp_path)
    db_path = tmp_path / ".gza" / "gza.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return SqliteTaskStore(db_path, prefix=config.project_prefix)


def _set_subdir_project_boundary(config: Config, tmp_path: Path) -> None:
    from gza.runner import ProjectBoundary

    repo_root = tmp_path
    project_dir = tmp_path / "services" / "foo"
    project_dir.mkdir(parents=True, exist_ok=True)
    (project_dir / "gza.yaml").write_text("project_name: foo\nverify_command: ./bin/foo-verify\n")
    config.project_dir = project_dir
    config.enforce_project_scope = True
    setattr(
        config,
        "_project_boundary_cache",
        ProjectBoundary(
            repo_root=repo_root,
            scope_root=Path("services/foo"),
            local_dependencies=(),
        ),
    )


def _set_subdir_project_boundary_with_dependency(config: Config, tmp_path: Path) -> None:
    from gza.runner import LocalDependency, ProjectBoundary

    repo_root = tmp_path
    project_dir = tmp_path / "services" / "foo"
    project_dir.mkdir(parents=True, exist_ok=True)
    (project_dir / "gza.yaml").write_text("project_name: foo\nverify_command: ./bin/foo-verify\n")
    dependency_path = tmp_path / "dre"
    dependency_path.mkdir(parents=True, exist_ok=True)
    config.project_dir = project_dir
    config.enforce_project_scope = True
    setattr(
        config,
        "_project_boundary_cache",
        ProjectBoundary(
            repo_root=repo_root,
            scope_root=Path("services/foo"),
            local_dependencies=(
                LocalDependency(
                    source_path=Path("../../dre"),
                    resolved_path=dependency_path.resolve(),
                    repo_relative_path=Path("dre"),
                ),
            ),
        ),
    )


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


def _make_completed_unmerged_impl(
    store: SqliteTaskStore,
    *,
    branch: str,
    when: datetime,
) -> DbTask:
    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = when
    impl.branch = branch
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)
    return impl


def _add_completed_review(
    store: SqliteTaskStore,
    impl: DbTask,
    *,
    when: datetime,
) -> DbTask:
    assert impl.id is not None
    review = store.add(f"Review {impl.id}", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = when
    review.report_file = f"reviews/{review.id}.md"
    store.update(review)
    return review


def _add_completed_improve_for_review(
    store: SqliteTaskStore,
    impl: DbTask,
    review: DbTask,
    *,
    when: datetime,
    changed_diff: bool = True,
) -> DbTask:
    assert impl.id is not None
    assert review.id is not None
    improve = store.add(
        "Improve attempt",
        task_type="improve",
        based_on=impl.id,
        depends_on=review.id,
        same_branch=True,
    )
    assert improve.id is not None
    improve.status = "completed"
    improve.completed_at = when
    improve.branch = impl.branch
    improve.changed_diff = changed_diff
    store.update(improve)
    return improve


def _add_completed_rebase(
    store: SqliteTaskStore,
    impl: DbTask,
    *,
    when: datetime,
    changed_diff: bool | None = True,
) -> DbTask:
    assert impl.id is not None
    rebase = store.add(
        "Completed rebase",
        task_type="rebase",
        based_on=impl.id,
        same_branch=True,
    )
    assert rebase.id is not None
    rebase.status = "completed"
    rebase.completed_at = when
    rebase.branch = impl.branch
    rebase.has_commits = True
    rebase.changed_diff = changed_diff
    store.update(rebase)
    return rebase


def _make_failed_owner_with_completed_resume_descendant(
    store: SqliteTaskStore,
    *,
    branch: str,
    failed_at: datetime,
    resumed_at: datetime,
) -> tuple[DbTask, DbTask]:
    owner = store.add("Original implement", task_type="implement")
    assert owner.id is not None
    owner.status = "failed"
    owner.failure_reason = "MAX_STEPS"
    owner.completed_at = failed_at
    owner.branch = branch
    owner.merge_status = "unmerged"
    owner.has_commits = True
    store.update(owner)
    store.get_or_create_merge_unit_for_task(owner)

    resumed = store.add("Resumed implement", task_type="implement", based_on=owner.id)
    assert resumed.id is not None
    resumed.status = "completed"
    resumed.completed_at = resumed_at
    resumed.branch = branch
    resumed.merge_status = "unmerged"
    resumed.has_commits = True
    store.update(resumed)
    store.get_or_create_merge_unit_for_task(resumed)
    return owner, resumed


def _add_failed_rebase_attempts(
    store: SqliteTaskStore,
    impl: DbTask,
    *,
    hours: tuple[int, ...],
) -> list[DbTask]:
    assert impl.id is not None
    failed_rebases: list[DbTask] = []
    for hour in hours:
        failed_rebase = store.add(
            f"Failed rebase {hour}",
            task_type="rebase",
            based_on=impl.id,
            same_branch=True,
        )
        assert failed_rebase.id is not None
        failed_rebase.status = "failed"
        failed_rebase.completed_at = datetime(2026, 5, 14, hour, 0, tzinfo=UTC)
        failed_rebase.branch = impl.branch
        failed_rebase.failure_reason = "GIT_ERROR"
        store.update(failed_rebase)
        failed_rebases.append(failed_rebase)
    return failed_rebases


def _blocker_report(
    title: str,
    *,
    citation: str | None = "src/gza/foo.py:10",
    fix: str | None = "Fix it",
) -> ParsedReviewReport:
    return ParsedReviewReport(
        verdict="CHANGES_REQUESTED",
        findings=(
            ReviewFinding(
                id="B1",
                severity="BLOCKER",
                title=title,
                body="body",
                evidence=None,
                impact=None,
                fix_or_followup=fix,
                tests=None,
                open_state_citation=citation,
            ),
        ),
        format_version="v2",
    )


def _timeout_only_review_report() -> str:
    return (
        "## Summary\n\n- Verify timed out.\n\n"
        "## Blockers\n\n"
        "### B1 verify_command failure: timed out during pytest\n"
        "Evidence: verify_command timed out after 120s while running the configured suite.\n"
        "Open-state citation: `src/gza/runner.py:903`\n"
        "Impact: the branch cannot be verified autonomously.\n"
        "Required fix: investigate the test-performance regression or prove the timeout is environmental.\n"
        "Required tests: rerun the exact verify command and add a narrow regression if this branch caused the slowdown.\n\n"
        "## Follow-Ups\n\nNone.\n\n"
        "## Questions / Assumptions\n\nNone.\n\n"
        "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    )


def _timeout_only_review_report_evidence_only() -> str:
    return (
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


def _mixed_review_report() -> str:
    return (
        "## Summary\n\n- Mixed blockers.\n\n"
        "## Blockers\n\n"
        "### B1 verify_command failure: timed out during pytest\n"
        "Evidence: verify_command timed out after 120s while running the configured suite.\n"
        "Impact: branch cannot be verified.\n"
        "Required fix: investigate the slowdown.\n"
        "Required tests: rerun the suite.\n\n"
        "### B2 Missing input validation\n"
        "Evidence: request path still accepts malformed IDs.\n"
        "Open-state citation: `src/gza/api.py:14`\n"
        "Impact: malformed requests still crash.\n"
        "Required fix: validate IDs before parsing.\n"
        "Required tests: add malformed-ID regression coverage.\n\n"
        "## Follow-Ups\n\nNone.\n\n"
        "## Questions / Assumptions\n\nNone.\n\n"
        "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    )


def _structured_code_blocker_with_timeout_evidence_review_report() -> str:
    return (
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


def _code_focused_blocker_with_timeout_in_open_state_review_report() -> str:
    return (
        "## Summary\n\n- Worker loop bug surfaces as a verify timeout.\n\n"
        "## Blockers\n\n"
        "### B1 Worker loop leaves mocked task incomplete until verify_command timeout\n"
        "Evidence: the worker loop keeps spinning until verify_command timed out after 120s.\n"
        "Open-state citation: `tests/cli/test_execution.py:7214`\n"
        "Impact: the task never completes and the suite cannot pass.\n"
        "Required fix: exit the worker loop when the mocked task reaches its terminal state.\n"
        "Required tests: add a worker-loop regression that asserts the task completes well before the configured verify_command timeout.\n\n"
        "## Follow-Ups\n\nNone.\n\n"
        "## Questions / Assumptions\n\nNone.\n\n"
        "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    )


def _unstructured_mixed_review_report() -> str:
    return (
        "## Summary\n\n- Mixed blockers.\n\n"
        "## Blockers\n\n"
        "- verify_command timed out after 120s\n"
        "- Missing validation still crashes malformed IDs\n\n"
        "## Follow-Ups\n\nNone.\n\n"
        "## Questions / Assumptions\n\nNone.\n\n"
        "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    )


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
    assert action["subject_task_id"] == explore.id
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
    assert action["subject_task_id"] == explore.id
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


def test_completed_held_plan_awaits_human_review(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan ingestion options", task_type="plan", auto_implement=False)
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == "awaiting_human"
    assert action["description"] == (
        f"Awaiting human review: review the plan, then run 'uv run gza implement {plan.id}' "
        "to create implementation, or drop it if you decided not to implement."
    )
    assert classify_advance_action(action) == "needs_attention"
    assert action["needs_attention_reason"] == "awaiting-human-review"
    assert action["subject_task_id"] == plan.id


def test_completed_impl_without_review_and_auto_review_disabled_needs_manual_creation_attention(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.advance_create_reviews = False

    impl = _make_completed_unmerged_impl(
        store,
        branch="feature/manual-review-creation",
        when=datetime(2026, 5, 18, 10, 0, tzinfo=UTC),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["description"] == "SKIP: no review exists and advance_create_reviews=false (run gza review manually)"
    assert action["needs_attention_reason"] == "review-needs-manual-creation"
    assert action["subject_task_id"] == impl.id
    assert classify_advance_action(action) == "needs_attention"


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


def test_stale_review_with_auto_review_disabled_needs_manual_refresh(tmp_path: Path, monkeypatch) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.advance_create_reviews = False

    impl = _make_completed_unmerged_impl(
        store,
        branch="feature/stale-review-manual-refresh",
        when=datetime(2026, 5, 10, 10, 0, tzinfo=UTC),
    )
    _add_completed_review(store, impl, when=datetime(2026, 5, 10, 11, 0, tzinfo=UTC))
    _add_completed_rebase(
        store,
        impl,
        when=datetime(2026, 5, 10, 12, 0, tzinfo=UTC),
        changed_diff=True,
    )

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda project_dir, r: ParsedReviewReport(verdict="APPROVED", findings=(), format_version="legacy"),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["description"] == "SKIP: review must be refreshed before merge"
    assert action["needs_attention_reason"] == "stale-review-needs-manual-refresh"
    assert action["subject_task_id"] == impl.id


@pytest.mark.parametrize("refresh_review_status", ["pending", "in_progress"])
def test_stale_refresh_review_with_auto_review_disabled_needs_manual_refresh(
    tmp_path: Path,
    monkeypatch,
    refresh_review_status: str,
) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.advance_create_reviews = False

    impl = _make_completed_unmerged_impl(
        store,
        branch=f"feature/stale-review-active-{refresh_review_status}",
        when=datetime(2026, 5, 10, 10, 0, tzinfo=UTC),
    )
    _add_completed_review(store, impl, when=datetime(2026, 5, 10, 11, 0, tzinfo=UTC))
    _add_completed_rebase(
        store,
        impl,
        when=datetime(2026, 5, 10, 12, 0, tzinfo=UTC),
        changed_diff=True,
    )
    refresh_review = store.add(f"Refresh review {impl.id}", task_type="review", depends_on=impl.id)
    assert refresh_review.id is not None
    refresh_review.status = refresh_review_status
    refresh_review.created_at = datetime(2026, 5, 10, 12, 30, tzinfo=UTC)
    store.update(refresh_review)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda project_dir, r: ParsedReviewReport(verdict="APPROVED", findings=(), format_version="legacy"),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["description"] == "SKIP: review must be refreshed before merge"
    assert action["needs_attention_reason"] == "stale-review-needs-manual-refresh"
    assert action["subject_task_id"] == impl.id


@pytest.mark.parametrize(
    ("refresh_review_status", "refresh_review_completed_at"),
    [
        (None, None),
        ("pending", None),
        ("in_progress", None),
    ],
)
def test_stale_review_with_review_requirement_disabled_merges(
    tmp_path: Path,
    monkeypatch,
    refresh_review_status: str | None,
    refresh_review_completed_at: datetime | None,
) -> None:
    from gza import advance_engine as advance_engine_module

    del refresh_review_completed_at
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.require_review_before_merge = False
    config.advance_create_reviews = True

    impl = _make_completed_unmerged_impl(
        store,
        branch="feature/stale-review-disabled",
        when=datetime(2026, 5, 10, 10, 0, tzinfo=UTC),
    )
    _add_completed_review(store, impl, when=datetime(2026, 5, 10, 11, 0, tzinfo=UTC))
    _add_completed_rebase(
        store,
        impl,
        when=datetime(2026, 5, 10, 12, 0, tzinfo=UTC),
        changed_diff=True,
    )

    if refresh_review_status is not None:
        refresh_review = store.add(f"Refresh review {impl.id}", task_type="review", depends_on=impl.id)
        assert refresh_review.id is not None
        refresh_review.status = refresh_review_status
        refresh_review.created_at = datetime(2026, 5, 10, 12, 30, tzinfo=UTC)
        store.update(refresh_review)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda project_dir, r: ParsedReviewReport(verdict="APPROVED", findings=(), format_version="legacy"),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "merge"
    assert action["type"] not in {"create_review", "run_review", "wait_review"}


def test_completed_rebase_without_prior_review_creates_owner_review(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feature/rebase-owner-no-review",
        when=datetime(2026, 5, 10, 10, 0, tzinfo=UTC),
    )
    store.get_or_create_merge_unit_for_task(impl)
    rebase = _add_completed_rebase(
        store,
        impl,
        when=datetime(2026, 5, 10, 12, 0, tzinfo=UTC),
        changed_diff=False,
    )
    store.get_or_create_merge_unit_for_task(rebase)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), rebase, "main")

    assert action["type"] == "create_review"
    assert action["description"] == "Create closing review (latest implementation has no review yet)"


def test_completed_rebase_under_resumed_implement_without_review_creates_review(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    _failed_owner, resumed = _make_failed_owner_with_completed_resume_descendant(
        store,
        branch="feature/rebase-resume-no-review",
        failed_at=datetime(2026, 5, 10, 10, 0, tzinfo=UTC),
        resumed_at=datetime(2026, 5, 10, 11, 0, tzinfo=UTC),
    )
    rebase = _add_completed_rebase(
        store,
        resumed,
        when=datetime(2026, 5, 10, 12, 0, tzinfo=UTC),
        changed_diff=False,
    )
    store.get_or_create_merge_unit_for_task(rebase)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), rebase, "main")

    assert action["type"] == "create_review"
    assert action["description"] == "Create closing review (latest implementation has no review yet)"


def test_completed_rebase_with_approved_owner_review_merges(tmp_path: Path, monkeypatch) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feature/rebase-owner-approved",
        when=datetime(2026, 5, 10, 10, 0, tzinfo=UTC),
    )
    store.get_or_create_merge_unit_for_task(impl)
    review = _add_completed_review(store, impl, when=datetime(2026, 5, 10, 11, 0, tzinfo=UTC))
    rebase = _add_completed_rebase(
        store,
        impl,
        when=datetime(2026, 5, 10, 12, 0, tzinfo=UTC),
        changed_diff=False,
    )
    store.get_or_create_merge_unit_for_task(rebase)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda project_dir, task: ParsedReviewReport(verdict="APPROVED", findings=(), format_version="legacy"),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), rebase, "main")

    assert review.id is not None
    assert action["type"] == "merge"
    assert action["review_task"].id == review.id
    assert action["description"] == f"Merge (review APPROVED, preserved across rebase {rebase.id})"


def test_completed_changed_rebase_under_resumed_implement_invalidates_prior_review(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    _failed_owner, resumed = _make_failed_owner_with_completed_resume_descendant(
        store,
        branch="feature/rebase-resume-invalidates-review",
        failed_at=datetime(2026, 5, 10, 10, 0, tzinfo=UTC),
        resumed_at=datetime(2026, 5, 10, 11, 0, tzinfo=UTC),
    )
    _add_completed_review(store, resumed, when=datetime(2026, 5, 10, 11, 30, tzinfo=UTC))
    rebase = _add_completed_rebase(
        store,
        resumed,
        when=datetime(2026, 5, 10, 12, 0, tzinfo=UTC),
        changed_diff=True,
    )
    store.get_or_create_merge_unit_for_task(rebase)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda project_dir, task: ParsedReviewReport(verdict="APPROVED", findings=(), format_version="legacy"),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), rebase, "main")

    assert action["type"] == "create_review"
    assert action["description"] == f"Create review (rebase {rebase.id} changed diff)"


def test_completed_rebase_with_changed_diff_invalidates_owner_review(tmp_path: Path, monkeypatch) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feature/rebase-owner-invalidates-review",
        when=datetime(2026, 5, 10, 10, 0, tzinfo=UTC),
    )
    store.get_or_create_merge_unit_for_task(impl)
    _add_completed_review(store, impl, when=datetime(2026, 5, 10, 11, 0, tzinfo=UTC))
    rebase = _add_completed_rebase(
        store,
        impl,
        when=datetime(2026, 5, 10, 12, 0, tzinfo=UTC),
        changed_diff=True,
    )
    store.get_or_create_merge_unit_for_task(rebase)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda project_dir, task: ParsedReviewReport(verdict="APPROVED", findings=(), format_version="legacy"),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), rebase, "main")

    assert action["type"] == "create_review"
    assert action["description"] == f"Create review (rebase {rebase.id} changed diff)"


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


def test_retry_limit_reached_failed_recovery_action_is_needs_attention(tmp_path: Path) -> None:
    store = _make_store(tmp_path)

    failed = store.add("Implement feature", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_STEPS"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    action = failed_recovery_decision_to_action(
        failed,
        decision,
        needs_attention_reason="retry-limit-reached",
        subject_task_id=failed.id,
    )

    assert decision.reason_code == "retry_limit_reached"
    assert classify_advance_action(action) == "needs_attention"
    assert action["subject_task_id"] == failed.id


def test_retry_limit_reached_failed_recovery_action_defaults_subject_to_failed_task(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)

    failed = store.add("Implement feature", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_STEPS"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    action = failed_recovery_decision_to_action(
        failed,
        decision,
        needs_attention_reason="retry-limit-reached",
    )

    assert decision.reason_code == "retry_limit_reached"
    assert classify_advance_action(action) == "needs_attention"
    assert action["subject_task_id"] == failed.id


def test_failed_review_terminal_skip_subjects_owning_implementation(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/failed-review-subject",
        when=datetime(2026, 5, 15, 9, 0, tzinfo=UTC),
    )
    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "failed"
    review.failure_reason = "UNKNOWN"
    review.completed_at = datetime(2026, 5, 15, 10, 0, tzinfo=UTC)
    store.update(review)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), review, "main")

    assert classify_advance_action(action) == "needs_attention"
    assert action["subject_task_id"] == impl.id


def test_failed_rebase_terminal_skip_subjects_owning_implementation(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/failed-rebase-subject",
        when=datetime(2026, 5, 15, 9, 0, tzinfo=UTC),
    )
    rebase = store.add("Failed rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    assert rebase.id is not None
    rebase.status = "failed"
    rebase.failure_reason = "UNKNOWN"
    rebase.completed_at = datetime(2026, 5, 15, 10, 0, tzinfo=UTC)
    rebase.branch = impl.branch
    store.update(rebase)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), rebase, "main")

    assert classify_advance_action(action) == "needs_attention"
    assert action["subject_task_id"] == impl.id


def test_failed_chained_improve_terminal_skip_subjects_owning_implementation(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/failed-chained-improve-subject",
        when=datetime(2026, 5, 15, 9, 0, tzinfo=UTC),
    )
    review = _add_completed_review(store, impl, when=datetime(2026, 5, 15, 10, 0, tzinfo=UTC))

    previous_improve = store.add(
        "Previous improve",
        task_type="improve",
        based_on=impl.id,
        depends_on=review.id,
        same_branch=True,
    )
    assert previous_improve.id is not None
    previous_improve.status = "completed"
    previous_improve.completed_at = datetime(2026, 5, 15, 11, 0, tzinfo=UTC)
    previous_improve.branch = impl.branch
    previous_improve.changed_diff = False
    store.update(previous_improve)

    failed_improve = store.add(
        "Failed chained improve",
        task_type="improve",
        based_on=previous_improve.id,
        depends_on=review.id,
        same_branch=True,
    )
    assert failed_improve.id is not None
    failed_improve.status = "failed"
    failed_improve.failure_reason = "UNKNOWN"
    failed_improve.completed_at = datetime(2026, 5, 15, 12, 0, tzinfo=UTC)
    failed_improve.branch = impl.branch
    store.update(failed_improve)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), failed_improve, "main")

    assert classify_advance_action(action) == "needs_attention"
    assert action["subject_task_id"] == impl.id


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


def test_completed_improve_with_auto_review_disabled_needs_manual_closing_review_attention(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """A required closing review must fail closed when auto-review creation is disabled."""
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.advance_create_reviews = False

    impl = _make_completed_unmerged_impl(
        store,
        branch="feature/closing-review-manual-refresh",
        when=datetime(2026, 5, 10, 10, 0, tzinfo=UTC),
    )
    review = _add_completed_review(store, impl, when=datetime(2026, 5, 10, 11, 0, tzinfo=UTC))
    improve = store.add(
        f"Improve {impl.id}",
        task_type="improve",
        based_on=impl.id,
        depends_on=review.id,
        same_branch=True,
    )
    assert improve.id is not None
    improve.status = "completed"
    improve.completed_at = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    improve.branch = impl.branch
    improve.has_commits = True
    store.update(improve)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda project_dir, r: ParsedReviewReport(verdict="APPROVED", findings=(), format_version="legacy"),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "needs_discussion"
    assert (
        action["description"]
        == "SKIP: closing review required before merge and advance_create_reviews=false (run gza review manually)"
    )
    assert action["needs_attention_reason"] == "closing-review-needs-manual-refresh"
    assert action["subject_task_id"] == impl.id


def test_out_of_scope_sibling_project_change_parks_for_human(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    _set_subdir_project_boundary(config, tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/scope-sibling",
        when=datetime(2026, 5, 16, 9, 0, tzinfo=UTC),
    )
    git = _FakeGit(
        can_merge=True,
        name_status_by_range={
            "main...feat/scope-sibling": "M\tservices/foo/app.py\nM\tdre/web/src/app.tsx\n",
        },
    )

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "project-scope-violation"
    assert action["subject_task_id"] == impl.id
    assert action["failure_reason"] == "PROJECT_SCOPE_VIOLATION"
    assert action["out_of_scope_paths"] == ("dre/web/src/app.tsx",)
    assert "Tag `cross-project` and re-advance if intended, or fix the branch." in action["description"]


def test_out_of_scope_declared_dependency_change_parks_for_human(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    _set_subdir_project_boundary_with_dependency(config, tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/scope-dependency",
        when=datetime(2026, 5, 16, 9, 0, tzinfo=UTC),
    )
    git = _FakeGit(
        can_merge=True,
        name_status_by_range={
            "main...feat/scope-dependency": "M\tservices/foo/app.py\nM\tdre/lib/util.py\n",
        },
    )

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "project-scope-violation"
    assert action["out_of_scope_paths"] == ("dre/lib/util.py",)


def test_in_project_only_change_advances_normally_under_strict_scope(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    _set_subdir_project_boundary(config, tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/in-scope-only",
        when=datetime(2026, 5, 16, 9, 0, tzinfo=UTC),
    )
    git = _FakeGit(
        can_merge=True,
        name_status_by_range={
            "main...feat/in-scope-only": "M\tservices/foo/app.py\nA\tservices/foo/src/feature.py\n",
        },
    )

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "create_review"
    assert "review" in action["description"].lower()


def test_cross_project_tag_allows_out_of_scope_change_to_advance(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    _set_subdir_project_boundary(config, tmp_path)
    sibling_project_dir = tmp_path / "dre" / "web"
    sibling_project_dir.mkdir(parents=True, exist_ok=True)
    (sibling_project_dir / "gza.yaml").write_text("project_name: dre-web\nverify_command: ./bin/web-verify\n")

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/cross-project-scope",
        when=datetime(2026, 5, 16, 9, 0, tzinfo=UTC),
    )
    impl.tags = ("cross-project",)
    store.update(impl)

    git = _FakeGit(
        can_merge=True,
        name_status_by_range={
            "main...feat/cross-project-scope": "M\tservices/foo/app.py\nM\tdre/web/src/app.tsx\n",
        },
    )

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "create_review"
    assert action.get("needs_attention_reason") is None


def test_cross_project_tag_still_parks_unknown_paths_outside_discovered_roots(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    _set_subdir_project_boundary(config, tmp_path)
    sibling_project_dir = tmp_path / "dre" / "web"
    sibling_project_dir.mkdir(parents=True, exist_ok=True)
    (sibling_project_dir / "gza.yaml").write_text("project_name: dre-web\nverify_command: ./bin/web-verify\n")

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/cross-project-unknown",
        when=datetime(2026, 5, 16, 9, 0, tzinfo=UTC),
    )
    impl.tags = ("cross-project",)
    store.update(impl)

    git = _FakeGit(
        can_merge=True,
        name_status_by_range={
            "main...feat/cross-project-unknown": "M\tservices/foo/app.py\nM\tmisc/tools.py\n",
        },
    )

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "project-scope-violation"
    assert action["out_of_scope_paths"] == ("misc/tools.py",)
    assert "outside all discovered project roots" in action["description"]


def test_cross_project_tag_branch_declared_project_root_advances_without_checkout_config(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    _set_subdir_project_boundary(config, tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/cross-project-branch-local-root",
        when=datetime(2026, 5, 16, 9, 0, tzinfo=UTC),
    )
    impl.tags = ("cross-project",)
    store.update(impl)

    git = _FakeGit(
        can_merge=True,
        name_status_by_range={
            "main...feat/cross-project-branch-local-root": (
                "M\tservices/foo/app.py\n"
                "A\tlibs/new/gza.yaml\n"
                "A\tlibs/new/src/file.py\n"
            ),
        },
    )

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "create_review"
    assert action.get("needs_attention_reason") is None


def test_cross_project_tag_rename_declared_project_root_still_parks_for_unknown_source_root(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    _set_subdir_project_boundary(config, tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/cross-project-rename-root",
        when=datetime(2026, 5, 16, 9, 0, tzinfo=UTC),
    )
    impl.tags = ("cross-project",)
    store.update(impl)

    git = _FakeGit(
        can_merge=True,
        name_status_by_range={
            "main...feat/cross-project-rename-root": (
                "M\tservices/foo/app.py\n"
                "R100\tlibs/old/gza.yaml\tlibs/renamed/gza.yaml\n"
                "R100\tlibs/old/src/file.py\tlibs/renamed/src/file.py\n"
            ),
        },
    )

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "project-scope-violation"
    assert action["out_of_scope_paths"] == ("libs/old/gza.yaml", "libs/old/src/file.py")


def test_cross_project_tag_copy_declared_project_root_advances_without_checkout_config(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    _set_subdir_project_boundary(config, tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/cross-project-copy-root",
        when=datetime(2026, 5, 16, 9, 0, tzinfo=UTC),
    )
    impl.tags = ("cross-project",)
    store.update(impl)

    git = _FakeGit(
        can_merge=True,
        name_status_by_range={
            "main...feat/cross-project-copy-root": (
                "M\tservices/foo/app.py\n"
                "C100\tlibs/template/gza.yaml\tlibs/copied/gza.yaml\n"
                "C100\tlibs/template/src/file.py\tlibs/copied/src/file.py\n"
            ),
        },
    )

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "create_review"
    assert action.get("needs_attention_reason") is None


def test_cross_project_tag_deleted_project_root_still_parks_without_declared_root(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    _set_subdir_project_boundary(config, tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/cross-project-deleted-root",
        when=datetime(2026, 5, 16, 9, 0, tzinfo=UTC),
    )
    impl.tags = ("cross-project",)
    store.update(impl)

    git = _FakeGit(
        can_merge=True,
        name_status_by_range={
            "main...feat/cross-project-deleted-root": (
                "M\tservices/foo/app.py\n"
                "D\tlibs/removed/gza.yaml\n"
                "D\tlibs/removed/src/file.py\n"
            ),
        },
    )

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "project-scope-violation"
    assert action["out_of_scope_paths"] == ("libs/removed/gza.yaml", "libs/removed/src/file.py")


def test_cross_project_tag_branch_local_path_without_declared_root_still_parks(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    _set_subdir_project_boundary(config, tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/cross-project-missing-branch-root",
        when=datetime(2026, 5, 16, 9, 0, tzinfo=UTC),
    )
    impl.tags = ("cross-project",)
    store.update(impl)

    git = _FakeGit(
        can_merge=True,
        name_status_by_range={
            "main...feat/cross-project-missing-branch-root": (
                "M\tservices/foo/app.py\n"
                "A\tlibs/new/src/file.py\n"
            ),
        },
    )

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "project-scope-violation"
    assert action["out_of_scope_paths"] == ("libs/new/src/file.py",)


def test_strict_scope_diff_exception_parks_for_human(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    _set_subdir_project_boundary(config, tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/scope-inspection-exception",
        when=datetime(2026, 5, 16, 9, 0, tzinfo=UTC),
    )

    class _RaisingGit(_FakeGit):
        def get_diff_name_status(
            self,
            revision_range: str,
            paths: tuple[str, ...] | list[str] = (),
            *,
            check: bool = False,
        ) -> str:
            self.name_status_calls.append(revision_range)
            raise RuntimeError("diff probe blew up")

    action = evaluate_advance_rules(config, store, _RaisingGit(can_merge=True), impl, "main")

    assert classify_advance_action(action) == "needs_attention"
    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "project-scope-unverified"
    assert action["subject_task_id"] == impl.id
    assert action["type"] not in {"create_review", "improve", "needs_rebase", "merge", "merge_with_followups"}
    assert "strict project scope could not be verified" in action["description"]
    assert "diff probe blew up" in action["description"]


def test_strict_scope_uninspectable_git_diff_parks_for_human(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    _set_subdir_project_boundary(config, tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/scope-uninspectable-diff",
        when=datetime(2026, 5, 16, 9, 0, tzinfo=UTC),
    )
    git = _FakeGit(
        can_merge=True,
        name_status_error_by_range={
            "main...feat/scope-uninspectable-diff": GitError("git diff --name-status main...feat/scope-uninspectable-diff failed:\nfatal: bad revision"),
        },
    )

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert classify_advance_action(action) == "needs_attention"
    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "project-scope-unverified"
    assert action["subject_task_id"] == impl.id
    assert action["type"] not in {"create_review", "improve", "needs_rebase", "merge", "merge_with_followups"}
    assert "strict project scope could not be verified" in action["description"]
    assert "fatal: bad revision" in action["description"]


def test_one_noop_improve_permits_another_improve_with_warning_description(tmp_path: Path, monkeypatch) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 14, 9, 0, tzinfo=UTC)
    impl.branch = "feat/noop-warning"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
    review.report_file = "reviews/fake.md"
    store.update(review)

    improve = store.add("Improve attempt", task_type="improve", based_on=impl.id, depends_on=review.id, same_branch=True)
    improve.status = "completed"
    improve.completed_at = datetime(2026, 5, 14, 11, 0, tzinfo=UTC)
    improve.branch = impl.branch
    improve.changed_diff = False
    store.update(improve)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review: ParsedReviewReport(
            verdict="CHANGES_REQUESTED",
            findings=(),
            format_version="legacy",
        ),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "improve"
    assert improve.id in action["description"]
    assert "no tracked diff change" in action["description"]


def test_two_consecutive_noop_improves_return_needs_discussion(tmp_path: Path, monkeypatch) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 14, 9, 0, tzinfo=UTC)
    impl.branch = "feat/noop-stop"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
    review.report_file = "reviews/fake.md"
    store.update(review)

    for hour in (11, 12):
        improve = store.add("Improve attempt", task_type="improve", based_on=impl.id, depends_on=review.id, same_branch=True)
        improve.status = "completed"
        improve.completed_at = datetime(2026, 5, 14, hour, 0, tzinfo=UTC)
        improve.branch = impl.branch
        improve.changed_diff = False
        store.update(improve)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review: ParsedReviewReport(
            verdict="CHANGES_REQUESTED",
            findings=(),
            format_version="legacy",
        ),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "improve-no-op"
    assert action["subject_task_id"] == impl.id
    assert "2 consecutive no-op improves" in action["description"]


def test_verify_blocked_noop_improves_return_reverify_action_when_review_sha_is_stale(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "uv run pytest tests/ -q"

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 14, 9, 0, tzinfo=UTC)
    impl.branch = "feat/noop-reverify"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
    review.output_content = (
        "## Summary\n\n- Verify failed.\n\n"
        "## Blockers\n\n"
        "### B1 verify_command failure: mypy error\n"
        "Evidence: src/gza/foo.py:1: error: boom.\n"
        "Impact: autonomous verify fails.\n"
        "Required fix: fix the verify failure.\n"
        "Required tests: rerun verify_command.\n\n"
        "## Follow-Ups\n\nNone.\n\n"
        "## Questions / Assumptions\n\nNone.\n\n"
        "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    )
    review.review_verify_head_sha = "oldsha"
    store.update(review)

    for hour in (11, 12):
        improve = store.add("No-op improve", task_type="improve", based_on=impl.id, depends_on=review.id, same_branch=True)
        improve.status = "completed"
        improve.completed_at = datetime(2026, 5, 14, hour, 0, tzinfo=UTC)
        improve.branch = impl.branch
        improve.changed_diff = False
        store.update(improve)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review: ParsedReviewReport(
            verdict="CHANGES_REQUESTED",
            findings=(),
            format_version="legacy",
        ),
    )

    git = _FakeGit(
        can_merge=True,
        existing_branches={impl.branch},
        ref_shas={impl.branch: "newsha"},
    )
    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "verify_noop_improve_then_review"
    assert action["verify_provenance_state"] == "stale"
    assert action["current_branch_head_sha"] == "newsha"


def test_verify_timeout_only_reviews_reverify_instead_of_parking_when_noop_limit_is_reached(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "uv run pytest tests/ -q"

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/verify-timeout-noop-rereview",
        when=datetime(2026, 5, 18, 9, 0, tzinfo=UTC),
    )
    review1 = store.add("Review round 1", task_type="review", depends_on=impl.id)
    assert review1.id is not None
    review1.status = "completed"
    review1.completed_at = datetime(2026, 5, 18, 10, 0, tzinfo=UTC)
    review1.output_content = _timeout_only_review_report()
    store.update(review1)

    improve1 = store.add("Improve round 1", task_type="improve", based_on=impl.id, depends_on=review1.id)
    assert improve1.id is not None
    improve1.status = "completed"
    improve1.completed_at = datetime(2026, 5, 18, 11, 0, tzinfo=UTC)
    improve1.branch = impl.branch
    improve1.has_commits = True
    store.update(improve1)

    review2 = store.add("Review round 2", task_type="review", depends_on=impl.id)
    assert review2.id is not None
    review2.status = "completed"
    review2.completed_at = datetime(2026, 5, 18, 12, 0, tzinfo=UTC)
    review2.output_content = _timeout_only_review_report()
    review2.review_verify_head_sha = "stale-sha"
    store.update(review2)

    _add_completed_improve_for_review(
        store,
        impl,
        review2,
        when=datetime(2026, 5, 18, 13, 0, tzinfo=UTC),
        changed_diff=False,
    )
    _add_completed_improve_for_review(
        store,
        impl,
        review2,
        when=datetime(2026, 5, 18, 14, 0, tzinfo=UTC),
        changed_diff=False,
    )

    git = _FakeGit(
        can_merge=True,
        existing_branches={impl.branch},
        existing_refs={f"origin/{impl.branch}"},
        ref_shas={f"origin/{impl.branch}": "fresh-sha"},
    )
    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "verify_noop_improve_then_review"
    assert action["current_branch_head_sha"] == "fresh-sha"
    assert action["verify_provenance_state"] == "stale"
    assert action.get("needs_attention_reason") != "verify-blocked-no-code-issues"


def test_verify_timeout_only_reviews_still_park_without_noop_limit_trigger(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "uv run pytest tests/ -q"

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/verify-timeout-noop-below-limit",
        when=datetime(2026, 5, 18, 9, 0, tzinfo=UTC),
    )
    review1 = store.add("Review round 1", task_type="review", depends_on=impl.id)
    assert review1.id is not None
    review1.status = "completed"
    review1.completed_at = datetime(2026, 5, 18, 10, 0, tzinfo=UTC)
    review1.output_content = _timeout_only_review_report()
    store.update(review1)

    improve1 = store.add("Improve round 1", task_type="improve", based_on=impl.id, depends_on=review1.id)
    assert improve1.id is not None
    improve1.status = "completed"
    improve1.completed_at = datetime(2026, 5, 18, 11, 0, tzinfo=UTC)
    improve1.branch = impl.branch
    improve1.has_commits = True
    store.update(improve1)

    review2 = store.add("Review round 2", task_type="review", depends_on=impl.id)
    assert review2.id is not None
    review2.status = "completed"
    review2.completed_at = datetime(2026, 5, 18, 12, 0, tzinfo=UTC)
    review2.output_content = _timeout_only_review_report()
    store.update(review2)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "verify-blocked-no-code-issues"


@pytest.mark.parametrize(
    ("git_kwargs", "expected_fragment"),
    [
        (
            {"resolve_fresh_merge_source_ref_error": RuntimeError("fresh ref lookup blew up")},
            "unable to resolve freshest branch ref",
        ),
        (
            {
                "existing_refs": {"origin/feat/noop-branch-tip-proof-failure"},
                "ref_shas": {},
                "rev_parse_errors": {
                    "origin/feat/noop-branch-tip-proof-failure": RuntimeError("rev-parse blew up")
                },
            },
            "unable to resolve branch tip SHA from 'origin/feat/noop-branch-tip-proof-failure'",
        ),
    ],
)
def test_verify_blocked_noop_improves_surface_branch_tip_probe_failures(
    tmp_path: Path,
    git_kwargs: dict[str, object],
    expected_fragment: str,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "uv run pytest tests/ -q"

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/noop-branch-tip-proof-failure",
        when=datetime(2026, 5, 19, 9, 0, tzinfo=UTC),
    )
    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 19, 10, 0, tzinfo=UTC)
    review.output_content = (
        "## Summary\n\n- Verify timed out.\n\n"
        "## Blockers\n\n"
        "### B1 verify_command timeout\n"
        "Evidence: verify_command timed out after 120s.\n"
        "Impact: autonomous verify could not confirm the current branch tip.\n"
        "Required fix: rerun verify_command against the current tip.\n"
        "Required tests: rerun verify_command.\n\n"
        "## Follow-Ups\n\nNone.\n\n"
        "## Questions / Assumptions\n\nNone.\n\n"
        "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    )
    review.review_verify_head_sha = "oldsha"
    store.update(review)

    for hour in (11, 12):
        improve = store.add("No-op improve", task_type="improve", based_on=impl.id, depends_on=review.id, same_branch=True)
        improve.status = "completed"
        improve.completed_at = datetime(2026, 5, 19, hour, 0, tzinfo=UTC)
        improve.branch = impl.branch
        improve.changed_diff = False
        store.update(improve)

    git = _FakeGit(can_merge=True, **git_kwargs)
    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "verify-noop-improve-branch-tip-unavailable"
    assert expected_fragment in action["description"]
    assert "improve-no-op" not in action["description"]

def test_cross_project_verify_blocked_noop_improves_return_reverify_action_without_root_verify_command(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    _set_subdir_project_boundary(config, tmp_path)
    config.enforce_project_scope = False
    config.verify_command = ""

    sibling_project_dir = tmp_path / "dre" / "web"
    sibling_project_dir.mkdir(parents=True, exist_ok=True)
    (sibling_project_dir / "gza.yaml").write_text("project_name: dre-web\nverify_command: ./bin/web-verify\n")

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/cross-project-noop-reverify",
        when=datetime(2026, 5, 16, 9, 0, tzinfo=UTC),
    )
    impl.tags = ("cross-project",)
    store.update(impl)

    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 16, 10, 0, tzinfo=UTC)
    review.output_content = (
        "## Summary\n\n- Verify failed.\n\n"
        "## Blockers\n\n"
        "### B1 verify_command failure: web verify error\n"
        "Evidence: dre/web/src/app.tsx:1: error: boom.\n"
        "Impact: autonomous verify fails.\n"
        "Required fix: fix the verify failure.\n"
        "Required tests: rerun verify_command.\n\n"
        "## Follow-Ups\n\nNone.\n\n"
        "## Questions / Assumptions\n\nNone.\n\n"
        "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    )
    review.review_verify_head_sha = "oldsha"
    store.update(review)

    for hour in (11, 12):
        improve = store.add("No-op improve", task_type="improve", based_on=impl.id, depends_on=review.id, same_branch=True)
        improve.status = "completed"
        improve.completed_at = datetime(2026, 5, 16, hour, 0, tzinfo=UTC)
        improve.branch = impl.branch
        improve.changed_diff = False
        store.update(improve)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review: ParsedReviewReport(
            verdict="CHANGES_REQUESTED",
            findings=(),
            format_version="legacy",
        ),
    )

    git = _FakeGit(
        can_merge=True,
        existing_branches={impl.branch},
        ref_shas={impl.branch: "newsha"},
        name_status_by_range={
            "main...feat/cross-project-noop-reverify": "M\tservices/foo/app.py\nM\tdre/web/src/app.tsx\n",
        },
    )

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "verify_noop_improve_then_review"
    assert action["current_branch_head_sha"] == "newsha"


def test_cross_project_verify_blocked_noop_improves_return_reverify_action_for_branch_local_project_root(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    _set_subdir_project_boundary(config, tmp_path)
    config.enforce_project_scope = False
    config.verify_command = ""

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/cross-project-branch-local-root",
        when=datetime(2026, 5, 16, 9, 0, tzinfo=UTC),
    )
    impl.tags = ("cross-project",)
    store.update(impl)

    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 16, 10, 0, tzinfo=UTC)
    review.output_content = (
        "## Summary\n\n- Verify failed.\n\n"
        "## Blockers\n\n"
        "### B1 verify_command failure: web verify error\n"
        "Evidence: dre/web/src/app.tsx:1: error: boom.\n"
        "Impact: autonomous verify fails.\n"
        "Required fix: fix the verify failure.\n"
        "Required tests: rerun verify_command.\n\n"
        "## Follow-Ups\n\nNone.\n\n"
        "## Questions / Assumptions\n\nNone.\n\n"
        "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    )
    review.review_verify_head_sha = "oldsha"
    store.update(review)

    for hour in (11, 12):
        improve = store.add("No-op improve", task_type="improve", based_on=impl.id, depends_on=review.id, same_branch=True)
        improve.status = "completed"
        improve.completed_at = datetime(2026, 5, 16, hour, 0, tzinfo=UTC)
        improve.branch = impl.branch
        improve.changed_diff = False
        store.update(improve)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review: ParsedReviewReport(
            verdict="CHANGES_REQUESTED",
            findings=(),
            format_version="legacy",
        ),
    )

    git = _FakeGit(
        can_merge=True,
        existing_branches={impl.branch},
        ref_shas={impl.branch: "newsha"},
        name_status_by_range={
            "main...feat/cross-project-branch-local-root": "A\tdre/web/gza.yaml\nM\tdre/web/src/app.tsx\n",
        },
    )

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "verify_noop_improve_then_review"
    assert action["current_branch_head_sha"] == "newsha"


def test_cross_project_verify_blocked_noop_improves_park_when_diff_probe_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    _set_subdir_project_boundary(config, tmp_path)
    config.enforce_project_scope = False
    config.verify_command = ""

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/cross-project-diff-probe-failure",
        when=datetime(2026, 5, 16, 9, 0, tzinfo=UTC),
    )
    impl.tags = ("cross-project",)
    store.update(impl)

    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 16, 10, 0, tzinfo=UTC)
    review.output_content = (
        "## Summary\n\n- Verify failed.\n\n"
        "## Blockers\n\n"
        "### B1 verify_command failure: web verify error\n"
        "Evidence: dre/web/src/app.tsx:1: error: boom.\n"
        "Impact: autonomous verify fails.\n"
        "Required fix: fix the verify failure.\n"
        "Required tests: rerun verify_command.\n\n"
        "## Follow-Ups\n\nNone.\n\n"
        "## Questions / Assumptions\n\nNone.\n\n"
        "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    )
    review.review_verify_head_sha = "oldsha"
    store.update(review)

    for hour in (11, 12):
        improve = store.add("No-op improve", task_type="improve", based_on=impl.id, depends_on=review.id, same_branch=True)
        improve.status = "completed"
        improve.completed_at = datetime(2026, 5, 16, hour, 0, tzinfo=UTC)
        improve.branch = impl.branch
        improve.changed_diff = False
        store.update(improve)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review: ParsedReviewReport(
            verdict="CHANGES_REQUESTED",
            findings=(),
            format_version="legacy",
        ),
    )

    git = _FakeGit(
        can_merge=True,
        existing_branches={impl.branch},
        ref_shas={impl.branch: "newsha"},
        name_status_error_by_range={
            "main...feat/cross-project-diff-probe-failure": RuntimeError("diff probe exploded\nwith stderr"),
        },
    )

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["type"] not in WORKER_CONSUMING_ACTIONS
    assert action["needs_attention_reason"] == "verify-noop-improve-diff-probe-unavailable"
    assert "main...feat/cross-project-diff-probe-failure" in action["description"]
    assert "diff probe exploded with stderr" in action["description"]
    assert action["verify_command_availability_revision_range"] == "main...feat/cross-project-diff-probe-failure"
    assert action["verify_command_availability_error"] == "diff probe exploded with stderr"


def test_noop_improve_limit_preempts_max_review_cycles_when_thresholds_match(tmp_path: Path, monkeypatch) -> None:
    from gza import advance_engine as advance_engine_module

    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\nmax_review_cycles: 2\nmax_noop_improve_cycles: 2\n"
    )
    config = Config.load(tmp_path)
    db_path = tmp_path / ".gza" / "gza.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    store = SqliteTaskStore(db_path, prefix=config.project_prefix)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 14, 9, 0, tzinfo=UTC)
    impl.branch = "feat/noop-preempts-max-cycles"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
    review.report_file = "reviews/fake.md"
    store.update(review)

    for hour in (11, 12):
        improve = store.add("Improve attempt", task_type="improve", based_on=impl.id, depends_on=review.id, same_branch=True)
        improve.status = "completed"
        improve.completed_at = datetime(2026, 5, 14, hour, 0, tzinfo=UTC)
        improve.branch = impl.branch
        improve.changed_diff = False
        store.update(improve)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review: ParsedReviewReport(
            verdict="CHANGES_REQUESTED",
            findings=(),
            format_version="legacy",
        ),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "improve-no-op"
    assert action.get("needs_attention_reason") != "review-max-cycles-reached"


def test_three_consecutive_identical_primary_blockers_returns_needs_attention(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/duplicate-blocker-stop",
        when=datetime(2026, 5, 14, 9, 0, tzinfo=UTC),
    )
    review1 = _add_completed_review(store, impl, when=datetime(2026, 5, 14, 10, 0, tzinfo=UTC))
    _add_completed_improve_for_review(store, impl, review1, when=datetime(2026, 5, 14, 11, 0, tzinfo=UTC))
    review2 = _add_completed_review(store, impl, when=datetime(2026, 5, 14, 12, 0, tzinfo=UTC))
    _add_completed_improve_for_review(store, impl, review2, when=datetime(2026, 5, 14, 13, 0, tzinfo=UTC))
    review3 = _add_completed_review(store, impl, when=datetime(2026, 5, 14, 14, 0, tzinfo=UTC))

    reports = {
        review1.id: _blocker_report("### B1 `Missing guard`", citation="`src/GZA/foo.py:10-12`"),
        review2.id: _blocker_report("B1 Missing   guard", citation="src/gza/foo.py:10-12"),
        review3.id: _blocker_report("Missing guard", citation=" src/gza/foo.py:10-12 "),
    }
    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, review: reports[review.id],
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "duplicate-blocker-no-progress"
    assert action["subject_task_id"] == impl.id
    assert "3 consecutive review cycles" in action["description"]
    assert action["duplicate_blocker"]["cycles"] == 3
    assert action["duplicate_blocker"]["review_task_ids"] == (review3.id, review2.id, review1.id)
    assert classify_advance_action(action) == "needs_attention"


def test_two_duplicate_blockers_then_different_primary_blocker_does_not_bail(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/duplicate-blocker-continues",
        when=datetime(2026, 5, 14, 9, 0, tzinfo=UTC),
    )
    review1 = _add_completed_review(store, impl, when=datetime(2026, 5, 14, 10, 0, tzinfo=UTC))
    _add_completed_improve_for_review(store, impl, review1, when=datetime(2026, 5, 14, 11, 0, tzinfo=UTC))
    review2 = _add_completed_review(store, impl, when=datetime(2026, 5, 14, 12, 0, tzinfo=UTC))
    _add_completed_improve_for_review(store, impl, review2, when=datetime(2026, 5, 14, 13, 0, tzinfo=UTC))
    review3 = _add_completed_review(store, impl, when=datetime(2026, 5, 14, 14, 0, tzinfo=UTC))

    reports = {
        review1.id: _blocker_report("Missing guard", citation="src/gza/foo.py:10"),
        review2.id: _blocker_report("Missing guard", citation="src/gza/foo.py:10"),
        review3.id: _blocker_report("Handle timeout", citation="src/gza/foo.py:20"),
    }
    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, review: reports[review.id],
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "improve"
    assert action.get("needs_attention_reason") is None


def test_duplicate_blocker_counter_resets_across_completed_rebase(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/duplicate-blocker-rebase-reset",
        when=datetime(2026, 5, 14, 9, 0, tzinfo=UTC),
    )
    review1 = _add_completed_review(store, impl, when=datetime(2026, 5, 14, 10, 0, tzinfo=UTC))
    _add_completed_improve_for_review(store, impl, review1, when=datetime(2026, 5, 14, 11, 0, tzinfo=UTC))
    _add_completed_rebase(store, impl, when=datetime(2026, 5, 14, 11, 30, tzinfo=UTC), changed_diff=False)
    review2 = _add_completed_review(store, impl, when=datetime(2026, 5, 14, 12, 0, tzinfo=UTC))
    _add_completed_improve_for_review(store, impl, review2, when=datetime(2026, 5, 14, 13, 0, tzinfo=UTC))
    review3 = _add_completed_review(store, impl, when=datetime(2026, 5, 14, 14, 0, tzinfo=UTC))

    reports = {
        review1.id: _blocker_report("Missing guard", citation="src/gza/foo.py:10"),
        review2.id: _blocker_report("Missing guard", citation="src/gza/foo.py:10"),
        review3.id: _blocker_report("Missing guard", citation="src/gza/foo.py:10"),
    }
    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, review: reports[review.id],
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "improve"
    assert action.get("needs_attention_reason") is None


def test_duplicate_blocker_falls_back_to_required_fix_without_citation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/duplicate-blocker-required-fix",
        when=datetime(2026, 5, 14, 9, 0, tzinfo=UTC),
    )
    review1 = _add_completed_review(store, impl, when=datetime(2026, 5, 14, 10, 0, tzinfo=UTC))
    _add_completed_improve_for_review(store, impl, review1, when=datetime(2026, 5, 14, 11, 0, tzinfo=UTC))
    review2 = _add_completed_review(store, impl, when=datetime(2026, 5, 14, 12, 0, tzinfo=UTC))
    _add_completed_improve_for_review(store, impl, review2, when=datetime(2026, 5, 14, 13, 0, tzinfo=UTC))
    review3 = _add_completed_review(store, impl, when=datetime(2026, 5, 14, 14, 0, tzinfo=UTC))

    reports = {
        review1.id: _blocker_report("Missing guard", citation=None, fix="Return early on empty input"),
        review2.id: _blocker_report("Missing guard", citation=None, fix="return early   on empty input"),
        review3.id: _blocker_report("Missing guard", citation=None, fix="Return early on empty input"),
    }
    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, review: reports[review.id],
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "duplicate-blocker-no-progress"


def test_noop_improve_limit_preempts_duplicate_blocker_backstop(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/noop-preempts-duplicate-blocker",
        when=datetime(2026, 5, 14, 9, 0, tzinfo=UTC),
    )
    review1 = _add_completed_review(store, impl, when=datetime(2026, 5, 14, 10, 0, tzinfo=UTC))
    _add_completed_improve_for_review(store, impl, review1, when=datetime(2026, 5, 14, 11, 0, tzinfo=UTC))
    review2 = _add_completed_review(store, impl, when=datetime(2026, 5, 14, 12, 0, tzinfo=UTC))
    _add_completed_improve_for_review(store, impl, review2, when=datetime(2026, 5, 14, 13, 0, tzinfo=UTC))
    review3 = _add_completed_review(store, impl, when=datetime(2026, 5, 14, 14, 0, tzinfo=UTC))
    _add_completed_improve_for_review(
        store,
        impl,
        review3,
        when=datetime(2026, 5, 14, 15, 0, tzinfo=UTC),
        changed_diff=False,
    )
    _add_completed_improve_for_review(
        store,
        impl,
        review3,
        when=datetime(2026, 5, 14, 16, 0, tzinfo=UTC),
        changed_diff=False,
    )

    reports = {
        review1.id: _blocker_report("Missing guard", citation="src/gza/foo.py:10"),
        review2.id: _blocker_report("Missing guard", citation="src/gza/foo.py:10"),
        review3.id: _blocker_report("Missing guard", citation="src/gza/foo.py:10"),
    }
    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, review: reports[review.id],
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "improve-no-op"


def test_duplicate_blocker_backstop_preempts_max_review_cycles(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    (tmp_path / "gza.yaml").write_text("project_name: test-project\nmax_review_cycles: 3\n")
    config = Config.load(tmp_path)
    db_path = tmp_path / ".gza" / "gza.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    store = SqliteTaskStore(db_path, prefix=config.project_prefix)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/duplicate-blocker-preempts-max-cycles",
        when=datetime(2026, 5, 14, 9, 0, tzinfo=UTC),
    )
    review1 = _add_completed_review(store, impl, when=datetime(2026, 5, 14, 10, 0, tzinfo=UTC))
    _add_completed_improve_for_review(store, impl, review1, when=datetime(2026, 5, 14, 11, 0, tzinfo=UTC))
    review2 = _add_completed_review(store, impl, when=datetime(2026, 5, 14, 12, 0, tzinfo=UTC))
    _add_completed_improve_for_review(store, impl, review2, when=datetime(2026, 5, 14, 13, 0, tzinfo=UTC))
    review3 = _add_completed_review(store, impl, when=datetime(2026, 5, 14, 14, 0, tzinfo=UTC))
    _add_completed_improve_for_review(store, impl, review3, when=datetime(2026, 5, 14, 15, 0, tzinfo=UTC))
    review4 = _add_completed_review(store, impl, when=datetime(2026, 5, 14, 16, 0, tzinfo=UTC))

    reports = {
        review1.id: _blocker_report("Missing guard", citation="src/gza/foo.py:10"),
        review2.id: _blocker_report("Missing guard", citation="src/gza/foo.py:10"),
        review3.id: _blocker_report("Missing guard", citation="src/gza/foo.py:10"),
        review4.id: _blocker_report("Missing guard", citation="src/gza/foo.py:10"),
    }
    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, review: reports[review.id],
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "duplicate-blocker-no-progress"
    assert action.get("needs_attention_reason") != "review-max-cycles-reached"


def test_threshold_reached_with_in_progress_improve_still_waits(tmp_path: Path, monkeypatch) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 14, 9, 0, tzinfo=UTC)
    impl.branch = "feat/noop-wait"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
    review.report_file = "reviews/fake.md"
    store.update(review)

    for hour in (11, 12):
        improve = store.add("No-op improve", task_type="improve", based_on=impl.id, depends_on=review.id, same_branch=True)
        improve.status = "completed"
        improve.completed_at = datetime(2026, 5, 14, hour, 0, tzinfo=UTC)
        improve.branch = impl.branch
        improve.changed_diff = False
        store.update(improve)

    active_improve = store.add("Active improve", task_type="improve", based_on=impl.id, depends_on=review.id, same_branch=True)
    assert active_improve.id is not None
    active_improve.status = "in_progress"
    active_improve.started_at = datetime(2026, 5, 14, 13, 0, tzinfo=UTC)
    active_improve.branch = impl.branch
    store.update(active_improve)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review: ParsedReviewReport(
            verdict="CHANGES_REQUESTED",
            findings=(),
            format_version="legacy",
        ),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "wait_improve"
    assert action["improve_task"].id == active_improve.id
    assert active_improve.id in action["description"]


def test_threshold_reached_with_pending_improve_still_runs(tmp_path: Path, monkeypatch) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 14, 9, 0, tzinfo=UTC)
    impl.branch = "feat/noop-run"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
    review.report_file = "reviews/fake.md"
    store.update(review)

    for hour in (11, 12):
        improve = store.add("No-op improve", task_type="improve", based_on=impl.id, depends_on=review.id, same_branch=True)
        improve.status = "completed"
        improve.completed_at = datetime(2026, 5, 14, hour, 0, tzinfo=UTC)
        improve.branch = impl.branch
        improve.changed_diff = False
        store.update(improve)

    pending_improve = store.add("Pending improve", task_type="improve", based_on=impl.id, depends_on=review.id, same_branch=True)
    assert pending_improve.id is not None
    pending_improve.status = "pending"
    pending_improve.branch = impl.branch
    store.update(pending_improve)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review: ParsedReviewReport(
            verdict="CHANGES_REQUESTED",
            findings=(),
            format_version="legacy",
        ),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "run_improve"
    assert action["improve_task"].id == pending_improve.id
    assert pending_improve.id in action["description"]


def test_allow_noop_improve_tag_bypasses_stop_rule(tmp_path: Path, monkeypatch) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement", tags=("allow-noop-improve",))
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 14, 9, 0, tzinfo=UTC)
    impl.branch = "feat/noop-tag"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
    review.report_file = "reviews/fake.md"
    store.update(review)

    for hour in (11, 12):
        improve = store.add("Improve attempt", task_type="improve", based_on=impl.id, depends_on=review.id, same_branch=True)
        improve.status = "completed"
        improve.completed_at = datetime(2026, 5, 14, hour, 0, tzinfo=UTC)
        improve.branch = impl.branch
        improve.changed_diff = False
        store.update(improve)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review: ParsedReviewReport(
            verdict="CHANGES_REQUESTED",
            findings=(),
            format_version="legacy",
        ),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "improve"
    assert "allow-noop-improve" in action["description"]


def test_legacy_unknown_changed_diff_does_not_trigger_noop_stop_rule(tmp_path: Path, monkeypatch) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 14, 9, 0, tzinfo=UTC)
    impl.branch = "feat/noop-legacy"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
    review.report_file = "reviews/fake.md"
    store.update(review)

    legacy_improve = store.add("Legacy improve", task_type="improve", based_on=impl.id, depends_on=review.id, same_branch=True)
    legacy_improve.status = "completed"
    legacy_improve.completed_at = datetime(2026, 5, 14, 11, 0, tzinfo=UTC)
    legacy_improve.branch = impl.branch
    legacy_improve.changed_diff = None
    store.update(legacy_improve)

    latest_improve = store.add("Latest improve", task_type="improve", based_on=legacy_improve.id, depends_on=review.id, same_branch=True)
    latest_improve.status = "completed"
    latest_improve.completed_at = datetime(2026, 5, 14, 12, 0, tzinfo=UTC)
    latest_improve.branch = impl.branch
    latest_improve.changed_diff = False
    store.update(latest_improve)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review: ParsedReviewReport(
            verdict="CHANGES_REQUESTED",
            findings=(),
            format_version="legacy",
        ),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "create_review"
    assert action.get("needs_attention_reason") is None


def test_comments_triggered_noop_improves_use_same_stop_rule(tmp_path: Path, monkeypatch) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 14, 9, 0, tzinfo=UTC)
    impl.branch = "feat/noop-comments"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
    review.report_file = "reviews/fake.md"
    store.update(review)
    store.add_comment(impl.id, "Unresolved comments still remain", source="direct")

    for hour in (11, 12):
        improve = store.add("Comments improve", task_type="improve", based_on=impl.id, depends_on=review.id, same_branch=True)
        improve.status = "completed"
        improve.completed_at = datetime(2026, 5, 14, hour, 0, tzinfo=UTC)
        improve.branch = impl.branch
        improve.changed_diff = False
        store.update(improve)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review: ParsedReviewReport(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "improve-no-op"
    assert action["subject_task_id"] == impl.id
    assert "unresolved comments" in action["description"]


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
    assert action["subject_task_id"] == impl.id


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
    assert action["subject_task_id"] == impl.id
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


def test_failed_timeout_implement_no_review_stays_in_recovery_not_merge(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.require_review_before_merge = False

    impl = store.add("Failed timeout implementation", task_type="implement")
    assert impl.id is not None
    impl.status = "failed"
    impl.failure_reason = "TIMEOUT"
    impl.session_id = "sess-timeout"
    impl.completed_at = datetime(2026, 5, 14, 9, 0, tzinfo=UTC)
    impl.branch = "feature/failed-timeout"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    action = evaluate_advance_rules(
        config,
        store,
        _FakeGit(
            can_merge=True,
            can_merge_by_ref={("origin/feature/failed-timeout", "main"): True},
            existing_refs={"origin/feature/failed-timeout"},
        ),
        impl,
        "main",
    )

    assert action["type"] == "resume"


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
    assert action["description"] == "SKIP: moot (no task commits)"
    refreshed_unit = store.resolve_merge_unit_for_task(impl.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "empty"

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=False),
        config=config,
        git=git,
        target_branch="main",
    )
    assert all(row.owner_task.id != impl.id for row in rows)


def test_already_merged_branch_persists_merged_when_tip_is_ancestor_not_equal_target_tip(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/ancestor-merged-not-equal-tip",
        when=datetime(2026, 5, 14, 9, 0, tzinfo=UTC),
    )

    git = _FakeGit(
        can_merge=False,
        is_merged_by_ref={("origin/feat/ancestor-merged-not-equal-tip", "main"): True},
        existing_refs={"origin/feat/ancestor-merged-not-equal-tip"},
        ref_shas={
            "origin/feat/ancestor-merged-not-equal-tip": "branch-sha",
            "main": "target-sha",
        },
        ancestor_pairs={("main", "origin/feat/ancestor-merged-not-equal-tip"): False},
    )

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "skip"
    assert action["description"] == "SKIP: already merged into target branch"
    refreshed = store.get(impl.id)
    assert refreshed is not None
    assert refreshed.merge_status == "merged"

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=False),
        config=config,
        git=_FakeGit(can_merge=False),
        target_branch="main",
    )
    assert all(row.owner_task.id != impl.id for row in rows)


def test_empty_branch_persists_empty_and_skips_merge_actions(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/empty-terminal",
        when=datetime(2026, 5, 14, 9, 0, tzinfo=UTC),
    )

    git = _FakeGit(
        can_merge=False,
        is_merged_by_ref={("origin/feat/empty-terminal", "main"): True},
        existing_refs={"origin/feat/empty-terminal"},
        ahead_count=0,
    )

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "skip"
    assert action["description"] == "SKIP: moot (no task commits)"
    refreshed_unit = store.resolve_merge_unit_for_task(impl.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "empty"
    assert refreshed_unit.merged_at is None
    assert refreshed_unit.merged_by_task_id is None

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=False),
        config=config,
        git=_FakeGit(can_merge=False),
        target_branch="main",
    )
    assert all(row.owner_task.id != impl.id for row in rows)


def test_post_merge_rebase_state_does_not_persist_merged_for_in_progress_implement(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "in_progress"
    impl.branch = "feature/in-progress-equals-target"
    impl.merge_status = "unmerged"
    impl.has_commits = False
    store.update(impl)

    git = _FakeGit(
        can_merge=False,
        ref_shas={
            impl.branch: "same-sha",
            "main": "same-sha",
        },
    )

    state = _resolve_and_persist_post_merge_rebase_state(store, git, impl, "main")

    assert state.already_merged is True
    assert state.reason == "branch-tip-equals-target-tip"
    assert store.resolve_merge_unit_for_task(impl.id) is None
    refreshed = store.get(impl.id)
    assert refreshed is not None
    assert refreshed.merge_status == "unmerged"


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


def test_unmerged_branch_does_not_persist_merged_from_live_check(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/still-unmerged",
        when=datetime(2026, 5, 14, 9, 0, tzinfo=UTC),
    )

    action = evaluate_advance_rules(
        config,
        store,
        _FakeGit(
            can_merge=True,
            is_merged_by_ref={("origin/feat/still-unmerged", "main"): False},
            existing_refs={"origin/feat/still-unmerged"},
        ),
        impl,
        "main",
    )

    assert action["type"] == "create_review"
    refreshed = store.get(impl.id)
    assert refreshed is not None
    assert refreshed.merge_status == "unmerged"


def test_already_rebased_incomplete_lineage_returns_needs_attention_instead_of_rebase(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    pending_resume = store.add("Resume implement", task_type="implement")
    assert pending_resume.id is not None
    pending_resume.status = "pending"
    pending_resume.branch = "feature/already-rebased-incomplete"
    pending_resume.merge_status = "unmerged"
    store.update(pending_resume)

    action = evaluate_advance_rules(
        config,
        store,
        _FakeGit(
            can_merge=False,
            ref_shas={
                pending_resume.branch: "branch-sha",
                "main": "target-sha",
            },
            ancestor_pairs={("main", pending_resume.branch): True},
        ),
        pending_resume,
        "main",
    )

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "branch-already-rebased-lineage-incomplete"
    assert action["subject_task_id"] == pending_resume.id
    assert "already contains the target tip" in action["description"]
    assert "no further rebase will help" in action["description"]
    assert classify_advance_action(action) == "needs_attention"


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


def test_conflict_needs_rebase_emitted_without_completed_rebase(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 14, 9, 0, tzinfo=UTC)
    impl.branch = "feature/rebase-needed"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=False), impl, "main")

    assert action["type"] == "needs_rebase"


def test_rebase_failure_circuit_breaker_trips_after_three_failures_without_progress(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feature/rebase-breaker",
        when=datetime(2026, 5, 14, 9, 0, tzinfo=UTC),
    )

    _add_failed_rebase_attempts(store, impl, hours=(10, 11, 12))

    action = evaluate_advance_rules(
        config,
        store,
        _FakeGit(
            can_merge=False,
            ref_shas={
                impl.branch: "branch-sha",
                "main": "target-sha",
            },
            ancestor_pairs={("main", impl.branch): True},
        ),
        impl,
        "main",
    )

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "rebase-failure-circuit-breaker"
    assert action["subject_task_id"] == impl.id
    assert "feature/rebase-breaker" in action["description"]
    assert "after 3 failed attempts" in action["description"]
    assert action["rebase_failure_streak"]["attempts"] == 3
    assert classify_advance_action(action) == "needs_attention"


def test_rebase_failure_circuit_breaker_resets_after_completed_rebase(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    impl = _make_completed_unmerged_impl(
        store,
        branch="feature/rebase-breaker-reset-rebase",
        when=datetime(2026, 5, 14, 9, 0, tzinfo=UTC),
    )
    _add_failed_rebase_attempts(store, impl, hours=(10, 11, 12))
    _add_completed_rebase(store, impl, when=datetime(2026, 5, 14, 13, 0, tzinfo=UTC))

    git = _FakeGit(
        can_merge=False,
        existing_refs={"origin/feature/rebase-breaker-reset-rebase"},
        ref_shas={
            "origin/feature/rebase-breaker-reset-rebase": "branch-tip",
            "main": "target-tip",
        },
        ancestor_pairs={("main", "origin/feature/rebase-breaker-reset-rebase"): True},
    )
    ctx = resolve_advance_context(config, store, git, impl, "main")
    assert ctx.rebase_failure_streak is None

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "rebase-did-not-unblock-merge"
    assert classify_advance_action(action) == "needs_attention"


def test_rebase_failure_circuit_breaker_resets_after_completed_review(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    impl = _make_completed_unmerged_impl(
        store,
        branch="feature/rebase-breaker-reset-review",
        when=datetime(2026, 5, 14, 9, 0, tzinfo=UTC),
    )
    _add_failed_rebase_attempts(store, impl, hours=(10, 11, 12))
    _add_completed_review(store, impl, when=datetime(2026, 5, 14, 13, 0, tzinfo=UTC))

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review: ParsedReviewReport(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    git = _FakeGit(can_merge=False)
    ctx = resolve_advance_context(config, store, git, impl, "main")
    assert ctx.rebase_failure_streak is None

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "needs_rebase"
    assert "needs_attention_reason" not in action


def test_rebase_failure_circuit_breaker_resets_after_completed_code_change(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    impl = _make_completed_unmerged_impl(
        store,
        branch="feature/rebase-breaker-reset-code-change",
        when=datetime(2026, 5, 14, 9, 0, tzinfo=UTC),
    )
    review = _add_completed_review(store, impl, when=datetime(2026, 5, 14, 9, 30, tzinfo=UTC))
    _add_failed_rebase_attempts(store, impl, hours=(10, 11, 12))
    _add_completed_improve_for_review(
        store,
        impl,
        review,
        when=datetime(2026, 5, 14, 13, 0, tzinfo=UTC),
        changed_diff=True,
    )

    git = _FakeGit(can_merge=False)
    ctx = resolve_advance_context(config, store, git, impl, "main")
    assert ctx.rebase_failure_streak is None

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "rebase-failed-needs-manual-resolution"
    assert classify_advance_action(action) == "needs_attention"


def test_completed_rebase_that_still_blocks_merge_needs_attention(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 14, 9, 0, tzinfo=UTC)
    impl.branch = "feature/rebase-still-blocked"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    rebase = store.add("Completed rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    assert rebase.id is not None
    rebase.status = "completed"
    rebase.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
    rebase.branch = impl.branch
    rebase.merge_status = "unmerged"
    rebase.has_commits = True
    store.update(rebase)

    git = _FakeGit(
        can_merge=False,
        existing_refs={"origin/feature/rebase-still-blocked"},
        ref_shas={
            "origin/feature/rebase-still-blocked": "branch-tip",
            "main": "target-tip",
        },
        ancestor_pairs={("main", "origin/feature/rebase-still-blocked"): True},
    )

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "rebase-did-not-unblock-merge"
    assert classify_advance_action(action) == "needs_attention"
    assert action["subject_task_id"] == impl.id


def test_stale_completed_rebase_falls_through_to_needs_rebase(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    impl = _make_completed_unmerged_impl(
        store,
        branch="feature/stale-completed-rebase",
        when=datetime(2026, 5, 14, 9, 0, tzinfo=UTC),
    )
    review = _add_completed_review(store, impl, when=datetime(2026, 5, 14, 10, 0, tzinfo=UTC))
    _add_completed_rebase(store, impl, when=datetime(2026, 5, 14, 11, 0, tzinfo=UTC))
    _add_completed_improve_for_review(
        store,
        impl,
        review,
        when=datetime(2026, 5, 14, 12, 0, tzinfo=UTC),
        changed_diff=True,
    )

    git = _FakeGit(
        can_merge=False,
        existing_refs={"origin/feature/stale-completed-rebase"},
        ref_shas={
            "origin/feature/stale-completed-rebase": "branch-tip",
            "main": "new-target-tip",
        },
        ancestor_pairs={("main", "origin/feature/stale-completed-rebase"): False},
    )

    ctx = resolve_advance_context(config, store, git, impl, "main")
    assert ctx.merge_source_ref == "origin/feature/stale-completed-rebase"
    assert ctx.post_merge_rebase_state is not None
    assert ctx.post_merge_rebase_state.branch_tip_sha == "branch-tip"
    assert ctx.post_merge_rebase_state.target_tip_sha == "new-target-tip"
    assert ctx.post_merge_rebase_state.target_is_ancestor_of_branch is False
    assert ctx.post_merge_rebase_state.reason is None
    assert ctx.post_merge_rebase_state.warning is None

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "needs_rebase"
    assert action.get("needs_attention_reason") != "rebase-did-not-unblock-merge"


def test_orphan_rebase_descendant_skips_when_canonical_target_merge_unit_is_merged(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 14, 9, 0, tzinfo=UTC)
    impl.branch = "feature/canonical-merged"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)
    unit = store.get_or_create_merge_unit_for_task(impl)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "merged", merged_by_task_id=impl.id)

    orphan_rebase = store.add(
        "Retry orphan rebase",
        task_type="rebase",
        based_on=impl.id,
        same_branch=True,
    )
    assert orphan_rebase.id is not None
    orphan_rebase.status = "completed"
    orphan_rebase.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
    orphan_rebase.branch = "feature/canonical-merged-orphan"
    orphan_rebase.merge_status = "unmerged"
    orphan_rebase.has_commits = True
    store.update(orphan_rebase)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=False), orphan_rebase, "main")

    assert action["type"] == "skip"
    assert action["description"] == "SKIP: target implementation already merged (merge-unit-merged)"


def test_orphan_rebase_descendant_skips_when_canonical_target_has_no_merge_unit(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 14, 9, 0, tzinfo=UTC)
    impl.branch = "feature/no-merge-unit"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)
    assert store.resolve_merge_unit_for_task(impl.id) is None

    orphan_rebase = store.add(
        "Retry orphan rebase",
        task_type="rebase",
        based_on=impl.id,
        same_branch=True,
    )
    assert orphan_rebase.id is not None
    orphan_rebase.status = "completed"
    orphan_rebase.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
    orphan_rebase.branch = "feature/no-merge-unit-orphan"
    orphan_rebase.merge_status = "unmerged"
    orphan_rebase.has_commits = True
    store.update(orphan_rebase)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=False), orphan_rebase, "main")

    assert action["type"] == "skip"
    assert action["description"] == (
        "SKIP: rebase target has no merge unit (rebase-target-missing-merge-unit)"
    )


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


def test_empty_branch_skips_with_moot_no_work_text(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement empty branch", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 14, 4, 0, tzinfo=UTC)
    impl.branch = "feat/empty-branch"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    action = evaluate_advance_rules(
        config,
        store,
        _FakeGit(
            ref_shas={
                "feat/empty-branch": "same-sha",
                "main": "same-sha",
            },
        ),
        impl,
        "main",
        persist_post_merge_rebase_state=False,
    )

    assert action["type"] == "skip"
    assert action["description"] == "SKIP: moot (no task commits)"


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


def test_two_consecutive_verify_timeout_only_reviews_need_attention(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    impl.branch = "feat/verify-timeout-only"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review1 = store.add("Review round 1", task_type="review", depends_on=impl.id)
    assert review1.id is not None
    review1.status = "completed"
    review1.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    review1.output_content = _timeout_only_review_report()
    store.update(review1)

    improve1 = store.add("Improve round 1", task_type="improve", based_on=impl.id, depends_on=review1.id)
    improve1.status = "completed"
    improve1.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    improve1.branch = impl.branch
    improve1.has_commits = True
    store.update(improve1)

    review2 = store.add("Review round 2", task_type="review", depends_on=impl.id)
    review2.status = "completed"
    review2.completed_at = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    review2.output_content = _timeout_only_review_report()
    store.update(review2)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert classify_advance_action(action) == "needs_attention"
    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "verify-blocked-no-code-issues"
    assert action["subject_task_id"] == impl.id


def test_single_verify_timeout_only_review_still_creates_improve(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    impl.branch = "feat/verify-timeout-one-review"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review = store.add("Review round 1", task_type="review", depends_on=impl.id)
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    review.output_content = _timeout_only_review_report()
    store.update(review)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "improve"
    assert action["review_task"].id == review.id


def test_two_consecutive_evidence_only_verify_timeout_reviews_need_attention(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    impl.branch = "feat/verify-timeout-evidence-only"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review1 = store.add("Review round 1", task_type="review", depends_on=impl.id)
    assert review1.id is not None
    review1.status = "completed"
    review1.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    review1.output_content = _timeout_only_review_report_evidence_only()
    store.update(review1)

    improve1 = store.add("Improve round 1", task_type="improve", based_on=impl.id, depends_on=review1.id)
    improve1.status = "completed"
    improve1.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    improve1.branch = impl.branch
    improve1.has_commits = True
    store.update(improve1)

    review2 = store.add("Review round 2", task_type="review", depends_on=impl.id)
    review2.status = "completed"
    review2.completed_at = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    review2.output_content = _timeout_only_review_report_evidence_only()
    store.update(review2)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert classify_advance_action(action) == "needs_attention"
    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "verify-blocked-no-code-issues"


def test_unstructured_mixed_reviews_do_not_trigger_verify_timeout_attention(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    impl.branch = "feat/unstructured-mixed-blockers"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review1 = store.add("Review round 1", task_type="review", depends_on=impl.id)
    assert review1.id is not None
    review1.status = "completed"
    review1.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    review1.output_content = _unstructured_mixed_review_report()
    store.update(review1)

    improve1 = store.add("Improve round 1", task_type="improve", based_on=impl.id, depends_on=review1.id)
    improve1.status = "completed"
    improve1.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    improve1.branch = impl.branch
    improve1.has_commits = True
    store.update(improve1)

    review2 = store.add("Review round 2", task_type="review", depends_on=impl.id)
    review2.status = "completed"
    review2.completed_at = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    review2.output_content = _unstructured_mixed_review_report()
    store.update(review2)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "improve"
    assert action["review_task"].id == review2.id
    assert action.get("needs_attention_reason") != "verify-blocked-no-code-issues"


def test_structured_code_blocker_with_timeout_evidence_does_not_trigger_verify_timeout_attention(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    impl.branch = "feat/structured-timeout-false-positive"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review1 = store.add("Review round 1", task_type="review", depends_on=impl.id)
    assert review1.id is not None
    review1.status = "completed"
    review1.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    review1.output_content = _structured_code_blocker_with_timeout_evidence_review_report()
    store.update(review1)

    improve1 = store.add("Improve round 1", task_type="improve", based_on=impl.id, depends_on=review1.id)
    improve1.status = "completed"
    improve1.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    improve1.branch = impl.branch
    improve1.has_commits = True
    store.update(improve1)

    review2 = store.add("Review round 2", task_type="review", depends_on=impl.id)
    review2.status = "completed"
    review2.completed_at = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    review2.output_content = _structured_code_blocker_with_timeout_evidence_review_report()
    store.update(review2)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "improve"
    assert action["review_task"].id == review2.id
    assert action.get("needs_attention_reason") != "verify-blocked-no-code-issues"


def test_code_focused_blocker_with_open_state_citation_does_not_trigger_verify_timeout_attention(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    impl.branch = "feat/code-focused-timeout-symptom"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review1 = store.add("Review round 1", task_type="review", depends_on=impl.id)
    assert review1.id is not None
    review1.status = "completed"
    review1.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    review1.output_content = _code_focused_blocker_with_timeout_in_open_state_review_report()
    store.update(review1)

    improve1 = store.add(
        "Improve round 1", task_type="improve", based_on=impl.id, depends_on=review1.id
    )
    improve1.status = "completed"
    improve1.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    improve1.branch = impl.branch
    improve1.has_commits = True
    store.update(improve1)

    review2 = store.add("Review round 2", task_type="review", depends_on=impl.id)
    review2.status = "completed"
    review2.completed_at = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    review2.output_content = _code_focused_blocker_with_timeout_in_open_state_review_report()
    store.update(review2)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "improve"
    assert action["review_task"].id == review2.id
    assert action.get("needs_attention_reason") != "verify-blocked-no-code-issues"


def test_mixed_blockers_still_hit_duplicate_blocker_reason(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.max_review_cycles = 2

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    impl.branch = "feat/mixed-blockers"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review1 = store.add("Review round 1", task_type="review", depends_on=impl.id)
    assert review1.id is not None
    review1.status = "completed"
    review1.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    review1.output_content = _mixed_review_report()
    store.update(review1)

    improve1 = store.add("Improve round 1", task_type="improve", based_on=impl.id, depends_on=review1.id)
    improve1.status = "completed"
    improve1.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    improve1.branch = impl.branch
    improve1.has_commits = True
    store.update(improve1)

    review2 = store.add("Review round 2", task_type="review", depends_on=impl.id)
    review2.status = "completed"
    review2.completed_at = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    review2.output_content = _mixed_review_report()
    store.update(review2)

    improve2 = store.add("Improve round 2", task_type="improve", based_on=improve1.id, depends_on=review2.id)
    improve2.status = "completed"
    improve2.completed_at = datetime(2026, 5, 10, 13, 0, tzinfo=UTC)
    improve2.branch = impl.branch
    improve2.has_commits = True
    store.update(improve2)

    review3 = store.add("Review round 3", task_type="review", depends_on=impl.id)
    review3.status = "completed"
    review3.completed_at = datetime(2026, 5, 10, 14, 0, tzinfo=UTC)
    review3.output_content = _mixed_review_report()
    store.update(review3)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert classify_advance_action(action) == "needs_attention"
    assert action["needs_attention_reason"] == "duplicate-blocker-no-progress"


def test_latest_two_timeout_only_reviews_override_max_cycles_reason(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    impl.branch = "feat/verify-timeout-late"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review1 = store.add("Review round 1", task_type="review", depends_on=impl.id)
    assert review1.id is not None
    review1.status = "completed"
    review1.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    review1.output_content = _mixed_review_report()
    store.update(review1)

    improve1 = store.add("Improve round 1", task_type="improve", based_on=impl.id, depends_on=review1.id)
    assert improve1.id is not None
    improve1.status = "completed"
    improve1.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    improve1.branch = impl.branch
    improve1.has_commits = True
    store.update(improve1)

    review2 = store.add("Review round 2", task_type="review", depends_on=impl.id)
    assert review2.id is not None
    review2.status = "completed"
    review2.completed_at = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    review2.output_content = _timeout_only_review_report()
    store.update(review2)

    improve2 = store.add("Improve round 2", task_type="improve", based_on=improve1.id, depends_on=review2.id)
    assert improve2.id is not None
    improve2.status = "completed"
    improve2.completed_at = datetime(2026, 5, 10, 13, 0, tzinfo=UTC)
    improve2.branch = impl.branch
    improve2.has_commits = True
    store.update(improve2)

    review3 = store.add("Review round 3", task_type="review", depends_on=impl.id)
    review3.status = "completed"
    review3.completed_at = datetime(2026, 5, 10, 14, 0, tzinfo=UTC)
    review3.output_content = _timeout_only_review_report()
    store.update(review3)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert classify_advance_action(action) == "needs_attention"
    assert action["needs_attention_reason"] == "verify-blocked-no-code-issues"


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


def test_diverged_local_and_origin_are_routed_to_reconcile(tmp_path: Path) -> None:
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

    assert action["type"] == "reconcile_branch_divergence"
    assert "Reconcile diverged local/origin refs" in action["description"]


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

    assert action["type"] == "reconcile_branch_divergence"
    assert "target implementation already merged" not in action["description"]

    refreshed_unit = store.resolve_merge_unit_for_task(impl.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"

    ctx = resolve_advance_context(config, store, git, impl, "main")
    assert ctx.post_merge_rebase_state is not None
    assert ctx.post_merge_rebase_state.already_merged is False
    assert ctx.post_merge_rebase_state.warning is not None
    assert "diverged" in ctx.post_merge_rebase_state.warning


def test_review_unknown_verdict_uses_reviewed_task_as_subject(tmp_path: Path, monkeypatch) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/review-unknown-verdict",
        when=datetime(2026, 5, 15, 9, 0, tzinfo=UTC),
    )
    review = _add_completed_review(store, impl, when=datetime(2026, 5, 15, 10, 0, tzinfo=UTC))

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review: ParsedReviewReport(
            verdict="SOMETHING_ELSE",
            findings=(),
            format_version="legacy",
        ),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "review-verdict-needs-manual-attention"
    assert action["subject_task_id"] == impl.id
    assert review.id is not None
    assert action["review_task"].id == review.id


def test_require_needs_attention_subject_rejects_missing_subject() -> None:
    with pytest.raises(AssertionError, match="missing subject_task_id"):
        require_needs_attention_subject(
            {
                "type": "needs_discussion",
                "needs_attention_reason": "needs-discussion",
                "description": "SKIP: manual intervention required",
            }
        )


@pytest.mark.parametrize(
    ("action", "expected_warning"),
    [
        (
            {
                "type": "needs_discussion",
                "description": "SKIP: manual intervention required",
                "needs_attention_reason": "retry-limit-reached",
            },
            "without subject_task_id",
        ),
        (
            {
                "type": "needs_discussion",
                "description": "SKIP: manual intervention required",
                "needs_attention_reason": "retry-limit-reached",
                "subject_task_id": "",
            },
            "with unusable subject_task_id=''",
        ),
        (
            {
                "type": "needs_discussion",
                "description": "SKIP: manual intervention required",
                "needs_attention_reason": "retry-limit-reached",
                "subject_task_id": 123,
            },
            "with unusable subject_task_id=123",
        ),
    ],
)
def test_resolve_subject_task_warns_before_falling_back_for_missing_or_unusable_subject(
    tmp_path: Path,
    action: dict[str, object],
    expected_warning: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = _make_store(tmp_path)
    fallback_task = store.add("Fallback implement", task_type="implement")
    assert fallback_task.id is not None

    with caplog.at_level("WARNING", logger="gza.advance_engine"):
        subject_task = resolve_subject_task(store, action, fallback_task=fallback_task)

    assert subject_task.id == fallback_task.id
    assert expected_warning in caplog.text


def test_all_needs_attention_rule_actions_declare_subject_task_id(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 18, 9, 0, tzinfo=UTC)
    impl.branch = "feature/generic-attention-subjects"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 18, 10, 0, tzinfo=UTC)
    store.update(review)

    noop_improve = store.add(
        "No-op improve",
        task_type="improve",
        based_on=impl.id,
        depends_on=review.id,
        same_branch=True,
    )
    assert noop_improve.id is not None
    noop_improve.status = "completed"
    noop_improve.completed_at = datetime(2026, 5, 18, 11, 0, tzinfo=UTC)
    noop_improve.changed_diff = False
    store.update(noop_improve)

    failed_rebase = store.add(
        "Failed rebase",
        task_type="rebase",
        based_on=impl.id,
        same_branch=True,
    )
    assert failed_rebase.id is not None
    failed_rebase.status = "failed"
    failed_rebase.failure_reason = "MERGE_CONFLICT"
    failed_rebase.completed_at = datetime(2026, 5, 18, 12, 0, tzinfo=UTC)
    failed_rebase.branch = impl.branch
    store.update(failed_rebase)

    ctx = SimpleNamespace(
        store=store,
        task=impl,
        task_type=impl.task_type,
        has_non_dropped_implement_descendant=False,
        auto_implement_enabled=False,
        merge_source_warning="feature branch diverged",
        strict_scope_inspection_error="fatal: bad revision",
        post_merge_rebase_state=SimpleNamespace(
            already_merged=False,
            rebase_target_missing_merge_unit=False,
            reason="manual-resolution",
        ),
            can_merge=False,
            rebase_pending_or_running=None,
            rebase_failed=failed_rebase,
            latest_completed_rebase=failed_rebase,
            rebase_failure_streak=SimpleNamespace(
                attempts=3,
                branch=impl.branch,
                failed_task_ids=(failed_rebase.id,),
            ),
            rebase_invalidates_review=False,
            active_review=None,
            review_invalidated_by_rebase=failed_rebase,
            review_preserved_by_rebase=None,
            latest_completed_review=review,
            latest_completed_code_change=impl,
            review_cleared=False,
        review_verdict="CHANGES_REQUESTED",
        followup_findings=(),
        recent_verify_timeout_only_reviews=(review, review),
        has_fresh_unresolved_comments_since_latest_review=True,
        active_improve_running=None,
        active_improve_pending=None,
        latest_noop_improve=noop_improve,
        consecutive_noop_improves=2,
        max_noop_improve_cycles=2,
        noop_improve_allowed=False,
        noop_improve_trigger="comments",
        duplicate_blocker_streak=SimpleNamespace(
            cycles=3,
            title="Repeated blocker",
            anchor="src/gza/module.py:1",
            review_task_ids=(review.id,),
        ),
        max_review_cycles=3,
        completed_review_cycles=3,
        failed_recovery_decision=FailedRecoveryDecision(
            task_id=impl.id,
            action="skip",
            reason_code="retry_limit_reached",
            reason_text="manual review required",
            launch_mode="none",
            attempt_index=1,
            attempt_limit=1,
        ),
        failed_recovery_attention_reason="retry-limit-reached",
        closing_review_action={
            "type": "needs_discussion",
            "description": "SKIP: closing review invariant needs manual attention",
            "needs_attention_reason": "closing-review-invariant",
        },
        requires_review=True,
        create_reviews=False,
    )

    names_with_needs_attention: set[str] = set()
    for rule in ADVANCE_RULES:
        action = rule.action(ctx)
        if classify_advance_action(action) != "needs_attention":
            continue
        names_with_needs_attention.add(rule.name)
        assert require_needs_attention_subject(action), rule.name

    assert names_with_needs_attention == {
        "failed_task_skip",
        "awaiting_human_plan_review",
        "explore_needs_followup_decision",
        "merge_source_needs_manual_resolution",
        "strict_project_scope_unverified",
        "strict_project_scope_violation",
        "conflict_rebase_failure_circuit_breaker",
        "conflict_rebase_failed",
        "conflict_rebase_completed_but_still_blocked",
        "already_rebased_but_lineage_incomplete",
        "stale_review_needs_manual_refresh",
        "failed_rebase_without_successful_review",
        "closing_review_invariant",
        "fresh_comments_noop_improve_limit",
        "review_verify_blocked_no_code_issues",
        "review_noop_improve_limit",
        "review_duplicate_blocker_no_progress",
        "review_max_cycles",
        "review_unknown_verdict",
        "implement_needs_manual_review",
    }


@pytest.mark.parametrize(
    ("closing_review_status", "expected_action_type"),
    [
        ("pending", "run_review"),
        ("in_progress", "wait_review"),
        ("completed", None),
        # A failed closing review must NOT satisfy the invariant — it blocks merge
        # and routes to recovery (create_review retry) rather than returning None.
        ("failed", "create_review"),
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
        # run_review / wait_review carry a review_task reference; create_review does not
        if expected_action_type in {"run_review", "wait_review"}:
            assert action.get("review_task").id == closing_review.id


def test_failed_closing_review_blocks_merge_and_routes_to_retry(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Regression: review_cleared=True + failed closing review must NOT produce a merge action.

    Reproduces the gza-4073 shape: stale CHANGES_REQUESTED verdict + review_cleared_at
    set by a completed improve + a FAILED follow-on closing review.  Before the fix,
    closing_review_action returned None (invariant 'satisfied') and has_valid_review_for_merge
    returned True via the review_cleared branch, causing auto-merge.
    """
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/failed-closing-review-no-merge",
        when=datetime(2026, 5, 1, 9, 0, tzinfo=UTC),
    )

    # Stale CHANGES_REQUESTED review (before the improve)
    stale_review = _add_completed_review(store, impl, when=datetime(2026, 5, 1, 10, 0, tzinfo=UTC))
    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda project_dir, r: ParsedReviewReport(
            verdict="CHANGES_REQUESTED",
            findings=(),
            format_version="legacy",
        ),
    )

    # Improve clears the review state
    improve = _add_completed_improve_for_review(
        store, impl, stale_review, when=datetime(2026, 5, 1, 11, 0, tzinfo=UTC)
    )
    impl.review_cleared_at = improve.completed_at
    store.update(impl)

    # Closing review fails (e.g., 429 Too Many Requests)
    closing_review = store.add("Closing review", task_type="review", depends_on=impl.id)
    assert closing_review.id is not None
    closing_review.status = "failed"
    closing_review.failure_reason = "429 Too Many Requests"
    closing_review.created_at = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    closing_review.completed_at = datetime(2026, 5, 1, 12, 30, tzinfo=UTC)
    store.update(closing_review)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    # Must NOT merge — must retry the closing review
    assert action["type"] != "merge", f"Expected retry action but got merge: {action}"
    assert action["type"] == "create_review", action
    assert "failed" in action["description"].lower()


def test_repeated_failed_closing_reviews_escalate_to_needs_attention(
    tmp_path: Path,
) -> None:
    """After max_failed_closing_review_retries consecutive failures, escalate to needs_attention."""
    store = _make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 1, 1, tzinfo=UTC)
    impl.branch = "feat/retry-exhausted"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    stale_review = store.add("Old review", task_type="review", depends_on=impl.id)
    assert stale_review.id is not None
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
    assert improve.id is not None
    improve.status = "completed"
    improve.completed_at = datetime(2026, 1, 3, tzinfo=UTC)
    store.update(improve)

    impl.review_cleared_at = datetime(2026, 1, 3, tzinfo=UTC)
    store.update(impl)

    max_retries = 2
    for i in range(max_retries):
        failed = store.add(f"Closing review attempt {i + 1}", task_type="review", depends_on=impl.id)
        assert failed.id is not None
        failed.status = "failed"
        failed.failure_reason = "UNKNOWN"
        failed.created_at = datetime(2026, 1, 4 + i, tzinfo=UTC)
        failed.completed_at = datetime(2026, 1, 4 + i, 1, tzinfo=UTC)
        store.update(failed)

    action = resolve_closing_review_action(
        task=impl,
        reviews=store.get_reviews_for_task(impl.id),
        latest_completed_review=stale_review,
        latest_completed_code_change=improve,
        max_failed_closing_review_retries=max_retries,
    )

    assert action is not None
    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "closing-review-failed-max-retries"
    assert action["subject_task_id"] == impl.id
    assert str(max_retries) in action["description"]


def test_single_failed_closing_review_below_retry_bound_retries(
    tmp_path: Path,
) -> None:
    """A single failed closing review below the retry bound produces a create_review retry."""
    store = _make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 1, 1, tzinfo=UTC)
    impl.branch = "feat/single-failure-retry"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    stale_review = store.add("Old review", task_type="review", depends_on=impl.id)
    assert stale_review.id is not None
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
    assert improve.id is not None
    improve.status = "completed"
    improve.completed_at = datetime(2026, 1, 3, tzinfo=UTC)
    store.update(improve)

    impl.review_cleared_at = datetime(2026, 1, 3, tzinfo=UTC)
    store.update(impl)

    failed_closing = store.add("Closing review", task_type="review", depends_on=impl.id)
    assert failed_closing.id is not None
    failed_closing.status = "failed"
    failed_closing.failure_reason = "429"
    failed_closing.created_at = datetime(2026, 1, 4, tzinfo=UTC)
    failed_closing.completed_at = datetime(2026, 1, 4, 1, tzinfo=UTC)
    store.update(failed_closing)

    action = resolve_closing_review_action(
        task=impl,
        reviews=store.get_reviews_for_task(impl.id),
        latest_completed_review=stale_review,
        latest_completed_code_change=improve,
        max_failed_closing_review_retries=3,
    )

    assert action is not None
    assert action["type"] == "create_review"
    assert "failed" in action["description"].lower()
    assert "needs_attention_reason" not in action


def test_completed_closing_review_still_satisfies_invariant_unchanged(
    tmp_path: Path,
) -> None:
    """A completed (non-failed) closing review must still satisfy the invariant (returns None)."""
    store = _make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 1, 1, tzinfo=UTC)
    impl.branch = "feat/completed-closing-review"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    stale_review = store.add("Old review", task_type="review", depends_on=impl.id)
    assert stale_review.id is not None
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
    assert improve.id is not None
    improve.status = "completed"
    improve.completed_at = datetime(2026, 1, 3, tzinfo=UTC)
    store.update(improve)

    completed_closing = store.add("Closing review", task_type="review", depends_on=impl.id)
    assert completed_closing.id is not None
    completed_closing.status = "completed"
    completed_closing.completed_at = datetime(2026, 1, 4, tzinfo=UTC)
    store.update(completed_closing)

    action = resolve_closing_review_action(
        task=impl,
        reviews=store.get_reviews_for_task(impl.id),
        latest_completed_review=stale_review,
        latest_completed_code_change=improve,
    )

    assert action is None


def test_completed_improve_with_review_requirement_disabled_merges_without_closing_review(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.require_review_before_merge = False

    impl = _make_completed_unmerged_impl(
        store,
        branch="feature/closing-review-disabled",
        when=datetime(2026, 5, 10, 10, 0, tzinfo=UTC),
    )
    review = _add_completed_review(store, impl, when=datetime(2026, 5, 10, 11, 0, tzinfo=UTC))
    improve = store.add(
        f"Improve {impl.id}",
        task_type="improve",
        based_on=impl.id,
        depends_on=review.id,
        same_branch=True,
    )
    assert improve.id is not None
    improve.status = "completed"
    improve.completed_at = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    improve.branch = impl.branch
    improve.has_commits = True
    store.update(improve)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda project_dir, r: ParsedReviewReport(verdict="APPROVED", findings=(), format_version="legacy"),
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "merge"
    assert action["description"] == "Merge (review APPROVED)"
    assert action.get("review_task") is not None
    assert action["review_task"].id == review.id


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


def test_mergeable_behind_branch_keeps_review_flow(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    task = store.add("Implement stale branch", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/stale-branch"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    git = _FakeGit(
        can_merge=True,
        existing_refs={"origin/feature/stale-branch"},
        behind_count=2,
    )

    action = evaluate_advance_rules(config, store, git, task, "main")

    assert action["type"] == "create_review"
    assert classify_advance_action(action) == "actionable"


def test_stale_conflicting_branch_emits_needs_rebase(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    task = store.add("Implement conflict branch", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/conflict-branch"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    git = _FakeGit(
        can_merge=False,
        existing_refs={"origin/feature/conflict-branch"},
        behind_count=3,
    )

    action = evaluate_advance_rules(config, store, git, task, "main")
    assert action["type"] == "needs_rebase"


def test_failed_rebase_manual_resolution_still_wins_over_clean_mergeable_tip(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl, failed_rebase = _make_completed_impl_with_failed_rebase(
        store,
        branch="feature/failed-rebase-stale",
    )
    git = _FakeGit(
        can_merge=True,
        existing_refs={"origin/feature/failed-rebase-stale"},
        behind_count=2,
    )

    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "rebase-failed-needs-manual-resolution"
    assert failed_rebase.id in action["description"]


def test_non_stale_branch_keeps_existing_review_action(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    task = store.add("Implement feature", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/review-needed"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    git = _FakeGit(
        can_merge=True,
        existing_refs={"origin/feature/review-needed"},
        behind_count=0,
    )

    action = evaluate_advance_rules(config, store, git, task, "main")
    assert action["type"] == "create_review"


def test_approved_but_behind_branch_merges_when_clean(tmp_path: Path, monkeypatch) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    task = store.add("Implement approved but behind", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/approved-behind"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    review = store.add("Approved review", task_type="review", depends_on=task.id, based_on=task.id)
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: APPROVED**"
    store.update(review)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda project_dir, r: ParsedReviewReport(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    git = _FakeGit(
        can_merge=True,
        existing_refs={"origin/feature/approved-behind"},
        behind_count=1,
    )
    action = evaluate_advance_rules(config, store, git, task, "main")

    assert action["type"] == "merge"
