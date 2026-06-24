"""Unit tests for the declarative advance rule engine."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from gza.advance_engine import (
    ADVANCE_RULES,
    WORKER_CONSUMING_ACTIONS,
    _count_consecutive_plan_review_cycles,
    _empty_merge_state_description,
    _resolve_and_persist_post_merge_rebase_state,
    classify_advance_action,
    evaluate_advance_rules,
    failed_recovery_decision_to_action,
    get_action_subject_task_id,
    require_needs_attention_subject,
    resolve_advance_context,
    resolve_closing_review_action,
    resolve_subject_task,
)
from gza.config import Config
from gza.db import NewTaskParams, SqliteTaskStore, Task as DbTask
from gza.git import Git, GitError
from gza.lineage_query import LineageOwnerQuery, query_lineage_owner_rows
from gza.plan_review_materialization import (
    PLAN_REVIEW_ARTIFACT_SCHEMA_VERSION,
    PLAN_REVIEW_MATERIALIZATION_ARTIFACT_KIND,
    build_plan_review_slice_task_specs,
    plan_review_manifest_digest,
)
from gza.recovery_engine import FailedRecoveryDecision, decide_failed_task_recovery
from gza.review_verify_state import refresh_preserved_rebase_review_verify_heads
from gza.review_verdict import ParsedReviewReport, ReviewFinding
from gza.runner import CROSS_PROJECT_TAG


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
        self.can_merge_calls: list[tuple[str, str]] = []
        self.rev_parse_calls: list[str] = []
        self.is_ancestor_calls: list[tuple[str, str]] = []
        self.behind_calls: list[tuple[str, str]] = []
        self.name_status_calls: list[str] = []
        self.resolve_fresh_merge_source_calls: list[str] = []

    def can_merge(self, source_branch: str, target_branch: str) -> bool:
        self.can_merge_calls.append((source_branch, target_branch))
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

        self.resolve_fresh_merge_source_calls.append(branch)
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


def _approved_plan_review_manifest(source_task_id: str) -> dict[str, object]:
    return {
        "schema_version": 1,
        "source_task_id": source_task_id,
        "source_task_type": "plan",
        "verdict": "APPROVED",
        "slice_quality": {
            "fits_single_task_budget": True,
            "timeout_budget_minutes": 30,
            "max_expected_files_changed_per_slice": 8,
            "rationale": "Bounded slices.",
        },
        "slices": [
            {
                "slice_id": "S1",
                "title": "Foundation",
                "prompt": "Implement slice S1.",
                "scope": ["Add parser"],
                "out_of_scope": ["Executor"],
                "acceptance_criteria": ["Parser works"],
                "depends_on_slices": [],
                "based_on_slice": None,
                "review_scope": "Parser only.",
                "estimated_complexity": "medium",
                "expected_timeout_minutes": 30,
                "requires_code_review": True,
                "tags": ["lifecycle"],
            },
            {
                "slice_id": "S2",
                "title": "Follow-up",
                "prompt": "Implement slice S2.",
                "scope": ["Add executor"],
                "out_of_scope": [],
                "acceptance_criteria": ["Executor works"],
                "depends_on_slices": ["S1"],
                "based_on_slice": "S1",
                "review_scope": "Executor only.",
                "estimated_complexity": "medium",
                "expected_timeout_minutes": 30,
                "requires_code_review": True,
                "tags": ["lifecycle"],
            },
        ],
    }


def _approved_plan_review_report(manifest: dict[str, object]) -> str:
    return "## Verdict\nVerdict: APPROVED\n\n## Slice Manifest\n```json\n" + json.dumps(manifest) + "\n```\n"


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
        "### B1 verify_command failure: full verification timed out\n"
        "Evidence: lifecycle verify timed out at `120s` while running `./bin/tests`; the timeout hit near `bin/tests:150-155`.\n"
        "Open-state citation: `bin/tests:150-155`\n"
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


def _timeout_review_with_product_code_open_state_citation() -> str:
    return (
        "## Summary\n\n- Verify timed out.\n\n"
        "## Blockers\n\n"
        "### B1 verify_command failure: full verification timed out\n"
        "Evidence: verify_command timed out after 120s while running the configured suite.\n"
        "Open-state citation: `src/gza/runner.py:903`\n"
        "Impact: product code still leaves the branch unable to pass autonomous verification.\n"
        "Required fix: fix the cited product-code path before rerunning verify_command.\n"
        "Required tests: add targeted runner coverage and rerun verify_command.\n\n"
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


def _verify_failure_only_review_report() -> str:
    return (
        "## Summary\n\n- Implementation matches the requested shape; verify failed at the current tip.\n\n"
        "## Blockers\n\n"
        "### B1 verify_command failure: failed root resume attention regression\n"
        "Evidence: `tests/cli/test_execution.py::test_failed_root_resume_with_existing_failed_resume_child_auto_iterate_uses_shared_attention` failed at the current branch head.\n"
        "Impact: autonomous verify failed even though the review found no code defect.\n"
        "Required fix: rerun verify_command at the same head and keep only durable runner-owned verify evidence.\n"
        "Required tests: rerun verify_command.\n\n"
        "## Follow-Ups\n\nNone.\n\n"
        "## Questions / Assumptions\n\nNone.\n\n"
        "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    )


def _verify_failure_plus_code_blocker_review_report() -> str:
    return (
        "## Summary\n\n- verify_command failed, and the review still sees a product-code defect.\n\n"
        "## Blockers\n\n"
        "### B1 verify_command failure: failed root resume attention regression\n"
        "Evidence: `tests/cli/test_execution.py::test_failed_root_resume_with_existing_failed_resume_child_auto_iterate_uses_shared_attention` failed at the current branch head.\n"
        "Impact: autonomous verify failed.\n"
        "Required fix: rerun verify_command at the same head.\n"
        "Required tests: rerun verify_command.\n\n"
        "### B2 Missing stale-review guard\n"
        "Evidence: `src/gza/advance_engine.py:1994` still allows the stale path to surface.\n"
        "Open-state citation: `src/gza/advance_engine.py:1994`\n"
        "Impact: the merge path can still park or merge from the wrong state.\n"
        "Required fix: guard the stale branch before merge.\n"
        "Required tests: add a targeted advance-engine regression.\n\n"
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
    config.max_noop_improve_cycles = 2

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
    removed_action = "verify_" + "noop_improve_then_review"
    assert "needs_rebase" in WORKER_CONSUMING_ACTIONS
    assert "create_implement" in WORKER_CONSUMING_ACTIONS
    assert "create_plan_review" in WORKER_CONSUMING_ACTIONS
    assert "run_plan_improve" in WORKER_CONSUMING_ACTIONS
    assert removed_action not in WORKER_CONSUMING_ACTIONS


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


def test_completed_plan_with_only_dropped_implement_descendant_creates_plan_review_by_default(tmp_path: Path) -> None:
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

    assert action["type"] == "create_plan_review"
    assert action["description"] == "Create and start plan review task"


def test_completed_plan_with_plan_review_creation_disabled_needs_manual_creation_attention(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.advance_create_plan_reviews = False

    plan = store.add("Plan ingestion options", task_type="plan")
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "plan-review-needs-manual-creation"


def test_completed_plan_uses_legacy_single_implement_only_when_plan_review_gate_disabled(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.require_plan_review_before_implement = False

    plan = store.add("Plan ingestion options", task_type="plan")
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == "create_implement"


def test_completed_plan_review_changes_requested_creates_plan_improve(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan ingestion options", task_type="plan")
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    store.update(review)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == "create_plan_improve"


def test_completed_plan_with_pending_plan_review_returns_run_plan_review(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan ingestion options", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    review.status = "pending"
    store.update(review)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == "run_plan_review"
    assert action["plan_review_task"].id == review.id


def test_completed_plan_with_running_plan_review_returns_wait_plan_review(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan ingestion options", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    review.status = "in_progress"
    review.started_at = datetime.now(UTC)
    store.update(review)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == "wait_plan_review"
    assert action["plan_review_task"].id == review.id


def test_completed_plan_with_pending_plan_review_uses_legacy_create_implement_when_gate_disabled(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.require_plan_review_before_implement = False

    plan = store.add("Plan ingestion options", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    review.status = "pending"
    store.update(review)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == "create_implement"


def test_completed_plan_with_failed_plan_reviews_below_retry_bound_creates_plan_review(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.max_failed_plan_review_retries = 3

    plan = store.add("Plan ingestion options", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    for index in range(config.max_failed_plan_review_retries - 1):
        review = store.add(f"Review attempt {index + 1}", task_type="plan_review", depends_on=plan.id)
        review.status = "failed"
        review.failure_reason = "INFRASTRUCTURE_ERROR"
        review.completed_at = datetime.now(UTC)
        store.update(review)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == "create_plan_review"
    assert action["description"] == "Create and start plan review task"


def test_completed_plan_with_failed_plan_reviews_at_retry_bound_needs_attention(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.max_failed_plan_review_retries = 3

    plan = store.add("Plan ingestion options", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    for index in range(config.max_failed_plan_review_retries):
        review = store.add(f"Review attempt {index + 1}", task_type="plan_review", depends_on=plan.id)
        review.status = "failed"
        review.failure_reason = "INFRASTRUCTURE_ERROR"
        review.completed_at = datetime.now(UTC)
        store.update(review)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "plan-review-repeatedly-failed"
    assert action["subject_task_id"] == plan.id
    assert "3 failed attempts" in action["description"]


def test_completed_plan_review_with_pending_plan_improve_returns_run_plan_improve(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan ingestion options", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    store.update(review)

    improve = store.add(
        "Revise the plan",
        task_type="plan_improve",
        based_on=plan.id,
        depends_on=review.id,
    )
    improve.status = "pending"
    store.update(improve)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == "run_plan_improve"
    assert action["plan_improve_task"].id == improve.id


def test_completed_plan_review_with_running_plan_improve_returns_wait_plan_improve(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan ingestion options", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    store.update(review)

    improve = store.add(
        "Revise the plan",
        task_type="plan_improve",
        based_on=plan.id,
        depends_on=review.id,
    )
    improve.status = "in_progress"
    improve.started_at = datetime.now(UTC)
    store.update(improve)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == "wait_plan_improve"
    assert action["plan_improve_task"].id == improve.id


def test_completed_plan_improve_supersedes_source_only_after_completion(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.max_plan_review_cycles = 3

    plan = store.add("Plan ingestion options", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
    store.update(plan)

    review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 6, 1, 13, 0, tzinfo=UTC)
    review.output_content = "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    store.update(review)

    improve = store.add(
        "Revise the plan",
        task_type="plan_improve",
        based_on=plan.id,
        depends_on=review.id,
    )
    assert improve.id is not None
    improve.status = "completed"
    improve.completed_at = datetime(2026, 6, 1, 14, 0, tzinfo=UTC)
    store.update(improve)

    revised_review = store.add("Review revised plan", task_type="plan_review", depends_on=improve.id)
    revised_review.status = "completed"
    revised_review.completed_at = datetime(2026, 6, 1, 15, 0, tzinfo=UTC)
    revised_review.output_content = "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    store.update(revised_review)

    original_action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")
    revised_action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), improve, "main")

    assert original_action["type"] == "skip"
    assert revised_action["type"] == "create_plan_improve"
    assert revised_action["plan_source_task"].id == improve.id
    assert revised_action["plan_review_task"].id == revised_review.id


def test_plan_review_cycle_limit_counts_across_plan_improve_chain(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.max_plan_review_cycles = 2

    plan = store.add("Plan ingestion options", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
    store.update(plan)

    first_review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    assert first_review.id is not None
    first_review.status = "completed"
    first_review.completed_at = datetime(2026, 6, 1, 13, 0, tzinfo=UTC)
    first_review.output_content = "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    store.update(first_review)

    improve = store.add("Revise the plan", task_type="plan_improve", based_on=plan.id, depends_on=first_review.id)
    assert improve.id is not None
    improve.status = "completed"
    improve.completed_at = datetime(2026, 6, 1, 14, 0, tzinfo=UTC)
    store.update(improve)

    second_review = store.add("Review revised plan", task_type="plan_review", depends_on=improve.id)
    second_review.status = "completed"
    second_review.completed_at = datetime(2026, 6, 1, 15, 0, tzinfo=UTC)
    second_review.output_content = "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    store.update(second_review)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), improve, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "plan-review-max-cycles-reached"


def test_plan_review_first_rejection_below_limit_creates_one_plan_improve(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.max_plan_review_cycles = 2

    plan = store.add("Plan ingestion options", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    store.update(review)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == "create_plan_improve"
    assert action["plan_review_task"].id == review.id


def test_completed_plan_review_with_invalid_approved_manifest_needs_discussion(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan ingestion options", task_type="plan")
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "## Verdict\n\nVerdict: APPROVED\n"
    store.update(review)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "plan-review-invalid-slices"


def test_completed_plan_review_with_needs_discussion_verdict_parks_without_implement(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan ingestion options", task_type="plan")
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "## Verdict\n\nVerdict: NEEDS_DISCUSSION\n"
    store.update(review)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "plan-review-needs-discussion"


def test_completed_plan_review_with_unknown_verdict_parks_without_implement(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan ingestion options", task_type="plan")
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "## Verdict\n\nVerdict: MAYBE\n"
    store.update(review)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "plan-review-unknown-verdict"


@pytest.mark.parametrize(
    ("review_output", "expected_type", "expected_reason"),
    [
        pytest.param("## Verdict\n\nVerdict: APPROVED\n", "needs_discussion", "plan-review-invalid-slices", id="invalid-approved"),
        pytest.param(
            "## Verdict\n\nVerdict: MAYBE\n",
            "needs_discussion",
            "plan-review-unknown-verdict",
            id="unknown-verdict",
        ),
        pytest.param(
            "## Verdict\n\nVerdict: NEEDS_DISCUSSION\n",
            "needs_discussion",
            "plan-review-needs-discussion",
            id="needs-discussion",
        ),
        pytest.param(
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n",
            "create_plan_improve",
            None,
            id="changes-requested",
        ),
    ],
)
def test_completed_plan_with_implement_descendant_respects_latest_plan_review_safety_state(
    tmp_path: Path,
    review_output: str,
    expected_type: str,
    expected_reason: str | None,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan ingestion options", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = review_output
    store.update(review)

    implement = store.add(
        "Manual implement descendant",
        task_type="implement",
        based_on=plan.id,
        trigger_source="manual",
    )
    assert implement.id is not None

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == expected_type
    assert action["type"] != "skip"
    if expected_reason is not None:
        assert action["needs_attention_reason"] == expected_reason
    else:
        assert action["plan_review_task"].id == review.id


def test_completed_plan_review_with_valid_approved_manifest_materializes_slices(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan ingestion options", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    manifest = {
        "schema_version": 1,
        "source_task_id": plan.id,
        "source_task_type": "plan",
        "verdict": "APPROVED",
        "slice_quality": {
            "fits_single_task_budget": True,
            "timeout_budget_minutes": 30,
            "max_expected_files_changed_per_slice": 8,
            "rationale": "Bounded slices.",
        },
        "slices": [
            {
                "slice_id": "S1",
                "title": "Foundation",
                "prompt": "Implement slice S1.",
                "scope": ["Add parser"],
                "out_of_scope": ["Executor"],
                "acceptance_criteria": ["Parser works"],
                "depends_on_slices": [],
                "based_on_slice": None,
                "review_scope": "Parser only.",
                "estimated_complexity": "medium",
                "expected_timeout_minutes": 30,
                "requires_code_review": True,
                "tags": ["lifecycle"],
            }
        ],
    }
    review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = (
        "## Verdict\nVerdict: APPROVED\n\n## Slice Manifest\n```json\n"
        + json.dumps(manifest)
        + "\n```\n"
    )
    store.update(review)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == "materialize_plan_slices"


def test_completed_plan_review_with_string_scope_manifest_still_materializes_slices(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan ingestion options", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    manifest = _approved_plan_review_manifest(plan.id)
    first_slice = manifest["slices"][0]
    assert isinstance(first_slice, dict)
    first_slice["scope"] = "Add parser"
    first_slice["out_of_scope"] = "Executor"

    review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = _approved_plan_review_report(manifest)
    store.update(review)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == "materialize_plan_slices"
    assert "needs_attention_reason" not in action
    assert action["manifest"].slices[0].scope == ("Add parser",)
    assert action["manifest"].slices[0].out_of_scope == ("Executor",)


def test_completed_plan_with_partial_unrecorded_plan_materialization_needs_repair(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan ingestion options", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    manifest = {
        "schema_version": 1,
        "source_task_id": plan.id,
        "source_task_type": "plan",
        "verdict": "APPROVED",
        "slice_quality": {
            "fits_single_task_budget": True,
            "timeout_budget_minutes": 30,
            "max_expected_files_changed_per_slice": 8,
            "rationale": "Bounded slices.",
        },
        "slices": [
            {
                "slice_id": "S1",
                "title": "Foundation",
                "prompt": "Implement slice S1.",
                "scope": ["Add parser"],
                "out_of_scope": ["Executor"],
                "acceptance_criteria": ["Parser works"],
                "depends_on_slices": [],
                "based_on_slice": None,
                "review_scope": "Parser only.",
                "estimated_complexity": "medium",
                "expected_timeout_minutes": 30,
                "requires_code_review": True,
                "tags": ["lifecycle"],
            },
            {
                "slice_id": "S2",
                "title": "Follow-up",
                "prompt": "Implement slice S2.",
                "scope": ["Add executor"],
                "out_of_scope": [],
                "acceptance_criteria": ["Executor works"],
                "depends_on_slices": ["S1"],
                "based_on_slice": "S1",
                "review_scope": "Executor only.",
                "estimated_complexity": "medium",
                "expected_timeout_minutes": 30,
                "requires_code_review": True,
                "tags": ["lifecycle"],
            },
        ],
    }
    review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = (
        "## Verdict\nVerdict: APPROVED\n\n## Slice Manifest\n```json\n"
        + json.dumps(manifest)
        + "\n```\n"
    )
    store.update(review)

    partial = store.add("Implement slice S1.", task_type="implement", based_on=plan.id, trigger_source="plan-review")
    assert partial.id is not None

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "plan-review-materialization-repair-needed"


def test_completed_plan_review_with_already_materialized_manifest_skips_rerun(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan ingestion options", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    manifest = {
        "schema_version": 1,
        "source_task_id": plan.id,
        "source_task_type": "plan",
        "verdict": "APPROVED",
        "slice_quality": {
            "fits_single_task_budget": True,
            "timeout_budget_minutes": 30,
            "max_expected_files_changed_per_slice": 8,
            "rationale": "Bounded slices.",
        },
        "slices": [
            {
                "slice_id": "S1",
                "title": "Foundation",
                "prompt": "Implement slice S1.",
                "scope": ["Add parser"],
                "out_of_scope": ["Executor"],
                "acceptance_criteria": ["Parser works"],
                "depends_on_slices": [],
                "based_on_slice": None,
                "review_scope": "Parser only.",
                "estimated_complexity": "medium",
                "expected_timeout_minutes": 30,
                "requires_code_review": True,
                "tags": ["lifecycle"],
            }
        ],
    }
    review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = (
        "## Verdict\nVerdict: APPROVED\n\n## Slice Manifest\n```json\n"
        + json.dumps(manifest)
        + "\n```\n"
    )
    store.update(review)
    initial_action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")
    assert initial_action["type"] == "materialize_plan_slices"
    task_specs = build_plan_review_slice_task_specs(
        plan_source_task=plan,
        review_task=review,
        manifest=initial_action["manifest"],
        trigger_source="plan-review",
        require_review_before_merge=True,
    )

    store.add_tasks_with_artifact_atomic(
        tasks=task_specs,
        artifact_task_id=review.id,
        artifact_kind=PLAN_REVIEW_MATERIALIZATION_ARTIFACT_KIND,
        artifact_label="plan_review_materialization",
        artifact_path=".gza/artifacts/materialized.txt",
        artifact_byte_size=0,
        artifact_sha256="",
        artifact_metadata_builder=lambda tasks: {
            "schema_version": PLAN_REVIEW_ARTIFACT_SCHEMA_VERSION,
            "review_task_id": review.id,
            "source_task_id": plan.id,
            "source_task_type": "plan",
            "manifest_digest": plan_review_manifest_digest(initial_action["manifest"]),
            "task_ids": [task.id for task in tasks if task.id is not None],
        },
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == "skip"
    assert "already materialized" in action["description"]


def test_completed_plan_with_incomplete_recorded_plan_materialization_needs_repair(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan ingestion options", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    manifest = {
        "schema_version": 1,
        "source_task_id": plan.id,
        "source_task_type": "plan",
        "verdict": "APPROVED",
        "slice_quality": {
            "fits_single_task_budget": True,
            "timeout_budget_minutes": 30,
            "max_expected_files_changed_per_slice": 8,
            "rationale": "Bounded slices.",
        },
        "slices": [
            {
                "slice_id": "S1",
                "title": "Foundation",
                "prompt": "Implement slice S1.",
                "scope": ["Add parser"],
                "out_of_scope": ["Executor"],
                "acceptance_criteria": ["Parser works"],
                "depends_on_slices": [],
                "based_on_slice": None,
                "review_scope": "Parser only.",
                "estimated_complexity": "medium",
                "expected_timeout_minutes": 30,
                "requires_code_review": True,
                "tags": ["lifecycle"],
            },
            {
                "slice_id": "S2",
                "title": "Follow-up",
                "prompt": "Implement slice S2.",
                "scope": ["Add executor"],
                "out_of_scope": [],
                "acceptance_criteria": ["Executor works"],
                "depends_on_slices": ["S1"],
                "based_on_slice": "S1",
                "review_scope": "Executor only.",
                "estimated_complexity": "medium",
                "expected_timeout_minutes": 30,
                "requires_code_review": True,
                "tags": ["lifecycle"],
            },
        ],
    }
    review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = (
        "## Verdict\nVerdict: APPROVED\n\n## Slice Manifest\n```json\n"
        + json.dumps(manifest)
        + "\n```\n"
    )
    store.update(review)
    manifest_digest = plan_review_manifest_digest(
        evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")["manifest"]
    )

    partial_materialized_tasks = store.add_tasks_with_artifact_atomic(
        tasks=[
            NewTaskParams(
                prompt="Implement slice S1.",
                task_type="implement",
                based_on=plan.id,
                trigger_source="plan-review",
            ),
            NewTaskParams(
                prompt="Implement slice S2.",
                task_type="implement",
                based_on=plan.id,
                trigger_source="plan-review",
            ),
        ],
        artifact_task_id=review.id,
        artifact_kind=PLAN_REVIEW_MATERIALIZATION_ARTIFACT_KIND,
        artifact_label="plan_review_materialization",
        artifact_path=".gza/artifacts/materialized.txt",
        artifact_byte_size=0,
        artifact_sha256="",
        artifact_metadata_builder=lambda tasks: {
            "schema_version": PLAN_REVIEW_ARTIFACT_SCHEMA_VERSION,
            "review_task_id": review.id,
            "source_task_id": plan.id,
            "source_task_type": "plan",
            "manifest_digest": manifest_digest,
            "task_ids": [tasks[0].id],
        },
    )
    assert len(partial_materialized_tasks) == 2

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "plan-review-materialization-repair-needed"


@pytest.mark.parametrize(
    "mismatch_kind",
    [
        pytest.param("wrong-task-type", id="wrong-task-type"),
        pytest.param("wrong-trigger-source", id="wrong-trigger-source"),
        pytest.param("wrong-slice-wiring", id="wrong-slice-wiring"),
        pytest.param("wrong-prompt", id="wrong-prompt"),
        pytest.param("wrong-review-scope", id="wrong-review-scope"),
        pytest.param("wrong-tags", id="wrong-tags"),
        pytest.param("wrong-create-review", id="wrong-create-review"),
    ],
)
def test_completed_plan_with_mismatched_recorded_plan_materialization_needs_repair(
    tmp_path: Path,
    mismatch_kind: str,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan ingestion options", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    manifest = _approved_plan_review_manifest(plan.id)
    review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = _approved_plan_review_report(manifest)
    store.update(review)
    manifest_digest = plan_review_manifest_digest(
        evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")["manifest"]
    )
    persisted_task_specs = build_plan_review_slice_task_specs(
        plan_source_task=plan,
        review_task=review,
        manifest=evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")["manifest"],
        trigger_source="plan-review",
        require_review_before_merge=True,
    )
    if mismatch_kind == "wrong-task-type":
        persisted_task_specs[0] = NewTaskParams(**{**persisted_task_specs[0].__dict__, "task_type": "review"})
    elif mismatch_kind == "wrong-trigger-source":
        persisted_task_specs[0] = NewTaskParams(**{**persisted_task_specs[0].__dict__, "trigger_source": "manual"})
    elif mismatch_kind == "wrong-slice-wiring":
        persisted_task_specs[1] = NewTaskParams(
            **{
                **persisted_task_specs[1].__dict__,
                "based_on": plan.id,
                "depends_on": None,
                "same_branch": False,
            }
        )
    elif mismatch_kind == "wrong-prompt":
        persisted_task_specs[0] = NewTaskParams(**{**persisted_task_specs[0].__dict__, "prompt": "Wrong prompt"})
    elif mismatch_kind == "wrong-review-scope":
        persisted_task_specs[0] = NewTaskParams(
            **{**persisted_task_specs[0].__dict__, "review_scope": "Wrong scope"}
        )
    elif mismatch_kind == "wrong-tags":
        persisted_task_specs[0] = NewTaskParams(**{**persisted_task_specs[0].__dict__, "tags": ()})
    elif mismatch_kind == "wrong-create-review":
        persisted_task_specs[0] = NewTaskParams(
            **{**persisted_task_specs[0].__dict__, "create_review": False}
        )
    else:
        raise AssertionError(f"Unhandled mismatch kind: {mismatch_kind}")
    store.add_tasks_with_artifact_atomic(
        tasks=persisted_task_specs,
        artifact_task_id=review.id,
        artifact_kind=PLAN_REVIEW_MATERIALIZATION_ARTIFACT_KIND,
        artifact_label="plan_review_materialization",
        artifact_path=".gza/artifacts/materialized.txt",
        artifact_byte_size=0,
        artifact_sha256="",
        artifact_metadata_builder=lambda tasks: {
            "schema_version": PLAN_REVIEW_ARTIFACT_SCHEMA_VERSION,
            "review_task_id": review.id,
            "source_task_id": plan.id,
            "source_task_type": "plan",
            "manifest_digest": manifest_digest,
            "task_ids": [task.id for task in tasks if task.id is not None],
        },
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "plan-review-materialization-repair-needed"


def test_completed_plan_review_uses_derived_timeout_budget_for_valid_approved_manifest(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.code_task_diff_timeout_cap_minutes = 45
    config.plan_slice_target_timeout_minutes = None

    plan = store.add("Plan ingestion options", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    manifest = {
        "schema_version": 1,
        "source_task_id": plan.id,
        "source_task_type": "plan",
        "verdict": "APPROVED",
        "slice_quality": {
            "fits_single_task_budget": True,
            "timeout_budget_minutes": 45,
            "max_expected_files_changed_per_slice": 8,
            "rationale": "Bounded slices.",
        },
        "slices": [
            {
                "slice_id": "S1",
                "title": "Foundation",
                "prompt": "Implement slice S1.",
                "scope": ["Add parser"],
                "out_of_scope": ["Executor"],
                "acceptance_criteria": ["Parser works"],
                "depends_on_slices": [],
                "based_on_slice": None,
                "review_scope": "Parser only.",
                "estimated_complexity": "medium",
                "expected_timeout_minutes": 45,
                "requires_code_review": True,
                "tags": ["lifecycle"],
            }
        ],
    }
    review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = (
        "## Verdict\nVerdict: APPROVED\n\n## Slice Manifest\n```json\n"
        + json.dumps(manifest)
        + "\n```\n"
    )
    store.update(review)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == "materialize_plan_slices"


@pytest.mark.parametrize(
    ("materialized_create_review", "current_require_review_before_merge"),
    [
        pytest.param(True, False, id="true-to-false"),
        pytest.param(False, True, id="false-to-true"),
    ],
)
def test_completed_plan_reuses_materialization_after_require_review_policy_changes(
    tmp_path: Path,
    materialized_create_review: bool,
    current_require_review_before_merge: bool,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.require_review_before_merge = current_require_review_before_merge

    plan = store.add("Plan ingestion options", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    manifest = _approved_plan_review_manifest(plan.id)
    review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = _approved_plan_review_report(manifest)
    store.update(review)

    parsed_manifest = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")["manifest"]
    persisted_task_specs = build_plan_review_slice_task_specs(
        plan_source_task=plan,
        review_task=review,
        manifest=parsed_manifest,
        trigger_source="plan-review",
        require_review_before_merge=materialized_create_review,
    )
    store.add_tasks_with_artifact_atomic(
        tasks=persisted_task_specs,
        artifact_task_id=review.id,
        artifact_kind=PLAN_REVIEW_MATERIALIZATION_ARTIFACT_KIND,
        artifact_label="plan_review_materialization",
        artifact_path=".gza/artifacts/materialized.txt",
        artifact_byte_size=0,
        artifact_sha256="",
        artifact_metadata_builder=lambda tasks: {
            "schema_version": PLAN_REVIEW_ARTIFACT_SCHEMA_VERSION,
            "review_task_id": review.id,
            "source_task_id": plan.id,
            "source_task_type": "plan",
            "manifest_digest": plan_review_manifest_digest(parsed_manifest),
            "trigger_source": "plan-review",
            "create_review": materialized_create_review,
            "task_ids": [task.id for task in tasks if task.id is not None],
        },
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), plan, "main")

    assert action["type"] == "skip"
    assert "already materialized" in action["description"]


def test_plan_review_cycle_count_uses_derived_timeout_budget_when_unset(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.code_task_diff_timeout_cap_minutes = 62
    config.plan_slice_target_timeout_minutes = None

    plan = store.add("Plan ingestion options", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    store.update(review)

    with patch("gza.advance_engine.get_plan_review_outcome") as mocked_outcome:
        mocked_outcome.return_value = SimpleNamespace(
            verdict="CHANGES_REQUESTED",
            manifest=None,
            validation_error=None,
        )

        cycles = _count_consecutive_plan_review_cycles(
            config=config,
            store=store,
            latest_plan_source=plan,
        )

    assert cycles == 1
    assert mocked_outcome.call_args.kwargs["max_slice_timeout_minutes"] == 62


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


def test_branch_unpushable_failed_recovery_lowers_to_reconcile_action(tmp_path: Path) -> None:
    store = _make_store(tmp_path)

    failed = store.add("Implement feature", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "BRANCH_UNPUSHABLE"
    failed.branch = "feature/advance-reconcile"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    action = failed_recovery_decision_to_action(failed, decision)

    assert decision.action == "reconcile"
    assert action["type"] == "reconcile_branch_divergence"
    assert classify_advance_action(action) == "actionable"


def test_branchless_branch_unpushable_failed_recovery_lowers_to_needs_attention(tmp_path: Path) -> None:
    store = _make_store(tmp_path)

    failed = store.add("Implement feature", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "BRANCH_UNPUSHABLE"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    action = failed_recovery_decision_to_action(
        failed,
        decision,
        needs_attention_reason="branch-publication-needs-manual-repair",
        subject_task_id=failed.id,
    )

    assert decision.action == "skip"
    assert decision.reason_code == "reconcile_branch_missing"
    assert classify_advance_action(action) == "needs_attention"
    assert action["subject_task_id"] == failed.id


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
    config.max_noop_improve_cycles = 2

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


def test_disputed_noop_improve_routes_to_review_blocker_adjudication(tmp_path: Path) -> None:
    from gza.runner import REVIEW_BLOCKER_RESOLUTION_ARTIFACT_KIND

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/disputed-blocker-adjudication",
        when=datetime(2026, 5, 14, 9, 0, tzinfo=UTC),
    )

    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
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

    improve = _add_completed_improve_for_review(
        store,
        impl,
        review,
        when=datetime(2026, 5, 14, 11, 0, tzinfo=UTC),
        changed_diff=False,
    )
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
        metadata={
            "schema_version": 1,
            "state": "disputed",
            "review_task_id": review.id,
            "impl_task_id": impl.id,
            "source_task_id": improve.id,
            "source_task_type": "improve",
            "finding_id": "B1",
            "reason": "already_satisfied",
            "evidence": "The guard already exists on the current branch tip.",
            "current_state_citation": "`src/api.py:12-18`",
            "finding_fingerprint": {
                "title": "missing api guard",
                "anchor": "src/api.py:12-18",
            },
        },
        created_at=improve.completed_at,
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "create_review_adjudication"
    candidate = action["review_blocker_adjudication_candidate"]
    assert candidate.finding.id == "B1"
    assert action["review_task"].id == review.id


def test_noop_improve_subject_is_implement_when_evaluated_from_improve_leaf(tmp_path: Path, monkeypatch) -> None:
    """When watch.py calls evaluate_advance_rules with the improve leaf as the task,
    the needs-attention subject_task_id must still point to the implement owner."""
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/noop-leaf-subject",
        when=datetime(2026, 5, 14, 9, 0, tzinfo=UTC),
    )

    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
    review.report_file = "reviews/fake.md"
    store.update(review)

    improve: DbTask | None = None
    for hour in (11, 12):
        improve = _add_completed_improve_for_review(
            store,
            impl,
            review,
            when=datetime(2026, 5, 14, hour, 0, tzinfo=UTC),
            changed_diff=False,
        )
    assert improve is not None
    assert improve.id is not None

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review: ParsedReviewReport(
            verdict="CHANGES_REQUESTED",
            findings=(),
            format_version="legacy",
        ),
    )

    # Simulate watch.py passing the improve leaf (lifecycle_action_task) to evaluate_advance_rules.
    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), improve, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "improve-no-op"
    # Subject must be the implement owner, not the improve leaf.
    assert get_action_subject_task_id(action) == impl.id
    assert get_action_subject_task_id(action) != improve.id
    # Description must still name the specific improve for context.
    assert improve.id in action["description"]
    # resolve_subject_task must resolve to the implement task.
    resolved = resolve_subject_task(store, action)
    assert resolved.id == impl.id


def test_verify_blocked_subject_is_implement_when_evaluated_from_improve_leaf(tmp_path: Path) -> None:
    """When watch.py calls evaluate_advance_rules with the improve leaf as the task,
    the verify-blocked-no-code-issues needs-attention subject_task_id must point to the implement owner."""
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "uv run pytest tests/ -q"

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/verify-blocked-leaf-subject",
        when=datetime(2026, 5, 14, 9, 0, tzinfo=UTC),
    )

    review1 = store.add("Review round 1", task_type="review", depends_on=impl.id)
    assert review1.id is not None
    review1.status = "completed"
    review1.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
    review1.output_content = _timeout_only_review_report()
    store.update(review1)

    improve_between = store.add(
        "Improve between reviews", task_type="improve", based_on=impl.id, depends_on=review1.id, same_branch=True
    )
    assert improve_between.id is not None
    improve_between.status = "completed"
    improve_between.completed_at = datetime(2026, 5, 14, 11, 0, tzinfo=UTC)
    improve_between.branch = impl.branch
    improve_between.has_commits = True
    store.update(improve_between)

    review2 = store.add("Review round 2", task_type="review", depends_on=impl.id)
    assert review2.id is not None
    review2.status = "completed"
    review2.completed_at = datetime(2026, 5, 14, 12, 0, tzinfo=UTC)
    review2.output_content = _timeout_only_review_report()
    store.update(review2)

    # Improve leaf after the second timeout review — simulates watch.py passing lifecycle_action_task.
    improve_leaf = _add_completed_improve_for_review(
        store,
        impl,
        review2,
        when=datetime(2026, 5, 14, 13, 0, tzinfo=UTC),
        changed_diff=False,
    )
    assert improve_leaf.id is not None

    # Simulate watch.py passing the improve leaf (lifecycle_action_task) to evaluate_advance_rules.
    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), improve_leaf, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "verify-blocked-no-code-issues"
    # Subject must be the implement owner, not the improve leaf.
    assert get_action_subject_task_id(action) == impl.id
    assert get_action_subject_task_id(action) != improve_leaf.id
    # resolve_subject_task must resolve to the implement task.
    resolved = resolve_subject_task(store, action)
    assert resolved.id == impl.id


def test_verify_only_noop_improve_with_cleared_review_becomes_mergeable(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/noop-verify-only-mergeable",
        when=datetime(2026, 5, 14, 9, 0, tzinfo=UTC),
    )

    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
    review.output_content = (
        "## Summary\n\n- The code still looks risky.\n\n"
        "## Blockers\n\n"
        "### B1 Missing provider-result normalization\n"
        "Evidence: src/gza/foo.py:10 still appears to surface raw provider failures.\n"
        "Impact: callers may still see inconsistent error behavior.\n"
        "Required fix: normalize the failure path, then rerun verify_command.\n"
        "Required tests: rerun verify_command.\n\n"
        "## Follow-Ups\n\nNone.\n\n"
        "## Questions / Assumptions\n\nNone.\n\n"
        "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    )
    review.review_verify_status = "failed"
    review.review_verify_branch = impl.branch
    review.review_verify_head_sha = "current-sha"
    store.update(review)

    improve = _add_completed_improve_for_review(
        store,
        impl,
        review,
        when=datetime(2026, 5, 14, 11, 0, tzinfo=UTC),
        changed_diff=False,
    )
    impl.review_cleared_at = improve.completed_at
    store.update(impl)

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "merge"
    assert "improve-no-op" not in action["description"]


def test_verify_only_noop_improve_with_persisted_green_verify_evidence_becomes_mergeable(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/noop-verify-only-persisted-green",
        when=datetime(2026, 5, 14, 9, 0, tzinfo=UTC),
    )

    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
    review.output_content = (
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
    review.review_verify_status = "failed"
    review.review_verify_branch = impl.branch
    review.review_verify_head_sha = "current-sha"
    store.update(review)

    improve = _add_completed_improve_for_review(
        store,
        impl,
        review,
        when=datetime(2026, 5, 14, 11, 0, tzinfo=UTC),
        changed_diff=False,
    )
    improve.review_verify_status = "passed"
    improve.review_verify_branch = impl.branch
    improve.review_verify_head_sha = "current-sha"
    improve.review_verify_captured_at = review.completed_at + timedelta(seconds=1)
    store.update(improve)

    git = _FakeGit(
        can_merge=True,
        existing_branches={impl.branch},
        ref_shas={impl.branch: "current-sha"},
    )

    ctx = resolve_advance_context(config, store, git, impl, "main")
    action = evaluate_advance_rules(config, store, git, improve, "main")

    assert ctx.review_cleared is True
    assert action["type"] == "merge"
    assert "improve-no-op" not in action["description"]


def test_verify_failure_only_noop_improve_with_same_head_green_evidence_merges(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/gza-5501-verify-failure-clear",
        when=datetime(2026, 6, 23, 9, 0, tzinfo=UTC),
    )

    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 6, 23, 10, 0, tzinfo=UTC)
    review.output_content = _verify_failure_only_review_report()
    review.review_verify_status = "failed"
    review.review_verify_branch = impl.branch
    review.review_verify_head_sha = "same-head-sha"
    store.update(review)

    improve = _add_completed_improve_for_review(
        store,
        impl,
        review,
        when=datetime(2026, 6, 23, 11, 0, tzinfo=UTC),
        changed_diff=False,
    )
    improve.review_verify_status = "passed"
    improve.review_verify_branch = impl.branch
    improve.review_verify_head_sha = "same-head-sha"
    improve.review_verify_captured_at = review.completed_at + timedelta(seconds=30)
    store.update(improve)

    git = _FakeGit(
        can_merge=True,
        existing_branches={impl.branch},
        ref_shas={impl.branch: "same-head-sha"},
    )

    ctx = resolve_advance_context(config, store, git, impl, "main")
    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert ctx.review_cleared is True
    assert action["type"] == "merge"
    assert action["type"] != "needs_discussion"
    assert action["description"].startswith("Merge")
    assert "improve-no-op" not in action["description"]


@pytest.mark.parametrize(
    ("label", "review_status", "review_branch", "review_head_sha", "review_report", "improve_status", "improve_branch", "improve_head_sha", "captured_offset_seconds"),
    [
        (
            "missing_passing_improve_evidence",
            "failed",
            "feat/gza-5501-negative",
            "same-head-sha",
            _verify_failure_only_review_report(),
            None,
            "feat/gza-5501-negative",
            "same-head-sha",
            None,
        ),
        (
            "mixed_code_blocker",
            "failed",
            "feat/gza-5501-negative",
            "same-head-sha",
            _verify_failure_plus_code_blocker_review_report(),
            "passed",
            "feat/gza-5501-negative",
            "same-head-sha",
            30,
        ),
        (
            "review_branch_mismatch",
            "failed",
            "feat/other-branch",
            "same-head-sha",
            _verify_failure_only_review_report(),
            "passed",
            "feat/gza-5501-negative",
            "same-head-sha",
            30,
        ),
        (
            "review_head_mismatch",
            "failed",
            "feat/gza-5501-negative",
            "stale-head-sha",
            _verify_failure_only_review_report(),
            "passed",
            "feat/gza-5501-negative",
            "same-head-sha",
            30,
        ),
        (
            "improve_captured_before_review_completed",
            "failed",
            "feat/gza-5501-negative",
            "same-head-sha",
            _verify_failure_only_review_report(),
            "passed",
            "feat/gza-5501-negative",
            "same-head-sha",
            -30,
        ),
        (
            "improve_branch_mismatch",
            "failed",
            "feat/gza-5501-negative",
            "same-head-sha",
            _verify_failure_only_review_report(),
            "passed",
            "feat/other-branch",
            "same-head-sha",
            30,
        ),
        (
            "improve_head_mismatch",
            "failed",
            "feat/gza-5501-negative",
            "same-head-sha",
            _verify_failure_only_review_report(),
            "passed",
            "feat/gza-5501-negative",
            "other-head-sha",
            30,
        ),
        (
            "review_verify_was_passed",
            "passed",
            "feat/gza-5501-negative",
            "same-head-sha",
            _verify_failure_only_review_report(),
            "passed",
            "feat/gza-5501-negative",
            "same-head-sha",
            30,
        ),
    ],
)
def test_verify_failure_only_noop_improve_mismatches_do_not_auto_clear(
    tmp_path: Path,
    label: str,
    review_status: str,
    review_branch: str,
    review_head_sha: str,
    review_report: str,
    improve_status: str | None,
    improve_branch: str,
    improve_head_sha: str,
    captured_offset_seconds: int | None,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "uv run pytest tests/ -q"

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/gza-5501-negative",
        when=datetime(2026, 6, 23, 9, 0, tzinfo=UTC),
    )

    review = store.add(f"Review {label}", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 6, 23, 10, 0, tzinfo=UTC)
    review.output_content = review_report
    review.review_verify_status = review_status
    review.review_verify_branch = review_branch
    review.review_verify_head_sha = review_head_sha
    store.update(review)

    improve = _add_completed_improve_for_review(
        store,
        impl,
        review,
        when=datetime(2026, 6, 23, 11, 0, tzinfo=UTC),
        changed_diff=False,
    )
    if improve_status is not None:
        improve.review_verify_status = improve_status
        improve.review_verify_branch = improve_branch
        improve.review_verify_head_sha = improve_head_sha
        assert captured_offset_seconds is not None
        improve.review_verify_captured_at = review.completed_at + timedelta(seconds=captured_offset_seconds)
        store.update(improve)

    git = _FakeGit(
        can_merge=True,
        existing_branches={impl.branch},
        ref_shas={impl.branch: "same-head-sha"},
    )

    ctx = resolve_advance_context(config, store, git, impl, "main")
    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert ctx.review_cleared is False, label
    assert action["type"] == "needs_discussion", label
    assert action["needs_attention_reason"] == "improve-no-op", label


def test_verify_only_noop_improves_without_green_resolution_do_not_auto_clear(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "uv run pytest tests/ -q"

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/noop-verify-only-not-cleared",
        when=datetime(2026, 5, 14, 9, 0, tzinfo=UTC),
    )

    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
    review.output_content = (
        "## Summary\n\n- Verify passed, but the code issue remains.\n\n"
        "## Blockers\n\n"
        "### B1 Missing provider-result normalization\n"
        "Evidence: src/gza/foo.py:10 still appears to surface raw provider failures.\n"
        "Impact: callers may still see inconsistent error behavior.\n"
        "Required fix: normalize the failure path.\n"
        "Required tests: add a regression for malformed provider responses.\n\n"
        "## Follow-Ups\n\nNone.\n\n"
        "## Questions / Assumptions\n\nNone.\n\n"
        "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    )
    review.review_verify_status = "passed"
    review.review_verify_branch = impl.branch
    review.review_verify_head_sha = "current-sha"
    store.update(review)

    for hour in (11, 12):
        improve = _add_completed_improve_for_review(
            store,
            impl,
            review,
            when=datetime(2026, 5, 14, hour, 0, tzinfo=UTC),
            changed_diff=False,
        )
        assert improve.id is not None

    git = _FakeGit(
        can_merge=True,
        existing_branches={impl.branch},
        ref_shas={impl.branch: "current-sha"},
    )
    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "improve-no-op"
    assert action["type"] != "merge"


def test_verify_only_noop_improve_with_persisted_failing_verify_evidence_stays_blocked(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "uv run pytest tests/ -q"

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/noop-verify-only-still-failing",
        when=datetime(2026, 5, 14, 9, 0, tzinfo=UTC),
    )

    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
    review.output_content = (
        "## Summary\n\n- Implementation is aligned; verify still fails.\n\n"
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
    review.review_verify_status = "failed"
    review.review_verify_branch = impl.branch
    review.review_verify_head_sha = "current-sha"
    store.update(review)

    improve = _add_completed_improve_for_review(
        store,
        impl,
        review,
        when=datetime(2026, 5, 14, 11, 0, tzinfo=UTC),
        changed_diff=False,
    )
    improve.review_verify_status = "failed"
    improve.review_verify_branch = impl.branch
    improve.review_verify_head_sha = "current-sha"
    improve.review_verify_captured_at = review.completed_at + timedelta(seconds=1)
    store.update(improve)

    git = _FakeGit(
        can_merge=True,
        existing_branches={impl.branch},
        ref_shas={impl.branch: "current-sha"},
    )

    ctx = resolve_advance_context(config, store, git, impl, "main")
    action = evaluate_advance_rules(config, store, git, improve, "main")

    assert ctx.review_cleared is False
    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "improve-no-op"


def test_verify_only_noop_improve_stays_mergeable_after_review_preserved_rebase(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/noop-verify-preserved-rebase",
        when=datetime(2026, 5, 14, 9, 0, tzinfo=UTC),
    )

    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
    review.output_content = (
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
    review.review_verify_status = "failed"
    review.review_verify_branch = impl.branch
    review.review_verify_head_sha = "pre-rebase-sha"
    store.update(review)

    improve = _add_completed_improve_for_review(
        store,
        impl,
        review,
        when=datetime(2026, 5, 14, 11, 0, tzinfo=UTC),
        changed_diff=False,
    )
    improve.review_verify_status = "passed"
    improve.review_verify_branch = impl.branch
    improve.review_verify_head_sha = "pre-rebase-sha"
    improve.review_verify_captured_at = review.completed_at + timedelta(seconds=1)
    store.update(improve)

    _add_completed_rebase(
        store,
        impl,
        when=datetime(2026, 5, 14, 12, 0, tzinfo=UTC),
        changed_diff=False,
    )
    refreshed = refresh_preserved_rebase_review_verify_heads(
        store,
        impl,
        branch=impl.branch,
        old_head_sha="pre-rebase-sha",
        new_head_sha="rebased-sha",
    )
    assert refreshed == 2

    git = _FakeGit(
        can_merge=True,
        existing_branches={impl.branch},
        ref_shas={impl.branch: "rebased-sha"},
    )

    ctx = resolve_advance_context(config, store, git, impl, "main")
    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert ctx.review_cleared is True
    assert ctx.review_preserved_by_rebase is not None
    assert action["type"] == "merge"


def test_noop_improve_limit_surfaces_branch_tip_probe_failure_and_skips_verify_availability_probe(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "uv run pytest tests/ -q"
    config.enforce_project_scope = False

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/noop-probe-cleanup",
        when=datetime(2026, 5, 14, 9, 0, tzinfo=UTC),
    )
    impl.tags = (CROSS_PROJECT_TAG,)
    store.update(impl)

    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
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
    store.update(review)

    for hour in (11, 12):
        improve = _add_completed_improve_for_review(
            store,
            impl,
            review,
            when=datetime(2026, 5, 14, hour, 0, tzinfo=UTC),
            changed_diff=False,
        )
        assert improve.id is not None

    git = _FakeGit(
        can_merge=True,
        rev_parse_errors={impl.branch: GitError("should not resolve branch tip")},
        name_status_error_by_range={f"main...{impl.branch}": GitError("should not inspect verify availability")},
    )
    ctx = resolve_advance_context(config, store, git, impl, "main")
    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert ctx.review_cleared is False
    assert ctx.noop_improve_verify_probe_warning == (
        f"branch-head probe failed for {impl.branch}: should not resolve branch tip"
    )
    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "improve-no-op"
    assert action["probe_warning"] == ctx.noop_improve_verify_probe_warning
    assert "branch-head probe failed" in action["description"]
    assert "verify-only auto-clear could not be validated" in action["description"]
    assert git.name_status_calls == []


def test_substantive_noop_improves_still_park_without_auto_clear(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/noop-substantive-still-parked",
        when=datetime(2026, 5, 14, 9, 0, tzinfo=UTC),
    )

    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
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
    store.update(review)

    for hour in (11, 12):
        _add_completed_improve_for_review(
            store,
            impl,
            review,
            when=datetime(2026, 5, 14, hour, 0, tzinfo=UTC),
            changed_diff=False,
        )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "improve-no-op"


def test_verify_blocked_noop_improves_park_when_review_sha_is_stale(tmp_path: Path) -> None:
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

    git = _FakeGit(
        can_merge=True,
        existing_branches={impl.branch},
        ref_shas={impl.branch: "newsha"},
    )
    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "improve-no-op"


def test_verify_timeout_only_reviews_park_with_verify_blocked_reason_when_noop_limit_is_reached(
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

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "verify-blocked-no-code-issues"


def test_verify_timeout_only_review_hits_threshold_one_with_single_noop_improve(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "uv run pytest tests/ -q"
    config.max_noop_improve_cycles = 1

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/verify-timeout-threshold-one",
        when=datetime(2026, 5, 18, 9, 0, tzinfo=UTC),
    )
    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 18, 10, 0, tzinfo=UTC)
    review.output_content = _timeout_only_review_report()
    review.review_verify_head_sha = "stale-sha"
    store.update(review)

    _add_completed_improve_for_review(
        store,
        impl,
        review,
        when=datetime(2026, 5, 18, 11, 0, tzinfo=UTC),
        changed_diff=False,
    )

    git = _FakeGit(
        can_merge=True,
        existing_branches={impl.branch},
        ref_shas={impl.branch: "fresh-sha"},
    )
    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "improve-no-op"


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


def test_code_blocker_hits_threshold_one_with_single_noop_improve_and_parks(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.max_noop_improve_cycles = 1

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/code-blocker-threshold-one",
        when=datetime(2026, 5, 18, 9, 0, tzinfo=UTC),
    )
    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 18, 10, 0, tzinfo=UTC)
    review.output_content = (
        "## Summary\n\n- Guard is still missing.\n\n"
        "## Blockers\n\n"
        "### B1 Missing empty-input guard\n"
        "Evidence: src/gza/foo.py:10-12 indexes the first item before validating input.\n"
        "Open-state citation: `src/gza/foo.py:10-12`\n"
        "Impact: empty selections still raise IndexError.\n"
        "Required fix: return early when the selection is empty.\n"
        "Required tests: add an empty-selection regression and rerun verify_command.\n\n"
        "## Follow-Ups\n\nNone.\n\n"
        "## Questions / Assumptions\n\nNone.\n\n"
        "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    )
    store.update(review)

    _add_completed_improve_for_review(
        store,
        impl,
        review,
        when=datetime(2026, 5, 18, 11, 0, tzinfo=UTC),
        changed_diff=False,
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "improve-no-op"


def test_product_code_open_state_citation_prevents_verify_noop_reverify(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "uv run pytest tests/ -q"
    config.max_noop_improve_cycles = 1

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/product-code-open-state-citation",
        when=datetime(2026, 5, 18, 9, 0, tzinfo=UTC),
    )
    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 18, 10, 0, tzinfo=UTC)
    review.output_content = _timeout_review_with_product_code_open_state_citation()
    store.update(review)

    _add_completed_improve_for_review(
        store,
        impl,
        review,
        when=datetime(2026, 5, 18, 11, 0, tzinfo=UTC),
        changed_diff=False,
    )

    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "improve-no-op"


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


def test_legacy_noop_override_tag_is_inert_at_noop_limit(tmp_path: Path, monkeypatch) -> None:
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    legacy_tag = "allow" + "-noop-improve"

    impl = store.add("Implement feature", task_type="implement", tags=(legacy_tag,))
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

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "improve-no-op"


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
    assert action["description"] == "SKIP: moot (commits already present on target)"
    refreshed_unit = store.resolve_merge_unit_for_task(impl.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "redundant"

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=False),
        config=config,
        git=git,
        target_branch="main",
    )
    assert all(row.owner_task.id != impl.id for row in rows)


def test_empty_and_redundant_advance_skip_descriptions_are_distinct() -> None:
    empty_description = _empty_merge_state_description(SimpleNamespace(merge_state="empty"))
    redundant_description = _empty_merge_state_description(SimpleNamespace(merge_state="redundant"))

    assert empty_description == "SKIP: moot (no unique commits vs target)"
    assert redundant_description == "SKIP: moot (commits already present on target)"
    assert empty_description != redundant_description


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
    assert action["description"] == "SKIP: moot (commits already present on target)"
    refreshed_unit = store.resolve_merge_unit_for_task(impl.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "redundant"
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


def test_redundant_branch_skips_with_commits_already_present_text(tmp_path: Path) -> None:
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
    assert action["description"] == "SKIP: moot (commits already present on target)"


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


def test_missing_max_noop_improve_config_falls_back_to_authoritative_default(tmp_path: Path) -> None:
    class _MissingMaxNoopImproveConfig:
        def __init__(self, base_config: Config) -> None:
            self._base_config = base_config

        def __getattr__(self, name: str) -> object:
            if name == "max_noop_improve_cycles":
                raise AttributeError(name)
            return getattr(self._base_config, name)

    store = _make_store(tmp_path)
    base_config = Config.load(tmp_path)
    base_config.verify_command = "uv run pytest tests/ -q"
    config = _MissingMaxNoopImproveConfig(base_config)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/missing-max-noop-config",
        when=datetime(2026, 5, 18, 9, 0, tzinfo=UTC),
    )
    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 18, 10, 0, tzinfo=UTC)
    review.output_content = _timeout_only_review_report()
    review.review_verify_head_sha = "stale-sha"
    store.update(review)

    _add_completed_improve_for_review(
        store,
        impl,
        review,
        when=datetime(2026, 5, 18, 11, 0, tzinfo=UTC),
        changed_diff=False,
    )

    git = _FakeGit(
        can_merge=True,
        existing_branches={impl.branch},
        ref_shas={impl.branch: "fresh-sha"},
    )
    ctx = resolve_advance_context(config, store, git, impl, "main")
    action = evaluate_advance_rules(config, store, git, impl, "main")

    assert ctx.max_noop_improve_cycles == 1
    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "improve-no-op"


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


def test_review_unknown_verdict_subject_is_implement_when_evaluated_from_improve_leaf(
    tmp_path: Path, monkeypatch
) -> None:
    """When evaluate_advance_rules is called with an improve leaf as the task,
    review_unknown_verdict must surface the implement owner, not the improve leaf."""
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/review-unknown-verdict-leaf",
        when=datetime(2026, 5, 15, 9, 0, tzinfo=UTC),
    )
    review = _add_completed_review(store, impl, when=datetime(2026, 5, 15, 10, 0, tzinfo=UTC))
    improve_leaf = _add_completed_improve_for_review(
        store,
        impl,
        review,
        when=datetime(2026, 5, 15, 11, 0, tzinfo=UTC),
        changed_diff=False,
    )
    assert improve_leaf.id is not None

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review: ParsedReviewReport(
            verdict="SOMETHING_ELSE",
            findings=(),
            format_version="legacy",
        ),
    )

    # Simulate watch.py passing the improve leaf (lifecycle_action_task) to evaluate_advance_rules.
    action = evaluate_advance_rules(config, store, _FakeGit(can_merge=True), improve_leaf, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "review-verdict-needs-manual-attention"
    # Subject must be the implement owner, not the improve leaf.
    assert get_action_subject_task_id(action) == impl.id
    assert get_action_subject_task_id(action) != improve_leaf.id
    # resolve_subject_task must resolve to the implement task.
    resolved = resolve_subject_task(store, action)
    assert resolved.id == impl.id


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


def test_resolve_subject_task_prefers_superseding_recovery_carrier_in_fallback(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    failed_owner = store.add("Failed owner", task_type="implement")
    assert failed_owner.id is not None
    failed_owner.status = "failed"
    failed_owner.failure_reason = "TIMEOUT"
    failed_owner.branch = "feature/recovery-carrier"
    failed_owner.completed_at = datetime.now(UTC)
    store.update(failed_owner)

    completed_retry = store.add(
        "Completed retry",
        task_type="implement",
        based_on=failed_owner.id,
        recovery_origin="retry",
    )
    assert completed_retry.id is not None
    completed_retry.status = "completed"
    completed_retry.branch = failed_owner.branch
    completed_retry.completed_at = datetime.now(UTC)
    store.update(completed_retry)

    row = SimpleNamespace(
        owner_task=failed_owner,
        members=(failed_owner, completed_retry),
        unresolved_tasks=(completed_retry,),
        lifecycle_action_task=completed_retry,
        recovery_action_task=completed_retry,
        recovery_leaf_task=completed_retry,
    )
    action = {
        "type": "needs_discussion",
        "description": "SKIP: manual intervention required",
        "needs_attention_reason": "watch-no-progress-backstop",
    }

    subject_task = resolve_subject_task(store, action, row, fallback_task=failed_owner)

    assert subject_task.id == completed_retry.id


def test_resolve_subject_task_keeps_explicit_live_subject_over_failed_owner_fallback(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)
    failed_owner = store.add("Failed owner", task_type="implement")
    assert failed_owner.id is not None
    failed_owner.status = "failed"
    failed_owner.failure_reason = "TIMEOUT"
    failed_owner.branch = "feature/live-subject"
    failed_owner.completed_at = datetime.now(UTC)
    store.update(failed_owner)

    completed_retry = store.add(
        "Completed retry",
        task_type="implement",
        based_on=failed_owner.id,
        recovery_origin="retry",
    )
    assert completed_retry.id is not None
    completed_retry.status = "completed"
    completed_retry.branch = failed_owner.branch
    completed_retry.completed_at = datetime.now(UTC)
    store.update(completed_retry)

    row = SimpleNamespace(
        owner_task=failed_owner,
        members=(failed_owner, completed_retry),
        unresolved_tasks=(completed_retry,),
        lifecycle_action_task=completed_retry,
        recovery_action_task=completed_retry,
        recovery_leaf_task=completed_retry,
    )
    action = {
        "type": "needs_discussion",
        "description": "SKIP: manual intervention required",
        "needs_attention_reason": "watch-no-progress-backstop",
        "subject_task_id": completed_retry.id,
    }

    subject_task = resolve_subject_task(store, action, row, fallback_task=failed_owner)

    assert subject_task.id == completed_retry.id


def test_resolve_advance_context_reuses_persisted_merge_state_resolution(tmp_path: Path, monkeypatch) -> None:
    from gza import advance_engine as advance_engine_module
    from gza.merge_state import resolve_task_merge_state_for_target as original_resolve_merge_state

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    impl = _make_completed_unmerged_impl(
        store,
        branch="feature/reuse-merge-state",
        when=datetime(2026, 5, 18, 9, 0, tzinfo=UTC),
    )

    warning = (
        "Could not resolve freshest merge source for branch 'feature/reuse-merge-state' "
        "against 'main': local/origin diverged"
    )
    git = _FakeGit(
        can_merge=False,
        existing_branches={impl.branch},
        existing_refs={f"origin/{impl.branch}"},
        ref_shas={f"origin/{impl.branch}": "branch-tip", "main": "target-tip"},
        merge_source_result=(f"origin/{impl.branch}", warning),
        ahead_count=1,
    )
    calls: list[str] = []

    def spy_resolve_merge_state(*args, **kwargs):
        task = kwargs.get("task") if "task" in kwargs else args[1]
        calls.append(task.id)
        return original_resolve_merge_state(*args, **kwargs)

    monkeypatch.setattr(
        advance_engine_module,
        "resolve_task_merge_state_for_target",
        spy_resolve_merge_state,
    )

    ctx = resolve_advance_context(config, store, git, impl, "main")

    assert calls == [impl.id]
    assert ctx.merge_state == "unmerged"
    assert ctx.merge_source_warning == warning


def test_resolve_advance_context_collapses_repeated_git_ref_requests_with_cache(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    impl = _make_completed_unmerged_impl(
        store,
        branch="feature/cache-refs",
        when=datetime(2026, 5, 18, 9, 0, tzinfo=UTC),
    )

    git = Git(tmp_path)
    git.can_merge = lambda *_args, **_kwargs: True  # type: ignore[method-assign]
    git.is_merged = lambda *_args, **_kwargs: False  # type: ignore[method-assign]

    calls: list[tuple[str, ...]] = []

    def fake_run(*args: str, check: bool = True, stdin: bytes | None = None):
        del stdin
        calls.append(args)
        if args == ("show-ref", "--verify", "--quiet", "refs/heads/feature/cache-refs"):
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        if args == ("rev-parse", "--verify", "--quiet", "origin/feature/cache-refs^{commit}"):
            return SimpleNamespace(returncode=1, stdout="", stderr="")
        if args == ("rev-parse", "--verify", "--quiet", "feature/cache-refs^{commit}"):
            return SimpleNamespace(returncode=0, stdout="branch-tip\n", stderr="")
        if args == ("rev-parse", "--verify", "--quiet", "main^{commit}"):
            return SimpleNamespace(returncode=0, stdout="target-tip\n", stderr="")
        if args == ("merge-base", "--is-ancestor", "main", "feature/cache-refs"):
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        if args == ("rev-list", "--count", "main..feature/cache-refs"):
            return SimpleNamespace(returncode=0, stdout="2\n", stderr="")
        raise AssertionError(f"Unexpected git command: {args!r} check={check}")

    git._run = fake_run  # type: ignore[method-assign]

    with git.cached():
        ctx = resolve_advance_context(config, store, git, impl, "main")

    assert ctx.merge_state == "unmerged"
    assert calls.count(("show-ref", "--verify", "--quiet", "refs/heads/feature/cache-refs")) == 1
    assert calls.count(("rev-parse", "--verify", "--quiet", "origin/feature/cache-refs^{commit}")) == 1
    assert calls.count(("rev-parse", "--verify", "--quiet", "feature/cache-refs^{commit}")) == 1
    assert calls.count(("rev-parse", "--verify", "--quiet", "main^{commit}")) == 1
    assert calls.count(("merge-base", "--is-ancestor", "main", "feature/cache-refs")) == 1
    assert calls.count(("rev-list", "--count", "main..feature/cache-refs")) == 1


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
        noop_improve_trigger="comments",
        noop_improve_verify_probe_warning=None,
        review_blocker_adjudication_candidate=None,
        active_review_blocker_adjudication=None,
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
        superseded_plan_source=SimpleNamespace(id="testproject-plan-improve"),
        active_plan_review_pending=SimpleNamespace(id="testproject-plan-review-pending"),
        active_plan_review_running=SimpleNamespace(id="testproject-plan-review-running"),
        latest_completed_plan_review=SimpleNamespace(id="testproject-plan-review-completed"),
        failed_plan_review_count=3,
        plan_review_verdict=None,
        validated_plan_review_manifest=None,
        plan_review_validation_error=None,
        active_plan_improve_pending=SimpleNamespace(id="testproject-plan-improve-pending"),
        active_plan_improve_running=SimpleNamespace(id="testproject-plan-improve-running"),
        advance_create_plan_reviews=True,
        require_plan_review_before_implement=True,
        completed_plan_review_cycles=0,
        max_plan_review_cycles=2,
        max_failed_plan_review_retries=3,
        latest_plan_source=None,
        plan_materialization_state=None,
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
        "plan_invalid_approved_review",
        "plan_max_cycles_reached",
        "plan_review_failed_retry_limit",
        "plan_partial_materialization_requires_repair",
        "plan_review_manual_creation_required",
        "plan_review_needs_discussion",
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


def test_closing_review_in_progress_db_known_wait_matches_full_path_and_skips_late_git_resolution(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module
    from gza.cli.advance_engine import determine_next_action

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/closing-review-in-progress",
        when=datetime(2026, 5, 20, 9, 0, tzinfo=UTC),
    )
    stale_review = _add_completed_review(store, impl, when=datetime(2026, 5, 20, 10, 0, tzinfo=UTC))
    improve = _add_completed_improve_for_review(
        store,
        impl,
        stale_review,
        when=datetime(2026, 5, 20, 11, 0, tzinfo=UTC),
    )
    impl.review_cleared_at = improve.completed_at
    store.update(impl)

    closing_review = store.add("Closing review", task_type="review", depends_on=impl.id)
    assert closing_review.id is not None
    closing_review.status = "in_progress"
    closing_review.created_at = datetime(2026, 5, 20, 12, 0, tzinfo=UTC)
    store.update(closing_review)

    early_git = _FakeGit(can_merge=True)
    early_action = determine_next_action(config, store, early_git, impl, "main")

    monkeypatch.setattr(advance_engine_module, "_resolve_db_known_wait_action", lambda _ctx: None)
    full_git = _FakeGit(can_merge=True)
    full_action = determine_next_action(config, store, full_git, impl, "main")

    assert early_action == full_action
    assert early_action["type"] == "wait_review"
    assert early_action["description"] == f"SKIP: closing review {closing_review.id} is in_progress"
    assert early_action.get("review_task") is not None
    assert early_action["review_task"].id == closing_review.id
    assert set(early_action) == {"type", "description", "review_task"}
    assert early_git.resolve_fresh_merge_source_calls
    assert early_git.can_merge_calls == [(impl.branch, "main")]
    assert early_git.rev_parse_calls
    assert full_git.rev_parse_calls == early_git.rev_parse_calls
    assert full_git.resolve_fresh_merge_source_calls == early_git.resolve_fresh_merge_source_calls

def test_closing_review_in_progress_still_respects_strict_scope_violation(tmp_path: Path) -> None:
    from gza.cli.advance_engine import determine_next_action

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)
    _set_subdir_project_boundary(config, tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/pending-closing-review-scope",
        when=datetime(2026, 5, 21, 9, 0, tzinfo=UTC),
    )
    stale_review = _add_completed_review(store, impl, when=datetime(2026, 5, 21, 10, 0, tzinfo=UTC))
    improve = _add_completed_improve_for_review(
        store,
        impl,
        stale_review,
        when=datetime(2026, 5, 21, 11, 0, tzinfo=UTC),
    )
    impl.review_cleared_at = improve.completed_at
    store.update(impl)

    closing_review = store.add("Closing review", task_type="review", depends_on=impl.id)
    assert closing_review.id is not None
    closing_review.status = "in_progress"
    closing_review.created_at = datetime(2026, 5, 21, 12, 0, tzinfo=UTC)
    store.update(closing_review)

    git = _FakeGit(
        can_merge=True,
        name_status_by_range={f"main...{impl.branch}": "A\tREADME.md\n"},
    )
    action = determine_next_action(config, store, git, impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["needs_attention_reason"] == "project-scope-violation"
    assert action["subject_task_id"] == impl.id
    assert git.name_status_calls == [f"main...{impl.branch}"]
    assert git.resolve_fresh_merge_source_calls


def test_closing_review_in_progress_still_respects_diverged_merge_source(tmp_path: Path) -> None:
    from gza.cli.advance_engine import determine_next_action

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/closing-review-diverged",
        when=datetime(2026, 5, 22, 9, 0, tzinfo=UTC),
    )
    stale_review = _add_completed_review(store, impl, when=datetime(2026, 5, 22, 10, 0, tzinfo=UTC))
    improve = _add_completed_improve_for_review(
        store,
        impl,
        stale_review,
        when=datetime(2026, 5, 22, 11, 0, tzinfo=UTC),
    )
    impl.review_cleared_at = improve.completed_at
    store.update(impl)

    closing_review = store.add("Closing review", task_type="review", depends_on=impl.id)
    assert closing_review.id is not None
    closing_review.status = "in_progress"
    closing_review.created_at = datetime(2026, 5, 22, 12, 0, tzinfo=UTC)
    store.update(closing_review)

    warning = (
        "Local branch 'feat/closing-review-diverged' and remote-tracking ref "
        "'origin/feat/closing-review-diverged' diverged. Push, fetch, or reconcile them before "
        "advancing or merging."
    )
    git = _FakeGit(
        can_merge=True,
        merge_source_result=(None, warning),
    )

    action = determine_next_action(config, store, git, impl, "main")

    assert action["type"] == "reconcile_branch_divergence"
    assert "Reconcile diverged local/origin refs" in action["description"]


def test_closing_review_in_progress_still_respects_manual_merge_source_warning(tmp_path: Path) -> None:
    from gza.cli.advance_engine import determine_next_action

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = _make_completed_unmerged_impl(
        store,
        branch="feat/closing-review-manual-warning",
        when=datetime(2026, 5, 23, 9, 0, tzinfo=UTC),
    )
    stale_review = _add_completed_review(store, impl, when=datetime(2026, 5, 23, 10, 0, tzinfo=UTC))
    improve = _add_completed_improve_for_review(
        store,
        impl,
        stale_review,
        when=datetime(2026, 5, 23, 11, 0, tzinfo=UTC),
    )
    impl.review_cleared_at = improve.completed_at
    store.update(impl)

    closing_review = store.add("Closing review", task_type="review", depends_on=impl.id)
    assert closing_review.id is not None
    closing_review.status = "in_progress"
    closing_review.created_at = datetime(2026, 5, 23, 12, 0, tzinfo=UTC)
    store.update(closing_review)

    warning = "Could not resolve freshest merge source for branch 'feat/closing-review-manual-warning' against 'main'"
    git = _FakeGit(
        can_merge=True,
        merge_source_result=(None, warning),
    )

    action = determine_next_action(config, store, git, impl, "main")

    assert action["type"] == "needs_discussion"
    assert action["description"] == f"SKIP: {warning}"
    assert action["needs_attention_reason"] == "merge-source-needs-manual-resolution"
    assert action["subject_task_id"] == impl.id


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


def test_approved_with_newer_review_scope_comment_does_not_run_improve(tmp_path: Path, monkeypatch):
    from gza import advance_engine as advance_engine_module

    store = _make_store(tmp_path)
    config = Config.load(tmp_path)

    task = store.add("Implement feature", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feat/approved-scope-only-comment"
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
        "Pending improve for scope-only comment",
        task_type="improve",
        based_on=task.id,
        depends_on=review.id,
    )
    pending_improve.status = "pending"
    pending_improve.created_at = datetime(2026, 1, 2, tzinfo=UTC)
    store.update(pending_improve)

    store.add_comment(task.id, "Scope clarification only.", kind="review_scope")

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
    assert action["type"] == "merge"


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
