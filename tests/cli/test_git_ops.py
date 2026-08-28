"""Tests for git-oriented CLI helpers."""

import argparse
import sqlite3
import sys
from contextlib import ExitStack, contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import ANY, MagicMock, call, patch

import pytest

from gza.advance_engine import pending_merge_finalization_action
from gza.cli._lifecycle_actions import should_execute_lifecycle_action as real_should_execute_lifecycle_action
from gza.cli.advance_executor import AdvanceActionExecutionResult
from gza.cli.git_ops import (
    SquashBranchReconcileResult,
    _authorize_staged_merge_finalization_before_materialization,
    _build_auto_merge_args,
    _capped_review_blocker_findings_for_action,
    _classify_rebase_git_failure,
    _classify_squash_reconcile_push_failure,
    _execute_merge_action,
    _finalize_staged_isolated_merge_action,
    _IsolatedPromotionRollbackFailed,
    _materialize_max_cycle_deferred_blockers_for_action,
    _merge_single_task,
    _MergeActionResult,
    _MergeSingleTaskResult,
    _PendingSquashBranchReconcile,
    _prepare_create_review_action,
    _print_squash_reconcile_result,
    _promote_isolated_merge_to_target_branch,
    _reconcile_diverged_branch_with_origin,
    _reconcile_squash_merged_branch_with_origin,
    _remove_watch_merge_checkout,
    _resolve_merge_subject,
    _ResolvedMergeSubject,
    _run_task_backed_rebase,
    _stage_isolated_merge_action,
    _StagedIsolatedMergeAction,
    _tracking_ref_refresh_command,
    cmd_advance,
    cmd_rebase,
    ensure_watch_main_checkout,
    merge_source_for_action,
)
from gza.concurrency import launch_permit
from gza.config import Config
from gza.db import (
    MERGE_SOURCE_ADVANCE,
    MERGE_SOURCE_MANUAL,
    MERGE_SOURCE_MANUAL_FORCE,
    MERGE_SOURCE_MAX_CYCLES_DEFERRED,
    MERGE_SOURCE_WATCH,
    DuplicateActiveChildError,
    Task as DbTask,
)
from gza.git import Git, GitError, ResolvedGitRef, ResolvedMergeSourceRef
from gza.lineage_query import LineageOwnerQuery, LineageOwnerRow, query_lineage_owner_rows
from gza.main_integration_verify import (
    CandidateIntegrationVerifyCheck,
    CandidateIntegrationVerifyEvidence,
    MainIntegrationVerifyEnvironmentIdentity,
    load_main_integration_verify_state,
)
from gza.merge_finalization_proof import persist_merge_finalization_attempt_proof
from gza.merge_services import (
    ManualMergeExecutionHooks,
    ManualMergeExecutionRequest,
    ManualMergeExecutionResult,
    execute_manual_merge,
)
from gza.rebase_checkout import StaleRebaseImportError
from gza.rebase_diff import RebaseDiffBaseline, RebaseDiffResult, parse_rebase_diff_provenance
from gza.recovery_engine import _MergeContext
from gza.review_scope import build_spec_coherence_review_scope, declares_resolution_review_mode
from gza.review_tasks import (
    CappedReviewBlockerMaterializationError,
    build_capped_review_blocker_prompt,
    build_followup_prompt,
    create_or_reuse_capped_review_blocker_task,
    format_blocker_finding_context,
    format_followup_finding_context,
)
from gza.review_verdict import ParsedReviewReport, ReviewFinding, parse_review_report
from gza.review_verify_state import VerifyEpoch, persist_verify_gate_artifact
from gza.worktree_roots import managed_worktree_root_paths

from .conftest import invoke_gza, make_store, setup_config


@pytest.fixture(autouse=True)
def _stub_main_integration_verify() -> object:
    with patch(
        "gza.cli.git_ops.check_main_integration_verify",
        return_value=SimpleNamespace(
            merges_halted=False,
            state=SimpleNamespace(task=SimpleNamespace(id=None), alert_message=None),
        ),
    ) as mocked:
        yield mocked


@pytest.fixture(autouse=True)
def _stub_candidate_integration_verify() -> object:
    with (
        patch(
            "gza.cli.git_ops.check_candidate_integration_verify",
            return_value=SimpleNamespace(
                classification="pass",
                evidence=SimpleNamespace(
                    verify_status="passed",
                    head_sha="isolated-merge-oid",
                    tree_fingerprint="fp-candidate",
                    gate_enabled=True,
                    verify_command="./bin/tests",
                    verify_timeout_seconds=300,
                    verify_timeout_grace_seconds=5.0,
                    environment_identity=None,
                    verify_exit_status="0",
                    failure=None,
                    failing_phase=None,
                    reviewed_branch="main",
                    working_directory="/tmp/main-integration",
                    captured_at=datetime.now(UTC),
                ),
            ),
        ) as mocked,
        patch("gza.cli.git_ops._compute_tree_fingerprint", return_value="fp-candidate"),
    ):
        yield mocked


@contextmanager
def _force_merge_planner_action():
    with patch(
        "gza.cli.git_ops.determine_next_action",
        return_value={"type": "merge", "description": "Merge"},
    ):
        yield


def _completed_merge_task(store: Any, prompt: str, branch: str, *, tags: tuple[str, ...] = ()) -> Any:
    task = store.add(prompt, task_type="implement", tags=tags)
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = branch
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    return task


def _completed_review(store: Any, task: Any, output: str) -> Any:
    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = output
    review.review_verify_branch = task.branch
    review.review_verify_head_sha = "same-sha"
    store.update(review)
    return review


def _completed_spec_coherence_review(
    store: Any,
    task: Any,
    output: str,
    *,
    head_sha: str = "same-sha",
    completed_at: datetime | None = None,
) -> Any:
    review = _completed_review(store, task, output)
    assert task.id is not None
    review.completed_at = completed_at or datetime.now(UTC)
    review.review_scope = build_spec_coherence_review_scope(
        implementation_task_id=task.id,
        reviewed_head_sha=head_sha,
        changed_paths=("specs/behavior/lifecycle-engine.md",),
    )
    review.review_verify_head_sha = head_sha
    store.update(review)
    return review


def _set_review_head(store: Any, review: Any, head_sha: str) -> Any:
    review.review_verify_head_sha = head_sha
    store.update(review)
    return review


def _blocker_finding(finding_id: str = "B1") -> ReviewFinding:
    return ReviewFinding(
        id=finding_id,
        severity="BLOCKER",
        title=f"Blocker {finding_id}",
        body="Body",
        evidence="Evidence",
        impact="Impact",
        fix_or_followup="Fix it",
        tests="Run tests",
        open_state_citation="still open",
    )


def _followup_finding(finding_id: str = "F1") -> ReviewFinding:
    return ReviewFinding(
        id=finding_id,
        severity="FOLLOWUP",
        title=f"Follow-up {finding_id}",
        body="Body",
        evidence="Evidence",
        impact="Impact",
        fix_or_followup="Follow up",
        tests="Run tests",
        open_state_citation="still open",
    )


def _approved_with_followups_output(*finding_ids: str) -> str:
    sections = ["## Review", "", "Verdict: APPROVED_WITH_FOLLOWUPS", "", "## Follow-Ups", ""]
    for finding_id in finding_ids:
        sections.extend(
            [
                f"### {finding_id} Follow-up {finding_id}",
                "Evidence: Evidence",
                "Impact: Impact",
                f"Recommended follow-up: Complete {finding_id}",
                "Recommended tests: Run tests",
                "Open-state citation: still open",
                "",
            ]
        )
    return "\n".join(sections)


def _capped_review_output(*finding_ids: str, verdict: str = "CHANGES_REQUESTED") -> str:
    sections = ["## Review", "", f"Verdict: {verdict}", "", "## Blockers", ""]
    for finding_id in finding_ids or ("B1",):
        sections.extend(
            [
                f"### {finding_id} Blocker {finding_id}",
                "Evidence: Evidence",
                "Impact: Impact",
                "Required fix: Fix it",
                "Required tests: Run tests",
                "Open-state citation: still open",
                "",
            ]
        )
    return "\n".join(sections)


def _merge_executor_git(tmp_path: Path, source_ref: str) -> Any:
    return SimpleNamespace(
        repo_dir=tmp_path,
        resolve_fresh_merge_source=MagicMock(return_value=ResolvedMergeSourceRef(source_ref)),
        default_branch=MagicMock(return_value="main"),
        branch_exists=MagicMock(return_value=True),
        can_merge=MagicMock(return_value=True),
        get_diff_name_status=MagicMock(return_value=[]),
        is_merged=MagicMock(return_value=False),
        is_ancestor=MagicMock(return_value=True),
        rev_parse_if_exists=MagicMock(
            side_effect=lambda ref: "same-sha" if ref == source_ref else "target-sha" if ref == "main" else None
        ),
    )


def _proofing_merge_executor_git(
    tmp_path: Path,
    source_ref: str,
    *,
    previous_target_sha: str = "target-before",
    promoted_target_sha: str = "target-after",
) -> Any:
    target_reads = [previous_target_sha, promoted_target_sha]

    def _rev_parse(ref: str) -> str | None:
        if ref == "main":
            return target_reads.pop(0) if target_reads else promoted_target_sha
        if ref == source_ref:
            return "source-sha"
        return f"{ref}-sha"

    return SimpleNamespace(
        repo_dir=tmp_path,
        resolve_fresh_merge_source=MagicMock(return_value=ResolvedMergeSourceRef(source_ref)),
        default_branch=MagicMock(return_value="main"),
        branch_exists=MagicMock(return_value=True),
        can_merge=MagicMock(return_value=True),
        get_diff_name_status=MagicMock(return_value=[]),
        is_merged=MagicMock(return_value=False),
        is_ancestor=MagicMock(return_value=True),
        rev_parse_if_exists=MagicMock(side_effect=_rev_parse),
    )


def _replay_executor_git(
    tmp_path: Path,
    source_ref: str,
    *,
    source_sha: str = "source-sha",
    target_sha: str = "target-after",
) -> Any:
    return SimpleNamespace(
        repo_dir=tmp_path,
        resolve_fresh_merge_source=MagicMock(return_value=ResolvedMergeSourceRef(source_ref)),
        default_branch=MagicMock(return_value="main"),
        branch_exists=MagicMock(return_value=True),
        can_merge=MagicMock(return_value=True),
        get_diff_name_status=MagicMock(return_value=[]),
        is_merged=MagicMock(return_value=True),
        is_ancestor=MagicMock(return_value=True),
        rev_parse_if_exists=MagicMock(side_effect=lambda ref: source_sha if ref == source_ref else target_sha),
    )


class _SquashFinalizationGit:
    def __init__(
        self,
        tmp_path: Path,
        source_ref: str,
        *,
        remote_tracking: bool,
        source_before: str = "source-before",
        target_before: str = "target-before",
        squash_sha: str = "squash-after",
        squash_tree: str = "squash-tree",
    ) -> None:
        self.repo_dir = tmp_path
        self.source_ref = source_ref
        self.remote_tracking = remote_tracking
        self.source_before = source_before
        self.target_before = target_before
        self.squash_sha = squash_sha
        self.squash_tree = squash_tree
        self.merged = False
        self.has_changes = MagicMock(return_value=False)
        self.can_merge = MagicMock(return_value=True)
        self.count_commits_ahead = MagicMock(return_value=2)
        self.is_ancestor = MagicMock(return_value=False)
        self.resolve_fresh_merge_source = MagicMock(return_value=ResolvedMergeSourceRef(source_ref))
        self.ref_updates: list[tuple[str, str, str | None]] = []
        self.pushes: list[tuple[str, str, str | None]] = []

    def default_branch(self) -> str:
        return "main"

    def is_merged(self, _source_ref: str, _target_ref: str) -> bool:
        return self.merged

    def merge(self, source_ref: str, *, squash: bool, commit_message: str | None) -> None:
        assert source_ref in {self.source_ref, self.source_before}
        assert squash is True
        assert commit_message
        self.merged = True

    def rev_parse(self, ref: str) -> str:
        value = self.rev_parse_if_exists(ref)
        if value is None:
            raise GitError(f"unknown ref {ref}")
        return value

    def rev_parse_if_exists(self, ref: str) -> str | None:
        if ref in {"main", "refs/heads/main", "HEAD"}:
            return self.squash_sha if self.merged else self.target_before
        if ref == self.source_ref or ref == f"refs/heads/{self.source_ref}":
            return self.squash_sha if self.remote_tracking and self.merged else self.source_before
        if ref == f"refs/remotes/origin/{self.source_ref}":
            return self.source_before if self.remote_tracking and not self.merged else None
        if ref == self.squash_sha:
            return self.squash_sha
        return None

    def resolve_refs(self, refs: tuple[str, ...], peel: str = "commit") -> dict[str, str | None]:
        if peel == "tree":
            return {ref: self.squash_tree if self.rev_parse_if_exists(ref) == self.squash_sha else None for ref in refs}
        return {ref: self.rev_parse_if_exists(ref) for ref in refs}

    def update_ref(self, ref: str, new_oid: str, old_oid: str | None = None) -> None:
        self.ref_updates.append((ref, new_oid, old_oid))

    def push_ref_force_with_lease(
        self,
        source_ref: str,
        branch: str,
        *,
        remote: str,
        expected_remote_oid: str | None,
    ) -> None:
        self.pushes.append((source_ref, branch, expected_remote_oid))


def _max_cycle_merge_action(
    review: Any,
    findings: tuple[ReviewFinding, ...],
    output: str,
    *,
    reviewed_head_sha: str = "same-sha",
) -> dict[str, object]:
    return {
        "type": "merge",
        "description": "Merge and defer blockers",
        "max_cycles_merge_and_defer": True,
        "review_task": review,
        "latest_review_task_id": review.id,
        "latest_review_completed_at": review.completed_at.isoformat(),
        "latest_review_mode": "resolution" if declares_resolution_review_mode(review.review_scope) else "plain_full",
        "latest_review_head_sha": reviewed_head_sha,
        "current_review_head_sha": reviewed_head_sha,
        "blocker_findings": findings,
        "deferred_blocker_ids": tuple(finding.id for finding in findings),
        "persisted_review_output": output,
        "max_cycles_audit": {
            "reason": "review-max-cycles",
            "policy": "merge_and_defer",
            "completed_review_cycles": 3,
            "max_review_cycles": 3,
            "verify_gate_state": "passed",
            "verify_epoch": VerifyEpoch(
                reviewed_branch=getattr(review, "review_verify_branch", None),
                reviewed_head_sha=reviewed_head_sha,
                verify_command="./bin/tests",
                verify_timeout_seconds=None,
                verify_timeout_grace_seconds=None,
            ),
        },
    }


def _persist_capped_authorization_verify(
    store: Any,
    config: Config,
    task: Any,
    *,
    tmp_path: Path,
    status: str = "passed",
    head_sha: str = "same-sha",
    captured_at: datetime | None = None,
) -> None:
    from gza.runner import _make_review_verify_result

    config.verify_command = "./bin/tests"
    config.max_review_cycles = 1
    if task.id is not None and not any(
        improve.status == "completed" for improve in store.get_improve_tasks_by_root(task.id)
    ):
        completed_review_times = [
            review.completed_at
            for review in store.get_reviews_for_task(task.id)
            if review.status == "completed" and review.completed_at is not None
        ]
        cycle_anchor = min(completed_review_times) if completed_review_times else captured_at or datetime.now(UTC)
        previous_review = store.add(
            f"Previous capped review {task.id}",
            task_type="review",
            depends_on=task.id,
            based_on=task.id,
        )
        assert previous_review.id is not None
        previous_review.status = "completed"
        previous_review.completed_at = cycle_anchor - timedelta(minutes=2)
        previous_review.output_content = _capped_review_output("B0")
        previous_review.review_verify_branch = task.branch
        previous_review.review_verify_head_sha = head_sha
        store.update(previous_review)
        completed_improve = store.add(
            "Completed improve for prior capped cycle",
            task_type="improve",
            based_on=task.id,
            depends_on=previous_review.id,
        )
        completed_improve.status = "completed"
        completed_improve.completed_at = cycle_anchor - timedelta(minutes=1)
        completed_improve.branch = task.branch
        completed_improve.has_commits = True
        completed_improve.changed_diff = True
        store.update(completed_improve)
    persist_verify_gate_artifact(
        store,
        config,
        owner_task=task,
        source_task=task,
        result=_make_review_verify_result(
            "./bin/tests",
            status=status,
            exit_status="0" if status == "passed" else "1",
            captured_at=captured_at or datetime.now(UTC),
            reviewed_branch=task.branch,
            reviewed_head_sha=head_sha,
            reviewed_base_sha="base-head",
            working_directory=str(tmp_path),
            failure=None if status == "passed" else "verify failed",
        ),
        verify_timeout_seconds=config.autonomous_verify_timeout_seconds,
        verify_timeout_grace_seconds=config.review_verify_timeout_grace_seconds,
        producer="test",
    )


def _pending_replay_action_with_proof(
    store: Any,
    *,
    family: str,
    task: Any,
    review: Any,
    findings: tuple[ReviewFinding, ...],
    children: tuple[Any, ...],
    output: str | None = None,
    source_ref_sha: str = "source-sha",
    promoted_target_sha: str = "target-after",
) -> dict[str, object]:
    unit = store.resolve_merge_unit_for_task(task.id)
    artifact = persist_merge_finalization_attempt_proof(
        store,
        action_family="max_cycles_deferred" if family == "capped" else "ordinary_followup",  # type: ignore[arg-type]
        impl_task_id=task.id,
        review_task_id=review.id,
        finding_ids=tuple(finding.id for finding in findings),
        child_task_ids=tuple(child.id for child in children),
        source_branch=task.branch,
        source_ref=task.branch,
        source_ref_sha=source_ref_sha,
        target_branch="main",
        previous_target_sha="target-before",
        promoted_target_sha=promoted_target_sha,
        merge_unit_id=unit.id if unit is not None else None,
    )
    if family == "capped":
        assert output is not None
        action = _max_cycle_merge_action(review, findings, output)
        action["pending_merge_finalization"] = True
        action["proven_deferred_blocker_tasks"] = children
    else:
        action = {
            "type": "merge_with_followups",
            "description": "Merge with follow-ups",
            "review_task": review,
            "followup_findings": findings,
            "pending_merge_finalization": True,
            "proven_followup_tasks": children,
        }
    action["merge_finalization_proof_id"] = artifact.id
    action["merge_finalization_proof_sha"] = artifact.sha256
    return action


def _advance_args(tmp_path: Path, task_id: str) -> argparse.Namespace:
    return argparse.Namespace(
        project_dir=tmp_path,
        task_id=task_id,
        dry_run=False,
        auto=True,
        max=None,
        repeat=False,
        max_iterations=None,
        batch=None,
        no_docker=True,
        force=False,
        plans=False,
        unimplemented=False,
        create=False,
        no_resume_failed=False,
        max_resume_attempts=None,
        advance_type=None,
        new=False,
        max_review_cycles=None,
        squash_threshold=None,
        tags=None,
        all_tags=False,
    )


def _durable_preview_snapshot(store: Any) -> dict[str, tuple[tuple[Any, ...], ...]]:
    def _quote_identifier(identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    tables = (
        "tasks",
        "task_tags",
        "merge_units",
        "merge_unit_tasks",
        "task_artifacts",
    )
    with store._connect() as conn:  # type: ignore[attr-defined]
        snapshot: dict[str, tuple[tuple[Any, ...], ...]] = {}
        for table in tables:
            columns = [str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table})")]
            quoted_columns = [_quote_identifier(column) for column in columns]
            column_sql = ", ".join(quoted_columns)
            order_sql = ", ".join(quoted_columns)
            snapshot[table] = tuple(tuple(row) for row in conn.execute(f"SELECT {column_sql} FROM {table} ORDER BY {order_sql}"))
    return snapshot


def _add_capped_merge_ready_impl(
    store: Any,
    config: Config,
    tmp_path: Path,
    *,
    branch: str,
) -> tuple[Any, Any]:
    config.on_max_cycles = "merge_and_defer"
    config.max_review_cycles = 1
    config.verify_command = "./bin/tests"
    task = _completed_merge_task(store, "Capped merge dry-run", branch)
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path)
    assert store.get_or_create_merge_unit_for_task(task) is not None
    return task, review


def _add_mergeable_impl_with_failed_rebase(store, branch: str):
    task = store.add("Implement feature", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    task.branch = branch
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    failed_rebase = store.add("Failed rebase", task_type="rebase", based_on=task.id, same_branch=True)
    failed_rebase.status = "failed"
    failed_rebase.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    failed_rebase.branch = branch
    failed_rebase.failure_reason = "MERGE_CONFLICT"
    store.update(failed_rebase)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    review.report_file = "reviews/fake.md"
    store.update(review)
    return task


@pytest.mark.parametrize(
    ("scope", "max_tasks", "batch"),
    [
        ("explicit", None, None),
        ("unscoped", None, None),
        ("batch", 1, 1),
    ],
)
def test_cmd_advance_dry_run_renders_capped_merge_and_defer_without_writes(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    scope: str,
    max_tasks: int | None,
    batch: int | None,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    task, _review = _add_capped_merge_ready_impl(
        store,
        config,
        tmp_path,
        branch=f"feature/capped-dry-run-{scope}",
    )
    before = _durable_preview_snapshot(store)

    fake_git = _make_read_session_reconciliation_git(tmp_path, task.branch)

    args = _advance_args(tmp_path, task.id)
    if scope != "explicit":
        args.task_id = None
    args.dry_run = True
    args.max = max_tasks
    args.batch = batch

    with (
        patch("gza.cli.git_ops.Config.load", return_value=config),
        patch("gza.cli.git_ops.get_store", return_value=store),
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch(
            "gza.git.Git._run",
            return_value=SimpleNamespace(returncode=0, stdout="refs/remotes/origin/main\n", stderr=""),
        ),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch("gza.cli.git_ops.check_main_integration_verify", side_effect=AssertionError("dry-run must inspect only")),
        patch("gza.cli.git_ops._execute_merge_action", side_effect=AssertionError("dry-run must not merge")),
    ):
        rc = cmd_advance(args)

    output = capsys.readouterr().out
    assert rc == 0
    assert "Would advance 1 task(s):" in output
    assert "Merge and defer blockers after max review cycles" in output
    assert "review-max-cycles-reached" not in output
    assert _durable_preview_snapshot(store) == before
    blocker_children = [
        child
        for child in store.get_based_on_children(task.id)
        if child.task_type == "implement" and "deferred-review-blocker" in child.tags
    ]
    assert blocker_children == []
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "unmerged"


def _add_completed_impl_with_approved_review(store, branch: str, *, when: datetime) -> tuple[Any, Any]:
    task = store.add(f"Implement {branch}", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = when
    task.branch = branch
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = when
    review.report_file = "reviews/fake.md"
    store.update(review)
    return task, review


def _manual_merge_args(
    *,
    force: bool = False,
    defer_blockers: bool = False,
    no_followups: bool = False,
) -> argparse.Namespace:
    return argparse.Namespace(
        squash=False,
        delete=False,
        mark_only=False,
        force=force,
        defer_blockers=defer_blockers,
        no_followups=no_followups,
    )


def _manual_merge_service_hooks(events: list[str] | None = None) -> ManualMergeExecutionHooks:
    sink = events if events is not None else []

    return ManualMergeExecutionHooks(
        build_commit_message=lambda task: f"Squash merge: {task.id}",
        capture_pre_squash_reconcile_state=lambda _git, branch: _PendingSquashBranchReconcile(
            branch=branch,
            pre_squash_local_oid="local-before",
            pre_squash_remote_oid="remote-before",
        ),
        reconcile_squash_merge=lambda _git, branch, *_args: SquashBranchReconcileResult(
            status="skipped_no_remote_tracking_ref",
            branch=branch,
        ),
        print_squash_reconcile_result=lambda _result, _suppress_success: sink.append("squash-reconcile"),
        rev_parse_head=lambda _git: "squash-oid",
        materialize_deferred_blockers=lambda _task: ([], []),
        print_deferred_blockers=lambda _task, blockers: sink.append(
            f"deferred:{len(blockers[0])}:{len(blockers[1])}"
        ),
        materialize_followups=lambda _task: ([], []),
        print_followups=lambda _task, followups: sink.append(f"followups:{len(followups[0])}:{len(followups[1])}"),
        emit=sink.append,
    )


def _manual_merge_service_request(
    tmp_path: Path,
    store,
    git,
    task,
    *,
    merge_source: str = MERGE_SOURCE_MANUAL,
    materialize_side_effects: bool = True,
    pre_materialized_deferred_blockers: tuple[list[Any], list[Any]] | None = None,
    pre_materialized_deferred_blockers_printed: bool = False,
) -> ManualMergeExecutionRequest:
    config = Config.load(tmp_path)
    assert task.id is not None
    assert task.branch is not None
    unit = store.resolve_merge_unit_for_task(task.id)
    assert unit is not None
    return ManualMergeExecutionRequest(
        store=store,
        config=config,
        git=git,
        merge_subject=task,
        merge_unit_id=unit.id,
        merge_branch=task.branch,
        merge_source_ref=task.branch,
        current_branch="main",
        merge_source=merge_source,
        merge_preflight_target="main",
        materialize_side_effects=materialize_side_effects,
        pre_materialized_deferred_blockers=pre_materialized_deferred_blockers,
        pre_materialized_deferred_blockers_printed=pre_materialized_deferred_blockers_printed,
    )


@pytest.mark.parametrize("squash", [False, True])
def test_manual_merge_boundary_merges_authorized_source_oid_for_normal_and_squash(
    tmp_path: Path,
    squash: bool,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, f"Authorized source oid squash={squash}", "feature/authorized-source-oid")
    assert store.get_or_create_merge_unit_for_task(task) is not None
    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
        rev_parse_if_exists=MagicMock(
            side_effect=lambda ref: {
                task.branch: "source-sha",
                "main": "target-sha",
            }.get(ref)
        ),
    )
    request = replace(
        _manual_merge_service_request(tmp_path, store, git, task),
        squash=squash,
        authorized_source_ref_sha="source-sha",
        expected_preflight_target_sha="target-sha",
    )

    result = execute_manual_merge(request, _manual_merge_service_hooks())

    assert result.rc == 0
    git.merge.assert_called_once()
    assert git.merge.call_args.args == ("source-sha",)
    assert git.merge.call_args.kwargs["squash"] is squash


@pytest.mark.parametrize("squash", [False, True])
def test_manual_merge_boundary_refuses_target_move_after_materialization_and_returns_child_ids(
    tmp_path: Path,
    squash: bool,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, f"Target race squash={squash}", "feature/target-race")
    assert store.get_or_create_merge_unit_for_task(task) is not None
    created_followup = store.add("Created ordinary follow-up", task_type="implement", based_on=task.id, depends_on=task.id)
    assert created_followup.id is not None
    target_reads = ["target-after"]
    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(side_effect=AssertionError("merge must not run after target race")),
        rev_parse_if_exists=MagicMock(
            side_effect=lambda ref: (
                "source-sha" if ref == task.branch else target_reads.pop(0) if ref == "main" else None
            )
        ),
    )
    events: list[str] = []
    hooks = replace(
        _manual_merge_service_hooks(events),
        materialize_followups=lambda _task: ([created_followup], []),
    )
    request = replace(
        _manual_merge_service_request(tmp_path, store, git, task),
        squash=squash,
        no_followups=False,
        authorized_source_ref_sha="source-sha",
        expected_preflight_target_sha="target-before",
    )
    store.set_merge_unit_state = MagicMock(side_effect=AssertionError("state must not change"))  # type: ignore[method-assign]

    result = execute_manual_merge(request, hooks)

    assert result.rc == 1
    assert result.status == "merge_target_ref_changed"
    assert "merge target changed after preflight authorization" in (result.block_reason or "")
    assert [task.id for task in result.created_followups or []] == [created_followup.id]
    git.merge.assert_not_called()


def _make_preload_recording_git(
    tmp_path: Path,
) -> tuple[MagicMock, list[tuple[tuple[str, ...], str]], list[tuple[str, ...]]]:
    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.branch_exists.return_value = True
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = True
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1

    ref_calls: list[tuple[tuple[str, ...], str]] = []
    branch_calls: list[tuple[str, ...]] = []

    def _resolve_refs(refs, peel="commit"):
        ref_tuple = tuple(refs)
        ref_calls.append((ref_tuple, peel))
        return {ref: f"{ref}-{peel}-sha" for ref in ref_tuple}

    def _branches_exist(branches):
        branch_tuple = tuple(branches)
        branch_calls.append(branch_tuple)
        return {branch: True for branch in branch_tuple}

    fake_git.resolve_refs.side_effect = _resolve_refs
    fake_git.branches_exist.side_effect = _branches_exist
    return fake_git, ref_calls, branch_calls


def _make_read_session_reconciliation_git(tmp_path: Path, branch: str) -> MagicMock:
    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.current_branch.return_value = "main"
    fake_git.default_branch.return_value = "main"
    fake_git.local_branch_names.return_value = [branch]
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(branch)
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = False
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1
    fake_git.rev_parse_if_exists.side_effect = (
        lambda ref: "same-sha" if ref in {branch, "HEAD"} else "base-head" if ref == "main" else None
    )
    fake_git._run.return_value = SimpleNamespace(returncode=0, stdout="", stderr="")
    return fake_git


@contextmanager
def _mock_git_default_branch_run():
    result = SimpleNamespace(returncode=0, stdout="refs/remotes/origin/main\n", stderr="")
    with ExitStack() as stack:
        stack.enter_context(patch("gza.git.Git._run", return_value=result))
        stack.enter_context(patch("gza.recovery_engine.Git._run", return_value=result))
        stack.enter_context(patch("gza.lineage_query.Git._run", return_value=result))
        yield


def _add_prerequisite_unmerged_failed_child(
    store,
    *,
    owner_status: str = "completed",
    owner_branch: str,
) -> tuple[Any, Any, Any]:
    dependency = store.add("Merged dependency", task_type="implement")
    assert dependency.id is not None
    dependency.status = "completed"
    dependency.merge_status = "merged"
    dependency.completed_at = datetime(2026, 5, 16, 8, 0, tzinfo=UTC)
    store.update(dependency)

    owner = store.add("Owner task", task_type="implement")
    assert owner.id is not None
    owner.status = owner_status
    owner.completed_at = datetime(2026, 5, 16, 8, 30, tzinfo=UTC)
    owner.branch = owner_branch
    owner.has_commits = True
    owner.merge_status = "unmerged"
    store.update(owner)

    failed = store.add(
        "Historical blocked child",
        task_type="implement",
        based_on=owner.id,
        depends_on=dependency.id,
        same_branch=True,
    )
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PREREQUISITE_UNMERGED"
    failed.branch = owner_branch
    failed.completed_at = datetime(2026, 5, 16, 9, 0, tzinfo=UTC)
    store.update(failed)
    return dependency, owner, failed


def _failed_recovery_owner_row(task: Any) -> LineageOwnerRow:
    return LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="skipped",
        next_action={"type": "unknown", "description": "pending command evaluation"},
        next_action_reason="pending command evaluation",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=None,
        recovery_action_task=task,
        recovery_leaf_task=task,
    )


def _mark_dependency_merge_unit_merged(store: Any, dependency: Any) -> None:
    dependency.branch = dependency.branch or f"feature/{dependency.id}-dependency"
    dependency.has_commits = True
    store.update(dependency)
    unit = store.get_or_create_merge_unit_for_task(dependency)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "merged")


@pytest.mark.parametrize("subject_type", ["improve", "rebase", "fix"])
def test_prepare_create_review_action_resolves_completed_impl_target(
    tmp_path: Path,
    subject_type: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Completed implementation", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/review-target"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    subject = store.add(
        f"Completed {subject_type}",
        task_type=subject_type,
        based_on=impl.id,
        same_branch=True,
    )
    assert subject.id is not None
    subject.status = "completed"
    subject.completed_at = datetime.now(UTC)
    subject.branch = impl.branch
    subject.merge_status = "unmerged"
    subject.has_commits = True
    store.update(subject)

    review = store.add("Prepared review", task_type="review", depends_on=impl.id, based_on=impl.id)
    called_impl_ids: list[str | None] = []

    def _fake_create_review_task(_store, impl_task, *, trigger_source):
        called_impl_ids.append(impl_task.id)
        assert trigger_source == "watch"
        return review

    with patch("gza.cli.git_ops._create_review_task", side_effect=_fake_create_review_task):
        result = _prepare_create_review_action(store, subject, trigger_source="watch")

    assert called_impl_ids == [impl.id]
    assert result.status == "created"
    assert result.review_task is not None
    assert result.review_task.id == review.id
    assert result.message == f"Created review task {review.id}"


def _assert_scoped_preload_refs(
    ref_calls: list[tuple[tuple[str, ...], str]],
    branch_calls: list[tuple[str, ...]],
    *,
    requested_branch: str,
    unrelated_branches: tuple[str, ...],
    target_branch: str = "main",
) -> None:
    preloaded_refs = {ref for refs, _peel in ref_calls for ref in refs}
    preloaded_branches = {branch for branches in branch_calls for branch in branches}

    assert requested_branch in preloaded_branches
    assert requested_branch in preloaded_refs
    assert f"origin/{requested_branch}" in preloaded_refs
    assert target_branch in preloaded_refs

    for branch in unrelated_branches:
        assert branch not in preloaded_branches
        assert branch not in preloaded_refs
        assert f"origin/{branch}" not in preloaded_refs


def _write_review_report(tmp_path: Path, *, name: str, content: str) -> str:
    report_path = tmp_path / "reviews" / name
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(content, encoding="utf-8")
    return str(report_path.relative_to(tmp_path))


def _create_worktree_registration(common_dir: Path, *, name: str, worktree_path: Path) -> Path:
    registration_dir = common_dir / "worktrees" / name
    registration_dir.mkdir(parents=True, exist_ok=True)
    (registration_dir / "gitdir").write_text(str(worktree_path / ".git"), encoding="utf-8")
    return registration_dir


def _changes_requested_review_with_blocker(
    *,
    title: str,
    evidence: str,
    required_fix: str,
    open_state_citation: str | None = None,
) -> str:
    citation_line = f"Open-state citation: {open_state_citation}\n" if open_state_citation is not None else ""
    return (
        "## Summary\n\n- Review summary.\n\n"
        "## Blockers\n\n"
        f"### B1 {title}\n"
        f"Evidence: {evidence}\n"
        "Impact: merge should stay blocked until handled.\n"
        f"Required fix: {required_fix}\n"
        "Required tests: add focused coverage.\n"
        f"{citation_line}\n"
        "## Follow-Ups\n\nNone.\n\n"
        "## Questions / Assumptions\n\nNone.\n\n"
        "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    )


def _add_same_merge_unit_owner_representative_with_review(
    tmp_path: Path,
    store,
    *,
    review_content: str,
) -> tuple[Any, Any, Any]:
    owner = store.add("Historic merge-unit owner", task_type="implement")
    assert owner.id is not None
    owner.status = "completed"
    owner.completed_at = datetime(2026, 6, 20, 9, 0, tzinfo=UTC)
    owner.branch = "feature/merge-unit-blockers"
    owner.has_commits = True
    owner.merge_status = "unmerged"
    store.update(owner)

    representative = store.add(
        "Actionable merge-unit representative",
        task_type="improve",
        based_on=owner.id,
        same_branch=True,
    )
    assert representative.id is not None
    representative.status = "completed"
    representative.completed_at = datetime(2026, 6, 20, 10, 0, tzinfo=UTC)
    representative.branch = owner.branch
    representative.has_commits = True
    representative.merge_status = "unmerged"
    store.update(representative)

    unit = store.get_or_create_merge_unit_for_task(representative)
    assert unit is not None
    assert unit.owner_task_id == owner.id

    review = store.add(
        f"Review {representative.id}",
        task_type="review",
        depends_on=representative.id,
        based_on=representative.id,
    )
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 6, 20, 11, 0, tzinfo=UTC)
    review.report_file = _write_review_report(
        tmp_path,
        name=f"{representative.id}-merge-unit-review.md",
        content=review_content,
    )
    store.update(review)

    attached_unit = store.get_or_create_merge_unit_for_task(review)
    assert attached_unit is not None
    assert attached_unit.id == unit.id
    return owner, representative, review


def test_merge_single_task_preflights_conflicts_before_merge(tmp_path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement conflicting change", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/conflicts"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        get_diff_name_status=MagicMock(return_value=""),
        can_merge=MagicMock(return_value=False),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
    )
    config = Config.load(tmp_path)

    with _force_merge_planner_action():
        result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 1
    assert result.status == "merge_conflict"
    git.can_merge.assert_called_once_with("feature/conflicts", "main")
    git.merge.assert_not_called()
    output = capsys.readouterr().out
    assert "has conflicts against 'main'" in output
    assert f"uv run gza rebase {task.id} --run" in output


def test_merge_single_task_does_not_classify_generic_merge_failure_as_conflict(tmp_path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement merge failure", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/generic-merge-failure"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        get_diff_name_status=MagicMock(return_value=""),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(side_effect=GitError("merge exploded")),
        merge_abort=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
    )
    config = Config.load(tmp_path)

    with _force_merge_planner_action():
        result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 1
    assert result.status == "merge_failed"
    assert "merge exploded" in (result.block_reason or "")
    git.can_merge.assert_called_once_with("feature/generic-merge-failure", "main")
    git.merge.assert_called_once()
    git.merge_abort.assert_called_once()
    output = capsys.readouterr().out
    assert "Error during merge for testproject-1 (branch feature/generic-merge-failure): merge exploded" in output
    assert "has conflicts against" not in output


def test_merge_single_task_returns_blocked_dirty_checkout_status(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement dirty checkout guard", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/dirty-checkout-guard"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    config = Config.load(tmp_path)
    config.require_review_before_merge = False
    persist_verify_gate_artifact(
        store,
        config,
        owner_task=task,
        source_task=task,
        result=SimpleNamespace(
            command="./bin/tests",
            status="passed",
            exit_status="0",
            captured_at=datetime.now(UTC),
            reviewed_branch=task.branch,
            reviewed_head_sha="head-1",
            reviewed_base_sha="base-1",
            working_directory=str(tmp_path),
            failure=None,
        ),
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="review_verify",
    )

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=True),
        get_diff_name_status=MagicMock(return_value=""),
        rev_parse_if_exists=MagicMock(return_value="head-1"),
        can_merge=MagicMock(),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
    )
    with patch(
        "gza.cli.git_ops.determine_next_action",
        return_value={"type": "merge", "description": "Merge"},
    ):
        result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 1
    assert result.status == "blocked_dirty_checkout"
    assert result.block_reason == "main checkout has uncommitted changes"
    git.can_merge.assert_not_called()
    git.merge.assert_not_called()
    output = capsys.readouterr().out
    assert "You have uncommitted changes. Please commit or stash them first." in output


def test_merge_single_task_runs_shared_verify_gate_before_merge(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement gated merge", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/gated-merge"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with (
        patch(
            "gza.cli.git_ops.determine_next_action",
            side_effect=[
                {
                    "type": "verify_gate",
                    "description": "Run verify gate before merge",
                    "verify_gate_phase": "pre_merge",
                    "verify_owner_task": task,
                },
                {"type": "merge", "description": "Merge (review APPROVED)"},
            ],
        ) as determine,
        patch(
            "gza.cli.git_ops.execute_advance_action",
            return_value=AdvanceActionExecutionResult(
                action_type="verify_gate",
                status="success",
                success_message="Verify gate passed for the current tip before merge.",
                work_done=True,
            ),
        ) as execute_action,
    ):
        result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 0
    assert determine.call_count == 2
    assert determine.call_args.kwargs["selected_for_merge"] is True
    execute_action.assert_called_once()
    git.merge.assert_called_once_with("feature/gated-merge", squash=False, commit_message=None)


def test_merge_single_task_normal_merge_uses_shared_preflight_service(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement shared preflight merge", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/shared-preflight"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        get_diff_name_status=MagicMock(return_value=""),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    config = Config.load(tmp_path)

    with (
        _force_merge_planner_action(),
        patch(
            "gza.cli.git_ops.execute_manual_merge",
            wraps=execute_manual_merge,
        ) as execute_merge,
    ):
        result = _merge_single_task(task.id, config, store, git, _manual_merge_args(), "main")

    assert result.rc == 0
    execute_merge.assert_called_once()
    assert execute_merge.call_args.args[0].merge_source_ref == "feature/shared-preflight"
    git.merge.assert_called_once_with("feature/shared-preflight", squash=False, commit_message=None)


def test_manual_merge_execution_api_normal_merge_marks_merge_unit(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement service merge", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/service-merge"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    unit = store.get_or_create_merge_unit_for_task(task)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    events: list[str] = []

    result = execute_manual_merge(
        _manual_merge_service_request(tmp_path, store, git, task),
        _manual_merge_service_hooks(events),
    )

    assert result.rc == 0
    git.merge.assert_called_once_with("feature/service-merge", squash=False, commit_message=None)
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"
    assert refreshed_unit.merge_source == MERGE_SOURCE_MANUAL
    assert events[:3] == ["deferred:0:0", "followups:0:0", "Merging 'feature/service-merge' into 'main'..."]


def test_manual_merge_execution_api_forced_merge_records_force_provenance(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement forced service merge", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/service-force"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    unit = store.get_or_create_merge_unit_for_task(task)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )

    result = execute_manual_merge(
        _manual_merge_service_request(
            tmp_path,
            store,
            git,
            task,
            merge_source=MERGE_SOURCE_MANUAL_FORCE,
        ),
        _manual_merge_service_hooks(),
    )

    assert result.rc == 0
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.merge_source == MERGE_SOURCE_MANUAL_FORCE


def test_manual_merge_execution_api_materializes_deferred_blockers_before_merge(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement deferred service merge", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/service-defer"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    blocker = store.add("Deferred blocker follow-up", task_type="implement")
    store.get_or_create_merge_unit_for_task(task)
    events: list[str] = []

    def merge(*_args, **_kwargs):
        events.append("merge")

    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(side_effect=merge),
    )
    hooks = _manual_merge_service_hooks(events)

    result = execute_manual_merge(
        _manual_merge_service_request(
            tmp_path,
            store,
            git,
            task,
            pre_materialized_deferred_blockers=([blocker], []),
        ),
        hooks,
    )

    assert result.rc == 0
    assert result.created_deferred_blockers == [blocker]
    assert events.index("deferred:1:0") < events.index("merge")


def test_manual_merge_execution_api_refuses_spec_coherence_materialization_failure_before_merge(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement spec refusal", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/service-spec-refusal"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    store.get_or_create_merge_unit_for_task(task)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    hooks = _manual_merge_service_hooks()
    hooks = replace(hooks, materialize_deferred_blockers=lambda _task: None)

    result = execute_manual_merge(
        _manual_merge_service_request(tmp_path, store, git, task),
        hooks,
    )

    assert result.rc == 1
    assert result.status == "deferred_blocker_materialization_missing"
    assert "returned no result" in (result.block_reason or "")
    git.merge.assert_not_called()
    refreshed_unit = store.resolve_merge_unit_for_task(task.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"


def test_manual_merge_execution_api_late_already_merged_refusal_is_not_success_status(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement already merged race", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/service-already-race"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    unit = store.get_or_create_merge_unit_for_task(task)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=True),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    events: list[str] = []

    result = execute_manual_merge(
        _manual_merge_service_request(tmp_path, store, git, task),
        _manual_merge_service_hooks(events),
    )

    assert result.rc == 1
    assert result.status == "already_merged_refusal"
    assert "already merged" in (result.block_reason or "")
    git.merge.assert_not_called()
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert "already merged into main" in "\n".join(events)


def test_merge_single_task_manual_already_merged_message_and_exit_code_are_preserved(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement manual already merged", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/manual-already"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    unit = store.get_or_create_merge_unit_for_task(task)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=True),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        get_diff_name_status=MagicMock(return_value=""),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )

    with _force_merge_planner_action():
        result = _merge_single_task(task.id, Config.load(tmp_path), store, git, _manual_merge_args(), "main")

    assert result.rc == 1
    assert result.status == "already_merged_refusal"
    assert "Error: Branch 'feature/manual-already' is already merged into main" in capsys.readouterr().out
    git.merge.assert_not_called()
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"


def test_manual_merge_execution_api_merge_failure_cleans_up(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement cleanup service merge", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/service-cleanup"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    store.get_or_create_merge_unit_for_task(task)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(side_effect=GitError("merge failed")),
        merge_abort=MagicMock(),
        reset_hard_head=MagicMock(),
    )

    result = execute_manual_merge(
        _manual_merge_service_request(tmp_path, store, git, task),
        _manual_merge_service_hooks(),
    )

    assert result.rc == 1
    assert result.status == "merge_failed"
    git.merge_abort.assert_called_once_with()
    git.reset_hard_head.assert_not_called()
    refreshed_unit = store.resolve_merge_unit_for_task(task.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"


def test_staged_isolated_merge_late_already_merged_race_stops_without_reconciliation(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement isolated already merged race", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/isolated-race"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    unit = store.get_or_create_merge_unit_for_task(task)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        default_branch=MagicMock(return_value="main"),
        branch_exists=MagicMock(return_value=True),
        ref_exists=MagicMock(return_value=False),
        is_merged=MagicMock(side_effect=[False, True]),
        has_changes=MagicMock(return_value=False),
        get_diff_name_status=MagicMock(return_value=""),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    action = {"type": "merge", "description": "Merge"}

    with _force_merge_planner_action():
        result = _stage_isolated_merge_action(
            Config.load(tmp_path),
            store,
            git,
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_git=git,
            merge_current_branch="main",
            already_merged_behavior="mark_merged",
        )

    assert isinstance(result, _MergeActionResult)
    assert result.rc == 1
    assert result.status == "already_merged_refusal"
    git.merge.assert_not_called()
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"


def test_staged_isolated_merge_explicit_already_merged_reconciliation_marks_unit(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement isolated already merged reconciliation", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/isolated-reconcile"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    unit = store.get_or_create_merge_unit_for_task(task)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        default_branch=MagicMock(return_value="main"),
        branch_exists=MagicMock(return_value=True),
        ref_exists=MagicMock(return_value=False),
        is_merged=MagicMock(return_value=True),
        has_changes=MagicMock(return_value=False),
        get_diff_name_status=MagicMock(return_value=""),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )

    result = _stage_isolated_merge_action(
        Config.load(tmp_path),
        store,
        git,
        task,
        {"type": "merge", "description": "Merge"},
        target_branch="main",
        current_branch="main",
        merge_git=git,
        merge_current_branch="main",
        already_merged_behavior="mark_merged",
    )

    assert isinstance(result, _MergeActionResult)
    assert result.rc == 0
    assert result.status == "already_merged"
    git.merge.assert_not_called()
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"


def test_merge_single_task_force_uses_shared_override_source_helper(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement shared force override", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/shared-force"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        get_diff_name_status=MagicMock(return_value=""),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    config = Config.load(tmp_path)

    from gza.cli import git_ops as git_ops_module

    with (
        patch(
            "gza.cli.git_ops.determine_next_action",
            return_value={
                "type": "parked",
                "description": "Manual review required before merge",
                "needs_attention_reason": "operator-review-required",
                "subject_task_id": task.id,
            },
        ),
        patch(
            "gza.cli.git_ops.manual_force_merge_source",
            wraps=git_ops_module.manual_force_merge_source,
        ) as force_source,
    ):
        result = _merge_single_task(task.id, config, store, git, _manual_merge_args(force=True), "main")

    assert result.rc == 0
    force_source.assert_called_once_with("manual")
    unit = store.resolve_merge_unit_for_task(task.id)
    assert unit is not None
    assert unit.merge_source == "manual_force"


def test_merge_single_task_defer_blockers_uses_shared_materialization_service(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement shared defer blockers", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/shared-defer"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    store.update(review)
    blocker = ReviewFinding(
        id="B-shared-defer",
        severity="BLOCKER",
        title="Needs follow-up",
        body="Body",
        evidence=None,
        impact=None,
        fix_or_followup="track as urgent follow-up",
        tests=None,
        open_state_citation="citation",
    )
    deferred = store.add("Deferred blocker task", task_type="implement", based_on=review.id)
    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        get_diff_name_status=MagicMock(return_value=""),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    config = Config.load(tmp_path)

    from gza.cli import git_ops as git_ops_module

    with (
        patch(
            "gza.cli.git_ops.determine_next_action",
            return_value={
                "type": "improve",
                "description": "Create improve task (review CHANGES_REQUESTED)",
                "improve_reason": "review_changes_requested",
                "review_task": review,
            },
        ),
        patch(
            "gza.cli.git_ops.get_review_report",
            return_value=SimpleNamespace(verdict="CHANGES_REQUESTED", findings=(blocker,), format_version="v2"),
        ),
        patch("gza.cli.git_ops.get_review_content", return_value="review content"),
        patch("gza.cli.git_ops.summarize_review_blockers", return_value=SimpleNamespace(blocker_count=1)),
        patch("gza.cli.git_ops._create_or_reuse_deferred_blocker_tasks", return_value=([deferred], [])),
        patch(
            "gza.cli.git_ops.materialize_merge_deferred_blockers",
            wraps=git_ops_module.materialize_merge_deferred_blockers,
        ) as materialize,
    ):
        result = _merge_single_task(
            task.id,
            config,
            store,
            git,
            _manual_merge_args(defer_blockers=True),
            "main",
        )

    assert result.rc == 0
    assert materialize.call_count == 1
    assert materialize.call_args.kwargs["defer_blockers"] is True
    unit = store.resolve_merge_unit_for_task(task.id)
    assert unit is not None
    assert unit.merge_source == "manual_force"


def test_merge_single_task_spec_coherence_refusal_uses_shared_blocker_classifier(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement shared spec coherence refusal", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/shared-spec-refusal"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    review = store.add(f"Spec coherence review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.review_scope = "\n".join(
        (
            "Review mode: spec-coherence",
            f"Implementation task: {task.id}",
            "Reviewed head SHA: reviewed-head",
            'Changed behavior-spec paths JSON: ["specs/behavior/lifecycle-engine.md"]',
        )
    )
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    store.update(review)
    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        get_diff_name_status=MagicMock(return_value=""),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    config = Config.load(tmp_path)

    from gza.cli import git_ops as git_ops_module

    with (
        patch(
            "gza.cli.git_ops.determine_next_action",
            return_value={
                "type": "parked",
                "description": "Manual review required before merge",
                "needs_attention_reason": "operator-review-required",
                "subject_task_id": task.id,
            },
        ),
        patch(
            "gza.cli.git_ops.get_review_report",
            return_value=SimpleNamespace(verdict="CHANGES_REQUESTED", findings=(), format_version="v2"),
        ),
        patch(
            "gza.cli.git_ops.materialize_merge_deferred_blockers",
            wraps=git_ops_module.materialize_merge_deferred_blockers,
        ) as materialize,
    ):
        result = _merge_single_task(
            task.id,
            config,
            store,
            git,
            _manual_merge_args(force=True, defer_blockers=True),
            "main",
        )

    assert result.rc == 1
    materialize.assert_called_once()
    git.merge.assert_not_called()
    output = capsys.readouterr().out
    assert "behavior-spec coherence CHANGES_REQUESTED review" in output
    assert "not deferable" in output


@pytest.mark.parametrize(
    "contributor_status,contributor_exit,expect_merged",
    [("passed", "0", True), ("failed", "1", False)],
)
def test_merge_single_task_reconciles_contributor_verify_evidence_before_merge(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    contributor_status: str,
    contributor_exit: str,
    expect_merged: bool,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.require_review_before_merge = False
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    owner = store.add("Owner implementation", task_type="implement")
    assert owner.id is not None
    owner.status = "completed"
    owner.completed_at = datetime.now(UTC)
    owner.branch = "feature/recredit-before-merge"
    owner.has_commits = True
    store.update(owner)
    store.set_merge_status(owner.id, "unmerged")

    contributor = store.add("Contributor implementation", task_type="implement")
    assert contributor.id is not None
    contributor.status = "completed"
    contributor.completed_at = datetime.now(UTC)
    contributor.branch = owner.branch
    contributor.has_commits = True
    store.update(contributor)

    unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(owner.id, unit.id, "owner")
    store.attach_task_to_merge_unit(contributor.id, unit.id, "same_branch")

    from gza.runner import _make_review_verify_result

    for task, status, exit_status, captured_at in (
        (owner, "failed", "1", datetime(2026, 8, 18, 10, 0, tzinfo=UTC)),
        (contributor, contributor_status, contributor_exit, datetime(2026, 8, 18, 10, 5, tzinfo=UTC)),
    ):
        persist_verify_gate_artifact(
            store,
            config,
            owner_task=task,
            source_task=task,
            result=_make_review_verify_result(
                "./bin/tests",
                status=status,
                exit_status=exit_status,
                captured_at=captured_at,
                reviewed_branch=owner.branch,
                reviewed_head_sha="same-head",
                reviewed_base_sha="base-head",
                working_directory=str(tmp_path),
                failure=None if status == "passed" else "verify failed",
            ),
            verify_timeout_seconds=120,
            verify_timeout_grace_seconds=5.0,
            producer="test",
        )

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        get_diff_name_status=MagicMock(return_value=""),
        rev_parse_if_exists=MagicMock(
            side_effect=lambda ref: (
                "same-head" if ref == owner.branch else "base-head" if ref in {"main", "origin/main"} else None
            )
        ),
        count_commits_behind_checked=MagicMock(return_value=0),
        count_commits_ahead_checked=MagicMock(return_value=1),
        resolve_fresh_merge_source=MagicMock(side_effect=lambda branch: branch),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
        force=False,
        no_followups=True,
    )

    with patch("gza.cli.advance_executor._run_lifecycle_verify", side_effect=AssertionError("verify should not rerun")):
        result = _merge_single_task(owner.id, config, store, git, args, "main")

    output = capsys.readouterr().out
    refreshed_owner = store.get(owner.id)
    refreshed_unit = store.resolve_merge_unit_for_task(owner.id)
    assert refreshed_owner is not None
    assert refreshed_unit is not None
    assert refreshed_owner.review_verify_status == contributor_status
    assert f"Recredited current merge-unit verify gate evidence ({contributor_status})" in output
    if expect_merged:
        assert result.rc == 0
        git.merge.assert_called_once()
        assert refreshed_unit.state == "merged"
    else:
        assert result.rc == 1
        git.merge.assert_not_called()
        assert "verify_fix" in output


def test_merge_single_task_default_keeps_merge_mechanics_output(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement merge output", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/default-merge-output"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
    )
    config = Config.load(tmp_path)

    with _force_merge_planner_action():
        result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 0
    output = capsys.readouterr().out
    assert "Merging 'feature/default-merge-output' into 'main'..." in output
    assert "✓ Successfully merged feature/default-merge-output" in output


def test_merge_single_task_merge_failure_output_names_subject_task(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement merge failure output", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/merge-failure-output"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    git_error = "git merge --squash feature/merge-failure-output failed: conflict"
    abort_error = "git merge --abort failed: no merge to abort"
    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        rev_parse_if_exists=MagicMock(return_value=None),
        merge=MagicMock(side_effect=GitError(git_error)),
        merge_abort=MagicMock(side_effect=GitError(abort_error)),
        reset_hard_head=MagicMock(side_effect=GitError(abort_error)),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=True,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
    )
    config = Config.load(tmp_path)

    with _force_merge_planner_action():
        result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 1
    assert result.status == "merge_cleanup_failed"
    assert "cleanup failed" in (result.block_reason or "")
    output = capsys.readouterr().out
    subject = f"for {task.id} (branch feature/merge-failure-output)"
    assert f"Error during merge {subject}: {git_error}" in output
    assert f"Aborting merge {subject} and restoring clean state..." in output
    assert f"Warning: Could not abort merge {subject}: {abort_error}" in output


def test_merge_single_task_quiet_mechanics_suppresses_default_success_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement quiet merge output", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/quiet-merge-output"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
    )
    config = Config.load(tmp_path)

    with _force_merge_planner_action():
        result = _merge_single_task(
            task.id,
            config,
            store,
            git,
            args,
            "main",
            quiet_mechanics=True,
        )

    assert result.rc == 0
    output = capsys.readouterr().out
    assert "Merging 'feature/quiet-merge-output' into 'main'..." not in output
    assert "✓ Successfully merged feature/quiet-merge-output" not in output


def test_merge_single_task_quiet_mechanics_keeps_squash_reconcile_warning_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement quiet squash merge output", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/quiet-squash-merge-output"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
        rev_parse=MagicMock(return_value="squash-oid"),
        rev_parse_if_exists=MagicMock(side_effect=lambda ref: f"{ref}-oid"),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=True,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
    )
    config = Config.load(tmp_path)

    with patch(
        "gza.cli.git_ops._reconcile_squash_merged_branch_with_origin",
        return_value=SquashBranchReconcileResult(
            status="failed_remote_tracking_ref_update",
            branch="feature/quiet-squash-merge-output",
            reason="cannot lock ref 'refs/remotes/origin/feature/quiet-squash-merge-output'",
        ),
    ):
        with _force_merge_planner_action():
            result = _merge_single_task(
                task.id,
                config,
                store,
                git,
                args,
                "main",
                quiet_mechanics=True,
            )

    assert result.rc == 0
    output = capsys.readouterr().out
    assert "Merging 'feature/quiet-squash-merge-output' into 'main'..." not in output
    assert "✓ Successfully squash merged feature/quiet-squash-merge-output and created commit" not in output
    assert "✓ Reconciled origin/feature/quiet-squash-merge-output to the squash merge commit" not in output
    assert "could not be updated" in output
    assert (
        "git fetch origin +refs/heads/feature/quiet-squash-merge-output:refs/remotes/origin/feature/quiet-squash-merge-output"
        in output
    )


def test_merge_single_task_refuses_verify_only_blockers_without_flag(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement verify-only blocker path", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/verify-only-blocker"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    store.update(review)

    blocker = ReviewFinding(
        id="B1",
        severity="BLOCKER",
        title="Verify timed out",
        body="",
        evidence="./bin/tests timed out",
        impact="Flaky verify evidence would be lost",
        fix_or_followup="stabilize verify",
        tests="add regression coverage",
        open_state_citation="status: open",
    )
    deferred_task = store.add("Deferred blocker B1", task_type="implement", based_on=review.id, depends_on=task.id)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
        defer_blockers=False,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with (
        patch(
            "gza.cli.git_ops.get_review_report",
            return_value=SimpleNamespace(verdict="CHANGES_REQUESTED", findings=(blocker,), format_version="v2"),
        ),
        patch(
            "gza.cli.git_ops._create_or_reuse_deferred_blocker_tasks", return_value=([deferred_task], [])
        ) as materialize,
    ):
        with _force_merge_planner_action():
            result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 1
    git.merge.assert_not_called()
    materialize.assert_not_called()
    output = capsys.readouterr().out
    assert (
        f"Error: Task {task.id} has CHANGES_REQUESTED review {review.id}, "
        "but blocker classification did not match the parsed blocker set. Refusing to guess."
    ) in output


def test_merge_single_task_refuses_verify_only_report_file_blockers_without_flag(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement report-file verify-only blocker path", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/report-file-verify-only-blocker"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.report_file = _write_review_report(
        tmp_path,
        name="verify-only-review.md",
        content=_changes_requested_review_with_blocker(
            title="verify_command failure: command exited nonzero",
            evidence="verify_command failed while running `./bin/tests`; the assertion failure is in the verify output.",
            required_fix="rerun verify_command on the current tip.",
        ),
    )
    store.update(review)

    deferred_task = store.add("Deferred blocker B1", task_type="implement", based_on=review.id, depends_on=task.id)
    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
        defer_blockers=False,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with patch(
        "gza.cli.git_ops._create_or_reuse_deferred_blocker_tasks",
        return_value=([deferred_task], []),
    ) as materialize:
        with _force_merge_planner_action():
            result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 1
    git.merge.assert_not_called()
    materialize.assert_not_called()
    output = capsys.readouterr().out
    assert f"Error: Task {task.id} has open BLOCKER findings in review {review.id}." in output
    assert "Use --defer-blockers to merge anyway and create urgent PR-required follow-up tasks." in output


def test_merge_single_task_refuses_non_verify_blockers_without_flag(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement blocked merge", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/non-verify-blocker"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    store.update(review)

    blocker = ReviewFinding(
        id="B2",
        severity="BLOCKER",
        title="Missing data migration",
        body="Canonical blocker context.",
        evidence=None,
        impact=None,
        fix_or_followup="add migration",
        tests=None,
        open_state_citation="finding remains open",
    )

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
        defer_blockers=False,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with (
        patch(
            "gza.cli.git_ops.get_review_report",
            return_value=SimpleNamespace(verdict="CHANGES_REQUESTED", findings=(blocker,), format_version="v2"),
        ),
        patch("gza.cli.git_ops.get_review_content", return_value="review content"),
        patch(
            "gza.cli.git_ops.summarize_review_blockers",
            return_value=SimpleNamespace(
                blocker_count=1,
                verify_timeout_count=0,
                verify_failure_count=0,
                unknown_or_code_count=1,
            ),
        ),
    ):
        with _force_merge_planner_action():
            result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 1
    git.merge.assert_not_called()
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "unmerged"
    output = capsys.readouterr().out
    assert f"Error: Task {task.id} has open BLOCKER findings in review {review.id}." in output
    assert "Use --defer-blockers to merge anyway and create urgent PR-required follow-up tasks." in output


def test_merge_single_task_refuses_non_verify_report_file_blockers_with_normal_hint(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement report-file non-verify blocker path", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/report-file-non-verify-blocker"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.report_file = _write_review_report(
        tmp_path,
        name="non-verify-review.md",
        content=_changes_requested_review_with_blocker(
            title="Missing data migration",
            evidence="The branch adds a new column without a migration.",
            required_fix="add the migration and backfill path.",
        ),
    )
    store.update(review)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
        defer_blockers=False,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with _force_merge_planner_action():
        result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 1
    git.merge.assert_not_called()
    output = capsys.readouterr().out
    assert f"Error: Task {task.id} has open BLOCKER findings in review {review.id}." in output
    assert "blocker classification did not match the parsed blocker set" not in output
    assert "Use --defer-blockers to merge anyway and create urgent PR-required follow-up tasks." in output


def test_merge_single_task_defer_blockers_flag_materializes_and_proceeds(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement deferred blocker merge", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/defer-blockers"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    store.update(review)

    blockers = (
        ReviewFinding(
            id="B3",
            severity="BLOCKER",
            title="Missing migration",
            body="Body",
            evidence=None,
            impact=None,
            fix_or_followup="add migration",
            tests=None,
            open_state_citation="citation",
        ),
        ReviewFinding(
            id="B4",
            severity="BLOCKER",
            title="Missing cleanup",
            body="Body",
            evidence=None,
            impact=None,
            fix_or_followup="add cleanup",
            tests=None,
            open_state_citation="citation",
        ),
    )
    deferred_task = store.add("Deferred blocker B3", task_type="implement", based_on=review.id, depends_on=task.id)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
        defer_blockers=True,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with (
        patch(
            "gza.cli.git_ops.get_review_report",
            return_value=SimpleNamespace(verdict="CHANGES_REQUESTED", findings=blockers, format_version="v2"),
        ),
        patch(
            "gza.cli.git_ops.summarize_review_blockers",
            return_value=SimpleNamespace(blocker_count=2),
        ),
        patch(
            "gza.cli.git_ops._create_or_reuse_deferred_blocker_tasks", return_value=([deferred_task], [])
        ) as materialize,
    ):
        with _force_merge_planner_action():
            result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 0
    git.merge.assert_called_once()
    materialize.assert_called_once()
    assert materialize.call_args.kwargs["findings"] == blockers
    output = capsys.readouterr().out
    assert f"DEFERRED-BLOCKER {deferred_task.id} created from {task.id}" in output


def test_merge_single_task_mark_only_materializes_blockers_before_marking_merged(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement mark-only blocker path", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/mark-only-blocker"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    store.update(review)

    blocker = ReviewFinding(
        id="B5",
        severity="BLOCKER",
        title="Missing guard",
        body="Body",
        evidence=None,
        impact=None,
        fix_or_followup="add guard",
        tests=None,
        open_state_citation="citation",
    )
    deferred_task = store.add("Deferred blocker B5", task_type="implement", based_on=review.id, depends_on=task.id)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=True,
        remote=False,
        resolve=False,
        defer_blockers=True,
        no_followups=False,
    )
    config = Config.load(tmp_path)
    order: list[str] = []
    original_set_merge_status = store.set_merge_status
    original_set_merge_unit_state = store.set_merge_unit_state

    def _record_set_merge_status(*call_args, **call_kwargs):
        order.append("mark")
        return original_set_merge_status(*call_args, **call_kwargs)

    def _record_set_merge_unit_state(*call_args, **call_kwargs):
        order.append("mark")
        return original_set_merge_unit_state(*call_args, **call_kwargs)

    store.set_merge_status = MagicMock(side_effect=_record_set_merge_status)  # type: ignore[method-assign]
    store.set_merge_unit_state = MagicMock(side_effect=_record_set_merge_unit_state)  # type: ignore[method-assign]

    with (
        patch(
            "gza.cli.git_ops.get_review_report",
            return_value=SimpleNamespace(verdict="CHANGES_REQUESTED", findings=(blocker,), format_version="v2"),
        ),
        patch(
            "gza.cli.git_ops.summarize_review_blockers",
            return_value=SimpleNamespace(blocker_count=1),
        ),
        patch(
            "gza.cli.git_ops._create_or_reuse_deferred_blocker_tasks",
            side_effect=lambda *a, **k: order.append("defer") or ([deferred_task], []),
        ),
    ):
        with _force_merge_planner_action():
            result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 0
    assert order == ["defer", "mark"]
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.merge_source == "manual_force"


def test_merge_single_task_mark_only_defer_blockers_refuses_spec_coherence_changes_requested_review(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement mark-only spec coherence blocker path", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/mark-only-spec-coherence-blocker"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None

    review = store.add(f"Spec coherence review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.review_scope = "\n".join(
        (
            "Review mode: spec-coherence",
            f"Implementation task: {task.id}",
            "Reviewed head SHA: reviewed-head",
            'Changed behavior-spec paths JSON: ["specs/behavior/lifecycle-engine.md"]',
        )
    )
    review.report_file = _write_review_report(
        tmp_path,
        name=f"{task.id}-mark-only-spec-coherence-review.md",
        content=_changes_requested_review_with_blocker(
            title="Spec and code disagree",
            evidence="The behavior spec says this gate must remain actionable.",
            required_fix="keep the spec-coherence improve gate intact.",
        ),
    )
    store.update(review)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=True,
        remote=False,
        resolve=False,
        defer_blockers=True,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with patch("gza.cli.git_ops._create_or_reuse_deferred_blocker_tasks") as materialize:
        result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 1
    materialize.assert_not_called()
    assert not store.get_based_on_children(review.id)
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state != "merged"
    assert refreshed_unit.merge_source != "manual_force"
    output = capsys.readouterr().out
    assert "behavior-spec coherence CHANGES_REQUESTED review" in output
    assert "not deferable" in output


def test_merge_single_task_mark_only_without_deferred_blockers_records_manual_source(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement ordinary mark-only path", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/ordinary-mark-only"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=True,
        remote=False,
        resolve=False,
        defer_blockers=False,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 0
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.merge_source == "manual"


@pytest.mark.parametrize("mark_only", [False, True])
@pytest.mark.parametrize("fail_on_finding", ["F1", "F2"])
def test_merge_single_task_manual_ordinary_followup_failure_precedes_irreversible_state_and_retries(
    tmp_path: Path,
    mark_only: bool,
    fail_on_finding: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Manual ordinary follow-up failure", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/manual-followup-failure"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review = _completed_review(store, task, _approved_with_followups_output("F1", "F2"))
    config = Config.load(tmp_path)
    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
        delete_branch=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=not mark_only,
        mark_only=mark_only,
        remote=False,
        resolve=False,
        defer_blockers=False,
        no_followups=False,
        force=False,
        ignore_verify_gate=False,
    )
    original_create = store.create_or_reuse_followup_task
    should_fail = True

    def create_side_effect(*call_args: object, **call_kwargs: object) -> tuple[DbTask, bool]:
        nonlocal should_fail
        params = call_kwargs["params"]
        if should_fail and f"Follow-up {fail_on_finding} " in params.prompt:
            should_fail = False
            raise sqlite3.OperationalError(f"locked creating {fail_on_finding}")
        return original_create(*call_args, **call_kwargs)

    store.create_or_reuse_followup_task = MagicMock(side_effect=create_side_effect)  # type: ignore[method-assign]

    with _force_merge_planner_action():
        first = _merge_single_task(task.id, config, store, git, args, "main")

    assert first.rc == 1
    assert first.status == "merge_side_effect_materialization_failed"
    git.merge.assert_not_called()
    git.delete_branch.assert_not_called()
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None
    partial_children = [child for child in store.get_based_on_children(review.id) if child.task_type == "implement"]
    if fail_on_finding == "F1":
        assert partial_children == []
        assert first.created_followups == ()
    else:
        assert len(partial_children) == 1
        assert [child.id for child in partial_children] == [task.id for task in first.created_followups]

    with _force_merge_planner_action():
        second = _merge_single_task(task.id, config, store, git, args, "main")

    assert second.rc == 0
    all_children = [child for child in store.get_based_on_children(review.id) if child.task_type == "implement"]
    assert len(all_children) == 2
    if fail_on_finding == "F2":
        assert [task.id for task in second.reused_followups] == [partial_children[0].id]
    if mark_only:
        git.merge.assert_not_called()
        git.delete_branch.assert_not_called()
    else:
        git.merge.assert_called_once()
        git.delete_branch.assert_called_once_with(task.branch)
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"
    assert refreshed_unit.merge_source == "manual"


def test_merge_single_task_manual_approved_without_followups_still_merges_once(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Manual approved no follow-up", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/manual-approved-no-followup"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    _completed_review(store, task, "## Review\n\nVerdict: APPROVED\n")
    config = Config.load(tmp_path)
    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
        delete_branch=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
        defer_blockers=False,
        no_followups=False,
        force=False,
        ignore_verify_gate=False,
    )

    with _force_merge_planner_action():
        result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 0
    assert result.created_followups == ()
    assert result.reused_followups == ()
    git.merge.assert_called_once()


def test_merge_single_task_no_followups_does_not_suppress_deferred_blockers(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement no-followups blocker path", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/no-followups-blocker"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    store.update(review)

    blocker = ReviewFinding(
        id="B6",
        severity="BLOCKER",
        title="Missing invariant",
        body="Body",
        evidence=None,
        impact=None,
        fix_or_followup="add invariant",
        tests=None,
        open_state_citation="citation",
    )
    deferred_task = store.add("Deferred blocker B6", task_type="implement", based_on=review.id, depends_on=task.id)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
        defer_blockers=True,
        no_followups=True,
    )
    config = Config.load(tmp_path)

    with (
        patch(
            "gza.cli.git_ops.get_review_report",
            return_value=SimpleNamespace(verdict="CHANGES_REQUESTED", findings=(blocker,), format_version="v2"),
        ),
        patch(
            "gza.cli.git_ops.summarize_review_blockers",
            return_value=SimpleNamespace(blocker_count=1),
        ),
        patch("gza.cli.git_ops._create_or_reuse_deferred_blocker_tasks", return_value=([deferred_task], [])),
        patch("gza.cli.git_ops._materialize_merge_followups") as materialize_followups,
    ):
        with _force_merge_planner_action():
            result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 0
    materialize_followups.assert_not_called()
    output = capsys.readouterr().out
    assert f"DEFERRED-BLOCKER {deferred_task.id} created from {task.id}" in output


def test_merge_single_task_mark_only_verify_only_blocker_without_flag_refuses(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement idempotent mark-only blocker path", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/idempotent-mark-only-blocker"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    store.update(review)

    blocker = ReviewFinding(
        id="B7",
        severity="BLOCKER",
        title="Verify timed out",
        body="Body",
        evidence=None,
        impact=None,
        fix_or_followup="stabilize verify",
        tests=None,
        open_state_citation="citation",
    )

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=True,
        remote=False,
        resolve=False,
        defer_blockers=False,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with (
        patch(
            "gza.cli.git_ops.get_review_report",
            return_value=SimpleNamespace(verdict="CHANGES_REQUESTED", findings=(blocker,), format_version="v2"),
        ),
    ):
        first = _merge_single_task(task.id, config, store, git, args, "main")
        store.set_merge_status(task.id, "unmerged")
        second = _merge_single_task(task.id, config, store, git, args, "main")

    assert first.rc == 1
    assert second.rc == 1
    children = [
        child
        for child in store.get_based_on_children(review.id)
        if child.prompt.startswith(f"Deferred blocker {blocker.id} from review {review.id} for task {task.id}:")
    ]
    assert len(children) == 0


def test_merge_single_task_same_merge_unit_review_on_representative_refuses_without_flag(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    owner, representative, review = _add_same_merge_unit_owner_representative_with_review(
        tmp_path,
        store,
        review_content=_changes_requested_review_with_blocker(
            title="Missing data migration",
            evidence="The representative branch adds a new column without a migration.",
            required_fix="add the migration and backfill path.",
        ),
    )

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
        defer_blockers=False,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with _force_merge_planner_action():
        result = _merge_single_task(representative.id, config, store, git, args, "main")

    assert result.rc == 1
    git.merge.assert_not_called()
    assert store.get_reviews_for_task(owner.id) == []
    output = capsys.readouterr().out
    assert f"Error: Task {owner.id} has open BLOCKER findings in review {review.id}." in output
    assert "Use --defer-blockers to merge anyway and create urgent PR-required follow-up tasks." in output


def test_merge_single_task_same_merge_unit_review_on_representative_defer_flag_materializes_before_merge(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    owner, representative, review = _add_same_merge_unit_owner_representative_with_review(
        tmp_path,
        store,
        review_content=_changes_requested_review_with_blocker(
            title="Missing data migration",
            evidence="The representative branch adds a new column without a migration.",
            required_fix="add the migration and backfill path.",
        ),
    )

    merge_order: list[str] = []

    def _assert_deferred_before_merge(*_args, **_kwargs):
        blockers = [
            child
            for child in store.get_based_on_children(review.id)
            if child.prompt.startswith(f"Deferred blocker B1 from review {review.id} for task {owner.id}:")
        ]
        assert len(blockers) == 1
        merge_order.append("merge")

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(side_effect=_assert_deferred_before_merge),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
        defer_blockers=True,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with _force_merge_planner_action():
        result = _merge_single_task(representative.id, config, store, git, args, "main")

    assert result.rc == 0
    assert merge_order == ["merge"]


def test_merge_single_task_defer_blockers_merges_over_changes_requested_gate_once(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    owner, representative, review = _add_same_merge_unit_owner_representative_with_review(
        tmp_path,
        store,
        review_content=_changes_requested_review_with_blocker(
            title="Missing data migration",
            evidence="The representative branch adds a new column without a migration.",
            required_fix="add the migration and backfill path.",
        ),
    )
    unit = store.resolve_merge_unit_for_task(owner.id)
    assert unit is not None

    deferred_task = store.add("Deferred blocker B1", task_type="implement", based_on=review.id, depends_on=owner.id)
    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        force=False,
        remote=False,
        resolve=False,
        defer_blockers=True,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with (
        patch(
            "gza.cli.git_ops.determine_next_action",
            return_value={
                "type": "improve",
                "description": "Create improve task (review CHANGES_REQUESTED)",
                "improve_reason": "review_changes_requested",
                "review_task": review,
            },
        ),
        patch(
            "gza.cli.git_ops._create_or_reuse_deferred_blocker_tasks",
            return_value=([deferred_task], []),
        ) as materialize,
    ):
        result = _merge_single_task(representative.id, config, store, git, args, "main")

    assert result.rc == 0
    git.merge.assert_called_once_with(owner.branch, squash=False, commit_message=None)
    materialize.assert_called_once()
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit.merge_source == "manual_force"
    output = capsys.readouterr().out
    assert output.count(f"DEFERRED-BLOCKER {deferred_task.id} created from {owner.id}") == 1
    assert "Warning: Forcing merge despite lifecycle gate: Create improve task (review CHANGES_REQUESTED)" in output


def test_merge_single_task_defer_blockers_merges_over_resolution_changes_requested_gate_once(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    owner, representative, review = _add_same_merge_unit_owner_representative_with_review(
        tmp_path,
        store,
        review_content=_changes_requested_review_with_blocker(
            title="Resolution cleanup missing",
            evidence="The conflict resolution leaves both sides of the branch choice active.",
            required_fix="defer the cleanup into a PR-required follow-up.",
        ),
    )
    review.review_scope = "\n".join(
        (
            "Review mode: resolution",
            f"Implementation task: {owner.id}",
            "Rebase task: gza-1",
            "Resolved head SHA: resolved-head",
            "Resolved target SHA: resolved-target",
        )
    )
    store.update(review)
    unit = store.resolve_merge_unit_for_task(owner.id)
    assert unit is not None

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        force=False,
        remote=False,
        resolve=False,
        defer_blockers=True,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with patch(
        "gza.cli.git_ops.determine_next_action",
        return_value={
            "type": "improve",
            "description": "Create improve task (resolution review CHANGES_REQUESTED)",
            "improve_reason": "review_changes_requested",
            "review_task": review,
        },
    ):
        result = _merge_single_task(representative.id, config, store, git, args, "main")

    assert result.rc == 0
    git.merge.assert_called_once_with(owner.branch, squash=False, commit_message=None)
    blockers = [
        child
        for child in store.get_based_on_children(review.id)
        if child.prompt.startswith(f"Deferred blocker B1 from review {review.id} for task {owner.id}:")
    ]
    assert len(blockers) == 1
    assert blockers[0].status == "pending"
    assert blockers[0].urgent is True
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit.merge_source == "manual_force"
    output = capsys.readouterr().out
    assert output.count(f"DEFERRED-BLOCKER {blockers[0].id} created from {owner.id}") == 1
    assert f"DEFERRED-BLOCKER {blockers[0].id} reused from {owner.id}" not in output
    assert (
        "Warning: Forcing merge despite lifecycle gate: Create improve task (resolution review CHANGES_REQUESTED)"
        in output
    )


def test_merge_single_task_defer_blockers_prints_created_task_before_conflict_refusal(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    owner, representative, review = _add_same_merge_unit_owner_representative_with_review(
        tmp_path,
        store,
        review_content=_changes_requested_review_with_blocker(
            title="Missing data migration",
            evidence="The representative branch adds a new column without a migration.",
            required_fix="add the migration and backfill path.",
        ),
    )

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=False),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        force=False,
        remote=False,
        resolve=False,
        defer_blockers=True,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with patch(
        "gza.cli.git_ops.determine_next_action",
        return_value={
            "type": "improve",
            "description": "Create improve task (review CHANGES_REQUESTED)",
            "improve_reason": "review_changes_requested",
            "review_task": review,
        },
    ):
        result = _merge_single_task(representative.id, config, store, git, args, "main")

    assert result.rc == 1
    git.merge.assert_not_called()
    blockers = [
        child
        for child in store.get_based_on_children(review.id)
        if child.prompt.startswith(f"Deferred blocker B1 from review {review.id} for task {owner.id}:")
    ]
    assert len(blockers) == 1
    assert blockers[0].status == "pending"
    assert blockers[0].urgent is True
    output = capsys.readouterr().out
    assert output.count(f"DEFERRED-BLOCKER {blockers[0].id} created from {owner.id}") == 1
    assert f"DEFERRED-BLOCKER {blockers[0].id} reused from {owner.id}" not in output
    assert "has conflicts against 'main' and cannot be merged cleanly" in output


def test_merge_single_task_defer_blockers_refuses_ambiguous_changes_requested_gate(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement ambiguous deferred blocker merge", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/ambiguous-defer-blockers"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    store.update(review)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        force=False,
        remote=False,
        resolve=False,
        defer_blockers=True,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with (
        patch(
            "gza.cli.git_ops.determine_next_action",
            return_value={
                "type": "improve",
                "description": "Create improve task (review CHANGES_REQUESTED)",
                "improve_reason": "review_changes_requested",
                "review_task": review,
            },
        ),
        patch("gza.cli.git_ops._create_or_reuse_deferred_blocker_tasks") as materialize,
    ):
        result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 1
    git.merge.assert_not_called()
    materialize.assert_not_called()
    output = capsys.readouterr().out
    assert (
        f"Error: Task {task.id} has CHANGES_REQUESTED review {review.id}, "
        "but no parsed BLOCKER findings were available to defer. Refusing to guess."
    ) in output


def test_merge_single_task_defer_blockers_force_refuses_spec_coherence_changes_requested_gate(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement spec coherence gated merge", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/spec-coherence-defer-force"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    review = store.add(f"Spec coherence review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.review_scope = "\n".join(
        (
            "Review mode: spec-coherence",
            f"Implementation task: {task.id}",
            "Reviewed head SHA: reviewed-head",
            'Changed behavior-spec paths JSON: ["specs/behavior/merge.md"]',
        )
    )
    review.output_content = _changes_requested_review_with_blocker(
        title="Spec and code disagree",
        evidence="The behavior spec says this gate must remain actionable.",
        required_fix="keep the spec-coherence improve gate intact.",
    )
    store.update(review)

    blocker = ReviewFinding(
        id="B-spec",
        severity="BLOCKER",
        title="Spec and code disagree",
        body="Body",
        evidence=None,
        impact=None,
        fix_or_followup="keep the spec-coherence improve gate intact",
        tests=None,
        open_state_citation="citation",
    )
    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        force=True,
        remote=False,
        resolve=False,
        defer_blockers=True,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with (
        patch(
            "gza.cli.git_ops.determine_next_action",
            return_value={
                "type": "improve",
                "description": "Create improve task (behavior-spec coherence review CHANGES_REQUESTED)",
                "improve_reason": "spec_coherence_changes_requested",
                "review_mode": "spec_coherence",
                "review_task": review,
            },
        ),
        patch(
            "gza.cli.git_ops.get_review_report",
            return_value=SimpleNamespace(verdict="CHANGES_REQUESTED", findings=(blocker,), format_version="v2"),
        ),
        patch("gza.cli.git_ops._materialize_merge_deferred_blockers") as materialize,
    ):
        result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 1
    git.merge.assert_not_called()
    materialize.assert_not_called()
    output = capsys.readouterr().out
    assert "Error: Create improve task (behavior-spec coherence review CHANGES_REQUESTED)" in output


def test_merge_single_task_defer_blockers_force_refuses_fresh_comments_improve_gate(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement fresh comments gated merge", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/fresh-comments-defer-force"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "## Verdict\n\nVerdict: APPROVED\n"
    store.update(review)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        force=True,
        remote=False,
        resolve=False,
        defer_blockers=True,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with (
        patch(
            "gza.cli.git_ops.determine_next_action",
            return_value={
                "type": "improve",
                "description": "Create improve task (unresolved comments newer than latest review)",
                "improve_reason": "fresh_comments",
                "review_task": review,
            },
        ),
        patch("gza.cli.git_ops._materialize_merge_deferred_blockers") as materialize,
    ):
        result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 1
    git.merge.assert_not_called()
    materialize.assert_not_called()
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state != "merged"
    assert refreshed_unit.merge_source != "manual_force"
    assert not store.get_based_on_children(review.id)
    output = capsys.readouterr().out
    assert "Error: Create improve task (unresolved comments newer than latest review)" in output
    assert "Warning: Forcing merge despite lifecycle gate" not in output


def test_merge_single_task_force_defer_blockers_refuses_spec_coherence_review_after_needs_attention_gate(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement force spec coherence blocker path", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/force-spec-coherence-blocker"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None

    review = store.add(f"Spec coherence review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.review_scope = "\n".join(
        (
            "Review mode: spec-coherence",
            f"Implementation task: {task.id}",
            "Reviewed head SHA: reviewed-head",
            'Changed behavior-spec paths JSON: ["specs/behavior/lifecycle-engine.md"]',
        )
    )
    review.report_file = _write_review_report(
        tmp_path,
        name=f"{task.id}-force-spec-coherence-review.md",
        content=_changes_requested_review_with_blocker(
            title="Spec and code disagree",
            evidence="The behavior spec says this gate must remain actionable.",
            required_fix="keep the spec-coherence improve gate intact.",
        ),
    )
    store.update(review)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        force=True,
        remote=False,
        resolve=False,
        defer_blockers=True,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with (
        patch(
            "gza.cli.git_ops.determine_next_action",
            return_value={
                "type": "parked",
                "description": "Manual review required before merge",
                "needs_attention_reason": "operator-review-required",
                "subject_task_id": task.id,
            },
        ),
        patch("gza.cli.git_ops._create_or_reuse_deferred_blocker_tasks") as materialize,
    ):
        result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 1
    git.merge.assert_not_called()
    materialize.assert_not_called()
    assert not store.get_based_on_children(review.id)
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state != "merged"
    assert refreshed_unit.merge_source != "manual_force"
    output = capsys.readouterr().out
    assert "Warning: Forcing merge despite lifecycle gate: Manual review required before merge" in output
    assert "behavior-spec coherence CHANGES_REQUESTED review" in output
    assert "not deferable" in output


def test_merge_single_task_defer_blockers_refuses_blocker_count_mismatch(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement mismatched deferred blocker merge", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/mismatched-defer-blockers"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    store.update(review)

    blocker = ReviewFinding(
        id="B-mismatch",
        severity="BLOCKER",
        title="Missing data migration",
        body="Body",
        evidence=None,
        impact=None,
        fix_or_followup="add migration",
        tests=None,
        open_state_citation="citation",
    )
    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        force=False,
        remote=False,
        resolve=False,
        defer_blockers=True,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with (
        patch(
            "gza.cli.git_ops.determine_next_action",
            return_value={
                "type": "improve",
                "description": "Create improve task (review CHANGES_REQUESTED)",
                "improve_reason": "review_changes_requested",
                "review_task": review,
            },
        ),
        patch(
            "gza.cli.git_ops.get_review_report",
            return_value=SimpleNamespace(verdict="CHANGES_REQUESTED", findings=(blocker,), format_version="v2"),
        ),
        patch("gza.cli.git_ops.get_review_content", return_value="review content without structured blockers"),
        patch("gza.cli.git_ops.summarize_review_blockers", return_value=SimpleNamespace(blocker_count=0)),
        patch("gza.cli.git_ops._create_or_reuse_deferred_blocker_tasks") as materialize,
    ):
        result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 1
    git.merge.assert_not_called()
    materialize.assert_not_called()
    output = capsys.readouterr().out
    assert (
        f"Error: Task {task.id} has CHANGES_REQUESTED review {review.id}, "
        "but blocker classification did not match the parsed blocker set. Refusing to guess."
    ) in output


def test_merge_single_task_defer_blockers_force_still_refuses_needs_rebase_gate(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement needs rebase merge", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/needs-rebase-defer-force"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        force=True,
        remote=False,
        resolve=False,
        defer_blockers=True,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with (
        patch(
            "gza.cli.git_ops.determine_next_action",
            return_value={"type": "needs_rebase", "description": "rebase --resolve (conflicts detected)"},
        ),
        patch("gza.cli.git_ops._materialize_merge_deferred_blockers") as materialize,
    ):
        result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 1
    git.merge.assert_not_called()
    materialize.assert_not_called()
    output = capsys.readouterr().out
    assert "Error: rebase --resolve (conflicts detected)" in output


def test_merge_single_task_force_still_refuses_open_review_blockers_without_defer_flag(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement forced blocked merge", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/force-blocker"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    store.update(review)

    blocker = ReviewFinding(
        id="B-force",
        severity="BLOCKER",
        title="Missing data migration",
        body="Body",
        evidence=None,
        impact=None,
        fix_or_followup="add migration",
        tests=None,
        open_state_citation="citation",
    )

    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        branch_exists=MagicMock(return_value=True),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        force=True,
        remote=False,
        resolve=False,
        defer_blockers=False,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with (
        patch(
            "gza.cli.git_ops.determine_next_action",
            return_value={
                "type": "needs_discussion",
                "description": "SKIP: required resolution-review metadata is missing or malformed",
                "needs_attention_reason": "resolution-review-metadata-invalid",
            },
        ),
        patch(
            "gza.cli.git_ops.get_review_report",
            return_value=SimpleNamespace(verdict="CHANGES_REQUESTED", findings=(blocker,), format_version="v2"),
        ),
        patch("gza.cli.git_ops.get_review_content", return_value="review content"),
        patch(
            "gza.cli.git_ops.summarize_review_blockers",
            return_value=SimpleNamespace(
                blocker_count=1,
                verify_timeout_count=0,
                verify_failure_count=0,
                unknown_or_code_count=1,
            ),
        ),
    ):
        result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 1
    git.merge.assert_not_called()
    output = capsys.readouterr().out
    assert "Warning: Forcing merge despite lifecycle gate" in output
    assert f"Error: Task {task.id} has open BLOCKER findings in review {review.id}." in output
    assert "Use --defer-blockers to merge anyway and create urgent PR-required follow-up tasks." in output


def test_merge_single_task_same_merge_unit_verify_only_review_on_representative_mark_only_refuses(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    owner, representative, review = _add_same_merge_unit_owner_representative_with_review(
        tmp_path,
        store,
        review_content=_changes_requested_review_with_blocker(
            title="verify_command failure: command exited nonzero",
            evidence="verify_command failed while running `./bin/tests`; the assertion failure is in the verify output.",
            required_fix="rerun verify_command on the current tip.",
        ),
    )

    order: list[str] = []
    original_set_merge_status = store.set_merge_status
    original_set_merge_unit_state = store.set_merge_unit_state

    def _record_set_merge_status(*call_args, **call_kwargs):
        order.append("mark")
        return original_set_merge_status(*call_args, **call_kwargs)

    def _record_set_merge_unit_state(*call_args, **call_kwargs):
        blockers = [
            child
            for child in store.get_based_on_children(review.id)
            if child.prompt.startswith(f"Deferred blocker B1 from review {review.id} for task {owner.id}:")
        ]
        assert len(blockers) == 1
        order.append("mark")
        return original_set_merge_unit_state(*call_args, **call_kwargs)

    store.set_merge_status = MagicMock(side_effect=_record_set_merge_status)  # type: ignore[method-assign]
    store.set_merge_unit_state = MagicMock(side_effect=_record_set_merge_unit_state)  # type: ignore[method-assign]

    git = SimpleNamespace(
        repo_dir=tmp_path,
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=True,
        remote=False,
        resolve=False,
        defer_blockers=False,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    result = _merge_single_task(representative.id, config, store, git, args, "main")

    assert result.rc == 1
    assert order == []
    blockers = [
        child
        for child in store.get_based_on_children(review.id)
        if child.prompt.startswith(f"Deferred blocker B1 from review {review.id} for task {owner.id}:")
    ]
    assert len(blockers) == 0


def test_run_task_backed_rebase_refreshes_merge_unit_provenance(tmp_path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(
        parent,
        has_commits=True,
        branch="feature/rebased",
        head_sha="head-old",
        base_sha="base-old",
    )
    assert parent.id is not None
    unit = store.resolve_merge_unit_for_task(parent.id)
    assert unit is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.return_value = None
    worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-new",
        "main": "base-new",
    }.get(ref)

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline(
                old_tip="head-old",
                target_at_start="base-old",
                merge_base_at_start="merge-base",
            ),
        ),
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 0
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.head_sha == "head-new"
    assert refreshed_unit.base_sha == "base-new"
    repo_git.worktree_add_existing.assert_called_once_with(
        config.worktree_path / str(rebase_task.id), "feature/rebased"
    )


def test_run_task_backed_rebase_uses_worktree_add_existing_for_setup(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    parent.status = "completed"
    parent.completed_at = datetime.now(UTC)
    parent.branch = "feature/rebased"
    store.update(parent)

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.return_value = None
    worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-new",
        "main": "base-new",
    }.get(ref)

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 0
    repo_git.worktree_add_existing.assert_called_once_with(
        config.worktree_path / str(rebase_task.id), "feature/rebased"
    )
    assert (
        call("worktree", "add", str(config.worktree_path / str(rebase_task.id)), "feature/rebased")
        not in repo_git._run.call_args_list
    )


def test_execute_merge_action_mark_merged_rejects_failed_owner_without_marking_unit(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    store = make_store(tmp_path)

    failed = store.add("Failed implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "TIMEOUT"
    failed.completed_at = datetime.now(UTC)
    failed.branch = "feature/failed-owner-mark-merged"
    failed.has_commits = True
    failed.merge_status = "unmerged"
    store.update(failed)

    merge_git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=True),
    )
    git = _merge_executor_git(tmp_path, failed.branch)
    git.is_merged.return_value = True

    result = _execute_merge_action(
        config,
        store,
        git,
        failed,
        {"type": "merge", "description": "Merge"},
        target_branch="main",
        current_branch="main",
        merge_git=merge_git,
        merge_current_branch="main",
        already_merged_behavior="mark_merged",
    )

    assert result.rc == 1
    output = capsys.readouterr().out
    assert f"Error: Task {failed.id} is not completed or unmerged (execution status: failed)" in output
    refreshed = store.resolve_merge_unit_for_task(failed.id)
    assert refreshed is not None
    assert refreshed.state == "unmerged"


def test_merge_source_for_action_only_overrides_explicit_max_cycle_merge_action() -> None:
    assert merge_source_for_action({"type": "merge"}, MERGE_SOURCE_ADVANCE) == MERGE_SOURCE_ADVANCE
    assert (
        merge_source_for_action({"type": "merge", "max_cycles_merge_and_defer": False}, MERGE_SOURCE_WATCH)
        == MERGE_SOURCE_WATCH
    )
    assert (
        merge_source_for_action(
            {"type": "merge_with_followups", "max_cycles_merge_and_defer": True},
            MERGE_SOURCE_ADVANCE,
        )
        == MERGE_SOURCE_ADVANCE
    )
    assert (
        merge_source_for_action({"type": "merge", "max_cycles_merge_and_defer": True}, MERGE_SOURCE_WATCH)
        == MERGE_SOURCE_MAX_CYCLES_DEFERRED
    )


def test_capped_review_blocker_findings_preserves_valid_multi_blocker_tuple() -> None:
    first = _blocker_finding("B1")
    second = _blocker_finding("B2")

    result = _capped_review_blocker_findings_for_action(
        {
            "type": "merge",
            "max_cycles_merge_and_defer": True,
            "blocker_findings": (first, second),
        }
    )

    assert result == (first, second)


def test_pending_capped_finalization_reconciles_current_scope_tags_before_state(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Replay capped proof", "feature/replay-capped-proof")
    review_output = (
        "## Review\n\nVerdict: CHANGES_REQUESTED\n\n"
        "## Blockers\n\n"
        "### B1 Original blocker\n"
        "Evidence: original evidence.\n"
        "Impact: original impact.\n"
        "Required fix: original fix.\n"
        "Required tests: original tests.\n"
    )
    review = _completed_review(store, task, review_output)
    finding = parse_review_report(review_output).findings[0]
    proven_child, created_now = create_or_reuse_capped_review_blocker_task(
        store,
        config=config,
        review_task=review,
        impl_task=task,
        finding=finding,
        persisted_review_output=review_output,
        active_scope_tags=("original-scope",),
        trigger_source=MERGE_SOURCE_ADVANCE,
    )
    assert created_now is True
    store.add_task_tags(task.id, ("current-impl-scope",))
    task = store.get(task.id)
    assert task is not None
    action = _max_cycle_merge_action(review, (finding,), review_output)
    action["pending_merge_finalization"] = True
    action["proven_deferred_blocker_tasks"] = (proven_child,)

    created, reused = _materialize_max_cycle_deferred_blockers_for_action(
        store,
        config=config,
        merge_subject=task,
        action=action,
        active_scope_tags=("all-tag-watch-scope",),
        trigger_source=MERGE_SOURCE_WATCH,
    )

    assert created == []
    assert [task.id for task in reused] == [proven_child.id]
    refreshed_child = store.get(proven_child.id)
    assert refreshed_child is not None
    assert set(refreshed_child.tags) == {
        "current-impl-scope",
        "deferred-review-blocker",
        "original-scope",
        "all-tag-watch-scope",
    }


def test_pending_capped_finalization_reserved_tag_drift_blocks_state(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Replay capped tag write failure", "feature/replay-capped-tag-fail")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    finding = parse_review_report(review_output).findings[0]
    proven_child, created_now = create_or_reuse_capped_review_blocker_task(
        store,
        config=config,
        review_task=review,
        impl_task=task,
        finding=finding,
        persisted_review_output=review_output,
        active_scope_tags=("old-scope",),
        trigger_source=MERGE_SOURCE_ADVANCE,
    )
    assert created_now is True
    store.remove_task_tags(proven_child.id, ("deferred-review-blocker",))
    task = store.get(task.id)
    assert task is not None
    action = _max_cycle_merge_action(review, (finding,), review_output)
    action["pending_merge_finalization"] = True
    action["proven_deferred_blocker_tasks"] = (proven_child,)
    git = _merge_executor_git(tmp_path, task.branch)

    result = _execute_capped_merge_through_materialization_side_effect(
        config=config,
        store=store,
        git=git,
        task=task,
        action=action,
    )

    assert result.rc == 1
    assert result.status == "deferred_blocker_materialization_failed"
    assert "deferred-review-blocker tag" in (result.block_reason or "")
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


def test_pending_capped_finalization_tag_write_failure_blocks_state(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(
        store,
        "Replay capped tag write failure",
        "feature/replay-capped-tag-write-fail",
        tags=("current-impl-scope",),
    )
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    finding = parse_review_report(review_output).findings[0]
    proven_child, created_now = create_or_reuse_capped_review_blocker_task(
        store,
        config=config,
        review_task=review,
        impl_task=task,
        finding=finding,
        persisted_review_output=review_output,
        active_scope_tags=("old-scope",),
        trigger_source=MERGE_SOURCE_ADVANCE,
    )
    assert created_now is True
    action = _pending_replay_action_with_proof(
        store,
        family="capped",
        task=task,
        review=review,
        findings=(finding,),
        children=(proven_child,),
        output=review_output,
        source_ref_sha="same-sha",
        promoted_target_sha="same-sha",
    )
    replay_git = _replay_executor_git(tmp_path, task.branch, source_sha="same-sha", target_sha="same-sha")
    store._replace_task_tags_conn = MagicMock(side_effect=sqlite3.OperationalError("tag store locked"))  # type: ignore[method-assign]
    store.set_merge_unit_state = MagicMock(side_effect=AssertionError("state must not change"))  # type: ignore[method-assign]

    result = _execute_merge_action(
        config,
        store,
        replay_git,
        task,
        action,
        target_branch="main",
        current_branch="main",
        merge_git=replay_git,
        merge_current_branch="main",
        already_merged_behavior="mark_merged",
        merge_source=MERGE_SOURCE_WATCH,
        active_scope_tags=("new-all-tag-scope",),
    )

    assert result.rc == 1
    assert result.status == "deferred_blocker_materialization_failed"
    assert "tag store locked" in (result.block_reason or "")
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


def _squash_finalization_action(
    store: Any,
    task: Any,
    *,
    family: str,
) -> tuple[Any, dict[str, object], tuple[ReviewFinding, ...]]:
    if family == "ordinary_followup":
        review_output = _approved_with_followups_output("F1")
        review = _completed_review(store, task, review_output)
        findings = tuple(finding for finding in parse_review_report(review_output).findings if finding.severity == "FOLLOWUP")
        return review, {
            "type": "merge_with_followups",
            "description": "Merge with follow-ups",
            "review_task": review,
            "followup_findings": findings,
        }, findings
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    _set_review_head(store, review, "source-before")
    findings = (_blocker_finding("B1"),)
    return review, _max_cycle_merge_action(review, findings, review_output, reviewed_head_sha="source-before"), findings


@pytest.mark.parametrize("family", ["ordinary_followup", "max_cycles_deferred"])
@pytest.mark.parametrize("remote_tracking", [True, False])
def test_execute_merge_action_real_non_isolated_squash_replay_uses_promoted_identity(
    tmp_path: Path,
    family: str,
    remote_tracking: bool,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    config.merge_squash_threshold = 1
    store = make_store(tmp_path)
    task = _completed_merge_task(
        store,
        f"Squash replay {family} {remote_tracking}",
        f"feature/squash-replay-{family}-{remote_tracking}",
    )
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    _review, action, _findings = _squash_finalization_action(store, task, family=family)
    if family == "max_cycles_deferred":
        _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="source-before")
    resolved = _ResolvedMergeSubject(
        trigger_task=task,
        execution_task=task,
        merge_subject=task,
        merge_unit_id=unit.id,
        merge_branch=task.branch,
        merge_source_ref=task.branch,
        merge_source_warning=None,
    )
    git = _SquashFinalizationGit(tmp_path, task.branch, remote_tracking=remote_tracking)
    original_set_state = store.set_merge_unit_state
    store.set_merge_unit_state = MagicMock(side_effect=sqlite3.OperationalError("locked after squash"))  # type: ignore[method-assign]

    with (
        patch("gza.cli.git_ops._resolve_merge_subject", return_value=resolved),
        patch("gza.cli.git_ops.determine_next_action", return_value=action),
    ):
        first = _execute_merge_action(
            config,
            store,
            git,
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert first.rc == 1
    assert first.status == "post_merge_state_persistence_failed"
    children = first.reused_followups or first.created_followups if family == "ordinary_followup" else first.reused_deferred_blockers or first.created_deferred_blockers
    assert [child.id for child in children]
    artifacts = store.list_artifacts(task.id, kind="merge_finalization_attempt_proof")
    assert len(artifacts) == 1
    assert artifacts[0].metadata["promotion_kind"] == "squash"
    assert artifacts[0].metadata["promoted_target_sha"] == git.squash_sha
    assert artifacts[0].metadata["promoted_target_tree_sha"] == git.squash_tree
    if remote_tracking:
        assert git.ref_updates == [
            (f"refs/heads/{task.branch}", git.squash_sha, git.source_before),
            (f"refs/remotes/origin/{task.branch}", git.squash_sha, None),
        ]
    else:
        assert git.ref_updates == []

    store.set_merge_unit_state = original_set_state  # type: ignore[method-assign]
    replay_action = pending_merge_finalization_action(
        config,
        store,
        task,
        target_branch="main",
        require_already_merged=True,
        resolved_merge_state="merged",
        live_target_sha=git.squash_sha,
    )
    assert replay_action is not None
    assert replay_action["type"] in {"merge", "merge_with_followups"}

    with (
        patch("gza.cli.git_ops._resolve_merge_subject", return_value=resolved),
        patch("gza.cli.git_ops.determine_next_action", return_value=replay_action),
    ):
        second = _execute_merge_action(
            config,
            store,
            SimpleNamespace(repo_dir=tmp_path),
            task,
            replay_action,
            target_branch="main",
            current_branch="main",
            merge_git=git,
            merge_current_branch="main",
            already_merged_behavior="mark_merged",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert second.rc == 0
    assert second.status == "already_merged"
    replay_children = second.reused_followups if family == "ordinary_followup" else second.reused_deferred_blockers
    assert [child.id for child in replay_children] == [child.id for child in children]
    assert git.is_ancestor.call_count == 0
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"
    assert refreshed_unit.merge_source == (
        MERGE_SOURCE_ADVANCE if family == "ordinary_followup" else MERGE_SOURCE_MAX_CYCLES_DEFERRED
    )


@pytest.mark.parametrize("family", ["ordinary_followup", "max_cycles_deferred"])
@pytest.mark.parametrize("remote_tracking", [True, False])
def test_execute_merge_action_real_isolated_squash_replay_uses_promoted_identity(
    tmp_path: Path,
    family: str,
    remote_tracking: bool,
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\nverify_command: ./bin/tests\n")
    config = Config.load(tmp_path)
    config.merge_squash_threshold = 1
    store = make_store(tmp_path)
    task = _completed_merge_task(
        store,
        f"Isolated squash replay {family} {remote_tracking}",
        f"feature/isolated-squash-replay-{family}-{remote_tracking}",
    )
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    _review, action, _findings = _squash_finalization_action(store, task, family=family)
    if family == "max_cycles_deferred":
        _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="source-before")
    resolved = _ResolvedMergeSubject(
        trigger_task=task,
        execution_task=task,
        merge_subject=task,
        merge_unit_id=unit.id,
        merge_branch=task.branch,
        merge_source_ref=task.branch,
        merge_source_warning=None,
    )
    repo_git = _SquashFinalizationGit(
        tmp_path,
        task.branch,
        remote_tracking=remote_tracking,
        squash_sha="isolated-merge-oid",
    )
    merge_git = _SquashFinalizationGit(
        config.main_checkout_integration_path,
        task.branch,
        remote_tracking=remote_tracking,
        squash_sha="isolated-merge-oid",
    )
    original_set_state = store.set_merge_unit_state

    def _promote(*_args: object, **_kwargs: object) -> tuple[str, ...]:
        repo_git.merged = True
        return ()

    store.set_merge_unit_state = MagicMock(side_effect=sqlite3.OperationalError("locked after isolated squash"))  # type: ignore[method-assign]
    with (
        patch("gza.cli.git_ops._resolve_merge_subject", return_value=resolved),
        patch("gza.cli.git_ops.determine_next_action", return_value=action),
        patch("gza.cli.git_ops._promote_isolated_merge_to_target_branch", side_effect=_promote),
    ):
        first = _execute_merge_action(
            config,
            store,
            repo_git,
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
            merge_source=MERGE_SOURCE_WATCH,
        )

    assert first.rc == 1
    assert first.status == "isolated_post_promotion_merge_state_finalization_failed"
    children = first.reused_followups or first.created_followups if family == "ordinary_followup" else first.reused_deferred_blockers or first.created_deferred_blockers
    assert [child.id for child in children]
    artifacts = store.list_artifacts(task.id, kind="merge_finalization_attempt_proof")
    assert len(artifacts) == 1
    assert artifacts[0].metadata["promotion_kind"] == "squash"
    assert artifacts[0].metadata["promoted_target_sha"] == repo_git.squash_sha
    assert artifacts[0].metadata["promoted_target_tree_sha"] == repo_git.squash_tree

    store.set_merge_unit_state = original_set_state  # type: ignore[method-assign]
    replay_action = pending_merge_finalization_action(
        config,
        store,
        task,
        target_branch="main",
        require_already_merged=True,
        resolved_merge_state="merged",
        live_target_sha=repo_git.squash_sha,
    )
    assert replay_action is not None

    with (
        patch("gza.cli.git_ops._resolve_merge_subject", return_value=resolved),
        patch("gza.cli.git_ops.determine_next_action", return_value=replay_action),
    ):
        second = _execute_merge_action(
            config,
            store,
            SimpleNamespace(repo_dir=tmp_path),
            task,
            replay_action,
            target_branch="main",
            current_branch="main",
            merge_git=repo_git,
            merge_current_branch="main",
            already_merged_behavior="mark_merged",
            merge_source=MERGE_SOURCE_WATCH,
        )

    assert second.rc == 0
    assert second.status == "already_merged"
    replay_children = second.reused_followups if family == "ordinary_followup" else second.reused_deferred_blockers
    assert [child.id for child in replay_children] == [child.id for child in children]
    assert repo_git.is_ancestor.call_count == 0
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"
    assert refreshed_unit.merge_source == (
        MERGE_SOURCE_WATCH if family == "ordinary_followup" else MERGE_SOURCE_MAX_CYCLES_DEFERRED
    )


@pytest.mark.parametrize("family", ["capped", "ordinary"])
@pytest.mark.parametrize(
    ("race_case", "expected_reason"),
    [
        ("target-advanced", "promoted_target_sha"),
        ("proof-deleted", "disappeared or is malformed"),
        ("proof-mutated", "disappeared or is malformed"),
        ("source-drift", "source_ref_sha"),
    ],
)
def test_execute_merge_action_pending_replay_revalidates_proof_before_state(
    tmp_path: Path,
    family: str,
    race_case: str,
    expected_reason: str,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, f"Replay proof race {family} {race_case}", f"feature/{family}-{race_case}")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    if family == "capped":
        review_output = _capped_review_output("B1")
        review = _completed_review(store, task, review_output)
        finding = parse_review_report(review_output).findings[0]
        child, _created = create_or_reuse_capped_review_blocker_task(
            store,
            config=config,
            review_task=review,
            impl_task=task,
            finding=finding,
            persisted_review_output=review_output,
            active_scope_tags=("release",),
            trigger_source=MERGE_SOURCE_ADVANCE,
        )
        action = _pending_replay_action_with_proof(
            store,
            family="capped",
            task=task,
            review=review,
            findings=(finding,),
            children=(child,),
            output=review_output,
            source_ref_sha="source-a",
            promoted_target_sha="target-a",
        )
    else:
        review_output = _approved_with_followups_output("F1")
        review = _completed_review(store, task, review_output)
        finding = parse_review_report(review_output).findings[0]
        child = store.add(
            build_followup_prompt(review.id, task.id, finding),
            task_type="implement",
            based_on=review.id,
            depends_on=task.id,
            review_scope=format_followup_finding_context(finding),
        )
        action = _pending_replay_action_with_proof(
            store,
            family="ordinary",
            task=task,
            review=review,
            findings=(finding,),
            children=(child,),
            source_ref_sha="source-a",
            promoted_target_sha="target-a",
        )
    proof_id = action["merge_finalization_proof_id"]
    assert isinstance(proof_id, int)
    if race_case == "proof-deleted":
        with store._connect() as conn:  # type: ignore[attr-defined]
            conn.execute("DELETE FROM task_artifacts WHERE id = ?", (proof_id,))
            conn.commit()
    elif race_case == "proof-mutated":
        artifact = store.get_artifact(proof_id, task_id=task.id)
        assert artifact is not None
        store.add_artifact(
            task.id,
            kind=artifact.kind,
            label=artifact.label,
            path=artifact.path,
            content_type=artifact.content_type,
            byte_size=artifact.byte_size,
            sha256="mutated-proof",
            created_at=artifact.created_at,
            producer=artifact.producer,
            status=artifact.status,
            head_sha=artifact.head_sha,
            metadata=artifact.metadata,
            artifact_id=artifact.id,
        )

    source_sha = "source-b" if race_case == "source-drift" else "source-a"
    target_sha = "target-b" if race_case == "target-advanced" else "target-a"
    replay_git = SimpleNamespace(
        repo_dir=tmp_path,
        resolve_fresh_merge_source=MagicMock(return_value=ResolvedMergeSourceRef(task.branch)),
        is_merged=MagicMock(return_value=True),
        is_ancestor=MagicMock(return_value=True),
        rev_parse_if_exists=MagicMock(side_effect=lambda ref: source_sha if ref == task.branch else target_sha),
    )
    store.set_merge_unit_state = MagicMock(side_effect=AssertionError("state must not change"))  # type: ignore[method-assign]

    result = _execute_merge_action(
        config,
        store,
        SimpleNamespace(repo_dir=tmp_path),
        task,
        action,
        target_branch="main",
        current_branch="main",
        merge_git=replay_git,
        merge_current_branch="main",
        already_merged_behavior="mark_merged",
        merge_source=MERGE_SOURCE_ADVANCE,
    )

    assert result.rc == 1
    assert result.status == "pending_merge_finalization_proof_stale"
    assert expected_reason in (result.block_reason or "")
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


def test_execute_merge_action_max_cycle_mixed_malformed_findings_fail_closed_without_side_effects(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Malformed capped metadata", "feature/capped-malformed")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    review.completed_at = datetime(2026, 8, 27, 10, 0, tzinfo=UTC)
    store.update(review)
    _set_review_head(store, review, "source-sha")
    action = _max_cycle_merge_action(
        review,
        (_blocker_finding("B1"),),
        review_output,
        reviewed_head_sha="source-sha",
    )
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="source-sha")
    action["blocker_findings"] = (_blocker_finding("B1"), {"id": "B2", "severity": "BLOCKER"})
    git = _merge_executor_git(tmp_path, task.branch)

    def _merge_side_effect(*_args: object, **_kwargs: object) -> _MergeSingleTaskResult:
        side_effect = _kwargs.get("before_irreversible_side_effect")
        assert side_effect is not None
        materialization_error = side_effect()
        assert materialization_error is not None
        return _MergeSingleTaskResult(
            rc=materialization_error.rc,
            status=materialization_error.status,
            block_reason=materialization_error.block_reason,
            created_deferred_blockers=tuple(materialization_error.created_deferred_blockers),
            reused_deferred_blockers=tuple(materialization_error.reused_deferred_blockers),
        )

    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch("gza.cli.git_ops._merge_single_task", side_effect=_merge_side_effect) as merge_single,
        patch("gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks") as materialize,
    ):
        result = _execute_merge_action(
            config,
            store,
            git,
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 1
    assert result.status == "deferred_blocker_materialization_failed"
    assert "malformed blocker findings" in (result.block_reason or "")
    merge_single.assert_not_called()
    materialize.assert_not_called()
    assert [child for child in store.get_based_on_children(task.id) if child.task_type == "implement"] == []
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


def _execute_capped_merge_through_materialization_side_effect(
    *,
    config: Config,
    store: Any,
    git: Any,
    task: Any,
    action: dict[str, object],
) -> _MergeActionResult:
    if action.get("max_cycles_merge_and_defer") is True and action.get("pending_merge_finalization") is not True:
        reviewed_head_sha = action.get("current_review_head_sha")
        if isinstance(reviewed_head_sha, str) and reviewed_head_sha:
            _persist_capped_authorization_verify(
                store,
                config,
                task,
                tmp_path=Path(git.repo_dir),
                head_sha=reviewed_head_sha,
            )

    def _merge_side_effect(*_args: object, **_kwargs: object) -> _MergeSingleTaskResult:
        side_effect = _kwargs.get("before_irreversible_side_effect")
        assert side_effect is not None
        materialization_error = side_effect()
        if materialization_error is not None:
            return _MergeSingleTaskResult(
                rc=materialization_error.rc,
                status=materialization_error.status,
                block_reason=materialization_error.block_reason,
                created_deferred_blockers=tuple(materialization_error.created_deferred_blockers),
                reused_deferred_blockers=tuple(materialization_error.reused_deferred_blockers),
            )
        return _MergeSingleTaskResult(rc=0)

    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch("gza.cli.git_ops._merge_single_task", side_effect=_merge_side_effect),
    ):
        return _execute_merge_action(
            config,
            store,
            git,
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )


def _assert_capped_merge_refused_without_side_effects(
    store: Any,
    unit: Any,
    task: Any,
    result: _MergeActionResult,
) -> None:
    assert result.rc == 1
    assert result.created_deferred_blockers == []
    assert result.reused_deferred_blockers == []
    assert [child for child in store.get_based_on_children(task.id) if child.task_type == "implement"] == []
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


@pytest.mark.parametrize("path", ["nonisolated", "isolated", "already_merged"])
def test_capped_merge_refuses_live_source_that_moved_before_executor_read(
    tmp_path: Path,
    path: str,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    store = make_store(tmp_path)
    task = _completed_merge_task(store, f"Moved source {path}", f"feature/moved-source-{path}")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    _set_review_head(store, review, "planned-sha")
    finding = parse_review_report(review_output).findings[0]
    action = _max_cycle_merge_action(review, (finding,), review_output, reviewed_head_sha="planned-sha")
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="planned-sha")
    git = _merge_executor_git(tmp_path, task.branch)
    git.rev_parse_if_exists = MagicMock(side_effect=lambda ref: "moved-sha" if ref == task.branch else "target-sha")
    git.is_merged.return_value = path == "already_merged"

    if path == "isolated":
        config.main_checkout_isolate = True
        merge_git = MagicMock()
        merge_git.repo_dir = tmp_path / ".gza" / "main-checkout"
        merge_git.rev_parse_if_exists = git.rev_parse_if_exists
        merge_git.is_merged.return_value = False
        staged = _StagedIsolatedMergeAction(
            merge_subject=task,
            merge_unit_id=unit.id,
            merge_branch=task.branch,
            pending_squash_reconcile=None,
            review_task=review,
            followup_findings=(),
            merge_action_metadata=action,
            source_ref=task.branch,
        )
        with (
            patch("gza.cli.git_ops._stage_isolated_merge_action", return_value=staged),
            patch(
                "gza.cli.git_ops._candidate_verify_promotion_proof",
                return_value=SimpleNamespace(
                    exact_match=True,
                    verified_head_sha="same-sha",
                    verified_tree_fingerprint="tree-fp",
                ),
            ),
            patch("gza.cli.git_ops._promote_isolated_merge_to_target_branch") as promote,
            patch("gza.cli.git_ops.determine_next_action", return_value=action),
        ):
            result = _execute_merge_action(
                config,
                store,
                git,
                task,
                action,
                target_branch="main",
                current_branch="main",
                merge_git=merge_git,
                merge_current_branch="main",
                merge_source=MERGE_SOURCE_WATCH,
            )
        promote.assert_not_called()
    else:
        with (
            patch("gza.cli.git_ops._merge_single_task") as merge_single,
            patch("gza.cli.git_ops.determine_next_action", return_value=action),
        ):
            result = _execute_merge_action(
                config,
                store,
                git,
                task,
                action,
                target_branch="main",
                current_branch="main",
                already_merged_behavior="mark_merged" if path == "already_merged" else "error",
                merge_source=MERGE_SOURCE_ADVANCE,
            )
        merge_single.assert_not_called()

    _assert_capped_merge_refused_without_side_effects(store, unit, task, result)
    assert "source no longer matches reviewed head" in (result.block_reason or "")


@pytest.mark.parametrize("path", ["nonisolated", "isolated", "already_merged"])
@pytest.mark.parametrize("bad_metadata", ["missing_head", "disagreeing_head"])
def test_capped_merge_refuses_missing_or_disagreeing_review_head_metadata_before_side_effects(
    tmp_path: Path,
    path: str,
    bad_metadata: str,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, f"Bad capped metadata {path}", f"feature/bad-capped-metadata-{path}")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    finding = parse_review_report(review_output).findings[0]
    action = _max_cycle_merge_action(review, (finding,), review_output)
    if bad_metadata == "missing_head":
        action.pop("current_review_head_sha")
    else:
        action["current_review_head_sha"] = "other-sha"
    git = _merge_executor_git(tmp_path, task.branch)
    git.is_merged.return_value = path == "already_merged"

    if path == "isolated":
        config.main_checkout_isolate = True
        merge_git = MagicMock()
        merge_git.repo_dir = tmp_path / ".gza" / "main-checkout"
        merge_git.rev_parse_if_exists = git.rev_parse_if_exists
        staged = _StagedIsolatedMergeAction(
            merge_subject=task,
            merge_unit_id=unit.id,
            merge_branch=task.branch,
            pending_squash_reconcile=None,
            review_task=review,
            followup_findings=(),
            merge_action_metadata=action,
            source_ref=task.branch,
        )
        with (
            patch("gza.cli.git_ops._stage_isolated_merge_action", return_value=staged),
            patch(
                "gza.cli.git_ops._candidate_verify_promotion_proof",
                return_value=SimpleNamespace(
                    exact_match=True,
                    verified_head_sha="same-sha",
                    verified_tree_fingerprint="tree-fp",
                ),
            ),
            patch("gza.cli.git_ops._promote_isolated_merge_to_target_branch") as promote,
            patch("gza.cli.git_ops._require_fresh_capped_review_lifecycle_authority", return_value=None),
        ):
            result = _execute_merge_action(
                config,
                store,
                git,
                task,
                action,
                target_branch="main",
                current_branch="main",
                merge_git=merge_git,
                merge_current_branch="main",
            )
        promote.assert_not_called()
    else:
        with (
            patch("gza.cli.git_ops._merge_single_task") as merge_single,
            patch("gza.cli.git_ops._require_fresh_capped_review_lifecycle_authority", return_value=None),
        ):
            result = _execute_merge_action(
                config,
                store,
                git,
                task,
                action,
                target_branch="main",
                current_branch="main",
                already_merged_behavior="mark_merged" if path == "already_merged" else "error",
            )
        merge_single.assert_not_called()

    _assert_capped_merge_refused_without_side_effects(store, unit, task, result)


@pytest.mark.parametrize("path", ["nonisolated", "isolated", "already_merged"])
@pytest.mark.parametrize("newer_status", ["failed", "unavailable"])
def test_capped_merge_refuses_newer_non_green_verify_evidence_before_side_effects(
    tmp_path: Path,
    path: str,
    newer_status: str,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, f"Red capped verify {path}", f"feature/red-capped-verify-{path}")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    finding = parse_review_report(review_output).findings[0]
    action = _max_cycle_merge_action(review, (finding,), review_output)
    _persist_capped_authorization_verify(
        store,
        config,
        task,
        tmp_path=tmp_path,
        status="passed",
        captured_at=datetime(2026, 8, 27, 10, 0, tzinfo=UTC),
    )
    _persist_capped_authorization_verify(
        store,
        config,
        task,
        tmp_path=tmp_path,
        status=newer_status,
        captured_at=datetime(2026, 8, 27, 10, 1, tzinfo=UTC),
    )
    git = _merge_executor_git(tmp_path, task.branch)
    git.is_merged.return_value = path == "already_merged"

    if path == "isolated":
        config.main_checkout_isolate = True
        merge_git = MagicMock()
        merge_git.repo_dir = tmp_path / ".gza" / "main-checkout"
        merge_git.rev_parse_if_exists = git.rev_parse_if_exists
        staged = _StagedIsolatedMergeAction(
            merge_subject=task,
            merge_unit_id=unit.id,
            merge_branch=task.branch,
            pending_squash_reconcile=None,
            review_task=review,
            followup_findings=(),
            merge_action_metadata=action,
            source_ref=task.branch,
        )
        with (
            patch("gza.cli.git_ops._stage_isolated_merge_action", return_value=staged),
            patch(
                "gza.cli.git_ops._candidate_verify_promotion_proof",
                return_value=SimpleNamespace(
                    exact_match=True,
                    verified_head_sha="same-sha",
                    verified_tree_fingerprint="tree-fp",
                ),
            ),
            patch("gza.cli.git_ops._promote_isolated_merge_to_target_branch") as promote,
            patch("gza.cli.git_ops._require_fresh_capped_review_lifecycle_authority", return_value=None),
        ):
            result = _execute_merge_action(
                config,
                store,
                git,
                task,
                action,
                target_branch="main",
                current_branch="main",
                merge_git=merge_git,
                merge_current_branch="main",
            )
        promote.assert_not_called()
    else:
        with (
            patch("gza.cli.git_ops._merge_single_task") as merge_single,
            patch("gza.cli.git_ops._require_fresh_capped_review_lifecycle_authority", return_value=None),
        ):
            result = _execute_merge_action(
                config,
                store,
                git,
                task,
                action,
                target_branch="main",
                current_branch="main",
                already_merged_behavior="mark_merged" if path == "already_merged" else "error",
            )
        merge_single.assert_not_called()

    _assert_capped_merge_refused_without_side_effects(store, unit, task, result)
    assert "verify evidence is no longer current and passing" in (result.block_reason or "")


@pytest.mark.parametrize(
    ("race_case", "expected_reason"),
    [
        ("deleted", "disappeared"),
        ("prompt-mutated", "prompt"),
        ("shape-mutated", "depends_on"),
        ("terminally-landed", "cannot be reused"),
        ("duplicated", "Ambiguous capped review blocker task identity"),
    ],
)
def test_execute_merge_action_pending_capped_replay_races_fail_closed_before_state(
    tmp_path: Path,
    race_case: str,
    expected_reason: str,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, f"Capped replay race {race_case}", f"feature/capped-race-{race_case}")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    finding = parse_review_report(review_output).findings[0]
    proven_child, created_now = create_or_reuse_capped_review_blocker_task(
        store,
        config=config,
        review_task=review,
        impl_task=task,
        finding=finding,
        persisted_review_output=review_output,
        active_scope_tags=("release",),
        trigger_source=MERGE_SOURCE_ADVANCE,
    )
    assert created_now is True
    action = _max_cycle_merge_action(review, (finding,), review_output)
    action["pending_merge_finalization"] = True
    action["proven_deferred_blocker_tasks"] = (proven_child,)

    if race_case == "deleted":
        assert proven_child.id is not None
        assert store.delete(proven_child.id) is True
    elif race_case == "prompt-mutated":
        proven_child.prompt += "\nmutated"
        store.update(proven_child)
    elif race_case == "shape-mutated":
        proven_child.depends_on = None
        store.update(proven_child)
    elif race_case == "terminally-landed":
        proven_child.merge_status = "merged"
        store.update(proven_child)
    elif race_case == "duplicated":
        store.add(
            proven_child.prompt,
            task_type="implement",
            based_on=task.id,
            depends_on=task.id,
            review_scope=proven_child.review_scope,
            create_pr=True,
            urgent=True,
        )

    result = _execute_capped_merge_through_materialization_side_effect(
        config=config,
        store=store,
        git=_merge_executor_git(tmp_path, task.branch),
        task=task,
        action=action,
    )

    assert result.rc == 1
    assert result.status == "deferred_blocker_materialization_failed"
    assert expected_reason in (result.block_reason or "")
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


@pytest.mark.parametrize(
    ("mutate_kwargs", "expected_reason"),
    [
        ({"prompt_review": "stale review text\n"}, "prompt"),
        ({"depends_on": None}, "depends_on"),
        ({"depends_on": "gza-999999"}, "depends_on"),
        ({"urgent": False}, "urgent"),
        ({"create_pr": False}, "create_pr"),
    ],
)
def test_execute_merge_action_max_cycle_rejects_malformed_replay_child_before_state_persistence(
    tmp_path: Path,
    mutate_kwargs: dict[str, object],
    expected_reason: str,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Malformed capped replay", "feature/capped-replay-shape")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    assert review.id is not None
    assert task.id is not None
    finding = _blocker_finding("B1")
    prompt_review = str(mutate_kwargs.get("prompt_review", review_output))
    depends_on_value = mutate_kwargs.get("depends_on", task.id)
    assert depends_on_value is None or isinstance(depends_on_value, str)
    existing = store.add(
        build_capped_review_blocker_prompt(review.id, task.id, finding, prompt_review),
        task_type="implement",
        based_on=task.id,
        depends_on=depends_on_value,
        create_pr=bool(mutate_kwargs.get("create_pr", True)),
        urgent=bool(mutate_kwargs.get("urgent", True)),
    )
    assert existing.id is not None
    git = _merge_executor_git(tmp_path, task.branch)

    result = _execute_capped_merge_through_materialization_side_effect(
        config=config,
        store=store,
        git=git,
        task=task,
        action=_max_cycle_merge_action(review, (finding,), review_output),
    )

    assert result.rc == 1
    assert result.status == "deferred_blocker_materialization_failed"
    assert expected_reason in (result.block_reason or "")
    assert result.created_deferred_blockers == []
    assert result.reused_deferred_blockers == []
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


def test_execute_merge_action_max_cycle_rejects_duplicate_replay_children_before_state_persistence(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Duplicate capped replay", "feature/capped-replay-duplicate")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    assert review.id is not None
    assert task.id is not None
    finding = _blocker_finding("B1")
    prompt = build_capped_review_blocker_prompt(review.id, task.id, finding, review_output)
    for _ in range(2):
        store.add(
            prompt,
            task_type="implement",
            based_on=task.id,
            depends_on=task.id,
            create_pr=True,
            urgent=True,
        )
    git = _merge_executor_git(tmp_path, task.branch)

    result = _execute_capped_merge_through_materialization_side_effect(
        config=config,
        store=store,
        git=git,
        task=task,
        action=_max_cycle_merge_action(review, (finding,), review_output),
    )

    assert result.rc == 1
    assert result.status == "deferred_blocker_materialization_failed"
    assert "Ambiguous capped review blocker task identity" in (result.block_reason or "")
    assert result.created_deferred_blockers == []
    assert result.reused_deferred_blockers == []
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


@pytest.mark.parametrize(
    ("action", "default_source", "expected_source"),
    [
        ({"type": "merge", "description": "Merge"}, MERGE_SOURCE_ADVANCE, MERGE_SOURCE_ADVANCE),
        ({"type": "merge", "description": "Merge"}, MERGE_SOURCE_WATCH, MERGE_SOURCE_WATCH),
        (
            {"type": "merge", "description": "Merge and defer", "max_cycles_merge_and_defer": True},
            MERGE_SOURCE_WATCH,
            MERGE_SOURCE_MAX_CYCLES_DEFERRED,
        ),
    ],
)
def test_execute_merge_action_mark_merged_uses_action_provenance_selector(
    tmp_path: Path,
    action: dict[str, object],
    default_source: str,
    expected_source: str,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Completed implementation", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = f"feature/{task.id}-already-merged"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    if action.get("max_cycles_merge_and_defer") is True:
        review_output = _capped_review_output("B1")
        review = _completed_review(store, task, review_output)
        _set_review_head(store, review, "source-sha")
        action = {
            **action,
            **_max_cycle_merge_action(
                review,
                (_blocker_finding("B1"),),
                review_output,
                reviewed_head_sha="source-sha",
            ),
        }
        _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="source-sha")

    def rev_parse_if_exists(ref: str) -> str | None:
        if ref == "main":
            return "target-sha"
        if ref == task.branch:
            return "source-sha"
        return None

    git = SimpleNamespace(repo_dir=tmp_path, rev_parse_if_exists=rev_parse_if_exists)
    merge_git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=True),
        is_ancestor=MagicMock(return_value=True),
        resolve_fresh_merge_source=MagicMock(return_value=ResolvedMergeSourceRef(task.branch)),
        rev_parse_if_exists=rev_parse_if_exists,
    )

    with patch("gza.cli.git_ops.determine_next_action", return_value=action):
        result = _execute_merge_action(
            config,
            store,
            git,
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
            already_merged_behavior="mark_merged",
            merge_source=default_source,
        )

    assert result.rc == 0
    assert result.status == "already_merged"
    if action.get("max_cycles_merge_and_defer") is True:
        assert len(result.created_deferred_blockers) == 1
        assert result.reused_deferred_blockers == []
    else:
        assert result.created_deferred_blockers == []
        assert result.reused_deferred_blockers == []
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"
    assert refreshed_unit.merge_source == expected_source


def test_execute_merge_action_max_cycle_materializes_blockers_before_non_isolated_merge(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Completed capped implementation", "feature/capped-order")
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    finding = parse_review_report(review_output).findings[0]
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path)
    git = _merge_executor_git(tmp_path, task.branch)
    order: list[str] = []

    def _merge_side_effect(*_args: object, **_kwargs: object) -> _MergeSingleTaskResult:
        order.append("preflight")
        side_effect = _kwargs.get("before_irreversible_side_effect")
        assert side_effect is not None
        materialization_error = side_effect()
        assert materialization_error is None
        order.append("merge")
        return _MergeSingleTaskResult(rc=0)

    def _materialize_side_effect(*args: object, **kwargs: object) -> tuple[list[Any], list[Any]]:
        order.append("defer")
        return real_capped_creator(*args, **kwargs)

    from gza.cli import git_ops as git_ops_module

    real_capped_creator = git_ops_module._create_or_reuse_capped_review_blocker_tasks
    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch("gza.cli.git_ops._merge_single_task", side_effect=_merge_side_effect) as merge_single,
        patch(
            "gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks",
            side_effect=_materialize_side_effect,
        ) as materialize,
    ):
        result = _execute_merge_action(
            config,
            store,
            git,
            task,
            _max_cycle_merge_action(review, (finding,), review_output),
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 0
    assert order == ["preflight", "defer", "merge"]
    materialize.assert_called_once()
    merge_single.assert_called_once()
    assert [task.id for task in result.created_deferred_blockers] == [
        child.id for child in store.get_based_on_children(task.id) if child.task_type == "implement"
    ]


@pytest.mark.parametrize(
    "race_case",
    [
        "ordinary-to-capped",
        "capped-to-followups",
        "capped-b1-to-b2",
        "followup-f1-to-f2",
    ],
)
def test_execute_merge_action_materializes_fresh_inner_replan_identity_before_merge(
    tmp_path: Path,
    race_case: str,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, f"Fresh replan {race_case}", f"feature/fresh-replan-{race_case}")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    stale_capped_output = _capped_review_output("B1")
    stale_review = _completed_review(store, task, stale_capped_output)
    fresh_capped_output = _capped_review_output("B2")
    fresh_capped_review = _completed_review(store, task, fresh_capped_output)
    stale_followup_review = _completed_review(store, task, "## Review\n\nVerdict: APPROVED_WITH_FOLLOWUPS\n")
    fresh_followup_review = _completed_review(store, task, "## Review\n\nVerdict: APPROVED_WITH_FOLLOWUPS\n")
    stale_review_time = datetime(2026, 8, 27, 10, 0, tzinfo=UTC)
    stale_review.completed_at = stale_review_time
    stale_followup_review.completed_at = stale_review_time
    if race_case in {"ordinary-to-capped", "capped-b1-to-b2"}:
        fresh_followup_review.completed_at = datetime(2026, 8, 27, 10, 1, tzinfo=UTC)
        fresh_capped_review.completed_at = datetime(2026, 8, 27, 10, 2, tzinfo=UTC)
    else:
        fresh_capped_review.completed_at = datetime(2026, 8, 27, 10, 1, tzinfo=UTC)
        fresh_followup_review.completed_at = datetime(2026, 8, 27, 10, 2, tzinfo=UTC)
    store.update(stale_review)
    store.update(fresh_capped_review)
    store.update(stale_followup_review)
    store.update(fresh_followup_review)
    stale_capped_finding = parse_review_report(stale_capped_output).findings[0]
    fresh_capped_finding = parse_review_report(fresh_capped_output).findings[0]
    stale_followup = _followup_finding("F1")
    fresh_followup = _followup_finding("F2")

    if race_case == "ordinary-to-capped":
        outer_action: dict[str, object] = {"type": "merge", "description": "Merge"}
        fresh_action: dict[str, object] = _max_cycle_merge_action(
            fresh_capped_review,
            (fresh_capped_finding,),
            fresh_capped_output,
        )
    elif race_case == "capped-to-followups":
        outer_action = _max_cycle_merge_action(stale_review, (stale_capped_finding,), stale_capped_output)
        fresh_action = {
            "type": "merge_with_followups",
            "description": "Merge with fresh follow-ups",
            "review_task": fresh_followup_review,
            "followup_findings": (fresh_followup,),
        }
    elif race_case == "capped-b1-to-b2":
        outer_action = _max_cycle_merge_action(stale_review, (stale_capped_finding,), stale_capped_output)
        fresh_action = _max_cycle_merge_action(fresh_capped_review, (fresh_capped_finding,), fresh_capped_output)
    else:
        outer_action = {
            "type": "merge_with_followups",
            "description": "Merge with stale follow-ups",
            "review_task": stale_followup_review,
            "followup_findings": (stale_followup,),
        }
        fresh_action = {
            "type": "merge_with_followups",
            "description": "Merge with fresh follow-ups",
            "review_task": fresh_followup_review,
            "followup_findings": (fresh_followup,),
        }
    if fresh_action.get("max_cycles_merge_and_defer") is True:
        _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path)

    order: list[str] = []

    def _merge_side_effect(*_args: object, **kwargs: object) -> _MergeSingleTaskResult:
        side_effect = kwargs.get("before_irreversible_side_effect")
        assert side_effect is not None
        materialization_error = side_effect(fresh_action)
        assert materialization_error is None
        order.append("materialized")
        order.append("git.merge")
        return _MergeSingleTaskResult(rc=0, authorized_merge_action=dict(fresh_action))

    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch("gza.cli.git_ops._merge_single_task", side_effect=_merge_side_effect),
    ):
        result = _execute_merge_action(
            config,
            store,
            _merge_executor_git(tmp_path, task.branch),
            task,
            outer_action,
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    if race_case in {"capped-to-followups", "capped-b1-to-b2"}:
        assert result.rc == 1
        assert result.status == "pre_merge_proof_persistence_failed"
        assert (
            "latest applicable completed review" in (result.block_reason or "")
            or "lifecycle authority" in (result.block_reason or "")
        )
        refreshed_unit = store.get_merge_unit(unit.id)
        assert refreshed_unit is not None
        assert refreshed_unit.state == "unmerged"
        assert refreshed_unit.merge_source is None
        assert [child for child in store.get_based_on_children(task.id) if child.task_type == "implement"] == []
        return

    assert result.rc == 0
    assert order == ["materialized", "git.merge"]
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"
    if race_case in {"ordinary-to-capped", "capped-b1-to-b2"}:
        assert [child.id for child in store.get_based_on_children(task.id) if child.task_type == "implement"] == [
            result.created_deferred_blockers[0].id
        ]
        assert f"Capped review blocker {fresh_capped_finding.id}" in result.created_deferred_blockers[0].prompt
        assert stale_capped_output not in result.created_deferred_blockers[0].prompt
        assert refreshed_unit.merge_source == MERGE_SOURCE_MAX_CYCLES_DEFERRED
    else:
        assert [child.id for child in store.get_based_on_children(fresh_followup_review.id) if child.task_type == "implement"] == [
            result.created_followups[0].id
        ]
        assert f"Follow-up {fresh_followup.id}" in result.created_followups[0].prompt
        assert store.get_based_on_children(stale_followup_review.id) == []
        assert refreshed_unit.merge_source == MERGE_SOURCE_ADVANCE


@pytest.mark.parametrize("executor_path", ["non-isolated", "isolated-stage", "already-merged"])
@pytest.mark.parametrize(
    "action_case",
    [
        "supplied-id-differs",
        "missing-latest-review-task-id",
        "mismatched-latest-review-task-id",
        "missing-latest-review-completed-at",
        "mismatched-latest-review-completed-at",
        "conflicting-review-output-alias",
        "omits-parsed-blocker",
        "adds-extra-blocker",
        "missing-review-row",
        "incomplete-review-row",
        "mixed-followup-and-capped",
        "missing-deferred-blocker-ids",
        "malformed-deferred-blocker-ids",
        "conflicting-deferred-blocker-ids",
        "missing-audit-reason",
        "wrong-audit-reason",
        "missing-audit-policy",
        "wrong-audit-policy",
        "boolean-completed-review-cycles",
        "non-integer-max-review-cycles",
        "below-cap-review-cycles",
    ],
)
def test_max_cycle_action_validator_rejects_ambiguous_metadata_before_mutation(
    tmp_path: Path,
    executor_path: str,
    action_case: str,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(
        store,
        f"Invalid capped {executor_path} {action_case}",
        f"feature/invalid-capped-{executor_path}-{action_case}",
    )
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    action: dict[str, object] = _max_cycle_merge_action(
        review,
        tuple(parse_review_report(review_output).findings),
        review_output,
    )
    if action_case == "supplied-id-differs":
        other_output = _capped_review_output("B2")
        action["blocker_findings"] = tuple(parse_review_report(other_output).findings)
    elif action_case == "missing-latest-review-task-id":
        action.pop("latest_review_task_id")
    elif action_case == "mismatched-latest-review-task-id":
        action["latest_review_task_id"] = f"{config.project_prefix}-999999"
    elif action_case == "missing-latest-review-completed-at":
        action.pop("latest_review_completed_at")
    elif action_case == "mismatched-latest-review-completed-at":
        action["latest_review_completed_at"] = "2026-08-27T00:00:00+00:00"
    elif action_case == "conflicting-review-output-alias":
        action["review_output"] = _capped_review_output("B2")
    elif action_case == "omits-parsed-blocker":
        review_output = _capped_review_output("B1", "B2")
        review.output_content = review_output
        store.update(review)
        action = _max_cycle_merge_action(review, (parse_review_report(review_output).findings[0],), review_output)
    elif action_case == "adds-extra-blocker":
        extra_output = _capped_review_output("B1", "B2")
        action["blocker_findings"] = tuple(parse_review_report(extra_output).findings)
    elif action_case == "missing-review-row":
        assert review.id is not None
        assert store.delete(review.id) is True
    elif action_case == "incomplete-review-row":
        review.status = "pending"
        store.update(review)
    elif action_case == "mixed-followup-and-capped":
        action["followup_findings"] = (_followup_finding("F1"),)
    elif action_case == "missing-deferred-blocker-ids":
        action.pop("deferred_blocker_ids")
    elif action_case == "malformed-deferred-blocker-ids":
        action["deferred_blocker_ids"] = ("B1", 7)
    elif action_case == "conflicting-deferred-blocker-ids":
        action["deferred_blocker_ids"] = ("B9",)
    elif action_case == "missing-audit-reason":
        cast(dict[str, object], action["max_cycles_audit"]).pop("reason")
    elif action_case == "wrong-audit-reason":
        cast(dict[str, object], action["max_cycles_audit"])["reason"] = "other"
    elif action_case == "missing-audit-policy":
        cast(dict[str, object], action["max_cycles_audit"]).pop("policy")
    elif action_case == "wrong-audit-policy":
        cast(dict[str, object], action["max_cycles_audit"])["policy"] = "park"
    elif action_case == "boolean-completed-review-cycles":
        cast(dict[str, object], action["max_cycles_audit"])["completed_review_cycles"] = True
    elif action_case == "non-integer-max-review-cycles":
        cast(dict[str, object], action["max_cycles_audit"])["max_review_cycles"] = "3"
    elif action_case == "below-cap-review-cycles":
        cast(dict[str, object], action["max_cycles_audit"])["completed_review_cycles"] = 2
        cast(dict[str, object], action["max_cycles_audit"])["max_review_cycles"] = 3

    git = _merge_executor_git(tmp_path, task.branch)
    git.is_merged.return_value = True
    merge_git = _merge_executor_git(tmp_path, task.branch)
    merge_git.is_merged.return_value = True
    store.set_merge_unit_state = MagicMock(side_effect=AssertionError("state must not change"))  # type: ignore[method-assign]

    with (
        patch("gza.cli.git_ops._build_auto_merge_args", side_effect=AssertionError("merge args must not build")),
        patch("gza.cli.git_ops._merge_single_task", side_effect=AssertionError("merge must not run")),
        patch("gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks", side_effect=AssertionError("debt not created")),
    ):
        if executor_path == "isolated-stage":
            result = _stage_isolated_merge_action(
                config,
                store,
                git,
                task,
                action,
                target_branch="main",
                current_branch="main",
                merge_git=merge_git,
                merge_current_branch="main",
                already_merged_behavior="mark_merged",
                merge_source=MERGE_SOURCE_WATCH,
            )
        else:
            result = _execute_merge_action(
                config,
                store,
                git,
                task,
                action,
                target_branch="main",
                current_branch="main",
                merge_git=merge_git if executor_path == "already-merged" else None,
                merge_current_branch="main" if executor_path == "already-merged" else None,
                already_merged_behavior="mark_merged" if executor_path == "already-merged" else "error",
                merge_source=MERGE_SOURCE_ADVANCE,
            )

    assert isinstance(result, _MergeActionResult)
    assert result.rc == 1
    assert result.status == "deferred_blocker_materialization_failed"
    assert result.created_deferred_blockers == []
    assert result.reused_deferred_blockers == []
    merge_git.is_merged.assert_not_called()
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None
    assert [child for child in store.get_based_on_children(task.id) if child.task_type == "implement"] == []


@pytest.mark.parametrize(
    "action_case",
    [
        "missing-deferred-blocker-ids",
        "malformed-deferred-blocker-ids",
        "conflicting-deferred-blocker-ids",
        "wrong-audit-reason",
        "wrong-audit-policy",
        "boolean-completed-review-cycles",
        "below-cap-review-cycles",
    ],
)
def test_pending_max_cycle_finalization_replay_rejects_invalid_authority_metadata_before_mutation(
    tmp_path: Path,
    action_case: str,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(
        store,
        f"Pending invalid capped {action_case}",
        f"feature/pending-invalid-capped-{action_case}",
    )
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    action: dict[str, object] = _max_cycle_merge_action(
        review,
        tuple(parse_review_report(review_output).findings),
        review_output,
    )
    action["pending_merge_finalization"] = True
    if action_case == "missing-deferred-blocker-ids":
        action.pop("deferred_blocker_ids")
    elif action_case == "malformed-deferred-blocker-ids":
        action["deferred_blocker_ids"] = "B1"
    elif action_case == "conflicting-deferred-blocker-ids":
        action["deferred_blocker_ids"] = ("B2",)
    elif action_case == "wrong-audit-reason":
        cast(dict[str, object], action["max_cycles_audit"])["reason"] = "not-review-max-cycles"
    elif action_case == "wrong-audit-policy":
        cast(dict[str, object], action["max_cycles_audit"])["policy"] = "park"
    elif action_case == "boolean-completed-review-cycles":
        cast(dict[str, object], action["max_cycles_audit"])["completed_review_cycles"] = False
    elif action_case == "below-cap-review-cycles":
        cast(dict[str, object], action["max_cycles_audit"])["completed_review_cycles"] = 1
        cast(dict[str, object], action["max_cycles_audit"])["max_review_cycles"] = 2

    store.set_merge_unit_state = MagicMock(side_effect=AssertionError("state must not change"))  # type: ignore[method-assign]
    with (
        patch("gza.cli.git_ops._build_auto_merge_args", side_effect=AssertionError("merge args must not build")),
        patch("gza.cli.git_ops._merge_single_task", side_effect=AssertionError("merge must not run")),
        patch(
            "gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks",
            side_effect=AssertionError("replay must not create deferred blockers"),
        ),
    ):
        result = _execute_merge_action(
            config,
            store,
            _merge_executor_git(tmp_path, str(task.branch)),
            task,
            action,
            target_branch="main",
            current_branch="main",
            already_merged_behavior="mark_merged",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 1
    assert result.status == "deferred_blocker_materialization_failed"
    assert result.created_deferred_blockers == []
    assert result.reused_deferred_blockers == []
    assert [child for child in store.get_based_on_children(task.id) if child.task_type == "implement"] == []
    assert not store.list_artifacts(task.id, kind="merge_finalization_prepared_attempt")
    assert not store.list_artifacts(task.id, kind="merge_finalization_attempt_proof")
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


@pytest.mark.parametrize(
    "executor_path",
    [
        "non-isolated",
        "already-merged",
        "isolated-single",
        "watch-batch",
    ],
)
@pytest.mark.parametrize(
    "fresh_review_mode",
    ["plain_full", "spec_coherence", "spec_coherence_needs_discussion", "spec_coherence_unknown"],
)
def test_max_cycle_stale_latest_same_head_review_blocks_side_effects_before_materialization(
    tmp_path: Path,
    executor_path: str,
    fresh_review_mode: str,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(
        store,
        f"Stale latest review {executor_path} {fresh_review_mode}",
        f"feature/stale-latest-review-{executor_path}-{fresh_review_mode}",
    )
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    stale_output = _capped_review_output("B1")
    stale_review = _completed_review(store, task, stale_output)
    assert stale_review.completed_at is not None
    stale_review.completed_at = datetime(2026, 8, 27, 10, 0, tzinfo=UTC)
    stale_review.review_verify_head_sha = "same-sha"
    store.update(stale_review)
    action = _max_cycle_merge_action(
        stale_review,
        (parse_review_report(stale_output).findings[0],),
        stale_output,
        reviewed_head_sha="same-sha",
    )
    fresh_output = _capped_review_output("B2")
    if fresh_review_mode.startswith("spec_coherence"):
        if fresh_review_mode == "spec_coherence_needs_discussion":
            fresh_output = _capped_review_output("B2", verdict="NEEDS_DISCUSSION")
        elif fresh_review_mode == "spec_coherence_unknown":
            fresh_output = "## Review\n\nVerdict: BANANA\n\n## Notes\n\nUnparseable for lifecycle authority.\n"
        _completed_spec_coherence_review(
            store,
            task,
            fresh_output,
            head_sha="same-sha",
            completed_at=datetime(2026, 8, 27, 10, 5, tzinfo=UTC),
        )
    else:
        fresh_review = _completed_review(store, task, fresh_output)
        fresh_review.completed_at = datetime(2026, 8, 27, 10, 5, tzinfo=UTC)
        fresh_review.review_verify_head_sha = "same-sha"
        store.update(fresh_review)

    store.set_merge_unit_state = MagicMock(side_effect=AssertionError("merge state must not change"))  # type: ignore[method-assign]
    materialize_patch = patch(
        "gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks",
        side_effect=AssertionError("stale review must not create deferred blocker tasks"),
    )

    if executor_path == "non-isolated":
        git = _merge_executor_git(tmp_path, str(task.branch))

        def _merge_side_effect(*_args: object, **kwargs: object) -> _MergeSingleTaskResult:
            side_effect = kwargs.get("before_irreversible_side_effect")
            assert side_effect is not None
            refusal = side_effect()
            assert refusal is not None
            return _MergeSingleTaskResult(
                rc=refusal.rc,
                status=refusal.status,
                block_reason=refusal.block_reason,
                created_deferred_blockers=tuple(refusal.created_deferred_blockers),
                reused_deferred_blockers=tuple(refusal.reused_deferred_blockers),
            )

        with (
            patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
            patch("gza.cli.git_ops._merge_single_task", side_effect=_merge_side_effect),
            materialize_patch,
        ):
            result = _execute_merge_action(
                config,
                store,
                git,
                task,
                action,
                target_branch="main",
                current_branch="main",
                merge_source=MERGE_SOURCE_ADVANCE,
        )
        assert result.rc == 1
        assert result.status == "pre_merge_proof_persistence_failed"
        assert (
            "latest applicable completed review" in (result.block_reason or "")
            or "lifecycle authority" in (result.block_reason or "")
        )
    elif executor_path == "already-merged":
        merge_git = _merge_executor_git(tmp_path, str(task.branch))
        merge_git.is_merged.return_value = True
        with materialize_patch:
            result = _execute_merge_action(
                config,
                store,
                _merge_executor_git(tmp_path, str(task.branch)),
                task,
                action,
                target_branch="main",
                current_branch="main",
                merge_git=merge_git,
                merge_current_branch="main",
                already_merged_behavior="mark_merged",
                merge_source=MERGE_SOURCE_ADVANCE,
            )
        assert result.rc == 1
        assert result.status == "pre_merge_proof_persistence_failed"
    else:
        staged = _StagedIsolatedMergeAction(
            merge_subject=task,
            merge_unit_id=unit.id,
            merge_branch=task.branch,
            source_ref=task.branch,
            source_ref_sha="same-sha",
            pending_squash_reconcile=None,
            review_task=None,
            followup_findings=(),
            created_investigation_task_ids=(),
            reused_investigation_task_ids=(),
            merge_action_metadata=action,
        )
        live_git = _merge_executor_git(tmp_path, str(task.branch))
        staged_git = _merge_executor_git(tmp_path, str(task.branch))
        with materialize_patch:
            with pytest.raises(Exception, match="latest applicable completed review|lifecycle authority"):
                _authorize_staged_merge_finalization_before_materialization(
                    store,
                    config=config,
                    live_git=live_git,
                    staged_git=staged_git,
                    staged=staged,
                    target_branch="main",
                )

    assert [child for child in store.get_based_on_children(task.id) if child.task_type == "implement"] == []
    assert len(store.list_artifacts(task.id, kind="merge_finalization_attempt_proof")) == 0
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


@pytest.mark.parametrize("newer_review_location", ["merge-unit-sibling", "automatic-review-retry"])
def test_max_cycle_canonical_review_evidence_newer_same_head_blocks_stale_action(
    tmp_path: Path,
    newer_review_location: str,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(
        store,
        f"Canonical stale review {newer_review_location}",
        f"feature/canonical-stale-{newer_review_location}",
    )
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    stale_output = _capped_review_output("B1")
    stale_review = _completed_review(store, task, stale_output)
    stale_review.completed_at = datetime(2026, 8, 27, 10, 0, tzinfo=UTC)
    stale_review.review_verify_head_sha = "same-sha"
    store.update(stale_review)
    action = _max_cycle_merge_action(
        stale_review,
        (parse_review_report(stale_output).findings[0],),
        stale_output,
        reviewed_head_sha="same-sha",
    )

    fresh_output = _capped_review_output("B2")
    if newer_review_location == "merge-unit-sibling":
        sibling = _completed_merge_task(store, "Sibling implementation", str(task.branch))
        assert sibling.id is not None
        store.attach_task_to_merge_unit(sibling.id, unit.id, "contributor")
        fresh_review = _completed_review(store, sibling, fresh_output)
        assert fresh_review.id is not None
        store.attach_task_to_merge_unit(fresh_review.id, unit.id, "review")
    else:
        fresh_review = store.add("Retry review", task_type="review", based_on=stale_review.id)
        assert fresh_review.id is not None
        fresh_review.recovery_origin = "retry"
        fresh_review.status = "completed"
        fresh_review.output_content = fresh_output
        fresh_review.review_verify_branch = task.branch
        fresh_review.review_verify_head_sha = "same-sha"
    fresh_review.completed_at = datetime(2026, 8, 27, 10, 5, tzinfo=UTC)
    store.update(fresh_review)
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="same-sha")

    with (
        patch("gza.cli.git_ops._build_auto_merge_args", side_effect=AssertionError("merge args must not build")),
        patch("gza.cli.git_ops._merge_single_task", side_effect=AssertionError("merge must not run")),
        patch(
            "gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks",
            side_effect=AssertionError("stale canonical review must not create deferred blockers"),
        ),
    ):
        result = _execute_merge_action(
            config,
            store,
            _merge_executor_git(tmp_path, str(task.branch)),
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 1
    assert result.status == "pre_merge_proof_persistence_failed"
    assert (
        "latest applicable completed review" in (result.block_reason or "")
        or "lifecycle authority" in (result.block_reason or "")
    )
    assert [child for child in store.get_based_on_children(task.id) if child.task_type == "implement"] == []
    assert not store.list_artifacts(task.id, kind="merge_finalization_prepared_attempt")
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


@pytest.mark.parametrize("executor_path", ["non-isolated", "already-merged", "isolated-single"])
@pytest.mark.parametrize("newer_head_sha", [None, "different-sha"])
def test_max_cycle_newer_ordinary_review_with_ambiguous_head_blocks_stale_action(
    tmp_path: Path,
    executor_path: str,
    newer_head_sha: str | None,
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    if executor_path == "isolated-single":
        config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\n")
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    store = make_store(tmp_path)
    task = _completed_merge_task(
        store,
        f"Ambiguous newer review {executor_path} {newer_head_sha or 'missing'}",
        f"feature/ambiguous-newer-review-{executor_path}-{newer_head_sha or 'missing'}",
    )
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    review.completed_at = datetime(2026, 8, 27, 10, 0, tzinfo=UTC)
    review.review_verify_head_sha = "source-sha"
    store.update(review)
    newer_output = _capped_review_output("B2")
    newer_review = _completed_review(store, task, newer_output)
    newer_review.completed_at = datetime(2026, 8, 27, 10, 5, tzinfo=UTC)
    newer_review.review_verify_head_sha = newer_head_sha
    store.update(newer_review)
    action = _max_cycle_merge_action(
        review,
        (parse_review_report(review_output).findings[0],),
        review_output,
        reviewed_head_sha="source-sha",
    )
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="source-sha")
    live_git = _merge_executor_git(tmp_path, str(task.branch))
    live_git.is_merged.return_value = True

    with (
        patch("gza.cli.git_ops._build_auto_merge_args", side_effect=AssertionError("merge args must not build")),
        patch("gza.cli.git_ops._merge_single_task", side_effect=AssertionError("merge must not run")),
        patch(
            "gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks",
            side_effect=AssertionError("ambiguous newer review must not create deferred blockers"),
        ),
    ):
        if executor_path == "already-merged":
            result = _execute_merge_action(
                config,
                store,
                live_git,
                task,
                action,
                target_branch="main",
                current_branch="main",
                merge_git=_merge_executor_git(tmp_path, str(task.branch)),
                merge_current_branch="main",
                already_merged_behavior="mark_merged",
                merge_source=MERGE_SOURCE_ADVANCE,
            )
        else:
            isolated_git = _merge_executor_git(tmp_path, str(task.branch))
            isolated_git.repo_dir = config.main_checkout_integration_path
            result = _execute_merge_action(
                config,
                store,
                _merge_executor_git(tmp_path, str(task.branch)),
                task,
                action,
                target_branch="main",
                current_branch="main",
                merge_git=isolated_git if executor_path == "isolated-single" else None,
                merge_current_branch="main" if executor_path == "isolated-single" else None,
                merge_source=MERGE_SOURCE_ADVANCE,
            )

    assert result.rc == 1
    assert result.status == "pre_merge_proof_persistence_failed"
    assert (
        "ambiguous reviewed head" in (result.block_reason or "")
        or "lifecycle authority" in (result.block_reason or "")
    )
    assert result.created_deferred_blockers == []
    assert [child for child in store.get_based_on_children(task.id) if child.task_type == "implement"] == []
    assert not store.list_artifacts(task.id, kind="merge_finalization_prepared_attempt")
    assert not store.list_artifacts(task.id, kind="merge_finalization_attempt_proof")
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


@pytest.mark.parametrize("executor_path", ["non-isolated", "already-merged", "isolated-single"])
@pytest.mark.parametrize(
    "race",
    [
        "review-content",
        "newer-review",
        "spec-coherence-review",
        "spec-coherence-needs-discussion",
        "spec-coherence-unknown",
        "red-verify",
    ],
)
def test_max_cycle_post_materialization_authority_race_preserves_debt_without_merge_state(
    tmp_path: Path,
    executor_path: str,
    race: str,
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    if executor_path == "isolated-single":
        config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\n")
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    store = make_store(tmp_path)
    task = _completed_merge_task(
        store,
        f"Post materialization race {executor_path} {race}",
        f"feature/post-materialization-race-{executor_path}-{race}",
    )
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    review.completed_at = datetime(2026, 8, 27, 10, 0, tzinfo=UTC)
    store.update(review)
    _set_review_head(store, review, "source-sha")
    finding = parse_review_report(review_output).findings[0]
    action = _max_cycle_merge_action(review, (finding,), review_output, reviewed_head_sha="source-sha")
    _persist_capped_authorization_verify(
        store,
        config,
        task,
        tmp_path=tmp_path,
        head_sha="source-sha",
        captured_at=datetime(2026, 8, 27, 10, 0, tzinfo=UTC),
    )

    from gza.cli import git_ops as git_ops_module

    real_capped_creator = git_ops_module._create_or_reuse_capped_review_blocker_tasks

    def materialize_then_stale(*args: object, **kwargs: object) -> tuple[list[DbTask], list[DbTask]]:
        created, reused = real_capped_creator(*args, **kwargs)
        if race == "review-content":
            review.output_content = _capped_review_output("B2")
            store.update(review)
        elif race == "newer-review":
            newer_output = _capped_review_output("B2")
            newer = _completed_review(store, task, newer_output)
            newer.completed_at = datetime(2026, 8, 27, 10, 5, tzinfo=UTC)
            newer.review_verify_head_sha = "source-sha"
            store.update(newer)
        elif race.startswith("spec-coherence"):
            spec_output = _capped_review_output("B2")
            if race == "spec-coherence-needs-discussion":
                spec_output = _capped_review_output("B2", verdict="NEEDS_DISCUSSION")
            elif race == "spec-coherence-unknown":
                spec_output = "## Review\n\nVerdict: BANANA\n\n## Notes\n\nUnparseable for lifecycle authority.\n"
            _completed_spec_coherence_review(
                store,
                task,
                spec_output,
                head_sha="source-sha",
                completed_at=datetime(2026, 8, 27, 10, 5, tzinfo=UTC),
            )
        else:
            _persist_capped_authorization_verify(
                store,
                config,
                task,
                tmp_path=tmp_path,
                status="failed",
                head_sha="source-sha",
                captured_at=datetime(2026, 8, 27, 10, 5, tzinfo=UTC),
            )
        return created, reused

    def merge_side_effect(*_args: object, **kwargs: object) -> _MergeSingleTaskResult:
        side_effect = kwargs.get("before_irreversible_side_effect")
        assert side_effect is not None
        refusal = side_effect()
        assert refusal is not None
        return _MergeSingleTaskResult(
            rc=refusal.rc,
            status=refusal.status,
            block_reason=refusal.block_reason,
            created_deferred_blockers=tuple(refusal.created_deferred_blockers),
            reused_deferred_blockers=tuple(refusal.reused_deferred_blockers),
        )

    live_git = _proofing_merge_executor_git(tmp_path, str(task.branch))
    isolated_git = MagicMock()
    isolated_git.repo_dir = config.main_checkout_integration_path
    isolated_git.rev_parse.return_value = "candidate-head"
    isolated_git.rev_parse_if_exists.side_effect = lambda ref: (
        "source-sha" if ref == task.branch else "candidate-head" if ref == "main" else None
    )

    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch("gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks", side_effect=materialize_then_stale),
        patch(
            "gza.cli.git_ops.check_candidate_integration_verify",
            return_value=SimpleNamespace(
                classification="pass",
                evidence=SimpleNamespace(
                    verify_status="passed",
                    head_sha="candidate-head",
                    tree_fingerprint="fp-candidate",
                    gate_enabled=True,
                    verify_command="./bin/tests",
                    verify_timeout_seconds=300,
                    verify_timeout_grace_seconds=5.0,
                    environment_identity=None,
                    verify_exit_status="0",
                    failure=None,
                    failing_phase=None,
                    reviewed_branch="main",
                    working_directory=str(tmp_path),
                    captured_at=datetime.now(UTC),
                ),
            ),
        ),
        patch("gza.cli.git_ops._promote_isolated_merge_to_target_branch") as promote,
    ):
        if executor_path == "already-merged":
            already_git = _replay_executor_git(tmp_path, str(task.branch), source_sha="source-sha")
            result = _execute_merge_action(
                config,
                store,
                live_git,
                task,
                action,
                target_branch="main",
                current_branch="main",
                merge_git=already_git,
                merge_current_branch="main",
                already_merged_behavior="mark_merged",
                merge_source=MERGE_SOURCE_ADVANCE,
            )
        else:
            with patch(
                "gza.cli.git_ops._merge_single_task",
                side_effect=merge_side_effect
                if executor_path == "non-isolated"
                else lambda *_args, **_kwargs: _MergeSingleTaskResult(
                    rc=0,
                    authorized_source_ref_sha="source-sha",
                ),
            ):
                result = _execute_merge_action(
                    config,
                    store,
                    live_git,
                    task,
                    action,
                    target_branch="main",
                    current_branch="main",
                    merge_git=isolated_git if executor_path == "isolated-single" else None,
                    merge_current_branch="main" if executor_path == "isolated-single" else None,
                    merge_source=MERGE_SOURCE_ADVANCE,
                )

    assert result.rc == 1
    assert len(result.created_deferred_blockers) == 1
    children = [child for child in store.get_based_on_children(task.id) if child.task_type == "implement"]
    assert [child.id for child in children] == [result.created_deferred_blockers[0].id]
    promote.assert_not_called()
    assert not store.list_artifacts(task.id, kind="merge_finalization_prepared_attempt")
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


def test_max_cycle_review_from_other_implementation_is_not_self_authorizing(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Capped action target", "feature/capped-action-target")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    other = _completed_merge_task(store, "Unrelated implementation", "feature/unrelated-capped-review")
    other_output = _capped_review_output("B1")
    other_review = _completed_review(store, other, other_output)
    action = _max_cycle_merge_action(
        other_review,
        (parse_review_report(other_output).findings[0],),
        other_output,
    )

    with (
        patch("gza.cli.git_ops._build_auto_merge_args", side_effect=AssertionError("merge args must not build")),
        patch("gza.cli.git_ops._merge_single_task", side_effect=AssertionError("merge must not run")),
        patch(
            "gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks",
            side_effect=AssertionError("unrelated review must not create deferred blockers"),
        ),
    ):
        result = _execute_merge_action(
            config,
            store,
            _merge_executor_git(tmp_path, str(task.branch)),
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 1
    assert result.status == "pre_merge_proof_persistence_failed"
    assert (
        "not canonical evidence" in (result.block_reason or "")
        or "lifecycle authority" in (result.block_reason or "")
    )
    assert [child for child in store.get_based_on_children(task.id) if child.task_type == "implement"] == []
    assert not store.list_artifacts(task.id, kind="merge_finalization_prepared_attempt")
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


def test_max_cycle_manual_same_type_followup_review_remains_excluded_from_authority(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Manual follow-up review excluded", "feature/manual-review-excluded")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    output = _capped_review_output("B1")
    root_review = _completed_review(store, task, output)
    manual_review = store.add("Manual follow-up review", task_type="review", based_on=root_review.id)
    assert manual_review.id is not None
    manual_review.status = "completed"
    manual_review.completed_at = datetime(2026, 8, 27, 10, 5, tzinfo=UTC)
    manual_review.output_content = output
    manual_review.review_verify_branch = task.branch
    manual_review.review_verify_head_sha = "same-sha"
    store.update(manual_review)
    action = _max_cycle_merge_action(manual_review, (parse_review_report(output).findings[0],), output)

    with (
        patch("gza.cli.git_ops._build_auto_merge_args", side_effect=AssertionError("merge args must not build")),
        patch("gza.cli.git_ops._merge_single_task", side_effect=AssertionError("merge must not run")),
        patch(
            "gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks",
            side_effect=AssertionError("manual same-type review follow-up must not create deferred blockers"),
        ),
    ):
        result = _execute_merge_action(
            config,
            store,
            _merge_executor_git(tmp_path, str(task.branch)),
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 1
    assert result.status == "pre_merge_proof_persistence_failed"
    assert (
        "not canonical evidence" in (result.block_reason or "")
        or "lifecycle authority" in (result.block_reason or "")
    )
    assert [child for child in store.get_based_on_children(task.id) if child.task_type == "implement"] == []
    assert not store.list_artifacts(task.id, kind="merge_finalization_prepared_attempt")
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


def test_execute_merge_action_max_cycle_materializes_blockers_before_already_merged_state(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Already merged capped implementation", "feature/capped-already")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B2")
    review = _completed_review(store, task, review_output)
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path)
    finding = _blocker_finding("B2")
    git = _merge_executor_git(tmp_path, task.branch)
    git.is_merged.return_value = True
    merge_git = _merge_executor_git(tmp_path, task.branch)
    merge_git.is_merged.return_value = True
    order: list[str] = []
    original_set_merge_unit_state = store.set_merge_unit_state

    def _record_set_merge_unit_state(*args: object, **kwargs: object) -> Any:
        order.append("state")
        return original_set_merge_unit_state(*args, **kwargs)

    def _materialize_side_effect(*_args: object, **_kwargs: object) -> tuple[list[Any], list[Any]]:
        order.append("defer")
        created = store.add("Deferred capped blocker", task_type="implement", based_on=task.id, depends_on=task.id)
        return [created], []

    store.set_merge_unit_state = MagicMock(side_effect=_record_set_merge_unit_state)  # type: ignore[method-assign]
    with patch(
        "gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks",
        side_effect=_materialize_side_effect,
    ):
        result = _execute_merge_action(
            config,
            store,
            git,
            task,
            _max_cycle_merge_action(review, (finding,), review_output),
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
            already_merged_behavior="mark_merged",
            merge_source=MERGE_SOURCE_WATCH,
        )

    assert result.rc == 0
    assert result.status == "already_merged"
    assert order == ["defer", "state"]
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.merge_source == MERGE_SOURCE_MAX_CYCLES_DEFERRED


def test_stage_isolated_merge_action_max_cycle_already_merged_materializes_before_state(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Staged already merged capped", "feature/staged-capped-already")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path)
    git = _merge_executor_git(tmp_path, task.branch)
    git.is_merged.return_value = True
    merge_git = _merge_executor_git(tmp_path, task.branch)
    merge_git.is_merged.return_value = True
    order: list[str] = []
    original_set_merge_unit_state = store.set_merge_unit_state

    def _record_set_merge_unit_state(*args: object, **kwargs: object) -> Any:
        order.append("state")
        return original_set_merge_unit_state(*args, **kwargs)

    def _materialize_side_effect(*_args: object, **_kwargs: object) -> tuple[list[Any], list[Any]]:
        order.append("defer")
        created = store.add("Deferred capped blocker", task_type="implement", based_on=task.id, depends_on=task.id)
        return [created], []

    store.set_merge_unit_state = MagicMock(side_effect=_record_set_merge_unit_state)  # type: ignore[method-assign]
    with patch(
        "gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks",
        side_effect=_materialize_side_effect,
    ):
        result = _stage_isolated_merge_action(
            config,
            store,
            git,
            task,
            _max_cycle_merge_action(review, (_blocker_finding("B1"),), review_output),
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
            already_merged_behavior="mark_merged",
            merge_source=MERGE_SOURCE_WATCH,
        )

    assert isinstance(result, _MergeActionResult)
    assert result.rc == 0
    assert result.status == "already_merged"
    assert order == ["defer", "state"]
    assert len(result.created_deferred_blockers) == 1
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"
    assert refreshed_unit.merge_source == MERGE_SOURCE_MAX_CYCLES_DEFERRED


@pytest.mark.parametrize("race", ["source-moved", "verify-failed", "verify-unavailable"])
def test_stage_isolated_merge_action_already_merged_max_cycle_final_authorization_blocks_side_effects(
    tmp_path: Path,
    race: str,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    store = make_store(tmp_path)
    task = _completed_merge_task(
        store,
        f"Staged already merged capped final authorization {race}",
        f"feature/staged-capped-final-auth-{race}",
    )
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    review.review_verify_branch = task.branch
    review.review_verify_head_sha = "source-sha"
    store.update(review)
    action = _max_cycle_merge_action(
        review,
        (_blocker_finding("B1"),),
        review_output,
        reviewed_head_sha="source-sha",
    )
    _persist_capped_authorization_verify(
        store,
        config,
        task,
        tmp_path=tmp_path,
        status="passed",
        head_sha="source-sha",
        captured_at=datetime(2026, 8, 27, 10, 0, tzinfo=UTC),
    )
    if race in {"verify-failed", "verify-unavailable"}:
        _persist_capped_authorization_verify(
            store,
            config,
            task,
            tmp_path=tmp_path,
            status="failed" if race == "verify-failed" else "unavailable",
            head_sha="source-sha",
            captured_at=datetime(2026, 8, 27, 10, 5, tzinfo=UTC),
        )

    canonical_source_reads = ["source-sha", "source-after"] if race == "source-moved" else ["source-sha", "source-sha"]
    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=True),
        is_ancestor=MagicMock(return_value=True),
        rev_parse_if_exists=MagicMock(
            side_effect=lambda ref: canonical_source_reads.pop(0)
            if ref == task.branch
            else "target-sha"
            if ref == "main"
            else None
        ),
    )
    merge_git = _merge_executor_git(tmp_path, task.branch)
    merge_git.is_merged.return_value = True
    merge_git.rev_parse_if_exists = MagicMock(  # type: ignore[method-assign]
        side_effect=lambda ref: "source-sha" if ref == task.branch else "candidate-target-sha" if ref == "main" else None
    )
    store.set_merge_unit_state = MagicMock(side_effect=AssertionError("state must not change"))  # type: ignore[method-assign]

    with patch(
        "gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks",
        side_effect=AssertionError("deferred blockers must not materialize before final authorization"),
    ):
        result = _stage_isolated_merge_action(
            config,
            store,
            git,
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
            already_merged_behavior="mark_merged",
            merge_source=MERGE_SOURCE_WATCH,
        )

    assert isinstance(result, _MergeActionResult)
    assert result.rc == 1
    assert result.created_deferred_blockers == []
    assert result.reused_deferred_blockers == []
    assert [child for child in store.get_based_on_children(task.id) if child.task_type == "implement"] == []
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


def test_stage_isolated_merge_action_max_cycle_already_merged_materialization_failure_leaves_unmerged(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Staged already merged capped failure", "feature/staged-capped-failure")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path)
    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=True),
        is_ancestor=MagicMock(return_value=True),
        rev_parse_if_exists=MagicMock(
            side_effect=lambda ref: "same-sha" if ref == task.branch else "target-sha" if ref == "main" else None
        ),
    )
    merge_git = _merge_executor_git(tmp_path, task.branch)
    merge_git.is_merged.return_value = True
    store.set_merge_unit_state = MagicMock(side_effect=AssertionError("state must not change"))  # type: ignore[method-assign]

    with patch(
        "gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks",
        side_effect=RuntimeError("database is locked"),
    ):
        result = _stage_isolated_merge_action(
            config,
            store,
            git,
            task,
            _max_cycle_merge_action(review, (_blocker_finding("B1"),), review_output),
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
            already_merged_behavior="mark_merged",
            merge_source=MERGE_SOURCE_WATCH,
        )

    assert isinstance(result, _MergeActionResult)
    assert result.rc == 1
    assert result.status == "deferred_blocker_materialization_failed"
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


def test_execute_merge_action_max_cycle_task_creation_failure_aborts_before_merge_or_state(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Creation failure capped implementation", "feature/capped-fail")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B3")
    review = _completed_review(store, task, review_output)
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path)
    finding = _blocker_finding("B3")
    git = _merge_executor_git(tmp_path, task.branch)

    def _merge_side_effect(*_args: object, **_kwargs: object) -> _MergeSingleTaskResult:
        side_effect = _kwargs.get("before_irreversible_side_effect")
        assert side_effect is not None
        materialization_error = side_effect()
        assert materialization_error is not None
        return _MergeSingleTaskResult(
            rc=materialization_error.rc,
            status=materialization_error.status,
            block_reason=materialization_error.block_reason,
            created_deferred_blockers=tuple(materialization_error.created_deferred_blockers),
            reused_deferred_blockers=tuple(materialization_error.reused_deferred_blockers),
        )

    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()) as build_args,
        patch("gza.cli.git_ops._merge_single_task", side_effect=_merge_side_effect) as merge_single,
        patch(
            "gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks",
            side_effect=RuntimeError("database is locked"),
        ),
    ):
        result = _execute_merge_action(
            config,
            store,
            git,
            task,
            _max_cycle_merge_action(review, (finding,), review_output),
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 1
    assert result.status == "deferred_blocker_materialization_failed"
    build_args.assert_called_once()
    merge_single.assert_called_once()
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


def test_execute_merge_action_max_cycle_partial_creation_failure_returns_created_work(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Partial capped implementation", "feature/capped-partial-fail")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1", "B2")
    review = _completed_review(store, task, review_output)
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path)
    first_blocker = store.add(
        "Capped review blocker B1 from review testproject-review for task testproject-impl reason review-max-cycles: Fix it",
        task_type="implement",
        based_on=task.id,
        depends_on=task.id,
    )
    git = _merge_executor_git(tmp_path, task.branch)

    def _merge_side_effect(*_args: object, **_kwargs: object) -> _MergeSingleTaskResult:
        side_effect = _kwargs.get("before_irreversible_side_effect")
        assert side_effect is not None
        materialization_error = side_effect()
        assert materialization_error is not None
        return _MergeSingleTaskResult(
            rc=materialization_error.rc,
            status=materialization_error.status,
            block_reason=materialization_error.block_reason,
            created_deferred_blockers=tuple(materialization_error.created_deferred_blockers),
            reused_deferred_blockers=tuple(materialization_error.reused_deferred_blockers),
        )

    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch("gza.cli.git_ops._merge_single_task", side_effect=_merge_side_effect),
        patch(
            "gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks",
            side_effect=CappedReviewBlockerMaterializationError(
                "database is locked while creating B2",
                created=[first_blocker],
            ),
        ),
    ):
        result = _execute_merge_action(
            config,
            store,
            git,
            task,
            _max_cycle_merge_action(
                review,
                (_blocker_finding("B1"), _blocker_finding("B2")),
                review_output,
            ),
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 1
    assert result.status == "deferred_blocker_materialization_failed"
    assert [blocker.id for blocker in result.created_deferred_blockers] == [first_blocker.id]
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


def test_execute_merge_action_max_cycle_conflict_preflight_creates_no_deferred_blockers(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Conflict capped implementation", "feature/capped-conflict")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B4")
    review = _completed_review(store, task, review_output)
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path)
    git = _merge_executor_git(tmp_path, task.branch)

    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch(
            "gza.cli.git_ops._merge_single_task",
            return_value=_MergeSingleTaskResult(
                rc=1,
                status="merge_conflict",
                block_reason="branch conflicts against main",
            ),
        ) as merge_single,
        patch("gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks") as materialize,
    ):
        result = _execute_merge_action(
            config,
            store,
            git,
            task,
            _max_cycle_merge_action(review, (_blocker_finding("B4"),), review_output),
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 1
    assert result.status == "merge_conflict"
    merge_single.assert_called_once()
    materialize.assert_not_called()
    assert [child for child in store.get_based_on_children(task.id) if child.task_type == "implement"] == []
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


@pytest.mark.parametrize("active_kind", ["review", "improve", "adjudication"])
@pytest.mark.parametrize("active_status", ["pending", "in_progress"])
def test_execute_merge_action_max_cycle_refuses_new_active_lifecycle_state_before_debt_creation(
    tmp_path: Path,
    active_kind: str,
    active_status: str,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    config.on_max_cycles = "merge_and_defer"
    config.max_review_cycles = 1
    store = make_store(tmp_path)
    task = _completed_merge_task(store, f"Lifecycle race {active_kind} {active_status}", f"feature/lifecycle-race-{active_kind}-{active_status}")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    previous_review_output = _capped_review_output("B0")
    previous_review = _completed_review(store, task, previous_review_output)
    previous_review.completed_at = datetime(2026, 8, 27, 10, 0, tzinfo=UTC)
    previous_review.review_verify_head_sha = "source-sha"
    store.update(previous_review)
    completed_improve = store.add(
        "Completed improve for prior capped cycle",
        task_type="improve",
        based_on=task.id,
        depends_on=previous_review.id,
    )
    completed_improve.status = "completed"
    completed_improve.completed_at = datetime(2026, 8, 27, 10, 30, tzinfo=UTC)
    completed_improve.branch = task.branch
    completed_improve.has_commits = True
    completed_improve.changed_diff = True
    store.update(completed_improve)
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    review.completed_at = datetime(2026, 8, 27, 11, 0, tzinfo=UTC)
    store.update(review)
    _set_review_head(store, review, "source-sha")
    finding = parse_review_report(review_output).findings[0]
    action = _max_cycle_merge_action(review, (finding,), review_output, reviewed_head_sha="source-sha")
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="source-sha")
    git = _proofing_merge_executor_git(tmp_path, str(task.branch), previous_target_sha="target-before")
    git.can_merge = MagicMock(return_value=True)
    git.branch_exists = MagicMock(return_value=True)
    git.default_branch = MagicMock(return_value="main")
    git.get_diff_name_status = MagicMock(return_value=[])

    def inject_active_state() -> None:
        if active_kind == "review":
            active = store.add(f"Active review {active_status}", task_type="review", depends_on=task.id, based_on=task.id)
        elif active_kind == "improve":
            active = store.add(f"Active improve {active_status}", task_type="improve", based_on=task.id, depends_on=review.id)
        else:
            from gza.review_tasks import build_review_blocker_dispute_metadata, create_or_reuse_review_blocker_adjudication_task
            from gza.runner import REVIEW_BLOCKER_RESOLUTION_ARTIFACT_KIND

            assert task.id is not None
            assert review.id is not None
            dispute_artifact = store.add_artifact(
                review.id,
                kind=REVIEW_BLOCKER_RESOLUTION_ARTIFACT_KIND,
                label="disputed-B1",
                path=".gza/artifacts/disputed-b1.txt",
                byte_size=0,
                sha256="0" * 64,
                status="disputed",
                exit_status="already_satisfied",
                head_sha="source-sha",
                metadata={
                    "schema_version": 1,
                    "state": "disputed",
                    "review_task_id": review.id,
                    "impl_task_id": task.id,
                    "source_task_id": task.id,
                    "source_task_type": task.task_type,
                    "finding_id": "B1",
                    "reason": "already_satisfied",
                    "evidence": "The guard exists.",
                    "current_state_citation": "src/gza/example.py:1",
                    "finding_fingerprint": {
                        "title": "fix it",
                        "anchor": "still open",
                    },
                },
                created_at=datetime.now(UTC),
            )
            active, _created_now = create_or_reuse_review_blocker_adjudication_task(
                store,
                review_task=review,
                impl_task=task,
                finding=finding,
                dispute_metadata=build_review_blocker_dispute_metadata(dispute_artifact),
                trigger_source="advance",
            )
        active.status = active_status
        store.update(active)

    def merge_side_effect(*_args: object, **kwargs: object) -> _MergeSingleTaskResult:
        inject_active_state()
        side_effect = kwargs.get("before_irreversible_side_effect")
        assert side_effect is not None
        refusal = side_effect()
        assert refusal is not None
        return _MergeSingleTaskResult(
            rc=refusal.rc,
            status=refusal.status,
            block_reason=refusal.block_reason,
            created_deferred_blockers=tuple(refusal.created_deferred_blockers),
            reused_deferred_blockers=tuple(refusal.reused_deferred_blockers),
        )

    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace(mark_only=False, squash=False, delete=False, force=False, no_followups=True)),
        patch("gza.cli.git_ops._merge_single_task", side_effect=merge_side_effect),
        patch("gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks", side_effect=AssertionError("deferred blockers must not be created")),
    ):
        result = _execute_merge_action(
            config,
            store,
            git,
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 1
    assert result.status == "max_cycle_lifecycle_authority_changed"
    assert result.created_deferred_blockers == []
    assert result.reused_deferred_blockers == []
    assert [child for child in store.get_based_on_children(task.id) if child.task_type == "implement"] == []
    assert store.list_artifacts(task.id, kind="merge_finalization_prepared_attempt") == []
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


@pytest.mark.parametrize(
    ("action_family", "preflight_status"),
    [
        ("capped", "dirty"),
        ("capped", "conflict"),
        ("ordinary", "dirty"),
        ("ordinary", "conflict"),
    ],
)
def test_execute_merge_action_real_merge_single_preflight_refuses_before_child_writes(
    tmp_path: Path,
    action_family: str,
    preflight_status: str,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, f"{action_family} {preflight_status}", f"feature/{action_family}-{preflight_status}")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    if action_family == "capped":
        review_output = _capped_review_output("B1")
        review = _completed_review(store, task, review_output)
        _set_review_head(store, review, "sha")
        action: dict[str, object] = _max_cycle_merge_action(
            review,
            (_blocker_finding("B1"),),
            review_output,
            reviewed_head_sha="sha",
        )
        _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="sha")
    else:
        review_output = _approved_with_followups_output("F1")
        review = _completed_review(store, task, review_output)
        action = {
            "type": "merge_with_followups",
            "description": "Merge with follow-ups",
            "review_task": review,
            "followup_findings": (_followup_finding("F1"),),
        }
    resolved = _ResolvedMergeSubject(
        trigger_task=task,
        execution_task=task,
        merge_subject=task,
        merge_unit_id=unit.id,
        merge_branch=task.branch,
        merge_source_ref=task.branch,
        merge_source_warning=None,
    )
    git = SimpleNamespace(
        repo_dir=tmp_path,
        default_branch=MagicMock(return_value="main"),
        is_merged=MagicMock(return_value=False),
        has_changes=MagicMock(return_value=preflight_status == "dirty"),
        can_merge=MagicMock(return_value=preflight_status != "conflict"),
        merge=MagicMock(side_effect=AssertionError("merge must not run")),
        rev_parse_if_exists=MagicMock(return_value="sha"),
    )

    with (
        patch("gza.cli.git_ops._resolve_merge_subject", return_value=resolved),
        patch("gza.cli.git_ops.determine_next_action", return_value=action),
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace(mark_only=False, squash=False, delete=False, force=False, no_followups=True)),
        patch("gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks") as capped_materialize,
        patch("gza.cli.git_ops._create_or_reuse_followup_tasks") as followup_materialize,
    ):
        result = _execute_merge_action(
            config,
            store,
            git,
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 1
    assert result.status == ("blocked_dirty_checkout" if preflight_status == "dirty" else "merge_conflict")
    capped_materialize.assert_not_called()
    followup_materialize.assert_not_called()
    assert [child for child in store.get_based_on_children(task.id) if child.task_type == "implement"] == []
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


def test_execute_merge_action_real_merge_single_clean_orders_materialize_before_merge_and_state(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Clean capped implementation", "feature/clean-capped")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    _set_review_head(store, review, "source-sha")
    action = _max_cycle_merge_action(
        review,
        (_blocker_finding("B1"),),
        review_output,
        reviewed_head_sha="source-sha",
    )
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="source-sha")
    resolved = _ResolvedMergeSubject(
        trigger_task=task,
        execution_task=task,
        merge_subject=task,
        merge_unit_id=unit.id,
        merge_branch=task.branch,
        merge_source_ref=task.branch,
        merge_source_warning=None,
    )
    order: list[str] = []
    target_reads = ["target-before", "target-before", "target-before", "target-after"]
    git = SimpleNamespace(
        repo_dir=tmp_path,
        default_branch=MagicMock(return_value="main"),
        is_merged=MagicMock(return_value=False),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(side_effect=lambda *_args: order.append("preflight") or True),
        merge=MagicMock(side_effect=lambda *_args, **_kwargs: order.append("git.merge")),
        rev_parse_if_exists=MagicMock(
            side_effect=lambda ref: target_reads.pop(0) if ref == "main" else "source-sha"
        ),
    )
    original_set_state = store.set_merge_unit_state

    def materialize(*_args: object, **_kwargs: object) -> tuple[list[DbTask], list[DbTask]]:
        order.append("materialize")
        child = store.add("Deferred blocker child", task_type="implement", based_on=task.id, depends_on=task.id)
        return [child], []

    def set_state(*args: object, **kwargs: object) -> Any:
        order.append("state")
        return original_set_state(*args, **kwargs)

    store.set_merge_unit_state = MagicMock(side_effect=set_state)  # type: ignore[method-assign]
    with (
        patch("gza.cli.git_ops._resolve_merge_subject", return_value=resolved),
        patch("gza.cli.git_ops.determine_next_action", return_value=action),
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace(mark_only=False, squash=False, delete=False, force=False, no_followups=True)),
        patch("gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks", side_effect=materialize),
    ):
        result = _execute_merge_action(
            config,
            store,
            git,
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 0
    assert order == ["preflight", "materialize", "git.merge", "state"]
    assert len(result.created_deferred_blockers) == 1
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"
    assert refreshed_unit.merge_source == MERGE_SOURCE_MAX_CYCLES_DEFERRED


@pytest.mark.parametrize("action_family", ["capped", "ordinary"])
def test_execute_merge_action_refuses_non_isolated_merge_when_source_ref_advances_after_authorization(
    tmp_path: Path,
    action_family: str,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(
        store,
        f"Source race {action_family}",
        f"feature/source-race-{action_family}",
    )
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    if action_family == "capped":
        review_output = _capped_review_output("B1")
        review = _completed_review(store, task, review_output)
        _set_review_head(store, review, "source-before")
        action: dict[str, object] = _max_cycle_merge_action(
            review,
            (parse_review_report(review_output).findings[0],),
            review_output,
            reviewed_head_sha="source-before",
        )
        _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="source-before")
    else:
        review_output = _approved_with_followups_output("F1")
        review = _completed_review(store, task, review_output)
        action = {
            "type": "merge_with_followups",
            "description": "Merge with follow-ups",
            "review_task": review,
            "followup_findings": (parse_review_report(review_output).findings[0],),
        }
    resolved = _ResolvedMergeSubject(
        trigger_task=task,
        execution_task=task,
        merge_subject=task,
        merge_unit_id=unit.id,
        merge_branch=task.branch,
        merge_source_ref=task.branch,
        merge_source_warning=None,
    )
    source_reads = ["source-before", "source-before", "source-before", "source-after", "source-after"]
    git = SimpleNamespace(
        repo_dir=tmp_path,
        default_branch=MagicMock(return_value="main"),
        is_merged=MagicMock(return_value=False),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(side_effect=AssertionError("merge must not run after source ref race")),
        rev_parse_if_exists=MagicMock(
            side_effect=lambda ref: source_reads.pop(0) if ref == task.branch else "target-before"
        ),
    )
    store.set_merge_unit_state = MagicMock(side_effect=AssertionError("state must not change"))  # type: ignore[method-assign]

    with (
        patch("gza.cli.git_ops._resolve_merge_subject", return_value=resolved),
        patch("gza.cli.git_ops.determine_next_action", return_value=action),
        patch(
            "gza.cli.git_ops._build_auto_merge_args",
            return_value=argparse.Namespace(mark_only=False, squash=False, delete=False, force=False, no_followups=True),
        ),
    ):
        result = _execute_merge_action(
            config,
            store,
            git,
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 1
    if action_family == "capped":
        assert result.status == "merge_source_ref_changed"
        assert "source changed after staging" in (result.block_reason or "")
    else:
        assert result.status == "merge_source_ref_changed"
        assert "merge source ref changed after lifecycle authorization" in (result.block_reason or "")
        assert "target is unchanged" in (result.block_reason or "")
    assert len(store.list_artifacts(task.id, kind="merge_finalization_attempt_proof")) == 0
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


@pytest.mark.parametrize("action_family", ["capped", "ordinary"])
@pytest.mark.parametrize(
    ("outer_source_ref", "inner_source_ref"),
    [
        ("feature/split-source-local", "origin/feature/split-source-local"),
        ("origin/feature/split-source-remote", "feature/split-source-remote"),
    ],
)
def test_execute_merge_action_uses_one_resolved_source_identity_for_merge_and_proof(
    tmp_path: Path,
    action_family: str,
    outer_source_ref: str,
    inner_source_ref: str,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    branch = outer_source_ref.removeprefix("origin/")
    task = _completed_merge_task(store, f"Split source {action_family}", branch)
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    if action_family == "capped":
        review_output = _capped_review_output("B1")
        review = _completed_review(store, task, review_output)
        _set_review_head(store, review, "outer-source-sha")
        action: dict[str, object] = _max_cycle_merge_action(
            review,
            (parse_review_report(review_output).findings[0],),
            review_output,
            reviewed_head_sha="outer-source-sha",
        )
        _persist_capped_authorization_verify(
            store,
            config,
            task,
            tmp_path=tmp_path,
            head_sha="outer-source-sha",
        )
        expected_merge_source = MERGE_SOURCE_MAX_CYCLES_DEFERRED
    else:
        review_output = _approved_with_followups_output("F1")
        review = _completed_review(store, task, review_output)
        action = {
            "type": "merge_with_followups",
            "description": "Merge with follow-ups",
            "review_task": review,
            "followup_findings": (parse_review_report(review_output).findings[0],),
        }
        expected_merge_source = MERGE_SOURCE_ADVANCE
    outer_resolved = _ResolvedMergeSubject(
        trigger_task=task,
        execution_task=task,
        merge_subject=task,
        merge_unit_id=unit.id,
        merge_branch=branch,
        merge_source_ref=outer_source_ref,
        merge_source_warning=None,
    )
    inner_resolved = replace(outer_resolved, merge_source_ref=inner_source_ref)
    source_shas = {
        outer_source_ref: "outer-source-sha",
        inner_source_ref: "outer-source-sha" if action_family == "capped" else "inner-source-sha",
    }
    target_reads = ["target-before", "target-after"]

    def _rev_parse(ref: str) -> str | None:
        if ref == "main":
            return target_reads.pop(0) if target_reads else "target-after"
        return source_shas.get(ref)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        resolve_fresh_merge_source=MagicMock(return_value=ResolvedMergeSourceRef(outer_source_ref)),
        default_branch=MagicMock(return_value="main"),
        is_merged=MagicMock(return_value=False),
        is_ancestor=MagicMock(return_value=True),
        rev_parse_if_exists=MagicMock(side_effect=_rev_parse),
    )
    merge_requests: list[ManualMergeExecutionRequest] = []

    def _execute_manual_merge(
        request: ManualMergeExecutionRequest,
        hooks: ManualMergeExecutionHooks,
    ) -> ManualMergeExecutionResult:
        merge_requests.append(request)
        assert hooks.before_irreversible_side_effect is not None
        assert hooks.before_irreversible_side_effect(request.merge_subject) is None
        return ManualMergeExecutionResult(rc=0, status="merged")

    with (
        patch("gza.cli.git_ops._resolve_merge_subject", side_effect=[outer_resolved, inner_resolved]) as resolve_subject,
        patch("gza.cli.git_ops.determine_next_action", return_value=action),
        patch(
            "gza.cli.git_ops._build_auto_merge_args",
            return_value=argparse.Namespace(
                mark_only=False,
                squash=False,
                delete=False,
                force=False,
                no_followups=False,
            ),
        ),
        patch("gza.cli.git_ops.execute_manual_merge", side_effect=_execute_manual_merge),
    ):
        result = _execute_merge_action(
            config,
            store,
            git,
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 0
    assert resolve_subject.call_count == 1
    assert [request.merge_source_ref for request in merge_requests] == [outer_source_ref]
    assert [request.authorized_source_ref_sha for request in merge_requests] == ["outer-source-sha"]
    proofs = store.list_artifacts(task.id, kind="merge_finalization_attempt_proof")
    assert len(proofs) == 1
    assert proofs[0].metadata is not None
    assert proofs[0].metadata["source_ref"] == outer_source_ref
    assert proofs[0].metadata["source_ref_sha"] == "outer-source-sha"
    assert proofs[0].metadata["promoted_target_sha"] == "target-after"
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"
    assert refreshed_unit.merge_source == expected_merge_source


@pytest.mark.parametrize(
    "race_case",
    ["source-moved", "target-moved-without-containing-source"],
)
def test_execute_merge_action_already_merged_rereads_refs_after_materialization_before_state(
    tmp_path: Path,
    race_case: str,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(
        store,
        f"Already merged ref race {race_case}",
        f"feature/already-merged-race-{race_case}",
    )
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    _set_review_head(store, review, "source-before")
    action = _max_cycle_merge_action(
        review,
        (parse_review_report(review_output).findings[0],),
        review_output,
        reviewed_head_sha="source-before",
    )
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="source-before")
    if race_case == "source-moved":
        source_reads = ["source-before", "source-after"]
        target_reads = ["target-after"]
        is_ancestor = MagicMock(return_value=True)
    else:
        source_reads = ["source-before", "source-before"]
        target_reads = ["target-after", "target-after"]
        is_ancestor = MagicMock(return_value=False)

    def _rev_parse(ref: str) -> str | None:
        if ref == task.branch:
            return source_reads.pop(0) if source_reads else "source-before"
        if ref == "main":
            return target_reads.pop(0) if target_reads else "target-after"
        return None

    git = _merge_executor_git(tmp_path, task.branch)
    git.is_merged.return_value = True
    git.is_ancestor = is_ancestor
    git.rev_parse_if_exists = MagicMock(side_effect=_rev_parse)

    merge_git = SimpleNamespace(
        repo_dir=tmp_path,
        resolve_fresh_merge_source=MagicMock(return_value=ResolvedMergeSourceRef(task.branch)),
        is_merged=MagicMock(return_value=True),
        is_ancestor=MagicMock(return_value=True),
        rev_parse_if_exists=MagicMock(
            side_effect=lambda ref: "source-before" if ref == task.branch else "candidate-target" if ref == "main" else None
        ),
    )
    store.set_merge_unit_state = MagicMock(side_effect=AssertionError("state must not change"))  # type: ignore[method-assign]

    with patch("gza.cli.git_ops._require_fresh_capped_review_lifecycle_authority"):
        result = _execute_merge_action(
            config,
            store,
            git,
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
            already_merged_behavior="mark_merged",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 1
    expected_status = (
        "pre_merge_proof_persistence_failed"
        if race_case == "source-moved"
        else "already_merged_proof_persistence_failed"
    )
    assert result.status == expected_status
    if race_case == "source-moved":
        assert "source changed after staging" in (result.block_reason or "")
    assert len(store.list_artifacts(task.id, kind="merge_finalization_attempt_proof")) == 0
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


def test_execute_merge_action_max_cycle_replay_reuses_existing_blockers_and_fills_missing(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Replay capped implementation", "feature/capped-replay")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B4", "B5")
    review = _completed_review(store, task, review_output)
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path)
    findings = (_blocker_finding("B4"), _blocker_finding("B5"))
    git = _merge_executor_git(tmp_path, task.branch)
    merge_attempts = 0

    def _merge_side_effect(*_args: object, **_kwargs: object) -> _MergeSingleTaskResult:
        nonlocal merge_attempts
        merge_attempts += 1
        side_effect = _kwargs.get("before_irreversible_side_effect")
        assert side_effect is not None
        materialization_error = side_effect()
        assert materialization_error is None
        if merge_attempts == 1:
            return _MergeSingleTaskResult(rc=1, status="merge_failed", block_reason="conflict after materialization")
        return _MergeSingleTaskResult(rc=0)

    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch("gza.cli.git_ops._merge_single_task", side_effect=_merge_side_effect),
    ):
        first = _execute_merge_action(
            config,
            store,
            git,
            task,
            _max_cycle_merge_action(review, findings, review_output),
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )
        after_failed_attempt = store.get_merge_unit(unit.id)
        assert after_failed_attempt is not None
        assert after_failed_attempt.state == "unmerged"
        assert after_failed_attempt.merge_source is None
        second = _execute_merge_action(
            config,
            store,
            git,
            task,
            _max_cycle_merge_action(review, findings, review_output),
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert len(first.created_deferred_blockers) == 2
    assert first.reused_deferred_blockers == []
    assert second.created_deferred_blockers == []
    assert [task.id for task in second.reused_deferred_blockers] == [
        task.id for task in first.created_deferred_blockers
    ]
    assert all(review_output in blocker.prompt for blocker in second.reused_deferred_blockers)
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"
    assert refreshed_unit.merge_source == MERGE_SOURCE_MAX_CYCLES_DEFERRED


def test_execute_merge_action_max_cycle_passes_active_scope_tags_to_blocker_creator(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(
        store,
        "Tagged capped implementation",
        "feature/capped-tags",
        tags=("backend",),
    )
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B6")
    review = _completed_review(store, task, review_output)
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path)
    finding = _blocker_finding("B6")
    git = _merge_executor_git(tmp_path, task.branch)
    order: list[str] = []

    def _merge_side_effect(*_args: object, **_kwargs: object) -> _MergeSingleTaskResult:
        side_effect = _kwargs.get("before_irreversible_side_effect")
        assert side_effect is not None
        order.append("preflight")
        materialization_error = side_effect()
        assert materialization_error is None
        order.append("git.merge")
        return _MergeSingleTaskResult(rc=0)

    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch("gza.cli.git_ops._merge_single_task", side_effect=_merge_side_effect),
    ):
        result = _execute_merge_action(
            config,
            store,
            git,
            task,
            _max_cycle_merge_action(review, (finding,), review_output),
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
            active_scope_tags=("release", "backend"),
        )

    assert result.rc == 0
    assert order == ["preflight", "git.merge"]
    assert len(result.created_deferred_blockers) == 1
    assert set(result.created_deferred_blockers[0].tags) >= {
        "backend",
        "release",
        "deferred-review-blocker",
    }
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"
    assert refreshed_unit.merge_source == MERGE_SOURCE_MAX_CYCLES_DEFERRED


def test_execute_merge_action_ordinary_merge_persists_state_and_followups_once(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Ordinary implementation", "feature/ordinary-merge")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = "## Review\n\nVerdict: APPROVED_WITH_FOLLOWUPS\n"
    review = _completed_review(store, task, review_output)
    followup = _followup_finding("F1")
    git = _merge_executor_git(tmp_path, task.branch)

    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch("gza.cli.git_ops._merge_single_task", return_value=_MergeSingleTaskResult(rc=0)),
        patch("gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks") as materialize,
    ):
        result = _execute_merge_action(
            config,
            store,
            git,
            task,
            {
                "type": "merge",
                "description": "Merge with follow-ups",
                "review_task": review,
                "followup_findings": (followup,),
            },
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 0
    assert len(result.created_followups) == 1
    assert result.reused_followups == []
    assert result.created_deferred_blockers == []
    assert result.reused_deferred_blockers == []
    materialize.assert_not_called()
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"
    assert refreshed_unit.merge_source == MERGE_SOURCE_ADVANCE
    assert [child.id for child in store.get_based_on_children(review.id) if child.task_type == "implement"] == [
        result.created_followups[0].id
    ]


def test_execute_merge_action_ordinary_followups_materialize_before_merge_and_state(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Ordinary ordered implementation", "feature/ordinary-ordered")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review = _completed_review(store, task, "## Review\n\nVerdict: APPROVED_WITH_FOLLOWUPS\n")
    followup = _followup_finding("F1")
    git = _merge_executor_git(tmp_path, task.branch)
    order: list[str] = []
    original_followup_creator = __import__("gza.cli.git_ops", fromlist=["_create_or_reuse_followup_tasks"])._create_or_reuse_followup_tasks
    original_set_merge_unit_state = store.set_merge_unit_state

    def merge_side_effect(*_args: object, **kwargs: object) -> _MergeSingleTaskResult:
        side_effect = kwargs.get("before_irreversible_side_effect")
        assert side_effect is not None
        materialization_error = side_effect()
        assert materialization_error is None
        order.append("git.merge")
        return _MergeSingleTaskResult(rc=0)

    def followup_side_effect(*args: object, **kwargs: object) -> tuple[list[DbTask], list[DbTask]]:
        order.append("followups")
        return original_followup_creator(*args, **kwargs)

    def set_state_side_effect(*args: object, **kwargs: object) -> Any:
        order.append("state")
        return original_set_merge_unit_state(*args, **kwargs)

    store.set_merge_unit_state = MagicMock(side_effect=set_state_side_effect)  # type: ignore[method-assign]
    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch("gza.cli.git_ops._merge_single_task", side_effect=merge_side_effect),
        patch("gza.cli.git_ops._create_or_reuse_followup_tasks", side_effect=followup_side_effect),
    ):
        result = _execute_merge_action(
            config,
            store,
            git,
            task,
            {
                "type": "merge",
                "description": "Merge with follow-ups",
                "review_task": review,
                "followup_findings": (followup,),
            },
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 0
    assert order == ["followups", "git.merge", "state"]
    assert len(result.created_followups) == 1
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"
    assert refreshed_unit.merge_source == MERGE_SOURCE_ADVANCE


def test_execute_merge_action_ordinary_followup_failure_aborts_before_merge_or_state(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Ordinary followup failure", "feature/ordinary-followup-failure")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review = _completed_review(store, task, "## Review\n\nVerdict: APPROVED_WITH_FOLLOWUPS\n")
    git = _merge_executor_git(tmp_path, task.branch)
    order: list[str] = []

    def merge_side_effect(*_args: object, **kwargs: object) -> _MergeSingleTaskResult:
        side_effect = kwargs.get("before_irreversible_side_effect")
        assert side_effect is not None
        materialization_error = side_effect()
        assert materialization_error is not None
        order.append("preflight-aborted")
        return _MergeSingleTaskResult(
            rc=materialization_error.rc,
            status=materialization_error.status,
            block_reason=materialization_error.block_reason,
        )

    store.set_merge_unit_state = MagicMock(side_effect=AssertionError("state must not change"))  # type: ignore[method-assign]
    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch("gza.cli.git_ops._merge_single_task", side_effect=merge_side_effect),
        patch("gza.cli.git_ops._create_or_reuse_followup_tasks", side_effect=sqlite3.OperationalError("locked")),
    ):
        result = _execute_merge_action(
            config,
            store,
            git,
            task,
            {
                "type": "merge",
                "description": "Merge with follow-ups",
                "review_task": review,
                "followup_findings": (_followup_finding("F1"),),
            },
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 1
    assert result.status == "merge_side_effect_materialization_failed"
    assert order == ["preflight-aborted"]
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


@pytest.mark.parametrize(
    ("race_case", "expected_reason"),
    [
        ("deleted", "disappeared"),
        ("prompt-mutated", "prompt"),
        ("shape-mutated", "depends_on"),
        ("terminally-landed", "cannot be reused"),
        ("duplicated", "Ambiguous follow-up task identity"),
    ],
)
def test_execute_merge_action_pending_ordinary_followup_replay_races_fail_closed_before_state(
    tmp_path: Path,
    race_case: str,
    expected_reason: str,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, f"Ordinary replay race {race_case}", f"feature/ordinary-race-{race_case}")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review = _completed_review(store, task, "## Review\n\nVerdict: APPROVED_WITH_FOLLOWUPS\n")
    finding = _followup_finding("F1")
    assert review.id is not None
    assert task.id is not None
    proven_child = store.add(
        build_followup_prompt(review.id, task.id, finding),
        task_type="implement",
        based_on=review.id,
        depends_on=task.id,
        review_scope=format_followup_finding_context(finding),
    )
    action: dict[str, object] = {
        "type": "merge_with_followups",
        "description": "Merge with follow-ups",
        "review_task": review,
        "followup_findings": (finding,),
        "pending_merge_finalization": True,
        "proven_followup_tasks": (proven_child,),
    }

    if race_case == "deleted":
        assert proven_child.id is not None
        assert store.delete(proven_child.id) is True
    elif race_case == "prompt-mutated":
        proven_child.prompt += "\nmutated"
        store.update(proven_child)
    elif race_case == "shape-mutated":
        proven_child.depends_on = None
        store.update(proven_child)
    elif race_case == "terminally-landed":
        proven_child.merge_status = "merged"
        store.update(proven_child)
    elif race_case == "duplicated":
        store.add(
            proven_child.prompt,
            task_type="implement",
            based_on=review.id,
            depends_on=task.id,
            review_scope=proven_child.review_scope,
        )

    def merge_side_effect(*_args: object, **kwargs: object) -> _MergeSingleTaskResult:
        side_effect = kwargs.get("before_irreversible_side_effect")
        assert side_effect is not None
        materialization_error = side_effect()
        assert materialization_error is not None
        return _MergeSingleTaskResult(
            rc=materialization_error.rc,
            status=materialization_error.status,
            block_reason=materialization_error.block_reason,
        )

    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch("gza.cli.git_ops._merge_single_task", side_effect=merge_side_effect),
    ):
        result = _execute_merge_action(
            config,
            store,
            _merge_executor_git(tmp_path, task.branch),
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 1
    assert result.status == "merge_side_effect_materialization_failed"
    assert expected_reason in (result.block_reason or "")
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


@pytest.mark.parametrize("executor_path", ["non-isolated", "isolated-stage", "already-merged"])
@pytest.mark.parametrize(
    "action_case",
    [
        "missing-review",
        "non-db-review",
        "all-malformed-findings",
        "mixed-valid-malformed-findings",
        "wrong-severity-findings",
        "duplicate-finding-identities",
    ],
)
def test_merge_with_followups_invalid_metadata_fails_before_mutation(
    tmp_path: Path,
    executor_path: str,
    action_case: str,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(
        store,
        f"Invalid ordinary followup {executor_path} {action_case}",
        f"feature/invalid-followup-{executor_path}-{action_case}",
    )
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review = _completed_review(store, task, "## Review\n\nVerdict: APPROVED_WITH_FOLLOWUPS\n")
    valid_followup = _followup_finding("F1")
    action: dict[str, object] = {
        "type": "merge_with_followups",
        "description": "Merge with follow-ups",
        "review_task": review,
        "followup_findings": (valid_followup,),
    }
    if action_case == "missing-review":
        action.pop("review_task")
    elif action_case == "non-db-review":
        action["review_task"] = SimpleNamespace(id=review.id, task_type="review")
    elif action_case == "all-malformed-findings":
        action["followup_findings"] = ({"id": "F1", "severity": "FOLLOWUP"},)
    elif action_case == "mixed-valid-malformed-findings":
        action["followup_findings"] = (valid_followup, {"id": "F2", "severity": "FOLLOWUP"})
    elif action_case == "wrong-severity-findings":
        action["followup_findings"] = (_blocker_finding("B1"),)
    elif action_case == "duplicate-finding-identities":
        action["followup_findings"] = (_followup_finding("F1"), _followup_finding("F1"))

    git = SimpleNamespace(repo_dir=tmp_path)
    merge_git = _merge_executor_git(tmp_path, task.branch)
    merge_git.is_merged.return_value = True
    store.set_merge_unit_state = MagicMock(side_effect=AssertionError("state must not change"))  # type: ignore[method-assign]

    with (
        patch("gza.cli.git_ops._build_auto_merge_args", side_effect=AssertionError("merge args must not build")),
        patch("gza.cli.git_ops._merge_single_task", side_effect=AssertionError("merge must not run")),
        patch("gza.cli.git_ops._create_or_reuse_followup_tasks", side_effect=AssertionError("followups not created")),
    ):
        if executor_path == "isolated-stage":
            result = _stage_isolated_merge_action(
                config,
                store,
                git,
                task,
                action,
                target_branch="main",
                current_branch="main",
                merge_git=merge_git,
                merge_current_branch="main",
                already_merged_behavior="mark_merged",
                merge_source=MERGE_SOURCE_WATCH,
            )
        else:
            result = _execute_merge_action(
                config,
                store,
                git,
                task,
                action,
                target_branch="main",
                current_branch="main",
                merge_git=merge_git if executor_path == "already-merged" else None,
                merge_current_branch="main" if executor_path == "already-merged" else None,
                already_merged_behavior="mark_merged" if executor_path == "already-merged" else "error",
                merge_source=MERGE_SOURCE_ADVANCE,
            )

    assert isinstance(result, _MergeActionResult)
    assert result.rc == 1
    assert result.status == "merge_side_effect_materialization_failed"
    assert result.created_followups == []
    assert result.reused_followups == []
    assert result.created_deferred_blockers == []
    assert result.reused_deferred_blockers == []
    merge_git.is_merged.assert_not_called()
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None
    assert [child for child in store.get_based_on_children(review.id) if child.task_type == "implement"] == []


def test_execute_merge_action_ordinary_followup_partial_failure_replay_reuses_prior_work(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Ordinary followup replay", "feature/ordinary-followup-replay")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review = _completed_review(store, task, "## Review\n\nVerdict: APPROVED_WITH_FOLLOWUPS\n")
    git = _merge_executor_git(tmp_path, task.branch)
    first_followup = store.add("Existing follow-up F1", task_type="implement", based_on=review.id, depends_on=task.id)
    merge_attempts = 0

    def merge_side_effect(*_args: object, **kwargs: object) -> _MergeSingleTaskResult:
        nonlocal merge_attempts
        merge_attempts += 1
        side_effect = kwargs.get("before_irreversible_side_effect")
        assert side_effect is not None
        materialization_error = side_effect()
        if merge_attempts == 1:
            assert materialization_error is not None
            return _MergeSingleTaskResult(
                rc=materialization_error.rc,
                status=materialization_error.status,
                block_reason=materialization_error.block_reason,
                created_followups=tuple(materialization_error.created_followups),
                reused_followups=tuple(materialization_error.reused_followups),
            )
        assert materialization_error is None
        return _MergeSingleTaskResult(rc=0)

    def followup_side_effect(*_args: object, **_kwargs: object) -> tuple[list[DbTask], list[DbTask]]:
        if merge_attempts == 1:
            raise sqlite3.OperationalError("locked after F1")
        return [], [first_followup]

    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch("gza.cli.git_ops._merge_single_task", side_effect=merge_side_effect),
        patch("gza.cli.git_ops._create_or_reuse_followup_tasks", side_effect=followup_side_effect),
    ):
        first = _execute_merge_action(
            config,
            store,
            git,
            task,
            {
                "type": "merge",
                "description": "Merge with follow-ups",
                "review_task": review,
                "followup_findings": (_followup_finding("F1"), _followup_finding("F2")),
            },
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )
        second = _execute_merge_action(
            config,
            store,
            git,
            task,
            {
                "type": "merge",
                "description": "Merge with follow-ups",
                "review_task": review,
                "followup_findings": (_followup_finding("F1"), _followup_finding("F2")),
            },
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert first.rc == 1
    assert first.status == "merge_side_effect_materialization_failed"
    assert second.rc == 0
    assert second.created_followups == []
    assert second.reused_followups == [first_followup]
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"
    assert refreshed_unit.merge_source == MERGE_SOURCE_ADVANCE


def test_execute_merge_action_ordinary_followup_real_partial_failure_returns_created_work_and_replays(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Ordinary followup real partial", "feature/ordinary-followup-real-partial")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review = _completed_review(store, task, "## Review\n\nVerdict: APPROVED_WITH_FOLLOWUPS\n")
    git = _merge_executor_git(tmp_path, task.branch)
    original_followup_create = store.create_or_reuse_followup_task
    fail_f2 = True
    merge_attempts = 0

    def followup_create_side_effect(*args: object, **kwargs: object) -> tuple[DbTask, bool]:
        nonlocal fail_f2
        params = kwargs["params"]
        if fail_f2 and "Follow-up F2 " in params.prompt:
            fail_f2 = False
            raise sqlite3.OperationalError("locked while creating F2")
        return original_followup_create(*args, **kwargs)

    def merge_side_effect(*_args: object, **kwargs: object) -> _MergeSingleTaskResult:
        nonlocal merge_attempts
        merge_attempts += 1
        side_effect = kwargs.get("before_irreversible_side_effect")
        assert side_effect is not None
        materialization_error = side_effect()
        if merge_attempts == 1:
            assert materialization_error is not None
            return _MergeSingleTaskResult(
                rc=materialization_error.rc,
                status=materialization_error.status,
                block_reason=materialization_error.block_reason,
                created_followups=tuple(materialization_error.created_followups),
                reused_followups=tuple(materialization_error.reused_followups),
            )
        assert materialization_error is None
        return _MergeSingleTaskResult(rc=0)

    store.create_or_reuse_followup_task = MagicMock(side_effect=followup_create_side_effect)  # type: ignore[method-assign]
    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch("gza.cli.git_ops._merge_single_task", side_effect=merge_side_effect),
    ):
        first = _execute_merge_action(
            config,
            store,
            git,
            task,
            {
                "type": "merge",
                "description": "Merge with follow-ups",
                "review_task": review,
                "followup_findings": (_followup_finding("F1"), _followup_finding("F2")),
            },
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert first.rc == 1
    assert first.status == "merge_side_effect_materialization_failed"
    assert "ordinary follow-up materialization failed" in (first.block_reason or "")
    assert [task.id for task in first.created_followups]
    assert first.reused_followups == []
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None

    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch("gza.cli.git_ops._merge_single_task", side_effect=merge_side_effect),
    ):
        second = _execute_merge_action(
            config,
            store,
            git,
            task,
            {
                "type": "merge",
                "description": "Merge with follow-ups",
                "review_task": review,
                "followup_findings": (_followup_finding("F1"), _followup_finding("F2")),
            },
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"
    assert refreshed_unit.merge_source == MERGE_SOURCE_ADVANCE
    assert second.rc == 0
    assert [task.id for task in second.reused_followups] == [task.id for task in first.created_followups]
    assert len(second.created_followups) == 1


def test_execute_merge_action_already_merged_materializes_ordinary_followups_before_state(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Already merged ordinary", "feature/ordinary-already")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review = _completed_review(store, task, "## Review\n\nVerdict: APPROVED_WITH_FOLLOWUPS\n")
    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=True),
        is_ancestor=MagicMock(return_value=True),
        rev_parse_if_exists=MagicMock(
            side_effect=lambda ref: "same-sha" if ref == task.branch else "target-sha" if ref == "main" else None
        ),
    )
    merge_git = _merge_executor_git(tmp_path, task.branch)
    merge_git.is_merged.return_value = True
    order: list[str] = []
    original_set_merge_unit_state = store.set_merge_unit_state

    def followup_side_effect(*_args: object, **_kwargs: object) -> tuple[list[DbTask], list[DbTask]]:
        order.append("followups")
        created = store.add("Ordinary follow-up", task_type="implement", based_on=review.id, depends_on=task.id)
        return [created], []

    def set_state_side_effect(*args: object, **kwargs: object) -> Any:
        order.append("state")
        return original_set_merge_unit_state(*args, **kwargs)

    store.set_merge_unit_state = MagicMock(side_effect=set_state_side_effect)  # type: ignore[method-assign]
    with patch("gza.cli.git_ops._create_or_reuse_followup_tasks", side_effect=followup_side_effect):
        result = _execute_merge_action(
            config,
            store,
            git,
            task,
            {
                "type": "merge",
                "description": "Merge with follow-ups",
                "review_task": review,
                "followup_findings": (_followup_finding("F1"),),
            },
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
            already_merged_behavior="mark_merged",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 0
    assert result.status == "already_merged"
    assert order == ["followups", "state"]
    assert len(result.created_followups) == 1
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"


def test_stage_isolated_merge_action_already_merged_followup_failure_leaves_unmerged(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Staged ordinary already failure", "feature/staged-ordinary-failure")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review = _completed_review(store, task, "## Review\n\nVerdict: APPROVED_WITH_FOLLOWUPS\n")
    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=True),
        is_ancestor=MagicMock(return_value=True),
        rev_parse_if_exists=MagicMock(
            side_effect=lambda ref: "same-sha" if ref == task.branch else "target-sha" if ref == "main" else None
        ),
    )
    merge_git = _merge_executor_git(tmp_path, task.branch)
    merge_git.is_merged.return_value = True
    store.set_merge_unit_state = MagicMock(side_effect=AssertionError("state must not change"))  # type: ignore[method-assign]

    with patch("gza.cli.git_ops._create_or_reuse_followup_tasks", side_effect=sqlite3.OperationalError("locked")):
        result = _stage_isolated_merge_action(
            config,
            store,
            git,
            task,
            {
                "type": "merge",
                "description": "Merge with follow-ups",
                "review_task": review,
                "followup_findings": (_followup_finding("F1"),),
            },
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
            already_merged_behavior="mark_merged",
            merge_source=MERGE_SOURCE_WATCH,
        )

    assert isinstance(result, _MergeActionResult)
    assert result.rc == 1
    assert result.status == "merge_side_effect_materialization_failed"
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


def test_execute_merge_action_legacy_no_merge_unit_marks_task_row_merged(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Legacy implementation", "feature/legacy-no-unit")
    git = _merge_executor_git(tmp_path, task.branch)
    resolved = _ResolvedMergeSubject(
        trigger_task=task,
        execution_task=task,
        merge_subject=task,
        merge_unit_id=None,
        merge_branch=task.branch,
        merge_source_ref=task.branch,
        merge_source_warning=None,
    )

    with (
        patch("gza.cli.git_ops._resolve_merge_subject", return_value=resolved),
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch("gza.cli.git_ops._merge_single_task", return_value=_MergeSingleTaskResult(rc=0)),
    ):
        result = _execute_merge_action(
            config,
            store,
            git,
            task,
            {"type": "merge", "description": "Merge"},
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 0
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "merged"


def test_execute_merge_action_non_isolated_post_merge_state_failure_preserves_debt_and_replays(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Post merge state failure", "feature/post-merge-state-failure")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    action = _max_cycle_merge_action(
        review,
        (_blocker_finding("B1"),),
        review_output,
        reviewed_head_sha="same-sha",
    )
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="same-sha")
    git = _merge_executor_git(tmp_path, task.branch)
    original_set_state = store.set_merge_unit_state

    def merge_side_effect(*_args: object, **kwargs: object) -> _MergeSingleTaskResult:
        side_effect = kwargs.get("before_irreversible_side_effect")
        assert side_effect is not None
        assert side_effect() is None
        return _MergeSingleTaskResult(rc=0)

    store.set_merge_unit_state = MagicMock(side_effect=sqlite3.OperationalError("locked after merge"))  # type: ignore[method-assign]
    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch("gza.cli.git_ops._require_fresh_capped_review_lifecycle_authority"),
        patch("gza.cli.git_ops._merge_single_task", side_effect=merge_side_effect),
    ):
        first = _execute_merge_action(
            config,
            store,
            git,
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert first.rc == 1
    assert first.status == "post_merge_state_persistence_failed"
    assert "target was already changed" in (first.block_reason or "")
    assert "replay will finalize promoted target state" in (first.block_reason or "")
    assert len(first.created_deferred_blockers) == 1
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None

    store.set_merge_unit_state = original_set_state  # type: ignore[method-assign]
    replay_action = pending_merge_finalization_action(
        config,
        store,
        task,
        target_branch="main",
        require_already_merged=True,
        resolved_merge_state="merged",
        live_target_sha="target-sha",
    )
    assert replay_action is not None
    replay_git = _replay_executor_git(tmp_path, task.branch, source_sha="same-sha", target_sha="target-sha")
    second = _execute_merge_action(
        config,
        store,
        SimpleNamespace(repo_dir=tmp_path),
        task,
        replay_action,
        target_branch="main",
        current_branch="main",
        merge_git=replay_git,
        merge_current_branch="main",
        already_merged_behavior="mark_merged",
        merge_source=MERGE_SOURCE_ADVANCE,
    )

    assert second.rc == 0
    assert second.status == "already_merged"
    assert second.created_deferred_blockers == []
    if second.reused_deferred_blockers:
        assert [task.id for task in second.reused_deferred_blockers] == [
            task.id for task in first.created_deferred_blockers
        ]
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"
    assert refreshed_unit.merge_source == MERGE_SOURCE_MAX_CYCLES_DEFERRED


@pytest.mark.parametrize("action_family", ["capped", "ordinary"])
def test_execute_merge_action_non_isolated_proof_persistence_failure_requires_attention(
    tmp_path: Path,
    action_family: str,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(
        store,
        f"Proof persistence failure {action_family}",
        f"feature/proof-failure-{action_family}",
    )
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    if action_family == "capped":
        review_output = _capped_review_output("B1")
        review = _completed_review(store, task, review_output)
        _set_review_head(store, review, "source-sha")
        action: dict[str, object] = _max_cycle_merge_action(
            review,
            (_blocker_finding("B1"),),
            review_output,
            reviewed_head_sha="source-sha",
        )
        _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="source-sha")
    else:
        review_output = _approved_with_followups_output("F1")
        review = _completed_review(store, task, review_output)
        action = {
            "type": "merge_with_followups",
            "description": "Merge with follow-ups",
            "review_task": review,
            "followup_findings": (_followup_finding("F1"),),
        }

    def merge_side_effect(*_args: object, **kwargs: object) -> _MergeSingleTaskResult:
        side_effect = kwargs.get("before_irreversible_side_effect")
        assert side_effect is not None
        assert side_effect() is None
        return _MergeSingleTaskResult(rc=0)

    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch("gza.cli.git_ops._require_fresh_capped_review_lifecycle_authority"),
        patch("gza.cli.git_ops._merge_single_task", side_effect=merge_side_effect),
        patch(
            "gza.cli.git_ops.persist_merge_finalization_attempt_proof",
            side_effect=sqlite3.OperationalError("artifact store locked"),
        ),
    ):
        result = _execute_merge_action(
            config,
            store,
            _proofing_merge_executor_git(tmp_path, task.branch),
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 1
    assert result.status == "post_merge_proof_persistence_failed"
    assert "merge finalization proof was not stored" in (result.block_reason or "")
    assert "operator attention required" in (result.block_reason or "")
    assert "replay will finalize" not in (result.block_reason or "")
    assert len(result.created_deferred_blockers if action_family == "capped" else result.created_followups) == 1
    replay_action = pending_merge_finalization_action(
        config,
        store,
        task,
        target_branch="main",
        require_already_merged=True,
        resolved_merge_state="merged",
        live_target_sha="target-after",
    )
    assert replay_action is not None
    assert replay_action["type"] == "needs_attention"
    assert replay_action["reason"] == (
        "pending-merge-finalization-capped-blocker-missing-proof"
        if action_family == "capped"
        else "pending-merge-finalization-ordinary-followup-missing-proof"
    )
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


def test_execute_merge_action_pre_promotion_source_unavailable_leaves_target_unchanged(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(
        store,
        "Pre-promotion source unavailable",
        "feature/pre-promotion-source-unavailable",
    )
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    action = _max_cycle_merge_action(review, (_blocker_finding("B1"),), review_output)
    git = _merge_executor_git(tmp_path, task.branch)
    git.rev_parse_if_exists = MagicMock(return_value=None)

    with (
        patch("gza.cli.git_ops._require_fresh_capped_review_lifecycle_authority"),
        patch("gza.cli.git_ops._merge_single_task") as merge_single,
    ):
        result = _execute_merge_action(
            config,
            store,
            git,
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 1
    assert result.status == "pre_merge_proof_persistence_failed"
    assert "failed before target promotion" in (result.block_reason or "")
    assert "target is unchanged" in (result.block_reason or "")
    assert "replay will finalize" not in (result.block_reason or "")
    assert result.created_deferred_blockers == []
    merge_single.assert_not_called()
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


def test_execute_merge_action_source_ref_change_before_capped_materialization_creates_no_children(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Pre-materialization source changed", "feature/pre-materialization-source")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    _set_review_head(store, review, "reviewed-before")
    action = _max_cycle_merge_action(
        review,
        (_blocker_finding("B1"),),
        review_output,
        reviewed_head_sha="reviewed-before",
    )
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="reviewed-before")
    git = _merge_executor_git(tmp_path, task.branch)
    git.rev_parse_if_exists = MagicMock(
        side_effect=lambda ref: "source-after" if ref == task.branch else "target-before" if ref == "main" else None
    )

    with (
        patch("gza.cli.git_ops._require_fresh_capped_review_lifecycle_authority"),
        patch("gza.cli.git_ops._merge_single_task") as merge_single,
    ):
        result = _execute_merge_action(
            config,
            store,
            git,
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 1
    assert result.status == "pre_merge_proof_persistence_failed"
    assert "failed before target promotion" in (result.block_reason or "")
    assert "target is unchanged" in (result.block_reason or "")
    assert "source no longer matches reviewed head" in (result.block_reason or "")
    assert result.created_deferred_blockers == []
    merge_single.assert_not_called()
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


@pytest.mark.parametrize("merge_source", [MERGE_SOURCE_ADVANCE, MERGE_SOURCE_WATCH])
@pytest.mark.parametrize("legacy_trigger_source", [None, "manual"])
def test_capped_replay_reuses_initially_accepted_child_without_trigger_source_identity(
    tmp_path: Path,
    merge_source: str,
    legacy_trigger_source: str | None,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(
        store,
        f"Legacy trigger capped replay {merge_source} {legacy_trigger_source}",
        f"feature/legacy-trigger-{merge_source}-{legacy_trigger_source or 'null'}",
    )
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    _set_review_head(store, review, "source-sha")
    finding = parse_review_report(review_output).findings[0]
    existing = store.add(
        build_capped_review_blocker_prompt(review.id, task.id, finding, review_output),
        task_type="implement",
        based_on=task.id,
        depends_on=task.id,
        review_scope=format_blocker_finding_context(finding),
        create_pr=True,
        urgent=True,
        tags=("deferred-review-blocker",),
        trigger_source=legacy_trigger_source,
    )
    action = _max_cycle_merge_action(review, (finding,), review_output, reviewed_head_sha="source-sha")
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="source-sha")

    original_set_state = store.set_merge_unit_state

    def merge_side_effect(*_args: object, **kwargs: object) -> _MergeSingleTaskResult:
        side_effect = kwargs.get("before_irreversible_side_effect")
        assert side_effect is not None
        assert side_effect() is None
        return _MergeSingleTaskResult(rc=0)

    store.set_merge_unit_state = MagicMock(side_effect=sqlite3.OperationalError("locked after merge"))  # type: ignore[method-assign]
    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch("gza.cli.git_ops._require_fresh_capped_review_lifecycle_authority"),
        patch("gza.cli.git_ops._merge_single_task", side_effect=merge_side_effect),
    ):
        first = _execute_merge_action(
            config,
            store,
            _proofing_merge_executor_git(tmp_path, task.branch),
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_source=merge_source,
        )

    assert first.rc == 1
    assert first.status == "post_merge_state_persistence_failed"
    assert first.created_deferred_blockers == []
    assert [child.id for child in first.reused_deferred_blockers] == [existing.id]

    store.set_merge_unit_state = original_set_state  # type: ignore[method-assign]
    replay_action = pending_merge_finalization_action(
        config,
        store,
        task,
        target_branch="main",
        require_already_merged=True,
        resolved_merge_state="merged",
        live_target_sha="target-after",
    )
    assert replay_action is not None
    assert replay_action["type"] == "merge"
    assert replay_action["proven_deferred_blocker_tasks"][0].id == existing.id

    replay_git = _replay_executor_git(tmp_path, task.branch)
    second = _execute_merge_action(
        config,
        store,
        SimpleNamespace(repo_dir=tmp_path),
        task,
        replay_action,
        target_branch="main",
        current_branch="main",
        merge_git=replay_git,
        merge_current_branch="main",
        already_merged_behavior="mark_merged",
        merge_source=merge_source,
    )
    assert second.rc == 0
    assert second.status == "already_merged"
    assert [child.id for child in second.reused_deferred_blockers] == [existing.id]
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"
    assert refreshed_unit.merge_source == MERGE_SOURCE_MAX_CYCLES_DEFERRED


@pytest.mark.parametrize("repeat", [False, True])
def test_cmd_advance_replays_capped_post_merge_state_failure_reusing_deferred_blocker(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    repeat: bool,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(
        store,
        "Replay capped post-merge state failure",
        f"feature/replay-capped-post-merge-{repeat}",
    )
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _changes_requested_review_with_blocker(
        title="Replay post-merge state failure",
        evidence="The target branch changed before state could persist.",
        required_fix="Replay the original deferred blocker finalization.",
    )
    review = _completed_review(store, task, review_output)
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path)
    for index in range(config.max_review_cycles):
        improve = store.add(
            f"Completed improve cycle {index}",
            task_type="improve",
            based_on=task.id,
            depends_on=review.id,
        )
        improve.status = "completed"
        improve.completed_at = datetime.now(UTC)
        store.update(improve)
    finding = parse_review_report(review_output).findings[0]
    action = _max_cycle_merge_action(review, (finding,), review_output)

    original_set_state = store.set_merge_unit_state

    def merge_side_effect(*_args: object, **kwargs: object) -> _MergeSingleTaskResult:
        side_effect = kwargs.get("before_irreversible_side_effect")
        assert side_effect is not None
        assert side_effect() is None
        return _MergeSingleTaskResult(rc=0)

    store.set_merge_unit_state = MagicMock(side_effect=sqlite3.OperationalError("locked after merge"))  # type: ignore[method-assign]
    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch("gza.cli.git_ops._require_fresh_capped_review_lifecycle_authority"),
        patch("gza.cli.git_ops._merge_single_task", side_effect=merge_side_effect),
    ):
        first = _execute_merge_action(
            config,
            store,
            _merge_executor_git(tmp_path, task.branch),
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )
    assert first.rc == 1
    assert first.status == "post_merge_state_persistence_failed"
    assert len(first.created_deferred_blockers) == 1

    store.set_merge_unit_state = original_set_state  # type: ignore[method-assign]
    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(task.branch)
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.is_merged.return_value = True
    fake_git.rev_parse_if_exists.side_effect = lambda ref: (
        "same-sha" if ref == task.branch else "target-sha" if ref == "main" else None
    )
    fake_git.is_ancestor.return_value = True

    args = _advance_args(tmp_path, task.id)
    args.repeat = repeat
    args.max_iterations = 1

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch("gza.cli.git_ops._merge_single_task", side_effect=AssertionError("replay should not merge again")),
    ):
        rc = cmd_advance(args)

    output = capsys.readouterr().out
    assert rc == 0
    assert (
        (
            "Reused deferred blocker task(s)" in output
            and str(first.created_deferred_blockers[0].id) in output
        )
        or "SKIP: already merged into target branch" in output
    )
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    if "SKIP: already merged into target branch" in output:
        assert refreshed_unit.state == "unmerged"
        assert refreshed_unit.merge_source is None
    else:
        assert refreshed_unit.state == "merged"
        assert refreshed_unit.merge_source == MERGE_SOURCE_MAX_CYCLES_DEFERRED
    children = [child for child in store.get_based_on_children(task.id) if child.task_type == "implement"]
    assert [child.id for child in children] == [first.created_deferred_blockers[0].id]


@pytest.mark.parametrize(
    ("stale_family", "fresh_family", "expected_type", "expected_max_cycles"),
    [
        ("capped", "ordinary", "merge_with_followups", False),
        ("ordinary", "capped", "merge", True),
        ("ordinary", "ordinary", "merge_with_followups", False),
    ],
)
def test_pending_finalization_replay_uses_only_fresh_promotion_bound_proof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stale_family: str,
    fresh_family: str,
    expected_type: str,
    expected_max_cycles: bool,
) -> None:
    from gza import advance_engine as advance_engine_module

    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(
        store,
        f"Two attempt replay {stale_family} to {fresh_family}",
        f"feature/two-attempt-{stale_family}-{fresh_family}",
    )
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None

    def _action_for_family(family: str, finding_id: str) -> tuple[dict[str, object], str, ReviewFinding]:
        if family == "capped":
            output = _capped_review_output(finding_id)
            review = _completed_review(store, task, output)
            _set_review_head(store, review, "source-sha")
            finding = parse_review_report(output).findings[0]
            _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="source-sha")
            return (
                _max_cycle_merge_action(review, (finding,), output, reviewed_head_sha="source-sha"),
                output,
                finding,
            )
        output = _approved_with_followups_output(finding_id)
        review = _completed_review(store, task, output)
        finding = parse_review_report(output).findings[0]
        action: dict[str, object] = {
            "type": "merge_with_followups",
            "description": "Merge with follow-ups",
            "review_task": review,
            "followup_findings": (finding,),
        }
        return action, output, finding

    stale_action, _stale_output, stale_finding = _action_for_family(stale_family, "B1" if stale_family == "capped" else "F1")
    fresh_action, fresh_output, fresh_finding = _action_for_family(
        fresh_family,
        "B2" if fresh_family == "capped" else "F2",
    )

    reports_by_review_id = {
        stale_action["review_task"].id: ParsedReviewReport(
            verdict="CHANGES_REQUESTED" if stale_family == "capped" else "APPROVED_WITH_FOLLOWUPS",
            findings=(stale_finding,),
            format_version="v2",
        ),
        fresh_action["review_task"].id: ParsedReviewReport(
            verdict="CHANGES_REQUESTED" if fresh_family == "capped" else "APPROVED_WITH_FOLLOWUPS",
            findings=(fresh_finding,),
            format_version="v2",
        ),
    }
    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, review: reports_by_review_id[review.id],
    )

    def stale_merge_side_effect(*_args: object, **kwargs: object) -> _MergeSingleTaskResult:
        side_effect = kwargs.get("before_irreversible_side_effect")
        assert side_effect is not None
        assert side_effect() is None
        return _MergeSingleTaskResult(rc=1, status="merge_failed", block_reason="git failed before target changed")

    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch("gza.cli.git_ops._merge_single_task", side_effect=stale_merge_side_effect),
    ):
        stale = _execute_merge_action(
            config,
            store,
            _proofing_merge_executor_git(tmp_path, task.branch),
            task,
            stale_action,
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )
    assert stale.rc == 1
    assert len(store.list_artifacts(task.id, kind="merge_finalization_attempt_proof")) == 0

    original_set_state = store.set_merge_unit_state

    def fresh_merge_side_effect(*_args: object, **kwargs: object) -> _MergeSingleTaskResult:
        side_effect = kwargs.get("before_irreversible_side_effect")
        assert side_effect is not None
        assert side_effect() is None
        return _MergeSingleTaskResult(rc=0)

    store.set_merge_unit_state = MagicMock(side_effect=sqlite3.OperationalError("locked after merge"))  # type: ignore[method-assign]
    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch("gza.cli.git_ops._require_fresh_capped_review_lifecycle_authority"),
        patch("gza.cli.git_ops._merge_single_task", side_effect=fresh_merge_side_effect),
    ):
        fresh = _execute_merge_action(
            config,
            store,
            _proofing_merge_executor_git(tmp_path, task.branch),
            task,
            fresh_action,
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )
    assert fresh.rc == 1
    assert fresh.status == "post_merge_state_persistence_failed"
    store.set_merge_unit_state = original_set_state  # type: ignore[method-assign]

    action = pending_merge_finalization_action(
        config,
        store,
        task,
        target_branch="main",
        require_already_merged=True,
        resolved_merge_state="merged",
        live_target_sha="target-after",
    )

    assert action is not None
    assert action["type"] == expected_type
    assert bool(action.get("max_cycles_merge_and_defer")) is expected_max_cycles
    assert action["review_task"].id == fresh_action["review_task"].id
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


def test_pending_finalization_replay_rejects_promotion_proof_target_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Proof mismatch replay", "feature/proof-mismatch")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    finding = parse_review_report(review_output).findings[0]
    child, _created = create_or_reuse_capped_review_blocker_task(
        store,
        config=config,
        review_task=review,
        impl_task=task,
        finding=finding,
        persisted_review_output=review_output,
        active_scope_tags=("release",),
        trigger_source="advance",
    )
    from gza.merge_finalization_proof import persist_merge_finalization_attempt_proof

    persist_merge_finalization_attempt_proof(
        store,
        action_family="max_cycles_deferred",
        impl_task_id=task.id,
        review_task_id=review.id,
        finding_ids=(finding.id,),
        child_task_ids=(child.id,),
        source_branch=task.branch,
        source_ref=task.branch,
        source_ref_sha="source-sha",
        target_branch="main",
        previous_target_sha="target-before",
        promoted_target_sha="different-target",
        merge_unit_id=unit.id,
    )
    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review: ParsedReviewReport(
            verdict="CHANGES_REQUESTED",
            findings=(finding,),
            format_version="v2",
        ),
    )

    action = pending_merge_finalization_action(
        config,
        store,
        task,
        target_branch="main",
        require_already_merged=True,
        resolved_merge_state="merged",
        live_target_sha="live-target",
    )

    assert action is None
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


def test_cmd_advance_replays_capped_post_merge_state_failure_for_merge_representative(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    review_output = _changes_requested_review_with_blocker(
        title="Replay representative",
        evidence="The representative branch has already reached main.",
        required_fix="Defer and replay through the representative action.",
    )
    owner, representative, review = _add_same_merge_unit_owner_representative_with_review(
        tmp_path,
        store,
        review_content=review_output,
    )
    review.review_verify_branch = representative.branch
    _set_review_head(store, review, "same-sha")
    unit = store.resolve_merge_unit_for_task(representative.id)
    assert unit is not None
    for index in range(config.max_review_cycles):
        improve = store.add(
            f"Completed representative improve cycle {index}",
            task_type="improve",
            based_on=representative.id,
            depends_on=review.id,
        )
        improve.status = "completed"
        improve.completed_at = datetime.now(UTC)
        store.update(improve)
        store.get_or_create_merge_unit_for_task(improve)
    _persist_capped_authorization_verify(store, config, owner, tmp_path=tmp_path)
    _persist_capped_authorization_verify(store, config, representative, tmp_path=tmp_path)
    finding = parse_review_report(review_output).findings[0]
    action = _max_cycle_merge_action(review, (finding,), review_output)

    original_set_state = store.set_merge_unit_state

    def merge_side_effect(*_args: object, **kwargs: object) -> _MergeSingleTaskResult:
        side_effect = kwargs.get("before_irreversible_side_effect")
        assert side_effect is not None
        assert side_effect() is None
        return _MergeSingleTaskResult(rc=0)

    store.set_merge_unit_state = MagicMock(side_effect=sqlite3.OperationalError("locked after merge"))  # type: ignore[method-assign]
    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch("gza.cli.git_ops._require_fresh_capped_review_lifecycle_authority"),
        patch("gza.cli.git_ops._merge_single_task", side_effect=merge_side_effect),
    ):
        first = _execute_merge_action(
            config,
            store,
            _merge_executor_git(tmp_path, representative.branch),
            representative,
            action,
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )
    assert first.rc == 1
    assert first.status == "post_merge_state_persistence_failed"
    assert len(first.created_deferred_blockers) == 1

    store.set_merge_unit_state = original_set_state  # type: ignore[method-assign]
    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(representative.branch)
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.is_merged.return_value = True
    fake_git.rev_parse_if_exists.side_effect = (
        lambda ref: "same-sha" if ref == representative.branch else "target-sha" if ref == "main" else None
    )
    fake_git.is_ancestor.return_value = True

    args = _advance_args(tmp_path, representative.id)
    args.max_iterations = 1

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch("gza.cli.git_ops._merge_single_task", side_effect=AssertionError("replay should not merge again")),
    ):
        rc = cmd_advance(args)

    output = capsys.readouterr().out
    assert rc == 0
    assert (
        (
            "Reused deferred blocker task(s)" in output
            and str(first.created_deferred_blockers[0].id) in output
        )
        or "SKIP: already merged into target branch" in output
    )
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.owner_task_id == owner.id
    if "SKIP: already merged into target branch" in output:
        assert refreshed_unit.state == "unmerged"
        assert refreshed_unit.merge_source is None
    else:
        assert refreshed_unit.state == "merged"
        assert refreshed_unit.merge_source == MERGE_SOURCE_MAX_CYCLES_DEFERRED
    children = [child for child in store.get_based_on_children(owner.id) if child.task_type == "implement"]
    assert [child.id for child in children] == [first.created_deferred_blockers[0].id]


@pytest.mark.parametrize("repeat", [False, True])
def test_cmd_advance_replays_ordinary_followup_post_merge_state_failure_reusing_followup(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    repeat: bool,
) -> None:
    from gza import advance_engine as advance_engine_module

    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(
        store,
        "Replay ordinary follow-up post-merge state failure",
        f"feature/replay-followup-post-merge-{repeat}",
    )
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = "## Review\n\nVerdict: APPROVED_WITH_FOLLOWUPS\n"
    review = _completed_review(store, task, review_output)
    finding = _followup_finding("F1")
    action = {
        "type": "merge_with_followups",
        "description": "Merge with follow-ups",
        "review_task": review,
        "followup_findings": (finding,),
    }

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review: ParsedReviewReport(
            verdict="APPROVED_WITH_FOLLOWUPS",
            findings=(finding,),
            format_version="v2",
        ),
    )

    original_set_state = store.set_merge_unit_state

    def merge_side_effect(*_args: object, **kwargs: object) -> _MergeSingleTaskResult:
        side_effect = kwargs.get("before_irreversible_side_effect")
        assert side_effect is not None
        assert side_effect() is None
        return _MergeSingleTaskResult(rc=0)

    store.set_merge_unit_state = MagicMock(side_effect=sqlite3.OperationalError("locked after merge"))  # type: ignore[method-assign]
    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch("gza.cli.git_ops._merge_single_task", side_effect=merge_side_effect),
    ):
        first = _execute_merge_action(
            config,
            store,
            _merge_executor_git(tmp_path, task.branch),
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )
    assert first.rc == 1
    assert first.status == "post_merge_state_persistence_failed"
    assert len(first.created_followups) == 1

    store.set_merge_unit_state = original_set_state  # type: ignore[method-assign]
    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(task.branch)
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.is_merged.return_value = True
    fake_git.rev_parse_if_exists.side_effect = lambda ref: (
        "same-sha" if ref == task.branch else "target-sha" if ref == "main" else None
    )
    fake_git.is_ancestor.return_value = True

    args = _advance_args(tmp_path, task.id)
    args.repeat = repeat
    args.max_iterations = 1

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch("gza.cli.git_ops._merge_single_task", side_effect=AssertionError("replay should not merge again")),
    ):
        rc = cmd_advance(args)

    output = capsys.readouterr().out
    assert rc == 0
    if repeat:
        assert "success: merged" in output or "SKIP: already merged into target branch" in output
    else:
        assert (
            ("Reused follow-up task(s)" in output and str(first.created_followups[0].id) in output)
            or "SKIP: already merged into target branch" in output
    )
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    if "SKIP: already merged into target branch" in output:
        assert refreshed_unit.state == "unmerged"
        assert refreshed_unit.merge_source is None
    else:
        assert refreshed_unit.state == "merged"
        assert refreshed_unit.merge_source == MERGE_SOURCE_ADVANCE
    children = [child for child in store.get_based_on_children(review.id) if child.task_type == "implement"]
    assert [child.id for child in children] == [first.created_followups[0].id]


@pytest.mark.parametrize("repeat", [False, True])
def test_cmd_advance_pending_finalization_refusal_stops_before_conflict_or_rebase(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    repeat: bool,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(
        store,
        f"Pending finalization refusal caller {repeat}",
        f"feature/pending-finalization-refusal-caller-{repeat}",
    )
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(task.branch)
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = False
    fake_git.can_merge.return_value = False
    fake_git.is_merged.return_value = False
    fake_git.rev_parse_if_exists.return_value = "same-sha"
    fake_git.is_ancestor.return_value = True
    refusal = _MergeActionResult(
        rc=1,
        created_followups=[],
        reused_followups=[],
        created_investigation_task_ids=[],
        reused_investigation_task_ids=[],
        status="pending_merge_finalization_proof_stale",
        block_reason=(
            "pending merge finalization refused before state finalization: proof artifact changed; "
            "merge state and provenance were left unchanged"
        ),
    )

    def refuse_merge(*_args: object, **_kwargs: object) -> _MergeActionResult:
        fake_git.can_merge.reset_mock()
        return refusal

    args = _advance_args(tmp_path, task.id)
    args.repeat = repeat
    args.max_iterations = 1
    action = {"type": "merge", "description": "Merge"}
    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="unmerged",
        next_action=action,
        next_action_reason="test",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=task,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch("gza.cli.git_ops.determine_next_action", return_value=action),
        patch("gza.cli.git_ops._execute_merge_action", side_effect=refuse_merge),
        patch("gza.cli.git_ops.execute_advance_action", side_effect=AssertionError("must not create rebase")),
    ):
        rc = cmd_advance(args)

    output = capsys.readouterr().out
    assert rc == 1
    assert "pending merge finalization refused before state finalization: proof artifact changed" in output
    assert "Merge had conflicts" not in output
    assert "merge conflict routed to rebase" not in output
    fake_git.can_merge.assert_not_called()
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


def test_execute_merge_action_isolated_checkpoint_failure_replays_action_provenance(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\nverify_command: ./bin/tests\n")
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Isolated checkpoint capped", "feature/isolated-checkpoint-capped")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    _set_review_head(store, review, "isolated-merge-oid")
    action = _max_cycle_merge_action(
        review,
        (_blocker_finding("B1"),),
        review_output,
        reviewed_head_sha="isolated-merge-oid",
    )
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="isolated-merge-oid")
    repo_git = SimpleNamespace(
        repo_dir=tmp_path,
        rev_parse_if_exists=lambda ref: "isolated-merge-oid"
        if ref == task.branch
        else "target-before"
        if ref == "main"
        else None,
    )
    merge_git = MagicMock()
    merge_git.repo_dir = config.main_checkout_integration_path
    merge_git.rev_parse.return_value = "isolated-merge-oid"
    merge_git.rev_parse_if_exists.return_value = "isolated-merge-oid"
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="isolated-merge-oid")
    merge_git.rev_parse_if_exists.return_value = "isolated-merge-oid"
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="isolated-merge-oid")
    merge_git.rev_parse_if_exists.return_value = "isolated-merge-oid"

    staged = _StagedIsolatedMergeAction(
        merge_subject=task,
        merge_unit_id=unit.id,
        merge_branch=task.branch,
        pending_squash_reconcile=None,
        source_ref_sha="isolated-merge-oid",
        review_task=review,
        followup_findings=(),
        merge_action_metadata=action,
    )

    with (
        patch("gza.cli.git_ops._stage_isolated_merge_action", return_value=staged),
        patch("gza.cli.git_ops._require_fresh_capped_review_lifecycle_authority"),
        patch("gza.cli.git_ops._promote_isolated_merge_to_target_branch", return_value=()),
        patch(
            "gza.cli.git_ops.promote_candidate_integration_verify_evidence",
            side_effect=sqlite3.OperationalError("checkpoint locked"),
        ),
    ):
        first = _execute_merge_action(
            config,
            store,
            repo_git,
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
            merge_source=MERGE_SOURCE_WATCH,
        )

    assert first.rc == 1
    assert first.status == "isolated_post_promotion_checkpoint_persistence_failed"
    assert "target was already changed" in (first.block_reason or "")
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None
    assert len(first.created_deferred_blockers) == 1

    replay_git = _merge_executor_git(tmp_path, task.branch)
    replay_git.is_merged.return_value = True
    replay_git.rev_parse_if_exists = MagicMock(
        side_effect=lambda ref: "isolated-merge-oid" if ref == task.branch else "target-before" if ref == "main" else None
    )
    second = _execute_merge_action(
        config,
        store,
        repo_git,
        task,
        action,
        target_branch="main",
        current_branch="main",
        merge_git=replay_git,
        merge_current_branch="main",
        already_merged_behavior="mark_merged",
        merge_source=MERGE_SOURCE_WATCH,
    )

    assert second.rc == 0
    assert second.status == "already_merged"
    assert second.created_deferred_blockers == []
    assert [task.id for task in second.reused_deferred_blockers] == [
        task.id for task in first.created_deferred_blockers
    ]
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"
    assert refreshed_unit.merge_source == MERGE_SOURCE_MAX_CYCLES_DEFERRED


def test_execute_merge_action_isolated_finalization_state_failure_preserves_debt_and_replays(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\nverify_command: ./bin/tests\n")
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Isolated finalization state failure", "feature/isolated-final-state")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    _set_review_head(store, review, "isolated-merge-oid")
    action = _max_cycle_merge_action(
        review,
        (_blocker_finding("B1"),),
        review_output,
        reviewed_head_sha="isolated-merge-oid",
    )
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="isolated-merge-oid")
    repo_git = SimpleNamespace(
        repo_dir=tmp_path,
        rev_parse_if_exists=lambda ref: "isolated-merge-oid"
        if ref == task.branch
        else "target-before"
        if ref == "main"
        else None,
    )
    merge_git = MagicMock()
    merge_git.repo_dir = config.main_checkout_integration_path
    merge_git.rev_parse.return_value = "isolated-merge-oid"
    merge_git.rev_parse_if_exists.return_value = "isolated-merge-oid"
    staged = _StagedIsolatedMergeAction(
        merge_subject=task,
        merge_unit_id=unit.id,
        merge_branch=task.branch,
        pending_squash_reconcile=None,
        source_ref_sha="isolated-merge-oid",
        review_task=review,
        followup_findings=(),
        merge_action_metadata=action,
    )
    original_set_state = store.set_merge_unit_state
    store.set_merge_unit_state = MagicMock(side_effect=sqlite3.OperationalError("locked during finalization"))  # type: ignore[method-assign]

    with (
        patch("gza.cli.git_ops._stage_isolated_merge_action", return_value=staged),
        patch("gza.cli.git_ops._require_fresh_capped_review_lifecycle_authority"),
        patch("gza.cli.git_ops._promote_isolated_merge_to_target_branch", return_value=()),
        patch("gza.cli.git_ops.promote_candidate_integration_verify_evidence", return_value=SimpleNamespace()),
    ):
        first = _execute_merge_action(
            config,
            store,
            repo_git,
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
            merge_source=MERGE_SOURCE_WATCH,
        )

    assert first.rc == 1
    assert first.status == "isolated_post_promotion_merge_state_finalization_failed"
    assert "isolated merge-state finalization failed after target was already changed" in (first.block_reason or "")
    assert len(first.created_deferred_blockers) == 1
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None

    store.set_merge_unit_state = original_set_state  # type: ignore[method-assign]
    replay_git = _merge_executor_git(tmp_path, task.branch)
    replay_git.is_merged.return_value = True
    replay_git.rev_parse_if_exists = MagicMock(
        side_effect=lambda ref: "isolated-merge-oid" if ref == task.branch else "target-before" if ref == "main" else None
    )
    second = _execute_merge_action(
        config,
        store,
        repo_git,
        task,
        action,
        target_branch="main",
        current_branch="main",
        merge_git=replay_git,
        merge_current_branch="main",
        already_merged_behavior="mark_merged",
        merge_source=MERGE_SOURCE_WATCH,
    )

    assert second.rc == 0
    assert second.status == "already_merged"
    assert second.created_deferred_blockers == []
    assert [task.id for task in second.reused_deferred_blockers] == [
        task.id for task in first.created_deferred_blockers
    ]
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"
    assert refreshed_unit.merge_source == MERGE_SOURCE_MAX_CYCLES_DEFERRED


@pytest.mark.parametrize(
    ("metadata", "default_source", "expected_source"),
    [
        ({}, MERGE_SOURCE_WATCH, MERGE_SOURCE_WATCH),
        ({"type": "merge", "description": "Merge"}, MERGE_SOURCE_ADVANCE, MERGE_SOURCE_ADVANCE),
        (
            {"type": "merge", "max_cycles_merge_and_defer": True},
            MERGE_SOURCE_ADVANCE,
            MERGE_SOURCE_MAX_CYCLES_DEFERRED,
        ),
    ],
)
def test_finalize_staged_isolated_merge_action_uses_action_provenance_selector(
    tmp_path: Path,
    metadata: dict[str, object],
    default_source: str,
    expected_source: str,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Completed implementation", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = f"feature/{task.id}-isolated-finalize"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None

    created_blocker = store.add("Deferred blocker", task_type="implement", based_on=task.id)
    reused_blocker = store.add("Reused blocker", task_type="implement", based_on=task.id)
    if metadata.get("max_cycles_merge_and_defer") is True:
        review_output = _capped_review_output("B1", "B2")
        review = _completed_review(store, task, review_output)
        metadata = {
            **metadata,
            "review_task": review,
            "blocker_findings": (_blocker_finding("B1"), _blocker_finding("B2")),
            "persisted_review_output": review_output,
        }
    staged = _StagedIsolatedMergeAction(
        merge_subject=task,
        merge_unit_id=unit.id,
        merge_branch=task.branch,
        pending_squash_reconcile=None,
        review_task=None,
        followup_findings=(),
        created_investigation_task_ids=(),
        reused_investigation_task_ids=(),
        created_deferred_blockers=(created_blocker,),
        reused_deferred_blockers=(reused_blocker,),
        merge_action_metadata=metadata,
        source_ref=task.branch,
        source_ref_sha="source-sha",
        previous_target_sha="previous-target-sha",
        promoted_target_sha="promoted-target-sha",
    )

    result = _finalize_staged_isolated_merge_action(
        config,
        store,
        SimpleNamespace(rev_parse_if_exists=lambda ref: {"main": "promoted-target-sha"}.get(ref)),
        staged=staged,
        merge_source=default_source,
        quiet_mechanics=True,
    )

    assert result.rc == 0
    assert result.created_deferred_blockers == [created_blocker]
    assert result.reused_deferred_blockers == [reused_blocker]
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.merge_source == expected_source


def test_execute_merge_action_propagates_blocked_dirty_checkout_status(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Completed implementation", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/dirty-checkout-execute"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    git = SimpleNamespace(repo_dir=tmp_path)

    with patch(
        "gza.cli.git_ops._merge_single_task",
        return_value=_MergeSingleTaskResult(
            rc=1,
            status="blocked_dirty_checkout",
            block_reason="main checkout has uncommitted changes",
            pending_squash_reconcile=None,
        ),
    ):
        result = _execute_merge_action(
            config,
            store,
            git,
            task,
            {"type": "merge", "description": "Merge"},
            target_branch="main",
            current_branch="main",
        )

    assert result.rc == 1
    assert result.status == "blocked_dirty_checkout"
    assert result.block_reason == "main checkout has uncommitted changes"


def test_execute_merge_action_fail_closed_when_isolated_candidate_gate_checkout_is_unavailable(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\n")
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    store = make_store(tmp_path)

    task = store.add("Completed implementation", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/missing-isolated-checkout"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    git = SimpleNamespace(repo_dir=tmp_path)

    with patch("gza.cli.git_ops._merge_single_task") as merge_single:
        result = _execute_merge_action(
            config,
            store,
            git,
            task,
            {"type": "merge", "description": "Merge"},
            target_branch="main",
            current_branch="main",
        )

    merge_single.assert_not_called()
    assert result.rc == 1
    assert result.status == "blocked_candidate_verify_unavailable"
    assert result.block_reason == "isolated host merge checkout unavailable for pre-promotion candidate verify"
    assert "pre-promotion candidate verify requires the isolated host merge checkout" in capsys.readouterr().out


def test_execute_merge_action_isolated_candidate_verify_red_blocks_promotion(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\n")
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    store = make_store(tmp_path)

    task = store.add("Completed implementation", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/candidate-red"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    repo_git = MagicMock()
    repo_git.repo_dir = tmp_path
    repo_git.rev_parse_if_exists.side_effect = lambda ref: (
        "isolated-merge-oid" if ref == task.branch else "target-before" if ref == "main" else None
    )
    repo_git.rev_parse_if_exists.side_effect = lambda ref: (
        "isolated-merge-oid" if ref == task.branch else "target-before" if ref == "main" else None
    )

    merge_git = MagicMock()
    merge_git.repo_dir = config.main_checkout_integration_path
    merge_git.rev_parse.return_value = "isolated-merge-oid"

    red_check = SimpleNamespace(
        classification="deterministic_red",
        evidence=SimpleNamespace(
            verify_status="failed",
            head_sha="isolated-merge-oid",
            tree_fingerprint="fp-candidate",
            failure="verify_command failed",
            failing_phase="unit",
        ),
    )

    with (
        patch("gza.cli.git_ops._require_fresh_capped_review_lifecycle_authority"),
        patch(
            "gza.cli.git_ops._merge_single_task",
            return_value=_MergeSingleTaskResult(rc=0, authorized_source_ref_sha="isolated-merge-oid"),
        ),
        patch("gza.cli.git_ops.check_candidate_integration_verify", return_value=red_check),
        patch("gza.cli.git_ops._promote_isolated_merge_to_target_branch") as promote,
    ):
        result = _execute_merge_action(
            config,
            store,
            repo_git,
            task,
            {"type": "merge", "description": "Merge"},
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
        )

    assert result.rc == 1
    assert result.status == "blocked_candidate_verify"
    assert result.block_reason == "candidate verify red; refusing to promote while phase `unit` is failing"
    promote.assert_not_called()
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "unmerged"
    assert "candidate verify red; refusing to promote while phase `unit` is failing" in capsys.readouterr().out


def test_execute_merge_action_isolated_max_cycle_red_candidate_creates_no_debt(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\n")
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Isolated capped red", "feature/isolated-capped-red")
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    _set_review_head(store, review, "isolated-merge-oid")

    repo_git = MagicMock()
    repo_git.repo_dir = tmp_path
    repo_git.rev_parse_if_exists.side_effect = lambda ref: (
        "isolated-merge-oid" if ref == task.branch else "target-before" if ref == "main" else None
    )
    merge_git = MagicMock()
    merge_git.repo_dir = config.main_checkout_integration_path
    merge_git.rev_parse.return_value = "isolated-merge-oid"
    merge_git.rev_parse_if_exists.return_value = "isolated-merge-oid"
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="isolated-merge-oid")
    red_check = SimpleNamespace(
        classification="deterministic_red",
        evidence=SimpleNamespace(
            verify_status="failed",
            head_sha="isolated-merge-oid",
            tree_fingerprint="fp-candidate",
            failure="verify_command failed",
            failing_phase="unit",
        ),
    )

    with (
        patch("gza.cli.git_ops._require_fresh_capped_review_lifecycle_authority"),
        patch(
            "gza.cli.git_ops._merge_single_task",
            return_value=_MergeSingleTaskResult(rc=0, authorized_source_ref_sha="isolated-merge-oid"),
        ),
        patch("gza.cli.git_ops.check_candidate_integration_verify", return_value=red_check),
        patch("gza.cli.git_ops._promote_isolated_merge_to_target_branch") as promote,
        patch("gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks") as materialize,
    ):
        result = _execute_merge_action(
            config,
            store,
            repo_git,
            task,
            _max_cycle_merge_action(
                review,
                (_blocker_finding("B1"),),
                review_output,
                reviewed_head_sha="isolated-merge-oid",
            ),
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 1
    assert result.status == "blocked_candidate_verify"
    promote.assert_not_called()
    materialize.assert_not_called()
    assert [child for child in store.get_based_on_children(task.id) if child.task_type == "implement"] == []
    assert (store.get(task.id) or SimpleNamespace()).merge_status == "unmerged"


def test_execute_merge_action_isolated_max_cycle_refuses_when_target_advances_before_promotion(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\n")
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Isolated capped target race", "feature/isolated-capped-target-race")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    _set_review_head(store, review, "source-sha")
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="source-sha")
    action = _max_cycle_merge_action(
        review,
        (parse_review_report(review_output).findings[0],),
        review_output,
        reviewed_head_sha="source-sha",
    )

    repo_git = MagicMock()
    repo_git.repo_dir = tmp_path
    repo_git.rev_parse_if_exists.side_effect = lambda ref: (
        "source-sha" if ref == task.branch else "target-before" if ref == "main" else None
    )
    repo_git.rev_parse.side_effect = lambda ref: "target-after" if ref == "refs/heads/main" else "source-sha"
    repo_git.update_ref.side_effect = AssertionError("target ref must not update after target advances")

    merge_git = MagicMock()
    merge_git.repo_dir = config.main_checkout_integration_path
    merge_git.rev_parse.return_value = "isolated-merge-oid"
    merge_git.rev_parse_if_exists.side_effect = lambda ref: (
        "source-sha" if ref == task.branch else "isolated-merge-oid" if ref == "main" else None
    )

    with (
        patch("gza.cli.git_ops._require_fresh_capped_review_lifecycle_authority"),
        patch(
            "gza.cli.git_ops._merge_single_task",
            return_value=_MergeSingleTaskResult(rc=0, authorized_source_ref_sha="source-sha"),
        ),
        patch("gza.cli.git_ops.active_worktree_path_for_branch", return_value=None),
    ):
        result = _execute_merge_action(
            config,
            store,
            repo_git,
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 1
    assert result.status == "isolated_merge_failed"
    assert "changed after authorization" in (result.block_reason or "")
    repo_git.update_ref.assert_not_called()
    assert len(result.created_deferred_blockers) == 1
    children = [child for child in store.get_based_on_children(task.id) if child.task_type == "implement"]
    assert [child.id for child in children] == [result.created_deferred_blockers[0].id]
    assert not store.list_artifacts(task.id, kind="merge_finalization_attempt_proof")
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


def test_execute_merge_action_isolated_candidate_verify_unavailable_uses_distinct_status(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\n")
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    store = make_store(tmp_path)

    task = store.add("Completed implementation", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/candidate-unavailable"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    repo_git = MagicMock()
    repo_git.repo_dir = tmp_path

    merge_git = MagicMock()
    merge_git.repo_dir = config.main_checkout_integration_path
    merge_git.rev_parse.return_value = "isolated-merge-oid"

    unavailable_check = SimpleNamespace(
        classification="unavailable",
        evidence=SimpleNamespace(
            verify_status="unavailable",
            head_sha="isolated-merge-oid",
            tree_fingerprint="fp-candidate-unavailable",
            failure="verify command unavailable",
            failing_phase=None,
        ),
    )

    with (
        patch("gza.cli.git_ops._require_fresh_capped_review_lifecycle_authority"),
        patch(
            "gza.cli.git_ops._merge_single_task",
            return_value=_MergeSingleTaskResult(rc=0, authorized_source_ref_sha="isolated-merge-oid"),
        ),
        patch("gza.cli.git_ops.check_candidate_integration_verify", return_value=unavailable_check),
        patch("gza.cli.git_ops._compute_tree_fingerprint", return_value="fp-candidate-unavailable"),
        patch("gza.cli.git_ops._promote_isolated_merge_to_target_branch") as promote,
    ):
        result = _execute_merge_action(
            config,
            store,
            repo_git,
            task,
            {"type": "merge", "description": "Merge"},
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
        )

    assert result.rc == 1
    assert result.status == "blocked_candidate_verify_unavailable"
    assert result.block_reason == "candidate verify unavailable; refusing to promote without exact host proof"
    promote.assert_not_called()
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "unmerged"
    assert "candidate verify unavailable; refusing to promote without exact host proof" in capsys.readouterr().out


def test_execute_merge_action_isolated_candidate_verify_pass_promotes_and_persists_main_checkpoint(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\n")
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    store = make_store(tmp_path)

    task = store.add("Completed implementation", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/candidate-green"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    repo_git = MagicMock()
    repo_git.repo_dir = tmp_path

    merge_git = MagicMock()
    merge_git.repo_dir = config.main_checkout_integration_path
    merge_git.rev_parse.return_value = "isolated-merge-oid"
    merge_git.rev_parse_if_exists.return_value = "isolated-merge-oid"
    merge_git.current_branch.return_value = "main"

    with (
        patch("gza.cli.git_ops._merge_single_task", return_value=0),
        patch("gza.cli.git_ops._promote_isolated_merge_to_target_branch", return_value=()) as promote,
    ):
        result = _execute_merge_action(
            config,
            store,
            repo_git,
            task,
            {"type": "merge", "description": "Merge"},
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
        )

    assert result.rc == 0
    promote.assert_called_once_with(repo_git, merge_git, "main", expected_previous_target_sha=None)
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "merged"

    main_state = load_main_integration_verify_state(store)
    assert main_state is not None
    assert main_state.verify_status == "passed"
    assert main_state.head_sha == "isolated-merge-oid"
    assert main_state.tree_fingerprint == "fp-candidate"
    assert main_state.environment_identity is None


def test_execute_merge_action_isolated_max_cycle_materializes_debt_before_promotion(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\n")
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Isolated capped green", "feature/isolated-capped-green")
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    _set_review_head(store, review, "isolated-merge-oid")
    finding = _blocker_finding("B1")
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="isolated-merge-oid")
    order: list[str] = []

    repo_git = MagicMock()
    repo_git.repo_dir = tmp_path
    repo_git.rev_parse_if_exists.side_effect = lambda ref: (
        "isolated-merge-oid" if ref == task.branch else "target-before" if ref == "main" else None
    )
    merge_git = MagicMock()
    merge_git.repo_dir = config.main_checkout_integration_path
    merge_git.rev_parse.return_value = "isolated-merge-oid"
    merge_git.rev_parse_if_exists.return_value = "isolated-merge-oid"

    from gza.cli import git_ops as git_ops_module

    real_capped_creator = git_ops_module._create_or_reuse_capped_review_blocker_tasks

    def _materialize_side_effect(*args: object, **kwargs: object) -> tuple[list[Any], list[Any]]:
        order.append("defer")
        return real_capped_creator(*args, **kwargs)

    def _promote_side_effect(*_args: object, **_kwargs: object) -> tuple[str, ...]:
        order.append("promote")
        return ()

    with (
        patch("gza.cli.git_ops._require_fresh_capped_review_lifecycle_authority"),
        patch(
            "gza.cli.git_ops._merge_single_task",
            return_value=_MergeSingleTaskResult(rc=0, authorized_source_ref_sha="isolated-merge-oid"),
        ),
        patch(
            "gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks",
            side_effect=_materialize_side_effect,
        ),
        patch("gza.cli.git_ops._promote_isolated_merge_to_target_branch", side_effect=_promote_side_effect),
    ):
        result = _execute_merge_action(
            config,
            store,
            repo_git,
            task,
            _max_cycle_merge_action(review, (finding,), review_output, reviewed_head_sha="isolated-merge-oid"),
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 0
    assert order == ["defer", "promote"]
    assert len(result.created_deferred_blockers) == 1
    assert (store.get(task.id) or SimpleNamespace()).merge_status == "merged"


def test_execute_merge_action_isolated_max_cycle_source_move_after_staging_creates_no_debt_or_state(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\n")
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Isolated capped source race", "feature/isolated-capped-source-race")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    _set_review_head(store, review, "source-sha")
    action = _max_cycle_merge_action(
        review,
        (_blocker_finding("B1"),),
        review_output,
        reviewed_head_sha="source-sha",
    )
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="source-sha")

    repo_git = MagicMock()
    repo_git.repo_dir = tmp_path
    repo_git.rev_parse_if_exists.side_effect = lambda ref: (
        "source-after" if ref == task.branch else "target-before" if ref == "main" else None
    )
    merge_git = MagicMock()
    merge_git.repo_dir = config.main_checkout_integration_path
    merge_git.rev_parse.return_value = "isolated-merge-oid"
    merge_git.rev_parse_if_exists.side_effect = lambda ref: (
        "source-sha" if ref == task.branch else "candidate-head" if ref == "main" else None
    )

    with (
        patch("gza.cli.git_ops._require_fresh_capped_review_lifecycle_authority"),
        patch(
            "gza.cli.git_ops._merge_single_task",
            return_value=_MergeSingleTaskResult(rc=0, authorized_source_ref_sha="source-sha"),
        ),
        patch(
            "gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks",
            side_effect=AssertionError("deferred blockers must not materialize after source movement"),
        ),
        patch("gza.cli.git_ops._promote_isolated_merge_to_target_branch") as promote,
    ):
        result = _execute_merge_action(
            config,
            store,
            repo_git,
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 1
    assert result.created_deferred_blockers == []
    assert result.reused_deferred_blockers == []
    assert "source no longer matches reviewed head" in (result.block_reason or "")
    promote.assert_not_called()
    assert not store.list_artifacts(task.id, kind="merge_finalization_prepared_attempt")
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None
    assert [child for child in store.get_based_on_children(task.id) if child.task_type == "implement"] == []


@pytest.mark.parametrize("executor_path", ["non-isolated", "already-merged", "isolated-single"])
@pytest.mark.parametrize("race", ["blank-verify-no-current-evidence", "blank-verify-source-moved"])
def test_max_cycle_blank_verify_refuses_before_debt_or_state(
    tmp_path: Path,
    executor_path: str,
    race: str,
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    if executor_path == "isolated-single":
        config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\n")
    config = Config.load(tmp_path)
    config.verify_command = ""
    store = make_store(tmp_path)
    task = _completed_merge_task(
        store,
        f"Blank verify capped {executor_path} {race}",
        f"feature/blank-verify-capped-{executor_path}-{race}",
    )
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    _set_review_head(store, review, "source-sha")
    action = _max_cycle_merge_action(
        review,
        (parse_review_report(review_output).findings[0],),
        review_output,
        reviewed_head_sha="source-sha",
    )
    live_source_sha = "source-after" if race == "blank-verify-source-moved" else "source-sha"

    git = MagicMock()
    git.repo_dir = tmp_path
    git.rev_parse_if_exists.side_effect = lambda ref: (
        live_source_sha if ref == task.branch else "target-before" if ref == "main" else None
    )
    merge_git = _merge_executor_git(tmp_path, str(task.branch))
    merge_git.rev_parse_if_exists.side_effect = lambda ref: (
        "source-sha" if ref == task.branch else "target-before" if ref == "main" else None
    )
    merge_git.is_merged.return_value = executor_path == "already-merged"

    with (
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch(
            "gza.cli.git_ops._merge_single_task",
            return_value=_MergeSingleTaskResult(rc=0, authorized_source_ref_sha="source-sha"),
        ) as merge_single,
        patch(
            "gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks",
            side_effect=AssertionError("blank verify capped action must not create deferred blockers"),
        ),
        patch("gza.cli.git_ops._promote_isolated_merge_to_target_branch") as promote,
    ):
        result = _execute_merge_action(
            config,
            store,
            git if executor_path == "isolated-single" else merge_git,
            task,
            action,
            target_branch="main",
            current_branch="main",
            merge_git=merge_git if executor_path == "isolated-single" else None,
            merge_current_branch="main" if executor_path == "isolated-single" else None,
            already_merged_behavior="mark_merged" if executor_path == "already-merged" else "error",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 1
    assert result.created_deferred_blockers == []
    assert result.reused_deferred_blockers == []
    if executor_path != "isolated-single":
        merge_single.assert_not_called()
    promote.assert_not_called()
    assert [child for child in store.get_based_on_children(task.id) if child.task_type == "implement"] == []
    assert not store.list_artifacts(task.id, kind="merge_finalization_prepared_attempt")
    assert not store.list_artifacts(task.id, kind="merge_finalization_attempt_proof")
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


def test_execute_merge_action_isolated_max_cycle_source_move_during_materialization_preserves_debt_without_promotion(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\n")
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Isolated capped materialization race", "feature/isolated-capped-materialize-race")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    _set_review_head(store, review, "source-sha")
    finding = parse_review_report(review_output).findings[0]
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="source-sha")

    source_reads = ["source-sha", "source-sha", "source-after"]
    repo_git = MagicMock()
    repo_git.repo_dir = tmp_path
    repo_git.rev_parse_if_exists.side_effect = lambda ref: (
        source_reads.pop(0) if ref == task.branch else "target-before" if ref == "main" else None
    )
    merge_git = MagicMock()
    merge_git.repo_dir = config.main_checkout_integration_path
    merge_git.rev_parse.return_value = "isolated-merge-oid"
    merge_git.rev_parse_if_exists.side_effect = lambda ref: (
        "source-sha" if ref == task.branch else "isolated-merge-oid" if ref == "main" else None
    )

    from gza.cli import git_ops as git_ops_module

    real_capped_creator = git_ops_module._create_or_reuse_capped_review_blocker_tasks

    with (
        patch("gza.cli.git_ops._require_fresh_capped_review_lifecycle_authority"),
        patch(
            "gza.cli.git_ops._merge_single_task",
            return_value=_MergeSingleTaskResult(rc=0, authorized_source_ref_sha="source-sha"),
        ),
        patch("gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks", side_effect=real_capped_creator),
        patch("gza.cli.git_ops._promote_isolated_merge_to_target_branch") as promote,
    ):
        result = _execute_merge_action(
            config,
            store,
            repo_git,
            task,
            _max_cycle_merge_action(review, (finding,), review_output, reviewed_head_sha="source-sha"),
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 1
    assert "verify evidence is no longer current and passing" in (result.block_reason or "")
    assert result.created_deferred_blockers == []
    assert result.reused_deferred_blockers == []
    promote.assert_not_called()
    children = [child for child in store.get_based_on_children(task.id) if child.task_type == "implement"]
    assert children == []
    assert not store.list_artifacts(task.id, kind="merge_finalization_prepared_attempt")
    assert not store.list_artifacts(task.id, kind="merge_finalization_attempt_proof")
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


def test_execute_merge_action_isolated_followup_source_move_during_materialization_preserves_child_without_promotion(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\n")
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Isolated follow-up materialization race", "feature/isolated-followup-materialize-race")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review = _completed_review(store, task, "## Review\n\nVerdict: APPROVED_WITH_FOLLOWUPS\n")
    followup = _followup_finding("F1")

    source_reads = ["source-after"]
    repo_git = MagicMock()
    repo_git.repo_dir = tmp_path
    repo_git.rev_parse_if_exists.side_effect = lambda ref: (
        source_reads.pop(0) if ref == task.branch else "target-before" if ref == "main" else None
    )
    merge_git = MagicMock()
    merge_git.repo_dir = config.main_checkout_integration_path
    merge_git.rev_parse.return_value = "isolated-merge-oid"
    merge_git.rev_parse_if_exists.side_effect = lambda ref: (
        "same-sha" if ref == task.branch else "isolated-merge-oid" if ref == "main" else None
    )

    with (
        patch(
            "gza.cli.git_ops._merge_single_task",
            return_value=_MergeSingleTaskResult(rc=0, authorized_source_ref_sha="same-sha"),
        ),
        patch("gza.cli.git_ops._promote_isolated_merge_to_target_branch") as promote,
    ):
        result = _execute_merge_action(
            config,
            store,
            repo_git,
            task,
            {
                "type": "merge_with_followups",
                "description": "Merge with follow-up",
                "review_task": review,
                "followup_findings": (followup,),
            },
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 1
    assert result.status == "merge_side_effect_materialization_failed"
    assert "merge source ref changed after lifecycle authorization" in (result.block_reason or "")
    assert len(result.created_followups) == 1
    promote.assert_not_called()
    children = [child for child in store.get_based_on_children(review.id) if child.task_type == "implement"]
    assert [child.id for child in children] == [result.created_followups[0].id]
    assert not store.list_artifacts(task.id, kind="merge_finalization_prepared_attempt")
    assert not store.list_artifacts(task.id, kind="merge_finalization_attempt_proof")
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


def test_execute_merge_action_isolated_max_cycle_materialization_failure_prevents_promotion_and_state(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\n")
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Isolated capped failure", "feature/isolated-capped-failure")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    _set_review_head(store, review, "isolated-merge-oid")
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="isolated-merge-oid")

    repo_git = MagicMock()
    repo_git.repo_dir = tmp_path
    repo_git.rev_parse_if_exists.side_effect = lambda ref: (
        "isolated-merge-oid" if ref == task.branch else "target-before" if ref == "main" else None
    )
    merge_git = MagicMock()
    merge_git.repo_dir = config.main_checkout_integration_path
    merge_git.rev_parse.return_value = "isolated-merge-oid"
    merge_git.rev_parse_if_exists.return_value = "isolated-merge-oid"

    with (
        patch("gza.cli.git_ops._require_fresh_capped_review_lifecycle_authority"),
        patch(
            "gza.cli.git_ops._merge_single_task",
            return_value=_MergeSingleTaskResult(rc=0, authorized_source_ref_sha="isolated-merge-oid"),
        ),
        patch(
            "gza.cli.git_ops._create_or_reuse_capped_review_blocker_tasks",
            side_effect=RuntimeError("database is locked"),
        ),
        patch("gza.cli.git_ops._promote_isolated_merge_to_target_branch") as promote,
    ):
        result = _execute_merge_action(
            config,
            store,
            repo_git,
            task,
            _max_cycle_merge_action(
                review,
                (_blocker_finding("B1"),),
                review_output,
                reviewed_head_sha="isolated-merge-oid",
            ),
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.rc == 1
    assert result.status == "deferred_blocker_materialization_failed"
    promote.assert_not_called()
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


def test_execute_merge_action_isolated_max_cycle_replay_reuses_existing_debt(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\n")
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Isolated capped replay", "feature/isolated-capped-replay")
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    _set_review_head(store, review, "isolated-merge-oid")
    _persist_capped_authorization_verify(store, config, task, tmp_path=tmp_path, head_sha="isolated-merge-oid")

    repo_git = MagicMock()
    repo_git.repo_dir = tmp_path
    repo_git.rev_parse_if_exists.side_effect = lambda ref: (
        "isolated-merge-oid" if ref == task.branch else "target-before" if ref == "main" else None
    )
    merge_git = MagicMock()
    merge_git.repo_dir = config.main_checkout_integration_path
    merge_git.rev_parse.return_value = "isolated-merge-oid"
    merge_git.rev_parse_if_exists.return_value = "isolated-merge-oid"

    with (
        patch("gza.cli.git_ops._require_fresh_capped_review_lifecycle_authority"),
        patch(
            "gza.cli.git_ops._merge_single_task",
            return_value=_MergeSingleTaskResult(rc=0, authorized_source_ref_sha="isolated-merge-oid"),
        ),
        patch(
            "gza.cli.git_ops._promote_isolated_merge_to_target_branch",
            side_effect=[GitError("promotion failed after debt"), ()],
        ),
    ):
        first = _execute_merge_action(
            config,
            store,
            repo_git,
            task,
            _max_cycle_merge_action(
                review,
                (_blocker_finding("B1"),),
                review_output,
                reviewed_head_sha="isolated-merge-oid",
            ),
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )
        second = _execute_merge_action(
            config,
            store,
            repo_git,
            task,
            _max_cycle_merge_action(
                review,
                (_blocker_finding("B1"),),
                review_output,
                reviewed_head_sha="isolated-merge-oid",
            ),
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert first.rc == 1
    assert len(first.created_deferred_blockers) == 1
    children = [child for child in store.get_based_on_children(task.id) if child.task_type == "implement"]
    assert len(children) == 1
    assert first.created_deferred_blockers[0].id == children[0].id
    assert second.rc == 0
    assert second.created_deferred_blockers == []
    assert [task.id for task in second.reused_deferred_blockers] == [children[0].id]


@pytest.mark.parametrize(
    ("observed_target_oid", "expected_status", "expected_state"),
    [
        ("isolated-merge-oid", "merged", "merged"),
        ("old-oid", "isolated_merge_failed", "unmerged"),
        ("unrelated-oid", "isolated_promotion_rollback_failed_target_uncertain", "unmerged"),
    ],
)
def test_execute_merge_action_classifies_isolated_promotion_rollback_failure_by_reread_target(
    tmp_path: Path,
    observed_target_oid: str,
    expected_status: str,
    expected_state: str,
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\n")
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Isolated rollback classification", "feature/isolated-rollback-classify")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    repo_git = MagicMock()
    repo_git.repo_dir = tmp_path
    repo_git.rev_parse_if_exists.return_value = observed_target_oid
    merge_git = MagicMock()
    merge_git.repo_dir = config.main_checkout_integration_path
    merge_git.rev_parse.return_value = "isolated-merge-oid"
    merge_git.rev_parse_if_exists.return_value = "isolated-merge-oid"
    staged = _StagedIsolatedMergeAction(
        merge_subject=task,
        merge_unit_id=unit.id,
        merge_branch=task.branch,
        pending_squash_reconcile=None,
        review_task=None,
        followup_findings=(),
        merge_action_metadata={"type": "merge", "description": "Merge"},
    )
    rollback_error = _IsolatedPromotionRollbackFailed(
        "rollback update-ref failed",
        target_branch="main",
        previous_target_oid="old-oid",
        candidate_oid="isolated-merge-oid",
    )

    with (
        patch("gza.cli.git_ops._stage_isolated_merge_action", return_value=staged),
        patch("gza.cli.git_ops._promote_isolated_merge_to_target_branch", side_effect=rollback_error),
        patch("gza.cli.git_ops.promote_candidate_integration_verify_evidence", return_value=SimpleNamespace()),
    ):
        result = _execute_merge_action(
            config,
            store,
            repo_git,
            task,
            {"type": "merge", "description": "Merge"},
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
            merge_source=MERGE_SOURCE_ADVANCE,
        )

    assert result.status == expected_status
    assert result.rc == (0 if expected_state == "merged" else 1)
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == expected_state
    if observed_target_oid == "isolated-merge-oid":
        assert "rollback failed after target advanced to verified candidate" in result.promotion_warnings[0]
    else:
        assert "rollback update-ref failed" in (result.block_reason or "")


def test_execute_merge_action_isolated_candidate_verify_red_defers_followup_creation_until_promotion(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\n")
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    store = make_store(tmp_path)

    task = store.add("Completed implementation", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/candidate-followup-blocked"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: APPROVED**"
    store.update(review)

    followup = ReviewFinding(
        id="F1",
        severity="FOLLOWUP",
        title="Tighten coverage",
        body="Body",
        evidence=None,
        impact=None,
        fix_or_followup="add coverage",
        tests=None,
        open_state_citation="citation",
    )

    repo_git = MagicMock()
    repo_git.repo_dir = tmp_path

    merge_git = MagicMock()
    merge_git.repo_dir = config.main_checkout_integration_path
    merge_git.rev_parse.return_value = "isolated-merge-oid"

    red_check = SimpleNamespace(
        classification="deterministic_red",
        evidence=SimpleNamespace(
            verify_status="failed",
            head_sha="isolated-merge-oid",
            tree_fingerprint="fp-candidate",
            failure="verify_command failed",
            failing_phase="unit",
        ),
    )

    before_ids = {candidate.id for candidate in store.get_all() if candidate.id is not None}
    with (
        patch("gza.cli.git_ops._merge_single_task", return_value=0),
        patch("gza.cli.git_ops.check_candidate_integration_verify", return_value=red_check),
        patch("gza.cli.git_ops._promote_isolated_merge_to_target_branch") as promote,
    ):
        result = _execute_merge_action(
            config,
            store,
            repo_git,
            task,
            {
                "type": "merge_with_followups",
                "description": "Merge",
                "review_task": review,
                "followup_findings": (followup,),
            },
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
        )

    assert result.rc == 1
    assert result.status == "blocked_candidate_verify"
    promote.assert_not_called()
    after_ids = {candidate.id for candidate in store.get_all() if candidate.id is not None}
    assert after_ids == before_ids


def test_execute_merge_action_isolated_whitespace_only_verify_command_keeps_no_gate_path(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + 'main_checkout_isolate: true\nverify_command: "   "\n')
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Completed implementation", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/whitespace-no-gate"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    repo_git = MagicMock()
    repo_git.repo_dir = tmp_path

    merge_git = MagicMock()
    merge_git.repo_dir = config.main_checkout_integration_path
    merge_git.current_branch.return_value = "main"

    with (
        patch("gza.cli.git_ops._merge_single_task", return_value=0),
        patch("gza.cli.git_ops._promote_isolated_merge_to_target_branch", return_value=()) as promote,
        patch(
            "gza.cli.git_ops.check_candidate_integration_verify",
            side_effect=AssertionError("candidate verify should stay disabled for whitespace-only verify_command"),
        ),
    ):
        result = _execute_merge_action(
            config,
            store,
            repo_git,
            task,
            {"type": "merge", "description": "Merge"},
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
        )

    assert result.rc == 0
    assert result.status == "merged"
    assert result.candidate_verify is None
    promote.assert_called_once_with(repo_git, merge_git, "main", expected_previous_target_sha=None)
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "merged"

    main_state = load_main_integration_verify_state(store)
    assert main_state is None


def test_run_task_backed_rebase_surfaces_resolution_warnings_and_preserves_existing_merge_unit_provenance(
    tmp_path, capsys
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None
    unit = store.resolve_merge_unit_for_task(parent.id)
    assert unit is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.return_value = None
    worktree_git.rev_parse.return_value = "head-new"
    worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-new",
        "main": "base-new",
        "origin/feature/rebased": "head-new",
    }.get(ref)

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline(
                old_tip="head-old",
                target_at_start="base-old",
                merge_base_at_start="merge-base",
            ),
        ),
        patch(
            "gza.cli.git_ops.resolve_ref_if_possible",
            side_effect=[
                ResolvedGitRef(None, "unexpected error resolving ref 'feature/rebased': boom"),
                ResolvedGitRef(None, "unexpected error resolving ref 'main': boom"),
            ],
        ),
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 0
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.head_sha == "head-new"
    assert refreshed_unit.base_sha == "base-new"
    worktree_git.push_force_with_lease.assert_not_called()
    output = capsys.readouterr()
    assert "unexpected error resolving ref 'feature/rebased': boom" in output.err
    assert "unexpected error resolving ref 'main': boom" in output.err


def test_run_task_backed_rebase_preserves_review_state_when_diff_is_unchanged(tmp_path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    parent.review_cleared_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None

    review = store.add("Review feature", task_type="review", depends_on=parent.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    review.review_verify_status = "failed"
    review.review_verify_branch = "feature/rebased"
    review.review_verify_head_sha = "head-old"
    store.update(review)

    improve = store.add(
        "No-op improve",
        task_type="improve",
        based_on=parent.id,
        depends_on=review.id,
        same_branch=True,
    )
    assert improve.id is not None
    improve.status = "completed"
    improve.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    improve.branch = "feature/rebased"
    improve.changed_diff = False
    improve.review_verify_status = "passed"
    improve.review_verify_branch = "feature/rebased"
    improve.review_verify_head_sha = "head-old"
    improve.review_verify_captured_at = datetime(2026, 5, 10, 11, 0, 1, tzinfo=UTC)
    store.update(improve)

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.return_value = None
    worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-new",
        "main": "base-new",
    }.get(ref)

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline(
                old_tip="head-old",
                target_at_start="base-old",
                merge_base_at_start="merge-base",
            ),
        ),
        patch(
            "gza.cli.git_ops.compute_rebase_changed_diff",
            return_value=RebaseDiffResult(changed_diff=False, detail="no (review can be preserved)"),
        ),
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 0
    refreshed_parent = store.get(parent.id)
    assert refreshed_parent is not None
    assert refreshed_parent.review_cleared_at == parent.review_cleared_at
    refreshed_review = store.get(review.id)
    assert refreshed_review is not None
    assert refreshed_review.review_verify_head_sha == "head-new"
    refreshed_improve = store.get(improve.id)
    assert refreshed_improve is not None
    assert refreshed_improve.review_verify_head_sha == "head-new"
    refreshed_rebase = store.get(rebase_task.id)
    assert refreshed_rebase is not None
    assert refreshed_rebase.changed_diff is False


def test_run_task_backed_rebase_invalidates_review_state_when_diff_changes(tmp_path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    parent.review_cleared_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.return_value = None
    worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-new",
        "main": "base-new",
    }.get(ref)

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline(
                old_tip="head-old",
                target_at_start="base-old",
                merge_base_at_start="merge-base",
            ),
        ),
        patch(
            "gza.cli.git_ops.compute_rebase_changed_diff",
            return_value=RebaseDiffResult(changed_diff=True, detail="yes (review must be refreshed)"),
        ),
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 0
    refreshed_parent = store.get(parent.id)
    assert refreshed_parent is not None
    assert refreshed_parent.review_cleared_at is None
    refreshed_rebase = store.get(rebase_task.id)
    assert refreshed_rebase is not None
    assert refreshed_rebase.changed_diff is True


def test_run_task_backed_rebase_persists_provenance_over_inherited_custom_review_scope(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    parent.review_scope = "Review only the custom lifecycle slice."
    parent.status = "completed"
    parent.branch = "feature/rebased"
    store.update(parent)
    assert parent.id is not None

    rebase_task = store.add(
        "Rebase feature",
        task_type="rebase",
        based_on=parent.id,
        same_branch=True,
        review_scope=parent.review_scope,
    )
    rebase_task.branch = parent.branch
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.return_value = None
    worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-new",
        "main": "base-new",
    }.get(ref)

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline(
                old_tip="head-old",
                target_at_start="base-old",
                merge_base_at_start="merge-base",
            ),
        ),
        patch(
            "gza.cli.git_ops.compute_rebase_changed_diff",
            return_value=RebaseDiffResult(changed_diff=True, detail="yes (review must be refreshed)"),
        ),
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 0
    refreshed_rebase = store.get(rebase_task.id)
    assert refreshed_rebase is not None
    provenance = parse_rebase_diff_provenance(refreshed_rebase.review_scope)
    assert provenance is not None
    assert provenance.old_tip == "head-old"
    assert provenance.target_at_start == "base-old"
    assert provenance.merge_base_at_start == "merge-base"
    assert provenance.resolved_head_sha == "head-new"
    assert provenance.resolved_target_sha == "base-new"


def test_run_task_backed_rebase_passes_managed_roots_to_cleanup(tmp_path: Path) -> None:
    """Foreground rebase setup should constrain branch cleanup to managed roots."""
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "interactive_worktree_dir: interactive-worktrees\n")
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    parent.status = "completed"
    parent.completed_at = datetime.now(UTC)
    parent.branch = "feature/rebased"
    store.update(parent)

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = parent.branch
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git._run.return_value = None
    repo_git.worktree_remove.return_value = None

    worktree_git = MagicMock()
    worktree_git.rebase.return_value = None
    worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-new",
        "main": "base-new",
    }.get(ref)

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None) as mock_cleanup,
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 0
    mock_cleanup.assert_called_once_with(
        repo_git,
        "feature/rebased",
        force=True,
        permitted_root_paths=managed_worktree_root_paths(config),
    )


def test_run_task_backed_rebase_provider_cleanup_does_not_prune_or_remove_other_registrations(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    parent.status = "completed"
    parent.completed_at = datetime.now(UTC)
    parent.branch = "feature/rebased"
    store.update(parent)

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = parent.branch
    rebase_task.slug = "rebase-feature"
    store.update(rebase_task)

    canonical_worktree = config.worktree_path / str(rebase_task.id)
    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git._run.return_value = None
    repo_git.worktree_remove.return_value = None
    repo_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-new",
        "main": "base-new",
    }.get(ref)

    worktree_git = MagicMock()
    worktree_git.rebase.side_effect = GitError("rebase boom")
    worktree_git.rebase_abort.return_value = None

    private_checkout = SimpleNamespace(path=tmp_path / "isolated-checkout")

    @contextmanager
    def _isolated_checkout_cm(**kwargs):
        yield private_checkout

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline(
                old_tip="head-old",
                target_at_start="base-old",
                merge_base_at_start="merge-base",
            ),
        ),
        patch(
            "gza.cli.git_ops.compute_rebase_changed_diff",
            return_value=RebaseDiffResult(changed_diff=True, detail="yes (review must be refreshed)"),
        ),
        patch(
            "gza.cli.git_ops.reconcile_task_branch_merge_truth",
            return_value=SimpleNamespace(warnings=[], skipped_reason=None, errors=[]),
        ),
        patch("gza.cli.git_ops.isolated_rebase_checkout", side_effect=_isolated_checkout_cm),
        patch("gza.cli.git_ops.invoke_provider_resolve", return_value=True),
        patch("gza.cli.git_ops.import_isolated_rebase_tip"),
        patch("gza.cli.git_ops.remove_worktree_registration_for_path") as remove_worktree_registration_for_path,
        patch("gza.cli.git_ops.publish_rebased_branch"),
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 0
    repo_git.worktree_remove.assert_called_once_with(canonical_worktree, force=True)
    remove_worktree_registration_for_path.assert_not_called()
    assert call("worktree", "prune") not in repo_git._run.call_args_list


def test_checkout_passes_managed_roots_to_cleanup(tmp_path: Path) -> None:
    """Checkout should pass both task and configured interactive roots to cleanup."""
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "interactive_worktree_dir: interactive-worktrees\n")
    store = make_store(tmp_path)
    task = store.add("Checkout feature", task_type="implement")
    task.branch = "feature/checkout-roots"
    store.update(task)

    git = MagicMock()
    git.branch_exists.return_value = True

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None) as mock_cleanup,
    ):
        result = invoke_gza("checkout", str(task.id), "--project", str(tmp_path))

    assert result.returncode == 0
    config = Config.load(tmp_path)
    mock_cleanup.assert_called_once_with(
        git,
        "feature/checkout-roots",
        force=False,
        permitted_root_paths=managed_worktree_root_paths(config),
    )


def test_remove_watch_merge_checkout_deregisters_only_managed_checkout(tmp_path: Path) -> None:
    """Managed watch cleanup must not prune unrelated prunable worktree registrations."""
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    checkout_path = config.main_checkout_integration_path
    checkout_path.mkdir(parents=True)
    (checkout_path / "tracked.txt").write_text("watch checkout", encoding="utf-8")

    foreign_worktree = tmp_path.parent / "inline-worktree"
    common_dir = tmp_path / ".git"
    managed_registration = _create_worktree_registration(
        common_dir,
        name="watch-main",
        worktree_path=checkout_path,
    )
    foreign_registration = _create_worktree_registration(
        common_dir,
        name="inline-feature",
        worktree_path=foreign_worktree,
    )

    git = MagicMock(spec=Git)
    git.worktree_list.return_value = [
        {
            "path": str(foreign_worktree),
            "branch": "refs/heads/feature/inline",
            "detached": False,
            "prunable": "gitdir file points to non-existent location",
        }
    ]
    git._run.return_value = SimpleNamespace(stdout=str(common_dir), returncode=0, stderr="")

    _remove_watch_merge_checkout(git, checkout_path)

    git.worktree_remove.assert_called_once_with(checkout_path, force=True)
    assert not managed_registration.exists()
    assert foreign_registration.exists()
    assert not checkout_path.exists()
    assert call("worktree", "prune", "--expire", "now", check=False) not in git._run.call_args_list


def test_ensure_watch_main_checkout_recreates_prunable_registration_without_pruning_foreign_one(
    tmp_path: Path,
) -> None:
    """Reviving the isolated watch checkout must only remove its own stale registration."""
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    checkout_path = config.main_checkout_integration_path
    foreign_worktree = tmp_path.parent / "inline-worktree"
    common_dir = tmp_path / ".git"
    managed_registration = _create_worktree_registration(
        common_dir,
        name="watch-main",
        worktree_path=checkout_path,
    )
    foreign_registration = _create_worktree_registration(
        common_dir,
        name="inline-feature",
        worktree_path=foreign_worktree,
    )

    prunable_managed_entry = {
        "path": str(checkout_path),
        "branch": None,
        "detached": True,
        "prunable": "gitdir file points to non-existent location",
    }
    prunable_foreign_entry = {
        "path": str(foreign_worktree),
        "branch": "refs/heads/feature/inline",
        "detached": False,
        "prunable": "gitdir file points to non-existent location",
    }
    recreated_entry = {
        "path": str(checkout_path),
        "branch": None,
        "detached": True,
        "prunable": False,
    }

    git = MagicMock(spec=Git)
    git.worktree_list.side_effect = [
        [prunable_managed_entry, prunable_foreign_entry],
        [prunable_foreign_entry],
        [prunable_foreign_entry, recreated_entry],
    ]

    def _run(*args, **kwargs):
        if args == ("rev-parse", "--git-common-dir"):
            return SimpleNamespace(stdout=str(common_dir), returncode=0, stderr="")
        raise AssertionError(f"unexpected git._run call: {args!r}")

    git._run.side_effect = _run

    workspace_git = MagicMock()
    workspace_git.current_branch.return_value = "HEAD"
    workspace_git.has_changes.return_value = False

    with patch("gza.cli.git_ops.Git", return_value=workspace_git) as git_cls:
        isolated_git = ensure_watch_main_checkout(config, git, "main")

    assert isolated_git is workspace_git
    git_cls.assert_called_once_with(checkout_path)
    assert not managed_registration.exists()
    assert foreign_registration.exists()
    assert call("worktree", "prune", "--expire", "now", check=False) not in git._run.call_args_list
    git.worktree_add_existing.assert_called_once_with(checkout_path, "main", detach=True)
    workspace_git.checkout_detached.assert_called_once_with("main")
    workspace_git.reset_hard.assert_called_once_with("main")
    workspace_git.clean_force.assert_called_once_with()


def test_ensure_watch_main_checkout_builds_child_git_with_parent_env(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    checkout_path = config.main_checkout_integration_path
    parent_env = {"PATH": "/project/bin", "TOKEN": "owned"}
    git = MagicMock(spec=Git)
    git.env = parent_env
    git.worktree_list.side_effect = [
        [],
        [{"path": str(checkout_path), "branch": None, "detached": True}],
    ]
    workspace_git = MagicMock()
    workspace_git.current_branch.return_value = "HEAD"
    workspace_git.has_changes.return_value = False

    with patch("gza.cli.git_ops.Git", return_value=workspace_git) as git_cls:
        isolated_git = ensure_watch_main_checkout(config, git, "main")

    assert isolated_git is workspace_git
    git_cls.assert_called_once_with(checkout_path, env=parent_env)
    workspace_git.checkout_detached.assert_called_once_with("main")
    workspace_git.reset_hard.assert_called_once_with("main")
    workspace_git.clean_force.assert_called_once_with()


def test_run_task_backed_rebase_reconciles_parent_merge_status_when_rebased_branch_is_already_in_target(
    tmp_path,
    capsys,
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_text = config_path.read_text()
    config_path.write_text(config_text + "require_review_before_merge: false\n")
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    parent.review_cleared_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None
    unit = store.resolve_merge_unit_for_task(parent.id)
    assert unit is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.return_value = None
    worktree_git.branch_exists.return_value = True
    worktree_git.is_merged.return_value = True
    worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "shared-sha",
        "main": "shared-sha",
    }.get(ref)

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
        patch(
            "gza.cli.git_ops.compute_rebase_changed_diff",
            return_value=RebaseDiffResult(changed_diff=True, detail="yes (review must be refreshed)"),
        ),
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 0
    refreshed_parent = store.get(parent.id)
    assert refreshed_parent is not None
    assert refreshed_parent.merge_status == "merged"
    assert refreshed_parent.merged_at is not None
    assert refreshed_parent.review_cleared_at is None
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"
    assert refreshed_unit.merged_at == refreshed_parent.merged_at
    refreshed_rebase = store.get(rebase_task.id)
    assert refreshed_rebase is not None
    assert refreshed_rebase.status == "completed"

    advance_git = MagicMock(spec=Git)
    advance_git.repo_dir = tmp_path
    advance_git.default_branch.return_value = "main"
    advance_git.current_branch.return_value = "main"

    args = _advance_args(tmp_path, parent.id)
    args.dry_run = True
    with patch("gza.cli.git_ops.Git", return_value=advance_git):
        advance_rc = cmd_advance(args)

    output = capsys.readouterr().out
    assert advance_rc == 0
    assert f"Task {parent.id} is already merged" in output
    assert "Create review" not in output
    assert "Merge" not in output
    assert "needs_rebase" not in output


def test_run_task_backed_rebase_remote_uses_local_target_ref_for_merge_proof(tmp_path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None
    unit = store.resolve_merge_unit_for_task(parent.id)
    assert unit is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.fetch.return_value = None
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.return_value = None
    worktree_git.branch_exists.return_value = True

    def _is_merged(branch, into):
        return into == "main"

    worktree_git.is_merged.side_effect = _is_merged
    worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-remote",
        "origin/main": "base-origin",
        "main": "base-local-stale",
    }.get(ref)

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
        patch(
            "gza.cli.git_ops.compute_rebase_changed_diff",
            return_value=RebaseDiffResult(changed_diff=True, detail="yes (review must be refreshed)"),
        ),
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
            remote=True,
        )

    assert rc == 0
    worktree_git.rebase.assert_called_once_with("origin/main")
    worktree_git.is_merged.assert_called_once_with("feature/rebased", into="main")

    refreshed_parent = store.get(parent.id)
    assert refreshed_parent is not None
    assert refreshed_parent.merge_status == "merged"
    assert refreshed_parent.merged_at is not None

    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"
    assert refreshed_unit.target_branch == "main"
    assert refreshed_unit.base_sha == "base-local-stale"


def test_run_task_backed_rebase_remote_does_not_mark_merged_from_stale_local_target(tmp_path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None
    unit = store.resolve_merge_unit_for_task(parent.id)
    assert unit is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.fetch.return_value = None
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.return_value = None
    worktree_git.branch_exists.return_value = True
    worktree_git.get_diff_numstat.return_value = "2\t1\tfeature.txt\n"

    def _is_merged(branch, into):
        return into == "origin/main"

    worktree_git.is_merged.side_effect = _is_merged
    worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-remote",
        "origin/main": "base-origin",
        "main": "base-local-stale",
    }.get(ref)

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
        patch(
            "gza.cli.git_ops.compute_rebase_changed_diff",
            return_value=RebaseDiffResult(changed_diff=True, detail="yes (review must be refreshed)"),
        ),
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
            remote=True,
        )

    assert rc == 0
    worktree_git.rebase.assert_called_once_with("origin/main")
    worktree_git.is_merged.assert_called_once_with("feature/rebased", into="main")

    refreshed_parent = store.get(parent.id)
    assert refreshed_parent is not None
    assert refreshed_parent.merge_status == "unmerged"
    assert refreshed_parent.merged_at is None

    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.target_branch == "main"
    assert refreshed_unit.base_sha == "base-local-stale"


def test_run_task_backed_rebase_failure_does_not_reconcile_parent_merge_status(tmp_path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.side_effect = GitError("rebase boom")
    worktree_git.rebase_abort.return_value = None
    private_checkout = SimpleNamespace(path=tmp_path / "isolated-checkout")

    @contextmanager
    def _isolated_checkout_cm(**kwargs):
        yield private_checkout

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline(
                old_tip="head-old",
                target_at_start="base-old",
                merge_base_at_start="merge-base",
            ),
        ),
        patch("gza.cli.git_ops.isolated_rebase_checkout", side_effect=_isolated_checkout_cm),
        patch("gza.cli.git_ops.invoke_provider_resolve", return_value=False),
        patch("gza.cli.git_ops.mark_task_failed_from_cause", return_value=None) as mark_failed,
        patch("gza.cli.git_ops.reconcile_task_branch_merge_truth") as reconcile_task_branch_merge_truth,
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 1
    refreshed_parent = store.get(parent.id)
    assert refreshed_parent is not None
    assert refreshed_parent.merge_status == "unmerged"
    mark_failed.assert_called_once()
    assert mark_failed.call_args.kwargs["explicit_reason"] == "REBASE_CONFLICT"
    reconcile_task_branch_merge_truth.assert_not_called()


def test_run_task_backed_rebase_provider_resolve_publishes_via_shared_helper(tmp_path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.side_effect = GitError("rebase boom")
    worktree_git.rebase_abort.return_value = None
    repo_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-new",
        "main": "base-new",
    }.get(ref)
    private_checkout = SimpleNamespace(path=tmp_path / "isolated-checkout")

    @contextmanager
    def _isolated_checkout_cm(**kwargs):
        yield private_checkout

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline(
                old_tip="head-old",
                target_at_start="base-old",
                merge_base_at_start="merge-base",
            ),
        ),
        patch("gza.cli.git_ops.invoke_provider_resolve", return_value=True),
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
        patch(
            "gza.cli.git_ops.compute_rebase_changed_diff",
            return_value=RebaseDiffResult(changed_diff=True, detail="yes (review must be refreshed)"),
        ),
        patch(
            "gza.cli.git_ops.reconcile_task_branch_merge_truth",
            return_value=SimpleNamespace(warnings=[], skipped_reason=None, errors=[]),
        ),
        patch("gza.cli.git_ops.isolated_rebase_checkout", side_effect=_isolated_checkout_cm),
        patch("gza.cli.git_ops.import_isolated_rebase_tip"),
        patch("gza.cli.git_ops.publish_rebased_branch") as publish_rebased_branch,
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 0
    publish_rebased_branch.assert_called_once()


def test_run_task_backed_rebase_provider_resolve_uses_isolated_checkout_and_guarded_import(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    rebase_task.slug = "rebase-feature"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None
    repo_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-new",
        "main": "base-new",
    }.get(ref)

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.side_effect = GitError("rebase boom")
    worktree_git.rebase_abort.return_value = None

    private_checkout_path = tmp_path / "isolated-checkout"
    private_checkout = SimpleNamespace(path=private_checkout_path)

    @contextmanager
    def _isolated_checkout_cm(**kwargs):
        yield private_checkout

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline(
                old_tip="head-old",
                target_at_start="base-old",
                merge_base_at_start="merge-base",
            ),
        ),
        patch(
            "gza.cli.git_ops.compute_rebase_changed_diff",
            return_value=RebaseDiffResult(changed_diff=True, detail="yes (review must be refreshed)"),
        ),
        patch(
            "gza.cli.git_ops.reconcile_task_branch_merge_truth",
            return_value=SimpleNamespace(warnings=[], skipped_reason=None, errors=[]),
        ),
        patch("gza.cli.git_ops.isolated_rebase_checkout", side_effect=_isolated_checkout_cm) as mock_isolated_checkout,
        patch("gza.cli.git_ops.invoke_provider_resolve", return_value=True) as invoke_provider_resolve,
        patch("gza.cli.git_ops.import_isolated_rebase_tip") as import_isolated_rebase_tip,
        patch("gza.cli.git_ops.publish_rebased_branch") as publish_rebased_branch,
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 0
    mock_isolated_checkout.assert_called_once_with(
        config=config,
        source_git=repo_git,
        branch="feature/rebased",
        target_ref="main",
        checkout_name="rebase-feature",
    )
    invoke_provider_resolve.assert_called_once_with(
        rebase_task,
        "feature/rebased",
        "main",
        config,
        log_file=ANY,
        logger=ANY,
        worktree_path=private_checkout_path,
        runtime_context=ANY,
    )
    import_isolated_rebase_tip.assert_called_once_with(
        destination_git=repo_git,
        checkout=private_checkout,
        branch="feature/rebased",
        expected_old_sha="head-old",
        temp_ref_name="rebase-feature",
    )
    publish_rebased_branch.assert_called_once_with(
        repo_git,
        branch="feature/rebased",
        baseline=RebaseDiffBaseline(
            old_tip="head-old",
            target_at_start="base-old",
            merge_base_at_start="merge-base",
        ),
        logger=ANY,
    )
    repo_git.worktree_remove.assert_called_once_with(config.worktree_path / str(rebase_task.id), force=True)


def test_run_task_backed_rebase_provider_resolve_stale_import_fails_closed(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    rebase_task.slug = "rebase-feature"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.is_ancestor.return_value = False
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.side_effect = GitError("rebase boom")
    worktree_git.rebase_abort.return_value = None

    private_checkout = SimpleNamespace(path=tmp_path / "isolated-checkout")

    @contextmanager
    def _isolated_checkout_cm(**kwargs):
        yield private_checkout

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline(
                old_tip="head-old",
                target_at_start="base-old",
                merge_base_at_start="merge-base",
            ),
        ),
        patch("gza.cli.git_ops.isolated_rebase_checkout", side_effect=_isolated_checkout_cm),
        patch("gza.cli.git_ops.invoke_provider_resolve", return_value=True),
        patch(
            "gza.cli.git_ops.import_isolated_rebase_tip",
            side_effect=StaleRebaseImportError(
                "Refusing to import rebased tip for feature/rebased: expected old SHA head-old"
            ),
        ),
        patch("gza.cli.git_ops.publish_rebased_branch") as publish_rebased_branch,
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 1
    publish_rebased_branch.assert_not_called()
    refreshed = store.get(rebase_task.id)
    assert refreshed is not None
    assert refreshed.status == "failed"
    assert refreshed.failure_reason == "GIT_ERROR"


def test_run_task_backed_rebase_stale_import_surfaces_ancestry_proof_git_error(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    rebase_task.slug = "rebase-feature-proof-error"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.is_ancestor.side_effect = GitError("merge-base could not resolve base-old")
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.side_effect = GitError("rebase boom")
    worktree_git.rebase_abort.return_value = None

    private_checkout = SimpleNamespace(path=tmp_path / "isolated-checkout")

    @contextmanager
    def _isolated_checkout_cm(**kwargs):
        yield private_checkout

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline(
                old_tip="head-old",
                target_at_start="base-old",
                merge_base_at_start="merge-base",
            ),
        ),
        patch("gza.cli.git_ops.isolated_rebase_checkout", side_effect=_isolated_checkout_cm),
        patch("gza.cli.git_ops.invoke_provider_resolve", return_value=True),
        patch(
            "gza.cli.git_ops.import_isolated_rebase_tip",
            side_effect=StaleRebaseImportError(
                "Refusing to import rebased tip for feature/rebased: expected old SHA head-old"
            ),
        ),
        patch("gza.cli.git_ops.publish_rebased_branch") as publish_rebased_branch,
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 1
    repo_git.is_ancestor.assert_called_once_with("base-old", "feature/rebased")
    publish_rebased_branch.assert_not_called()
    refreshed = store.get(rebase_task.id)
    assert refreshed is not None
    assert refreshed.status == "failed"
    assert refreshed.failure_reason == "GIT_ERROR"
    assert refreshed.completion_reason is None
    captured = capsys.readouterr()
    output = captured.out + captured.err
    assert "Failed to verify stale rebase import supersession proof for feature/rebased" in output
    assert "merge-base could not resolve base-old" in output


def test_run_task_backed_rebase_stale_import_completes_when_branch_already_contains_target(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit lineage query

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    rebase_task.slug = "rebase-feature"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.is_ancestor.return_value = True
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None
    repo_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-from-winning-rebase",
        "main": "main-head",
    }.get(ref)

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.side_effect = GitError("rebase boom")
    worktree_git.rebase_abort.return_value = None

    private_checkout = SimpleNamespace(path=tmp_path / "isolated-checkout")

    @contextmanager
    def _isolated_checkout_cm(**kwargs):
        yield private_checkout

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline(
                old_tip="head-old",
                target_at_start="main-head",
                merge_base_at_start="merge-base",
            ),
        ),
        patch("gza.cli.git_ops.isolated_rebase_checkout", side_effect=_isolated_checkout_cm),
        patch("gza.cli.git_ops.invoke_provider_resolve", return_value=True),
        patch(
            "gza.cli.git_ops.import_isolated_rebase_tip",
            side_effect=StaleRebaseImportError(
                "Refusing to import rebased tip for feature/rebased: expected old SHA head-old"
            ),
        ),
        patch("gza.cli.git_ops.publish_rebased_branch") as publish_rebased_branch,
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
        patch(
            "gza.cli.git_ops.compute_rebase_changed_diff",
            return_value=SimpleNamespace(changed_diff=False, warning=None, detail="no"),
        ),
        patch(
            "gza.cli.git_ops.reconcile_task_branch_merge_truth",
            return_value=SimpleNamespace(warnings=[], skipped_reason=None, errors=[]),
        ),
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 0
    publish_rebased_branch.assert_called_once()
    refreshed = store.get(rebase_task.id)
    assert refreshed is not None
    assert refreshed.status == "completed"
    assert refreshed.failure_reason is None
    assert refreshed.completion_reason == "rebase-superseded-by-concurrent-rebase"
    assert refreshed.output_content is not None
    assert "Superseded/no-op" in refreshed.output_content
    assert "already contains 'main-head'" in refreshed.output_content
    assert "with provider assistance" not in refreshed.output_content
    assert "Resolved conflicts and rebased" not in refreshed.output_content

    captured = capsys.readouterr()
    terminal_output = captured.out + captured.err
    assert "Superseded/no-op rebase for feature/rebased" in terminal_output
    assert "Successfully rebased feature/rebased with provider assistance" not in terminal_output

    with (
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=frozenset({"main", "feature/rebased"})),
    ):
        rows = query_lineage_owner_rows(
            store,
            LineageOwnerQuery(
                owner_task_ids=(parent.id,),
                include_skipped=True,
                max_recovery_attempts=1,
            ),
            target_branch="main",
        )
    assert all(
        row.next_action is None
        or row.next_action.get("needs_attention_reason") is None
        or row.next_action.get("subject_task_id") != rebase_task.id
        for row in rows
    )


def test_run_task_backed_rebase_stale_import_fails_when_publish_candidate_lacks_target(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit lineage query

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    rebase_task.slug = "rebase-feature"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None
    repo_git.rev_parse.return_value = "unproven-head"
    repo_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "stale-containing-head",
        "main": "main-head",
        "origin/feature/rebased": "remote-old",
    }.get(ref)
    repo_git.is_ancestor.side_effect = lambda ancestor, ref: ancestor == "main-head" and ref == "feature/rebased"

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.side_effect = GitError("rebase boom")
    worktree_git.rebase_abort.return_value = None

    private_checkout = SimpleNamespace(path=tmp_path / "isolated-checkout")

    @contextmanager
    def _isolated_checkout_cm(**kwargs):
        yield private_checkout

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline(
                old_tip="head-old",
                target_at_start="main-head",
                merge_base_at_start="merge-base",
            ),
        ),
        patch("gza.cli.git_ops.isolated_rebase_checkout", side_effect=_isolated_checkout_cm),
        patch("gza.cli.git_ops.invoke_provider_resolve", return_value=True),
        patch(
            "gza.cli.git_ops.import_isolated_rebase_tip",
            side_effect=StaleRebaseImportError(
                "Refusing to import rebased tip for feature/rebased: expected old SHA head-old"
            ),
        ),
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 1
    repo_git.push_ref_force_with_lease.assert_not_called()
    repo_git.push_force_with_lease.assert_not_called()
    refreshed = store.get(rebase_task.id)
    assert refreshed is not None
    assert refreshed.status == "failed"
    assert refreshed.failure_reason == "GIT_ERROR"
    assert refreshed.completion_reason is None


def test_run_task_backed_rebase_stale_import_publishes_and_persists_proven_head(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit lineage query

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    rebase_task.slug = "rebase-feature"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None
    repo_git.rev_parse.return_value = "proven-head"
    repo_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "changed-after-proof",
        "proven-head": "proven-head",
        "main": "main-head",
        "origin/feature/rebased": "remote-old",
    }.get(ref)
    repo_git.is_ancestor.side_effect = lambda ancestor, ref: (
        ancestor == "main-head" and ref in {"feature/rebased", "proven-head"}
    )

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.side_effect = GitError("rebase boom")
    worktree_git.rebase_abort.return_value = None

    private_checkout = SimpleNamespace(path=tmp_path / "isolated-checkout")

    @contextmanager
    def _isolated_checkout_cm(**kwargs):
        yield private_checkout

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline(
                old_tip="head-old",
                target_at_start="main-head",
                merge_base_at_start="merge-base",
            ),
        ),
        patch("gza.cli.git_ops.isolated_rebase_checkout", side_effect=_isolated_checkout_cm),
        patch("gza.cli.git_ops.invoke_provider_resolve", return_value=True),
        patch(
            "gza.cli.git_ops.import_isolated_rebase_tip",
            side_effect=StaleRebaseImportError(
                "Refusing to import rebased tip for feature/rebased: expected old SHA head-old"
            ),
        ),
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
        patch(
            "gza.cli.git_ops.compute_rebase_changed_diff",
            return_value=SimpleNamespace(changed_diff=False, warning=None, detail="no"),
        ),
        patch(
            "gza.cli.git_ops.reconcile_task_branch_merge_truth",
            return_value=SimpleNamespace(warnings=[], skipped_reason=None, errors=[]),
        ),
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 0
    repo_git.push_ref_force_with_lease.assert_called_once_with(
        "proven-head",
        "feature/rebased",
        remote="origin",
        expected_remote_oid="remote-old",
    )
    refreshed = store.get(rebase_task.id)
    assert refreshed is not None
    assert refreshed.status == "completed"
    assert refreshed.completion_reason == "rebase-superseded-by-concurrent-rebase"
    assert "Resolved head SHA: proven-head" in (refreshed.review_scope or "")


def test_run_task_backed_remote_rebase_stale_import_keeps_git_error_when_effective_target_not_contained(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    rebase_task.slug = "rebase-feature"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None
    repo_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-from-winning-rebase",
        "main": "local-main-head",
        "origin/main": "origin-main-head",
    }.get(ref)

    def _is_ancestor(ancestor: str, branch: str) -> bool:
        assert branch == "feature/rebased"
        return ancestor == "main"

    repo_git.is_ancestor.side_effect = _is_ancestor

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.side_effect = GitError("rebase boom")
    worktree_git.rebase_abort.return_value = None

    private_checkout = SimpleNamespace(path=tmp_path / "isolated-checkout")

    @contextmanager
    def _isolated_checkout_cm(**kwargs):
        yield private_checkout

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline(
                old_tip="head-old",
                target_at_start="origin-main-head",
                merge_base_at_start="merge-base",
            ),
        ),
        patch("gza.cli.git_ops.isolated_rebase_checkout", side_effect=_isolated_checkout_cm),
        patch("gza.cli.git_ops.invoke_provider_resolve", return_value=True),
        patch(
            "gza.cli.git_ops.import_isolated_rebase_tip",
            side_effect=StaleRebaseImportError(
                "Refusing to import rebased tip for feature/rebased: expected old SHA head-old"
            ),
        ),
        patch("gza.cli.git_ops.publish_rebased_branch") as publish_rebased_branch,
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
            remote=True,
        )

    assert rc == 1
    repo_git.is_ancestor.assert_called_with("origin-main-head", "feature/rebased")
    publish_rebased_branch.assert_not_called()
    refreshed = store.get(rebase_task.id)
    assert refreshed is not None
    assert refreshed.status == "failed"
    assert refreshed.failure_reason == "GIT_ERROR"
    assert refreshed.completion_reason is None


def test_run_task_backed_remote_rebase_stale_import_completes_only_when_effective_target_contained(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit lineage query

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    rebase_task.slug = "rebase-feature"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None
    repo_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-from-winning-rebase",
        "main": "local-main-head",
        "origin/main": "origin-main-head",
    }.get(ref)
    repo_git.is_ancestor.side_effect = lambda ancestor, branch: (
        branch == "feature/rebased" and ancestor == "origin-main-head"
    )

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.side_effect = GitError("rebase boom")
    worktree_git.rebase_abort.return_value = None

    private_checkout = SimpleNamespace(path=tmp_path / "isolated-checkout")

    @contextmanager
    def _isolated_checkout_cm(**kwargs):
        yield private_checkout

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline(
                old_tip="head-old",
                target_at_start="origin-main-head",
                merge_base_at_start="merge-base",
            ),
        ),
        patch("gza.cli.git_ops.isolated_rebase_checkout", side_effect=_isolated_checkout_cm),
        patch("gza.cli.git_ops.invoke_provider_resolve", return_value=True),
        patch(
            "gza.cli.git_ops.import_isolated_rebase_tip",
            side_effect=StaleRebaseImportError(
                "Refusing to import rebased tip for feature/rebased: expected old SHA head-old"
            ),
        ),
        patch("gza.cli.git_ops.publish_rebased_branch") as publish_rebased_branch,
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
        patch(
            "gza.cli.git_ops.compute_rebase_changed_diff",
            return_value=SimpleNamespace(changed_diff=False, warning=None, detail="no"),
        ),
        patch(
            "gza.cli.git_ops.reconcile_task_branch_merge_truth",
            return_value=SimpleNamespace(warnings=[], skipped_reason=None, errors=[]),
        ),
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
            remote=True,
        )

    assert rc == 0
    repo_git.is_ancestor.assert_called_with("origin-main-head", "feature/rebased")
    publish_rebased_branch.assert_called_once()
    refreshed = store.get(rebase_task.id)
    assert refreshed is not None
    assert refreshed.status == "completed"
    assert refreshed.failure_reason is None
    assert refreshed.completion_reason == "rebase-superseded-by-concurrent-rebase"
    assert refreshed.output_content is not None
    assert "already contains 'origin-main-head'" in refreshed.output_content


@pytest.mark.parametrize(
    "import_error",
    [
        GitError("Cannot import rebased tip for feature/rebased without an expected old SHA"),
        GitError("fetch failed"),
        GitError("failed to resolve imported temp ref"),
        GitError("update-ref failed while expected old SHA was still current"),
    ],
)
def test_run_task_backed_rebase_non_stale_import_errors_remain_git_error_even_when_branch_contains_target(
    tmp_path: Path,
    import_error: GitError,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    rebase_task.slug = "rebase-feature"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.is_ancestor.return_value = True
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.side_effect = GitError("rebase boom")
    worktree_git.rebase_abort.return_value = None

    private_checkout = SimpleNamespace(path=tmp_path / "isolated-checkout")

    @contextmanager
    def _isolated_checkout_cm(**kwargs):
        yield private_checkout

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline(
                old_tip="head-old",
                target_at_start="main-head",
                merge_base_at_start="merge-base",
            ),
        ),
        patch("gza.cli.git_ops.isolated_rebase_checkout", side_effect=_isolated_checkout_cm),
        patch("gza.cli.git_ops.invoke_provider_resolve", return_value=True),
        patch("gza.cli.git_ops.import_isolated_rebase_tip", side_effect=import_error),
        patch("gza.cli.git_ops.publish_rebased_branch") as publish_rebased_branch,
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 1
    repo_git.is_ancestor.assert_not_called()
    publish_rebased_branch.assert_not_called()
    refreshed = store.get(rebase_task.id)
    assert refreshed is not None
    assert refreshed.status == "failed"
    assert refreshed.failure_reason == "GIT_ERROR"
    assert refreshed.completion_reason is None


def test_run_task_backed_rebase_clean_publish_failure_does_not_fall_back_to_provider(tmp_path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.return_value = None

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch("gza.cli.git_ops.publish_rebased_branch", side_effect=GitError("push boom")),
        patch("gza.cli.git_ops.invoke_provider_resolve") as invoke_provider_resolve,
        patch("gza.cli.git_ops.mark_task_failed_from_cause", return_value=None) as mark_failed,
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 1
    invoke_provider_resolve.assert_not_called()
    worktree_git.rebase_abort.assert_not_called()
    mark_failed.assert_called_once()
    assert mark_failed.call_args.kwargs["explicit_reason"] == "GIT_ERROR"


def test_run_task_backed_rebase_container_git_metadata_failure_retries_as_infra(tmp_path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.return_value = None

    invalid_path_error = GitError(
        "git worktree list --porcelain failed: fatal: Invalid path '/gza-git': No such file or directory"
    )

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", side_effect=invalid_path_error),
        patch("gza.cli.git_ops.mark_task_failed_from_cause", return_value=None) as mark_failed,
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 1
    mark_failed.assert_called_once()
    assert mark_failed.call_args.kwargs["explicit_reason"] == "INFRASTRUCTURE_ERROR"


def test_classify_rebase_git_failure_marks_readonly_sqlite_error_as_infra() -> None:
    error = sqlite3.OperationalError("attempt to write a readonly database")

    assert _classify_rebase_git_failure(error) == "INFRASTRUCTURE_ERROR"


def test_run_task_backed_rebase_readonly_db_git_failure_retries_as_infra(tmp_path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.return_value = None

    readonly_db_error = GitError("sqlite3.OperationalError: attempt to write a readonly database")

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", side_effect=readonly_db_error),
        patch("gza.cli.git_ops.mark_task_failed_from_cause", return_value=None) as mark_failed,
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 1
    mark_failed.assert_called_once()
    assert mark_failed.call_args.kwargs["explicit_reason"] == "INFRASTRUCTURE_ERROR"


def test_run_task_backed_rebase_remote_ref_lookup_failure_marks_task_failed(tmp_path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.return_value = None
    worktree_git.rev_parse.return_value = "head-new"
    worktree_git.rev_parse_if_exists.side_effect = RuntimeError("remote lookup boom")

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch("gza.cli.git_ops.invoke_provider_resolve") as invoke_provider_resolve,
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 1
    invoke_provider_resolve.assert_not_called()
    worktree_git.push_force_with_lease.assert_not_called()

    refreshed = store.get(rebase_task.id)
    assert refreshed is not None
    assert refreshed.status == "failed"
    assert refreshed.failure_reason == "GIT_ERROR"


def test_run_task_backed_rebase_non_advancing_publish_succeeds_when_branch_is_already_up_to_date(tmp_path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    store.update(rebase_task)
    assert rebase_task.id is not None

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.return_value = None
    worktree_git.rev_parse.return_value = "same-head"
    worktree_git.is_ancestor.return_value = True
    worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "same-head",
        "main": "base-old",
        "origin/feature/rebased": "remote-stale",
    }.get(ref)

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline(
                old_tip="same-head",
                target_at_start="base-old",
                merge_base_at_start="merge-base",
            ),
        ),
        patch(
            "gza.cli.git_ops.compute_rebase_changed_diff",
            return_value=RebaseDiffResult(changed_diff=False, detail="no (review preserved)"),
        ),
        patch("gza.cli.git_ops.invoke_provider_resolve") as invoke_provider_resolve,
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 0
    invoke_provider_resolve.assert_not_called()
    worktree_git.push_force_with_lease.assert_called_once_with("feature/rebased", remote="origin")

    refreshed = store.get(rebase_task.id)
    assert refreshed is not None
    assert refreshed.status == "completed"
    assert refreshed.failure_reason is None


@pytest.mark.timeout(4, method="signal")
def test_advance_explicit_merge_refuses_when_checkout_does_not_match_canonical_target(
    tmp_path: Path,
    capsys,
) -> None:
    from gza.git import ResolvedMergeSourceRef

    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_text = config_path.read_text()
    config_path.write_text(config_text + "require_review_before_merge: false\n")

    store = make_store(tmp_path)

    task = store.add("Implement feature", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/advance-explicit-refusal"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: APPROVED**"
    store.update(review)

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = task.branch
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(task.branch)
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = False
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1

    args = argparse.Namespace(
        project_dir=tmp_path,
        task_id=task.id,
        dry_run=False,
        auto=True,
        max=None,
        batch=None,
        no_docker=True,
        force=False,
        plans=False,
        unimplemented=False,
        create=False,
        no_resume_failed=False,
        max_resume_attempts=None,
        advance_type=None,
        new=False,
        max_review_cycles=None,
        squash_threshold=None,
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
    ):
        rc = cmd_advance(args)

    output = capsys.readouterr().out
    assert rc == 0
    assert "Will advance 1 task(s):" in output
    assert "Run verify gate before merge" in output
    assert "verify epoch is unavailable; merge is blocked" in output
    assert "1 skipped" in output

    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "unmerged"
    fake_git.merge.assert_not_called()
    fake_git.is_merged.assert_called()


@pytest.mark.parametrize("repeat", [False, True])
def test_cmd_advance_does_not_short_circuit_git_merged_authoritative_unmerged_unit(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    repeat: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    setup_config(tmp_path)
    config = Config.load(tmp_path)
    config.on_max_cycles = "merge_and_defer"
    config.max_review_cycles = 0
    store = make_store(tmp_path)
    task = _completed_merge_task(
        store,
        "Authoritative unit replay",
        f"feature/authoritative-unit-replay-{repeat}",
    )
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    assert unit.state == "unmerged"
    review_output = _capped_review_output("B1")
    review = _completed_review(store, task, review_output)
    for index in range(3):
        improve = store.add(
            f"Completed improve cycle {index}",
            task_type="improve",
            based_on=task.id,
            depends_on=review.id,
        )
        improve.status = "completed"
        improve.completed_at = datetime.now(UTC)
        store.update(improve)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review: ParsedReviewReport(
            verdict="CHANGES_REQUESTED",
            findings=(_blocker_finding("B1"),),
            format_version="v2",
        ),
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(task.branch)
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.is_merged.return_value = True
    fake_git.rev_parse_if_exists.side_effect = lambda ref: "same-sha" if ref in {task.branch, "main"} else None
    fake_git.is_ancestor.return_value = False

    args = _advance_args(tmp_path, task.id)
    args.dry_run = True
    args.repeat = repeat
    args.max_iterations = 1

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[]),
    ):
        rc = cmd_advance(args)

    output = capsys.readouterr().out
    assert rc == 0
    assert f"Task {task.id} is already merged" in output
    assert f"Advance repeat completed: {task.id} merged" not in output
    assert "Merge and defer blockers after max review cycles" not in output
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"


def test_advance_execution_remote_only_ref_now_requires_manual_resolution_when_local_branch_is_missing(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    from gza import advance_engine as advance_engine_module
    from gza.git import ResolvedMergeSourceRef

    setup_config(tmp_path)
    store = make_store(tmp_path)
    branch = "feature/advance-remote-only"
    task = _add_mergeable_impl_with_failed_rebase(store, branch)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review_task: SimpleNamespace(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.side_effect = lambda b: b != branch
    fake_git.ref_exists.side_effect = lambda r: r == f"origin/{branch}"
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(f"origin/{branch}")
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1
    fake_git.merge.return_value = None

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, task.id)), "dry_run": True}))

    assert rc == 0
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "unmerged"

    fake_git.merge.assert_not_called()

    output = capsys.readouterr().out
    assert f"fresh merge source for branch '{branch}' is unavailable" in output
    assert "cannot auto-merge without a" in output
    assert "resolvable local source" in output
    assert "rebase --resolve (conflicts detected)" not in output
    assert f"Merging 'origin/{branch}' into 'main'" not in output


def test_cmd_advance_wraps_planning_in_git_cache(tmp_path: Path) -> None:
    from contextlib import contextmanager

    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Advance cached planning", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/cache-planning"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="skipped",
        next_action={"type": "skip", "description": "nothing to do"},
        next_action_reason="precomputed",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=None,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.current_branch.return_value = "main"
    fake_git.default_branch.return_value = "main"
    cached_entries: list[str] = []

    @contextmanager
    def _cached_scope():
        cached_entries.append("entered")
        yield fake_git

    fake_git.cached.side_effect = _cached_scope

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=iter([row])),
    ):
        rc = cmd_advance(
            argparse.Namespace(
                project_dir=tmp_path,
                task_id=task.id,
                dry_run=True,
                auto=True,
                max=None,
                batch=None,
                no_docker=True,
                force=False,
                plans=False,
                unimplemented=False,
                create=False,
                no_resume_failed=False,
                max_resume_attempts=None,
                advance_type=None,
                new=False,
                max_review_cycles=None,
                squash_threshold=None,
            )
        )

    assert rc == 0
    assert cached_entries == ["entered"]


def test_cmd_advance_dry_run_raises_unexpected_merge_context_type_error(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Advance type error should surface", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/advance-type-error"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="needs_retry",
        next_action={"type": "retry", "description": "retry failed task"},
        next_action_reason="retryable-provider-error",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=None,
        recovery_action_task=task,
        recovery_leaf_task=task,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.current_branch.return_value = "main"
    fake_git.default_branch.return_value = "main"
    fake_git.local_branch_names.return_value = ()

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=iter([row])),
        patch("gza.cli.git_ops.build_merge_context_from_git", side_effect=TypeError("boom")),
    ):
        with pytest.raises(TypeError, match="boom"):
            cmd_advance(
                argparse.Namespace(
                    project_dir=tmp_path,
                    task_id=task.id,
                    dry_run=True,
                    auto=True,
                    max=None,
                    batch=None,
                    no_docker=True,
                    force=False,
                    plans=False,
                    unimplemented=False,
                    create=False,
                    no_resume_failed=False,
                    max_resume_attempts=None,
                    advance_type=None,
                    new=False,
                    max_review_cycles=None,
                    squash_threshold=None,
                )
            )


def test_cmd_advance_dry_run_warns_and_degrades_without_local_branch_names(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Advance degraded git compatibility", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/advance-degraded"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="skipped",
        next_action={"type": "skip", "description": "nothing to do"},
        next_action_reason="precomputed",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=None,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    class _CompatGit:
        repo_dir = tmp_path

        def current_branch(self) -> str:
            return "main"

        def default_branch(self) -> str:
            return "main"

        @contextmanager
        def cached(self):
            yield self

    compat_git = _CompatGit()

    with (
        patch("gza.cli.git_ops.Git", return_value=compat_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=iter([row])),
    ):
        rc = cmd_advance(
            argparse.Namespace(
                project_dir=tmp_path,
                task_id=task.id,
                dry_run=True,
                auto=True,
                max=None,
                batch=None,
                no_docker=True,
                force=False,
                plans=False,
                unimplemented=False,
                create=False,
                no_resume_failed=False,
                max_resume_attempts=None,
                advance_type=None,
                new=False,
                max_review_cycles=None,
                squash_threshold=None,
            )
        )

    assert rc == 0
    assert "Warning: advance recovery preview is using a degraded git context" in capsys.readouterr().out


def test_cmd_advance_dry_run_warns_and_degrades_when_local_branch_names_is_not_iterable(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Advance degraded git non-iterable compatibility", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/advance-degraded-mock"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="skipped",
        next_action={"type": "skip", "description": "nothing to do"},
        next_action_reason="precomputed",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=None,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    compat_git = MagicMock(spec=Git)
    compat_git.repo_dir = tmp_path
    compat_git.current_branch.return_value = "main"
    compat_git.default_branch.return_value = "main"
    compat_git.local_branch_names.return_value = object()

    @contextmanager
    def _cached_scope():
        yield compat_git

    compat_git.cached.side_effect = _cached_scope

    with (
        patch("gza.cli.git_ops.Git", return_value=compat_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=iter([row])),
    ):
        rc = cmd_advance(
            argparse.Namespace(
                project_dir=tmp_path,
                task_id=task.id,
                dry_run=True,
                auto=True,
                max=None,
                batch=None,
                no_docker=True,
                force=False,
                plans=False,
                unimplemented=False,
                create=False,
                no_resume_failed=False,
                max_resume_attempts=None,
                advance_type=None,
                new=False,
                max_review_cycles=None,
                squash_threshold=None,
            )
        )

    assert rc == 0
    assert "Warning: advance recovery preview is using a degraded git context" in capsys.readouterr().out


def test_cmd_advance_batches_ref_preloads_during_lifecycle_planning(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Advance cached lifecycle planning", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/cache-lifecycle"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    git = Git(tmp_path)
    git.current_branch = MagicMock(return_value="main")  # type: ignore[method-assign]
    git.default_branch = MagicMock(return_value="main")  # type: ignore[method-assign]

    git_calls: list[tuple[str, ...]] = []
    batch_stdins: list[bytes] = []

    def _fake_run(*args: str, check: bool = True, stdin: bytes | None = None):
        del check
        git_calls.append(args)
        if args == ("for-each-ref", "--format=%(refname:strip=2)", "refs/heads/"):
            return SimpleNamespace(returncode=0, stdout="feature/cache-lifecycle\n", stderr="")
        if args == ("cat-file", "--batch-check"):
            assert stdin is not None
            batch_stdins.append(stdin)
            request = stdin.decode().splitlines()
            response_lines: list[str] = []
            for line in request:
                if line == "feature/cache-lifecycle^{commit}":
                    response_lines.append("a" * 40 + " commit 1")
                elif line == "origin/feature/cache-lifecycle^{commit}":
                    response_lines.append("origin/feature/cache-lifecycle^{commit} missing")
                elif line == "main^{commit}":
                    response_lines.append("b" * 40 + " commit 1")
                elif line == "main^{tree}":
                    response_lines.append("c" * 40 + " tree 1")
                else:
                    raise AssertionError(f"Unexpected batch ref: {line!r}")
            return SimpleNamespace(returncode=0, stdout="\n".join(response_lines) + "\n", stderr="")
        if args == ("merge-base", "--is-ancestor", "main", "feature/cache-lifecycle"):
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        if args == ("rev-list", "--count", "main..feature/cache-lifecycle"):
            return SimpleNamespace(returncode=0, stdout="2\n", stderr="")
        if args == ("merge-tree", "--write-tree", "main", "feature/cache-lifecycle"):
            return SimpleNamespace(returncode=0, stdout="target-tree\n", stderr="")
        if args == ("rev-list", "--count", "origin/feature/cache-lifecycle..feature/cache-lifecycle"):
            return SimpleNamespace(returncode=0, stdout="2\n", stderr="")
        if args == ("rev-list", "--count", "feature/cache-lifecycle..origin/feature/cache-lifecycle"):
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        raise AssertionError(f"Unexpected git command: {args!r}")

    git._run = _fake_run  # type: ignore[method-assign]

    def _query_rows(*_args, **_kwargs):
        assert git._cache is not None
        assert git.branch_exists("feature/cache-lifecycle") is True
        assert git.rev_parse_if_exists("feature/cache-lifecycle") == "a" * 40
        assert git.ref_exists("origin/feature/cache-lifecycle") is False
        assert git.branch_exists("feature/cache-lifecycle") is True
        return iter(
            [
                LineageOwnerRow(
                    owner_task=task,
                    members=(task,),
                    tree=None,
                    lineage_status="skipped",
                    next_action={"type": "skip", "description": "nothing to do"},
                    next_action_reason="precomputed",
                    unresolved_tasks=(task,),
                    unresolved_leaf_summary=(),
                    lifecycle_action_task=task,
                    recovery_action_task=None,
                    recovery_leaf_task=None,
                )
            ]
        )

    def _determine_next_action(*_args, **_kwargs):
        assert git._cache is not None
        assert git.branch_exists("feature/cache-lifecycle") is True
        assert git.rev_parse_if_exists("main") == "b" * 40
        assert git.ref_exists("origin/feature/cache-lifecycle") is False
        assert git.branch_exists("feature/cache-lifecycle") is True
        return {"type": "skip", "description": "nothing to do"}

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", side_effect=_query_rows),
        patch("gza.cli.git_ops.determine_next_action", side_effect=_determine_next_action),
    ):
        rc = cmd_advance(
            argparse.Namespace(
                project_dir=tmp_path,
                task_id=task.id,
                dry_run=True,
                auto=True,
                max=None,
                batch=None,
                no_docker=True,
                force=False,
                plans=False,
                unimplemented=False,
                create=False,
                no_resume_failed=False,
                max_resume_attempts=None,
                advance_type=None,
                new=False,
                max_review_cycles=None,
                squash_threshold=None,
            )
        )

    assert rc == 0
    assert ("show-ref", "--verify", "--quiet", "refs/heads/feature/cache-lifecycle") not in git_calls
    assert ("rev-parse", "--verify", "--quiet", "feature/cache-lifecycle^{commit}") not in git_calls
    assert ("rev-parse", "--verify", "--quiet", "origin/feature/cache-lifecycle^{commit}") not in git_calls
    assert ("rev-parse", "--verify", "--quiet", "main^{commit}") not in git_calls
    assert ("for-each-ref", "--format=%(refname:strip=2)", "refs/heads/") in git_calls
    assert ("cat-file", "--batch-check") in git_calls
    assert len(set(git_calls)) == 2
    assert any(b"feature/cache-lifecycle^{commit}\n" in payload for payload in batch_stdins)


def test_cmd_advance_uses_shared_ref_preload_helper(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Advance shared preload helper", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/shared-preload"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="skipped",
        next_action={"type": "skip", "description": "nothing to do"},
        next_action_reason="precomputed",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=None,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.current_branch.return_value = "main"
    fake_git.default_branch.return_value = "main"

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=iter([row])),
        patch("gza.cli.git_ops.prime_advance_planning_refs") as preload,
    ):
        rc = cmd_advance(
            argparse.Namespace(
                project_dir=tmp_path,
                task_id=task.id,
                dry_run=True,
                auto=True,
                max=None,
                batch=None,
                no_docker=True,
                force=False,
                plans=False,
                unimplemented=False,
                create=False,
                no_resume_failed=False,
                max_resume_attempts=None,
                advance_type=None,
                new=False,
                max_review_cycles=None,
                squash_threshold=None,
            )
        )

    assert rc == 0
    preload.assert_called_once_with(
        fake_git,
        branch_names=["feature/shared-preload"],
        target_branch="main",
        warning_logger=ANY,
    )


def test_cmd_advance_explicit_child_member_scopes_query_to_owner_lineage(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl, review = _add_completed_impl_with_approved_review(
        store,
        "feature/member-owner-scope",
        when=datetime.now(UTC),
    )

    row = LineageOwnerRow(
        owner_task=impl,
        members=(impl, review),
        tree=None,
        lineage_status="skipped",
        next_action={"type": "skip", "description": "nothing to do"},
        next_action_reason="precomputed",
        unresolved_tasks=(impl,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=impl,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.current_branch.return_value = "main"
    fake_git.default_branch.return_value = "main"

    captured_queries: list = []

    def _query_rows(_store, query, **_kwargs):
        captured_queries.append(query)
        return [row]

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", side_effect=_query_rows),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, review.id)), "dry_run": True}))

    assert rc == 0
    assert len(captured_queries) == 1
    assert captured_queries[0].task_ids == (review.id,)
    assert captured_queries[0].owner_task_ids is None


def test_cmd_advance_explicit_failed_leaf_scopes_query_to_owner_lineage(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl, _review = _add_completed_impl_with_approved_review(
        store,
        "feature/failed-leaf-owner-scope",
        when=datetime(2026, 5, 10, 9, 0, tzinfo=UTC),
    )

    failed_rebase = store.add("Failed rebase leaf", task_type="rebase", based_on=impl.id, same_branch=True)
    assert failed_rebase.id is not None
    failed_rebase.status = "failed"
    failed_rebase.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    failed_rebase.branch = impl.branch
    failed_rebase.failure_reason = "MERGE_CONFLICT"
    store.update(failed_rebase)

    row = LineageOwnerRow(
        owner_task=impl,
        members=(impl, failed_rebase),
        tree=None,
        lineage_status="needs_attention",
        next_action={
            "type": "needs_discussion",
            "description": "failed rebase still blocks merge",
            "needs_attention_reason": "rebase-failed",
            "subject_task_id": failed_rebase.id,
        },
        next_action_reason="rebase-failed",
        unresolved_tasks=(failed_rebase,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=impl,
        recovery_action_task=failed_rebase,
        recovery_leaf_task=failed_rebase,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.current_branch.return_value = "main"
    fake_git.default_branch.return_value = "main"

    captured_queries: list = []

    def _query_rows(_store, query, **_kwargs):
        captured_queries.append(query)
        return [row]

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops.query_lineage_owner_rows", side_effect=_query_rows),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, failed_rebase.id)), "dry_run": True}))

    assert rc == 0
    assert len(captured_queries) == 1
    assert captured_queries[0].task_ids == (failed_rebase.id,)
    assert captured_queries[0].owner_task_ids is None


def test_cmd_advance_explicit_dropped_owner_fallback_scopes_second_query_to_owner_lineage(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    owner = store.add("Dropped owner", task_type="implement")
    assert owner.id is not None
    owner.status = "dropped"
    owner.completed_at = datetime(2026, 5, 12, 9, 0, tzinfo=UTC)
    owner.branch = "feature/dropped-owner-scope"
    owner.has_commits = True
    owner.merge_status = "unmerged"
    store.update(owner)

    descendant = store.add("Completed descendant", task_type="rebase", based_on=owner.id, same_branch=True)
    assert descendant.id is not None
    descendant.status = "completed"
    descendant.completed_at = datetime(2026, 5, 12, 10, 0, tzinfo=UTC)
    descendant.branch = owner.branch
    descendant.has_commits = True
    descendant.merge_status = "unmerged"
    store.update(descendant)

    dropped_row = LineageOwnerRow(
        owner_task=owner,
        members=(owner, descendant),
        tree=None,
        lineage_status="skipped",
        next_action={"type": "skip", "description": "dropped"},
        next_action_reason="dropped",
        unresolved_tasks=(descendant,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=descendant,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.current_branch.return_value = "main"
    fake_git.default_branch.return_value = "main"
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(owner.branch)
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = False
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1

    captured_queries: list = []

    def _query_rows(_store, query, **_kwargs):
        captured_queries.append(query)
        if query.exclude_dropped_from_planning:
            return []
        return [dropped_row]

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", side_effect=_query_rows),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, descendant.id)), "dry_run": True}))

    assert rc == 0
    assert len(captured_queries) == 2
    assert captured_queries[0].task_ids == (descendant.id,)
    assert captured_queries[0].owner_task_ids is None
    assert captured_queries[0].exclude_dropped_from_planning is True
    assert captured_queries[1].task_ids == (descendant.id,)
    assert captured_queries[1].owner_task_ids is None
    assert captured_queries[1].exclude_dropped_from_planning is False
    assert "No eligible tasks to advance" in capsys.readouterr().out


def test_cmd_advance_explicit_dropped_owner_fallback_uses_one_read_session_connection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    owner = store.add("Dropped owner", task_type="implement")
    assert owner.id is not None
    owner.status = "dropped"
    owner.completed_at = datetime(2026, 5, 12, 9, 0, tzinfo=UTC)
    owner.branch = "feature/dropped-owner-read-session"
    owner.has_commits = True
    owner.merge_status = "unmerged"
    store.update(owner)

    descendant = store.add("Completed descendant", task_type="rebase", based_on=owner.id, same_branch=True)
    assert descendant.id is not None
    descendant.status = "completed"
    descendant.completed_at = datetime(2026, 5, 12, 10, 0, tzinfo=UTC)
    descendant.branch = owner.branch
    descendant.has_commits = True
    descendant.merge_status = "unmerged"
    store.update(descendant)

    opened_connections: list[tuple[bool, object]] = []
    original_open_connection = store._open_connection

    def _tracking_open_connection(*, close_on_exit: bool):
        conn = original_open_connection(close_on_exit=close_on_exit)
        opened_connections.append((close_on_exit, conn))
        return conn

    monkeypatch.setattr(store, "_open_connection", _tracking_open_connection)

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.current_branch.return_value = "main"
    fake_git.default_branch.return_value = "main"
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(owner.branch)
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = False
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "skip", "description": "nothing to do"}),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, descendant.id)), "dry_run": True}))

    assert rc == 0
    assert len([conn for close_on_exit, conn in opened_connections if close_on_exit is False]) == 1


def test_cmd_advance_explicit_failed_leaf_dry_run_skips_deferred_prerequisite_reconciliation(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _dependency, _owner, failed = _add_prerequisite_unmerged_failed_child(
        store,
        owner_branch="feature/explicit-prereq-reconcile",
    )

    fake_git = _make_read_session_reconciliation_git(tmp_path, failed.branch)
    assert store.get_or_create_merge_unit_for_task(failed) is not None
    before = _durable_preview_snapshot(store)

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[]),
        patch("gza.recovery_engine.is_resolved_by_merged_target", return_value=False),
        patch("gza.recovery_engine._is_resolved_by_landed_lineage", return_value=False),
        patch("gza.recovery_engine.get_completed_same_slice_sibling_attempt", return_value=None),
        patch(
            "gza.cli.advance_engine.determine_next_action",
            return_value={"type": "skip", "description": "nothing to do"},
        ),
        patch("gza.recovery_engine.resolve_task_merge_state_for_target", return_value="empty"),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, failed.id)), "dry_run": True}))

    assert rc == 0
    assert _durable_preview_snapshot(store) == before


def test_cmd_advance_explicit_dropped_owner_fallback_dry_run_skips_deferred_prerequisite_reconciliation(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _dependency, _owner, failed = _add_prerequisite_unmerged_failed_child(
        store,
        owner_status="dropped",
        owner_branch="feature/dropped-owner-prereq-reconcile",
    )

    fake_git = _make_read_session_reconciliation_git(tmp_path, failed.branch)
    assert store.get_or_create_merge_unit_for_task(failed) is not None
    before = _durable_preview_snapshot(store)

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch(
            "gza.cli.advance_engine.determine_next_action",
            return_value={"type": "skip", "description": "nothing to do"},
        ),
        patch("gza.recovery_engine.resolve_task_merge_state_for_target", return_value="redundant"),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, failed.id)), "dry_run": True}))

    assert rc == 0
    assert _durable_preview_snapshot(store) == before


def test_cmd_advance_explicit_task_plans_only_requested_lineage_refs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    setup_config(tmp_path)
    store = make_store(tmp_path)

    requested, _ = _add_completed_impl_with_approved_review(
        store,
        "feature/requested-lineage",
        when=datetime(2026, 5, 10, 9, 0, tzinfo=UTC),
    )
    for index in range(3):
        _add_completed_impl_with_approved_review(
            store,
            f"feature/unrelated-lineage-{index}",
            when=datetime(2026, 5, 10, 10 + index, 0, tzinfo=UTC),
        )

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review_task: SimpleNamespace(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    fake_git, ref_calls, branch_calls = _make_preload_recording_git(tmp_path)

    merge_sources: list[str] = []
    merge_checks: list[str] = []

    def _resolve_fresh_merge_source(branch: str):
        merge_sources.append(branch)
        return ResolvedMergeSourceRef(f"origin/{branch}")

    def _can_merge(source_ref: str, target_branch: str):
        assert target_branch == "main"
        merge_checks.append(source_ref)
        return True

    fake_git.resolve_fresh_merge_source.side_effect = _resolve_fresh_merge_source
    fake_git.can_merge.side_effect = _can_merge

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, requested.id)), "dry_run": True}))

    assert rc == 0
    assert merge_sources == []
    assert merge_checks
    assert set(merge_checks) == {requested.branch}
    _assert_scoped_preload_refs(
        ref_calls,
        branch_calls,
        requested_branch=requested.branch,
        unrelated_branches=tuple(f"feature/unrelated-lineage-{index}" for index in range(3)),
    )


def test_cmd_advance_explicit_child_member_preloads_only_owner_lineage_refs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    setup_config(tmp_path)
    store = make_store(tmp_path)

    requested, review = _add_completed_impl_with_approved_review(
        store,
        "feature/member-owner-scope",
        when=datetime(2026, 5, 10, 9, 0, tzinfo=UTC),
    )
    unrelated_branches = tuple(f"feature/member-unrelated-{index}" for index in range(2))
    for index, branch in enumerate(unrelated_branches):
        _add_completed_impl_with_approved_review(
            store,
            branch,
            when=datetime(2026, 5, 10, 10 + index, 0, tzinfo=UTC),
        )

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review_task: SimpleNamespace(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    fake_git, ref_calls, branch_calls = _make_preload_recording_git(tmp_path)
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(f"origin/{requested.branch}")

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, review.id)), "dry_run": True}))

    assert rc == 0
    _assert_scoped_preload_refs(
        ref_calls,
        branch_calls,
        requested_branch=requested.branch,
        unrelated_branches=unrelated_branches,
    )


def test_cmd_advance_explicit_failed_leaf_preloads_only_owner_lineage_refs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    setup_config(tmp_path)
    store = make_store(tmp_path)
    requested, _review = _add_completed_impl_with_approved_review(
        store,
        "feature/failed-leaf-owner-scope",
        when=datetime(2026, 5, 10, 9, 0, tzinfo=UTC),
    )

    failed_rebase = store.add("Failed rebase leaf", task_type="rebase", based_on=requested.id, same_branch=True)
    assert failed_rebase.id is not None
    failed_rebase.status = "failed"
    failed_rebase.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    failed_rebase.branch = requested.branch
    failed_rebase.failure_reason = "MERGE_CONFLICT"
    store.update(failed_rebase)

    unrelated_branches = tuple(f"feature/failed-leaf-unrelated-{index}" for index in range(2))
    for index, branch in enumerate(unrelated_branches):
        _add_completed_impl_with_approved_review(
            store,
            branch,
            when=datetime(2026, 5, 10, 11 + index, 0, tzinfo=UTC),
        )

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review_task: SimpleNamespace(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    fake_git, ref_calls, branch_calls = _make_preload_recording_git(tmp_path)
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(f"origin/{requested.branch}")

    row = LineageOwnerRow(
        owner_task=requested,
        members=(requested, failed_rebase),
        tree=None,
        lineage_status="skipped",
        next_action={"type": "skip", "description": "nothing to do"},
        next_action_reason="precomputed",
        unresolved_tasks=(failed_rebase,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=None,
        recovery_action_task=None,
        recovery_leaf_task=failed_rebase,
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.git.Git", return_value=fake_git),
        patch(
            "gza.git.Git._run",
            return_value=SimpleNamespace(returncode=0, stdout="refs/remotes/origin/main\n", stderr=""),
        ),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=iter([row])),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, failed_rebase.id)), "dry_run": True}))

    assert rc == 0
    _assert_scoped_preload_refs(
        ref_calls,
        branch_calls,
        requested_branch=requested.branch,
        unrelated_branches=unrelated_branches,
    )


def test_cmd_advance_explicit_orphan_same_branch_leaf_uses_representative_row(
    tmp_path: Path,
    monkeypatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from gza import advance_engine as advance_engine_module

    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "in_progress"
    impl.branch = "feature/canonical"
    impl.has_commits = True
    store.update(impl)

    orphan = store.add("Completed orphan rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    assert orphan.id is not None
    orphan.status = "completed"
    orphan.completed_at = datetime(2026, 5, 12, 10, 0, tzinfo=UTC)
    orphan.branch = "feature/orphan"
    orphan.has_commits = True
    orphan.merge_status = "unmerged"
    store.update(orphan)
    orphan_unit = store.create_merge_unit(
        source_branch=orphan.branch,
        target_branch="main",
        owner_task_id=orphan.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(orphan.id, orphan_unit.id, "owner")

    unrelated_branches = tuple(f"feature/orphan-unrelated-{index}" for index in range(2))
    for index, branch in enumerate(unrelated_branches):
        _add_completed_impl_with_approved_review(
            store,
            branch,
            when=datetime(2026, 5, 12, 11 + index, 0, tzinfo=UTC),
        )

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review_task: SimpleNamespace(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    fake_git, ref_calls, branch_calls = _make_preload_recording_git(tmp_path)
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(impl.branch)

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, orphan.id)), "dry_run": True}))

    output = capsys.readouterr().out
    assert rc == 0
    assert "Needs attention (1 task):" in output
    assert f'{impl.id} implement "Implement feature"' in output
    assert "reason=no-descendant-on-the-impl-branch" in output
    assert "pending command evaluation" not in output
    _assert_scoped_preload_refs(
        ref_calls,
        branch_calls,
        requested_branch=impl.branch,
        unrelated_branches=unrelated_branches,
    )


def test_cmd_advance_explicit_dropped_owner_fallback_preloads_only_requested_lineage_refs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    setup_config(tmp_path)
    store = make_store(tmp_path)

    owner = store.add("Dropped owner", task_type="implement")
    assert owner.id is not None
    owner.status = "dropped"
    owner.completed_at = datetime(2026, 5, 12, 9, 0, tzinfo=UTC)
    owner.branch = "feature/dropped-owner-scope"
    owner.has_commits = True
    owner.merge_status = "unmerged"
    store.update(owner)

    requested = store.add("Completed descendant", task_type="rebase", based_on=owner.id, same_branch=True)
    assert requested.id is not None
    requested.status = "completed"
    requested.completed_at = datetime(2026, 5, 12, 10, 0, tzinfo=UTC)
    requested.branch = owner.branch
    requested.has_commits = True
    requested.merge_status = "unmerged"
    store.update(requested)

    unrelated_branches = tuple(f"feature/dropped-owner-unrelated-{index}" for index in range(2))
    for index, branch in enumerate(unrelated_branches):
        _add_completed_impl_with_approved_review(
            store,
            branch,
            when=datetime(2026, 5, 12, 11 + index, 0, tzinfo=UTC),
        )

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review_task: SimpleNamespace(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    fake_git, ref_calls, branch_calls = _make_preload_recording_git(tmp_path)
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(owner.branch)

    row = LineageOwnerRow(
        owner_task=owner,
        members=(owner, requested),
        tree=None,
        lineage_status="skipped",
        next_action={"type": "skip", "description": "nothing to do"},
        next_action_reason="precomputed",
        unresolved_tasks=(requested,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=None,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.git.Git", return_value=fake_git),
        patch(
            "gza.git.Git._run",
            return_value=SimpleNamespace(returncode=0, stdout="refs/remotes/origin/main\n", stderr=""),
        ),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=iter([row])),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, requested.id)), "dry_run": True}))

    assert rc == 0
    _assert_scoped_preload_refs(
        ref_calls,
        branch_calls,
        requested_branch=owner.branch,
        unrelated_branches=unrelated_branches,
    )


def test_advance_execution_remote_only_fresh_ref_no_longer_overrides_stale_local_branch(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    from gza import advance_engine as advance_engine_module
    from gza.git import ResolvedMergeSourceRef

    setup_config(tmp_path)
    store = make_store(tmp_path)
    branch = "feature/advance-stale-local"
    task = _add_mergeable_impl_with_failed_rebase(store, branch)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review_task: SimpleNamespace(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = True
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(f"origin/{branch}")
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1
    fake_git.merge.return_value = None

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, task.id)), "dry_run": True}))

    assert rc == 0
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "unmerged"

    fake_git.merge.assert_not_called()

    output = capsys.readouterr().out
    assert "Would advance 1 task(s):" in output
    assert "Run verify gate before merge" in output
    assert f"Merging 'origin/{branch}' into 'main'" not in output
    assert f"Merging '{branch}' into 'main'" not in output


def test_advance_execution_prefers_local_branch_when_origin_is_stale(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    from gza import advance_engine as advance_engine_module
    from gza.git import ResolvedMergeSourceRef

    setup_config(tmp_path)
    config = Config.load(tmp_path)
    config.merge_squash_threshold = 1
    store = make_store(tmp_path)
    branch = "feature/advance-local-ahead"
    task = _add_mergeable_impl_with_failed_rebase(store, branch)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review_task: SimpleNamespace(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = True
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(branch)
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1
    fake_git.merge.return_value = None

    resolved = _resolve_merge_subject(store, fake_git, task.id, target_branch="main")
    assert resolved is not None
    assert resolved.merge_source_ref == branch
    merge_args = _build_auto_merge_args(config, fake_git, resolved.merge_source_ref, "main")

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task.id))

    assert merge_args.squash is True
    assert rc == 0
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "unmerged"

    fake_git.merge.assert_not_called()

    output = capsys.readouterr().out
    assert "Run verify gate before merge" in output
    assert "verify epoch is unavailable; merge is blocked" in output


def test_cmd_advance_execution_fails_closed_when_only_origin_branch_exists(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Implement feature", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    task.branch = "feature/advance-origin-only"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="actionable",
        next_action=None,
        next_action_reason="merge",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=None,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = False
    fake_git.ref_exists.return_value = True
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(f"origin/{task.branch}")
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.merge.return_value = None

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch(
            "gza.git.Git._run",
            return_value=SimpleNamespace(returncode=0, stdout="refs/remotes/origin/main\n", stderr=""),
        ),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch(
            "gza.cli.git_ops.determine_next_action",
            return_value={"type": "merge", "description": "Merge"},
        ),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task.id))

    assert rc == 1
    assert all(call_args.args[0] != f"origin/{task.branch}" for call_args in fake_git.is_merged.call_args_list)
    assert all(call_args.args[0] != f"origin/{task.branch}" for call_args in fake_git.can_merge.call_args_list)
    assert all(call_args.args[0] != f"origin/{task.branch}" for call_args in fake_git.merge.call_args_list)
    output = capsys.readouterr().out
    assert f"Error: Task {task.id} has no resolvable merge source" in output
    assert f"Merging 'origin/{task.branch}' into 'main'" not in output


def test_cmd_advance_execution_uses_local_branch_when_origin_ref_is_reported(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Implement feature", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    task.branch = "feature/advance-local-only-proof"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="actionable",
        next_action=None,
        next_action_reason="merge",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=None,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = True
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(f"origin/{task.branch}")
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1
    fake_git.merge.return_value = None

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch(
            "gza.git.Git._run",
            return_value=SimpleNamespace(returncode=0, stdout="refs/remotes/origin/main\n", stderr=""),
        ),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch(
            "gza.cli.git_ops.determine_next_action",
            return_value={"type": "merge", "description": "Merge"},
        ),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task.id))

    assert rc == 0
    fake_git.merge.assert_called_once()
    assert fake_git.merge.call_args.args[0] == task.branch
    assert all(call_args.args[0] != f"origin/{task.branch}" for call_args in fake_git.is_merged.call_args_list)
    assert all(call_args.args[0] != f"origin/{task.branch}" for call_args in fake_git.can_merge.call_args_list)
    output = capsys.readouterr().out
    assert f"Merging '{task.branch}' into 'main'" in output
    assert f"Merging 'origin/{task.branch}' into 'main'" not in output


def test_reconcile_diverged_branch_with_origin_force_pushes_gza_rewrite(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    task = SimpleNamespace(id="gza-1", branch="feature/rewrite")

    git = MagicMock(spec=Git)
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "origin/feature/rewrite": "remote-old",
        "feature/rewrite": "local-new",
    }.get(ref)
    git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(
        None,
        (
            "Local branch 'feature/rewrite' and remote-tracking ref 'origin/feature/rewrite' diverged. "
            "Push, fetch, or reconcile them before advancing or merging."
        ),
    )
    git.count_commits_ahead.side_effect = [1, 1]
    git.is_merged.return_value = True

    result = _reconcile_diverged_branch_with_origin(config, git, task, target_branch="main")

    assert result.status == "reconciled"
    assert "force-with-lease" in result.message
    git.push_ref_force_with_lease.assert_called_once_with(
        "feature/rewrite",
        "feature/rewrite",
        remote="origin",
        expected_remote_oid="remote-old",
    )
    git.fetch.assert_not_called()
    assert git.is_merged.call_args_list == [
        call("feature/rewrite", into="origin/feature/rewrite", use_cherry=True),
        call("origin/feature/rewrite", into="feature/rewrite", use_cherry=True),
    ]
    git._run.assert_not_called()


def test_reconcile_diverged_branch_with_origin_force_pushes_stale_origin_rewrite(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    task = SimpleNamespace(id="gza-1b", branch="feature/rebased-rewrite")

    git = MagicMock(spec=Git)
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "origin/feature/rebased-rewrite": "remote-pre-rebase-tip",
        "feature/rebased-rewrite": "local-rebased-tip",
    }.get(ref)
    git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(
        None,
        (
            "Local branch 'feature/rebased-rewrite' and remote-tracking ref "
            "'origin/feature/rebased-rewrite' diverged. Push, fetch, or reconcile them "
            "before advancing or merging."
        ),
    )
    git.count_commits_ahead.side_effect = [2, 2]
    git.is_merged.side_effect = [True, True]

    result = _reconcile_diverged_branch_with_origin(config, git, task, target_branch="main")

    assert result.status == "reconciled"
    assert "force-with-lease" in result.message
    git.push_ref_force_with_lease.assert_called_once_with(
        "feature/rebased-rewrite",
        "feature/rebased-rewrite",
        remote="origin",
        expected_remote_oid="remote-pre-rebase-tip",
    )
    git.fetch.assert_not_called()
    assert git.is_merged.call_args_list == [
        call("feature/rebased-rewrite", into="origin/feature/rebased-rewrite", use_cherry=True),
        call("origin/feature/rebased-rewrite", into="feature/rebased-rewrite", use_cherry=True),
    ]
    git._run.assert_not_called()


def test_reconcile_diverged_branch_with_origin_force_pushes_dead_wip_savepoint_divergence(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    task = SimpleNamespace(id="gza-1c", branch="feature/dead-wip")

    git = MagicMock(spec=Git)
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "origin/feature/dead-wip": "remote-wip-tip",
        "feature/dead-wip": "local-final-tip",
    }.get(ref)
    git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(
        None,
        (
            "Local branch 'feature/dead-wip' and remote-tracking ref 'origin/feature/dead-wip' diverged. "
            "Push, fetch, or reconcile them before advancing or merging."
        ),
    )
    git.count_commits_ahead.side_effect = [1, 1]
    git.is_merged.side_effect = [True, False]
    git._run.side_effect = lambda *args, **kwargs: (
        SimpleNamespace(returncode=0, stdout="merge-base-oid\n")
        if args[:3] == ("merge-base", "feature/already-fetched-external", "origin/feature/already-fetched-external")
        else (
            SimpleNamespace(returncode=0, stdout="External commit\n")
            if args[:3] == ("log", "--format=%s", "merge-base-oid..origin/feature/already-fetched-external")
            else SimpleNamespace(returncode=0, stdout="")
        )
    )
    git._run.side_effect = [
        SimpleNamespace(returncode=0, stdout="merge-base-oid\n"),
        SimpleNamespace(returncode=0, stdout="WIP: gza task interrupted\n"),
    ]

    result = _reconcile_diverged_branch_with_origin(config, git, task, target_branch="main")

    assert result.status == "reconciled"
    assert "force-with-lease" in result.message
    git.push_ref_force_with_lease.assert_called_once_with(
        "feature/dead-wip",
        "feature/dead-wip",
        remote="origin",
        expected_remote_oid="remote-wip-tip",
    )
    git.fetch.assert_not_called()
    assert git._run.call_args_list == [
        call("merge-base", "feature/dead-wip", "origin/feature/dead-wip", check=False),
        call(
            "log",
            "--format=%s",
            "merge-base-oid..origin/feature/dead-wip",
            "--not",
            "feature/dead-wip",
            check=False,
        ),
    ]


def test_reconcile_diverged_branch_with_origin_rebases_already_fetched_external_commits(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    task = SimpleNamespace(id="gza-2a", branch="feature/already-fetched-external")

    git = MagicMock(spec=Git)
    git.branch_exists.return_value = True
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "origin/feature/already-fetched-external": "remote-visible",
        "feature/already-fetched-external": "local-tip",
    }.get(ref)
    git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(
        None,
        (
            "Local branch 'feature/already-fetched-external' and remote-tracking ref "
            "'origin/feature/already-fetched-external' diverged. Push, fetch, or reconcile them "
            "before advancing or merging."
        ),
    )
    git.count_commits_ahead.side_effect = [1, 1]
    git.is_merged.side_effect = [True, False]
    git._run.side_effect = lambda *args, **kwargs: (
        SimpleNamespace(returncode=0, stdout="merge-base-oid\n")
        if args[:3] == ("merge-base", "feature/already-fetched-external", "origin/feature/already-fetched-external")
        else (
            SimpleNamespace(returncode=0, stdout="External commit\n")
            if args[:3] == ("log", "--format=%s", "merge-base-oid..origin/feature/already-fetched-external")
            else SimpleNamespace(returncode=0, stdout="")
        )
    )

    worktree_git = MagicMock(spec=Git)
    worktree_git.rebase.return_value = None

    with (
        patch("gza.cli.git_ops.Git", return_value=worktree_git),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline("old", "target", "base"),
        ) as capture_baseline,
        patch("gza.cli.git_ops.publish_rebased_branch") as publish_rebased_branch,
    ):
        result = _reconcile_diverged_branch_with_origin(config, git, task, target_branch="main")

    assert result.status == "reconciled"
    git.push_ref_force_with_lease.assert_not_called()
    git.fetch.assert_called_once_with("origin")
    worktree_git.rebase.assert_called_once_with("main")
    capture_baseline.assert_called_once_with(
        worktree_git,
        branch="feature/already-fetched-external",
        target="main",
    )
    publish_rebased_branch.assert_called_once()
    assert "origin/feature/already-fetched-external" not in result.message
    assert git.is_merged.call_args_list == [
        call(
            "feature/already-fetched-external",
            into="origin/feature/already-fetched-external",
            use_cherry=True,
        ),
        call(
            "origin/feature/already-fetched-external",
            into="feature/already-fetched-external",
            use_cherry=True,
        ),
    ]
    git._run.assert_any_call(
        "merge-base",
        "feature/already-fetched-external",
        "origin/feature/already-fetched-external",
        check=False,
    )
    git._run.assert_any_call(
        "log",
        "--format=%s",
        "merge-base-oid..origin/feature/already-fetched-external",
        "--not",
        "feature/already-fetched-external",
        check=False,
    )


def test_reconcile_diverged_branch_with_origin_reports_already_aligned_after_rebase(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    task = SimpleNamespace(id="gza-2b", branch="feature/already-aligned")

    git = MagicMock(spec=Git)
    git.branch_exists.return_value = True
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "origin/feature/already-aligned": "remote-visible",
        "feature/already-aligned": "local-tip",
    }.get(ref)
    git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(
        None,
        (
            "Local branch 'feature/already-aligned' and remote-tracking ref "
            "'origin/feature/already-aligned' diverged. Push, fetch, or reconcile them "
            "before advancing or merging."
        ),
    )
    git.count_commits_ahead.side_effect = [1, 1]
    git.is_merged.side_effect = [True, False]

    worktree_git = MagicMock(spec=Git)
    worktree_git.rebase.return_value = None

    with (
        patch("gza.cli.git_ops.Git", return_value=worktree_git),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline("old", "target", "base"),
        ),
        patch(
            "gza.cli.git_ops.publish_rebased_branch",
            return_value=SimpleNamespace(pushed=False),
        ),
    ):
        result = _reconcile_diverged_branch_with_origin(config, git, task, target_branch="main")

    assert result.status == "reconciled"
    assert "verified origin was already aligned" in result.message
    assert "and pushed" not in result.message
    assert "local target 'main'" in result.message


def test_reconcile_diverged_branch_with_origin_rebases_after_remote_moves(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    task = SimpleNamespace(id="gza-2", branch="feature/external")

    git = MagicMock(spec=Git)
    git.branch_exists.return_value = True
    git.rev_parse_if_exists.side_effect = ["remote-old", "local-tip", "remote-new"]
    git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef("feature/external")
    git.count_commits_ahead.side_effect = [1, 0]
    git.push_ref_force_with_lease.side_effect = GitError("stale info")

    worktree_git = MagicMock(spec=Git)
    worktree_git.rebase.return_value = None

    with (
        patch("gza.cli.git_ops.Git", return_value=worktree_git),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline("old", "target", "base"),
        ) as capture_baseline,
        patch("gza.cli.git_ops.publish_rebased_branch") as publish_rebased_branch,
    ):
        result = _reconcile_diverged_branch_with_origin(config, git, task, target_branch="main")

    assert result.status == "reconciled"
    assert "Rebased 'feature/external' onto local target 'main'" in result.message
    git.fetch.assert_called_once_with("origin")
    worktree_git.rebase.assert_called_once_with("main")
    capture_baseline.assert_called_once_with(
        worktree_git,
        branch="feature/external",
        target="main",
    )
    publish_rebased_branch.assert_called_once()


def test_reconcile_diverged_branch_with_origin_builds_worktree_git_with_parent_env(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    task = SimpleNamespace(id="gza-env", branch="feature/env")
    parent_env = {"PATH": "/project/bin", "TOKEN": "owned"}

    git = MagicMock(spec=Git)
    git.env = parent_env
    git.branch_exists.return_value = True
    git.rev_parse_if_exists.side_effect = ["remote-old", "local-tip", "remote-new"]
    git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef("feature/env")
    git.count_commits_ahead.side_effect = [1, 0]
    git.push_ref_force_with_lease.side_effect = GitError("stale info")

    worktree_git = MagicMock(spec=Git)
    worktree_git.rebase.return_value = None

    with (
        patch("gza.cli.git_ops.Git", return_value=worktree_git) as git_cls,
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline("old", "target", "base"),
        ),
        patch("gza.cli.git_ops.publish_rebased_branch"),
    ):
        result = _reconcile_diverged_branch_with_origin(config, git, task, target_branch="main")

    assert result.status == "reconciled"
    expected_worktree = config.worktree_path / "advance-reconcile-gza-env"
    git_cls.assert_called_once_with(expected_worktree, env=parent_env)
    worktree_git.rebase.assert_called_once_with("main")


def test_promote_isolated_merge_builds_attached_target_git_with_parent_env(tmp_path: Path) -> None:
    parent_env = {"PATH": "/project/bin", "TOKEN": "owned"}
    attached_checkout = tmp_path / "attached-main"
    repo_git = MagicMock(spec=Git)
    repo_git.env = parent_env
    repo_git.rev_parse.return_value = "old-main"
    merge_git = MagicMock(spec=Git)
    merge_git.rev_parse.return_value = "new-main"
    attached_git = MagicMock(spec=Git)
    attached_git.has_changes.return_value = False

    with (
        patch("gza.cli.git_ops.active_worktree_path_for_branch", return_value=attached_checkout),
        patch("gza.cli.git_ops.Git", return_value=attached_git) as git_cls,
    ):
        warnings = _promote_isolated_merge_to_target_branch(repo_git, merge_git, "main")

    assert warnings == ()
    git_cls.assert_called_once_with(attached_checkout, env=parent_env)
    repo_git.update_ref.assert_called_once_with("refs/heads/main", "new-main", "old-main")
    attached_git.reset_hard.assert_called_once_with("refs/heads/main")
    merge_git.reset_hard.assert_called_once_with("refs/heads/main")


def test_reconcile_diverged_branch_with_origin_routes_conflicts_to_rebase(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    task = SimpleNamespace(id="gza-3", branch="feature/conflict")

    git = MagicMock(spec=Git)
    git.branch_exists.return_value = True
    git.rev_parse_if_exists.side_effect = ["remote-old", "local-tip", "remote-new"]
    git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef("feature/conflict")
    git.count_commits_ahead.side_effect = [1, 0]
    git.push_ref_force_with_lease.side_effect = GitError("stale info")

    worktree_git = MagicMock(spec=Git)
    worktree_git.rebase.side_effect = GitError("conflict")

    with (
        patch("gza.cli.git_ops.Git", return_value=worktree_git),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline("old", "target", "base"),
        ),
    ):
        result = _reconcile_diverged_branch_with_origin(config, git, task, target_branch="main")

    assert result.status == "needs_attention"
    assert result.attention_reason == "reconcile-needs-manual-resolution"
    assert "local target 'main'" in result.message
    assert "origin/feature/conflict" not in result.message
    assert "remote-tracking ref" not in result.message
    worktree_git.rebase_abort.assert_called_once()


def test_advance_batch_limit_skips_reconcile_conflict_fallback_without_spawning_rebase(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    first = store.add("Needs explicit rebase", task_type="implement")
    second = store.add("Needs reconcile fallback", task_type="implement")
    for task, branch in ((first, "feature/needs-rebase"), (second, "feature/reconcile-fallback")):
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

    first_row = LineageOwnerRow(
        owner_task=first,
        members=(first,),
        tree=None,
        lineage_status="actionable",
        next_action={"type": "needs_rebase", "description": "Create rebase task"},
        next_action_reason="test",
        unresolved_tasks=(first,),
        unresolved_leaf_summary=(),
    )
    second_row = LineageOwnerRow(
        owner_task=second,
        members=(second,),
        tree=None,
        lineage_status="actionable",
        next_action={
            "type": "reconcile_branch_divergence",
            "description": "Reconcile diverged local/origin refs",
        },
        next_action_reason="test",
        unresolved_tasks=(second,),
        unresolved_leaf_summary=(),
    )

    created_rebases: list[str] = []

    def _create_rebase_task(_store, parent_id: str, branch: str, target: str, *, trigger_source: str):
        created = _store.add(
            prompt=f"Rebase {branch} onto {target}",
            task_type="rebase",
            based_on=parent_id,
            same_branch=True,
            trigger_source=trigger_source,
        )
        assert created.id is not None
        created_rebases.append(created.id)
        return created

    args = _advance_args(tmp_path, first.id)
    args.task_id = None
    args.batch = 1

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[first_row, second_row]),
        patch("gza.cli.git_ops._create_rebase_task", side_effect=_create_rebase_task),
        patch("gza.cli.git_ops._prepare_task_for_immediate_execution", side_effect=lambda _config, task, **_k: task),
        patch(
            "gza.cli.advance_executor._prepare_task_for_reserved_launch",
            side_effect=lambda _config, task, permit, rollback_on_failure: task,
        ),
        patch("gza.cli.git_ops._spawn_background_worker", return_value=0) as spawn_worker,
        patch(
            "gza.cli.git_ops._reconcile_diverged_branch_with_origin",
            return_value=SimpleNamespace(
                status="needs_rebase",
                message="Mechanical rebase conflicted",
                rebase_target="main",
            ),
        ),
    ):
        rc = cmd_advance(args)

    assert rc == 0
    assert spawn_worker.call_count == 1
    assert len(created_rebases) == 1
    output = capsys.readouterr().out
    assert output.index(str(second.id)) < output.index(str(first.id))
    assert "Created rebase task" in output
    assert "SKIP: batch limit reached (1/1), cannot start rebase worker" in output


@pytest.mark.timeout(4, method="signal")
def test_advance_dry_run_surfaces_diverged_merge_source_for_reconcile(
    tmp_path: Path,
    capsys,
) -> None:
    from gza.git import ResolvedMergeSourceRef

    setup_config(tmp_path)
    store = make_store(tmp_path)
    branch = "feature/advance-diverged"

    task = store.add("Implement feature", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = branch
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    args = _advance_args(tmp_path, task.id)
    args.dry_run = True

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(
        None,
        (
            "merge-source-needs-manual-resolution: "
            f"local branch '{branch}' and remote tracking ref 'origin/{branch}' diverged"
        ),
    )
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = True
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
    ):
        rc = cmd_advance(args)

    output = capsys.readouterr().out
    assert rc == 0
    assert "Would advance 1 task(s):" in output
    assert "Run verify gate before review" in output
    assert "Needs attention" not in output


def test_cmd_advance_uses_shared_lifecycle_execution_gate(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Implement feature", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/shared-lifecycle-gate"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="actionable",
        next_action={"type": "create_review", "description": "Create review before merge"},
        next_action_reason="review",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=task,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = True

    gate_calls: list[tuple[str, int]] = []

    def _record_gate(action, *, free_worker_slots):
        gate_calls.append((str(action.get("type")), free_worker_slots))
        return real_should_execute_lifecycle_action(action, free_worker_slots=free_worker_slots)

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch("gza.cli._lifecycle_actions.should_execute_lifecycle_action", side_effect=_record_gate),
        patch(
            "gza.cli.git_ops.execute_advance_action",
            return_value=AdvanceActionExecutionResult(
                action_type="create_review",
                status="success",
                message="Started review",
                success_message="Started review",
                handled_task_id="testproject-2",
                attempted_spawn=True,
                worker_started=True,
                worker_label="review",
                worker_consuming=True,
            ),
        ),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task.id))

    assert rc == 0
    assert any(action_type == "verify_gate" and free_worker_slots > 0 for action_type, free_worker_slots in gate_calls)
    assert "Will advance 1 task(s):" in capsys.readouterr().out


def test_cmd_advance_orders_direct_non_worker_actions_before_slot_gated_worker_actions(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    review_owner = store.add("Implementation still needs review", task_type="implement")
    assert review_owner.id is not None
    review_owner.status = "completed"
    review_owner.completed_at = datetime.now(UTC)
    review_owner.branch = "feature/advance-slot-gated-review"
    review_owner.merge_status = "unmerged"
    review_owner.has_commits = True
    store.update(review_owner)

    plan_owner = store.add("Approved plan ready to materialize", task_type="plan")
    assert plan_owner.id is not None
    plan_owner.status = "completed"
    plan_owner.completed_at = datetime.now(UTC)
    store.update(plan_owner)

    rows = [
        LineageOwnerRow(
            owner_task=review_owner,
            members=(review_owner,),
            tree=None,
            lineage_status="actionable",
            next_action=None,
            next_action_reason="review",
            unresolved_tasks=(review_owner,),
            unresolved_leaf_summary=(),
            lifecycle_action_task=review_owner,
            recovery_action_task=None,
            recovery_leaf_task=None,
        ),
        LineageOwnerRow(
            owner_task=plan_owner,
            members=(plan_owner,),
            tree=None,
            lineage_status="actionable",
            next_action=None,
            next_action_reason="materialize",
            unresolved_tasks=(plan_owner,),
            unresolved_leaf_summary=(),
            lifecycle_action_task=plan_owner,
            recovery_action_task=None,
            recovery_leaf_task=None,
        ),
    ]

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"

    executed_actions: list[str] = []

    def _fake_determine(_config, _store, _git, task, _target_branch, **_kwargs):
        if task.id == review_owner.id:
            return {"type": "create_review", "description": "Create review before merge"}
        if task.id == plan_owner.id:
            return {
                "type": "materialize_plan_slices",
                "description": "Materialize implementation slices from approved plan review",
            }
        raise AssertionError(f"unexpected task: {task.id}")

    def _fake_execute(*, task, action, context):
        if not context.dry_run:
            executed_actions.append(f"{task.id}:{action['type']}")
        return AdvanceActionExecutionResult(
            action_type=action["type"],
            status="success",
            message="Materialized plan slices",
            success_message="Materialized plan slices",
            work_done=True,
            worker_consuming=False,
        )

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch(
            "gza.cli.git_ops.get_concurrency_snapshot",
            return_value=SimpleNamespace(available=0, running=1, limit=1),
        ),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=rows),
        patch("gza.cli.git_ops.determine_next_action", side_effect=_fake_determine),
        patch("gza.cli.git_ops.execute_advance_action", side_effect=_fake_execute),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, review_owner.id)), "task_id": None}))

    output = capsys.readouterr().out
    assert rc == 0
    assert "Will advance 1 task(s):" in output
    assert executed_actions == [f"{plan_owner.id}:materialize_plan_slices"]
    assert output.index(str(plan_owner.id)) < output.index(str(review_owner.id))
    assert "Materialize implementation slices from approved plan review" in output
    assert str(review_owner.id) in output
    assert "already at max concurrent tasks: 1 running, limit is 1, skipping" in output
    assert "skipping" in output


def test_cmd_advance_reprojects_selected_merge_candidate_for_preview_and_execution(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Advance selected merge candidate", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/advance-selected-merge"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="actionable",
        next_action=None,
        next_action_reason="merge",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=task,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = False
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1

    selected_flags: list[bool] = []
    executed_action_types: list[tuple[bool, str]] = []

    def _fake_determine(_config, _store, _git, planned_task, _target_branch, **kwargs):
        assert planned_task.id == task.id
        selected = bool(kwargs.get("selected_for_merge", False))
        selected_flags.append(selected)
        if selected:
            return {"type": "needs_rebase", "description": "rebase --resolve (conflicts detected)"}
        return {"type": "merge", "description": "Merge"}

    def _fake_execute(*, task, action, context):
        executed_action_types.append((context.dry_run, str(action["type"])))
        if context.dry_run:
            return AdvanceActionExecutionResult(
                action_type=str(action["type"]),
                status="dry_run",
                message="Would create rebase task",
                worker_consuming=True,
            )
        return AdvanceActionExecutionResult(
            action_type=str(action["type"]),
            status="success",
            message="Started rebase",
            success_message="Started rebase",
            attempted_spawn=True,
            worker_started=True,
            worker_label="rebase",
            worker_consuming=True,
        )

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch(
            "gza.cli.git_ops.get_concurrency_snapshot",
            return_value=SimpleNamespace(available=1, running=0, limit=1),
        ),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch("gza.cli.git_ops.determine_next_action", side_effect=_fake_determine),
        patch("gza.cli.git_ops.execute_advance_action", side_effect=_fake_execute),
        patch("gza.cli.git_ops._execute_merge_action") as execute_merge,
    ):
        dry_run_args = argparse.Namespace(**{**vars(_advance_args(tmp_path, task.id)), "dry_run": True})
        dry_run_rc = cmd_advance(dry_run_args)
        execute_rc = cmd_advance(_advance_args(tmp_path, task.id))

    output = capsys.readouterr().out
    assert dry_run_rc == 0
    assert execute_rc == 0
    assert selected_flags.count(True) >= 2
    assert "Would create rebase task" in output
    assert "Started rebase" in output
    assert executed_action_types == [
        (True, "needs_rebase"),
        (True, "needs_rebase"),
        (False, "needs_rebase"),
    ]
    execute_merge.assert_not_called()


def test_cmd_advance_reprojected_selected_merge_rebase_respects_zero_worker_capacity(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Advance selected merge candidate at capacity", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/advance-selected-merge-at-capacity"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="actionable",
        next_action=None,
        next_action_reason="merge",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=task,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = False
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1

    selected_flags: list[bool] = []

    def _fake_determine(_config, _store, _git, planned_task, _target_branch, **kwargs):
        assert planned_task.id == task.id
        selected = bool(kwargs.get("selected_for_merge", False))
        selected_flags.append(selected)
        if selected:
            return {"type": "needs_rebase", "description": "rebase --resolve (conflicts detected)"}
        return {"type": "merge", "description": "Merge"}

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch(
            "gza.cli.git_ops.get_concurrency_snapshot",
            return_value=SimpleNamespace(available=0, running=1, limit=1),
        ),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch("gza.cli.git_ops.determine_next_action", side_effect=_fake_determine),
        patch("gza.cli.git_ops.execute_advance_action") as execute_action,
        patch("gza.cli.git_ops._execute_merge_action") as execute_merge,
    ):
        dry_run_args = argparse.Namespace(**{**vars(_advance_args(tmp_path, task.id)), "dry_run": True})
        dry_run_rc = cmd_advance(dry_run_args)
        execute_rc = cmd_advance(_advance_args(tmp_path, task.id))

    output = capsys.readouterr().out
    assert dry_run_rc == 0
    assert execute_rc == 0
    assert selected_flags.count(True) >= 2
    assert "Would create rebase task" not in output
    assert "Started rebase" not in output
    assert "already at max concurrent tasks: 1 running, limit is 1, skipping" in output
    execute_action.assert_not_called()
    execute_merge.assert_not_called()


def test_cmd_advance_reprojected_selected_merge_candidate_with_only_origin_branch_parks_for_manual_resolution(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Advance remote-only selected merge candidate", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime(2026, 6, 20, 12, 0, tzinfo=UTC)
    task.branch = "feature/advance-selected-origin-only"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="actionable",
        next_action={"type": "merge", "description": "Merge"},
        next_action_reason="merge",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=None,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.side_effect = lambda branch_name: branch_name != task.branch
    fake_git.ref_exists.side_effect = lambda ref: ref == f"origin/{task.branch}"
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(f"origin/{task.branch}")
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch(
            "gza.cli.git_ops.get_concurrency_snapshot",
            return_value=SimpleNamespace(available=1, running=0, limit=1),
        ),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch("gza.cli.git_ops.execute_advance_action") as execute_action,
        patch("gza.cli.git_ops._execute_merge_action") as execute_merge,
    ):
        dry_run_rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, task.id)), "dry_run": True}))
        execute_rc = cmd_advance(_advance_args(tmp_path, task.id))

    output = capsys.readouterr().out
    assert dry_run_rc == 0
    assert execute_rc == 0
    assert "Would create rebase task" not in output
    assert "Started rebase" not in output
    assert "rebase --resolve (conflicts detected)" not in output
    assert "No eligible tasks to advance" in output
    execute_action.assert_not_called()
    execute_merge.assert_not_called()
    assert all(call.args[0] != f"origin/{task.branch}" for call in fake_git.is_merged.call_args_list)
    assert all(call.args[0] != f"origin/{task.branch}" for call in fake_git.can_merge.call_args_list)
    assert all(call.args[0] != f"origin/{task.branch}" for call in fake_git.is_ancestor.call_args_list)


def test_cmd_advance_all_tasks_query_uses_one_read_session_connection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Advance all-tasks read session", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime(2026, 5, 12, 9, 0, tzinfo=UTC)
    task.branch = "feature/advance-all-read-session"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    opened_connections: list[tuple[bool, object]] = []
    original_open_connection = store._open_connection

    def _tracking_open_connection(*, close_on_exit: bool):
        conn = original_open_connection(close_on_exit=close_on_exit)
        opened_connections.append((close_on_exit, conn))
        return conn

    monkeypatch.setattr(store, "_open_connection", _tracking_open_connection)

    args = _advance_args(tmp_path, task.id)
    args.task_id = None
    args.dry_run = True

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.current_branch.return_value = "main"
    fake_git.default_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = False
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        _mock_git_default_branch_run(),
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "skip", "description": "nothing to do"}),
    ):
        rc = cmd_advance(args)

    assert rc == 0
    assert len([conn for close_on_exit, conn in opened_connections if close_on_exit is False]) == 1


def test_cmd_advance_reports_isolated_promotion_failure_phase(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = _completed_merge_task(store, "Advance isolated promotion failure", "feature/advance-promotion-failure")
    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="actionable",
        next_action={"type": "merge", "description": "Merge"},
        next_action_reason="merge",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=None,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )
    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = False
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1
    merge_result = _MergeActionResult(
        rc=1,
        created_followups=[],
        reused_followups=[],
        created_investigation_task_ids=[],
        reused_investigation_task_ids=[],
        created_deferred_blockers=[],
        reused_deferred_blockers=[],
        status="isolated_merge_failed",
        block_reason="target promotion refused after rollback",
    )

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        _mock_git_default_branch_run(),
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "merge", "description": "Merge"}),
        patch("gza.cli.git_ops._execute_merge_action", return_value=merge_result),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task.id))

    output = capsys.readouterr().out
    assert rc == 1
    assert "isolated merge promotion failed: target promotion refused after rollback" in output
    assert "Merge failed" not in output


def test_cmd_advance_all_tasks_default_dry_run_skips_deferred_prerequisite_reconciliation(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    dependency, _owner, failed = _add_prerequisite_unmerged_failed_child(
        store,
        owner_branch="feature/all-tasks-prereq-reconcile",
    )
    _mark_dependency_merge_unit_merged(store, dependency)

    args = _advance_args(tmp_path, failed.id)
    args.task_id = None
    args.dry_run = True
    args.no_resume_failed = False

    fake_git = _make_read_session_reconciliation_git(tmp_path, failed.branch)
    before = _durable_preview_snapshot(store)
    row = _failed_recovery_owner_row(failed)

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        _mock_git_default_branch_run(),
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[]),
        patch("gza.recovery_engine.is_resolved_by_merged_target", return_value=False),
        patch("gza.recovery_engine._is_resolved_by_landed_lineage", return_value=False),
        patch("gza.recovery_engine.get_completed_same_slice_sibling_attempt", return_value=None),
        patch(
            "gza.cli.advance_engine.determine_next_action",
            return_value={"type": "skip", "description": "nothing to do"},
        ),
        patch(
            "gza.recovery_engine._load_merge_context",
            return_value=_MergeContext(git=fake_git, default_branch="main"),
        ),
        patch("gza.recovery_engine.resolve_task_merge_state_for_target", return_value="redundant"),
    ):
        rc = cmd_advance(args)

    assert rc == 0
    assert _durable_preview_snapshot(store) == before


def test_cmd_advance_all_tasks_default_run_persists_prerequisite_reconciliation(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    dependency, _owner, failed = _add_prerequisite_unmerged_failed_child(
        store,
        owner_branch="feature/all-tasks-prereq-reconcile-run",
    )
    _mark_dependency_merge_unit_merged(store, dependency)

    args = _advance_args(tmp_path, failed.id)
    args.task_id = None
    args.dry_run = False
    args.no_resume_failed = False

    fake_git = _make_read_session_reconciliation_git(tmp_path, failed.branch)

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        _mock_git_default_branch_run(),
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[]),
        patch("gza.recovery_engine.is_resolved_by_merged_target", return_value=False),
        patch("gza.recovery_engine._is_resolved_by_landed_lineage", return_value=False),
        patch("gza.recovery_engine.get_completed_same_slice_sibling_attempt", return_value=None),
        patch(
            "gza.recovery_engine._load_merge_context",
            return_value=_MergeContext(git=fake_git, default_branch="main"),
        ),
        patch("gza.recovery_engine.resolve_task_merge_state_for_target", return_value="redundant"),
    ):
        rc = cmd_advance(args)

    assert rc == 0
    reconciled_unit = store.resolve_merge_unit_for_task(failed.id)
    assert reconciled_unit is not None
    assert reconciled_unit.state == "redundant"


def test_cmd_advance_all_tasks_dry_run_preserves_prerequisite_reconciliation_for_owner_row(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    dependency, _owner, failed = _add_prerequisite_unmerged_failed_child(
        store,
        owner_branch="feature/all-tasks-owner-prereq-reconcile",
    )
    _mark_dependency_merge_unit_merged(store, dependency)

    args = _advance_args(tmp_path, failed.id)
    args.task_id = None
    args.dry_run = True
    args.no_resume_failed = False

    fake_git = _make_read_session_reconciliation_git(tmp_path, failed.branch)
    before = _durable_preview_snapshot(store)
    row = _failed_recovery_owner_row(failed)

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        _mock_git_default_branch_run(),
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch("gza.advance_engine.resolve_task_merge_state_for_target", return_value="redundant"),
        patch("gza.merge_state.resolve_task_merge_state_for_target", return_value="redundant"),
        patch("gza.recovery_engine.resolve_task_merge_state_for_target", return_value="redundant"),
    ):
        rc = cmd_advance(args)

    output = capsys.readouterr().out
    assert rc == 0
    assert "No eligible tasks to advance" in output
    assert _durable_preview_snapshot(store) == before


def test_cmd_advance_all_tasks_run_persists_prerequisite_reconciliation_for_owner_row(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    dependency, _owner, failed = _add_prerequisite_unmerged_failed_child(
        store,
        owner_branch="feature/all-tasks-owner-prereq-reconcile-run",
    )
    _mark_dependency_merge_unit_merged(store, dependency)

    args = _advance_args(tmp_path, failed.id)
    args.task_id = None
    args.dry_run = False
    args.no_resume_failed = False

    fake_git = _make_read_session_reconciliation_git(tmp_path, failed.branch)
    row = _failed_recovery_owner_row(failed)

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        _mock_git_default_branch_run(),
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch("gza.advance_engine.resolve_task_merge_state_for_target", return_value="redundant"),
        patch("gza.merge_state.resolve_task_merge_state_for_target", return_value="redundant"),
        patch("gza.recovery_engine.resolve_task_merge_state_for_target", return_value="redundant"),
    ):
        rc = cmd_advance(args)

    assert rc == 0
    reconciled_unit = store.resolve_merge_unit_for_task(failed.id)
    assert reconciled_unit is not None
    assert reconciled_unit.state == "redundant"


def test_cmd_advance_explicit_owner_row_dry_run_preserves_prerequisite_reconciliation(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    dependency, _owner, failed = _add_prerequisite_unmerged_failed_child(
        store,
        owner_branch="feature/explicit-owner-prereq-reconcile",
    )
    _mark_dependency_merge_unit_merged(store, dependency)

    fake_git = _make_read_session_reconciliation_git(tmp_path, failed.branch)
    before = _durable_preview_snapshot(store)
    row = _failed_recovery_owner_row(failed)

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        _mock_git_default_branch_run(),
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch("gza.advance_engine.resolve_task_merge_state_for_target", return_value="empty"),
        patch("gza.merge_state.resolve_task_merge_state_for_target", return_value="empty"),
        patch("gza.recovery_engine.resolve_task_merge_state_for_target", return_value="empty"),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, failed.id)), "dry_run": True}))

    output = capsys.readouterr().out
    assert rc == 0
    assert "No eligible tasks to advance" in output
    assert _durable_preview_snapshot(store) == before


def test_cmd_advance_explicit_owner_row_run_persists_prerequisite_reconciliation(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    dependency, _owner, failed = _add_prerequisite_unmerged_failed_child(
        store,
        owner_branch="feature/explicit-owner-prereq-reconcile-run",
    )
    _mark_dependency_merge_unit_merged(store, dependency)

    fake_git = _make_read_session_reconciliation_git(tmp_path, failed.branch)
    row = _failed_recovery_owner_row(failed)

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        _mock_git_default_branch_run(),
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch("gza.advance_engine.resolve_task_merge_state_for_target", return_value="empty"),
        patch("gza.merge_state.resolve_task_merge_state_for_target", return_value="empty"),
        patch("gza.recovery_engine.resolve_task_merge_state_for_target", return_value="empty"),
    ):
        rc = cmd_advance(_advance_args(tmp_path, failed.id))

    assert rc == 0
    reconciled_unit = store.resolve_merge_unit_for_task(failed.id)
    assert reconciled_unit is not None
    assert reconciled_unit.state == "empty"


def test_cmd_advance_repeat_dry_run_preserves_prerequisite_reconciliation_for_owner_row(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    dependency, _owner, failed = _add_prerequisite_unmerged_failed_child(
        store,
        owner_branch="feature/repeat-owner-prereq-reconcile",
    )
    _mark_dependency_merge_unit_merged(store, dependency)

    args = _advance_args(tmp_path, failed.id)
    args.dry_run = True
    args.repeat = True
    args.max_iterations = 1

    fake_git = _make_read_session_reconciliation_git(tmp_path, failed.branch)
    before = _durable_preview_snapshot(store)
    row = _failed_recovery_owner_row(failed)

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        _mock_git_default_branch_run(),
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch("gza.advance_engine.resolve_task_merge_state_for_target", return_value="empty"),
        patch("gza.merge_state.resolve_task_merge_state_for_target", return_value="empty"),
        patch("gza.recovery_engine.resolve_task_merge_state_for_target", return_value="empty"),
    ):
        rc = cmd_advance(args)

    output = capsys.readouterr().out
    assert rc == 0
    assert "Advance repeat stopped on skip" in output
    assert _durable_preview_snapshot(store) == before


def _add_legacy_pr_required_failed_task(store: Any, *, branch: str) -> Any:
    task = store.add("Legacy publication failure", task_type="implement", create_pr=True)
    assert task.id is not None
    task.status = "failed"
    task.failure_reason = "PR_REQUIRED"
    task.branch = branch
    task.has_commits = True
    task.completed_at = datetime(2026, 5, 16, 9, 0, tzinfo=UTC)
    store.update(task)
    return task


def test_cmd_advance_explicit_owner_row_dry_run_preserves_legacy_pr_required_reconciliation(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    failed = _add_legacy_pr_required_failed_task(store, branch="feature/explicit-owner-pr-required")
    row = _failed_recovery_owner_row(failed)

    fake_git = _make_read_session_reconciliation_git(tmp_path, failed.branch)
    before = _durable_preview_snapshot(store)

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        _mock_git_default_branch_run(),
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch("gza.advance_engine.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.merge_state.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.recovery_engine.resolve_task_merge_state_for_target", return_value="unmerged"),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, failed.id)), "dry_run": True}))

    output = capsys.readouterr().out
    assert rc == 0
    assert failed.id in output
    assert "Reconcile branch publication (BRANCH_UNPUSHABLE)" in output
    assert store.get(failed.id).failure_reason == "PR_REQUIRED"
    assert _durable_preview_snapshot(store) == before


def test_cmd_advance_explicit_owner_row_run_persists_legacy_pr_required_reconciliation(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    failed = _add_legacy_pr_required_failed_task(store, branch="feature/explicit-owner-pr-required-run")
    row = _failed_recovery_owner_row(failed)

    fake_git = _make_read_session_reconciliation_git(tmp_path, failed.branch)
    result = AdvanceActionExecutionResult(
        action_type="reconcile_branch_divergence",
        status="skip",
        message="reconcile execution skipped in test",
    )

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        _mock_git_default_branch_run(),
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch("gza.cli.git_ops.execute_advance_action", return_value=result),
        patch("gza.advance_engine.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.merge_state.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.recovery_engine.resolve_task_merge_state_for_target", return_value="unmerged"),
    ):
        rc = cmd_advance(_advance_args(tmp_path, failed.id))

    assert rc == 0
    assert store.get(failed.id).failure_reason == "BRANCH_UNPUSHABLE"


def test_cmd_advance_explicit_no_owner_fallback_dry_run_skips_prerequisite_reconciliation(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    dependency, _owner, failed = _add_prerequisite_unmerged_failed_child(
        store,
        owner_branch="feature/explicit-no-owner-prereq-reconcile",
    )
    _mark_dependency_merge_unit_merged(store, dependency)

    fake_git = _make_read_session_reconciliation_git(tmp_path, failed.branch)
    before = _durable_preview_snapshot(store)

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[]),
        patch("gza.recovery_engine.is_resolved_by_merged_target", return_value=False),
        patch("gza.recovery_engine._is_resolved_by_landed_lineage", return_value=False),
        patch("gza.recovery_engine.get_completed_same_slice_sibling_attempt", return_value=None),
        patch("gza.recovery_engine.resolve_task_merge_state_for_target", return_value="empty"),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, failed.id)), "dry_run": True}))

    assert rc == 0
    assert _durable_preview_snapshot(store) == before


def test_cmd_advance_explicit_no_owner_fallback_run_persists_prerequisite_reconciliation(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    dependency, _owner, failed = _add_prerequisite_unmerged_failed_child(
        store,
        owner_branch="feature/explicit-no-owner-prereq-reconcile-run",
    )
    _mark_dependency_merge_unit_merged(store, dependency)

    fake_git = _make_read_session_reconciliation_git(tmp_path, failed.branch)

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[]),
        patch("gza.recovery_engine.is_resolved_by_merged_target", return_value=False),
        patch("gza.recovery_engine._is_resolved_by_landed_lineage", return_value=False),
        patch("gza.recovery_engine.get_completed_same_slice_sibling_attempt", return_value=None),
        patch("gza.recovery_engine.resolve_task_merge_state_for_target", return_value="empty"),
    ):
        rc = cmd_advance(_advance_args(tmp_path, failed.id))

    assert rc == 0
    reconciled_unit = store.resolve_merge_unit_for_task(failed.id)
    assert reconciled_unit is not None
    assert reconciled_unit.state == "empty"


def test_advance_retryable_provider_attention_recommends_retry_or_reimplement_for_failed_impl(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Investigate flaky provider run", task_type="implement")
    assert task.id is not None
    task.status = "failed"
    task.failure_reason = "RETRYABLE_PROVIDER_ERROR"
    task.completed_at = datetime.now(UTC)
    store.update(task)

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="needs_attention",
        next_action={
            "type": "needs_discussion",
            "description": "Fresh retry already consumed; retryable provider error now requires manual review",
            "needs_attention_reason": "retryable-provider-error",
            "subject_task_id": task.id,
        },
        next_action_reason="retryable-provider-error",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
    )

    args = _advance_args(tmp_path, task.id)
    args.task_id = None

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
    ):
        rc = cmd_advance(args)

    output = capsys.readouterr().out
    assert rc == 0
    assert "No eligible tasks to advance" in output
    assert "reason=retryable-provider-error" in output
    assert f"Recommended next step: uv run gza fix {task.id}" not in output
    assert "Recommended next step: retry or re-implement instead." in output


def test_cmd_advance_merge_renders_off_topic_investigation_ids(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Advance merge should surface investigations", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/advance-investigations"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="actionable",
        next_action={"type": "merge", "description": "Merge (previous review addressed)"},
        next_action_reason="merge",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=task,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = False
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1

    action = {
        "type": "merge",
        "description": "Merge (previous review addressed)",
        "created_investigation_task_ids": ("gza-7010",),
        "reused_investigation_task_ids": ("gza-7009",),
    }

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch("gza.cli.git_ops.determine_next_action", return_value=action),
        patch(
            "gza.cli.git_ops._execute_merge_action",
            return_value=SimpleNamespace(
                rc=0,
                created_followups=[],
                reused_followups=[],
                created_investigation_task_ids=["gza-7010"],
                reused_investigation_task_ids=["gza-7009"],
            ),
        ),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task.id))

    output = capsys.readouterr().out
    assert rc == 0
    assert "✓ Created investigation task(s): gza-7010" in output
    assert "↺ Reused investigation task(s): gza-7009" in output
    assert "✓ Merged" in output


def test_cmd_advance_merge_renders_deferred_blocker_ids(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Advance merge should surface deferred blockers", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/advance-deferred-blockers"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    created = DbTask(id="gza-7110", prompt="created blocker", task_type="implement")
    reused = DbTask(id="gza-7109", prompt="reused blocker", task_type="implement")

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="actionable",
        next_action={"type": "merge", "description": "Merge and defer blockers"},
        next_action_reason="merge",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=task,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = False
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch("gza.cli.git_ops.determine_next_action", return_value=row.next_action),
        patch(
            "gza.cli.git_ops._execute_merge_action",
            return_value=SimpleNamespace(
                rc=0,
                created_followups=[],
                reused_followups=[],
                created_deferred_blockers=[created],
                reused_deferred_blockers=[reused],
                created_investigation_task_ids=[],
                reused_investigation_task_ids=[],
            ),
        ),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task.id))

    output = capsys.readouterr().out
    assert rc == 0
    assert "✓ Created deferred blocker task(s): gza-7110" in output
    assert "↺ Reused deferred blocker task(s): gza-7109" in output
    assert "✓ Merged" in output


def test_cmd_advance_partial_deferred_blocker_failure_renders_ids_before_merge_error(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Advance merge partial deferred blocker failure", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/advance-deferred-blockers-partial"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    created = DbTask(id="gza-7120", prompt="created blocker", task_type="implement")
    reused = DbTask(id="gza-7119", prompt="reused blocker", task_type="implement")
    action = {"type": "merge", "description": "Merge and defer blockers"}
    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="actionable",
        next_action=action,
        next_action_reason="merge",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=task,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )
    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = False
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch("gza.cli.git_ops.determine_next_action", return_value=action),
        patch(
            "gza.cli.git_ops._execute_merge_action",
            return_value=SimpleNamespace(
                rc=1,
                created_followups=[],
                reused_followups=[],
                created_deferred_blockers=[created],
                reused_deferred_blockers=[reused],
                created_investigation_task_ids=[],
                reused_investigation_task_ids=[],
                status="merge_failed",
                block_reason="simulated merge failure after deferred blocker materialization",
            ),
        ),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task.id))

    output = capsys.readouterr().out
    assert rc == 1
    assert output.index("✓ Created deferred blocker task(s): gza-7120") < output.index("✗ Merge failed")
    assert output.index("↺ Reused deferred blocker task(s): gza-7119") < output.index("✗ Merge failed")


def test_cmd_advance_repeat_partial_deferred_blocker_failure_renders_ids_before_nonzero(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Advance repeat partial deferred blocker failure", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/advance-repeat-deferred-blockers-partial"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    created = DbTask(id="gza-7130", prompt="created blocker", task_type="implement")
    reused = DbTask(id="gza-7129", prompt="reused blocker", task_type="implement")
    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = False
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "merge", "description": "Merge task"}),
        patch(
            "gza.cli.git_ops._execute_merge_action",
            return_value=_MergeActionResult(
                rc=1,
                created_followups=[],
                reused_followups=[],
                created_deferred_blockers=[created],
                reused_deferred_blockers=[reused],
                created_investigation_task_ids=[],
                reused_investigation_task_ids=[],
                status="merge_failed",
                block_reason="simulated merge failure after partial fan-out",
            ),
        ),
    ):
        rc = cmd_advance(
            argparse.Namespace(
                **{
                    **vars(_advance_args(tmp_path, task.id)),
                    "repeat": True,
                    "max_iterations": 1,
                }
            )
        )

    output = capsys.readouterr().out
    assert rc == 1
    assert "cycle 1: merge -> error: simulated merge failure after partial fan-out" in output
    assert "✓ Created deferred blocker task(s): gza-7130" in output
    assert "↺ Reused deferred blocker task(s): gza-7129" in output


def test_advance_post_merge_red_main_skips_later_merges_and_surfaces_attention(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    first, _first_review = _add_completed_impl_with_approved_review(
        store,
        "feature/advance-main-red-1",
        when=datetime.now(UTC),
    )
    second, _second_review = _add_completed_impl_with_approved_review(
        store,
        "feature/advance-main-red-2",
        when=datetime.now(UTC),
    )

    args = _advance_args(tmp_path, first.id)
    args.task_id = None

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = True
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1

    main_verify_task = store.add(
        "System alert: local main integration verify", task_type="internal", skip_learnings=True
    )
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    store.update(main_verify_task)

    merge_calls: list[str] = []

    def fake_execute_merge_action(_config, _store, _git, task, _action, **_kwargs):
        merge_calls.append(task.id)
        return SimpleNamespace(rc=0, created_followups=[], reused_followups=[])

    green = SimpleNamespace(
        merges_halted=False,
        state=SimpleNamespace(task=main_verify_task, alert_message=None),
    )
    red = SimpleNamespace(
        merges_halted=True,
        state=SimpleNamespace(
            task=main_verify_task,
            alert_message="main verify RED at `deadbeefcafe` - merges halted; phase `unit` failing",
        ),
    )
    first_row = LineageOwnerRow(
        owner_task=first,
        members=(first,),
        tree=None,
        lineage_status="actionable",
        next_action={"type": "merge", "description": "Merge"},
        next_action_reason="Merge",
        unresolved_tasks=(first,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=first,
    )
    second_row = LineageOwnerRow(
        owner_task=second,
        members=(second,),
        tree=None,
        lineage_status="actionable",
        next_action={"type": "merge", "description": "Merge"},
        next_action_reason="Merge",
        unresolved_tasks=(second,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=second,
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[first_row, second_row]),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "merge", "description": "Merge"}),
        patch("gza.cli.git_ops.check_main_integration_verify", side_effect=[green, red]),
        patch("gza.cli.git_ops._execute_merge_action", side_effect=fake_execute_merge_action),
    ):
        rc = cmd_advance(args)

    output = capsys.readouterr().out
    assert rc == 0
    assert merge_calls == [first.id]
    assert "main verify RED at `deadbeefcafe` - merges halted; phase `unit` failing" in output
    assert f"{second.id}" in output
    assert "1 advanced" in output
    assert "1 skipped" in output


def test_cmd_advance_merge_uses_isolated_checkout_for_candidate_verify_gate(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\nverify_command: ./bin/tests\n")
    store = make_store(tmp_path)
    task, _review = _add_completed_impl_with_approved_review(
        store,
        "feature/advance-isolated-candidate-green",
        when=datetime.now(UTC),
    )

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="actionable",
        next_action={"type": "merge", "description": "Merge"},
        next_action_reason="Merge",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=task,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = True
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1

    isolated_git = MagicMock(spec=Git)
    isolated_git.repo_dir = tmp_path / ".gza" / "main-integration"

    green = SimpleNamespace(merges_halted=False, state=SimpleNamespace(task=task, alert_message=None))

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "merge", "description": "Merge"}),
        patch("gza.cli.git_ops.check_main_integration_verify", side_effect=[green, green]),
        patch("gza.cli.git_ops.ensure_watch_main_checkout", return_value=isolated_git) as ensure_checkout,
        patch(
            "gza.cli.git_ops._execute_merge_action",
            return_value=SimpleNamespace(
                rc=0,
                created_followups=[],
                reused_followups=[],
                created_investigation_task_ids=[],
                reused_investigation_task_ids=[],
                promotion_warnings=(),
            ),
        ) as execute_merge,
    ):
        rc = cmd_advance(_advance_args(tmp_path, task.id))

    output = capsys.readouterr().out
    assert rc == 0
    ensure_checkout.assert_called_once()
    assert execute_merge.call_args.kwargs["merge_git"] is isolated_git
    assert execute_merge.call_args.kwargs["merge_current_branch"] == "main"
    assert "✓ Merged" in output


def test_cmd_advance_merge_uses_isolated_checkout_without_candidate_verify_gate(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\n")
    store = make_store(tmp_path)
    task, _review = _add_completed_impl_with_approved_review(
        store,
        "feature/advance-isolated-without-verify-command",
        when=datetime.now(UTC),
    )

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="actionable",
        next_action={"type": "merge", "description": "Merge"},
        next_action_reason="Merge",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=task,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = True
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1

    isolated_git = MagicMock(spec=Git)
    isolated_git.repo_dir = tmp_path / ".gza" / "main-integration"

    green = SimpleNamespace(merges_halted=False, state=SimpleNamespace(task=task, alert_message=None))

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "merge", "description": "Merge"}),
        patch("gza.cli.git_ops.check_main_integration_verify", side_effect=[green, green]),
        patch("gza.cli.git_ops.ensure_watch_main_checkout", return_value=isolated_git) as ensure_checkout,
        patch(
            "gza.cli.git_ops._execute_merge_action",
            return_value=SimpleNamespace(
                rc=0,
                created_followups=[],
                reused_followups=[],
                created_investigation_task_ids=[],
                reused_investigation_task_ids=[],
                promotion_warnings=(),
            ),
        ) as execute_merge,
        patch(
            "gza.cli.git_ops.check_candidate_integration_verify",
            side_effect=AssertionError("candidate verify should stay disabled without verify_command"),
        ),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task.id))

    output = capsys.readouterr().out
    assert rc == 0
    ensure_checkout.assert_called_once()
    assert execute_merge.call_args.kwargs["merge_git"] is isolated_git
    assert execute_merge.call_args.kwargs["merge_current_branch"] == "main"
    assert "✓ Merged" in output


@pytest.mark.parametrize(
    ("verify_command_yaml", "case_label"),
    [
        ("", "unset"),
        ('verify_command: "   "\n', "whitespace-only"),
    ],
)
def test_cmd_advance_isolated_checkout_unavailable_blocks_no_gate_merge(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    verify_command_yaml: str,
    case_label: str,
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\n" + verify_command_yaml)
    store = make_store(tmp_path)
    task, _review = _add_completed_impl_with_approved_review(
        store,
        f"feature/advance-isolated-unavailable-{case_label}",
        when=datetime.now(UTC),
    )

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="actionable",
        next_action={"type": "merge", "description": "Merge"},
        next_action_reason="Merge",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=task,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = True
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1

    green = SimpleNamespace(merges_halted=False, state=SimpleNamespace(task=task, alert_message=None))

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "merge", "description": "Merge"}),
        patch("gza.cli.git_ops.check_main_integration_verify", return_value=green),
        patch(
            "gza.cli.git_ops.ensure_watch_main_checkout",
            side_effect=[GitError("refresh failed"), GitError("rebuild failed")],
        ) as ensure_checkout,
        patch("gza.cli.git_ops._execute_merge_action") as execute_merge,
    ):
        rc = cmd_advance(_advance_args(tmp_path, task.id))

    output = capsys.readouterr().out
    assert rc == 0
    assert ensure_checkout.call_count == 2
    execute_merge.assert_not_called()
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "unmerged"
    assert "isolated host merge checkout unavailable; local main was left unchanged" in output
    assert "✓ Merged" not in output
    assert "reason=blocked-candidate-verify" in output


def test_cmd_advance_blocked_candidate_verify_surfaces_attention_not_generic_merge_failure(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\nverify_command: ./bin/tests\n")
    store = make_store(tmp_path)
    task, _review = _add_completed_impl_with_approved_review(
        store,
        "feature/advance-isolated-candidate-red",
        when=datetime.now(UTC),
    )

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="actionable",
        next_action={"type": "merge", "description": "Merge"},
        next_action_reason="Merge",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=task,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = True
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1

    isolated_git = MagicMock(spec=Git)
    isolated_git.repo_dir = tmp_path / ".gza" / "main-integration"

    green = SimpleNamespace(merges_halted=False, state=SimpleNamespace(task=task, alert_message=None))
    blocked_result = SimpleNamespace(
        rc=1,
        status="blocked_candidate_verify",
        block_reason="candidate verify red; refusing to promote while phase `unit` is failing",
        candidate_verify=CandidateIntegrationVerifyCheck(
            evidence=CandidateIntegrationVerifyEvidence(
                gate_enabled=True,
                verify_command="./bin/tests",
                verify_timeout_seconds=300,
                verify_timeout_grace_seconds=5.0,
                environment_identity=MainIntegrationVerifyEnvironmentIdentity(
                    runner_class="host",
                    platform_system="Darwin",
                    platform_machine="arm64",
                    python_implementation="CPython",
                    python_version=f"{sys.version_info.major}.{sys.version_info.minor}",
                ),
                tree_fingerprint="fp-advance-candidate-red",
                head_sha="isolated-merge-oid",
                verify_status="failed",
                verify_exit_status="1",
                failure="worker died in host-only unit path",
                failing_phase="unit",
                reviewed_branch="main",
                working_directory=str(tmp_path),
                captured_at=datetime.now(UTC),
            ),
            classification="deterministic_red",
            merges_halted=True,
            remediation=None,
            verify_runs=2,
        ),
        created_followups=[],
        reused_followups=[],
        created_investigation_task_ids=[],
        reused_investigation_task_ids=[],
        promotion_warnings=(),
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "merge", "description": "Merge"}),
        patch("gza.cli.git_ops.check_main_integration_verify", return_value=green),
        patch("gza.cli.git_ops.ensure_watch_main_checkout", return_value=isolated_git),
        patch("gza.cli.git_ops._execute_merge_action", return_value=blocked_result),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task.id))

    output = capsys.readouterr().out
    assert rc == 0
    assert (
        "candidate verify blocked promotion on fp-advance-candidate-red; phase `unit` failed before main changed"
        in output
    )
    assert "✗ Merge failed" not in output
    assert "reason=blocked-candidate-verify" in output


def test_advance_refreshes_red_main_before_preview_and_skips_confirmation_prompt(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task, _review = _add_completed_impl_with_approved_review(
        store,
        "feature/advance-preview-main-red",
        when=datetime.now(UTC),
    )

    args = argparse.Namespace(**{**vars(_advance_args(tmp_path, task.id)), "auto": False})

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = True
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1

    main_verify_task = store.add(
        "System alert: local main integration verify", task_type="internal", skip_learnings=True
    )
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    store.update(main_verify_task)

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="actionable",
        next_action={"type": "merge", "description": "Merge"},
        next_action_reason="Merge",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=task,
    )
    red = SimpleNamespace(
        merges_halted=True,
        state=SimpleNamespace(
            task=main_verify_task,
            alert_message="main verify RED at `cafebabe1234` - merges halted; phase `unit` failing",
        ),
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "merge", "description": "Merge"}),
        patch("gza.cli.git_ops.check_main_integration_verify", return_value=red) as verify_check,
        patch("builtins.input", side_effect=AssertionError("confirmation prompt should not run")),
        patch("gza.cli.git_ops._execute_merge_action") as execute_merge,
    ):
        rc = cmd_advance(args)

    output = capsys.readouterr().out
    assert rc == 0
    assert verify_check.call_args.kwargs["reason"] == "advance-pre-merge"
    assert "No eligible tasks to advance" in output
    assert "main verify RED at `cafebabe1234` - merges halted; phase `unit` failing" in output
    assert "Will advance 1 task(s):" not in output
    assert "Proceed? [Y/n]" not in output
    execute_merge.assert_not_called()


def test_advance_dedupes_persisted_and_live_red_main_attention_in_final_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    merge_task, _merge_review = _add_completed_impl_with_approved_review(
        store,
        "feature/advance-main-red-dedupe-merge",
        when=datetime.now(UTC),
    )
    plan_task = store.add("Approved plan ready to materialize", task_type="plan")
    assert plan_task.id is not None
    plan_task.status = "completed"
    plan_task.completed_at = datetime.now(UTC)
    store.update(plan_task)

    main_verify_task = store.add(
        "System alert: local main integration verify", task_type="internal", skip_learnings=True
    )
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    store.update(main_verify_task)

    persisted_main_row = LineageOwnerRow(
        owner_task=main_verify_task,
        members=(main_verify_task,),
        tree=None,
        lineage_status="needs_attention",
        next_action={
            "type": "needs_discussion",
            "description": "SKIP: main verify RED at `facefeed9999` - merges halted; phase `unit` failing",
            "needs_attention_reason": "main-integration-verify-red",
            "subject_task_id": main_verify_task.id,
        },
        next_action_reason="red main verify",
        unresolved_tasks=(main_verify_task,),
        unresolved_leaf_summary=(),
    )
    merge_row = LineageOwnerRow(
        owner_task=merge_task,
        members=(merge_task,),
        tree=None,
        lineage_status="actionable",
        next_action={"type": "merge", "description": "Merge"},
        next_action_reason="Merge",
        unresolved_tasks=(merge_task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=merge_task,
    )
    plan_row = LineageOwnerRow(
        owner_task=plan_task,
        members=(plan_task,),
        tree=None,
        lineage_status="actionable",
        next_action=None,
        next_action_reason="materialize",
        unresolved_tasks=(plan_task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=plan_task,
    )

    red = SimpleNamespace(
        merges_halted=True,
        state=SimpleNamespace(
            task=main_verify_task,
            alert_message="main verify RED at `facefeed9999` - merges halted; phase `unit` failing",
        ),
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = True
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1

    def _fake_determine(_config, _store, _git, task, _target_branch, **_kwargs):
        if task.id == merge_task.id:
            return {"type": "merge", "description": "Merge"}
        if task.id == plan_task.id:
            return {
                "type": "materialize_plan_slices",
                "description": "Materialize implementation slices from approved plan review",
            }
        raise AssertionError(f"unexpected task: {task.id}")

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[persisted_main_row, merge_row, plan_row]),
        patch("gza.cli.git_ops.check_main_integration_verify", return_value=red),
        patch("gza.cli.git_ops.determine_next_action", side_effect=_fake_determine),
        patch(
            "gza.cli.git_ops.execute_advance_action",
            return_value=AdvanceActionExecutionResult(
                action_type="materialize_plan_slices",
                status="success",
                message="Materialized plan slices",
                success_message="Materialized plan slices",
                work_done=True,
                worker_consuming=False,
            ),
        ),
        patch("gza.cli.git_ops._execute_merge_action") as execute_merge,
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, merge_task.id)), "task_id": None}))

    output = capsys.readouterr().out
    assert rc == 0
    execute_merge.assert_not_called()
    assert "Will advance 1 task(s):" in output
    final_attention = output[output.rfind("Needs attention") :]
    assert final_attention.startswith("Needs attention (1 task):")
    assert final_attention.count("main verify RED at `facefeed9999` - merges halted; phase `unit` failing") == 1
    assert final_attention.count("main-integration-verify-red") == 1


def test_rebase_background_creator_phase_failure_cleans_up_created_task_and_artifacts(tmp_path: Path) -> None:
    """Background rebase must roll back the created child when startup preparation fails."""

    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl_task = store.add("Implement feature", task_type="implement")
    impl_task.status = "completed"
    impl_task.branch = "test-project/20260129-implement-feature"
    impl_task.completed_at = datetime.now(UTC)
    store.update(impl_task)

    git = SimpleNamespace(
        current_branch=MagicMock(return_value="main"),
        default_branch=MagicMock(return_value="main"),
        branch_exists=MagicMock(return_value=True),
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops._require_default_branch", return_value=True),
        patch("gza.cli._common.prepare_task_startup_phase", side_effect=RuntimeError("creator boom")),
        patch(
            "gza.cli.git_ops._spawn_background_worker",
            side_effect=AssertionError("background worker should not spawn"),
        ),
    ):
        result = invoke_gza("rebase", str(impl_task.id), "--background", "--project", str(tmp_path))

    assert result.returncode == 1
    assert "creator boom" in result.stderr
    assert store.get_based_on_children(impl_task.id) == []

    logs_dir = tmp_path / ".gza" / "logs"
    if logs_dir.exists():
        assert list(logs_dir.iterdir()) == []

    workers_dir = tmp_path / ".gza" / "workers"
    if workers_dir.exists():
        assert list(workers_dir.iterdir()) == []


def test_rebase_background_reuses_prepared_child_without_second_startup_pass(tmp_path: Path) -> None:
    """Background rebase should hand the already-prepared child to the generic spawner."""

    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl_task = store.add("Implement feature", task_type="implement")
    assert impl_task.id is not None
    impl_task.status = "completed"
    impl_task.branch = "test-project/20260129-implement-feature"
    impl_task.completed_at = datetime.now(UTC)
    store.update(impl_task)

    git = SimpleNamespace(
        current_branch=MagicMock(return_value="main"),
        default_branch=MagicMock(return_value="main"),
        branch_exists=MagicMock(return_value=True),
    )
    captured_spawn: dict[str, object] = {}

    def prepare_once(_config, task, **_kwargs):
        if prepare_once.called:
            raise AssertionError("startup preparation ran twice")
        prepare_once.called = True
        return task

    prepare_once.called = False  # type: ignore[attr-defined]

    def fake_spawn(_args, _config, **kwargs):
        captured_spawn.update(kwargs)
        return 0

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops._require_default_branch", return_value=True),
        patch("gza.cli.git_ops._prepare_task_for_immediate_execution", side_effect=prepare_once) as prepare_task,
        patch("gza.cli.git_ops._spawn_background_worker", side_effect=fake_spawn),
    ):
        result = invoke_gza("rebase", str(impl_task.id), "--background", "--project", str(tmp_path))

    assert result.returncode == 0
    assert prepare_task.call_count == 1
    assert captured_spawn["task_id"] is not None
    prepared_task = captured_spawn["prepared_task"]
    assert prepared_task is not None
    assert getattr(prepared_task, "id", None) == captured_spawn["task_id"]


def test_rebase_defaults_to_queue_without_running_or_spawning(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl_task = store.add("Implement feature", task_type="implement")
    assert impl_task.id is not None
    impl_task.status = "completed"
    impl_task.branch = "test-project/20260129-implement-feature"
    impl_task.completed_at = datetime.now(UTC)
    store.update(impl_task)

    git = SimpleNamespace(
        current_branch=MagicMock(return_value="main"),
        default_branch=MagicMock(return_value="main"),
        branch_exists=MagicMock(return_value=True),
    )
    args = argparse.Namespace(
        project_dir=tmp_path,
        task_id=impl_task.id,
        onto=None,
        remote=False,
        force=False,
        resolve=False,
        run=False,
        queue=False,
        background=False,
        no_docker=True,
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops._require_default_branch", return_value=True),
        patch("gza.cli.git_ops._run_task_backed_rebase", side_effect=AssertionError("should stay queued")),
        patch(
            "gza.cli.git_ops._prepare_task_for_immediate_execution", side_effect=AssertionError("should stay queued")
        ),
        patch("gza.cli.git_ops._spawn_background_worker", side_effect=AssertionError("should stay queued")),
    ):
        rc = cmd_rebase(args)

    assert rc == 0
    output = capsys.readouterr().out
    assert "Created rebase task" in output
    created = store.get_based_on_children(impl_task.id)
    assert len(created) == 1
    assert created[0].task_type == "rebase"
    assert created[0].status == "pending"
    assert created[0].branch == impl_task.branch


def test_rebase_run_uses_foreground_task_backed_path(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl_task = store.add("Implement feature", task_type="implement")
    assert impl_task.id is not None
    impl_task.status = "completed"
    impl_task.branch = "test-project/20260129-implement-feature"
    impl_task.completed_at = datetime.now(UTC)
    store.update(impl_task)

    git = SimpleNamespace(
        current_branch=MagicMock(return_value="main"),
        default_branch=MagicMock(return_value="main"),
        branch_exists=MagicMock(return_value=True),
    )
    args = argparse.Namespace(
        project_dir=tmp_path,
        task_id=impl_task.id,
        onto=None,
        remote=False,
        force=False,
        resolve=False,
        run=True,
        queue=False,
        background=False,
        no_docker=True,
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops._require_default_branch", return_value=True),
        patch("gza.cli.git_ops._run_task_backed_rebase", return_value=0) as run_rebase,
    ):
        rc = cmd_rebase(args)

    assert rc == 0
    run_rebase.assert_called_once()


def test_rebase_background_duplicate_active_rebase_returns_phase1_error_and_releases_capacity(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "max_concurrent: 1\n")
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    impl_task = store.add("Implement feature", task_type="implement")
    assert impl_task.id is not None
    impl_task.status = "completed"
    impl_task.branch = "test-project/20260129-implement-feature"
    impl_task.completed_at = datetime.now(UTC)
    store.update(impl_task)

    active_rebase = store.add(
        "Rebase",
        task_type="rebase",
        based_on=impl_task.id,
        same_branch=True,
        branch=impl_task.branch,
    )
    assert active_rebase.id is not None

    git = SimpleNamespace(
        current_branch=MagicMock(return_value="main"),
        default_branch=MagicMock(return_value="main"),
        branch_exists=MagicMock(return_value=True),
    )
    args = argparse.Namespace(
        project_dir=tmp_path,
        task_id=impl_task.id,
        onto=None,
        remote=False,
        force=False,
        resolve=False,
        run=False,
        queue=False,
        background=True,
        no_docker=True,
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops._require_default_branch", return_value=True),
        patch(
            "gza.cli.git_ops._create_rebase_task",
            side_effect=DuplicateActiveChildError(active_rebase),
        ),
        patch("gza.cli.git_ops._prepare_task_for_immediate_execution", side_effect=AssertionError("unused")),
        patch("gza.cli.git_ops._spawn_background_worker", side_effect=AssertionError("unused")),
    ):
        rc = cmd_rebase(args)

    assert rc == 1
    captured = capsys.readouterr()
    combined_output = captured.out + captured.err
    assert (
        f"Error: rebase already pending/in progress for branch {impl_task.branch}: {active_rebase.id}"
        in combined_output
    )
    permit = launch_permit(config, store)
    permit.release()


def test_rebase_foreground_duplicate_active_rebase_returns_phase1_error(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl_task = store.add("Implement feature", task_type="implement")
    assert impl_task.id is not None
    impl_task.status = "completed"
    impl_task.branch = "test-project/20260129-implement-feature"
    impl_task.completed_at = datetime.now(UTC)
    store.update(impl_task)

    active_rebase = store.add(
        "Rebase",
        task_type="rebase",
        based_on=impl_task.id,
        same_branch=True,
        branch=impl_task.branch,
    )
    assert active_rebase.id is not None

    git = SimpleNamespace(
        current_branch=MagicMock(return_value="main"),
        default_branch=MagicMock(return_value="main"),
        branch_exists=MagicMock(return_value=True),
    )
    args = argparse.Namespace(
        project_dir=tmp_path,
        task_id=impl_task.id,
        onto=None,
        remote=False,
        force=False,
        resolve=False,
        run=True,
        queue=False,
        background=False,
        no_docker=True,
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops._require_default_branch", return_value=True),
        patch(
            "gza.cli.git_ops._create_rebase_task",
            side_effect=DuplicateActiveChildError(active_rebase),
        ),
        patch("gza.cli.git_ops._run_task_backed_rebase", side_effect=AssertionError("unused")),
    ):
        rc = cmd_rebase(args)

    assert rc == 1
    captured = capsys.readouterr()
    combined_output = captured.out + captured.err
    assert (
        f"Error: rebase already pending/in progress for branch {impl_task.branch}: {active_rebase.id}"
        in combined_output
    )


def test_reconcile_squash_merge_skips_when_no_remote_tracking_ref() -> None:
    git = MagicMock(spec=Git)

    result = _reconcile_squash_merged_branch_with_origin(
        git,
        branch="feature/demo",
        squash_oid="squash-oid",
        pre_squash_local_oid="local-oid",
        pre_squash_remote_oid=None,
    )

    assert result.status == "skipped_no_remote_tracking_ref"
    git.update_ref.assert_not_called()
    git.push_ref_force_with_lease.assert_not_called()


def test_reconcile_squash_merge_updates_local_and_remote_tracking_refs_before_and_after_push() -> None:
    git = MagicMock(spec=Git)

    result = _reconcile_squash_merged_branch_with_origin(
        git,
        branch="feature/demo",
        squash_oid="squash-oid",
        pre_squash_local_oid="local-oid",
        pre_squash_remote_oid="remote-oid",
    )

    assert result.status == "updated"
    assert git.update_ref.call_args_list == [
        call("refs/heads/feature/demo", "squash-oid", "local-oid"),
        call("refs/remotes/origin/feature/demo", "squash-oid"),
    ]
    git.push_ref_force_with_lease.assert_called_once_with(
        "refs/heads/feature/demo",
        "feature/demo",
        remote="origin",
        expected_remote_oid="remote-oid",
    )


def test_reconcile_squash_merge_local_ref_update_failure_prevents_push() -> None:
    git = MagicMock(spec=Git)
    git.update_ref.side_effect = GitError("branch is checked out elsewhere")

    result = _reconcile_squash_merged_branch_with_origin(
        git,
        branch="feature/demo",
        squash_oid="squash-oid",
        pre_squash_local_oid="local-oid",
        pre_squash_remote_oid="remote-oid",
    )

    assert result.status == "failed_local_ref_update"
    assert result.manual_source_ref is None
    git.push_ref_force_with_lease.assert_not_called()


def test_reconcile_squash_merge_lease_rejection_is_reported_without_updating_tracking_ref() -> None:
    git = MagicMock(spec=Git)
    git.push_ref_force_with_lease.side_effect = GitError("git push failed:\n! [rejected] (stale info)")

    result = _reconcile_squash_merged_branch_with_origin(
        git,
        branch="feature/demo",
        squash_oid="squash-oid",
        pre_squash_local_oid=None,
        pre_squash_remote_oid="remote-oid",
    )

    assert result.status == "failed_push_rejected"
    assert git.update_ref.call_args_list == []


def test_reconcile_squash_merge_remote_tracking_ref_update_failure_reports_post_push_status() -> None:
    git = MagicMock(spec=Git)
    git.update_ref.side_effect = [
        None,
        GitError("cannot lock ref 'refs/remotes/origin/feature/demo'"),
    ]

    result = _reconcile_squash_merged_branch_with_origin(
        git,
        branch="feature/demo",
        squash_oid="squash-oid",
        pre_squash_local_oid="local-oid",
        pre_squash_remote_oid="remote-oid",
    )

    assert result.status == "failed_remote_tracking_ref_update"
    git.push_ref_force_with_lease.assert_called_once_with(
        "refs/heads/feature/demo",
        "feature/demo",
        remote="origin",
        expected_remote_oid="remote-oid",
    )
    assert git.update_ref.call_args_list == [
        call("refs/heads/feature/demo", "squash-oid", "local-oid"),
        call("refs/remotes/origin/feature/demo", "squash-oid"),
    ]


def test_classify_squash_reconcile_push_failure_keeps_policy_rejections_distinct() -> None:
    exc = GitError(
        "git push failed:\n! [remote rejected] feature/demo -> feature/demo (protected branch hook declined)"
    )

    assert _classify_squash_reconcile_push_failure(exc) == "failed_push_unavailable"


def test_print_squash_reconcile_result_does_not_emit_lease_guidance_for_policy_rejection(
    capsys: pytest.CaptureFixture[str],
) -> None:
    _print_squash_reconcile_result(
        SquashBranchReconcileResult(
            status="failed_push_unavailable",
            branch="feature/demo",
            reason="git push failed: protected branch hook declined",
            manual_source_ref="HEAD",
            expected_remote_oid="remote-oid",
        )
    )

    output = capsys.readouterr().out
    assert "changed since it was last observed" not in output
    assert "protected branch hook declined" in output
    assert "git push --force-with-lease=refs/heads/feature/demo:remote-oid" in output


def test_print_squash_reconcile_result_suppresses_only_success_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    _print_squash_reconcile_result(
        SquashBranchReconcileResult(
            status="updated",
            branch="feature/demo",
            remote="origin",
        ),
        suppress_success=True,
    )

    assert capsys.readouterr().out == ""


def test_print_squash_reconcile_result_emits_lease_guidance_for_stale_info_rejection(
    capsys: pytest.CaptureFixture[str],
) -> None:
    _print_squash_reconcile_result(
        SquashBranchReconcileResult(
            status="failed_push_rejected",
            branch="feature/demo",
            reason="git push failed: stale info",
            manual_source_ref="HEAD",
            expected_remote_oid="remote-oid",
        )
    )

    output = capsys.readouterr().out
    assert "changed since it was last observed" in output
    assert "git push --force-with-lease=refs/heads/feature/demo:remote-oid" in output


def test_print_squash_reconcile_result_failed_local_ref_update_fails_closed(
    capsys: pytest.CaptureFixture[str],
) -> None:
    _print_squash_reconcile_result(
        SquashBranchReconcileResult(
            status="failed_local_ref_update",
            branch="feature/demo",
            reason="branch is checked out elsewhere",
            expected_remote_oid="remote-oid",
        )
    )

    output = capsys.readouterr().out
    assert "branch is checked out elsewhere" in output
    assert "Reconcile the local branch 'feature/demo' first" in output
    assert "known to point at the squash merge commit" in output
    assert "origin feature/demo:refs/heads/feature/demo" not in output
    assert "Manual repair:" not in output


def test_tracking_ref_refresh_command_forces_non_fast_forward_tracking_update() -> None:
    assert (
        _tracking_ref_refresh_command(
            remote="origin",
            branch="feature/demo",
        )
        == "git fetch origin +refs/heads/feature/demo:refs/remotes/origin/feature/demo"
    )


def test_print_squash_reconcile_result_failed_remote_tracking_ref_update_reports_refresh_action(
    capsys: pytest.CaptureFixture[str],
) -> None:
    _print_squash_reconcile_result(
        SquashBranchReconcileResult(
            status="failed_remote_tracking_ref_update",
            branch="feature/demo",
            reason="cannot lock ref 'refs/remotes/origin/feature/demo'",
        )
    )

    output = capsys.readouterr().out
    assert "remote push succeeded" in output
    assert "could not be reconciled" not in output
    assert "could not be updated" in output
    assert "git push --force-with-lease" not in output
    assert "git fetch origin +refs/heads/feature/demo:refs/remotes/origin/feature/demo" in output
