"""Tests for shared advance action execution."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from gza.advance_engine import (
    NOOP_IMPROVE_KIND_VERIFY_ONLY,
    REVIEW_CLEARANCE_ARTIFACT_KIND,
    VERIFY_ONLY_NOOP_RECOVERY_ATTENTION_ARTIFACT_KIND,
    VERIFY_ONLY_NOOP_RECOVERY_ATTENTION_STATUS,
    VERIFY_ONLY_NOOP_REVIEW_CLEARANCE_KIND,
)
from gza.branch_publication import BranchPublicationState, persist_branch_publication_state
from gza.cli._common import _create_retry_task, _materialize_plan_review_slices, resolve_improve_action
from gza.cli.advance_executor import (
    _WORKER_ACTIONS,
    ITERATE_ROUTABLE_ACTIONS,
    AdvanceActionExecutionContext,
    AdvanceActionExecutionResult,
    BranchDivergenceReconcileResult,
    build_improve_needs_attention_result,
    execute_advance_action,
    resolve_execution_needs_attention,
)
from gza.concurrency import launch_permit
from gza.config import Config
from gza.db import Task as DbTask
from gza.flaky_investigations import (
    FlakyInvestigationEvidence,
    build_flaky_reproduction_plan,
    normalize_flaky_investigation_dedup_key,
)
from gza.log_paths import ops_log_path_for
from gza.off_topic_verify import FailingNode, PytestPassFailCounts, PytestXdistMetadata
from gza.pickup import count_worker_consuming_actions, is_worker_consuming_advance_action
from gza.plan_review_materialization import (
    PLAN_REVIEW_MATERIALIZATION_ARTIFACT_KIND,
    build_plan_review_slice_task_specs,
    plan_review_manifest_digest,
)
from gza.plan_review_verdict import validate_plan_review_manifest
from gza.recovery_engine import FailedRecoveryDecision, decide_failed_task_recovery
from gza.review_tasks import OffTopicVerifyPersistenceError
from gza.review_verdict import ReviewFinding
from gza.runner import CROSS_PROJECT_TAG, _make_review_verify_result

from .conftest import make_store, setup_config


def _mark_completed(task: DbTask, *, branch: str | None = None) -> None:
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    if branch is not None:
        task.branch = branch


@pytest.mark.parametrize(
    ("failure_reason", "session_id", "expected_mode", "expected_status"),
    [
        (None, None, "new", "dry_run"),
        ("MAX_STEPS", "sess-1", "resume", "dry_run"),
        ("TEST_FAILURE", None, "manual_review", "skip"),
    ],
)
def test_improve_dry_run_modes_do_not_mutate_db(
    tmp_path: Path,
    failure_reason: str | None,
    session_id: str | None,
    expected_mode: str,
    expected_status: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/improve-dry-run")
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    store.update(review)

    if failure_reason is not None:
        failed = store.add(
            "Improve attempt",
            task_type="improve",
            depends_on=review.id,
            based_on=impl.id,
            same_branch=True,
        )
        assert failed.id is not None
        failed.status = "failed"
        failed.failure_reason = failure_reason
        failed.session_id = session_id
        failed.completed_at = datetime.now(UTC)
        store.update(failed)

    before_count = len(store.get_all())
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=True,
        max_resume_attempts=3,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("create_review should not run in dry-run"),
        create_resume_task=lambda _task: pytest.fail("create_resume should not run in dry-run"),
        create_rebase_task=lambda _task: pytest.fail("create_rebase should not run in dry-run"),
        create_implement_task=lambda _task: pytest.fail("create_implement should not run in dry-run"),
        spawn_worker=lambda _task, _kind: pytest.fail("spawn_worker should not run in dry-run"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("spawn_resume should not run in dry-run"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("spawn_iterate should not run in dry-run"),
    )

    result = execute_advance_action(
        task=impl,
        action={"type": "improve", "review_task": review, "description": "Create improve"},
        context=context,
    )

    assert result.status == expected_status
    assert result.improve_mode == expected_mode
    if expected_status == "dry_run":
        assert result.worker_consuming is True
        assert result.work_done is True
    else:
        assert result.attention_type == "manual_review_required"
    assert len(store.get_all()) == before_count


def test_improve_manual_review_returns_skip_without_mutation(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/improve-cap")
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    store.update(review)

    first = store.add(
        "Improve 0",
        task_type="improve",
        depends_on=review.id,
        based_on=impl.id,
        same_branch=True,
    )
    assert first.id is not None
    first.status = "failed"
    first.failure_reason = "MAX_STEPS"
    first.session_id = "sess-0"
    first.completed_at = datetime.now(UTC)
    store.update(first)

    second = store.add(
        first.prompt,
        task_type="improve",
        depends_on=review.id,
        based_on=first.id,
        same_branch=True,
    )
    assert second.id is not None
    second.status = "failed"
    second.failure_reason = "INFRASTRUCTURE_ERROR"
    second.session_id = first.session_id
    second.completed_at = datetime.now(UTC)
    store.update(second)

    before_count = len(store.get_all())
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
    )

    result = execute_advance_action(
        task=impl,
        action={"type": "improve", "review_task": review},
        context=context,
    )
    improve_mode, failed_improve, improve_decision = resolve_improve_action(
        store,
        impl.id,
        review.id,
        max_resume_attempts=1,
    )
    expected = build_improve_needs_attention_result(
        store=store,
        impl_task=impl,
        review_task=review,
        improve_mode=improve_mode,
        failed_improve=failed_improve,
        improve_decision=improve_decision,
        max_resume_attempts=1,
    )

    assert expected is not None
    assert result == expected
    assert len(store.get_all()) == before_count


def test_create_review_adjudication_spawns_internal_worker(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/review-adjudication")
    store.update(impl)

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    store.update(review)

    spawned: list[tuple[str, str]] = []
    captured_dispute_metadata: dict[str, Any] | None = None

    def _create_review_adjudication(
        impl_task: DbTask,
        review_task: DbTask,
        finding: ReviewFinding,
        dispute_metadata: dict[str, Any],
    ) -> DbTask:
        nonlocal captured_dispute_metadata
        captured_dispute_metadata = dict(dispute_metadata)
        return store.add(
            f"Adjudicate {finding.id}",
            task_type="internal",
            based_on=review_task.id,
            depends_on=impl_task.id,
            same_branch=True,
        )

    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=3,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("create_review should not run"),
        create_resume_task=lambda _task: pytest.fail("create_resume should not run"),
        create_rebase_task=lambda _task: pytest.fail("create_rebase should not run"),
        create_implement_task=lambda _task: pytest.fail("create_implement should not run"),
        create_review_adjudication_task=_create_review_adjudication,
        spawn_worker=lambda task, kind: spawned.append((task.id or "", kind)) or 0,
        spawn_resume_worker=lambda _task, _kind: pytest.fail("spawn_resume should not run"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("spawn_iterate should not run"),
    )

    result = execute_advance_action(
        task=impl,
        action={
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
                dispute_artifact=SimpleNamespace(
                    id=47,
                    metadata={"reason": "already_satisfied"},
                ),
            ),
        },
        context=context,
    )

    assert result.status == "success"
    assert result.created_task is not None
    assert result.created_task.task_type == "internal"
    assert captured_dispute_metadata is not None
    assert captured_dispute_metadata["disputed_artifact_id"] == 47
    assert spawned == [(result.created_task.id or "", "review_adjudication")]


def _off_topic_clearance_candidate(review: DbTask, impl: DbTask, *, working_directory: str = "/workspace"):
    from gza.advance_engine import OffTopicVerifyClearanceCandidate

    assert review.id is not None
    assert impl.id is not None
    node = FailingNode(
        nodeid="tests/cli/test_watch.py::test_worker_registry_race",
        path="tests/cli/test_watch.py",
        outcome="FAILED",
        assertion_signature="assert running == completed",
        failure_path="tests/cli/test_watch.py",
        failure_line=42,
        traceback_paths=("tests/cli/test_watch.py",),
        trustworthy_attribution=True,
    )
    evidence = FlakyInvestigationEvidence(
        node=node,
        dedup_key=normalize_flaky_investigation_dedup_key(node.nodeid, node.assertion_signature),
        review_task_id=review.id,
        impl_task_id=impl.id,
        merge_unit_id=None,
        reviewed_head_sha="deadbeef",
        tree_fingerprint="f" * 64,
        observed_branch="feature/off-topic",
        target_branch="main",
        verify_command="./bin/tests",
        targeted_command=None,
        working_directory=working_directory,
        branch_pass_fail_counts=PytestPassFailCounts(failed=1, passed=412),
        xdist=PytestXdistMetadata(enabled=True, worker_count=8, worker_count_raw="8"),
        branch_verify_status="failed",
        branch_verify_exit_status="1",
    )
    return OffTopicVerifyClearanceCandidate(
        review_task=review,
        reviewed_head_sha="deadbeef",
        tree_fingerprint="f" * 64,
        evidences=(evidence,),
    )


class _VerifyOnlyNoopGit:
    def __init__(self, branch: str, head_sha: str):
        self.branch = branch
        self.head_sha = head_sha
        self.worktree_add_calls: list[tuple[Path, str, bool]] = []
        self.worktree_remove_calls: list[tuple[Path, bool]] = []

    def rev_parse_if_exists(self, ref: str) -> str | None:
        if ref in {self.branch, "HEAD"}:
            return self.head_sha
        return None

    def worktree_add_existing(self, path: Path, ref: str, *, detach: bool = False) -> Path:
        self.worktree_add_calls.append((path, ref, detach))
        path.mkdir(parents=True, exist_ok=True)
        return path

    def worktree_remove(self, path: Path, force: bool = False):
        self.worktree_remove_calls.append((path, force))
        return SimpleNamespace(returncode=0)


def test_clear_off_topic_verify_blocker_creates_investigation_and_clears_review(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/off-topic-clearance")
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    store.update(review)

    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="advance",
        dry_run=False,
        max_resume_attempts=3,
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
        config=Config.load(tmp_path),
    )

    action = {
        "type": "clear_off_topic_verify_blocker",
        "review_task": review,
        "off_topic_verify_clearance_candidate": _off_topic_clearance_candidate(
            review,
            impl,
            working_directory=str(tmp_path),
        ),
    }

    result = execute_advance_action(
        task=impl,
        action=action,
        context=context,
    )

    refreshed = store.get(impl.id)
    assert result.status == "success"
    assert refreshed is not None
    assert refreshed.review_cleared_at is not None
    assert len(result.created_investigations) == 1
    assert result.reused_investigations == ()
    assert result.created_investigations[0].id in result.success_message
    created_id = result.created_investigations[0].id
    assert created_id is not None

    plan = build_flaky_reproduction_plan(
        store,
        project_dir=tmp_path,
        task_id=created_id,
        enable_xdist=False,
        enable_randomization=False,
    )
    assert "uv run pytest tests/cli/test_watch.py::test_worker_registry_race --maxfail=0" in plan.command

    repeat = execute_advance_action(
        task=impl,
        action=action,
        context=context,
    )
    assert repeat.status == "success"
    assert repeat.created_investigations == ()
    assert len(repeat.reused_investigations) == 1


def test_clear_off_topic_verify_blocker_fails_closed_when_investigation_persistence_fails(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/off-topic-fail-closed")
    store.update(impl)

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    store.update(review)

    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="advance",
        dry_run=False,
        max_resume_attempts=3,
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
        config=Config.load(tmp_path),
    )

    with patch(
        "gza.cli.advance_executor.create_or_reuse_flaky_investigations",
        side_effect=RuntimeError("artifact write failed"),
    ):
        result = execute_advance_action(
            task=impl,
            action={
                "type": "clear_off_topic_verify_blocker",
                "review_task": review,
                "off_topic_verify_clearance_candidate": _off_topic_clearance_candidate(
                    review,
                    impl,
                    working_directory=str(tmp_path),
                ),
            },
            context=context,
        )

    refreshed = store.get(impl.id)
    assert result.status == "error"
    assert "artifact write failed" in result.message
    assert refreshed is not None
    assert refreshed.review_cleared_at is None


def test_clear_off_topic_verify_blocker_fails_closed_when_evidence_cannot_build_targeted_command(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/off-topic-untargetable")
    store.update(impl)

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    store.update(review)

    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="advance",
        dry_run=False,
        max_resume_attempts=3,
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
        config=Config.load(tmp_path),
    )

    candidate = _off_topic_clearance_candidate(review, impl, working_directory=str(tmp_path))
    [evidence] = candidate.evidences
    untargetable_evidence = FlakyInvestigationEvidence(
        node=evidence.node,
        dedup_key=evidence.dedup_key,
        review_task_id=evidence.review_task_id,
        impl_task_id=evidence.impl_task_id,
        merge_unit_id=evidence.merge_unit_id,
        reviewed_head_sha=evidence.reviewed_head_sha,
        tree_fingerprint=evidence.tree_fingerprint,
        observed_branch=evidence.observed_branch,
        target_branch=evidence.target_branch,
        verify_command="make test",
        targeted_command=None,
        working_directory=evidence.working_directory,
        branch_pass_fail_counts=evidence.branch_pass_fail_counts,
        xdist=evidence.xdist,
        branch_verify_status=evidence.branch_verify_status,
        branch_verify_exit_status=evidence.branch_verify_exit_status,
    )

    result = execute_advance_action(
        task=impl,
        action={
            "type": "clear_off_topic_verify_blocker",
            "review_task": review,
            "off_topic_verify_clearance_candidate": candidate.__class__(
                review_task=candidate.review_task,
                reviewed_head_sha=candidate.reviewed_head_sha,
                tree_fingerprint=candidate.tree_fingerprint,
                evidences=(untargetable_evidence,),
            ),
        },
        context=context,
    )

    refreshed = store.get(impl.id)
    assert result.status == "error"
    assert "cannot produce a bounded targeted pytest command" in result.message
    assert refreshed is not None
    assert refreshed.review_cleared_at is None


def test_recover_verify_only_noop_review_persists_clearance_without_creating_review(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "uv run pytest tests/unit -q"

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/verify-only-noop-recovery")
    store.update(impl)

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    review.output_content = (
        "## Summary\n\n- Implementation is aligned; verify failed.\n\n"
        "## Blockers\n\n"
        "### B1 verify_command failure\n"
        "Evidence: verify_command failed.\n"
        "Impact: autonomous verify fails.\n"
        "Required fix: rerun verify.\n"
        "Required tests: rerun verify.\n\n"
        "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    )
    review.review_verify_status = "failed"
    review.review_verify_branch = impl.branch
    review.review_verify_head_sha = "same-head"
    store.update(review)

    improve = store.add(
        "Improve attempt",
        task_type="improve",
        depends_on=review.id,
        based_on=impl.id,
        same_branch=True,
    )
    assert improve.id is not None
    improve.status = "completed"
    improve.completed_at = datetime.now(UTC)
    improve.branch = impl.branch
    improve.changed_diff = False
    store.update(improve)

    git = _VerifyOnlyNoopGit(impl.branch or "", "same-head")
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="advance",
        dry_run=False,
        max_resume_attempts=3,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("prepare_create_review should not run"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=git,
    )

    with (
        patch("gza.cli.advance_executor.Git", side_effect=lambda path: SimpleNamespace(repo_dir=Path(path), default_branch=lambda: "main", rev_parse_if_exists=lambda ref: "same-head")),
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
        result = execute_advance_action(
            task=impl,
            action={
                "type": "recover_verify_only_noop_review",
                "review_task": review,
                "latest_noop_improve_task": improve,
                "current_branch_head_sha": "same-head",
            },
            context=context,
        )

    refreshed_impl = store.get(impl.id)
    refreshed_improve = store.get(improve.id)
    artifacts = store.list_artifacts(impl.id, kind=REVIEW_CLEARANCE_ARTIFACT_KIND)

    assert result.status == "success"
    assert result.success_message.startswith("Fresh verify passed")
    assert refreshed_impl is not None
    assert refreshed_impl.review_cleared_at is not None
    assert refreshed_improve is not None
    assert refreshed_improve.review_verify_status == "passed"
    assert artifacts
    assert artifacts[0].metadata is not None
    assert artifacts[0].metadata["clearance_kind"] == VERIFY_ONLY_NOOP_REVIEW_CLEARANCE_KIND
    assert artifacts[0].metadata["review_task_id"] == review.id


def test_recover_verify_only_noop_review_failed_verify_returns_attention(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "uv run pytest tests/unit -q"

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/verify-only-noop-red")
    store.update(impl)

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    review.review_verify_status = "failed"
    review.review_verify_branch = impl.branch
    review.review_verify_head_sha = "same-head"
    store.update(review)

    improve = store.add("Improve attempt", task_type="improve", depends_on=review.id, based_on=impl.id, same_branch=True)
    assert improve.id is not None
    improve.status = "completed"
    improve.completed_at = datetime.now(UTC)
    improve.branch = impl.branch
    improve.changed_diff = False
    store.update(improve)

    git = _VerifyOnlyNoopGit(impl.branch or "", "same-head")
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="advance",
        dry_run=False,
        max_resume_attempts=3,
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
        config=config,
        git=git,
    )

    with (
        patch("gza.cli.advance_executor.Git", side_effect=lambda path: SimpleNamespace(repo_dir=Path(path), default_branch=lambda: "main", rev_parse_if_exists=lambda ref: "same-head")),
        patch("gza.cli.advance_executor._resolve_review_verify_base_sha", return_value="base-sha"),
        patch(
            "gza.cli.advance_executor._run_review_verify_command",
            return_value=_make_review_verify_result(
                "uv run pytest tests/unit -q",
                status="failed",
                exit_status="1",
                captured_at=datetime(2026, 6, 27, 12, 0, tzinfo=UTC),
                reviewed_branch=impl.branch,
                reviewed_head_sha="same-head",
                reviewed_base_sha="base-sha",
                working_directory=str(tmp_path),
                failure="tests failed",
            ),
        ),
    ):
        result = execute_advance_action(
            task=impl,
            action={
                "type": "recover_verify_only_noop_review",
                "review_task": review,
                "latest_noop_improve_task": improve,
                "current_branch_head_sha": "same-head",
            },
            context=context,
        )

    assert result.status == "skip"
    assert result.attention_reason == "improve-no-op"
    assert result.noop_improve_kind == NOOP_IMPROVE_KIND_VERIFY_ONLY
    assert store.get(impl.id).review_cleared_at is None
    parked = store.list_artifacts(improve.id, kind=VERIFY_ONLY_NOOP_RECOVERY_ATTENTION_ARTIFACT_KIND)
    assert len(parked) == 1
    assert parked[0].status == VERIFY_ONLY_NOOP_RECOVERY_ATTENTION_STATUS
    assert parked[0].metadata["review_task_id"] == review.id


def test_recover_verify_only_noop_review_head_mismatch_fails_closed(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "uv run pytest tests/unit -q"

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/verify-only-noop-head-mismatch")
    store.update(impl)

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    review.review_verify_status = "failed"
    review.review_verify_branch = impl.branch
    review.review_verify_head_sha = "same-head"
    store.update(review)

    improve = store.add("Improve attempt", task_type="improve", depends_on=review.id, based_on=impl.id, same_branch=True)
    assert improve.id is not None
    improve.status = "completed"
    improve.completed_at = datetime.now(UTC)
    improve.branch = impl.branch
    improve.changed_diff = False
    store.update(improve)

    git = _VerifyOnlyNoopGit(impl.branch or "", "new-head")
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="advance",
        dry_run=False,
        max_resume_attempts=3,
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
        config=config,
        git=git,
    )

    result = execute_advance_action(
        task=impl,
        action={
            "type": "recover_verify_only_noop_review",
            "review_task": review,
            "latest_noop_improve_task": improve,
            "current_branch_head_sha": "same-head",
        },
        context=context,
    )

    assert result.status == "skip"
    assert result.attention_reason == "improve-no-op"
    assert store.get(impl.id).review_cleared_at is None
    parked = store.list_artifacts(improve.id, kind=VERIFY_ONLY_NOOP_RECOVERY_ATTENTION_ARTIFACT_KIND)
    assert len(parked) == 1
    assert parked[0].metadata["outcome_kind"] == "head_drift_before_verify"


def test_recover_verify_only_noop_review_setup_failure_returns_attention(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "uv run pytest tests/unit -q"

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/verify-only-noop-setup-failure")
    store.update(impl)

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    review.review_verify_status = "failed"
    review.review_verify_branch = impl.branch
    review.review_verify_head_sha = "same-head"
    store.update(review)

    improve = store.add("Improve attempt", task_type="improve", depends_on=review.id, based_on=impl.id, same_branch=True)
    assert improve.id is not None
    improve.status = "completed"
    improve.completed_at = datetime.now(UTC)
    improve.branch = impl.branch
    improve.changed_diff = False
    store.update(improve)

    git = _VerifyOnlyNoopGit(impl.branch or "", "same-head")
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="advance",
        dry_run=False,
        max_resume_attempts=3,
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
        config=config,
        git=git,
    )

    with patch.object(git, "worktree_add_existing", side_effect=RuntimeError("boom during add")):
        result = execute_advance_action(
            task=impl,
            action={
                "type": "recover_verify_only_noop_review",
                "review_task": review,
                "latest_noop_improve_task": improve,
                "current_branch_head_sha": "same-head",
            },
            context=context,
        )

    assert result.status == "skip"
    assert result.attention_reason == "improve-no-op"
    assert result.noop_improve_kind == NOOP_IMPROVE_KIND_VERIFY_ONLY
    assert "Setup failure: boom during add" in result.message
    assert store.get(impl.id).review_cleared_at is None
    parked = store.list_artifacts(improve.id, kind=VERIFY_ONLY_NOOP_RECOVERY_ATTENTION_ARTIFACT_KIND)
    assert len(parked) == 1
    assert parked[0].metadata["outcome_kind"] == "setup_failure"


def test_recover_verify_only_noop_review_cleanup_failure_returns_attention(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "uv run pytest tests/unit -q"

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/verify-only-noop-cleanup-failure")
    store.update(impl)

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    review.review_verify_status = "failed"
    review.review_verify_branch = impl.branch
    review.review_verify_head_sha = "same-head"
    store.update(review)

    improve = store.add("Improve attempt", task_type="improve", depends_on=review.id, based_on=impl.id, same_branch=True)
    assert improve.id is not None
    improve.status = "completed"
    improve.completed_at = datetime.now(UTC)
    improve.branch = impl.branch
    improve.changed_diff = False
    store.update(improve)

    git = _VerifyOnlyNoopGit(impl.branch or "", "same-head")
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="advance",
        dry_run=False,
        max_resume_attempts=3,
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
        config=config,
        git=git,
    )

    with (
        patch("gza.cli.advance_executor.Git", side_effect=lambda path: SimpleNamespace(repo_dir=Path(path), default_branch=lambda: "main", rev_parse_if_exists=lambda ref: "same-head")),
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
        patch.object(git, "worktree_remove", side_effect=RuntimeError("cleanup exploded")),
    ):
        result = execute_advance_action(
            task=impl,
            action={
                "type": "recover_verify_only_noop_review",
                "review_task": review,
                "latest_noop_improve_task": improve,
                "current_branch_head_sha": "same-head",
            },
            context=context,
        )

    refreshed_improve = store.get(improve.id)
    artifacts = store.list_artifacts(impl.id, kind=REVIEW_CLEARANCE_ARTIFACT_KIND)

    assert result.status == "skip"
    assert result.attention_reason == "improve-no-op"
    assert result.noop_improve_kind == NOOP_IMPROVE_KIND_VERIFY_ONLY
    assert "Cleanup failure: worktree removal failed: cleanup exploded" in result.message
    assert store.get(impl.id).review_cleared_at is None
    assert refreshed_improve is not None
    assert refreshed_improve.review_verify_status == "passed"
    assert artifacts == []
    parked = store.list_artifacts(improve.id, kind=VERIFY_ONLY_NOOP_RECOVERY_ATTENTION_ARTIFACT_KIND)
    assert len(parked) == 1
    assert parked[0].metadata["outcome_kind"] == "cleanup_failure"


def test_recover_verify_only_noop_review_cross_project_cleanup_failure_returns_attention(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "uv run pytest tests/unit -q"

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/verify-only-noop-cross-project-cleanup-failure")
    store.update(impl)

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    review.review_verify_status = "failed"
    review.review_verify_branch = impl.branch
    review.review_verify_head_sha = "same-head"
    store.update(review)

    improve = store.add(
        "Improve attempt",
        task_type="improve",
        depends_on=review.id,
        based_on=impl.id,
        same_branch=True,
    )
    assert improve.id is not None
    improve.status = "completed"
    improve.completed_at = datetime.now(UTC)
    improve.branch = impl.branch
    improve.changed_diff = False
    improve.tags = (CROSS_PROJECT_TAG,)
    store.update(improve)

    git = _VerifyOnlyNoopGit(impl.branch or "", "same-head")
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="advance",
        dry_run=False,
        max_resume_attempts=3,
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
        config=config,
        git=git,
    )

    with (
        patch(
            "gza.cli.advance_executor.Git",
            side_effect=lambda path: SimpleNamespace(
                repo_dir=Path(path),
                default_branch=lambda: "main",
                rev_parse_if_exists=lambda ref: "same-head",
            ),
        ),
        patch("gza.cli.advance_executor._resolve_review_verify_base_sha", return_value="base-sha"),
        patch("gza.cli.advance_executor._run_review_verify_commands_for_projects", return_value=None),
        patch.object(git, "worktree_remove", side_effect=RuntimeError("cleanup exploded")),
    ):
        result = execute_advance_action(
            task=impl,
            action={
                "type": "recover_verify_only_noop_review",
                "review_task": review,
                "latest_noop_improve_task": improve,
                "current_branch_head_sha": "same-head",
            },
            context=context,
        )

    assert result.status == "skip"
    assert result.attention_reason == "improve-no-op"
    assert result.noop_improve_kind == NOOP_IMPROVE_KIND_VERIFY_ONLY
    assert "Cleanup failure: worktree removal failed: cleanup exploded" in result.message
    parked = store.list_artifacts(improve.id, kind=VERIFY_ONLY_NOOP_RECOVERY_ATTENTION_ARTIFACT_KIND)
    assert len(parked) == 1
    assert parked[0].metadata["outcome_kind"] == "cleanup_failure"


def test_recover_verify_only_noop_review_clearance_persistence_failure_returns_structured_error(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "uv run pytest tests/unit -q"

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/verify-only-noop-clearance-failure")
    store.update(impl)

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    review.review_verify_status = "failed"
    review.review_verify_branch = impl.branch
    review.review_verify_head_sha = "same-head"
    store.update(review)

    improve = store.add(
        "Improve attempt",
        task_type="improve",
        depends_on=review.id,
        based_on=impl.id,
        same_branch=True,
    )
    assert improve.id is not None
    improve.status = "completed"
    improve.completed_at = datetime.now(UTC)
    improve.branch = impl.branch
    improve.changed_diff = False
    store.update(improve)

    git = _VerifyOnlyNoopGit(impl.branch or "", "same-head")
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="advance",
        dry_run=False,
        max_resume_attempts=3,
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
        config=config,
        git=git,
    )

    with (
        patch(
            "gza.cli.advance_executor.Git",
            side_effect=lambda path: SimpleNamespace(
                repo_dir=Path(path),
                default_branch=lambda: "main",
                rev_parse_if_exists=lambda ref: "same-head",
            ),
        ),
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
        patch(
            "gza.cli.advance_executor._persist_verify_only_noop_clearance",
            side_effect=OffTopicVerifyPersistenceError("review clearance persistence failed: disk full"),
        ),
    ):
        result = execute_advance_action(
            task=impl,
            action={
                "type": "recover_verify_only_noop_review",
                "review_task": review,
                "latest_noop_improve_task": improve,
                "current_branch_head_sha": "same-head",
            },
            context=context,
        )

    refreshed_impl = store.get(impl.id)
    refreshed_improve = store.get(improve.id)
    clearance_artifacts = store.list_artifacts(impl.id, kind=REVIEW_CLEARANCE_ARTIFACT_KIND)
    verify_artifacts = store.list_artifacts(improve.id, kind="verify_command_output")

    assert result.status == "error"
    assert result.noop_improve_kind == NOOP_IMPROVE_KIND_VERIFY_ONLY
    assert result.message == (
        "failed to persist verify-only no-op clearance: "
        "review clearance persistence failed: disk full"
    )
    assert refreshed_impl is not None
    assert refreshed_impl.review_cleared_at is None
    assert refreshed_improve is not None
    assert refreshed_improve.review_verify_status == "passed"
    assert clearance_artifacts == []
    assert len(verify_artifacts) == 1
    parked = store.list_artifacts(improve.id, kind=VERIFY_ONLY_NOOP_RECOVERY_ATTENTION_ARTIFACT_KIND)
    assert len(parked) == 1
    assert parked[0].metadata["outcome_kind"] == "clearance_persistence_failure"
    assert "structured review_clearance could not be persisted" in parked[0].metadata["message"]


def test_materialize_plan_review_slices_includes_slice_prompt_and_provenance(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    plan = store.add("Plan lifecycle slices", task_type="plan")
    assert plan.id is not None
    plan.tags = ("root-tag",)
    store.update(plan)

    review = store.add("Review plan lifecycle slices", task_type="plan_review", depends_on=plan.id)
    assert review.id is not None
    store.update(review)

    manifest = validate_plan_review_manifest(
        {
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
                    "title": "Materialize prompts",
                    "prompt": "Use this distinctive reviewer-authored slice prompt.",
                    "scope": ["Keep provenance"],
                    "out_of_scope": ["CLI changes"],
                    "acceptance_criteria": ["Prompt preserved exactly"],
                    "depends_on_slices": [],
                    "based_on_slice": None,
                    "review_scope": "Prompt materialization only.",
                    "estimated_complexity": "small",
                    "expected_timeout_minutes": 30,
                    "requires_code_review": True,
                    "tags": ["slice-tag"],
                }
            ],
        },
        markdown_verdict="APPROVED",
        source_task_id=plan.id,
        source_task_type="plan",
        max_slice_timeout_minutes=30,
    )

    materialization = _materialize_plan_review_slices(
        Config.load(tmp_path),
        store,
        plan,
        review,
        manifest,
        trigger_source="manual",
        require_review_before_merge=True,
    )

    assert materialization.created is True
    assert len(materialization.tasks) == 1
    created_task = store.get(materialization.tasks[0].id)
    assert created_task is not None
    assert "Use this distinctive reviewer-authored slice prompt." in created_task.prompt
    assert f"- Plan source: {plan.id}" in created_task.prompt
    assert f"- Plan review: {review.id}" in created_task.prompt
    assert "- Slice: S1 (Materialize prompts)" in created_task.prompt
    assert "Scope:\n- Keep provenance" in created_task.prompt
    assert "Out of scope:\n- CLI changes" in created_task.prompt
    assert "Acceptance criteria:\n- Prompt preserved exactly" in created_task.prompt


def test_materialize_plan_review_slices_revalidates_manifest_before_creating_tasks(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan lifecycle slices", task_type="plan")
    assert plan.id is not None
    review = store.add("Review plan lifecycle slices", task_type="plan_review", depends_on=plan.id)
    assert review.id is not None

    manifest = validate_plan_review_manifest(
        {
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
                    "prompt": "Create the slice.",
                    "scope": ["One"],
                    "out_of_scope": [],
                    "acceptance_criteria": ["Slice exists"],
                    "depends_on_slices": [],
                    "based_on_slice": None,
                    "review_scope": "Foundation only.",
                    "estimated_complexity": "small",
                    "expected_timeout_minutes": 30,
                    "requires_code_review": True,
                    "tags": [],
                }
            ],
        },
        markdown_verdict="APPROVED",
        source_task_id=plan.id,
        source_task_type="plan",
        max_slice_timeout_minutes=30,
    )

    with patch("gza.cli._common.validate_plan_review_manifest", side_effect=ValueError("invalid manifest")):
        with pytest.raises(ValueError, match="invalid manifest"):
            _materialize_plan_review_slices(
                config,
                store,
                plan,
                review,
                manifest,
                trigger_source="manual",
                require_review_before_merge=True,
            )

    assert [task for task in store.get_all() if task.task_type == "implement"] == []
    assert store.list_artifacts(review.id, kind="plan_review_materialization") == []


def test_execute_create_plan_review_reports_created_task_when_spawn_fails(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    plan = store.add("Plan lifecycle slices", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=3,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("unused"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task, _kind: 1,
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        create_plan_review_task=lambda task: store.add(
            f"Review {task.id}",
            task_type="plan_review",
            depends_on=task.id,
            trigger_source="manual",
        ),
    )

    result = execute_advance_action(
        task=plan,
        action={"type": "create_plan_review"},
        context=context,
    )

    assert result.status == "error"
    assert result.created_task is not None
    assert result.created_task.task_type == "plan_review"
    assert result.created_task.id == result.handled_task_id
    assert result.error_message == f"Failed to start plan_review worker for task {result.handled_task_id}"
    persisted = store.get(result.handled_task_id)
    assert persisted is not None
    assert persisted.task_type == "plan_review"


def test_execute_create_plan_improve_reports_created_task_when_spawn_fails(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    plan = store.add("Plan lifecycle slices", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    review = store.add("Review plan lifecycle slices", task_type="plan_review", depends_on=plan.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    store.update(review)

    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=3,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("unused"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task, _kind: 1,
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        create_plan_improve_task=lambda source_task, review_task: store.add(
            f"Improve {source_task.id} from {review_task.id}",
            task_type="plan_improve",
            based_on=source_task.id,
            depends_on=review_task.id,
            trigger_source="manual",
        ),
    )

    result = execute_advance_action(
        task=plan,
        action={
            "type": "create_plan_improve",
            "plan_source_task": plan,
            "plan_review_task": review,
        },
        context=context,
    )

    assert result.status == "error"
    assert result.created_task is not None
    assert result.created_task.task_type == "plan_improve"
    assert result.created_task.id == result.handled_task_id
    assert result.error_message == f"Failed to start plan_improve worker for task {result.handled_task_id}"
    persisted = store.get(result.handled_task_id)
    assert persisted is not None
    assert persisted.task_type == "plan_improve"


def test_materialize_plan_review_slices_reuses_existing_materialization(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan lifecycle slices", task_type="plan")
    assert plan.id is not None
    review = store.add("Review plan lifecycle slices", task_type="plan_review", depends_on=plan.id)
    assert review.id is not None

    manifest = validate_plan_review_manifest(
        {
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
                    "title": "Materialize prompts",
                    "prompt": "Use this distinctive reviewer-authored slice prompt.",
                    "scope": ["Keep provenance"],
                    "out_of_scope": [],
                    "acceptance_criteria": ["Prompt preserved exactly"],
                    "depends_on_slices": [],
                    "based_on_slice": None,
                    "review_scope": "Prompt materialization only.",
                    "estimated_complexity": "small",
                    "expected_timeout_minutes": 30,
                    "requires_code_review": True,
                    "tags": ["slice-tag"],
                }
            ],
        },
        markdown_verdict="APPROVED",
        source_task_id=plan.id,
        source_task_type="plan",
        max_slice_timeout_minutes=30,
    )

    first = _materialize_plan_review_slices(
        config,
        store,
        plan,
        review,
        manifest,
        trigger_source="manual",
        require_review_before_merge=True,
    )
    second = _materialize_plan_review_slices(
        config,
        store,
        plan,
        review,
        manifest,
        trigger_source="manual",
        require_review_before_merge=True,
    )

    assert first.created is True
    assert second.created is False
    assert [task.id for task in first.tasks] == [task.id for task in second.tasks]
    assert len([task for task in store.get_all() if task.task_type == "implement"]) == 1


def test_materialize_plan_review_slices_reuses_legacy_manual_materialization_without_trigger_metadata(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan lifecycle slices", task_type="plan")
    assert plan.id is not None
    review = store.add("Review plan lifecycle slices", task_type="plan_review", depends_on=plan.id)
    assert review.id is not None

    manifest = validate_plan_review_manifest(
        {
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
                    "title": "Materialize prompts",
                    "prompt": "Use this distinctive reviewer-authored slice prompt.",
                    "scope": ["Keep provenance"],
                    "out_of_scope": [],
                    "acceptance_criteria": ["Prompt preserved exactly"],
                    "depends_on_slices": [],
                    "based_on_slice": None,
                    "review_scope": "Prompt materialization only.",
                    "estimated_complexity": "small",
                    "expected_timeout_minutes": 30,
                    "requires_code_review": True,
                    "tags": ["slice-tag"],
                }
            ],
        },
        markdown_verdict="APPROVED",
        source_task_id=plan.id,
        source_task_type="plan",
        max_slice_timeout_minutes=30,
    )

    legacy_task_specs = build_plan_review_slice_task_specs(
        plan_source_task=plan,
        review_task=review,
        manifest=manifest,
        trigger_source="manual",
        require_review_before_merge=True,
    )
    store.add_tasks_with_artifact_atomic(
        tasks=legacy_task_specs,
        artifact_task_id=review.id,
        artifact_kind=PLAN_REVIEW_MATERIALIZATION_ARTIFACT_KIND,
        artifact_label="plan_review_materialization",
        artifact_path=".gza/artifacts/materialized.txt",
        artifact_byte_size=0,
        artifact_sha256="",
        artifact_metadata_builder=lambda tasks: {
            "schema_version": 1,
            "review_task_id": review.id,
            "source_task_id": plan.id,
            "source_task_type": "plan",
            "manifest_digest": plan_review_manifest_digest(manifest),
            "task_ids": [task.id for task in tasks if task.id is not None],
        },
    )

    second = _materialize_plan_review_slices(
        config,
        store,
        plan,
        review,
        manifest,
        trigger_source="manual",
        require_review_before_merge=True,
    )

    assert second.created is False
    assert len(second.tasks) == 1
    assert second.tasks[0].trigger_source == "manual"
    assert len([task for task in store.get_all() if task.task_type == "implement"]) == 1


def test_materialize_plan_review_slices_rolls_back_partial_task_creation_on_failure(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan lifecycle slices", task_type="plan")
    assert plan.id is not None
    review = store.add("Review plan lifecycle slices", task_type="plan_review", depends_on=plan.id)
    assert review.id is not None

    manifest = validate_plan_review_manifest(
        {
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
                    "prompt": "Create the first slice.",
                    "scope": ["One"],
                    "out_of_scope": [],
                    "acceptance_criteria": ["First slice exists"],
                    "depends_on_slices": [],
                    "based_on_slice": None,
                    "review_scope": "Foundation only.",
                    "estimated_complexity": "small",
                    "expected_timeout_minutes": 30,
                    "requires_code_review": True,
                    "tags": [],
                },
                {
                    "slice_id": "S2",
                    "title": "Follow-up",
                    "prompt": "Create the second slice.",
                    "scope": ["Two"],
                    "out_of_scope": [],
                    "acceptance_criteria": ["Second slice exists"],
                    "depends_on_slices": ["S1"],
                    "based_on_slice": None,
                    "review_scope": "Follow-up only.",
                    "estimated_complexity": "small",
                    "expected_timeout_minutes": 30,
                    "requires_code_review": True,
                    "tags": [],
                },
            ],
        },
        markdown_verdict="APPROVED",
        source_task_id=plan.id,
        source_task_type="plan",
        max_slice_timeout_minutes=30,
    )

    original_add_task_conn = store._add_task_conn
    call_count = 0

    def flaky_add_task_conn(conn: Any, params: Any) -> DbTask:
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise RuntimeError("boom during second slice insert")
        return original_add_task_conn(conn, params)

    with patch.object(store, "_add_task_conn", side_effect=flaky_add_task_conn):
        with pytest.raises(RuntimeError, match="boom during second slice insert"):
            _materialize_plan_review_slices(
                config,
                store,
                plan,
                review,
                manifest,
                trigger_source="manual",
                require_review_before_merge=True,
            )

    assert [task for task in store.get_all() if task.task_type == "implement"] == []
    assert store.list_artifacts(review.id, kind="plan_review_materialization") == []


def test_materialize_plan_review_slices_rerun_recovers_after_artifact_write_failure(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan lifecycle slices", task_type="plan")
    assert plan.id is not None
    review = store.add("Review plan lifecycle slices", task_type="plan_review", depends_on=plan.id)
    assert review.id is not None

    manifest = validate_plan_review_manifest(
        {
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
                    "prompt": "Create the slice.",
                    "scope": ["One"],
                    "out_of_scope": [],
                    "acceptance_criteria": ["Slice exists"],
                    "depends_on_slices": [],
                    "based_on_slice": None,
                    "review_scope": "Foundation only.",
                    "estimated_complexity": "small",
                    "expected_timeout_minutes": 30,
                    "requires_code_review": True,
                    "tags": [],
                }
            ],
        },
        markdown_verdict="APPROVED",
        source_task_id=plan.id,
        source_task_type="plan",
        max_slice_timeout_minutes=30,
    )

    with patch.object(store, "delete", side_effect=AssertionError("delete cleanup should not run")):
        with patch.object(store, "_add_artifact_conn", side_effect=RuntimeError("artifact write failed")):
            with pytest.raises(RuntimeError, match="artifact write failed"):
                _materialize_plan_review_slices(
                    config,
                    store,
                    plan,
                    review,
                    manifest,
                    trigger_source="manual",
                    require_review_before_merge=True,
                )

    assert [task for task in store.get_all() if task.task_type == "implement"] == []
    assert store.list_artifacts(review.id, kind="plan_review_materialization") == []

    materialization = _materialize_plan_review_slices(
        config,
        store,
        plan,
        review,
        manifest,
        trigger_source="manual",
        require_review_before_merge=True,
    )

    assert materialization.created is True
    assert len(materialization.tasks) == 1
    assert len([task for task in store.get_all() if task.task_type == "implement"]) == 1
    artifacts = store.list_artifacts(review.id, kind="plan_review_materialization")
    assert len(artifacts) == 1
    assert artifacts[0].metadata["task_ids"] == [materialization.tasks[0].id]


def test_improve_dry_run_preserves_noop_warning_description(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/improve-noop-warning")
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    store.update(review)

    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=True,
        max_resume_attempts=3,
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
        action={
            "type": "improve",
            "review_task": review,
            "description": "Create improve task (review CHANGES_REQUESTED); previous no-op improve gza-9 made no tracked diff change",
        },
        context=context,
    )

    assert result.status == "dry_run"
    assert result.message is not None
    assert "previous no-op improve gza-9" in result.message


@pytest.mark.parametrize(
    ("reason_code", "reason_text"),
    [
        ("dependency_not_ready", "dependency precondition not satisfied"),
        ("recovery_already_running", "recovery child already in progress"),
    ],
)
def test_improve_skip_without_attention_for_shared_non_attention_recovery_reasons(
    tmp_path: Path,
    reason_code: str,
    reason_text: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/improve-shared-skip")
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    store.update(review)

    if reason_code == "dependency_not_ready":
        dependency = store.add("Dependency", task_type="implement")
        assert dependency.id is not None
        _mark_completed(dependency, branch="feature/dependency")
        dependency.merge_status = "unmerged"
        store.update(dependency)

        failed_improve = store.add(
            "Improve attempt",
            task_type="improve",
            depends_on=dependency.id,
            based_on=impl.id,
        )
        assert failed_improve.id is not None
        failed_improve.status = "failed"
        failed_improve.failure_reason = "PREREQUISITE_UNMERGED"
        failed_improve.completed_at = datetime.now(UTC)
        store.update(failed_improve)
    else:
        failed_improve = store.add(
            "Improve attempt",
            task_type="improve",
            depends_on=review.id,
            based_on=impl.id,
            same_branch=True,
        )
        assert failed_improve.id is not None
        failed_improve.status = "failed"
        failed_improve.failure_reason = "MAX_TURNS"
        failed_improve.session_id = "sess-improve"
        failed_improve.completed_at = datetime.now(UTC)
        store.update(failed_improve)

        running_child = store.add(
            failed_improve.prompt,
            task_type="improve",
            based_on=failed_improve.id,
            depends_on=failed_improve.depends_on,
            same_branch=failed_improve.same_branch,
        )
        assert running_child.id is not None
        running_child.status = "in_progress"
        running_child.session_id = failed_improve.session_id
        store.update(running_child)

    improve_decision = decide_failed_task_recovery(
        store,
        failed_improve,
        max_recovery_attempts=1,
    )
    assert improve_decision.reason_code == reason_code

    result = build_improve_needs_attention_result(
        store=store,
        impl_task=impl,
        review_task=review,
        improve_mode="manual_review",
        failed_improve=failed_improve,
        improve_decision=improve_decision,
        max_resume_attempts=1,
    )

    assert result is not None
    assert result.status == "skip"
    assert result.attention_type is None
    assert result.attention_reason is None
    assert reason_text in result.message
    assert resolve_execution_needs_attention(impl, result) is None


def test_improve_give_up_reports_automatic_recovery_disabled(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/improve-disabled")
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    store.update(review)

    failed = store.add(
        "Improve 0",
        task_type="improve",
        depends_on=review.id,
        based_on=impl.id,
        same_branch=True,
    )
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-0"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    before_count = len(store.get_all())
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
    expected = build_improve_needs_attention_result(
        store=store,
        impl_task=impl,
        review_task=review,
        improve_mode="give_up",
        failed_improve=failed,
        improve_decision=None,
        max_resume_attempts=0,
    )

    assert expected is not None
    assert result.status == "skip"
    assert result.attention_type == "automatic_recovery_disabled"
    assert result == expected
    assert len(store.get_all()) == before_count
    attention = resolve_execution_needs_attention(impl, result)
    assert attention is not None
    assert attention.task.id == impl.id
    assert attention.action["subject_task_id"] == impl.id


@pytest.mark.parametrize("trigger_source", ["manual", "watch"])
def test_improve_retry_uses_context_trigger_source_and_preserves_review_backed_execution_settings(
    tmp_path: Path,
    trigger_source: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/improve-retry-preserve")
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    store.update(review)

    failed = store.add(
        "Improve attempt",
        task_type="improve",
        depends_on=review.id,
        based_on=impl.id,
        same_branch=True,
    )
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.create_review = True
    failed.create_pr = True
    failed.model = "gpt-5.4"
    failed.provider = "codex"
    failed.provider_is_explicit = True
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    spawned: list[tuple[str, str]] = []
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source=trigger_source,
        dry_run=False,
        max_resume_attempts=3,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("unused"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda task_obj, kind: spawned.append((str(task_obj.id), kind)) or 0,
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
    )

    result = execute_advance_action(
        task=impl,
        action={"type": "improve", "review_task": review},
        context=context,
    )

    assert result.status == "success"
    assert result.improve_mode == "retry"
    assert result.created_task is not None
    assert result.created_task.id is not None
    assert result.created_task.id != failed.id
    assert result.created_task.based_on == failed.id
    assert result.created_task.create_review is True
    assert result.created_task.create_pr is True
    assert result.created_task.model == "gpt-5.4"
    assert result.created_task.provider == "codex"
    assert result.created_task.provider_is_explicit is True
    assert result.created_task.trigger_source == trigger_source
    assert spawned == [(result.created_task.id, "improve")]


@pytest.mark.parametrize("trigger_source", ["manual", "watch"])
def test_improve_executor_uses_context_trigger_source_for_followup_after_completed_noop_improve(
    tmp_path: Path,
    trigger_source: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/improve-noop-followup")
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    store.update(review)

    noop_improve = store.add(
        "Improve attempt",
        task_type="improve",
        depends_on=review.id,
        based_on=impl.id,
        same_branch=True,
    )
    assert noop_improve.id is not None
    noop_improve.status = "completed"
    noop_improve.changed_diff = False
    noop_improve.completed_at = datetime.now(UTC)
    store.update(noop_improve)

    spawned: list[tuple[str, str]] = []
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source=trigger_source,
        dry_run=False,
        max_resume_attempts=3,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("unused"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda task_obj, kind: spawned.append((str(task_obj.id), kind)) or 0,
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
    )

    result = execute_advance_action(
        task=impl,
        action={"type": "improve", "review_task": review},
        context=context,
    )

    assert result.status == "success"
    assert result.improve_mode == "new"
    assert result.created_task is not None
    assert result.created_task.id is not None
    assert result.created_task.based_on == noop_improve.id
    assert result.created_task.depends_on == review.id
    assert result.created_task.trigger_source == trigger_source
    assert spawned == [(result.created_task.id, "improve")]


@pytest.mark.parametrize(
    "retired_symbol",
    [
        "verify_" "noop_improve_then_review",
        "NoopVerify" "ThenReview",
        "run_" "noop_improve_verify_then_review",
        "fresh_" "verify_resolves_verify_only_review",
        "_build_" "noop_verify_attention_result",
    ],
)
def test_retired_noop_verify_symbols_are_removed_from_live_code(retired_symbol: str) -> None:
    root = Path(__file__).resolve().parents[2]
    live_dirs = ("src", "tests", "specs", "docs")
    matches: list[str] = []

    for relative_dir in live_dirs:
        for path in (root / relative_dir).rglob("*"):
            if not path.is_file():
                continue
            if path.suffix not in {".py", ".md"}:
                continue
            text = path.read_text(encoding="utf-8")
            if retired_symbol in text:
                matches.append(str(path.relative_to(root)))

    assert matches == []


def test_retired_noop_verify_action_is_not_routable_or_worker_consuming() -> None:
    retired_action = "verify_" "noop_improve_then_review"

    assert retired_action not in ITERATE_ROUTABLE_ACTIONS
    assert retired_action not in _WORKER_ACTIONS
    assert is_worker_consuming_advance_action(retired_action) is False
    assert count_worker_consuming_actions(
        [
            {"type": "create_review"},
            {"type": retired_action},
            {"type": "run_improve"},
        ]
    ) == 2


def test_execute_advance_action_rejects_retired_noop_verify_action(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/retired-noop-verify")
    store.update(impl)

    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=3,
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
        action={"type": "verify_" "noop_improve_then_review"},
        context=context,
    )

    assert result.status == "unsupported"
    assert result.message == "unsupported action: verify_" "noop_improve_then_review"


def test_create_review_skip_propagates_message_without_spawning(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Implement feature", task_type="implement")
    assert task.id is not None
    _mark_completed(task, branch="feature/create-review-skip")
    store.update(task)

    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=1,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: type(
            "_R",
            (),
            {"status": "skip", "review_task": None, "message": "SKIP: review already pending"},
        )(),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task, _kind: pytest.fail("spawn should not run"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
    )

    result = execute_advance_action(task=task, action={"type": "create_review"}, context=context)

    assert result.status == "skip"
    assert result.message == "SKIP: review already pending"


def test_create_review_can_route_through_iterate_before_creating_child(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/create-review-iterate")
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    spawned: list[tuple[str, str]] = []
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=1,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("plain review creation should not run"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task, _kind: pytest.fail("plain worker should not run"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda task_obj, kind: spawned.append((str(task_obj.id), kind)) or 0,
        prefer_iterate_for_action=lambda task, _action: task,
    )

    result = execute_advance_action(task=impl, action={"type": "create_review"}, context=context)

    assert result.status == "success"
    assert result.handled_task_id == impl.id
    assert result.worker_label == "iterate"
    assert spawned == [(impl.id, "iterate")]


def test_retry_iterate_missing_launcher_releases_reserved_launch_permit(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "max_concurrent: 1\n")
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

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
        create_retry_task=lambda task: _create_retry_task(store, task, trigger_source="manual"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_recovery=None,
        config=config,
    )

    result = execute_advance_action(
        task=failed,
        action={"type": "retry", "launch_mode": "iterate"},
        context=context,
    )

    assert result.status == "error"
    assert result.message == "missing iterate recovery launcher"

    permit = launch_permit(config, store)
    permit.release()


def test_run_improve_can_return_fail_closed_iterate_skip_result(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/run-improve-iterate-skip")
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    store.update(review)

    improve = store.add(
        "Improve feature",
        task_type="improve",
        depends_on=review.id,
        based_on=impl.id,
        same_branch=True,
    )
    assert improve.id is not None

    expected = AdvanceActionExecutionResult(
        action_type="run_improve",
        status="skip",
        message=f"{impl.id}: iterate already running for implementation chain",
        worker_label="iterate",
        guarded_pending_task_id=improve.id,
    )
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
        spawn_worker=lambda _task, _kind: pytest.fail("plain worker should not run"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("iterate spawn should not run"),
        prefer_iterate_for_action=lambda _task, _action: expected,
    )

    result = execute_advance_action(
        task=impl,
        action={"type": "run_improve", "improve_task": improve},
        context=context,
    )

    assert result == expected


@pytest.mark.parametrize(
    ("action_type", "expected_message"),
    [
        ("resume", "Reused pending resume task"),
        ("retry", "Reused pending retry task"),
    ],
)
def test_reused_failed_task_recovery_reports_reuse_message(
    tmp_path: Path,
    action_type: str,
    expected_message: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed task", task_type="plan")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS" if action_type == "resume" else "INFRASTRUCTURE_ERROR"
    failed.session_id = "sess-1" if action_type == "resume" else None
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    reused = store.add("Pending recovery task", task_type=failed.task_type, based_on=failed.id)
    assert reused.id is not None
    reused.status = "pending"
    if action_type == "resume":
        reused.depends_on = failed.depends_on
        reused.session_id = failed.session_id
        reused.spec = failed.spec
        reused.branch = failed.branch
    store.update(reused)

    spawned: list[tuple[str, str]] = []
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=1,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("unused"),
        create_resume_task=lambda _task: pytest.fail("should reuse existing task"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda task_obj, kind: spawned.append((str(task_obj.id), kind)) or 0,
        spawn_resume_worker=lambda task_obj, kind: spawned.append((str(task_obj.id), kind)) or 0,
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        create_retry_task=lambda _task: pytest.fail("should reuse existing task"),
    )

    result = execute_advance_action(
        task=failed,
        action={
            "type": action_type,
            "launch_mode": "worker",
            "recovery_task_id": reused.id,
            "reuse_existing": True,
        },
        context=context,
    )

    assert result.status == "success"
    assert result.success_message == f"{expected_message} {reused.id}"
    assert result.created_task is not None
    assert result.created_task.id == reused.id
    expected_kind = failed.task_type or "task"
    assert spawned == [(reused.id, expected_kind)]


@pytest.mark.parametrize("trigger_source", ["manual", "watch"])
def test_retry_action_uses_context_retry_factory_trigger_source(
    tmp_path: Path,
    trigger_source: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed task", task_type="plan")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    spawned: list[tuple[str, str]] = []
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source=trigger_source,
        dry_run=False,
        max_resume_attempts=1,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("unused"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_retry_task=lambda task: _create_retry_task(store, task, trigger_source=trigger_source),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda task_obj, kind: spawned.append((str(task_obj.id), kind)) or 0,
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
    )

    result = execute_advance_action(task=failed, action={"type": "retry"}, context=context)

    assert result.status == "success"
    assert result.created_task is not None
    assert result.created_task.trigger_source == trigger_source
    assert spawned == [(result.created_task.id, "plan")]


def test_create_implement_uses_shared_lineage_and_selected_spawn_path(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    plan = store.add("Plan feature", task_type="plan")
    assert plan.id is not None
    _mark_completed(plan)
    store.update(plan)

    spawned: dict[str, int] = {"worker": 0, "iterate": 0}

    def _create_implement(parent: DbTask) -> DbTask:
        assert parent.id is not None
        return store.add(
            prompt=f"Implement plan {parent.id}",
            task_type="implement",
            depends_on=parent.id,
            group=parent.group,
        )

    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=1,
        use_iterate_for_create_implement=True,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("unused"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=_create_implement,
        spawn_worker=lambda _task, _kind: spawned.__setitem__("worker", spawned["worker"] + 1) or 0,
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: spawned.__setitem__("iterate", spawned["iterate"] + 1) or 0,
    )

    result = execute_advance_action(task=plan, action={"type": "create_implement"}, context=context)

    assert result.status == "success"
    assert result.created_task is not None
    assert result.created_task.depends_on == plan.id
    assert spawned["iterate"] == 1
    assert spawned["worker"] == 0


def test_needs_rebase_dry_run_does_not_create_task(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Implement feature", task_type="implement")
    assert task.id is not None
    _mark_completed(task, branch="feature/rebase-dry-run")
    store.update(task)

    before_count = len(store.get_all())
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=True,
        max_resume_attempts=1,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("unused"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("should not create rebase task in dry-run"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
    )

    result = execute_advance_action(task=task, action={"type": "needs_rebase"}, context=context)

    assert result.status == "dry_run"
    assert result.worker_consuming is True
    assert len(store.get_all()) == before_count


def test_advance_executor_skips_needs_rebase_if_target_already_merged_before_create(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Implement feature", task_type="implement")
    assert task.id is not None
    _mark_completed(task, branch="feature/rebase-skip")
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
        create_rebase_task=lambda _task: pytest.fail("should not create rebase task"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        is_rebase_target_already_merged=lambda _task: True,
    )

    result = execute_advance_action(task=task, action={"type": "needs_rebase"}, context=context)

    assert result.status == "skip"
    assert result.message == "target implementation already merged"
    assert result.worker_consuming is False


def test_needs_rebase_iterate_rolls_back_when_prepare_fails(tmp_path: Path) -> None:
    """advance_mode=iterate must create+prepare the rebase child in the parent and
    surface preparation failures without spawning iterate or leaving an orphan row."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/needs-rebase-iterate-fail")
    store.update(impl)

    before_count = len(store.get_all())
    rollback_calls: list[bool] = []

    def _create_rebase(parent: DbTask) -> DbTask:
        assert parent.id is not None
        assert parent.branch is not None
        return store.add(
            prompt=f"Rebase {parent.branch}",
            task_type="rebase",
            based_on=parent.id,
            same_branch=True,
        )

    def _prepare_fails(task: DbTask, rollback_on_failure: bool) -> DbTask | None:
        rollback_calls.append(rollback_on_failure)
        if rollback_on_failure and task.id is not None:
            store.delete(task.id)
        return None

    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=1,
        use_iterate_for_create_implement=True,
        use_iterate_for_needs_rebase=True,
        prepare_task_for_background_start=_prepare_fails,
        prepare_create_review=lambda _task: pytest.fail("unused"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=_create_rebase,
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task, _kind: pytest.fail("worker spawn must not run when prepare fails"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda *a, **kw: pytest.fail("iterate spawn must not run when prepare fails"),
    )

    result = execute_advance_action(task=impl, action={"type": "needs_rebase"}, context=context)

    assert result.status == "error"
    assert result.error_message  # caller-visible failure surface
    assert rollback_calls == [True]
    # The just-created rebase row was rolled back: no new tasks remain.
    assert len(store.get_all()) == before_count
    rebase_rows = [t for t in store.get_all() if t.task_type == "rebase"]
    assert rebase_rows == []


def test_needs_rebase_skips_at_max_concurrent_without_creating_task(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "max_concurrent: 1\n")
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    running = store.add("Running task", task_type="implement")
    running.status = "in_progress"
    running.running_pid = os.getpid()
    store.update(running)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/needs-rebase-cap")
    store.update(impl)

    before_count = len(store.get_all())

    def _create_rebase(parent: DbTask) -> DbTask:
        assert parent.id is not None
        assert parent.branch is not None
        return store.add(
            prompt=f"Rebase {parent.branch}",
            task_type="rebase",
            based_on=parent.id,
            same_branch=True,
        )

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
        create_rebase_task=_create_rebase,
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task, _kind: pytest.fail("spawn must not run at max concurrent"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda *_args, **_kwargs: pytest.fail("unused"),
        config=config,
    )

    result = execute_advance_action(task=impl, action={"type": "needs_rebase"}, context=context)

    assert result.status == "skip"
    assert result.message == "SKIP: already at max concurrent tasks: 1 running, limit is 1"
    assert len(store.get_all()) == before_count
    assert [task for task in store.get_all() if task.task_type == "rebase"] == []


def test_needs_rebase_iterate_hands_prepared_metadata_to_spawn(tmp_path: Path) -> None:
    """advance_mode=iterate's needs_rebase path must spawn iterate with the
    prepared rebase task id and action metadata, and point worker output at the
    rebase child rather than the original implementation."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/needs-rebase-iterate-ok")
    store.update(impl)

    captured: dict[str, object] = {}

    def _create_rebase(parent: DbTask) -> DbTask:
        assert parent.id is not None
        assert parent.branch is not None
        return store.add(
            prompt=f"Rebase {parent.branch}",
            task_type="rebase",
            based_on=parent.id,
            same_branch=True,
        )

    def _prepare_returns_task(task: DbTask, rollback_on_failure: bool) -> DbTask | None:
        captured["prepare_rollback"] = rollback_on_failure
        captured["prepare_task_id"] = task.id
        return task

    def _spawn_iterate(
        task_obj: DbTask,
        kind: str,
        *,
        prepared_task: DbTask | None = None,
        prepared_phase: str | None = None,
        prepared_action_type: str | None = None,
    ) -> int:
        captured["spawn_task_id"] = task_obj.id
        captured["spawn_kind"] = kind
        captured["spawn_prepared_task_id"] = prepared_task.id if prepared_task else None
        captured["spawn_prepared_phase"] = prepared_phase
        captured["spawn_prepared_action_type"] = prepared_action_type
        return 0

    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=1,
        use_iterate_for_create_implement=True,
        use_iterate_for_needs_rebase=True,
        prepare_task_for_background_start=_prepare_returns_task,
        prepare_create_review=lambda _task: pytest.fail("unused"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=_create_rebase,
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task, _kind: pytest.fail("plain worker must not run in iterate mode"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=_spawn_iterate,
    )

    result = execute_advance_action(task=impl, action={"type": "needs_rebase"}, context=context)

    rebase_rows = [t for t in store.get_all() if t.task_type == "rebase"]
    assert len(rebase_rows) == 1
    rebase = rebase_rows[0]
    assert rebase.id is not None

    assert captured["prepare_rollback"] is True
    assert captured["prepare_task_id"] == rebase.id
    # Iterate runs against the implementation task, but the prepared metadata
    # points the worker at the rebase child.
    assert captured["spawn_task_id"] == impl.id
    assert captured["spawn_kind"] == "rebase"
    assert captured["spawn_prepared_task_id"] == rebase.id
    assert captured["spawn_prepared_phase"] == "iteration"
    assert captured["spawn_prepared_action_type"] == "needs_rebase"

    assert result.status == "success"
    assert result.worker_label == "iterate"
    assert result.created_task is not None
    # Worker metadata + handled id reflect the prepared rebase row, not the impl.
    assert result.created_task.id == rebase.id
    assert result.handled_task_id == rebase.id
    assert result.success_message == f"Created rebase task {rebase.id}"


def test_reconcile_branch_divergence_dry_run_does_not_mutate_db(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Implement feature", task_type="implement")
    assert task.id is not None
    _mark_completed(task, branch="feature/reconcile-dry-run")
    store.update(task)

    before_count = len(store.get_all())
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=True,
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
    )

    result = execute_advance_action(
        task=task,
        action={"type": "reconcile_branch_divergence", "description": "Reconcile diverged refs"},
        context=context,
    )

    assert result.status == "dry_run"
    assert result.worker_consuming is False
    assert len(store.get_all()) == before_count


def test_reconcile_branch_divergence_reports_direct_success(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Implement feature", task_type="implement")
    assert task.id is not None
    _mark_completed(task, branch="feature/reconcile-direct")
    store.update(task)
    config = Config.load(tmp_path)
    git = SimpleNamespace()

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
        reconcile_diverged_branch=lambda _task: BranchDivergenceReconcileResult(
            status="reconciled",
            message="Reconciled 'feature/reconcile-direct' with --force-with-lease",
        ),
        config=config,
        git=git,
    )

    with (
        patch(
            "gza.cli.git_ops.complete_branch_unpushable_after_reconcile",
            side_effect=AssertionError("ordinary reconcile should not continue PR publication"),
        ) as complete_after_reconcile,
        patch(
            "gza.runner.ensure_task_pr",
            side_effect=AssertionError("ordinary reconcile should not touch PR publication"),
        ) as ensure_pr,
    ):
        result = execute_advance_action(
            task=task,
            action={"type": "reconcile_branch_divergence"},
            context=context,
        )

    assert result.status == "success"
    assert result.work_done is True
    assert "force-with-lease" in result.message
    complete_after_reconcile.assert_not_called()
    ensure_pr.assert_not_called()


def test_reconcile_branch_divergence_completes_failed_branch_unpushable_task(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Implement feature", task_type="implement", create_pr=True)
    assert task.id is not None
    task.status = "failed"
    task.failure_reason = "BRANCH_UNPUSHABLE"
    task.branch = "feature/reconcile-complete"
    task.has_commits = True
    task.log_file = "logs/reconcile.log"
    task.output_content = "summary"
    task.diff_files_changed = 2
    task.diff_lines_added = 5
    task.diff_lines_removed = 1
    task.completed_at = datetime.now(UTC)
    store.update(task)

    config = Config.load(tmp_path)
    git = SimpleNamespace(
        default_branch=lambda: "main",
        count_commits_ahead=lambda *_args: 1,
        rev_parse_if_exists=lambda ref: {"feature/reconcile-complete": "head123", "main": "base456"}.get(ref),
    )
    ensure_result = SimpleNamespace(ok=True, status="created", error=None, pr_url="https://example.test/pr/1")

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
        reconcile_diverged_branch=lambda _task: BranchDivergenceReconcileResult(
            status="reconciled",
            message="Reconciled 'feature/reconcile-complete' with --force-with-lease",
        ),
        config=config,
        git=git,
    )

    with (
        patch("gza.runner.ensure_task_pr", return_value=ensure_result) as ensure_pr,
        patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        patch("gza.runner.task_footer"),
    ):
        result = execute_advance_action(
            task=task,
            action={
                "type": "reconcile_branch_divergence",
                "decision": FailedRecoveryDecision(
                    task_id=task.id,
                    action="reconcile",
                    reason_code="BRANCH_UNPUSHABLE",
                    reason_text="branch publication failed; reconcile local/origin refs",
                    launch_mode="none",
                    attempt_index=1,
                    attempt_limit=1,
                ),
            },
            context=context,
        )

    assert result.status == "success"
    ensure_pr.assert_called_once()
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.status == "completed"
    assert refreshed.failure_reason is None
    assert refreshed.pr_number is None


def test_reconcile_branch_divergence_completes_with_nonfatal_pr_creation_note(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Implement feature", task_type="implement", create_pr=True)
    assert task.id is not None
    task.status = "failed"
    task.failure_reason = "BRANCH_UNPUSHABLE"
    task.branch = "feature/reconcile-nonfatal-pr-note"
    task.has_commits = True
    task.log_file = "logs/reconcile-nonfatal.log"
    task.output_content = "summary"
    task.diff_files_changed = 2
    task.diff_lines_added = 5
    task.diff_lines_removed = 1
    task.completed_at = datetime.now(UTC)
    store.update(task)

    config = Config.load(tmp_path)
    git = SimpleNamespace(
        default_branch=lambda: "main",
        count_commits_ahead=lambda *_args: 1,
        rev_parse_if_exists=lambda ref: {
            "feature/reconcile-nonfatal-pr-note": "head123",
            "main": "base456",
        }.get(ref),
    )
    ensure_result = SimpleNamespace(ok=False, status="create_failed", error="gh create failed", pr_url=None)

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
        reconcile_diverged_branch=lambda _task: BranchDivergenceReconcileResult(
            status="reconciled",
            message="Reconciled 'feature/reconcile-nonfatal-pr-note' with --force-with-lease",
        ),
        config=config,
        git=git,
    )

    with (
        patch("gza.runner.ensure_task_pr", return_value=ensure_result) as ensure_pr,
        patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        patch("gza.runner.task_footer"),
    ):
        result = execute_advance_action(
            task=task,
            action={
                "type": "reconcile_branch_divergence",
                "decision": FailedRecoveryDecision(
                    task_id=task.id,
                    action="reconcile",
                    reason_code="BRANCH_UNPUSHABLE",
                    reason_text="branch publication failed; reconcile local/origin refs",
                    launch_mode="none",
                    attempt_index=1,
                    attempt_limit=1,
                ),
            },
            context=context,
        )

    assert result.status == "success"
    ensure_pr.assert_called_once()
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.status == "completed"
    assert refreshed.failure_reason is None
    log_text = ops_log_path_for(tmp_path / "logs" / "reconcile-nonfatal.log").read_text()
    assert '"subtype": "pr_publication_note"' in log_text
    assert '"status": "create_failed"' in log_text


def test_reconcile_branch_divergence_push_still_failing_keeps_branch_unpushable(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Implement feature", task_type="implement", create_pr=True)
    assert task.id is not None
    task.status = "failed"
    task.failure_reason = "BRANCH_UNPUSHABLE"
    task.branch = "feature/reconcile-still-failing"
    task.has_commits = True
    task.log_file = "logs/reconcile-still-failing.log"
    task.output_content = "summary"
    task.diff_files_changed = 2
    task.diff_lines_added = 5
    task.diff_lines_removed = 1
    task.completed_at = datetime.now(UTC)
    store.update(task)

    config = Config.load(tmp_path)
    git = SimpleNamespace(
        default_branch=lambda: "main",
        count_commits_ahead=lambda *_args: 1,
        rev_parse_if_exists=lambda ref: {
            "feature/reconcile-still-failing": "head123",
            "main": "base456",
        }.get(ref),
    )
    ensure_result = SimpleNamespace(ok=False, status="push_failed", error="push failed again", pr_url=None)

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
        reconcile_diverged_branch=lambda _task: BranchDivergenceReconcileResult(
            status="reconciled",
            message="Reconciled 'feature/reconcile-still-failing' with --force-with-lease",
        ),
        config=config,
        git=git,
    )

    with (
        patch("gza.runner.ensure_task_pr", return_value=ensure_result) as ensure_pr,
        patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        patch("gza.runner.task_footer"),
    ):
        result = execute_advance_action(
            task=task,
            action={
                "type": "reconcile_branch_divergence",
                "decision": FailedRecoveryDecision(
                    task_id=task.id,
                    action="reconcile",
                    reason_code="BRANCH_UNPUSHABLE",
                    reason_text="branch publication failed; reconcile local/origin refs",
                    launch_mode="none",
                    attempt_index=1,
                    attempt_limit=1,
                ),
            },
            context=context,
        )

    assert result.status == "error"
    ensure_pr.assert_called_once()
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.status == "failed"
    assert refreshed.failure_reason == "BRANCH_UNPUSHABLE"
    follow_up_decision = decide_failed_task_recovery(store, refreshed, max_recovery_attempts=1)
    assert follow_up_decision.reason_code == "retry_limit_reached"


def test_reconcile_branch_divergence_fix_continuation_preserves_follow_up_review_decision(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/fix-reconcile")
    store.update(impl)
    impl_unit = store.get_or_create_merge_unit_for_task(impl)
    assert impl_unit is not None
    store.set_merge_unit_state(impl_unit.id, "merged")

    prior_review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert prior_review.id is not None
    _mark_completed(prior_review)
    store.update(prior_review)

    fix = store.add(
        "Fix feature",
        task_type="fix",
        based_on=impl.id,
        same_branch=True,
        create_review=True,
    )
    assert fix.id is not None
    fix.branch = impl.branch
    store.mark_failed(
        fix,
        log_file="logs/fix-reconcile.log",
        has_commits=True,
        branch=fix.branch,
        failure_reason="BRANCH_UNPUSHABLE",
        head_sha="head123",
        base_sha="base456",
    )
    fix.output_content = "summary"
    fix.diff_files_changed = 1
    fix.diff_lines_added = 2
    fix.diff_lines_removed = 0
    store.update(fix)
    persist_branch_publication_state(
        store=store,
        task=fix,
        config=Config.load(tmp_path),
        state=BranchPublicationState(
            fix_commits_ahead_before_run=2,
            fix_default_branch="main",
            fix_was_merged_before_run=True,
        ),
        status="BRANCH_UNPUSHABLE",
        exit_status="initial_failure",
        head_sha="head123",
    )

    config = Config.load(tmp_path)
    git = SimpleNamespace(
        default_branch=lambda: "main",
        count_commits_ahead=lambda *_args: 3,
        rev_parse_if_exists=lambda ref: {"feature/fix-reconcile": "head123", "main": "base456"}.get(ref),
    )
    ensure_result = SimpleNamespace(ok=True, status="created", error=None, pr_url="https://example.test/pr/3")

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
        reconcile_diverged_branch=lambda _task: BranchDivergenceReconcileResult(
            status="reconciled",
            message="Reconciled 'feature/fix-reconcile' with --force-with-lease",
        ),
        config=config,
        git=git,
    )

    with (
        patch("gza.runner.ensure_task_pr", return_value=ensure_result),
        patch("gza.runner.sync_task_branch_if_live_pr", return_value=SimpleNamespace(ok=True, status="pushed")),
        patch("gza.runner._create_and_run_review_task", return_value=0) as run_review,
        patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        patch("gza.runner.task_footer"),
    ):
        result = execute_advance_action(
            task=fix,
            action={
                "type": "reconcile_branch_divergence",
                "decision": FailedRecoveryDecision(
                    task_id=fix.id,
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
    run_review.assert_called_once()
    refreshed_impl = store.get(impl.id)
    assert refreshed_impl is not None
    assert refreshed_impl.merge_status == "unmerged"
    assert refreshed_impl.review_cleared_at is not None


def test_reconcile_branch_divergence_fix_continuation_restores_merged_state_without_new_commits(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/fix-reconcile")
    store.update(impl)
    impl_unit = store.get_or_create_merge_unit_for_task(impl)
    assert impl_unit is not None
    store.set_merge_unit_state(impl_unit.id, "merged")

    prior_review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert prior_review.id is not None
    _mark_completed(prior_review)
    store.update(prior_review)

    fix = store.add(
        "Fix feature",
        task_type="fix",
        based_on=impl.id,
        same_branch=True,
        create_review=True,
    )
    assert fix.id is not None
    fix.branch = impl.branch
    store.mark_failed(
        fix,
        log_file="logs/fix-reconcile.log",
        has_commits=True,
        branch=fix.branch,
        failure_reason="BRANCH_UNPUSHABLE",
        head_sha="head123",
        base_sha="base456",
    )
    fix.output_content = "summary"
    fix.diff_files_changed = 1
    fix.diff_lines_added = 2
    fix.diff_lines_removed = 0
    store.update(fix)
    persist_branch_publication_state(
        store=store,
        task=fix,
        config=Config.load(tmp_path),
        state=BranchPublicationState(
            fix_commits_ahead_before_run=2,
            fix_default_branch="main",
            fix_was_merged_before_run=True,
        ),
        status="BRANCH_UNPUSHABLE",
        exit_status="initial_failure",
        head_sha="head123",
    )

    config = Config.load(tmp_path)
    git = SimpleNamespace(
        default_branch=lambda: "main",
        count_commits_ahead=lambda *_args: 2,
        rev_parse_if_exists=lambda ref: {"feature/fix-reconcile": "head123", "main": "base456"}.get(ref),
    )
    ensure_result = SimpleNamespace(ok=True, status="created", error=None, pr_url="https://example.test/pr/4")

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
        reconcile_diverged_branch=lambda _task: BranchDivergenceReconcileResult(
            status="reconciled",
            message="Reconciled 'feature/fix-reconcile' with --force-with-lease",
        ),
        config=config,
        git=git,
    )

    with (
        patch("gza.runner.ensure_task_pr", return_value=ensure_result),
        patch("gza.runner.sync_task_branch_if_live_pr", side_effect=AssertionError("sync should not run")),
        patch("gza.runner._create_and_run_review_task", side_effect=AssertionError("review should not run")),
        patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        patch("gza.runner.task_footer"),
    ):
        result = execute_advance_action(
            task=fix,
            action={
                "type": "reconcile_branch_divergence",
                "decision": FailedRecoveryDecision(
                    task_id=fix.id,
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
    refreshed_impl = store.get(impl.id)
    assert refreshed_impl is not None
    assert refreshed_impl.merge_status == "merged"
    assert refreshed_impl.review_cleared_at is None


def test_reconcile_branch_divergence_conflict_creates_targeted_rebase_task(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/reconcile-conflict")
    store.update(impl)

    captured: dict[str, object] = {}

    def _create_targeted_rebase(parent: DbTask, rebase_target: str) -> DbTask:
        captured["target"] = rebase_target
        return store.add(
            prompt=f"Rebase {parent.branch} onto {rebase_target}",
            task_type="rebase",
            based_on=parent.id,
            same_branch=True,
        )

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
        create_targeted_rebase_task=_create_targeted_rebase,
        spawn_worker=lambda _task, _kind: 0,
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        reconcile_diverged_branch=lambda _task: BranchDivergenceReconcileResult(
            status="needs_rebase",
            message="Mechanical rebase conflicted",
            rebase_target="main",
        ),
    )

    result = execute_advance_action(
        task=impl,
        action={"type": "reconcile_branch_divergence"},
        context=context,
    )

    assert result.status == "success"
    assert captured["target"] == "main"
    assert result.success_message.startswith("Created rebase task ")


def test_reconcile_branch_divergence_needs_rebase_without_target_fails_closed(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/reconcile-missing-target")
    store.update(impl)

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
        reconcile_diverged_branch=lambda _task: BranchDivergenceReconcileResult(
            status="needs_rebase",
            message="Mechanical rebase conflicted",
            rebase_target=None,
        ),
    )

    result = execute_advance_action(
        task=impl,
        action={"type": "reconcile_branch_divergence"},
        context=context,
    )

    assert result.status == "error"
    assert "needs_rebase without a rebase_target" in result.message


def test_reconcile_branch_divergence_local_target_conflict_returns_needs_attention(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/reconcile-origin-conflict")
    store.update(impl)

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
        reconcile_diverged_branch=lambda _task: BranchDivergenceReconcileResult(
            status="needs_attention",
            message=(
                "SKIP: mechanical rebase onto local target 'main' hit conflicts: "
                "conflict. Resolve the local-target rebase manually before continuing."
            ),
            attention_reason="reconcile-needs-manual-resolution",
        ),
    )

    result = execute_advance_action(
        task=impl,
        action={"type": "reconcile_branch_divergence"},
        context=context,
    )

    assert result.status == "skip"
    assert result.attention_reason == "reconcile-needs-manual-resolution"
    attention = resolve_execution_needs_attention(impl, result)
    assert attention is not None
    assert attention.task.id == impl.id
    assert attention.action["subject_task_id"] == impl.id
    assert attention.action["needs_attention_reason"] == "reconcile-needs-manual-resolution"
