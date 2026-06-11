"""Tests for shared advance action execution."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from gza.cli._common import _create_retry_task, _materialize_plan_review_slices, resolve_improve_action
from gza.branch_publication import BranchPublicationState, persist_branch_publication_state
from gza.cli.advance_executor import (
    AdvanceActionExecutionContext,
    AdvanceActionExecutionResult,
    BranchDivergenceReconcileResult,
    build_improve_needs_attention_result,
    execute_advance_action,
    resolve_execution_needs_attention,
    run_noop_improve_verify_then_review,
)
from gza.config import Config
from gza.concurrency import launch_permit
from gza.db import Task as DbTask
from gza.git import GitError
from gza.plan_review_materialization import (
    PLAN_REVIEW_MATERIALIZATION_ARTIFACT_KIND,
    build_plan_review_slice_task_specs,
    plan_review_manifest_digest,
)
from gza.recovery_engine import FailedRecoveryDecision, decide_failed_task_recovery
from gza.runner import ReviewVerifyResult
from gza.runner import CrossProjectReviewVerifyResult, ProjectBoundary
from gza.plan_review_verdict import validate_plan_review_manifest

from .conftest import make_store, setup_config


def _mark_completed(task: DbTask, *, branch: str | None = None) -> None:
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    if branch is not None:
        task.branch = branch


def _make_noop_verify_fixture(tmp_path: Path) -> tuple[Any, Any, DbTask, DbTask]:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.slug = "20260605-implement-feature"
    _mark_completed(impl, branch="feature/noop-reverify")
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.slug = "20260605-review-feature"
    _mark_completed(review)
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    store.update(review)

    config = SimpleNamespace(
        worktree_path=tmp_path / "worktrees",
        log_path=tmp_path / "logs",
        project_dir=tmp_path,
        verify_command="uv run pytest tests/ -q",
        review_verify_timeout_seconds=120,
        project_dir_raw=tmp_path,
    )
    config.worktree_path.mkdir(parents=True, exist_ok=True)
    config.log_path.mkdir(parents=True, exist_ok=True)
    return store, config, impl, review


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
    attention = resolve_execution_needs_attention(impl, result)
    assert attention is not None
    assert attention.task.id == impl.id
    assert attention.action["subject_task_id"] == impl.id


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


def test_run_noop_improve_verify_then_review_creates_review_after_green_verify(tmp_path: Path) -> None:
    store, config, impl, review = _make_noop_verify_fixture(tmp_path)

    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=3,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda parent: type(
            "_R", (), {"status": "created", "review_task": store.add("Fresh review", task_type="review", depends_on=parent.id), "message": "Created review"}
        )(),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=SimpleNamespace(
            default_branch=lambda: "main",
            rev_parse_if_exists=lambda _ref: "cafebabe",
            worktree_remove=lambda _path, force=True: None,
        ),
    )

    with patch("gza.cli.advance_executor._create_detached_review_worktree"), \
         patch("gza.cli.advance_executor.Git.rev_parse_if_exists", return_value="deadbeef"), \
         patch(
             "gza.cli.advance_executor._run_review_verify_command",
             return_value=ReviewVerifyResult(
                 command=config.verify_command,
                 status="passed",
                 exit_status="0",
                 captured_at=datetime(2026, 6, 1, 19, 0, tzinfo=UTC),
                 reviewed_branch=impl.branch,
                 reviewed_head_sha="deadbeef",
                 reviewed_base_sha="cafebabe",
             ),
         ):
        outcome = run_noop_improve_verify_then_review(
            task=impl,
            action={"type": "verify_noop_improve_then_review", "review_task": review},
            context=context,
        )

    assert outcome.status == "create_review"
    assert outcome.review_task is not None
    assert outcome.review_task.depends_on == impl.id
    assert "Fresh verify passed" in outcome.message


def test_run_noop_improve_verify_then_review_persists_verify_evidence_before_clearing_verify_only_review(
    tmp_path: Path,
) -> None:
    store, config, impl, review = _make_noop_verify_fixture(tmp_path)
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
    store.update(review)

    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=3,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("fresh review should not run"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=SimpleNamespace(
            default_branch=lambda: "main",
            rev_parse_if_exists=lambda _ref: "cafebabe",
            worktree_remove=lambda _path, force=True: None,
        ),
    )

    captured_at = datetime(2026, 6, 1, 19, 0, tzinfo=UTC)
    with patch("gza.cli.advance_executor._create_detached_review_worktree"), patch(
        "gza.cli.advance_executor.Git.rev_parse_if_exists",
        return_value="deadbeef",
    ), patch(
        "gza.cli.advance_executor._run_review_verify_command",
        return_value=ReviewVerifyResult(
            command=config.verify_command,
            status="passed",
            exit_status="0",
            captured_at=captured_at,
            reviewed_branch=impl.branch,
            reviewed_head_sha="deadbeef",
            reviewed_base_sha="cafebabe",
        ),
    ):
        outcome = run_noop_improve_verify_then_review(
            task=impl,
            action={
                "type": "verify_noop_improve_then_review",
                "review_task": review,
                "current_branch_head_sha": "deadbeef",
            },
            context=context,
        )

    assert outcome.status == "review_cleared"
    refreshed_impl = store.get(impl.id)
    assert refreshed_impl is not None
    assert refreshed_impl.review_cleared_at is not None
    assert refreshed_impl.review_verify_status == "passed"
    assert refreshed_impl.review_verify_branch == impl.branch
    assert refreshed_impl.review_verify_head_sha == "deadbeef"
    assert refreshed_impl.review_verify_captured_at == captured_at
    assert refreshed_impl.review_verify_captured_at > review.completed_at
    assert refreshed_impl.review_verify_artifact_file is None
    artifacts = store.list_artifacts(impl.id, kind="verify_command_output")
    assert len(artifacts) == 1
    assert artifacts[0].producer == "noop_review_verify"
    assert artifacts[0].status == "passed"
    assert artifacts[0].metadata == {
        "reviewed_base_sha": "cafebabe",
        "reviewed_branch": impl.branch,
        "reviewed_head_sha": "deadbeef",
        "triggering_review_task_id": review.id,
        "working_directory": None,
    }


def test_run_noop_improve_verify_then_review_parks_when_worktree_creation_fails(tmp_path: Path) -> None:
    store, config, impl, review = _make_noop_verify_fixture(tmp_path)
    spawn_calls: list[tuple[str, str]] = []
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=3,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("review creation should not run"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda task_obj, kind: spawn_calls.append((str(task_obj.id), kind)) or 0,
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=SimpleNamespace(
            default_branch=lambda: "main",
            rev_parse_if_exists=lambda _ref: "cafebabe",
            worktree_remove=lambda _path, force=True: None,
        ),
    )

    with patch(
        "gza.cli.advance_executor._create_detached_review_worktree",
        side_effect=GitError("cannot create detached worktree"),
    ):
        outcome = run_noop_improve_verify_then_review(
            task=impl,
            action={"type": "verify_noop_improve_then_review", "review_task": review},
            context=context,
        )

        result = execute_advance_action(
            task=impl,
            action={"type": "verify_noop_improve_then_review", "review_task": review},
            context=context,
        )

    assert outcome.status == "needs_attention"
    assert "unable to prepare or run fresh verify_command" in outcome.message
    assert "cannot create detached worktree" in outcome.message
    assert spawn_calls == []

    assert result.status == "skip"
    assert result.attention_type == "needs_discussion"
    assert result.attention_reason == "improve-no-op"
    assert "unable to prepare or run fresh verify_command" in result.message
    assert spawn_calls == []


@pytest.mark.parametrize(
    ("git_obj", "patch_target", "failure_message"),
    [
        (
            SimpleNamespace(
                default_branch=lambda: (_ for _ in ()).throw(GitError("default branch lookup failed")),
                rev_parse_if_exists=lambda _ref: "cafebabe",
                worktree_remove=lambda _path, force=True: None,
            ),
            None,
            "default branch lookup failed",
        ),
        (
            SimpleNamespace(
                default_branch=lambda: "main",
                rev_parse_if_exists=lambda _ref: "cafebabe",
                worktree_remove=lambda _path, force=True: None,
            ),
            "gza.cli.advance_executor._resolve_review_verify_base_sha",
            "base SHA lookup failed",
        ),
    ],
)
def test_run_noop_improve_verify_then_review_parks_when_default_branch_or_base_sha_setup_fails(
    tmp_path: Path,
    git_obj: Any,
    patch_target: str | None,
    failure_message: str,
) -> None:
    store, config, impl, review = _make_noop_verify_fixture(tmp_path)
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=3,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("review creation should not run"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task, _kind: pytest.fail("spawn should not run"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=git_obj,
    )

    if patch_target is None:
        outcome = run_noop_improve_verify_then_review(
            task=impl,
            action={"type": "verify_noop_improve_then_review", "review_task": review},
            context=context,
        )
    else:
        with patch(patch_target, side_effect=GitError(failure_message)):
            outcome = run_noop_improve_verify_then_review(
                task=impl,
                action={"type": "verify_noop_improve_then_review", "review_task": review},
                context=context,
            )

    assert outcome.status == "needs_attention"
    assert "unable to prepare or run fresh verify_command" in outcome.message
    assert failure_message in outcome.message


@pytest.mark.parametrize(
    ("patch_target", "failure_message"),
    [
        ("gza.cli.advance_executor._resolve_review_verify_base_sha", "base SHA lookup failed"),
        ("gza.cli.advance_executor._create_detached_review_worktree", "cannot create detached worktree"),
    ],
)
def test_execute_advance_action_verify_noop_improve_parks_on_reverify_setup_failures_without_spawning(
    tmp_path: Path,
    patch_target: str,
    failure_message: str,
) -> None:
    store, config, impl, review = _make_noop_verify_fixture(tmp_path)
    spawn_calls: list[tuple[str, str]] = []
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=3,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("review creation should not run"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda task_obj, kind: spawn_calls.append((str(task_obj.id), kind)) or 0,
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=SimpleNamespace(
            default_branch=lambda: "main",
            rev_parse_if_exists=lambda _ref: "cafebabe",
            worktree_remove=lambda _path, force=True: None,
        ),
    )

    with patch(patch_target, side_effect=GitError(failure_message)):
        result = execute_advance_action(
            task=impl,
            action={"type": "verify_noop_improve_then_review", "review_task": review},
            context=context,
        )

    assert result.status == "skip"
    assert result.attention_type == "needs_discussion"
    assert result.attention_reason == "improve-no-op"
    assert "unable to prepare or run fresh verify_command" in result.message
    assert failure_message in result.message
    assert spawn_calls == []


def test_execute_advance_action_verify_noop_improve_parks_when_default_branch_resolution_fails(
    tmp_path: Path,
) -> None:
    store, config, impl, review = _make_noop_verify_fixture(tmp_path)
    spawn_calls: list[tuple[str, str]] = []
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=3,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("review creation should not run"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda task_obj, kind: spawn_calls.append((str(task_obj.id), kind)) or 0,
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=SimpleNamespace(
            default_branch=lambda: (_ for _ in ()).throw(GitError("default branch lookup failed")),
            rev_parse_if_exists=lambda _ref: "cafebabe",
            worktree_remove=lambda _path, force=True: None,
        ),
    )

    result = execute_advance_action(
        task=impl,
        action={"type": "verify_noop_improve_then_review", "review_task": review},
        context=context,
    )

    assert result.status == "skip"
    assert result.attention_type == "needs_discussion"
    assert result.attention_reason == "improve-no-op"
    assert "unable to prepare or run fresh verify_command" in result.message
    assert "default branch lookup failed" in result.message
    assert spawn_calls == []


@pytest.mark.parametrize(
    ("head_side_effect", "expected_fragment"),
    [
        (None, "unable to resolve review worktree HEAD before verify_command ran"),
        (GitError("HEAD lookup failed"), "unable to prepare or run fresh verify_command: HEAD lookup failed"),
    ],
)
def test_run_noop_improve_verify_then_review_parks_when_head_resolution_is_missing_or_raises(
    tmp_path: Path,
    head_side_effect: str | Exception | None,
    expected_fragment: str,
) -> None:
    store, config, impl, review = _make_noop_verify_fixture(tmp_path)
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=3,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("review creation should not run"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task, _kind: pytest.fail("spawn should not run"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=SimpleNamespace(
            default_branch=lambda: "main",
            rev_parse_if_exists=lambda _ref: "cafebabe",
            worktree_remove=lambda _path, force=True: None,
        ),
    )

    patch_kwargs = {"return_value": head_side_effect} if head_side_effect is None else {"side_effect": head_side_effect}
    with patch("gza.cli.advance_executor._create_detached_review_worktree"), patch(
        "gza.cli.advance_executor.Git.rev_parse_if_exists",
        **patch_kwargs,
    ):
        outcome = run_noop_improve_verify_then_review(
            task=impl,
            action={"type": "verify_noop_improve_then_review", "review_task": review},
            context=context,
        )

    assert outcome.status == "needs_attention"
    assert expected_fragment in outcome.message


@pytest.mark.parametrize(
    ("patch_target", "failure"),
    [
        ("gza.cli.advance_executor._worktree_execution_dir", RuntimeError("execution dir unavailable")),
        ("gza.cli.advance_executor._run_review_verify_command", RuntimeError("verify runner crashed")),
    ],
)
def test_run_noop_improve_verify_then_review_parks_when_verify_setup_or_runner_raises(
    tmp_path: Path,
    patch_target: str,
    failure: Exception,
) -> None:
    store, config, impl, review = _make_noop_verify_fixture(tmp_path)
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=3,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("review creation should not run"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task, _kind: pytest.fail("spawn should not run"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=SimpleNamespace(
            default_branch=lambda: "main",
            rev_parse_if_exists=lambda _ref: "cafebabe",
            worktree_remove=lambda _path, force=True: None,
        ),
    )

    with patch("gza.cli.advance_executor._create_detached_review_worktree"), patch(
        "gza.cli.advance_executor.Git.rev_parse_if_exists",
        return_value="deadbeef",
    ), patch(patch_target, side_effect=failure):
        outcome = run_noop_improve_verify_then_review(
            task=impl,
            action={"type": "verify_noop_improve_then_review", "review_task": review},
            context=context,
        )

    assert outcome.status == "needs_attention"
    assert "unable to prepare or run fresh verify_command" in outcome.message
    assert str(failure) in outcome.message


def test_run_noop_improve_verify_then_review_preserves_original_failure_when_cleanup_also_fails(tmp_path: Path) -> None:
    store, config, impl, review = _make_noop_verify_fixture(tmp_path)
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=3,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("review creation should not run"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task, _kind: pytest.fail("spawn should not run"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=SimpleNamespace(
            default_branch=lambda: "main",
            rev_parse_if_exists=lambda _ref: "cafebabe",
            worktree_remove=lambda _path, force=True: (_ for _ in ()).throw(GitError("cleanup exploded")),
        ),
    )

    with patch("gza.cli.advance_executor._create_detached_review_worktree"), patch(
        "gza.cli.advance_executor.Git.rev_parse_if_exists",
        return_value="deadbeef",
    ), patch(
        "gza.cli.advance_executor._run_review_verify_command",
        side_effect=RuntimeError("verify runner crashed"),
    ):
        outcome = run_noop_improve_verify_then_review(
            task=impl,
            action={"type": "verify_noop_improve_then_review", "review_task": review},
            context=context,
        )

    assert outcome.status == "needs_attention"
    assert "verify runner crashed" in outcome.message
    assert "Cleanup also failed: cleanup exploded" in outcome.message


def test_run_noop_improve_verify_then_review_parks_when_cleanup_fails_after_green_verify(tmp_path: Path) -> None:
    store, config, impl, review = _make_noop_verify_fixture(tmp_path)
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=3,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("review creation should not run"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task, _kind: pytest.fail("spawn should not run"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=SimpleNamespace(
            default_branch=lambda: "main",
            rev_parse_if_exists=lambda _ref: "cafebabe",
            worktree_remove=lambda _path, force=True: (_ for _ in ()).throw(GitError("cleanup exploded")),
        ),
    )

    with patch("gza.cli.advance_executor._create_detached_review_worktree"), patch(
        "gza.cli.advance_executor.Git.rev_parse_if_exists",
        return_value="deadbeef",
    ), patch(
        "gza.cli.advance_executor._run_review_verify_command",
        return_value=ReviewVerifyResult(
            command=config.verify_command,
            status="passed",
            exit_status="0",
            captured_at=datetime(2026, 6, 1, 19, 0, tzinfo=UTC),
            reviewed_branch=impl.branch,
            reviewed_head_sha="deadbeef",
            reviewed_base_sha="cafebabe",
        ),
    ):
        outcome = run_noop_improve_verify_then_review(
            task=impl,
            action={"type": "verify_noop_improve_then_review", "review_task": review},
            context=context,
        )

    assert outcome.status == "needs_attention"
    assert "cleanup failed: cleanup exploded" in outcome.message


def test_run_noop_improve_verify_then_review_uses_cross_project_review_verifier(tmp_path: Path) -> None:
    store, config, impl, review = _make_noop_verify_fixture(tmp_path)
    impl.tags = ("cross-project",)
    store.update(impl)
    config.verify_command = ""
    setattr(
        config,
        "_project_boundary_cache",
        ProjectBoundary(
            repo_root=tmp_path,
            scope_root=Path("."),
            local_dependencies=(),
        ),
    )

    fresh_review = store.add("Fresh review", task_type="review", depends_on=impl.id)
    assert fresh_review.id is not None
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=3,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: type(
            "_R",
            (),
            {"status": "created", "review_task": fresh_review, "message": "created"},
        )(),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task, _kind: pytest.fail("spawn should not run"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=SimpleNamespace(
            default_branch=lambda: "main",
            rev_parse_if_exists=lambda _ref: "cafebabe",
            worktree_remove=lambda _path, force=True: None,
        ),
    )
    aggregate = ReviewVerifyResult(
        command="(per-project verify_command)",
        status="passed",
        exit_status="2 passed, 0 failed, 0 unavailable",
        captured_at=datetime(2026, 6, 1, 19, 0, tzinfo=UTC),
        reviewed_branch=impl.branch,
        reviewed_head_sha="deadbeef",
        reviewed_base_sha="cafebabe",
    )

    with patch("gza.cli.advance_executor._create_detached_review_worktree"), patch(
        "gza.cli.advance_executor.Git.rev_parse_if_exists",
        return_value="deadbeef",
    ), patch(
        "gza.cli.advance_executor._run_review_verify_commands_for_projects",
        return_value=CrossProjectReviewVerifyResult(
            markdown="## verify_command result\n\n### services/foo\n\n- Status: passed\n",
            aggregate_result=aggregate,
            project_results=(),
        ),
    ) as cross_project_verify, patch(
        "gza.cli.advance_executor._run_review_verify_command",
        side_effect=AssertionError("root verify runner should not be used for cross-project no-op reverify"),
    ):
        outcome = run_noop_improve_verify_then_review(
            task=impl,
            action={"type": "verify_noop_improve_then_review", "review_task": review},
            context=context,
        )

    assert outcome.status == "create_review"
    assert outcome.review_task is fresh_review
    assert cross_project_verify.call_count == 1
    assert "Fresh verify passed" in outcome.message


@pytest.mark.parametrize(
    ("status", "exit_status", "failure"),
    [
        ("failed", "1 passed, 1 failed, 0 unavailable", "one or more affected projects failed review verification"),
        ("unavailable", "0 passed, 0 failed, 0 unavailable, 1 skipped", "one or more affected projects could not run review verification"),
    ],
)
def test_run_noop_improve_verify_then_review_parks_on_cross_project_aggregate_failure_or_unavailable(
    tmp_path: Path,
    status: str,
    exit_status: str,
    failure: str,
) -> None:
    store, config, impl, review = _make_noop_verify_fixture(tmp_path)
    impl.tags = ("cross-project",)
    store.update(impl)
    config.verify_command = ""
    setattr(
        config,
        "_project_boundary_cache",
        ProjectBoundary(
            repo_root=tmp_path,
            scope_root=Path("."),
            local_dependencies=(),
        ),
    )
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=3,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("review creation should not run"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task, _kind: pytest.fail("spawn should not run"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=SimpleNamespace(
            default_branch=lambda: "main",
            rev_parse_if_exists=lambda _ref: "cafebabe",
            worktree_remove=lambda _path, force=True: None,
        ),
    )
    aggregate = ReviewVerifyResult(
        command="(per-project verify_command)",
        status=status,
        exit_status=exit_status,
        captured_at=datetime(2026, 6, 1, 19, 0, tzinfo=UTC),
        reviewed_branch=impl.branch,
        reviewed_head_sha="deadbeef",
        reviewed_base_sha="cafebabe",
        failure=failure,
    )

    with patch("gza.cli.advance_executor._create_detached_review_worktree"), patch(
        "gza.cli.advance_executor.Git.rev_parse_if_exists",
        return_value="deadbeef",
    ), patch(
        "gza.cli.advance_executor._run_review_verify_commands_for_projects",
        return_value=CrossProjectReviewVerifyResult(
            markdown="## verify_command result\n\n### dre/web\n\n- Status: skipped\n",
            aggregate_result=aggregate,
            project_results=(),
        ),
    ), patch(
        "gza.cli.advance_executor._run_review_verify_command",
        side_effect=AssertionError("root verify runner should not be used for cross-project no-op reverify"),
    ):
        outcome = run_noop_improve_verify_then_review(
            task=impl,
            action={"type": "verify_noop_improve_then_review", "review_task": review},
            context=context,
        )

    assert outcome.status == "needs_attention"
    assert failure in outcome.message
    assert outcome.verify_markdown is not None


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


def test_verify_noop_improve_then_review_can_route_through_iterate_before_running_reverify(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/noop-reverify-iterate")
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    store.update(review)

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
        config=SimpleNamespace(),
        git=SimpleNamespace(),
    )

    with patch(
        "gza.cli.advance_executor.run_noop_improve_verify_then_review",
        side_effect=AssertionError("parent executor should route through iterate before reverify"),
    ):
        result = execute_advance_action(
            task=impl,
            action={"type": "verify_noop_improve_then_review", "review_task": review},
            context=context,
        )

    assert result.status == "success"
    assert result.handled_task_id == impl.id
    assert result.worker_label == "iterate"
    assert result.guarded_pending_task_id is None
    assert spawned == [(impl.id, "iterate")]


def test_verify_noop_improve_then_review_skips_at_max_concurrent_without_creating_review(
    tmp_path: Path,
) -> None:
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
    _mark_completed(impl, branch="feature/noop-capacity")
    store.update(impl)

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    store.update(review)

    before_ids = {task.id for task in store.get_all()}
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=1,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("review creation should not run at max concurrent"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task, _kind: pytest.fail("spawn should not run"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=SimpleNamespace(),
    )

    def _fake_reverify(*, task: DbTask, action: dict[str, Any], context: AdvanceActionExecutionContext):
        del action
        create_result = context.prepare_create_review(task)
        if create_result.status != "created":
            return SimpleNamespace(
                status="error",
                message=create_result.message,
                verify_markdown="## verify_command result\n\nPassed\n",
                review_task=None,
            )
        return SimpleNamespace(
            status="create_review",
            message=create_result.message,
            verify_markdown="## verify_command result\n\nPassed\n",
            review_task=create_result.review_task,
        )

    with patch("gza.cli.advance_executor.run_noop_improve_verify_then_review", side_effect=_fake_reverify):
        result = execute_advance_action(
            task=impl,
            action={"type": "verify_noop_improve_then_review", "review_task": review},
            context=context,
        )

    assert result.status == "error"
    assert result.message == "SKIP: already at max concurrent tasks: 1 running, limit is 1"
    assert {task.id for task in store.get_all()} == before_ids


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

    task = store.add("Implement feature", task_type="implement")
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
            rebase_target="origin/feature/reconcile-conflict",
        ),
    )

    result = execute_advance_action(
        task=impl,
        action={"type": "reconcile_branch_divergence"},
        context=context,
    )

    assert result.status == "success"
    assert captured["target"] == "origin/feature/reconcile-conflict"
    assert result.success_message.startswith("Created rebase task ")


def test_reconcile_branch_divergence_conflict_against_origin_returns_needs_attention(tmp_path: Path) -> None:
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
                "SKIP: mechanical rebase onto 'origin/feature/reconcile-origin-conflict' hit conflicts: "
                "conflict. Resolve the origin divergence manually; the sandboxed rebase worker cannot access "
                "that remote-tracking ref."
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
