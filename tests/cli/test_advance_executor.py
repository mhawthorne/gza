"""Tests for shared advance action execution."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest

from gza.advance_engine import (
    NOOP_IMPROVE_KIND_VERIFY_ONLY,
    REVIEW_CLEARANCE_ARTIFACT_KIND,
    VERIFY_ONLY_NOOP_RECOVERY_ATTENTION_ARTIFACT_KIND,
    VERIFY_ONLY_NOOP_RECOVERY_ATTENTION_STATUS,
    VERIFY_ONLY_NOOP_REVIEW_CLEARANCE_KIND,
    evaluate_advance_rules,
)
from gza.artifacts import store_command_output_artifact
from gza.branch_publication import BranchPublicationState, persist_branch_publication_state
from gza.cli._common import (
    PLAN_REVIEW_MATERIALIZATION_AUTO_REPAIR_DROP_REASON,
    _create_rebase_task,
    _create_retry_task,
    _materialize_plan_review_slices,
    _repair_plan_review_slice_materialization,
    resolve_improve_action,
)
from gza.cli.advance_executor import (
    _WORKER_ACTIONS,
    ITERATE_ROUTABLE_ACTIONS,
    AdvanceActionExecutionContext,
    AdvanceActionExecutionResult,
    BranchDivergenceReconcileResult,
    _prepare_resolution_review_action,
    _prepare_spec_coherence_review_action,
    _resolve_canonical_verify_gate_owner,
    _resolve_verify_gate_subject_task,
    build_improve_needs_attention_result,
    execute_advance_action,
    resolve_execution_needs_attention,
)
from gza.concurrency import launch_permit
from gza.config import Config, ConfigError
from gza.db import DuplicateActiveChildError, SqliteTaskStore, Task as DbTask
from gza.flaky_investigations import (
    FlakyInvestigationEvidence,
    build_flaky_reproduction_plan,
    normalize_flaky_investigation_dedup_key,
)
from gza.git import Git, GitError, ResolvedMergeSourceRef
from gza.log_paths import ops_log_path_for
from gza.off_topic_verify import FailingNode, PytestPassFailCounts, PytestXdistMetadata
from gza.pickup import count_worker_consuming_actions, is_worker_consuming_advance_action
from gza.plan_review_materialization import (
    PLAN_REVIEW_ARTIFACT_SCHEMA_VERSION,
    PLAN_REVIEW_MATERIALIZATION_ARTIFACT_KIND,
    build_plan_review_slice_task_specs,
    plan_review_manifest_digest,
)
from gza.plan_review_verdict import validate_plan_review_manifest
from gza.recovery_engine import FailedRecoveryDecision, decide_failed_task_recovery
from gza.review_tasks import OffTopicVerifyPersistenceError, build_verify_fix_prompt, create_or_reuse_verify_fix_task
from gza.review_verdict import ReviewFinding
from gza.review_verify_state import (
    VERIFY_GATE_ARTIFACT_KIND,
    VerifyEpoch,
    VerifyGateResult,
    latest_verify_result_for_epoch,
    owner_task_verify_epoch,
    persist_recredited_verify_gate_artifact,
    persist_verify_gate_artifact,
)
from gza.runner import (
    CROSS_PROJECT_TAG,
    LifecycleVerifyBudgetError,
    LifecycleVerifyExecution,
    _format_review_verify_result,
    _make_review_verify_result,
)
from gza.runtime_context import RuntimeExecutionContext
from gza.verify_fix_outcome import effective_verify_fix_completion_outcome

from .conftest import make_store, setup_config


def _mark_completed(task: DbTask, *, branch: str | None = None) -> None:
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    if branch is not None:
        task.branch = branch


class _PlanRepairFakeGit:
    def can_merge(self, _source_branch: str, _target_branch: str) -> bool:
        return True


def _build_plan_review_manifest_payload(source_task_id: str, *, source_task_type: str = "plan") -> dict[str, Any]:
    return {
        "schema_version": 1,
        "source_task_id": source_task_id,
        "source_task_type": source_task_type,
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
                "tags": ["slice"],
            },
            {
                "slice_id": "S2",
                "title": "Follow-up",
                "prompt": "Create the second slice.",
                "scope": ["Two"],
                "out_of_scope": [],
                "acceptance_criteria": ["Second slice exists"],
                "depends_on_slices": ["S1"],
                "based_on_slice": "S1",
                "review_scope": "Follow-up only.",
                "estimated_complexity": "small",
                "expected_timeout_minutes": 30,
                "requires_code_review": True,
                "tags": ["slice"],
            },
        ],
    }


def _build_plan_review_repair_context(
    *,
    config: Config,
    store: SqliteTaskStore,
) -> AdvanceActionExecutionContext:
    return AdvanceActionExecutionContext(
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
        repair_plan_slice_materialization=lambda source_task, plan_review_task, repair_manifest, repair_task_ids, repair_trigger_source: (
            _repair_plan_review_slice_materialization(
                config,
                store,
                source_task,
                plan_review_task,
                repair_manifest,
                partial_task_ids=repair_task_ids,
                trigger_source=repair_trigger_source,
                require_review_before_merge=True,
            )
        ),
        config=config,
    )


def _setup_plan_review_repair_candidate(
    *,
    tmp_path: Path,
) -> tuple[Config, SqliteTaskStore, DbTask, DbTask, Any, list[Any], DbTask]:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan lifecycle slices", task_type="plan")
    assert plan.id is not None
    _mark_completed(plan)
    store.update(plan)

    review = store.add("Review plan lifecycle slices", task_type="plan_review", depends_on=plan.id)
    assert review.id is not None
    _mark_completed(review)
    manifest_payload = _build_plan_review_manifest_payload(plan.id)
    review.output_content = (
        "## Verdict\nVerdict: APPROVED\n\n## Slice Manifest\n```json\n"
        + json.dumps(manifest_payload)
        + "\n```\n"
    )
    store.update(review)

    manifest = validate_plan_review_manifest(
        manifest_payload,
        markdown_verdict="APPROVED",
        source_task_id=plan.id,
        source_task_type="plan",
        max_slice_timeout_minutes=30,
    )
    task_specs = build_plan_review_slice_task_specs(
        plan_source_task=plan,
        review_task=review,
        manifest=manifest,
        trigger_source="plan-review",
        require_review_before_merge=True,
    )
    partial = store.add(
        task_specs[0].prompt,
        task_type="implement",
        based_on=plan.id,
        trigger_source="plan-review",
        tags=task_specs[0].tags,
        review_scope=task_specs[0].review_scope,
        create_review=task_specs[0].create_review,
    )
    assert partial.id is not None
    return config, store, plan, review, manifest, task_specs, partial


def _add_plan_review_materialization_artifact(
    store: SqliteTaskStore,
    *,
    review_id: str,
    metadata: dict[str, Any] | None,
) -> None:
    artifacts = store.list_artifacts(review_id, kind=PLAN_REVIEW_MATERIALIZATION_ARTIFACT_KIND)
    artifact_id = artifacts[0].id if artifacts else None
    store.add_artifact(
        review_id,
        kind=PLAN_REVIEW_MATERIALIZATION_ARTIFACT_KIND,
        label="plan_review_materialization",
        path=".gza/artifacts/materialized.txt",
        byte_size=0,
        sha256="",
        metadata=metadata,
        artifact_id=artifact_id,
    )


def _mutate_materialized_task_tags(
    store: SqliteTaskStore,
    task: DbTask,
) -> None:
    task.tags = ("mutated-tag",)
    store.update(task)


def _base_executor_context(
    *,
    store: SqliteTaskStore,
    config: Config,
    **overrides: Any,
) -> AdvanceActionExecutionContext:
    values: dict[str, Any] = {
        "store": store,
        "trigger_source": "manual",
        "dry_run": False,
        "max_resume_attempts": 3,
        "use_iterate_for_create_implement": False,
        "use_iterate_for_needs_rebase": False,
        "prepare_task_for_background_start": lambda task, _rollback: task,
        "prepare_create_review": lambda _task: pytest.fail("unused"),
        "create_resume_task": lambda _task: pytest.fail("unused"),
        "create_rebase_task": lambda _task: pytest.fail("unused"),
        "create_implement_task": lambda _task: pytest.fail("unused"),
        "spawn_worker": lambda _task, _kind: pytest.fail("unused"),
        "spawn_resume_worker": lambda _task, _kind: pytest.fail("unused"),
        "spawn_iterate_worker": lambda _task, _kind: pytest.fail("unused"),
        "config": config,
    }
    values.update(overrides)
    return AdvanceActionExecutionContext(**values)


def _assert_permit_released(config: Config, store: SqliteTaskStore) -> None:
    permit = launch_permit(config, store)
    permit.release()


def test_run_review_rejects_selected_head_mismatch(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.branch = "feature/live-head-review"
    store.update(impl)
    review = store.add("Review feature", task_type="review", depends_on=impl.id, based_on=impl.id)
    assert review.id is not None
    review.status = "pending"
    review.review_verify_head_sha = "stale-head"
    store.update(review)

    context = _base_executor_context(
        store=store,
        config=config,
        spawn_worker=lambda _task, _kind: pytest.fail("stale selected review must not launch"),
    )

    result = execute_advance_action(
        task=impl,
        action={"type": "run_review", "review_task": review, "review_head_sha": "live-head"},
        context=context,
    )

    assert result.status == "error"
    assert "pending review head does not match selected live head" in result.message
    reloaded_review = store.get(review.id)
    assert reloaded_review is not None
    assert reloaded_review.review_verify_head_sha == "stale-head"


@pytest.mark.parametrize(
    ("action_type", "context_overrides", "action_extra", "patch_target"),
    [
        pytest.param("create_verify_fix", {}, {}, "gza.cli.advance_executor.create_or_reuse_verify_fix_task", id="verify-fix"),
        pytest.param("create_review", {}, {}, None, id="ordinary-review"),
        pytest.param(
            "create_review",
            {},
            {"review_mode": "resolution"},
            "gza.cli.advance_executor._prepare_resolution_review_action",
            id="resolution-review",
        ),
        pytest.param(
            "create_review",
            {},
            {"review_mode": "spec_coherence"},
            "gza.cli.advance_executor._prepare_spec_coherence_review_action",
            id="spec-coherence-review",
        ),
        pytest.param("create_review_adjudication", {}, {}, None, id="adjudication"),
        pytest.param("improve", {}, {"improve_mode": "new"}, "gza.cli.advance_executor._create_improve_task", id="improve-new"),
        pytest.param("improve", {}, {"improve_mode": "resume"}, None, id="improve-resume"),
        pytest.param("improve", {}, {"improve_mode": "retry"}, None, id="improve-retry"),
    ],
)
def test_creation_time_config_errors_return_executor_error_and_release_permit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    action_type: str,
    context_overrides: dict[str, Any],
    action_extra: dict[str, Any],
    patch_target: str | None,
) -> None:
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\nprovider: codex\nmodel: gpt-5.5\nmax_concurrent: 1\n",
        encoding="utf-8",
    )
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    impl = store.add("Implementation", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/config-error")
    store.update(impl)
    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    store.update(review)
    before_ids = {task.id for task in store.get_all()}
    exc = ConfigError("task type 'implement' with provider 'codex' has no model")

    failed_improve: DbTask | None = None
    if action_extra.get("improve_mode") in {"resume", "retry"}:
        failed_improve = store.add("Improve failed", task_type="improve", depends_on=review.id, based_on=impl.id)
        assert failed_improve.id is not None
        failed_improve.status = "failed"
        failed_improve.failure_reason = "MAX_TURNS" if action_extra["improve_mode"] == "resume" else "INFRASTRUCTURE_ERROR"
        failed_improve.session_id = "sess-1" if action_extra["improve_mode"] == "resume" else None
        failed_improve.completed_at = datetime.now(UTC)
        store.update(failed_improve)
        before_ids.add(failed_improve.id)

    def _raise_config_error(*_args: Any, **_kwargs: Any) -> Any:
        raise exc

    context_kwargs: dict[str, Any] = dict(context_overrides)
    if action_type == "create_review":
        context_kwargs["prepare_create_review"] = _raise_config_error
    elif action_type == "create_review_adjudication":
        context_kwargs["create_review_adjudication_task"] = _raise_config_error
    elif action_type == "improve":
        context_kwargs["create_resume_task"] = _raise_config_error
        context_kwargs["create_retry_task"] = _raise_config_error

    context = _base_executor_context(store=store, config=config, **context_kwargs)
    action: dict[str, Any] = {"type": action_type, "review_task": review, **action_extra}
    if action_type == "create_verify_fix":
        action.update(
            {
                "impl_task": impl,
                "based_on_task": impl,
                "verify_epoch": VerifyEpoch(
                    reviewed_branch=impl.branch,
                    reviewed_head_sha="abc123",
                    verify_command="./bin/tests",
                    verify_timeout_seconds=300,
                    verify_timeout_grace_seconds=5.0,
                ),
            }
        )
    if action_type == "create_review_adjudication":
        action["review_blocker_adjudication_candidate"] = SimpleNamespace(
            finding=ReviewFinding(
                id="B1",
                severity="BLOCKER",
                title="Missing guard",
                body="Evidence",
                evidence="Evidence",
                impact="Impact",
                fix_or_followup="Fix",
                tests="Tests",
            ),
            dispute_artifact=SimpleNamespace(id=1, metadata={}),
        )

    if patch_target is None:
        result = execute_advance_action(task=impl, action=action, context=context)
    else:
        monkeypatch.setattr(patch_target, _raise_config_error)
        result = execute_advance_action(task=impl, action=action, context=context)

    assert result.status == "error"
    assert "task type 'implement' with provider 'codex'" in result.message
    assert {task.id for task in store.get_all()} == before_ids
    _assert_permit_released(config, store)


@pytest.mark.parametrize("action_type", ["materialize_plan_slices", "repair_plan_slice_materialization"])
def test_plan_materialization_config_errors_return_executor_error_without_mutation(
    tmp_path: Path,
    action_type: str,
) -> None:
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "provider: codex\n"
        "providers:\n"
        "  codex:\n"
        "    task_types:\n"
        "      plan:\n"
        "        model: gpt-5.5\n",
        encoding="utf-8",
    )
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    plan = store.add("Plan lifecycle slices", task_type="plan")
    assert plan.id is not None
    _mark_completed(plan)
    store.update(plan)
    review = store.add("Review plan lifecycle slices", task_type="plan_review", depends_on=plan.id)
    assert review.id is not None
    _mark_completed(review)
    manifest_payload = _build_plan_review_manifest_payload(plan.id)
    review.output_content = (
        "## Verdict\nVerdict: APPROVED\n\n## Slice Manifest\n```json\n"
        + json.dumps(manifest_payload)
        + "\n```\n"
    )
    store.update(review)
    manifest = validate_plan_review_manifest(
        manifest_payload,
        markdown_verdict="APPROVED",
        source_task_id=plan.id,
        source_task_type="plan",
        max_slice_timeout_minutes=30,
    )
    task_specs = build_plan_review_slice_task_specs(
        plan_source_task=plan,
        review_task=review,
        manifest=manifest,
        trigger_source="plan-review",
        require_review_before_merge=True,
    )
    partial = store.add(
        task_specs[0].prompt,
        task_type="implement",
        based_on=plan.id,
        trigger_source="plan-review",
        tags=task_specs[0].tags,
        review_scope=task_specs[0].review_scope,
        create_review=task_specs[0].create_review,
    )
    assert partial.id is not None
    partial.status = "pending"
    store.update(partial)
    before_status = store.get(partial.id).status

    context = _base_executor_context(
        store=store,
        config=config,
        materialize_plan_slices=lambda source, review_task, materialization_manifest: _materialize_plan_review_slices(
            config,
            store,
            source,
            review_task,
            materialization_manifest,
            trigger_source="plan-review",
            require_review_before_merge=True,
        ),
        repair_plan_slice_materialization=lambda source, review_task, repair_manifest, partial_ids, repair_trigger_source: (
            _repair_plan_review_slice_materialization(
                config,
                store,
                source,
                review_task,
                repair_manifest,
                partial_task_ids=partial_ids,
                trigger_source=repair_trigger_source,
                require_review_before_merge=True,
            )
        ),
    )
    action: dict[str, Any] = {
        "type": action_type,
        "plan_source_task": plan,
        "plan_review_task": review,
        "manifest": manifest,
    }
    if action_type == "repair_plan_slice_materialization":
        action.update({"partial_task_ids": (partial.id,), "repair_trigger_source": "plan-review"})

    result = execute_advance_action(task=plan, action=action, context=context)

    assert result.status == "error"
    assert "task type 'implement' with provider 'codex'" in result.message
    assert not any(task.id != partial.id and task.task_type == "implement" for task in store.get_all())
    assert store.get(partial.id).status == before_status


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
        runtime_context=RuntimeExecutionContext.from_config(config),
    )

    with (
        patch("gza.cli.advance_executor.Git", side_effect=lambda path, **_kwargs: SimpleNamespace(repo_dir=Path(path), default_branch=lambda: "main", rev_parse_if_exists=lambda ref: "same-head")),
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
    verify_gate_artifacts = store.list_artifacts(impl.id, kind=VERIFY_GATE_ARTIFACT_KIND)

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
    assert len(verify_gate_artifacts) == 1
    assert store.list_artifacts(improve.id, kind=VERIFY_GATE_ARTIFACT_KIND) == []

    lookup = latest_verify_result_for_epoch(
        store,
        refreshed_impl,
        current_epoch=owner_task_verify_epoch(refreshed_impl, config, git),
    )
    assert lookup.source == "owner_artifact"
    assert lookup.is_current is True
    assert lookup.result is not None
    assert lookup.result.reviewed_head_sha == "same-head"


def test_recover_verify_only_noop_review_uses_runtime_env_for_direct_verify(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "uv run pytest tests/unit -q"
    selected_db = tmp_path / ".gza" / "selected.db"
    runtime_context = RuntimeExecutionContext(
        cwd=tmp_path,
        env={
            "PATH": "/selected/bin",
            "PWD": "/stale-runtime-pwd",
            "GZA_DB_PATH": str(selected_db),
            "PROJECT_ONLY_TOKEN": "selected-token",
            "GIT_DIR": "/selected-must-be-stripped",
        },
        project_id=config.project_id,
        db_path=selected_db,
    )
    supervisor_cwd = tmp_path / "supervisor"
    supervisor_cwd.mkdir()
    monkeypatch.chdir(supervisor_cwd)
    monkeypatch.setenv("PWD", str(supervisor_cwd))
    monkeypatch.setenv("GZA_DB_PATH", str(tmp_path / "ambient.db"))
    monkeypatch.setenv("PROJECT_ONLY_TOKEN", "ambient-token")
    monkeypatch.setenv("GIT_DIR", "/ambient-git-dir")

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/verify-only-noop-runtime-env")
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
    context = _base_executor_context(
        store=store,
        config=config,
        trigger_source="advance",
        git=git,
        runtime_context=runtime_context,
    )
    observed_git_envs: list[dict[str, str] | None] = []

    def fake_git(path: Path, **kwargs: Any) -> SimpleNamespace:
        observed_git_envs.append(kwargs.get("env"))
        return SimpleNamespace(
            repo_dir=Path(path),
            default_branch=lambda: "main",
            rev_parse_if_exists=lambda ref: "same-head",
        )

    with (
        patch("gza.cli.advance_executor.Git", side_effect=fake_git),
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
        ) as run_verify,
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

    assert result.status == "success"
    assert observed_git_envs == [runtime_context.env]
    verify_env = run_verify.call_args.kwargs["env"]
    provider_cwd = Path(run_verify.call_args.kwargs["cwd"])
    assert verify_env["PATH"] == "/selected/bin"
    assert verify_env["PWD"] == str(provider_cwd.resolve())
    assert verify_env["GZA_DB_PATH"] == str(selected_db)
    assert verify_env["PROJECT_ONLY_TOKEN"] == "selected-token"
    assert "GIT_DIR" not in verify_env
    assert os.environ["PWD"] == str(supervisor_cwd)
    assert os.environ["GZA_DB_PATH"] == str(tmp_path / "ambient.db")
    assert os.environ["PROJECT_ONLY_TOKEN"] == "ambient-token"
    assert os.environ["GIT_DIR"] == "/ambient-git-dir"


def test_recover_verify_only_noop_review_passes_runtime_context_to_cross_project_verify(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "uv run pytest tests/unit -q"
    runtime_context = RuntimeExecutionContext(
        cwd=tmp_path,
        env={
            "PATH": "/selected/bin",
            "PWD": "/stale-runtime-pwd",
            "GZA_DB_PATH": str(tmp_path / ".gza" / "selected-cross.db"),
            "PROJECT_ONLY_TOKEN": "selected-cross-token",
        },
        project_id=config.project_id,
        db_path=tmp_path / ".gza" / "selected-cross.db",
    )
    supervisor_cwd = tmp_path / "supervisor"
    supervisor_cwd.mkdir()
    monkeypatch.chdir(supervisor_cwd)
    monkeypatch.setenv("PWD", str(supervisor_cwd))
    monkeypatch.setenv("GZA_DB_PATH", str(tmp_path / "ambient-cross.db"))
    monkeypatch.setenv("PROJECT_ONLY_TOKEN", "ambient-cross-token")

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/verify-only-noop-cross-runtime")
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
    improve.tags = (CROSS_PROJECT_TAG,)
    store.update(improve)

    git = _VerifyOnlyNoopGit(impl.branch or "", "same-head")
    context = _base_executor_context(
        store=store,
        config=config,
        trigger_source="advance",
        git=git,
        runtime_context=runtime_context,
    )
    cross_result = _make_review_verify_result(
        "(per-project verify_command)",
        status="passed",
        exit_status="1 passed",
        captured_at=datetime(2026, 6, 27, 12, 0, tzinfo=UTC),
        reviewed_branch=impl.branch,
        reviewed_head_sha="same-head",
        reviewed_base_sha="base-sha",
        working_directory="(per-project; see artifact)",
    )
    from gza.runner import CrossProjectReviewVerifyResult

    with (
        patch(
            "gza.cli.advance_executor.Git",
            side_effect=lambda path, **kwargs: SimpleNamespace(
                repo_dir=Path(path),
                default_branch=lambda: "main",
                rev_parse_if_exists=lambda ref: "same-head",
            ),
        ),
        patch("gza.cli.advance_executor._resolve_review_verify_base_sha", return_value="base-sha"),
        patch(
            "gza.cli.advance_executor._run_review_verify_commands_for_projects",
            return_value=CrossProjectReviewVerifyResult(
                markdown="## verify_command result\n\n- Status: passed",
                aggregate_result=cross_result,
                project_results=(),
            ),
        ) as cross_verify,
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

    assert result.status == "success"
    assert cross_verify.call_args.kwargs["runtime_context"] is runtime_context
    assert os.environ["PWD"] == str(supervisor_cwd)
    assert os.environ["GZA_DB_PATH"] == str(tmp_path / "ambient-cross.db")
    assert os.environ["PROJECT_ONLY_TOKEN"] == "ambient-cross-token"


def test_recover_verify_only_noop_review_uses_child_project_verify_budgets(
    tmp_path: Path,
) -> None:
    (tmp_path / "gza.yaml").write_text(
        "project_name: root\n"
        "provider: codex\n"
        "model: gpt-5.5\n"
        "verify_command: ./bin/root-verify\n"
        "autonomous_verify_timeout_seconds: 120\n"
        "review_verify_timeout_grace_seconds: 3\n"
    )
    service_dir = tmp_path / "services" / "foo"
    lib_dir = tmp_path / "libs" / "bar"
    service_dir.mkdir(parents=True)
    lib_dir.mkdir(parents=True)
    (service_dir / "gza.yaml").write_text(
        "project_name: foo\n"
        "provider: codex\n"
        "model: gpt-5.5\n"
        "verify_command: ./bin/foo-verify\n"
        "autonomous_verify_timeout_seconds: 300\n"
        "review_verify_timeout_grace_seconds: 11\n"
    )
    (lib_dir / "gza.yaml").write_text(
        "project_name: bar\n"
        "provider: codex\n"
        "model: gpt-5.5\n"
        "verify_command: ./bin/bar-verify\n"
        "autonomous_verify_timeout_seconds: 222\n"
        "review_verify_timeout_grace_seconds: 22\n"
    )
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/verify-only-noop-cross-budget")
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
    improve.tags = (CROSS_PROJECT_TAG,)
    store.update(improve)
    store.add_artifact(
        impl.id,
        kind=VERIFY_GATE_ARTIFACT_KIND,
        path=".gza/artifacts/scoped-timeout-budget.json",
        byte_size=2,
        sha256="0" * 64,
        created_at=datetime(2026, 8, 29, 12, 0, tzinfo=UTC),
        metadata={
            "schema_version": 1,
            "result": {"command": "(per-project verify_command)", "status": "failed", "failure_origin": "timeout"},
            "verify_epoch": {"verify_command": "(per-project verify_command)"},
            "aggregate_details": {
                "scopes": [
                    {
                        "scope": "services/foo",
                        "status": "failed",
                        "failure_origin": "timeout",
                        "command_identity": "./bin/foo-verify",
                        "verify_timeout_seconds": 600,
                        "verify_timeout_grace_seconds": 11,
                        "phase_diagnostics": {
                            "phase_results": [{"name": "unit", "status": "passed", "duration_seconds": 1.0}],
                            "started_phase_names": ["unit"],
                            "completed_phase_names": ["unit"],
                            "failed_phase_names": [],
                            "expected_phase_names": ["unit"],
                            "not_started_phase_names": [],
                        },
                    }
                ]
            },
        },
        status="failed",
    )

    class CrossProjectBudgetGit(_VerifyOnlyNoopGit):
        def worktree_add_existing(self, path: Path, ref: str, *, detach: bool = False) -> Path:
            super().worktree_add_existing(path, ref, detach=detach)
            worktree_service_dir = path / "services" / "foo"
            worktree_lib_dir = path / "libs" / "bar"
            worktree_service_dir.mkdir(parents=True)
            worktree_lib_dir.mkdir(parents=True)
            (worktree_service_dir / "gza.yaml").write_text((service_dir / "gza.yaml").read_text())
            (worktree_lib_dir / "gza.yaml").write_text((lib_dir / "gza.yaml").read_text())
            return path

    git = CrossProjectBudgetGit(impl.branch or "", "same-head")
    context = _base_executor_context(
        store=store,
        config=config,
        trigger_source="advance",
        git=git,
        runtime_context=RuntimeExecutionContext.from_config(config),
    )

    def make_worktree_git(path: Path, **_kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(
            repo_dir=Path(path),
            default_branch=lambda: "main",
            rev_parse_if_exists=lambda ref: "same-head",
            get_diff_name_status=lambda *_args, **_kwargs: "M\tservices/foo/app.py\nM\tlibs/bar/lib.py\n",
        )

    with (
        patch("gza.cli.advance_executor.Git", side_effect=make_worktree_git),
        patch("gza.cli.advance_executor._resolve_review_verify_base_sha", return_value="base-sha"),
        patch(
            "gza.runner._run_review_verify_command",
            side_effect=lambda command, **_kwargs: {
                "./bin/foo-verify": _make_review_verify_result(
                    "./bin/foo-verify",
                    status="passed",
                    exit_status="0",
                    captured_at=datetime(2026, 8, 29, 12, 1, tzinfo=UTC),
                    reviewed_branch=impl.branch,
                    reviewed_head_sha="same-head",
                    reviewed_base_sha="base-sha",
                ),
                "./bin/bar-verify": _make_review_verify_result(
                    "./bin/bar-verify",
                    status="passed",
                    exit_status="0",
                    captured_at=datetime(2026, 8, 29, 12, 2, tzinfo=UTC),
                    reviewed_branch=impl.branch,
                    reviewed_head_sha="same-head",
                    reviewed_base_sha="base-sha",
                ),
            }[command],
        ) as verify_command,
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

    assert result.status == "success"
    calls_by_command = {verify_call.args[0]: verify_call.kwargs for verify_call in verify_command.call_args_list}
    assert set(calls_by_command) == {"./bin/foo-verify", "./bin/bar-verify"}
    assert calls_by_command["./bin/foo-verify"]["timeout_seconds"] == 780
    assert calls_by_command["./bin/foo-verify"]["timeout_grace_seconds"] == 11.0
    assert calls_by_command["./bin/bar-verify"]["timeout_seconds"] == 222
    assert calls_by_command["./bin/bar-verify"]["timeout_grace_seconds"] == 22.0


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
        runtime_context=RuntimeExecutionContext.from_config(config),
    )

    with (
        patch("gza.cli.advance_executor.Git", side_effect=lambda path, **_kwargs: SimpleNamespace(repo_dir=Path(path), default_branch=lambda: "main", rev_parse_if_exists=lambda ref: "same-head")),
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
        runtime_context=RuntimeExecutionContext.from_config(config),
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
        runtime_context=RuntimeExecutionContext.from_config(config),
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
        runtime_context=RuntimeExecutionContext.from_config(config),
    )

    with (
        patch("gza.cli.advance_executor.Git", side_effect=lambda path, **_kwargs: SimpleNamespace(repo_dir=Path(path), default_branch=lambda: "main", rev_parse_if_exists=lambda ref: "same-head")),
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
        runtime_context=RuntimeExecutionContext.from_config(config),
    )

    with (
        patch(
            "gza.cli.advance_executor.Git",
            side_effect=lambda path, **_kwargs: SimpleNamespace(
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
        runtime_context=RuntimeExecutionContext.from_config(config),
    )

    with (
        patch(
            "gza.cli.advance_executor.Git",
            side_effect=lambda path, **_kwargs: SimpleNamespace(
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
    verify_artifacts = store.list_artifacts(impl.id, kind="verify_command_output")

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
    assert store.list_artifacts(improve.id, kind="verify_command_output") == []
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


def test_materialize_plan_review_slices_requires_implement_model_before_mutation(tmp_path: Path) -> None:
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "provider: codex\n"
        "providers:\n"
        "  codex:\n"
        "    task_types:\n"
        "      plan:\n"
        "        model: gpt-5.5\n",
        encoding="utf-8",
    )
    config = Config.load(tmp_path)
    store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)

    plan = store.add("Plan lifecycle slices", task_type="plan")
    review = store.add("Review plan lifecycle slices", task_type="plan_review", depends_on=plan.id)
    assert plan.id is not None
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
    task_count = len(store.get_all())

    with pytest.raises(ConfigError) as exc_info:
        _materialize_plan_review_slices(
            config,
            store,
            plan,
            review,
            manifest,
            trigger_source="manual",
            require_review_before_merge=True,
        )

    assert "task type 'implement' with provider 'codex'" in str(exc_info.value)
    assert len(store.get_all()) == task_count
    assert store.list_artifacts(review.id, kind=PLAN_REVIEW_MATERIALIZATION_ARTIFACT_KIND) == []


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


def test_execute_release_approved_plan_review_persists_hold_release_without_materializing(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    plan = store.add("Held plan", task_type="plan", auto_implement=False)
    assert plan.id is not None
    _mark_completed(plan)
    store.update(plan)

    review = store.add("Approved review", task_type="plan_review", depends_on=plan.id)
    assert review.id is not None
    _mark_completed(review)
    store.update(review)

    before_count = len(store.get_all())
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
        task=plan,
        action={
            "type": "release_approved_plan_review",
            "plan_source_task": plan,
            "plan_review_task": review,
        },
        context=context,
    )

    refreshed = store.get(plan.id)
    assert refreshed is not None
    assert result.status == "success"
    assert result.work_done is True
    assert result.handled_task_id == plan.id
    assert result.success_message == f"Released held plan {plan.id} after approved plan review {review.id}"
    assert refreshed.auto_implement is True
    assert len(store.get_all()) == before_count


def test_execute_release_approved_plan_review_dry_run_does_not_mutate(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    plan = store.add("Held plan", task_type="plan", auto_implement=False)
    assert plan.id is not None
    _mark_completed(plan)
    store.update(plan)

    review = store.add("Approved review", task_type="plan_review", depends_on=plan.id)
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
        task=plan,
        action={
            "type": "release_approved_plan_review",
            "plan_source_task": plan,
            "plan_review_task": review,
        },
        context=context,
    )

    refreshed = store.get(plan.id)
    assert refreshed is not None
    assert result.status == "dry_run"
    assert result.work_done is True
    assert result.message == f"Released held plan {plan.id} after approved plan review {review.id}"
    assert refreshed.auto_implement is False


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


def test_execute_repair_plan_slice_materialization_drops_partial_tasks_and_recreates_full_artifact(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan lifecycle slices", task_type="plan")
    assert plan.id is not None
    _mark_completed(plan)
    store.update(plan)

    review = store.add("Review plan lifecycle slices", task_type="plan_review", depends_on=plan.id)
    assert review.id is not None
    _mark_completed(review)
    manifest_payload = _build_plan_review_manifest_payload(plan.id)
    review.output_content = (
        "## Verdict\nVerdict: APPROVED\n\n## Slice Manifest\n```json\n"
        + json.dumps(manifest_payload)
        + "\n```\n"
    )
    store.update(review)

    manifest = validate_plan_review_manifest(
        manifest_payload,
        markdown_verdict="APPROVED",
        source_task_id=plan.id,
        source_task_type="plan",
        max_slice_timeout_minutes=30,
    )
    task_specs = build_plan_review_slice_task_specs(
        plan_source_task=plan,
        review_task=review,
        manifest=manifest,
        trigger_source="plan-review",
        require_review_before_merge=True,
    )
    partial = store.add(
        task_specs[0].prompt,
        task_type="implement",
        based_on=plan.id,
        trigger_source="plan-review",
        tags=task_specs[0].tags,
        review_scope=task_specs[0].review_scope,
        create_review=task_specs[0].create_review,
    )
    assert partial.id is not None

    context = _build_plan_review_repair_context(config=config, store=store)

    result = execute_advance_action(
        task=plan,
        action={
            "type": "repair_plan_slice_materialization",
            "description": "Repair partial plan-review slice materialization",
            "plan_review_task": review,
            "plan_source_task": plan,
            "manifest": manifest,
            "partial_task_ids": (partial.id,),
            "repair_trigger_source": "plan-review",
        },
        context=context,
    )

    repaired_partial = store.get(partial.id)
    assert repaired_partial is not None
    assert result.status == "success"
    assert result.worker_consuming is False
    assert repaired_partial.status == "dropped"
    assert repaired_partial.drop_reason == PLAN_REVIEW_MATERIALIZATION_AUTO_REPAIR_DROP_REASON
    assert "Dropped partial plan-review slices" in result.message
    assert partial.id in result.message
    assert result.plan_review_materialization is not None
    assert result.plan_review_materialization.created is True

    active_implement_tasks = [task for task in store.get_all() if task.task_type == "implement" and task.status != "dropped"]
    assert len(active_implement_tasks) == 2
    assert {task.id for task in active_implement_tasks if task.id is not None}.isdisjoint({partial.id})

    artifacts = store.list_artifacts(review.id, kind=PLAN_REVIEW_MATERIALIZATION_ARTIFACT_KIND)
    assert len(artifacts) == 1
    assert artifacts[0].metadata["schema_version"] == PLAN_REVIEW_ARTIFACT_SCHEMA_VERSION
    assert artifacts[0].metadata["review_task_id"] == review.id
    assert artifacts[0].metadata["source_task_id"] == plan.id
    assert artifacts[0].metadata["source_task_type"] == "plan"
    assert artifacts[0].metadata["manifest_digest"] == plan_review_manifest_digest(manifest)
    assert artifacts[0].metadata["trigger_source"] == "plan-review"
    assert artifacts[0].metadata["create_review"] is True
    assert set(artifacts[0].metadata["task_ids"]) == {task.id for task in active_implement_tasks if task.id is not None}

    next_action = evaluate_advance_rules(config, store, _PlanRepairFakeGit(), plan, "main")

    assert next_action["type"] == "skip"
    assert next_action["reason"] == "already_materialized"


def test_execute_repair_plan_slice_materialization_drops_extra_pending_partial_and_reuses_complete_artifact(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan lifecycle slices", task_type="plan")
    assert plan.id is not None
    _mark_completed(plan)
    store.update(plan)

    review = store.add("Review plan lifecycle slices", task_type="plan_review", depends_on=plan.id)
    assert review.id is not None
    _mark_completed(review)
    manifest_payload = _build_plan_review_manifest_payload(plan.id)
    review.output_content = (
        "## Verdict\nVerdict: APPROVED\n\n## Slice Manifest\n```json\n"
        + json.dumps(manifest_payload)
        + "\n```\n"
    )
    store.update(review)

    manifest = validate_plan_review_manifest(
        manifest_payload,
        markdown_verdict="APPROVED",
        source_task_id=plan.id,
        source_task_type="plan",
        max_slice_timeout_minutes=30,
    )
    task_specs = build_plan_review_slice_task_specs(
        plan_source_task=plan,
        review_task=review,
        manifest=manifest,
        trigger_source="plan-review",
        require_review_before_merge=True,
    )

    created_tasks = store.add_tasks_with_artifact_atomic(
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
            "manifest_digest": plan_review_manifest_digest(manifest),
            "trigger_source": "plan-review",
            "create_review": True,
            "task_ids": [task.id for task in tasks if task.id is not None],
        },
    )
    created_task_ids = {task.id for task in created_tasks if task.id is not None}

    extra_partial = store.add(
        task_specs[0].prompt,
        task_type="implement",
        based_on=plan.id,
        trigger_source="plan-review",
        tags=task_specs[0].tags,
        review_scope=task_specs[0].review_scope,
        create_review=task_specs[0].create_review,
    )
    assert extra_partial.id is not None

    repair_action = evaluate_advance_rules(config, store, _PlanRepairFakeGit(), plan, "main")
    assert repair_action["type"] == "repair_plan_slice_materialization"
    assert repair_action["partial_task_ids"] == (extra_partial.id,)
    assert repair_action["repair_trigger_source"] == "plan-review"

    result = execute_advance_action(
        task=plan,
        action=repair_action,
        context=_build_plan_review_repair_context(config=config, store=store),
    )

    repaired_partial = store.get(extra_partial.id)
    assert repaired_partial is not None
    assert result.status == "success"
    assert repaired_partial.status == "dropped"
    assert repaired_partial.drop_reason == PLAN_REVIEW_MATERIALIZATION_AUTO_REPAIR_DROP_REASON
    assert "Dropped partial plan-review slices" in result.message
    assert extra_partial.id in result.message
    assert "Reused implementation slices" in result.message
    assert result.plan_review_materialization is not None
    assert result.plan_review_materialization.created is False
    assert {task.id for task in result.plan_review_materialization.tasks if task.id is not None} == created_task_ids

    active_implement_tasks = [task for task in store.get_all() if task.task_type == "implement" and task.status != "dropped"]
    active_implement_ids = {task.id for task in active_implement_tasks if task.id is not None}
    assert active_implement_ids == created_task_ids

    artifacts = store.list_artifacts(review.id, kind=PLAN_REVIEW_MATERIALIZATION_ARTIFACT_KIND)
    assert len(artifacts) == 1
    assert set(artifacts[0].metadata["task_ids"]) == created_task_ids

    next_action = evaluate_advance_rules(config, store, _PlanRepairFakeGit(), plan, "main")
    assert next_action["type"] == "skip"
    assert next_action["reason"] == "already_materialized"


def test_execute_repair_plan_slice_materialization_skips_stale_overlap_when_full_artifact_now_reuses_partial(
    tmp_path: Path,
) -> None:
    config, store, plan, review, manifest, task_specs, partial = _setup_plan_review_repair_candidate(tmp_path=tmp_path)
    assert review.id is not None
    advance_action = evaluate_advance_rules(config, store, _PlanRepairFakeGit(), plan, "main")

    assert advance_action["type"] == "repair_plan_slice_materialization"
    assert advance_action["partial_task_ids"] == (partial.id,)

    sibling = store.add(
        task_specs[1].prompt,
        task_type="implement",
        depends_on=partial.id,
        based_on=plan.id,
        same_branch=task_specs[1].same_branch,
        trigger_source="plan-review",
        tags=task_specs[1].tags,
        review_scope=task_specs[1].review_scope,
        create_review=task_specs[1].create_review,
    )
    assert sibling.id is not None
    _add_plan_review_materialization_artifact(
        store,
        review_id=review.id,
        metadata={
            "schema_version": PLAN_REVIEW_ARTIFACT_SCHEMA_VERSION,
            "review_task_id": review.id,
            "source_task_id": plan.id,
            "source_task_type": "plan",
            "manifest_digest": plan_review_manifest_digest(manifest),
            "trigger_source": "plan-review",
            "create_review": True,
            "task_ids": [partial.id, sibling.id],
        },
    )

    result = execute_advance_action(
        task=plan,
        action=advance_action,
        context=_build_plan_review_repair_context(config=config, store=store),
    )

    partial_after = store.get(partial.id)
    assert partial_after is not None
    sibling_after = store.get(sibling.id)
    assert sibling_after is not None
    assert result.status == "skip"
    assert result.attention_reason == "plan-review-materialization-repair-needed"
    assert partial_after.status == "pending"
    assert sibling_after.status == "pending"
    assert "reusable materialization already references partial slice task id(s)" in result.message
    assert partial.id in result.message

    active_implement_tasks = [task for task in store.get_all() if task.task_type == "implement" and task.status != "dropped"]
    assert {task.id for task in active_implement_tasks if task.id is not None} == {
        partial.id,
        sibling.id,
    }
    next_action = evaluate_advance_rules(config, store, _PlanRepairFakeGit(), plan, "main")

    assert next_action["type"] == "skip"
    assert next_action["reason"] == "already_materialized"


@pytest.mark.parametrize(
    ("artifact_metadata_builder", "task_mutator", "expected_message"),
    [
        pytest.param(
            lambda plan, review, manifest, task_specs, created_tasks: None,
            None,
            "invalid metadata",
            id="missing-metadata",
        ),
        pytest.param(
            lambda plan, review, manifest, task_specs, created_tasks: {
                "schema_version": PLAN_REVIEW_ARTIFACT_SCHEMA_VERSION,
                "review_task_id": review.id,
                "source_task_id": plan.id,
                "source_task_type": "plan",
                "manifest_digest": "different-digest",
                "trigger_source": "plan-review",
                "create_review": True,
                "task_ids": [task.id for task in created_tasks if task.id is not None],
            },
            None,
            "different manifest digest",
            id="different-manifest-digest",
        ),
        pytest.param(
            lambda plan, review, manifest, task_specs, created_tasks: {
                "schema_version": PLAN_REVIEW_ARTIFACT_SCHEMA_VERSION,
                "review_task_id": review.id,
                "source_task_id": plan.id,
                "source_task_type": "plan",
                "manifest_digest": plan_review_manifest_digest(manifest),
                "trigger_source": "plan-review",
                "create_review": True,
                "task_ids": [created_tasks[0].id, created_tasks[0].id],
            },
            None,
            "duplicate task ids",
            id="duplicate-task-ids",
        ),
        pytest.param(
            lambda plan, review, manifest, task_specs, created_tasks: {
                "schema_version": PLAN_REVIEW_ARTIFACT_SCHEMA_VERSION,
                "review_task_id": review.id,
                "source_task_id": plan.id,
                "source_task_type": "plan",
                "manifest_digest": plan_review_manifest_digest(manifest),
                "trigger_source": "plan-review",
                "create_review": True,
                "task_ids": [created_tasks[0].id, 7],
            },
            None,
            "malformed task ids",
            id="non-string-task-id",
        ),
        pytest.param(
            lambda plan, review, manifest, task_specs, created_tasks: {
                "schema_version": PLAN_REVIEW_ARTIFACT_SCHEMA_VERSION,
                "review_task_id": review.id,
                "source_task_id": plan.id,
                "source_task_type": "plan",
                "manifest_digest": plan_review_manifest_digest(manifest),
                "trigger_source": "plan-review",
                "create_review": True,
            },
            None,
            "missing complete task ids",
            id="missing-task-ids",
        ),
        pytest.param(
            lambda plan, review, manifest, task_specs, created_tasks: {
                "schema_version": PLAN_REVIEW_ARTIFACT_SCHEMA_VERSION,
                "review_task_id": review.id,
                "source_task_id": plan.id,
                "source_task_type": "plan",
                "manifest_digest": plan_review_manifest_digest(manifest),
                "trigger_source": "plan-review",
                "create_review": True,
                "task_ids": [task.id for task in created_tasks if task.id is not None],
            },
            lambda store, created_tasks, task_specs: _mutate_materialized_task_tags(store, created_tasks[0]),
            "no longer validates against the manifest",
            id="invalid-task-metadata",
        ),
        pytest.param(
            lambda plan, review, manifest, task_specs, created_tasks: {
                "schema_version": PLAN_REVIEW_ARTIFACT_SCHEMA_VERSION,
                "review_task_id": review.id,
                "source_task_id": f"{plan.id}-other",
                "source_task_type": "plan",
                "manifest_digest": plan_review_manifest_digest(manifest),
                "trigger_source": "plan-review",
                "create_review": True,
                "task_ids": [task.id for task in created_tasks if task.id is not None],
            },
            None,
            "invalid source provenance",
            id="conflicting-source-task-id",
        ),
    ],
)
def test_execute_repair_plan_slice_materialization_skips_when_same_pair_artifact_is_ambiguous(
    tmp_path: Path,
    artifact_metadata_builder,
    task_mutator,
    expected_message: str,
) -> None:
    config, store, plan, review, manifest, task_specs, partial = _setup_plan_review_repair_candidate(tmp_path=tmp_path)
    assert review.id is not None
    created_tasks = store.add_tasks_with_artifact_atomic(
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
            "manifest_digest": plan_review_manifest_digest(manifest),
            "trigger_source": "plan-review",
            "create_review": True,
            "task_ids": [task.id for task in tasks if task.id is not None],
        },
    )
    if task_mutator is not None:
        task_mutator(store, created_tasks, task_specs)
    _add_plan_review_materialization_artifact(
        store,
        review_id=review.id,
        metadata=artifact_metadata_builder(plan, review, manifest, task_specs, created_tasks),
    )

    result = execute_advance_action(
        task=plan,
        action={
            "type": "repair_plan_slice_materialization",
            "description": "Repair partial plan-review slice materialization",
            "plan_review_task": review,
            "plan_source_task": plan,
            "manifest": manifest,
            "partial_task_ids": (partial.id,),
            "repair_trigger_source": "plan-review",
        },
        context=_build_plan_review_repair_context(config=config, store=store),
    )

    partial_after = store.get(partial.id)
    assert partial_after is not None
    assert result.status == "skip"
    assert result.attention_reason == "plan-review-materialization-repair-needed"
    assert expected_message in result.message
    assert partial_after.status == "pending"
    artifacts = store.list_artifacts(review.id, kind=PLAN_REVIEW_MATERIALIZATION_ARTIFACT_KIND)
    assert len(artifacts) == 1


def test_execute_repair_plan_slice_materialization_skips_when_descendant_set_changes(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan lifecycle slices", task_type="plan")
    assert plan.id is not None
    _mark_completed(plan)
    store.update(plan)

    review = store.add("Review plan lifecycle slices", task_type="plan_review", depends_on=plan.id)
    assert review.id is not None
    _mark_completed(review)
    manifest_payload = _build_plan_review_manifest_payload(plan.id)
    review.output_content = (
        "## Verdict\nVerdict: APPROVED\n\n## Slice Manifest\n```json\n"
        + json.dumps(manifest_payload)
        + "\n```\n"
    )
    store.update(review)

    manifest = validate_plan_review_manifest(
        manifest_payload,
        markdown_verdict="APPROVED",
        source_task_id=plan.id,
        source_task_type="plan",
        max_slice_timeout_minutes=30,
    )
    task_specs = build_plan_review_slice_task_specs(
        plan_source_task=plan,
        review_task=review,
        manifest=manifest,
        trigger_source="plan-review",
        require_review_before_merge=True,
    )
    partial = store.add(
        task_specs[0].prompt,
        task_type="implement",
        based_on=plan.id,
        trigger_source="plan-review",
        tags=task_specs[0].tags,
        review_scope=task_specs[0].review_scope,
        create_review=task_specs[0].create_review,
    )
    assert partial.id is not None
    new_descendant = store.add(
        task_specs[1].prompt,
        task_type="implement",
        based_on=partial.id,
        depends_on=partial.id,
        same_branch=True,
        trigger_source="plan-review",
        tags=task_specs[1].tags,
        review_scope=task_specs[1].review_scope,
        create_review=task_specs[1].create_review,
    )
    assert new_descendant.id is not None

    result = execute_advance_action(
        task=plan,
        action={
            "type": "repair_plan_slice_materialization",
            "description": "Repair partial plan-review slice materialization",
            "plan_review_task": review,
            "plan_source_task": plan,
            "manifest": manifest,
            "partial_task_ids": (partial.id,),
            "repair_trigger_source": "plan-review",
        },
        context=_build_plan_review_repair_context(config=config, store=store),
    )

    partial_after = store.get(partial.id)
    assert partial_after is not None
    assert result.status == "skip"
    assert result.attention_reason == "plan-review-materialization-repair-needed"
    assert "no longer matches the validated manifest" in result.message
    assert partial_after.status == "pending"
    assert store.list_artifacts(review.id, kind=PLAN_REVIEW_MATERIALIZATION_ARTIFACT_KIND) == []


def test_execute_repair_plan_slice_materialization_skips_when_review_row_changes(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan lifecycle slices", task_type="plan")
    assert plan.id is not None
    _mark_completed(plan)
    store.update(plan)

    review = store.add("Review plan lifecycle slices", task_type="plan_review", depends_on=plan.id)
    assert review.id is not None
    _mark_completed(review)
    manifest_payload = _build_plan_review_manifest_payload(plan.id)
    review.output_content = (
        "## Verdict\nVerdict: APPROVED\n\n## Slice Manifest\n```json\n"
        + json.dumps(manifest_payload)
        + "\n```\n"
    )
    store.update(review)

    manifest = validate_plan_review_manifest(
        manifest_payload,
        markdown_verdict="APPROVED",
        source_task_id=plan.id,
        source_task_type="plan",
        max_slice_timeout_minutes=30,
    )
    task_specs = build_plan_review_slice_task_specs(
        plan_source_task=plan,
        review_task=review,
        manifest=manifest,
        trigger_source="plan-review",
        require_review_before_merge=True,
    )
    partial = store.add(
        task_specs[0].prompt,
        task_type="implement",
        based_on=plan.id,
        trigger_source="plan-review",
        tags=task_specs[0].tags,
        review_scope=task_specs[0].review_scope,
        create_review=task_specs[0].create_review,
    )
    assert partial.id is not None

    review.output_content = "## Verdict\nVerdict: CHANGES_REQUESTED\n"
    store.update(review)

    result = execute_advance_action(
        task=plan,
        action={
            "type": "repair_plan_slice_materialization",
            "description": "Repair partial plan-review slice materialization",
            "plan_review_task": review,
            "plan_source_task": plan,
            "manifest": manifest,
            "partial_task_ids": (partial.id,),
            "repair_trigger_source": "plan-review",
        },
        context=_build_plan_review_repair_context(config=config, store=store),
    )

    partial_after = store.get(partial.id)
    assert partial_after is not None
    assert result.status == "skip"
    assert result.attention_reason == "plan-review-materialization-repair-needed"
    assert "no longer validates for auto-repair" in result.message
    assert partial_after.status == "pending"
    assert store.list_artifacts(review.id, kind=PLAN_REVIEW_MATERIALIZATION_ARTIFACT_KIND) == []


def test_execute_repair_plan_slice_materialization_uses_rule_selected_manual_trigger_source_for_plan(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan lifecycle slices", task_type="plan")
    assert plan.id is not None
    _mark_completed(plan)
    store.update(plan)

    review = store.add("Review plan lifecycle slices", task_type="plan_review", depends_on=plan.id)
    assert review.id is not None
    _mark_completed(review)
    manifest_payload = _build_plan_review_manifest_payload(plan.id)
    review.output_content = (
        "## Verdict\nVerdict: APPROVED\n\n## Slice Manifest\n```json\n"
        + json.dumps(manifest_payload)
        + "\n```\n"
    )
    store.update(review)

    action = evaluate_advance_rules(config, store, _PlanRepairFakeGit(), plan, "main")
    assert action["type"] == "materialize_plan_slices"
    manual_specs = build_plan_review_slice_task_specs(
        plan_source_task=plan,
        review_task=review,
        manifest=action["manifest"],
        trigger_source="manual",
        require_review_before_merge=True,
    )
    partial = store.add(
        manual_specs[0].prompt,
        task_type="implement",
        based_on=plan.id,
        trigger_source="manual",
        tags=manual_specs[0].tags,
        review_scope=manual_specs[0].review_scope,
        create_review=manual_specs[0].create_review,
    )
    assert partial.id is not None

    repair_action = evaluate_advance_rules(config, store, _PlanRepairFakeGit(), plan, "main")
    assert repair_action["type"] == "repair_plan_slice_materialization"
    assert repair_action["partial_task_ids"] == (partial.id,)
    assert repair_action["repair_trigger_source"] == "manual"

    result = execute_advance_action(
        task=plan,
        action=repair_action,
        context=_build_plan_review_repair_context(config=config, store=store),
    )

    assert result.status == "success"
    partial_after = store.get(partial.id)
    assert partial_after is not None
    assert partial_after.status == "dropped"
    artifacts = store.list_artifacts(review.id, kind=PLAN_REVIEW_MATERIALIZATION_ARTIFACT_KIND)
    assert len(artifacts) == 1
    assert artifacts[0].metadata["trigger_source"] == "manual"
    next_action = evaluate_advance_rules(config, store, _PlanRepairFakeGit(), plan, "main")
    assert next_action["type"] == "skip"
    assert next_action["reason"] == "already_materialized"


def test_execute_repair_plan_slice_materialization_uses_rule_selected_manual_trigger_source_for_plan_improve(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    root_plan = store.add("Plan", task_type="plan")
    assert root_plan.id is not None
    _mark_completed(root_plan)
    store.update(root_plan)

    initial_review = store.add("Initial review", task_type="plan_review", depends_on=root_plan.id)
    assert initial_review.id is not None
    _mark_completed(initial_review)
    initial_review.output_content = "## Verdict\nVerdict: CHANGES_REQUESTED\n"
    store.update(initial_review)

    revised_plan = store.add("Revised plan", task_type="plan_improve", depends_on=initial_review.id, based_on=root_plan.id)
    assert revised_plan.id is not None
    _mark_completed(revised_plan)
    store.update(revised_plan)

    revised_review = store.add("Revised review", task_type="plan_review", depends_on=revised_plan.id)
    assert revised_review.id is not None
    _mark_completed(revised_review)
    manifest_payload = _build_plan_review_manifest_payload(revised_plan.id)
    manifest_payload["source_task_type"] = "plan_improve"
    revised_review.output_content = (
        "## Verdict\nVerdict: APPROVED\n\n## Slice Manifest\n```json\n"
        + json.dumps(manifest_payload)
        + "\n```\n"
    )
    store.update(revised_review)

    action = evaluate_advance_rules(config, store, _PlanRepairFakeGit(), revised_plan, "main")
    assert action["type"] == "materialize_plan_slices"
    manual_specs = build_plan_review_slice_task_specs(
        plan_source_task=revised_plan,
        review_task=revised_review,
        manifest=action["manifest"],
        trigger_source="manual",
        require_review_before_merge=True,
    )
    partial = store.add(
        manual_specs[0].prompt,
        task_type="implement",
        based_on=revised_plan.id,
        trigger_source="manual",
        tags=manual_specs[0].tags,
        review_scope=manual_specs[0].review_scope,
        create_review=manual_specs[0].create_review,
    )
    assert partial.id is not None

    repair_action = evaluate_advance_rules(config, store, _PlanRepairFakeGit(), revised_plan, "main")
    assert repair_action["type"] == "repair_plan_slice_materialization"
    assert repair_action["partial_task_ids"] == (partial.id,)
    assert repair_action["repair_trigger_source"] == "manual"

    result = execute_advance_action(
        task=revised_plan,
        action=repair_action,
        context=_build_plan_review_repair_context(config=config, store=store),
    )

    assert result.status == "success"
    partial_after = store.get(partial.id)
    assert partial_after is not None
    assert partial_after.status == "dropped"
    artifacts = store.list_artifacts(revised_review.id, kind=PLAN_REVIEW_MATERIALIZATION_ARTIFACT_KIND)
    assert len(artifacts) == 1
    assert artifacts[0].metadata["trigger_source"] == "manual"
    next_action = evaluate_advance_rules(config, store, _PlanRepairFakeGit(), revised_plan, "main")
    assert next_action["type"] == "skip"
    assert next_action["reason"] == "already_materialized"


def test_repair_plan_slice_materialization_is_supported_direct_action(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    plan = store.add("Plan lifecycle slices", task_type="plan")
    assert plan.id is not None
    _mark_completed(plan)
    store.update(plan)

    assert "repair_plan_slice_materialization" not in _WORKER_ACTIONS

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
        task=plan,
        action={
            "type": "repair_plan_slice_materialization",
            "description": "Repair partial plan-review slice materialization",
            "plan_review_task": plan,
            "plan_source_task": plan,
            "manifest": SimpleNamespace(),
            "partial_task_ids": (),
            "repair_trigger_source": "plan-review",
        },
        context=context,
    )

    assert result.status != "unsupported"


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
    assert attention.action["subject_task_id"] == failed.id


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


def test_verify_only_noop_recovery_blocks_unsafe_cross_project_child_budget(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    root_config = tmp_path / "gza.yaml"
    root_config.write_text(
        root_config.read_text(encoding="utf-8")
        + "\nverify_command: ./bin/root-verify\n"
        + "autonomous_verify_timeout_seconds: 600\n"
        + "autonomous_verify_bootstrap_timeout_seconds: 600\n"
        + "autonomous_verify_min_margin_seconds: 60\n",
        encoding="utf-8",
    )
    worktree_path = tmp_path / "verify-only-worktree"
    child_dir = worktree_path / "libs" / "bar"
    child_dir.mkdir(parents=True)
    (child_dir / "gza.yaml").write_text(
        "project_name: bar\n"
        "project_id: bar\n"
        "provider: codex\n"
        "model: gpt-5.5\n"
        "verify_command: ./bin/tests\n"
        "autonomous_verify_timeout_seconds: 120\n"
        "autonomous_verify_bootstrap_timeout_seconds: 600\n"
        "autonomous_verify_min_margin_seconds: 60\n",
        encoding="utf-8",
    )
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    runtime_context = RuntimeExecutionContext.from_config(config)

    impl = store.add("Implement verify-only no-op recovery", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/verify-only-cross-project-budget")
    store.update(impl)

    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 8, 29, 10, 0, tzinfo=UTC)
    review.output_content = "## Blockers\n\n### B1 verify_command failure\n\n## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    store.update(review)

    noop_improve = store.add(
        "No-op improve",
        task_type="improve",
        based_on=impl.id,
        depends_on=review.id,
        same_branch=True,
        tags=(CROSS_PROJECT_TAG,),
    )
    assert noop_improve.id is not None
    noop_improve.branch = impl.branch
    noop_improve.status = "completed"
    noop_improve.completed_at = datetime(2026, 8, 29, 11, 0, tzinfo=UTC)
    noop_improve.changed_diff = False
    store.update(noop_improve)

    root_git = SimpleNamespace(
        repo_dir=tmp_path,
        rev_parse_if_exists=lambda ref: "head-1" if ref == impl.branch else None,
        worktree_add_existing=lambda *_args, **_kwargs: None,
        worktree_remove=lambda *_args, **_kwargs: None,
    )
    worktree_git = SimpleNamespace(
        repo_dir=worktree_path,
        default_branch=lambda: "main",
        rev_parse_if_exists=lambda ref: {"HEAD": "head-1", "main": "base-1", "origin/main": "base-1"}.get(ref),
        get_diff_name_status=lambda *_args, **_kwargs: "M\tlibs/bar/lib.py\n",
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
        spawn_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=root_git,
        runtime_context=runtime_context,
    )

    with (
        patch("gza.cli.advance_executor.tempfile.mkdtemp", return_value=str(worktree_path)),
        patch("gza.cli.advance_executor.Git", return_value=worktree_git),
        patch("gza.runner._run_review_verify_command") as mock_verify,
    ):
        result = execute_advance_action(
            task=impl,
            action={
                "type": "recover_verify_only_noop_review",
                "review_task": review,
                "latest_noop_improve_task": noop_improve,
                "current_branch_head_sha": "head-1",
            },
            context=context,
        )

    mock_verify.assert_not_called()
    assert result.status == "skip"
    assert result.attention_type == "needs_discussion"
    artifacts = store.list_artifacts(impl.id, kind=VERIFY_GATE_ARTIFACT_KIND)
    assert artifacts
    latest = artifacts[0].metadata
    assert latest is not None
    assert latest["result"]["status"] == "unavailable"
    assert latest["aggregate_details"]["scopes"][0]["exit_status"] == "insufficient verify budget margin"


def test_verify_gate_execution_persists_current_passing_owner_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    (tmp_path / ".env").write_text("PROJECT_ONLY_TOKEN=captured-token\n", encoding="utf-8")
    monkeypatch.setenv("PATH", "/captured/bin")
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0
    runtime_context = RuntimeExecutionContext.from_config(config)
    monkeypatch.setenv("PATH", "/ambient/bin")
    monkeypatch.setenv("PROJECT_ONLY_TOKEN", "ambient-token")
    monkeypatch.setenv("GZA_DB_PATH", str(tmp_path / "ambient.db"))

    impl = store.add("Implement verify gate", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/verify-gate")
    store.update(impl)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        rev_parse_if_exists=lambda _ref: "head-1",
        worktree_add_existing=lambda *_args, **_kwargs: None,
        worktree_remove=lambda *_args, **_kwargs: None,
    )
    worktree_git = SimpleNamespace(
        repo_dir=tmp_path / "tmp-worktree",
        default_branch=lambda: "main",
        rev_parse_if_exists=lambda ref: "base-1" if ref in {"main", "origin/main"} else "head-1",
    )
    verify_result = _make_review_verify_result(
        "./bin/tests",
        status="passed",
        exit_status="0",
        captured_at=datetime(2026, 6, 29, 12, 0, tzinfo=UTC),
        reviewed_branch=impl.branch,
        reviewed_head_sha="head-1",
        reviewed_base_sha="base-1",
        working_directory=str(tmp_path),
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
        spawn_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=git,
        runtime_context=runtime_context,
    )

    with (
        patch("gza.cli.advance_executor.Git", return_value=worktree_git),
        patch(
            "gza.cli.advance_executor._run_lifecycle_verify",
            return_value=SimpleNamespace(
                markdown="verify markdown",
                aggregate_result=verify_result,
                project_results=(),
            ),
        ) as run_verify,
    ):
        result = execute_advance_action(
            task=impl,
            action={
                "type": "verify_gate",
                "description": "Run verify gate before review",
                "verify_gate_phase": "pre_review",
                "verify_owner_task": impl,
            },
            context=context,
        )

    refreshed_impl = store.get(impl.id)
    assert refreshed_impl is not None
    lookup = latest_verify_result_for_epoch(
        store,
        refreshed_impl,
        current_epoch=owner_task_verify_epoch(refreshed_impl, config, git),
    )
    assert result.status == "success"
    assert lookup.is_current is True
    assert lookup.result is not None
    assert lookup.result.status == "passed"
    run_verify.assert_called_once()
    assert run_verify.call_args.kwargs["cwd"] == worktree_git.repo_dir
    assert run_verify.call_args.kwargs["runtime_context"] is runtime_context
    assert runtime_context.env["PATH"] == "/captured/bin"
    assert runtime_context.env["PROJECT_ONLY_TOKEN"] == "captured-token"
    assert runtime_context.env["GZA_DB_PATH"] == str(config.db_path.resolve())


def test_verify_gate_owner_for_review_followup_implement_stops_at_same_branch_owner(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    shared_branch = "gza/gza-work"

    original = store.add("Original implementation", task_type="implement")
    assert original.id is not None
    _mark_completed(original, branch=shared_branch)
    store.update(original)

    review = store.add(
        "Review original implementation",
        task_type="review",
        depends_on=original.id,
        based_on=original.id,
    )
    assert review.id is not None
    _mark_completed(review)
    store.update(review)

    followup = store.add(
        "Review follow-up implementation",
        task_type="implement",
        based_on=review.id,
    )
    assert followup.id is not None
    _mark_completed(followup, branch=shared_branch)
    store.update(followup)
    assert store.get_or_create_merge_unit_for_task(followup) is not None

    owner = _resolve_canonical_verify_gate_owner(store, followup)
    subject = _resolve_verify_gate_subject_task(store, followup)

    assert owner.id == followup.id
    assert subject.id == followup.id


def test_verify_gate_recredits_same_branch_failed_parent_pass_to_live_owner(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0
    shared_branch = "gza/resumed-work"

    failed_parent = store.add("Failed first attempt", task_type="implement")
    assert failed_parent.id is not None
    failed_parent.status = "failed"
    failed_parent.completed_at = datetime(2026, 8, 18, 5, 0, tzinfo=UTC)
    failed_parent.failure_reason = "worker failed"
    failed_parent.branch = shared_branch
    store.update(failed_parent)

    live_child = store.add("Completed resumed attempt", task_type="implement", based_on=failed_parent.id)
    assert live_child.id is not None
    live_child.status = "completed"
    live_child.completed_at = datetime(2026, 8, 18, 5, 5, tzinfo=UTC)
    live_child.branch = shared_branch
    store.update(live_child)

    unit = store.get_or_create_merge_unit_for_task(live_child)
    assert unit is not None
    assert store.resolve_merge_unit_owner_task(unit).id == live_child.id

    contributor_result = _make_review_verify_result(
        "./bin/tests",
        status="passed",
        exit_status="0",
        captured_at=datetime(2026, 8, 18, 5, 7, tzinfo=UTC),
        reviewed_branch=shared_branch,
        reviewed_head_sha="live-head",
        reviewed_base_sha="base-1",
        working_directory=str(tmp_path),
    )
    persist_verify_gate_artifact(
        store,
        config,
        owner_task=failed_parent,
        source_task=live_child,
        result=contributor_result,
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="test",
    )

    git = SimpleNamespace(
        repo_dir=tmp_path,
        rev_parse_if_exists=lambda ref: {
            shared_branch: "live-head",
            "main": "base-1",
            "origin/main": "base-1",
        }.get(ref),
        worktree_add_existing=lambda *_args, **_kwargs: pytest.fail("verify should not rerun"),
        worktree_remove=lambda *_args, **_kwargs: None,
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
        spawn_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=git,
        runtime_context=RuntimeExecutionContext.from_config(config),
    )

    with patch("gza.cli.advance_executor._run_lifecycle_verify") as run_verify:
        result = execute_advance_action(
            task=live_child,
            action={
                "type": "verify_gate",
                "description": "Run verify gate before review",
                "verify_gate_phase": "pre_review",
                "verify_owner_task": live_child,
            },
            context=context,
        )

    refreshed_child = store.get(live_child.id)
    assert refreshed_child is not None
    assert result.status == "success"
    assert result.handled_task_id == live_child.id
    run_verify.assert_not_called()
    assert store.list_artifacts(failed_parent.id, kind=VERIFY_GATE_ARTIFACT_KIND)
    child_artifacts = store.list_artifacts(live_child.id, kind=VERIFY_GATE_ARTIFACT_KIND)
    assert child_artifacts
    assert child_artifacts[0].metadata is not None
    assert child_artifacts[0].metadata["source_task_id"] == live_child.id
    assert child_artifacts[0].metadata["source_task_type"] == live_child.task_type
    assert child_artifacts[0].metadata["verify_epoch"]["verify_timeout_seconds"] == 120
    assert child_artifacts[0].metadata["verify_epoch"]["verify_timeout_grace_seconds"] == 5.0
    assert child_artifacts[0].metadata["reconciliation"] == {
        "producer": "advance_verify_gate_recredit",
        "credited_owner_task_id": live_child.id,
        "evidence_holder_task_id": failed_parent.id,
        "evidence_holder_task_type": failed_parent.task_type,
    }
    assert refreshed_child.review_verify_status == "passed"
    assert refreshed_child.review_verify_branch == shared_branch
    assert refreshed_child.review_verify_head_sha == "live-head"
    lookup = latest_verify_result_for_epoch(
        store,
        refreshed_child,
        current_epoch=owner_task_verify_epoch(refreshed_child, config, git),
    )
    assert lookup.result is not None
    assert lookup.result.source_task_id == live_child.id


def test_verify_gate_recredits_failed_holder_evidence_without_rewriting_source_or_epoch(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0
    shared_branch = "gza/resumed-red-work"

    failed_parent = store.add("Failed first attempt", task_type="implement")
    assert failed_parent.id is not None
    failed_parent.status = "failed"
    failed_parent.completed_at = datetime(2026, 8, 18, 5, 0, tzinfo=UTC)
    failed_parent.failure_reason = "worker failed"
    failed_parent.branch = shared_branch
    store.update(failed_parent)

    live_child = store.add("Completed resumed attempt", task_type="implement", based_on=failed_parent.id)
    assert live_child.id is not None
    live_child.status = "completed"
    live_child.completed_at = datetime(2026, 8, 18, 5, 5, tzinfo=UTC)
    live_child.branch = shared_branch
    live_child.has_commits = True
    store.update(live_child)

    unit = store.get_or_create_merge_unit_for_task(live_child)
    assert unit is not None
    assert store.resolve_merge_unit_owner_task(unit).id == live_child.id

    output_artifact = store_command_output_artifact(
        store,
        failed_parent,
        config,
        kind="verify_command_output",
        producer="test",
        label="verify_command_output",
        output="pytest failed\nAssertionError: still red\n",
        command="./bin/tests",
        status="failed",
        exit_status="1",
        head_sha="live-head",
        created_at=datetime(2026, 8, 18, 5, 7, tzinfo=UTC),
    )
    provenance = {
        "schema_version": 1,
        "projects": [
            {
                "scope": "src",
                "status": "failed",
                "command": "./bin/tests",
                "timeout_seconds": 120,
                "timeout_grace_seconds": 5.0,
            }
        ],
    }
    aggregate_details = {
        "schema_version": 1,
        "failed_count": 1,
        "passed_count": 0,
        "scopes": [{"scope": "src", "status": "failed"}],
    }
    contributor_result = _make_review_verify_result(
        "./bin/tests",
        status="failed",
        exit_status="1",
        captured_at=datetime(2026, 8, 18, 5, 8, tzinfo=UTC),
        reviewed_branch=shared_branch,
        reviewed_head_sha="live-head",
        reviewed_base_sha="base-1",
        working_directory=str(tmp_path),
        failure="pytest failed",
    )
    persist_verify_gate_artifact(
        store,
        config,
        owner_task=failed_parent,
        source_task=live_child,
        result=contributor_result,
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        output_artifact_id=output_artifact.id,
        output_artifact_task_id=failed_parent.id,
        output_artifact_path=output_artifact.path,
        producer="test",
        provenance=provenance,
        aggregate_details=aggregate_details,
    )

    config.autonomous_verify_timeout_seconds = 900
    config.review_verify_timeout_grace_seconds = 30.0
    git = SimpleNamespace(
        repo_dir=tmp_path,
        rev_parse_if_exists=lambda ref: {
            shared_branch: "live-head",
            "main": "base-1",
            "origin/main": "base-1",
        }.get(ref),
        worktree_add_existing=lambda *_args, **_kwargs: pytest.fail("verify should not rerun"),
        worktree_remove=lambda *_args, **_kwargs: None,
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
        spawn_worker=lambda task, _kind: 0,
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=git,
        runtime_context=RuntimeExecutionContext.from_config(config),
    )

    with patch("gza.cli.advance_executor._run_lifecycle_verify") as run_verify:
        result = execute_advance_action(
            task=live_child,
            action={
                "type": "verify_gate",
                "description": "Run verify gate before review",
                "verify_gate_phase": "pre_review",
                "verify_owner_task": live_child,
            },
            context=context,
        )

    refreshed_child = store.get(live_child.id)
    assert refreshed_child is not None
    assert result.status == "skip"
    assert result.attention_reason == "verify-gate-blocked"
    run_verify.assert_not_called()
    child_artifacts = store.list_artifacts(live_child.id, kind=VERIFY_GATE_ARTIFACT_KIND)
    assert child_artifacts
    assert child_artifacts[0].metadata is not None
    child_metadata = child_artifacts[0].metadata
    assert child_metadata["source_task_id"] == live_child.id
    assert child_metadata["source_task_type"] == live_child.task_type
    assert child_metadata["output_artifact_id"] == output_artifact.id
    assert child_metadata["output_artifact_task_id"] == failed_parent.id
    assert child_metadata["output_artifact_path"] == output_artifact.path
    assert child_metadata["verify_epoch"]["verify_timeout_seconds"] == 120
    assert child_metadata["verify_epoch"]["verify_timeout_grace_seconds"] == 5.0
    assert child_metadata["provenance"] == provenance
    assert child_metadata["aggregate_details"] == aggregate_details
    assert child_metadata["reconciliation"] == {
        "producer": "advance_verify_gate_recredit",
        "credited_owner_task_id": live_child.id,
        "evidence_holder_task_id": failed_parent.id,
        "evidence_holder_task_type": failed_parent.task_type,
    }

    verify_epoch = owner_task_verify_epoch(refreshed_child, config, git)
    assert verify_epoch is not None
    lookup = latest_verify_result_for_epoch(store, refreshed_child, current_epoch=verify_epoch)
    assert lookup.result is not None
    assert lookup.result.source_task_id == live_child.id
    assert lookup.result.output_artifact_path == output_artifact.path
    assert lookup.result.status == "failed"

    lifecycle_git = _build_merge_unit_lifecycle_git(refreshed_child, head_sha="live-head")
    action = evaluate_advance_rules(config, store, lifecycle_git, refreshed_child, "main")
    assert action["type"] == "create_verify_fix"
    assert action["based_on_task"].id == live_child.id

    created, did_create = create_or_reuse_verify_fix_task(
        store,
        config,
        impl_task=action["impl_task"],
        based_on_task=action["based_on_task"],
        verify_epoch=action["verify_epoch"],
        trigger_source="manual",
    )
    assert did_create is True
    assert created.based_on == live_child.id


@pytest.mark.parametrize(
    ("output", "expected_reason", "expected_message"),
    [
        (
            "gza-verify phase=start name=ruff\n"
            "gza-verify phase=passed name=ruff duration_seconds=1.0",
            "verify-budget-exceeded",
            "completed phases: ruff; never-started phases: ty, mypy, checks, unit, functional",
        ),
        (
            "gza-verify phase=start name=ruff\n"
            "gza-verify phase=failed name=ruff duration_seconds=1.0",
            "verify-gate-blocked",
            "verify gate remained failed",
        ),
    ],
)
def test_verify_gate_projects_persisted_timeout_budget_classification(
    tmp_path: Path,
    output: str,
    expected_reason: str,
    expected_message: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    impl = store.add("Implement timeout verify gate", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/timeout-verify-gate")
    impl.has_commits = True
    store.update(impl)

    head_sha = "timeout-head"
    git = SimpleNamespace(
        repo_dir=tmp_path,
        rev_parse_if_exists=lambda ref: {
            impl.branch: head_sha,
            "main": "base-1",
            "origin/main": "base-1",
        }.get(ref),
        worktree_add_existing=lambda path, *_args, **_kwargs: Path(path).mkdir(parents=True, exist_ok=True),
        worktree_remove=lambda *_args, **_kwargs: None,
    )

    def _git_for_path(path: Path) -> SimpleNamespace:
        return SimpleNamespace(
            repo_dir=Path(path),
            default_branch=lambda: "main",
            rev_parse_if_exists=lambda ref: head_sha if ref == "HEAD" else None,
        )

    result_payload = _make_review_verify_result(
        "./bin/tests",
        status="failed",
        exit_status="timed out",
        captured_at=datetime(2026, 8, 18, 10, 0, tzinfo=UTC),
        reviewed_branch=impl.branch,
        reviewed_head_sha=head_sha,
        reviewed_base_sha="base-1",
        working_directory=str(tmp_path),
        failure="verify_command timed out after 120s",
        output=output,
        duration_seconds=120.0,
    )
    execution = LifecycleVerifyExecution(
        markdown=_format_review_verify_result(result_payload),
        aggregate_result=result_payload,
        project_results=(),
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
        spawn_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=git,
        runtime_context=RuntimeExecutionContext.from_config(config),
    )

    with (
        patch("gza.cli.advance_executor.Git", side_effect=_git_for_path),
        patch("gza.cli.advance_executor._resolve_review_verify_base_sha", return_value="base-1"),
        patch("gza.cli.advance_executor._run_lifecycle_verify", return_value=execution),
    ):
        result = execute_advance_action(
            task=impl,
            action={
                "type": "verify_gate",
                "description": "Run verify gate before review",
                "verify_gate_phase": "pre_review",
                "verify_owner_task": impl,
            },
            context=context,
        )

    assert result.status == "skip"
    assert result.attention_reason == expected_reason
    assert result.message is not None
    assert expected_message in result.message
    assert result.handled_task_id == impl.id


def test_verify_gate_no_merge_unit_compat_copy_preserves_source_and_epoch(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    impl = store.add("Implement compat copy", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/no-merge-unit-compat")
    store.update(impl)
    review = store.add("Review compat copy", task_type="review", depends_on=impl.id, based_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    store.update(review)

    provenance = {"schema_version": 1, "projects": [{"scope": ".", "status": "passed"}]}
    aggregate_details = {"schema_version": 1, "passed_count": 1, "failed_count": 0}
    persist_verify_gate_artifact(
        store,
        config,
        owner_task=impl,
        source_task=impl,
        result=_make_review_verify_result(
            "./bin/tests",
            status="passed",
            exit_status="0",
            captured_at=datetime(2026, 8, 18, 6, 0, tzinfo=UTC),
            reviewed_branch=impl.branch,
            reviewed_head_sha="head-1",
            reviewed_base_sha="base-1",
            working_directory=str(tmp_path),
        ),
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="test",
        provenance=provenance,
        aggregate_details=aggregate_details,
    )
    config.autonomous_verify_timeout_seconds = 900
    config.review_verify_timeout_grace_seconds = 30.0
    git = SimpleNamespace(
        repo_dir=tmp_path,
        rev_parse_if_exists=lambda ref: {
            impl.branch: "head-1",
            "main": "base-1",
            "origin/main": "base-1",
        }.get(ref),
        worktree_add_existing=lambda *_args, **_kwargs: pytest.fail("verify should not rerun"),
        worktree_remove=lambda *_args, **_kwargs: None,
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
        spawn_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=git,
        runtime_context=RuntimeExecutionContext.from_config(config),
    )

    with patch("gza.cli.advance_executor._run_lifecycle_verify") as run_verify:
        result = execute_advance_action(
            task=review,
            action={
                "type": "verify_gate",
                "description": "Run verify gate before review",
                "verify_gate_phase": "pre_review",
                "verify_owner_task": review,
            },
            context=context,
        )

    assert result.status == "success"
    run_verify.assert_not_called()
    review_artifacts = store.list_artifacts(review.id, kind=VERIFY_GATE_ARTIFACT_KIND)
    assert review_artifacts
    assert review_artifacts[0].metadata is not None
    metadata = review_artifacts[0].metadata
    assert metadata["source_task_id"] == impl.id
    assert metadata["source_task_type"] == impl.task_type
    assert metadata["verify_epoch"]["verify_timeout_seconds"] == 120
    assert metadata["verify_epoch"]["verify_timeout_grace_seconds"] == 5.0
    assert metadata["provenance"] == provenance
    assert metadata["aggregate_details"] == aggregate_details
    assert metadata["reconciliation"] == {
        "producer": "advance_verify_gate_prepared_owner_compat",
        "credited_owner_task_id": review.id,
        "evidence_holder_task_id": impl.id,
        "evidence_holder_task_type": impl.task_type,
    }


def test_verify_gate_no_merge_unit_rejects_unrelated_prepared_owner_without_writes(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    owner = store.add("Implement owner with green proof", task_type="implement")
    assert owner.id is not None
    _mark_completed(owner, branch="feature/no-merge-unit-owner")
    store.update(owner)
    unrelated = store.add("Unrelated prepared owner", task_type="implement")
    assert unrelated.id is not None
    _mark_completed(unrelated, branch="feature/unrelated-prepared-owner")
    store.update(unrelated)

    _persist_current_verify_gate_result(
        store=store,
        config=config,
        task=owner,
        status="passed",
        captured_at=datetime(2026, 8, 18, 6, 0, tzinfo=UTC),
    )
    context = _build_verify_gate_merge_unit_context(tmp_path=tmp_path, store=store, config=config, owner=owner)

    with patch("gza.cli.advance_executor._run_lifecycle_verify", side_effect=AssertionError("verify should not rerun")):
        result = execute_advance_action(
            task=owner,
            action={
                "type": "verify_gate",
                "description": "Run verify gate before review",
                "verify_gate_phase": "pre_review",
                "verify_owner_task": unrelated,
            },
            context=context,
        )

    refreshed_unrelated = store.get(unrelated.id)
    assert refreshed_unrelated is not None
    assert result.status == "skip"
    assert "owner mismatch" in result.message
    assert store.list_artifacts(unrelated.id, kind=VERIFY_GATE_ARTIFACT_KIND) == []
    assert refreshed_unrelated.review_verify_status is None
    assert refreshed_unrelated.review_verify_artifact_file is None


def test_verify_gate_owner_and_subject_match_same_branch_attempt_tip(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    shared_branch = "gza/multi-attempt"

    first = store.add("Failed first attempt", task_type="implement")
    assert first.id is not None
    first.status = "failed"
    first.completed_at = datetime(2026, 8, 18, 4, 0, tzinfo=UTC)
    first.branch = shared_branch
    store.update(first)

    second = store.add("Dropped second attempt", task_type="implement", based_on=first.id)
    assert second.id is not None
    second.status = "dropped"
    second.completed_at = datetime(2026, 8, 18, 4, 5, tzinfo=UTC)
    second.branch = shared_branch
    store.update(second)

    third = store.add("Completed third attempt", task_type="implement", based_on=second.id)
    assert third.id is not None
    third.status = "completed"
    third.completed_at = datetime(2026, 8, 18, 4, 10, tzinfo=UTC)
    third.branch = shared_branch
    store.update(third)

    unit = store.get_or_create_merge_unit_for_task(third)
    assert unit is not None
    assert store.resolve_merge_unit_owner_task(unit).id == third.id

    for task in (first, second, third):
        owner = _resolve_canonical_verify_gate_owner(store, task)
        subject = _resolve_verify_gate_subject_task(store, task)
        assert owner.id == third.id
        assert subject.id == third.id


def test_verify_gate_review_followup_same_branch_runs_live_head_and_persists_to_followup(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0
    shared_branch = "gza/gza-work"

    original = store.add("Original implementation", task_type="implement")
    assert original.id is not None
    _mark_completed(original, branch=shared_branch)
    store.update(original)
    persist_verify_gate_artifact(
        store,
        config,
        owner_task=original,
        source_task=original,
        result=_make_review_verify_result(
            "./bin/tests",
            status="passed",
            exit_status="0",
            captured_at=datetime(2026, 8, 17, 10, 0, tzinfo=UTC),
            reviewed_branch=original.branch,
            reviewed_head_sha="original-head",
            reviewed_base_sha="base-1",
            working_directory=str(tmp_path),
        ),
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="test",
    )

    review = store.add("Review original", task_type="review", depends_on=original.id, based_on=original.id)
    assert review.id is not None
    _mark_completed(review)
    store.update(review)

    followup = store.add("Follow-up from review", task_type="implement", based_on=review.id)
    assert followup.id is not None
    _mark_completed(followup, branch=shared_branch)
    store.update(followup)
    assert store.get_or_create_merge_unit_for_task(followup) is not None

    heads = {
        shared_branch: "followup-head",
        "main": "base-1",
        "origin/main": "base-1",
    }
    added_refs: list[str] = []
    git = SimpleNamespace(
        repo_dir=tmp_path,
        rev_parse_if_exists=lambda ref: heads.get(ref),
        worktree_add_existing=lambda _path, ref, detach=True: added_refs.append(ref),
        worktree_remove=lambda *_args, **_kwargs: None,
    )
    worktree_git = SimpleNamespace(
        repo_dir=tmp_path / "tmp-worktree",
        default_branch=lambda: "main",
        rev_parse_if_exists=lambda ref: heads.get(ref),
    )
    verify_result = _make_review_verify_result(
        "./bin/tests",
        status="passed",
        exit_status="0",
        captured_at=datetime(2026, 8, 17, 10, 5, tzinfo=UTC),
        reviewed_branch=followup.branch,
        reviewed_head_sha="followup-head",
        reviewed_base_sha="base-1",
        working_directory=str(tmp_path),
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
        spawn_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=git,
        runtime_context=RuntimeExecutionContext.from_config(config),
    )

    with (
        patch("gza.cli.advance_executor.Git", return_value=worktree_git),
        patch(
            "gza.cli.advance_executor._run_lifecycle_verify",
            return_value=SimpleNamespace(markdown="verify markdown", aggregate_result=verify_result, project_results=()),
        ) as run_verify,
    ):
        result = execute_advance_action(
            task=followup,
            action={
                "type": "verify_gate",
                "description": "Run verify gate before review",
                "verify_gate_phase": "pre_review",
                "verify_owner_task": followup,
            },
            context=context,
        )

    refreshed_followup = store.get(followup.id)
    assert refreshed_followup is not None
    assert result.status == "success"
    assert result.work_done is True
    run_verify.assert_called_once()
    assert added_refs == ["followup-head"]
    assert len(store.list_artifacts(original.id, kind=VERIFY_GATE_ARTIFACT_KIND)) == 1
    assert store.list_artifacts(followup.id, kind=VERIFY_GATE_ARTIFACT_KIND)
    lookup = latest_verify_result_for_epoch(
        store,
        refreshed_followup,
        current_epoch=owner_task_verify_epoch(refreshed_followup, config, git),
    )
    assert lookup.is_current is True
    assert lookup.result is not None
    assert lookup.result.reviewed_branch == followup.branch
    assert lookup.result.reviewed_head_sha == "followup-head"
    assert refreshed_followup.review_verify_status == "passed"
    assert refreshed_followup.review_verify_branch == followup.branch
    assert refreshed_followup.review_verify_head_sha == "followup-head"


def test_verify_gate_same_branch_ancestor_cached_pass_does_not_short_circuit_followup(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0
    shared_branch = "gza/gza-work"

    original = store.add("Original implementation", task_type="implement")
    assert original.id is not None
    _mark_completed(original, branch=shared_branch)
    store.update(original)
    persist_verify_gate_artifact(
        store,
        config,
        owner_task=original,
        source_task=original,
        result=_make_review_verify_result(
            "./bin/tests",
            status="passed",
            exit_status="0",
            captured_at=datetime(2026, 8, 17, 10, 0, tzinfo=UTC),
            reviewed_branch=shared_branch,
            reviewed_head_sha="ancestor-head",
            reviewed_base_sha="base-1",
            working_directory=str(tmp_path),
        ),
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="test",
    )

    review = store.add("Review original", task_type="review", depends_on=original.id, based_on=original.id)
    assert review.id is not None
    _mark_completed(review)
    store.update(review)

    followup = store.add("Follow-up from review", task_type="implement", based_on=review.id)
    assert followup.id is not None
    _mark_completed(followup, branch=shared_branch)
    store.update(followup)
    assert store.get_or_create_merge_unit_for_task(followup) is not None

    heads = {
        shared_branch: "followup-head",
        "main": "base-1",
        "origin/main": "base-1",
    }
    added_refs: list[str] = []
    git = SimpleNamespace(
        repo_dir=tmp_path,
        rev_parse_if_exists=lambda ref: heads.get(ref),
        worktree_add_existing=lambda _path, ref, detach=True: added_refs.append(ref),
        worktree_remove=lambda *_args, **_kwargs: None,
    )
    worktree_git = SimpleNamespace(
        repo_dir=tmp_path / "tmp-worktree",
        default_branch=lambda: "main",
        rev_parse_if_exists=lambda ref: heads.get(ref),
    )
    verify_result = _make_review_verify_result(
        "./bin/tests",
        status="passed",
        exit_status="0",
        captured_at=datetime(2026, 8, 17, 10, 5, tzinfo=UTC),
        reviewed_branch=shared_branch,
        reviewed_head_sha="followup-head",
        reviewed_base_sha="base-1",
        working_directory=str(tmp_path),
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
        spawn_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=git,
        runtime_context=RuntimeExecutionContext.from_config(config),
    )

    with (
        patch("gza.cli.advance_executor.Git", return_value=worktree_git),
        patch(
            "gza.cli.advance_executor._run_lifecycle_verify",
            return_value=SimpleNamespace(markdown="verify markdown", aggregate_result=verify_result, project_results=()),
        ) as run_verify,
    ):
        result = execute_advance_action(
            task=followup,
            action={
                "type": "verify_gate",
                "description": "Run verify gate before review",
                "verify_gate_phase": "pre_review",
                "verify_owner_task": followup,
            },
            context=context,
        )

    refreshed_followup = store.get(followup.id)
    assert refreshed_followup is not None
    assert result.status == "success"
    assert result.work_done is True
    run_verify.assert_called_once()
    assert added_refs == ["followup-head"]
    assert refreshed_followup.review_verify_status == "passed"
    assert refreshed_followup.review_verify_head_sha == "followup-head"


def test_verify_gate_cached_pass_with_mismatched_subject_head_runs_verify(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    followup = store.add("Follow-up implementation", task_type="implement")
    assert followup.id is not None
    _mark_completed(followup, branch="feature/cached-pass-mismatch")
    store.update(followup)

    heads = {
        followup.branch: "subject-head",
        "main": "base-1",
        "origin/main": "base-1",
    }
    added_refs: list[str] = []
    git = SimpleNamespace(
        repo_dir=tmp_path,
        rev_parse_if_exists=lambda ref: heads.get(ref),
        worktree_add_existing=lambda _path, ref, detach=True: added_refs.append(ref),
        worktree_remove=lambda *_args, **_kwargs: None,
    )
    worktree_git = SimpleNamespace(
        repo_dir=tmp_path / "tmp-worktree",
        default_branch=lambda: "main",
        rev_parse_if_exists=lambda ref: heads.get(ref),
    )
    stale_pass = SimpleNamespace(
        reviewed_branch="feature/other-branch",
        reviewed_head_sha="other-head",
    )
    mismatched_decision = SimpleNamespace(
        state="passed",
        current_epoch=VerifyEpoch(
            reviewed_branch="feature/other-branch",
            reviewed_head_sha="other-head",
            verify_command="./bin/tests",
            verify_timeout_seconds=120,
            verify_timeout_grace_seconds=5.0,
        ),
        lookup=SimpleNamespace(result=stale_pass),
    )
    verify_result = _make_review_verify_result(
        "./bin/tests",
        status="passed",
        exit_status="0",
        captured_at=datetime(2026, 8, 17, 10, 10, tzinfo=UTC),
        reviewed_branch=followup.branch,
        reviewed_head_sha="subject-head",
        reviewed_base_sha="base-1",
        working_directory=str(tmp_path),
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
        spawn_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=git,
        runtime_context=RuntimeExecutionContext.from_config(config),
    )

    with (
        patch("gza.cli.advance_executor.Git", return_value=worktree_git),
        patch("gza.cli.advance_executor.resolve_verify_gate_decision", return_value=mismatched_decision),
        patch(
            "gza.cli.advance_executor._run_lifecycle_verify",
            return_value=SimpleNamespace(markdown="verify markdown", aggregate_result=verify_result, project_results=()),
        ) as run_verify,
    ):
        result = execute_advance_action(
            task=followup,
            action={
                "type": "verify_gate",
                "description": "Run verify gate before review",
                "verify_gate_phase": "pre_review",
                "verify_owner_task": followup,
            },
            context=context,
        )

    assert result.status == "success"
    assert result.work_done is True
    run_verify.assert_called_once()
    assert added_refs == ["subject-head"]
    assert store.list_artifacts(followup.id, kind=VERIFY_GATE_ARTIFACT_KIND)


def _write_verify_gate_cross_project_configs(root: Path, worktree: Path) -> Config:
    root_config_text = (
        "project_name: root\n"
        "project_id: root\n"
        "provider: codex\n"
        "model: gpt-5.5\n"
        "verify_command: ./bin/tests\n"
        "autonomous_verify_timeout_seconds: 120\n"
        "review_verify_timeout_grace_seconds: 5\n"
        "autonomous_verify_min_margin_seconds: 60\n"
        "autonomous_verify_bootstrap_timeout_seconds: 600\n"
    )
    child_config_text = (
        "project_name: bar\n"
        "project_id: bar\n"
        "provider: codex\n"
        "model: gpt-5.5\n"
        "verify_command: ./bin/child-verify\n"
        "autonomous_verify_timeout_seconds: 405\n"
        "review_verify_timeout_grace_seconds: 9\n"
        "autonomous_verify_min_margin_seconds: 60\n"
        "autonomous_verify_bootstrap_timeout_seconds: 600\n"
    )
    (root / "libs" / "bar").mkdir(parents=True, exist_ok=True)
    (worktree / "libs" / "bar").mkdir(parents=True, exist_ok=True)
    (root / "gza.yaml").write_text(root_config_text)
    (worktree / "gza.yaml").write_text(root_config_text)
    (root / "libs" / "bar" / "gza.yaml").write_text(child_config_text)
    (worktree / "libs" / "bar" / "gza.yaml").write_text(child_config_text)
    return Config.load(root)


def _persist_root_full_suite_runtime_observation(
    *,
    store: SqliteTaskStore,
    config: Config,
    owner_task: DbTask,
    duration_seconds: float,
) -> None:
    source = store.add("Root full-suite runtime source", task_type="review", depends_on=owner_task.id)
    assert source.id is not None
    phase_durations = (10.0, 10.0, 10.0, 10.0, 10.0, duration_seconds - 50.0)
    output_lines: list[str] = []
    for phase, duration in zip(
        ("ruff", "ty", "mypy", "checks", "unit", "functional"),
        phase_durations,
        strict=True,
    ):
        output_lines.append(f"gza-verify phase=start name={phase}")
        output_lines.append(f"gza-verify phase=passed name={phase} duration_seconds={duration:.1f}")
    persist_verify_gate_artifact(
        store,
        config,
        owner_task=owner_task,
        source_task=source,
        result=_make_review_verify_result(
            "./bin/tests",
            status="passed",
            exit_status="0",
            captured_at=datetime(2026, 8, 17, 10, 0, tzinfo=UTC),
            reviewed_branch="feature/old-root-runtime",
            reviewed_head_sha="old-root-head",
            reviewed_base_sha="base-1",
            working_directory=str(config.project_dir),
            output="\n".join(output_lines),
            duration_seconds=duration_seconds,
        ),
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="test",
    )


def _execute_cross_project_verify_gate_with_child_command(
    *,
    tmp_path: Path,
    config: Config,
    store: SqliteTaskStore,
    owner_task: DbTask,
):
    heads = {
        owner_task.branch: "head-1",
        "main": "base-1",
        "origin/main": "base-1",
    }

    def populate_worktree(path: Path, _ref: str, *, detach: bool = False) -> Path:
        _write_verify_gate_cross_project_configs(config.project_dir, path)
        return path

    git = SimpleNamespace(
        repo_dir=tmp_path,
        rev_parse_if_exists=lambda ref: heads.get(ref),
        worktree_add_existing=populate_worktree,
        worktree_remove=lambda *_args, **_kwargs: None,
    )

    def build_worktree_git(path: Path, **_kwargs: Any):
        return SimpleNamespace(
            repo_dir=Path(path),
            default_branch=lambda: "main",
            rev_parse_if_exists=lambda ref: heads.get(ref),
            get_diff_name_status=lambda *_args, **_kwargs: "M\tlibs/bar/app.py\n",
        )

    context = _base_executor_context(
        store=store,
        config=config,
        git=git,
        runtime_context=RuntimeExecutionContext.from_config(config),
    )

    def child_verify_result(command: str, *, cwd: Path, **_kwargs: Any):
        return _make_review_verify_result(
            command,
            status="passed",
            exit_status="0",
            captured_at=datetime(2026, 8, 17, 10, 5, tzinfo=UTC),
            reviewed_branch=owner_task.branch,
            reviewed_head_sha="head-1",
            reviewed_base_sha="base-1",
            working_directory=str(cwd),
            output=(
                "gza-verify phase=start name=unit\n"
                "gza-verify phase=passed name=unit duration_seconds=12.0\n"
            ),
            duration_seconds=12.0,
        )

    with (
        patch("gza.cli.advance_executor.Git", side_effect=build_worktree_git),
        patch("gza.cli.advance_executor._resolve_review_verify_base_sha", return_value="base-1"),
        patch("gza.runner._run_review_verify_command", side_effect=child_verify_result) as run_child_verify,
    ):
        result = execute_advance_action(
            task=owner_task,
            action={
                "type": "verify_gate",
                "description": "Run verify gate before review",
                "verify_gate_phase": "pre_review",
                "verify_owner_task": owner_task,
            },
            context=context,
        )

    return result, run_child_verify


def _assert_child_cross_project_verify_persisted(
    *,
    store: SqliteTaskStore,
    owner_task: DbTask,
    result: AdvanceActionExecutionResult,
    run_child_verify: MagicMock,
) -> None:
    assert result.status == "success"
    run_child_verify.assert_called_once()
    assert run_child_verify.call_args.args == ("./bin/child-verify",)
    assert run_child_verify.call_args.kwargs["timeout_seconds"] == 405
    assert run_child_verify.call_args.kwargs["timeout_grace_seconds"] == 9.0

    refreshed = store.get(owner_task.id)
    assert refreshed is not None
    assert refreshed.review_verify_status == "passed"
    assert refreshed.review_verify_cwd == "(per-project; see artifact)"

    artifacts = store.list_artifacts(owner_task.id, kind=VERIFY_GATE_ARTIFACT_KIND)
    assert artifacts
    metadata = next(
        (
            artifact.metadata
            for artifact in artifacts
            if isinstance(artifact.metadata, dict)
            and isinstance(artifact.metadata.get("aggregate_details"), dict)
            and isinstance(artifact.metadata["aggregate_details"].get("scopes"), list)
        ),
        None,
    )
    assert isinstance(metadata, dict)
    aggregate_details = metadata.get("aggregate_details")
    assert isinstance(aggregate_details, dict)
    assert aggregate_details["affected_scope_count"] == 1
    assert aggregate_details["passed_count"] == 1
    assert aggregate_details["unavailable_count"] == 0
    scopes = aggregate_details["scopes"]
    assert len(scopes) == 1
    assert scopes[0]["scope"] == "libs/bar"
    assert scopes[0]["command_identity"] == "./bin/child-verify"
    assert scopes[0]["verify_timeout_seconds"] == 405
    assert scopes[0]["verify_timeout_grace_seconds"] == 9.0
    assert scopes[0]["phase_diagnostics"]["completed_phase_names"] == ["unit"]


def test_cross_project_verify_gate_bypasses_unaffected_root_margin_failure(tmp_path: Path) -> None:
    worktree_seed = tmp_path / "worktree-seed"
    config = _write_verify_gate_cross_project_configs(tmp_path, worktree_seed)
    store = make_store(tmp_path)
    owner = store.add("Implement cross-project gate", task_type="implement", tags=(CROSS_PROJECT_TAG,))
    assert owner.id is not None
    _mark_completed(owner, branch="feature/cross-project-gate")
    store.update(owner)
    _persist_root_full_suite_runtime_observation(
        store=store,
        config=config,
        owner_task=owner,
        duration_seconds=70.0,
    )

    result, run_child_verify = _execute_cross_project_verify_gate_with_child_command(
        tmp_path=tmp_path,
        config=config,
        store=store,
        owner_task=owner,
    )

    _assert_child_cross_project_verify_persisted(
        store=store,
        owner_task=owner,
        result=result,
        run_child_verify=run_child_verify,
    )


def test_cross_project_verify_gate_bypasses_unaffected_root_budget_read_failure(tmp_path: Path) -> None:
    worktree_seed = tmp_path / "worktree-seed"
    config = _write_verify_gate_cross_project_configs(tmp_path, worktree_seed)
    store = make_store(tmp_path)
    owner = store.add("Implement cross-project gate", task_type="implement", tags=(CROSS_PROJECT_TAG,))
    assert owner.id is not None
    _mark_completed(owner, branch="feature/cross-project-gate")
    store.update(owner)

    with patch(
        "gza.cli.advance_executor.resolve_lifecycle_verify_timeout_settings",
        side_effect=LifecycleVerifyBudgetError("root artifact inspection failed"),
    ):
        result, run_child_verify = _execute_cross_project_verify_gate_with_child_command(
            tmp_path=tmp_path,
            config=config,
            store=store,
            owner_task=owner,
        )

    _assert_child_cross_project_verify_persisted(
        store=store,
        owner_task=owner,
        result=result,
        run_child_verify=run_child_verify,
    )


def test_verify_gate_explicit_refresh_reruns_even_when_current_decision_already_passed(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    impl = store.add("Implement explicit verify refresh", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/explicit-verify-refresh")
    store.update(impl)
    persist_verify_gate_artifact(
        store,
        config,
        owner_task=impl,
        source_task=impl,
        result=_make_review_verify_result(
            "./bin/tests",
            status="passed",
            exit_status="0",
            captured_at=datetime(2026, 6, 29, 11, 0, tzinfo=UTC),
            reviewed_branch=impl.branch,
            reviewed_head_sha="head-1",
            reviewed_base_sha="base-1",
            working_directory=str(tmp_path),
        ),
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="test",
    )

    git = SimpleNamespace(
        repo_dir=tmp_path,
        rev_parse_if_exists=lambda _ref: "head-1",
        worktree_add_existing=lambda *_args, **_kwargs: None,
        worktree_remove=lambda *_args, **_kwargs: None,
    )
    worktree_git = SimpleNamespace(
        repo_dir=tmp_path / "tmp-worktree",
        default_branch=lambda: "main",
        rev_parse_if_exists=lambda ref: "base-1" if ref in {"main", "origin/main"} else "head-1",
    )
    refreshed_result = _make_review_verify_result(
        "./bin/tests",
        status="passed",
        exit_status="0",
        captured_at=datetime(2026, 6, 29, 12, 0, tzinfo=UTC),
        reviewed_branch=impl.branch,
        reviewed_head_sha="head-1",
        reviewed_base_sha="base-1",
        working_directory=str(tmp_path),
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
        spawn_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=git,
        runtime_context=RuntimeExecutionContext.from_config(config),
    )

    with (
        patch("gza.cli.advance_executor.Git", return_value=worktree_git),
        patch(
            "gza.cli.advance_executor._run_lifecycle_verify",
            return_value=SimpleNamespace(
                markdown="fresh verify markdown",
                aggregate_result=refreshed_result,
                project_results=(),
            ),
        ) as run_verify,
    ):
        result = execute_advance_action(
            task=impl,
            action={
                "type": "verify_gate",
                "description": "Run verify gate before review",
                "verify_gate_phase": "pre_review",
                "verify_gate_explicit_refresh": True,
                "verify_owner_task": impl,
            },
            context=context,
        )

    refreshed_impl = store.get(impl.id)
    assert refreshed_impl is not None
    artifacts = store.list_artifacts(impl.id, kind=VERIFY_GATE_ARTIFACT_KIND)
    lookup = latest_verify_result_for_epoch(
        store,
        refreshed_impl,
        current_epoch=owner_task_verify_epoch(refreshed_impl, config, git),
    )
    assert result.status == "success"
    run_verify.assert_called_once()
    assert len(artifacts) == 2
    assert lookup.result is not None
    assert lookup.result.captured_at == datetime(2026, 6, 29, 12, 0, tzinfo=UTC)


def test_verify_gate_execution_blocks_current_red_evidence_without_rerun(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    impl = store.add("Implement red verify gate", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/red-verify-gate")
    store.update(impl)
    review = store.add("Review red verify gate", task_type="review", depends_on=impl.id, based_on=impl.id)
    assert review.id is not None
    persist_verify_gate_artifact(
        store,
        config,
        owner_task=impl,
        source_task=review,
        result=_make_review_verify_result(
            "./bin/tests",
            status="failed",
            exit_status="7",
            captured_at=datetime(2026, 6, 29, 12, 0, tzinfo=UTC),
            reviewed_branch=impl.branch,
            reviewed_head_sha="head-1",
            reviewed_base_sha="base-1",
            working_directory=str(tmp_path),
        ),
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="review_verify",
    )

    git = SimpleNamespace(
        repo_dir=tmp_path,
        rev_parse_if_exists=lambda _ref: "head-1",
        worktree_add_existing=lambda *_args, **_kwargs: pytest.fail("worktree should not be prepared"),
        worktree_remove=lambda *_args, **_kwargs: None,
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
        spawn_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=git,
        runtime_context=RuntimeExecutionContext.from_config(config),
    )

    with patch("gza.cli.advance_executor._run_lifecycle_verify", side_effect=AssertionError("verify should not rerun")):
        result = execute_advance_action(
            task=impl,
            action={
                "type": "verify_gate",
                "description": "SKIP: current verify gate is red; merge is blocked",
                "verify_gate_phase": "pre_merge",
                "verify_owner_task": impl,
            },
            context=context,
        )

    assert result.status == "skip"
    assert result.attention_reason == "verify-gate-blocked"
    assert "current verify gate is red" in result.message


def _build_verify_gate_merge_unit_context(
    *,
    tmp_path: Path,
    store: SqliteTaskStore,
    config: Config,
    owner: DbTask,
    head_sha: str = "head-1",
) -> AdvanceActionExecutionContext:
    git = SimpleNamespace(
        repo_dir=tmp_path,
        rev_parse_if_exists=lambda ref: {
            owner.branch: head_sha,
            "main": "base-1",
            "origin/main": "base-1",
        }.get(ref),
        worktree_add_existing=lambda *_args, **_kwargs: pytest.fail("verify should not rerun"),
        worktree_remove=lambda *_args, **_kwargs: None,
    )
    return AdvanceActionExecutionContext(
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
        config=config,
        git=git,
        runtime_context=RuntimeExecutionContext.from_config(config),
    )


def _build_merge_unit_lifecycle_git(owner: DbTask, *, head_sha: str = "head-1") -> MagicMock:
    lifecycle_git = MagicMock()
    lifecycle_git.can_merge.return_value = True
    lifecycle_git.count_commits_behind.return_value = 0
    lifecycle_git.is_merged.return_value = False
    lifecycle_git.branch_exists.return_value = True
    lifecycle_git.ref_exists.return_value = False
    lifecycle_git.rev_parse_if_exists.side_effect = lambda ref: {
        "main": "base-1",
        "origin/main": "base-1",
        owner.branch: head_sha,
    }.get(ref)
    lifecycle_git.is_ancestor.return_value = False
    lifecycle_git.count_commits_behind_checked.return_value = 0
    lifecycle_git.count_commits_ahead_checked.return_value = 1
    lifecycle_git.get_diff_name_status.return_value = ""
    lifecycle_git.resolve_fresh_merge_source.side_effect = lambda branch: ResolvedMergeSourceRef(branch)
    return lifecycle_git


def _add_completed_merge_unit_member(
    store: SqliteTaskStore,
    *,
    prompt: str,
    branch: str,
    unit_id: str,
) -> DbTask:
    member = store.add(prompt, task_type="implement")
    assert member.id is not None
    _mark_completed(member, branch=branch)
    store.update(member)
    store.attach_task_to_merge_unit(member.id, unit_id, "same_branch")
    return member


def _persist_current_verify_gate_result(
    *,
    store: SqliteTaskStore,
    config: Config,
    task: DbTask,
    status: str,
    captured_at: datetime,
    head_sha: str = "head-1",
) -> None:
    persist_verify_gate_artifact(
        store,
        config,
        owner_task=task,
        source_task=task,
        result=_make_review_verify_result(
            "./bin/tests",
            status=status,
            exit_status="0" if status == "passed" else "1",
            captured_at=captured_at,
            reviewed_branch=task.branch,
            reviewed_head_sha=head_sha,
            reviewed_base_sha="base-1",
            working_directory=str(config.project_dir),
            failure=None if status == "passed" else f"verify {status}",
        ),
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="test",
    )


def _persist_legacy_review_verify_result(
    store: SqliteTaskStore,
    *,
    task: DbTask,
    status: str,
    captured_at: datetime,
    head_sha: str = "head-1",
) -> DbTask:
    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    _mark_completed(review)
    review.review_verify_command = "./bin/tests"
    review.review_verify_status = status
    review.review_verify_exit_status = "0" if status == "passed" else "1"
    review.review_verify_failure = None if status == "passed" else f"verify {status}"
    review.review_verify_captured_at = captured_at
    review.review_verify_head_sha = head_sha
    review.review_verify_base_sha = "base-1"
    review.review_verify_branch = task.branch
    review.review_verify_cwd = "/tmp/legacy-review-worktree"
    store.update(review)
    return review


def test_verify_gate_recredits_newer_merge_unit_contributor_green_over_older_owner_red(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    owner = store.add("Implement owner", task_type="implement")
    assert owner.id is not None
    _mark_completed(owner, branch="feature/merge-unit-newer-green")
    store.update(owner)
    unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(owner.id, unit.id, "owner")
    contributor = _add_completed_merge_unit_member(
        store,
        prompt="Contributor current green",
        branch=owner.branch,
        unit_id=unit.id,
    )
    captured_at = datetime(2026, 8, 18, 10, 0, tzinfo=UTC)
    _persist_current_verify_gate_result(
        store=store,
        config=config,
        task=owner,
        status="failed",
        captured_at=captured_at,
    )
    _persist_current_verify_gate_result(
        store=store,
        config=config,
        task=contributor,
        status="passed",
        captured_at=captured_at + timedelta(minutes=1),
    )

    context = _build_verify_gate_merge_unit_context(tmp_path=tmp_path, store=store, config=config, owner=owner)

    with patch("gza.cli.advance_executor._run_lifecycle_verify", side_effect=AssertionError("verify should not rerun")):
        result = execute_advance_action(
            task=owner,
            action={
                "type": "verify_gate",
                "description": "Run verify gate before merge",
                "verify_gate_phase": "pre_merge",
                "verify_owner_task": owner,
            },
            context=context,
        )

    refreshed_owner = store.get(owner.id)
    assert refreshed_owner is not None
    assert result.status == "success"
    assert refreshed_owner.review_verify_status == "passed"
    lookup = latest_verify_result_for_epoch(
        store,
        refreshed_owner,
        current_epoch=owner_task_verify_epoch(refreshed_owner, config, context.git),
    )
    assert lookup.result is not None
    assert lookup.result.source_task_id == contributor.id


def test_pre_review_planner_recredits_newer_merge_unit_green_before_verify_fix(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    owner = store.add("Implement owner", task_type="implement")
    assert owner.id is not None
    _mark_completed(owner, branch="feature/pre-review-merge-unit-newer-green")
    owner.has_commits = True
    store.update(owner)
    unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(owner.id, unit.id, "owner")
    contributor = _add_completed_merge_unit_member(
        store,
        prompt="Contributor current green",
        branch=owner.branch,
        unit_id=unit.id,
    )
    captured_at = datetime(2026, 8, 18, 10, 0, tzinfo=UTC)
    _persist_current_verify_gate_result(
        store=store,
        config=config,
        task=owner,
        status="failed",
        captured_at=captured_at,
    )
    _persist_current_verify_gate_result(
        store=store,
        config=config,
        task=contributor,
        status="passed",
        captured_at=captured_at + timedelta(minutes=1),
    )

    lifecycle_git = _build_merge_unit_lifecycle_git(owner)
    action = evaluate_advance_rules(config, store, lifecycle_git, owner, "main")
    assert action["type"] == "reconcile_verify_gate_evidence"

    context = _build_verify_gate_merge_unit_context(tmp_path=tmp_path, store=store, config=config, owner=owner)
    result = execute_advance_action(task=owner, action=action, context=context)

    refreshed_owner = store.get(owner.id)
    assert refreshed_owner is not None
    assert result.status == "success"
    assert refreshed_owner.review_verify_status == "passed"
    assert [task for task in store.get_all() if task.task_type == "verify_fix"] == []
    lookup = latest_verify_result_for_epoch(
        store,
        refreshed_owner,
        current_epoch=owner_task_verify_epoch(refreshed_owner, config, context.git),
    )
    assert lookup.result is not None
    assert lookup.result.source_task_id == contributor.id

    next_action = evaluate_advance_rules(config, store, lifecycle_git, refreshed_owner, "main")
    assert next_action["type"] == "create_review"


def test_reconcile_verify_gate_evidence_rejects_mismatched_action_owner_without_writes(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    owner = store.add("Implement canonical owner", task_type="implement")
    assert owner.id is not None
    _mark_completed(owner, branch="feature/reconcile-owner")
    owner.has_commits = True
    store.update(owner)
    owner_unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(owner.id, owner_unit.id, "owner")
    owner_contributor = _add_completed_merge_unit_member(
        store,
        prompt="Owner unit contributor",
        branch=owner.branch,
        unit_id=owner_unit.id,
    )

    unrelated_owner = store.add("Unrelated canonical owner", task_type="implement")
    assert unrelated_owner.id is not None
    _mark_completed(unrelated_owner, branch="feature/unrelated-reconcile-owner")
    unrelated_owner.has_commits = True
    store.update(unrelated_owner)
    unrelated_unit = store.create_merge_unit(
        source_branch=unrelated_owner.branch,
        target_branch="main",
        owner_task_id=unrelated_owner.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(unrelated_owner.id, unrelated_unit.id, "owner")
    unrelated_contributor = _add_completed_merge_unit_member(
        store,
        prompt="Unrelated unit contributor",
        branch=unrelated_owner.branch,
        unit_id=unrelated_unit.id,
    )

    captured_at = datetime(2026, 8, 18, 10, 0, tzinfo=UTC)
    for task, status, offset in (
        (owner, "failed", 0),
        (owner_contributor, "passed", 1),
        (unrelated_owner, "failed", 2),
        (unrelated_contributor, "passed", 3),
    ):
        _persist_current_verify_gate_result(
            store=store,
            config=config,
            task=task,
            status=status,
            captured_at=captured_at + timedelta(minutes=offset),
        )
    before_owner_artifacts = len(store.list_artifacts(owner.id, kind=VERIFY_GATE_ARTIFACT_KIND))
    before_unrelated_artifacts = len(store.list_artifacts(unrelated_owner.id, kind=VERIFY_GATE_ARTIFACT_KIND))
    context = _build_verify_gate_merge_unit_context(tmp_path=tmp_path, store=store, config=config, owner=owner)

    with patch("gza.cli.advance_executor._run_lifecycle_verify", side_effect=AssertionError("verify should not rerun")):
        result = execute_advance_action(
            task=owner,
            action={
                "type": "reconcile_verify_gate_evidence",
                "description": "Recredit unrelated stale action owner",
                "verify_owner_task": unrelated_owner,
            },
            context=context,
        )

    refreshed_owner = store.get(owner.id)
    refreshed_unrelated = store.get(unrelated_owner.id)
    assert refreshed_owner is not None
    assert refreshed_unrelated is not None
    assert result.status == "skip"
    assert "owner mismatch" in result.message
    assert refreshed_owner.review_verify_status is None
    assert refreshed_unrelated.review_verify_status is None
    assert len(store.list_artifacts(owner.id, kind=VERIFY_GATE_ARTIFACT_KIND)) == before_owner_artifacts
    assert len(store.list_artifacts(unrelated_owner.id, kind=VERIFY_GATE_ARTIFACT_KIND)) == before_unrelated_artifacts


def test_verify_gate_recredits_legacy_contributor_review_without_rewriting_source(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    owner = store.add("Implement canonical owner", task_type="implement")
    assert owner.id is not None
    _mark_completed(owner, branch="feature/legacy-review-recredit")
    owner.has_commits = True
    store.update(owner)
    unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(owner.id, unit.id, "owner")
    contributor = _add_completed_merge_unit_member(
        store,
        prompt="Contributor with legacy review evidence",
        branch=owner.branch,
        unit_id=unit.id,
    )
    review = _persist_legacy_review_verify_result(
        store,
        task=contributor,
        status="passed",
        captured_at=datetime(2026, 8, 18, 10, 5, tzinfo=UTC),
    )

    lifecycle_git = _build_merge_unit_lifecycle_git(owner)
    action = evaluate_advance_rules(config, store, lifecycle_git, owner, "main")
    assert action["type"] == "reconcile_verify_gate_evidence"
    assert action["verify_source_task"].id == contributor.id

    context = _build_verify_gate_merge_unit_context(tmp_path=tmp_path, store=store, config=config, owner=owner)
    with patch("gza.cli.advance_executor._run_lifecycle_verify", side_effect=AssertionError("verify should not rerun")):
        result = execute_advance_action(task=owner, action=action, context=context)

    assert result.status == "success"
    owner_artifacts = store.list_artifacts(owner.id, kind=VERIFY_GATE_ARTIFACT_KIND)
    assert owner_artifacts
    metadata = owner_artifacts[0].metadata
    assert metadata is not None
    assert metadata["source_task_id"] == review.id
    assert metadata["source_task_type"] == "review"
    assert metadata["reconciliation"] == {
        "producer": "advance_verify_gate_recredit",
        "credited_owner_task_id": owner.id,
        "evidence_holder_task_id": contributor.id,
        "evidence_holder_task_type": contributor.task_type,
    }

    refreshed_owner = store.get(owner.id)
    assert refreshed_owner is not None
    lookup = latest_verify_result_for_epoch(
        store,
        refreshed_owner,
        current_epoch=owner_task_verify_epoch(refreshed_owner, config, context.git),
    )
    assert lookup.result is not None
    assert lookup.result.source_task_id == review.id
    assert lookup.result.source_task_type == "review"


def test_failed_legacy_merge_unit_evidence_preserves_review_source_for_verify_fix(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    owner = store.add("Implement canonical owner", task_type="implement")
    assert owner.id is not None
    _mark_completed(owner, branch="feature/legacy-review-red-recredit")
    owner.has_commits = True
    store.update(owner)
    unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="unmerged",
        head_sha="head-1",
        base_sha="base-1",
    )
    store.attach_task_to_merge_unit(owner.id, unit.id, "owner")
    contributor = _add_completed_merge_unit_member(
        store,
        prompt="Contributor with failed legacy review evidence",
        branch=owner.branch,
        unit_id=unit.id,
    )
    contributor.based_on = owner.id
    store.update(contributor)
    review = _persist_legacy_review_verify_result(
        store,
        task=contributor,
        status="failed",
        captured_at=datetime(2026, 8, 18, 10, 5, tzinfo=UTC),
    )

    lifecycle_git = _build_merge_unit_lifecycle_git(owner)
    action = evaluate_advance_rules(config, store, lifecycle_git, owner, "main")
    assert action["type"] == "reconcile_verify_gate_evidence"

    context = _build_verify_gate_merge_unit_context(tmp_path=tmp_path, store=store, config=config, owner=owner)
    with patch("gza.cli.advance_executor._run_lifecycle_verify", side_effect=AssertionError("verify should not rerun")):
        result = execute_advance_action(task=owner, action=action, context=context)

    assert result.status == "success"
    refreshed_owner = store.get(owner.id)
    assert refreshed_owner is not None
    verify_epoch = owner_task_verify_epoch(refreshed_owner, config, context.git)
    lookup = latest_verify_result_for_epoch(store, refreshed_owner, current_epoch=verify_epoch)
    assert lookup.result is not None
    assert lookup.result.status == "failed"
    assert lookup.result.source_task_id == review.id
    assert lookup.result.source_task_type == "review"

    next_action = evaluate_advance_rules(config, store, lifecycle_git, refreshed_owner, "main")
    assert next_action["type"] == "create_verify_fix"
    assert next_action["based_on_task"].id == contributor.id


def test_recredited_legacy_evidence_with_unresolvable_source_uses_holder_fallback(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    owner = store.add("Implement canonical owner", task_type="implement")
    assert owner.id is not None
    _mark_completed(owner, branch="feature/unresolvable-legacy-source")
    store.update(owner)
    holder = store.add("Evidence holder", task_type="implement")
    assert holder.id is not None
    _mark_completed(holder, branch=owner.branch)
    store.update(holder)

    result = VerifyGateResult(
        command="./bin/tests",
        status="passed",
        exit_status="0",
        captured_at=datetime(2026, 8, 18, 10, 10, tzinfo=UTC),
        reviewed_branch=owner.branch,
        reviewed_head_sha="head-1",
        reviewed_base_sha="base-1",
        working_directory=str(tmp_path),
        source_task_id="gza-999999",
        source_task_type="review",
    )

    persist_recredited_verify_gate_artifact(
        store,
        config,
        owner_task=owner,
        evidence_holder_task=holder,
        result=result,
        source_metadata=None,
        producer="test_recredit",
    )

    artifacts = store.list_artifacts(owner.id, kind=VERIFY_GATE_ARTIFACT_KIND)
    assert artifacts
    metadata = artifacts[0].metadata
    assert metadata is not None
    assert metadata["source_task_id"] == holder.id
    assert metadata["source_task_type"] == holder.task_type
    assert metadata["reconciliation"] == {
        "producer": "test_recredit",
        "credited_owner_task_id": owner.id,
        "evidence_holder_task_id": holder.id,
        "evidence_holder_task_type": holder.task_type,
        "source_provenance_fallback_reason": "unresolvable_source_provenance",
    }


def test_pre_merge_planner_recredits_newer_merge_unit_green_before_verify_fix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    owner = store.add("Implement owner", task_type="implement")
    assert owner.id is not None
    _mark_completed(owner, branch="feature/pre-merge-merge-unit-newer-green")
    owner.has_commits = True
    store.update(owner)
    unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(owner.id, unit.id, "owner")
    contributor = _add_completed_merge_unit_member(
        store,
        prompt="Contributor current green",
        branch=owner.branch,
        unit_id=unit.id,
    )
    review = store.add("Review owner", task_type="review", depends_on=owner.id, based_on=owner.id)
    assert review.id is not None
    _mark_completed(review)
    review.output_content = "## Verdict\n\nVerdict: APPROVED\n"
    review.review_verify_head_sha = "head-1"
    review.review_verify_branch = owner.branch
    store.update(review)
    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review: SimpleNamespace(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )
    captured_at = datetime(2026, 8, 18, 10, 0, tzinfo=UTC)
    _persist_current_verify_gate_result(
        store=store,
        config=config,
        task=owner,
        status="failed",
        captured_at=captured_at,
    )
    _persist_current_verify_gate_result(
        store=store,
        config=config,
        task=contributor,
        status="passed",
        captured_at=captured_at + timedelta(minutes=1),
    )

    lifecycle_git = _build_merge_unit_lifecycle_git(owner)
    action = evaluate_advance_rules(config, store, lifecycle_git, owner, "main")
    assert action["type"] == "reconcile_verify_gate_evidence"

    context = _build_verify_gate_merge_unit_context(tmp_path=tmp_path, store=store, config=config, owner=owner)
    result = execute_advance_action(task=owner, action=action, context=context)

    refreshed_owner = store.get(owner.id)
    assert refreshed_owner is not None
    assert result.status == "success"
    assert refreshed_owner.review_verify_status == "passed"
    assert [task for task in store.get_all() if task.task_type == "verify_fix"] == []
    lookup = latest_verify_result_for_epoch(
        store,
        refreshed_owner,
        current_epoch=owner_task_verify_epoch(refreshed_owner, config, context.git),
    )
    assert lookup.result is not None
    assert lookup.result.source_task_id == contributor.id

    next_action = evaluate_advance_rules(config, store, lifecycle_git, refreshed_owner, "main")
    assert next_action["type"] == "merge"


def _add_approved_review_for_merge(
    store: SqliteTaskStore,
    owner: DbTask,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    review = store.add("Review owner", task_type="review", depends_on=owner.id, based_on=owner.id)
    assert review.id is not None
    _mark_completed(review)
    review.output_content = "## Verdict\n\nVerdict: APPROVED\n"
    review.review_verify_head_sha = "head-1"
    review.review_verify_branch = owner.branch
    store.update(review)
    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review: SimpleNamespace(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )


def _setup_owner_green_contributor_non_green_merge_unit(
    tmp_path: Path,
    newer_status: str,
) -> tuple[SqliteTaskStore, Config, DbTask, DbTask]:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    owner = store.add("Implement owner", task_type="implement")
    assert owner.id is not None
    _mark_completed(owner, branch=f"feature/merge-unit-owner-green-contributor-{newer_status}")
    owner.has_commits = True
    store.update(owner)
    unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(owner.id, unit.id, "owner")
    contributor = _add_completed_merge_unit_member(
        store,
        prompt="Contributor newer non-green",
        branch=owner.branch,
        unit_id=unit.id,
    )
    contributor.based_on = owner.id
    store.update(contributor)
    captured_at = datetime(2026, 8, 18, 10, 0, tzinfo=UTC)
    _persist_current_verify_gate_result(
        store=store,
        config=config,
        task=owner,
        status="passed",
        captured_at=captured_at,
    )
    _persist_current_verify_gate_result(
        store=store,
        config=config,
        task=contributor,
        status=newer_status,
        captured_at=captured_at + timedelta(minutes=1),
    )
    return store, config, owner, contributor


@pytest.mark.parametrize("newer_status", ["failed", "unavailable"])
def test_pre_review_planner_recredits_newer_merge_unit_non_green_over_older_owner_green(
    tmp_path: Path,
    newer_status: str,
) -> None:
    store, config, owner, contributor = _setup_owner_green_contributor_non_green_merge_unit(
        tmp_path,
        newer_status,
    )
    lifecycle_git = _build_merge_unit_lifecycle_git(owner)

    action = evaluate_advance_rules(config, store, lifecycle_git, owner, "main")

    assert action["type"] == "reconcile_verify_gate_evidence"
    assert action["verify_owner_task"].id == owner.id
    assert action["verify_source_task"].id == contributor.id

    context = _build_verify_gate_merge_unit_context(tmp_path=tmp_path, store=store, config=config, owner=owner)
    result = execute_advance_action(task=owner, action=action, context=context)
    assert result.status == "success"

    refreshed_owner = store.get(owner.id)
    assert refreshed_owner is not None
    next_action = evaluate_advance_rules(config, store, lifecycle_git, refreshed_owner, "main")
    if newer_status == "failed":
        assert next_action["type"] == "create_verify_fix"
    else:
        assert next_action["type"] == "needs_discussion"
        assert next_action["needs_attention_reason"] == "verify-unavailable"
    assert next_action["type"] != "create_review"


@pytest.mark.parametrize("newer_status", ["failed", "unavailable"])
def test_pre_merge_planner_recredits_newer_merge_unit_non_green_over_older_owner_green(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    newer_status: str,
) -> None:
    store, config, owner, contributor = _setup_owner_green_contributor_non_green_merge_unit(
        tmp_path,
        newer_status,
    )
    _add_approved_review_for_merge(store, owner, monkeypatch)
    lifecycle_git = _build_merge_unit_lifecycle_git(owner)

    action = evaluate_advance_rules(config, store, lifecycle_git, owner, "main")

    assert action["type"] == "reconcile_verify_gate_evidence"
    assert action["verify_owner_task"].id == owner.id
    assert action["verify_source_task"].id == contributor.id

    context = _build_verify_gate_merge_unit_context(tmp_path=tmp_path, store=store, config=config, owner=owner)
    result = execute_advance_action(task=owner, action=action, context=context)
    assert result.status == "success"

    refreshed_owner = store.get(owner.id)
    assert refreshed_owner is not None
    next_action = evaluate_advance_rules(config, store, lifecycle_git, refreshed_owner, "main")
    if newer_status == "failed":
        assert next_action["type"] == "create_verify_fix"
    else:
        assert next_action["type"] == "verify_gate"
        assert next_action["verify_gate_state"] == "unavailable"
    assert next_action["type"] != "merge"


def test_explicit_contributor_lifecycle_uses_canonical_merge_unit_verify_owner(
    tmp_path: Path,
) -> None:
    store, config, owner, contributor = _setup_owner_green_contributor_non_green_merge_unit(
        tmp_path,
        "failed",
    )
    lifecycle_git = _build_merge_unit_lifecycle_git(owner)

    action = evaluate_advance_rules(config, store, lifecycle_git, contributor, "main")

    assert action["type"] == "reconcile_verify_gate_evidence"
    assert action["verify_owner_task"].id == owner.id
    assert action["verify_source_task"].id == contributor.id

    context = _build_verify_gate_merge_unit_context(tmp_path=tmp_path, store=store, config=config, owner=owner)
    result = execute_advance_action(task=contributor, action=action, context=context)

    refreshed_owner = store.get(owner.id)
    refreshed_contributor = store.get(contributor.id)
    assert refreshed_owner is not None
    assert refreshed_contributor is not None
    assert result.status == "success"
    assert refreshed_owner.review_verify_status == "failed"
    assert refreshed_contributor.review_verify_status is None
    lookup = latest_verify_result_for_epoch(
        store,
        refreshed_owner,
        current_epoch=owner_task_verify_epoch(refreshed_owner, config, context.git),
    )
    assert lookup.result is not None
    assert lookup.result.source_task_id == contributor.id

    next_action = evaluate_advance_rules(config, store, lifecycle_git, contributor, "main")
    assert next_action["type"] == "create_verify_fix"
    assert next_action["impl_task"].id == owner.id


def test_verify_gate_preserves_newer_owner_red_over_older_merge_unit_contributor_green(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    owner = store.add("Implement owner", task_type="implement")
    assert owner.id is not None
    _mark_completed(owner, branch="feature/merge-unit-newer-owner-red")
    store.update(owner)
    unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(owner.id, unit.id, "owner")
    contributor = _add_completed_merge_unit_member(
        store,
        prompt="Contributor older green",
        branch=owner.branch,
        unit_id=unit.id,
    )
    captured_at = datetime(2026, 8, 18, 10, 0, tzinfo=UTC)
    _persist_current_verify_gate_result(
        store=store,
        config=config,
        task=contributor,
        status="passed",
        captured_at=captured_at,
    )
    _persist_current_verify_gate_result(
        store=store,
        config=config,
        task=owner,
        status="failed",
        captured_at=captured_at + timedelta(minutes=1),
    )

    context = _build_verify_gate_merge_unit_context(tmp_path=tmp_path, store=store, config=config, owner=owner)

    with patch("gza.cli.advance_executor._run_lifecycle_verify", side_effect=AssertionError("verify should not rerun")):
        result = execute_advance_action(
            task=owner,
            action={
                "type": "verify_gate",
                "description": "Run verify gate before merge",
                "verify_gate_phase": "pre_merge",
                "verify_owner_task": owner,
            },
            context=context,
        )

    refreshed_owner = store.get(owner.id)
    assert refreshed_owner is not None
    assert result.status == "skip"
    assert result.attention_reason == "verify-gate-blocked"
    assert refreshed_owner.review_verify_status is None
    lookup = latest_verify_result_for_epoch(
        store,
        refreshed_owner,
        current_epoch=owner_task_verify_epoch(refreshed_owner, config, context.git),
    )
    assert lookup.result is not None
    assert lookup.result.status == "failed"
    assert lookup.result.source_task_id == owner.id


@pytest.mark.parametrize("newer_status", ["failed", "unavailable"])
def test_verify_gate_recredits_newer_merge_unit_non_green_over_older_contributor_green(
    tmp_path: Path,
    newer_status: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    owner = store.add("Implement owner", task_type="implement")
    assert owner.id is not None
    _mark_completed(owner, branch=f"feature/merge-unit-newer-{newer_status}")
    store.update(owner)
    unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(owner.id, unit.id, "owner")
    green_contributor = _add_completed_merge_unit_member(
        store,
        prompt="Contributor older green",
        branch=owner.branch,
        unit_id=unit.id,
    )
    non_green_contributor = _add_completed_merge_unit_member(
        store,
        prompt="Contributor newer non-green",
        branch=owner.branch,
        unit_id=unit.id,
    )
    captured_at = datetime(2026, 8, 18, 10, 0, tzinfo=UTC)
    _persist_current_verify_gate_result(
        store=store,
        config=config,
        task=green_contributor,
        status="passed",
        captured_at=captured_at,
    )
    _persist_current_verify_gate_result(
        store=store,
        config=config,
        task=non_green_contributor,
        status=newer_status,
        captured_at=captured_at + timedelta(minutes=1),
    )

    context = _build_verify_gate_merge_unit_context(tmp_path=tmp_path, store=store, config=config, owner=owner)

    with patch("gza.cli.advance_executor._run_lifecycle_verify", side_effect=AssertionError("verify should not rerun")):
        result = execute_advance_action(
            task=owner,
            action={
                "type": "verify_gate",
                "description": "Run verify gate before merge",
                "verify_gate_phase": "pre_merge",
                "verify_owner_task": owner,
            },
            context=context,
        )

    refreshed_owner = store.get(owner.id)
    assert refreshed_owner is not None
    assert result.status == "skip"
    assert result.attention_reason == "verify-gate-blocked"
    assert refreshed_owner.review_verify_status == newer_status
    lookup = latest_verify_result_for_epoch(
        store,
        refreshed_owner,
        current_epoch=owner_task_verify_epoch(refreshed_owner, config, context.git),
    )
    assert lookup.result is not None
    assert lookup.result.status == newer_status
    assert lookup.result.source_task_id == non_green_contributor.id


def test_completed_no_source_timeout_verify_fix_rerun_persists_green_then_plans_merge(tmp_path: Path, monkeypatch) -> None:
    from gza import advance_engine as advance_engine_module

    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0
    config.worktree_path.mkdir(parents=True, exist_ok=True)

    impl = store.add("Implement timeout verify fix", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/completed-timeout-verify-fix")
    impl.has_commits = True
    store.update(impl)
    review = store.add("Review timeout verify fix", task_type="review", depends_on=impl.id, based_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    review.output_content = "## Verdict\n\nVerdict: APPROVED\n"
    store.update(review)
    timeout_result = _make_review_verify_result(
        "./bin/tests",
        status="failed",
        exit_status="timed out",
        captured_at=datetime(2026, 8, 17, 10, 0, tzinfo=UTC),
        reviewed_branch=impl.branch,
        reviewed_head_sha="head-1",
        reviewed_base_sha="base-1",
        working_directory=str(tmp_path),
        failure="verify_command timed out after 120s",
        output="gza-verify phase=start name=unit\ngza-verify phase=failed name=unit duration_seconds=1.0",
    )
    persist_verify_gate_artifact(
        store,
        config,
        owner_task=impl,
        source_task=review,
        result=timeout_result,
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="review_verify",
    )
    verify_epoch = VerifyEpoch(
        reviewed_branch=impl.branch,
        reviewed_head_sha="head-1",
        verify_command="./bin/tests",
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
    )
    verify_fix = store.add(
        build_verify_fix_prompt(impl.id, verify_epoch),
        task_type="verify_fix",
        based_on=impl.id,
        same_branch=True,
    )
    assert verify_fix.id is not None
    verify_fix.slug = "20260817-completed-timeout-verify-fix"
    store.update(verify_fix)
    store.mark_completed(
        verify_fix,
        branch=impl.branch,
        has_commits=False,
        changed_diff=False,
        head_sha="head-1",
        base_sha="base-1",
    )
    verify_fix = store.get(verify_fix.id)
    assert verify_fix is not None
    assert verify_fix.review_verify_head_sha == "head-1"
    worktree_path = config.worktree_path / verify_fix.slug
    worktree_path.mkdir(parents=True)

    lifecycle_git = MagicMock()
    lifecycle_git.can_merge.return_value = True
    lifecycle_git.count_commits_behind.return_value = 0
    lifecycle_git.is_merged.return_value = False
    lifecycle_git.branch_exists.return_value = True
    lifecycle_git.ref_exists.return_value = False
    lifecycle_git.rev_parse_if_exists.side_effect = lambda ref: {"main": "base-1", impl.branch: "head-1"}.get(ref)
    lifecycle_git.is_ancestor.return_value = False
    lifecycle_git.count_commits_behind_checked.return_value = 0
    lifecycle_git.count_commits_ahead_checked.return_value = 1
    lifecycle_git.get_diff_name_status.return_value = ""
    lifecycle_git.resolve_fresh_merge_source.side_effect = lambda branch: ResolvedMergeSourceRef(branch)
    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review: SimpleNamespace(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    action = evaluate_advance_rules(config, store, lifecycle_git, impl, "main")
    assert action["type"] == "rerun_completed_verify_fix"

    worktree_git = MagicMock()
    worktree_git.repo_dir = worktree_path
    worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
        "HEAD": "head-1",
        "main": "base-1",
        impl.branch: "head-1",
    }.get(ref)
    worktree_git.status_porcelain.return_value = set()
    rerun_result = _make_review_verify_result(
        "./bin/tests",
        status="passed",
        exit_status="0",
        captured_at=datetime(2026, 8, 17, 10, 20, tzinfo=UTC),
        reviewed_branch=impl.branch,
        reviewed_head_sha="head-1",
        reviewed_base_sha="base-1",
        working_directory=str(worktree_path),
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
        spawn_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=lifecycle_git,
    )

    with (
        patch("gza.cli.advance_executor.Git", return_value=worktree_git),
        patch(
            "gza.runner._run_lifecycle_verify",
            return_value=SimpleNamespace(markdown="verify passed", aggregate_result=rerun_result, project_results=()),
        ),
    ):
        result = execute_advance_action(task=impl, action=action, context=context)

    assert result.status == "success"
    lookup = latest_verify_result_for_epoch(store, impl, current_epoch=verify_epoch)
    assert lookup.is_current
    assert lookup.result is not None
    assert lookup.result.status == "passed"
    assert lookup.result.source_task_id == verify_fix.id

    next_action = evaluate_advance_rules(config, store, lifecycle_git, impl, "main")
    assert next_action["type"] == "merge"


def test_completed_legacy_timeout_verify_fix_dry_run_does_not_persist_upgrade(tmp_path: Path, monkeypatch) -> None:
    from gza import advance_engine as advance_engine_module

    setup_config(tmp_path)
    (tmp_path / ".env").write_text("PROJECT_ONLY_TOKEN=captured-token\n", encoding="utf-8")
    monkeypatch.setenv("PATH", "/captured/bin")
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0
    config.worktree_path.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("PATH", "/ambient/bin")
    monkeypatch.setenv("PROJECT_ONLY_TOKEN", "ambient-token")
    monkeypatch.setenv("GZA_DB_PATH", str(tmp_path / "ambient.db"))

    impl = store.add("Implement timeout verify fix", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/completed-timeout-dry-run")
    impl.has_commits = True
    store.update(impl)
    review = store.add("Review timeout verify fix", task_type="review", depends_on=impl.id, based_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    review.output_content = "## Verdict\n\nVerdict: APPROVED\n"
    store.update(review)
    timeout_result = _make_review_verify_result(
        "./bin/tests",
        status="failed",
        exit_status="timed out",
        captured_at=datetime(2026, 8, 17, 10, 0, tzinfo=UTC),
        reviewed_branch=impl.branch,
        reviewed_head_sha="head-1",
        reviewed_base_sha="base-1",
        working_directory=str(tmp_path),
        failure="verify_command timed out after 120s",
        output="gza-verify phase=start name=unit\ngza-verify phase=failed name=unit duration_seconds=1.0",
    )
    persist_verify_gate_artifact(
        store,
        config,
        owner_task=impl,
        source_task=review,
        result=timeout_result,
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="review_verify",
    )
    verify_epoch = VerifyEpoch(
        reviewed_branch=impl.branch,
        reviewed_head_sha="head-1",
        verify_command="./bin/tests",
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
    )
    verify_fix = store.add(
        build_verify_fix_prompt(impl.id, verify_epoch),
        task_type="verify_fix",
        based_on=impl.id,
        same_branch=True,
    )
    assert verify_fix.id is not None
    verify_fix.status = "completed"
    verify_fix.branch = impl.branch
    verify_fix.slug = "20260817-completed-timeout-dry-run"
    verify_fix.has_commits = False
    store.update(verify_fix)
    (config.worktree_path / verify_fix.slug).mkdir(parents=True)

    lifecycle_git = MagicMock()
    lifecycle_git.can_merge.return_value = True
    lifecycle_git.count_commits_behind.return_value = 0
    lifecycle_git.is_merged.return_value = False
    lifecycle_git.branch_exists.return_value = True
    lifecycle_git.ref_exists.return_value = False
    lifecycle_git.rev_parse_if_exists.side_effect = lambda ref: {"main": "base-1", impl.branch: "head-1"}.get(ref)
    lifecycle_git.is_ancestor.return_value = False
    lifecycle_git.count_commits_behind_checked.return_value = 0
    lifecycle_git.count_commits_ahead_checked.return_value = 1
    lifecycle_git.get_diff_name_status.return_value = ""
    lifecycle_git.resolve_fresh_merge_source.side_effect = lambda branch: ResolvedMergeSourceRef(branch)
    lifecycle_git.status_porcelain.return_value = set()
    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review: SimpleNamespace(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    action = evaluate_advance_rules(config, store, lifecycle_git, impl, "main")
    assert action["type"] == "rerun_completed_verify_fix"
    assert action.get("legacy_completion_proof") is not None
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
        config=config,
        git=lifecycle_git,
    )

    result = execute_advance_action(task=impl, action=action, context=context)

    assert result.status == "dry_run"
    refreshed_fix = store.get(verify_fix.id)
    assert refreshed_fix is not None
    assert refreshed_fix.changed_diff is None
    assert refreshed_fix.review_verify_head_sha is None
    assert refreshed_fix.verify_fix_completion_outcome_json is None


def test_completed_legacy_timeout_verify_fix_rerun_uses_managed_worktree_not_dirty_canonical_checkout(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    setup_config(tmp_path)
    (tmp_path / ".env").write_text("PROJECT_ONLY_TOKEN=captured-token\n", encoding="utf-8")
    monkeypatch.setenv("PATH", "/captured/bin")
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0
    config.worktree_path.mkdir(parents=True, exist_ok=True)
    runtime_context = RuntimeExecutionContext.from_config(config)
    monkeypatch.setenv("PATH", "/ambient/bin")
    monkeypatch.setenv("PROJECT_ONLY_TOKEN", "ambient-token")
    monkeypatch.setenv("GZA_DB_PATH", str(tmp_path / "ambient.db"))

    impl = store.add("Implement timeout verify fix", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/completed-timeout-managed-clean")
    impl.has_commits = True
    store.update(impl)
    review = store.add("Review timeout source", task_type="review", depends_on=impl.id, based_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    review.output_content = "## Verdict\n\nVerdict: APPROVED\n"
    store.update(review)
    timeout_result = _make_review_verify_result(
        "./bin/tests",
        status="failed",
        exit_status="timed out",
        captured_at=datetime(2026, 8, 17, 10, 0, tzinfo=UTC),
        reviewed_branch=impl.branch,
        reviewed_head_sha="head-1",
        reviewed_base_sha="base-1",
        working_directory=str(tmp_path),
        failure="verify_command timed out after 120s",
        output="gza-verify phase=start name=unit\ngza-verify phase=failed name=unit duration_seconds=1.0",
    )
    persist_verify_gate_artifact(
        store,
        config,
        owner_task=impl,
        source_task=review,
        result=timeout_result,
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="review_verify",
    )
    verify_epoch = VerifyEpoch(
        reviewed_branch=impl.branch,
        reviewed_head_sha="head-1",
        verify_command="./bin/tests",
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
    )
    verify_fix = store.add(
        build_verify_fix_prompt(impl.id, verify_epoch),
        task_type="verify_fix",
        based_on=impl.id,
        same_branch=True,
    )
    assert verify_fix.id is not None
    verify_fix.status = "completed"
    verify_fix.branch = impl.branch
    verify_fix.slug = "20260817-completed-timeout-managed-clean"
    verify_fix.has_commits = False
    store.update(verify_fix)
    worktree_path = config.worktree_path / verify_fix.slug
    worktree_path.mkdir(parents=True)
    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review: SimpleNamespace(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    lifecycle_git = MagicMock()
    lifecycle_git.can_merge.return_value = True
    lifecycle_git.count_commits_behind.return_value = 0
    lifecycle_git.is_merged.return_value = False
    lifecycle_git.branch_exists.return_value = True
    lifecycle_git.ref_exists.return_value = False
    lifecycle_git.rev_parse_if_exists.side_effect = lambda ref: {"main": "base-1", impl.branch: "head-1"}.get(ref)
    lifecycle_git.is_ancestor.return_value = False
    lifecycle_git.count_commits_behind_checked.return_value = 0
    lifecycle_git.count_commits_ahead_checked.return_value = 1
    lifecycle_git.get_diff_name_status.return_value = ""
    lifecycle_git.resolve_fresh_merge_source.side_effect = lambda branch: ResolvedMergeSourceRef(branch)
    lifecycle_git.status_porcelain.return_value = {("M", "src/local.py")}
    lifecycle_git.env = runtime_context.env

    action = evaluate_advance_rules(config, store, lifecycle_git, impl, "main")
    assert action["type"] == "rerun_completed_verify_fix"
    assert action.get("legacy_completion_proof") is not None

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
        config=config,
        git=lifecycle_git,
        runtime_context=runtime_context,
    )
    worktree_git = MagicMock()
    worktree_git.repo_dir = worktree_path
    worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
        "HEAD": "head-1",
        "main": "base-1",
        impl.branch: "head-1",
    }.get(ref)
    worktree_git.status_porcelain.return_value = set()
    rerun_result = _make_review_verify_result(
        "./bin/tests",
        status="passed",
        exit_status="0",
        captured_at=datetime(2026, 8, 17, 10, 20, tzinfo=UTC),
        reviewed_branch=impl.branch,
        reviewed_head_sha="head-1",
        reviewed_base_sha="base-1",
        working_directory=str(worktree_path),
    )

    proof_git_envs: list[dict[str, str] | None] = []

    def _proof_git_factory(repo_dir: Path, *, env: dict[str, str] | None = None):
        assert repo_dir == worktree_path
        proof_git_envs.append(dict(env) if env is not None else None)
        return worktree_git

    with (
        patch("gza.cli.advance_executor.Git", side_effect=_proof_git_factory),
        patch(
            "gza.runner._run_lifecycle_verify",
            return_value=SimpleNamespace(markdown="verify", aggregate_result=rerun_result, project_results=()),
        ),
    ):
        result = execute_advance_action(task=impl, action=action, context=context)

    assert result.status == "success"
    refreshed_fix = store.get(verify_fix.id)
    assert refreshed_fix is not None
    outcome = effective_verify_fix_completion_outcome(refreshed_fix)
    assert outcome is not None
    assert outcome.recovery_rerun_attempted is True
    assert proof_git_envs
    assert all(env == runtime_context.env for env in proof_git_envs)
    assert proof_git_envs[0]["PATH"] == "/captured/bin"
    assert proof_git_envs[0]["PROJECT_ONLY_TOKEN"] == "captured-token"
    assert proof_git_envs[0]["GZA_DB_PATH"] == str(config.db_path.resolve())
    next_action = evaluate_advance_rules(config, store, lifecycle_git, impl, "main")
    assert next_action["type"] == "merge"


@pytest.mark.parametrize("rerun_status", ["passed", "failed"])
def test_completed_no_source_timeout_verify_fix_rerun_atomic_persistence_failure_stays_retryable(
    tmp_path: Path,
    rerun_status: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0
    config.worktree_path.mkdir(parents=True, exist_ok=True)

    impl = store.add("Implement timeout verify fix", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/completed-timeout-consume-fails")
    impl.has_commits = True
    store.update(impl)
    source = store.add("Verify timeout source", task_type="review", depends_on=impl.id, based_on=impl.id)
    assert source.id is not None
    _mark_completed(source)
    source.output_content = "## Verdict\n\nVerdict: APPROVED\n"
    store.update(source)
    timeout_result = _make_review_verify_result(
        "./bin/tests",
        status="failed",
        exit_status="timed out",
        captured_at=datetime(2026, 8, 17, 10, 0, tzinfo=UTC),
        reviewed_branch=impl.branch,
        reviewed_head_sha="head-1",
        reviewed_base_sha="base-1",
        working_directory=str(tmp_path),
        failure="verify_command timed out after 120s",
        output="gza-verify phase=start name=unit\ngza-verify phase=failed name=unit duration_seconds=1.0",
    )
    persist_verify_gate_artifact(
        store,
        config,
        owner_task=impl,
        source_task=source,
        result=timeout_result,
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="review_verify",
    )
    verify_epoch = VerifyEpoch(
        reviewed_branch=impl.branch,
        reviewed_head_sha="head-1",
        verify_command="./bin/tests",
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
    )
    verify_fix = store.add(
        build_verify_fix_prompt(impl.id, verify_epoch),
        task_type="verify_fix",
        based_on=impl.id,
        same_branch=True,
    )
    assert verify_fix.id is not None
    verify_fix.status = "completed"
    verify_fix.branch = impl.branch
    verify_fix.slug = "20260817-completed-timeout-consume-fails"
    verify_fix.changed_diff = False
    verify_fix.review_verify_head_sha = "head-1"
    store.update(verify_fix)
    worktree_path = config.worktree_path / verify_fix.slug
    worktree_path.mkdir(parents=True)
    before_artifact_count = len(store.list_artifacts(impl.id, kind="verify_gate_result"))

    lifecycle_git = MagicMock()
    lifecycle_git.can_merge.return_value = True
    lifecycle_git.count_commits_behind.return_value = 0
    lifecycle_git.is_merged.return_value = False
    lifecycle_git.branch_exists.return_value = True
    lifecycle_git.ref_exists.return_value = False
    lifecycle_git.rev_parse_if_exists.side_effect = lambda ref: {"main": "base-1", impl.branch: "head-1"}.get(ref)
    lifecycle_git.is_ancestor.return_value = False
    lifecycle_git.count_commits_behind_checked.return_value = 0
    lifecycle_git.count_commits_ahead_checked.return_value = 1
    lifecycle_git.get_diff_name_status.return_value = ""
    lifecycle_git.resolve_fresh_merge_source.side_effect = lambda branch: ResolvedMergeSourceRef(branch)
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
        config=config,
        git=lifecycle_git,
    )
    worktree_git = MagicMock()
    worktree_git.repo_dir = worktree_path
    worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
        "HEAD": "head-1",
        "main": "base-1",
        impl.branch: "head-1",
    }.get(ref)
    worktree_git.status_porcelain.return_value = set()
    rerun_result = _make_review_verify_result(
        "./bin/tests",
        status=rerun_status,
        exit_status="0" if rerun_status == "passed" else "timed out",
        captured_at=datetime(2026, 8, 17, 10, 20, tzinfo=UTC),
        reviewed_branch=impl.branch,
        reviewed_head_sha="head-1",
        reviewed_base_sha="base-1",
        working_directory=str(worktree_path),
        failure=None if rerun_status == "passed" else "verify_command timed out after 120s",
        output=(
            None
            if rerun_status == "passed"
            else "gza-verify phase=start name=unit\ngza-verify phase=failed name=unit duration_seconds=1.0"
        ),
    )
    with (
        patch("gza.cli.advance_executor.Git", return_value=worktree_git),
        patch(
            "gza.runner._run_lifecycle_verify",
            return_value=SimpleNamespace(markdown="verify", aggregate_result=rerun_result, project_results=()),
        ),
        patch(
            "gza.runner.persist_verify_gate_artifact_with_verify_fix_outcome",
            side_effect=RuntimeError("simulated atomic verify persistence failure"),
        ),
    ):
        result = execute_advance_action(
            task=impl,
            action={
                "type": "rerun_completed_verify_fix",
                "verify_fix_task": verify_fix,
                "verify_owner_task": impl,
                "verify_epoch": verify_epoch,
                "verify_base_sha": "base-1",
            },
            context=context,
        )

    assert result.status == "skip"
    assert result.attention_type == "needs_discussion"
    assert result.attention_reason == "verify-fix-proof-unavailable"
    assert "simulated atomic verify persistence failure" in result.message
    assert len(store.list_artifacts(impl.id, kind="verify_gate_result")) == before_artifact_count
    refreshed_fix = store.get(verify_fix.id)
    assert refreshed_fix is not None
    outcome = effective_verify_fix_completion_outcome(refreshed_fix)
    assert outcome is not None
    assert outcome.no_source_changes is True
    assert outcome.completion_head_sha == "head-1"
    assert outcome.recovery_rerun_attempted is False

    next_action = evaluate_advance_rules(config, store, lifecycle_git, impl, "main")
    assert next_action["type"] == "rerun_completed_verify_fix"
    assert next_action.get("needs_attention_reason") != "verify-fix-failed"


def test_completed_legacy_timeout_verify_fix_executor_branch_proof_failure_is_proof_unavailable(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.worktree_path.mkdir(parents=True, exist_ok=True)
    impl = store.add("Implement timeout verify fix", task_type="implement")
    assert impl.id is not None
    impl.branch = "feature/legacy-branch-proof"
    store.update(impl)
    verify_fix = store.add("Legacy verify fix", task_type="verify_fix", based_on=impl.id, same_branch=True)
    assert verify_fix.id is not None
    verify_fix.status = "completed"
    verify_fix.branch = impl.branch
    verify_fix.slug = "20260817-legacy-branch-proof"
    store.update(verify_fix)
    (config.worktree_path / verify_fix.slug).mkdir(parents=True)
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
        config=config,
        git=MagicMock(),
    )
    worktree_git = MagicMock()
    worktree_git.rev_parse_if_exists.side_effect = GitError("branch ref probe failed")

    with patch("gza.cli.advance_executor.Git", return_value=worktree_git):
        result = execute_advance_action(
            task=impl,
            action={
                "type": "rerun_completed_verify_fix",
                "verify_fix_task": verify_fix,
                "verify_owner_task": impl,
                "verify_epoch": VerifyEpoch(
                    reviewed_branch=impl.branch,
                    reviewed_head_sha="head-1",
                    verify_command="./bin/tests",
                    verify_timeout_seconds=120,
                    verify_timeout_grace_seconds=5.0,
                ),
                "legacy_completion_proof": SimpleNamespace(
                    branch_name=impl.branch,
                    expected_head_sha="head-1",
                ),
            },
            context=context,
        )

    assert result.status == "skip"
    assert result.attention_reason == "verify-fix-proof-unavailable"
    assert "branch ref probe failed" in result.message
    assert result.attention_reason != "verify-fix-failed"


def test_completed_legacy_timeout_verify_fix_executor_refuses_dirty_managed_worktree(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.worktree_path.mkdir(parents=True, exist_ok=True)
    impl = store.add("Implement timeout verify fix", task_type="implement")
    assert impl.id is not None
    impl.branch = "feature/legacy-managed-dirty"
    store.update(impl)
    verify_fix = store.add("Legacy verify fix", task_type="verify_fix", based_on=impl.id, same_branch=True)
    assert verify_fix.id is not None
    verify_fix.status = "completed"
    verify_fix.branch = impl.branch
    verify_fix.slug = "20260817-legacy-managed-dirty"
    store.update(verify_fix)
    worktree_path = config.worktree_path / verify_fix.slug
    worktree_path.mkdir(parents=True)
    canonical_git = MagicMock()
    canonical_git.status_porcelain.return_value = set()
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
        config=config,
        git=canonical_git,
    )
    worktree_git = MagicMock()
    worktree_git.rev_parse_if_exists.return_value = "head-1"
    worktree_git.status_porcelain.return_value = {("M", "src/dirty.py")}

    with (
        patch("gza.cli.advance_executor.Git", return_value=worktree_git),
        patch("gza.cli.advance_executor.persist_verify_fix_completion_outcome") as persist_outcome,
        patch("gza.runner._run_lifecycle_verify") as run_verify,
    ):
        result = execute_advance_action(
            task=impl,
            action={
                "type": "rerun_completed_verify_fix",
                "verify_fix_task": verify_fix,
                "verify_owner_task": impl,
                "verify_epoch": VerifyEpoch(
                    reviewed_branch=impl.branch,
                    reviewed_head_sha="head-1",
                    verify_command="./bin/tests",
                    verify_timeout_seconds=120,
                    verify_timeout_grace_seconds=5.0,
                ),
                "legacy_completion_proof": SimpleNamespace(
                    branch_name=impl.branch,
                    expected_head_sha="head-1",
                ),
            },
            context=context,
        )

    assert result.status == "skip"
    assert result.attention_reason == "verify-fix-proof-unavailable"
    assert "clean managed worktree" in result.message
    persist_outcome.assert_not_called()
    run_verify.assert_not_called()
    refreshed_fix = store.get(verify_fix.id)
    assert refreshed_fix is not None
    assert refreshed_fix.verify_fix_completion_outcome_json is None
    canonical_git.status_porcelain.assert_not_called()


def test_completed_legacy_timeout_verify_fix_upgrade_persistence_failure_is_proof_unavailable(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.worktree_path.mkdir(parents=True, exist_ok=True)
    impl = store.add("Implement timeout verify fix", task_type="implement")
    assert impl.id is not None
    impl.branch = "feature/legacy-upgrade-persistence-fails"
    store.update(impl)
    verify_fix = store.add("Legacy verify fix", task_type="verify_fix", based_on=impl.id, same_branch=True)
    assert verify_fix.id is not None
    verify_fix.status = "completed"
    verify_fix.branch = impl.branch
    verify_fix.slug = "20260817-legacy-upgrade-persistence-fails"
    verify_fix.has_commits = False
    store.update(verify_fix)
    worktree_path = config.worktree_path / verify_fix.slug
    worktree_path.mkdir(parents=True)
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
        config=config,
        git=MagicMock(),
    )
    worktree_git = MagicMock()
    worktree_git.rev_parse_if_exists.return_value = "head-1"
    worktree_git.status_porcelain.return_value = set()
    original_update = store.update

    def fail_verify_fix_update(task: DbTask) -> None:
        if task.id == verify_fix.id and task.verify_fix_completion_outcome_json is not None:
            raise RuntimeError("simulated legacy upgrade write failure")
        original_update(task)

    with (
        patch("gza.cli.advance_executor.Git", return_value=worktree_git),
        patch.object(store, "update", side_effect=fail_verify_fix_update),
        patch("gza.runner._run_lifecycle_verify", side_effect=AssertionError("verify should not rerun")),
    ):
        result = execute_advance_action(
            task=impl,
            action={
                "type": "rerun_completed_verify_fix",
                "verify_fix_task": verify_fix,
                "verify_owner_task": impl,
                "verify_epoch": VerifyEpoch(
                    reviewed_branch=impl.branch,
                    reviewed_head_sha="head-1",
                    verify_command="./bin/tests",
                    verify_timeout_seconds=120,
                    verify_timeout_grace_seconds=5.0,
                ),
                "legacy_completion_proof": SimpleNamespace(
                    branch_name=impl.branch,
                    expected_head_sha="head-1",
                ),
            },
            context=context,
        )

    assert result.status == "skip"
    assert result.attention_type == "needs_discussion"
    assert result.attention_reason == "verify-fix-proof-unavailable"
    assert "simulated legacy upgrade write failure" in result.message
    refreshed_fix = store.get(verify_fix.id)
    assert refreshed_fix is not None
    assert refreshed_fix.changed_diff is None
    assert refreshed_fix.review_verify_head_sha is None
    assert refreshed_fix.verify_fix_completion_outcome_json is None
    assert result.created_task is not None
    assert result.created_task.verify_fix_completion_outcome_json is None


@pytest.mark.parametrize("refusal", ["dirty", "missing_head", "missing_worktree", "rerun_failed", "rerun_timeout"])
def test_completed_no_source_timeout_verify_fix_rerun_refusals_do_not_clear_gate(
    tmp_path: Path,
    refusal: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0
    config.worktree_path.mkdir(parents=True, exist_ok=True)

    impl = store.add("Implement timeout verify fix", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/completed-timeout-refusal")
    impl.has_commits = True
    store.update(impl)
    source = store.add("Verify timeout source", task_type="review", depends_on=impl.id, based_on=impl.id)
    assert source.id is not None
    _mark_completed(source)
    source.output_content = "## Verdict\n\nVerdict: APPROVED\n"
    store.update(source)
    timeout_result = _make_review_verify_result(
        "./bin/tests",
        status="failed",
        exit_status="timed out",
        captured_at=datetime(2026, 8, 17, 10, 0, tzinfo=UTC),
        reviewed_branch=impl.branch,
        reviewed_head_sha="head-1",
        reviewed_base_sha="base-1",
        working_directory=str(tmp_path),
        failure="verify_command timed out after 120s",
        output="gza-verify phase=start name=unit\ngza-verify phase=failed name=unit duration_seconds=1.0",
    )
    persist_verify_gate_artifact(
        store,
        config,
        owner_task=impl,
        source_task=source,
        result=timeout_result,
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="review_verify",
    )
    verify_epoch = VerifyEpoch(
        reviewed_branch=impl.branch,
        reviewed_head_sha="head-1",
        verify_command="./bin/tests",
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
    )
    verify_fix = store.add(
        build_verify_fix_prompt(impl.id, verify_epoch),
        task_type="verify_fix",
        based_on=impl.id,
        same_branch=True,
    )
    assert verify_fix.id is not None
    verify_fix.status = "completed"
    verify_fix.branch = impl.branch
    verify_fix.slug = f"20260817-completed-timeout-{refusal}"
    verify_fix.changed_diff = False
    verify_fix.review_verify_head_sha = None if refusal == "missing_head" else "head-1"
    store.update(verify_fix)
    worktree_path = config.worktree_path / verify_fix.slug
    if refusal != "missing_worktree":
        worktree_path.mkdir(parents=True)

    lifecycle_git = MagicMock()
    lifecycle_git.can_merge.return_value = True
    lifecycle_git.count_commits_behind.return_value = 0
    lifecycle_git.is_merged.return_value = False
    lifecycle_git.branch_exists.return_value = True
    lifecycle_git.ref_exists.return_value = False
    lifecycle_git.rev_parse_if_exists.side_effect = lambda ref: {"main": "base-1", impl.branch: "head-1"}.get(ref)
    lifecycle_git.is_ancestor.return_value = False
    lifecycle_git.count_commits_behind_checked.return_value = 0
    lifecycle_git.count_commits_ahead_checked.return_value = 1
    lifecycle_git.get_diff_name_status.return_value = ""
    lifecycle_git.resolve_fresh_merge_source.side_effect = lambda branch: ResolvedMergeSourceRef(branch)
    lifecycle_git.status_porcelain.return_value = set()
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
        config=config,
        git=lifecycle_git,
    )
    worktree_git = MagicMock()
    worktree_git.repo_dir = worktree_path
    worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
        "HEAD": "head-1",
        "main": "base-1",
        impl.branch: "head-1",
    }.get(ref)
    worktree_git.status_porcelain.return_value = {("M", "src/dirty.py")} if refusal == "dirty" else set()
    rerun_result = _make_review_verify_result(
        "./bin/tests",
        status="failed" if refusal in {"rerun_failed", "rerun_timeout"} else "passed",
        exit_status="timed out" if refusal == "rerun_timeout" else ("7" if refusal == "rerun_failed" else "0"),
        captured_at=datetime(2026, 8, 17, 10, 20, tzinfo=UTC),
        reviewed_branch=impl.branch,
        reviewed_head_sha="head-1",
        reviewed_base_sha="base-1",
        working_directory=str(worktree_path),
        failure=(
            "verify_command timed out after 120s"
            if refusal == "rerun_timeout"
            else ("pytest failed" if refusal == "rerun_failed" else None)
        ),
        output=(
            "gza-verify phase=start name=unit\ngza-verify phase=failed name=unit duration_seconds=1.0"
            if refusal == "rerun_timeout"
            else None
        ),
    )

    with (
        patch("gza.cli.advance_executor.Git", return_value=worktree_git) as git_ctor,
        patch(
            "gza.runner._run_lifecycle_verify",
            return_value=SimpleNamespace(markdown="verify", aggregate_result=rerun_result, project_results=()),
        ) as run_verify,
    ):
        result = execute_advance_action(
            task=impl,
            action={
                "type": "rerun_completed_verify_fix",
                "verify_fix_task": verify_fix,
                "verify_owner_task": impl,
                "verify_epoch": verify_epoch,
                "verify_base_sha": "base-1",
            },
            context=context,
        )

    assert result.status == "skip"
    lookup = latest_verify_result_for_epoch(store, impl, current_epoch=verify_epoch)
    assert lookup.is_current
    assert lookup.result is not None
    assert lookup.result.status == "failed"
    if refusal in {"missing_head", "missing_worktree", "dirty"}:
        assert lookup.result.source_task_id == source.id
    if refusal == "missing_head":
        git_ctor.assert_not_called()
        run_verify.assert_not_called()
    elif refusal in {"missing_worktree", "dirty"}:
        run_verify.assert_not_called()
    else:
        run_verify.assert_called_once()
    if refusal == "rerun_timeout":
        next_action = evaluate_advance_rules(config, store, lifecycle_git, impl, "main")
        assert next_action["type"] == "needs_discussion"
        assert next_action["needs_attention_reason"] == "verify-fix-failed"
        assert "already consumed its exact-head recovery rerun" in next_action["description"]


def test_create_verify_fix_action_creates_and_spawns_worker(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement verify_fix", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/create-verify-fix")
    store.update(impl)
    verify_fix = DbTask(
        id="testproject-verify-fix",
        prompt="Fix verify failures",
        status="pending",
        task_type="verify_fix",
        based_on=impl.id,
    )
    spawned: list[str] = []
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
        spawn_worker=lambda task, _kind: spawned.append(task.id) or 0,
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
        git=SimpleNamespace(),
    )

    with (
        patch("gza.cli.advance_executor.create_or_reuse_verify_fix_task", return_value=(verify_fix, True)),
        patch(
            "gza.cli.advance_executor._prepare_background_start",
            return_value=(verify_fix, None),
        ),
    ):
        result = execute_advance_action(
            task=impl,
            action={
                "type": "create_verify_fix",
                "description": "Create verify_fix task",
                "impl_task": impl,
                "based_on_task": impl,
                "verify_epoch": VerifyEpoch(
                    reviewed_branch=impl.branch,
                    reviewed_head_sha="head-1",
                    verify_command="./bin/tests",
                    verify_timeout_seconds=120,
                    verify_timeout_grace_seconds=5.0,
                ),
            },
            context=context,
        )

    assert result.status == "success"
    assert result.created_task is not None
    assert result.created_task.task_type == "verify_fix"
    assert spawned == [result.created_task.id]


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


def test_prepare_resolution_review_action_persists_rebase_completion_target_after_target_moves(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/resolution-target-moved")
    store.update(impl)

    rebase = store.add("Rebase feature", task_type="rebase", based_on=impl.id, same_branch=True)
    assert rebase.id is not None
    _mark_completed(rebase, branch=impl.branch)
    rebase.review_scope = (
        "Rebase diff provenance: yes\n"
        "Pre-rebase head SHA: old-head\n"
        "Pre-rebase target SHA: target-before\n"
        "Pre-rebase merge-base SHA: old-base\n"
        "Resolved head SHA: rebased-head\n"
        "Resolved target SHA: target-at-rebase\n"
        "Recovered baseline: no"
    )
    store.update(rebase)

    result = _prepare_resolution_review_action(
        store,
        impl,
        {
            "type": "create_review",
            "review_mode": "resolution",
            "resolution_rebase_task_id": rebase.id,
            "resolution_head_sha": "rebased-head",
            "resolution_target_sha": "target-at-rebase",
        },
        trigger_source="manual",
    )

    assert result.status == "created"
    assert result.review_task is not None
    persisted = store.get(result.review_task.id)
    assert persisted is not None
    assert persisted.review_scope is not None
    assert "Resolved head SHA: rebased-head" in persisted.review_scope
    assert "Resolved target SHA: target-at-rebase" in persisted.review_scope


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


@pytest.mark.parametrize("action_type", ["resume", "retry"])
def test_duplicate_singleton_recovery_action_skips_and_releases_reserved_launch_permit(
    tmp_path: Path,
    action_type: str,
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "max_concurrent: 1\n")
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.branch = "feature/canonical-rebased"
    failed = store.add(
        "Failed rebase",
        task_type="rebase",
        based_on=impl.id,
        same_branch=True,
        base_branch="main",
    )
    assert failed.id is not None
    failed.branch = "feature/orphan-rebased"
    failed.status = "failed"
    failed.failure_reason = "WORKER_DIED" if action_type == "resume" else "INFRASTRUCTURE_ERROR"
    failed.session_id = "resume-session-1" if action_type == "resume" else None
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    active_child = store.add(
        f"Pending {action_type}",
        task_type="rebase",
        based_on=failed.id,
        same_branch=True,
        branch=impl.branch,
        base_branch="main",
        enforce_single_active_sibling=True,
    )
    assert active_child.id is not None

    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=1,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("unused"),
        create_resume_task=(
            (lambda _task: (_ for _ in ()).throw(DuplicateActiveChildError(active_child)))
            if action_type == "resume"
            else lambda _task: pytest.fail("unused")
        ),
        create_retry_task=(
            (lambda _task: (_ for _ in ()).throw(DuplicateActiveChildError(active_child)))
            if action_type == "retry"
            else lambda _task: pytest.fail("unused")
        ),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        config=config,
    )

    result = execute_advance_action(
        task=failed,
        action={"type": action_type, "launch_mode": "worker"},
        context=context,
    )

    assert result.status == "skip"
    assert result.worker_consuming is False
    assert result.work_done is False
    assert result.message == f"SKIP: rebase already pending/in progress for branch {impl.branch}: {active_child.id}"
    assert "feature/orphan-rebased" not in result.message
    permit = launch_permit(config, store)
    permit.release()


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


def test_needs_rebase_recovery_preflight_uses_explicit_rebase_parent_task(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/recovery-preflight-parent")
    store.update(impl)

    failed_improve = store.add(
        "Failed improve",
        task_type="improve",
        based_on=impl.id,
        same_branch=True,
    )
    assert failed_improve.id is not None
    failed_improve.status = "failed"
    failed_improve.branch = impl.branch
    store.update(failed_improve)

    created_from: list[str] = []
    spawned: list[tuple[str, str]] = []

    def _create_rebase(parent: DbTask) -> DbTask:
        assert parent.id is not None
        created_from.append(parent.id)
        return store.add(
            prompt=f"Rebase {parent.id}",
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
        create_retry_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=_create_rebase,
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda task_obj, kind: spawned.append((str(task_obj.id), kind)) or 0,
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
    )

    result = execute_advance_action(
        task=failed_improve,
        action={
            "type": "needs_rebase",
            "reason": "recovery-preflight-rebase",
            "rebase_parent_task_id": impl.id,
        },
        context=context,
    )

    assert result.status == "success"
    assert created_from == [impl.id]
    assert result.created_task is not None
    assert result.created_task.based_on == impl.id
    assert spawned == [(result.created_task.id, "rebase")]


@pytest.mark.parametrize("use_iterate_for_needs_rebase", [False, True])
def test_needs_rebase_uses_canonical_parent_from_general_pre_dispatch_action(
    tmp_path: Path,
    use_iterate_for_needs_rebase: bool,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/pre-dispatch-parent")
    store.update(impl)

    descendant = store.add(
        "Completed improve",
        task_type="improve",
        based_on=impl.id,
        same_branch=True,
    )
    assert descendant.id is not None
    _mark_completed(descendant, branch=impl.branch)
    store.update(descendant)

    created_from: list[str] = []
    already_merged_checked: list[str] = []
    spawned: list[tuple[str | None, str, str | None]] = []

    def _create_rebase(parent: DbTask) -> DbTask:
        assert parent.id is not None
        created_from.append(parent.id)
        return store.add(
            prompt=f"Rebase {parent.id}",
            task_type="rebase",
            based_on=parent.id,
            same_branch=True,
        )

    def _spawn_iterate(
        task_obj: DbTask,
        kind: str,
        *,
        prepared_task: DbTask | None = None,
        prepared_phase: str | None = None,
        prepared_action_type: str | None = None,
    ) -> int:
        spawned.append((task_obj.id, kind, prepared_task.id if prepared_task else None))
        assert prepared_phase == "iteration"
        assert prepared_action_type == "needs_rebase"
        return 0

    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=1,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=use_iterate_for_needs_rebase,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("unused"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_retry_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=_create_rebase,
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda task_obj, kind: spawned.append((task_obj.id, kind, None)) or 0,
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=_spawn_iterate,
        is_rebase_target_already_merged=lambda task_obj: already_merged_checked.append(str(task_obj.id)) or False,
    )

    result = execute_advance_action(
        task=descendant,
        action={
            "type": "needs_rebase",
            "reason": "pre-dispatch-rebase",
            "rebase_parent_task_id": impl.id,
            "rebase_parent_branch": impl.branch,
        },
        context=context,
    )

    assert result.status == "success"
    assert created_from == [impl.id]
    assert already_merged_checked == [impl.id]
    assert result.created_task is not None
    assert result.created_task.based_on == impl.id
    if use_iterate_for_needs_rebase:
        assert spawned == [(impl.id, "rebase", result.created_task.id)]
        assert result.worker_label == "iterate"
    else:
        assert spawned == [(result.created_task.id, "rebase", None)]
        assert result.worker_label == "rebase"


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


def test_needs_rebase_duplicate_active_rebase_skips_and_releases_capacity(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "max_concurrent: 1\n")
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/needs-rebase-duplicate")
    store.update(impl)

    active_rebase = store.add(
        "Rebase",
        task_type="rebase",
        based_on=impl.id,
        same_branch=True,
        branch=impl.branch,
    )
    assert active_rebase.id is not None

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
        create_rebase_task=lambda _task: (_ for _ in ()).throw(DuplicateActiveChildError(active_rebase)),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda *_args, **_kwargs: pytest.fail("unused"),
        config=config,
    )

    result = execute_advance_action(task=impl, action={"type": "needs_rebase"}, context=context)

    assert result.status == "skip"
    assert result.worker_consuming is False
    assert result.work_done is False
    assert result.message == f"SKIP: rebase already pending/in progress for branch {impl.branch}: {active_rebase.id}"
    permit = launch_permit(config, store)
    permit.release()


def test_needs_rebase_orphan_recovery_duplicate_skips_and_releases_capacity(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "max_concurrent: 1\n")
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/impl")
    store.update(impl)

    failed_rebase = store.add(
        "Failed rebase",
        task_type="rebase",
        based_on=impl.id,
        same_branch=True,
        branch="feature/failed-rebase",
        base_branch="main",
    )
    assert failed_rebase.id is not None
    failed_rebase.status = "failed"
    failed_rebase.failure_reason = "WORKER_DIED"
    failed_rebase.completed_at = datetime.now(UTC)
    store.update(failed_rebase)

    active_orphan_retry = store.add(
        "Active orphan retry",
        task_type="rebase",
        based_on=failed_rebase.id,
        same_branch=True,
        branch="feature/orphan-retry",
        base_branch="main",
    )
    assert active_orphan_retry.id is not None

    def _create_guarded_rebase(parent: DbTask) -> DbTask:
        assert parent.id is not None
        assert parent.branch is not None
        return _create_rebase_task(
            store,
            parent.id,
            parent.branch,
            "main",
            trigger_source="manual",
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
        create_rebase_task=_create_guarded_rebase,
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task, _kind: pytest.fail("duplicate skip must not spawn a worker"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda *_args, **_kwargs: pytest.fail("unused"),
        config=config,
    )

    result = execute_advance_action(task=impl, action={"type": "needs_rebase"}, context=context)

    assert result.status == "skip"
    assert result.worker_consuming is False
    assert result.work_done is False
    assert result.message == (
        f"SKIP: rebase already pending/in progress for branch {impl.branch}: {active_orphan_retry.id}"
    )
    assert {task.id for task in store.get_all() if task.task_type == "rebase"} == {
        failed_rebase.id,
        active_orphan_retry.id,
    }
    permit = launch_permit(config, store)
    permit.release()


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


@pytest.mark.parametrize(
    ("action", "worker_label"),
    [
        ({"type": "create_plan_review"}, "plan_review"),
        ({"type": "create_plan_improve"}, "plan_improve"),
        ({"type": "create_implement", "plan_review_cycle_limit_reached": True}, "implement"),
        ({"type": "needs_rebase"}, "rebase"),
        ({"type": "resume", "launch_mode": "worker"}, "resume"),
        ({"type": "retry", "launch_mode": "worker"}, "retry"),
    ],
)
def test_advance_creation_config_error_is_structured_and_releases_permit(
    tmp_path: Path,
    action: dict[str, object],
    worker_label: str,
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text(encoding="utf-8") + "max_concurrent: 1\n", encoding="utf-8")
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Source", task_type="plan" if action["type"] in {"create_plan_review", "create_plan_improve", "create_implement"} else "implement")
    assert task.id is not None
    _mark_completed(task, branch="feature/source")
    if action["type"] in {"resume", "retry"}:
        task.status = "failed"
        task.failure_reason = "WORKER_DIED"
        task.completed_at = datetime.now(UTC)
        if action["type"] == "resume":
            task.session_id = "session-123"
    store.update(task)

    review = store.add("Plan review", task_type="plan_review", depends_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    store.update(review)
    if action["type"] == "create_plan_improve":
        action = {**action, "plan_review_task": review, "plan_source_task": task}

    config_error = ConfigError(f"task type '{worker_label}' with provider 'codex' is uncovered")

    def _raise_config_error(*_args: object, **_kwargs: object) -> DbTask:
        raise config_error

    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=1,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda _task, _rollback: pytest.fail("prepare must not run"),
        prepare_create_review=lambda _task: pytest.fail("unused"),
        create_resume_task=_raise_config_error,
        create_retry_task=_raise_config_error,
        create_rebase_task=_raise_config_error,
        create_implement_task=_raise_config_error,
        create_plan_review_task=_raise_config_error,
        create_plan_improve_task=_raise_config_error,
        spawn_worker=lambda _task, _kind: pytest.fail("worker spawn must not run"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("resume spawn must not run"),
        spawn_iterate_worker=lambda *_args, **_kwargs: pytest.fail("iterate spawn must not run"),
        config=config,
    )

    before_ids = {existing.id for existing in store.get_all()}
    result = execute_advance_action(task=task, action=action, context=context)

    assert result.status == "error"
    assert result.execution_phase == "worker_launch"
    assert result.worker_consuming is False
    assert "with provider 'codex'" in result.message
    assert {existing.id for existing in store.get_all()} == before_ids
    permit = launch_permit(config, store)
    permit.release()


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
        runtime_context=RuntimeExecutionContext.from_config(config),
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
        runtime_context=RuntimeExecutionContext.from_config(config),
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


def test_reconcile_branch_divergence_skips_pr_publication_when_open_pr_known(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Implement feature", task_type="implement", create_pr=True)
    assert task.id is not None
    task.status = "failed"
    task.failure_reason = "BRANCH_UNPUSHABLE"
    task.branch = "feature/reconcile-open-pr"
    task.has_commits = True
    task.output_content = "summary"
    task.diff_files_changed = 2
    task.diff_lines_added = 5
    task.diff_lines_removed = 1
    task.pr_state = "open"
    task.pr_number = 17
    task.completed_at = datetime.now(UTC)
    store.update(task)

    config = Config.load(tmp_path)
    git = SimpleNamespace(
        default_branch=lambda: "main",
        count_commits_ahead=lambda *_args: 1,
        rev_parse_if_exists=lambda ref: {
            "feature/reconcile-open-pr": "head123",
            "main": "base456",
        }.get(ref),
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
        spawn_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        reconcile_diverged_branch=lambda _task: BranchDivergenceReconcileResult(
            status="reconciled",
            message="Reconciled 'feature/reconcile-open-pr' with --force-with-lease",
        ),
        config=config,
        git=git,
        runtime_context=RuntimeExecutionContext.from_config(config),
    )

    with (
        patch("gza.runner.ensure_task_pr", side_effect=AssertionError("ensure_task_pr should not run")) as ensure_pr,
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
    ensure_pr.assert_not_called()
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.status == "completed"
    assert refreshed.failure_reason is None
    assert refreshed.pr_state == "open"
    assert refreshed.pr_number == 17


def test_reconcile_branch_divergence_stale_wip_savepoint_becomes_pushable_and_completes(
    tmp_path: Path,
) -> None:
    from gza.cli.git_ops import _reconcile_diverged_branch_with_origin
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Implement stale WIP reconcile", task_type="implement", create_pr=True)
    assert task.id is not None
    task.status = "failed"
    task.failure_reason = "BRANCH_UNPUSHABLE"
    task.branch = "feature/stale-wip-e2e"
    task.has_commits = True
    task.log_file = "logs/stale-wip-e2e.log"
    task.output_content = "summary"
    task.diff_files_changed = 2
    task.diff_lines_added = 5
    task.diff_lines_removed = 1
    task.completed_at = datetime.now(UTC)
    store.update(task)

    config = Config.load(tmp_path)
    git = MagicMock(spec=Git)
    git.branch_exists.return_value = True
    git.default_branch.return_value = "main"
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "origin/feature/stale-wip-e2e": "remote-wip-tip",
        "feature/stale-wip-e2e": "local-final-tip",
        "main": "base456",
    }.get(ref)
    git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(
        None,
        (
            "Local branch 'feature/stale-wip-e2e' and remote-tracking ref "
            "'origin/feature/stale-wip-e2e' diverged. Push, fetch, or reconcile them "
            "before advancing or merging."
        ),
    )
    git.count_commits_ahead.side_effect = [1, 1, 1]
    git.is_merged.side_effect = [True, False]
    git._run.side_effect = [
        SimpleNamespace(returncode=0, stdout="merge-base-oid\n"),
        SimpleNamespace(returncode=0, stdout="WIP: gza task interrupted\n"),
    ]
    ensure_result = SimpleNamespace(ok=True, status="created", error=None, pr_url="https://example.test/pr/4492")

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
        runtime_context=RuntimeExecutionContext.from_config(config),
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
                    attempt_limit=2,
                ),
            },
            context=context,
        )

    assert result.status == "success"
    assert "force-with-lease" in result.message
    git.push_ref_force_with_lease.assert_called_once_with(
        "feature/stale-wip-e2e",
        "feature/stale-wip-e2e",
        remote="origin",
        expected_remote_oid="remote-wip-tip",
    )
    ensure_pr.assert_called_once()
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.status == "completed"
    assert refreshed.failure_reason is None
    assert git._run.call_args_list == [
        call("merge-base", "feature/stale-wip-e2e", "origin/feature/stale-wip-e2e", check=False),
        call(
            "log",
            "--format=%s",
            "merge-base-oid..origin/feature/stale-wip-e2e",
            "--not",
            "feature/stale-wip-e2e",
            check=False,
        ),
    ]


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
        runtime_context=RuntimeExecutionContext.from_config(config),
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
        runtime_context=RuntimeExecutionContext.from_config(config),
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
        runtime_context=RuntimeExecutionContext.from_config(config),
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
        runtime_context=RuntimeExecutionContext.from_config(config),
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


def test_reconcile_branch_divergence_duplicate_targeted_rebase_skips_and_releases_capacity(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "max_concurrent: 1\n")
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/reconcile-duplicate")
    store.update(impl)

    active_rebase = store.add(
        "Rebase",
        task_type="rebase",
        based_on=impl.id,
        same_branch=True,
        branch=impl.branch,
    )
    assert active_rebase.id is not None

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
        create_targeted_rebase_task=lambda _task, _target: (_ for _ in ()).throw(DuplicateActiveChildError(active_rebase)),
        spawn_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda *_task, **_kwargs: pytest.fail("unused"),
        reconcile_diverged_branch=lambda _task: BranchDivergenceReconcileResult(
            status="needs_rebase",
            message="Mechanical rebase conflicted",
            rebase_target="main",
        ),
        config=config,
    )

    result = execute_advance_action(
        task=impl,
        action={"type": "reconcile_branch_divergence"},
        context=context,
    )

    assert result.status == "skip"
    assert result.worker_consuming is False
    assert result.work_done is False
    assert result.message == f"SKIP: rebase already pending/in progress for branch {impl.branch}: {active_rebase.id}"
    permit = launch_permit(config, store)
    permit.release()


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


def test_reconcile_branch_divergence_non_benign_remote_conflict_parks_without_pr_required(
    tmp_path: Path,
) -> None:
    from gza.cli.git_ops import _reconcile_diverged_branch_with_origin
    from gza.rebase_diff import RebaseDiffBaseline

    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Recover genuine remote conflict", task_type="implement")
    assert task.id is not None
    task.status = "failed"
    task.failure_reason = "BRANCH_UNPUSHABLE"
    task.branch = "feature/non-benign-conflict"
    task.has_commits = True
    task.completed_at = datetime.now(UTC)
    store.update(task)

    config = Config.load(tmp_path)
    git = MagicMock(spec=Git)
    git.branch_exists.return_value = True
    git.default_branch.return_value = "main"
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "origin/feature/non-benign-conflict": "remote-tip",
        "feature/non-benign-conflict": "local-tip",
    }.get(ref)
    git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(
        None,
        (
            "Local branch 'feature/non-benign-conflict' and remote-tracking ref "
            "'origin/feature/non-benign-conflict' diverged. Push, fetch, or reconcile them "
            "before advancing or merging."
        ),
    )
    git.count_commits_ahead.side_effect = [1, 1]
    git.is_merged.side_effect = [True, False]
    git._run.side_effect = [
        SimpleNamespace(returncode=0, stdout="merge-base-oid\n"),
        SimpleNamespace(returncode=0, stdout="External commit\n"),
    ]

    worktree_git = MagicMock(spec=Git)
    worktree_git.rebase.side_effect = GitError("conflict")

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
        runtime_context=RuntimeExecutionContext.from_config(config),
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=worktree_git),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline("old", "target", "base"),
        ),
    ):
        result = execute_advance_action(
            task=task,
            action={"type": "reconcile_branch_divergence"},
            context=context,
        )

    assert result.status == "skip"
    assert result.attention_reason == "reconcile-needs-manual-resolution"
    assert "PR_REQUIRED" not in result.message
    attention = resolve_execution_needs_attention(task, result)
    assert attention is not None
    assert attention.action["needs_attention_reason"] == "reconcile-needs-manual-resolution"


def test_prepare_spec_coherence_review_action_creates_after_ordinary_review_completed(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Update behavior spec", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/spec-coherence-executor")
    impl.tags = ("lineage-tag",)
    store.update(impl)

    ordinary_review = store.add("Ordinary review", task_type="review", depends_on=impl.id)
    assert ordinary_review.id is not None
    _mark_completed(ordinary_review)
    store.update(ordinary_review)

    result = _prepare_spec_coherence_review_action(
        store,
        impl,
        {
            "review_mode": "spec_coherence",
            "review_head_sha": "head123",
            "review_changed_paths": ("specs/behavior/lifecycle-engine.md",),
        },
        trigger_source="advance",
    )

    assert result.status == "created"
    assert result.review_task is not None
    assert "Review mode: spec-coherence" in (result.review_task.review_scope or "")
    assert "Reviewed head SHA: head123" in (result.review_task.review_scope or "")
    assert result.review_task.tags == ("lineage-tag", "spec-coherence", "specs-behavior")
    assert "`## Verdict`" in result.review_task.prompt
