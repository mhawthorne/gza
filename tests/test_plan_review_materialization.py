"""Tests for durable plan-review slice materialization state."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from gza.config import Config
from gza.db import NewTaskParams, SqliteTaskStore
from gza.plan_review_materialization import (
    PLAN_REVIEW_ARTIFACT_SCHEMA_VERSION,
    PLAN_REVIEW_MATERIALIZATION_ARTIFACT_KIND,
    build_plan_review_slice_task_specs,
    load_materialized_plan_slice_set,
    plan_review_manifest_digest,
)
from gza.plan_review_verdict import validate_plan_review_report


def _make_store(tmp_path: Path) -> SqliteTaskStore:
    (tmp_path / "gza.yaml").write_text("project_name: test-project\n")
    config = Config.load(tmp_path)
    db_path = tmp_path / ".gza" / "gza.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return SqliteTaskStore(db_path, prefix=config.project_prefix)


def _base_manifest(source_task_id: str) -> dict:
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
                "out_of_scope": [],
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


def _validated_manifest(source_task_id: str):
    manifest = _base_manifest(source_task_id)
    report = (
        "## Verdict\nVerdict: APPROVED\n\n## Slice Manifest\n```json\n"
        f"{json.dumps(manifest)}\n```\n"
    )
    return validate_plan_review_report(
        report,
        source_task_id=source_task_id,
        source_task_type="plan",
        max_slice_timeout_minutes=30,
    )


def _persist_materialization_artifact(
    store: SqliteTaskStore,
    *,
    review_id: str,
    plan_id: str,
    manifest,
    tasks: list[NewTaskParams],
    trigger_source: str = "plan-review",
) -> None:
    store.add_tasks_with_artifact_atomic(
        tasks=tasks,
        artifact_task_id=review_id,
        artifact_kind=PLAN_REVIEW_MATERIALIZATION_ARTIFACT_KIND,
        artifact_label="plan_review_materialization",
        artifact_path=".gza/artifacts/materialized.txt",
        artifact_byte_size=0,
        artifact_sha256="",
        artifact_metadata_builder=lambda created_tasks: {
            "schema_version": PLAN_REVIEW_ARTIFACT_SCHEMA_VERSION,
            "review_task_id": review_id,
            "source_task_id": plan_id,
            "source_task_type": "plan",
            "manifest_digest": plan_review_manifest_digest(manifest),
            "trigger_source": trigger_source,
            "task_ids": [task.id for task in created_tasks if task.id is not None],
        },
    )


@pytest.mark.parametrize(
    "task_ids_builder",
    [
        pytest.param(lambda task_ids: task_ids[:1], id="incomplete-task-id-list"),
        pytest.param(lambda task_ids: [task_ids[0], task_ids[0]], id="duplicate-task-id-list"),
    ],
)
def test_load_materialized_plan_slice_set_rejects_incomplete_or_duplicate_task_ids(
    tmp_path: Path,
    task_ids_builder,
) -> None:
    store = _make_store(tmp_path)

    plan = store.add("Plan ingestion options", task_type="plan")
    review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    assert plan.id is not None
    assert review.id is not None
    manifest = _validated_manifest(plan.id)
    assert manifest is not None

    materialized_tasks = store.add_tasks_with_artifact_atomic(
        tasks=[
            NewTaskParams(prompt="Implement slice S1.", task_type="implement", based_on=plan.id),
            NewTaskParams(prompt="Implement slice S2.", task_type="implement", based_on=plan.id),
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
            "manifest_digest": plan_review_manifest_digest(manifest),
            "task_ids": task_ids_builder([task.id for task in tasks if task.id is not None]),
        },
    )
    assert len(materialized_tasks) == 2

    loaded = load_materialized_plan_slice_set(
        store,
        review_task=review,
        plan_source_task=plan,
        manifest=manifest,
    )

    assert loaded is None


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
def test_load_materialized_plan_slice_set_rejects_task_metadata_that_does_not_match_manifest(
    tmp_path: Path,
    mismatch_kind: str,
) -> None:
    store = _make_store(tmp_path)

    plan = store.add("Plan ingestion options", task_type="plan")
    review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    assert plan.id is not None
    assert review.id is not None
    manifest = _validated_manifest(plan.id)
    assert manifest is not None
    task_specs = build_plan_review_slice_task_specs(
        plan_source_task=plan,
        review_task=review,
        manifest=manifest,
        trigger_source="plan-review",
        require_review_before_merge=True,
    )

    if mismatch_kind == "wrong-task-type":
        task_specs[0] = NewTaskParams(**{**task_specs[0].__dict__, "task_type": "review"})
    elif mismatch_kind == "wrong-trigger-source":
        task_specs[0] = NewTaskParams(**{**task_specs[0].__dict__, "trigger_source": "manual"})
    elif mismatch_kind == "wrong-slice-wiring":
        task_specs[1] = NewTaskParams(
            **{
                **task_specs[1].__dict__,
                "based_on": plan.id,
                "depends_on": None,
                "same_branch": False,
            }
        )
    elif mismatch_kind == "wrong-prompt":
        task_specs[0] = NewTaskParams(**{**task_specs[0].__dict__, "prompt": "Wrong prompt"})
    elif mismatch_kind == "wrong-review-scope":
        task_specs[0] = NewTaskParams(**{**task_specs[0].__dict__, "review_scope": "Wrong scope"})
    elif mismatch_kind == "wrong-tags":
        task_specs[0] = NewTaskParams(**{**task_specs[0].__dict__, "tags": ()})
    elif mismatch_kind == "wrong-create-review":
        task_specs[0] = NewTaskParams(**{**task_specs[0].__dict__, "create_review": False})
    else:
        raise AssertionError(f"Unhandled mismatch kind: {mismatch_kind}")

    _persist_materialization_artifact(
        store,
        review_id=review.id,
        plan_id=plan.id,
        manifest=manifest,
        tasks=[
            NewTaskParams(
                prompt=task_specs[0].prompt,
                task_type=task_specs[0].task_type,
                based_on=plan.id,
                depends_on=task_specs[0].depends_on,
                tags=task_specs[0].tags,
                review_scope=task_specs[0].review_scope,
                create_review=task_specs[0].create_review,
                same_branch=task_specs[0].same_branch,
                trigger_source=task_specs[0].trigger_source,
            ),
            NewTaskParams(
                prompt=task_specs[1].prompt,
                task_type=task_specs[1].task_type,
                based_on=task_specs[1].based_on,
                depends_on=task_specs[1].depends_on,
                tags=task_specs[1].tags,
                review_scope=task_specs[1].review_scope,
                create_review=task_specs[1].create_review,
                same_branch=task_specs[1].same_branch,
                trigger_source=task_specs[1].trigger_source,
            ),
        ],
    )

    loaded = load_materialized_plan_slice_set(
        store,
        review_task=review,
        plan_source_task=plan,
        manifest=manifest,
    )

    assert loaded is None


def test_load_materialized_plan_slice_set_reuses_legacy_manual_materialization_without_trigger_metadata(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)

    plan = store.add("Plan ingestion options", task_type="plan")
    review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    assert plan.id is not None
    assert review.id is not None
    manifest = _validated_manifest(plan.id)
    assert manifest is not None
    legacy_schema_version = 1
    task_specs = build_plan_review_slice_task_specs(
        plan_source_task=plan,
        review_task=review,
        manifest=manifest,
        trigger_source="manual",
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
            "schema_version": legacy_schema_version,
            "review_task_id": review.id,
            "source_task_id": plan.id,
            "source_task_type": "plan",
            "manifest_digest": plan_review_manifest_digest(manifest),
            "task_ids": [task.id for task in tasks if task.id is not None],
        },
    )

    loaded = load_materialized_plan_slice_set(
        store,
        review_task=review,
        plan_source_task=plan,
        manifest=manifest,
    )

    assert loaded is not None
    assert [task.id for task in loaded] == [task.id for task in created_tasks]


def test_build_plan_review_slice_task_specs_maps_linear_dependencies_and_branch_continuation(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)

    plan = store.add("Plan ingestion options", task_type="plan")
    review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    assert plan.id is not None
    assert review.id is not None
    manifest = _validated_manifest(plan.id)
    assert manifest is not None

    task_specs = build_plan_review_slice_task_specs(
        plan_source_task=plan,
        review_task=review,
        manifest=manifest,
        trigger_source="plan-review",
        require_review_before_merge=True,
    )

    assert len(task_specs) == 2
    assert task_specs[0].based_on == plan.id
    assert task_specs[0].depends_on is None
    assert task_specs[0].same_branch is False
    assert task_specs[0].create_review is True
    assert task_specs[0].review_scope == "Parser only."
    assert task_specs[0].trigger_source == "plan-review"
    assert task_specs[1].depends_on == "__new_task_idx__:0"
    assert task_specs[1].based_on == "__new_task_idx__:0"
    assert task_specs[1].same_branch is True
    assert task_specs[1].create_review is True


def test_build_plan_review_slice_task_specs_keeps_independent_ordered_slice_on_plan_source_branch(
    tmp_path: Path,
) -> None:
    store = _make_store(tmp_path)

    plan = store.add("Plan ingestion options", task_type="plan")
    review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
    assert plan.id is not None
    assert review.id is not None
    manifest = _validated_manifest(plan.id)
    assert manifest is not None

    independent_manifest = replace(
        manifest,
        slices=(
            manifest.slices[0],
            replace(manifest.slices[1], based_on_slice=None),
        ),
    )

    task_specs = build_plan_review_slice_task_specs(
        plan_source_task=plan,
        review_task=review,
        manifest=independent_manifest,
        trigger_source="plan-review",
        require_review_before_merge=True,
    )

    assert task_specs[1].depends_on == "__new_task_idx__:0"
    assert task_specs[1].based_on == plan.id
    assert task_specs[1].same_branch is False
