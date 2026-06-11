"""Shared helpers for approved plan-review slice materialization state."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict
from hashlib import sha256
from typing import TypedDict

from gza.db import NewTaskParams, SqliteTaskStore, Task
from gza.plan_review_verdict import PlanReviewManifest

PLAN_REVIEW_MATERIALIZATION_ARTIFACT_KIND = "plan_review_materialization"
PLAN_REVIEW_ARTIFACT_SCHEMA_VERSION = 2
_LEGACY_PLAN_REVIEW_ARTIFACT_SCHEMA_VERSION = 1


class _MaterializedSliceAttributes(TypedDict):
    trigger_source: str
    create_review: bool


def plan_review_manifest_digest(manifest: PlanReviewManifest) -> str:
    """Return the stable digest used to identify one validated slice manifest."""
    payload = json.dumps(asdict(manifest), indent=2, sort_keys=True) + "\n"
    return sha256(payload.encode("utf-8")).hexdigest()


def build_plan_review_slice_task_specs(
    *,
    plan_source_task: Task,
    review_task: Task,
    manifest: PlanReviewManifest,
    trigger_source: str,
    require_review_before_merge: bool,
) -> list[NewTaskParams]:
    """Build the exact implement-task specs for one approved slice manifest."""
    assert plan_source_task.id is not None
    assert review_task.id is not None

    task_specs: list[NewTaskParams] = []
    slice_task_index_by_id: dict[str, int] = {}
    for slice_manifest in manifest.slices:
        depends_on_task_id = None
        if slice_manifest.depends_on_slices:
            depends_on_task_id = f"__new_task_idx__:{slice_task_index_by_id[slice_manifest.depends_on_slices[0]]}"

        based_on_task_id = plan_source_task.id
        same_branch = False
        if slice_manifest.based_on_slice is not None:
            based_on_task_id = f"__new_task_idx__:{slice_task_index_by_id[slice_manifest.based_on_slice]}"
            same_branch = True

        prompt = _build_plan_review_slice_prompt(
            plan_source_task_id=plan_source_task.id,
            review_task_id=review_task.id,
            slice_manifest=slice_manifest,
        )

        task_specs.append(
            NewTaskParams(
                prompt=prompt,
                task_type="implement",
                depends_on=depends_on_task_id,
                based_on=based_on_task_id,
                same_branch=same_branch,
                tags=tuple(dict.fromkeys((*plan_source_task.tags, *slice_manifest.tags))),
                review_scope=slice_manifest.review_scope,
                create_review=require_review_before_merge,
                trigger_source=trigger_source,
            )
        )
        slice_task_index_by_id[slice_manifest.slice_id] = len(task_specs) - 1

    return task_specs


def _build_plan_review_slice_prompt(
    *,
    plan_source_task_id: str,
    review_task_id: str,
    slice_manifest: object,
) -> str:
    """Build the compact provenance prompt for one materialized implement slice."""
    slice_id = getattr(slice_manifest, "slice_id")
    title = getattr(slice_manifest, "title")
    prompt = getattr(slice_manifest, "prompt")
    scope = getattr(slice_manifest, "scope")
    out_of_scope = getattr(slice_manifest, "out_of_scope")
    acceptance_criteria = getattr(slice_manifest, "acceptance_criteria")

    lines = [
        f"Implement approved plan-review slice {slice_id}: {title}",
        "",
        "Provenance:",
        f"- Plan source: {plan_source_task_id}",
        f"- Plan review: {review_task_id}",
        f"- Slice: {slice_id} ({title})",
        "",
        "Slice prompt:",
        prompt,
        "",
        "Scope:",
        *(f"- {item}" for item in scope),
    ]
    if out_of_scope:
        lines.extend(
            [
                "",
                "Out of scope:",
                *(f"- {item}" for item in out_of_scope),
            ]
        )
    lines.extend(
        [
            "",
            "Acceptance criteria:",
            *(f"- {item}" for item in acceptance_criteria),
        ]
    )
    return "\n".join(lines)


def load_materialized_plan_slice_set(
    store: SqliteTaskStore,
    *,
    review_task: Task,
    plan_source_task: Task,
    manifest: PlanReviewManifest,
) -> list[Task] | None:
    """Return the existing non-dropped materialized slice set for this manifest."""
    if review_task.id is None or plan_source_task.id is None:
        return None
    expected_digest = plan_review_manifest_digest(manifest)
    artifacts = sorted(
        store.list_artifacts(review_task.id, kind=PLAN_REVIEW_MATERIALIZATION_ARTIFACT_KIND),
        key=lambda artifact: artifact.created_at,
        reverse=True,
    )
    for artifact in artifacts:
        metadata = artifact.metadata or {}
        schema_version = metadata.get("schema_version")
        if schema_version not in {
            PLAN_REVIEW_ARTIFACT_SCHEMA_VERSION,
            _LEGACY_PLAN_REVIEW_ARTIFACT_SCHEMA_VERSION,
        }:
            continue
        if metadata.get("review_task_id") != review_task.id:
            continue
        if metadata.get("source_task_id") != plan_source_task.id:
            continue
        if metadata.get("manifest_digest") != expected_digest:
            continue
        task_ids = metadata.get("task_ids")
        if not isinstance(task_ids, list) or not task_ids:
            continue
        if len(task_ids) != len(manifest.slices):
            continue
        if any(not isinstance(task_id, str) for task_id in task_ids):
            continue
        if len(set(task_ids)) != len(task_ids):
            continue
        materialized_tasks: list[Task] = []
        for task_id in task_ids:
            materialized_task = store.get(task_id)
            if materialized_task is None or materialized_task.status == "dropped":
                materialized_tasks = []
                break
            materialized_tasks.append(materialized_task)
        persisted_attributes = _resolve_materialized_slice_attributes(
            metadata=metadata,
            materialized_tasks=materialized_tasks,
        )
        if materialized_tasks and persisted_attributes is not None and _materialized_tasks_match_manifest(
            materialized_tasks=materialized_tasks,
            manifest=manifest,
            plan_source_task=plan_source_task,
            review_task=review_task,
            expected_trigger_source=persisted_attributes["trigger_source"],
            expected_create_review=persisted_attributes["create_review"],
        ):
            return materialized_tasks
    return None


def _materialized_tasks_match_manifest(
    *,
    materialized_tasks: list[Task],
    manifest: PlanReviewManifest,
    plan_source_task: Task,
    review_task: Task,
    expected_trigger_source: str,
    expected_create_review: bool,
) -> bool:
    """Return whether persisted tasks exactly match the expected manifest slice set."""
    if plan_source_task.id is None or len(materialized_tasks) != len(manifest.slices):
        return False

    task_ids = [task.id for task in materialized_tasks]
    if any(task_id is None for task_id in task_ids):
        return False
    expected_task_specs = build_plan_review_slice_task_specs(
        plan_source_task=plan_source_task,
        review_task=review_task,
        manifest=manifest,
        trigger_source=expected_trigger_source,
        require_review_before_merge=expected_create_review,
    )

    for expected_spec, task in zip(expected_task_specs, materialized_tasks, strict=True):
        expected_depends_on = _resolve_materialized_task_ref(expected_spec.depends_on, task_ids)
        expected_based_on = _resolve_materialized_task_ref(expected_spec.based_on, task_ids)

        if task.prompt != expected_spec.prompt:
            return False
        if task.task_type != expected_spec.task_type:
            return False
        if task.trigger_source != expected_spec.trigger_source:
            return False
        if task.depends_on != expected_depends_on:
            return False
        if task.based_on != expected_based_on:
            return False
        if task.same_branch != expected_spec.same_branch:
            return False
        if task.review_scope != expected_spec.review_scope:
            return False
        if task.tags != expected_spec.tags:
            return False
        if task.create_review != expected_spec.create_review:
            return False

    return True


def _resolve_materialized_slice_attributes(
    *,
    metadata: Mapping[str, object],
    materialized_tasks: list[Task],
) -> _MaterializedSliceAttributes | None:
    """Resolve durable slice attributes from artifact metadata or persisted tasks."""
    if not materialized_tasks:
        return None
    trigger_source = metadata.get("trigger_source")
    if not isinstance(trigger_source, str) or not trigger_source:
        trigger_values = {task.trigger_source for task in materialized_tasks if task.trigger_source}
        if len(trigger_values) != 1:
            return None
        trigger_source = next(iter(trigger_values))

    create_review = metadata.get("create_review")
    if not isinstance(create_review, bool):
        create_review_values = {task.create_review for task in materialized_tasks}
        if len(create_review_values) != 1:
            return None
        create_review = next(iter(create_review_values))

    return {
        "trigger_source": trigger_source,
        "create_review": create_review,
    }


def _resolve_materialized_task_ref(value: str | None, task_ids: list[str | None]) -> str | None:
    """Resolve __new_task_idx__ placeholders against a persisted created-task list."""
    if value is None:
        return None
    if not value.startswith("__new_task_idx__:"):
        return value
    index = int(value.split(":", 1)[1])
    resolved = task_ids[index]
    if resolved is None:
        raise ValueError(f"materialized task at index {index} is missing an id")
    return resolved
