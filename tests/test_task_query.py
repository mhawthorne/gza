"""Tests for unified task query service."""

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from gza.db import SqliteTaskStore
from gza.task_query import (
    DateFilter,
    ProjectionSpec,
    TaskProjectionPreset,
    TaskQuery,
    TaskQueryPresets,
    TaskQueryService,
)


def _store(tmp_path: Path) -> SqliteTaskStore:
    return SqliteTaskStore(tmp_path / "test.db")


def test_search_default_matches_pending_and_internal(tmp_path: Path) -> None:
    store = _store(tmp_path)
    pending = store.add("needle pending", task_type="implement")
    internal = store.add("needle internal", task_type="internal")

    service = TaskQueryService(store)
    result = service.run(TaskQueryPresets.search("needle", limit=None))

    prompts = [row.task.prompt for row in result.rows if hasattr(row, "task")]
    assert pending.prompt in prompts
    assert internal.prompt in prompts


def test_date_filter_completed_excludes_rows_without_completed_at(tmp_path: Path) -> None:
    store = _store(tmp_path)
    pending = store.add("needle pending", task_type="implement")
    pending.created_at = datetime.now(UTC)
    store.update(pending)

    completed = store.add("needle completed", task_type="implement")
    completed.status = "completed"
    completed.created_at = datetime.now(UTC) - timedelta(days=7)
    completed.completed_at = datetime.now(UTC)
    store.update(completed)

    service = TaskQueryService(store)
    query = TaskQueryPresets.search(
        "needle",
        limit=None,
        date_filter=DateFilter(field="completed", start=date.today()),
    )
    result = service.run(query)

    prompts = [row.task.prompt for row in result.rows if hasattr(row, "task")]
    assert "needle completed" in prompts
    assert "needle pending" not in prompts


def test_incomplete_preset_projects_next_action_fields(tmp_path: Path) -> None:
    store = _store(tmp_path)
    failed = store.add("failed impl", task_type="implement")
    failed.status = "failed"
    failed.completed_at = datetime.now(UTC)
    failed.failure_reason = "TEST_FAILURE"
    store.update(failed)

    service = TaskQueryService(store)
    result = service.run(TaskQueryPresets.incomplete(limit=None))

    assert len(result.rows) == 1
    row = result.rows[0]
    assert hasattr(row, "owner_task")
    assert row.values["next_action"] == "unknown"
    assert "missing config/git context" in str(row.values["next_action_reason"])


def test_merge_chain_unmerged_matches_legacy_unmerged_status(tmp_path: Path) -> None:
    store = _store(tmp_path)
    legacy = store.add("legacy unmerged", task_type="implement")
    legacy.status = "unmerged"
    legacy.completed_at = datetime.now(UTC)
    legacy.has_commits = True
    store.update(legacy)

    service = TaskQueryService(store)
    query = TaskQuery(
        statuses=("completed", "unmerged"),
        merge_chain_state=("unmerged",),
        lifecycle_state=("terminal",),
        limit=None,
    )
    result = service.run(query)

    prompts = [row.task.prompt for row in result.rows if hasattr(row, "task")]
    assert "legacy unmerged" in prompts


def test_projection_fields_override_applies_to_task_and_lineage_json(tmp_path: Path) -> None:
    store = _store(tmp_path)
    task = store.add("needle task", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    store.update(task)

    failed = store.add("needle failed", task_type="implement")
    failed.status = "failed"
    failed.completed_at = datetime.now(UTC)
    failed.failure_reason = "TEST_FAILURE"
    store.update(failed)

    service = TaskQueryService(store)

    task_query = replace(
        TaskQueryPresets.search("needle", limit=None),
        projection=ProjectionSpec(fields=("id", "status")),
    )
    task_json = service.run(task_query).to_json()
    assert task_json
    assert set(task_json[0].keys()) == {"id", "status"}

    lineage_query = replace(
        TaskQueryPresets.incomplete(limit=None),
        projection=ProjectionSpec(fields=("id", "next_action")),
    )
    lineage_json = service.run(lineage_query).to_json()
    assert lineage_json
    assert set(lineage_json[0].keys()) == {"id", "next_action"}


def test_projection_preset_override_changes_output_shape(tmp_path: Path) -> None:
    store = _store(tmp_path)
    task = store.add("needle", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    store.update(task)

    service = TaskQueryService(store)
    default_json = service.run(TaskQueryPresets.search("needle", limit=None)).to_json()

    minimal_query = replace(
        TaskQueryPresets.search("needle", limit=None),
        projection=ProjectionSpec(preset=TaskProjectionPreset.JSON_MINIMAL),
    )
    minimal_json = service.run(minimal_query).to_json()

    assert default_json and minimal_json
    assert set(default_json[0].keys()) != set(minimal_json[0].keys())
    assert set(minimal_json[0].keys()) == {"id", "prompt", "status", "task_type"}


def test_incomplete_date_field_created_vs_effective_affects_lineage_selection(tmp_path: Path) -> None:
    store = _store(tmp_path)

    stale_failed = store.add("stale failed", task_type="implement")
    stale_failed.status = "failed"
    stale_failed.created_at = datetime.now(UTC)
    stale_failed.completed_at = datetime.now(UTC) - timedelta(days=5)
    stale_failed.failure_reason = "TEST_FAILURE"
    store.update(stale_failed)

    service = TaskQueryService(store)
    created_result = service.run(
        TaskQueryPresets.incomplete(
            limit=None,
            date_filter=DateFilter(field="created", days=1),
        )
    )
    effective_result = service.run(
        TaskQueryPresets.incomplete(
            limit=None,
            date_filter=DateFilter(field="effective", days=1),
        )
    )

    assert len(created_result.rows) == 1
    assert len(effective_result.rows) == 0


def test_incomplete_date_field_completed_excludes_missing_completed_at(tmp_path: Path) -> None:
    store = _store(tmp_path)

    failed_no_completed = store.add("failed unresolved", task_type="implement")
    failed_no_completed.status = "failed"
    failed_no_completed.created_at = datetime.now(UTC)
    failed_no_completed.completed_at = None
    failed_no_completed.failure_reason = "TEST_FAILURE"
    store.update(failed_no_completed)

    service = TaskQueryService(store)
    created_result = service.run(
        TaskQueryPresets.incomplete(
            limit=None,
            date_filter=DateFilter(field="created", days=1),
        )
    )
    completed_result = service.run(
        TaskQueryPresets.incomplete(
            limit=None,
            date_filter=DateFilter(field="completed", days=1),
        )
    )

    assert len(created_result.rows) == 1
    assert len(completed_result.rows) == 0


def test_lineages_incomplete_rejects_multi_task_type_filter(tmp_path: Path) -> None:
    store = _store(tmp_path)
    failed = store.add("failed unresolved", task_type="implement")
    failed.status = "failed"
    failed.completed_at = datetime.now(UTC)
    failed.failure_reason = "TEST_FAILURE"
    store.update(failed)

    service = TaskQueryService(store)
    query = TaskQuery(
        scope="lineages",
        lifecycle_state=("incomplete",),
        task_types=("implement", "review"),
        limit=None,
    )

    with pytest.raises(
        ValueError,
        match="lineages scope with lifecycle_state=incomplete supports at most one task type",
    ):
        service.run(query)


def test_incomplete_limit_applies_once_at_owner_row_level(tmp_path: Path) -> None:
    store = _store(tmp_path)

    root_a = store.add("Root A owner old", task_type="implement")
    root_a.status = "completed"
    root_a.completed_at = datetime.now(UTC) - timedelta(days=30)
    root_a.has_commits = True
    root_a.merge_status = "merged"
    store.update(root_a)
    assert root_a.id is not None

    recent_failed_descendant = store.add(
        "Recent unresolved descendant on A",
        task_type="improve",
        based_on=root_a.id,
        same_branch=True,
    )
    recent_failed_descendant.status = "failed"
    recent_failed_descendant.completed_at = datetime.now(UTC) - timedelta(hours=1)
    recent_failed_descendant.failure_reason = "TEST_FAILURE"
    store.update(recent_failed_descendant)

    root_b = store.add("Root B owner newer", task_type="implement")
    root_b.status = "failed"
    root_b.completed_at = datetime.now(UTC) - timedelta(hours=2)
    root_b.failure_reason = "TEST_FAILURE"
    store.update(root_b)
    assert root_b.id is not None

    service = TaskQueryService(store)
    result = service.run(TaskQueryPresets.incomplete(limit=1))

    assert len(result.rows) == 1


def test_queue_preset_matches_runnable_pickup_order(tmp_path: Path) -> None:
    store = _store(tmp_path)
    first = store.add("First runnable")
    blocked_parent = store.add("Blocking task")
    blocked = store.add("Blocked task", depends_on=blocked_parent.id)
    internal = store.add("Internal task", task_type="internal")
    bumped = store.add("Bumped task")
    assert first.id is not None
    assert blocked.id is not None
    assert internal.id is not None
    assert bumped.id is not None
    store.set_urgent(bumped.id, True)

    service = TaskQueryService(store)
    result = service.run(TaskQueryPresets.queue(limit=None))

    prompts = [row.task.prompt for row in result.rows if hasattr(row, "task")]
    assert prompts == ["Bumped task", "First runnable", "Blocking task"]


def test_queue_preset_filters_to_group(tmp_path: Path) -> None:
    store = _store(tmp_path)
    release = store.add("Release runnable", group="release")
    backlog = store.add("Backlog runnable", group="backlog")
    assert release.id is not None
    assert backlog.id is not None

    service = TaskQueryService(store)
    result = service.run(TaskQueryPresets.queue(limit=None, group="release"))

    prompts = [row.task.prompt for row in result.rows if hasattr(row, "task")]
    assert prompts == ["Release runnable"]


def test_task_query_group_filter_matches_any_of_selected_group_names(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.add("Release task", tags=("release",))
    store.add("Backlog task", tags=("backlog",))
    store.add("Ops task", tags=("ops",))

    service = TaskQueryService(store)
    result = service.run(
        TaskQuery(
            scope="tasks",
            groups=("release", "ops"),
            limit=None,
        )
    )

    prompts = [row.task.prompt for row in result.rows if hasattr(row, "task")]
    assert "Release task" in prompts
    assert "Ops task" in prompts
    assert "Backlog task" not in prompts


def test_default_projection_includes_group_field(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.add("Release task", group="release")

    service = TaskQueryService(store)
    rows = service.run(TaskQueryPresets.search("Release", limit=None)).to_json()

    assert rows
    assert "group" in rows[0]
    assert rows[0]["group"] == "release"


def test_dependency_state_blocked_by_dropped_dep_filters_pending_only(tmp_path: Path) -> None:
    store = _store(tmp_path)

    dropped_dep = store.add("Dropped dependency", task_type="implement")
    dropped_dep.status = "dropped"
    dropped_dep.completed_at = datetime.now(UTC)
    store.update(dropped_dep)
    assert dropped_dep.id is not None

    blocked_pending = store.add("Blocked pending", task_type="implement", depends_on=dropped_dep.id)
    blocked_pending_dropped = store.add("Blocked dropped", task_type="implement", depends_on=dropped_dep.id)
    blocked_pending_dropped.status = "dropped"
    blocked_pending_dropped.completed_at = datetime.now(UTC)
    store.update(blocked_pending_dropped)

    resolved_dep = store.add("Dropped with retry", task_type="implement")
    resolved_dep.status = "dropped"
    resolved_dep.completed_at = datetime.now(UTC) - timedelta(hours=2)
    store.update(resolved_dep)
    assert resolved_dep.id is not None
    retry = store.add("Resolved retry", task_type="implement", based_on=resolved_dep.id)
    retry.status = "completed"
    retry.completed_at = datetime.now(UTC) - timedelta(hours=1)
    retry.has_commits = True
    retry.merge_status = "unmerged"
    store.update(retry)
    blocked_resolved = store.add("Blocked but resolved", task_type="implement", depends_on=resolved_dep.id)

    service = TaskQueryService(store)
    result = service.run(
        TaskQuery(
            scope="tasks",
            statuses=("pending",),
            dependency_state=("blocked_by_dropped_dep",),
            limit=None,
        )
    )

    ids = [row.task.id for row in result.rows if hasattr(row, "task")]
    assert blocked_pending.id in ids
    assert blocked_pending_dropped.id not in ids
    assert blocked_resolved.id not in ids
