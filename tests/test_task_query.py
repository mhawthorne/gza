"""Tests for unified task query service."""

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

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
