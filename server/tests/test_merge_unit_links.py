"""The task views must expose the merge unit each task belongs to."""

from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient
from gza_server.app import create_app
from gza_server.task_list import (
    _resolve_merge_units,
)

from gza.db import SqliteTaskStore, Task
from gza.task_query import TaskRow


def _client(store: SqliteTaskStore) -> TestClient:
    return TestClient(create_app(store_factory=lambda: store))


def _store(tmp_path: Path) -> SqliteTaskStore:
    return SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")


def _attached_task(store: SqliteTaskStore, index: int) -> tuple[Task, str]:
    """Create one task on its own branch, attached to its own merge unit."""
    task = store.add(f"Task {index}", task_type="implement", branch=f"feature/{index}")
    task.status = "completed"
    task.created_at = datetime(2026, 8, 18, 9, index % 60, tzinfo=UTC)
    store.update(task)
    unit = store.create_merge_unit(
        source_branch=f"feature/{index}",
        target_branch="main",
        owner_task_id=task.id,
    )
    assert task.id is not None
    store.attach_task_to_merge_unit(task.id, unit.id, "owner")
    return task, unit.id


def test_task_list_links_attached_rows_and_leaves_others_blank(tmp_path: Path) -> None:
    store = _store(tmp_path)
    attached, unit_id = _attached_task(store, 1)
    unattached = store.add("Loose task", task_type="implement")
    client = _client(store)

    html = client.get("/tasks").text
    rows = {row["id"]: row for row in client.get("/api/tasks").json()}

    assert rows[attached.id]["merge_unit_id"] == unit_id
    assert rows[attached.id]["merge_unit_url"] == (
        f"/projects/server-test/merge-units/{unit_id}"
    )
    assert rows[attached.id]["merge_unit_state"] == "unmerged"
    assert rows[unattached.id]["merge_unit_id"] is None
    assert rows[unattached.id]["merge_unit_url"] is None

    assert f'href="/projects/server-test/merge-units/{unit_id}"' in html
    # The unattached row must not render a link that goes nowhere.
    assert html.count("/merge-units/") == 1


def test_task_detail_links_to_its_merge_unit(tmp_path: Path) -> None:
    store = _store(tmp_path)
    attached, unit_id = _attached_task(store, 1)
    client = _client(store)

    page = client.get(f"/tasks/{attached.id}")
    record = client.get(f"/api/tasks/{attached.id}").json()

    assert page.status_code == 200
    assert f'href="/projects/server-test/merge-units/{unit_id}"' in page.text
    assert record["merge_unit_id"] == unit_id
    assert record["merge_unit_url"] == f"/projects/server-test/merge-units/{unit_id}"
    assert record["merge_unit_state"] == "unmerged"


def test_task_detail_without_a_merge_unit_reports_none(tmp_path: Path) -> None:
    store = _store(tmp_path)
    loose = store.add("Loose task", task_type="implement")
    client = _client(store)

    page = client.get(f"/tasks/{loose.id}")
    record = client.get(f"/api/tasks/{loose.id}").json()

    assert page.status_code == 200
    assert "/merge-units/" not in page.text
    assert record["merge_unit_id"] is None
    assert record["merge_unit_url"] is None


def test_merge_unit_resolution_does_not_scale_with_row_count(tmp_path: Path) -> None:
    """The link resolver must cost one query per project, not one per row.

    Scoped to ``_resolve_merge_units`` on purpose. ``TaskQueryService`` does its
    own per-task merge-unit work further up the stack, which predates this view;
    asserting over the whole listing would measure that instead of this, and
    would fail for a reason the reader of this test cannot act on.
    """
    store = _store(tmp_path)
    tasks = [_attached_task(store, index)[0] for index in range(12)]
    rows = [
        TaskRow(task=task, values={}, project_id="server-test", project_root=None)
        for task in tasks
    ]

    batched_calls: list[int] = []
    per_task_calls: list[str] = []
    original_batched = SqliteTaskStore.resolve_merge_units_for_tasks
    original_single = SqliteTaskStore.resolve_merge_unit_for_task

    def counting_batched(self, task_ids):  # type: ignore[no-untyped-def]
        task_ids = list(task_ids)
        batched_calls.append(len(task_ids))
        return original_batched(self, task_ids)

    def counting_single(self, task_id):  # type: ignore[no-untyped-def]
        per_task_calls.append(task_id)
        return original_single(self, task_id)

    SqliteTaskStore.resolve_merge_units_for_tasks = counting_batched  # type: ignore[method-assign]
    SqliteTaskStore.resolve_merge_unit_for_task = counting_single  # type: ignore[method-assign]
    try:
        resolved = _resolve_merge_units(store, rows)
    finally:
        SqliteTaskStore.resolve_merge_units_for_tasks = original_batched  # type: ignore[method-assign]
        SqliteTaskStore.resolve_merge_unit_for_task = original_single  # type: ignore[method-assign]

    # One batched call for the one project, carrying every visible row...
    assert batched_calls == [12]
    # ...and no per-row fallback behind it.
    assert per_task_calls == []
    assert len(resolved) == 12


def test_merge_unit_resolution_handles_an_empty_page(tmp_path: Path) -> None:
    store = _store(tmp_path)

    assert _resolve_merge_units(store, []) == {}


def test_batched_resolution_matches_per_task_resolution(tmp_path: Path) -> None:
    store = _store(tmp_path)
    attached = [_attached_task(store, index) for index in range(5)]
    loose = store.add("Loose task", task_type="implement")
    assert loose.id is not None

    task_ids = [task.id for task, _unit_id in attached if task.id is not None]
    batched = store.resolve_merge_units_for_tasks([*task_ids, loose.id])

    assert loose.id not in batched
    for task, unit_id in attached:
        assert task.id is not None
        assert batched[task.id].id == unit_id
        single = store.resolve_merge_unit_for_task(task.id)
        assert single is not None
        assert single.id == unit_id


def test_batched_resolution_ignores_empty_and_unknown_ids(tmp_path: Path) -> None:
    store = _store(tmp_path)
    task, unit_id = _attached_task(store, 1)
    assert task.id is not None

    resolved = store.resolve_merge_units_for_tasks(["", "srv-does-not-exist", task.id, task.id])

    assert list(resolved) == [task.id]
    assert resolved[task.id].id == unit_id
    assert store.resolve_merge_units_for_tasks([]) == {}
