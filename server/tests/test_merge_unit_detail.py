from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient
from gza_server.app import create_app

from gza.db import MergeUnit, SqliteTaskStore, Task


def _client(store: SqliteTaskStore) -> TestClient:
    return TestClient(create_app(store_factory=lambda: store))


def _store(tmp_path: Path) -> SqliteTaskStore:
    return SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")


def _add(
    store: SqliteTaskStore,
    prompt: str,
    *,
    task_type: str,
    status: str,
    created_at: datetime,
    branch: str = "feature/unit",
) -> Task:
    task = store.add(prompt, task_type=task_type, branch=branch)
    task.status = status
    task.created_at = created_at
    store.update(task)
    return task


def _unit_with_members(store: SqliteTaskStore) -> tuple[MergeUnit, list[Task]]:
    """Build an implement plus a review and an improve, attached out of order.

    Attachment order is deliberately not creation order so the chronological
    contract is actually exercised rather than accidentally satisfied.
    """
    implement = _add(
        store,
        "Implement the merge unit page",
        task_type="implement",
        status="completed",
        created_at=datetime(2026, 8, 18, 9, 0, tzinfo=UTC),
    )
    review = _add(
        store,
        "Review the merge unit page",
        task_type="review",
        status="completed",
        created_at=datetime(2026, 8, 18, 10, 0, tzinfo=UTC),
    )
    improve = _add(
        store,
        "Address review feedback",
        task_type="improve",
        status="completed",
        created_at=datetime(2026, 8, 18, 11, 0, tzinfo=UTC),
    )
    unit = store.create_merge_unit(
        source_branch="feature/unit",
        target_branch="main",
        owner_task_id=implement.id,
        state="merged",
        merged_at=datetime(2026, 8, 18, 12, 0, tzinfo=UTC),
        diff_files_changed=3,
        diff_lines_added=120,
        diff_lines_removed=8,
    )
    for task, role in ((improve, "improve"), (implement, "owner"), (review, "review")):
        assert task.id is not None
        store.attach_task_to_merge_unit(task.id, unit.id, role)
    return unit, [implement, review, improve]


def test_merge_unit_page_lists_members_chronologically(tmp_path: Path) -> None:
    store = _store(tmp_path)
    unit, (implement, review, improve) = _unit_with_members(store)
    client = _client(store)

    response = client.get(f"/merge-units/{unit.id}")

    assert response.status_code == 200
    body = response.text
    assert f"Merge unit {unit.id}" in body
    assert "feature/unit" in body
    assert "main" in body
    assert 'class="status status-merged"' in body
    assert "2026-08-18 12:00:00 UTC" in body

    positions = [body.index(str(task.id)) for task in (implement, review, improve)]
    assert positions == sorted(positions)


def test_merge_unit_page_shows_diff_stats_and_owner(tmp_path: Path) -> None:
    store = _store(tmp_path)
    unit, (implement, _review, _improve) = _unit_with_members(store)
    client = _client(store)

    response = client.get(f"/merge-units/{unit.id}")

    assert response.status_code == 200
    assert "<h2>Diff</h2>" in response.text
    assert ">3<" in response.text
    assert ">120<" in response.text
    assert f'href="/projects/server-test/tasks/{implement.id}"' in response.text
    assert "(owner)" in response.text


def test_merge_unit_without_diff_stats_omits_the_diff_section(tmp_path: Path) -> None:
    store = _store(tmp_path)
    implement = _add(
        store,
        "Unmeasured unit",
        task_type="implement",
        status="completed",
        created_at=datetime(2026, 8, 18, 9, 0, tzinfo=UTC),
    )
    unit = store.create_merge_unit(
        source_branch="feature/unmeasured",
        target_branch="main",
        owner_task_id=implement.id,
    )
    assert implement.id is not None
    store.attach_task_to_merge_unit(implement.id, unit.id, "owner")
    client = _client(store)

    response = client.get(f"/merge-units/{unit.id}")

    assert response.status_code == 200
    assert "<h2>Diff</h2>" not in response.text
    assert "<h2>Pull request</h2>" not in response.text


def test_merge_unit_api_returns_unit_and_members(tmp_path: Path) -> None:
    store = _store(tmp_path)
    unit, (implement, review, improve) = _unit_with_members(store)
    client = _client(store)

    response = client.get(f"/api/merge-units/{unit.id}")

    assert response.status_code == 200
    record = response.json()
    assert record["id"] == unit.id
    assert record["project_id"] == "server-test"
    assert record["detail_url"] == f"/projects/server-test/merge-units/{unit.id}"
    assert record["api_url"] == f"/api/projects/server-test/merge-units/{unit.id}"
    assert record["source_branch"] == "feature/unit"
    assert record["target_branch"] == "main"
    assert record["state"] == "merged"
    assert record["diff_files_changed"] == 3
    assert [member["id"] for member in record["members"]] == [
        implement.id,
        review.id,
        improve.id,
    ]
    assert [member["role"] for member in record["members"]] == [
        "owner",
        "review",
        "improve",
    ]
    assert record["owner"]["id"] == implement.id
    assert record["members"][0]["is_owner"] is True
    assert record["members"][1]["is_owner"] is False


def test_qualified_merge_unit_routes_match_the_bare_routes(tmp_path: Path) -> None:
    store = _store(tmp_path)
    unit, _tasks = _unit_with_members(store)
    client = _client(store)

    page = client.get(f"/projects/server-test/merge-units/{unit.id}")
    api = client.get(f"/api/projects/server-test/merge-units/{unit.id}")

    assert page.status_code == 200
    assert f"Merge unit {unit.id}" in page.text
    assert api.status_code == 200
    assert api.json()["id"] == unit.id


def test_unknown_merge_unit_renders_404(tmp_path: Path) -> None:
    store = _store(tmp_path)
    client = _client(store)

    page = client.get("/merge-units/srv-unit-999")
    api = client.get("/api/merge-units/srv-unit-999")

    assert page.status_code == 404
    assert "Merge unit not found" in page.text
    assert api.status_code == 404


def test_merge_unit_for_a_wrong_project_is_not_found(tmp_path: Path) -> None:
    store = _store(tmp_path)
    unit, _tasks = _unit_with_members(store)
    client = _client(store)

    response = client.get(f"/projects/other-project/merge-units/{unit.id}")

    assert response.status_code == 404
