import re
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from gza_server.app import create_app

from gza.db import SqliteTaskStore, Task, edit_task_interactive
from gza.task_types import ALL_TASK_STATUSES, ALL_TASK_TYPES


@pytest.fixture
def seeded_store(tmp_path: Path) -> tuple[SqliteTaskStore, dict[str, Task]]:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    tasks = {
        "alpha": store.add(
            "Alpha release database migration",
            task_type="implement",
            tags=("release", "backend"),
        ),
        "beta": store.add(
            "Beta release planning",
            task_type="plan",
            tags=("release",),
        ),
        "gamma": store.add(
            "Gamma backend investigation",
            task_type="implement",
            tags=("backend",),
        ),
        "delta": store.add("Delta task without a tag", task_type="implement"),
    }

    _set_times_and_status(
        store,
        tasks["alpha"],
        completed_at=datetime(2027, 1, 5, tzinfo=UTC),
        status="completed",
    )
    _set_times_and_status(
        store,
        tasks["beta"],
        status="pending",
    )
    _set_times_and_status(
        store,
        tasks["gamma"],
        completed_at=datetime(2026, 12, 4, tzinfo=UTC),
        status="failed",
    )
    _set_times_and_status(
        store,
        tasks["delta"],
        status="pending",
    )
    return store, tasks


def _set_times_and_status(
    store: SqliteTaskStore,
    task: Task,
    *,
    status: str,
    completed_at: datetime | None = None,
) -> None:
    task.completed_at = completed_at
    task.status = status
    store.update(task)


def _client(store: SqliteTaskStore) -> TestClient:
    return TestClient(create_app(store_factory=lambda: store))


def _html_task_ids(html: str) -> list[str]:
    return re.findall(r'<tr data-task-id="([^"]+)"(?:\s[^>]*)?>', html)


def _html_updated_at(html: str, task_id: str) -> str:
    match = re.search(
        rf'<tr data-task-id="{re.escape(task_id)}"(?:\s[^>]*)?>.*?<time class="updated" datetime="([^"]*)">',
        html,
        flags=re.DOTALL,
    )
    assert match is not None
    return match.group(1)


@pytest.mark.parametrize(
    ("query", "expected_names"),
    [
        (
            "q=alpha&tag=release&tag=backend&status=completed&type=implement",
            {"alpha"},
        ),
        ("tag=Release&tag=BACKEND", {"alpha", "beta", "gamma"}),
        ("untagged=true", {"delta"}),
        ("status=pending&type=plan", {"beta"}),
    ],
)
def test_html_and_json_share_gza_search_filter_results(
    seeded_store: tuple[SqliteTaskStore, dict[str, Task]],
    query: str,
    expected_names: set[str],
) -> None:
    store, tasks = seeded_store
    client = _client(store)

    api_response = client.get(f"/api/tasks?{query}")
    html_response = client.get(f"/tasks?{query}")

    assert api_response.status_code == 200
    assert html_response.status_code == 200
    expected_ids = {tasks[name].id for name in expected_names}
    api_ids = {row["id"] for row in api_response.json()}
    html_ids = set(_html_task_ids(html_response.text))
    assert api_ids == html_ids == expected_ids


def test_html_and_json_query_all_projects_in_shared_database(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    alpha_store = SqliteTaskStore(db_path, prefix="alpha", project_id="alpha")
    beta_store = SqliteTaskStore(db_path, prefix="beta", project_id="beta")
    alpha = alpha_store.add(
        "Shared release candidate alpha",
        task_type="implement",
        tags=("shared", "alpha-only"),
    )
    beta = beta_store.add(
        "Shared release candidate beta",
        task_type="implement",
        tags=("shared", "beta-only"),
    )
    beta_store.add("Unrelated completed release", task_type="plan", tags=("beta-only",))
    client = _client(alpha_store)
    query = "q=release+candidate&tag=shared&status=pending&type=implement"

    api_response = client.get(f"/api/tasks?{query}")
    html_response = client.get(f"/tasks?{query}")

    assert api_response.status_code == 200
    assert html_response.status_code == 200
    expected_ids = {alpha.id, beta.id}
    assert {row["id"] for row in api_response.json()} == expected_ids
    assert set(_html_task_ids(html_response.text)) == expected_ids
    assert '<option value="alpha-only"' in html_response.text
    assert '<option value="beta-only"' in html_response.text


def test_task_rows_include_requested_fields_and_detail_links(
    seeded_store: tuple[SqliteTaskStore, dict[str, Task]],
) -> None:
    store, tasks = seeded_store
    client = _client(store)

    api_row = client.get("/api/tasks?q=alpha").json()[0]
    html = client.get("/tasks?q=alpha").text

    # updated_at reports when the row was last actually mutated, so it reflects the
    # seeding write rather than the backdated completion time the fixture assigns.
    stored = store.get(tasks["alpha"].id or "")
    assert stored is not None
    assert stored.updated_at is not None
    assert api_row == {
        "id": tasks["alpha"].id,
        "project_id": "server-test",
        "detail_url": f"/projects/server-test/tasks/{tasks['alpha'].id}",
        "api_url": f"/api/projects/server-test/tasks/{tasks['alpha'].id}",
        "type": "implement",
        "status": "completed",
        "tags": ["backend", "release"],
        "prompt": "Alpha release database migration",
        "prompt_excerpt": "Alpha release database migration",
        "created_at": tasks["alpha"].created_at.isoformat().replace("+00:00", "Z"),
        "updated_at": stored.updated_at.isoformat().replace("+00:00", "Z"),
        "age": api_row["age"],
    }
    assert f'href="/projects/server-test/tasks/{tasks["alpha"].id}"' in html
    assert 'class="status status-completed"' in html
    assert '<span class="tag">backend</span>' in html


def test_task_prompt_excerpt_is_collapsed_and_truncated(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    task = store.add("First line\n\n" + ("word " * 50), task_type="implement")

    row = _client(store).get("/api/tasks?q=first").json()[0]

    assert row["id"] == task.id
    assert "\n" not in row["prompt_excerpt"]
    assert len(row["prompt_excerpt"]) == 160
    assert row["prompt_excerpt"].endswith("…")


def test_task_list_sorts_by_created_and_edit_aware_updated_time(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    older = store.add("Older pending task", task_type="implement")
    older.created_at = datetime(2025, 1, 1, tzinfo=UTC)
    store.update(older)
    newer = store.add("Newer pending task", task_type="implement")
    newer.created_at = datetime(2025, 2, 1, tzinfo=UTC)
    store.update(newer)
    monkeypatch.setattr("gza.db.edit_prompt", lambda **_kwargs: "Older pending task, edited")
    assert edit_task_interactive(store, older) is True
    assert older.id is not None
    edited = store.get(older.id)
    assert edited is not None
    assert edited.last_edited_at is not None
    assert edited.updated_at is not None
    client = _client(store)

    api_response = client.get("/api/tasks?sort=updated&direction=desc")
    html_response = client.get("/tasks?sort=updated&direction=desc")
    created_api_response = client.get("/api/tasks?sort=created&direction=asc")
    created_html_response = client.get("/tasks?sort=created&direction=asc")

    api_rows = api_response.json()
    assert [row["id"] for row in api_rows] == [older.id, newer.id]
    assert _html_task_ids(html_response.text) == [older.id, newer.id]
    expected_updated_at = edited.updated_at.isoformat().replace("+00:00", "Z")
    assert api_rows[0]["updated_at"] == expected_updated_at
    assert _html_updated_at(html_response.text, older.id) == edited.updated_at.isoformat()
    assert [row["id"] for row in created_api_response.json()] == [older.id, newer.id]
    assert _html_task_ids(created_html_response.text) == [older.id, newer.id]


@pytest.mark.parametrize("previous_status", ["completed", "failed"])
def test_status_reset_makes_older_task_newest_with_matching_html_json_timestamp(
    tmp_path: Path,
    previous_status: str,
) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    older = store.add("Older task to reset", task_type="implement")
    older.status = previous_status
    older.completed_at = datetime.now(UTC)
    store.update(older)
    newer = store.add("Newer untouched task", task_type="implement")

    older.status = "pending"
    older.completed_at = None
    older.failure_reason = None
    older.completion_reason = None
    older.drop_reason = None
    store.update(older)
    reloaded = store.get(older.id or "")
    assert reloaded is not None
    assert reloaded.updated_at is not None

    client = _client(store)
    api_rows = client.get("/api/tasks?sort=updated&direction=desc").json()
    html = client.get("/tasks?sort=updated&direction=desc").text

    assert [row["id"] for row in api_rows[:2]] == [older.id, newer.id]
    assert _html_task_ids(html)[:2] == [older.id, newer.id]
    expected = reloaded.updated_at.isoformat().replace("+00:00", "Z")
    assert api_rows[0]["updated_at"] == expected
    assert _html_updated_at(html, older.id or "") == reloaded.updated_at.isoformat()


def test_tag_only_mutation_makes_task_newest_with_matching_html_json_timestamp(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    tag_target = store.add("Older task to tag", task_type="implement")
    newer = store.add("Newer untouched task", task_type="implement")

    assert tag_target.id is not None
    store.add_task_tags(tag_target.id, ("fresh",))
    reloaded = store.get(tag_target.id)
    assert reloaded is not None
    assert reloaded.updated_at is not None

    client = _client(store)
    api_rows = client.get("/api/tasks?sort=updated&direction=desc").json()
    html = client.get("/tasks?sort=updated&direction=desc").text

    assert [row["id"] for row in api_rows[:2]] == [tag_target.id, newer.id]
    assert _html_task_ids(html)[:2] == [tag_target.id, newer.id]
    expected = reloaded.updated_at.isoformat().replace("+00:00", "Z")
    assert api_rows[0]["updated_at"] == expected
    assert _html_updated_at(html, tag_target.id) == reloaded.updated_at.isoformat()


@pytest.mark.parametrize("mutation", ["claim", "rename_tag"])
def test_direct_mutation_makes_older_task_newest_with_matching_html_json_timestamp(
    tmp_path: Path,
    mutation: str,
) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    older = store.add("Older mutation target", task_type="implement", tags=("release",))
    newer = store.add("Newer untouched task", task_type="implement", tags=("backlog",))
    assert older.id is not None

    if mutation == "claim":
        claim = store.try_mark_in_progress(older.id, 1234)
        assert claim.task is not None
    else:
        assert store.rename_tag("release", "launch") == 1
    reloaded = store.get(older.id)
    assert reloaded is not None
    assert reloaded.updated_at is not None

    client = _client(store)
    api_rows = client.get("/api/tasks?sort=updated&direction=desc").json()
    html = client.get("/tasks?sort=updated&direction=desc").text

    assert [row["id"] for row in api_rows[:2]] == [older.id, newer.id]
    assert _html_task_ids(html)[:2] == [older.id, newer.id]
    expected = reloaded.updated_at.isoformat().replace("+00:00", "Z")
    assert api_rows[0]["updated_at"] == expected
    assert _html_updated_at(html, older.id) == reloaded.updated_at.isoformat()


def test_task_filter_form_lists_known_tags_and_preserves_selections(
    seeded_store: tuple[SqliteTaskStore, dict[str, Task]],
) -> None:
    store, _tasks = seeded_store

    response = _client(store).get("/tasks?tag=release&status=completed&type=implement&sort=created&direction=asc")

    assert response.status_code == 200
    assert '<option value="backend"' in response.text
    assert '<option value="release" selected>' in response.text
    assert '<option value="completed" selected>' in response.text
    assert '<option value="implement" selected>' in response.text
    assert '<option value="created" selected>' in response.text
    assert '<option value="asc" selected>' in response.text


def test_task_type_filter_exposes_and_preserves_persisted_task_type(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    task = store.add("Legacy general task", task_type="task")
    client = _client(store)

    api_response = client.get("/api/tasks?type=task")
    html_response = client.get("/tasks?type=task")

    assert {row["id"] for row in api_response.json()} == {task.id}
    assert set(_html_task_ids(html_response.text)) == {task.id}
    assert '<option value="task" selected>' in html_response.text


@pytest.mark.parametrize("task_type", ALL_TASK_TYPES)
def test_task_filter_form_lists_every_persisted_task_type(
    seeded_store: tuple[SqliteTaskStore, dict[str, Task]],
    task_type: str,
) -> None:
    store, _tasks = seeded_store

    response = _client(store).get(f"/tasks?type={task_type}")

    assert f'<option value="{task_type}" selected>' in response.text


@pytest.mark.parametrize("status", ALL_TASK_STATUSES)
def test_task_filter_form_lists_every_persisted_status(
    seeded_store: tuple[SqliteTaskStore, dict[str, Task]],
    status: str,
) -> None:
    store, _tasks = seeded_store

    response = _client(store).get(f"/tasks?status={status}")

    assert f'<option value="{status}" selected>' in response.text


@pytest.mark.parametrize("path", ["/tasks", "/api/tasks"])
def test_tag_and_untagged_are_rejected_together(
    seeded_store: tuple[SqliteTaskStore, dict[str, Task]],
    path: str,
) -> None:
    store, _tasks = seeded_store

    response = _client(store).get(f"{path}?tag=release&untagged=true")

    assert response.status_code == 422
    assert response.json()["detail"] == "tag and untagged filters cannot be combined"


@pytest.fixture
def many_tasks_store(tmp_path: Path) -> tuple[SqliteTaskStore, list[Task]]:
    store = SqliteTaskStore(tmp_path / "many.db", prefix="srv", project_id="server-test")
    tasks = [store.add(f"Bulk task {index:03d}", task_type="implement") for index in range(120)]
    return store, tasks


def test_tasks_page_defaults_to_fifty_rows(
    many_tasks_store: tuple[SqliteTaskStore, list[Task]],
) -> None:
    store, tasks = many_tasks_store
    html = _client(store).get("/tasks").text

    assert len(_html_task_ids(html)) == 50
    assert f"of {len(tasks)} tasks" in html
    assert "Page 1 of 3" in html


def test_tasks_pages_partition_the_full_result_set(
    many_tasks_store: tuple[SqliteTaskStore, list[Task]],
) -> None:
    store, tasks = many_tasks_store
    client = _client(store)

    paged: list[str] = []
    for page in (1, 2, 3):
        paged.extend(_html_task_ids(client.get(f"/tasks?page={page}").text))

    assert len(paged) == len(tasks)
    assert len(set(paged)) == len(tasks)
    assert set(paged) == {task.id for task in tasks}


def test_tasks_last_page_holds_the_remainder(
    many_tasks_store: tuple[SqliteTaskStore, list[Task]],
) -> None:
    store, _ = many_tasks_store
    html = _client(store).get("/tasks?page=3").text

    assert len(_html_task_ids(html)) == 20
    assert "Showing <strong>101–120</strong>" in html


def test_tasks_per_page_is_clamped_to_supported_sizes(
    many_tasks_store: tuple[SqliteTaskStore, list[Task]],
) -> None:
    store, _ = many_tasks_store
    client = _client(store)

    assert len(_html_task_ids(client.get("/tasks?per_page=25").text)) == 25
    assert len(_html_task_ids(client.get("/tasks?per_page=100").text)) == 100
    # 200 is supported; the corpus is smaller than that, so it returns everything.
    assert len(_html_task_ids(client.get("/tasks?per_page=200").text)) == 120
    # An unsupported size falls back to the default rather than rendering everything.
    assert len(_html_task_ids(client.get("/tasks?per_page=9000").text)) == 50


def test_tasks_page_past_the_end_shows_the_last_page(
    many_tasks_store: tuple[SqliteTaskStore, list[Task]],
) -> None:
    store, _ = many_tasks_store
    html = _client(store).get("/tasks?page=999").text

    assert len(_html_task_ids(html)) == 20
    assert "Page 3 of 3" in html


def test_pagination_links_preserve_active_filters(
    seeded_store: tuple[SqliteTaskStore, dict[str, Task]],
) -> None:
    store, _ = seeded_store
    html = _client(store).get("/tasks?status=pending&type=plan&per_page=25").text

    # Every pager link carries the active filters forward.
    pager_links = re.findall(r'href="(/tasks\?[^"]+)"', html)
    assert pager_links
    for link in pager_links:
        assert "status=pending" in link
        assert "type=plan" in link

    # The active page size is shown as current rather than as a link.
    assert "<strong>25</strong>" in html
    assert "per_page=50" in html


def test_pagination_does_not_narrow_bulk_retag_scope(
    many_tasks_store: tuple[SqliteTaskStore, list[Task]],
) -> None:
    """Bulk retag targets every filtered task, not just the rendered page."""
    store, tasks = many_tasks_store
    client = _client(store)

    response = client.post(
        "/api/tasks/tags/bulk",
        data={"q": "Bulk task", "mutation": "add", "mutation_tag": "swept"},
        headers={"origin": "http://testserver", "referer": "http://testserver/tasks"},
    )

    assert response.status_code == 200
    assert str(len(tasks)) in response.text
