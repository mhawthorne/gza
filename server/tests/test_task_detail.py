from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient

from gza.db import SqliteTaskStore
from gza_server.app import create_app


def _client(store: SqliteTaskStore, *, project_dir: Path | None = None) -> TestClient:
    return TestClient(create_app(store_factory=lambda: store, project_dir=project_dir))


def test_implement_task_detail_renders_metadata_prompt_and_full_json(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    task = store.add(
        "## Ship safely\n\nUse **targeted checks** before release.",
        task_type="implement",
        tags=("backend", "release"),
        branch="feature/task-detail",
    )
    task.status = "completed"
    task.started_at = datetime(2026, 8, 18, 10, 0, tzinfo=UTC)
    task.completed_at = datetime(2026, 8, 18, 10, 2, tzinfo=UTC)
    task.duration_seconds = 120.5
    store.update(task)
    stored = store.get(task.id or "")
    assert stored is not None
    client = _client(store)

    response = client.get(f"/tasks/{task.id}")
    api_response = client.get(f"/api/tasks/{task.id}")

    assert response.status_code == 200
    assert f"<h1>{task.id}</h1>" in response.text
    assert "implement" in response.text
    assert 'class="status status-completed"' in response.text
    assert '<span class="tag">backend</span>' in response.text
    assert "feature/task-detail" in response.text
    assert "2026-08-18 10:00:00 UTC" in response.text
    assert "2026-08-18 10:02:00 UTC" in response.text
    assert "120.5 seconds" in response.text
    assert "<h2>Ship safely</h2>" in response.text
    assert "Use <strong>targeted checks</strong> before release." in response.text
    assert "<h2>Plan</h2>" not in response.text

    assert api_response.status_code == 200
    record = api_response.json()
    assert record["id"] == task.id
    assert record["type"] == "implement"
    assert record["task_type"] == "implement"
    assert record["prompt"] == task.prompt
    assert record["tags"] == ["backend", "release"]
    assert record["branch"] == "feature/task-detail"
    assert record["duration_seconds"] == 120.5
    assert record["updated_at"] == stored.updated_at.isoformat().replace("+00:00", "Z")
    assert record["plan_content"] is None


def test_plan_task_detail_renders_cli_plan_content_below_prompt(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    plan = store.add("# Planning prompt\n\nDefine the rollout.", task_type="plan")
    plan_content = "## Rollout plan\n\n1. Ship canary\n2. Measure results"
    store.mark_completed(plan, output_content=plan_content)
    client = _client(store, project_dir=tmp_path)

    response = client.get(f"/tasks/{plan.id}")
    api_response = client.get(f"/api/tasks/{plan.id}")

    assert response.status_code == 200
    assert "<h1>Planning prompt</h1>" in response.text
    assert "<h2>Plan</h2>" in response.text
    assert "<h2>Rollout plan</h2>" in response.text
    assert "<li>Ship canary</li>" in response.text
    assert response.text.index("Planning prompt") < response.text.index("Rollout plan")
    assert api_response.status_code == 200
    assert api_response.json()["output_content"] == plan_content
    assert api_response.json()["plan_content"] == plan_content


def test_task_detail_links_direct_parents_and_children(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    based_on_parent = store.add("Plan parent", task_type="plan")
    dependency_parent = store.add("Dependency parent", task_type="implement")
    task = store.add(
        "Lineage center",
        task_type="implement",
        based_on=based_on_parent.id,
        depends_on=dependency_parent.id,
    )
    based_on_child = store.add("Review child", task_type="review", based_on=task.id)
    dependency_child = store.add("Dependent child", task_type="implement", depends_on=task.id)
    client = _client(store)

    response = client.get(f"/tasks/{task.id}")
    record = client.get(f"/api/tasks/{task.id}").json()

    assert response.status_code == 200
    for related in (based_on_parent, dependency_parent, based_on_child, dependency_child):
        assert f'href="/tasks/{related.id}"' in response.text
    assert {parent["id"] for parent in record["parents"]} == {
        based_on_parent.id,
        dependency_parent.id,
    }
    assert {child["id"] for child in record["children"]} == {
        based_on_child.id,
        dependency_child.id,
    }
    assert client.get(f"/tasks/{based_on_child.id}").status_code == 200


def test_unknown_task_detail_returns_clean_html_and_json_404(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    client = _client(store)

    response = client.get("/tasks/srv-999")
    api_response = client.get("/api/tasks/srv-999")

    assert response.status_code == 404
    assert "<h1>Task not found</h1>" in response.text
    assert "srv-999" in response.text
    assert 'href="/tasks"' in response.text
    assert api_response.status_code == 404
    assert api_response.json() == {"detail": "Task srv-999 not found"}


def test_task_detail_resolves_task_from_another_shared_project(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    first_store = SqliteTaskStore(db_path, prefix="alpha", project_id="alpha")
    second_store = SqliteTaskStore(db_path, prefix="beta", project_id="beta")
    first_store.add("Alpha task", task_type="implement")
    beta = second_store.add("## Beta task", task_type="implement")

    response = _client(first_store).get(f"/tasks/{beta.id}")

    assert response.status_code == 200
    assert f"<h1>{beta.id}</h1>" in response.text
    assert "<h2>Beta task</h2>" in response.text
