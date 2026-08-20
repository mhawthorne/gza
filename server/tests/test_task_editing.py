from __future__ import annotations

import html
from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient

from gza.db import SqliteTaskStore
from gza_server.app import create_app


def _client(store: SqliteTaskStore) -> TestClient:
    return TestClient(
        create_app(
            store_factory=lambda: store,
            mutation_store_factory=lambda project_id: store,
        ),
        headers={"origin": "http://testserver"},
    )


def test_pending_prompt_form_edit_persists_redirects_and_rerenders(tmp_path: Path) -> None:
    store = SqliteTaskStore(
        tmp_path / "tasks.db",
        prefix="srv",
        project_id="server-test",
        project_root=tmp_path,
        project_name="Server Test",
    )
    task = store.add("Original prompt text", task_type="implement")
    assert task.id is not None
    client = _client(store)

    edit_page = client.get(f"/tasks/{task.id}?edit=prompt")
    response = client.post(
        f"/api/tasks/{task.id}/prompt",
        data={
            "project_id": "server-test",
            "prompt": "## Updated prompt\n\nShip the **safe** version.",
        },
        follow_redirects=False,
    )

    assert edit_page.status_code == 200
    assert 'name="prompt"' in edit_page.text
    assert "Original prompt text" in edit_page.text
    assert response.status_code == 303
    assert response.headers["location"] == f"/projects/server-test/tasks/{task.id}"
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.prompt == "## Updated prompt\n\nShip the **safe** version."
    assert refreshed.last_edited_at is not None
    rendered = client.get(response.headers["location"])
    assert "<h2>Updated prompt</h2>" in rendered.text
    assert "Ship the <strong>safe</strong> version." in rendered.text


def test_non_pending_prompt_has_no_affordance_and_rejection_preserves_text(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(
        tmp_path / "tasks.db",
        prefix="srv",
        project_id="server-test",
        project_root=tmp_path,
    )
    task = store.add("Original pending prompt", task_type="implement")
    assert task.id is not None
    client = _client(store)
    assert 'name="prompt"' in client.get(f"/tasks/{task.id}?edit=prompt").text

    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    store.update(task)
    attempted = "## Work in progress\n\nDo not lose this edited text."
    response = client.post(
        f"/api/tasks/{task.id}/prompt",
        data={"project_id": "server-test", "prompt": attempted},
    )
    api_response = client.post(
        f"/api/tasks/{task.id}/prompt",
        json={"project_id": "server-test", "prompt": attempted},
    )

    assert response.status_code == 409
    assert "prompt edits are only allowed for pending tasks" in response.text
    assert html.escape(attempted) in response.text
    assert 'name="prompt"' in response.text
    assert api_response.status_code == 409
    assert "prompt edits are only allowed for pending tasks" in api_response.json()["detail"]
    unchanged = store.get(task.id)
    assert unchanged is not None and unchanged.prompt == "Original pending prompt"

    read_only = client.get(f"/tasks/{task.id}")
    forced_edit = client.get(f"/tasks/{task.id}?edit=prompt")
    assert "?edit=prompt" not in read_only.text
    assert 'name="prompt"' not in read_only.text
    assert 'name="prompt"' not in forced_edit.text


def test_plan_form_edit_round_trips_through_report_file_and_db(tmp_path: Path) -> None:
    store = SqliteTaskStore(
        tmp_path / "tasks.db",
        prefix="srv",
        project_id="server-test",
        project_root=tmp_path,
        project_name="Server Test",
    )
    task = store.add("Create a rollout plan", task_type="plan")
    original = "## Original plan\n\n1. Start small"
    store.mark_completed(task, output_content=original)
    refreshed = store.get(task.id or "")
    assert refreshed is not None
    report_path = tmp_path / ".gza" / "plans" / "round-trip.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(original, encoding="utf-8")
    refreshed.report_file = report_path.relative_to(tmp_path).as_posix()
    store.update(refreshed)
    client = _client(store)
    updated = "## Revised rollout\n\n1. Ship canary\n2. Measure results"

    edit_page = client.get(f"/tasks/{task.id}?edit=plan")
    response = client.post(
        f"/api/tasks/{task.id}/plan",
        data={"project_id": "server-test", "plan": updated},
        follow_redirects=False,
    )

    assert edit_page.status_code == 200
    assert 'name="plan"' in edit_page.text
    assert response.status_code == 303
    assert response.headers["location"] == f"/projects/server-test/tasks/{task.id}"
    persisted = store.get(task.id or "")
    assert persisted is not None
    assert persisted.output_content == updated
    assert persisted.report_file == report_path.relative_to(tmp_path).as_posix()
    assert report_path.read_text(encoding="utf-8") == updated
    rendered = client.get(response.headers["location"])
    assert "<h2>Revised rollout</h2>" in rendered.text
    assert "<li>Ship canary</li>" in rendered.text
