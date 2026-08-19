import re
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlencode

from fastapi.testclient import TestClient

from gza.db import SqliteTaskStore
from gza.task_query import TaskQuery, TaskQueryService, TaskRow
from gza_server.app import create_app


def _client(
    store: SqliteTaskStore,
    *,
    mutation_stores: dict[str, SqliteTaskStore] | None = None,
) -> TestClient:
    stores = mutation_stores or {store.project_id: store}
    return TestClient(
        create_app(
            store_factory=lambda: store,
            mutation_store_factory=lambda project_id: stores[project_id],
        )
    )


def _query_tags(
    store: SqliteTaskStore,
    task_id: str,
    *,
    project_id: str,
) -> tuple[str, ...]:
    rows = TaskQueryService(store).run(TaskQuery(limit=None), all_projects=True).rows
    match = next(
        row
        for row in rows
        if isinstance(row, TaskRow) and row.project_id == project_id and row.task.id == task_id
    )
    return match.task.tags


def _hidden_value(html: str, name: str) -> str:
    match = re.search(rf'name="{name}" value="([^"]+)"', html)
    assert match is not None
    return match.group(1)


def test_task_tag_editor_adds_and_removes_tags_on_completed_task(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    task = store.add("Already shipped", tags=("release", "keep"))
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    store.update(task)
    assert task.id is not None
    client = _client(store)

    page = client.get(f"/tasks/{task.id}")
    added = client.post(f"/api/tasks/{task.id}/tags", json={"add": [" Launch "]})
    removed = client.post(f"/api/tasks/{task.id}/tags", json={"remove": ["release"]})

    assert page.status_code == 200
    assert f'action="/api/tasks/{task.id}/tags"' in page.text
    assert 'name="add"' in page.text
    assert 'name="remove" value="release"' in page.text
    assert added.status_code == 200
    assert added.json()["tags"] == ["keep", "launch", "release"]
    assert removed.status_code == 200
    assert removed.json() == {
        "id": task.id,
        "project_id": "server-test",
        "tags": ["keep", "launch"],
        "changed": True,
    }
    assert _query_tags(store, task.id, project_id="server-test") == ("keep", "launch")


def test_task_tag_write_store_resolves_through_project_config(tmp_path: Path) -> None:
    (tmp_path / "gza.yaml").write_text(
        "project_name: server-test\n"
        "project_id: servertest\n"
        "project_prefix: srv\n"
        "db_path: .gza/gza.db\n",
        encoding="utf-8",
    )
    store = SqliteTaskStore(
        tmp_path / ".gza" / "gza.db",
        prefix="srv",
        project_id="servertest",
        project_root=tmp_path,
    )
    task = store.add("Configured task", tags=("old",))
    assert task.id is not None

    response = TestClient(create_app(project_dir=tmp_path)).post(
        f"/api/tasks/{task.id}/tags",
        json={"add": ["new"]},
    )

    assert response.status_code == 200
    assert _query_tags(store, task.id, project_id="servertest") == ("new", "old")


def test_bulk_replace_previews_then_applies_to_cross_project_filtered_set(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "shared.db"
    alpha_store = SqliteTaskStore(db_path, prefix="alpha", project_id="alpha")
    beta_store = SqliteTaskStore(db_path, prefix="beta", project_id="beta")
    alpha = alpha_store.add("Alpha release", task_type="implement", tags=("old", "keep"))
    beta = beta_store.add("Beta release", task_type="implement", tags=("old",))
    pending = alpha_store.add("Pending release", task_type="implement", tags=("old",))
    plan = beta_store.add("Completed plan", task_type="plan", tags=("old",))
    for store, task in ((alpha_store, alpha), (beta_store, beta), (beta_store, plan)):
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)
    assert alpha.id and beta.id and pending.id and plan.id
    client = _client(
        alpha_store,
        mutation_stores={"alpha": alpha_store, "beta": beta_store},
    )
    request_data = {
        "status": "completed",
        "type": "implement",
        "mutation": "replace",
        "old_tag": "old",
        "new_tag": "new",
    }

    preview = client.post("/api/tasks/tags/bulk", data=request_data)

    assert preview.status_code == 200
    assert "Confirm bulk retag" in preview.text
    assert "replace tag &#39;old&#39; with &#39;new&#39;" in preview.text
    assert f"<code>{alpha.id}</code>" in preview.text
    assert f"<code>{beta.id}</code>" in preview.text
    assert pending.id not in preview.text
    assert plan.id not in preview.text
    assert _query_tags(alpha_store, alpha.id, project_id="alpha") == ("keep", "old")
    assert _query_tags(alpha_store, beta.id, project_id="beta") == ("old",)

    targets = re.findall(r'name="target" value="([^"]+)"', preview.text)
    assert set(targets) == {f"alpha|{alpha.id}", f"beta|{beta.id}"}

    applied = client.post(
        "/api/tasks/tags/bulk",
        content=urlencode(
            [
                *(request_data.items()),
                *(("target", target) for target in targets),
                ("preview_token", _hidden_value(preview.text, "preview_token")),
                ("confirmed", "true"),
            ]
        ),
        headers={"content-type": "application/x-www-form-urlencoded"},
    )

    assert applied.status_code == 200
    assert "Bulk retag complete" in applied.text
    assert "Updated 2 tasks." in applied.text
    assert _query_tags(alpha_store, alpha.id, project_id="alpha") == ("keep", "new")
    assert _query_tags(alpha_store, beta.id, project_id="beta") == ("new",)
    assert _query_tags(alpha_store, pending.id, project_id="alpha") == ("old",)
    assert _query_tags(alpha_store, plan.id, project_id="beta") == ("old",)


def test_bulk_confirm_freezes_an_empty_match_set(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    client = _client(store)
    request_data = {"status": "failed", "mutation": "add", "mutation_tag": "triage"}

    preview = client.post("/api/tasks/tags/bulk", data=request_data)
    task = store.add("Failed after preview")
    task.status = "failed"
    store.update(task)
    assert task.id is not None
    applied = client.post(
        "/api/tasks/tags/bulk",
        data={
            **request_data,
            "preview_token": _hidden_value(preview.text, "preview_token"),
            "confirmed": "true",
        },
    )

    assert "Matched 0 tasks" in preview.text
    assert "Updated 0 tasks." in applied.text
    assert _query_tags(store, task.id, project_id="server-test") == ()


def test_bulk_retag_refuses_no_selection_without_mutating(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    task = store.add("Unscoped task", tags=("old",))
    assert task.id is not None

    response = _client(store).post(
        "/api/tasks/tags/bulk",
        json={"add": "new"},
    )

    assert response.status_code == 422
    assert response.json()["detail"] == "bulk retag requires at least one selection filter"
    assert _query_tags(store, task.id, project_id="server-test") == ("old",)


def test_bulk_apply_rejects_target_tampering_without_mutating(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    failed = store.add("Failed task", tags=("old",))
    failed.status = "failed"
    store.update(failed)
    pending = store.add("Pending task", tags=("old",))
    assert failed.id and pending.id
    client = _client(store)
    request_data = {"status": "failed", "mutation": "add", "mutation_tag": "new"}
    preview = client.post("/api/tasks/tags/bulk", data=request_data)
    targets = re.findall(r'name="target" value="([^"]+)"', preview.text)

    response = client.post(
        "/api/tasks/tags/bulk",
        content=urlencode(
            [
                *request_data.items(),
                *(("target", target) for target in targets),
                ("target", f"server-test|{pending.id}"),
                ("preview_token", _hidden_value(preview.text, "preview_token")),
                ("confirmed", "true"),
            ]
        ),
        headers={"content-type": "application/x-www-form-urlencoded"},
    )

    assert response.status_code == 422
    assert _query_tags(store, failed.id, project_id="server-test") == ("old",)
    assert _query_tags(store, pending.id, project_id="server-test") == ("old",)


def test_bulk_apply_rejects_mutation_tampering_without_mutating(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    task = store.add("Failed task", tags=("old",))
    task.status = "failed"
    store.update(task)
    assert task.id
    client = _client(store)
    request_data = {"status": "failed", "mutation": "add", "mutation_tag": "new"}
    preview = client.post("/api/tasks/tags/bulk", data=request_data)

    response = client.post(
        "/api/tasks/tags/bulk",
        data={
            **request_data,
            "target": f"server-test|{task.id}",
            "mutation_tag": "tampered",
            "preview_token": _hidden_value(preview.text, "preview_token"),
            "confirmed": "true",
        },
    )

    assert response.status_code == 422
    assert _query_tags(store, task.id, project_id="server-test") == ("old",)


def test_bulk_apply_rejects_confirmation_without_preview_state(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    task = store.add("Pending task", tags=("old",))
    assert task.id

    response = _client(store).post(
        "/api/tasks/tags/bulk",
        json={
            "status": ["pending"],
            "mutation": "add",
            "mutation_tag": "new",
            "target": [f"server-test|{task.id}"],
            "confirmed": True,
        },
    )

    assert response.status_code == 422
    assert _query_tags(store, task.id, project_id="server-test") == ("old",)


def test_bulk_apply_skips_task_deleted_after_cross_project_preview(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    alpha_store = SqliteTaskStore(db_path, prefix="alpha", project_id="alpha")
    beta_store = SqliteTaskStore(db_path, prefix="beta", project_id="beta")
    alpha = alpha_store.add("Alpha", tags=("old",))
    beta = beta_store.add("Beta", tags=("old",))
    assert alpha.id and beta.id
    client = _client(
        alpha_store,
        mutation_stores={"alpha": alpha_store, "beta": beta_store},
    )
    request_data = {"status": ["pending"], "mutation": "add", "mutation_tag": "new"}
    preview = client.post("/api/tasks/tags/bulk", json=request_data).json()
    targets = [
        f'{task["project_id"]}|{task["id"]}'
        for task in preview["matched_tasks"]
    ]
    assert beta_store.delete(beta.id)

    response = client.post(
        "/api/tasks/tags/bulk",
        json={
            **request_data,
            "target": targets,
            "preview_token": preview["preview_token"],
            "confirmed": True,
        },
    )

    assert response.status_code == 200
    assert response.json()["changed_tasks"] == [{"project_id": "alpha", "id": alpha.id}]
    assert response.json()["skipped_tasks"] == [{"project_id": "beta", "id": beta.id}]
    assert response.json()["changed_count"] == 1
    assert response.json()["skipped_count"] == 1
    assert _query_tags(alpha_store, alpha.id, project_id="alpha") == ("new", "old")


def test_bulk_preview_preflights_every_store_before_any_mutation(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    alpha_store = SqliteTaskStore(db_path, prefix="alpha", project_id="alpha")
    beta_store = SqliteTaskStore(db_path, prefix="beta", project_id="beta")
    alpha = alpha_store.add("Alpha", tags=("old",))
    beta = beta_store.add("Beta", tags=("old",))
    assert alpha.id and beta.id
    client = _client(alpha_store, mutation_stores={"alpha": alpha_store})

    response = client.post(
        "/api/tasks/tags/bulk",
        json={"status": ["pending"], "mutation": "add", "mutation_tag": "new"},
    )

    assert response.status_code == 422
    assert response.json()["detail"] == "could not resolve mutation store for project beta"
    assert _query_tags(alpha_store, alpha.id, project_id="alpha") == ("old",)
    assert _query_tags(alpha_store, beta.id, project_id="beta") == ("old",)


def test_bulk_apply_reports_committed_and_failed_targets(tmp_path: Path) -> None:
    class FailingStore:
        def mutate_task_tags(self, *args: object, **kwargs: object) -> dict[str, bool]:
            raise RuntimeError("write unavailable")

    db_path = tmp_path / "shared.db"
    alpha_store = SqliteTaskStore(db_path, prefix="alpha", project_id="alpha")
    beta_store = SqliteTaskStore(db_path, prefix="beta", project_id="beta")
    alpha = alpha_store.add("Alpha", tags=("old",))
    beta = beta_store.add("Beta", tags=("old",))
    assert alpha.id and beta.id
    client = _client(
        alpha_store,
        mutation_stores={"alpha": alpha_store, "beta": FailingStore()},  # type: ignore[dict-item]
    )
    request_data = {"status": ["pending"], "mutation": "add", "mutation_tag": "new"}
    preview = client.post("/api/tasks/tags/bulk", json=request_data).json()

    response = client.post(
        "/api/tasks/tags/bulk",
        json={
            **request_data,
            "target": [
                f'{task["project_id"]}|{task["id"]}'
                for task in preview["matched_tasks"]
            ],
            "preview_token": preview["preview_token"],
            "confirmed": True,
        },
    )

    assert response.status_code == 200
    assert response.json()["changed_tasks"] == [{"project_id": "alpha", "id": alpha.id}]
    assert response.json()["failed_tasks"] == [{"project_id": "beta", "id": beta.id}]
    assert response.json()["failures"] == [
        {"project_id": "beta", "error": "write unavailable"}
    ]
    assert _query_tags(alpha_store, alpha.id, project_id="alpha") == ("new", "old")
    assert _query_tags(alpha_store, beta.id, project_id="beta") == ("old",)


def test_bulk_retag_requires_exactly_one_mutation(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    task = store.add("Selected task", tags=("old",))
    assert task.id is not None

    response = _client(store).post(
        "/api/tasks/tags/bulk",
        json={"status": ["pending"], "add": "new", "remove": "old"},
    )

    assert response.status_code == 422
    assert response.json()["detail"] == "exactly one tag mutation is required"
    assert _query_tags(store, task.id, project_id="server-test") == ("old",)
