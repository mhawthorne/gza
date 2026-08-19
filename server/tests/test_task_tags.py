import re
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlencode

import pytest
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
        ),
        headers={"origin": "http://testserver"},
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

    response = TestClient(
        create_app(project_dir=tmp_path),
        headers={"origin": "http://testserver"},
    ).post(
        f"/api/tasks/{task.id}/tags",
        json={"add": ["new"]},
    )

    assert response.status_code == 200
    assert _query_tags(store, task.id, project_id="servertest") == ("new", "old")


@pytest.mark.parametrize("field", ["add", "remove"])
@pytest.mark.parametrize(
    "invalid_value",
    [
        "scalar",
        {"unexpected": True},
        None,
        [["nested"]],
        [None],
        [{"unexpected": True}],
        [7],
        [""],
    ],
    ids=[
        "scalar",
        "object",
        "null",
        "nested-array",
        "null-member",
        "object-member",
        "scalar-member",
        "empty-member",
    ],
)
def test_task_tag_json_rejects_non_array_or_non_string_values_without_mutating(
    tmp_path: Path,
    field: str,
    invalid_value: object,
) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    task = store.add("Tagged task", tags=("old",))
    assert task.id is not None

    response = _client(store).post(
        f"/api/tasks/{task.id}/tags",
        json={field: invalid_value},
    )

    assert response.status_code == 422
    assert _query_tags(store, task.id, project_id="server-test") == ("old",)


@pytest.mark.parametrize("project_id", [None, 7, {"project": "server-test"}])
def test_task_tag_json_rejects_non_string_project_id_without_mutating(
    tmp_path: Path,
    project_id: object,
) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    task = store.add("Tagged task", tags=("old",))
    assert task.id is not None

    response = _client(store).post(
        f"/api/tasks/{task.id}/tags",
        json={"project_id": project_id, "add": ["new"]},
    )

    assert response.status_code == 422
    assert _query_tags(store, task.id, project_id="server-test") == ("old",)


def test_task_tag_form_accepts_scalar_add_value(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    task = store.add("Tagged task", tags=("old",))
    assert task.id is not None

    response = _client(store).post(
        f"/api/tasks/{task.id}/tags",
        data={"add": "new"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert _query_tags(store, task.id, project_id="server-test") == ("new", "old")


def test_task_tag_form_accepts_scalar_remove_with_blank_add_field(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    task = store.add("Tagged task", tags=("old",))
    assert task.id is not None

    response = _client(store).post(
        f"/api/tasks/{task.id}/tags",
        data={"project_id": "server-test", "add": "", "remove": "old"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert _query_tags(store, task.id, project_id="server-test") == ()


@pytest.mark.parametrize(
    ("endpoint", "content_type", "body"),
    [
        ("task", "application/json", b'{"add": ['),
        ("task", "application/x-www-form-urlencoded", b"add=\xff"),
        ("bulk", "application/json", b'{"status": ['),
        ("bulk", "application/x-www-form-urlencoded", b"status=\xff"),
    ],
    ids=["task-json", "task-form", "bulk-json", "bulk-form"],
)
def test_tag_mutation_endpoints_reject_malformed_bodies_before_resolution(
    tmp_path: Path,
    endpoint: str,
    content_type: str,
    body: bytes,
) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    task = store.add("Tagged task", tags=("old",))
    assert task.id is not None
    query_resolutions: list[None] = []
    mutation_resolutions: list[str] = []

    def store_factory() -> SqliteTaskStore:
        query_resolutions.append(None)
        return store

    def mutation_store_factory(project_id: str) -> SqliteTaskStore:
        mutation_resolutions.append(project_id)
        return store

    client = TestClient(
        create_app(
            store_factory=store_factory,
            mutation_store_factory=mutation_store_factory,
        ),
        headers={"origin": "http://testserver"},
    )
    path = f"/api/tasks/{task.id}/tags" if endpoint == "task" else "/api/tasks/tags/bulk"

    response = client.post(path, content=body, headers={"content-type": content_type})

    assert response.status_code == 400
    assert "preview_token" not in response.json()
    assert query_resolutions == []
    assert mutation_resolutions == []
    assert _query_tags(store, task.id, project_id="server-test") == ("old",)


def test_task_tag_form_rejects_cross_origin_write_then_accepts_rendered_form(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    task = store.add("Tagged task", tags=("old",))
    assert task.id is not None
    client = _client(store)
    page = client.get(f"/tasks/{task.id}")
    action_match = re.search(r'<form method="post" action="([^"]+)">', page.text)
    assert action_match is not None

    rejected = client.post(
        action_match.group(1),
        data={"project_id": "server-test", "add": "new"},
        headers={"origin": "https://hostile.example"},
        follow_redirects=False,
    )

    assert rejected.status_code == 403
    assert _query_tags(store, task.id, project_id="server-test") == ("old",)

    accepted = client.post(
        action_match.group(1),
        data={"project_id": "server-test", "add": "new"},
        follow_redirects=False,
    )

    assert accepted.status_code == 303
    assert _query_tags(store, task.id, project_id="server-test") == ("new", "old")


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


@pytest.mark.parametrize(
    "mutation_payload",
    [
        {kind: value}
        for kind in ("add", "remove")
        for value in (None, 7, {"unexpected": True}, ["nested"])
    ],
    ids=[
        f"{kind}-{value_kind}"
        for kind in ("add", "remove")
        for value_kind in ("null", "scalar", "object", "array")
    ],
)
def test_bulk_preview_rejects_non_string_add_remove_without_confirmation_or_mutation(
    tmp_path: Path,
    mutation_payload: dict[str, object],
) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    task = store.add("Selected task", tags=("old",))
    assert task.id is not None
    resolved_projects: list[str] = []

    def mutation_store_factory(project_id: str) -> SqliteTaskStore:
        resolved_projects.append(project_id)
        return store

    client = TestClient(
        create_app(
            store_factory=lambda: store,
            mutation_store_factory=mutation_store_factory,
        ),
        headers={"origin": "http://testserver"},
    )
    response = client.post(
        "/api/tasks/tags/bulk",
        json={"status": ["pending"], **mutation_payload},
    )

    assert response.status_code == 422
    assert "preview_token" not in response.json()
    assert resolved_projects == []
    assert _query_tags(store, task.id, project_id="server-test") == ("old",)


@pytest.mark.parametrize("kind", ["add", "remove"])
@pytest.mark.parametrize(
    "invalid_value",
    [None, 7, {"unexpected": True}, ["nested"], ""],
    ids=["null", "scalar", "object", "array", "empty"],
)
def test_bulk_preview_rejects_invalid_mutation_tag_without_confirmation_or_mutation(
    tmp_path: Path,
    kind: str,
    invalid_value: object,
) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    task = store.add("Selected task", tags=("old",))
    assert task.id is not None
    resolved_projects: list[str] = []

    def mutation_store_factory(project_id: str) -> SqliteTaskStore:
        resolved_projects.append(project_id)
        return store

    response = TestClient(
        create_app(
            store_factory=lambda: store,
            mutation_store_factory=mutation_store_factory,
        ),
        headers={"origin": "http://testserver"},
    ).post(
        "/api/tasks/tags/bulk",
        json={
            "status": ["pending"],
            "mutation": kind,
            "mutation_tag": invalid_value,
        },
    )

    assert response.status_code == 422
    assert "preview_token" not in response.json()
    assert resolved_projects == []
    assert _query_tags(store, task.id, project_id="server-test") == ("old",)


@pytest.mark.parametrize("field", ["old_tag", "new_tag"])
@pytest.mark.parametrize(
    "invalid_value",
    [None, 7, {"unexpected": True}, ["nested"], ""],
    ids=["null", "scalar", "object", "array", "empty"],
)
def test_bulk_preview_rejects_non_string_replace_fields_without_confirmation_or_mutation(
    tmp_path: Path,
    field: str,
    invalid_value: object,
) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    task = store.add("Selected task", tags=("old",))
    assert task.id is not None
    resolved_projects: list[str] = []
    replace_fields: dict[str, object] = {"old_tag": "old", "new_tag": "new"}
    replace_fields[field] = invalid_value

    def mutation_store_factory(project_id: str) -> SqliteTaskStore:
        resolved_projects.append(project_id)
        return store

    response = TestClient(
        create_app(
            store_factory=lambda: store,
            mutation_store_factory=mutation_store_factory,
        ),
        headers={"origin": "http://testserver"},
    ).post(
        "/api/tasks/tags/bulk",
        json={
            "status": ["pending"],
            "mutation": "replace",
            **replace_fields,
        },
    )

    assert response.status_code == 422
    assert "preview_token" not in response.json()
    assert resolved_projects == []
    assert _query_tags(store, task.id, project_id="server-test") == ("old",)


@pytest.mark.parametrize("operand", [0, 1], ids=["old", "new"])
@pytest.mark.parametrize(
    "invalid_value",
    [None, 7, {"unexpected": True}, ["nested"], ""],
    ids=["null", "scalar", "object", "array", "empty"],
)
def test_bulk_preview_rejects_non_string_direct_replace_operands_without_mutation(
    tmp_path: Path,
    operand: int,
    invalid_value: object,
) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    task = store.add("Selected task", tags=("old",))
    assert task.id is not None
    replace: list[object] = ["old", "new"]
    replace[operand] = invalid_value

    response = _client(store).post(
        "/api/tasks/tags/bulk",
        json={"status": ["pending"], "replace": replace},
    )

    assert response.status_code == 422
    assert "preview_token" not in response.json()
    assert _query_tags(store, task.id, project_id="server-test") == ("old",)


@pytest.mark.parametrize(
    "direct_field",
    [
        {"add": "other"},
        {"remove": "old"},
        {"replace": ["old", "other"]},
    ],
    ids=["add", "remove", "replace"],
)
def test_bulk_preview_rejects_discriminator_mixed_with_direct_mutation(
    tmp_path: Path,
    direct_field: dict[str, object],
) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    task = store.add("Selected task", tags=("old",))
    assert task.id is not None
    resolved_projects: list[str] = []
    client = TestClient(
        create_app(
            store_factory=lambda: store,
            mutation_store_factory=lambda project_id: (
                resolved_projects.append(project_id) or store
            ),
        )
    )

    response = client.post(
        "/api/tasks/tags/bulk",
        json={
            "status": ["pending"],
            "mutation": "add",
            "mutation_tag": "new",
            **direct_field,
        },
    )

    assert response.status_code == 422
    assert "preview_token" not in response.json()
    assert resolved_projects == []
    assert _query_tags(store, task.id, project_id="server-test") == ("old",)


@pytest.mark.parametrize(
    "mutation_payload",
    [
        {"add": "new", "remove": None},
        {"add": "new", "replace": None},
        {"remove": "old", "add": None},
        {"remove": "old", "replace": None},
        {"replace": ["old", "new"], "add": None},
        {"replace": ["old", "new"], "remove": None},
    ],
    ids=[
        "add-null-remove",
        "add-null-replace",
        "remove-null-add",
        "remove-null-replace",
        "replace-null-add",
        "replace-null-remove",
    ],
)
def test_bulk_preview_rejects_direct_mutation_with_null_competing_field(
    tmp_path: Path,
    mutation_payload: dict[str, object],
) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    task = store.add("Selected task", tags=("old",))
    assert task.id is not None
    resolved_projects: list[str] = []
    client = TestClient(
        create_app(
            store_factory=lambda: store,
            mutation_store_factory=lambda project_id: (
                resolved_projects.append(project_id) or store
            ),
        )
    )

    response = client.post(
        "/api/tasks/tags/bulk",
        json={"status": ["pending"], **mutation_payload},
    )

    assert response.status_code == 422
    assert "preview_token" not in response.json()
    assert resolved_projects == []
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


def test_bulk_form_apply_requires_same_origin_and_preserves_preview_state(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    task = store.add("Pending task", tags=("old",))
    assert task.id is not None
    client = _client(store)
    request_data = {"status": "pending", "mutation": "add", "mutation_tag": "new"}
    preview = client.post("/api/tasks/tags/bulk", data=request_data)
    apply_data = {
        **request_data,
        "target": f"server-test|{task.id}",
        "preview_token": _hidden_value(preview.text, "preview_token"),
        "confirmed": "true",
    }

    rejected = client.post(
        "/api/tasks/tags/bulk",
        data=apply_data,
        headers={"origin": "https://hostile.example"},
    )

    assert rejected.status_code == 403
    assert _query_tags(store, task.id, project_id="server-test") == ("old",)

    applied = client.post("/api/tasks/tags/bulk", data=apply_data)

    assert applied.status_code == 200
    assert "Updated 1 task." in applied.text
    assert _query_tags(store, task.id, project_id="server-test") == ("new", "old")


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
