from __future__ import annotations

import html
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from gza_server import task_edit as task_edit_module
from gza_server.app import create_app

import gza.db as db_module
from gza.db import SqliteTaskStore


def _client(store: SqliteTaskStore) -> TestClient:
    return TestClient(
        create_app(
            store_factory=lambda: store,
            mutation_store_factory=lambda project_id: store,
        ),
        headers={"origin": "http://testserver"},
    )


def _shared_project_stores(
    tmp_path: Path,
) -> tuple[SqliteTaskStore, SqliteTaskStore]:
    db_path = tmp_path / "shared.db"
    stores: list[SqliteTaskStore] = []
    for project_id in ("alpha", "beta"):
        project_root = tmp_path / project_id
        project_root.mkdir()
        (project_root / "gza.yaml").write_text(
            f"project_name: {project_id.title()}\n"
            f"project_id: {project_id}\n"
            "project_prefix: same\n"
            f"db_path: {db_path}\n",
            encoding="utf-8",
        )
        stores.append(
            SqliteTaskStore(
                db_path,
                prefix="same",
                project_id=project_id,
                project_root=project_root,
                project_name=project_id.title(),
            )
        )
    return stores[0], stores[1]


def _production_mutation_client(store: SqliteTaskStore) -> TestClient:
    return TestClient(
        create_app(store_factory=lambda: store),
        headers={"origin": "http://testserver"},
    )


def test_shared_database_prompt_edit_selects_qualified_project_store(
    tmp_path: Path,
) -> None:
    alpha_store, beta_store = _shared_project_stores(tmp_path)
    alpha = alpha_store.add("Alpha original prompt", task_type="implement")
    beta = beta_store.add("Beta original prompt", task_type="implement")
    assert alpha.id is not None and alpha.id == beta.id

    response = _production_mutation_client(alpha_store).post(
        f"/api/tasks/{alpha.id}/prompt",
        json={"project_id": "alpha", "prompt": "Alpha updated prompt"},
    )

    assert response.status_code == 200
    assert response.json()["project_id"] == "alpha"
    assert alpha_store.get(alpha.id).prompt == "Alpha updated prompt"  # type: ignore[union-attr]
    assert beta_store.get(beta.id).prompt == "Beta original prompt"  # type: ignore[union-attr]


def test_shared_database_plan_edit_uses_qualified_owner_root_and_row(
    tmp_path: Path,
) -> None:
    alpha_store, beta_store = _shared_project_stores(tmp_path)
    alpha = alpha_store.add("Alpha plan", task_type="plan")
    beta = beta_store.add("Beta plan", task_type="plan")
    assert alpha.id is not None and alpha.id == beta.id
    relative_report = Path("plans") / "shared.md"
    original_by_project = {
        "alpha": "## Alpha original plan\n",
        "beta": "## Beta original plan\n",
    }
    for store, task in ((alpha_store, alpha), (beta_store, beta)):
        original = original_by_project[store.project_id]
        report_path = store.project_root / relative_report  # type: ignore[operator]
        report_path.parent.mkdir(parents=True)
        report_path.write_text(original, encoding="utf-8")
        task.report_file = relative_report.as_posix()
        task.output_content = original
        store.update(task)

    updated = "## Alpha revised plan\n\n1. Ship safely\n"
    response = _production_mutation_client(alpha_store).post(
        f"/api/tasks/{alpha.id}/plan",
        json={"project_id": "alpha", "plan": updated},
    )

    assert response.status_code == 200
    assert response.json()["project_id"] == "alpha"
    assert alpha_store.get(alpha.id).output_content == updated  # type: ignore[union-attr]
    assert beta_store.get(beta.id).output_content == original_by_project["beta"]  # type: ignore[union-attr]
    assert (alpha_store.project_root / relative_report).read_text() == updated  # type: ignore[operator]
    assert (beta_store.project_root / relative_report).read_text() == original_by_project["beta"]  # type: ignore[operator]


def test_shared_database_prompt_edit_rejects_ambiguous_unknown_and_mismatched_project(
    tmp_path: Path,
) -> None:
    alpha_store, beta_store = _shared_project_stores(tmp_path)
    alpha = alpha_store.add("Alpha original prompt", task_type="implement")
    beta = beta_store.add("Beta original prompt", task_type="implement")
    assert alpha.id is not None and alpha.id == beta.id
    client = _production_mutation_client(alpha_store)

    ambiguous = client.post(
        f"/api/tasks/{alpha.id}/prompt",
        json={"prompt": "Ambiguous update"},
    )
    unknown = client.post(
        f"/api/tasks/{alpha.id}/prompt",
        json={"project_id": "unknown", "prompt": "Unknown update"},
    )

    assert alpha_store.project_root is not None
    (alpha_store.project_root / "gza.yaml").write_text(
        "project_name: Mismatched\n"
        "project_id: mismatched\n"
        "project_prefix: same\n"
        f"db_path: {alpha_store.db_path}\n",
        encoding="utf-8",
    )
    mismatched = client.post(
        f"/api/tasks/{alpha.id}/prompt",
        json={"project_id": "alpha", "prompt": "Mismatched update"},
    )

    assert ambiguous.status_code == 409
    assert "ambiguous across projects: alpha, beta" in ambiguous.json()["detail"]
    assert unknown.status_code == 404
    assert mismatched.status_code == 422
    assert alpha_store.get(alpha.id).prompt == "Alpha original prompt"  # type: ignore[union-attr]
    assert beta_store.get(beta.id).prompt == "Beta original prompt"  # type: ignore[union-attr]


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
            "prompt": "  \n## Updated prompt\n\nShip the **safe** version.\n  ",
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
    assert 'name="prompt"' not in response.text
    assert 'class="content-editor"' not in response.text
    assert "Save prompt" not in response.text
    assert 'class="submitted-content"' in response.text
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


def test_prompt_edit_rejects_worker_claim_between_read_and_persistence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "tasks.db"
    editor_store = SqliteTaskStore(
        db_path,
        prefix="srv",
        project_id="server-test",
        project_root=tmp_path,
    )
    worker_store = SqliteTaskStore(
        db_path,
        prefix="srv",
        project_id="server-test",
        project_root=tmp_path,
    )
    task = editor_store.add("Original prompt", task_type="implement")
    assert task.id is not None
    read_complete = threading.Event()
    allow_persistence = threading.Event()
    original_normalize = db_module.normalize_task_prompt

    def paused_normalize(prompt: str) -> str:
        read_complete.set()
        assert allow_persistence.wait(timeout=5)
        return original_normalize(prompt)

    monkeypatch.setattr(db_module, "normalize_task_prompt", paused_normalize)
    with ThreadPoolExecutor(max_workers=1) as executor:
        edit = executor.submit(
            task_edit_module.edit_task_prompt,
            editor_store,
            task.id,
            "Updated prompt",
        )
        assert read_complete.wait(timeout=5)
        claim = worker_store.try_mark_in_progress(task.id, pid=4321)
        assert claim.task is not None
        allow_persistence.set()
        with pytest.raises(
            task_edit_module.TaskEditConflict,
            match="prompt edits are only allowed for pending tasks",
        ):
            edit.result(timeout=5)

    persisted = worker_store.get(task.id)
    assert persisted is not None
    assert persisted.prompt == "Original prompt"
    assert persisted.status == "in_progress"
    assert persisted.started_at == claim.task.started_at
    assert persisted.running_pid == 4321


def test_concurrent_plan_edits_keep_file_and_scoped_db_fields_synchronized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "tasks.db"
    first_store = SqliteTaskStore(
        db_path,
        prefix="srv",
        project_id="server-test",
        project_root=tmp_path,
    )
    second_store = SqliteTaskStore(
        db_path,
        prefix="srv",
        project_id="server-test",
        project_root=tmp_path,
    )
    observer_store = SqliteTaskStore(
        db_path,
        prefix="srv",
        project_id="server-test",
        project_root=tmp_path,
    )
    task = first_store.add("Create a plan", task_type="plan")
    assert task.id is not None
    report_path = tmp_path / ".gza" / "plans" / "concurrent.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    original = "## Original\n\nKeep this synchronized."
    report_path.write_text(original, encoding="utf-8")
    task.report_file = report_path.relative_to(tmp_path).as_posix()
    task.output_content = original
    first_store.update(task)

    first_content = "## Writer A\n\nA complete submitted version."
    second_content = "## Writer B\n\nAnother complete submitted version."
    first_file_written = threading.Event()
    release_first_writer = threading.Event()
    second_replace_entered = threading.Event()
    original_replace = task_edit_module._replace_text

    def controlled_replace(path: Path, content: str) -> None:
        original_replace(path, content)
        if content == first_content:
            first_file_written.set()
            assert release_first_writer.wait(timeout=5)
        elif content == second_content:
            second_replace_entered.set()

    monkeypatch.setattr(task_edit_module, "_replace_text", controlled_replace)
    with ThreadPoolExecutor(max_workers=2) as executor:
        first_save = executor.submit(
            task_edit_module.edit_task_plan,
            first_store,
            task.id,
            first_content,
        )
        assert first_file_written.wait(timeout=5)

        claim = observer_store.try_mark_in_progress(task.id, pid=9876)
        assert claim.task is not None
        second_save = executor.submit(
            task_edit_module.edit_task_plan,
            second_store,
            task.id,
            second_content,
        )
        assert not second_replace_entered.wait(timeout=0.2)

        release_first_writer.set()
        first_save.result(timeout=5)
        second_save.result(timeout=5)

    persisted = observer_store.get(task.id)
    assert persisted is not None
    assert report_path.read_bytes() == second_content.encode("utf-8")
    assert persisted.output_content == second_content
    assert persisted.report_file == report_path.relative_to(tmp_path).as_posix()
    assert persisted.status == "in_progress"
    assert persisted.started_at == claim.task.started_at
    assert persisted.running_pid == 9876


def test_non_plan_form_rejection_preserves_error_and_submitted_markdown(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(
        tmp_path / "tasks.db",
        prefix="srv",
        project_id="server-test",
        project_root=tmp_path,
    )
    task = store.add("Implement the plan", task_type="implement")
    assert task.id is not None
    client = _client(store)
    attempted = "## Rejected\n\n<em>Keep & show this</em>"

    response = client.post(
        f"/api/tasks/{task.id}/plan",
        data={"project_id": "server-test", "plan": attempted},
    )

    assert response.status_code == 422
    assert f"Task {task.id} is not a plan task" in response.text
    assert html.escape(attempted) in response.text
    assert 'name="plan"' not in response.text
    assert 'class="content-editor"' not in response.text
    assert "Save plan" not in response.text
    assert 'class="submitted-content"' in response.text
    assert "?edit=plan" not in response.text
    forced_edit = client.get(f"/tasks/{task.id}?edit=plan")
    assert 'name="plan"' not in forced_edit.text


def test_plan_form_rejection_preserves_text_when_file_content_disappears(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(
        tmp_path / "tasks.db",
        prefix="srv",
        project_id="server-test",
        project_root=tmp_path,
    )
    task = store.add("Create a file-only plan", task_type="plan")
    assert task.id is not None
    report_path = tmp_path / ".gza" / "plans" / "file-only.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("## File-only plan", encoding="utf-8")
    task.report_file = report_path.relative_to(tmp_path).as_posix()
    store.update(task)

    def mutation_store_factory(project_id: str) -> SqliteTaskStore:
        assert project_id == "server-test"
        report_path.unlink(missing_ok=True)
        return store

    client = TestClient(
        create_app(
            store_factory=lambda: store,
            mutation_store_factory=mutation_store_factory,
        ),
        headers={"origin": "http://testserver"},
    )
    attempted = "## Still here\n\n<mark>Preserve & retry</mark>"

    response = client.post(
        f"/api/tasks/{task.id}/plan",
        data={"project_id": "server-test", "plan": attempted},
    )

    assert response.status_code == 422
    assert f"Task {task.id} has no plan content to edit" in response.text
    assert html.escape(attempted) in response.text
    assert 'name="plan"' not in response.text
    assert 'class="content-editor"' not in response.text
    assert "Save plan" not in response.text
    assert 'class="submitted-content"' in response.text
    assert "?edit=plan" not in response.text


def test_eligible_prompt_validation_error_preserves_exact_text_in_active_editor(
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
    attempted = "  short  "

    response = client.post(
        f"/api/tasks/{task.id}/prompt",
        data={"project_id": "server-test", "prompt": attempted},
    )

    assert response.status_code == 422
    assert "Prompt is too short" in response.text
    assert f">{html.escape(attempted)}</textarea>" in response.text
    assert 'name="prompt"' in response.text
    assert 'class="content-editor"' in response.text
    assert "Save prompt" in response.text
