import json
from pathlib import Path

from fastapi.testclient import TestClient
from gza_server.app import create_app

from gza.db import SqliteTaskStore


def _client(store: SqliteTaskStore, project_dir: Path) -> TestClient:
    return TestClient(create_app(store_factory=lambda: store, project_dir=project_dir))


def _project(tmp_path: Path) -> tuple[SqliteTaskStore, Path]:
    """A minimal gza project whose config resolves a real log directory."""
    (tmp_path / ".gza").mkdir(parents=True, exist_ok=True)
    (tmp_path / "gza.yaml").write_text(
        "project_name: server-test\n"
        "provider: claude\n"
        "model: claude-opus-5\n"
        "db_path: .gza/gza.db\n"
    )
    store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix="srv", project_id="server-test")
    return store, tmp_path


def _write_log(project_dir: Path, slug: str, records: list[dict]) -> Path:
    log_dir = project_dir / ".gza" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / f"{slug}.log"
    path.write_text("".join(json.dumps(record) + "\n" for record in records))
    return path


def _assistant(text: str) -> dict:
    return {"type": "assistant", "message": {"content": [{"type": "text", "text": text}]}}


def test_missing_log_names_the_path_it_checked(tmp_path: Path) -> None:
    store, project_dir = _project(tmp_path)
    task = store.add("Do the thing.", task_type="implement")
    task.slug = "do-the-thing"
    store.update(task)
    client = _client(store, project_dir)

    response = client.get(f"/tasks/{task.id}/log")
    record = client.get(f"/api/tasks/{task.id}/log").json()

    assert response.status_code == 200
    assert "No log found at" in response.text
    assert "do-the-thing.log" in response.text
    assert record["missing"] is not None
    assert record["events"] == []


def test_log_page_renders_events_and_links_from_task_detail(tmp_path: Path) -> None:
    store, project_dir = _project(tmp_path)
    task = store.add("Do the thing.", task_type="implement")
    task.slug = "do-the-thing"
    store.update(task)
    _write_log(
        project_dir,
        "do-the-thing",
        [
            {"type": "system", "subtype": "init", "model": "claude-opus-5"},
            _assistant("Starting work."),
            {
                "type": "assistant",
                "message": {
                    "content": [
                        {"type": "tool_use", "name": "Bash", "input": {"command": "pytest -q"}}
                    ]
                },
            },
            {"type": "result", "num_steps": 3, "duration_ms": 1500},
        ],
    )
    client = _client(store, project_dir)

    detail = client.get(f"/tasks/{task.id}")
    page = client.get(f"/tasks/{task.id}/log")
    record = client.get(f"/api/tasks/{task.id}/log").json()

    assert f"{detail.url.path}/log" in detail.text
    assert page.status_code == 200
    assert "Starting work." in page.text
    assert "pytest -q" in page.text
    assert [event["kind"] for event in record["events"]] == [
        "system",
        "text",
        "tool_use",
        "result",
    ]
    assert record["eof"] is True
    assert record["next_offset"] == record["size"]


def test_offset_cursor_pages_forward_without_repeating_entries(tmp_path: Path) -> None:
    store, project_dir = _project(tmp_path)
    task = store.add("Do the thing.", task_type="implement")
    task.slug = "paged"
    store.update(task)
    _write_log(project_dir, "paged", [_assistant(f"line {index}") for index in range(40)])
    client = _client(store, project_dir)

    first = client.get(f"/api/tasks/{task.id}/log?offset=0&limit=200").json()
    second = client.get(
        f"/api/tasks/{task.id}/log?offset={first['next_offset']}&limit=200"
    ).json()

    assert first["eof"] is False
    assert first["start_offset"] == 0
    first_bodies = [event["body"] for event in first["events"]]
    second_bodies = [event["body"] for event in second["events"]]
    assert first_bodies
    assert second_bodies
    assert not set(first_bodies) & set(second_bodies)
    assert second["start_offset"] == first["next_offset"]


def test_log_defaults_to_the_tail_of_a_large_log(tmp_path: Path) -> None:
    store, project_dir = _project(tmp_path)
    task = store.add("Do the thing.", task_type="implement")
    task.slug = "big"
    store.update(task)
    _write_log(project_dir, "big", [_assistant(f"line {index}") for index in range(400)])
    client = _client(store, project_dir)

    record = client.get(f"/api/tasks/{task.id}/log?limit=400").json()

    assert record["truncated_head"] is True
    assert record["eof"] is True
    assert record["events"][-1]["body"] == "line 399"


def test_ops_stream_is_served_separately_from_the_conversation(tmp_path: Path) -> None:
    store, project_dir = _project(tmp_path)
    task = store.add("Do the thing.", task_type="implement")
    task.slug = "with-ops"
    store.update(task)
    _write_log(project_dir, "with-ops", [_assistant("conversation only")])
    ops = project_dir / ".gza" / "logs" / "with-ops.ops.jsonl"
    ops.write_text(json.dumps({"type": "gza", "subtype": "phase", "message": "verify"}) + "\n")
    client = _client(store, project_dir)

    conversation = client.get(f"/api/tasks/{task.id}/log").json()
    ops_record = client.get(f"/api/tasks/{task.id}/log?stream=ops").json()

    assert [event["body"] for event in conversation["events"]] == ["conversation only"]
    assert ops_record["stream"] == "ops"
    assert [event["title"] for event in ops_record["events"]] == ["phase"]


def test_running_task_is_marked_so_the_page_can_follow_it(tmp_path: Path) -> None:
    store, project_dir = _project(tmp_path)
    task = store.add("Do the thing.", task_type="implement")
    task.slug = "running"
    task.status = "in_progress"
    store.update(task)
    _write_log(project_dir, "running", [_assistant("working")])
    client = _client(store, project_dir)

    record = client.get(f"/api/tasks/{task.id}/log").json()
    page = client.get(f"/tasks/{task.id}/log")

    assert record["is_running"] is True
    assert 'data-running="true"' in page.text


def test_unknown_task_log_is_a_404(tmp_path: Path) -> None:
    store, project_dir = _project(tmp_path)
    client = _client(store, project_dir)

    assert client.get("/tasks/srv-9999/log").status_code == 404
    assert client.get("/api/tasks/srv-9999/log").status_code == 404
