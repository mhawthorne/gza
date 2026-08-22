import asyncio
import json
from pathlib import Path

from fastapi.testclient import TestClient
from gza_server.app import create_app
from gza_server.log_stream import stream_log
from gza_server.task_detail import query_task_detail

from gza.db import SqliteTaskStore


def _project(tmp_path: Path) -> tuple[SqliteTaskStore, Path]:
    (tmp_path / ".gza").mkdir(parents=True, exist_ok=True)
    (tmp_path / "gza.yaml").write_text(
        "project_name: server-test\n"
        "provider: claude\n"
        "model: claude-opus-5\n"
        "db_path: .gza/gza.db\n"
    )
    store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix="srv", project_id="server-test")
    return store, tmp_path


def _append(project_dir: Path, slug: str, text: str) -> None:
    log_dir = project_dir / ".gza" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    record = {"type": "assistant", "message": {"content": [{"type": "text", "text": text}]}}
    with (log_dir / f"{slug}.log").open("a") as handle:
        handle.write(json.dumps(record) + "\n")


def _frames(chunks: list[str], event: str) -> list[dict]:
    payloads = []
    for chunk in chunks:
        if chunk.startswith(f"event: {event}\n"):
            payloads.append(json.loads(chunk.split("data: ", 1)[1].strip()))
    return payloads


def _collect(store: SqliteTaskStore, task_id: str, project_dir: Path, on_poll) -> list[str]:
    """Drive the stream with a fake clock, stepping the world on each poll."""
    polls = {"count": 0}

    async def sleep(_seconds: float) -> None:
        polls["count"] += 1
        on_poll(polls["count"])

    async def run() -> list[str]:
        chunks: list[str] = []
        agen = stream_log(
            lambda: query_task_detail(store, task_id),
            fallback_root=project_dir,
            poll_seconds=0,
            sleep=sleep,
            clock=lambda: float(polls["count"]),
            max_seconds=50,
        )
        async for chunk in agen:
            chunks.append(chunk)
        return chunks

    return asyncio.run(run())


def test_stream_emits_appended_entries_and_closes_on_terminal_status(tmp_path: Path) -> None:
    store, project_dir = _project(tmp_path)
    task = store.add("Do the thing.", task_type="implement")
    task.slug = "running"
    task.status = "in_progress"
    store.update(task)
    _append(project_dir, "running", "first")

    def on_poll(count: int) -> None:
        if count == 1:
            _append(project_dir, "running", "second")
        if count == 2:
            _append(project_dir, "running", "third")
            current = store.get(task.id or "")
            assert current is not None
            current.status = "completed"
            store.update(current)

    chunks = _collect(store, task.id or "", project_dir, on_poll)

    bodies = [event["body"] for frame in _frames(chunks, "entries") for event in frame["events"]]
    assert bodies == ["first", "second", "third"]

    (end,) = _frames(chunks, "end")
    assert end["reason"] == "task_finished"
    assert end["status"] == "completed"


def test_stream_resumes_from_the_callers_offset_without_repeating(tmp_path: Path) -> None:
    store, project_dir = _project(tmp_path)
    task = store.add("Do the thing.", task_type="implement")
    task.slug = "resumed"
    task.status = "in_progress"
    store.update(task)
    _append(project_dir, "resumed", "already read")
    already = (project_dir / ".gza" / "logs" / "resumed.log").stat().st_size

    def on_poll(count: int) -> None:
        if count == 1:
            _append(project_dir, "resumed", "brand new")
            current = store.get(task.id or "")
            assert current is not None
            current.status = "completed"
            store.update(current)

    async def sleep(_seconds: float) -> None:
        on_poll(1)

    async def run() -> list[str]:
        return [
            chunk
            async for chunk in stream_log(
                lambda: query_task_detail(store, task.id or ""),
                offset=already,
                fallback_root=project_dir,
                poll_seconds=0,
                sleep=sleep,
                clock=lambda: 0.0,
            )
        ]

    chunks = asyncio.run(run())
    bodies = [event["body"] for frame in _frames(chunks, "entries") for event in frame["events"]]
    assert bodies == ["brand new"]


def test_stream_reports_a_missing_log_instead_of_failing(tmp_path: Path) -> None:
    store, project_dir = _project(tmp_path)
    task = store.add("Do the thing.", task_type="implement")
    task.slug = "not-started"
    task.status = "in_progress"
    store.update(task)

    def on_poll(count: int) -> None:
        current = store.get(task.id or "")
        assert current is not None
        current.status = "failed"
        store.update(current)

    chunks = _collect(store, task.id or "", project_dir, on_poll)

    waiting = _frames(chunks, "waiting")
    assert waiting and "No log found at" in waiting[0]["message"]
    assert _frames(chunks, "end")[0]["reason"] == "task_finished"


def test_stream_stops_at_the_time_limit(tmp_path: Path) -> None:
    store, project_dir = _project(tmp_path)
    task = store.add("Do the thing.", task_type="implement")
    task.slug = "endless"
    task.status = "in_progress"
    store.update(task)
    _append(project_dir, "endless", "working")

    polls = {"count": 0}

    async def sleep(_seconds: float) -> None:
        polls["count"] += 1

    async def run() -> list[str]:
        return [
            chunk
            async for chunk in stream_log(
                lambda: query_task_detail(store, task.id or ""),
                fallback_root=project_dir,
                poll_seconds=0,
                sleep=sleep,
                clock=lambda: float(polls["count"]),
                max_seconds=3,
            )
        ]

    chunks = asyncio.run(run())

    assert _frames(chunks, "end")[0]["reason"] == "time_limit"


def test_stream_endpoint_refuses_a_finished_task_and_points_at_paging(tmp_path: Path) -> None:
    store, project_dir = _project(tmp_path)
    task = store.add("Do the thing.", task_type="implement")
    task.slug = "done"
    task.status = "completed"
    store.update(task)
    client = TestClient(create_app(store_factory=lambda: store, project_dir=project_dir))

    response = client.get(f"/api/tasks/{task.id}/log/stream")

    assert response.status_code == 409
    assert "?offset=" in response.json()["detail"]
