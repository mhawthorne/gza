"""The tag dashboard answers what merged, what runs, and what waits."""

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from fastapi.testclient import TestClient
from gza_server.app import create_app
from gza_server.tag_dashboard import (
    DEFAULT_WINDOW_KEY,
    default_dashboard_tag,
    query_tag_dashboard,
    resolve_window,
)

from gza.db import SqliteTaskStore, Task

NOW = datetime(2026, 8, 21, 18, 0, tzinfo=UTC)
TAG = "v0.5.1"


def _client(store: SqliteTaskStore) -> TestClient:
    return TestClient(create_app(store_factory=lambda: store))


def _store(tmp_path: Path) -> SqliteTaskStore:
    return SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")


def _task(
    store: SqliteTaskStore,
    prompt: str,
    *,
    status: str,
    tags: tuple[str, ...] = (TAG,),
    branch: str | None = None,
    created_at: datetime = NOW - timedelta(hours=2),
    started_at: datetime | None = None,
) -> Task:
    task = store.add(prompt, task_type="implement", tags=tags, branch=branch)
    task.status = status
    task.created_at = created_at
    task.started_at = started_at
    store.update(task)
    return task


def _merged_unit(
    store: SqliteTaskStore,
    *,
    prompt: str,
    merged_at: datetime,
    tags: tuple[str, ...] = (TAG,),
    branch: str = "feature/merged",
) -> tuple[Task, str]:
    owner = _task(store, prompt, status="completed", tags=tags, branch=branch)
    unit = store.create_merge_unit(
        source_branch=branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="merged",
        merged_at=merged_at,
        diff_files_changed=2,
        diff_lines_added=30,
        diff_lines_removed=4,
    )
    assert owner.id is not None
    store.attach_task_to_merge_unit(owner.id, unit.id, "owner")
    return owner, unit.id


def test_dashboard_separates_merged_in_flight_and_queued(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _owner, unit_id = _merged_unit(
        store, prompt="Landed work", merged_at=NOW - timedelta(hours=2)
    )
    running = _task(
        store,
        "Running work",
        status="in_progress",
        started_at=NOW - timedelta(minutes=30),
    )
    waiting = _task(store, "Waiting work", status="pending")

    dashboard = query_tag_dashboard(store, TAG, now=NOW)

    assert [item.id for item in dashboard.merged] == [unit_id]
    assert [item.id for item in dashboard.in_flight] == [running.id]
    assert [item.id for item in dashboard.queued] == [waiting.id]
    assert dashboard.merged[0].files_changed == 2
    assert dashboard.merged[0].merge_unit_url == (
        f"/projects/server-test/merge-units/{unit_id}"
    )
    assert dashboard.in_flight[0].age == "30m"


def test_other_tags_are_excluded_from_every_section(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _merged_unit(
        store,
        prompt="Other merged",
        merged_at=NOW - timedelta(hours=1),
        tags=("other",),
        branch="feature/other",
    )
    _task(store, "Other running", status="in_progress", tags=("other",))
    _task(store, "Other waiting", status="pending", tags=("other",))

    dashboard = query_tag_dashboard(store, TAG, now=NOW)

    assert dashboard.merged == ()
    assert dashboard.in_flight == ()
    assert dashboard.queued == ()


def test_the_window_bounds_the_merged_section_only(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _recent_owner, recent_unit = _merged_unit(
        store,
        prompt="Recent merge",
        merged_at=NOW - timedelta(hours=2),
        branch="feature/recent",
    )
    _merged_unit(
        store,
        prompt="Old merge",
        merged_at=NOW - timedelta(days=5),
        branch="feature/old",
    )
    waiting = _task(store, "Waiting work", status="pending")

    day = query_tag_dashboard(store, TAG, window=resolve_window("24h"), now=NOW)
    week = query_tag_dashboard(store, TAG, window=resolve_window("7d"), now=NOW)

    assert [item.id for item in day.merged] == [recent_unit]
    assert len(week.merged) == 2
    # The queue is a live view; the window must not touch it.
    assert [item.id for item in day.queued] == [waiting.id]
    assert [item.id for item in week.queued] == [waiting.id]


def test_a_merge_just_outside_the_window_is_excluded(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _merged_unit(
        store,
        prompt="Just too old",
        merged_at=NOW - timedelta(hours=24, minutes=1),
        branch="feature/edge",
    )

    dashboard = query_tag_dashboard(store, TAG, window=resolve_window("24h"), now=NOW)

    assert dashboard.merged == ()


def test_merged_rows_are_newest_first(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _owner_old, older = _merged_unit(
        store,
        prompt="Older",
        merged_at=NOW - timedelta(hours=6),
        branch="feature/older",
    )
    _owner_new, newer = _merged_unit(
        store,
        prompt="Newer",
        merged_at=NOW - timedelta(hours=1),
        branch="feature/newer",
    )

    dashboard = query_tag_dashboard(store, TAG, now=NOW)

    assert [item.id for item in dashboard.merged] == [newer, older]


def test_an_unresolved_window_key_falls_back_to_the_default(tmp_path: Path) -> None:
    assert resolve_window("nonsense").key == DEFAULT_WINDOW_KEY
    assert resolve_window(None).key == DEFAULT_WINDOW_KEY
    assert resolve_window("7d").key == "7d"


def test_page_renders_sections_and_round_trips_the_window(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _owner, unit_id = _merged_unit(
        store, prompt="Landed work", merged_at=NOW - timedelta(hours=2)
    )
    client = _client(store)

    response = client.get(f"/tags/{TAG}?window=7d")

    assert response.status_code == 200
    assert f"<h1>{TAG}</h1>" in response.text
    assert "Merged" in response.text
    assert "In flight" in response.text
    assert "Queued" in response.text
    assert f'href="/projects/server-test/merge-units/{unit_id}"' in response.text
    assert '<option value="7d" selected>' in response.text


def test_api_reports_all_three_sections(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _owner, unit_id = _merged_unit(
        store, prompt="Landed work", merged_at=NOW - timedelta(hours=2)
    )
    waiting = _task(store, "Waiting work", status="pending")
    client = _client(store)

    record = client.get(f"/api/tags/{TAG}").json()

    assert record["tag"] == TAG
    assert record["window"] == DEFAULT_WINDOW_KEY
    assert [item["id"] for item in record["merged"]] == [unit_id]
    assert [item["id"] for item in record["queued"]] == [waiting.id]
    assert record["watch_is_running_on_this_tag"] is False


def test_dashboard_reports_a_live_watch_covering_the_tag(tmp_path: Path) -> None:
    store = _store(tmp_path)
    # Recorded at real "now": the rendered page has no injectable clock, and a
    # backdated heartbeat would correctly read as a watch that has gone away.
    store.record_watch_session(
        owner_pid=os.getpid(),
        tags=(TAG, "system"),
        batch_size=4,
        poll_seconds=300.0,
    )
    client = _client(store)

    dashboard = query_tag_dashboard(store, TAG)
    page = client.get(f"/tags/{TAG}")

    assert dashboard.watch_is_running_on_this_tag is True
    assert dashboard.watch_scopes[0].batch_size == 4
    assert "Watch is running on this tag." in page.text


def test_a_watch_whose_heartbeat_went_stale_is_not_reported_as_running(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    store.record_watch_session(
        owner_pid=os.getpid(),
        tags=(TAG,),
        poll_seconds=300.0,
        started_at=NOW - timedelta(hours=6),
        now=NOW - timedelta(hours=6),
    )

    dashboard = query_tag_dashboard(store, TAG, now=NOW)

    assert dashboard.watch_scopes == ()
    assert dashboard.watch_is_running_on_this_tag is False


def test_a_watch_on_another_tag_is_not_claimed_as_covering_this_one(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    store.record_watch_session(owner_pid=os.getpid(), tags=("other",))

    dashboard = query_tag_dashboard(store, TAG)

    assert dashboard.watch_scopes != ()
    assert dashboard.watch_is_running_on_this_tag is False


def test_default_tag_comes_from_the_running_watch(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.record_watch_session(owner_pid=os.getpid(), tags=(TAG, "system"))

    assert default_dashboard_tag(store) == TAG


def test_default_tag_is_absent_without_a_running_watch(tmp_path: Path) -> None:
    store = _store(tmp_path)

    assert default_dashboard_tag(store, now=NOW) is None


def test_tags_index_redirects_to_the_watched_tag(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.record_watch_session(owner_pid=os.getpid(), tags=(TAG,))
    client = _client(store)

    response = client.get("/tags", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"].startswith(f"/tags/{TAG}")


def test_tags_index_offers_a_picker_without_a_running_watch(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _task(store, "Some work", status="pending")
    client = _client(store)

    response = client.get("/tags", follow_redirects=False)

    assert response.status_code == 200
    assert "Pick a tag" in response.text
    assert f'href="/tags/{TAG}"' in response.text


def test_watch_scopes_api_exposes_the_live_scope(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.record_watch_session(owner_pid=os.getpid(), tags=(TAG,), batch_size=4)
    client = _client(store)

    record = client.get("/api/watch-scopes").json()

    assert record["default_tag"] == TAG
    assert record["scopes"][0]["tags"] == [TAG]
    assert record["scopes"][0]["batch_size"] == 4


def test_an_unknown_tag_renders_an_empty_dashboard_rather_than_an_error(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    client = _client(store)

    response = client.get("/tags/never-used")

    assert response.status_code == 200
    assert "Nothing merged on this tag" in response.text
    assert "Nothing is queued on this tag." in response.text


def test_dependency_blocked_work_is_shown_separately_from_the_queue(
    tmp_path: Path,
) -> None:
    """A blocked task must not vanish behind an empty queue.

    ``gza next`` reports these as "blocked by dependencies" rather than listing
    them, so a page that showed only the pickup lane would claim nothing is
    waiting while real work sits behind a dependency.
    """
    store = _store(tmp_path)
    blocker = _task(store, "Blocking work", status="pending")
    blocked = store.add(
        "Dependent work",
        task_type="implement",
        tags=(TAG,),
        depends_on=blocker.id,
    )

    dashboard = query_tag_dashboard(store, TAG, now=NOW)

    assert [item.id for item in dashboard.queued] == [blocker.id]
    assert [item.id for item in dashboard.blocked] == [blocked.id]


def test_blocked_section_is_empty_when_nothing_waits_on_a_dependency(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    _task(store, "Free work", status="pending")

    dashboard = query_tag_dashboard(store, TAG, now=NOW)

    assert len(dashboard.queued) == 1
    assert dashboard.blocked == ()
