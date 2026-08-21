"""A running watch publishes the scope it is covering."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from gza.db import SqliteTaskStore


@pytest.fixture
def store(tmp_path: Path) -> SqliteTaskStore:
    return SqliteTaskStore(tmp_path / "tasks.db", prefix="wat", project_id="watch-test")


def test_recording_captures_the_full_tag_scope(store: SqliteTaskStore) -> None:
    session = store.record_watch_session(
        owner_pid=os.getpid(),
        tags=("v0.5.1", "system"),
        batch_size=4,
        poll_seconds=300.0,
    )

    assert session.tags == ("v0.5.1", "system")
    assert session.batch_size == 4
    assert session.poll_seconds == 300.0
    assert session.project_id == "watch-test"
    assert store.get_active_watch_session() is not None


def test_recording_drops_duplicate_tags_but_keeps_order(store: SqliteTaskStore) -> None:
    session = store.record_watch_session(
        owner_pid=os.getpid(),
        tags=("system", "v0.5.1", "system", ""),
    )

    assert session.tags == ("system", "v0.5.1")


def test_an_untagged_watch_records_an_empty_scope(store: SqliteTaskStore) -> None:
    session = store.record_watch_session(owner_pid=os.getpid(), tags=())

    assert session.tags == ()
    assert store.get_active_watch_session() is not None


def test_heartbeat_advances_without_moving_the_start_time(store: SqliteTaskStore) -> None:
    started = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)
    store.record_watch_session(
        owner_pid=os.getpid(),
        tags=("v0.5.1",),
        poll_seconds=300.0,
        started_at=started,
        now=started,
    )

    later = started + timedelta(minutes=5)
    assert store.heartbeat_watch_session(owner_pid=os.getpid(), now=later) is True

    session = store.list_watch_sessions(active_only=False)[0]
    assert session.started_at == started
    assert session.heartbeat_at == later


def test_re_recording_an_existing_pid_preserves_the_original_start(
    store: SqliteTaskStore,
) -> None:
    started = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)
    store.record_watch_session(
        owner_pid=os.getpid(),
        tags=("v0.5.1",),
        started_at=started,
        now=started,
    )

    later = started + timedelta(hours=1)
    updated = store.record_watch_session(
        owner_pid=os.getpid(),
        tags=("system",),
        started_at=later,
        now=later,
    )

    assert updated.started_at == started
    assert updated.heartbeat_at == later
    assert updated.tags == ("system",)


def test_heartbeat_for_an_unrecorded_pid_reports_no_row(store: SqliteTaskStore) -> None:
    assert store.heartbeat_watch_session(owner_pid=os.getpid()) is False


def test_a_stale_heartbeat_reads_as_inactive(store: SqliteTaskStore) -> None:
    started = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)
    store.record_watch_session(
        owner_pid=os.getpid(),
        tags=("v0.5.1",),
        poll_seconds=300.0,
        started_at=started,
        now=started,
    )

    # Well past four poll intervals: this watch has not checked in.
    long_after = started + timedelta(hours=3)

    assert store.get_active_watch_session(now=long_after) is None
    assert store.list_watch_sessions(active_only=True, now=long_after) == []
    # The raw ledger still holds the row, so a crash remains diagnosable.
    assert len(store.list_watch_sessions(active_only=False, now=long_after)) == 1


def test_a_slow_cycle_is_not_mistaken_for_a_crash(store: SqliteTaskStore) -> None:
    started = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)
    store.record_watch_session(
        owner_pid=os.getpid(),
        tags=("v0.5.1",),
        poll_seconds=300.0,
        started_at=started,
        now=started,
    )

    # Two poll intervals is a slow cycle, not a dead watch.
    slightly_after = started + timedelta(seconds=600)

    assert store.get_active_watch_session(now=slightly_after) is not None


def test_a_dead_pid_reads_as_inactive_despite_a_fresh_heartbeat(
    store: SqliteTaskStore,
) -> None:
    """A pid that no longer exists cannot be running, however recent its row."""
    dead_pid = _unused_pid()
    now = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)
    store.record_watch_session(
        owner_pid=dead_pid,
        tags=("v0.5.1",),
        poll_seconds=300.0,
        started_at=now,
        now=now,
    )

    assert store.get_active_watch_session(now=now) is None
    assert len(store.list_watch_sessions(active_only=False, now=now)) == 1


def test_clearing_removes_the_row(store: SqliteTaskStore) -> None:
    store.record_watch_session(owner_pid=os.getpid(), tags=("v0.5.1",))

    assert store.clear_watch_session(owner_pid=os.getpid()) is True
    assert store.list_watch_sessions(active_only=False) == []
    assert store.clear_watch_session(owner_pid=os.getpid()) is False


def test_two_projects_record_independently(tmp_path: Path) -> None:
    db_path = tmp_path / "tasks.db"
    first = SqliteTaskStore(db_path, prefix="one", project_id="project-one")
    second = SqliteTaskStore(db_path, prefix="two", project_id="project-two")

    first.record_watch_session(owner_pid=os.getpid(), tags=("v0.5.1",))
    second.record_watch_session(owner_pid=os.getpid(), tags=("system",))

    first_session = first.get_active_watch_session()
    second_session = second.get_active_watch_session()
    assert first_session is not None and first_session.tags == ("v0.5.1",)
    assert second_session is not None and second_session.tags == ("system",)

    first.clear_watch_session(owner_pid=os.getpid())
    assert first.get_active_watch_session() is None
    assert second.get_active_watch_session() is not None


def test_the_newest_live_watch_wins_when_several_are_recorded(
    store: SqliteTaskStore,
) -> None:
    now = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)
    store.record_watch_session(
        owner_pid=os.getpid(),
        tags=("older",),
        started_at=now - timedelta(minutes=10),
        now=now - timedelta(minutes=10),
    )
    store.record_watch_session(
        owner_pid=os.getppid(),
        tags=("newer",),
        started_at=now,
        now=now,
    )

    active = store.get_active_watch_session(now=now)

    assert active is not None
    assert active.tags == ("newer",)


def _unused_pid() -> int:
    """Return a pid that is not running.

    Walks down from a high pid rather than picking one, so the test does not
    depend on a specific number being free on this machine.
    """
    for candidate in range(4_000_000, 3_999_000, -1):
        try:
            os.kill(candidate, 0)
        except ProcessLookupError:
            return candidate
        except (PermissionError, OverflowError):
            continue
    pytest.skip("could not find an unused pid on this host")
