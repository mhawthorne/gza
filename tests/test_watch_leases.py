"""Tests for watch-supervisor project lease helpers."""

import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

from gza.db import SqliteTaskStore
from gza.watch_leases import (
    WATCH_SUPERVISOR_LEASE_NAME,
    WatchLeaseConflict,
    WatchLeaseReleaseError,
    WatchLeaseSet,
    WatchLeaseStore,
    WatchLeaseTarget,
    acquire_watch_project_leases,
)


def _store(tmp_path: Path, name: str) -> SqliteTaskStore:
    return SqliteTaskStore(tmp_path / f"{name}.db", project_id=name, prefix=name)


class RecordingStore:
    def __init__(self, key: str, store: SqliteTaskStore, release_order: list[str]) -> None:
        self.key = key
        self.store = store
        self.release_order = release_order

    @property
    def project_id(self) -> str:
        return self.store.project_id

    def try_acquire_project_lease(self, **kwargs):
        return self.store.try_acquire_project_lease(**kwargs)

    def release_project_lease(self, **kwargs) -> bool:
        self.release_order.append(self.key)
        return self.store.release_project_lease(**kwargs)


class RaisingAcquireStore:
    def __init__(self, key: str, error: Exception) -> None:
        self.key = key
        self.error = error

    @property
    def project_id(self) -> str:
        return self.key

    def try_acquire_project_lease(self, **kwargs):
        raise self.error

    def release_project_lease(self, **kwargs) -> bool:
        raise AssertionError("unacquired store should not be released")


class RaisingReleaseStore(RecordingStore):
    def __init__(
        self,
        key: str,
        store: SqliteTaskStore,
        release_order: list[str],
        error: Exception,
    ) -> None:
        super().__init__(key, store, release_order)
        self.error = error

    def release_project_lease(self, **kwargs) -> bool:
        self.release_order.append(self.key)
        raise self.error


def test_watch_lease_helper_acquires_and_releases_single_store(tmp_path: Path) -> None:
    store = _store(tmp_path, "core")

    leases = acquire_watch_project_leases(
        [WatchLeaseTarget("core", store)],
        owner_token="run-token",
    )

    assert [held.target.key for held in leases.held] == ["core"]
    assert [result.released for result in leases.release()] == [True]
    assert (
        store.try_acquire_project_lease(
            lease_name=WATCH_SUPERVISOR_LEASE_NAME,
            owner_pid=os.getpid(),
            owner_token="other-token",
        )
        is not None
    )


def test_watch_lease_helper_acquires_two_stores_and_releases_in_reverse_order(tmp_path: Path) -> None:
    first = _store(tmp_path, "first")
    second = _store(tmp_path, "second")

    leases = acquire_watch_project_leases(
        [
            WatchLeaseTarget("first", first),
            WatchLeaseTarget("second", second),
        ],
        owner_token="run-token",
    )

    assert [held.target.key for held in leases.held] == ["first", "second"]
    assert [result.target_key for result in leases.release()] == ["second", "first"]


def test_watch_lease_conflict_rolls_back_earlier_acquired_leases(tmp_path: Path) -> None:
    first = _store(tmp_path, "first")
    second = _store(tmp_path, "second")
    third = _store(tmp_path, "third")
    release_order: list[str] = []
    first_recording: WatchLeaseStore = RecordingStore("first", first, release_order)
    third_recording: WatchLeaseStore = RecordingStore("third", third, release_order)
    second_recording: WatchLeaseStore = RecordingStore("second", second, release_order)
    blocking = second.try_acquire_project_lease(
        lease_name=WATCH_SUPERVISOR_LEASE_NAME,
        owner_pid=os.getpid(),
        owner_token="blocking-token",
    )
    assert blocking is not None

    with pytest.raises(WatchLeaseConflict) as exc:
        acquire_watch_project_leases(
            [
                WatchLeaseTarget("first", first_recording),
                WatchLeaseTarget("third", third_recording),
                WatchLeaseTarget("second", second_recording),
            ],
            owner_token="run-token",
        )

    assert exc.value.target_key == "second"
    assert release_order == ["third", "first"]
    assert (
        first.try_acquire_project_lease(
            lease_name=WATCH_SUPERVISOR_LEASE_NAME,
            owner_pid=os.getpid(),
            owner_token="after-rollback",
        )
        is not None
    )
    assert (
        second.try_acquire_project_lease(
            lease_name=WATCH_SUPERVISOR_LEASE_NAME,
            owner_pid=os.getpid(),
            owner_token="after-conflict",
        )
        is None
    )
    assert (
        third.try_acquire_project_lease(
            lease_name=WATCH_SUPERVISOR_LEASE_NAME,
            owner_pid=os.getpid(),
            owner_token="after-third-conflict",
        )
        is not None
    )


def test_watch_lease_acquisition_error_rolls_back_earlier_acquired_lease(tmp_path: Path) -> None:
    first = _store(tmp_path, "first")
    release_order: list[str] = []
    first_recording: WatchLeaseStore = RecordingStore("first", first, release_order)
    second_raising: WatchLeaseStore = RaisingAcquireStore("second", RuntimeError("database unavailable"))

    with pytest.raises(RuntimeError, match="database unavailable"):
        acquire_watch_project_leases(
            [
                WatchLeaseTarget("first", first_recording),
                WatchLeaseTarget("second", second_raising),
            ],
            owner_token="run-token",
        )

    assert release_order == ["first"]
    assert (
        first.try_acquire_project_lease(
            lease_name=WATCH_SUPERVISOR_LEASE_NAME,
            owner_pid=os.getpid(),
            owner_token="after-rollback",
        )
        is not None
    )


def test_watch_lease_release_attempts_all_targets_and_surfaces_release_failure(tmp_path: Path) -> None:
    first = _store(tmp_path, "first")
    second = _store(tmp_path, "second")
    third = _store(tmp_path, "third")
    release_order: list[str] = []

    leases = acquire_watch_project_leases(
        [
            WatchLeaseTarget("first", RecordingStore("first", first, release_order)),
            WatchLeaseTarget(
                "second",
                RaisingReleaseStore("second", second, release_order, RuntimeError("release failed")),
            ),
            WatchLeaseTarget("third", RecordingStore("third", third, release_order)),
        ],
        owner_token="run-token",
    )

    with pytest.raises(WatchLeaseReleaseError) as exc:
        leases.release()

    assert release_order == ["third", "second", "first"]
    assert [result.target_key for result in exc.value.results] == ["third", "second", "first"]
    assert [result.released for result in exc.value.results] == [True, False, True]
    assert [failure.target_key for failure in exc.value.failures] == ["second"]
    assert str(exc.value.failures[0].error) == "release failed"
    assert (
        first.try_acquire_project_lease(
            lease_name=WATCH_SUPERVISOR_LEASE_NAME,
            owner_pid=os.getpid(),
            owner_token="after-release",
        )
        is not None
    )


def test_watch_lease_conflict_with_rollback_failure_preserves_both_errors(tmp_path: Path) -> None:
    first = _store(tmp_path, "first")
    second = _store(tmp_path, "second")
    release_order: list[str] = []
    first_raising: WatchLeaseStore = RaisingReleaseStore(
        "first",
        first,
        release_order,
        RuntimeError("rollback release failed"),
    )
    blocking = second.try_acquire_project_lease(
        lease_name=WATCH_SUPERVISOR_LEASE_NAME,
        owner_pid=os.getpid(),
        owner_token="blocking-token",
    )
    assert blocking is not None

    with pytest.raises(ExceptionGroup) as exc:
        acquire_watch_project_leases(
            [
                WatchLeaseTarget("first", first_raising),
                WatchLeaseTarget("second", second),
            ],
            owner_token="run-token",
        )

    conflict = next(error for error in exc.value.exceptions if isinstance(error, WatchLeaseConflict))
    cleanup = next(error for error in exc.value.exceptions if isinstance(error, WatchLeaseReleaseError))
    assert conflict.target_key == "second"
    assert [failure.target_key for failure in cleanup.failures] == ["first"]
    assert str(cleanup.failures[0].error) == "rollback release failed"
    assert release_order == ["first"]


def test_watch_lease_helper_steals_stale_owner_through_store_api(tmp_path: Path) -> None:
    store = _store(tmp_path, "core")
    stale = store.try_acquire_project_lease(
        lease_name=WATCH_SUPERVISOR_LEASE_NAME,
        owner_pid=-1,
        owner_token="stale-token",
    )
    assert stale is not None

    leases = acquire_watch_project_leases(
        [WatchLeaseTarget("core", store)],
        owner_token="fresh-token",
    )

    assert leases.held[0].lease.owner_token == "fresh-token"
    assert leases.held[0].lease.owner_pid == os.getpid()


def test_watch_lease_release_never_clears_another_owner_token(tmp_path: Path) -> None:
    store = _store(tmp_path, "core")
    leases = acquire_watch_project_leases(
        [WatchLeaseTarget("core", store)],
        owner_token="real-token",
    )
    wrong_owner = WatchLeaseSet(
        owner_pid=leases.owner_pid,
        owner_token="wrong-token",
        held=leases.held,
    )

    assert [result.released for result in wrong_owner.release()] == [False]
    assert (
        store.try_acquire_project_lease(
            lease_name=WATCH_SUPERVISOR_LEASE_NAME,
            owner_pid=os.getpid(),
            owner_token="other-token",
        )
        is None
    )
    assert [result.released for result in leases.release()] == [True]


def test_watch_lease_helper_adopts_already_owned_live_lease_with_same_run_token(tmp_path: Path) -> None:
    store = _store(tmp_path, "core")
    first_acquired_at = datetime(2026, 1, 1, tzinfo=UTC)
    renewed_at = datetime(2026, 1, 2, tzinfo=UTC)
    first = acquire_watch_project_leases(
        [WatchLeaseTarget("core", store)],
        owner_token="run-token",
        acquired_at=first_acquired_at,
    )

    adopted = acquire_watch_project_leases(
        [WatchLeaseTarget("core", store)],
        owner_token=first.owner_token,
        acquired_at=renewed_at,
    )

    assert adopted.owner_token == first.owner_token
    assert adopted.held[0].lease.owner_pid == os.getpid()
    assert adopted.held[0].lease.acquired_at == renewed_at
    assert (
        store.try_acquire_project_lease(
            lease_name=WATCH_SUPERVISOR_LEASE_NAME,
            owner_pid=os.getpid(),
            owner_token="other-token",
        )
        is None
    )
    assert [result.released for result in adopted.release()] == [True]
