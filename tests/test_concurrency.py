import os
import threading
import time

import pytest

from gza.concurrency import MaxConcurrentTasksError, get_concurrency_snapshot, launch_permit
from gza.config import Config
from gza.workers import WorkerMetadata, WorkerRegistry
from tests.cli.conftest import make_store, setup_config


def _append_config(tmp_path, extra: str) -> None:
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + extra)


def test_launch_permit_allows_under_limit(tmp_path) -> None:
    setup_config(tmp_path)
    _append_config(tmp_path, "max_concurrent: 1\n")
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    permit = launch_permit(config, store)
    try:
        assert permit.snapshot.limit == 1
        assert permit.snapshot.running == 0
        assert permit.snapshot.available == 1
    finally:
        permit.release()


def test_launch_permit_rejects_at_limit(tmp_path) -> None:
    setup_config(tmp_path)
    _append_config(tmp_path, "max_concurrent: 1\n")
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Running task", task_type="implement")
    task.status = "in_progress"
    task.running_pid = os.getpid()
    store.update(task)

    with pytest.raises(MaxConcurrentTasksError, match="already at max concurrent tasks: 1 running, limit is 1"):
        launch_permit(config, store)


def test_launch_permit_allows_same_pid_reentry(tmp_path) -> None:
    setup_config(tmp_path)
    _append_config(tmp_path, "max_concurrent: 1\n")
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Running task", task_type="implement")
    task.status = "in_progress"
    task.running_pid = os.getpid()
    store.update(task)

    permit = launch_permit(config, store, current_pid=os.getpid())
    try:
        assert permit.snapshot.current_pid_counted is True
    finally:
        permit.release()


def test_snapshot_ignores_dead_registry_and_task_pids(tmp_path) -> None:
    setup_config(tmp_path)
    _append_config(tmp_path, "max_concurrent: 2\n")
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Dead task", task_type="implement")
    task.status = "in_progress"
    task.running_pid = 999999
    store.update(task)

    registry = WorkerRegistry(config.workers_path)
    registry.register(
        WorkerMetadata(
            worker_id=registry.generate_worker_id(),
            task_id=task.id,
            pid=999999,
        )
    )

    snapshot = get_concurrency_snapshot(config, store)
    assert snapshot.running == 0
    assert snapshot.available == 2
    assert snapshot.running_task_ids == ()


def test_launch_permit_allows_same_process_reentry_without_deadlock(tmp_path) -> None:
    setup_config(tmp_path)
    _append_config(tmp_path, "max_concurrent: 2\n")
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    events: list[str] = []

    def _worker(name: str, delay: float) -> None:
        permit = launch_permit(config, store)
        events.append(f"{name}-acquired")
        time.sleep(delay)
        permit.release()
        events.append(f"{name}-released")

    first = threading.Thread(target=_worker, args=("first", 0.2))
    second = threading.Thread(target=_worker, args=("second", 0.0))

    first.start()
    time.sleep(0.05)
    second.start()
    first.join()
    second.join()

    assert sorted(events) == [
        "first-acquired",
        "first-released",
        "second-acquired",
        "second-released",
    ]


def test_launch_permit_blocks_other_threads_until_owner_releases(tmp_path) -> None:
    setup_config(tmp_path)
    _append_config(tmp_path, "max_concurrent: 1\n")
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    ready = threading.Event()
    release_first = threading.Event()
    events: list[str] = []

    def _first() -> None:
        permit = launch_permit(config, store)
        events.append("first-acquired")
        ready.set()
        release_first.wait(timeout=1)
        permit.release()
        events.append("first-released")

    def _second() -> None:
        ready.wait(timeout=1)
        events.append("second-waiting")
        permit = launch_permit(config, store)
        events.append("second-acquired")
        permit.release()
        events.append("second-released")

    first = threading.Thread(target=_first)
    second = threading.Thread(target=_second)
    first.start()
    second.start()
    ready.wait(timeout=1)
    time.sleep(0.05)
    assert events == ["first-acquired", "second-waiting"]
    release_first.set()
    first.join()
    second.join()

    assert events == [
        "first-acquired",
        "second-waiting",
        "first-released",
        "second-acquired",
        "second-released",
    ]
