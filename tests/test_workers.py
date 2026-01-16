"""Tests for worker management."""

import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from gza.workers import WorkerMetadata, WorkerRegistry


@pytest.fixture
def temp_workers_dir():
    """Create a temporary workers directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


def test_worker_metadata_serialization():
    """Test WorkerMetadata to_dict and from_dict."""
    worker = WorkerMetadata(
        worker_id="w-20260107-123456",
        pid=12345,
        task_id=1,
        task_slug="20260107-test-task",
        started_at=datetime.now(timezone.utc).isoformat(),
        status="running",
        log_file=".gza/logs/20260107-test-task.log",
        worktree="/tmp/gza-worktrees/test/20260107-test-task",
    )

    # Convert to dict and back
    data = worker.to_dict()
    restored = WorkerMetadata.from_dict(data)

    assert restored.worker_id == worker.worker_id
    assert restored.pid == worker.pid
    assert restored.task_id == worker.task_id
    assert restored.task_slug == worker.task_slug
    assert restored.status == worker.status


def test_registry_generate_worker_id(temp_workers_dir):
    """Test worker ID generation."""
    registry = WorkerRegistry(temp_workers_dir)

    worker_id1 = registry.generate_worker_id()
    worker_id2 = registry.generate_worker_id()

    # Should be unique
    assert worker_id1 != worker_id2

    # Should start with w-
    assert worker_id1.startswith("w-")
    assert worker_id2.startswith("w-")


def test_registry_register_and_get(temp_workers_dir):
    """Test registering and retrieving a worker."""
    registry = WorkerRegistry(temp_workers_dir)

    worker = WorkerMetadata(
        worker_id="w-test-001",
        pid=12345,
        task_id=1,
        task_slug="20260107-test",
        started_at=datetime.now(timezone.utc).isoformat(),
        status="running",
        log_file=".gza/logs/test.log",
        worktree=None,
    )

    registry.register(worker)

    # Should be able to retrieve it
    retrieved = registry.get("w-test-001")
    assert retrieved is not None
    assert retrieved.worker_id == worker.worker_id
    assert retrieved.pid == worker.pid

    # PID file should exist
    pid_file = temp_workers_dir / "w-test-001.pid"
    assert pid_file.exists()
    assert pid_file.read_text().strip() == "12345"

    # Metadata file should exist
    metadata_file = temp_workers_dir / "w-test-001.json"
    assert metadata_file.exists()


def test_registry_update(temp_workers_dir):
    """Test updating worker metadata."""
    registry = WorkerRegistry(temp_workers_dir)

    worker = WorkerMetadata(
        worker_id="w-test-002",
        pid=12346,
        task_id=2,
        task_slug="20260107-test-2",
        started_at=datetime.now(timezone.utc).isoformat(),
        status="running",
        log_file=None,
        worktree=None,
    )

    registry.register(worker)

    # Update with log file
    worker.log_file = ".gza/logs/20260107-test-2.log"
    registry.update(worker)

    # Retrieve and verify
    retrieved = registry.get("w-test-002")
    assert retrieved.log_file == ".gza/logs/20260107-test-2.log"


def test_registry_list_all(temp_workers_dir):
    """Test listing all workers."""
    registry = WorkerRegistry(temp_workers_dir)

    # Register multiple workers
    for i in range(3):
        worker = WorkerMetadata(
            worker_id=f"w-test-{i:03d}",
            pid=10000 + i,
            task_id=i,
            task_slug=f"20260107-task-{i}",
            started_at=datetime.now(timezone.utc).isoformat(),
            status="running",
            log_file=None,
            worktree=None,
        )
        registry.register(worker)

    # List all
    workers = registry.list_all(include_completed=False)
    assert len(workers) == 3

    # Verify sorted by started_at
    assert workers[0].worker_id == "w-test-000"
    assert workers[1].worker_id == "w-test-001"
    assert workers[2].worker_id == "w-test-002"


def test_registry_list_filter_completed(temp_workers_dir):
    """Test filtering completed workers."""
    registry = WorkerRegistry(temp_workers_dir)

    # Register running worker
    running = WorkerMetadata(
        worker_id="w-running",
        pid=10001,
        task_id=1,
        task_slug="20260107-running",
        started_at=datetime.now(timezone.utc).isoformat(),
        status="running",
        log_file=None,
        worktree=None,
    )
    registry.register(running)

    # Register completed worker
    completed = WorkerMetadata(
        worker_id="w-completed",
        pid=10002,
        task_id=2,
        task_slug="20260107-completed",
        started_at=datetime.now(timezone.utc).isoformat(),
        status="completed",
        log_file=None,
        worktree=None,
        exit_code=0,
        completed_at=datetime.now(timezone.utc).isoformat(),
    )
    registry.register(completed)

    # Without include_completed
    workers = registry.list_all(include_completed=False)
    assert len(workers) == 1
    assert workers[0].worker_id == "w-running"

    # With include_completed
    workers = registry.list_all(include_completed=True)
    assert len(workers) == 2


def test_registry_is_running(temp_workers_dir):
    """Test checking if worker is running."""
    registry = WorkerRegistry(temp_workers_dir)

    # Register with our own PID (which is running)
    my_pid = os.getpid()
    worker = WorkerMetadata(
        worker_id="w-test-running",
        pid=my_pid,
        task_id=1,
        task_slug="20260107-test",
        started_at=datetime.now(timezone.utc).isoformat(),
        status="running",
        log_file=None,
        worktree=None,
    )
    registry.register(worker)

    # Should be running (it's our process)
    assert registry.is_running("w-test-running")

    # Register with fake PID (not running)
    fake_worker = WorkerMetadata(
        worker_id="w-test-fake",
        pid=999999,  # Very unlikely to be a real PID
        task_id=2,
        task_slug="20260107-fake",
        started_at=datetime.now(timezone.utc).isoformat(),
        status="running",
        log_file=None,
        worktree=None,
    )
    registry.register(fake_worker)

    # Should not be running
    assert not registry.is_running("w-test-fake")


def test_registry_mark_completed(temp_workers_dir):
    """Test marking a worker as completed."""
    registry = WorkerRegistry(temp_workers_dir)

    worker = WorkerMetadata(
        worker_id="w-test-complete",
        pid=12347,
        task_id=3,
        task_slug="20260107-complete",
        started_at=datetime.now(timezone.utc).isoformat(),
        status="running",
        log_file=None,
        worktree=None,
    )
    registry.register(worker)

    # Mark as completed
    registry.mark_completed("w-test-complete", exit_code=0, status="completed")

    # Retrieve and verify
    retrieved = registry.get("w-test-complete")
    assert retrieved.status == "completed"
    assert retrieved.exit_code == 0
    assert retrieved.completed_at is not None

    # PID file should be removed
    pid_file = temp_workers_dir / "w-test-complete.pid"
    assert not pid_file.exists()


def test_registry_remove(temp_workers_dir):
    """Test removing a worker."""
    registry = WorkerRegistry(temp_workers_dir)

    worker = WorkerMetadata(
        worker_id="w-test-remove",
        pid=12348,
        task_id=4,
        task_slug="20260107-remove",
        started_at=datetime.now(timezone.utc).isoformat(),
        status="running",
        log_file=None,
        worktree=None,
    )
    registry.register(worker)

    # Remove
    registry.remove("w-test-remove")

    # Should not be found
    assert registry.get("w-test-remove") is None

    # Files should be gone
    pid_file = temp_workers_dir / "w-test-remove.pid"
    metadata_file = temp_workers_dir / "w-test-remove.json"
    assert not pid_file.exists()
    assert not metadata_file.exists()


def test_registry_cleanup_stale(temp_workers_dir):
    """Test cleaning up stale workers."""
    registry = WorkerRegistry(temp_workers_dir)

    # Register a worker with fake PID
    stale_worker = WorkerMetadata(
        worker_id="w-test-stale",
        pid=999998,  # Fake PID
        task_id=5,
        task_slug="20260107-stale",
        started_at=datetime.now(timezone.utc).isoformat(),
        status="running",
        log_file=None,
        worktree=None,
    )
    registry.register(stale_worker)

    # Run cleanup
    count = registry.cleanup_stale()
    assert count == 1

    # Worker should be removed
    assert registry.get("w-test-stale") is None
