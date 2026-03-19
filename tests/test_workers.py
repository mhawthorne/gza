"""Tests for worker management."""

import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

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
        startup_log_file=".gza/workers/w-20260107-123456-startup.log",
    )

    # Convert to dict and back
    data = worker.to_dict()
    restored = WorkerMetadata.from_dict(data)

    assert restored.worker_id == worker.worker_id
    assert restored.pid == worker.pid
    assert restored.task_id == worker.task_id
    assert restored.task_slug == worker.task_slug
    assert restored.status == worker.status
    assert restored.startup_log_file == worker.startup_log_file


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


def test_registry_list_all_ignores_entries_without_pid(temp_workers_dir):
    """Malformed metadata without pid should be ignored."""
    registry = WorkerRegistry(temp_workers_dir)
    (temp_workers_dir / "w-bad-missing-pid.json").write_text('{"worker_id": "w-bad-missing-pid", "task_id": 1}')

    workers = registry.list_all(include_completed=True)
    assert workers == []


def test_registry_list_all_ignores_entries_with_zero_pid(temp_workers_dir):
    """Non-positive pid metadata should be ignored."""
    registry = WorkerRegistry(temp_workers_dir)
    (temp_workers_dir / "w-bad-zero-pid.json").write_text('{"worker_id": "w-bad-zero-pid", "task_id": 1, "pid": 0}')

    workers = registry.list_all(include_completed=True)
    assert workers == []


def test_registry_stop_does_not_signal_for_non_numeric_pid(temp_workers_dir):
    """Malformed non-numeric pid should not trigger os.kill."""
    registry = WorkerRegistry(temp_workers_dir)
    (temp_workers_dir / "w-bad-nonnumeric-pid.json").write_text(
        '{"worker_id": "w-bad-nonnumeric-pid", "task_id": 1, "pid": "not-a-number"}'
    )

    with patch("gza.workers.os.kill") as mock_kill:
        assert registry.stop("w-bad-nonnumeric-pid") is False
        assert registry.is_running("w-bad-nonnumeric-pid") is False

    mock_kill.assert_not_called()


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


def test_registry_remove_deletes_startup_log_artifact(temp_workers_dir):
    """Removing a worker also deletes its startup log file."""
    registry = WorkerRegistry(temp_workers_dir)
    startup_log = temp_workers_dir / "w-test-remove-startup-startup.log"
    startup_log.write_text("startup output")

    worker = WorkerMetadata(
        worker_id="w-test-remove-startup",
        pid=12349,
        task_id=5,
        task_slug="20260107-remove-startup",
        started_at=datetime.now(timezone.utc).isoformat(),
        status="failed",
        log_file=None,
        worktree=None,
        startup_log_file=".gza/workers/w-test-remove-startup-startup.log",
    )
    registry.register(worker)

    registry.remove("w-test-remove-startup")

    assert not startup_log.exists()


def test_registry_remove_rejects_absolute_path_outside_workers_dir(temp_workers_dir, tmp_path):
    """startup_log_file pointing outside workers_dir via absolute path is NOT deleted."""
    registry = WorkerRegistry(temp_workers_dir)
    outside_file = tmp_path / "outside-secret.log"
    outside_file.write_text("sensitive data")

    worker = WorkerMetadata(
        worker_id="w-test-abs-escape",
        pid=12350,
        task_id=6,
        task_slug="20260107-abs-escape",
        started_at=datetime.now(timezone.utc).isoformat(),
        status="failed",
        log_file=None,
        worktree=None,
        startup_log_file=str(outside_file),
    )
    registry.register(worker)
    registry.remove("w-test-abs-escape")

    assert outside_file.exists(), "File outside workers_dir must not be deleted"


def test_registry_remove_rejects_traversal_path(temp_workers_dir, tmp_path):
    """startup_log_file with ../ traversal outside workers_dir is NOT deleted."""
    registry = WorkerRegistry(temp_workers_dir)
    # Create a file that traversal would resolve to
    outside_file = temp_workers_dir.parent / "traversal-target.log"
    outside_file.write_text("should survive")

    worker = WorkerMetadata(
        worker_id="w-test-traversal",
        pid=12351,
        task_id=7,
        task_slug="20260107-traversal",
        started_at=datetime.now(timezone.utc).isoformat(),
        status="failed",
        log_file=None,
        worktree=None,
        startup_log_file="../traversal-target.log",
    )
    registry.register(worker)
    registry.remove("w-test-traversal")

    assert outside_file.exists(), "Traversal path outside workers_dir must not be deleted"


def test_registry_remove_deletes_valid_startup_log_under_workers_dir(temp_workers_dir):
    """startup_log_file within workers_dir is still properly deleted."""
    registry = WorkerRegistry(temp_workers_dir)
    startup_log = temp_workers_dir / "w-test-valid-startup.log"
    startup_log.write_text("startup output")

    worker = WorkerMetadata(
        worker_id="w-test-valid",
        pid=12352,
        task_id=8,
        task_slug="20260107-valid",
        started_at=datetime.now(timezone.utc).isoformat(),
        status="failed",
        log_file=None,
        worktree=None,
        startup_log_file=".gza/workers/w-test-valid-startup.log",
    )
    registry.register(worker)
    registry.remove("w-test-valid")

    assert not startup_log.exists(), "Valid startup log under workers_dir should be deleted"


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
