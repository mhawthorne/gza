"""Worker management for background task execution."""

import json
import os
import signal
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass
class WorkerMetadata:
    """Metadata for a background worker."""
    worker_id: str
    pid: int
    task_id: int | None  # Database task ID (numeric)
    task_slug: str | None  # Task slug (YYYYMMDD-slug format)
    started_at: str  # ISO format timestamp
    status: str  # running, completed, failed
    log_file: str | None
    worktree: str | None
    is_background: bool = True  # True for background workers, False for foreground
    exit_code: int | None = None
    completed_at: str | None = None

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return asdict(self)

    @staticmethod
    def from_dict(data: dict) -> "WorkerMetadata":
        """Create from dictionary."""
        return WorkerMetadata(**data)


class WorkerRegistry:
    """Manages the registry of background workers."""

    def __init__(self, workers_dir: Path):
        """Initialize the worker registry.

        Args:
            workers_dir: Directory to store worker metadata and PID files
        """
        self.workers_dir = Path(workers_dir)
        self.workers_dir.mkdir(parents=True, exist_ok=True)

    def _metadata_path(self, worker_id: str) -> Path:
        """Get path to worker metadata file."""
        return self.workers_dir / f"{worker_id}.json"

    def _pid_path(self, worker_id: str) -> Path:
        """Get path to worker PID file."""
        return self.workers_dir / f"{worker_id}.pid"

    _last_timestamp: str | None = None
    _last_counter: int = 0

    def generate_worker_id(self) -> str:
        """Generate a unique worker ID."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

        # Track within-instance collisions
        if timestamp == WorkerRegistry._last_timestamp:
            WorkerRegistry._last_counter += 1
        else:
            WorkerRegistry._last_timestamp = timestamp
            WorkerRegistry._last_counter = 0

        # Find next available suffix if timestamp collision
        counter = WorkerRegistry._last_counter
        if counter == 0:
            worker_id = f"w-{timestamp}"
        else:
            worker_id = f"w-{timestamp}-{counter}"

        # Also check for file-based collisions (from previous runs)
        while self._metadata_path(worker_id).exists():
            counter += 1
            WorkerRegistry._last_counter = counter
            worker_id = f"w-{timestamp}-{counter}"

        return worker_id

    def register(self, worker: WorkerMetadata) -> None:
        """Register a new worker.

        Args:
            worker: Worker metadata to register
        """
        # Write metadata file
        metadata_path = self._metadata_path(worker.worker_id)
        with open(metadata_path, "w") as f:
            json.dump(worker.to_dict(), f, indent=2)

        # Write PID file
        pid_path = self._pid_path(worker.worker_id)
        pid_path.write_text(str(worker.pid))

    def update(self, worker: WorkerMetadata) -> None:
        """Update worker metadata.

        Args:
            worker: Updated worker metadata
        """
        metadata_path = self._metadata_path(worker.worker_id)
        with open(metadata_path, "w") as f:
            json.dump(worker.to_dict(), f, indent=2)

    def get(self, worker_id: str) -> WorkerMetadata | None:
        """Get worker metadata by ID.

        Args:
            worker_id: Worker ID to lookup

        Returns:
            Worker metadata if found, None otherwise
        """
        metadata_path = self._metadata_path(worker_id)
        if not metadata_path.exists():
            return None

        with open(metadata_path) as f:
            data = json.load(f)
            return WorkerMetadata.from_dict(data)

    def list_all(self, include_completed: bool = False) -> list[WorkerMetadata]:
        """List all workers.

        Args:
            include_completed: If True, include completed/failed workers

        Returns:
            List of worker metadata
        """
        workers = []
        for metadata_path in self.workers_dir.glob("w-*.json"):
            with open(metadata_path) as f:
                data = json.load(f)
                worker = WorkerMetadata.from_dict(data)

                # Check if still running
                if worker.status == "running":
                    if not self.is_running(worker.worker_id):
                        worker.status = "stale"

                # Filter by status
                if not include_completed and worker.status in ("completed", "failed"):
                    continue

                workers.append(worker)

        # Sort by started_at timestamp
        workers.sort(key=lambda w: w.started_at)
        return workers

    def is_running(self, worker_id: str) -> bool:
        """Check if a worker process is still running.

        Args:
            worker_id: Worker ID to check

        Returns:
            True if process is running, False otherwise
        """
        pid_path = self._pid_path(worker_id)
        if not pid_path.exists():
            return False

        try:
            pid = int(pid_path.read_text().strip())
            # Send signal 0 to check if process exists
            os.kill(pid, 0)
            return True
        except (OSError, ValueError):
            return False

    def mark_completed(
        self,
        worker_id: str,
        exit_code: int,
        status: str = "completed"
    ) -> None:
        """Mark a worker as completed.

        Args:
            worker_id: Worker ID to mark
            exit_code: Process exit code
            status: Final status (completed or failed)
        """
        worker = self.get(worker_id)
        if worker:
            worker.status = status
            worker.exit_code = exit_code
            worker.completed_at = datetime.now(timezone.utc).isoformat()
            self.update(worker)

        # Remove PID file
        pid_path = self._pid_path(worker_id)
        if pid_path.exists():
            pid_path.unlink()

    def stop(self, worker_id: str, force: bool = False) -> bool:
        """Stop a running worker.

        Args:
            worker_id: Worker ID to stop
            force: If True, use SIGKILL instead of SIGTERM

        Returns:
            True if signal sent successfully, False otherwise
        """
        worker = self.get(worker_id)
        if not worker:
            return False

        pid_path = self._pid_path(worker_id)
        if not pid_path.exists():
            return False

        try:
            pid = int(pid_path.read_text().strip())
            sig = signal.SIGKILL if force else signal.SIGTERM
            os.kill(pid, sig)
            return True
        except (OSError, ValueError):
            return False

    def cleanup_stale(self) -> int:
        """Remove metadata for stale workers (PID gone but files remain).

        Returns:
            Number of stale workers cleaned up
        """
        count = 0
        for worker in self.list_all(include_completed=True):
            if worker.status == "stale":
                # Remove metadata file
                metadata_path = self._metadata_path(worker.worker_id)
                if metadata_path.exists():
                    metadata_path.unlink()

                # Remove PID file if it exists
                pid_path = self._pid_path(worker.worker_id)
                if pid_path.exists():
                    pid_path.unlink()

                count += 1

        return count

    def remove(self, worker_id: str) -> None:
        """Remove worker metadata and PID files.

        Args:
            worker_id: Worker ID to remove
        """
        metadata_path = self._metadata_path(worker_id)
        if metadata_path.exists():
            metadata_path.unlink()

        pid_path = self._pid_path(worker_id)
        if pid_path.exists():
            pid_path.unlink()
