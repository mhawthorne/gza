"""Worker registry for background task execution."""

import json
import os
import signal
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass
class WorkerMetadata:
    """Minimal metadata for a worker process index."""

    worker_id: str
    task_id: int | None
    pid: int
    # Legacy compatibility fields. New writes should avoid relying on these.
    task_slug: str | None = None
    started_at: str | None = None
    status: str = "running"
    log_file: str | None = None
    worktree: str | None = None
    startup_log_file: str | None = None
    is_background: bool = True
    exit_code: int | None = None
    completed_at: str | None = None

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return asdict(self)

    @staticmethod
    def from_dict(data: dict) -> "WorkerMetadata":
        """Create from dictionary, ignoring unknown legacy keys."""
        raw_pid = data.get("pid")
        if raw_pid is None:
            raise ValueError("Worker metadata is missing required 'pid'")
        try:
            pid = int(raw_pid)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Worker metadata has invalid pid: {raw_pid!r}") from exc
        if pid <= 0:
            raise ValueError(f"Worker metadata has non-positive pid: {pid}")

        raw_task_id = data.get("task_id")
        task_id: int | None
        if raw_task_id is None:
            task_id = None
        else:
            try:
                task_id = int(raw_task_id)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"Worker metadata has invalid task_id: {raw_task_id!r}") from exc

        return WorkerMetadata(
            worker_id=str(data.get("worker_id", "")),
            task_id=task_id,
            pid=pid,
            task_slug=data.get("task_slug"),
            started_at=data.get("started_at"),
            status=data.get("status", "running"),
            log_file=data.get("log_file"),
            worktree=data.get("worktree"),
            startup_log_file=data.get("startup_log_file"),
            is_background=bool(data.get("is_background", True)),
            exit_code=data.get("exit_code"),
            completed_at=data.get("completed_at"),
        )


class WorkerRegistry:
    """Manages worker metadata and pid files."""

    def __init__(self, workers_dir: Path):
        self.workers_dir = Path(workers_dir)
        self.workers_dir.mkdir(parents=True, exist_ok=True)

    def _metadata_path(self, worker_id: str) -> Path:
        return self.workers_dir / f"{worker_id}.json"

    def _pid_path(self, worker_id: str) -> Path:
        return self.workers_dir / f"{worker_id}.pid"

    _last_timestamp: str | None = None
    _last_counter: int = 0

    def generate_worker_id(self) -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        if timestamp == WorkerRegistry._last_timestamp:
            WorkerRegistry._last_counter += 1
        else:
            WorkerRegistry._last_timestamp = timestamp
            WorkerRegistry._last_counter = 0

        counter = WorkerRegistry._last_counter
        worker_id = f"w-{timestamp}" if counter == 0 else f"w-{timestamp}-{counter}"
        while self._metadata_path(worker_id).exists():
            counter += 1
            WorkerRegistry._last_counter = counter
            worker_id = f"w-{timestamp}-{counter}"
        return worker_id

    def register(self, worker: WorkerMetadata) -> None:
        if worker.pid <= 0:
            raise ValueError(f"Cannot register worker with non-positive pid: {worker.pid}")
        if worker.started_at is None:
            worker.started_at = datetime.now(timezone.utc).isoformat()
        metadata_path = self._metadata_path(worker.worker_id)
        metadata_path.write_text(json.dumps(worker.to_dict(), indent=2))
        self._pid_path(worker.worker_id).write_text(str(worker.pid))

    def update(self, worker: WorkerMetadata) -> None:
        self._metadata_path(worker.worker_id).write_text(json.dumps(worker.to_dict(), indent=2))

    def get(self, worker_id: str) -> WorkerMetadata | None:
        metadata_path = self._metadata_path(worker_id)
        if not metadata_path.exists():
            return None
        try:
            data = json.loads(metadata_path.read_text())
            return WorkerMetadata.from_dict(data)
        except (json.JSONDecodeError, OSError, TypeError, ValueError):
            return None

    def list_all(self, include_completed: bool = False) -> list[WorkerMetadata]:
        workers: list[WorkerMetadata] = []
        for metadata_path in self.workers_dir.glob("w-*.json"):
            try:
                data = json.loads(metadata_path.read_text())
                worker = WorkerMetadata.from_dict(data)
            except (json.JSONDecodeError, OSError, TypeError, ValueError):
                continue
            if not include_completed and worker.status in ("completed", "failed"):
                continue
            workers.append(worker)
        workers.sort(key=lambda w: (w.started_at or "", w.worker_id))
        return workers

    def is_running(self, worker_id: str) -> bool:
        worker = self.get(worker_id)
        if not worker or worker.pid <= 0:
            return False
        try:
            os.kill(worker.pid, 0)
            return True
        except OSError:
            return False

    def mark_completed(self, worker_id: str, exit_code: int, status: str = "completed") -> None:
        worker = self.get(worker_id)
        if worker:
            worker.status = status
            worker.exit_code = exit_code
            worker.completed_at = datetime.now(timezone.utc).isoformat()
            self.update(worker)
        pid_path = self._pid_path(worker_id)
        if pid_path.exists():
            pid_path.unlink()

    def stop(self, worker_id: str, force: bool = False) -> bool:
        worker = self.get(worker_id)
        if not worker or worker.pid <= 0:
            return False
        try:
            sig = signal.SIGKILL if force else signal.SIGTERM
            os.kill(worker.pid, sig)
            return True
        except (OSError, ValueError):
            return False

    def cleanup_stale(self) -> int:
        count = 0
        for worker in self.list_all(include_completed=True):
            if not self.is_running(worker.worker_id):
                self.remove(worker.worker_id)
                count += 1
        return count

    def cleanup_finished(self) -> int:
        count = 0
        for worker in self.list_all(include_completed=True):
            if worker.status in ("completed", "failed"):
                self.remove(worker.worker_id)
                count += 1
        return count

    def remove(self, worker_id: str) -> None:
        worker = self.get(worker_id)
        startup_log_path: Path | None = None
        if worker and worker.startup_log_file:
            candidate = self.workers_dir / Path(worker.startup_log_file).name
            try:
                resolved = candidate.resolve()
                allowed_root = self.workers_dir.resolve()
                if resolved.is_relative_to(allowed_root):
                    startup_log_path = resolved
            except (OSError, ValueError):
                pass

        metadata_path = self._metadata_path(worker_id)
        if metadata_path.exists():
            metadata_path.unlink()
        pid_path = self._pid_path(worker_id)
        if pid_path.exists():
            pid_path.unlink()
        if startup_log_path and startup_log_path.exists():
            startup_log_path.unlink()
