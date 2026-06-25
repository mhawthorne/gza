"""Helpers for durable task artifact storage under the host project .gza directory."""

from __future__ import annotations

import re
import secrets
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .artifact_paths import artifact_live_root
from .db import SqliteTaskStore, Task

if TYPE_CHECKING:
    from .config import Config


_SLUG_PART_RE = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True)
class StoredCommandOutputArtifact:
    """Compact return value for newly persisted command output artifacts."""

    id: int
    path: str
    bytes: int
    digest: str


@dataclass(frozen=True)
class PreparedCommandOutputArtifact:
    """Prepared filesystem payload for a command output artifact before DB insertion."""

    path: str
    absolute_path: Path
    bytes: int
    digest: str


def _sanitize_slug_part(value: str | None, *, fallback: str) -> str:
    if not value:
        return fallback
    cleaned = value.replace("/", "-").replace("\\", "-").strip().lower()
    cleaned = _SLUG_PART_RE.sub("-", cleaned).strip("-")
    return cleaned or fallback


def prepare_command_output_artifact(
    project_dir: Path,
    task_id: str,
    *,
    label: str,
    output: str | None,
    scope: str | None = None,
    created_at: datetime | None = None,
) -> PreparedCommandOutputArtifact:
    """Write artifact content to disk and return metadata for later DB persistence."""
    resolved_project_dir = project_dir.resolve()
    artifact_dir = artifact_live_root(resolved_project_dir) / task_id
    timestamp = (created_at or datetime.now(UTC)).astimezone(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    label_slug = _sanitize_slug_part(label, fallback="artifact")
    scope_slug = _sanitize_slug_part(scope, fallback="")
    nonce = secrets.token_hex(4)
    stem = (
        f"{label_slug}-{timestamp}-{nonce}"
        if not scope_slug
        else f"{label_slug}-{scope_slug}-{timestamp}-{nonce}"
    )

    encoded_output = (output or "").encode("utf-8", errors="replace")
    digest = sha256(encoded_output).hexdigest()
    byte_size = len(encoded_output)

    final_path = artifact_dir / f"{stem}.txt"
    if encoded_output:
        artifact_dir.mkdir(parents=True, exist_ok=True)
        temp_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=artifact_dir,
                prefix=f".{stem}-",
                suffix=".tmp",
                delete=False,
            ) as handle:
                temp_path = Path(handle.name)
                handle.write(encoded_output)
            temp_path.replace(final_path)
        finally:
            if temp_path is not None and temp_path.exists():
                temp_path.unlink()

    relative_path = final_path.relative_to(resolved_project_dir).as_posix()
    return PreparedCommandOutputArtifact(
        path=relative_path,
        absolute_path=final_path,
        bytes=byte_size,
        digest=digest,
    )


def store_command_output_artifact(
    store: SqliteTaskStore,
    task: Task,
    config: Config,
    *,
    kind: str,
    producer: str,
    label: str,
    output: str | None,
    command: str | None = None,
    status: str | None = None,
    exit_status: str | None = None,
    head_sha: str | None = None,
    scope: str | None = None,
    metadata: dict[str, Any] | None = None,
    created_at: datetime | None = None,
    content_type: str = "text/plain; charset=utf-8",
) -> StoredCommandOutputArtifact:
    """Persist command output under the host project .gza tree and record DB metadata."""
    if task.id is None:
        raise ValueError("task.id is required to persist an artifact")

    project_dir = Path(config.project_dir).resolve()
    prepared = prepare_command_output_artifact(
        project_dir,
        task.id,
        label=label,
        output=output,
        scope=scope,
        created_at=created_at,
    )
    stored = store.add_artifact(
        task.id,
        kind=kind,
        label=label,
        path=prepared.path,
        content_type=content_type,
        byte_size=prepared.bytes,
        sha256=prepared.digest,
        created_at=created_at,
        producer=producer,
        command=command,
        status=status,
        exit_status=exit_status,
        head_sha=head_sha,
        metadata=metadata,
    )
    return StoredCommandOutputArtifact(
        id=stored.id,
        path=stored.path,
        bytes=stored.byte_size,
        digest=stored.sha256,
    )
