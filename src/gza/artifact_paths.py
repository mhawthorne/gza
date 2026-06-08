"""Shared artifact path validation and resolution helpers."""

from __future__ import annotations

from pathlib import Path, PurePosixPath


class InvalidArtifactPathError(ValueError):
    """Raised when a stored artifact path escapes the managed artifact roots."""


def artifact_live_root(project_dir: Path) -> Path:
    """Return the live artifact root under the project state directory."""
    return project_dir.resolve() / ".gza" / "artifacts"


def artifact_archive_root(project_dir: Path) -> Path:
    """Return the archived artifact root under the project state directory."""
    return project_dir.resolve() / ".gza" / "archives" / "artifacts"


def normalize_artifact_path(stored_path: str) -> str:
    """Validate and normalize a stored artifact path to managed project-relative POSIX form."""
    candidate = PurePosixPath(stored_path)
    if not stored_path or stored_path.strip() == "":
        raise InvalidArtifactPathError("artifact path must not be empty")
    if candidate.is_absolute():
        raise InvalidArtifactPathError(f"artifact path must be project-relative: {stored_path}")
    if any(part in {"", ".", ".."} for part in candidate.parts):
        raise InvalidArtifactPathError(f"artifact path must not contain traversal segments: {stored_path}")

    parts = candidate.parts
    live_prefix = (".gza", "artifacts")
    archive_prefix = (".gza", "archives", "artifacts")
    if parts[: len(live_prefix)] == live_prefix:
        return candidate.as_posix()
    if parts[: len(archive_prefix)] == archive_prefix:
        return candidate.as_posix()
    raise InvalidArtifactPathError(
        f"artifact path must stay under .gza/artifacts or .gza/archives/artifacts: {stored_path}"
    )


def is_archived_artifact_path(stored_path: str) -> bool:
    """Return whether a stored artifact path points at the archive artifact root."""
    normalized = normalize_artifact_path(stored_path)
    return PurePosixPath(normalized).parts[:3] == (".gza", "archives", "artifacts")


def is_live_artifact_path(stored_path: str) -> bool:
    """Return whether a stored artifact path points at the live artifact root."""
    normalized = normalize_artifact_path(stored_path)
    return PurePosixPath(normalized).parts[:2] == (".gza", "artifacts")


def resolve_artifact_path(project_dir: Path, stored_path: str) -> Path:
    """Resolve one stored artifact path and refuse paths outside managed artifact roots."""
    project_root = project_dir.resolve()
    normalized = normalize_artifact_path(stored_path)
    candidate = (project_root / Path(normalized)).resolve(strict=False)
    allowed_roots = (artifact_live_root(project_root), artifact_archive_root(project_root))
    if any(candidate.is_relative_to(root) for root in allowed_roots):
        return candidate
    raise InvalidArtifactPathError(
        f"artifact path resolves outside managed roots: {stored_path} -> {candidate}"
    )
