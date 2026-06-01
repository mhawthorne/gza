"""Monorepo project discovery helpers for cross-project execution and verification."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from .config import CONFIG_FILENAME, Config, ConfigError

if TYPE_CHECKING:
    from collections.abc import Iterable

logger = logging.getLogger(__name__)

_SKIP_DISCOVERY_DIRS = frozenset({".git", ".gza", ".claude", ".venv", "__pycache__", "node_modules"})


@dataclass(frozen=True)
class RepoProjectConfig:
    """Resolved repo-relative project configuration metadata."""

    project_dir: Path
    scope_root: Path
    verify_command: str
    inner_verify_command: str


@dataclass(frozen=True)
class AffectedRepoProjects:
    """Resolved project matches for a changed-path set."""

    projects: tuple[RepoProjectConfig, ...]
    unknown_paths: tuple[str, ...]


@dataclass(frozen=True)
class ParsedNameStatusProjectPaths:
    """Parsed repo-relative paths and declared project roots from name-status output."""

    changed_paths: tuple[str, ...]
    declared_project_roots: tuple[Path, ...]


def infer_declared_repo_project_roots(
    changed_paths: Iterable[str],
) -> tuple[Path, ...]:
    """Infer repo-relative project roots declared by changed ``gza.yaml`` paths."""
    declared_roots: set[Path] = set()
    for raw_path in changed_paths:
        normalized = _normalize_repo_relative_path(raw_path)
        path = Path(normalized)
        if path.name != CONFIG_FILENAME:
            continue
        declared_roots.add(path.parent if normalized else Path("."))
    return tuple(
        sorted(
            declared_roots,
            key=lambda root: (
                -len(root.parts if root != Path(".") else ()),
                root.as_posix(),
            ),
        )
    )


def parse_name_status_project_paths(name_status_output: str) -> ParsedNameStatusProjectPaths:
    """Parse changed repo paths and branch-declared project roots from name-status output."""
    if not isinstance(name_status_output, str):
        return ParsedNameStatusProjectPaths(changed_paths=(), declared_project_roots=())

    changed_paths: set[str] = set()
    declared_root_candidates: set[str] = set()
    for line in name_status_output.splitlines():
        parts = [part.strip() for part in line.split("\t") if part.strip()]
        if len(parts) < 2:
            continue
        status = parts[0]
        candidate_paths = parts[1:]
        changed_path_candidates = candidate_paths
        if status.startswith("R") and len(candidate_paths) >= 2:
            changed_path_candidates = candidate_paths[:2]
        elif status.startswith("C") and len(candidate_paths) >= 2:
            changed_path_candidates = [candidate_paths[-1]]
        changed_paths.update(changed_path_candidates)

        if status.startswith("D"):
            continue
        if status.startswith(("R", "C")) and len(candidate_paths) >= 2:
            declared_root_candidates.add(candidate_paths[-1])
            continue
        declared_root_candidates.update(candidate_paths)

    return ParsedNameStatusProjectPaths(
        changed_paths=tuple(sorted(changed_paths)),
        declared_project_roots=infer_declared_repo_project_roots(declared_root_candidates),
    )


def resolve_repo_root(start: Path) -> Path:
    """Resolve the git repo root that contains ``start``."""
    current = start.resolve()
    if current.is_file():
        current = current.parent
    while True:
        if (current / ".git").exists():
            return current
        parent = current.parent
        if parent == current:
            return start.resolve() if start.is_dir() else start.resolve().parent
        current = parent


def _normalize_repo_relative_path(path: str) -> str:
    normalized = path.replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized.lstrip("/")


def _path_within(root: Path, path: Path) -> bool:
    if root == Path("."):
        return True
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _discovery_repo_root(config: Config, repo_root: Path | None) -> Path:
    if repo_root is not None:
        return repo_root.resolve()
    boundary = getattr(config, "_project_boundary_cache", None)
    if boundary is not None and hasattr(boundary, "repo_root"):
        return Path(boundary.repo_root).resolve()
    return resolve_repo_root(config.project_dir)


def _discovery_current_project_dir(config: Config, repo_root: Path) -> Path:
    boundary = getattr(config, "_project_boundary_cache", None)
    scope_root = getattr(boundary, "scope_root", None)
    if isinstance(scope_root, Path):
        return (repo_root / scope_root).resolve()
    return config.project_dir.resolve()


def discover_repo_project_configs(
    config: Config,
    *,
    repo_root: Path | None = None,
) -> tuple[RepoProjectConfig, ...]:
    """Discover all project configs in the containing repo or inspected tree."""
    discovery_root = _discovery_repo_root(config, repo_root)
    cache_key = str(discovery_root)
    cached_by_root = getattr(config, "_repo_project_configs_cache_by_root", None)
    if isinstance(cached_by_root, dict):
        cached = cached_by_root.get(cache_key)
        if isinstance(cached, tuple):
            return cached

    current_project_dir = _discovery_current_project_dir(config, discovery_root)
    discovered: list[RepoProjectConfig] = []
    seen: set[Path] = set()
    for config_path in sorted(discovery_root.rglob(CONFIG_FILENAME)):
        rel_path = config_path.relative_to(discovery_root)
        if any(part in _SKIP_DISCOVERY_DIRS for part in rel_path.parts[:-1]):
            continue
        project_dir = config_path.parent.resolve()
        if project_dir in seen:
            continue
        if project_dir != current_project_dir:
            try:
                current_project_dir.relative_to(project_dir)
                continue
            except ValueError:
                pass
        seen.add(project_dir)
        try:
            project_config = Config.load(project_dir)
        except (ConfigError, OSError) as exc:
            logger.warning("Skipping repo project config %s during discovery: %s", config_path, exc)
            continue
        try:
            scope_root = project_dir.relative_to(discovery_root)
        except ValueError:
            continue
        discovered.append(
            RepoProjectConfig(
                project_dir=project_dir,
                scope_root=scope_root if str(scope_root) else Path("."),
                verify_command=project_config.verify_command.strip(),
                inner_verify_command=project_config.inner_verify_command.strip(),
            )
        )

    resolved = tuple(
        sorted(
            discovered,
            key=lambda project: (
                -len(project.scope_root.parts if project.scope_root != Path(".") else ()),
                project.scope_root.as_posix(),
            ),
        )
    )
    if not isinstance(cached_by_root, dict):
        cached_by_root = {}
        setattr(config, "_repo_project_configs_cache_by_root", cached_by_root)
    cached_by_root[cache_key] = resolved
    return resolved


def match_repo_project(
    path_str: str,
    projects: tuple[RepoProjectConfig, ...],
) -> RepoProjectConfig | None:
    """Return the most specific discovered project that contains ``path_str``."""
    path = Path(_normalize_repo_relative_path(path_str))
    for project in projects:
        if _path_within(project.scope_root, path):
            return project
    return None


def resolve_affected_repo_projects(
    config: Config,
    changed_paths: Iterable[str],
    *,
    repo_root: Path | None = None,
    declared_project_roots: Iterable[Path] = (),
) -> AffectedRepoProjects:
    """Resolve affected discovered projects for repo-relative changed paths."""
    projects = discover_repo_project_configs(config, repo_root=repo_root)
    declared_roots = tuple(
        sorted(
            {root if root != Path("") else Path(".") for root in declared_project_roots},
            key=lambda root: (
                -len(root.parts if root != Path(".") else ()),
                root.as_posix(),
            ),
        )
    )
    matched: dict[Path, RepoProjectConfig] = {}
    unknown_paths: list[str] = []
    for raw_path in sorted({_normalize_repo_relative_path(path) for path in changed_paths if path}):
        project = match_repo_project(raw_path, projects)
        if project is None:
            path = Path(raw_path)
            if not any(_path_within(root, path) for root in declared_roots):
                unknown_paths.append(raw_path)
            continue
        matched[project.scope_root] = project
    boundary = getattr(config, "_project_boundary_cache", None)
    current_scope_root = getattr(boundary, "scope_root", None)
    return AffectedRepoProjects(
        projects=tuple(
            sorted(
                matched.values(),
                key=lambda project: (
                    0 if current_scope_root is not None and project.scope_root == current_scope_root else 1,
                    project.scope_root.as_posix(),
                ),
            )
        ),
        unknown_paths=tuple(sorted(unknown_paths)),
    )
