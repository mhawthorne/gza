"""Main Gza runner orchestration."""

import inspect
import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
import tomllib
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import gza.colors as _colors

from .artifacts import store_command_output_artifact
from .branch_naming import generate_branch_name
from .branch_publication import (
    BranchPublicationState,
    load_branch_publication_state,
    persist_branch_publication_state,
)
from .branch_resolution import resolve_rebase_target_branch
from .commit_messages import build_task_commit_message
from .config import (
    APP_NAME,
    DEFAULT_REVIEW_CONTEXT_FILE_LIMIT,
    DEFAULT_REVIEW_DIFF_MEDIUM_THRESHOLD,
    DEFAULT_REVIEW_DIFF_SMALL_THRESHOLD,
    DEFAULT_REVIEW_VERIFY_TIMEOUT_SECONDS,
    BranchStrategy,
    Config,
    is_model_compatible_with_provider,
    provider_model_mismatch_error,
)
from .console import (
    console,
    error_message,
    task_footer,
    task_header,
)
from .db import (
    SqliteTaskStore,
    Task,
    TaskStats,
    extract_failure_reason as _extract_failure_reason,
    task_id_numeric_key,
)
from .dependency_preconditions import get_unmerged_dependency_precondition
from .extractions import (
    MANIFEST_FILENAME,
    PATCH_FILENAME,
    ExtractionError,
    copy_bundle_to_worktree,
    extraction_bundle_path,
    load_manifest,
    load_patch_text,
    parse_patch_touched_paths,
    resolve_manifest_patch_path,
)
from .failure_reasons import (
    mark_task_failed_from_cause as _mark_task_failed,
    resolve_failure_reason as _resolve_failure_reason,
)
from .git import (
    Git,
    GitApplyResult,
    GitError,
    cleanup_worktree_for_branch,
    is_rebase_in_progress,
    parse_diff_numstat,
)
from .github import GitHub, GitHubError, is_github_repo_unsupported_error
from .improve_diff import (
    ImproveDiffBaseline,
    capture_improve_diff_baseline,
    compute_improve_changed_diff,
)
from .learnings import maybe_auto_regenerate_learnings
from .lifecycle_completion import (
    auto_review_skip_message_for_completed_code_task,
    should_auto_create_review_for_completed_code_task,
)
from .lineage import get_plan_for_task
from .log_paths import TaskLogPaths, ops_log_path_for, resolve_ops_log_path, resolve_task_log_paths
from .pr_ops import build_task_pr_content, ensure_task_pr, sync_task_branch_if_live_pr
from .project_discovery import (
    RepoProjectConfig,
    parse_name_status_project_paths,
    resolve_affected_repo_projects,
    resolve_repo_root,
)
from .prompt_sanitization import sanitize_provider_prompt
from .prompts import PromptBuilder
from .providers import Provider, RunResult, get_provider
from .providers.base import PreflightCheckResult
from .providers.log_renderers import UnknownLogProviderError, get_log_renderer
from .rebase_diff import (
    RebaseDiffBaseline,
    capture_rebase_diff_baseline,
    compute_rebase_changed_diff,
)
from .rebase_publish import publish_rebased_branch
from .review_scope import resolve_review_scope_for_impl
from .review_tasks import DuplicateReviewError, create_review_task, extract_followup_prompt_parts
from .review_verdict import (
    compute_review_score,
    is_verify_blocked_only_review,
    is_verify_timeout_only_review,
    parse_review_report,
    parse_review_template,
    parse_review_verdict,
    validate_review_report_contract,
)
from .sync_ops import resolve_branch_pr
from .task_slug import (
    extract_task_id_suffix,
    get_base_task_slug,
    strip_derived_implement_prefixes,
)
from .worktree_roots import managed_worktree_root_paths

logger = logging.getLogger(__name__)

# Keep the legacy patch target available for extraction tests that stub the
# fallback parser on ``gza.runner``.
extract_failure_reason = _extract_failure_reason

EXTRACTION_PRECHECK_FAILURE_REASON = "EXTRACTION_PRECHECK_FAILED"
EXTRACTION_ALREADY_MERGED_COMPLETION_REASON = "EXTRACTION_ALREADY_MERGED"

PR_REQUIRED_FAILURE_REASON = "PR_REQUIRED"
BRANCH_UNPUSHABLE_FAILURE_REASON = "BRANCH_UNPUSHABLE"
PROJECT_SCOPE_VIOLATION_FAILURE_REASON = "PROJECT_SCOPE_VIOLATION"
CROSS_PROJECT_TAG = "cross-project"
DEPENDENCY_BLOCKED_NOT_RUN_EXIT_CODE = 3
_GZA_OWNED_DIR_NAMES = (".gza", ".claude")


@dataclass(frozen=True)
class RunInvocationContext:
    """Execution invocation metadata for runner UX/provenance behavior."""

    command: str
    execution_mode: str
    interaction_mode: str = "observe_only"


@dataclass(frozen=True)
class ExtractionSeedResult:
    """Outcome of extraction bundle preflight/application."""

    seeded_paths: frozenset[str] = frozenset()
    completion_reason: str | None = None


@dataclass(frozen=True)
class CompletedCodeTaskPrPublicationOutcome:
    """Runner-owned classification of post-completion PR publication results."""

    kind: str
    status: str
    message: str
    error: str | None = None


@dataclass(frozen=True)
class ResolvedRunFailure:
    """Resolved provider-run failure with user-facing status text."""

    reason: str
    status: str
    outcome_message: str


@dataclass(frozen=True)
class ResolvedTimeoutBudget:
    """Resolved runtime budget for a task execution attempt."""

    minutes: int
    reason: str
    diff_lines: int | None = None
    diff_files: int | None = None


@dataclass(frozen=True)
class LocalDependency:
    """Resolved local dependency path from ``uv.lock``."""

    source_path: Path
    resolved_path: Path
    repo_relative_path: Path | None

    @property
    def is_in_repo(self) -> bool:
        return self.repo_relative_path is not None


@dataclass(frozen=True)
class ProjectBoundary:
    """Repo/project boundary and resolved local-dependency set."""

    repo_root: Path
    scope_root: Path
    local_dependencies: tuple[LocalDependency, ...]

    @property
    def in_repo_dependency_paths(self) -> frozenset[Path]:
        return frozenset(
            dep.repo_relative_path
            for dep in self.local_dependencies
            if dep.repo_relative_path is not None
        )

    @property
    def out_of_repo_dependency_paths(self) -> frozenset[Path]:
        return frozenset(dep.resolved_path for dep in self.local_dependencies if not dep.is_in_repo)

    @property
    def project_rooted_paths(self) -> frozenset[Path]:
        return frozenset({self.scope_root, *self.in_repo_dependency_paths})

    @property
    def strict_project_rooted_paths(self) -> frozenset[Path]:
        return frozenset({self.scope_root})
def _git_error_failure() -> ResolvedRunFailure:
    return ResolvedRunFailure(
        reason="GIT_ERROR",
        status="Failed: git error",
        outcome_message="Outcome: failed (GIT_ERROR)",
    )


def _task_is_cross_project(task: Task) -> bool:
    """Return whether a task carries the reserved cross-project scope tag."""
    return CROSS_PROJECT_TAG in task.tags


def _normalize_repo_relative_path(path: str) -> str:
    """Normalize a repo-relative path string for prefix comparisons."""
    normalized = path.replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized.lstrip("/")


def _gza_owned_path_prefixes(boundary: ProjectBoundary | None = None) -> tuple[str, ...]:
    """Return repo-relative prefixes that represent gza-owned directories."""
    prefixes = list(_GZA_OWNED_DIR_NAMES)
    if boundary is not None and boundary.scope_root != Path("."):
        scope_prefix = _normalize_repo_relative_path(boundary.scope_root.as_posix())
        prefixes.extend(f"{scope_prefix}/{dirname}" for dirname in _GZA_OWNED_DIR_NAMES)
    return tuple(prefixes)


def _is_gza_owned_path(path: str, *, boundary: ProjectBoundary | None = None) -> bool:
    """Return whether a path points at gza-owned worktree/project state."""
    normalized = _normalize_repo_relative_path(path)
    return any(normalized == prefix or normalized.startswith(f"{prefix}/") for prefix in _gza_owned_path_prefixes(boundary))


def _filter_owned_artifact_paths(
    paths: set[str] | frozenset[str] | tuple[str, ...] | list[str],
    *,
    boundary: ProjectBoundary | None = None,
) -> set[str]:
    """Drop gza-owned artifact paths from a path collection."""
    return {path for path in paths if not _is_gza_owned_path(path, boundary=boundary)}


def _strip_owned_artifact_patch_sections(
    patch_text: str,
    *,
    boundary: ProjectBoundary | None = None,
) -> tuple[str, tuple[str, ...]]:
    """Drop diff sections that touch gza-owned artifact paths."""
    if not patch_text.strip() or "diff --git " not in patch_text:
        return patch_text, ()

    preamble: list[str] = []
    current_section: list[str] = []
    sections: list[list[str]] = []

    for line in patch_text.splitlines(keepends=True):
        if line.startswith("diff --git "):
            if current_section:
                sections.append(current_section)
            current_section = [line]
            continue
        if current_section:
            current_section.append(line)
        else:
            preamble.append(line)

    if current_section:
        sections.append(current_section)

    kept_sections: list[str] = []
    stripped_paths: set[str] = set()
    for section in sections:
        section_text = "".join(section)
        touched_paths = parse_patch_touched_paths(section_text)
        owned_paths = {path for path in touched_paths if _is_gza_owned_path(path, boundary=boundary)}
        if owned_paths:
            stripped_paths.update(owned_paths)
            continue
        kept_sections.append(section_text)

    if not kept_sections:
        return "", tuple(sorted(stripped_paths))

    return "".join([*preamble, *kept_sections]), tuple(sorted(stripped_paths))


def _resolve_repo_root(project_dir: Path) -> Path:
    """Resolve the git repo root that contains ``project_dir``."""
    return resolve_repo_root(project_dir)


def _resolve_local_dependencies_from_uv_lock(project_dir: Path, repo_root: Path) -> tuple[LocalDependency, ...]:
    """Resolve local path dependencies from ``uv.lock``."""
    uv_lock_path = project_dir / "uv.lock"
    if not uv_lock_path.exists():
        return ()

    try:
        with open(uv_lock_path, "rb") as f:
            data = tomllib.load(f)
    except Exception:
        logger.warning("Failed to read/parse %s; skipping local dependency resolution", uv_lock_path)
        return ()

    packages = data.get("package")
    if not isinstance(packages, list):
        return ()

    resolved: dict[Path, LocalDependency] = {}
    for package in packages:
        if not isinstance(package, dict):
            continue
        source = package.get("source")
        if not isinstance(source, dict):
            continue

        raw_path: str | None = None
        for key in ("editable", "directory", "path"):
            value = source.get(key)
            if isinstance(value, str) and value.strip():
                raw_path = value.strip()
                break
        if raw_path is None or raw_path == ".":
            continue

        source_path = Path(raw_path)
        dep_path = source_path if source_path.is_absolute() else (project_dir / source_path)
        resolved_path = dep_path.resolve()
        if resolved_path == project_dir.resolve():
            continue

        repo_relative_path: Path | None
        try:
            repo_relative_path = resolved_path.relative_to(repo_root)
        except ValueError:
            repo_relative_path = None

        resolved.setdefault(
            resolved_path,
            LocalDependency(
                source_path=source_path,
                resolved_path=resolved_path,
                repo_relative_path=repo_relative_path,
            ),
        )

    return tuple(sorted(resolved.values(), key=lambda dep: str(dep.resolved_path)))


def _project_boundary(config: Config) -> ProjectBoundary:
    """Return cached repo/project boundary metadata for ``config.project_dir``."""
    cached = getattr(config, "_project_boundary_cache", None)
    if isinstance(cached, ProjectBoundary):
        return cached

    project_dir = config.project_dir.resolve()
    repo_root = _resolve_repo_root(project_dir)
    try:
        scope_root = project_dir.relative_to(repo_root)
    except ValueError:
        scope_root = Path(".")

    boundary = ProjectBoundary(
        repo_root=repo_root,
        scope_root=scope_root,
        local_dependencies=_resolve_local_dependencies_from_uv_lock(project_dir, repo_root),
    )
    setattr(config, "_project_boundary_cache", boundary)
    return boundary


def _container_project_root(boundary: ProjectBoundary) -> Path:
    """Return the mounted container path for the configured project root."""
    container_root = Path("/workspace")
    if boundary.scope_root == Path("."):
        return container_root
    return container_root / boundary.scope_root


def _container_execution_dir(boundary: ProjectBoundary) -> Path:
    """Return the container cwd for provider execution."""
    if boundary.scope_root == Path("."):
        return Path("/workspace")
    return _container_project_root(boundary)


def _worktree_project_root(worktree_path: Path, boundary: ProjectBoundary) -> Path:
    """Return the host worktree path that corresponds to ``config.project_dir``."""
    if boundary.scope_root == Path("."):
        return worktree_path
    return worktree_path / boundary.scope_root


def _worktree_execution_dir(worktree_path: Path, boundary: ProjectBoundary) -> Path:
    """Return the host cwd for provider execution."""
    if boundary.scope_root == Path("."):
        return worktree_path
    return _worktree_project_root(worktree_path, boundary)


def _format_repo_project_scope(scope_root: Path) -> str:
    """Format a repo-relative project root for display."""
    return "." if scope_root == Path(".") else scope_root.as_posix()


def _worktree_path_for_project_path(config: Config, worktree_path: Path, project_path: Path) -> Path:
    """Map a project-local path into the matching worktree path."""
    rel_path = project_path.relative_to(config.project_dir)
    return _worktree_project_root(worktree_path, _project_boundary(config)) / rel_path


def _container_path_for_project_path(config: Config, project_path: Path) -> Path:
    """Map a project-local path into the matching container path."""
    rel_path = project_path.relative_to(config.project_dir)
    return _container_project_root(_project_boundary(config)) / rel_path


def _build_runtime_docker_volumes(config: Config) -> list[str]:
    """Return configured Docker volumes plus read-only mounts for out-of-repo deps."""
    volumes = list(getattr(config, "docker_volumes", []))
    for dep_path in sorted(_project_boundary(config).out_of_repo_dependency_paths):
        mount = f"{dep_path}:{dep_path}:ro"
        if mount not in volumes:
            volumes.append(mount)
    return volumes


def _path_is_under_any_scope(path: Path, allowed_roots: frozenset[Path]) -> bool:
    """Return whether ``path`` is within one of the allowed repo-relative roots."""
    if Path(".") in allowed_roots:
        return True
    for root in allowed_roots:
        try:
            path.relative_to(root)
            return True
        except ValueError:
            continue
    return False


def _find_out_of_scope_paths(
    config: Config,
    files_to_stage: set[str],
    *,
    task: Task | None = None,
    strict_scope: bool = False,
    repo_root: Path | None = None,
    declared_project_roots: tuple[Path, ...] = (),
) -> list[str]:
    """Return sorted repo-relative paths that violate the current project scope."""
    if task is not None and _task_is_cross_project(task):
        return list(
            resolve_affected_repo_projects(
                config,
                files_to_stage,
                repo_root=repo_root,
                declared_project_roots=declared_project_roots,
            ).unknown_paths
        )

    boundary = _project_boundary(config)
    allowed_roots = (
        boundary.strict_project_rooted_paths
        if strict_scope
        else boundary.project_rooted_paths
    )
    violations: list[str] = []
    for path_str in sorted(files_to_stage):
        path = Path(path_str)
        if not _path_is_under_any_scope(path, allowed_roots):
            violations.append(path_str)
    return violations


def _reviewable_diff_scope_paths(task: Task, config: Config) -> tuple[str, ...]:
    """Return repo-relative path roots that define the reviewable diff scope."""
    if _task_is_cross_project(task):
        return ()

    allowed_roots = _project_boundary(config).project_rooted_paths
    if Path(".") in allowed_roots:
        return ()

    return tuple(
        sorted(_normalize_repo_relative_path(path.as_posix()) for path in allowed_roots)
    )


def _interrupt_signal_name() -> str | None:
    """Return the current interrupt signal name, if one was recorded."""
    return os.environ.get("GZA_INTERRUPT_SIGNAL")


def _provider_accepts_ops_log_file(provider: Provider) -> bool:
    """Return whether provider.run accepts an explicit ops log path."""
    params = inspect.signature(provider.run).parameters.values()
    return any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD or parameter.name == "ops_log_file"
        for parameter in params
    )


def _call_provider_run(
    provider: Provider,
    config: Config,
    prompt: str,
    log_file: Path,
    work_dir: Path,
    *,
    provider_run_kwargs: dict[str, Any],
) -> RunResult:
    """Run a provider while tolerating legacy test doubles without ops_log_file."""
    try:
        return provider.run(
            config,
            prompt,
            log_file,
            work_dir,
            **provider_run_kwargs,
        )
    except TypeError as exc:
        if "unexpected keyword argument 'ops_log_file'" not in str(exc):
            raise
        fallback_kwargs = dict(provider_run_kwargs)
        fallback_kwargs.pop("ops_log_file", None)
        return provider.run(
            config,
            prompt,
            log_file,
            work_dir,
            **fallback_kwargs,
        )


def _interruption_metadata() -> dict[str, str]:
    """Return structured metadata describing the current interrupt context."""
    metadata: dict[str, str] = {}
    signal_name = os.environ.get("GZA_INTERRUPT_SIGNAL")
    if signal_name:
        metadata["signal"] = signal_name
    source = os.environ.get("GZA_INTERRUPT_SOURCE")
    if source:
        metadata["source"] = source
    detail = os.environ.get("GZA_INTERRUPT_DETAIL")
    if detail:
        metadata["detail"] = detail
    return metadata


def _resolve_run_failure(
    *,
    provider_name: str,
    timeout_minutes: int,
    step_limit: int | None,
    turn_limit: int | None,
    error_type: str | None,
    exit_code: int,
    log_file: Path,
    stats: TaskStats,
) -> ResolvedRunFailure | None:
    """Resolve a provider-run failure and the matching operator-facing messages."""
    if exit_code == 0 and error_type is None:
        return None

    reason = _resolve_failure_reason(
        error_type=error_type,
        exit_code=exit_code,
        log_file=log_file,
        stats=stats,
        step_limit=step_limit,
        turn_limit=turn_limit,
        fallback_to_log=True,
    )

    if reason == "TIMEOUT":
        return ResolvedRunFailure(
            reason=reason,
            status=f"Failed: {provider_name} timed out after {timeout_minutes} minutes",
            outcome_message=f"Outcome: failed (timeout after {timeout_minutes}m)",
        )
    if reason == "MAX_STEPS":
        return ResolvedRunFailure(
            reason=reason,
            status=f"Failed: max steps of {step_limit} exceeded",
            outcome_message="Outcome: failed (max_steps)",
        )
    if reason == "MAX_TURNS":
        return ResolvedRunFailure(
            reason=reason,
            status=f"Failed: max turns of {turn_limit} exceeded",
            outcome_message="Outcome: failed (max_turns)",
        )
    if error_type is not None and exit_code == 0:
        return ResolvedRunFailure(
            reason=reason,
            status=f"Failed: {provider_name} reported {error_type}",
            outcome_message=f"Outcome: failed (error_type={error_type})",
        )
    return ResolvedRunFailure(
        reason=reason,
        status=f"Failed: {provider_name} exited with code {exit_code}",
        outcome_message=f"Outcome: failed (exit_code={exit_code})",
    )
def _write_stats_entry(log_file: Path, stats: TaskStats) -> None:
    """Write the standard stats log entry for a completed provider run."""
    write_log_entry(
        log_file,
        {
            "type": "gza",
            "subtype": "stats",
            "message": (
                f"Stats: {stats.num_steps_computed or stats.num_steps_reported or 0} steps, "
                f"{stats.duration_seconds or 0.0:.1f}s, ${stats.cost_usd or 0.0:.4f}"
            ),
            "duration_seconds": stats.duration_seconds,
            "cost_usd": stats.cost_usd,
            "num_steps": stats.num_steps_computed or stats.num_steps_reported or 0,
        },
    )


def _observed_step_count(stats: TaskStats) -> int:
    """Return the runner's canonical observed step count for a provider run."""
    return stats.num_steps_computed or stats.num_steps_reported or 0


def _extract_provider_stderr_tail(log_file: Path) -> str:
    """Return the last useful provider stderr/process-output tail from the ops log."""
    ops_log = ops_log_path_for(log_file)
    process_lines: list[str] = []
    stderr_tail = ""
    try:
        with open(ops_log) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(entry, dict):
                    continue
                candidate = entry.get("stderr_tail")
                if isinstance(candidate, str) and candidate.strip():
                    stderr_tail = candidate
                if entry.get("source") != "provider" or entry.get("subtype") != "process_output":
                    continue
                message = entry.get("provider_output") or entry.get("message")
                if isinstance(message, str) and message.strip():
                    process_lines.append(message)
    except OSError:
        return ""
    if process_lines:
        return "\n".join(process_lines)[-2000:]
    return stderr_tail[-2000:]


def _log_has_empty_turn_signature(log_file: Path) -> bool:
    """Return whether the conversation log matches a provider empty-turn hiccup."""
    saw_started = False
    saw_turn_completed = False
    saw_activity_item = False
    try:
        with open(log_file) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(entry, dict):
                    continue
                event_type = entry.get("type")
                if event_type in {"thread.started", "turn.started"}:
                    saw_started = True
                    continue
                if event_type == "turn.completed":
                    saw_turn_completed = True
                    continue
                if event_type in {"assistant", "result", "item.started", "item.completed", "tool_call"}:
                    saw_activity_item = True
    except OSError:
        return False
    return saw_started and not saw_turn_completed and not saw_activity_item


def _finalize_completed_code_task(
    *,
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    log_file: Path,
    branch_name: str,
    output_content: str | None,
    stats: TaskStats,
    diff_files: int,
    diff_added: int,
    diff_removed: int,
    head_sha: str | None,
    base_sha: str | None,
) -> None:
    """Write terminal success logs and persist completed state for a code task."""
    # Write final log entries before marking completed in DB, so that
    # `gza log -f` (which checks task status) doesn't break out of the
    # follow loop before the log file is fully written.
    write_log_entry(log_file, {"type": "gza", "subtype": "outcome", "message": "Outcome: completed", "exit_code": 0})
    _write_stats_entry(log_file, stats)

    # Mark completed — after log entries are flushed so readers see the
    # full log before the status transitions away from in_progress.
    store.mark_completed(
        task,
        branch=branch_name,
        log_file=str(log_file.relative_to(config.project_dir)),
        output_content=output_content,
        has_commits=True,
        stats=stats,
        diff_files_changed=diff_files,
        diff_lines_added=diff_added,
        diff_lines_removed=diff_removed,
        head_sha=head_sha,
        base_sha=base_sha,
    )


def _record_pr_publication_note(
    *,
    task: Task,
    log_file: Path | None,
    branch_name: str,
    status: str,
    error: str | None,
) -> str:
    """Surface a non-fatal completed-task PR publication note."""
    message = (
        f"Warning: Task {task.id} completed and branch '{branch_name}' is published to origin, "
        f"but PR was not created ({status})"
    )
    if error:
        message = f"{message}: {error}"
    print(message)
    if log_file is not None:
        write_log_entry(
            log_file,
            {
                "type": "gza",
                "subtype": "pr_publication_note",
                "message": message,
                "status": status,
                "error": error,
                "branch": branch_name,
                "task_id": task.id,
            },
        )
    return message


def _ensure_completed_task_branch_is_published(
    *,
    task: Task,
    git: Git,
) -> str | None:
    """Ensure the completed task branch is published to origin before non-fatal PR notes."""
    assert task.branch is not None
    try:
        if git.needs_push(task.branch):
            print(f"Pushing branch '{task.branch}' to origin...")
            git.push_branch(task.branch)
    except GitError as exc:
        return str(exc)
    return None


def _persist_branch_unpushable_failure(
    *,
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    log_file: Path | str | None,
    stats: TaskStats,
    branch_name: str,
    output_content: str | None,
    diff_files: int,
    diff_added: int,
    diff_removed: int,
    head_sha: str | None,
    base_sha: str | None,
    message: str,
    fix_commits_ahead_before_run: int | None = None,
    fix_default_branch: str | None = None,
    fix_was_merged_before_run: bool = False,
    record_reconcile_attempt: bool = False,
) -> int:
    """Persist a completion-time branch publication failure."""
    print(message)
    task.output_content = output_content
    task.diff_files_changed = diff_files
    task.diff_lines_added = diff_added
    task.diff_lines_removed = diff_removed
    prior_state = load_branch_publication_state(store, task.id)
    persist_branch_publication_state(
        store=store,
        task=task,
        config=config,
        state=BranchPublicationState(
            reconcile_attempts_consumed=(
                prior_state.reconcile_attempts_consumed + 1
                if record_reconcile_attempt
                else prior_state.reconcile_attempts_consumed
            ),
            fix_commits_ahead_before_run=(
                fix_commits_ahead_before_run
                if fix_commits_ahead_before_run is not None
                else prior_state.fix_commits_ahead_before_run
            ),
            fix_default_branch=fix_default_branch or prior_state.fix_default_branch,
            fix_was_merged_before_run=(
                fix_was_merged_before_run or prior_state.fix_was_merged_before_run
            ),
        ),
        status=BRANCH_UNPUSHABLE_FAILURE_REASON,
        exit_status="reconcile_retry_failed" if record_reconcile_attempt else "initial_failure",
        head_sha=head_sha,
    )
    _mark_task_failed(
        task=task,
        config=config,
        store=store,
        log_file=log_file,
        has_commits=True,
        stats=stats,
        branch=branch_name,
        explicit_reason=BRANCH_UNPUSHABLE_FAILURE_REASON,
        error_type=None,
        exit_code=1,
        head_sha=head_sha,
        base_sha=base_sha,
    )
    if isinstance(log_file, Path):
        write_log_entry(
            log_file,
            {
                "type": "gza",
                "subtype": "outcome",
                "message": f"Outcome: failed ({BRANCH_UNPUSHABLE_FAILURE_REASON})",
                "exit_code": 1,
                "failure_reason": BRANCH_UNPUSHABLE_FAILURE_REASON,
            },
        )
        _write_stats_entry(log_file, stats)
    return 1


def _finalize_rebase_completion(
    *,
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    worktree_git: Git,
    branch_name: str,
    stats: TaskStats,
    log_file: Path,
    output_content: str | None,
    diff_files: int,
    diff_added: int,
    diff_removed: int,
    head_sha: str | None,
    base_sha: str | None,
    task_logger: "TaskExecutionLogger",
    target_branch: str,
    create_pr: bool = False,
    fix_commits_ahead_before_run: int | None = None,
    fix_default_branch: str | None = None,
    fix_was_merged_before_run: bool = False,
    improve_diff_baseline: ImproveDiffBaseline | None = None,
    rebase_diff_baseline: RebaseDiffBaseline | None = None,
) -> int:
    """Publish a completed rebase before persisting completed task state."""
    post_complete_rc = _post_complete_code_task(
        task,
        config,
        store,
        worktree_git,
        branch_name,
        stats,
        task_logger=task_logger,
        target_branch=target_branch,
        fix_commits_ahead_before_run=fix_commits_ahead_before_run,
        fix_default_branch=fix_default_branch,
        fix_was_merged_before_run=fix_was_merged_before_run,
        improve_diff_baseline=improve_diff_baseline,
        rebase_diff_baseline=rebase_diff_baseline,
    )
    if post_complete_rc != 0:
        return post_complete_rc
    if create_pr:
        pr_outcome = _ensure_work_pr_for_completed_code_task(task, config, store, worktree_git)
        if pr_outcome.kind == "nonfatal_missing_pr":
            _record_pr_publication_note(
                task=task,
                log_file=log_file,
                branch_name=branch_name,
                status=pr_outcome.status,
                error=pr_outcome.error,
            )
        elif pr_outcome.kind == "branch_unpushable":
            return _persist_branch_unpushable_failure(
                task=task,
                config=config,
                store=store,
                log_file=log_file,
                stats=stats,
                branch_name=branch_name,
                output_content=output_content,
                diff_files=diff_files,
                diff_added=diff_added,
                diff_removed=diff_removed,
                head_sha=head_sha,
                base_sha=base_sha,
                message=pr_outcome.message,
            )
    _finalize_completed_code_task(
        task=task,
        config=config,
        store=store,
        log_file=log_file,
        branch_name=branch_name,
        output_content=output_content,
        stats=stats,
        diff_files=diff_files,
        diff_added=diff_added,
        diff_removed=diff_removed,
        head_sha=head_sha,
        base_sha=base_sha,
    )
    return 0


def _finalize_already_published_rebase_pr_retry(
    *,
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    branch_name: str,
    stats: TaskStats,
    log_file: Path,
    output_content: str | None,
    diff_files: int,
    diff_added: int,
    diff_removed: int,
    head_sha: str | None,
    base_sha: str | None,
    task_logger: "TaskExecutionLogger",
) -> int:
    """Complete a rebase PR retry after the rebase-side effects already ran.

    A rebase task only reaches ``PR_REQUIRED`` after ``_post_complete_code_task``
    has already published the rebased branch and recorded the rebase-only
    review/merge-state side effects. Retrying PR creation must therefore verify
    the current branch tip is still published without replaying those rebase
    completion effects against a post-rebase baseline.
    """

    publish_rebased_branch(
        git,
        branch=branch_name,
        baseline=None,
        logger=task_logger,
    )
    pr_outcome = _ensure_work_pr_for_completed_code_task(task, config, store, git)
    if pr_outcome.kind == "nonfatal_missing_pr":
        _record_pr_publication_note(
            task=task,
            log_file=log_file,
            branch_name=branch_name,
            status=pr_outcome.status,
            error=pr_outcome.error,
        )
    elif pr_outcome.kind == "branch_unpushable":
        return _persist_branch_unpushable_failure(
            task=task,
            config=config,
            store=store,
            log_file=log_file,
            stats=stats,
            branch_name=branch_name,
            output_content=output_content,
            diff_files=diff_files,
            diff_added=diff_added,
            diff_removed=diff_removed,
            head_sha=head_sha,
            base_sha=base_sha,
            message=pr_outcome.message,
        )
    _finalize_completed_code_task(
        task=task,
        config=config,
        store=store,
        log_file=log_file,
        branch_name=branch_name,
        output_content=output_content,
        stats=stats,
        diff_files=diff_files,
        diff_added=diff_added,
        diff_removed=diff_removed,
        head_sha=head_sha,
        base_sha=base_sha,
    )
    return 0


def _complete_failed_code_task_after_pr_publication(
    *,
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    branch_name: str,
    stats: TaskStats,
    log_file: Path | None,
    output_content: str | None,
    diff_files: int,
    diff_added: int,
    diff_removed: int,
    head_sha: str | None,
    base_sha: str | None,
    task_logger: Any = None,
    target_branch: str | None = None,
    fix_commits_ahead_before_run: int | None = None,
    fix_default_branch: str | None = None,
    fix_was_merged_before_run: bool = False,
    record_reconcile_attempt: bool = False,
) -> int:
    """Retry PR publication for a previously failed completed code task."""
    pr_outcome = _ensure_work_pr_for_completed_code_task(task, config, store, git)
    if pr_outcome.kind == "nonfatal_missing_pr":
        _record_pr_publication_note(
            task=task,
            log_file=log_file,
            branch_name=branch_name,
            status=pr_outcome.status,
            error=pr_outcome.error,
        )
    elif pr_outcome.kind == "branch_unpushable":
        return _persist_branch_unpushable_failure(
            task=task,
            config=config,
            store=store,
            log_file=log_file if log_file is not None else task.log_file,
            stats=stats,
            branch_name=branch_name,
            output_content=output_content,
            diff_files=diff_files,
            diff_added=diff_added,
            diff_removed=diff_removed,
            head_sha=head_sha,
            base_sha=base_sha,
            message=pr_outcome.message,
            fix_commits_ahead_before_run=fix_commits_ahead_before_run,
            fix_default_branch=fix_default_branch,
            fix_was_merged_before_run=fix_was_merged_before_run,
            record_reconcile_attempt=record_reconcile_attempt,
        )

    if log_file is not None:
        _finalize_completed_code_task(
            task=task,
            config=config,
            store=store,
            log_file=log_file,
            branch_name=branch_name,
            output_content=output_content,
            stats=stats,
            diff_files=diff_files,
            diff_added=diff_added,
            diff_removed=diff_removed,
            head_sha=head_sha,
            base_sha=base_sha,
        )
    else:
        store.mark_completed(
            task,
            branch=branch_name,
            log_file=task.log_file,
            output_content=output_content,
            has_commits=True,
            stats=stats,
            diff_files_changed=diff_files,
            diff_lines_added=diff_added,
            diff_lines_removed=diff_removed,
            head_sha=head_sha,
            base_sha=base_sha,
        )
    if task.task_type == "rebase":
        return 0
    return _post_complete_code_task(
        task,
        config,
        store,
        git,
        branch_name,
        stats,
        task_logger=task_logger,
        target_branch=target_branch,
        fix_commits_ahead_before_run=fix_commits_ahead_before_run,
        fix_default_branch=fix_default_branch,
        fix_was_merged_before_run=fix_was_merged_before_run,
    )


def _record_run_failure(
    *,
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    log_file: Path,
    stats: TaskStats,
    failure: ResolvedRunFailure,
    exit_code: int,
    branch: str | None = None,
    worktree: Path | None = None,
    has_commits: bool | None = None,
) -> None:
    """Emit failure logs/footer and persist the resolved failure reason."""
    task_footer(
        task,
        stats,
        status=failure.status,
        branch=branch,
        worktree=worktree,
        store=store,
    )
    write_log_entry(
        log_file,
        {
            "type": "gza",
            "subtype": "outcome",
            "message": failure.outcome_message,
            "exit_code": exit_code,
            "failure_reason": failure.reason,
        },
    )
    _write_stats_entry(log_file, stats)
    _mark_task_failed(
        task=task,
        config=config,
        store=store,
        log_file=log_file,
        stats=stats,
        branch=branch,
        has_commits=has_commits,
        explicit_reason=failure.reason,
        error_type=None,
        exit_code=exit_code,
    )


_TASK_EXECUTION_MODE_BY_INVOCATION_MODE: dict[str, str] = {
    "background_worker": "worker_background",
    "foreground_worker": "worker_foreground",
    "foreground_inline": "foreground_inline",
    "foreground_attach_resume": "foreground_attach_resume",
}

__all__ = [
    "RunInvocationContext",
    "run",
    "build_prompt",
    "write_log_entry",
    "write_ops_entry",
    "TaskExecutionLogger",
    "ensure_task_log_path",
    "ensure_task_log_paths",
    "task_log_storage_path",
    "extract_content_from_log",
    "get_effective_config_for_task",
    "post_review_to_pr",
    "open_task_startup_log",
    "open_task_startup_logs",
    "rename_startup_log_to_slug",
]


def write_log_entry(log_file: "Path", entry: dict) -> None:
    """Append a JSONL entry to the task log file."""
    target = log_file
    payload = dict(entry)
    if payload.get("type") == "gza" and log_file.suffix == ".log":
        target = ops_log_path_for(log_file)
        target.parent.mkdir(parents=True, exist_ok=True)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        if not log_file.exists():
            log_file.touch()
        payload.setdefault("stream", "ops")
        payload.setdefault("source", "gza")
        payload.setdefault("timestamp", _ops_timestamp())
    try:
        with open(target, "a") as f:
            f.write(json.dumps(payload) + "\n")
            f.flush()
    except Exception:
        logger.warning("Failed to write log entry to %s", target, exc_info=True)


def _ops_timestamp() -> str:
    """Return ISO-8601 UTC timestamp for structured ops entries."""
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def write_ops_entry(ops_log_file: "Path", entry: dict) -> None:
    """Append a structured JSONL entry to the task ops log file."""
    payload = dict(entry)
    payload.setdefault("type", "gza")
    payload.setdefault("stream", "ops")
    payload.setdefault("source", "gza")
    payload.setdefault("timestamp", _ops_timestamp())
    write_log_entry(ops_log_file, payload)


def task_log_storage_path(config: Config, path: Path) -> str:
    """Convert a task log path to the DB storage string (project-relative when possible)."""
    try:
        return str(path.relative_to(config.project_dir))
    except ValueError:
        return str(path)


def ensure_task_log_path(config: Config, store: SqliteTaskStore, task: Task) -> Path:
    """Ensure the task owns a canonical conversation log path and persist it."""
    paths = ensure_task_log_paths(config, store, task)
    return paths.conversation


def ensure_task_log_paths(config: Config, store: SqliteTaskStore, task: Task) -> TaskLogPaths:
    """Ensure the task owns canonical conversation and ops log paths."""
    paths = resolve_task_log_paths(config, task)
    paths.conversation.parent.mkdir(parents=True, exist_ok=True)
    if not paths.conversation.exists():
        paths.conversation.touch()
    storage_path = task_log_storage_path(config, paths.conversation)
    if task.log_file != storage_path:
        task.log_file = storage_path
        store.update(task)
    return paths


def prepare_task_startup_phase(config: Config, store: SqliteTaskStore, task: Task) -> Task:
    """Synchronously materialize task startup metadata before execution detaches."""
    if task.slug is None:
        git = Git(config.project_dir)
        slug_override = _compute_slug_override(task, store)
        task.slug = generate_slug(
            task.prompt,
            existing_id=None,
            log_path=config.log_path,
            git=git,
            store=store,
            exclude_task_id=task.id,
            project_name=config.project_name,
            project_prefix=config.project_prefix,
            slug_override=slug_override,
            branch_strategy=config.branch_strategy,
            explicit_type=task.task_type_hint,
        )

    ensure_task_log_paths(config, store, task)
    # Phase 1 ends here: the task row is durably committed with its slug and log
    # path before provider preflight, worktree setup, or detached execution starts.
    if task.id is None:
        return task
    return store.get(task.id) or task


def remove_task_startup_artifacts(config: Config, task: Task) -> None:
    """Best-effort cleanup for startup artifacts created before execution begins."""
    paths = resolve_task_log_paths(config, task)
    for path in {
        paths.conversation,
        paths.ops,
        paths.startup_conversation,
        paths.startup_ops,
    }:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            logger.warning("Failed to remove startup artifact %s", path, exc_info=True)


class TaskExecutionLogger:
    """Emit provider-agnostic task execution events to the canonical ops log."""

    def __init__(self, ops_log_file: Path, *, echo: bool = True) -> None:
        self.ops_log_file = ops_log_file
        self.echo = echo

    def _emit(self, subtype: str, message: str, *, stderr: bool = False, extra: dict | None = None) -> None:
        payload: dict[str, object] = {
            "type": "gza",
            "subtype": subtype,
            "message": message,
        }
        if extra:
            payload.update(extra)
        write_ops_entry(self.ops_log_file, payload)
        if self.echo:
            print(message, file=sys.stderr if stderr else sys.stdout)

    def info(self, message: str, *, extra: dict | None = None) -> None:
        self._emit("info", message, extra=extra)

    def warning(self, message: str, *, extra: dict | None = None) -> None:
        self._emit("warning", message, stderr=True, extra=extra)

    def error(self, message: str, *, extra: dict | None = None) -> None:
        self._emit("error", message, stderr=True, extra=extra)

    def phase(self, message: str, *, extra: dict | None = None) -> None:
        self._emit("phase", message, extra=extra)

    def command(self, message: str, *, extra: dict | None = None) -> None:
        self._emit("command", message, extra=extra)


def open_task_startup_log(config: Config, task: Task) -> Path:
    """Return the startup log path for a task, creating parent directories."""
    return open_task_startup_logs(config, task).startup_conversation


def open_task_startup_logs(config: Config, task: Task) -> TaskLogPaths:
    """Return startup log paths for a task, creating parent directories."""
    paths = resolve_task_log_paths(config, task)
    selected_conversation = paths.startup_conversation
    selected_ops = paths.startup_ops
    if task.log_file or task.slug:
        selected_conversation = paths.conversation
        selected_ops = paths.ops
    selected_conversation.parent.mkdir(parents=True, exist_ok=True)
    if not selected_conversation.exists():
        selected_conversation.touch()
    return TaskLogPaths(
        conversation=paths.conversation,
        ops=paths.ops,
        startup_conversation=selected_conversation,
        startup_ops=selected_ops,
        layout=paths.layout,
    )


def rename_startup_log_to_slug(config: Config, startup_log: Path, slug: str) -> Path:
    """Rename startup conversation and ops logs to final slug log paths."""
    final_log = config.log_path / f"{slug}.log"
    final_ops = resolve_ops_log_path(config, final_log)
    startup_ops = resolve_ops_log_path(config, startup_log)
    if startup_log != final_log:
        final_log.parent.mkdir(parents=True, exist_ok=True)
        if startup_log.exists():
            startup_log.replace(final_log)
    if startup_ops != final_ops:
        final_ops.parent.mkdir(parents=True, exist_ok=True)
        if startup_ops.exists():
            startup_ops.replace(final_ops)
    return final_log


def write_worker_start_event(ops_log_file: "Path", *, resumed: bool) -> None:
    """Write a worker start lifecycle event when running under worker mode."""
    if os.environ.get("GZA_WORKER_MODE") != "1":
        return
    worker_id = os.environ.get("GZA_WORKER_ID")
    if not worker_id:
        return
    mode = "pipe mode, resumed" if resumed else "pipe mode"
    write_ops_entry(
        ops_log_file,
        {
            "subtype": "worker_lifecycle",
            "event": "start",
            "worker_id": worker_id,
            "message": f"Worker {worker_id} started ({mode})",
        },
    )


def _resolve_default_invocation_context() -> "RunInvocationContext":
    """Build default invocation context from process mode."""
    if os.environ.get("GZA_WORKER_MODE") == "1":
        return RunInvocationContext(command="work", execution_mode="background_worker")
    return RunInvocationContext(command="work", execution_mode="foreground_worker")


def _task_execution_mode_from_invocation(invocation: "RunInvocationContext") -> str:
    """Map runner invocation mode to persisted task execution mode."""
    return _TASK_EXECUTION_MODE_BY_INVOCATION_MODE.get(invocation.execution_mode, "worker_foreground")


def _resolve_interaction_mode(
    invocation: "RunInvocationContext",
    provider: "Provider",
) -> str:
    """Resolve actual interaction mode using provider capabilities."""
    requested = invocation.interaction_mode
    if requested == "auto":
        resolved = "interactive" if provider.supports_interactive_foreground else "observe_only"
    elif requested == "interactive" and not provider.supports_interactive_foreground:
        resolved = "observe_only"
    else:
        resolved = requested

    return resolved


def write_execution_provenance_event(
    ops_log_file: Path,
    *,
    invocation: "RunInvocationContext",
    provider: "Provider | str",
    interaction_mode: str,
    resumed: bool,
) -> None:
    """Write structured runner execution provenance before provider launch."""
    provider_name = provider if isinstance(provider, str) else provider.name.lower()
    canonical_execution_mode = _task_execution_mode_from_invocation(invocation)
    worker_mode = canonical_execution_mode in {"worker_background", "worker_foreground"}
    message = (
        f"Execution: command={invocation.command}, mode={canonical_execution_mode}, "
        f"interaction={interaction_mode}, provider={provider_name}, resumed={resumed}"
    )
    write_ops_entry(
        ops_log_file,
        {
            "subtype": "execution",
            "message": message,
            "command": invocation.command,
            "execution_mode": canonical_execution_mode,
            "interaction_mode": interaction_mode,
            "provider": provider_name,
            "worker_mode": worker_mode,
            "resumed": resumed,
        },
    )


def _mark_preflight_provider_unavailable(
    *,
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    provider: Provider,
    invocation: "RunInvocationContext",
    interaction_mode: str,
    resume: bool,
    message: str,
) -> None:
    """Persist preflight credential failures as provider-unavailable task failures."""
    log_file = ensure_task_log_path(config, store, task)

    write_worker_start_event(log_file, resumed=resume)
    write_log_entry(
        log_file,
        {"type": "gza", "subtype": "info", "message": f"Task: {task.id} {task.slug or ''}".strip()},
    )
    write_log_entry(
        log_file,
        {
            "type": "gza",
            "subtype": "info",
            "message": f"Provider: {provider.name}, Model: {config.model or 'default'}",
        },
    )
    write_execution_provenance_event(
        log_file,
        invocation=invocation,
        provider=provider,
        interaction_mode=interaction_mode,
        resumed=resume,
    )
    write_log_entry(
        log_file,
        {
            "type": "gza",
            "subtype": "outcome",
            "message": message,
            "failure_reason": "PROVIDER_UNAVAILABLE",
        },
    )
    _mark_task_failed(
        task=task,
        config=config,
        store=store,
        log_file=log_file,
        explicit_reason="PROVIDER_UNAVAILABLE",
        error_type=None,
        exit_code=None,
    )


def _normalize_preflight_result(result: bool | PreflightCheckResult) -> PreflightCheckResult:
    """Normalize legacy bool provider preflight results to structured outcomes."""
    if isinstance(result, PreflightCheckResult):
        return result
    if result:
        return PreflightCheckResult.success()
    return PreflightCheckResult.failure(
        failure_reason="PROVIDER_UNAVAILABLE",
        message="Preflight failed: provider credential verification failed",
    )


def _mark_preflight_failure(
    *,
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    provider: Provider,
    invocation: "RunInvocationContext",
    interaction_mode: str,
    resume: bool,
    message: str,
    failure_reason: str,
) -> None:
    """Persist structured preflight failures as task failures with provenance."""
    log_file = (
        config.project_dir / Path(task.log_file)
        if task.log_file
        else open_task_startup_log(config, task)
    )
    log_file_relative = str(log_file.relative_to(config.project_dir))
    if task.log_file != log_file_relative:
        task.log_file = log_file_relative
        store.update(task)

    write_worker_start_event(log_file, resumed=resume)
    write_log_entry(
        log_file,
        {"type": "gza", "subtype": "info", "message": f"Task: {task.id} {task.slug or ''}".strip()},
    )
    write_log_entry(
        log_file,
        {
            "type": "gza",
            "subtype": "info",
            "message": f"Provider: {provider.name}, Model: {config.model or 'default'}",
        },
    )
    write_execution_provenance_event(
        log_file,
        invocation=invocation,
        provider=provider,
        interaction_mode=interaction_mode,
        resumed=resume,
    )
    write_log_entry(
        log_file,
        {
            "type": "gza",
            "subtype": "outcome",
            "message": message,
            "failure_reason": failure_reason,
        },
    )
    _mark_task_failed(
        task=task,
        config=config,
        store=store,
        log_file=log_file,
        explicit_reason=failure_reason,
        error_type=None,
        exit_code=None,
    )


def _mark_preflight_model_mismatch(
    *,
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    provider_name: str,
    model: str,
    invocation: "RunInvocationContext",
    resume: bool,
    message: str,
) -> None:
    """Persist provider/model parity pre-flight failures before provider instantiation."""
    log_file = ensure_task_log_path(config, store, task)
    write_worker_start_event(log_file, resumed=resume)
    write_log_entry(
        log_file,
        {"type": "gza", "subtype": "info", "message": f"Task: {task.id} {task.slug or ''}".strip()},
    )
    write_log_entry(
        log_file,
        {"type": "gza", "subtype": "info", "message": f"Provider: {provider_name}, Model: {model}"},
    )
    write_execution_provenance_event(
        log_file,
        invocation=invocation,
        provider=provider_name,
        interaction_mode=invocation.interaction_mode,
        resumed=resume,
    )
    write_log_entry(
        log_file,
        {"type": "gza", "subtype": "outcome", "message": message, "failure_reason": "CONFIG_ERROR"},
    )
    _mark_task_failed(
        task=task,
        config=config,
        store=store,
        log_file=log_file,
        explicit_reason="CONFIG_ERROR",
        error_type=None,
        exit_code=None,
    )


def extract_content_from_log(log_file: "Path") -> str | None:
    """Scan a JSONL log file for a provider 'result' entry and return its text.

    Providers emit a ``{"type": "result", "result": "<text>"}`` line when the
    agent finishes.  If the agent output the review (or plan/explore) as text
    rather than writing the expected file artifact, the content lives here.

    Returns the last non-empty result entry, since a resumed session may emit
    an intermediate result before the final one.
    """
    last_result: str | None = None
    try:
        with open(log_file) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    if entry.get("type") == "result":
                        result_text = entry.get("result", "")
                        if isinstance(result_text, str) and result_text.strip():
                            last_result = result_text
                except json.JSONDecodeError:
                    continue
    except OSError:
        logger.warning("Failed to read log file %s for content recovery", log_file)
    return last_result


def _persist_run_steps_from_result(
    store: SqliteTaskStore,
    run_id: str,
    provider_name: str,
    result: RunResult,
) -> bool:
    """Persist provider-emitted step/substep events into run_steps tables."""
    accumulated = getattr(result, "_accumulated_data", None)
    if not isinstance(accumulated, dict):
        return False
    events: list[Any] = accumulated.get("run_step_events")  # type: ignore[assignment]
    if not isinstance(events, list):
        return False
    store.set_log_schema_version(run_id, 2)

    has_non_completed = any(
        isinstance(event, dict) and str(event.get("outcome") or "completed") != "completed"
        for event in events
    )
    fallback_outcome: str | None = None
    if not has_non_completed:
        if result.error_type in ("max_steps", "max_turns"):
            fallback_outcome = "interrupted"
        elif result.error_type is not None or result.exit_code != 0:
            fallback_outcome = "failed"
        if fallback_outcome is not None:
            for event in reversed(events):
                if isinstance(event, dict):
                    cast(dict[str, Any], event)["outcome"] = fallback_outcome
                    break

    for event in events:
        if not isinstance(event, dict):
            continue
        step_ref = store.emit_step(
            run_id,
            event.get("message_text"),
            provider=provider_name,
            message_role=str(event.get("message_role") or "assistant"),
            legacy_turn_id=event.get("legacy_turn_id"),
            legacy_event_id=event.get("legacy_event_id"),
        )
        for substep in event.get("substeps", []):
            if not isinstance(substep, dict):
                continue
            store.emit_substep(
                step_ref,
                str(substep.get("type") or "event"),
                substep.get("payload"),
                source=str(substep.get("source") or "provider"),
                call_id=substep.get("call_id"),
                legacy_turn_id=substep.get("legacy_turn_id"),
                legacy_event_id=substep.get("legacy_event_id"),
            )
        store.finalize_step(
            step_ref,
            str(event.get("outcome") or "completed"),
            event.get("summary"),
        )
    return True


def get_effective_config_for_task(task: Task, config: Config) -> tuple[str | None, str, int]:
    """Get the effective model, provider, and max_steps for a task.

    Priority order for provider selection:
    1. Explicit task-specific provider override (task.provider when provider_is_explicit)
    2. Task-type route (config.task_providers.<task_type>)
    3. Config default (config.provider, already env-merged in Config.load)

    Priority order for model selection:
    1. Explicit task-specific model override (task.model when model_is_explicit)
    2. Provider-aware config resolution (Config.get_model_for_task)

    Priority order for max_steps selection:
    1. Provider-aware config resolution (Config.get_max_steps_for_task)

    Args:
        task: The task to get config for
        config: The base configuration

    Returns:
        Tuple of (model, provider, max_steps) where model can be None
    """
    provider_is_explicit = bool(getattr(task, "provider_is_explicit", False))
    model_is_explicit = bool(getattr(task, "model_is_explicit", False))
    provider_override = task.provider if provider_is_explicit and task.provider else None
    provider = provider_override if provider_override else config.get_provider_for_task(task.task_type)
    model_override = task.model if model_is_explicit and task.model else None
    model = model_override if model_override else config.get_model_for_task(task.task_type, provider)
    max_steps = config.get_max_steps_for_task(task.task_type, provider)
    return model, provider, max_steps


_CODE_TASK_TIMEOUT_SCALING_TYPES = frozenset({"implement", "improve", "fix", "rebase"})


def _resolve_task_timeout_budget(
    *,
    task: Task,
    config: Config,
    provider: str,
    git: Git | None = None,
    branch_name: str | None = None,
    default_branch: str | None = None,
    task_logger: "TaskExecutionLogger | None" = None,
) -> ResolvedTimeoutBudget:
    """Resolve the effective runtime budget for one task attempt."""
    def _int_config(value: object, default: int) -> int:
        return value if isinstance(value, int) else default

    def _cap_budget(
        minutes: int,
        reason: str,
        *,
        diff_lines: int | None = None,
        diff_files: int | None = None,
    ) -> ResolvedTimeoutBudget:
        capped_minutes = min(minutes, cap_minutes)
        if capped_minutes < minutes:
            reason = f"{reason}; hard-capped at {cap_minutes}m from {minutes}m"
        return ResolvedTimeoutBudget(
            minutes=capped_minutes,
            reason=reason,
            diff_lines=diff_lines,
            diff_files=diff_files,
        )

    base_minutes = _int_config(
        config.get_timeout_minutes_for_task(task.task_type, provider),
        _int_config(getattr(config, "timeout_minutes", None), 10),
    )
    if task.task_type not in _CODE_TASK_TIMEOUT_SCALING_TYPES:
        return ResolvedTimeoutBudget(
            minutes=base_minutes,
            reason=f"base timeout for task type '{task.task_type}'",
        )

    cap_minutes = _int_config(
        getattr(config, "code_task_diff_timeout_cap_minutes", None),
        45,
    )
    if git is None or branch_name is None or default_branch is None:
        return _cap_budget(
            base_minutes,
            f"base timeout for task type '{task.task_type}'; diff inspection unavailable",
        )

    revision_range = f"{default_branch}...{branch_name}"
    diff_scope_paths = _reviewable_diff_scope_paths(task, config)
    try:
        numstat_output = git.get_diff_numstat_checked(revision_range, diff_scope_paths)
        if not isinstance(numstat_output, str):
            numstat_output = ""
        diff_files, diff_added, diff_removed = parse_diff_numstat(numstat_output)
    except GitError as exc:
        warning = (
            f"Warning: failed to inspect reviewable diff for timeout scaling on {task.task_type} "
            f"task {task.id}: {exc}. Using base timeout."
        )
        if task_logger is not None:
            task_logger.warning(warning)
        else:
            logger.warning(warning)
        return _cap_budget(
            base_minutes,
            f"base timeout for task type '{task.task_type}' (diff inspection unavailable)",
        )

    diff_lines = diff_added + diff_removed
    diff_minutes = base_minutes
    diff_reason = "below scaling thresholds"

    large_threshold = _int_config(
        getattr(config, "code_task_diff_timeout_large_threshold", None),
        1200,
    )
    medium_threshold = _int_config(
        getattr(config, "code_task_diff_timeout_medium_threshold", None),
        400,
    )
    large_minutes = _int_config(
        getattr(config, "code_task_diff_timeout_large_minutes", None),
        45,
    )
    medium_minutes = _int_config(
        getattr(config, "code_task_diff_timeout_medium_minutes", None),
        30,
    )
    if diff_lines >= large_threshold:
        diff_minutes = large_minutes
        diff_reason = (
            f"large reviewable diff ({diff_lines} changed lines across {diff_files} files)"
        )
    elif diff_lines >= medium_threshold:
        diff_minutes = medium_minutes
        diff_reason = (
            f"medium reviewable diff ({diff_lines} changed lines across {diff_files} files)"
        )

    if diff_minutes > base_minutes:
        return _cap_budget(
            diff_minutes,
            f"{diff_reason}; scaled from base {base_minutes}m",
            diff_lines=diff_lines,
            diff_files=diff_files,
        )

    return _cap_budget(
        base_minutes,
        f"base timeout for task type '{task.task_type}'; {diff_reason}",
        diff_lines=diff_lines,
        diff_files=diff_files,
    )


DEFAULT_REPORT_DIR = f".{APP_NAME}/explorations"
PLAN_DIR = f".{APP_NAME}/plans"
PLAN_REVIEW_DIR = f".{APP_NAME}/plan-reviews"
PLAN_IMPROVE_DIR = f".{APP_NAME}/revised-plans"
REVIEW_DIR = f".{APP_NAME}/reviews"
INTERNAL_DIR = f".{APP_NAME}/internal"
SUMMARY_DIR = f".{APP_NAME}/summaries"
WIP_DIR = f".{APP_NAME}/wip"
WIP_INTERRUPTED_COMMIT_SUBJECT = "WIP: gza task interrupted"
BACKUP_DIR = f".{APP_NAME}/backups"


def get_task_output_paths(
    task: Task, project_dir: Path
) -> tuple[Path | None, Path | None]:
    """Determine report_path and summary_path for a task based on its type.

    This is the single source of truth for where task outputs go.
    Used by the runner and by ``gza show --prompt``.

    Returns:
        (report_path, summary_path) — one or both may be None.
    """
    report_path: Path | None = None
    summary_path: Path | None = None

    if not task.slug:
        return None, None

    if task.task_type in ("task", "implement", "improve", "fix", "rebase"):
        summary_path = project_dir / SUMMARY_DIR / f"{task.slug}.md"
    elif task.task_type == "explore":
        report_path = project_dir / DEFAULT_REPORT_DIR / f"{task.slug}.md"
    elif task.task_type == "plan":
        report_path = project_dir / PLAN_DIR / f"{task.slug}.md"
    elif task.task_type == "plan_review":
        report_path = project_dir / PLAN_REVIEW_DIR / f"{task.slug}.md"
    elif task.task_type == "plan_improve":
        report_path = project_dir / PLAN_IMPROVE_DIR / f"{task.slug}.md"
    elif task.task_type == "review":
        report_path = project_dir / REVIEW_DIR / f"{task.slug}.md"
    elif task.task_type in ("internal", "learn"):
        report_path = project_dir / INTERNAL_DIR / f"{task.slug}.md"
    else:
        report_path = project_dir / DEFAULT_REPORT_DIR / f"{task.slug}.md"

    return report_path, summary_path


# Diff size thresholds for tiered diff strategy in review prompts
DIFF_SMALL_THRESHOLD = DEFAULT_REVIEW_DIFF_SMALL_THRESHOLD
DIFF_MEDIUM_THRESHOLD = DEFAULT_REVIEW_DIFF_MEDIUM_THRESHOLD
REVIEW_CONTEXT_FILE_LIMIT = DEFAULT_REVIEW_CONTEXT_FILE_LIMIT
REVIEW_IMPROVE_LINEAGE_LIMIT = 5
REVIEW_IMPROVE_SUMMARY_MAX_CHARS = 320
REVIEW_VERIFY_OUTPUT_MAX_CHARS = 4000
REVIEW_VERIFY_TIMEOUT_SECONDS = DEFAULT_REVIEW_VERIFY_TIMEOUT_SECONDS
COMMIT_SUBJECT_MAX_CHARS = 72


def _extract_review_verdict(content: str | None) -> str | None:
    """Backward-compatible wrapper around the shared verdict parser."""
    return parse_review_verdict(content)


def _backup_sqlite_file(source_path: Path, destination_path: Path) -> None:
    """Copy a SQLite database file using SQLite's backup API."""
    source = sqlite3.connect(str(source_path))
    try:
        destination = sqlite3.connect(str(destination_path))
        try:
            source.backup(destination)
        finally:
            destination.close()
    finally:
        source.close()


def backup_database(db_path: Path, project_dir: Path) -> None:
    """Create an hourly backup of the SQLite database if one doesn't exist yet.

    Checks if a backup for the current hour already exists. If not, creates
    a timestamped backup using SQLite's backup API (safe for concurrent access).

    Backup filename format: gza-YYYYMMDDHH.db (e.g., gza-2026021414.db)

    Args:
        db_path: Path to the source SQLite database
        project_dir: Project directory (used for project-local DB backup location)
    """
    if not db_path.exists():
        return

    local_db = project_dir / f".{APP_NAME}/{APP_NAME}.db"
    if db_path.resolve() == local_db.resolve():
        backup_dir = project_dir / BACKUP_DIR
    else:
        backup_dir = db_path.parent / "backups"
    hour_stamp = datetime.now().strftime("%Y%m%d%H")
    backup_path = backup_dir / f"gza-{hour_stamp}.db"

    if backup_path.exists():
        return

    backup_dir.mkdir(parents=True, exist_ok=True)

    _backup_sqlite_file(db_path, backup_path)


def load_dotenv(project_dir: Path) -> None:
    """Load .env files from project .gza dir, project root, and home directory.

    Load order (lowest priority first — higher-priority sources are loaded last and
    use override=True to win over shell environment variables and earlier sources):
    1. ~/.{APP_NAME}/.env (home defaults, lowest priority; uses setdefault)
    2. <project_dir>/.env (overrides shell vars and home defaults)
    3. <project_dir>/.gza/.env (highest priority; overrides project .env and shell vars)

    Shell environment variables are preserved unless overridden by sources loaded
    with override=True (i.e., project .env and .gza/.env).
    """
    def _load(path: Path, override: bool) -> None:
        if not path.exists():
            return
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, value = line.split("=", 1)
                    k = key.strip()
                    v = value.strip()
                    if override:
                        os.environ[k] = v
                    else:
                        os.environ.setdefault(k, v)

    # Lowest priority: home ~/.{APP_NAME}/.env (does not override shell or project values)
    _load(Path.home() / f".{APP_NAME}" / ".env", override=False)

    # Mid priority: project root .env (overrides shell and home; backwards compat)
    _load(project_dir / ".env", override=True)

    # Highest priority: project .gza/.env (shared across worktrees via symlink)
    _load(project_dir / f".{APP_NAME}" / ".env", override=True)


def slugify(text: str, max_length: int = 50) -> str:
    """Convert text to a URL/filename-safe slug."""
    # Lowercase and replace spaces/special chars with hyphens
    slug = re.sub(r'[^a-z0-9]+', '-', text.lower())
    # Remove leading/trailing hyphens
    slug = slug.strip('-')
    # Truncate to max length, avoiding cutting mid-word
    if len(slug) > max_length:
        slug = slug[:max_length].rsplit('-', 1)[0]
    return slug


def generate_slug(
    prompt: str,
    existing_id: str | None = None,
    log_path: Path | None = None,
    git: Git | None = None,
    store: SqliteTaskStore | None = None,
    exclude_task_id: str | None = None,
    project_name: str | None = None,
    project_prefix: str | None = None,
    slug_override: str | None = None,
    branch_strategy: "BranchStrategy | None" = None,
    explicit_type: str | None = None,
) -> str:
    """Generate a task slug in YYYYMMDD-{project_prefix}-slug format, with suffix for retries."""
    if existing_id:
        # This is a retry - strip any existing suffix to get base
        base_id = re.sub(r'-\d+$', '', existing_id)
    else:
        # Fresh task - generate base ID
        date_prefix = datetime.now().strftime("%Y%m%d")
        if slug_override is not None:
            # slug_override already encodes full lineage context
            # (e.g. "0000mr-rev-myproj-add-feature")
            # so do not prepend project_prefix — it would double-embed the prefix for chained tasks
            base_id = f"{date_prefix}-{slug_override}"
        else:
            slug = slugify(prompt)
            if project_prefix:
                base_id = f"{date_prefix}-{project_prefix}-{slug}"
            else:
                base_id = f"{date_prefix}-{slug}"

    # Check if base ID is available
    if not _slug_exists(
        base_id,
        log_path=log_path,
        git=git,
        project_name=project_name,
        prompt=prompt,
        branch_strategy=branch_strategy,
        explicit_type=explicit_type,
        project_prefix=project_prefix,
        store=store,
        exclude_task_id=exclude_task_id,
    ):
        return base_id

    # Find next available suffix
    suffix = 2
    new_id = f"{base_id}-{suffix}"
    while _slug_exists(
        new_id,
        log_path=log_path,
        git=git,
        project_name=project_name,
        prompt=prompt,
        branch_strategy=branch_strategy,
        explicit_type=explicit_type,
        project_prefix=project_prefix,
        store=store,
        exclude_task_id=exclude_task_id,
    ):
        suffix += 1
        new_id = f"{base_id}-{suffix}"
    return new_id


def _compute_slug_override(task: "Task", store: "SqliteTaskStore") -> str | None:
    """Compute a semantic slug override for review/implement/improve tasks."""
    if task.task_type not in {"review", "implement", "improve"}:
        return None

    def _known_lineage_suffixes(candidate: Task) -> set[str]:
        suffixes: set[str] = set()
        current: Task | None = candidate
        visited: set[str] = set()
        while current is not None and current.id is not None and current.id not in visited:
            visited.add(current.id)
            suffix = extract_task_id_suffix(current.id)
            if suffix:
                suffixes.add(suffix)
            if current.based_on is None:
                break
            current = store.get(current.based_on)
        return suffixes

    def _slug_from_task(candidate: Task) -> str:
        base_slug = get_base_task_slug(candidate.slug) if candidate.slug else None
        if base_slug:
            normalized = strip_derived_implement_prefixes(
                base_slug,
                known_task_id_suffixes=_known_lineage_suffixes(candidate),
            )
            if normalized:
                return normalized
            return base_slug
        return slugify(candidate.prompt)

    if task.task_type == "review":
        if task.depends_on is None:
            return slugify(task.prompt)
        target = store.get(task.depends_on)
        if target is None:
            logger.warning(
                "Slug override review target missing for task #%s: depends_on=%s; "
                "falling back to review task prompt",
                task.id,
                task.depends_on,
            )
            return slugify(task.prompt)
        return _slug_from_task(target)

    anchor_id = task.based_on or task.depends_on
    if anchor_id is None:
        return slugify(task.prompt)

    root = store.get(anchor_id)
    if root is None:
        logger.warning(
            "Slug override ancestor missing for task #%s while walking based_on chain: "
            "missing_parent=%s; using task prompt",
            task.id,
            anchor_id,
        )
        return slugify(task.prompt)

    seen: set[str] = set()
    last_resolved = root
    while root.based_on:
        next_id = root.based_on
        if root.id is not None:
            seen.add(root.id)
        if next_id in seen:
            logger.warning(
                "Slug override cycle detected for task #%s while walking based_on chain: "
                "ancestor=%s; using last resolved ancestor #%s",
                task.id,
                next_id,
                last_resolved.id,
            )
            break
        parent = store.get(next_id)
        if parent is None:
            logger.warning(
                "Slug override ancestor missing for task #%s while walking based_on chain: "
                "missing_parent=%s; using last resolved ancestor #%s",
                task.id,
                next_id,
                last_resolved.id,
            )
            break
        last_resolved = parent
        root = parent

    return _slug_from_task(last_resolved)


def _slug_exists(
    task_id: str,
    log_path: Path | None,
    git: Git | None,
    project_name: str | None,
    prompt: str = "",
    branch_strategy: "BranchStrategy | None" = None,
    explicit_type: str | None = None,
    project_prefix: str | None = None,
    store: SqliteTaskStore | None = None,
    exclude_task_id: str | None = None,
) -> bool:
    """Check if a slug is already in use (task row, log file, or branch exists)."""
    if store is not None:
        existing = store.get_by_slug(task_id)
        if existing is not None and existing.id != exclude_task_id:
            return True
    # Check log file
    if log_path and (log_path / f"{task_id}.log").exists():
        return True
    # Check branch using the actual branch naming pattern from config
    if git and project_name:
        if branch_strategy is not None:
            branch_name = generate_branch_name(
                pattern=branch_strategy.pattern,
                project_name=project_name,
                task_slug=task_id,
                prompt=prompt,
                default_type=branch_strategy.default_type,
                explicit_type=explicit_type,
                # task.id is not yet assigned at slug-generation time; patterns
                # that depend on {task_id} won't collision-check cleanly.
                task_id="",
                project_prefix=project_prefix or "",
            )
        else:
            # Fallback for callers that don't supply a strategy (e.g., tests or legacy callers).
            branch_name = f"{project_name}/{task_id}"
        if git.branch_exists(branch_name):
            return True
    return False


def build_prompt(
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    report_path: Path | None = None,
    summary_path: Path | None = None,
    git: Git | None = None,
    review_verify_result: str | None = None,
) -> str:
    """Build the prompt for Claude."""
    return PromptBuilder().build(
        task,
        config,
        store,
        report_path=report_path,
        summary_path=summary_path,
        git=git,
        review_verify_result=review_verify_result,
    )


def _get_task_output(task: Task, project_dir: Path) -> str | None:
    """Get task output content, preferring DB over filesystem.

    Auto-sync: If report_file exists and is newer than completed_at,
    read from disk instead of DB (allows users to edit plans).
    """
    # Check if file has been modified after task completion
    if task.report_file and task.completed_at:
        path = project_dir / task.report_file
        if path.exists():
            file_mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
            # If file is newer than task completion, read from file
            if file_mtime > task.completed_at:
                return path.read_text()

    # Prefer DB content (works in distributed mode)
    if task.output_content:
        return task.output_content

    # Fall back to file (local mode, backward compat)
    if task.report_file:
        path = project_dir / task.report_file
        if path.exists():
            return path.read_text()

    # Final fallback for code-task summaries when report_file/output_content are absent.
    # This supports older tasks where summary content exists only on disk.
    if task.slug and task.task_type in {"task", "implement", "improve", "fix"}:
        summary_path = project_dir / SUMMARY_DIR / f"{task.slug}.md"
        if summary_path.exists():
            return summary_path.read_text()

    return None


def _compact_output_summary(content: str, max_chars: int = REVIEW_IMPROVE_SUMMARY_MAX_CHARS) -> str:
    """Reduce markdown output content to a compact, single-line summary."""
    lines = []
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line == "```" or line.startswith("```"):
            continue
        if line.startswith("#"):
            line = line.lstrip("#").strip()
            if not line:
                continue
        if line.startswith("- "):
            line = line[2:].strip()
        lines.append(line)
        if len(lines) >= 4:
            break

    compact = " ".join(lines).strip()
    compact = re.sub(r"\s+", " ", compact)
    if len(compact) > max_chars:
        return compact[: max_chars - 3].rstrip() + "..."
    return compact


def _truncate_to_word_boundary(text: str, max_chars: int) -> str:
    """Truncate text on word boundaries, adding ellipsis when shortened."""
    compact = re.sub(r"\s+", " ", text).strip()
    if len(compact) <= max_chars:
        return compact

    cutoff = max_chars - 3
    if cutoff <= 0:
        return "." * max_chars

    candidate = compact[:cutoff].rstrip()
    split = candidate.rfind(" ")
    if split > 0:
        candidate = candidate[:split].rstrip()
    if not candidate:
        candidate = compact[:cutoff].rstrip()
    return f"{candidate}..."


def _decode_subprocess_output(output: str | bytes | None) -> str:
    """Normalize subprocess output payloads to text."""
    if output is None:
        return ""
    if isinstance(output, bytes):
        return output.decode("utf-8", errors="replace")
    return output


@dataclass(frozen=True)
class ReviewVerifyResult:
    """Structured review verify result with branch-tip provenance."""

    command: str
    status: str
    exit_status: str
    captured_at: datetime
    reviewed_branch: str | None = None
    reviewed_head_sha: str | None = None
    reviewed_base_sha: str | None = None
    working_directory: str | None = None
    failure: str | None = None
    output: str | None = None


@dataclass(frozen=True)
class ProjectReviewVerifyResult:
    """Per-project verification outcome for cross-project review runs."""

    project: RepoProjectConfig | None
    scope: str
    working_directory: str
    result: ReviewVerifyResult | None = None
    skip_reason: str | None = None


@dataclass(frozen=True)
class CrossProjectReviewVerifyResult:
    """Rendered cross-project review verification plus aggregate persistence state."""

    markdown: str
    aggregate_result: ReviewVerifyResult
    project_results: tuple[ProjectReviewVerifyResult, ...]


def _combine_review_verify_output(*parts: str | bytes | None) -> str:
    """Combine stdout/stderr fragments for review verify reporting."""
    return "\n".join(
        text.strip()
        for text in (_decode_subprocess_output(part) for part in parts)
        if text.strip()
    ).strip()


def _make_review_verify_result(
    command: str,
    *,
    status: str,
    exit_status: str,
    captured_at: datetime,
    reviewed_branch: str | None = None,
    reviewed_head_sha: str | None = None,
    reviewed_base_sha: str | None = None,
    working_directory: str | None = None,
    failure: str | None = None,
    output: str | bytes | None = None,
) -> ReviewVerifyResult:
    """Build a structured review verify result."""
    return ReviewVerifyResult(
        command=command,
        status=status,
        exit_status=exit_status,
        captured_at=captured_at,
        reviewed_branch=reviewed_branch,
        reviewed_head_sha=reviewed_head_sha,
        reviewed_base_sha=reviewed_base_sha,
        working_directory=working_directory,
        failure=failure,
        output=_combine_review_verify_output(output),
    )


def _format_review_verify_failure(
    command: str,
    *,
    exit_status: str,
    failure: str,
    output: str | bytes | None = None,
    reviewed_branch: str | None = None,
    reviewed_head_sha: str | None = None,
    reviewed_base_sha: str | None = None,
    working_directory: str | None = None,
    captured_at: datetime | None = None,
) -> str:
    """Compatibility wrapper for tests and legacy callers that expect markdown."""
    return _format_review_verify_result(
        _make_review_verify_result(
            command,
            status="failed" if exit_status != "launch failed" else "unavailable",
            exit_status=exit_status,
            captured_at=captured_at or datetime.now(UTC),
            reviewed_branch=reviewed_branch,
            reviewed_head_sha=reviewed_head_sha,
            reviewed_base_sha=reviewed_base_sha,
            working_directory=working_directory,
            failure=failure,
            output=output,
        )
    )


def _extract_review_verify_phase_results(output: str | None) -> list[dict[str, Any]]:
    """Parse structured per-phase verification lines from captured command output."""
    if not output:
        return []
    matches = re.finditer(
        r"^gza-verify phase=(?P<status>passed|failed) name=(?P<name>[A-Za-z0-9_.-]+) "
        r"duration_seconds=(?P<duration>[0-9.]+)"
        r"(?: tree_fingerprint=(?P<tree_fingerprint>[0-9a-f]{64}))?$",
        output,
        re.MULTILINE,
    )
    phases: list[dict[str, Any]] = []
    for match in matches:
        phase: dict[str, Any] = {
            "name": match.group("name"),
            "status": match.group("status"),
            "duration_seconds": float(match.group("duration")),
        }
        tree_fingerprint = match.group("tree_fingerprint")
        if tree_fingerprint:
            phase["tree_fingerprint"] = tree_fingerprint
        phases.append(phase)
    return phases


def _store_review_verify_artifact_records(
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    result: ReviewVerifyResult,
    project_results: tuple[ProjectReviewVerifyResult, ...] = (),
    *,
    producer: str,
    metadata: dict[str, Any] | None = None,
) -> str | None:
    """Persist durable verify artifacts and return the latest content-bearing path."""
    stored_paths: list[tuple[datetime, str]] = []
    shared_metadata = {
        "reviewed_branch": result.reviewed_branch,
        "reviewed_head_sha": result.reviewed_head_sha,
        "reviewed_base_sha": result.reviewed_base_sha,
        **(metadata or {}),
    }
    if project_results:
        for entry in project_results:
            entry_result = entry.result
            entry_status = entry_result.status if entry_result is not None else "skipped"
            entry_exit_status = entry_result.exit_status if entry_result is not None else "skipped"
            entry_command = entry_result.command if entry_result is not None else None
            entry_head_sha = entry_result.reviewed_head_sha if entry_result is not None else result.reviewed_head_sha
            entry_branch = entry_result.reviewed_branch if entry_result is not None else result.reviewed_branch
            entry_base_sha = entry_result.reviewed_base_sha if entry_result is not None else result.reviewed_base_sha
            entry_created_at = entry_result.captured_at if entry_result is not None else result.captured_at
            entry_output = entry_result.output if entry_result is not None else None
            stored = store_command_output_artifact(
                store,
                task,
                config,
                kind="verify_command_output",
                producer=producer,
                label="verify_command",
                output=entry_output,
                command=entry_command,
                status=entry_status,
                exit_status=entry_exit_status,
                head_sha=entry_head_sha,
                scope=entry.scope,
                metadata={
                    "scope": entry.scope,
                    "working_directory": entry.working_directory,
                    "skip_reason": entry.skip_reason,
                    "reviewed_branch": entry_branch,
                    "reviewed_head_sha": entry_head_sha,
                    "reviewed_base_sha": entry_base_sha,
                    **(metadata or {}),
                },
                created_at=entry_created_at,
            )
            if entry_output:
                stored_paths.append((entry_created_at, stored.path))
    else:
        stored = store_command_output_artifact(
            store,
            task,
            config,
            kind="verify_command_output",
            producer=producer,
            label="verify_command",
            output=result.output,
            command=result.command,
            status=result.status,
            exit_status=result.exit_status,
            head_sha=result.reviewed_head_sha,
            metadata={
                "working_directory": result.working_directory,
                **shared_metadata,
            },
            created_at=result.captured_at,
        )
        if result.output:
            stored_paths.append((result.captured_at, stored.path))
    if not stored_paths:
        return None
    stored_paths.sort(key=lambda item: item[0], reverse=True)
    return stored_paths[0][1]


def _persist_review_verify_result(
    task: Task,
    result: ReviewVerifyResult | None,
    *,
    markdown: str | None = None,
    artifact_file: str | None = None,
) -> None:
    """Copy structured review verify provenance onto a persisted task row."""
    if result is None:
        task.review_verify_command = None
        task.review_verify_status = None
        task.review_verify_exit_status = None
        task.review_verify_failure = None
        task.review_verify_captured_at = None
        task.review_verify_head_sha = None
        task.review_verify_base_sha = None
        task.review_verify_branch = None
        task.review_verify_markdown = None
        task.review_verify_cwd = None
        task.review_verify_artifact_file = None
        return

    task.review_verify_command = result.command
    task.review_verify_status = result.status
    task.review_verify_exit_status = result.exit_status
    task.review_verify_failure = result.failure
    task.review_verify_captured_at = result.captured_at
    task.review_verify_head_sha = result.reviewed_head_sha
    task.review_verify_base_sha = result.reviewed_base_sha
    task.review_verify_branch = result.reviewed_branch
    task.review_verify_markdown = markdown
    task.review_verify_cwd = result.working_directory
    task.review_verify_artifact_file = artifact_file


def _capture_review_verify_result(
    config: Config,
    store: SqliteTaskStore,
    task: Task,
    result: ReviewVerifyResult,
    *,
    markdown: str,
    project_results: tuple[ProjectReviewVerifyResult, ...] = (),
    task_logger: TaskExecutionLogger | None = None,
    producer: str = "review_verify",
    metadata: dict[str, Any] | None = None,
) -> str:
    """Persist review verify provenance, artifact, and optional ops-log evidence."""
    artifact_file = _store_review_verify_artifact_records(
        task,
        config,
        store,
        result=result,
        project_results=project_results,
        producer=producer,
        metadata=metadata,
    )
    _persist_review_verify_result(
        task,
        result,
        markdown=markdown,
        artifact_file=artifact_file,
    )
    store.update(task)
    if task_logger is not None:
        task_logger.phase(
            f"Captured review verify result: {result.status} ({result.exit_status})",
            extra={
                "event": "review_verify_result",
                "review_verify_status": result.status,
                "review_verify_exit_status": result.exit_status,
                "review_verify_command": result.command,
                "review_verify_captured_at": result.captured_at.isoformat(),
                "review_verify_branch": result.reviewed_branch,
                "review_verify_head_sha": result.reviewed_head_sha,
                "review_verify_base_sha": result.reviewed_base_sha,
                "review_verify_cwd": result.working_directory,
                "review_verify_artifact_file": artifact_file,
            },
        )
    return artifact_file or ""


def _format_review_verify_result(
    result: ReviewVerifyResult | str,
    completed: subprocess.CompletedProcess[str] | None = None,
) -> str:
    """Format a review-iteration verify result as prompt context."""
    if isinstance(result, str):
        if completed is None:
            raise TypeError("completed result is required when formatting from command + CompletedProcess")
        result = _make_review_verify_result(
            result,
            status="passed" if completed.returncode == 0 else "failed",
            exit_status=str(completed.returncode),
            captured_at=datetime.now(UTC),
            output=_combine_review_verify_output(completed.stdout, completed.stderr),
        )
    lines = [
        "## verify_command result",
        "",
        f"- Command: `{result.command}`",
        f"- Status: {result.status}",
        f"- Exit status: {result.exit_status}",
        f"- Captured at: {result.captured_at.isoformat()}",
    ]
    if result.working_directory:
        lines.append(f"- Working directory: `{result.working_directory}`")
    if result.reviewed_branch:
        lines.append(f"- Reviewed branch: `{result.reviewed_branch}`")
    if result.reviewed_head_sha:
        lines.append(f"- Reviewed head: `{result.reviewed_head_sha}`")
    if result.reviewed_base_sha:
        lines.append(f"- Reviewed base/default SHA: `{result.reviewed_base_sha}`")
    if result.failure:
        lines.append(f"- Failure: {result.failure}")
    if result.status != "passed":
        trimmed_output = _truncate_to_word_boundary(
            result.output or result.failure or "(no failing output captured)",
            REVIEW_VERIFY_OUTPUT_MAX_CHARS,
        )
        lines.extend(
            [
                "",
                "Failing output (trimmed):",
                "```text",
                trimmed_output,
                "```",
            ]
        )
    return "\n".join(lines)


def _run_review_verify_command(
    verify_command: str,
    *,
    cwd: Path,
    reviewed_branch: str | None = None,
    reviewed_head_sha: str | None = None,
    reviewed_base_sha: str | None = None,
    timeout_seconds: int = REVIEW_VERIFY_TIMEOUT_SECONDS,
) -> ReviewVerifyResult:
    """Run the configured verify command for an autonomous review iteration."""
    captured_at = datetime.now(UTC)
    started_at = time.monotonic()
    try:
        result = subprocess.run(
            ["bash", "-lc", verify_command],
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        return _make_review_verify_result(
            verify_command,
            status="failed",
            exit_status="timed out",
            captured_at=captured_at,
            reviewed_branch=reviewed_branch,
            reviewed_head_sha=reviewed_head_sha,
            reviewed_base_sha=reviewed_base_sha,
            working_directory=str(cwd),
            failure=f"verify_command timed out after {timeout_seconds}s",
            output=_combine_review_verify_output(exc.stdout, exc.stderr),
        )
    except OSError as exc:
        return _make_review_verify_result(
            verify_command,
            status="unavailable",
            exit_status="launch failed",
            captured_at=captured_at,
            reviewed_branch=reviewed_branch,
            reviewed_head_sha=reviewed_head_sha,
            reviewed_base_sha=reviewed_base_sha,
            working_directory=str(cwd),
            failure=f"failed to launch verify_command: {exc}",
        )
    elapsed = time.monotonic() - started_at
    if elapsed > (0.8 * timeout_seconds):
        warning_message = (
            f"verify_command used {elapsed:.1f}s of {timeout_seconds}s budget - "
            "suite is approaching the review wall; profile before it starts timing out"
        )
        logger.warning(warning_message)
        console.print(f"[yellow]Warning: {warning_message}[/yellow]")
    return _make_review_verify_result(
        verify_command,
        status="passed" if result.returncode == 0 else "failed",
        exit_status=str(result.returncode),
        captured_at=captured_at,
        reviewed_branch=reviewed_branch,
        reviewed_head_sha=reviewed_head_sha,
        reviewed_base_sha=reviewed_base_sha,
        working_directory=str(cwd),
        output=_combine_review_verify_output(result.stdout, result.stderr),
    )


def _format_review_verify_skip(project: RepoProjectConfig, reason: str, *, working_directory: str) -> str:
    """Format an explicit skipped verification entry for a discovered project."""
    scope = _format_repo_project_scope(project.scope_root)
    return "\n".join(
        [
            f"### {scope}",
            "",
            f"- Working directory: `{working_directory}`",
            "- Status: skipped",
            f"- Reason: {reason}",
        ]
    )


def _strip_review_verify_heading(markdown: str) -> list[str]:
    """Return verify markdown without the top-level section header."""
    lines = markdown.splitlines()
    if lines[:2] == ["## verify_command result", ""]:
        return lines[2:]
    return lines


def _aggregate_cross_project_review_verify_result(
    *,
    command: str,
    captured_at: datetime,
    reviewed_branch: str | None,
    reviewed_head_sha: str | None,
    reviewed_base_sha: str | None,
    project_results: list[ProjectReviewVerifyResult],
) -> ReviewVerifyResult:
    """Summarize per-project review verification into one persisted aggregate result."""
    runnable_results = [entry.result for entry in project_results if entry.result is not None]
    passed_count = sum(1 for result in runnable_results if result.status == "passed")
    failed_count = sum(1 for result in runnable_results if result.status == "failed")
    unavailable_count = sum(1 for result in runnable_results if result.status == "unavailable")
    skipped_count = sum(1 for entry in project_results if entry.result is None)
    cannot_run_count = unavailable_count + skipped_count

    if failed_count > 0:
        status = "failed"
    elif cannot_run_count > 0:
        status = "unavailable"
    elif passed_count > 0:
        status = "passed"
    else:
        status = "unavailable"

    summary_parts = [
        f"{passed_count} passed",
        f"{failed_count} failed",
        f"{unavailable_count} unavailable",
    ]
    if skipped_count > 0:
        summary_parts.append(f"{skipped_count} skipped")
    exit_status = ", ".join(summary_parts)

    failure: str | None = None
    if status != "passed":
        if failed_count > 0:
            failure = "one or more affected projects failed review verification"
        elif cannot_run_count > 0:
            failure = "one or more affected projects could not run review verification"
        else:
            failure = "no affected project had a runnable verify_command"

    return _make_review_verify_result(
        command,
        status=status,
        exit_status=exit_status,
        captured_at=captured_at,
        reviewed_branch=reviewed_branch,
        reviewed_head_sha=reviewed_head_sha,
        reviewed_base_sha=reviewed_base_sha,
        working_directory="(per-project; see artifact)",
        failure=failure,
    )


def _run_review_verify_commands_for_projects(
    *,
    config: Config,
    task: Task,
    worktree_git: Git,
    worktree_path: Path,
    timeout_seconds: int,
    reviewed_branch: str | None = None,
    reviewed_head_sha: str | None = None,
    reviewed_base_sha: str | None = None,
) -> CrossProjectReviewVerifyResult | None:
    """Run autonomous review verification from each affected project root."""
    if not _task_is_cross_project(task):
        verify_command = config.verify_command if isinstance(config.verify_command, str) else ""
        if not verify_command.strip():
            return None
        project_cwd = _worktree_project_root(worktree_path, _project_boundary(config))
        result = _run_review_verify_command(
            verify_command.strip(),
            cwd=project_cwd,
            reviewed_branch=reviewed_branch,
            reviewed_head_sha=reviewed_head_sha,
            reviewed_base_sha=reviewed_base_sha,
            timeout_seconds=timeout_seconds,
        )
        scope = _format_repo_project_scope(_project_boundary(config).scope_root)
        return CrossProjectReviewVerifyResult(
            markdown=_format_review_verify_result(result),
            aggregate_result=result,
            project_results=(
                ProjectReviewVerifyResult(
                    project=None,
                    scope=scope,
                    working_directory=str(project_cwd),
                    result=result,
                ),
            )
        )

    default_branch = worktree_git.default_branch()
    parsed_name_status = parse_name_status_project_paths(
        worktree_git.get_diff_name_status(f"{default_branch}...HEAD", check=True)
    )
    if not parsed_name_status.changed_paths:
        return None

    affected = resolve_affected_repo_projects(
        config,
        parsed_name_status.changed_paths,
        repo_root=worktree_path,
        declared_project_roots=parsed_name_status.declared_project_roots,
    )
    if not affected.projects and not affected.unknown_paths:
        return None

    project_results: list[ProjectReviewVerifyResult] = []
    section_entries: list[str] = []
    for project in affected.projects:
        scope = _format_repo_project_scope(project.scope_root)
        project_cwd = worktree_path if project.scope_root == Path(".") else worktree_path / project.scope_root
        if not project.verify_command:
            project_results.append(
                ProjectReviewVerifyResult(
                    project=project,
                    scope=scope,
                    working_directory=str(project_cwd),
                    skip_reason="no verify_command configured for this affected project",
                )
            )
            section_entries.extend(
                [
                    _format_review_verify_skip(
                        project,
                        "no verify_command configured for this affected project",
                        working_directory=str(project_cwd),
                    ),
                    "",
                ]
            )
            continue

        result = _run_review_verify_command(
            project.verify_command,
            cwd=project_cwd,
            reviewed_branch=reviewed_branch,
            reviewed_head_sha=reviewed_head_sha,
            reviewed_base_sha=reviewed_base_sha,
            timeout_seconds=timeout_seconds,
        )
        project_results.append(
            ProjectReviewVerifyResult(
                project=project,
                scope=scope,
                working_directory=str(project_cwd),
                result=result,
            )
        )
        result_lines = _strip_review_verify_heading(_format_review_verify_result(result))
        section_entries.extend([f"### {scope}", "", *result_lines, ""])

    if affected.unknown_paths:
        project_results.append(
            ProjectReviewVerifyResult(
                project=None,
                scope="unknown paths",
                working_directory="unknown paths",
                skip_reason="affected paths fell outside all discovered project roots",
            )
        )
        section_entries.extend(
            [
                "### unknown paths",
                "",
                "- Status: skipped",
                "- Reason: affected paths fell outside all discovered project roots",
                f"- Paths: {', '.join(affected.unknown_paths)}",
                "",
            ]
        )

    aggregate_result = _aggregate_cross_project_review_verify_result(
        command="(per-project verify_command)",
        captured_at=datetime.now(UTC),
        reviewed_branch=reviewed_branch,
        reviewed_head_sha=reviewed_head_sha,
        reviewed_base_sha=reviewed_base_sha,
        project_results=project_results,
    )
    sections: list[str] = _format_review_verify_result(aggregate_result).splitlines()
    sections.extend(["", "Per affected project:", ""])
    sections.extend(section_entries)
    return CrossProjectReviewVerifyResult(
        markdown="\n".join(sections).rstrip(),
        aggregate_result=aggregate_result,
        project_results=tuple(project_results),
    )


def _default_code_task_commit_subject(task_slug: str | None, task_db_id: str | None) -> str:
    """Build deterministic fallback commit subject for code tasks."""
    if task_slug and task_slug.strip():
        return f"gza task {task_slug.strip()}"
    if task_db_id is not None:
        return f"Task {task_db_id}"
    return "gza task"


def _resolve_review_base_ref(
    task: Task,
    store: SqliteTaskStore,
    git: Git | None,
    default_branch: str,
) -> tuple[str, str | None]:
    """Return the detached review checkout ref and reviewed implementation branch when available."""
    if task.task_type == "review" and task.depends_on:
        dep_task = store.get(task.depends_on)
        if dep_task and dep_task.branch and dep_task.status == "completed":
            return dep_task.branch, dep_task.branch

    base_ref = f"origin/{default_branch}"
    if git:
        git_result = git._run("rev-parse", "--verify", base_ref, check=False)
        if git_result.returncode != 0:
            base_ref = default_branch
    return base_ref, None


def _create_detached_review_worktree(git: Git | None, worktree_path: Path, base_ref: str) -> None:
    """Create a detached review worktree from the requested ref."""
    if worktree_path.exists() and git:
        git.worktree_remove(worktree_path, force=True)
    if git:
        git._run("worktree", "add", "--detach", str(worktree_path), base_ref)


def _resolve_review_verify_base_sha(git: Git | None, default_branch: str) -> str | None:
    """Resolve the default/base branch SHA used as review context when available."""
    if git is None:
        return None
    for ref in (default_branch, f"origin/{default_branch}"):
        sha = git.rev_parse_if_exists(ref)
        if isinstance(sha, str) and sha:
            return sha
    return None


def _build_code_task_commit_subject(task_prompt: str, worktree_summary_path: Path, fallback_subject: str | None = None) -> str:
    """Build commit subject from worktree summary, with prompt fallback."""
    fallback = (fallback_subject or "").strip() or "gza task"
    if worktree_summary_path.exists():
        try:
            summary_content = worktree_summary_path.read_text().strip()
        except (OSError, UnicodeError):
            logger.warning(
                "Failed to read summary file for commit subject at %s; falling back",
                worktree_summary_path,
                exc_info=True,
            )
        else:
            if summary_content:
                compact_summary = _compact_output_summary(summary_content)
                summary_subject = _truncate_to_word_boundary(compact_summary, max_chars=COMMIT_SUBJECT_MAX_CHARS)
                if summary_subject:
                    return summary_subject

    prompt_subject = _truncate_to_word_boundary(task_prompt, max_chars=COMMIT_SUBJECT_MAX_CHARS)
    if prompt_subject:
        return prompt_subject
    return fallback


def _is_improve_in_impl_chain(improve_task: Task, impl_task: Task, tasks_by_id: dict[str, Task]) -> bool:
    """Return True when an improve task belongs to an implementation's improve chain."""
    if impl_task.id is None or improve_task.based_on is None:
        return False
    current_based_on = improve_task.based_on
    seen: set[str] = set()
    while True:
        if current_based_on == impl_task.id:
            return True
        if current_based_on in seen:
            return False
        seen.add(current_based_on)
        parent = tasks_by_id.get(current_based_on)
        if parent is None or parent.task_type != "improve" or parent.based_on is None:
            return False
        current_based_on = parent.based_on


def _get_completed_improves_for_implementation_chain(store: SqliteTaskStore, impl_task: Task) -> list[Task]:
    """Collect completed improve tasks tied to an implementation, including retry/resume descendants."""
    all_tasks = store.get_all()
    tasks_by_id = {task.id: task for task in all_tasks if task.id is not None}
    return [
        task for task in all_tasks
        if task.task_type == "improve"
        and task.id is not None
        and task.status == "completed"
        and _is_improve_in_impl_chain(task, impl_task, tasks_by_id)
    ]


def _build_review_improve_lineage_context(review_task: Task, impl_task: Task, store: SqliteTaskStore, project_dir: Path) -> str:
    """Build compact improve lineage context for review prompts."""
    improves = _get_completed_improves_for_implementation_chain(store, impl_task)
    if not improves:
        return ""

    review_created_at = review_task.created_at
    prior_improves = []
    for improve in improves:
        if review_created_at is None:
            prior_improves.append(improve)
            continue
        if improve.created_at is None:
            if review_task.id is not None and improve.id is not None and task_id_numeric_key(improve.id) < task_id_numeric_key(review_task.id):
                prior_improves.append(improve)
            continue

        if (improve.created_at, task_id_numeric_key(improve.id)) < (review_created_at, task_id_numeric_key(review_task.id)):
            prior_improves.append(improve)

    if not prior_improves:
        return ""

    # Most recent first by completion/creation, then id.
    prior_improves.sort(
        key=lambda t: (
            t.completed_at or t.created_at or datetime.min.replace(tzinfo=UTC),
            task_id_numeric_key(t.id),
        ),
        reverse=True,
    )

    included = prior_improves[:REVIEW_IMPROVE_LINEAGE_LIMIT]
    omitted_count = max(0, len(prior_improves) - len(included))
    n_iterations = len(prior_improves)
    latest_improve = prior_improves[0]
    latest_review_task = store.get(latest_improve.depends_on) if latest_improve.depends_on else None
    latest_review_report = (
        parse_review_report(_get_task_output(latest_review_task, project_dir))
        if latest_review_task is not None
        else None
    )
    state_parts = [
        f"prior iterations: {n_iterations}",
        f"latest review: {latest_improve.depends_on or 'unknown'}",
        f"verdict={latest_review_report.verdict if latest_review_report is not None and latest_review_report.verdict else 'unknown'}",
        f"score={latest_review_task.review_score if latest_review_task is not None and latest_review_task.review_score is not None else 'unknown'}",
        f"latest improve: {latest_improve.id or 'unknown'}",
        f"status={latest_improve.status or 'unknown'}",
    ]
    if omitted_count:
        state_parts.append(f"older iterations omitted: {omitted_count}")

    lines = [
        "## Improve Lineage Context",
        "",
        "Prior iteration history is coordination context only; it is not evidence that any blocker is still open.",
        "Current state: " + ", ".join(state_parts) + ".",
        "",
    ]

    for index, improve in enumerate(included, start=1):
        iteration_number = n_iterations - (index - 1)
        review_task_for_iteration = store.get(improve.depends_on) if improve.depends_on else None
        review_report = (
            parse_review_report(_get_task_output(review_task_for_iteration, project_dir))
            if review_task_for_iteration is not None
            else None
        )
        verdict = review_report.verdict if review_report is not None else None
        score = (
            review_task_for_iteration.review_score
            if review_task_for_iteration is not None and review_task_for_iteration.review_score is not None
            else "unknown"
        )
        completed = improve.completed_at.isoformat() if improve.completed_at is not None else "unknown"
        lines.append(
            f"- iteration {iteration_number}: review {improve.depends_on or '?'} "
            f"verdict={verdict or 'unknown'} score={score} -> "
            f"improve {improve.id or '?'} status={improve.status or 'unknown'} completed={completed}"
        )

    return "\n".join(lines)


def _parse_changed_files_from_numstat(numstat_output: str) -> list[str]:
    """Extract changed file paths from git diff --numstat output."""
    changed_files: list[str] = []
    for line in numstat_output.splitlines():
        parts = line.split("\t", 2)
        if len(parts) < 3:
            continue
        changed_files.append(parts[2].strip())
    return changed_files


def _build_review_diff_context(
    git: Git,
    revision_range: str,
    default_branch: str,
    branch_name: str,
    *,
    diff_small_threshold: int = DIFF_SMALL_THRESHOLD,
    diff_medium_threshold: int = DIFF_MEDIUM_THRESHOLD,
    review_context_file_limit: int = REVIEW_CONTEXT_FILE_LIMIT,
) -> str:
    """Build self-contained review diff context for prompts."""
    numstat_output = git.get_diff_numstat(revision_range)
    if not isinstance(numstat_output, str):
        numstat_output = ""
    files_changed, lines_added, lines_removed = parse_diff_numstat(numstat_output)
    total_lines = lines_added + lines_removed
    changed_files = _parse_changed_files_from_numstat(numstat_output)
    review_base_sha = git.merge_base(default_branch, branch_name)
    default_branch_sha = git.rev_parse(default_branch)
    branch_head_sha = git.rev_parse(branch_name)

    parts = [
        "## Implementation Diff Context",
        "",
        f"Implementation branch: {branch_name}",
        f"Implementation head: {branch_name} ({branch_head_sha})",
        f"Local default branch: {default_branch} ({default_branch_sha})",
        f"Review base (merge-base): {review_base_sha}",
        f"Revision range: {revision_range}",
        f"Files changed: {files_changed}, lines added: {lines_added}, lines removed: {lines_removed}",
    ]

    if changed_files:
        parts.append("")
        parts.append("Changed files:")
        for file_path in changed_files:
            parts.append(f"- {file_path}")

    stat_summary = git.get_diff_stat(revision_range)
    if not isinstance(stat_summary, str):
        stat_summary = ""
    if stat_summary:
        parts.append("")
        parts.append("Diff summary:")
        parts.append(stat_summary)

    if total_lines < diff_small_threshold:
        diff_content = git.get_diff(revision_range)
        if not isinstance(diff_content, str):
            diff_content = ""
        if diff_content:
            parts.append("")
            parts.append("Full diff:")
            parts.append(diff_content)
        return "\n".join(parts)

    if total_lines < diff_medium_threshold:
        diff_content = git.get_diff(revision_range)
        if not isinstance(diff_content, str):
            diff_content = ""
        if diff_content:
            parts.append("")
            parts.append("Full diff:")
            parts.append(diff_content)
        return "\n".join(parts)

    # Large diff: include targeted per-file diff excerpts for the most relevant files.
    selected_files = changed_files[:review_context_file_limit]
    if selected_files:
        excerpt_result = git._run(
            "diff",
            "--unified=8",
            revision_range,
            "--",
            *selected_files,
            check=False,
        )
        excerpt_stdout = excerpt_result.stdout if isinstance(excerpt_result.stdout, str) else ""
        excerpt_content = excerpt_stdout.strip()
        if excerpt_content:
            parts.append("")
            parts.append(
                f"Targeted diff excerpts (first {len(selected_files)} changed files; total changed lines: {total_lines}):"
            )
            parts.append(excerpt_content)
        if len(changed_files) > len(selected_files):
            parts.append("")
            parts.append(
                f"Additional changed files not expanded inline: {len(changed_files) - len(selected_files)}"
            )

    return "\n".join(parts)


def _build_context_from_chain(
    task: Task,
    store: SqliteTaskStore,
    project_dir: Path,
    git: Git | None,
    config: Config | None = None,
    review_verify_result: str | None = None,
) -> str:
    """Build context by walking the depends_on and based_on chain."""
    context_parts = []

    def _int_or_default(value: object, default: int) -> int:
        return value if isinstance(value, int) else default

    def _plan_source_context_block(heading: str, source_task: Task) -> str:
        metadata = (
            f"{heading}"
            f"Source task id: {source_task.id}\n"
            f"Source task type: {source_task.task_type}\n"
        )
        source_content = _get_task_output(source_task, project_dir)
        if source_content:
            return metadata + "\n" + source_content
        return (
            metadata
            + f"\n(plan source task {source_task.id} exists but content unavailable on this machine - flag as blocker)"
        )

    # For plan review tasks, include the reviewed plan source.
    if task.task_type == "plan_review" and task.depends_on:
        source_task = store.get(task.depends_on)
        if source_task is not None:
            context_parts.append(
                _plan_source_context_block("## Plan source to review:\n", source_task)
            )

    # For plan improvement tasks, include the triggering plan review and latest plan source.
    if task.task_type == "plan_improve":
        if task.depends_on:
            review_task = store.get(task.depends_on)
            if review_task is not None and review_task.task_type == "plan_review":
                review_content = _get_task_output(review_task, project_dir)
                if review_content:
                    context_parts.append("## Review feedback to address:\n")
                    context_parts.append(review_content)
                else:
                    context_parts.append(
                        "## Review feedback to address:\n"
                        f"(plan review task {review_task.id} exists but content unavailable on this machine - flag as blocker)"
                    )

        if task.based_on:
            source_task = store.get(task.based_on)
            if source_task is not None:
                context_parts.append(
                    "\n"
                    + _plan_source_context_block(
                        "## Latest plan source:\n", source_task
                    )
                )

    # For improve tasks, include review feedback and original plan
    if task.task_type == "improve":
        impl_ancestor = _resolve_impl_ancestor(store, task)
        if impl_ancestor is not None and impl_ancestor.id is not None:
            unresolved_comments = store.get_comments(
                impl_ancestor.id,
                unresolved_only=True,
                created_on_or_before=task.created_at,
            )
            if unresolved_comments:
                context_parts.append("## Comments:\n")
                for comment in unresolved_comments:
                    source_author = f"source={comment.source}"
                    if comment.author:
                        source_author += f", author={comment.author}"
                    context_parts.append(
                        f"- #{comment.id} ({comment.created_at.strftime('%Y-%m-%d %H:%M:%S')} UTC, {source_author})"
                    )
                    context_parts.append(comment.content)

        # Get the review we're addressing
        if task.depends_on:
            review_task = store.get(task.depends_on)
            if review_task and review_task.task_type == "review":
                review_content = _get_task_output(review_task, project_dir)
                if review_content:
                    context_parts.append("## Review feedback to address:\n")
                    context_parts.append(review_content)
                    if is_verify_timeout_only_review(review_content):
                        context_parts.append("\n## Verify Timeout Guidance\n")
                        context_parts.append(
                            "The inbound review's only Blocker is a `verify_command` timeout. "
                            "Treat this as a test-performance investigation first, not a generic "
                            "code-correctness fix.\n"
                        )
                        context_parts.append(
                            "- Re-run the exact configured `verify_command` once from the current branch tip to confirm the timeout is still current."
                        )
                        context_parts.append(
                            "- If it still times out, run a narrower pytest duration probe such as `uv run pytest tests/ --durations=20` or the configured pytest subset with `--durations=20`."
                        )
                        context_parts.append(
                            "- Compare against the baseline branch when practical to determine whether this branch introduced the slowdown."
                        )
                        context_parts.append(
                            "- If this branch introduced the slowdown, fix or narrow the offending test."
                        )
                        context_parts.append(
                            "- If the slowdown is pre-existing or environmental, report that explicitly and escalate; do not silently relax suite-wide guardrails or change `verify_timeout`."
                        )
                else:
                    context_parts.append(
                        "## Review feedback to address:\n"
                        f"(review task {review_task.id} exists but content unavailable on this machine - flag as blocker)"
                    )

        if impl_ancestor is not None:
            plan_task = get_plan_for_task(store, impl_ancestor)
            if plan_task:
                plan_content = _get_task_output(plan_task, project_dir)
                if plan_content:
                    context_parts.append("\n## Original plan:\n")
                    context_parts.append(plan_content)
                else:
                    context_parts.append(
                        "\n## Original plan:\n"
                        f"(plan task {plan_task.id} exists but content unavailable on this machine - flag as blocker)"
                    )

    if task.task_type == "fix":
        root_impl = _resolve_root_implementation_for_fix(task, store)
        if root_impl is not None and root_impl.id is not None:
            context_parts.append("## Fix Rescue Context\n")
            context_parts.append(f"Root implementation: {root_impl.id}")

            reviews = [
                candidate
                for candidate in store.get_reviews_for_task(root_impl.id)
                if candidate.status == "completed"
            ]
            latest_review = reviews[0] if reviews else None
            if latest_review is not None and latest_review.id is not None:
                context_parts.append(f"Latest completed review: {latest_review.id}")
                latest_review_content = _get_task_output(latest_review, project_dir)
                if latest_review_content:
                    context_parts.append("\n## Review feedback to address:\n")
                    context_parts.append(latest_review_content)

            repeated = _extract_repeated_required_fixes(reviews[:2], project_dir)
            if repeated:
                context_parts.append("\n## Repeated Blockers\n")
                context_parts.extend(f"- {item}" for item in repeated)

            failed_improves = [
                candidate
                for candidate in store.get_improve_tasks_by_root(root_impl.id)
                if candidate.status == "failed"
            ]
            if failed_improves:
                latest_failed_improve = max(
                    failed_improves,
                    key=lambda candidate: candidate.completed_at or candidate.created_at or datetime.min.replace(tzinfo=UTC),
                )
                if latest_failed_improve.id is not None:
                    context_parts.append(f"\nLatest failed improve/resume attempt: {latest_failed_improve.id}")
                    context_parts.append(_extract_failure_context(latest_failed_improve, project_dir))

            failed_impl_retries = [
                candidate
                for candidate in store.get_based_on_children(root_impl.id)
                if candidate.task_type == "implement" and candidate.status == "failed"
            ]
            if failed_impl_retries:
                latest_failed_impl = max(
                    failed_impl_retries,
                    key=lambda candidate: candidate.completed_at or candidate.created_at or datetime.min.replace(tzinfo=UTC),
                )
                if latest_failed_impl.id is not None:
                    context_parts.append(
                        f"Latest failed implementation retry/resume attempt: {latest_failed_impl.id}"
                    )

            plan_task = get_plan_for_task(store, root_impl)

            if plan_task:
                plan_content = _get_task_output(plan_task, project_dir)
                if plan_content:
                    context_parts.append("\n## Original plan:\n")
                    context_parts.append(plan_content)
                else:
                    context_parts.append(
                        "\n## Original plan:\n"
                        f"(plan task {plan_task.id} exists but content unavailable on this machine - flag as blocker)"
                    )
            elif root_impl.prompt:
                context_parts.append("\n## Original request:\n")
                context_parts.append(root_impl.prompt)

    # For implement tasks, include plan from lineage chain.
    if task.task_type == "implement":
        followup_parts = extract_followup_prompt_parts(task.prompt)
        if followup_parts is not None:
            marker = "## Follow-up finding to implement:"
            if marker in task.prompt:
                context_parts.append(task.prompt[task.prompt.index(marker):].strip())
        plan_task = get_plan_for_task(store, task)
        if plan_task:
            plan_content = _get_task_output(plan_task, project_dir)
            if plan_content:
                context_parts.append("## Plan to implement:\n")
                context_parts.append(plan_content)

    # For review tasks, include both plan and diff
    if task.task_type == "review":
        # Find the implement task via depends_on
        if task.depends_on:
            impl_task = store.get(task.depends_on)
            if impl_task:
                # Include spec file content if the implementation task has a spec field
                if impl_task.spec:
                    spec_path = project_dir / impl_task.spec
                    if spec_path.exists():
                        spec_content = spec_path.read_text()
                        context_parts.append(f"## Specification\n\nThe following specification file ({impl_task.spec}) provides context for this implementation:\n\n{spec_content}")

                # Inject ask context: plan output for plan-driven work, else full original request.
                plan_task = get_plan_for_task(store, impl_task)
                resolved_scope = resolve_review_scope_for_impl(store, impl_task)
                review_scope_text = (task.review_scope or "").strip() or (
                    resolved_scope.summary if resolved_scope is not None else None
                )

                if review_scope_text:
                    context_parts.append("\n## Review scope:\n")
                    context_parts.append(f"Implementation task: {impl_task.id}")
                    context_parts.append(
                        "This is the only gradeable ask for this review. Treat sibling or deferred slices as out of scope unless the current diff breaks an explicit integration contract described here."
                    )
                    context_parts.append("")
                    context_parts.append(review_scope_text)
                    if resolved_scope is not None and resolved_scope.out_of_scope_context:
                        context_parts.append("")
                        context_parts.append("Out-of-scope sibling context:")
                        context_parts.append(resolved_scope.out_of_scope_context)

                if plan_task:
                    plan_content = _get_task_output(plan_task, project_dir)
                    plan_header = (
                        "\n## Original plan context (out of scope except for the review scope):\n"
                        if review_scope_text
                        else "\n## Original plan:\n"
                    )
                    if plan_content:
                        context_parts.append(plan_header)
                        context_parts.append(plan_content)
                    else:
                        context_parts.append(
                            plan_header +
                            f"(plan task {plan_task.id} exists but content unavailable on this machine - flag as blocker)"
                        )
                elif impl_task.prompt and not review_scope_text:
                    context_parts.append("\n## Original request:\n")
                    context_parts.append(impl_task.prompt)

                if review_verify_result:
                    context_parts.append("\n")
                    context_parts.append(review_verify_result)

                # Get diff if we have a branch (tiered strategy based on diff size)
                if impl_task.branch and git:
                    try:
                        default_branch = git.default_branch()
                        revision_range = f"{default_branch}...{impl_task.branch}"
                        context_parts.append(
                            _build_review_diff_context(
                                git,
                                revision_range,
                                default_branch,
                                impl_task.branch,
                                diff_small_threshold=_int_or_default(
                                    getattr(config, "review_diff_small_threshold", None),
                                    DIFF_SMALL_THRESHOLD,
                                ),
                                diff_medium_threshold=_int_or_default(
                                    getattr(config, "review_diff_medium_threshold", None),
                                    DIFF_MEDIUM_THRESHOLD,
                                ),
                                review_context_file_limit=_int_or_default(
                                    getattr(config, "review_context_file_limit", None),
                                    REVIEW_CONTEXT_FILE_LIMIT,
                                ),
                            )
                        )
                    except GitError:
                        pass  # Ignore git errors

                improve_lineage_context = _build_review_improve_lineage_context(task, impl_task, store, project_dir)
                if improve_lineage_context:
                    context_parts.append(improve_lineage_context)

    # Fallback for generic based_on references
    if task.based_on and not context_parts:
        parent_task = store.get(task.based_on)
        if parent_task and parent_task.report_file:
            context_parts.append(f"This task is based on the findings in: {parent_task.report_file}")
            context_parts.append("Read and review that report for context before implementing.")
        elif parent_task:
            context_parts.append(f"This task is a follow-up to task {parent_task.id}: {parent_task.prompt[:100]}")

    return "\n".join(context_parts) if context_parts else ""


def _resolve_root_implementation_for_fix(task: Task, store: SqliteTaskStore) -> Task | None:
    """Resolve the implementation root for fix and resumed/retried fix chains."""
    visited: set[str] = set()
    current: Task | None = task
    while current is not None:
        if current.id is not None:
            if current.id in visited:
                return None
            visited.add(current.id)
        if current.task_type == "implement":
            return current
        if current.based_on is None:
            return None
        current = store.get(current.based_on)
    return None


def _resolve_impl_ancestor(store: SqliteTaskStore, task: Task) -> Task | None:
    """Resolve the implementation ancestor by walking based_on lineage."""
    if task.task_type == "implement":
        return task
    visited: set[str] = set()
    current: Task | None = task
    while current is not None:
        if current.id is not None:
            if current.id in visited:
                return None
            visited.add(current.id)
        if current.task_type == "implement":
            return current
        if current.based_on is None:
            return None
        current = store.get(current.based_on)
    return None


def _task_has_tag(task: Task | None, tag: str) -> bool:
    """Return whether a task carries the given tag."""
    return task is not None and tag in task.tags


def _noop_improve_warning_text(
    store: SqliteTaskStore,
    task: Task,
    impl_ancestor: Task | None,
) -> str:
    """Build explicit operator-facing warning text for a no-op improve."""
    review_task = store.get(task.depends_on) if task.depends_on is not None else None
    opt_out = (
        _task_has_tag(impl_ancestor, "allow-noop-improve")
        or _task_has_tag(review_task, "allow-noop-improve")
        or _task_has_tag(task, "allow-noop-improve")
    )
    warning = "Improve completed with no tracked diff change."
    if opt_out:
        warning += " Tag `allow-noop-improve` is present, so continuation is explicitly allowed."
    return warning


def _resolved_merge_state_for_task(store: SqliteTaskStore, task: Task) -> str | None:
    """Resolve persisted merge state for a task without live git classification."""
    if task.id is None:
        return task.merge_status
    unit = store.resolve_merge_unit_for_task(task.id)
    if unit is not None:
        return unit.state
    return task.merge_status


def _emit_auto_review_suppressed(
    *,
    task: Task,
    config: Config,
    message: str,
    task_logger: TaskExecutionLogger | None,
) -> None:
    """Emit and persist an operator-visible auto-review suppression event."""
    if task_logger is not None:
        task_logger.info(message, extra={"task_id": task.id, "auto_review": "skipped"})
        return
    print(message)
    if not task.log_file:
        return
    write_log_entry(
        config.project_dir / Path(task.log_file),
        {
            "type": "gza",
            "subtype": "info",
            "message": message,
            "task_id": task.id,
            "auto_review": "skipped",
        },
    )


def _task_has_current_passing_review_verify_evidence(
    *,
    task: Task,
    review_task: Task,
    current_branch: str | None,
    current_head_sha: str | None,
) -> bool:
    """Require durable green verify evidence for the current improve tip."""
    if task.review_verify_status != "passed":
        return False
    if task.review_verify_captured_at is None:
        return False
    if review_task.completed_at is not None and task.review_verify_captured_at < review_task.completed_at:
        return False
    if not current_branch or not task.review_verify_branch:
        return False
    if task.review_verify_branch != current_branch:
        return False
    if not current_head_sha or not task.review_verify_head_sha:
        return False
    return task.review_verify_head_sha == current_head_sha


def _noop_improve_resolves_verify_only_review(
    *,
    config: Config,
    store: SqliteTaskStore,
    task: Task,
    impl_ancestor: Task | None,
    current_branch: str | None,
    current_head_sha: str | None,
) -> bool:
    """Return whether this no-op improve should clear a verify-only review block."""
    if task.task_type != "improve" or task.depends_on is None:
        return False
    if impl_ancestor is None or impl_ancestor.id is None:
        return False

    review_task = store.get(task.depends_on)
    if review_task is None or review_task.task_type != "review" or review_task.status != "completed":
        return False

    review_content = _get_task_output(review_task, Path(config.project_dir))
    if not is_verify_blocked_only_review(review_content):
        return False

    if not _task_has_current_passing_review_verify_evidence(
        task=task,
        review_task=review_task,
        current_branch=current_branch,
        current_head_sha=current_head_sha,
    ):
        return False

    store.clear_review_state(impl_ancestor.id)
    return True


def _capture_noop_improve_review_verify_result(
    *,
    config: Config,
    store: SqliteTaskStore,
    task: Task,
    worktree_git: Git,
    branch_name: str,
    task_logger: TaskExecutionLogger | None = None,
) -> ReviewVerifyResult | None:
    """Persist fresh verify evidence for a no-op improve blocked only by review verify."""
    if task.task_type != "improve" or task.depends_on is None:
        return None

    review_task = store.get(task.depends_on)
    if review_task is None or review_task.task_type != "review" or review_task.status != "completed":
        return None

    review_content = _get_task_output(review_task, Path(config.project_dir))
    if not is_verify_blocked_only_review(review_content):
        return None

    verify_command = config.verify_command if isinstance(config.verify_command, str) else ""
    if not _task_is_cross_project(task) and not verify_command.strip():
        return None

    timeout_seconds = getattr(config, "review_verify_timeout_seconds", REVIEW_VERIFY_TIMEOUT_SECONDS)
    if not isinstance(timeout_seconds, int) or timeout_seconds < 1:
        timeout_seconds = REVIEW_VERIFY_TIMEOUT_SECONDS

    provider_cwd = _worktree_execution_dir(worktree_git.repo_dir, _project_boundary(config))
    reviewed_base_sha: str | None = None
    reviewed_head_sha: str | None = None
    project_results: tuple[ProjectReviewVerifyResult, ...] = ()
    command_label = verify_command.strip() or "(review verify unavailable)"

    try:
        default_branch = worktree_git.default_branch()
        reviewed_base_sha = _resolve_review_verify_base_sha(worktree_git, default_branch)
        reviewed_head_sha = worktree_git.rev_parse_if_exists(branch_name)
        if reviewed_head_sha is None:
            result = _make_review_verify_result(
                command_label,
                status="unavailable",
                exit_status="unresolved head",
                captured_at=datetime.now(UTC),
                reviewed_branch=branch_name,
                reviewed_head_sha=None,
                reviewed_base_sha=reviewed_base_sha,
                working_directory=str(provider_cwd),
                failure="unable to resolve review worktree HEAD before verify_command ran",
            )
            markdown = _format_review_verify_result(result)
        elif _task_is_cross_project(task):
            cross_project_verify = _run_review_verify_commands_for_projects(
                config=config,
                task=task,
                worktree_git=worktree_git,
                worktree_path=worktree_git.repo_dir,
                timeout_seconds=timeout_seconds,
                reviewed_branch=branch_name,
                reviewed_head_sha=reviewed_head_sha,
                reviewed_base_sha=reviewed_base_sha,
            )
            if cross_project_verify is None:
                return None
            result = cross_project_verify.aggregate_result
            markdown = cross_project_verify.markdown
            project_results = cross_project_verify.project_results
        else:
            result = _run_review_verify_command(
                verify_command.strip(),
                cwd=provider_cwd,
                reviewed_branch=branch_name,
                reviewed_head_sha=reviewed_head_sha,
                reviewed_base_sha=reviewed_base_sha,
                timeout_seconds=timeout_seconds,
            )
            markdown = _format_review_verify_result(result)
    except (GitError, OSError, RuntimeError, ValueError) as exc:
        result = _make_review_verify_result(
            command_label,
            status="unavailable",
            exit_status="launch failed",
            captured_at=datetime.now(UTC),
            reviewed_branch=branch_name,
            reviewed_head_sha=reviewed_head_sha,
            reviewed_base_sha=reviewed_base_sha,
            working_directory=str(provider_cwd),
            failure=f"unable to prepare or run verify_command for no-op improve: {exc}",
        )
        markdown = _format_review_verify_result(result)

    _capture_review_verify_result(
        config,
        store,
        task,
        result,
        markdown=markdown,
        project_results=project_results,
        task_logger=task_logger,
    )
    return result


def _is_recovered_rebase_lineage(task: Task, *, resume: bool) -> bool:
    """Return whether rebase diff classification must fail closed for this run."""
    if task.task_type != "rebase":
        return False
    if resume:
        return True
    return task.recovery_origin in {"resume", "retry"}


def _is_recovered_improve_lineage(task: Task, *, resume: bool) -> bool:
    """Return whether improve diff classification must fail open for this run."""
    if task.task_type != "improve":
        return False
    if resume:
        return True
    return task.recovery_origin in {"resume", "retry"}


def _normalize_repeated_blocker_text(text: str) -> str:
    """Normalize blocker text for repeated-fix matching."""
    return " ".join(text.split()).strip().lower()


def _extract_blocker_signal_lines(blocker_body: str) -> list[str]:
    """Extract potential blocker-fix signal lines from canonical blocker body text."""
    signals: list[str] = []
    for raw_line in blocker_body.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        lowered = stripped.lower()
        if lowered.startswith("required tests:") or lowered.startswith("recommended tests:"):
            continue
        if lowered.startswith("evidence:") or lowered.startswith("impact:"):
            continue
        if ":" in stripped:
            _, value = stripped.split(":", 1)
            if value.strip():
                signals.append(value.strip())
                continue
        signals.append(stripped)
    return signals


def _extract_required_fix_candidates(content: str) -> dict[str, str]:
    """Extract blocker/fix candidates from parsed review markdown plus legacy fallbacks."""
    candidates: dict[str, str] = {}

    parsed = parse_review_report(content)
    for finding in parsed.findings:
        if finding.severity != "BLOCKER":
            continue

        signals: list[str] = []
        if finding.fix_or_followup:
            signals.append(finding.fix_or_followup)
        signals.extend(_extract_blocker_signal_lines(finding.body))
        signals.append(finding.title)

        for signal in signals:
            normalized = _normalize_repeated_blocker_text(signal)
            if normalized:
                candidates.setdefault(normalized, signal.strip())

    for match in re.finditer(r"(?im)^Required fix:\s*(.+)$", content):
        signal = match.group(1).strip()
        normalized = _normalize_repeated_blocker_text(signal)
        if normalized:
            candidates.setdefault(normalized, signal)

    return candidates


def _extract_repeated_required_fixes(reviews: list[Task], project_dir: Path) -> list[str]:
    """Extract repeated blockers from the most recent completed reviews."""
    if len(reviews) < 2:
        return []

    required_by_review: list[dict[str, str]] = []
    for review in reviews:
        content = _get_task_output(review, project_dir) or ""
        required_by_review.append(_extract_required_fix_candidates(content))

    repeated_keys = set(required_by_review[0]).intersection(required_by_review[1])
    repeated = [required_by_review[0][key] for key in repeated_keys]
    return sorted(repeated, key=str.lower)


def _extract_failure_context(task: Task, project_dir: Path) -> str:
    """Return a compact failed-attempt context block for fix rescue prompts."""
    lines: list[str] = []
    if task.failure_reason:
        lines.append(f"failure_reason={task.failure_reason}")
    if task.log_file:
        log_path = project_dir / Path(task.log_file)
        if log_path.exists():
            try:
                tail = log_path.read_text(encoding="utf-8", errors="replace").strip().splitlines()
            except OSError:
                tail = []
            if tail:
                lines.extend(tail[-20:])
    if not lines:
        return "(no failed-attempt context available)"
    return "\n".join(lines)


def _run_result_to_stats(result: RunResult) -> TaskStats:
    """Convert a provider RunResult to TaskStats for storage."""
    return TaskStats(
        duration_seconds=result.duration_seconds,
        num_steps_reported=result.num_steps_reported,
        num_steps_computed=result.num_steps_computed,
        num_turns_reported=result.num_turns_reported,
        num_turns_computed=result.num_turns_computed,
        cost_usd=result.cost_usd,
        input_tokens=result.input_tokens,
        output_tokens=result.output_tokens,
        tokens_estimated=result.tokens_estimated,
        cost_estimated=result.cost_estimated,
    )


def _load_transcript_stats(
    log_file: Path,
    *,
    provider_name: str,
    configured_model: str | None,
) -> TaskStats | None:
    """Replay a provider transcript through the shared log renderer for stats."""
    provider = provider_name.strip().lower()
    try:
        renderer = get_log_renderer(provider, configured_model=configured_model, verbose=False)
    except UnknownLogProviderError:
        return None

    saw_entries = False
    for entry in _load_jsonl_entries(log_file):
        saw_entries = True
        renderer.handle_log(entry, live=False)

    if not saw_entries:
        return None

    rendered = renderer.stats
    if (
        rendered.step_count <= 0
        and rendered.input_tokens <= 0
        and rendered.output_tokens <= 0
        and rendered.cost_usd <= 0.0
    ):
        return None

    return TaskStats(
        num_steps_computed=rendered.step_count or None,
        num_steps_reported=rendered.step_count or None,
        cost_usd=rendered.cost_usd or None,
        input_tokens=rendered.input_tokens or None,
        output_tokens=rendered.output_tokens or None,
    )


def _apply_transcript_stats_fallback(
    result: RunResult,
    *,
    log_file: Path,
    provider_name: str,
    configured_model: str | None,
    prefer_transcript_usage: bool = False,
) -> bool:
    """Hydrate missing or partial run stats from the provider transcript."""
    transcript_stats = _load_transcript_stats(
        log_file,
        provider_name=provider_name,
        configured_model=configured_model,
    )
    if transcript_stats is None:
        return False

    updated = False

    transcript_steps = transcript_stats.num_steps_computed or transcript_stats.num_steps_reported or 0
    result_steps = result.num_steps_computed or result.num_steps_reported or 0
    if transcript_steps > result_steps:
        result.num_steps_computed = transcript_steps
        result.num_steps_reported = transcript_steps
        updated = True

    transcript_input = transcript_stats.input_tokens or 0
    transcript_output = transcript_stats.output_tokens or 0
    transcript_total = transcript_input + transcript_output
    result_input = result.input_tokens or 0
    result_output = result.output_tokens or 0
    result_total = result_input + result_output

    should_replace_usage = transcript_total > 0 and (
        prefer_transcript_usage
        or result_total <= 0
        or result.tokens_estimated
        or result.cost_estimated
        or transcript_total > result_total
        or (result.cost_usd or 0.0) <= 0.0
    )
    if should_replace_usage:
        result.input_tokens = transcript_stats.input_tokens
        result.output_tokens = transcript_stats.output_tokens
        result.cost_usd = transcript_stats.cost_usd
        result.tokens_estimated = False
        result.cost_estimated = False
        updated = True

    return updated


def _checkpoint_dir(config: Config) -> Path:
    """Return the directory used for timeout resume checkpoints."""
    path = config.project_dir / f".{APP_NAME}" / "checkpoints"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _checkpoint_path(config: Config, task_id: str) -> Path:
    """Return the timeout resume checkpoint path for a task id."""
    return _checkpoint_dir(config) / f"{task_id}.json"


def _checked_git_probe_output(git: Git, *args: str) -> str:
    """Return git probe stdout or raise when the probe does not complete safely."""
    result = git._run(*args, check=False)
    if result.returncode != 0:
        error_output = result.stderr if isinstance(result.stderr, str) else ""
        if not error_output:
            error_output = result.stdout if isinstance(result.stdout, str) else ""
        raise GitError(f"git {' '.join(args)} failed:\n{error_output}")
    return result.stdout if isinstance(result.stdout, str) else ""


def _compute_tree_fingerprint(git: Git) -> str | None:
    """Compute a conservative tree fingerprint for checkpoint reuse decisions."""
    head_sha = git.rev_parse_if_exists("HEAD")
    if not isinstance(head_sha, str) or not head_sha:
        head_sha = "missing-head"

    try:
        staged_diff = _checked_git_probe_output(
            git,
            "diff",
            "--cached",
            "--binary",
            "--no-ext-diff",
            "--submodule=diff",
        )
        unstaged_diff = _checked_git_probe_output(
            git,
            "diff",
            "--binary",
            "--no-ext-diff",
            "--submodule=diff",
        )
        raw_untracked = _checked_git_probe_output(
            git,
            "ls-files",
            "--others",
            "--exclude-standard",
            "-z",
        )
    except GitError as exc:
        logger.warning(
            "Warning: failed to compute exact tree fingerprint for timeout resume checkpoints: %s",
            exc,
        )
        return None

    untracked_entries: list[str] = []
    untracked_paths = sorted(path for path in raw_untracked.split("\0") if path)
    for relative_path in untracked_paths:
        untracked_path = git.repo_dir / relative_path
        try:
            digest = sha256(untracked_path.read_bytes()).hexdigest()
        except OSError as exc:
            logger.warning(
                "Warning: failed to read untracked file '%s' for timeout resume fingerprinting: %s",
                relative_path,
                exc,
            )
            return None
        untracked_entries.append(f"{relative_path}\0{digest}")

    payload = "\n".join(
        [
            f"head={head_sha}",
            "staged_diff:",
            staged_diff,
            "unstaged_diff:",
            unstaged_diff,
            "untracked_files:",
            "\n".join(untracked_entries),
        ]
    )
    return sha256(payload.encode("utf-8")).hexdigest()


def _load_jsonl_entries(path: Path) -> list[dict[str, Any]]:
    """Load dict entries from a JSONL file, skipping invalid lines."""
    entries: list[dict[str, Any]] = []
    try:
        with open(path) as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(entry, dict):
                    entries.append(entry)
    except OSError:
        return []
    return entries


def _extract_last_command_context(log_file: Path) -> dict[str, Any] | None:
    """Extract the last runnable agent shell command from conversation logs."""
    last_command: dict[str, Any] | None = None
    for entry in _load_jsonl_entries(log_file):
        event_type = entry.get("type")
        if event_type == "item.started":
            item = entry.get("item") or {}
            if isinstance(item, dict) and item.get("type") == "command_execution":
                command = item.get("command")
                if isinstance(command, str) and command.strip():
                    last_command = {"command": command.strip(), "status": "running"}
        elif event_type == "item.completed":
            item = entry.get("item") or {}
            if isinstance(item, dict) and item.get("type") == "command_execution":
                command = item.get("command")
                if isinstance(command, str) and command.strip():
                    last_command = {
                        "command": command.strip(),
                        "status": "completed",
                        "exit_code": item.get("exit_code"),
                    }
        elif event_type == "tool_call":
            tool_name = entry.get("tool_name")
            tool_input = entry.get("tool_input") or {}
            if tool_name == "Bash" and isinstance(tool_input, dict):
                command = tool_input.get("command")
                if isinstance(command, str) and command.strip():
                    last_command = {"command": command.strip(), "status": "started"}

    return last_command


def _extract_provider_exec_breadcrumb(log_file: Path) -> dict[str, Any] | None:
    """Extract the outer provider wrapper command as a non-runnable breadcrumb."""
    ops_log = ops_log_path_for(log_file)
    for entry in reversed(_load_jsonl_entries(ops_log)):
        if entry.get("event") == "provider_exec_start":
            command = entry.get("command")
            if isinstance(command, str) and command.strip():
                return {"command": command.strip(), "status": "provider_exec_start"}
    return None


_VERIFY_PHASE_RESULT_RE = re.compile(
    r"^gza-verify phase=(?P<status>passed|failed) name=(?P<name>[A-Za-z0-9_.-]+) duration_seconds=(?P<duration>[0-9.]+)"
    r"(?: tree_fingerprint=(?P<tree_fingerprint>[0-9a-f]{64}))?$"
)


def _iter_command_output_lines(log_file: Path, ops_log_file: Path) -> Iterator[tuple[str, Any]]:
    """Yield command-output lines from provider logs and fallback ops logs."""
    for entry in _load_jsonl_entries(log_file):
        event_type = entry.get("type")
        if event_type == "item.completed":
            item = entry.get("item") or {}
            if not isinstance(item, dict) or item.get("type") != "command_execution":
                continue
            item_output = item.get("aggregated_output")
            if not isinstance(item_output, str):
                item_output = item.get("output")
            if not isinstance(item_output, str):
                continue
            for line in item_output.splitlines():
                stripped = line.strip()
                if stripped:
                    yield stripped, entry.get("timestamp")
            continue

        if event_type not in {"tool_output", "tool_error"}:
            continue
        payload = entry.get("payload")
        tool_output: Any = None
        if isinstance(payload, dict):
            tool_output = payload.get("output")
        if not isinstance(tool_output, str):
            tool_output = entry.get("output")
        if not isinstance(tool_output, str):
            tool_output = entry.get("content")
        if not isinstance(tool_output, str):
            continue
        for line in tool_output.splitlines():
            stripped = line.strip()
            if stripped:
                yield stripped, entry.get("timestamp")

    for entry in _load_jsonl_entries(ops_log_file):
        if entry.get("source") != "provider" or entry.get("subtype") != "process_output":
            continue
        message = entry.get("provider_output") or entry.get("message")
        if not isinstance(message, str):
            continue
        stripped = message.strip()
        if stripped:
            yield stripped, entry.get("timestamp")


def _extract_verify_phase_checkpoints(log_file: Path, ops_log_file: Path) -> list[dict[str, Any]]:
    """Extract successful verify phase checkpoints from provider and ops output."""
    checkpoints: list[dict[str, Any]] = []
    for message, timestamp in _iter_command_output_lines(log_file, ops_log_file):
        match = _VERIFY_PHASE_RESULT_RE.match(message)
        if not match:
            continue
        if match.group("status") != "passed":
            continue
        phase_fingerprint = match.group("tree_fingerprint")
        if not phase_fingerprint:
            continue
        checkpoints.append(
            {
                "phase": match.group("name"),
                "status": "passed",
                "duration_seconds": float(match.group("duration")),
                "completed_at": timestamp,
                "tree_fingerprint": phase_fingerprint,
            }
        )
    return checkpoints


def _apply_timeout_resume_fingerprint_aliases(
    *,
    verify_phases: list[dict[str, Any]],
    pre_save_tree_fingerprint: str | None,
    resume_tree_fingerprint: str,
    wip_state: str,
) -> list[dict[str, Any]]:
    """Translate exact-tree checkpoints onto the saved WIP commit tree when safe."""
    if (
        pre_save_tree_fingerprint is None
        or pre_save_tree_fingerprint == resume_tree_fingerprint
        or wip_state not in {"commit", "commit+diff"}
    ):
        return verify_phases

    aliased_phases: list[dict[str, Any]] = []
    for phase in verify_phases:
        if not isinstance(phase, dict):
            continue
        updated_phase = dict(phase)
        if updated_phase.get("tree_fingerprint") == pre_save_tree_fingerprint:
            updated_phase["resume_tree_fingerprint"] = resume_tree_fingerprint
        aliased_phases.append(updated_phase)
    return aliased_phases


def _write_timeout_resume_checkpoint(
    *,
    config: Config,
    task_id: str,
    log_file: Path,
    worktree_git: Git,
    wip_state: str,
    pre_save_tree_fingerprint: str | None = None,
) -> None:
    """Persist timeout resume context for later resume prompts."""
    tree_fingerprint = _compute_tree_fingerprint(worktree_git)
    verify_phases = _apply_timeout_resume_fingerprint_aliases(
        verify_phases=_extract_verify_phase_checkpoints(log_file, ops_log_path_for(log_file)),
        pre_save_tree_fingerprint=pre_save_tree_fingerprint,
        resume_tree_fingerprint=tree_fingerprint or "",
        wip_state=wip_state,
    )
    payload = {
        "task_id": task_id,
        "updated_at": _ops_timestamp(),
        "tree_fingerprint": tree_fingerprint,
        "tree_fingerprint_available": tree_fingerprint is not None,
        "conversation_log_file": str(log_file),
        "ops_log_file": str(ops_log_path_for(log_file)),
        "last_command": _extract_last_command_context(log_file),
        "provider_exec_breadcrumb": _extract_provider_exec_breadcrumb(log_file),
        "verify_phases": verify_phases,
        "wip_state": wip_state,
    }
    _checkpoint_path(config, task_id).write_text(json.dumps(payload, indent=2, sort_keys=True))


def _persist_timeout_resume_checkpoint(
    *,
    config: Config,
    task_id: str,
    log_file: Path,
    worktree_git: Git,
    wip_state: str,
    task_logger: TaskExecutionLogger,
    pre_save_tree_fingerprint: str | None = None,
) -> None:
    """Write timeout resume context without blocking canonical failure recording."""
    try:
        _write_timeout_resume_checkpoint(
            config=config,
            task_id=task_id,
            log_file=log_file,
            worktree_git=worktree_git,
            wip_state=wip_state,
            pre_save_tree_fingerprint=pre_save_tree_fingerprint,
        )
    except (OSError, TypeError, ValueError) as exc:
        warning = (
            "Warning: failed to persist timeout resume checkpoint; "
            f"continuing with TIMEOUT failure recording: {exc}"
        )
        logger.warning(warning)
        task_logger.warning(
            warning,
            extra={
                "event": "timeout_resume_checkpoint_write_failed",
                "task_id": task_id,
                "error_type": type(exc).__name__,
            },
        )


def _build_timeout_resume_context(
    *,
    config: Config,
    checkpoint_task_id: str | None,
    worktree_git: Git,
) -> str | None:
    """Build timeout-aware resume guidance when a reusable checkpoint exists."""
    if checkpoint_task_id is None:
        return None
    checkpoint_file = _checkpoint_path(config, checkpoint_task_id)
    if not checkpoint_file.exists():
        return None

    def checkpoint_unavailable_context(reason: str) -> str:
        warning = (
            "Warning: timeout resume checkpoint exists but could not be reused; "
            f"continuing without reusable verify phases: {reason}"
        )
        logger.warning(warning)
        return "\n".join(
            [
                "## Timeout Resume Context",
                "",
                "- Timeout checkpoint context was unavailable from the interrupted run.",
                f"- Reason: {reason}",
                "- No reusable verify phases should be trusted from the interrupted run.",
                "- First command to run next: inspect the current worktree, rerun targeted or inner verification as needed, and only rerun the full final verify command once the tree is ready.",
                "- Any new edit invalidates prior verify checkpoints for affected phases.",
            ]
        )

    try:
        payload = json.loads(checkpoint_file.read_text())
    except OSError as exc:
        return checkpoint_unavailable_context(
            f"failed to read checkpoint file `{checkpoint_file}`: {exc}"
        )
    except json.JSONDecodeError as exc:
        return checkpoint_unavailable_context(
            f"failed to parse checkpoint file `{checkpoint_file}` as JSON: {exc}"
        )
    if not isinstance(payload, dict):
        return checkpoint_unavailable_context(
            f"checkpoint file `{checkpoint_file}` did not contain a JSON object"
        )

    current_fingerprint = _compute_tree_fingerprint(worktree_git)
    reusable_phases: list[dict[str, Any]] = []
    verify_phases = payload.get("verify_phases")
    current_fingerprint_available = isinstance(current_fingerprint, str) and bool(current_fingerprint)
    if current_fingerprint_available and isinstance(verify_phases, list):
        reusable_phases = [
            phase
            for phase in verify_phases
            if isinstance(phase, dict)
            and (
                phase.get("tree_fingerprint") == current_fingerprint
                or phase.get("resume_tree_fingerprint") == current_fingerprint
            )
        ]

    lines = ["## Timeout Resume Context", ""]
    last_command = payload.get("last_command")
    if isinstance(last_command, dict) and isinstance(last_command.get("command"), str):
        command_status = last_command.get("status") or "unknown"
        exit_code = last_command.get("exit_code")
        exit_detail = f", exit={exit_code}" if exit_code is not None else ""
        lines.append(
            f"- Last known command: `{last_command['command']}` (status: {command_status}{exit_detail})"
        )
    provider_exec_breadcrumb = payload.get("provider_exec_breadcrumb")
    if isinstance(provider_exec_breadcrumb, dict) and isinstance(provider_exec_breadcrumb.get("command"), str):
        lines.append(
            "- Provider wrapper at timeout: "
            f"`{provider_exec_breadcrumb['command']}` "
            "(breadcrumb only; do not rerun it inside the resumed agent session)"
        )

    wip_state = payload.get("wip_state")
    if isinstance(wip_state, str):
        lines.append(f"- Saved WIP state: {wip_state}")

    if reusable_phases:
        phase_names = ", ".join(str(phase.get("phase")) for phase in reusable_phases if phase.get("phase"))
        lines.append(
            f"- Reusable successful verify phases for the current tree fingerprint: {phase_names}"
        )
        lines.append("- If you do not edit the tree before resuming verification, treat those phases as already passed.")
    elif not current_fingerprint_available:
        lines.append(
            "- No reusable verify checkpoints are valid because exact-tree fingerprinting failed for the current worktree."
        )
    else:
        lines.append("- No reusable verify checkpoints are valid for the current tree fingerprint.")

    next_command = None
    if reusable_phases:
        next_command = "continue from the first unfinished verification phase or rerun the final verify command if only the last phase was interrupted"
    elif isinstance(last_command, dict) and isinstance(last_command.get("command"), str):
        next_command = last_command["command"]
    if next_command:
        lines.append(f"- First command to run next: {next_command}")
    else:
        lines.append(
            "- First command to run next: unknown; inspect the current worktree and verification state, then continue from the interrupted step without relaunching the provider wrapper."
        )
    lines.append("- Any new edit invalidates prior verify checkpoints for affected phases.")
    return "\n".join(lines)


def _save_wip_changes(
    task: Task,
    worktree_git: Git,
    config: Config,
    branch_name: str,
) -> str:
    """Save WIP changes when task fails or is interrupted.

    This does two things:
    1. Commits any uncommitted changes with --no-verify
    2. Backs up the diff to .gza/wip/<task-id>.diff

    Args:
        task: The task that failed/was interrupted
        worktree_git: Git instance for the worktree
        config: Configuration object
        branch_name: Name of the branch with the WIP changes
    """
    # Check if there are any changes to save
    if not worktree_git.has_changes("."):
        return "none"

    # Create WIP directory
    wip_dir = config.project_dir / WIP_DIR
    wip_dir.mkdir(parents=True, exist_ok=True)

    # Stage tracked modifications/deletions only (avoid staging unrelated files)
    worktree_git._run("add", "--update", ".", check=False)
    # Also stage any new untracked files (agent-created files)
    untracked = worktree_git._run("ls-files", "--others", "--exclude-standard", check=False).stdout
    for f in untracked.splitlines():
        if f.strip():
            worktree_git.add(f.strip())
    diff = worktree_git._run("diff", "--cached", check=False).stdout

    # Save diff to backup file
    saved_diff = False
    if task.slug and diff:
        wip_file = wip_dir / f"{task.slug}.diff"
        wip_file.write_text(diff)
        saved_diff = True
        console.print(f"[yellow]Saved WIP diff to: {wip_file.relative_to(config.project_dir)}[/yellow]")

    # Commit changes with --no-verify
    try:
        worktree_git._run(
            "commit",
            "--no-verify",
            "-m",
            f"{WIP_INTERRUPTED_COMMIT_SUBJECT}\n\nTask ID: {task.slug}",
        )
        console.print(f"[yellow]Saved WIP commit on branch: {branch_name}[/yellow]")
        return "commit+diff" if saved_diff else "commit"
    except GitError as e:
        # If commit fails, that's okay - we have the diff backup
        console.print(f"[yellow]Warning: Could not create WIP commit: {e}[/yellow]")
        return "diff" if saved_diff else "none"


def _restore_wip_changes(
    task: Task,
    worktree_git: Git,
    config: Config,
    branch_name: str,
    original_task_id: str | None = None,
) -> None:
    """Restore WIP changes when resuming a task.

    Checks if the branch has a WIP commit. If not, tries to apply the
    stored diff from .gza/wip/<task-id>.diff.

    Args:
        task: The task being resumed
        worktree_git: Git instance for the worktree
        config: Configuration object
        branch_name: Name of the branch to restore WIP changes to
        original_task_id: Optional task_id of the original failed task (for
            finding the WIP diff file when resuming via a new task).
    """
    if not task.slug and not original_task_id:
        return
    boundary = _project_boundary(config)

    # Check if the last commit is a WIP commit
    try:
        last_commit_msg = worktree_git._run("log", "-1", "--pretty=%B", check=False).stdout.strip()
        if last_commit_msg.startswith(WIP_INTERRUPTED_COMMIT_SUBJECT):
            console.print("[green]Found WIP commit on branch - resuming from there[/green]")
            return
    except GitError:
        pass

    # No WIP commit found - try to apply stored diff.
    # When resuming via a new task, the WIP diff was saved with the original
    # task's id, so check that first, then fall back to the new task's id.
    wip_dir = config.project_dir / WIP_DIR
    wip_file = None
    for candidate_id in filter(None, [original_task_id, task.slug]):
        candidate = wip_dir / f"{candidate_id}.diff"
        if candidate.exists():
            wip_file = candidate
            break

    if wip_file and wip_file.exists():
        diff_content = wip_file.read_text()
        if diff_content.strip():
            diff_content, stripped_paths = _strip_owned_artifact_patch_sections(
                diff_content,
                boundary=boundary,
            )
            if stripped_paths:
                console.print(
                    "[yellow]Ignoring gza-owned artifact paths while restoring WIP diff: "
                    f"{', '.join(stripped_paths)}[/yellow]"
                )
            if not diff_content.strip():
                console.print("[yellow]Stored WIP diff only touched gza-owned artifacts; nothing to restore[/yellow]")
                return
            console.print(f"[yellow]WIP commit not found - applying stored diff from {wip_file.relative_to(config.project_dir)}[/yellow]")
            try:
                # Apply the diff
                result = worktree_git._run("apply", "--cached", stdin=diff_content.encode(), check=False)
                if result.returncode == 0:
                    # Commit the restored changes
                    worktree_git._run("commit", "--no-verify", "-m", f"WIP: restored from diff\n\nTask ID: {task.slug}")
                    console.print("[green]Successfully restored WIP changes from diff[/green]")
                else:
                    console.print(f"[yellow]Warning: Could not apply WIP diff: {result.stderr}[/yellow]")
            except GitError as e:
                console.print(f"[yellow]Warning: Could not apply WIP diff: {e}[/yellow]")


def _squash_wip_commits(
    worktree_git: Git,
    task: Task,
) -> None:
    """Squash WIP commits into the final commit.

    If there are WIP commits on the branch, this will squash them
    into the final task commit before marking the task complete.

    Args:
        worktree_git: Git instance for the worktree
        task: The task being completed
    """
    # Check if there are any WIP commits to squash
    try:
        # Look for WIP commits in the recent history
        log_output = worktree_git._run("log", "-10", "--pretty=%s", check=False).stdout.strip()
        if not log_output:
            return

        commit_messages = log_output.split("\n")
        wip_count = sum(1 for msg in commit_messages if msg.startswith("WIP:"))

        if wip_count == 0:
            return

        console.print(f"[yellow]Found {wip_count} WIP commit(s) - squashing into final commit[/yellow]")

        # Use git reset --soft to squash commits
        # Reset back to before the WIP commits, keeping all changes staged
        worktree_git._run("reset", "--soft", f"HEAD~{wip_count}")

        console.print("[green]WIP commits squashed successfully[/green]")

    except GitError as e:
        # If squashing fails, log but continue - the WIP commits will remain
        console.print(f"[yellow]Warning: Could not squash WIP commits: {e}[/yellow]")


def post_review_to_pr(
    review_task: Task,
    impl_task: Task,
    store: SqliteTaskStore,
    project_dir: Path,
    *,
    pr_integration: bool = True,
    required: bool = False,
) -> None:
    """Post a review task's output to its associated PR.

    Args:
        review_task: The review task
        impl_task: The implementation task being reviewed
        store: Task store
        project_dir: Project directory
        required: If True, error if PR not found; if False, skip silently
    """
    if not pr_integration:
        if required:
            print("Info: PR requested but skipped: PR integration disabled by project config")
        return

    gh = GitHub()
    if gh.cached_pr_support() is False:
        if required:
            print("Info: PR requested but skipped: project has no GitHub-capable remote")
        return

    # Check gh is available
    if not gh.is_available():
        if required:
            print("Error: GitHub CLI not available, cannot post review")
            return
        else:
            print("Info: GitHub CLI not available, skipping PR comment")
            return

    # Find an open PR, preferring cached metadata but falling back to branch lookup.
    pr_number = None
    if impl_task.branch:
        try:
            resolved_pr = resolve_branch_pr(
                gh,
                impl_task.branch,
                cached_pr_numbers=((impl_task.pr_number,) if impl_task.pr_number is not None else ()),
                allow_discovery=True,
            )
        except GitHubError as exc:
            if is_github_repo_unsupported_error(exc):
                if required:
                    print("Info: PR requested but skipped: project has no GitHub-capable remote")
                return
            if required:
                print(f"Error: Failed to look up PR for task {impl_task.id}: {exc}")
            else:
                print(f"Info: Failed to look up PR for task {impl_task.id}, skipping PR comment: {exc}")
            return
        if resolved_pr.details is not None and resolved_pr.details.state == "open":
            pr_number = resolved_pr.details.number
            impl_task.pr_number = resolved_pr.details.number
            impl_task.pr_state = resolved_pr.details.state
            impl_task.pr_last_synced_at = datetime.now(UTC)
            store.update(impl_task)
            if resolved_pr.source == "cached":
                print(f"Found PR #{pr_number} (cached)")
            else:
                print(f"Found PR #{pr_number} for branch {impl_task.branch}")

    if not pr_number:
        if required:
            print(f"Error: No PR found for task {impl_task.id}")
            if impl_task.branch:
                print(f"Branch '{impl_task.branch}' has no associated PR")
            else:
                print("Task has no branch")
            return
        else:
            print(f"Info: No PR found for task {impl_task.id}, skipping PR comment")
            return

    # Get review content
    review_content = _get_task_output(review_task, project_dir)
    if not review_content:
        print(f"Warning: Review task {review_task.id} has no output content")
        return

    # Format as PR comment
    comment_body = f"""## 🤖 Automated Code Review

**Review Task**: {review_task.id}
**Implementation Task**: {impl_task.id}

---

{review_content}

---

*Generated by `gza review` task*
"""

    # Post to PR
    try:
        gh.add_pr_comment(pr_number, comment_body)
        print(f"✓ Posted review to PR #{pr_number}")
    except GitHubError as e:
        print(f"Warning: Failed to post review to PR: {e}")


def _create_and_run_review_task(
    completed_task: Task,
    config: Config,
    store: SqliteTaskStore,
) -> int:
    """Create and immediately execute a review task for a completed implementation.

    Returns:
        Exit code from running the review task.
    """
    review_target = completed_task
    if completed_task.task_type == "improve":
        resolved_impl = _resolve_impl_ancestor(store, completed_task)
        if resolved_impl is None:
            console.print(
                f"\n[yellow]Could not resolve the implementation ancestor for improve task {completed_task.id}; "
                "skipping auto-review.[/yellow]"
            )
            return 0
        review_target = resolved_impl
    elif completed_task.task_type == "fix":
        resolved_impl = _resolve_root_implementation_for_fix(completed_task, store)
        if resolved_impl is None:
            console.print(
                f"\n[yellow]Could not resolve the implementation ancestor for fix task {completed_task.id}; "
                "skipping auto-review.[/yellow]"
            )
            return 0
        review_target = resolved_impl
    elif completed_task.task_type == "rebase":
        resolved_impl = _resolve_impl_ancestor(store, completed_task)
        if resolved_impl is None:
            console.print(
                f"\n[yellow]Could not resolve the implementation ancestor for rebase task {completed_task.id}; "
                "skipping auto-review.[/yellow]"
            )
            return 0
        review_target = resolved_impl

    try:
        review_task = create_review_task(
            store, review_target, trigger_source="auto-recovery", prompt_mode="auto",
            project_prefix=config.project_prefix or None,
        )
    except DuplicateReviewError as e:
        review_task = e.active_review
        if review_task.status == "in_progress":
            console.print(
                f"\n[yellow]Review task {review_task.id} is already in progress; skipping.[/yellow]"
            )
            return 0
        console.print(
            f"\n[yellow]Review task {review_task.id} is already {review_task.status}; running it.[/yellow]"
        )

    console.print(f"\n[bold cyan]=== Auto-created review task {review_task.id} ===[/bold cyan]")
    console.print("Running review task...")

    # Run the review task immediately
    # Note: PR posting happens in _run_non_code_task, no need to do it here
    return run(config, task_id=review_task.id)


def _sync_completed_code_task_branch_for_live_pr(
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    git: Git,
) -> bool:
    """Best-effort sync for same-branch code tasks that already have an open PR.

    Returns False only when follow-up review should be held until the branch is
    published. Lookup or `gh` availability gaps preserve the historical
    auto-review flow because no PR-facing action can be taken anyway.
    """
    task_label = f"{task.task_type.capitalize()} task {task.id}"
    result = sync_task_branch_if_live_pr(task, store, git, pr_integration=config.pr_integration)
    if result.ok or result.status in {"gh_unavailable", "disabled", "unsupported"}:
        return True

    if result.status == "lookup_failed":
        print(
            f"Warning: {task_label} completed, but gza could not look up a live PR for "
            f"branch '{task.branch}': {result.error}. Continuing with auto-review without PR sync."
        )
        return True
    if result.status == "push_failed":
        pr_ref = f"PR #{result.pr_number}" if result.pr_number is not None else "the live PR"
        print(
            f"Warning: {task_label} completed, but branch '{task.branch}' could not be "
            f"pushed to {pr_ref}: {result.error}"
        )
    else:
        print(
            f"Warning: {task_label} completed, but branch '{task.branch}' could not be "
            "synchronized for follow-up PR actions."
        )
    return False


def _ensure_work_pr_for_completed_code_task(
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    git: Git,
) -> CompletedCodeTaskPrPublicationOutcome:
    """Ensure a PR exists for a completed code task branch when `gza work --pr` is set.

    Returns a runner-owned publication outcome that distinguishes non-fatal
    missing-PR states from branch publication failures.
    """
    if not task.branch:
        return CompletedCodeTaskPrPublicationOutcome(
            kind="ready",
            status="no_branch",
            message=f"Info: Task {task.id} has no branch, skipping PR creation",
        )

    default_branch = git.default_branch()
    if git.count_commits_ahead(task.branch, default_branch) <= 0:
        message = f"Info: Task {task.id} has no commits on branch '{task.branch}', skipping PR creation"
        print(message)
        return CompletedCodeTaskPrPublicationOutcome(
            kind="ready",
            status="no_commits",
            message=message,
        )

    result = ensure_task_pr(
        task,
        store,
        git,
        pr_integration=config.pr_integration,
        content_builder=lambda: build_task_pr_content(task, git, config, store),
        draft=False,
        merged_behavior="skip",
    )
    if result.ok and result.status == "disabled":
        print("Info: PR requested but skipped: PR integration disabled by project config")
        return CompletedCodeTaskPrPublicationOutcome(
            kind="ready",
            status=result.status,
            message="Info: PR requested but skipped: PR integration disabled by project config",
            error=result.error,
        )
    if result.ok and result.status == "unsupported":
        print("Info: PR requested but skipped: project has no GitHub-capable remote")
        return CompletedCodeTaskPrPublicationOutcome(
            kind="ready",
            status=result.status,
            message="Info: PR requested but skipped: project has no GitHub-capable remote",
            error=result.error,
        )
    if result.ok and result.status == "cached" and result.pr_number:
        message = f"Info: Reusing cached PR #{result.pr_number} for task {task.id}: {result.pr_url}"
        print(message)
        return CompletedCodeTaskPrPublicationOutcome(
            kind="ready",
            status=result.status,
            message=message,
            error=result.error,
        )
    if result.ok and result.status == "existing":
        message = f"Info: Reusing existing PR for branch {task.branch}: {result.pr_url}"
        print(message)
        return CompletedCodeTaskPrPublicationOutcome(
            kind="ready",
            status=result.status,
            message=message,
            error=result.error,
        )
    if result.ok and result.status == "merged":
        message = f"Info: Branch '{task.branch}' is already merged into {default_branch}, skipping PR creation"
        print(message)
        return CompletedCodeTaskPrPublicationOutcome(
            kind="ready",
            status=result.status,
            message=message,
            error=result.error,
        )
    if result.ok and result.status == "created":
        message = f"✓ Created PR: {result.pr_url}"
        print(message)
        return CompletedCodeTaskPrPublicationOutcome(
            kind="ready",
            status=result.status,
            message=message,
            error=result.error,
        )
    if result.status in {"gh_unavailable", "lookup_failed"}:
        push_error = _ensure_completed_task_branch_is_published(task=task, git=git)
        if push_error is not None:
            return CompletedCodeTaskPrPublicationOutcome(
                kind="branch_unpushable",
                status="push_failed",
                message=(
                    f"Error: Task {task.id} completed locally, but branch '{task.branch}' "
                    "could not be pushed after PR publication setup failed"
                    f" ({result.status})"
                    f": {push_error}"
                ),
                error=push_error,
            )
    if result.status == "push_failed":
        return CompletedCodeTaskPrPublicationOutcome(
            kind="branch_unpushable",
            status=result.status,
            message=(
                f"Error: Task {task.id} completed locally, but branch '{task.branch}' "
                f"could not be pushed ({result.status})"
                f"{f': {result.error}' if result.error else ''}"
            ),
            error=result.error,
        )

    return CompletedCodeTaskPrPublicationOutcome(
        kind="nonfatal_missing_pr",
        status=result.status,
        message=(
            f"Warning: Task {task.id} completed and branch '{task.branch}' is published to origin, "
            f"but PR was not created ({result.status})"
            f"{f': {result.error}' if result.error else ''}"
        ),
        error=result.error,
    )


def _copy_learnings_to_worktree(config: Config, worktree_path: Path) -> None:
    """Copy .gza/learnings.md into the worktree so the agent can read it.

    The learnings file lives in config.project_dir/.gza/ which is gitignored
    and not present in worktrees. The agent prompt references it as a relative
    path, so it must exist in the worktree for the agent to find it.
    """
    import shutil

    src = config.project_dir / ".gza" / "learnings.md"
    if not src.exists():
        return
    dst_dir = worktree_path / ".gza"
    dst_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst_dir / "learnings.md")


def _count_patch_hunks(patch_text: str) -> int:
    """Count unified-diff hunks in patch text."""
    return sum(1 for line in patch_text.splitlines() if line.startswith("@@"))


def _write_runtime_patch_file(bundle_dir: Path, filename: str, patch_text: str) -> Path:
    """Persist a runtime-generated patch alongside the copied extraction bundle."""
    patch_path = bundle_dir / filename
    patch_path.write_text(patch_text)
    return patch_path


_UNMERGED_PORCELAIN_STATUSES = frozenset({"DD", "AU", "UD", "UA", "DU", "AA", "UU"})


def _git_apply_failure_message(patch_path: Path, result: GitApplyResult) -> str:
    """Format a consistent error message for failed patch applications."""
    error_output = result.error_output
    return f"git apply --3way {patch_path} failed:\n{error_output}"


def _apply_left_relevant_conflicts(
    worktree_git: Git,
    touched_paths: set[str],
) -> bool:
    """Return True when `git apply --3way` left unmerged entries on seeded paths."""
    for status, path in worktree_git.status_porcelain():
        if path not in touched_paths:
            continue
        if status in _UNMERGED_PORCELAIN_STATUSES:
            return True
    return False


def _already_merged_extraction_seed_result(
    task: Task,
    log_file: Path,
    *,
    message: str,
) -> ExtractionSeedResult:
    """Log and return the canonical extraction already-merged completion outcome."""
    write_log_entry(
        log_file,
        {
            "type": "gza",
            "subtype": "info",
            "message": message,
            "completion_reason": EXTRACTION_ALREADY_MERGED_COMPLETION_REASON,
        },
    )
    return ExtractionSeedResult(
        completion_reason=EXTRACTION_ALREADY_MERGED_COMPLETION_REASON,
    )


def _seed_extraction_bundle_if_present(
    task: Task,
    config: Config,
    worktree_path: Path,
    worktree_git: Git,
    log_file: Path,
    *,
    resume: bool,
) -> ExtractionSeedResult:
    """Copy/apply extraction bundle before provider execution when configured for the task."""
    if resume or not task.slug:
        return ExtractionSeedResult()
    boundary = _project_boundary(config)

    project_bundle_dir = extraction_bundle_path(config.project_dir, task.slug)
    if not project_bundle_dir.exists():
        return ExtractionSeedResult()

    worktree_bundle_dir = copy_bundle_to_worktree(project_bundle_dir, worktree_path)
    manifest = load_manifest(worktree_bundle_dir / MANIFEST_FILENAME)
    manifest_target_slug = manifest.get("target_slug")
    manifest_target_task_id = manifest.get("target_task_id")
    if not isinstance(manifest_target_slug, str) or not manifest_target_slug:
        raise ExtractionError("Extraction manifest missing required target identity field: target_slug")
    if not isinstance(manifest_target_task_id, str) or not manifest_target_task_id:
        raise ExtractionError("Extraction manifest missing required target identity field: target_task_id")
    if manifest_target_slug != task.slug or manifest_target_task_id != task.id:
        raise ExtractionError(
            "Extraction bundle target identity mismatch "
            f"(manifest task={manifest_target_task_id} slug={manifest_target_slug}, "
            f"current task={task.id} slug={task.slug})"
        )

    patch_path = resolve_manifest_patch_path(
        worktree_bundle_dir,
        manifest.get("patch_path", PATCH_FILENAME),
    )

    patch_text = load_patch_text(patch_path)
    patch_text, stripped_patch_paths = _strip_owned_artifact_patch_sections(
        patch_text,
        boundary=boundary,
    )
    if stripped_patch_paths:
        patch_path = _write_runtime_patch_file(
            worktree_bundle_dir,
            "selected.runtime.patch",
            patch_text,
        )
    stored_touched_paths = parse_patch_touched_paths(patch_text)
    owned_manifest_paths: set[str] = set()
    if not stored_touched_paths and not stripped_patch_paths:
        raise ExtractionError("Extraction patch has no touched file paths")

    declared_raw = manifest.get("touched_paths")
    if declared_raw is None and "touched_paths" not in manifest:
        declared_raw = manifest.get("selected_paths", [])

    if not isinstance(declared_raw, (list, tuple)):
        raise ExtractionError("Extraction manifest selected/touched path declarations must be a list")

    declared_paths: set[str] = set()
    for path_value in declared_raw:
        if not isinstance(path_value, str) or not path_value:
            raise ExtractionError("Extraction manifest selected/touched paths must be non-empty strings")
        declared_paths.add(path_value)
    owned_manifest_paths.update(path for path in declared_paths if _is_gza_owned_path(path, boundary=boundary))
    declared_paths = _filter_owned_artifact_paths(declared_paths, boundary=boundary)

    if not declared_paths and not stripped_patch_paths:
        raise ExtractionError("Extraction manifest is missing selected/touched path declarations")

    unexpected = sorted(set(stored_touched_paths) - declared_paths)
    if unexpected:
        raise ExtractionError(
            "Extraction patch touches undeclared paths: " + ", ".join(unexpected),
        )

    selected_paths_raw = manifest.get("selected_paths")
    if not isinstance(selected_paths_raw, (list, tuple)) or not selected_paths_raw:
        raise ExtractionError("Extraction manifest selected_paths must be a non-empty list")
    if any(not isinstance(path_value, str) or not path_value for path_value in selected_paths_raw):
        raise ExtractionError("Extraction manifest selected_paths must contain non-empty strings")
    owned_manifest_paths.update(path for path in selected_paths_raw if _is_gza_owned_path(path, boundary=boundary))
    selected_paths = tuple(path for path in selected_paths_raw if not _is_gza_owned_path(path, boundary=boundary))

    excluded_owned_paths = sorted(set(stripped_patch_paths) | owned_manifest_paths)
    if excluded_owned_paths:
        write_log_entry(
            log_file,
            {
                "type": "gza",
                "subtype": "info",
                "message": "Ignored gza-owned artifact paths while preparing extraction seed",
                "excluded_paths": excluded_owned_paths,
            },
        )
    if not stored_touched_paths and excluded_owned_paths:
        return ExtractionSeedResult()
    if not selected_paths:
        return ExtractionSeedResult()

    stored_hunk_count = _count_patch_hunks(patch_text)
    source_branch = manifest.get("source_branch")
    source_base_ref = manifest.get("source_base_ref")
    source_commits_raw = manifest.get("source_commits", [])
    if source_commits_raw is None:
        source_commits_raw = []
    if not isinstance(source_commits_raw, (list, tuple)):
        raise ExtractionError("Extraction manifest source_commits must be a list when present")
    if any(not isinstance(value, str) or not value for value in source_commits_raw):
        raise ExtractionError("Extraction manifest source_commits must contain non-empty strings")
    source_commits = tuple(source_commits_raw)

    current_patch_text: str | None = None
    runtime_refresh_available = False
    source_context: dict[str, object]
    refresh_message: str

    if source_commits:
        missing_commits = [commit for commit in source_commits if not worktree_git.ref_exists(commit)]
        if not missing_commits:
            runtime_refresh_available = True
            current_patch_parts = [
                worktree_git.get_commit_patch_for_paths(commit, selected_paths, binary=True).rstrip("\n")
                for commit in source_commits
            ]
            current_patch_text = "\n".join(part for part in current_patch_parts if part).strip("\n")
            if current_patch_text:
                current_patch_text += "\n"
            source_context = {
                "source_commits": list(source_commits),
                "selected_paths": list(selected_paths),
            }
            refresh_message = (
                f"Extraction patch runtime refresh: re-derived hunks={{rederived}}, stored hunks={stored_hunk_count}"
            )
        else:
            source_context = {
                "source_commits": list(source_commits),
                "selected_paths": list(selected_paths),
            }
            refresh_message = (
                "Extraction patch runtime refresh: re-derived hunks=unavailable "
                f"(source commits unreachable: {', '.join(missing_commits)}), stored hunks={stored_hunk_count}"
            )
    else:
        if not isinstance(source_branch, str) or not source_branch:
            raise ExtractionError("Extraction manifest missing required source_branch")
        if not isinstance(source_base_ref, str) or not source_base_ref:
            raise ExtractionError("Extraction manifest missing required source_base_ref")
        revision_range = f"{source_base_ref}...{source_branch}"
        source_context = {
            "source_branch": source_branch,
            "source_base_ref": source_base_ref,
            "selected_paths": list(selected_paths),
        }
        if worktree_git.ref_exists(source_branch):
            if not worktree_git.ref_exists(source_base_ref):
                raise ExtractionError(f"Extraction source base ref not found: {source_base_ref}")
            runtime_refresh_available = True
            current_patch_text = worktree_git.get_diff_patch_for_paths(
                revision_range,
                selected_paths,
                binary=True,
            )
            refresh_message = (
                f"Extraction patch runtime refresh: re-derived hunks={{rederived}}, stored hunks={stored_hunk_count}"
            )
        else:
            refresh_message = (
                f"Extraction patch runtime refresh: re-derived hunks=unavailable "
                f"(source branch '{source_branch}' unreachable), stored hunks={stored_hunk_count}"
            )

    if runtime_refresh_available:
        assert current_patch_text is not None
        current_patch_text, stripped_runtime_paths = _strip_owned_artifact_patch_sections(
            current_patch_text,
            boundary=boundary,
        )
        if stripped_runtime_paths:
            write_log_entry(
                log_file,
                {
                    "type": "gza",
                    "subtype": "info",
                    "message": "Ignored gza-owned artifact paths while refreshing extraction seed patch",
                    "excluded_paths": list(stripped_runtime_paths),
                },
            )
        current_hunk_count = _count_patch_hunks(current_patch_text)
        write_log_entry(
            log_file,
            {
                "type": "gza",
                "subtype": "info",
                "message": refresh_message.format(rederived=current_hunk_count),
                **source_context,
                "rederived_hunk_count": current_hunk_count,
                "stored_hunk_count": stored_hunk_count,
            },
        )
        if not current_patch_text.strip():
            empty_message = (
                f"Extraction source diff is empty against current base; marking task {task.id} "
                f"{EXTRACTION_ALREADY_MERGED_COMPLETION_REASON}"
            )
            if source_commits:
                empty_message = (
                    f"Extraction source commit set is empty for selected paths; marking task {task.id} "
                    f"{EXTRACTION_ALREADY_MERGED_COMPLETION_REASON}"
                )
            return _already_merged_extraction_seed_result(
                task,
                log_file,
                message=empty_message,
            )

        if not source_commits:
            assert isinstance(source_base_ref, str)
            assert isinstance(source_branch, str)
            current_base_delta = worktree_git.get_diff_patch_for_paths(
                f"{source_base_ref}..{source_branch}",
                selected_paths,
                binary=True,
            )
            if not current_base_delta.strip():
                return _already_merged_extraction_seed_result(
                    task,
                    log_file,
                    message=(
                        "Extraction source branch adds nothing to the current base for selected paths; "
                        f"marking task {task.id} {EXTRACTION_ALREADY_MERGED_COMPLETION_REASON}"
                    ),
                )
        current_touched_paths = parse_patch_touched_paths(current_patch_text)
        if not current_touched_paths and not stripped_runtime_paths:
            raise ExtractionError("Runtime re-derived extraction patch has no touched file paths")
        if not current_touched_paths:
            return ExtractionSeedResult()
        unexpected_runtime = sorted(set(current_touched_paths) - declared_paths)
        if unexpected_runtime:
            raise ExtractionError(
                "Runtime extraction patch touches undeclared paths: " + ", ".join(unexpected_runtime),
            )

        runtime_patch_path = _write_runtime_patch_file(
            worktree_bundle_dir,
            "selected.runtime.patch",
            current_patch_text,
        )
        reverse_check_result = worktree_git.reverse_check_patch_file_result(runtime_patch_path)
        if reverse_check_result.returncode == 0:
            return _already_merged_extraction_seed_result(
                task,
                log_file,
                message=(
                    "Extraction source changes are already present on selected paths; "
                    f"marking task {task.id} {EXTRACTION_ALREADY_MERGED_COMPLETION_REASON}"
                ),
            )
        apply_result = worktree_git.apply_patch_file_result(runtime_patch_path)
        if apply_result.returncode != 0:
            if _apply_left_relevant_conflicts(worktree_git, set(current_touched_paths)):
                write_log_entry(
                    log_file,
                    {
                        "type": "gza",
                        "subtype": "warning",
                        "message": (
                            f"Applied extraction seed bundle from {project_bundle_dir.relative_to(config.project_dir)} "
                            f"using runtime re-derived patch with conflicts ({len(current_touched_paths)} files); "
                            "provider must resolve conflict markers"
                        ),
                        "seeded_paths": sorted(current_touched_paths),
                        "patch_source": "rederived",
                        "apply_conflicts": True,
                    },
                )
                return ExtractionSeedResult(seeded_paths=frozenset(current_touched_paths))
            raise GitError(_git_apply_failure_message(runtime_patch_path, apply_result))
        write_log_entry(
            log_file,
            {
                "type": "gza",
                "subtype": "info",
                "message": (
                    f"Applied extraction seed bundle from {project_bundle_dir.relative_to(config.project_dir)} "
                    f"using runtime re-derived patch ({len(current_touched_paths)} files)"
                ),
                "seeded_paths": sorted(current_touched_paths),
                "patch_source": "rederived",
            },
        )
        return ExtractionSeedResult(seeded_paths=frozenset(current_touched_paths))

    write_log_entry(
        log_file,
        {
            "type": "gza",
            "subtype": "info",
            "message": refresh_message,
            **source_context,
            "rederived_hunk_count": None,
            "stored_hunk_count": stored_hunk_count,
        },
    )
    apply_result = worktree_git.apply_patch_file_result(patch_path)
    if apply_result.returncode != 0:
        if _apply_left_relevant_conflicts(worktree_git, set(stored_touched_paths)):
            write_log_entry(
                log_file,
                {
                    "type": "gza",
                    "subtype": "warning",
                    "message": (
                        f"Applied extraction seed bundle from {project_bundle_dir.relative_to(config.project_dir)} "
                        f"using stored patch fallback with conflicts ({len(stored_touched_paths)} files); "
                        "provider must resolve conflict markers"
                    ),
                    "seeded_paths": sorted(stored_touched_paths),
                    "patch_source": "stored_fallback",
                    "apply_conflicts": True,
                },
            )
            return ExtractionSeedResult(seeded_paths=frozenset(stored_touched_paths))
        raise GitError(_git_apply_failure_message(patch_path, apply_result))
    write_log_entry(
        log_file,
        {
            "type": "gza",
            "subtype": "info",
            "message": (
                f"Applied extraction seed bundle from {project_bundle_dir.relative_to(config.project_dir)} "
                f"using stored patch fallback ({len(stored_touched_paths)} files)"
            ),
            "seeded_paths": sorted(stored_touched_paths),
            "patch_source": "stored_fallback",
        },
    )
    return ExtractionSeedResult(seeded_paths=frozenset(stored_touched_paths))


def _resolve_task_db_path(config: Config) -> Path:
    """Resolve the live task DB path for worktree snapshotting."""
    db_path = getattr(config, "db_path", None)
    if isinstance(db_path, Path):
        return db_path

    project_dir = getattr(config, "project_dir", None)
    if isinstance(project_dir, Path):
        return project_dir / ".gza" / "gza.db"

    return Path(".gza") / "gza.db"


def _snapshot_task_db_to_worktree(db_path: Path, worktree_path: Path) -> None:
    """Create a consistent read-only DB snapshot in the task worktree.

    Uses SQLite's backup API so the snapshot is transactionally consistent even
    while the live DB is being written by the host runner.
    """
    if not db_path.exists():
        return

    dst_dir = worktree_path / ".gza"
    dst_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = dst_dir / "gza.db"

    if snapshot_path.exists():
        snapshot_path.unlink()

    _backup_sqlite_file(db_path, snapshot_path)

    snapshot_path.chmod(0o444)


def _stage_worktree_agent_resources(config: Config, worktree_path: Path, boundary: ProjectBoundary) -> int:
    """Install bundled skills and the read-only DB snapshot at the scoped project root."""
    from .skills_utils import ensure_all_skills

    scoped_worktree_root = _worktree_project_root(worktree_path, boundary)
    skills_dir = scoped_worktree_root / ".claude" / "skills"
    n_installed = ensure_all_skills(skills_dir)
    _snapshot_task_db_to_worktree(_resolve_task_db_path(config), scoped_worktree_root)
    return n_installed


def _create_local_dep_symlinks(config: Config, worktree_path: Path) -> None:
    """Create symlinks for local path dependencies so uv can resolve them in worktrees.

    Uses the resolved local-dependency set from ``uv.lock`` and creates
    symlinks for relative out-of-repo deps in the worktree's ancestor
    directories so path references resolve exactly as they do from the project
    checkout. Absolute host paths already resolve natively and are skipped.
    """
    for dep in _project_boundary(config).local_dependencies:
        dep_rel = dep.source_path
        if dep.is_in_repo or dep_rel.is_absolute():
            continue
        dep_real_path = dep.resolved_path
        if not dep_real_path.exists():
            logger.debug("Local dep %s does not exist on disk; skipping symlink", dep_real_path)
            continue
        # Compute where the symlink should land (resolve relative path from worktree)
        symlink_location = (worktree_path / dep_rel).resolve()
        # Skip paths inside the worktree itself (workspace members)
        try:
            symlink_location.relative_to(worktree_path)
            continue
        except ValueError:
            pass
        symlink_location.parent.mkdir(parents=True, exist_ok=True)
        if symlink_location.exists() or symlink_location.is_symlink():
            if symlink_location.is_symlink() and symlink_location.resolve() == dep_real_path:
                logger.debug("Symlink %s already points to %s; skipping", symlink_location, dep_real_path)
                continue
            logger.warning(
                "Path %s already exists and does not point to %s; skipping symlink creation",
                symlink_location,
                dep_real_path,
            )
            continue
        try:
            symlink_location.symlink_to(dep_real_path)
            logger.info("Created symlink %s -> %s", symlink_location, dep_real_path)
        except FileExistsError:
            # Lost the race with a concurrent task — verify the winner created the right symlink
            if symlink_location.is_symlink() and symlink_location.resolve() == dep_real_path:
                logger.debug("Symlink %s created by concurrent task; skipping", symlink_location)
            else:
                logger.warning(
                    "Path %s appeared during symlink creation and does not point to %s; skipping",
                    symlink_location,
                    dep_real_path,
                )


def run(
    config: Config,
    task_id: str | None = None,
    resume: bool = False,
    open_after: bool = False,
    skip_precondition_check: bool = False,
    on_task_claimed: Callable[[Task], None] | None = None,
    create_pr: bool = False,
    invocation: RunInvocationContext | None = None,
) -> int:
    """Run Gza on the next pending task or a specific task.

    Uses git worktrees to isolate task execution from the main working directory.
    This allows concurrent work in the main checkout while gza runs.

    Args:
        config: Configuration object
        task_id: Optional specific task ID to run. If None, runs next pending task.
        resume: If True, resume from previous session using stored session_id.
        open_after: If True, open the report file in $EDITOR after completion (for review tasks).
        skip_precondition_check: If True, skip dependency merge precondition checks.
        on_task_claimed: Optional callback invoked after task ownership is established.
        create_pr: If True, create/reuse a PR after successful code-task completion.
        invocation: Optional execution invocation context for UX/provenance.
    """
    load_dotenv(config.project_dir)

    # Create hourly backup before running
    backup_database(config.db_path, config.project_dir)

    # Load tasks from SQLite
    store = SqliteTaskStore.from_config(config)
    invocation_context = invocation or _resolve_default_invocation_context()
    task_execution_mode = _task_execution_mode_from_invocation(invocation_context)

    pr_retry_mode = False
    if task_id:
        task = store.get(task_id)
        if not task:
            error_message(f"Error: Task {task_id} not found")
            return 1

        # Resume mode validation
        if resume:
            if task.status not in ("failed", "pending"):
                error_message(f"Error: Can only resume failed tasks (task is {task.status})")
                return 1
            if not task.session_id:
                error_message(f"Error: Task {task_id} has no session ID (cannot resume)")
                console.print(
                    "Use 'gza retry' to create a new retry attempt with a fresh conversation instead"
                    " (implement retries may fork fresh; same-branch follow-ups stay on the shared branch)"
                )
                return 1
            if task.status == "pending":
                assert task.id is not None
                claim = store.try_mark_in_progress(task.id, os.getpid())
                claimed = claim.task if claim is not None else None
                if claimed is None:
                    refreshed = store.get(task.id)
                    status = refreshed.status if refreshed else "unknown"
                    if claim is not None and claim.refusal_reason == "blocked":
                        error_message(
                            f"Error: Task {task_id} is blocked by task "
                            f"{claim.blocking_task_id} ({claim.blocking_task_status})"
                        )
                        return DEPENDENCY_BLOCKED_NOT_RUN_EXIT_CODE
                    error_message(f"Error: Task {task_id} is no longer pending (status: {status})")
                    return 1
                task = claimed
                task.execution_mode = task_execution_mode
                assert task.id is not None
                store.set_execution_mode(task.id, task_execution_mode)
            else:
                task.status = "in_progress"
                task.started_at = datetime.now(UTC)
                task.completed_at = None
                task.failure_reason = None
                task.completion_reason = None
                task.running_pid = os.getpid()
                task.execution_mode = task_execution_mode
                store.update(task)
        else:
            # Check if task is blocked by dependencies
            is_blocked, blocking_id, blocking_status = store.is_task_blocked(task)
            merge_precondition_blocked = (
                skip_precondition_check
                and task.depends_on
                and not task.same_branch
                and get_unmerged_dependency_precondition(store, task) is not None
            )
            if is_blocked and not merge_precondition_blocked:
                error_message(f"Error: Task {task_id} is blocked by task {blocking_id} ({blocking_status})")
                return DEPENDENCY_BLOCKED_NOT_RUN_EXIT_CODE
            requested_create_pr = bool(create_pr or task.create_pr)
            allow_pr_retry = (
                requested_create_pr
                and task.status == "failed"
                and task.failure_reason == PR_REQUIRED_FAILURE_REASON
                and bool(task.branch)
            )
            if task.status == "in_progress":
                task.running_pid = os.getpid()
                task.execution_mode = task_execution_mode
                store.update(task)
            elif allow_pr_retry:
                task.status = "in_progress"
                task.started_at = datetime.now(UTC)
                task.completed_at = None
                task.running_pid = os.getpid()
                task.execution_mode = task_execution_mode
                store.update(task)
                pr_retry_mode = True
            elif task.status != "pending":
                error_message(f"Error: Task {task_id} is no longer pending (status: {task.status})")
                return 1
            else:
                assert task.id is not None
                if skip_precondition_check and merge_precondition_blocked:
                    task.status = "in_progress"
                    task.started_at = datetime.now(UTC)
                    task.completed_at = None
                    task.failure_reason = None
                    task.completion_reason = None
                    task.running_pid = os.getpid()
                    task.execution_mode = task_execution_mode
                    store.update(task)
                else:
                    claim = store.try_mark_in_progress(task.id, os.getpid())
                    claimed = claim.task if claim is not None else None
                    if claimed is None:
                        refreshed = store.get(task.id)
                        status = refreshed.status if refreshed else "unknown"
                        if claim is not None and claim.refusal_reason == "blocked":
                            error_message(
                                f"Error: Task {task_id} is blocked by task "
                                f"{claim.blocking_task_id} ({claim.blocking_task_status})"
                            )
                            return DEPENDENCY_BLOCKED_NOT_RUN_EXIT_CODE
                        error_message(f"Error: Task {task_id} is no longer pending (status: {status})")
                        return 1
                    task = claimed
                task.execution_mode = task_execution_mode
                assert task.id is not None
                store.set_execution_mode(task.id, task_execution_mode)
    else:
        if resume:
            error_message("Error: Cannot resume without specifying a task ID")
            return 1
        task = None
        while True:
            candidate = store.get_next_pending()
            if candidate is None:
                break
            assert candidate.id is not None
            claim = store.try_mark_in_progress(candidate.id, os.getpid())
            claimed = claim.task if claim is not None else None
            if claimed is None:
                continue
            task = claimed
            task.execution_mode = task_execution_mode
            assert task.id is not None
            store.set_execution_mode(task.id, task_execution_mode)
            break

    if not task:
        console.print("No pending tasks found")
        return 0
    requested_create_pr = bool(create_pr or task.create_pr)
    ensure_task_log_path(config, store, task)
    if on_task_claimed is not None:
        on_task_claimed(task)
    if pr_retry_mode:
        return _retry_pr_required_code_task_completion(task, config, store)

    # Get effective model and provider for this task
    effective_model, effective_provider, effective_max_steps = get_effective_config_for_task(task, config)

    # Persist resolved model/provider to the task DB row immediately so analytics
    # can track which configuration actually ran, even if it crashes before completion.
    # provider_is_explicit is intentionally left unchanged so resolved provider
    # state does not become a sticky override for future executions. The same
    # applies to model_is_explicit: resolved defaults are recorded for analytics
    # only and must not become sticky model pins that can later violate parity.
    task.model = effective_model
    task.provider = effective_provider
    store.update(task)

    # Parity gate: reject cross-family pairs before provider instantiation.
    # This surfaces a clear actionable error instead of an opaque HTTP 404.
    if effective_model and not is_model_compatible_with_provider(effective_provider, effective_model):
        mismatch_msg = provider_model_mismatch_error("model", effective_provider, effective_model)
        error_message(f"Error: {mismatch_msg}")
        _mark_preflight_model_mismatch(
            task=task,
            config=config,
            store=store,
            provider_name=effective_provider,
            model=effective_model,
            invocation=invocation_context,
            resume=resume,
            message=f"Preflight failed: {mismatch_msg}",
        )
        return 1

    # Create a modified config with task-specific settings
    from copy import copy
    task_config = copy(config)
    task_config.model = effective_model or ""
    task_config.provider = effective_provider
    task_config.reasoning_effort = config.get_reasoning_effort_for_task(task.task_type, effective_provider) or ""
    task_config.max_steps = effective_max_steps
    task_config.max_turns = effective_max_steps
    task_config.timeout_minutes = config.get_timeout_minutes_for_task(task.task_type, effective_provider)

    # Get the provider for this task
    provider = get_provider(task_config)
    resolved_interaction_mode = _resolve_interaction_mode(invocation_context, provider)
    preflight_logs = ensure_task_log_paths(config, store, task)

    if not provider.check_credentials():
        error_message(f"Error: No {provider.name} credentials found")
        console.print(f"  {provider.credential_setup_hint}")
        _mark_preflight_provider_unavailable(
            task=task,
            config=config,
            store=store,
            provider=provider,
            invocation=invocation_context,
            interaction_mode=resolved_interaction_mode,
            resume=resume,
            message=f"Preflight failed: missing {provider.name} credentials",
        )
        return 1

    # Verify credentials work before proceeding
    console.print(f"Verifying {provider.name} credentials...")
    preflight_result = _normalize_preflight_result(
        provider.verify_credentials(task_config, log_file=preflight_logs.ops)
    )
    if not preflight_result.ok:
        _mark_preflight_failure(
            task=task,
            config=config,
            store=store,
            provider=provider,
            invocation=invocation_context,
            interaction_mode=resolved_interaction_mode,
            resume=resume,
            message=preflight_result.message or f"Preflight failed: {provider.name} verification failed",
            failure_reason=preflight_result.failure_reason or "PROVIDER_UNAVAILABLE",
        )
        return 1
    rc = _colors.RUNNER_COLORS
    console.print(f"[{rc.success}]Credentials verified ✓[/{rc.success}]")

    # Setup git on the main repo (for worktree operations)
    git = Git(config.project_dir)
    default_branch = git.default_branch()

    # Refresh origin/default_branch when possible, then choose the worktree base later via
    # _select_worktree_base_ref(), which prefers the local default branch unless origin is
    # strictly ahead.
    try:
        git._run("fetch", "origin", default_branch)
    except GitError:
        pass  # May fail if offline, continue anyway

    # Generate slug — checks for collisions with existing branches/logs.
    # Always generate when slug is not set (new tasks, including new resume tasks).
    # Keep existing slug only when resuming a task that already has one assigned.
    if task.slug is None:
        slug_override = _compute_slug_override(task, store)
        task.slug = generate_slug(
            task.prompt,
            existing_id=None,
            log_path=config.log_path,
            git=git,
            store=store,
            exclude_task_id=task.id,
            project_name=config.project_name,
            project_prefix=config.project_prefix,
            slug_override=slug_override,
            branch_strategy=config.branch_strategy,
            explicit_type=task.task_type_hint,
        )
    if task.slug and task.log_file:
        startup_log = config.project_dir / Path(task.log_file)
        if startup_log.name.endswith(".startup.log"):
            slug_log = rename_startup_log_to_slug(config, startup_log, task.slug)
            slug_log_relative = str(slug_log.relative_to(config.project_dir))
            if task.log_file != slug_log_relative:
                task.log_file = slug_log_relative
                store.update(task)

    task_header(
        task.prompt,
        str(task.id) if task.id is not None else "",
        task.task_type,
        slug=task.slug,
    )
    if invocation_context.execution_mode == "foreground_inline":
        if resolved_interaction_mode == "interactive":
            console.print(
                f"Foreground inline execution: interactive mode for provider '{provider.name.lower()}'. "
                "Press Ctrl-C to interrupt.",
            )
        else:
            console.print(
                f"Foreground inline execution: observe-only for provider '{provider.name.lower()}'. "
                "Interrupt to redirect.",
            )

    return _run_inner(
        task,
        task_config,
        config,
        store,
        provider,
        git,
        resume=resume,
        open_after=open_after,
        skip_precondition_check=skip_precondition_check,
        create_pr=requested_create_pr,
        invocation=invocation_context,
        interaction_mode=resolved_interaction_mode,
    )


def _check_dependency_merge_precondition(
    task: Task,
    store: SqliteTaskStore,
    git: Git,
    *,
    default_branch: str,
) -> tuple[Task | None, str | None, str | None]:
    """Return unmet dependency merge prerequisite or a git operational error."""
    dep = get_unmerged_dependency_precondition(store, task)
    if dep is None:
        return (None, None, None)
    return (dep, default_branch, None)


def _park_task_pending_after_blocked_precondition(task: Task, store: SqliteTaskStore) -> None:
    """Restore a task to pending after a defensive dependency gate refusal."""
    task.status = "pending"
    task.started_at = None
    task.running_pid = None
    task.completed_at = None
    task.failure_reason = None
    task.completion_reason = None
    task.execution_mode = None
    store.update(task)


def _resolve_code_task_branch_name(
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    *,
    resume: bool,
) -> str | None:
    """Resolve the branch name for implement/improve task execution."""
    if resume and task.branch:
        # Resume uses the existing branch from the failed task
        branch_name = task.branch
        console.print(f"Resuming on existing branch: [blue]{branch_name}[/blue]")
        return branch_name

    if resume:
        # Resume but branch wasn't saved - derive from task_id using branch naming strategy
        assert config.branch_strategy is not None
        assert task.slug is not None
        branch_name = generate_branch_name(
            pattern=config.branch_strategy.pattern,
            project_name=config.project_name,
            task_slug=task.slug,
            prompt=task.prompt,
            default_type=config.branch_strategy.default_type,
            explicit_type=task.task_type_hint,
            task_id=task.id or "",
            project_prefix=config.project_prefix,
        )
        console.print(f"Resuming on branch: [blue]{branch_name}[/blue]")
        return branch_name

    if task.same_branch:
        if task.task_type == "rebase":
            rebase_branch = resolve_rebase_target_branch(store, task)
            if rebase_branch and git.branch_exists(rebase_branch):
                console.print(f"Using rebase target branch: [blue]{rebase_branch}[/blue]")
                return rebase_branch
            if rebase_branch:
                error_message(
                    f"Error: Rebase task {task.id} resolved target branch {rebase_branch} but it does not exist"
                )
                return None
        merge_unit = store.resolve_merge_unit_for_task(task.id) if task.id is not None else None
        canonical_same_branch = merge_unit.source_branch if merge_unit is not None else task.branch
        if canonical_same_branch:
            if git.branch_exists(canonical_same_branch):
                if merge_unit is not None:
                    console.print(
                        f"Using merge-unit source branch: [blue]{canonical_same_branch}[/blue]"
                    )
                else:
                    console.print(
                        f"Using existing branch from task {task.id}: [blue]{canonical_same_branch}[/blue]"
                    )
                return canonical_same_branch
            error_message(
                f"Error: Task {task.id} resolved canonical same-branch target "
                f"{canonical_same_branch} but it does not exist"
            )
            return None
        # Use the branch from based_on task (for improve tasks) or depends_on task (fallback).
        # Walk the based_on chain until we find an ancestor with a valid, existing branch.
        source_task = None
        if task.based_on:
            source_task = store.get(task.based_on)
        elif task.depends_on:
            source_task = store.get(task.depends_on)

        resolved_branch: str | None = None
        visited_ids: list[str | None] = []
        seen_ids: set[str | None] = set()
        current = source_task
        while current is not None:
            if current.branch and git.branch_exists(current.branch):
                resolved_branch = current.branch
                if visited_ids:
                    via = " -> ".join(str(i) for i in visited_ids)
                    console.print(
                        f"Using branch from task {current.id} (via {via}): [blue]{resolved_branch}[/blue]"
                    )
                else:
                    console.print(f"Using existing branch from task {current.id}: [blue]{resolved_branch}[/blue]")
                break
            seen_ids.add(current.id)
            visited_ids.append(current.id)
            # Walk up the based_on chain, with cycle detection
            if current.based_on and current.based_on not in seen_ids:
                current = store.get(current.based_on)
            elif current.based_on:
                error_message(f"Error: Cycle detected in based_on chain for task {task.id}")
                return None
            else:
                current = None

        if resolved_branch is None:
            error_message(f"Error: Task {task.id} has same_branch=True but no ancestor has a valid branch")
            return None
        return resolved_branch

    if config.branch_mode == "single":
        return f"{config.project_name}/gza-work"

    # multi branch mode uses branch naming strategy
    assert config.branch_strategy is not None
    assert task.slug is not None
    branch_name = generate_branch_name(
        pattern=config.branch_strategy.pattern,
        project_name=config.project_name,
        task_slug=task.slug,
        prompt=task.prompt,
        default_type=config.branch_strategy.default_type,
        explicit_type=task.task_type_hint,
        task_id=task.id or "",
        project_prefix=config.project_prefix,
    )
    console.print(
        f"Branch strategy: [{_colors.RUNNER_COLORS.label}]{config.branch_strategy.pattern}[/] "
        f"→ [blue]{branch_name}[/blue]"
    )
    return branch_name


def _select_worktree_base_ref(git: Git, default_branch: str) -> str:
    """Select base ref for a new worktree using local/default divergence logic."""
    base_ref = default_branch
    origin_ref = f"origin/{default_branch}"

    # Check if origin ref exists
    origin_exists = git._run("rev-parse", "--verify", origin_ref, check=False).returncode == 0

    if not origin_exists:
        return base_ref

    # Compare local vs origin - use whichever is ahead
    local_ahead = git.count_commits_ahead(default_branch, origin_ref)
    origin_ahead = git.count_commits_ahead(origin_ref, default_branch)

    if origin_ahead > 0 and local_ahead == 0:
        # Origin is strictly ahead, use it
        return origin_ref
    if local_ahead > 0 and origin_ahead == 0:
        # Local is strictly ahead, use it
        return default_branch
    if local_ahead > 0 and origin_ahead > 0:
        # Diverged - prefer local to include unpushed changes
        return default_branch
    # Same commit, use either (default to local)
    return default_branch


def _setup_code_task_worktree(
    task: Task,
    config: Config,
    git: Git,
    *,
    branch_name: str,
    worktree_path: Path,
    default_branch: str,
    resume: bool,
) -> bool:
    """Create or re-create a code-task worktree and check out the target branch."""
    if resume or task.same_branch:
        # Validate branch exists before attempting to check it out
        if not git.branch_exists(branch_name):
            error_message(f"Error: Branch '{branch_name}' no longer exists. Cannot resume.")
            console.print("The branch may have been deleted or merged.")
            return False

        # Check out existing branch in worktree
        try:
            # Remove any existing worktree for this branch (may be at a different path
            # from a previous task run), then also remove worktree at target path if present
            cleanup_worktree_for_branch(
                git,
                branch_name,
                force=True,
                permitted_root_paths=managed_worktree_root_paths(config),
            )
            if worktree_path.exists():
                git.worktree_remove(worktree_path, force=True)

            console.print(f"Creating worktree with existing branch: {worktree_path}")
            # For existing branch, use git worktree add <path> <branch>
            git._run("worktree", "add", str(worktree_path), branch_name)
            return True
        except GitError as e:
            error_message(f"Error: Could not check out branch {branch_name} in worktree: {e}")
            return False

    # Delete existing branch if in single mode (worktree_add will recreate it)
    if config.branch_mode == "single" and git.branch_exists(branch_name):
        git._run("branch", "-D", branch_name, check=False)

    try:
        base_ref = task.base_branch or _select_worktree_base_ref(git, default_branch)
        if task.base_branch:
            console.print(f"Creating retry branch from base branch: [blue]{task.base_branch}[/blue]")
        console.print(f"Creating worktree: {worktree_path}")
        git.worktree_add(worktree_path, branch_name, base_ref)
        return True
    except GitError as e:
        error_message(f"Git error: {e}")
        return False


def _filter_stageable_paths(
    candidate_paths: set[str],
    status_paths: set[str],
    *,
    boundary: ProjectBoundary | None = None,
) -> set[str]:
    """Keep only paths that can be staged without pathspec failures."""
    return {path for path in candidate_paths if path in status_paths and not _is_gza_owned_path(path, boundary=boundary)}


def _complete_code_task(
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    worktree_git: Git,
    log_file: Path,
    branch_name: str,
    stats: TaskStats,
    exit_code: int,
    pre_run_status: set[tuple[str, str]],
    worktree_summary_path: Path,
    summary_path: Path,
    summary_dir: Path,
    *,
    target_branch: str | None = None,
    skip_commit: bool = False,
    create_pr: bool = False,
    fix_commits_ahead_before_run: int | None = None,
    fix_default_branch: str | None = None,
    seeded_paths: set[str] | None = None,
    improve_diff_baseline: ImproveDiffBaseline | None = None,
    rebase_diff_baseline: RebaseDiffBaseline | None = None,
    error_type: str | None = None,
) -> int:
    """Handle successful code-task completion (staging, commit, completion state, output).

    Args:
        skip_commit: If True, skip staging/committing changes. Used for rebase
            tasks where the agent handles rebases directly
            and no new commits should be created by the runner.
    """
    if skip_commit:
        has_uncommitted = False
    else:
        seeded_paths = seeded_paths or set()
        boundary = _project_boundary(config)
        # Compute which files changed during the provider run (selective staging)
        post_run_status = worktree_git.status_porcelain()
        new_changes = post_run_status - pre_run_status
        status_paths = {filepath for _, filepath in post_run_status}
        candidate_stage_paths = {filepath for _, filepath in new_changes} | seeded_paths
        files_to_stage = _filter_stageable_paths(
            candidate_stage_paths,
            status_paths,
            boundary=boundary,
        )
        if (
            files_to_stage
            and getattr(config, "enforce_project_scope", True)
        ):
            worktree_repo_root = getattr(worktree_git, "repo_dir", None)
            if not isinstance(worktree_repo_root, Path):
                worktree_repo_root = None
            out_of_scope_paths = _find_out_of_scope_paths(
                config,
                files_to_stage,
                task=task,
                repo_root=worktree_repo_root,
            )
            if out_of_scope_paths:
                failure_message = (
                    "Project scope violation: refusing to commit out-of-scope paths:\n"
                    + "\n".join(f"- {path}" for path in out_of_scope_paths)
                )
                error_message(failure_message)
                write_log_entry(
                    log_file,
                    {
                        "type": "gza",
                        "subtype": "outcome",
                        "message": failure_message,
                        "failure_reason": PROJECT_SCOPE_VIOLATION_FAILURE_REASON,
                        "out_of_scope_paths": out_of_scope_paths,
                    },
                )
                _mark_task_failed(
                    task=task,
                    config=config,
                    store=store,
                    log_file=log_file,
                    stats=stats,
                    branch=branch_name,
                    explicit_reason=PROJECT_SCOPE_VIOLATION_FAILURE_REASON,
                    error_type=None,
                    exit_code=1,
                )
                return 0
        has_uncommitted = bool(files_to_stage)

        if not has_uncommitted:
            # Check if branch already has commits from a previous run
            default_branch = worktree_git.default_branch()
            commits_ahead = worktree_git.count_commits_ahead(branch_name, default_branch)
            if commits_ahead == 0:
                # No uncommitted changes and no commits on branch - real failure
                # Note: No need to save WIP here since there are no changes
                empty_turn = (
                    exit_code == 0
                    and error_type is None
                    and _observed_step_count(stats) == 0
                    and _log_has_empty_turn_signature(log_file)
                )
                failure_reason = (
                    "PROVIDER_EMPTY_TURN"
                    if empty_turn
                    else _resolve_failure_reason(
                        error_type=None,
                        exit_code=exit_code,
                        log_file=log_file,
                        stats=stats,
                        fallback_to_log=True,
                    )
                )
                provider_stderr_tail = _extract_provider_stderr_tail(log_file) if empty_turn else ""
                task_footer(
                    task,
                    stats,
                    status="No changes made",
                    branch=branch_name,
                )
                write_log_entry(
                    log_file,
                    {
                        "type": "gza",
                        "subtype": "outcome",
                        "message": "Outcome: failed (no changes made)",
                        "exit_code": exit_code,
                        "failure_reason": failure_reason,
                        **({"stderr_tail": provider_stderr_tail} if provider_stderr_tail else {}),
                    },
                )
                write_log_entry(
                    log_file,
                    {
                        "type": "gza",
                        "subtype": "stats",
                        "message": f"Stats: {_observed_step_count(stats)} steps, {stats.duration_seconds or 0.0:.1f}s, ${stats.cost_usd or 0.0:.4f}",
                        "duration_seconds": stats.duration_seconds,
                        "cost_usd": stats.cost_usd,
                        "num_steps": _observed_step_count(stats),
                    },
                )
                _mark_task_failed(
                    task=task,
                    config=config,
                    store=store,
                    log_file=log_file,
                    stats=stats,
                    branch=branch_name,
                    explicit_reason=failure_reason,
                    error_type=None,
                    exit_code=exit_code,
                )
                return 0
            # else: branch has commits from a previous run - treat as success without committing

        if has_uncommitted:
            assert task.id is not None, "Task ID must be set before committing"
            # Squash any WIP commits before creating final commit
            _squash_wip_commits(worktree_git, task)

            # Stage only files changed in this run plus extraction-seeded files.
            for f in sorted(files_to_stage):
                worktree_git.add(f)

            review_task_id = None
            if task.task_type == "improve" and task.depends_on:
                review_task = store.get(task.depends_on)
                if review_task and review_task.task_type == "review":
                    review_task_id = review_task.id

            commit_subject = _build_code_task_commit_subject(
                task.prompt,
                worktree_summary_path,
                fallback_subject=_default_code_task_commit_subject(task.slug, task.id),
            )

            commit_message = build_task_commit_message(
                commit_subject,
                task_id=task.id,
                task_slug=task.slug,
                review_task_id=review_task_id,
            )
            worktree_git.commit(commit_message)

    # Copy summary file from worktree to main project directory
    output_content = None
    if worktree_summary_path.exists():
        try:
            summary_content = worktree_summary_path.read_text()
        except (OSError, UnicodeError):
            logger.warning(
                "Failed to read summary file for task completion output at %s; continuing without output_content",
                worktree_summary_path,
                exc_info=True,
            )
        else:
            # Ensure target directory exists
            summary_dir.mkdir(parents=True, exist_ok=True)
            # Copy summary content from worktree to project dir
            summary_path.write_text(summary_content)
            output_content = summary_content

    # Compute diff stats vs. default branch before marking completed
    default_branch = target_branch if target_branch is not None else worktree_git.default_branch()
    numstat_output = worktree_git.get_diff_numstat(f"{default_branch}...{branch_name}")
    diff_files, diff_added, diff_removed = parse_diff_numstat(numstat_output)
    head_sha = worktree_git.rev_parse_if_exists(branch_name)
    base_sha = worktree_git.rev_parse_if_exists(default_branch)

    # Keep branch context on the in-memory task so PR ensure can run before
    # the final completed-state DB transition.
    task.branch = branch_name
    fix_was_merged_before_run = False
    if task.task_type == "fix":
        root_impl = _resolve_root_implementation_for_fix(task, store)
        if root_impl is not None and root_impl.id is not None:
            root_impl_unit = store.resolve_merge_unit_for_task(root_impl.id)
            fix_was_merged_before_run = (
                (root_impl_unit.state if root_impl_unit is not None else root_impl.merge_status) == "merged"
            )
    if create_pr and task.task_type != "rebase":
        pr_outcome = _ensure_work_pr_for_completed_code_task(task, config, store, worktree_git)
        if pr_outcome.kind == "nonfatal_missing_pr":
            _record_pr_publication_note(
                task=task,
                log_file=log_file,
                branch_name=branch_name,
                status=pr_outcome.status,
                error=pr_outcome.error,
            )
        elif pr_outcome.kind == "branch_unpushable":
            return _persist_branch_unpushable_failure(
                task=task,
                config=config,
                store=store,
                log_file=log_file,
                stats=stats,
                branch_name=branch_name,
                output_content=output_content,
                diff_files=diff_files,
                diff_added=diff_added,
                diff_removed=diff_removed,
                head_sha=head_sha,
                base_sha=base_sha,
                message=pr_outcome.message,
                fix_commits_ahead_before_run=fix_commits_ahead_before_run,
                fix_default_branch=fix_default_branch,
                fix_was_merged_before_run=fix_was_merged_before_run,
            )

    task_logger = TaskExecutionLogger(resolve_ops_log_path(config, log_file), echo=True)
    if task.task_type == "rebase":
        return _finalize_rebase_completion(
            task=task,
            config=config,
            store=store,
            worktree_git=worktree_git,
            branch_name=branch_name,
            stats=stats,
            log_file=log_file,
            output_content=output_content,
            diff_files=diff_files,
            diff_added=diff_added,
            diff_removed=diff_removed,
            head_sha=head_sha,
            base_sha=base_sha,
            task_logger=task_logger,
            target_branch=default_branch,
            create_pr=create_pr,
            fix_commits_ahead_before_run=fix_commits_ahead_before_run,
            fix_default_branch=fix_default_branch,
            fix_was_merged_before_run=fix_was_merged_before_run,
            improve_diff_baseline=improve_diff_baseline,
            rebase_diff_baseline=rebase_diff_baseline,
        )

    _finalize_completed_code_task(
        task=task,
        config=config,
        store=store,
        log_file=log_file,
        branch_name=branch_name,
        output_content=output_content,
        stats=stats,
        diff_files=diff_files,
        diff_added=diff_added,
        diff_removed=diff_removed,
        head_sha=head_sha,
        base_sha=base_sha,
    )
    return _post_complete_code_task(
        task,
        config,
        store,
        worktree_git,
        branch_name,
        stats,
        task_logger=task_logger,
        target_branch=default_branch,
        fix_commits_ahead_before_run=fix_commits_ahead_before_run,
        fix_default_branch=fix_default_branch,
        fix_was_merged_before_run=fix_was_merged_before_run,
        improve_diff_baseline=improve_diff_baseline,
        rebase_diff_baseline=rebase_diff_baseline,
    )


def _post_complete_code_task(
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    worktree_git: Git,
    branch_name: str,
    stats: TaskStats,
    *,
    task_logger: TaskExecutionLogger | None = None,
    target_branch: str | None = None,
    fix_commits_ahead_before_run: int | None = None,
    fix_default_branch: str | None = None,
    fix_was_merged_before_run: bool = False,
    improve_diff_baseline: ImproveDiffBaseline | None = None,
    rebase_diff_baseline: RebaseDiffBaseline | None = None,
) -> int:
    """Run shared post-completion side effects for completed code tasks."""
    auto_learnings = maybe_auto_regenerate_learnings(store, config)
    improve_follow_up_ready = True
    improve_changed_diff: bool | None = None
    fix_code_changed = False
    fix_auto_review_ready = True
    impl_ancestor: Task | None = None

    # Improve retries/resumes may chain based_on through previous improves, so
    # resolve the implementation ancestor first. Only clear review state,
    # resolve comments, and create a follow-up review when the improve changed
    # the tracked review diff or comparison could not be proven.
    if task.task_type == "improve":
        impl_ancestor = _resolve_impl_ancestor(store, task)
        improve_comparison = compute_improve_changed_diff(
            worktree_git,
            baseline=(
                improve_diff_baseline
                if improve_diff_baseline is not None
                else ImproveDiffBaseline(branch_tip_before=None, target_at_start=None, recovered=True)
            ),
            branch=branch_name,
        )
        improve_changed_diff = improve_comparison.changed_diff
        assert task.id is not None
        store.set_task_changed_diff(task.id, improve_comparison.changed_diff)
        task.changed_diff = improve_comparison.changed_diff
        if improve_comparison.warning:
            logger.warning(improve_comparison.warning)
            console.print(f"[yellow]Warning: {improve_comparison.warning}[/yellow]")
        if improve_comparison.changed_diff:
            if impl_ancestor and impl_ancestor.id is not None:
                # If the implementation was already merged, flip it back to unmerged:
                # improve writes add commits on the shared implementation branch even
                # when publishing those commits still needs operator intervention.
                refreshed_impl = store.get(impl_ancestor.id)
                refreshed_unit = (
                    store.resolve_merge_unit_for_task(refreshed_impl.id)
                    if refreshed_impl and refreshed_impl.id is not None
                    else None
                )
                if refreshed_impl and refreshed_impl.id is not None and (
                    refreshed_unit.state if refreshed_unit is not None else refreshed_impl.merge_status
                ) == "merged":
                    store.set_merge_status(refreshed_impl.id, "unmerged")
            if task.create_review:
                improve_follow_up_ready = _sync_completed_code_task_branch_for_live_pr(
                    task,
                    config,
                    store,
                    worktree_git,
                )
            if improve_follow_up_ready and impl_ancestor and impl_ancestor.id is not None:
                store.clear_review_state(impl_ancestor.id)
                store.resolve_comments(
                    impl_ancestor.id,
                    created_on_or_before=task.created_at,
                )
        else:
            current_noop_head_sha = worktree_git.rev_parse_if_exists(branch_name) if branch_name else None
            cleared_verify_only_blocker = _noop_improve_resolves_verify_only_review(
                config=config,
                store=store,
                task=task,
                impl_ancestor=impl_ancestor,
                current_branch=branch_name,
                current_head_sha=current_noop_head_sha,
            )
            if not cleared_verify_only_blocker:
                _capture_noop_improve_review_verify_result(
                    config=config,
                    store=store,
                    task=task,
                    worktree_git=worktree_git,
                    branch_name=branch_name,
                    task_logger=task_logger,
                )
                cleared_verify_only_blocker = _noop_improve_resolves_verify_only_review(
                    config=config,
                    store=store,
                    task=task,
                    impl_ancestor=impl_ancestor,
                    current_branch=branch_name,
                    current_head_sha=current_noop_head_sha,
                )
            if cleared_verify_only_blocker:
                console.print(
                    "[blue]Review State: cleared verify-only blocker from persisted passing no-op improve verify evidence.[/blue]"
                )

    # Rebase tasks run provider-side conflict resolution in the worktree.
    # Force-push from the host runner so SSH/auth follows host environment.
    if task.task_type == "rebase":
        publish_rebased_branch(
            worktree_git,
            branch=branch_name,
            baseline=rebase_diff_baseline,
            logger=task_logger,
        )

    rebase_changed_diff: bool | None = None
    # Invalidate review state after rebase completes only when the patch changed
    # or equivalence could not be proven, but only after publication succeeds.
    if task.task_type == "rebase" and task.based_on:
        impl_ancestor = _resolve_impl_ancestor(store, task)
        rebase_comparison = compute_rebase_changed_diff(
            worktree_git,
            baseline=(
                rebase_diff_baseline
                if rebase_diff_baseline is not None
                else RebaseDiffBaseline(old_tip=None, target_at_start=None, merge_base_at_start=None, recovered=True)
            ),
            branch=branch_name,
            target=target_branch if target_branch is not None else worktree_git.default_branch(),
        )
        rebase_changed_diff = rebase_comparison.changed_diff
        assert task.id is not None
        store.set_rebase_changed_diff(task.id, rebase_comparison.changed_diff)
        task.changed_diff = rebase_comparison.changed_diff
        if rebase_comparison.warning:
            logger.warning(rebase_comparison.warning)
            console.print(f"[yellow]Warning: {rebase_comparison.warning}[/yellow]")
        rebase_review_target_id = (
            impl_ancestor.id if impl_ancestor and impl_ancestor.id is not None else task.based_on
        )
        if rebase_comparison.changed_diff:
            store.invalidate_review_state(rebase_review_target_id)
        parent = store.get(rebase_review_target_id)
        parent_unit = (
            store.resolve_merge_unit_for_task(parent.id) if parent and parent.id is not None else None
        )
        if parent and parent.id is not None and (
            parent_unit.state if parent_unit is not None else parent.merge_status
        ) == "merged":
            store.set_merge_status(parent.id, "unmerged")

    if task.task_type == "fix":
        fix_code_changed = _prepare_fix_follow_up_review(
            task,
            store,
            worktree_git,
            branch_name,
            fix_commits_ahead_before_run=fix_commits_ahead_before_run,
            fix_default_branch=fix_default_branch,
            fix_was_merged_before_run=fix_was_merged_before_run,
        )
        if fix_code_changed:
            if task.create_review:
                fix_auto_review_ready = _sync_completed_code_task_branch_for_live_pr(
                    task,
                    config,
                    store,
                    worktree_git,
                )
            else:
                _create_fix_follow_up_review_task(task, store)

    console.print("")
    if task.task_type == "improve" and improve_changed_diff is not None:
        if improve_changed_diff is False:
            console.print(f"[yellow]Warning: {_noop_improve_warning_text(store, task, impl_ancestor)}[/yellow]")
        changed_diff_text = (
            "yes (tracked improve diff changed or comparison unavailable)"
            if improve_changed_diff
            else "no (no tracked improve changes)"
        )
        console.print(f"Changed Diff: {changed_diff_text}")
    if task.task_type == "rebase" and rebase_changed_diff is not None:
        changed_diff_text = "yes (review must be refreshed)" if rebase_changed_diff else "no (review can be preserved)"
        console.print(f"Changed Diff: {changed_diff_text}")
    task_footer(
        task,
        stats,
        status="Done",
        branch=branch_name,
        learnings=auto_learnings,
        store=store,
    )

    # Auto-create and run review task if requested
    if task.create_review:
        if task.task_type == "implement":
            refreshed_task = store.get(task.id) if task.id is not None else None
            if refreshed_task is not None:
                task = refreshed_task
            merge_state = _resolved_merge_state_for_task(store, task)
            if not should_auto_create_review_for_completed_code_task(task, merge_state=merge_state):
                message = auto_review_skip_message_for_completed_code_task(task, merge_state=merge_state)
                if message is not None:
                    _emit_auto_review_suppressed(
                        task=task,
                        config=config,
                        message=message,
                        task_logger=task_logger,
                    )
                return 0
        if task.task_type == "improve" and improve_changed_diff is False:
            return 0
        if task.task_type == "improve" and not improve_follow_up_ready:
            review_target = _resolve_impl_ancestor(store, task)
            if review_target and review_target.id is not None:
                print(
                    "Warning: Skipping auto-review until the improve branch is safely published. "
                    f"After resolving the PR sync issue, run `uv run gza review {review_target.id}`."
                )
            else:
                print(
                    "Warning: Skipping auto-review until the improve branch is safely published."
                )
            return 0
        if task.task_type == "fix":
            if not fix_code_changed:
                return 0
            if not fix_auto_review_ready:
                review_target = _resolve_root_implementation_for_fix(task, store)
                if review_target and review_target.id is not None:
                    print(
                        "Warning: Skipping auto-review until the fix branch is safely published. "
                        f"After resolving the PR sync issue, run `uv run gza review {review_target.id}`."
                    )
                else:
                    print(
                        "Warning: Skipping auto-review until the fix branch is safely published."
                    )
                return 0
        if target_branch is None:
            return _create_and_run_review_task(task, config, store)
        return _create_and_run_review_task(task, config, store)

    return 0


def _prepare_fix_follow_up_review(
    task: Task,
    store: SqliteTaskStore,
    worktree_git: Git,
    branch_name: str,
    *,
    fix_commits_ahead_before_run: int | None,
    fix_default_branch: str | None,
    fix_was_merged_before_run: bool = False,
) -> bool:
    """Return True when a completed fix added commits that require follow-up review."""
    root_impl = _resolve_root_implementation_for_fix(task, store)
    if root_impl is None or root_impl.id is None:
        return False
    root_impl_id = root_impl.id
    default_branch = fix_default_branch

    def _restore_prior_merged_state() -> None:
        if not fix_was_merged_before_run:
            return
        refreshed_impl = store.get(root_impl_id)
        if refreshed_impl is not None and refreshed_impl.id is not None:
            store.set_merge_status(refreshed_impl.id, "merged")

    if fix_commits_ahead_before_run is None:
        _restore_prior_merged_state()
        print("Warning: Could not determine fix commit baseline before run")
        print("Warning: Could not determine whether the fix run changed code")
        return False

    if not default_branch:
        try:
            default_branch = worktree_git.default_branch()
        except GitError as exc:
            _restore_prior_merged_state()
            print(f"Warning: Could not determine fix commit delta: {exc}")
            print("Warning: Could not determine whether the fix run changed code")
            return False

    try:
        commits_after = worktree_git.count_commits_ahead(branch_name, default_branch)
    except GitError as exc:
        _restore_prior_merged_state()
        print(f"Warning: Could not determine fix commit delta: {exc}")
        print("Warning: Could not determine whether the fix run changed code")
        return False

    commits_before = fix_commits_ahead_before_run
    if commits_after <= commits_before:
        _restore_prior_merged_state()
        print("Fix completed without new commits; no follow-up review was auto-created.")
        return False

    store.clear_review_state(root_impl_id)
    refreshed_impl = store.get(root_impl_id)
    refreshed_unit = (
        store.resolve_merge_unit_for_task(refreshed_impl.id)
        if refreshed_impl and refreshed_impl.id is not None
        else None
    )
    if refreshed_impl and refreshed_impl.id is not None and (
        refreshed_unit.state if refreshed_unit is not None else refreshed_impl.merge_status
    ) == "merged":
        store.set_merge_status(refreshed_impl.id, "unmerged")

    return True


def _create_fix_follow_up_review_task(task: Task, store: SqliteTaskStore) -> None:
    """Create a pending follow-up review task for a completed fix run."""
    root_impl = _resolve_root_implementation_for_fix(task, store)
    if root_impl is None or root_impl.id is None:
        return

    try:
        review_task = create_review_task(store, root_impl, trigger_source="auto-recovery", prompt_mode="auto")
    except DuplicateReviewError as exc:
        active = exc.active_review
        print(
            f"Follow-up review already exists for implementation {root_impl.id}: "
            f"{active.id} ({active.status})."
        )
        return
    except ValueError as exc:
        print(
            f"Warning: Could not auto-create follow-up review for implementation {root_impl.id}: {exc}"
        )
        print(f"Next step: run `uv run gza review {root_impl.id}` after validating task state.")
        return
    print(f"Created follow-up review task {review_task.id} for implementation {root_impl.id}")


def _retry_pr_required_code_task_completion(task: Task, config: Config, store: SqliteTaskStore) -> int:
    """Retry post-code PR/completion steps for tasks blocked on required PR creation."""
    if not task.branch:
        print(f"Error: Task {task.id} has no branch to create/reuse PR")
        _mark_task_failed(
            task=task,
            config=config,
            store=store,
            log_file=task.log_file,
            has_commits=bool(task.has_commits),
            explicit_reason="GIT_ERROR",
            error_type=None,
            exit_code=1,
        )
        return 1

    git = Git(config.project_dir)
    stats = TaskStats(
        duration_seconds=task.duration_seconds,
        num_steps_reported=task.num_steps_reported,
        num_steps_computed=task.num_steps_computed,
        num_turns_reported=task.num_turns_reported,
        num_turns_computed=task.num_turns_computed,
        cost_usd=task.cost_usd,
        input_tokens=task.input_tokens,
        output_tokens=task.output_tokens,
    )

    task.failure_reason = None
    task.completion_reason = None
    target_branch: str | None = git.default_branch() if task.branch and task.has_commits else None
    head_sha = git.rev_parse_if_exists(task.branch) if task.branch and task.has_commits else None
    base_sha = git.rev_parse_if_exists(target_branch) if target_branch and task.has_commits else None
    retry_logger = None
    retry_log_path: Path | None = None
    if task.log_file:
        retry_log_path = config.project_dir / Path(task.log_file)
        retry_log_path.parent.mkdir(parents=True, exist_ok=True)
        retry_logger = TaskExecutionLogger(
            resolve_ops_log_path(config, retry_log_path),
            echo=True,
        )
    if task.task_type == "rebase":
        try:
            return _finalize_already_published_rebase_pr_retry(
                task=task,
                config=config,
                store=store,
                git=git,
                branch_name=task.branch,
                stats=stats,
                log_file=retry_log_path if retry_log_path is not None else config.project_dir / "retry.log",
                output_content=task.output_content,
                diff_files=task.diff_files_changed or 0,
                diff_added=task.diff_lines_added or 0,
                diff_removed=task.diff_lines_removed or 0,
                head_sha=head_sha,
                base_sha=base_sha,
                task_logger=retry_logger
                or TaskExecutionLogger(resolve_ops_log_path(config, config.project_dir / "retry.log"), echo=True),
            )
        except GitError as e:
            error_message(f"Git error: {e}")
            if retry_log_path is not None:
                write_log_entry(
                    retry_log_path,
                    {
                        "type": "gza",
                        "subtype": "outcome",
                        "message": "Outcome: failed (GIT_ERROR)",
                        "exit_code": 1,
                        "failure_reason": "GIT_ERROR",
                    },
                )
                _write_stats_entry(retry_log_path, stats)
            _mark_task_failed(
                task=task,
                config=config,
                store=store,
                log_file=task.log_file,
                stats=stats,
                branch=task.branch,
                has_commits=bool(task.has_commits),
                explicit_reason="GIT_ERROR",
                error_type=None,
                exit_code=1,
                head_sha=head_sha,
                base_sha=base_sha,
            )
            return 1

    publication_state = load_branch_publication_state(store, task.id)
    return _complete_failed_code_task_after_pr_publication(
        task=task,
        config=config,
        store=store,
        git=git,
        branch_name=task.branch,
        stats=stats,
        log_file=retry_log_path,
        output_content=task.output_content,
        diff_files=task.diff_files_changed or 0,
        diff_added=task.diff_lines_added or 0,
        diff_removed=task.diff_lines_removed or 0,
        head_sha=head_sha,
        base_sha=base_sha,
        task_logger=retry_logger,
        target_branch=target_branch,
        fix_commits_ahead_before_run=publication_state.fix_commits_ahead_before_run,
        fix_default_branch=publication_state.fix_default_branch,
        fix_was_merged_before_run=publication_state.fix_was_merged_before_run,
    )


def _run_inner(
    task: "Task",
    task_config: Config,
    config: Config,
    store: SqliteTaskStore,
    provider: "Provider",
    git: "Git | None",
    resume: bool = False,
    open_after: bool = False,
    skip_precondition_check: bool = False,
    create_pr: bool = False,
    invocation: RunInvocationContext | None = None,
    interaction_mode: str = "observe_only",
) -> int:
    """Inner task execution logic, split out to allow foreground worker cleanup."""
    # For branchless task types, run without creating a branch.
    # Keep temporary "learn" compatibility for pre-migration rows.
    if task.task_type in (
        "explore",
        "plan",
        "plan_review",
        "plan_improve",
        "review",
        "internal",
        "learn",
    ):
        return _run_non_code_task(
            task,
            task_config,
            store,
            provider,
            git,
            resume=resume,
            open_after=open_after,
            invocation=invocation,
            interaction_mode=interaction_mode,
        )

    # Code tasks (implement/improve) require git
    assert git is not None, "git is required for code tasks"
    default_branch = git.default_branch()

    branch_name = _resolve_code_task_branch_name(task, config, store, git, resume=resume)
    if branch_name is None:
        return 1

    # Create worktree path
    assert task.slug is not None
    worktree_path = config.worktree_path / task.slug

    if not _setup_code_task_worktree(
        task,
        config,
        git,
        branch_name=branch_name,
        worktree_path=worktree_path,
        default_branch=default_branch,
        resume=resume,
    ):
        return 1

    # Create a Git instance for the worktree
    worktree_git = Git(worktree_path)

    # Restore WIP changes if resuming
    if resume:
        # When resuming via a new task (based_on points to the original failed task),
        # the WIP diff file was saved under the original task's task_id.
        original_task_id = None
        if task.based_on:
            original_task = store.get(task.based_on)
            if original_task:
                original_task_id = original_task.slug
        _restore_wip_changes(task, worktree_git, config, branch_name, original_task_id=original_task_id)

    # Persist branch early so it's available if the process is killed before completion
    task.branch = branch_name

    store.update(task)

    # Setup logging using the canonical task log path selected during preflight.
    if task.log_file:
        log_file = config.project_dir / Path(task.log_file)
    else:
        config.log_path.mkdir(parents=True, exist_ok=True)
        log_file = config.log_path / f"{task.slug}.log"
        task.log_file = str(log_file.relative_to(config.project_dir))
        store.update(task)

    # Write orchestration pre-run entries
    write_worker_start_event(log_file, resumed=resume)
    write_log_entry(log_file, {"type": "gza", "subtype": "info", "message": f"Task: {task.id} {task.slug}"})
    write_log_entry(log_file, {"type": "gza", "subtype": "branch", "message": f"Branch: {branch_name}", "branch": branch_name})
    write_log_entry(log_file, {"type": "gza", "subtype": "info", "message": f"Provider: {provider.name}, Model: {task_config.model or 'default'}"})
    write_execution_provenance_event(
        log_file,
        invocation=invocation or _resolve_default_invocation_context(),
        provider=provider,
        interaction_mode=interaction_mode,
        resumed=resume,
    )

    if skip_precondition_check and task.depends_on and not task.same_branch:
        write_log_entry(
            log_file,
            {
                "type": "gza",
                "subtype": "info",
                "message": (
                    f"Skipped dependency merge precondition check (--force) "
                    f"for depends_on task {task.depends_on}"
                ),
            },
        )
    else:
        blocking_dep, target_branch, precondition_error = _check_dependency_merge_precondition(
            task,
            store,
            git,
            default_branch=default_branch,
        )
        if precondition_error is not None:
            error_message(f"Git error: {precondition_error}")
            write_log_entry(
                log_file,
                {
                    "type": "gza",
                    "subtype": "outcome",
                    "message": precondition_error,
                    "failure_reason": "GIT_ERROR",
                },
            )
            _mark_task_failed(
                task=task,
                config=config,
                store=store,
                log_file=log_file,
                branch=branch_name,
                explicit_reason="GIT_ERROR",
                error_type=None,
                exit_code=1,
            )
            return 1
        if blocking_dep is not None:
            assert blocking_dep.id is not None
            dep_branch = blocking_dep.branch or "<none>"
            failure_message = (
                f"Dependency {blocking_dep.id} on branch '{dep_branch}' is not merged into "
                f"'{target_branch}'. Leaving task pending without provider run."
            )
            error_message(f"Error: {failure_message}")
            write_log_entry(
                log_file,
                {
                    "type": "gza",
                    "subtype": "blocked",
                    "message": failure_message,
                    "reason": "dependency_merge_precondition",
                    "dependency_task_id": blocking_dep.id,
                    "dependency_branch": dep_branch,
                    "target_branch": target_branch,
                    "task_status": "pending",
                },
            )
            _park_task_pending_after_blocked_precondition(task, store)
            return DEPENDENCY_BLOCKED_NOT_RUN_EXIT_CODE

    # Setup summary directory and path for task/implement types
    _, summary_path = get_task_output_paths(task, config.project_dir)
    assert summary_path is not None, f"Code task type '{task.task_type}' must have a summary path"
    summary_dir = summary_path.parent
    summary_dir.mkdir(parents=True, exist_ok=True)
    boundary = _project_boundary(config)
    provider_cwd = _worktree_execution_dir(worktree_path, boundary)

    # Create summary directory structure in worktree
    worktree_summary_dir = _worktree_path_for_project_path(config, worktree_path, summary_dir)
    worktree_summary_dir.mkdir(parents=True, exist_ok=True)
    worktree_summary_path = _worktree_path_for_project_path(config, worktree_path, summary_path)

    # For Docker containers, use /workspace-relative path instead of host worktree path
    # For native mode, use the actual worktree path
    if config.use_docker:
        prompt_summary_path = _container_path_for_project_path(config, summary_path)
    else:
        prompt_summary_path = worktree_summary_path

    def _on_session_id(session_id: str) -> None:
        """Persist session_id to the task record as soon as it is first seen.

        This ensures that even if the run is killed mid-stream (e.g. Ctrl+C),
        the session_id is already saved and ``gza resume`` can still work.
        """
        if task.session_id == session_id:
            return
        task.session_id = session_id
        store.update(task)

    def _on_step_count(count: int) -> None:
        """Update task.num_steps_computed in real time during streaming."""
        task.num_steps_computed = count
        store.update(task)

    # Ensure bundled skills and the readonly task DB snapshot are available at project scope.
    n_installed = _stage_worktree_agent_resources(config, worktree_path, boundary)
    if n_installed:
        console.print(f"Installed {n_installed} skill(s) into worktree")

    # Copy learnings file into worktree so the agent can read it
    _copy_learnings_to_worktree(config, worktree_path)

    task_config.provider_cwd = provider_cwd
    task_config.docker_workdir = str(_container_execution_dir(boundary))
    task_config.docker_volumes = _build_runtime_docker_volumes(config)

    if not config.use_docker:
        _create_local_dep_symlinks(config, worktree_path)

    seeded_paths: set[str] = set()
    try:
        extraction_seed = _seed_extraction_bundle_if_present(
            task,
            config,
            worktree_path,
            worktree_git,
            log_file,
            resume=resume,
        )
        seeded_paths = set(extraction_seed.seeded_paths)
    except (ExtractionError, GitError) as exc:
        failure_message = f"Extraction preflight/apply failed: {exc}"
        error_message(f"Error: {failure_message}")
        write_log_entry(
            log_file,
            {
                "type": "gza",
                "subtype": "outcome",
                "message": failure_message,
                "failure_reason": EXTRACTION_PRECHECK_FAILURE_REASON,
            },
        )
        _mark_task_failed(
            task=task,
            config=config,
            store=store,
            log_file=log_file,
            branch=branch_name,
            explicit_reason=EXTRACTION_PRECHECK_FAILURE_REASON,
            error_type=None,
            exit_code=1,
        )
        return 1

    if extraction_seed.completion_reason:
        write_log_entry(
            log_file,
            {
                "type": "gza",
                "subtype": "outcome",
                "message": "Outcome: completed without provider execution",
                "completion_reason": extraction_seed.completion_reason,
            },
        )
        store.mark_completed(
            task,
            branch=branch_name,
            log_file=str(log_file.relative_to(config.project_dir)),
            has_commits=False,
            completion_reason=extraction_seed.completion_reason,
        )
        return 0

    # Run provider in the worktree
    if resume:
        timeout_resume_context = None
        checkpoint_task_id: str | None = None
        if task.based_on:
            based_on_task = store.get(task.based_on)
            if based_on_task is not None and based_on_task.failure_reason == "TIMEOUT":
                checkpoint_task_id = based_on_task.id
        elif task.failure_reason == "TIMEOUT":
            checkpoint_task_id = task.id
        if checkpoint_task_id is not None and task.id is not None:
            timeout_resume_context = _build_timeout_resume_context(
                config=config,
                checkpoint_task_id=checkpoint_task_id,
                worktree_git=worktree_git,
            )
        prompt = PromptBuilder().resume_prompt(resume_context=timeout_resume_context)
    else:
        prompt = build_prompt(task, task_config, store, report_path=None, summary_path=prompt_summary_path, git=git)

    # Snapshot worktree state before provider runs so we can selectively stage only new changes
    pre_run_status = worktree_git.status_porcelain()
    task_logger = TaskExecutionLogger(resolve_ops_log_path(config, log_file), echo=True)
    timeout_budget = _resolve_task_timeout_budget(
        task=task,
        config=config,
        provider=task_config.provider,
        git=worktree_git,
        branch_name=branch_name,
        default_branch=default_branch,
        task_logger=task_logger,
    )
    task_config.timeout_minutes = timeout_budget.minutes
    timeout_message = f"Resolved timeout budget: {timeout_budget.minutes}m ({timeout_budget.reason})"
    task_logger.phase(
        timeout_message,
        extra={
            "event": "resolved_timeout_budget",
            "timeout_minutes": timeout_budget.minutes,
            "reason_detail": timeout_budget.reason,
            "diff_lines": timeout_budget.diff_lines,
            "diff_files": timeout_budget.diff_files,
        },
    )
    improve_diff_baseline: ImproveDiffBaseline | None = None
    rebase_diff_baseline: RebaseDiffBaseline | None = None
    if task.task_type == "rebase":
        rebase_diff_baseline = capture_rebase_diff_baseline(
            worktree_git,
            branch=branch_name,
            target=default_branch,
            recovered=_is_recovered_rebase_lineage(task, resume=resume),
        )
    if task.task_type == "improve":
        improve_diff_baseline = capture_improve_diff_baseline(
            worktree_git,
            branch=branch_name,
            target=default_branch,
            recovered=_is_recovered_improve_lineage(task, resume=resume),
        )
    fix_commits_ahead_before_run: int | None = None
    fix_default_branch: str | None = None
    if task.task_type == "fix":
        fix_default_branch = worktree_git.default_branch()
        try:
            fix_commits_ahead_before_run = worktree_git.count_commits_ahead(branch_name, fix_default_branch)
        except GitError:
            fix_commits_ahead_before_run = None

    try:
        provider_run_kwargs: dict[str, Any] = {
            "resume_session_id": task.session_id if resume else None,
            "on_session_id": _on_session_id,
            "on_step_count": _on_step_count,
        }
        if interaction_mode == "interactive":
            provider_run_kwargs["interactive"] = True
        if _provider_accepts_ops_log_file(provider):
            provider_run_kwargs["ops_log_file"] = resolve_ops_log_path(config, log_file)
        provider_prompt = sanitize_provider_prompt(prompt, task_type=task.task_type)
        result = _call_provider_run(
            provider,
            task_config,
            provider_prompt,
            log_file,
            worktree_path,
            provider_run_kwargs=provider_run_kwargs,
        )
        _apply_transcript_stats_fallback(
            result,
            log_file=log_file,
            provider_name=provider.name,
            configured_model=task_config.model or None,
            prefer_transcript_usage=result.exit_code == 124,
        )

        exit_code = result.exit_code
        stats = _run_result_to_stats(result)
        assert task.id is not None
        has_step_events = _persist_run_steps_from_result(store, task.id, provider.name.lower(), result)
        if has_step_events:
            task.log_schema_version = 2

        # Store session_id if available and not already persisted by _on_session_id callback
        if result.session_id and result.session_id != task.session_id:
            task.session_id = result.session_id
            store.update(task)

        resolved_failure = _resolve_run_failure(
            provider_name=provider.name,
            timeout_minutes=task_config.timeout_minutes,
            step_limit=task_config.max_steps,
            turn_limit=task_config.max_turns,
            error_type=result.error_type,
            exit_code=exit_code,
            log_file=log_file,
            stats=stats,
        )

        if resolved_failure is not None:
            # Save WIP changes before marking failed
            pre_save_tree_fingerprint = None
            if resolved_failure.reason == "TIMEOUT" and task.id is not None:
                pre_save_tree_fingerprint = _compute_tree_fingerprint(worktree_git)
            wip_state = _save_wip_changes(task, worktree_git, config, branch_name)
            if resolved_failure.reason == "TIMEOUT" and task.id is not None:
                _persist_timeout_resume_checkpoint(
                    config=config,
                    task_id=task.id,
                    log_file=log_file,
                    worktree_git=worktree_git,
                    wip_state=wip_state,
                    task_logger=task_logger,
                    pre_save_tree_fingerprint=pre_save_tree_fingerprint,
                )
            _record_run_failure(
                task=task,
                config=config,
                store=store,
                log_file=log_file,
                stats=stats,
                failure=resolved_failure,
                exit_code=exit_code,
                branch=branch_name,
            )
            return 0

        if task.task_type == "rebase":
            if is_rebase_in_progress(worktree_git.repo_dir):
                task_logger.error("Rebase still in progress after provider success.")
                _save_wip_changes(task, worktree_git, config, branch_name)
                _record_run_failure(
                    task=task,
                    config=config,
                    store=store,
                    log_file=log_file,
                    stats=stats,
                    failure=_git_error_failure(),
                    exit_code=1,
                    branch=branch_name,
                )
                return 0

        return _complete_code_task(
            task,
            config,
            store,
            worktree_git,
            log_file,
            branch_name,
            stats,
            exit_code,
            pre_run_status,
            worktree_summary_path,
            summary_path,
            summary_dir,
            target_branch=default_branch,
            skip_commit=task.task_type == "rebase",
            create_pr=create_pr,
            fix_commits_ahead_before_run=fix_commits_ahead_before_run,
            fix_default_branch=fix_default_branch,
            seeded_paths=seeded_paths,
            improve_diff_baseline=improve_diff_baseline,
            rebase_diff_baseline=rebase_diff_baseline,
            error_type=result.error_type,
        )

    except GitError as e:
        error_message(f"Git error: {e}")
        _record_run_failure(
            task=task,
            config=config,
            store=store,
            log_file=log_file,
            stats=stats,
            branch=branch_name,
            failure=_git_error_failure(),
            exit_code=1,
        )
        return 1
    except KeyboardInterrupt:
        failure_reason = _resolve_failure_reason(
            interrupt_signal=_interrupt_signal_name(),
            interrupted=True,
            error_type=None,
            exit_code=None,
            log_file=None,
        )
        interrupt_metadata = _interruption_metadata()
        # Save WIP changes before returning
        _save_wip_changes(task, worktree_git, config, branch_name)
        write_log_entry(
            log_file,
            {
                "type": "gza",
                "subtype": "interrupt",
                "message": "Task interrupted by signal",
                "failure_reason": failure_reason,
                **interrupt_metadata,
            },
        )
        _mark_task_failed(
            task=task,
            config=config,
            store=store,
            log_file=log_file,
            branch=branch_name,
            interrupt_signal=_interrupt_signal_name(),
            interrupted=True,
            error_type=None,
            exit_code=None,
        )
        console.print("\nInterrupted")
        return 130


def _run_non_code_task(
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    provider: Provider,
    git: Git | None = None,
    resume: bool = False,
    open_after: bool = False,
    invocation: RunInvocationContext | None = None,
    interaction_mode: str = "observe_only",
) -> int:
    """Run a branchless task in a worktree without branch creation.

    Args:
        task: Task to run
        config: Configuration object
        store: Task store
        provider: AI provider
        git: Git instance for the main repository
        resume: If True, resume from previous session
        open_after: If True, open the report file in $EDITOR after completion
    """
    if resume and task.session_id:
        console.print(f"Resuming with session: [dim]{task.session_id[:12]}...[/dim]")

    # Setup logging using the canonical task log path selected during preflight.
    if task.log_file:
        log_file = config.project_dir / Path(task.log_file)
    else:
        config.log_path.mkdir(parents=True, exist_ok=True)
        log_file = config.log_path / f"{task.slug}.log"
        task.log_file = str(log_file.relative_to(config.project_dir))
        store.update(task)

    # Write orchestration pre-run entries
    write_worker_start_event(log_file, resumed=resume)
    write_log_entry(log_file, {"type": "gza", "subtype": "info", "message": f"Task: {task.id} {task.slug}"})
    write_log_entry(log_file, {"type": "gza", "subtype": "info", "message": f"Provider: {provider.name}, Model: {config.model or 'default'}"})
    write_execution_provenance_event(
        log_file,
        invocation=invocation or _resolve_default_invocation_context(),
        provider=provider,
        interaction_mode=interaction_mode,
        resumed=resume,
    )

    # Setup report file based on task type
    report_path, _ = get_task_output_paths(task, config.project_dir)
    assert report_path is not None, f"Non-code task type '{task.task_type}' must have a report path"
    report_path.parent.mkdir(parents=True, exist_ok=True)

    report_file_relative = str(report_path.relative_to(config.project_dir))

    # Create worktree in /tmp for Docker compatibility on macOS
    assert task.slug is not None
    worktree_path = config.worktree_path / f"{task.slug}-{task.task_type}"

    try:
        # Get default branch to base worktree on
        default_branch = git.default_branch() if git else "main"

        # Remove existing worktree if it exists
        if worktree_path.exists() and git:
            git.worktree_remove(worktree_path, force=True)

        base_ref, reviewed_branch = _resolve_review_base_ref(task, store, git, default_branch)
        if reviewed_branch:
            console.print(f"Running review on implementation branch: [blue]{reviewed_branch}[/blue]")

        # Create worktree without creating a new branch (use --detach to check out HEAD)
        # This creates a worktree in detached HEAD state based on the specified ref
        console.print(f"Creating worktree: {worktree_path}")
        _create_detached_review_worktree(git, worktree_path, base_ref)
        worktree_git = Git(worktree_path)

    # Create report directory structure in worktree
        boundary = _project_boundary(config)
        provider_cwd = _worktree_execution_dir(worktree_path, boundary)
        worktree_report_dir = _worktree_path_for_project_path(config, worktree_path, report_path.parent)
        worktree_report_dir.mkdir(parents=True, exist_ok=True)
        worktree_report_path = _worktree_path_for_project_path(config, worktree_path, report_path)

        # For Docker containers, use /workspace-relative path instead of host worktree path
        # The container only has /workspace mounted, so we need to use a path inside that
        # For native mode, use the actual worktree path
        if config.use_docker:
            prompt_report_path = _container_path_for_project_path(config, report_path)
        else:
            prompt_report_path = worktree_report_path

        # Ensure bundled skills and the readonly task DB snapshot are available at project scope.
        n_installed = _stage_worktree_agent_resources(config, worktree_path, boundary)
        if n_installed:
            console.print(f"Installed {n_installed} skill(s) into worktree")

        # Internal orchestration tasks do not implicitly consume learnings context.
        if task.task_type not in ("internal", "learn"):
            _copy_learnings_to_worktree(config, worktree_path)

        config.provider_cwd = provider_cwd
        config.docker_workdir = str(_container_execution_dir(boundary))
        config.docker_volumes = _build_runtime_docker_volumes(config)

        if not config.use_docker:
            _create_local_dep_symlinks(config, worktree_path)

        task_logger = TaskExecutionLogger(resolve_ops_log_path(config, log_file), echo=True)
        timeout_message = (
            f"Resolved timeout budget: {config.timeout_minutes}m "
            f"(base timeout for task type '{task.task_type}')"
        )
        task_logger.phase(
            timeout_message,
            extra={
                "event": "resolved_timeout_budget",
                "timeout_minutes": config.timeout_minutes,
                "reason_detail": f"base timeout for task type '{task.task_type}'",
            },
        )

        # Run provider in the worktree
        if resume:
            prompt = PromptBuilder().resume_prompt(
                task_id=task.id,
                task_slug=task.slug,
                report_path=prompt_report_path,
            )
        else:
            review_verify_result: ReviewVerifyResult | None = None
            review_verify_markdown: str | None = None
            verify_command = config.verify_command if isinstance(config.verify_command, str) else ""
            should_run_review_verify = task.task_type == "review" and (
                _task_is_cross_project(task) or verify_command.strip()
            )
            if should_run_review_verify:
                review_verify_timeout_seconds = getattr(
                    config,
                    "review_verify_timeout_seconds",
                    REVIEW_VERIFY_TIMEOUT_SECONDS,
                )
                if not isinstance(review_verify_timeout_seconds, int) or review_verify_timeout_seconds < 1:
                    review_verify_timeout_seconds = REVIEW_VERIFY_TIMEOUT_SECONDS
                reviewed_head_sha = worktree_git.rev_parse_if_exists("HEAD")
                reviewed_base_sha = _resolve_review_verify_base_sha(git, default_branch)
                if reviewed_head_sha is None:
                    review_verify_result = _make_review_verify_result(
                        verify_command.strip() or "(review verify unavailable)",
                        status="unavailable",
                        exit_status="unresolved head",
                        captured_at=datetime.now(UTC),
                        reviewed_branch=reviewed_branch,
                        reviewed_head_sha=None,
                        reviewed_base_sha=reviewed_base_sha,
                        working_directory=str(provider_cwd),
                        failure="unable to resolve review worktree HEAD before verify_command ran",
                    )
                    review_verify_markdown = _format_review_verify_result(review_verify_result)
                else:
                    persisted_project_results: tuple[ProjectReviewVerifyResult, ...] = ()
                    if _task_is_cross_project(task):
                        cross_project_verify = _run_review_verify_commands_for_projects(
                            config=config,
                            task=task,
                            worktree_git=worktree_git,
                            worktree_path=worktree_path,
                            timeout_seconds=review_verify_timeout_seconds,
                            reviewed_branch=reviewed_branch,
                            reviewed_head_sha=reviewed_head_sha,
                            reviewed_base_sha=reviewed_base_sha,
                        )
                        if cross_project_verify is not None:
                            review_verify_markdown = cross_project_verify.markdown
                            review_verify_result = cross_project_verify.aggregate_result
                            persisted_project_results = cross_project_verify.project_results
                    elif verify_command.strip():
                        review_verify_result = _run_review_verify_command(
                            verify_command.strip(),
                            cwd=provider_cwd,
                            reviewed_branch=reviewed_branch,
                            reviewed_head_sha=reviewed_head_sha,
                            reviewed_base_sha=reviewed_base_sha,
                            timeout_seconds=review_verify_timeout_seconds,
                        )
                        review_verify_markdown = _format_review_verify_result(review_verify_result)
                if review_verify_result is not None:
                    assert review_verify_markdown is not None
                    _capture_review_verify_result(
                        config,
                        store,
                        task,
                        review_verify_result,
                        markdown=review_verify_markdown,
                        project_results=persisted_project_results if reviewed_head_sha is not None else (),
                        task_logger=task_logger,
                    )
            prompt = build_prompt(
                task,
                config,
                store,
                report_path=prompt_report_path,
                git=git,
                review_verify_result=review_verify_markdown,
            )

        def _on_session_id_non_code(session_id: str) -> None:
            """Persist session_id as soon as it is first seen during streaming."""
            if task.session_id == session_id:
                return
            task.session_id = session_id
            store.update(task)

        def _on_step_count_non_code(count: int) -> None:
            """Update task.num_steps_computed in real time during streaming."""
            task.num_steps_computed = count
            store.update(task)

        # When running in Docker, the worktree .git file contains a host-specific
        # gitdir path that is invalid inside the container.  Hide it before the
        # provider run and restore it afterwards so the host worktree stays valid.
        host_git_file = worktree_path / ".git"
        hidden_git_file = worktree_path / ".git.gza-host-worktree"
        hide_git = config.use_docker and host_git_file.is_file()
        if hide_git:
            host_git_file.rename(hidden_git_file)

        try:
            provider_run_kwargs: dict[str, Any] = {
                "resume_session_id": task.session_id if resume else None,
                "on_session_id": _on_session_id_non_code,
                "on_step_count": _on_step_count_non_code,
            }
            if interaction_mode == "interactive":
                provider_run_kwargs["interactive"] = True
            if _provider_accepts_ops_log_file(provider):
                provider_run_kwargs["ops_log_file"] = resolve_ops_log_path(config, log_file)
            provider_prompt = sanitize_provider_prompt(prompt, task_type=task.task_type)
            result = _call_provider_run(
                provider,
                config,
                provider_prompt,
                log_file,
                worktree_path,
                provider_run_kwargs=provider_run_kwargs,
            )
            _apply_transcript_stats_fallback(
                result,
                log_file=log_file,
                provider_name=provider.name,
                configured_model=config.model or None,
                prefer_transcript_usage=result.exit_code == 124,
            )
        except KeyboardInterrupt:
            failure_reason = _resolve_failure_reason(
                interrupt_signal=_interrupt_signal_name(),
                interrupted=True,
                error_type=None,
                exit_code=None,
                log_file=None,
            )
            interrupt_metadata = _interruption_metadata()
            write_log_entry(
                log_file,
                {
                    "type": "gza",
                    "subtype": "interrupt",
                    "message": "Task interrupted by signal",
                    "failure_reason": failure_reason,
                    **interrupt_metadata,
                },
            )
            _mark_task_failed(
                task=task,
                config=config,
                store=store,
                log_file=log_file,
                interrupt_signal=_interrupt_signal_name(),
                interrupted=True,
                error_type=None,
                exit_code=None,
            )
            console.print("\nInterrupted")
            return 130
        finally:
            if hide_git and hidden_git_file.exists():
                hidden_git_file.rename(host_git_file)
        exit_code = result.exit_code
        stats = _run_result_to_stats(result)
        assert task.id is not None
        has_step_events = _persist_run_steps_from_result(store, task.id, provider.name.lower(), result)
        if has_step_events:
            task.log_schema_version = 2

        # Store session_id if available and not already persisted by _on_session_id_non_code callback
        if result.session_id and result.session_id != task.session_id:
            task.session_id = result.session_id
            store.update(task)

        resolved_failure = _resolve_run_failure(
            provider_name=provider.name,
            timeout_minutes=config.timeout_minutes,
            step_limit=config.max_steps,
            turn_limit=config.max_turns,
            error_type=result.error_type,
            exit_code=exit_code,
            log_file=log_file,
            stats=stats,
        )

        if resolved_failure is not None:
            _record_run_failure(
                task=task,
                config=config,
                store=store,
                log_file=log_file,
                stats=stats,
                failure=resolved_failure,
                exit_code=exit_code,
                worktree=worktree_path,
            )
            return 0

        # Copy expected report artifact from worktree to main project directory.
        # For non-code tasks, provider success requires this file contract.
        recovered_from_log = False
        if not worktree_report_path.exists():
            # Before failing, try to recover content from the provider's 'result' log entry.
            # Agents sometimes output the review/report as text rather than writing the file.
            recovered_content = extract_content_from_log(log_file)
            if recovered_content:
                logger.warning(
                    "Task %s: expected report artifact missing; recovering content from provider log",
                    task.slug,
                )
                console.print(
                    "[yellow]Warning: expected report artifact was not created; "
                    "recovering content from provider log[/yellow]"
                )
                # Write the recovered content into the worktree path so the copy-back
                # logic below proceeds as if the agent had written it normally.
                worktree_report_path.write_text(recovered_content)
                recovered_from_log = True
            else:
                expected_relative = str(worktree_report_path.relative_to(worktree_path))
                stale_candidates = sorted(
                    path.relative_to(worktree_path)
                    for path in worktree_report_dir.glob("*.md")
                    if path != worktree_report_path
                )
                if len(stale_candidates) == 1:
                    actual_relative = stale_candidates[0]
                    logger.warning(
                        "Task %s: expected report artifact %s missing; recovering from mismatched file %s",
                        task.slug,
                        expected_relative,
                        actual_relative,
                    )
                    console.print(
                        "[yellow]Warning: expected report artifact "
                        f"{expected_relative} was not created; recovering from mismatched file "
                        f"{actual_relative}[/yellow]"
                    )
                    recovered_content = (worktree_path / actual_relative).read_text()
                    worktree_report_path.write_text(recovered_content)
                else:
                    mismatch_note = (
                        f" (found other report files: {', '.join(str(p) for p in stale_candidates)})"
                        if stale_candidates
                        else ""
                    )
                    failure_message = (
                        f"Outcome: failed (missing report artifact: expected {expected_relative}{mismatch_note})"
                    )
                    console.print(f"Expected report file: [yellow]{report_file_relative}[/yellow]")
                    if stale_candidates:
                        console.print(
                            "Detected report files with other names in worktree "
                            f"(possible stale resume session state): {', '.join(str(p) for p in stale_candidates)}"
                        )
                    console.print(f"See log file for details: {log_file.relative_to(config.project_dir)}")
                    task_footer(
                        task,
                        stats,
                        status="Failed: expected report artifact was not created",
                        worktree=worktree_path,
                        store=store,
                    )
                    write_log_entry(
                        log_file,
                        {
                            "type": "gza",
                            "subtype": "outcome",
                            "message": failure_message,
                            "exit_code": exit_code,
                            "failure_reason": "MISSING_REPORT_ARTIFACT",
                        },
                    )
                    _write_stats_entry(log_file, stats)
                    _mark_task_failed(
                        task=task,
                        config=config,
                        store=store,
                        log_file=log_file,
                        stats=stats,
                        explicit_reason="MISSING_REPORT_ARTIFACT",
                        error_type=None,
                        exit_code=exit_code,
                    )
                    return 0

        console.print(f"Report written to: {report_file_relative}")
        # Ensure target directory exists
        report_path.parent.mkdir(parents=True, exist_ok=True)
        # Copy report content from worktree to project dir
        report_path.write_text(worktree_report_path.read_text())

        # Read output content for storage in DB
        output_content = report_path.read_text()
        if task.task_type == "review":
            task.review_score = compute_review_score(parse_review_template(output_content))
            contract_validation = validate_review_report_contract(output_content)
            if contract_validation.blockers_missing_open_state_citation:
                blocker_ids = ", ".join(contract_validation.blockers_missing_open_state_citation)
                warning_message = (
                    f"Review contract warning: blockers missing open-state citations: {blocker_ids}"
                )
                logger.warning(warning_message)
                console.print(f"[yellow]{warning_message}[/yellow]")
            if contract_validation.blockers_with_malformed_open_state_citation:
                blocker_ids = ", ".join(contract_validation.blockers_with_malformed_open_state_citation)
                warning_message = (
                    "Review contract warning: blockers with malformed open-state citations: "
                    f"{blocker_ids}"
                )
                logger.warning(warning_message)
                console.print(f"[yellow]{warning_message}[/yellow]")

        # Clean up non-code worktree on success — report has been copied back, no further use
        if git:
            try:
                git.worktree_remove(worktree_path, force=True)
                if worktree_path.exists():
                    shutil.rmtree(worktree_path, ignore_errors=True)
            except GitError:
                logger.warning("Failed to remove worktree %s", worktree_path)
                if worktree_path.exists():
                    shutil.rmtree(worktree_path, ignore_errors=True)

        # Write final log entries before marking completed in DB, so that
        # `gza log -f` doesn't break out of the follow loop prematurely.
        outcome_msg = "Outcome: completed (recovered from provider log)" if recovered_from_log else "Outcome: completed"
        write_log_entry(log_file, {"type": "gza", "subtype": "outcome", "message": outcome_msg, "exit_code": 0})
        write_log_entry(log_file, {"type": "gza", "subtype": "stats", "message": f"Stats: {stats.num_steps_computed or stats.num_steps_reported or 0} steps, {stats.duration_seconds or 0.0:.1f}s, ${stats.cost_usd or 0.0:.4f}", "duration_seconds": stats.duration_seconds, "cost_usd": stats.cost_usd, "num_steps": stats.num_steps_computed or stats.num_steps_reported or 0})

        # Mark completed — after log entries are flushed.
        store.mark_completed(
            task,
            branch=None,
            log_file=str(log_file.relative_to(config.project_dir)),
            report_file=report_file_relative,
            output_content=output_content,
            has_commits=False,
            stats=stats,
        )
        auto_learnings = None
        if task.task_type not in ("internal", "learn") and not task.skip_learnings:
            auto_learnings = maybe_auto_regenerate_learnings(store, config)

        # For review tasks, post to PR if applicable
        if task.task_type == "review" and task.depends_on:
            impl_task = store.get(task.depends_on)
            if impl_task:
                post_review_to_pr(
                    task,
                    impl_task,
                    store,
                    config.project_dir,
                    pr_integration=config.pr_integration,
                    required=False,
                )

        verdict: str | None = None
        if task.task_type == "review":
            verdict = _extract_review_verdict(output_content)

        console.print("")
        task_footer(
            task,
            stats,
            status="Done",
            report=report_file_relative,
            verdict=verdict,
            learnings=auto_learnings,
            store=store,
        )

        # Open review file in $EDITOR if requested
        if open_after and task.task_type == "review" and report_path.exists():
            import os
            import subprocess

            editor = os.environ.get("EDITOR")
            if editor:
                try:
                    console.print(f"\nOpening review in {editor}...")
                    subprocess.run([editor, str(report_path)], check=True)
                except subprocess.CalledProcessError as e:
                    console.print(f"[yellow]Warning: Failed to open editor: {e}[/yellow]")
                except FileNotFoundError:
                    console.print(f"[yellow]Warning: Editor '{editor}' not found[/yellow]")
            else:
                console.print("[yellow]Warning: $EDITOR not set, skipping auto-open[/yellow]")

        return 0

    except GitError as e:
        error_message(f"Git error: {e}")
        _mark_task_failed(
            task=task,
            config=config,
            store=store,
            log_file=log_file,
            explicit_reason="GIT_ERROR",
            error_type=None,
            exit_code=1,
        )
        return 1
