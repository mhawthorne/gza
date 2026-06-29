"""CLI commands for querying and displaying task state.

Covers: next, history, unmerged, ps, kill, delete, show, attach.
"""

import argparse
import contextlib
import datetime as _dt
import json
import os
import select
import shlex
import shutil
import signal
import sqlite3
import subprocess
import sys
import termios
import time
import tty
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol, cast

from rich.markup import escape as rich_escape

import gza.colors as _colors

from ..artifact_paths import InvalidArtifactPathError, resolve_artifact_path
from ..colors import PS_STATUS_COLORS, SHOW_COLORS_DICT
from ..config import Config
from ..console import (
    MAX_PROMPT_DISPLAY,
    build_console,
    console,
    prompt_available_width,
    shorten_prompt,
    truncate,
)
from ..db import (
    MergeUnit,
    SqliteTaskStore,
    Task as DbTask,
    TaskArtifact,
    _is_readonly_snapshot_operational_error,
    task_id_numeric_key as _task_id_numeric_key,
    task_owns_merge_status,
)
from ..dependency_preconditions import DependencyReadiness
from ..failure_reasons import mark_task_failed_from_cause
from ..git import Git, GitError, active_worktree_path_for_branch
from ..github import GitHub
from ..lifecycle_completion import TERMINAL_MERGE_STATES
from ..lineage import resolve_lineage_root as _resolve_lineage_root_task, walk_based_on_descendants
from ..lineage_query import (
    StaleUnmergedSweepCandidate,
    collect_stale_unmerged_sweep_candidates,
    filter_display_unresolved_tasks_for_incomplete,
)
from ..operator_state import (
    blocked_by_empty_prereq_label,
    blocked_dependency_label,
    effective_no_work_merge_state,
    moot_empty_lifecycle_detail,
)
from ..pr_ops import lookup_task_pr
from ..query import (
    _LINEAGE_REL_LABELS as _QUERY_LINEAGE_REL_LABELS,
    TaskLineageNode,
    _classify_child_relationship as _classify_lineage_child_relationship,
    _lineage_child_sort_key as _lineage_child_sort_key,
    build_ancestor_forest as _build_ancestor_forest_for_task,
    build_lineage_tree as _build_lineage_tree_for_root,
    flatten_lineage_tree as _flatten_query_lineage_tree,
    get_code_changing_descendants_for_root as _get_code_changing_descendants_for_root_task,
    get_reviews_for_root as _get_reviews_for_root_task,
)
from ..review_verify_state import (
    VerifyReadModel,
    owner_task_verify_epoch,
    read_verify_output_excerpt,
    resolve_verify_owner_task,
    resolve_verify_read_model,
    verify_output_artifact_path,
)
from ..runner import _get_task_output, get_effective_config_for_task, write_log_entry
from ..status_ops import apply_manual_task_status
from ..sync_ops import (
    BranchCohort,
    BranchSyncResult,
    build_branch_cohorts_for_tasks,
    build_unmerged_branch_cohorts,
    reconcile_branch_merge_truth,
    sync_branch_cohorts,
)
from ..task_query import (
    DateFilter as _TaskDateFilter,
    LineageRow as _LineageRow,
    PresentationSpec as _TaskPresentationSpec,
    ProjectionSpec as _TaskProjectionSpec,
    TaskProjectionPreset as _TaskProjectionPreset,
    TaskQuery as _TaskQuery,
    TaskQueryPresets as _TaskQueryPresets,
    TaskQueryResult as _TaskQueryResult,
    TaskQueryService as _TaskQueryService,
    TaskRow as _TaskRow,
    apply_projection_values as _apply_query_projection_values,
    normalize_tag_filters,
    parse_csv as _parse_csv,
    projection_fields as _projection_fields,
    task_matches_tag_filters,
)
from ..workers import WorkerMetadata, WorkerRegistry
from ._common import (
    TASK_COLORS,
    _build_failure_diagnostics,
    _failure_next_steps,
    _format_lineage,
    _lineage_tree_prefix,
    _parse_iso,
    _render_failure_diagnostics,
    _resolve_task_log_path,
    _spawn_background_worker,
    format_stats,
    format_task_merge_label,
    format_task_status_text,
    get_review_score,
    get_review_verdict,
    get_store,
    get_task_status_color,
    pager_context,
    parse_cli_tag_filters,
    resolve_effective_plan_review_manifest_state,
    resolve_id,
    validate_cli_tag_values,
)
from ._lifecycle_actions import collect_lifecycle_action_entries, print_lifecycle_action_entries
from ._queue_render import (
    QueueRenderRow as _QueueRenderRow,
    build_blocked_count_summary as _build_blocked_count_summary,
    build_queue_summary as _build_queue_summary,
    format_quiet_available_at as _format_quiet_available_at,
    partition_queue_rows as _partition_queue_rows,
    print_queue_rows as _print_queue_rows,
    queue_render_widths as _queue_render_widths,
)
from ._recovery_lane import RecoveryLaneEntry, collect_recovery_lane_entries
from .advance_engine import (
    _resolve_subject_fallback_task,
    classify_advance_action,
    determine_next_action,
    format_needs_attention_entry_for_display,
    format_needs_attention_lifecycle,
    get_action_subject_task_id,
    resolve_advance_context,
    resolve_subject_task,
)

_LINEAGE_REL_LABELS = _QUERY_LINEAGE_REL_LABELS
_QueryDateField = Literal["created", "completed", "effective"]
_PresentationMode = Literal["flat", "blocks", "grouped", "lineage", "tree", "one_line", "json", "rich"]
_stderr_console = build_console(highlight=False, stderr=True)
_HISTORY_PROJECTION_FIELDS: tuple[str, ...] = _projection_fields(
    _TaskProjectionSpec(preset=_TaskProjectionPreset.HISTORY_DEFAULT),
    scope="tasks",
)
_SEARCH_PROJECTION_FIELDS: tuple[str, ...] = _projection_fields(
    _TaskProjectionSpec(preset=_TaskProjectionPreset.SEARCH_DEFAULT),
    scope="tasks",
)
_TASK_FIELDS_WITHOUT_NEXT_ACTION: tuple[str, ...] = tuple(
    field_name
    for field_name in _HISTORY_PROJECTION_FIELDS
    if field_name not in {"next_action", "next_action_reason", "next_action_owner_id"}
)
_TASK_EXPLICIT_PROJECTION_FIELDS: tuple[str, ...] = (
    *_TASK_FIELDS_WITHOUT_NEXT_ACTION,
    "trigger_source",
    "verify_status",
    "verify_exit_status",
    "verify_captured_at",
    "verify_branch",
    "verify_head_sha",
    "verify_base_sha",
    "verify_working_directory",
    "verify_failure",
    "verify_artifact_path",
    "verify_source",
    "verify_current",
    "verify_has_owner_artifact",
)
_HISTORY_EXPLICIT_PROJECTION_FIELDS: tuple[str, ...] = _TASK_EXPLICIT_PROJECTION_FIELDS
_SEARCH_EXPLICIT_PROJECTION_FIELDS: tuple[str, ...] = _TASK_EXPLICIT_PROJECTION_FIELDS
_INCOMPLETE_PROJECTION_FIELDS: tuple[str, ...] = _projection_fields(
    _TaskProjectionSpec(preset=_TaskProjectionPreset.INCOMPLETE_SUMMARY),
    scope="lineages",
)
_INCOMPLETE_PROJECTION_FIELDS = (
    *_INCOMPLETE_PROJECTION_FIELDS,
    "trigger_source",
    "verify_status",
    "verify_exit_status",
    "verify_captured_at",
    "verify_branch",
    "verify_head_sha",
    "verify_base_sha",
    "verify_working_directory",
    "verify_failure",
    "verify_artifact_path",
    "verify_source",
    "verify_current",
    "verify_has_owner_artifact",
)
_INCOMPLETE_BLOCKED_DROPPED_PROJECTION_FIELDS: tuple[str, ...] = (
    "id",
    "prompt",
    "status",
    "task_type",
    "blocked",
    "blocking_id",
    "blocking_status",
)
_UNMERGED_PROJECTION_FIELDS: tuple[str, ...] = _projection_fields(
    _TaskProjectionSpec(preset=_TaskProjectionPreset.UNMERGED_DEFAULT),
    scope="lineages",
)
_MERGED_PROJECTION_FIELDS: tuple[str, ...] = (
    "merge_unit_id",
    "owner_task_id",
    "merge_source",
    "merged_at",
    "branch",
    "target_branch",
)

_INCOMPLETE_DEPRECATION_LINES: tuple[str, ...] = (
    "Error: `gza incomplete` is deprecated and no longer supported.",
    "",
    "Use these dedicated surfaces instead:",
    "  `uv run gza unmerged` for unmerged code work",
    "  `uv run gza advance --unimplemented` for completed plan/explore work without implementation",
    "  `uv run gza history --status failed` for factual failed-task history",
    "  `uv run gza watch --recovery-only --dry-run` for failed-task recovery decisions",
    "  `uv run gza next` / `uv run gza next --all` for pending and blocked queue state",
    "  `/gza-summary` for synthesized operator triage and next-step guidance",
    "",
    "For dropped-dependency blockers, use `uv run gza next --all`.",
    "After `gza unimplemented` ships, it will replace the temporary `advance --unimplemented` spelling.",
)

_SHOW_STATUS_COLOR_KEYS: dict[str, str] = {
    "pending": "status_pending",
    "in_progress": "status_running",
    "completed": "status_completed",
    "failed": "status_failed",
    "unmerged": "status_pending",
    "dropped": "status_failed",
}


@dataclass(frozen=True)
class _ResolvedTaskArtifact:
    """Resolved latest-artifact metadata for read-only operator surfaces."""

    artifact: TaskArtifact
    path: Path | None
    invalid_path_error: str | None = None

    def retrieval_error(self, *, task_id: str) -> str | None:
        if self.invalid_path_error is not None:
            return (
                f"Artifact {self.artifact.id} for task {task_id} has an invalid stored path: "
                f"{self.invalid_path_error}"
            )
        if self.artifact.byte_size == 0:
            return f"Latest artifact {self.artifact.id} for task {task_id} has no content file"
        if self.path is None or not self.path.exists():
            return (
                f"Artifact {self.artifact.id} for task {task_id} is missing on disk: "
                f"{self.artifact.path}"
            )
        return None


def _format_task_artifact_summary(artifact: TaskArtifact, *, config: Config) -> str:
    """Render one compact artifact metadata line for operator surfaces."""
    created_text = artifact.created_at.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    detail_parts = [artifact.kind]
    if artifact.status:
        detail_parts.append(artifact.status)
    if artifact.exit_status:
        detail_parts.append(f"exit={artifact.exit_status}")
    detail_parts.append(f"created={created_text}")
    detail_parts.append(f"path={artifact.path}")
    try:
        resolved_path = resolve_artifact_path(config.project_dir, artifact.path)
    except InvalidArtifactPathError:
        detail_parts.append("invalid-path")
    else:
        if not resolved_path.exists():
            detail_parts.append("missing")
    return " ".join(detail_parts)


def _latest_task_artifact(
    store: SqliteTaskStore,
    task_id: str,
    *,
    kind: str | None = None,
) -> TaskArtifact | None:
    """Return the newest matching artifact row for one task."""
    artifacts = store.list_artifacts(task_id, kind=kind)
    if not artifacts:
        return None
    return artifacts[0]


def _resolve_latest_task_artifact(
    store: SqliteTaskStore,
    config: Config,
    task_id: str,
    *,
    kind: str | None = None,
) -> _ResolvedTaskArtifact | None:
    """Resolve the newest matching artifact row and any managed on-disk path."""
    artifact = _latest_task_artifact(store, task_id, kind=kind)
    if artifact is None:
        return None
    try:
        artifact_path = resolve_artifact_path(config.project_dir, artifact.path)
    except InvalidArtifactPathError as exc:
        return _ResolvedTaskArtifact(artifact=artifact, path=None, invalid_path_error=str(exc))
    return _ResolvedTaskArtifact(artifact=artifact, path=artifact_path)


def _show_status_color(task: DbTask, colors: dict[str, str]) -> str:
    return colors.get(_SHOW_STATUS_COLOR_KEYS.get(task.status or "", "status_default"), colors["status_default"])


def _lineage_has_descendants(lineage_tree: TaskLineageNode) -> bool:
    return bool(lineage_tree.children)


_LifecycleSeverity = Literal["default", "running", "completed", "failed"]


@dataclass(frozen=True)
class _LifecycleSummary:
    text: str
    severity: _LifecycleSeverity


def _with_recovered_lifecycle_prefix(detail: str, *, recovered: bool, severity: _LifecycleSeverity) -> _LifecycleSummary:
    return _LifecycleSummary(f"recovered, {detail}" if recovered else detail, severity)


def _format_changed_diff_label(changed_diff: bool | None) -> str:
    if changed_diff is False:
        return "no"
    if changed_diff is True:
        return "yes"
    return "unknown (treated as yes)"


def _implementation_review_rebase_detail(
    task: DbTask,
    *,
    config: Config,
    store: SqliteTaskStore,
) -> str | None:
    if task.task_type != "implement" or not task.branch:
        return None
    try:
        git = Git(config.project_dir)
        target_branch = git.default_branch()
        ctx = resolve_advance_context(
            config,
            store,
            git,
            task,
            target_branch,
            persist_post_merge_rebase_state=False,
            persist_review_clearance=False,
        )
    except (GitError, OSError, ValueError):
        return None

    if ctx.review_preserved_by_rebase is not None and ctx.review_verdict in {"APPROVED", "APPROVED_WITH_FOLLOWUPS"}:
        return f"{ctx.review_verdict} (carried across rebase {ctx.review_preserved_by_rebase.id})"
    if ctx.review_invalidated_by_rebase is not None:
        reason = "diff changed" if ctx.review_invalidated_by_rebase.changed_diff is True else "change unknown"
        return f"invalidated by rebase {ctx.review_invalidated_by_rebase.id} ({reason})"
    return None


def _plan_review_source_task(store: SqliteTaskStore, task: DbTask) -> DbTask | None:
    """Resolve the reviewed plan source for one plan_review task."""
    if task.task_type != "plan_review" or not task.depends_on:
        return None
    source_task = store.get(task.depends_on)
    if source_task is None or source_task.task_type not in {"plan", "plan_improve"}:
        return None
    return source_task


def _plan_review_detail(
    *,
    task: DbTask,
    config: Config,
    store: SqliteTaskStore,
) -> tuple[str | None, str | None]:
    """Return parsed plan-review verdict plus compact manifest state text."""
    source_task = _plan_review_source_task(store, task)
    if source_task is None:
        return None, None
    manifest_state = resolve_effective_plan_review_manifest_state(
        store,
        config,
        review_task=task,
        plan_source_task=source_task,
    )
    verdict = manifest_state.verdict
    manifest_detail: str | None = None
    if manifest_state.manifest is not None:
        manifest_detail = (
            f"{manifest_state.source} manifest valid "
            f"({len(manifest_state.manifest.slices)} slices)"
        )
    elif manifest_state.validation_error:
        manifest_detail = f"{manifest_state.source} manifest invalid ({manifest_state.validation_error})"
    return verdict, manifest_detail


def _load_optional_query_git_context(config: Config) -> tuple[Git | None, str | None]:
    """Best-effort git context for query projections that can tolerate stale verify evidence."""
    try:
        git = Git(config.project_dir)
        return git, git.default_branch()
    except GitError:
        return None, None


def _query_git_cache_scope(git: Git | None) -> contextlib.AbstractContextManager[Git | None]:
    if git is not None and hasattr(git, "cached"):
        return git.cached()
    return contextlib.nullcontext(git)


def _resolve_show_lifecycle_task(store: SqliteTaskStore, task: DbTask) -> DbTask:
    """Return the lineage task whose lifecycle best represents the unit of work."""
    from ..recovery_engine import resolve_recovery_planning_task

    planning_task = resolve_recovery_planning_task(store, task)
    if planning_task.task_type != "plan":
        return planning_task

    implement_descendants = list(walk_based_on_descendants(store, planning_task, task_type="implement"))
    if not implement_descendants:
        return planning_task

    return max(
        implement_descendants,
        key=lambda descendant: _task_id_numeric_key(descendant.id),
    )


def _summarize_lifecycle(
    task: DbTask,
    *,
    config: Config,
    store: SqliteTaskStore,
) -> _LifecycleSummary | None:
    root_task = _resolve_lineage_root_task(store, task)
    lineage_tree = _build_lineage_tree_for_root(store, root_task)
    if not _lineage_has_descendants(lineage_tree):
        return None

    planning_task = _resolve_show_lifecycle_task(store, task)
    recovered = task.status == "failed" and planning_task is not task

    if planning_task.status == "pending":
        detail = f"pending ({planning_task.id} {planning_task.task_type})"
        return _with_recovered_lifecycle_prefix(detail, recovered=recovered, severity="default")
    if planning_task.status == "in_progress":
        detail = f"in progress ({planning_task.id} {planning_task.task_type})"
        return _with_recovered_lifecycle_prefix(detail, recovered=recovered, severity="running")
    if planning_task.status == "completed" and planning_task.merge_status == "merged" and task_owns_merge_status(planning_task):
        detail = "completed and merged"
        return _with_recovered_lifecycle_prefix(detail, recovered=recovered, severity="completed")

    try:
        git = Git(config.project_dir)
        target_branch = git.default_branch()
    except (GitError, OSError, ValueError) as exc:
        detail = f"lifecycle unavailable - failed to resolve default branch: {' '.join(str(exc).split())}"
        return _with_recovered_lifecycle_prefix(detail, recovered=recovered, severity="failed")

    try:
        action = determine_next_action(
            config,
            store,
            git,
            planning_task,
            target_branch,
            persist_post_merge_rebase_state=False,
            persist_review_clearance=False,
        )
    except (GitError, OSError, ValueError) as exc:
        detail = f"lifecycle unavailable - failed to classify lifecycle: {' '.join(str(exc).split())}"
        return _with_recovered_lifecycle_prefix(detail, recovered=recovered, severity="failed")

    action_type = str(action.get("type", "skip"))
    action_class = classify_advance_action(action)

    if action_class == "needs_attention":
        detail = format_needs_attention_lifecycle(action)
        return _with_recovered_lifecycle_prefix(detail, recovered=recovered, severity="failed")

    if action_type == "wait_review":
        review_task = action.get("review_task")
        review_id = review_task.id if isinstance(review_task, DbTask) and review_task.id else "unknown"
        detail = f"review in_progress ({review_id})"
        return _with_recovered_lifecycle_prefix(detail, recovered=recovered, severity="running")
    if action_type == "run_review":
        review_task = action.get("review_task")
        review_id = review_task.id if isinstance(review_task, DbTask) and review_task.id else "unknown"
        detail = f"review pending ({review_id})"
        return _with_recovered_lifecycle_prefix(detail, recovered=recovered, severity="default")
    if action_type in {"merge", "merge_with_followups"}:
        if planning_task.merge_status == "merged":
            detail = "completed and merged"
        else:
            detail = "completed, ready to merge"
        return _with_recovered_lifecycle_prefix(detail, recovered=recovered, severity="completed")
    if action_type == "needs_rebase":
        detail = "needs rebase"
        return _with_recovered_lifecycle_prefix(detail, recovered=recovered, severity="default")
    if action_type == "wait_improve":
        improve_task = action.get("improve_task")
        improve_id = improve_task.id if isinstance(improve_task, DbTask) and improve_task.id else "unknown"
        detail = f"improve in_progress ({improve_id})"
        return _with_recovered_lifecycle_prefix(detail, recovered=recovered, severity="running")
    if action_type == "run_improve":
        improve_task = action.get("improve_task")
        improve_id = improve_task.id if isinstance(improve_task, DbTask) and improve_task.id else "unknown"
        detail = f"improve pending ({improve_id})"
        return _with_recovered_lifecycle_prefix(detail, recovered=recovered, severity="default")
    if action_type == "improve":
        detail = "changes requested"
        return _with_recovered_lifecycle_prefix(detail, recovered=recovered, severity="default")
    if action_type == "create_review":
        detail = "ready for review"
        return _with_recovered_lifecycle_prefix(detail, recovered=recovered, severity="default")

    detail = str(action.get("description", "")).strip()
    if detail.startswith("SKIP: "):
        detail = detail[6:]
    if not detail:
        return _with_recovered_lifecycle_prefix("recovered", recovered=False, severity="completed") if recovered else None
    return _with_recovered_lifecycle_prefix(detail, recovered=recovered, severity="default")


def _task_lineage_root_id(store: SqliteTaskStore, task_id: str) -> str | None:
    """Resolve a task selection to its canonical lineage root ID."""
    task = store.get(task_id)
    if task is None or task.id is None:
        return None
    root = _resolve_lineage_root_task(store, task)
    return root.id


def _coalesce_search_lineage_root_filter(
    *,
    store: SqliteTaskStore,
    canonical_task_id: str | None,
    deprecated_task_id: str | None,
) -> tuple[tuple[str, ...] | None, bool]:
    """Resolve negative lineage selectors to the root IDs they should exclude."""
    deprecated_used = deprecated_task_id is not None
    task_ids = [task_id for task_id in (canonical_task_id, deprecated_task_id) if task_id is not None]
    if not task_ids:
        return None, deprecated_used

    resolved_root_ids: list[str] = []
    for task_id in task_ids:
        root_id = _task_lineage_root_id(store, task_id)
        if root_id is not None:
            resolved_root_ids.append(root_id)

    if not resolved_root_ids:
        return None, deprecated_used
    return tuple(dict.fromkeys(resolved_root_ids)), deprecated_used


def _coalesce_search_lineage_task_filter(
    *,
    store: SqliteTaskStore,
    canonical_task_id: str | None,
    deprecated_task_id: str | None,
) -> tuple[str | None, bool, bool]:
    """Collapse canonical and deprecated positive lineage selectors to one query filter.

    Returns the task ID to pass through ``TaskQuery.lineage_of``, whether the filter is
    impossible and should force an empty result set, and whether the deprecated alias
    was used.
    """
    deprecated_used = deprecated_task_id is not None
    task_ids = [task_id for task_id in (canonical_task_id, deprecated_task_id) if task_id is not None]
    if not task_ids:
        return None, False, deprecated_used
    if len(task_ids) == 1:
        return task_ids[0], False, deprecated_used

    root_ids = {_task_lineage_root_id(store, task_id) for task_id in task_ids}
    if len(root_ids) != 1 or next(iter(root_ids)) is None:
        return None, True, deprecated_used

    return canonical_task_id or deprecated_task_id, False, deprecated_used


class _UnmergedGit(Protocol):
    def default_branch(self) -> str:
        ...

    def current_branch(self) -> str:
        ...

    def branch_exists(self, branch: str) -> bool:
        ...

    def ref_exists(self, ref: str) -> bool:
        ...

    def is_merged(
        self,
        branch: str,
        into: str | None = None,
        use_cherry: bool = False,
    ) -> bool:
        ...

    def count_commits_ahead(self, branch: str, target: str) -> int:
        ...

    def get_diff_stat_parsed(self, revision_range: str) -> tuple[int, int, int]:
        ...

    def get_diff_numstat(self, revision_range: str) -> str:
        ...

    def can_merge(self, branch: str, into: str | None = None) -> bool:
        ...

    def fetch(self, remote: str = "origin") -> None:
        ...

def _parse_cli_date(value: str | None) -> _dt.date | None:
    parsed = _parse_iso(value) if value else None
    return parsed.date() if parsed else None


def _collect_incomplete_legacy_args(args: argparse.Namespace) -> tuple[str, ...]:
    """Render accepted legacy `incomplete` flags in a stable human-readable order."""
    legacy_args: list[str] = []
    if getattr(args, "legacy_help", False):
        legacy_args.append("--help")
    if getattr(args, "json", False):
        legacy_args.append("--json")
    if getattr(args, "verbose", False):
        legacy_args.append("--verbose")
    if getattr(args, "blocked_by_dropped", False):
        legacy_args.append("--blocked-by-dropped")
    if getattr(args, "last", None) is not None:
        legacy_args.extend(["--last", str(args.last)])
    if getattr(args, "tree", False):
        legacy_args.append("--tree")
    if getattr(args, "type", None):
        legacy_args.extend(["--type", str(args.type)])
    if getattr(args, "days", None) is not None:
        legacy_args.extend(["--days", str(args.days)])
    if getattr(args, "date_field", None):
        legacy_args.extend(["--date-field", str(args.date_field)])
    if getattr(args, "fields", None):
        legacy_args.extend(["--fields", str(args.fields)])
    return tuple(legacy_args)


def cmd_incomplete_deprecated(args: argparse.Namespace) -> int:
    """Print migration guidance for the deprecated `gza incomplete` entrypoint."""
    legacy_args = _collect_incomplete_legacy_args(args)
    if legacy_args:
        _stderr_console.print(
            rich_escape(
                "Ignoring legacy arguments: " + " ".join(shlex.quote(arg) for arg in legacy_args)
            )
        )
    for line in _INCOMPLETE_DEPRECATION_LINES:
        _stderr_console.print(line)
    return 2


def _normalize_task_timestamp(value: datetime | None) -> datetime:
    """Normalize task timestamps for stable ordering across legacy/current rows."""
    if value is None:
        return datetime.min.replace(tzinfo=UTC)
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _format_blocked_dependency_label(
    blocking_id: str | None,
    blocking_status: str | None,
    *,
    blocking_merge_state: str | None = None,
    blocking_merge_owner_id: str | None = None,
    blocking_source_branch: str | None = None,
    blocking_target_branch: str | None = None,
) -> str:
    """Render a truthful blocked-dependency label for `gza next --all` rows."""
    if blocking_merge_state:
        bits = [f"blocked: dependency {blocking_id or 'unknown'}"]
        bits.append(f"merge unit {blocking_merge_state}")
        if blocking_merge_owner_id and blocking_merge_owner_id != blocking_id:
            bits.append(f"owned by {blocking_merge_owner_id}")
        if blocking_source_branch:
            bits.append(f"on {blocking_source_branch}")
        if blocking_target_branch:
            bits.append(f"-> {blocking_target_branch}")
        return f"({' '.join(bits)})"
    if blocking_status and blocking_id:
        return f"(blocked-by-{blocking_status} {blocking_id})"
    if blocking_status:
        return f"(blocked-by-{blocking_status})"
    if blocking_id:
        return f"(blocked by {blocking_id})"
    return "(blocked by dependency)"


def _task_show_merge_status(store: SqliteTaskStore, task: DbTask) -> str | None:
    """Return the operator-facing merge status for `gza show`."""
    if task.id is not None:
        unit = store.resolve_merge_unit_for_task(task.id)
        if unit is not None and unit.owner_task_id == task.id:
            return effective_no_work_merge_state(task, unit.state)
    if task_owns_merge_status(task):
        return task.merge_status
    return None


def _reconcile_unmerged_tasks(store: SqliteTaskStore, git: Git, default_branch: str) -> tuple[int, int]:
    """Refresh merge truth and diff stats for tasks currently marked unmerged."""
    merged_count = 0
    refreshed_count = 0

    for task in store.get_unmerged():
        if task.id is None or not task.branch:
            continue

        if git.is_merged(task.branch, default_branch):
            store.set_merge_status(task.id, "merged")
            merged_count += 1
            continue

        files_changed, insertions, deletions = git.get_diff_stat_parsed(f"{default_branch}...{task.branch}")
        store.update_diff_stats(task.id, files_changed, insertions, deletions)
        refreshed_count += 1

    return merged_count, refreshed_count


def _is_branch_target_live(args: argparse.Namespace) -> bool:
    """Whether unmerged should use a live git target instead of canonical DB state."""
    return bool(getattr(args, "into_current", False) or getattr(args, "target", None))


def _format_recovery_lane_detail(entry: RecoveryLaneEntry) -> str:
    if entry.attention_action is not None:
        return format_needs_attention_entry_for_display(entry.task, action=entry.attention_action)
    action = entry.action or {}
    action_type = str(action.get("type", "")).strip()
    if action_type and action_type not in {"resume", "retry", "reconcile_branch_divergence"}:
        detail = (
            f"{action_type:<12} {entry.task.id} [{entry.task.task_type}] "
            f"{shorten_prompt(entry.task.prompt, prompt_available_width(prefix=38, suffix=0))} "
            f"{str(action.get('description', '')).strip()}"
        )
        reason = action.get("reason")
        if isinstance(reason, str) and reason:
            detail += f" reason={reason}"
        deferred = action.get("deferred_action_type")
        if isinstance(deferred, str) and deferred:
            detail += f" deferred={deferred}"
        return detail
    decision = entry.decision
    return (
        f"{decision.action:<6} {entry.task.id} [{entry.task.task_type}] "
        f"{shorten_prompt(entry.task.prompt, prompt_available_width(prefix=32, suffix=0))} "
        f"via {decision.launch_mode} reason={decision.reason_code} "
        f"attempt={decision.attempt_index}/{decision.attempt_limit}"
    )


def _print_recovery_lane_section(entries: list[RecoveryLaneEntry]) -> None:
    console.print(
        _build_queue_summary(
            "Recovery lane: `advance` / `watch` only. Evaluated ahead of pending pickup."
        )
    )
    if not entries:
        console.print("No recovery candidates")
        return
    for entry in entries:
        console.print(_format_recovery_lane_detail(entry))


def _print_pending_lane_header(*, preview_label: str) -> None:
    console.print()
    console.print(
        _build_queue_summary(
            f"Pending lane: `{preview_label}` preview only. `gza work` / `watch` start from this lane."
        )
    )


def _print_quiet_lane_header(*, preview_label: str) -> None:
    console.print()
    console.print(
        _build_queue_summary(
            f"Quiet lane: `{preview_label}` shows held tasks without giving them runnable positions."
        )
    )


def _print_lifecycle_action_section(entries) -> None:
    console.print()
    console.print(
        _build_queue_summary(
            "Lifecycle actions: `advance` / `watch` lifecycle work visible ahead of pending pickup."
        )
    )
    if not entries:
        console.print("No lifecycle actions")
        return
    print_lifecycle_action_entries(console, entries)


def cmd_next(args: argparse.Namespace) -> int:
    """List recovery candidates and upcoming pending tasks in their distinct lanes."""
    config = Config.load(args.project_dir)
    store = get_store(config, open_mode="query_only")
    service = _TaskQueryService(store)
    try:
        tag_filters, any_tag = parse_cli_tag_filters(args)
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1

    git = Git(config.project_dir)
    target_branch = git.default_branch()
    recovery_entries = collect_recovery_lane_entries(
        store,
        tags=tag_filters,
        any_tag=any_tag,
        max_recovery_attempts=config.max_resume_attempts,
        git=git,
        target_branch=target_branch,
    )
    lifecycle_entries = collect_lifecycle_action_entries(
        store,
        config=config,
        git=git,
        target_branch=target_branch,
        tags=tag_filters,
        any_tag=any_tag,
        max_recovery_attempts=config.max_resume_attempts,
        persist_post_merge_rebase_state=False,
    )
    queue_rows = [
        row
        for row in service.run(
            _TaskQueryPresets.queue_listing(limit=None, tags=tag_filters, any_tag=any_tag),
            config=config,
        ).rows
        if isinstance(row, _TaskRow)
    ]
    runnable_rows, quiet_rows, blocked_rows = _partition_queue_rows(queue_rows)

    # Check for orphaned/stale tasks once, regardless of whether pending tasks exist
    registry = WorkerRegistry(config.workers_path)
    orphaned = _get_orphaned_tasks(registry, store)

    if not queue_rows and not recovery_entries and not lifecycle_entries:
        if tag_filters:
            console.print(f"No pending tasks matching tags: {', '.join(tag_filters)}")
        else:
            console.print("No pending tasks")
        if orphaned:
            _print_orphaned_warning(orphaned)
        return 0

    _print_recovery_lane_section(recovery_entries)
    _print_lifecycle_action_section(lifecycle_entries)
    _print_pending_lane_header(preview_label="gza next")

    # Filter blocked tasks unless --all is specified
    show_all = bool(getattr(args, "all", False))
    rendered_rows = [
        _QueueRenderRow(task=row.task, position_text=str(index))
        for index, row in enumerate(runnable_rows, 1)
    ]
    quiet_rendered_rows = [
        _QueueRenderRow(
            task=row.task,
            position_text="-",
            blocked_by_text=(
                _format_blocked_dependency_label(
                    cast(str | None, row.values.get("blocking_id")),
                    cast(str | None, row.values.get("blocking_status")),
                    blocking_merge_state=cast(str | None, row.values.get("blocking_merge_state")),
                    blocking_merge_owner_id=cast(str | None, row.values.get("blocking_merge_owner_id")),
                    blocking_source_branch=cast(str | None, row.values.get("blocking_source_branch")),
                    blocking_target_branch=cast(str | None, row.values.get("blocking_target_branch")),
                )[1:-1]
                if bool(row.values.get("blocked"))
                else None
            ),
            quiet_available_text=_format_quiet_available_at(row.values.get("quiet_available_at")),
        )
        for row in quiet_rows
    ]
    if show_all:
        rendered_rows.extend(
            _QueueRenderRow(
                task=row.task,
                position_text="-",
                blocked=True,
                blocked_by_text=(
                    blocked_dependency_label(store, row.task)
                    or _format_blocked_dependency_label(
                        cast(str | None, row.values.get("blocking_id")),
                        cast(str | None, row.values.get("blocking_status")),
                        blocking_merge_state=cast(str | None, row.values.get("blocking_merge_state")),
                        blocking_merge_owner_id=cast(str | None, row.values.get("blocking_merge_owner_id")),
                        blocking_source_branch=cast(str | None, row.values.get("blocking_source_branch")),
                        blocking_target_branch=cast(str | None, row.values.get("blocking_target_branch")),
                    )[1:-1]
                ),
            )
            for row in blocked_rows
        )
    widths = _queue_render_widths(rendered_rows + quiet_rendered_rows)

    # Show runnable tasks
    if rendered_rows:
        _print_queue_rows(
            console,
            [row for row in rendered_rows if not row.blocked],
            widths=widths,
        )
    else:
        if not show_all:
            if tag_filters:
                console.print(f"No runnable tasks matching tags: {', '.join(tag_filters)}")
            else:
                console.print("No runnable tasks")

    if quiet_rendered_rows:
        _print_quiet_lane_header(preview_label="gza next")
        _print_queue_rows(console, quiet_rendered_rows, widths=widths)

    # Show blocked tasks if --all is specified
    if show_all and blocked_rows:
        if runnable_rows or quiet_rendered_rows:
            console.print()
        _print_queue_rows(
            console,
            [row for row in rendered_rows if row.blocked],
            widths=widths,
        )

    # Show blocked count at the bottom (only if not showing all)
    if not show_all and blocked_rows:
        console.print()
        console.print(_build_blocked_count_summary(len(blocked_rows)))

    if orphaned:
        _print_orphaned_warning(orphaned)

    return 0


def cmd_history(args: argparse.Namespace) -> int:
    """List recent completed/failed tasks."""
    from gza.query import HistoryFilter, TaskLineageNode, query_history, query_history_with_lineage

    if getattr(args, "list_fields", False):
        return _print_projection_fields("history")

    config = Config.load(args.project_dir)
    store = get_store(config, open_mode="query_only")
    service = _TaskQueryService(store)

    status = getattr(args, 'status', None)
    task_type = getattr(args, 'type', None)
    days = getattr(args, 'days', None)
    start_date = getattr(args, 'start_date', None)
    end_date = getattr(args, 'end_date', None)
    date_field = cast(_QueryDateField, getattr(args, 'date_field', "effective"))
    lineage_depth = getattr(args, 'lineage_depth', 0)
    projection_fields = _validate_projection_fields(
        _parse_csv(getattr(args, "fields", None)),
        command_name="history",
    )
    if getattr(args, "fields", None) is not None and projection_fields is None:
        return 2
    use_json = bool(getattr(args, "json", False))
    try:
        tags = validate_cli_tag_values(tuple(getattr(args, "tags", None) or ()))
        tags_not = validate_cli_tag_values(tuple(getattr(args, "tags_not", None) or ()))
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1
    any_tag = not bool(getattr(args, "all_tags", False))

    # If a date-based filter is active and --last/-n wasn't explicitly provided,
    # don't cap results with the default limit.
    has_date_filter = days is not None or start_date is not None or end_date is not None
    explicit_last = '--last' in sys.argv or '-n' in sys.argv
    limit = args.last if (explicit_last or not has_date_filter) else None

    f = HistoryFilter(
        limit=limit,
        status=status,
        status_not=getattr(args, "status_not", None),
        task_type=task_type,
        task_type_not=getattr(args, "type_not", None),
        days=days,
        start_date=start_date,
        end_date=end_date,
        date_field=date_field,
        lineage_depth=lineage_depth,
        tags=tags or None,
        tags_not=tags_not or None,
        any_tag=any_tag,
    )

    if use_json or projection_fields is not None:
        selected_tasks = query_history(store, f)
        if not selected_tasks:
            if use_json:
                print("[]")
            else:
                _print_history_empty_message(status, task_type, days)
            return 0

        selected_ids = [task.id for task in selected_tasks if task.id is not None]
        if not selected_ids:
            if use_json:
                print("[]")
            else:
                _print_history_empty_message(status, task_type, days)
            return 0

        query = _TaskQuery(
            scope="tasks",
            limit=None,
            projection=_TaskProjectionSpec(
                preset=_TaskProjectionPreset.HISTORY_DEFAULT,
                fields=projection_fields,
            ),
            presentation=_TaskPresentationSpec(mode="json" if use_json else "blocks"),
        )
        git, target_branch = _load_optional_query_git_context(config)
        with _query_git_cache_scope(git):
            all_rows = tuple(
                row
                for row in service.run(query, config=config, git=git, target_branch=target_branch).rows
                if isinstance(row, _TaskRow)
            )
        rows_by_id = {row.task.id: row for row in all_rows if row.task.id is not None}
        ordered_rows = tuple(rows_by_id[task_id] for task_id in selected_ids if task_id in rows_by_id)
        result = _TaskQueryResult(query=query, rows=ordered_rows)
        _render_projection_result(result, use_json=use_json)
        return 0

    c = TASK_COLORS
    default_merge_target = store.default_merge_target()

    # Fixed width for status labels to ensure alignment
    STATUS_WIDTH = 9  # "completed" is the longest at 9 chars

    def _task_merge_unit_state(task: DbTask) -> str | None:
        if task.id is not None:
            unit = store.resolve_merge_unit_for_task(task.id)
            if unit is not None and unit.target_branch == default_merge_target:
                return effective_no_work_merge_state(task, unit.state)
        return task.merge_status

    def _task_shares_parent_branch(task: DbTask, parent_task: DbTask | None) -> bool:
        """Return True when a child task is anchored to the parent's branch."""
        if parent_task is None or parent_task.id is None:
            return False
        if not task.branch or not parent_task.branch:
            return False
        if task.branch != parent_task.branch:
            return False
        return task.same_branch or task.based_on == parent_task.id

    def _is_resume_attempt(parent_task: DbTask, child_task: DbTask) -> bool:
        """Best-effort detection for resume attempts based on session + branch reuse."""
        if not parent_task.session_id or not child_task.session_id:
            return False
        if parent_task.session_id != child_task.session_id:
            return False
        if not parent_task.branch or not child_task.branch:
            return False
        return parent_task.branch == child_task.branch

    def _resolve_retry_annotation(task: DbTask) -> tuple[str, DbTask] | None:
        """Resolve final retry/resume descendant for failed tasks of the same type."""
        if task.id is None:
            return None

        visited: set[str] = {task.id}
        descendants: list[tuple[str, DbTask]] = []
        frontier: list[tuple[DbTask, str]] = []

        for child in store.get_based_on_children_by_type(task.id, task.task_type):
            if child.id is None:
                continue
            action = "resumed" if _is_resume_attempt(task, child) else "retried"
            frontier.append((child, action))

        while frontier:
            current, root_action = frontier.pop()
            if current.id is None or current.id in visited:
                continue
            visited.add(current.id)
            descendants.append((root_action, current))
            for child in store.get_based_on_children_by_type(current.id, task.task_type):
                frontier.append((child, root_action))

        if not descendants:
            return None
        return max(
            descendants,
            key=lambda item: (
                item[1].created_at or datetime.min,
                _task_id_numeric_key(item[1].id if isinstance(item[1].id, str) else None),
            ),
        )

    def _retry_outcome_annotation(attempt: DbTask) -> tuple[str, str] | None:
        """Return (label, color) for retry/resume final-attempt outcome annotation."""
        if attempt.status in {"completed", "unmerged"}:
            return ("✓", c['success'])
        if attempt.status in {"failed", "dropped"}:
            return ("✗", c['failure'])
        return None

    def _render_task_line(
        task: DbTask,
        *,
        first_prefix: str = "",
        detail_prefix: str = "",
        parent_task: DbTask | None = None,
        compact_child: bool = False,
    ) -> None:
        """Render a single task entry."""
        shares_parent_branch = _task_shares_parent_branch(task, parent_task)
        merge_state = _task_merge_unit_state(task)
        lifecycle_detail = moot_empty_lifecycle_detail(merge_state)
        use_merge_status = merge_state == "unmerged" and task_owns_merge_status(task)
        if use_merge_status:
            status_label = "unmerged"
            status_color = c['unmerged']
        elif task.status == "completed":
            status_label = "completed"
            status_color = c['success']
        elif task.status == "dropped":
            status_label = "dropped"
            status_color = c['failure']
        else:
            status_label = "failed"
            status_color = c['failure']
        status_padded = f"{status_label:<{STATUS_WIDTH}}"
        status_icon = f"[{status_color}]{status_padded}[/{status_color}]"
        date_str = (
            f"[{c['date']}]({task.completed_at.strftime('%Y-%m-%d %H:%M')})[/{c['date']}]"
            if task.completed_at
            else ""
        )
        # prefix: first_prefix + "completed " (STATUS_WIDTH+1) + ID " + "(YYYY-MM-DD HH:MM) "
        task_id_len = len(str(task.id))
        date_len = 19 if task.completed_at else 0  # "(YYYY-MM-DD HH:MM) "
        prefix_len = len(first_prefix) + STATUS_WIDTH + 1 + task_id_len + date_len
        prompt_display = shorten_prompt(task.prompt, prompt_available_width(prefix=prefix_len))
        console.print(
            f"{first_prefix}{status_icon} [{c['task_id']}]{task.id}[/{c['task_id']}] {date_str}"
            f" [{c['prompt']}]{prompt_display}[/{c['prompt']}]"
        )
        if task.status == "failed":
            reason = task.failure_reason or "UNKNOWN"
            console.print(f"{detail_prefix}    [{c['failure']}]reason: {reason}[/{c['failure']}]")
            retry_annotation = _resolve_retry_annotation(task)
            if retry_annotation is not None:
                action, final_attempt = retry_annotation
                if final_attempt.id is not None:
                    outcome_annotation = _retry_outcome_annotation(final_attempt)
                    suffix = ""
                    if outcome_annotation is not None:
                        outcome_label, outcome_color = outcome_annotation
                        suffix = f" [{outcome_color}]{outcome_label}[/{outcome_color}]"
                    console.print(
                        f"{detail_prefix}    [{c['lineage']}]→ {action} as[/{c['lineage']}] "
                        f"[{c['task_id']}]{final_attempt.id}[/{c['task_id']}]"
                        f"{suffix}"
                    )
        elif task.status == "completed" and task.completion_reason:
            console.print(
                f"{detail_prefix}    [{c['success']}]completion: {task.completion_reason}[/{c['success']}]"
            )

        type_label = f"\\[{task.task_type}]"
        merge_label = ""
        if merge_state in {"merged", "empty", "redundant"} and task_owns_merge_status(task):
            merge_label = f" \\[{merge_state}]"
        tid = c['task_id']
        if task.based_on and task.depends_on:
            parent_label = f" ← [{tid}]{task.based_on}[/{tid}] (dep [{tid}]{task.depends_on}[/{tid}])"
        elif task.based_on:
            parent_label = f" ← [{tid}]{task.based_on}[/{tid}]"
        elif task.depends_on:
            parent_label = f" ← [{tid}]{task.depends_on}[/{tid}]"
        else:
            parent_label = ""

        if compact_child and task.task_type in {"review", "improve", "plan_review", "plan_improve"}:
            compact_parts = [f"{type_label}{merge_label}{parent_label}"]
            if task.task_type == "review":
                verdict = get_review_verdict(config, task)
                if verdict:
                    verdict_part = f"verdict: {verdict}"
                    if task.review_score is not None:
                        verdict_part = f"{verdict_part} ({task.review_score})"
                    compact_parts.append(verdict_part)
            elif task.task_type == "plan_review":
                verdict, manifest_detail = _plan_review_detail(task=task, config=config, store=store)
                if verdict:
                    compact_parts.append(f"verdict: {verdict}")
                if manifest_detail:
                    compact_parts.append(manifest_detail)
            compact_parts.append(f"model: [{c['stats']}]{task.model or '-'}[/{c['stats']}]")
            stats_str = format_stats(task)
            if stats_str:
                compact_parts.append(f"stats: [{c['stats']}]{stats_str}[/{c['stats']}]")
            console.print(f"{detail_prefix}    " + " | ".join(compact_parts))
            return

        console.print(f"{detail_prefix}    {type_label}{merge_label}{parent_label}")
        if lifecycle_detail is not None:
            console.print(
                f"{detail_prefix}    [{c['success']}]lifecycle: {lifecycle_detail}[/{c['success']}]"
            )
        show_branch = bool(task.branch) and not shares_parent_branch
        if show_branch:
            console.print(f"{detail_prefix}    branch: [{c['branch']}]{task.branch}[/{c['branch']}]")
        if task.report_file:
            console.print(f"{detail_prefix}    report: [{c['file']}]{task.report_file}[/{c['file']}]")
        console.print(f"{detail_prefix}    model: [{c['stats']}]{task.model or '-'}[/{c['stats']}]")
        stats_str = format_stats(task)
        if stats_str:
            console.print(f"{detail_prefix}    stats: [{c['stats']}]{stats_str}[/{c['stats']}]")
        if task.id is not None:
            comment_count = len(store.get_comments(task.id))
            if comment_count > 0:
                console.print(f"{detail_prefix}    comments: [{c['stats']}]{comment_count}[/{c['stats']}]")

    def _render_lineage_node(node: TaskLineageNode) -> None:
        """Render a lineage tree using branch connectors."""

        def _render_subtree(
            current: TaskLineageNode,
            *,
            parent_task: DbTask | None = None,
            prefix: str = "",
            is_last: bool = True,
        ) -> None:
            if parent_task is not None:
                connector = "└── " if is_last else "├── "
                child_prefix_raw = f"{prefix}{'    ' if is_last else '│   '}"
                first_prefix = f"[{c['lineage']}]{prefix}{connector}[/{c['lineage']}]"
                detail_prefix = f"[{c['lineage']}]{child_prefix_raw}[/{c['lineage']}]"
            else:
                child_prefix_raw = ""
                first_prefix = ""
                detail_prefix = ""
            _render_task_line(
                current.task,
                first_prefix=first_prefix,
                detail_prefix=detail_prefix,
                parent_task=parent_task,
                compact_child=parent_task is not None,
            )

            for index, child in enumerate(current.children):
                _render_subtree(
                    child,
                    parent_task=current.task,
                    prefix=child_prefix_raw,
                    is_last=index == (len(current.children) - 1),
                )

        _render_subtree(node)
        print()

    # Check for orphaned tasks (only when no status filter is active)
    orphaned: list[DbTask] = []
    if not status:
        registry = WorkerRegistry(config.workers_path)
        orphaned = _get_orphaned_tasks(registry, store)

    if lineage_depth > 0:
        nodes = query_history_with_lineage(store, f)
        if not nodes and not orphaned:
            _print_history_empty_message(status, task_type, days)
            return 0
        # Show orphaned tasks at the top
        for task in orphaned:
            _render_orphaned_task(task, c)
        for node in nodes:
            _render_lineage_node(node)
    else:
        recent = query_history(store, f)
        if not recent and not orphaned:
            _print_history_empty_message(status, task_type, days)
            return 0

        # Show orphaned tasks at the top so they're immediately visible
        for task in orphaned:
            _render_orphaned_task(task, c)

        for task in recent:
            _render_task_line(task, first_prefix="", detail_prefix="")
            print()

    return 0


def _print_history_empty_message(
    status: str | None,
    task_type: str | None,
    days: int | None,
) -> None:
    """Print an appropriate 'no tasks found' message for gza history."""
    status_msg = f" with status '{status}'" if status else ""
    type_msg = f" with type '{task_type}'" if task_type else ""
    lookback_msg = f" in the last {days} days" if days is not None else ""
    console.print(
        f"No completed or failed tasks{status_msg}{type_msg}{lookback_msg}"
    )


def _render_orphaned_task(task: "DbTask", c: dict) -> None:
    """Render a single orphaned task entry for gza history."""
    status_padded = f"{'orphaned':<9}"
    status_icon = f"[{c['orphaned']}]⚠ {status_padded}[/{c['orphaned']}]"
    date_str = ""
    if task.started_at:
        date_str = (
            f"[{c['task_id']}](started {task.started_at.strftime('%Y-%m-%d %H:%M')})"
            f"[/{c['task_id']}]"
        )
    # prefix: "⚠ orphaned  ID " + optional date
    task_id_len = len(str(task.id))
    date_len = 28 if task.started_at else 0  # "(started YYYY-MM-DD HH:MM) "
    prefix_len = 2 + 9 + 1 + task_id_len + date_len
    prompt_display = shorten_prompt(task.prompt, prompt_available_width(prefix=prefix_len))
    console.print(
        f"{status_icon} [{c['task_id']}]{task.id}[/{c['task_id']}] {date_str}"
        f" [{c['prompt']}]{prompt_display}[/{c['prompt']}]"
    )
    type_label = f"\\[{task.task_type}]"
    console.print(f"    {type_label}")
    if task.branch:
        console.print(f"    branch: [{c['branch']}]{task.branch}[/{c['branch']}]")
    console.print(f"    [{c['task_id']}]Run 'gza work {task.id}' to resume[/{c['task_id']}]")
    print()


def cmd_search(args: argparse.Namespace) -> int:
    """Search tasks by substring in prompt text."""
    if getattr(args, "list_fields", False):
        return _print_projection_fields("search")

    config = Config.load(args.project_dir)
    store = get_store(config, open_mode="query_only")
    service = _TaskQueryService(store)
    term = args.term
    limit = None if args.last == 0 else args.last
    projection_fields = _validate_projection_fields(
        _parse_csv(getattr(args, "fields", None)),
        command_name="search",
    )
    if getattr(args, "fields", None) is not None and projection_fields is None:
        return 2
    use_json = bool(getattr(args, "json", False))
    try:
        tags = validate_cli_tag_values(tuple(getattr(args, "tags", None) or ()))
        tags_not = validate_cli_tag_values(tuple(getattr(args, "tags_not", None) or ()))
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1
    any_tag = not bool(getattr(args, "all_tags", False))

    date_filter = _TaskDateFilter(
        field=cast(_QueryDateField, getattr(args, "date_field", "created")),
        days=getattr(args, "days", None),
        start=_parse_cli_date(getattr(args, "start_date", None)),
        end=_parse_cli_date(getattr(args, "end_date", None)),
    )
    lineage_of = resolve_id(config, args.lineage_of) if getattr(args, "lineage_of", None) else None
    lineage_of_not = (
        resolve_id(config, args.lineage_of_not) if getattr(args, "lineage_of_not", None) else None
    )
    related_to = resolve_id(config, args.related_to) if getattr(args, "related_to", None) else None
    related_to_not = (
        resolve_id(config, args.related_to_not) if getattr(args, "related_to_not", None) else None
    )
    lineage_filter_task_id, lineage_filter_impossible, used_related_to = _coalesce_search_lineage_task_filter(
        store=store,
        canonical_task_id=lineage_of,
        deprecated_task_id=related_to,
    )
    exclude_lineage_root_ids, used_related_to_not = _coalesce_search_lineage_root_filter(
        store=store,
        canonical_task_id=lineage_of_not,
        deprecated_task_id=related_to_not,
    )
    if used_related_to:
        print("Warning: --related-to is deprecated; use --lineage-of instead.", file=sys.stderr)
    if used_related_to_not:
        print("Warning: --related-to-not is deprecated; use --lineage-of-not instead.", file=sys.stderr)
    root_ids = None
    if getattr(args, "root", None):
        parsed_roots = _parse_csv(args.root)
        root_ids = tuple(resolve_id(config, value) for value in parsed_roots) if parsed_roots else None
    exclude_root_ids = None
    if getattr(args, "root_not", None):
        parsed_roots_not = _parse_csv(args.root_not)
        exclude_root_ids = (
            tuple(resolve_id(config, value) for value in parsed_roots_not)
            if parsed_roots_not
            else None
        )
    if lineage_filter_impossible:
        root_ids = ()
    if exclude_lineage_root_ids is not None:
        exclude_root_ids = tuple(dict.fromkeys((*(exclude_lineage_root_ids or ()), *(exclude_root_ids or ()))))

    query = _TaskQueryPresets.search(
        term=term,
        limit=limit,
        statuses=_parse_csv(getattr(args, "status", None)),
        exclude_statuses=_parse_csv(getattr(args, "status_not", None)),
        task_types=_parse_csv(getattr(args, "type", None)),
        exclude_task_types=_parse_csv(getattr(args, "type_not", None)),
        date_filter=date_filter,
        lineage_of=lineage_filter_task_id,
        root_ids=root_ids,
        exclude_root_ids=exclude_root_ids,
    )
    query = replace(
        query,
        tag_filters=tags or None,
        exclude_tag_filters=tags_not or None,
        any_tag=any_tag,
    )
    if projection_fields is not None:
        query = replace(
            query,
            projection=_TaskProjectionSpec(
                preset=query.projection.preset,
                fields=projection_fields,
            ),
            presentation=_TaskPresentationSpec(mode="json" if use_json else "blocks"),
        )
    git, target_branch = _load_optional_query_git_context(config)
    with _query_git_cache_scope(git):
        result = service.run(query, config=config, git=git, target_branch=target_branch)
    matches = [row.task for row in result.rows if isinstance(row, _TaskRow)]

    if use_json:
        _render_projection_result(result, use_json=True)
        return 0

    if projection_fields is not None:
        if not result.rows:
            console.print(f"No tasks found matching '{term}'")
            return 0
        _render_projection_result(result, use_json=False)
        return 0

    total_matches = result.total_count or 0
    displayed_count = len(matches)
    displayed_start = 1 if displayed_count else 0
    displayed_end = displayed_count
    summary = f"Showing results {displayed_start}-{displayed_end} out of {total_matches}"

    if not matches:
        console.print(f"No tasks found matching '{term}'")
        console.print(summary)
        return 0

    c = TASK_COLORS
    STATUS_WIDTH = 11  # Align pending/in_progress task IDs in one column.

    def _status_label_and_color(task: DbTask) -> tuple[str, str]:
        if task.status == "completed":
            return ("completed", c['success'])
        if task.status == "failed":
            return ("failed", c['failure'])
        if task.status == "unmerged":
            return ("unmerged", c['unmerged'])
        if task.status == "dropped":
            return ("dropped", c['failure'])
        if task.status == "in_progress":
            return ("in_progress", c['lineage'])
        return ("pending", c['lineage'])

    for task in matches:
        status_label, status_color = _status_label_and_color(task)
        status_padded = f"{status_label:<{STATUS_WIDTH}}"
        status_icon = f"[{status_color}]{status_padded}[/{status_color}]"
        date_str = (
            f"[{c['date']}]({task.completed_at.strftime('%Y-%m-%d %H:%M')})[/{c['date']}]"
            if task.completed_at
            else ""
        )
        task_id_len = len(str(task.id))
        date_len = 19 if task.completed_at else 0
        prefix_len = STATUS_WIDTH + 1 + task_id_len + date_len
        prompt_display = shorten_prompt(task.prompt, prompt_available_width(prefix=prefix_len))
        console.print(
            f"{status_icon} [{c['task_id']}]{task.id}[/{c['task_id']}] {date_str}"
            f" [{c['prompt']}]{prompt_display}[/{c['prompt']}]"
        )
        type_label = f"\\[{task.task_type}]"
        console.print(f"    {type_label}")
        if task.branch:
            console.print(f"    branch: [{c['branch']}]{task.branch}[/{c['branch']}]")
        stats_str = format_stats(task)
        if stats_str:
            console.print(f"    stats: [{c['stats']}]{stats_str}[/{c['stats']}]")
        print()

    console.print(summary)
    return 0


def cmd_incomplete(args: argparse.Namespace) -> int:
    """Show unresolved task lineages that still need attention."""
    blocked_by_dropped_only = bool(getattr(args, "blocked_by_dropped", False))
    if getattr(args, "list_fields", False):
        return _print_projection_fields("incomplete", blocked_by_dropped=blocked_by_dropped_only)

    config = Config.load(args.project_dir)
    store = get_store(config, open_mode="query_only")
    service = _TaskQueryService(store)
    limit = None if args.last == 0 else args.last
    mode = cast(_PresentationMode, "tree" if getattr(args, "tree", False) else "one_line")
    task_type_filter: str | None = getattr(args, "type", None)
    projection_fields = _validate_projection_fields(
        _parse_csv(getattr(args, "fields", None)),
        command_name="incomplete",
        blocked_by_dropped=blocked_by_dropped_only,
    )
    if getattr(args, "fields", None) is not None and projection_fields is None:
        return 2
    date_filter = _TaskDateFilter(
        field=cast(_QueryDateField, getattr(args, "date_field", "effective")),
        days=getattr(args, "days", None),
    )
    try:
        tag_filters, any_tag = parse_cli_tag_filters(args)
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1
    normalized_tag_filters = normalize_tag_filters(tag_filters)
    if blocked_by_dropped_only:
        query = _TaskQuery(
            scope="tasks",
            limit=limit,
            statuses=("pending",),
            task_types=(task_type_filter,) if task_type_filter else None,
            dependency_state=("blocked_by_dropped_dep",),
            tag_filters=normalized_tag_filters,
            any_tag=any_tag,
            date_filter=date_filter,
            projection=_TaskProjectionSpec(fields=projection_fields or ("id", "prompt", "status", "task_type", "blocking_id")),
            presentation=_TaskPresentationSpec(mode="json" if getattr(args, "json", False) else "blocks"),
        )
    else:
        query = _TaskQueryPresets.incomplete(
            limit=limit,
            task_types=(task_type_filter,) if task_type_filter else None,
            tags=tag_filters,
            any_tag=any_tag,
            date_filter=date_filter,
            mode=mode,
        )
        if projection_fields is not None:
            query = replace(
                query,
                projection=_TaskProjectionSpec(
                    preset=query.projection.preset,
                    fields=projection_fields,
                ),
                presentation=_TaskPresentationSpec(
                    mode="json" if getattr(args, "json", False) else "blocks"
                ),
            )

    target_branch: str | None = None
    git: Git | None = None
    if not blocked_by_dropped_only:
        try:
            git = Git(config.project_dir)
            target_branch = git.default_branch()
        except GitError:
            git = None
            target_branch = None

    cache_scope = (
        git.cached() if git is not None and hasattr(git, "cached") else contextlib.nullcontext(git)
    )
    with cache_scope:
        result = service.run(query, config=config, git=git, target_branch=target_branch)
        if not blocked_by_dropped_only:
            result = _normalize_incomplete_result_rows(
                result,
                service=service,
                store=store,
                config=config,
                git=git,
                target_branch=target_branch,
            )
    if getattr(args, "json", False):
        _render_projection_result(result, use_json=True)
        return 0

    if not result.rows:
        if blocked_by_dropped_only:
            console.print("No pending tasks blocked by dropped dependencies")
        else:
            console.print("No unresolved task lineages")
        return 0

    if projection_fields is not None:
        _render_projection_result(result, use_json=False)
        return 0

    if blocked_by_dropped_only:
        for row in result.rows:
            if not isinstance(row, _TaskRow):
                continue
            task = row.task
            blocking_id = row.values.get("blocking_id")
            marker = (
                f" [blocked-by-dropped {blocking_id}]"
                if isinstance(blocking_id, str) and blocking_id
                else " [blocked-by-dropped]"
            )
            console.print(rich_escape(f"{task.id}: {task.prompt}{marker}"))
        return 0

    rendered = result.render(mode)
    if rendered:
        console.print(rendered)

    dirty_merge_warning = _incomplete_dirty_checkout_warning(
        result,
        config=config,
        git=git,
    )
    if dirty_merge_warning is not None:
        console.print(dirty_merge_warning)

    blocked_dependents = _collect_incomplete_blocked_dependents(
        store,
        result,
        task_type_filter=task_type_filter,
        tag_filters=normalized_tag_filters,
        any_tag=any_tag,
    )
    if blocked_dependents:
        console.print()
        console.print("Blocked dependents:")
        for task, readiness in blocked_dependents:
            assert task.id is not None
            console.print(_blocked_dependent_detail(store, task, readiness))

    if getattr(args, "verbose", False) and mode != "tree":
        c = TASK_COLORS
        for row in result.rows:
            if not isinstance(row, _LineageRow):
                continue
            owner = row.owner_task
            owner_time = owner.completed_at or owner.created_at
            owner_time_text = owner_time.strftime('%Y-%m-%d %H:%M') if owner_time else ""
            console.print(
                f"    [{c['task_id']}]{owner.id}[/{c['task_id']}]"
                f" \\[{owner.task_type}]"
                f" [{c['date']}]{owner_time_text}[/{c['date']}]"
            )

    return 0


def _collect_incomplete_blocked_dependents(
    store: SqliteTaskStore,
    result: _TaskQueryResult,
    *,
    task_type_filter: str | None,
    tag_filters: tuple[str, ...] | None,
    any_tag: bool,
) -> list[tuple[DbTask, DependencyReadiness]]:
    owner_ids = {
        row.owner_task.id
        for row in result.rows
        if isinstance(row, _LineageRow) and row.owner_task.id is not None
    }
    owner_unit_ids = {
        cast(str, row.values.get("merge_unit_id"))
        for row in result.rows
        if isinstance(row, _LineageRow) and isinstance(row.values.get("merge_unit_id"), str)
    }

    blocked: list[tuple[DbTask, DependencyReadiness]] = []
    for task in store.get_pending(limit=None):
        if task.task_type == "internal":
            continue
        if task_type_filter is not None and task.task_type != task_type_filter:
            continue
        if not task_matches_tag_filters(task_tags=task.tags, tag_filters=tag_filters, any_tag=any_tag):
            continue
        readiness = store.get_dependency_readiness(task)
        if readiness.ready:
            continue
        if (
            readiness.blocking_task_id not in owner_ids
            and readiness.blocking_merge_unit_owner_task_id not in owner_ids
            and readiness.blocking_merge_unit_id not in owner_unit_ids
        ):
            continue
        blocked.append((task, readiness))
    blocked.sort(key=lambda item: _normalize_task_timestamp(item[0].created_at))
    return blocked


def _blocked_dependent_detail(
    store: SqliteTaskStore,
    task: DbTask,
    readiness: DependencyReadiness,
) -> str:
    operator_label = blocked_dependency_label(store, task, readiness=readiness)
    if operator_label is not None:
        return f"  {task.id} pending {task.task_type} {operator_label}"
    detail = (
        f"  {task.id} pending {task.task_type} blocked by "
        f"{readiness.blocking_merge_state or 'unmerged'} dependency "
        f"{readiness.blocking_task_id or task.depends_on or 'unknown'}"
    )
    owner_id = readiness.blocking_merge_unit_owner_task_id
    if owner_id and owner_id != readiness.blocking_task_id:
        detail += f" (merge unit owned by {owner_id}"
        if readiness.blocking_source_branch:
            detail += f", branch {readiness.blocking_source_branch}"
        detail += ")"
    elif readiness.blocking_source_branch:
        detail += f" (branch {readiness.blocking_source_branch})"
    return detail


def _incomplete_dirty_checkout_warning(
    result: _TaskQueryResult,
    *,
    config: Config,
    git: Git | None,
) -> str | None:
    if config.main_checkout_isolate or git is None:
        return None
    has_mergeable_rows = any(
        isinstance(row, _LineageRow) and row.values.get("next_action") in {"merge", "merge_with_followups"}
        for row in result.rows
    )
    if not has_mergeable_rows:
        return None
    try:
        if not git.has_changes(include_untracked=False):
            return None
    except GitError:
        return None
    return "merges blocked: main checkout has uncommitted changes - commit or stash them first"


def _normalize_incomplete_result_rows(
    result: _TaskQueryResult,
    *,
    service: _TaskQueryService,
    store: SqliteTaskStore,
    config: Config,
    git: Git | None,
    target_branch: str | None,
) -> _TaskQueryResult:
    """Re-root incomplete rows on the implementation that owns the merge unit."""
    normalized_rows: list[_TaskRow | _LineageRow] = []
    changed = False
    tag_filters = result.query.tag_filters
    any_tag = result.query.any_tag

    for row in result.rows:
        if not isinstance(row, _LineageRow):
            normalized_rows.append(row)
            continue

        owner_task = _resolve_incomplete_owner_task(store, row)
        action = row.next_action_data
        stale_fallback_action = False
        if action is not None and classify_advance_action(action) == "needs_attention":
            explicit_subject = _resolve_incomplete_explicit_subject_task(store, action, row)
            owner_task = resolve_subject_task(store, action, row, fallback_task=owner_task)
            stale_fallback_action = owner_task.id != row.owner_task.id and explicit_subject is None
        merge_units_by_task_id: dict[str, MergeUnit] = {}
        for task in (owner_task, *row.members, *row.unresolved_tasks):
            if task is None or task.id is None or task.id in merge_units_by_task_id:
                continue
            unit = store.resolve_merge_unit_for_task(task.id)
            if unit is not None:
                merge_units_by_task_id[task.id] = unit
        unresolved_tasks = filter_display_unresolved_tasks_for_incomplete(
            row.unresolved_tasks,
            merge_units_by_task_id=merge_units_by_task_id,
            exclude_dropped=True,
        )
        if owner_task.status == "dropped":
            changed = True
            continue
        if (
            not unresolved_tasks
            and owner_task.task_type == "plan"
            and action is not None
            and classify_advance_action(action) == "needs_attention"
        ):
            unresolved_tasks = (owner_task,)
        if not unresolved_tasks:
            changed = True
            continue
        if not task_matches_tag_filters(
            task_tags=owner_task.tags,
            tag_filters=tag_filters,
            any_tag=any_tag,
        ):
            changed = True
            continue

        tree = row.tree
        members = row.members
        if owner_task.task_type == "implement":
            tree = _descendants_only_unmerged_lineage_tree(store, owner_task=owner_task)
            members = tuple(_flatten_query_lineage_tree(tree)) if tree is not None else ()

        owner_changed = owner_task.id != row.owner_task.id
        tree_changed = tree is not row.tree
        members_changed = tuple(task.id for task in members) != tuple(task.id for task in row.members)
        if not owner_changed and not tree_changed and not members_changed:
            normalized_rows.append(row)
            continue

        changed = True
        normalized_rows.append(
            service._project_lineage_row(  # noqa: SLF001
                _LineageRow(
                    owner_task=owner_task,
                    members=members,
                    tree=tree,
                    unresolved_tasks=unresolved_tasks,
                    lifecycle_action_task=row.lifecycle_action_task,
                    recovery_action_task=row.recovery_action_task,
                    recovery_leaf_task=row.recovery_leaf_task,
                    lineage_status=row.lineage_status,
                    next_action_data=None if stale_fallback_action else row.next_action_data,
                ),
                result.query,
                config=config,
                git=git,
                target_branch=target_branch,
            )
        )

    if not changed:
        return result
    return _TaskQueryResult(query=result.query, rows=tuple(normalized_rows), total_count=result.total_count)


def _resolve_incomplete_explicit_subject_task(
    store: SqliteTaskStore,
    action: object,
    row: _LineageRow,
) -> DbTask | None:
    """Return the action's explicit subject when it still resolves within the row lineage."""

    if not isinstance(action, Mapping):
        return None
    action_mapping = cast("Mapping[str, object]", action)
    subject_task_id = get_action_subject_task_id(action_mapping)
    if subject_task_id is None:
        return None
    subject_task = store.get(subject_task_id)
    if subject_task is None or subject_task.id != subject_task_id:
        return None
    candidate_ids: set[str] = {
        task.id
        for task in (
            row.owner_task,
            *row.members,
            *row.unresolved_tasks,
            row.lifecycle_action_task,
            row.recovery_action_task,
            row.recovery_leaf_task,
        )
        if isinstance(task, DbTask) and task.id is not None
    }
    if subject_task.id not in candidate_ids:
        subject_root_id = _resolve_lineage_root_task(store, subject_task).id
        if subject_root_id is None:
            return None
        if not any(
            (candidate := store.get(candidate_id)) is not None
            and candidate.id is not None
            and _resolve_lineage_root_task(store, candidate).id == subject_root_id
            for candidate_id in candidate_ids
        ):
            return None
    return subject_task


def _resolve_incomplete_owner_task(store: SqliteTaskStore, row: _LineageRow) -> DbTask:
    """Return the implementation that owns an incomplete row's branch when one exists."""

    def _iter_candidates() -> list[DbTask]:
        ordered: list[DbTask] = []
        seen: set[str | None] = set()
        for task in (
            row.owner_task,
            row.lifecycle_action_task,
            row.recovery_action_task,
            row.recovery_leaf_task,
            *row.unresolved_tasks,
            *row.members,
        ):
            if task is None or task.id in seen:
                continue
            seen.add(task.id)
            ordered.append(task)
        return ordered

    candidates = _iter_candidates()
    for candidate in candidates:
        if blocked_by_empty_prereq_label(store, candidate) is not None:
            return candidate
    for candidate in candidates:
        owner_task = _resolve_lineage_owner_task(store, candidate)
        if owner_task.task_type == "implement" and owner_task.branch:
            return _resolve_subject_fallback_task(store, row, fallback_task=owner_task)
    for candidate in candidates:
        owner_task = _resolve_lineage_owner_task(store, candidate)
        if owner_task.task_type == "implement":
            return _resolve_subject_fallback_task(store, row, fallback_task=owner_task)
    return _resolve_subject_fallback_task(store, row, fallback_task=row.owner_task)


def _format_review_verdict_label(review_verdict: str | None) -> str | None:
    """Return the unmerged review verdict badge label."""
    if review_verdict == "APPROVED":
        return "✓ approved"
    if review_verdict == "APPROVED_WITH_FOLLOWUPS":
        return "↺ approved with follow-ups"
    if review_verdict == "CHANGES_REQUESTED":
        return "⚠ changes requested"
    if review_verdict == "NEEDS_DISCUSSION":
        return "💬 needs discussion"
    return None


def _print_unmerged_progress(message: str, *, to_stderr: bool = False) -> None:
    target = _stderr_console if to_stderr else console
    target.print(f"[dim]Progress:[/dim] {message}")


def _print_unmerged_status(message: str, *, to_stderr: bool = False) -> None:
    target = _stderr_console if to_stderr else console
    target.print(message)


def _print_unmerged_empty(*, use_json: bool) -> None:
    if use_json:
        console.print("[]")
        return
    console.print("No unmerged tasks")


def _print_merged_empty(*, use_json: bool) -> None:
    if use_json:
        console.print("[]")
        return
    console.print("No merged units")


def _format_merged_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat()


def _merged_row_dict(store: SqliteTaskStore, unit: MergeUnit) -> dict[str, object | None]:
    owner = store.resolve_merge_unit_owner_task(unit)
    return {
        "merge_unit_id": unit.id,
        "owner_task_id": owner.id if owner is not None else unit.owner_task_id,
        "merge_source": unit.merge_source,
        "merged_at": _format_merged_timestamp(unit.merged_at),
        "branch": unit.source_branch,
        "target_branch": unit.target_branch,
    }


def _render_merged_rows(
    rows: list[dict[str, object | None]],
    *,
    fields: tuple[str, ...] | None,
    use_json: bool,
) -> None:
    selected_fields = fields or _MERGED_PROJECTION_FIELDS
    payload = [{field: row.get(field) for field in selected_fields} for row in rows]
    if use_json:
        import json

        print(json.dumps(payload, indent=2))
        return
    if fields is not None:
        if len(selected_fields) == 1:
            field = selected_fields[0]
            for row in payload:
                print("" if row.get(field) is None else str(row.get(field)))
            return
        for index, row in enumerate(payload):
            for field in selected_fields:
                print(f"{field}: {row.get(field)}")
            if index != len(payload) - 1:
                print()
        return

    owner_width = max(len("owner task"), *(len(str(row["owner_task_id"] or "-")) for row in payload))
    source_width = max(len("source"), *(len(str(row["merge_source"] or "-")) for row in payload))
    merged_width = max(len("merged_at"), *(len(str(row["merged_at"] or "-")) for row in payload))
    print(
        f"{'unit id':<16} {'owner task':<{owner_width}} {'source':<{source_width}} "
        f"{'merged_at':<{merged_width}} branch"
    )
    for row in payload:
        print(
            f"{str(row['merge_unit_id']):<16} "
            f"{str(row['owner_task_id'] or '-'): <{owner_width}} "
            f"{str(row['merge_source'] or '-'): <{source_width}} "
            f"{str(row['merged_at'] or '-'): <{merged_width}} "
            f"{row['branch']}"
        )


def cmd_merged(args: argparse.Namespace) -> int:
    """List merged merge units with optional provenance and time filters."""
    if getattr(args, "list_fields", False):
        return _print_projection_fields("merged")

    config = Config.load(args.project_dir)
    store = get_store(config, open_mode="query_only")
    projection_fields = _validate_projection_fields(
        _parse_csv(getattr(args, "fields", None)),
        command_name="merged",
    )
    if getattr(args, "fields", None) is not None and projection_fields is None:
        return 2
    use_json = bool(getattr(args, "json", False))
    try:
        tag_filters, any_tag = parse_cli_tag_filters(args)
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1
    normalized_tag_filters = normalize_tag_filters(tag_filters)

    after: datetime | None = None
    since_value = getattr(args, "since", None)
    if since_value:
        parsed_since = _parse_iso(since_value)
        if parsed_since is None:
            print("Error: --since must be an ISO date/time or YYYY-MM-DD")
            return 1
        after = parsed_since if parsed_since.tzinfo is not None else parsed_since.replace(tzinfo=UTC)
    last_days = getattr(args, "last_days", None)
    if last_days is not None:
        last_days_after = datetime.now(UTC) - _dt.timedelta(days=last_days)
        after = max(after, last_days_after) if after is not None else last_days_after
    if after is None and not getattr(args, "all", False):
        after = datetime.now(UTC) - _dt.timedelta(days=1)

    units = store.list_merged_units(
        source=getattr(args, "source", None),
        after=after,
    )
    if normalized_tag_filters is not None:
        filtered_units: list[MergeUnit] = []
        for unit in units:
            owner = store.resolve_merge_unit_owner_task(unit)
            if owner is None:
                continue
            if task_matches_tag_filters(
                task_tags=owner.tags,
                tag_filters=normalized_tag_filters,
                any_tag=any_tag,
            ):
                filtered_units.append(unit)
        units = filtered_units
    if not units:
        _print_merged_empty(use_json=use_json)
        return 0
    rows = [_merged_row_dict(store, unit) for unit in units]
    _render_merged_rows(rows, fields=projection_fields, use_json=use_json)
    return 0


def _unmerged_effective_fields(query: _TaskQuery) -> set[str]:
    return set(_projection_fields(query.projection, scope="lineages"))


def _resolve_same_branch_unmerged_lineage_root(store: SqliteTaskStore, task: DbTask) -> DbTask:
    """Return the highest ancestor that remains on ``task.branch``.

    ``gza unmerged`` uses this to pick a branch owner without crossing into
    branchless or different-branch ancestors that only provide broader lineage
    context (for example plan/explore dependencies).
    """
    from gza.query import resolve_same_branch_lineage_root

    return resolve_same_branch_lineage_root(store, task)


def _resolve_unmerged_branch_owner(store: SqliteTaskStore, task: DbTask) -> DbTask:
    from gza.query import resolve_unmerged_branch_owner

    return resolve_unmerged_branch_owner(store, task)


def _resolve_lineage_owner_task(store: SqliteTaskStore, task: DbTask) -> DbTask:
    from gza.query import resolve_lineage_owner_task

    return resolve_lineage_owner_task(store, task)


def _descendants_only_unmerged_lineage_tree(
    store: SqliteTaskStore,
    *,
    owner_task: DbTask,
) -> TaskLineageNode | None:
    """Return the unmerged display subtree rooted at the selected branch owner.

    This view follows only `based_on` descendants plus explicit review attachments
    for included tasks. It intentionally excludes unrelated `depends_on`-only
    descendants from the slim `gza unmerged` output.
    """
    if owner_task.id is None:
        return TaskLineageNode(task=owner_task, depth=0, relationship="root")

    def _build_node(
        task: DbTask,
        *,
        parent_task: DbTask | None,
        depth: int,
    ) -> TaskLineageNode:
        relationship = (
            "root"
            if parent_task is None
            else _classify_lineage_child_relationship(parent_task, task)
        )
        node = TaskLineageNode(task=task, depth=depth, relationship=relationship)
        if task.id is None:
            return node

        based_on_children = store.get_based_on_children(task.id)
        review_children = store.get_reviews_for_task(task.id)
        review_by_id = {
            review.id: review for review in review_children if review.id is not None
        }
        review_attached_children: dict[str, list[DbTask]] = {
            review_id: [] for review_id in review_by_id
        }
        direct_based_on_children: list[DbTask] = []
        for child in based_on_children:
            if child.id is not None and child.id in review_by_id:
                continue
            if child.depends_on is not None and child.depends_on in review_attached_children:
                review_attached_children[child.depends_on].append(child)
            else:
                direct_based_on_children.append(child)

        direct_children = [*review_children, *direct_based_on_children]
        direct_children.sort(key=lambda child: _lineage_child_sort_key(task, child))

        for child in direct_children:
            child_node = _build_node(child, parent_task=task, depth=depth + 1)
            if child.task_type == "review" and child.id is not None:
                nested_children = review_attached_children.get(child.id, [])
                nested_children.sort(
                    key=lambda nested_child: _lineage_child_sort_key(child, nested_child)
                )
                child_node.children.extend(
                    _build_node(
                        nested_child,
                        parent_task=child,
                        depth=depth + 2,
                    )
                    for nested_child in nested_children
                )
            node.children.append(child_node)

        return node

    return _build_node(owner_task, parent_task=None, depth=0)


def _projection_field_choices(command_name: str, *, blocked_by_dropped: bool = False) -> tuple[str, ...]:
    """Return the valid explicit projection fields for a query command surface."""
    if command_name == "history":
        return _HISTORY_EXPLICIT_PROJECTION_FIELDS
    if command_name == "search":
        return _SEARCH_EXPLICIT_PROJECTION_FIELDS
    if command_name == "incomplete":
        return (
            _INCOMPLETE_BLOCKED_DROPPED_PROJECTION_FIELDS
            if blocked_by_dropped
            else _INCOMPLETE_PROJECTION_FIELDS
        )
    if command_name == "unmerged":
        return _UNMERGED_PROJECTION_FIELDS
    if command_name == "merged":
        return _MERGED_PROJECTION_FIELDS
    raise ValueError(f"unknown projection command surface: {command_name}")


def _projection_field_command_label(command_name: str) -> str:
    """Return the user-facing command label for projection errors."""
    return f"gza {command_name}"


def _projection_field_invocation(command_name: str, *, blocked_by_dropped: bool = False) -> str:
    """Return the exact command invocation for projection field discovery."""
    if command_name == "incomplete" and blocked_by_dropped:
        return "gza incomplete --blocked-by-dropped"
    return f"gza {command_name}"


def _projection_field_hint(command_name: str, *, blocked_by_dropped: bool = False) -> str:
    """Return the actionable help hint for invalid projection requests."""
    return (
        "Run uv run "
        f"{_projection_field_invocation(command_name, blocked_by_dropped=blocked_by_dropped)} "
        "--list-fields to list valid fields."
    )


def _format_projection_fields(fields: tuple[str, ...]) -> str:
    """Format projection field lists in the canonical CLI order."""
    return ", ".join(fields)


def _print_projection_fields(command_name: str, *, blocked_by_dropped: bool = False) -> int:
    """Print the valid projection fields for a command surface and exit."""
    print(_format_projection_fields(_projection_field_choices(command_name, blocked_by_dropped=blocked_by_dropped)))
    return 0


def _validate_projection_fields(
    fields: tuple[str, ...] | None,
    *,
    command_name: str,
    blocked_by_dropped: bool = False,
) -> tuple[str, ...] | None:
    """Validate requested projection fields for a specific command."""
    if fields is None:
        return None
    allowed_fields = _projection_field_choices(command_name, blocked_by_dropped=blocked_by_dropped)
    allowed = set(allowed_fields)
    invalid = tuple(field_name for field_name in fields if field_name not in allowed)
    if invalid:
        noun = "field" if len(invalid) == 1 else "fields"
        print(
            f"error: unknown {noun} for {_projection_field_command_label(command_name)}: {', '.join(invalid)}\n"
            f"valid fields: {_format_projection_fields(allowed_fields)}\n"
            f"{_projection_field_hint(command_name, blocked_by_dropped=blocked_by_dropped)}",
            file=sys.stderr,
        )
        return None
    return fields


def _render_projection_result(result: _TaskQueryResult, *, use_json: bool) -> None:
    """Render an explicit projection result in either text or JSON mode."""
    rendered = result.render(cast(_PresentationMode, "json" if use_json else "blocks"))
    if not rendered:
        return
    if use_json:
        print(rendered)
    else:
        console.print(rendered)


def _enrich_unmerged_result(
    result: _TaskQueryResult,
    *,
    store: SqliteTaskStore,
    config: Config,
    git_client: _UnmergedGit,
    target_branch: str,
    default_branch: str,
    live_branch_states: dict[str, str | None] | None = None,
) -> _TaskQueryResult:
    effective_fields = _unmerged_effective_fields(result.query)
    needs_lineage_text = "lineage_text" in effective_fields
    needs_branch_metadata = bool(
        effective_fields
        & {
            "branch",
            "target_branch",
            "branch_deleted",
            "commit_count",
            "files_changed",
            "insertions",
            "deletions",
            "has_conflicts",
        }
    )
    needs_review_metadata = bool(
        effective_fields
        & {
            "review_status",
            "review_detail",
            "review_verdict",
            "review_score",
        }
    )
    needs_pr_metadata = "pr_url" in effective_fields
    gh: GitHub | None = None
    gh_available: bool | None = None
    if needs_pr_metadata and config.pr_integration:
        gh = GitHub()
    rows: list[_TaskRow | _LineageRow] = []

    for row in result.rows:
        if not isinstance(row, _LineageRow):
            rows.append(row)
            continue

        owner_task = _resolve_lineage_owner_task(store, row.owner_task)
        merge_unit = (
            store.resolve_merge_unit_for_task(owner_task.id)
            if owner_task.id is not None
            else None
        )
        pruned_tree = _descendants_only_unmerged_lineage_tree(store, owner_task=owner_task)
        members = tuple(_flatten_query_lineage_tree(pruned_tree)) if pruned_tree is not None else row.members
        representative_branch = owner_task.branch

        def _task_recency_key(task: DbTask) -> tuple[int, datetime]:
            return (
                _task_id_numeric_key(task.id),
                _normalize_task_timestamp(task.completed_at or task.created_at),
            )

        branch_implement_tasks = [
            task
            for task in members
            if (
                task.task_type == "implement"
                and task.branch == representative_branch
                and task.status in {"completed", "unmerged"}
            )
        ]
        if branch_implement_tasks:
            representative_task = max(branch_implement_tasks, key=_task_recency_key)
        elif members:
            representative_task = max(members, key=_task_recency_key)
        else:
            representative_task = owner_task

        representative_branch = representative_task.branch or representative_branch
        lineage_root = _resolve_lineage_root_task(store, representative_task)

        root_branch = lineage_root.branch
        include_root_review_fallback = bool(
            lineage_root.id is not None
            and (
                lineage_root.id == representative_task.id
                or not representative_branch
                or not root_branch
                or root_branch == representative_branch
            )
        )
        effective_review_cleared_at = (
            lineage_root.review_cleared_at if include_root_review_fallback else None
        )
        review_classification = "no review"
        review_detail = None
        review_verdict = None
        review_score: int | None = None
        if needs_review_metadata or needs_lineage_text:
            code_changing_tasks = _get_code_changing_descendants_for_root_task(store, lineage_root)
            review_source_task_ids: set[str] = set()
            same_branch_code_changing_tasks: list[DbTask] = []
            same_branch_code_change_ids: set[str] = set()
            for task in code_changing_tasks:
                if not task.branch or task.branch != representative_branch:
                    continue
                if task.id is not None and task.id in same_branch_code_change_ids:
                    continue
                if task.id is not None:
                    same_branch_code_change_ids.add(task.id)
                same_branch_code_changing_tasks.append(task)

            stack = [pruned_tree] if pruned_tree is not None else []
            while stack:
                node = stack.pop()
                task = node.task
                if (
                    task.task_type == "implement"
                    and task.branch
                    and task.branch == representative_branch
                    and task.id != lineage_root.id
                ):
                    if task.id is None or task.id not in same_branch_code_change_ids:
                        if task.id is not None:
                            same_branch_code_change_ids.add(task.id)
                        same_branch_code_changing_tasks.append(task)
                if (
                    task.id is not None
                    and task.task_type == "implement"
                    and (
                        (representative_branch and task.branch == representative_branch)
                        or (include_root_review_fallback and task.id == lineage_root.id)
                    )
                ):
                    review_source_task_ids.add(task.id)
                stack.extend(node.children)

            for task in same_branch_code_changing_tasks:
                if task.id is not None:
                    review_source_task_ids.add(task.id)

            reviews: list[DbTask] = []
            seen_review_ids: set[str] = set()
            for task_id in review_source_task_ids:
                for review in store.get_reviews_for_task(task_id):
                    if review.id is None or review.id in seen_review_ids:
                        continue
                    seen_review_ids.add(review.id)
                    reviews.append(review)
            if reviews:
                reviews.sort(
                    key=lambda review: (
                        _normalize_task_timestamp(review.completed_at),
                        _task_id_numeric_key(review.id if isinstance(review.id, str) else None),
                    ),
                    reverse=True,
                )
            elif include_root_review_fallback:
                reviews = _get_reviews_for_root_task(store, lineage_root)

            latest_review = next((review for review in reviews if review.status == "completed"), None)
            latest_code_change = max(
                (task for task in same_branch_code_changing_tasks if task.completed_at is not None),
                key=lambda task: _normalize_task_timestamp(task.completed_at),
                default=None,
            )

            if latest_review is not None and latest_review.completed_at is not None:
                latest_review_completed = _normalize_task_timestamp(latest_review.completed_at)
                review_cleared_stale = bool(
                    effective_review_cleared_at
                    and _normalize_task_timestamp(effective_review_cleared_at) >= latest_review_completed
                )
                latest_code_change_stale = bool(
                    latest_code_change
                    and latest_code_change.completed_at
                    and _normalize_task_timestamp(latest_code_change.completed_at) > latest_review_completed
                )
                review_is_stale = review_cleared_stale or latest_code_change_stale

                if review_is_stale:
                    review_classification = "review stale"
                    latest_review_id = latest_review.id if latest_review.id is not None else "?"
                    if review_cleared_stale:
                        review_detail = f"review state cleared after last review {latest_review_id}"
                    elif latest_code_change_stale and latest_code_change and latest_code_change.id is not None:
                        review_detail = (
                            f"last review {latest_review_id} before latest "
                            f"{latest_code_change.task_type} {latest_code_change.id}"
                        )
                    else:
                        review_detail = f"last review {latest_review_id} is stale"
                else:
                    review_classification = "reviewed"

                if review_classification != "review stale":
                    for review in reviews:
                        if review.status != "completed" or review.completed_at is None:
                            continue
                        if (
                            effective_review_cleared_at
                            and _normalize_task_timestamp(effective_review_cleared_at)
                            >= _normalize_task_timestamp(review.completed_at)
                        ):
                            continue
                        parsed_verdict = get_review_verdict(config, review)
                        if parsed_verdict:
                            review_verdict = parsed_verdict
                            review_score = review.review_score
                            break

        verdict_label = None
        if needs_review_metadata:
            verdict_label = _format_review_verdict_label(review_verdict)

        branch_deleted = False
        commit_count: int | None = None
        files_changed: int | None = None
        insertions: int | None = None
        deletions: int | None = None
        has_conflicts = False
        if needs_branch_metadata:
            if representative_branch and git_client.branch_exists(representative_branch):
                use_cached_stats = (
                    target_branch == default_branch
                    and representative_task.diff_files_changed is not None
                )
                if use_cached_stats:
                    files_changed = representative_task.diff_files_changed
                    insertions = representative_task.diff_lines_added or 0
                    deletions = representative_task.diff_lines_removed or 0
                else:
                    revision_range = f"{target_branch}...{representative_branch}"
                    files_changed, insertions, deletions = git_client.get_diff_stat_parsed(revision_range)
                commit_count = git_client.count_commits_ahead(representative_branch, target_branch)
                has_conflicts = not git_client.can_merge(representative_branch, target_branch)
            else:
                branch_deleted = bool(representative_branch)

        pr_url: str | None = None
        if needs_pr_metadata and gh is not None and config.pr_integration:
            if gh_available is None:
                gh_available = gh.is_available()
            pr_lookup = lookup_task_pr(
                owner_task,
                gh=gh,
                available=gh_available,
                pr_integration=config.pr_integration,
                include_number=False,
            )
            if pr_lookup.found and pr_lookup.pr_url:
                pr_url = pr_lookup.pr_url

        unmerged_values = dict(row.values)
        live_merge_state = (
            live_branch_states.get(representative_branch)
            if live_branch_states is not None and representative_branch is not None
            else None
        )
        merge_unit_id = merge_unit.id if merge_unit is not None else None
        merge_unit_state = merge_unit.state if merge_unit is not None else owner_task.merge_status
        source_branch = merge_unit.source_branch if merge_unit is not None else representative_branch
        if live_merge_state is not None:
            merge_unit_state = live_merge_state
            if merge_unit is None or merge_unit.target_branch != target_branch:
                merge_unit_id = None
                source_branch = representative_branch
        unmerged_values.update(
            {
                "member_ids": [member.id for member in members if member.id is not None],
                "unresolved_ids": [task.id for task in row.unresolved_tasks if task.id is not None],
                "lineage_text": (
                    _format_lineage(
                        pruned_tree,
                        annotate=True,
                        review_verdict_resolver=lambda review_task: get_review_verdict(config, review_task),
                    )
                    if needs_lineage_text and pruned_tree is not None
                    else None
                ),
                "branch": representative_branch,
                "source_branch": source_branch,
                "target_branch": target_branch,
                "merge_unit_id": merge_unit_id,
                "merge_unit_state": merge_unit_state,
                "branch_deleted": branch_deleted,
                "commit_count": commit_count,
                "files_changed": files_changed,
                "insertions": insertions,
                "deletions": deletions,
                "has_conflicts": has_conflicts,
                "pr_url": pr_url,
                "id": owner_task.id,
                "prompt": owner_task.prompt,
                "status": owner_task.status,
                "task_type": owner_task.task_type,
                "completed_at": owner_task.completed_at,
                "review_status": review_classification,
                "review_detail": review_detail,
                "review_verdict": verdict_label or review_verdict,
                "review_score": review_score,
                "report_file": owner_task.report_file,
                "stats": format_stats(owner_task),
                "completion_reason": owner_task.completion_reason,
                "failure_reason": (
                    owner_task.failure_reason
                    if owner_task.failure_reason
                    and owner_task.failure_reason != "UNKNOWN"
                    else None
                ),
            }
        )
        rows.append(
            _LineageRow(
                owner_task=owner_task,
                members=members,
                tree=pruned_tree,
                unresolved_tasks=row.unresolved_tasks,
                values=_apply_query_projection_values(
                    unmerged_values,
                    result.query.projection,
                    scope="lineages",
                ),
            )
        )

    return _TaskQueryResult(query=result.query, rows=tuple(rows), total_count=result.total_count)


def cmd_unmerged(args: argparse.Namespace, git: _UnmergedGit | None = None) -> int:
    """List tasks with unmerged work on branches."""
    from gza.db import needs_merge_status_migration

    def _is_readonly_snapshot_refresh_error(
        exc: sqlite3.OperationalError,
        *,
        db_path: Path,
        project_dir: Path,
    ) -> bool:
        if _is_readonly_snapshot_operational_error(exc):
            return True
        if "disk i/o error" not in str(exc).lower():
            return False
        try:
            if db_path.resolve() != (project_dir / ".gza" / "gza.db").resolve():
                return False
            return (db_path.stat().st_mode & 0o222) == 0
        except OSError:
            return False

    if getattr(args, "list_fields", False):
        return _print_projection_fields("unmerged")

    config = Config.load(args.project_dir)
    git_client: _UnmergedGit = git if git is not None else cast(_UnmergedGit, Git(config.project_dir))
    default_branch = git_client.default_branch()
    current_branch = git_client.current_branch()
    try:
        tag_filters, any_tag = parse_cli_tag_filters(args)
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1
    normalized_tag_filters = normalize_tag_filters(tag_filters)
    projection_fields = _validate_projection_fields(
        _parse_csv(getattr(args, "fields", None)),
        command_name="unmerged",
    )
    if getattr(args, "fields", None) is not None and projection_fields is None:
        return 2
    use_json = bool(getattr(args, "json", False))
    view_mode: _PresentationMode = "json" if use_json else ("blocks" if projection_fields is not None else "rich")
    if not use_json:
        print(f"On branch {current_branch}")
    target_branch = current_branch if getattr(args, "into_current", False) else (getattr(args, "target", None) or default_branch)
    live_target = _is_branch_target_live(args)

    store: SqliteTaskStore | None = None
    service: _TaskQueryService | None = None
    selected_tasks: list[DbTask] = []
    try:
        store = get_store(config, open_mode="query_only" if live_target else "readwrite")
        service = _TaskQueryService(store)
        live_branch_states: dict[str, str | None] | None = None

        if live_target:
            history = store.get_history(limit=None)
            branch_candidates = [
                task
                for task in history
                if task.status == "completed"
                and task.branch
                and task.has_commits
                and (task.task_type not in ("improve", "rebase", "fix") or task.based_on is None)
            ]
            _print_unmerged_progress(
                f"refreshing live merge truth against {target_branch} for "
                f"{len(branch_candidates)} candidate tasks",
                to_stderr=True,
            )
            live_results = reconcile_branch_merge_truth(
                cast(Git, git_client),
                build_branch_cohorts_for_tasks(store, branch_candidates),
                target_branch=target_branch,
                include_diff_stats=True,
                preserve_recorded_merged=False,
            )
            first_live_error = next(
                (
                    (result.branch, result.errors[0])
                    for result in live_results
                    if result.errors
                ),
                None,
            )
            if first_live_error is not None:
                failing_branch, error_text = first_live_error
                print(
                    "Error: failed to reconcile unmerged branches relative to "
                    f"{target_branch}: {failing_branch}: {error_text}"
                )
                return 1
            live_unmerged_branches = {
                result.branch
                for result in live_results
                if result.skipped_reason is None and result.merge_status not in TERMINAL_MERGE_STATES
            }
            live_branch_states = {
                result.branch: result.merge_status
                for result in live_results
                if result.skipped_reason is None and result.merge_status is not None
            }
            selected_tasks = [
                task for task in branch_candidates if task.branch in live_unmerged_branches
            ]
            if not use_json:
                console.print(
                    f"[{TASK_COLORS['task_id']}]Showing tasks unmerged relative to {target_branch}"
                    f"[/{TASK_COLORS['task_id']}]"
                )
        else:
            if needs_merge_status_migration(store):
                _print_unmerged_status(
                    f"[{TASK_COLORS['task_id']}]Migrating merge status for existing tasks..."
                    f"[/{TASK_COLORS['task_id']}]",
                    to_stderr=use_json,
                )
            refresh_cohorts = build_unmerged_branch_cohorts(store)
            _reconcile_results, refresh_error = _refresh_canonical_default_branch_merge_truth(
                store,
                cast(Git, git_client),
                refresh_cohorts,
                fetch_remote=bool(getattr(args, "fetch", False)),
                dry_run=False,
                include_diff_stats=True,
            )
            if refresh_error is not None:
                print(f"Error: failed to refresh canonical merge truth: {refresh_error}")
                return 1
            if store.supports_merge_units():
                selected_tasks = []
                for unit in store.get_unmerged_merge_units():
                    representative = store.resolve_merge_unit_representative_task(unit, require_actionable=True)
                    if representative is not None:
                        selected_tasks.append(representative)
            else:
                selected_tasks = [task for task in store.get_unmerged() if task.status == "completed"]
    except sqlite3.OperationalError as exc:
        if _is_readonly_snapshot_refresh_error(
            exc,
            db_path=store.db_path if store is not None else config.db_path,
            project_dir=config.project_dir,
        ):
            print(
                "Error: `gza unmerged` refreshes canonical default-branch merge truth and "
                "needs a writable task DB. This database is read-only."
            )
            return 1
        raise

    if not selected_tasks:
        _print_unmerged_empty(use_json=use_json)
        return 0

    owner_selected_pairs = [
        (_resolve_lineage_owner_task(store, task), task)
        for task in selected_tasks
    ]
    if normalized_tag_filters is not None:
        owner_selected_pairs = [
            (owner, task)
            for owner, task in owner_selected_pairs
            if task_matches_tag_filters(
                task_tags=owner.tags,
                tag_filters=normalized_tag_filters,
                any_tag=any_tag,
            )
        ]
    if not owner_selected_pairs:
        _print_unmerged_empty(use_json=use_json)
        return 0

    owner_ids = tuple(
        dict.fromkeys(owner.id for owner, _task in owner_selected_pairs if owner.id is not None)
    )
    if not owner_ids:
        _print_unmerged_empty(use_json=use_json)
        return 0

    limit = None if getattr(args, "limit", 5) == 0 else getattr(args, "limit", 5)
    merge_unit_ids: tuple[str, ...] | None = None
    if not live_target:
        merge_unit_ids_list: list[str] = []
        for _owner, task in owner_selected_pairs:
            if task.id is None:
                continue
            resolved_unit = store.resolve_merge_unit_for_task(task.id)
            if resolved_unit is not None:
                merge_unit_ids_list.append(resolved_unit.id)
        merge_unit_ids = tuple(dict.fromkeys(merge_unit_ids_list))
    projection = _TaskProjectionSpec(
        preset=_TaskProjectionPreset.UNMERGED_DEFAULT,
        fields=projection_fields,
    )
    query = _TaskQueryPresets.unmerged(
        branch_owner_ids=owner_ids,
        merge_unit_ids=merge_unit_ids,
        task_ids=tuple(task.id for _owner, task in owner_selected_pairs if task.id is not None),
        limit=limit,
        mode=view_mode,
        projection=projection,
    )

    scan_count = len(store.get_all())
    _print_unmerged_progress(
        f"running unmerged query over {scan_count} task rows for {len(owner_ids)} selected branches",
        to_stderr=True,
    )
    result = service.run(query)
    result = _enrich_unmerged_result(
        result,
        store=store,
        config=config,
        git_client=git_client,
        target_branch=target_branch,
        default_branch=default_branch,
        live_branch_states=live_branch_states,
    )
    _print_unmerged_progress(
        f"rendering {len(result.rows)} row(s) from {result.total_count or 0} filtered result(s) as {view_mode}",
        to_stderr=True,
    )

    rendered = result.render(view_mode)
    if rendered:
        if use_json:
            print(rendered)
        else:
            console.print(rendered)
    else:
        _print_unmerged_empty(use_json=use_json)
        return 0

    if limit is not None and result.total_count and result.total_count > limit:
        footer = f"\n[dim]Showing {limit} of {result.total_count} unmerged tasks (use -n 0 for all)[/dim]"
        if use_json:
            _stderr_console.print(footer)
        else:
            console.print(footer)

    return 0


def cmd_stale_unmerged(args: argparse.Namespace) -> int:
    """Report or drop abandoned unmerged merge units conservatively."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    candidates = collect_stale_unmerged_sweep_candidates(
        store,
        threshold_days=args.days,
    )
    git_client = Git(config.project_dir)
    candidates, refresh_error = _prove_stale_unmerged_candidates_against_canonical_target(
        store,
        git_client,
        candidates,
    )
    if refresh_error is not None:
        print(f"Error: failed to refresh canonical merge truth: {refresh_error}")
        return 1
    execute = bool(getattr(args, "execute", False))
    use_json = bool(getattr(args, "json", False))

    applied_drop_task_ids_by_unit: dict[str, tuple[str, ...]] = {}
    dropped_task_count = 0
    if execute:
        for candidate in candidates:
            applied_drop_task_ids = _apply_stale_unmerged_candidate_drops(
                config=config,
                store=store,
                candidate=candidate,
            )
            applied_drop_task_ids_by_unit[candidate.merge_unit.id] = applied_drop_task_ids
            dropped_task_count += len(applied_drop_task_ids)

    if use_json:
        rows = [
            _stale_unmerged_candidate_row(
                candidate,
                execute_requested=execute,
                applied_drop_task_ids=applied_drop_task_ids_by_unit.get(candidate.merge_unit.id, ()),
            )
            for candidate in candidates
        ]
        print(json.dumps(rows, indent=2))
        return 0

    if not candidates:
        console.print("No stale unmerged merge units found")
        return 0

    action_label = "Dropping" if execute else "Would drop"
    console.print(
        f"{action_label} {len(candidates)} stale unmerged merge unit(s) older than {args.days} day(s):"
    )
    for candidate in candidates:
        owner_id = candidate.owner_task.id or "unknown"
        last_activity = candidate.last_activity_at.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")
        console.print(
            rich_escape(
                f"{owner_id}: {candidate.owner_task.prompt} "
                f"[{candidate.merge_unit.source_branch} -> {candidate.merge_unit.target_branch}] "
                f"last-activity={last_activity} stale={candidate.stale_days}d "
                f"drop={', '.join(candidate.drop_task_ids)}"
            )
        )
    if not execute:
        console.print("Dry run only. Re-run with `--execute` to apply these drops.")
        return 0

    console.print(
        f"Dropped {dropped_task_count} task(s) across {len(candidates)} stale unmerged merge unit(s)"
    )
    return 0


def _refresh_canonical_default_branch_merge_truth(
    store: SqliteTaskStore,
    git_client: Git,
    cohorts: list[BranchCohort],
    *,
    fetch_remote: bool,
    dry_run: bool,
    include_diff_stats: bool,
) -> tuple[list[BranchSyncResult], str | None]:
    refresh_candidate_count = sum(len(cohort.tasks) for cohort in cohorts)
    _print_unmerged_progress(
        f"refreshing canonical merge truth for {refresh_candidate_count} candidate tasks "
        f"across {len(cohorts)} branches",
        to_stderr=True,
    )
    reconcile_results, partial = sync_branch_cohorts(
        store,
        git_client,
        cohorts,
        include_git=True,
        include_pr=False,
        include_diff_stats=include_diff_stats,
        dry_run=dry_run,
        fetch_remote=fetch_remote,
        allow_cached_remote_target_ref_without_fetch=True,
    )
    errors = [error for result in reconcile_results for error in result.errors]
    if partial and errors:
        return reconcile_results, errors[0]
    if errors:
        return reconcile_results, errors[0]
    return reconcile_results, None


def _stale_unmerged_proof_failure_message(
    candidate: StaleUnmergedSweepCandidate,
    result: BranchSyncResult | None,
) -> str | None:
    owner_id = candidate.owner_task.id or "unknown"
    if result is None:
        return (
            f"candidate {owner_id} (merge unit {candidate.merge_unit.id}) "
            "could not be proven against the canonical target: missing canonical proof result"
        )
    if result.skipped_reason is not None:
        return (
            f"candidate {owner_id} (merge unit {candidate.merge_unit.id}) "
            f"could not be proven against the canonical target: {result.skipped_reason}"
        )

    # The stale sweep is destructive under ``--execute``, so warning-only
    # degradation that preserves the cached non-terminal state is not enough
    # proof to keep a candidate drop-eligible.
    proof_warning = next(
        (
            warning
            for warning in result.warnings
            if "could not determine unique commit count" in warning
            and "preserving existing merge state" in warning
        ),
        None,
    )
    if proof_warning is not None:
        return (
            f"candidate {owner_id} (merge unit {candidate.merge_unit.id}) "
            f"could not be proven against the canonical target: {proof_warning}"
        )
    return None


def _prove_stale_unmerged_candidates_against_canonical_target(
    store: SqliteTaskStore,
    git_client: Git,
    candidates: tuple[StaleUnmergedSweepCandidate, ...],
) -> tuple[tuple[StaleUnmergedSweepCandidate, ...], str | None]:
    if not candidates:
        return candidates, None

    candidate_tasks = [candidate.owner_task for candidate in candidates if candidate.owner_task.id is not None]
    candidate_cohorts = build_branch_cohorts_for_tasks(store, candidate_tasks)
    reconcile_results, refresh_error = _refresh_canonical_default_branch_merge_truth(
        store,
        git_client,
        candidate_cohorts,
        fetch_remote=False,
        dry_run=True,
        include_diff_stats=False,
    )
    if refresh_error is not None:
        return (), refresh_error

    results_by_unit_id = {
        cohort.merge_unit_id: result
        for cohort, result in zip(candidate_cohorts, reconcile_results, strict=True)
        if cohort.merge_unit_id is not None
    }
    filtered_candidates = []
    for candidate in candidates:
        result = results_by_unit_id.get(candidate.merge_unit.id)
        proof_failure = _stale_unmerged_proof_failure_message(candidate, result)
        if proof_failure is not None:
            return (), proof_failure
        assert result is not None
        if result.merge_status in TERMINAL_MERGE_STATES:
            continue
        filtered_candidates.append(candidate)
    return tuple(filtered_candidates), None


def _apply_stale_unmerged_candidate_drops(
    *,
    config: Config,
    store: SqliteTaskStore,
    candidate: StaleUnmergedSweepCandidate,
) -> tuple[str, ...]:
    applied_drop_task_ids: list[str] = []
    for task_id in candidate.drop_task_ids:
        task = store.get(task_id)
        if task is None or task.status == "dropped":
            continue
        apply_manual_task_status(
            config=config,
            store=store,
            task=task,
            status="dropped",
        )
        applied_drop_task_ids.append(task_id)
    return tuple(applied_drop_task_ids)


def _stale_unmerged_candidate_row(
    candidate: StaleUnmergedSweepCandidate,
    *,
    execute_requested: bool,
    applied_drop_task_ids: tuple[str, ...],
) -> dict[str, object]:
    return {
        "owner_task_id": candidate.owner_task.id,
        "prompt": candidate.owner_task.prompt,
        "merge_unit_id": candidate.merge_unit.id,
        "source_branch": candidate.merge_unit.source_branch,
        "target_branch": candidate.merge_unit.target_branch,
        "merge_unit_state": candidate.merge_unit.state,
        "member_task_ids": list(candidate.member_task_ids),
        "drop_task_ids": list(candidate.drop_task_ids),
        "last_activity_at": candidate.last_activity_at.astimezone(UTC).isoformat(),
        "stale_days": candidate.stale_days,
        "execute_requested": execute_requested,
        "applied_drop_task_ids": list(applied_drop_task_ids),
    }


def _print_ps_output(
    args: argparse.Namespace,
    registry: "WorkerRegistry",
    store: "SqliteTaskStore",
    poll_interval: int | None = None,
    seen_tasks: "dict | None" = None,
    show_all: bool = False,
    recent_minutes: int = 1,
    poll_started_at: "_dt.datetime | None" = None,
    last_poll_at: "_dt.datetime | None" = None,
    sort_mode: str = "status",
    order: str = "desc",
) -> None:
    """Print ps output once. Used by cmd_ps directly and in poll loop.

    When seen_tasks is provided (poll mode), rows from this dict are merged with
    live results so that completed/failed tasks remain visible.
    """
    # Include completed workers so startup failures and poll transitions remain visible.
    live_rows, _ = _build_ps_rows(registry, store, include_completed=True)

    # In poll mode: update seen_tasks with new live data, preserving vanished tasks.
    if seen_tasks is not None:
        poll_now = _dt.datetime.now(_dt.UTC)
        recent_cutoff = poll_now - _dt.timedelta(minutes=recent_minutes) if recent_minutes > 0 else None
        live_keys = set()
        for row in live_rows:
            key = row["task_id"] if row["task_id"] is not None else row["worker_id"]
            # Only adopt a row into seen_tasks if it's currently active, if we
            # already track it (status transition), or if it is a startup
            # failure. This preserves first-seen startup failures in poll mode
            # while still avoiding unrelated completed history.
            ended_at_iso = row.get("ended_at")
            ended_after_last_poll = False
            ended_recently = False
            if ended_at_iso:
                try:
                    ended_dt = _dt.datetime.fromisoformat(ended_at_iso)
                    if ended_dt.tzinfo is None:
                        ended_dt = ended_dt.replace(tzinfo=_dt.UTC)
                    if last_poll_at is not None:
                        ended_after_last_poll = ended_dt >= last_poll_at
                    if (
                        recent_cutoff is not None
                        and row["status"] in ("completed", "failed")
                        and recent_cutoff <= ended_dt <= poll_now
                    ):
                        ended_recently = True
                except (TypeError, ValueError):
                    pass
            if (
                key in seen_tasks
                or row["status"] in ("in_progress", "stale")
                or row.get("startup_failure", False)
                or ended_after_last_poll
                or ended_recently
            ):
                seen_tasks[key] = row
            live_keys.add(key)

        # Re-fetch DB status for ALL tracked tasks that still appear active.
        # This catches status transitions regardless of whether the task is
        # still in live_rows (e.g. worker exists but task completed in DB).
        for key, row in list(seen_tasks.items()):
            if isinstance(key, str) and row.get("task_id") is not None and row["status"] == "in_progress":
                task = store.get(key)
                if task and task.status in ("completed", "failed"):
                    row["status"] = task.status

        rows = list(seen_tasks.values())
    else:
        rows = live_rows

    # Outside poll mode, filter out completed/failed tasks except startup failures.
    # In poll mode, completed tasks remain visible via seen_tasks.
    # With --all, show everything including ordinary completed/failed rows.
    if seen_tasks is None and not show_all:
        rows = [
            r
            for r in rows
            if r["status"] not in ("completed", "failed") or r.get("startup_failure", False)
        ]
    _ps_sort_rows(rows, store, mode=sort_mode, descending=(order == "desc"))

    if poll_interval is not None:
        now = _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
        started_str = (
            poll_started_at.strftime("%Y-%m-%d %H:%M:%S UTC")
            if poll_started_at is not None
            else now
        )
        print(
            f"Refreshing every {poll_interval}s — started: {started_str} — "
            f"last updated: {now}  (Ctrl+C to exit)"
        )
        if _ps_poll_is_interactive(sys.stdin, sys.stdout):
            print(f"keys: [l]ineage [d]ate [s]tatus  [t]oggle order  [q]uit   sort: {sort_mode} {order}")
        print()

    if not rows:
        print("No in-progress tasks (use --poll to monitor)")
        return

    if hasattr(args, "quiet") and args.quiet:
        for row in rows:
            if row["task_id"] is not None:
                print(row["task_id"])
        return

    if hasattr(args, "json") and args.json:
        import json as json_lib
        print(json_lib.dumps(rows, indent=2))
        return

    # Color scheme for ps output — defined in gza.colors.
    STATUS_COLORS = PS_STATUS_COLORS
    task_id_color = _colors.TASK_COLORS.task_id
    task_prompt_color = _colors.TASK_COLORS.prompt
    header_color = _colors.TASK_COLORS.header

    columns = [
        ("TASK ID", "task_id"),
        ("TYPE", "type"),
        ("STATUS", "status"),
        ("PID", "pid"),
        ("STARTED", "started"),
        ("STEPS", "steps"),
        ("DURATION", "duration"),
        ("MODEL", "model"),
        ("MERGE UNIT", "merge_unit"),
    ]
    cells = []
    for row in rows:
        status = row["status"]
        if status == "failed" and row.get("startup_failure"):
            status = "failed(startup)"
        cells.append(
            {
                "task_id": f"{row['task_id']}" if row["task_id"] is not None else "",
                "type": row["type"],
                "status": status,
                "sc": STATUS_COLORS.get(status, "white"),
                "pid": str(row["pid"]),
                "started": row["started"],
                "steps": str(row["steps"]),
                "duration": row["duration"],
                "model": row["model"] or "-",
                "merge_unit": row.get("merge_unit") or "-",
                "task": row["task"].replace("[", "\\[") if row["task"] else "",
            }
        )

    widths = {key: len(label) for label, key in columns}
    for cell in cells:
        for _, key in columns:
            widths[key] = max(widths[key], len(cell[key]))

    header = " ".join(f"{label:<{widths[key]}}" for label, key in columns) + " TASK"
    console.print(f"[{header_color}]{header}[/{header_color}]", soft_wrap=True)
    console.print(f"[{header_color}]" + "─" * len(header) + f"[/{header_color}]", soft_wrap=True)

    for cell in cells:
        console.print(
            f"[{task_id_color}]{cell['task_id']:<{widths['task_id']}}[/{task_id_color}] "
            f"{cell['type']:<{widths['type']}} "
            f"[{cell['sc']}]{cell['status']:<{widths['status']}}[/{cell['sc']}] "
            f"{cell['pid']:<{widths['pid']}} {cell['started']:<{widths['started']}} "
            f"{cell['steps']:<{widths['steps']}} {cell['duration']:<{widths['duration']}} "
            f"{cell['model']:<{widths['model']}} {cell['merge_unit']:<{widths['merge_unit']}} "
            f"[{task_prompt_color}]{cell['task']}[/{task_prompt_color}]",
            soft_wrap=True,
        )


def cmd_ps(args: argparse.Namespace) -> int:
    """List running and completed workers."""
    import time
    config = Config.load(args.project_dir)
    registry = WorkerRegistry(config.workers_path)
    store = get_store(config, open_mode="query_only")
    # Worker registry is now a thin process index; no ps-specific cleanup.
    poll_interval: int | None = getattr(args, "poll", None)
    show_all: bool = getattr(args, "all", False)
    recent_minutes = getattr(args, "recent_minutes", 1)
    sort_mode = getattr(args, "sort", "status")
    order = getattr(args, "order", "desc")
    if recent_minutes < 0:
        print(
            f"error: --recent-minutes must be >= 0 (got {recent_minutes})",
            file=sys.stderr,
        )
        return 1

    if poll_interval is not None:
        if poll_interval < 1:
            print(f"error: --poll value must be at least 1 second (got {poll_interval})", file=sys.stderr)
            return 1
        # Poll runs indefinitely until Ctrl+C — no auto-stop when tasks complete,
        # since new tasks may start at any time.
        import datetime as _dt
        seen_tasks: dict = {}
        poll_started_at = _dt.datetime.now(_dt.UTC)
        last_poll_at: _dt.datetime | None = None
        interactive_poll = _ps_poll_is_interactive(sys.stdin, sys.stdout)
        original_term_attrs: Any = None
        try:
            if interactive_poll:
                original_term_attrs = termios.tcgetattr(sys.stdin.fileno())
                tty.setcbreak(sys.stdin.fileno())
            while True:
                if sys.stdout.isatty():
                    print("\033[2J\033[H", end="")  # clear screen, move cursor to top
                _print_ps_output(
                    args, registry, store,
                    poll_interval=poll_interval,
                    seen_tasks=seen_tasks,
                    show_all=show_all,
                    recent_minutes=recent_minutes,
                    poll_started_at=poll_started_at,
                    last_poll_at=last_poll_at,
                    sort_mode=sort_mode,
                    order=order,
                )
                last_poll_at = _dt.datetime.now(_dt.UTC)
                if not interactive_poll:
                    time.sleep(poll_interval)
                    continue

                deadline = time.monotonic() + poll_interval
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    ready, _, _ = select.select([sys.stdin], [], [], remaining)
                    if not ready:
                        break
                    key = sys.stdin.read(1)
                    next_mode, next_order, refresh_now = _apply_ps_key(key, sort_mode, order)
                    if key == "q":
                        return 0
                    if not refresh_now:
                        continue
                    sort_mode = next_mode
                    order = next_order
                    break
        except KeyboardInterrupt:
            return 0
        finally:
            if original_term_attrs is not None:
                termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, original_term_attrs)
    else:
        _print_ps_output(
            args,
            registry,
            store,
            show_all=show_all,
            recent_minutes=recent_minutes,
            sort_mode=sort_mode,
            order=order,
        )

    return 0


def _build_ps_rows(
    registry: WorkerRegistry,
    store: SqliteTaskStore,
    include_completed: bool,
) -> tuple[list[dict], list[DbTask]]:
    """Build reconciled ps rows from worker registry and DB in-progress tasks.

    Returns a tuple of (rows, in_progress_tasks) so callers can reuse the
    already-fetched in-progress task objects without an extra DB round-trip.
    """
    workers = registry.list_all(include_completed=include_completed)
    in_progress_tasks = store.get_in_progress()
    merged: dict[tuple[str, str], dict] = {}

    for worker in workers:
        if worker.status == "running" and not registry.is_running(worker.worker_id):
            worker.status = "stale"

        key = ("task", str(worker.task_id)) if worker.task_id is not None else ("worker", worker.worker_id)
        existing = merged.get(key)
        if existing and existing["worker"] is not None:
            if _prefer_worker(existing["worker"], worker):
                existing["worker"] = worker
            continue

        task = store.get(worker.task_id) if worker.task_id is not None else None
        merged[key] = {"worker": worker, "task": task}

    for task in in_progress_tasks:
        assert task.id is not None
        key = ("task", str(task.id))
        if key in merged:
            merged[key]["task"] = task
        else:
            merged[key] = {"worker": None, "task": task}

    rows = [_to_ps_row(item["worker"], item["task"], store) for item in merged.values()]
    rows.sort(key=_ps_sort_key)
    return rows, in_progress_tasks


def _get_orphaned_tasks(registry: WorkerRegistry, store: SqliteTaskStore) -> list[DbTask]:
    """Return in-progress tasks that have no active worker (orphaned/stale)."""
    rows, in_progress = _build_ps_rows(registry, store, include_completed=False)
    orphaned_task_ids = {
        row["task_id"] for row in rows
        if row["is_orphaned"] and row["task_id"] is not None
    }
    if not orphaned_task_ids:
        return []
    return [t for t in in_progress if t.id in orphaned_task_ids]


def _print_orphaned_warning(orphaned: list[DbTask], *, to_stderr: bool = False) -> None:
    """Print a warning about orphaned tasks with a suggestion to resume."""
    out = _stderr_console if to_stderr else console
    count = len(orphaned)
    plural = "tasks" if count != 1 else "task"
    out.print(f"\n[yellow]⚠  {count} orphaned {plural} found (in-progress with no active worker):[/yellow]")
    for task in orphaned:
        type_label = f"[{task.task_type}] " if task.task_type != "implement" else ""
        first_line = task.prompt.split('\n')[0].strip()
        prompt_display = truncate(first_line, MAX_PROMPT_DISPLAY)
        # Keep the task line literal so bracketed prompt text remains searchable
        # even when stderr/stdout is captured from a colorized terminal session.
        out.print(f"   ({task.id}) {type_label}{prompt_display}", markup=False, highlight=False)
    out.print(
        "   Run [cyan]gza work <full-task-id>[/cyan] to resume, or "
        "[cyan]gza mark-completed --force <full-task-id>[/cyan] to clear."
    )


def _ps_sort_key(row: dict) -> tuple[int, bool, float, int, str]:
    """Sort ps rows by status group, then by start time, then stable identifiers.

    In-progress tasks sort first (ascending start time so longest-running is
    top).  Failed tasks next, then completed, then everything else.
    Non-in-progress groups sort by start time *descending* so the most
    recently started task appears right after the running ones."""
    status = row.get("status", "")
    # in_progress=0 (top), failed=1, completed=2, dropped/other=3 (bottom)
    # pending tasks are not shown in ps output so not handled here.
    if status == "in_progress":
        status_group = 0
    elif status == "failed":
        status_group = 1
    elif status == "completed":
        status_group = 2
    else:
        status_group = 3

    sort_timestamp = row["sort_timestamp"] or ""
    has_no_timestamp = sort_timestamp == ""

    # Convert to numeric so we can negate for descending sort.
    if sort_timestamp:
        try:
            ts_numeric = datetime.fromisoformat(sort_timestamp).timestamp()
        except (ValueError, OSError):
            ts_numeric = 0.0
    else:
        ts_numeric = 0.0

    # In-progress: ascending (longest running first = earliest start).
    # Everything else: descending (most recently started first).
    if status_group != 0:
        ts_numeric = -ts_numeric

    raw_task_id = row.get("task_id")
    if isinstance(raw_task_id, str):
        # Decode numeric suffix for ordering (handles "prefix-<decimal>" format)
        decoded = _task_id_numeric_key(raw_task_id)
        if decoded != 0:
            task_id_sort = decoded
        else:
            # Fallback for legacy worker metadata files with bare-integer task IDs
            # (e.g. "123" stored without prefix during rolling migration)
            try:
                task_id_sort = int(raw_task_id)
            except (ValueError, TypeError):
                task_id_sort = sys.maxsize
    elif isinstance(raw_task_id, int):
        task_id_sort = raw_task_id  # backward compat for any stale integer values
    else:
        task_id_sort = sys.maxsize  # worker-only rows (no task) sort last
    worker_id = row.get("worker_id", "")
    return (status_group, has_no_timestamp, ts_numeric, task_id_sort, worker_id)


def _ps_sort_rows(
    rows: list[dict],
    store: SqliteTaskStore,
    *,
    mode: str = "status",
    descending: bool = True,
) -> None:
    """Sort ps rows in place for the selected presentation mode."""
    if mode == "status":
        rows.sort(key=lambda row: _ps_status_sort_key(row, descending=descending))
        return

    if mode == "date":
        rows.sort(key=lambda row: _ps_date_sort_key(row, descending=descending))
        return

    if mode != "lineage":
        raise ValueError(f"Unsupported ps sort mode: {mode}")

    task_cache: dict[str, DbTask | None] = {}
    root_cache: dict[str, str | None] = {}
    group_recency: dict[str, float] = {}
    group_is_worker_only: dict[str, bool] = {}
    root_numeric_sort: dict[str, int] = {}
    root_text_sort: dict[str, str] = {}

    for row in rows:
        task_id = row.get("task_id")
        ts_numeric = _ps_sort_timestamp_numeric(row)
        if isinstance(task_id, str):
            task = task_cache.get(task_id)
            if task_id not in task_cache:
                task = store.get(task_id)
                task_cache[task_id] = task
            root_id = root_cache.get(task_id)
            if task_id not in root_cache:
                root_task = _resolve_lineage_root_task(store, task) if task is not None else None
                root_id = root_task.id if root_task is not None else task_id
                root_cache[task_id] = root_id
            assert root_id is not None
            group_id = root_id
            group_is_worker_only[group_id] = False
            root_numeric_sort[group_id] = _task_id_sort_value(root_id)
            root_text_sort[group_id] = root_id
        else:
            worker_id = str(row.get("worker_id", ""))
            group_id = f"~worker:{worker_id}"
            group_is_worker_only[group_id] = True
            root_numeric_sort[group_id] = sys.maxsize
            root_text_sort[group_id] = worker_id

        row["_ps_group_id"] = group_id
        group_recency[group_id] = max(group_recency.get(group_id, float("-inf")), ts_numeric)

    rows.sort(
        key=lambda row: _ps_lineage_sort_key(
            row,
            descending=descending,
            group_recency=group_recency,
            group_is_worker_only=group_is_worker_only,
            root_numeric_sort=root_numeric_sort,
            root_text_sort=root_text_sort,
        )
    )
    for row in rows:
        row.pop("_ps_group_id", None)


def _apply_ps_key(key: str, mode: str, order: str) -> tuple[str, str, bool]:
    """Apply a single poll-mode keypress to ps sort state."""
    if key == "l":
        return ("lineage", order, True)
    if key == "d":
        return ("date", order, True)
    if key == "s":
        return ("status", order, True)
    if key == "t":
        return (mode, "asc" if order == "desc" else "desc", True)
    if key == "q":
        return (mode, order, True)
    return (mode, order, False)


def _ps_poll_is_interactive(stdin: Any, stdout: Any) -> bool:
    """Return whether ps poll may safely use live terminal key handling."""
    stdin_is_tty = getattr(stdin, "isatty", lambda: False)
    stdout_is_tty = getattr(stdout, "isatty", lambda: False)
    return bool(stdin_is_tty() and stdout_is_tty())


def _ps_status_group(status: str) -> int:
    if status == "in_progress":
        return 0
    if status == "failed":
        return 1
    if status == "completed":
        return 2
    return 3


def _ps_sort_timestamp_numeric(row: Mapping[str, object]) -> float:
    sort_timestamp = row.get("sort_timestamp") or ""
    if isinstance(sort_timestamp, str) and sort_timestamp:
        try:
            return datetime.fromisoformat(sort_timestamp).timestamp()
        except (ValueError, OSError):
            return 0.0
    return 0.0


def _ps_row_has_no_timestamp(row: Mapping[str, object]) -> bool:
    return (row.get("sort_timestamp") or "") == ""


def _task_id_sort_value(raw_task_id: object) -> int:
    if isinstance(raw_task_id, str):
        decoded = _task_id_numeric_key(raw_task_id)
        if decoded != 0:
            return decoded
        try:
            return int(raw_task_id)
        except (ValueError, TypeError):
            return sys.maxsize
    if isinstance(raw_task_id, int):
        return raw_task_id
    return sys.maxsize


def _ps_status_sort_key(row: dict, *, descending: bool) -> tuple[int, bool, float, int, str]:
    status_group = _ps_status_group(str(row.get("status", "")))
    ts_numeric = _ps_sort_timestamp_numeric(row)
    if status_group == 0:
        time_key = ts_numeric if descending else -ts_numeric
    else:
        time_key = -ts_numeric if descending else ts_numeric
    return (
        status_group,
        _ps_row_has_no_timestamp(row),
        time_key,
        _task_id_sort_value(row.get("task_id")),
        str(row.get("worker_id", "")),
    )


def _ps_date_sort_key(row: dict, *, descending: bool) -> tuple[bool, float, int, int, str]:
    ts_numeric = _ps_sort_timestamp_numeric(row)
    return (
        _ps_row_has_no_timestamp(row),
        -ts_numeric if descending else ts_numeric,
        _ps_status_group(str(row.get("status", ""))),
        _task_id_sort_value(row.get("task_id")),
        str(row.get("worker_id", "")),
    )


def _ps_lineage_sort_key(
    row: dict,
    *,
    descending: bool,
    group_recency: Mapping[str, float],
    group_is_worker_only: Mapping[str, bool],
    root_numeric_sort: Mapping[str, int],
    root_text_sort: Mapping[str, str],
) -> tuple[bool, float, int, str, bool, float, int, int, str]:
    group_id = str(row["_ps_group_id"])
    ts_numeric = _ps_sort_timestamp_numeric(row)
    return (
        group_is_worker_only.get(group_id, False),
        -(group_recency.get(group_id, 0.0)) if descending else group_recency.get(group_id, 0.0),
        root_numeric_sort.get(group_id, sys.maxsize),
        root_text_sort.get(group_id, ""),
        _ps_row_has_no_timestamp(row),
        -ts_numeric if descending else ts_numeric,
        _ps_status_group(str(row.get("status", ""))),
        _task_id_sort_value(row.get("task_id")),
        str(row.get("worker_id", "")),
    )


def _worker_failed_during_startup(worker: WorkerMetadata | None, task: DbTask | None) -> bool:
    """Return True when worker failed before main task logging initialized."""
    if worker is None:
        return False
    has_startup_hint = bool(worker.startup_log_file) or bool(task and task.slug)
    if worker.status != "failed" or not has_startup_hint:
        return False
    has_main_log = bool(task and task.log_file)
    return not has_main_log


def _pid_is_alive(pid: int | None) -> bool:
    """Return whether the given PID currently exists."""
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _prefer_worker(existing: WorkerMetadata, candidate: WorkerMetadata) -> bool:
    """Return True when candidate worker should replace existing worker."""
    priority = {"running": 3, "stale": 2, "failed": 1, "completed": 0}
    existing_rank = priority.get(existing.status, -1)
    candidate_rank = priority.get(candidate.status, -1)
    if candidate_rank != existing_rank:
        return candidate_rank > existing_rank

    existing_started = _parse_iso(existing.started_at)
    candidate_started = _parse_iso(candidate.started_at)
    if existing_started and candidate_started:
        return candidate_started > existing_started
    if candidate_started:
        return True
    return False


def _get_ps_steps(task: "DbTask | None", store: "SqliteTaskStore | None") -> str:
    """Return step count for display: use num_steps_computed when available, else count DB rows."""
    if task is None or task.id is None:
        return "-"
    if task.num_steps_computed is not None:
        return str(task.num_steps_computed)
    if store is not None:
        count = store.count_steps(task.id)
        return str(count) if count > 0 else "-"
    return "-"


def _to_ps_row(worker: WorkerMetadata | None, task: DbTask | None, store: "SqliteTaskStore | None" = None) -> dict:
    """Convert a reconciled worker/task pair into display data."""
    source = "both" if worker and task else "worker" if worker else "db"

    status = "unknown"
    if source == "db":
        # Use actual DB status instead of assuming in_progress — the task
        # may have already completed/failed by the time the worker is gone.
        status = task.status if task and task.status else "in_progress"
    elif source == "worker" and worker is not None:
        status = worker.status if worker.status in ("failed", "completed", "stale") else "in_progress"
    elif worker is not None and task is not None:
        # Both worker and task exist.
        if task.status in ("completed", "failed"):
            status = task.status
        elif worker.status in ("stale", "failed", "completed"):
            status = worker.status
        else:
            status = "in_progress"
    elif worker is not None:
        status = worker.status if worker.status in ("failed", "completed", "stale") else "in_progress"

    is_stale = worker is not None and worker.status == "stale"
    is_orphaned = (
        task is not None
        and task.status == "in_progress"
        and (worker is None or worker.status != "running")
        and not _pid_is_alive(task.running_pid if task is not None else None)
    )

    started = _started_at(worker, task)
    ended = _ended_at(worker, task)
    duration = _format_duration(started, ended)

    worker_id = worker.worker_id if worker else "-"
    pid = str(worker.pid) if worker else "-"
    if task:
        task_type_display = task.task_type
    else:
        task_type_display = "-"

    task_id = task.id if task and task.id is not None else worker.task_id if worker else None
    merge_unit_display = None
    if store is not None and task is not None and task.id is not None:
        merge_unit = store.resolve_merge_unit_for_task(task.id)
        if merge_unit is not None:
            merge_unit_display = (
                f"{merge_unit.id} / {merge_unit.owner_task_id}"
                if merge_unit.owner_task_id
                else merge_unit.id
            )
    task_display = ""
    if task and task.slug:
        task_display = task.slug
    elif task:
        task_display = truncate(task.prompt, 25)
    elif worker:
        if worker.task_slug:
            task_display = worker.task_slug
        else:
            task_display = f"task {worker.task_id}" if worker.task_id is not None else ""

    flags = []
    if is_stale:
        flags.append("stale")
    if is_orphaned:
        flags.append("orphaned")
    startup_failure = _worker_failed_during_startup(worker, task)
    if startup_failure:
        flags.append("startup-failure")

    return {
        "worker_id": worker_id,
        "pid": pid,
        "type": task_type_display,
        "merge_unit": merge_unit_display,
        "model": task.model if task else None,
        "source": source,
        "task_id": task_id,
        "status": status,
        "flags": ",".join(flags),
        "task": task_display,
        "started": _format_started(started),
        "started_at": started.isoformat() if started else None,
        "ended_at": ended.isoformat() if ended else None,
        "steps": _get_ps_steps(task, store),
        "duration": duration,
        "is_stale": is_stale,
        "is_orphaned": is_orphaned,
        "startup_failure": startup_failure,
        "startup_log_file": (f".gza/workers/{task.slug}.startup.log" if task and task.slug else (worker.startup_log_file if worker else None)),
        "sort_timestamp": started.isoformat() if started else "",
    }



def _started_at(worker: WorkerMetadata | None, task: DbTask | None) -> datetime | None:
    """Get the best available started timestamp."""
    if task:
        return task.started_at or task.created_at
    if worker:
        started = _parse_iso(worker.started_at)
        if started:
            return started
    return None


def _ended_at(worker: WorkerMetadata | None, task: DbTask | None = None) -> datetime | None:
    """Get completed timestamp when available."""
    if task and task.status in ("completed", "failed") and task.completed_at:
        return task.completed_at
    if worker:
        ended = _parse_iso(worker.completed_at)
        if ended:
            return ended
    return None


def _format_duration(started: datetime | None, ended: datetime | None = None) -> str:
    """Format duration from timestamps."""
    if not started:
        return "-"
    end_time = ended or datetime.now(UTC)
    duration_sec = max(0.0, (end_time - started).total_seconds())
    if duration_sec < 60:
        return f"{duration_sec:.0f}s"
    minutes = int(duration_sec // 60)
    seconds = int(duration_sec % 60)
    return f"{minutes}m {seconds}s"


def _format_started(started: datetime | None) -> str:
    """Format start timestamp for ps output."""
    if not started:
        return "-"
    if started.tzinfo is None:
        return started.strftime("%Y-%m-%d %H:%M:%S")
    started_utc = started.astimezone(UTC)
    return started_utc.strftime("%Y-%m-%d %H:%M:%S UTC")


def _kill_task(
    task: DbTask,
    config: Config,
    registry: WorkerRegistry,
    store: SqliteTaskStore,
    force: bool,
    workers: list[WorkerMetadata] | None = None,
) -> bool:
    """Kill a single in-progress task. Returns True on success.

    Resolves the PID from the worker record if available, falling back to
    task.running_pid for the tmux-bug case where no worker record exists.
    Sends SIGTERM, waits 3 seconds, escalates to SIGKILL if still alive.
    With force=True, skips straight to SIGKILL.
    Always marks the task failed with failure_reason=KILLED.

    Pass pre-fetched ``workers`` to avoid redundant registry scans when
    killing multiple tasks in sequence.
    """
    # Resolve PID: prefer live worker record, fall back to task.running_pid
    if workers is None:
        workers = registry.list_all(include_completed=False)
    worker = next(
        (w for w in workers if w.task_id == task.id and w.status == "running"),
        None,
    )

    if worker is not None:
        pid = worker.pid
    elif task.running_pid is not None:
        pid = task.running_pid
    else:
        print(f"Error: Task {task.id} has no associated process to kill")
        return False

    if force:
        try:
            os.kill(pid, signal.SIGKILL)
            print(f"✓ Sent SIGKILL to task {task.id} (PID {pid})")
        except OSError as exc:
            print(f"✗ Failed to kill task {task.id}: {exc}")
            return False
    else:
        registry.record_interrupt_request(
            pid,
            signal_name="SIGTERM",
            source="gza_kill",
            task_id=str(task.id) if task.id is not None else None,
            detail="manual gza kill request",
        )
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError as exc:
            print(f"✗ Failed to kill task {task.id}: {exc}")
            return False
        print(f"Sent SIGTERM to task {task.id} (PID {pid}), waiting 3s...")
        time.sleep(3)
        try:
            os.kill(pid, 0)
            # Still running — escalate
            try:
                os.kill(pid, signal.SIGKILL)
                print("  Process still alive — escalated to SIGKILL")
            except OSError:
                pass
        except OSError:
            pass  # Already dead after SIGTERM

    # Mark the task as failed with KILLED reason
    mark_task_failed_from_cause(
        task=task,
        config=config,
        store=store,
        log_file=task.log_file,
        branch=task.branch,
        has_commits=task.has_commits or False,
        explicit_reason="KILLED",
        error_type=None,
        exit_code=None,
    )

    # Clean up worker record if present
    if worker is not None:
        registry.mark_completed(worker.worker_id, exit_code=1, status="failed")

    print(f"✓ Task {task.id} killed")
    return True


def cmd_kill(args: argparse.Namespace) -> int:
    """Kill a running task."""
    config = Config.load(args.project_dir)
    registry = WorkerRegistry(config.workers_path)
    store = get_store(config)
    force = args.force

    if args.all:
        tasks = store.get_in_progress()
        if not tasks:
            print("No running tasks to kill")
            return 0
        # Pre-fetch worker list once to avoid O(N) registry scans.
        workers = registry.list_all(include_completed=False)
        results = [_kill_task(task, config, registry, store, force, workers) for task in tasks]
        return 0 if all(results) else 1

    if not args.task_id:
        print("Error: Must specify task_id or use --all")
        return 1

    task_id = resolve_id(config, args.task_id)
    maybe_task = store.get(task_id)
    if maybe_task is None:
        print(f"Error: Task {task_id} not found")
        return 1

    if maybe_task.status != "in_progress":
        print(f"Error: Task {task_id} is not running (status: {maybe_task.status})")
        return 1

    return 0 if _kill_task(maybe_task, config, registry, store, force) else 1


def cmd_delete(args: argparse.Namespace) -> int:
    """Delete a task."""
    config = Config.load(args.project_dir)
    store = get_store(config)

    task_id = resolve_id(config, args.task_id)
    task = store.get(task_id)
    if not task:
        print(f"Error: Task {task_id} not found")
        return 1

    if task.status == "in_progress":
        print("Error: Cannot delete in-progress task")
        return 1

    # Support both --force (deprecated) and --yes/-y
    skip_confirmation = args.force or args.yes

    if not skip_confirmation:
        prompt_display = truncate(task.prompt, MAX_PROMPT_DISPLAY)
        confirm = input(f"Delete task {task.id}: {prompt_display}? [y/N] ")
        if confirm.lower() != 'y':
            print("Cancelled")
            return 0

    if store.delete(task_id):
        print(f"✓ Deleted task {task_id}")
        return 0
    else:
        print("Error: Failed to delete task")
        return 1


def cmd_lineage(args: argparse.Namespace) -> int:
    """Show lineage for a given task."""
    config = Config.load(args.project_dir)
    store = get_store(config, open_mode="query_only")
    service = _TaskQueryService(store)

    task_id: str = resolve_id(config, args.task_id)
    task = store.get(task_id)
    if task is None:
        console.print(f"[red]Error: Task {task_id} not found[/red]")
        return 1

    lineage_query = _TaskQueryPresets.lineage(task_id)
    lineage_result = service.run(lineage_query)
    if not lineage_result.rows or not isinstance(lineage_result.rows[0], _LineageRow):
        console.print(f"[red]Error: unable to build lineage for {task_id}[/red]")
        return 1
    lineage_tree = lineage_result.rows[0].tree
    if lineage_tree is None:
        console.print(f"[red]Error: unable to build lineage for {task_id}[/red]")
        return 1
    lineage_tree = cast(TaskLineageNode, lineage_tree)
    owner_task = _resolve_lineage_owner_task(store, task)
    if (
        owner_task.task_type == "implement"
        and owner_task.branch
        and owner_task.id is not None
        and owner_task.id != lineage_tree.task.id
    ):
        lineage_tree = _build_lineage_tree_for_root(store, owner_task, max_depth=None)

    def _format_utc_timestamp(value: datetime) -> str:
        ts = value.astimezone(UTC) if value.tzinfo is not None else value
        return f"{ts.strftime('%Y-%m-%d %H:%M:%S')} UTC"

    def _strip_slug_date_prefix(slug: str) -> str:
        if len(slug) > 9 and slug[8] == "-" and slug[:8].isdigit():
            return slug[9:]
        return slug

    def _prompt_text(t: DbTask) -> str:
        type_str = t.task_type or "implement"
        if type_str not in {"plan", "plan_improve", "plan_review", "implement"}:
            return ""
        if t.slug:
            value = _strip_slug_date_prefix(t.slug)
        else:
            value = t.prompt.split("\n")[0].strip()
        return value[:60] + "…" if len(value) > 60 else value

    def _collect_rows(
        node: TaskLineageNode,
        *,
        ancestors_last: tuple[bool, ...] = (),
    ) -> list[tuple[TaskLineageNode, str]]:
        rows: list[tuple[TaskLineageNode, str]] = []
        rows.append((node, _lineage_tree_prefix(ancestors_last)))

        for idx, child in enumerate(node.children):
            rows.extend(
                _collect_rows(
                    child,
                    ancestors_last=(*ancestors_last, idx == len(node.children) - 1),
                )
            )
        return rows

    def _render_lineage_tree(tree: TaskLineageNode) -> None:
        rows = _collect_rows(tree)

        id_width = 1
        when_width = 1
        type_width = 1
        status_width = 1
        merge_width = 0
        prefix_width = 0

        for node, prefix in rows:
            t = node.task
            when = t.completed_at or t.started_at or t.created_at
            type_str = t.task_type or "implement"
            rel = _LINEAGE_REL_LABELS.get(node.relationship, "")
            type_display = f"{type_str} [{rel}]" if rel and rel != type_str else type_str
            status_text = format_task_status_text(t)
            merge_label = format_task_merge_label(t)
            merge_text = f"[{merge_label}]" if merge_label else ""

            id_width = max(id_width, len(t.id or "-"))
            when_width = max(when_width, len(_format_utc_timestamp(when)) if when else 1)
            type_width = max(type_width, len(type_display))
            status_width = max(status_width, len(status_text))
            merge_width = max(merge_width, len(merge_text))
            prefix_width = max(prefix_width, len(prefix))

        lc = _colors.LINEAGE_COLORS
        merged_color = _colors.STATUS_COLORS.completed
        unmerged_color = _colors.STATUS_COLORS.unmerged

        for node, prefix in rows:
            t = node.task
            is_target = t.id == task_id
            when = t.completed_at or t.started_at or t.created_at

            task_id_text = t.id or "-"
            timestamp_text = _format_utc_timestamp(when) if when else "-"
            type_str = t.task_type or "implement"
            rel = _LINEAGE_REL_LABELS.get(node.relationship, "")
            type_display = f"{type_str} [{rel}]" if rel and rel != type_str else type_str
            status_text = format_task_status_text(t)
            merge_label = format_task_merge_label(t)
            merge_text = f"[{merge_label}]" if merge_label else ""
            prompt_text = _prompt_text(t)

            status_color = get_task_status_color(t)
            if merge_text == "[merged]":
                merge_color = merged_color
            elif merge_text == "[unmerged]":
                merge_color = unmerged_color
            else:
                merge_color = lc.annotation

            prefix_part = f"[{lc.connector}]{rich_escape(prefix.ljust(prefix_width))}[/{lc.connector}]"
            arrow_char = "→" if is_target else " "
            arrow_part = f"[{lc.target_highlight}]{arrow_char}[/{lc.target_highlight}]"
            task_id_part = f"[{lc.task_id}]{rich_escape(task_id_text.ljust(id_width))}[/{lc.task_id}]"
            timestamp_part = f"[{lc.stats}]{rich_escape(timestamp_text.ljust(when_width))}[/{lc.stats}]"
            type_part = f"[{lc.type_label}]{rich_escape(type_display.ljust(type_width))}[/{lc.type_label}]"
            status_part = f"[{status_color}]{rich_escape(status_text.ljust(status_width))}[/{status_color}]"
            if merge_width > 0:
                merge_cell = merge_text.ljust(merge_width)
                merge_part = f"[{merge_color}]{rich_escape(merge_cell)}[/{merge_color}]"
            else:
                merge_part = ""
            prompt_part = f"[{lc.prompt}]{rich_escape(prompt_text)}[/{lc.prompt}]"

            pieces = [
                prefix_part,
                arrow_part,
                task_id_part,
                timestamp_part,
                type_part,
                status_part,
            ]
            if merge_width > 0:
                pieces.append(merge_part)
            pieces.append(prompt_part)
            console.print(" ".join(pieces).rstrip())

            detail_prefix = " " * prefix_width + "   "
            if t.task_type == "review":
                verdict = get_review_verdict(config, t)
                if verdict:
                    console.print(f"{detail_prefix}verdict: {rich_escape(verdict)}")
            elif t.task_type == "plan_review":
                verdict, manifest_detail = _plan_review_detail(task=t, config=config, store=store)
                if verdict:
                    console.print(f"{detail_prefix}verdict: {rich_escape(verdict)}")
                if manifest_detail:
                    console.print(f"{detail_prefix}{rich_escape(manifest_detail)}")

    def _immediate_parent_entries(current_task: DbTask) -> list[tuple[DbTask, str]]:
        entries: list[tuple[DbTask, str]] = []
        seen: set[str] = set()
        for edge_name, parent_id in (("based_on", current_task.based_on), ("depends_on", current_task.depends_on)):
            if parent_id is None or parent_id in seen:
                continue
            parent = store.get(parent_id)
            if parent is None or parent.id is None:
                continue
            seen.add(parent.id)
            relationship = _classify_lineage_child_relationship(parent, current_task)
            label = relationship if relationship in {"resume", "retry"} else edge_name
            entries.append((parent, label))
        return entries

    show_full = bool(getattr(args, "full", False))
    show_parents_only = bool(getattr(args, "parents_only", False))
    show_children_only = bool(getattr(args, "children_only", False))
    parent_entries = _immediate_parent_entries(task)

    if show_parents_only:
        for index, tree in enumerate(_build_ancestor_forest_for_task(store, task)):
            if index > 0:
                console.print()
            _render_lineage_tree(tree)
        return 0

    if show_full:
        console.print("Parents:")
        for index, tree in enumerate(_build_ancestor_forest_for_task(store, task)):
            if index > 0:
                console.print()
            _render_lineage_tree(tree)
        console.print()
        console.print("Children:")
        _render_lineage_tree(lineage_tree)
        return 0

    _render_lineage_tree(lineage_tree)

    if not show_children_only and parent_entries:
        parent_summary = ", ".join(
            f"{parent.id} [{label}]"
            for parent, label in parent_entries
            if parent.id is not None
        )
        console.print(rich_escape(f"Parents: {parent_summary}. Use --full or --parents-only to inspect ancestors."))

    return 0


def _show_built_prompt(task: DbTask, config: "Config", store: "SqliteTaskStore") -> int:
    """Build and print only the full prompt text for a task.

    Uses the shared build_prompt() path used by background execution. Autonomous
    review runs may append runtime verify output during actual execution.
    """
    from ..git import Git
    from ..runner import build_prompt, get_task_output_paths

    report_path, summary_path = get_task_output_paths(task, config.project_dir)

    git = Git(config.project_dir)
    prompt = build_prompt(task, config, store, report_path=report_path, summary_path=summary_path, git=git)
    print(prompt)
    return 0


def cmd_show(args: argparse.Namespace) -> int:
    """Show details of a specific task."""
    incompatible_flags = _show_incompatible_flags(args)
    if incompatible_flags:
        print(
            "Error: --metadata-only cannot be used with "
            + ", ".join(incompatible_flags)
            + ".",
        )
        return 1

    config = Config.load(args.project_dir)
    store = get_store(config, open_mode="query_only")

    task_id = resolve_id(config, args.task_id)
    task = store.get(task_id)
    if not task:
        console.print(f"[red]Error: Task {task_id} not found[/red]")
        return 1

    # --prompt: emit only the fully built prompt text and exit
    if getattr(args, "prompt", False):
        return _show_built_prompt(task, config, store)

    # --path: print only the report file path and exit
    if getattr(args, "path", False):
        if task.report_file:
            report_path = config.project_dir / task.report_file
            print(report_path)
            return 0
        console.print(f"[red]Error: Task {task_id} has no report file[/red]")
        return 1

    # --output: print only the raw output content and exit
    if getattr(args, "output", False):
        output = _get_task_output(task, config.project_dir)
        if output:
            print(output)
            return 0
        console.print(f"[red]Error: Task {task_id} has no output content[/red]")
        return 1

    with pager_context(getattr(args, 'page', False), config.project_dir):
        return _cmd_show_output(task, args, config, store)


def cmd_artifact(args: argparse.Namespace) -> int:
    """Print the latest matching task artifact content or path."""
    config = Config.load(args.project_dir)
    store = get_store(config, open_mode="query_only")

    task_id = resolve_id(config, args.task_id)
    task = store.get(task_id)
    if task is None:
        console.print(f"[red]Error: Task {task_id} not found[/red]")
        return 1

    resolved = _resolve_latest_task_artifact(store, config, task_id, kind=getattr(args, "kind", None))
    if resolved is None:
        kind_note = f" of kind {args.kind}" if getattr(args, "kind", None) else ""
        console.print(f"[red]Error: Task {task_id} has no artifacts{kind_note}[/red]")
        return 1

    retrieval_error = resolved.retrieval_error(task_id=task_id)
    if retrieval_error is not None:
        console.print(f"[red]Error: {retrieval_error}[/red]")
        return 1

    if getattr(args, "path", False):
        print(resolved.path)
        return 0

    if resolved.path is None:
        return 1

    try:
        _write_stdout_bytes(resolved.path.read_bytes())
    except OSError as exc:
        console.print(f"[red]Error: Failed to read artifact {resolved.artifact.id}: {exc}[/red]")
        return 1
    return 0


def _write_stdout_bytes(payload: bytes) -> None:
    """Write artifact content without text-mode newline normalization."""
    stdout_buffer = getattr(sys.stdout, "buffer", None)
    if stdout_buffer is not None:
        stdout_buffer.write(payload)
        stdout_buffer.flush()
        return

    sys.stdout.write(payload.decode("utf-8", errors="replace"))


def _show_incompatible_flags(args: argparse.Namespace) -> list[str]:
    """Return incompatible flags when --metadata-only is combined with other show modes."""
    if not getattr(args, "metadata_only", False):
        return []

    incompatible_flags: list[str] = []
    if getattr(args, "prompt", False):
        incompatible_flags.append("--prompt")
    if getattr(args, "output", False):
        incompatible_flags.append("--output")
    if getattr(args, "path", False):
        incompatible_flags.append("--path")
    if getattr(args, "full", False):
        incompatible_flags.append("--full")
    return incompatible_flags


def _find_active_worktree_path_for_branch(config: Config, branch: str) -> tuple[Path | None, str | None]:
    """Return active worktree path and optional lookup error for a branch."""
    try:
        git = Git(config.project_dir)
        return active_worktree_path_for_branch(git, branch), None
    except (GitError, OSError) as exc:
        return None, " ".join(str(exc).split())


def _render_verify_markdown(read_model: VerifyReadModel | None, *, config: Config) -> str | None:
    if read_model is None:
        return None

    legacy_markdown = getattr(read_model, "legacy_markdown", None)
    if isinstance(legacy_markdown, str) and legacy_markdown.strip():
        return legacy_markdown

    result = getattr(read_model, "result", None)
    if result is None:
        return None

    lines = [
        "## verify_command result",
        "",
        f"- Command: `{result.command}`",
        f"- Status: {result.status}",
        f"- Exit status: {result.exit_status}",
    ]
    if result.reviewed_branch:
        lines.append(f"- Branch: `{result.reviewed_branch}`")
    if result.reviewed_head_sha:
        lines.append(f"- Head SHA: `{result.reviewed_head_sha}`")
    if result.reviewed_base_sha:
        lines.append(f"- Base SHA: `{result.reviewed_base_sha}`")
    if result.working_directory:
        lines.append(f"- Working directory: `{result.working_directory}`")
    if result.failure:
        lines.append(f"- Failure: {result.failure}")

    excerpt = read_verify_output_excerpt(config.project_dir, read_model)
    if excerpt:
        heading = "Failing output (trimmed):" if result.status != "passed" else "Captured output (trimmed):"
        lines.extend(["", heading, "```text", excerpt, "```"])
    return "\n".join(lines)


def _resolve_show_verify_read_model(
    task: DbTask,
    *,
    config: Config,
    store: SqliteTaskStore,
) -> VerifyReadModel | None:
    verify_owner = resolve_verify_owner_task(store, task) if task.task_type == "review" else _resolve_lineage_owner_task(store, task)
    try:
        git = Git(config.project_dir)
    except (GitError, OSError):
        git = None
    current_epoch = owner_task_verify_epoch(verify_owner, config, git)
    return resolve_verify_read_model(
        store,
        task,
        owner_task=verify_owner,
        current_epoch=current_epoch,
    )


def _render_show_verify_section(
    *,
    task: DbTask,
    config: Config,
    store: SqliteTaskStore,
    colors: dict[str, str],
    metadata_only: bool,
    full_mode: bool,
) -> None:
    verify_read_model = _resolve_show_verify_read_model(task, config=config, store=store) if task.id is not None else None
    latest_verify_artifact = (
        _resolve_latest_task_artifact(store, config, task.id, kind="verify_command_output")
        if task.task_type == "review" and task.id is not None
        else None
    )
    verify_markdown = _render_verify_markdown(verify_read_model, config=config)
    verify_artifact_path = (
        verify_output_artifact_path(verify_read_model) if verify_read_model is not None else None
    )
    if verify_read_model is None and latest_verify_artifact is None and verify_markdown is None:
        return

    c = colors
    console.print(
        f"[{c['label']}]Verify Status:[/{c['label']}] "
        f"[{c['value']}]{getattr(getattr(verify_read_model, 'result', None), 'status', None) or 'unknown'}[/{c['value']}]"
    )
    verify_result = getattr(verify_read_model, "result", None)
    if verify_result is not None and verify_result.exit_status:
        console.print(
            f"[{c['label']}]Verify Exit:[/{c['label']}] "
            f"[{c['value']}]{verify_result.exit_status}[/{c['value']}]"
        )
    if verify_read_model is not None:
        verify_current = "yes" if verify_read_model.is_current else "no"
        console.print(
            f"[{c['label']}]Verify Current:[/{c['label']}] "
            f"[{c['value']}]{verify_current}[/{c['value']}]"
        )
    if verify_result is not None and verify_result.captured_at:
        console.print(
            f"[{c['label']}]Verify At:[/{c['label']}] "
            f"[{c['value']}]{_format_show_utc_timestamp(verify_result.captured_at)}[/{c['value']}]"
        )
    if verify_result is not None and verify_result.reviewed_branch:
        console.print(
            f"[{c['label']}]Verify Branch:[/{c['label']}] "
            f"[{c['value']}]{verify_result.reviewed_branch}[/{c['value']}]"
        )
    if verify_result is not None and verify_result.reviewed_head_sha:
        console.print(
            f"[{c['label']}]Verify Head:[/{c['label']}] "
            f"[{c['value']}]{verify_result.reviewed_head_sha}[/{c['value']}]"
        )
    if verify_result is not None and verify_result.reviewed_base_sha:
        console.print(
            f"[{c['label']}]Verify Base:[/{c['label']}] "
            f"[{c['value']}]{verify_result.reviewed_base_sha}[/{c['value']}]"
        )
    if verify_result is not None and verify_result.working_directory:
        console.print(
            f"[{c['label']}]Verify Cwd:[/{c['label']}] "
            f"[{c['value']}]{verify_result.working_directory}[/{c['value']}]"
        )
    if verify_artifact_path is None and latest_verify_artifact is not None:
        verify_artifact_path = latest_verify_artifact.artifact.path
    if verify_artifact_path:
        verify_artifact_text = verify_artifact_path
        if latest_verify_artifact is not None and latest_verify_artifact.invalid_path_error is not None:
            verify_artifact_text = f"{verify_artifact_text} (invalid path)"
        else:
            try:
                resolved_verify_artifact = resolve_artifact_path(config.project_dir, verify_artifact_path)
            except InvalidArtifactPathError:
                verify_artifact_text = f"{verify_artifact_text} (invalid path)"
            else:
                if not resolved_verify_artifact.exists():
                    verify_artifact_text = f"{verify_artifact_text} (missing)"
        console.print(
            f"[{c['label']}]Verify Artifact:[/{c['label']}] "
            f"[{c['value']}]{verify_artifact_text}[/{c['value']}]"
        )
    if verify_result is not None and verify_result.failure:
        console.print(
            f"[{c['label']}]Verify Failure:[/{c['label']}] "
            f"[{c['value']}]{verify_result.failure}[/{c['value']}]"
        )

    if not metadata_only and verify_markdown:
        console.print()
        console.print(f"[{c['label']}]Verify Result:[/{c['label']}]")
        console.print(f"[{c['section']}]{'-' * 50}[/{c['section']}]")
        verify_lines = verify_markdown.splitlines()
        if not full_mode and len(verify_lines) > 30:
            truncated = "\n".join(verify_lines[:20])
            remainder = len(verify_lines) - 20
            console.print(truncated)
            console.print(
                f"[{c['section']}](... truncated, {remainder} more lines — use `gza show {task.id} --full` to see all)[/{c['section']}]"
            )
        else:
            console.print(verify_markdown)
        console.print(f"[{c['section']}]{'-' * 50}[/{c['section']}]")
        console.print()


def _format_show_utc_timestamp(value: datetime) -> str:
    ts = value.astimezone(UTC) if value.tzinfo is not None else value
    return f"{ts.strftime('%Y-%m-%d %H:%M:%S')} UTC"


def _cmd_show_output(
    task: DbTask,
    args: argparse.Namespace,
    config: Config,
    store: SqliteTaskStore,
) -> int:
    """Render the full show output. Called within pager_context when needed."""
    from .log import _latest_worker_for_task

    # Colors for show output — defined in gza.colors.
    SHOW_COLORS = SHOW_COLORS_DICT
    c = SHOW_COLORS

    status_color = _show_status_color(task, c)

    console.print(f"[{c['heading']}]Task {task.id}[/{c['heading']}]")
    console.print(f"[{c['section']}]{'=' * 50}[/{c['section']}]")
    console.print(f"[{c['label']}]Status:[/{c['label']}] [{status_color}]{task.status}[/{status_color}]")
    metadata_only = getattr(args, "metadata_only", False)
    full_mode = getattr(args, "full", False)
    lifecycle_summary = _summarize_lifecycle(task, config=config, store=store)
    if lifecycle_summary is not None:
        lifecycle_color = c["value"]
        if lifecycle_summary.severity == "failed":
            lifecycle_color = c["status_failed"]
        elif lifecycle_summary.severity == "completed":
            lifecycle_color = c["status_completed"]
        elif lifecycle_summary.severity == "running":
            lifecycle_color = c["status_running"]
        console.print(
            f"[{c['label']}]Lifecycle:[/{c['label']}] [{lifecycle_color}]{lifecycle_summary.text}[/{lifecycle_color}]"
        )
    if task.failure_reason:
        console.print(f"[{c['label']}]Failure Reason:[/{c['label']}] [{c['value']}]{task.failure_reason}[/{c['value']}]")
    if task.completion_reason:
        console.print(f"[{c['label']}]Completion Reason:[/{c['label']}] [{c['value']}]{task.completion_reason}[/{c['value']}]")
    if task.drop_reason:
        console.print(f"[{c['label']}]Drop Reason:[/{c['label']}] [{c['value']}]{task.drop_reason}[/{c['value']}]")
    if task.task_type in {"rebase", "improve"}:
        console.print(
            f"[{c['label']}]Changed Diff:[/{c['label']}] "
            f"[{c['value']}]{_format_changed_diff_label(task.changed_diff)}[/{c['value']}]"
        )
    merge_status = _task_show_merge_status(store, task)
    if merge_status:
        console.print(f"[{c['label']}]Merge Status:[/{c['label']}] [{c['value']}]{merge_status}[/{c['value']}]")
    console.print(f"[{c['label']}]Type:[/{c['label']}] [{c['value']}]{task.task_type}[/{c['value']}]")
    console.print(f"[{c['label']}]Provider:[/{c['label']}] [{c['value']}]{task.provider or '-'}[/{c['value']}]")
    console.print(f"[{c['label']}]Model:[/{c['label']}] [{c['value']}]{task.model or '-'}[/{c['value']}]")
    if task.task_type == "plan":
        auto_implement_detail = "yes"
        if task.auto_implement is False:
            auto_implement_detail = (
                f"no (hold for review; run uv run gza implement {task.id})"
                if task.id is not None
                else "no (hold for review)"
            )
        console.print(
            f"[{c['label']}]Auto Implement:[/{c['label']}] "
            f"[{c['value']}]{auto_implement_detail}[/{c['value']}]"
        )
    if task.execution_mode:
        console.print(f"[{c['label']}]Execution Mode:[/{c['label']}] [{c['value']}]{task.execution_mode}[/{c['value']}]")
    console.print(
        f"[{c['label']}]Trigger Source:[/{c['label']}] "
        f"[{c['value']}]{task.trigger_source or 'unknown'}[/{c['value']}]"
    )
    if task.slug:
        console.print(f"[{c['label']}]Slug:[/{c['label']}] [{c['value']}]{task.slug}[/{c['value']}]")
    if task.based_on:
        console.print(f"[{c['label']}]Based on:[/{c['label']}] [{c['value']}]task {task.based_on}[/{c['value']}]")
    if task.depends_on:
        console.print(f"[{c['label']}]Depends on:[/{c['label']}] [{c['value']}]task {task.depends_on}[/{c['value']}]")
    if task.id is not None:
        depended_on_by = [
            t for t in store.get_all()
            if t.depends_on == task.id or t.based_on == task.id
        ]
        if depended_on_by:
            dep_parts = [f"{t.id}[{t.task_type}]" for t in depended_on_by if t.id is not None]
            console.print(f"[{c['label']}]Depended on by:[/{c['label']}] [{c['value']}]{', '.join(dep_parts)}[/{c['value']}]")
    if task.tags:
        console.print(f"[{c['label']}]Tags:[/{c['label']}] [{c['value']}]{', '.join(task.tags)}[/{c['value']}]")
    if task.spec:
        console.print(f"[{c['label']}]Spec:[/{c['label']}] [{c['value']}]{task.spec}[/{c['value']}]")
    console.print(
        f"[{c['label']}]Create PR:[/{c['label']}] "
        f"[{c['value']}]{'yes' if task.create_pr else 'no'}[/{c['value']}]"
    )
    if task.pr_number is not None:
        console.print(f"[{c['label']}]PR Number:[/{c['label']}] [{c['value']}]{task.pr_number}[/{c['value']}]")
    if task.pr_state is not None:
        console.print(f"[{c['label']}]PR State:[/{c['label']}] [{c['value']}]{task.pr_state}[/{c['value']}]")
    review_rebase_detail = _implementation_review_rebase_detail(task, config=config, store=store)
    if review_rebase_detail is not None:
        console.print(f"[{c['label']}]Review:[/{c['label']}] [{c['value']}]{review_rebase_detail}[/{c['value']}]")
    if task.skip_learnings:
        console.print(f"[{c['label']}]Skip Learnings:[/{c['label']}] [green]yes[/green]")
    if task.branch:
        console.print(f"[{c['label']}]Branch:[/{c['label']}] [{c['branch']}]{task.branch}[/{c['branch']}]")
        active_worktree_path, worktree_lookup_error = _find_active_worktree_path_for_branch(config, task.branch)
        if active_worktree_path:
            console.print(f"[{c['label']}]Worktree:[/{c['label']}] [{c['value']}]{active_worktree_path}[/{c['value']}]")
        elif worktree_lookup_error:
            console.print(f"[yellow]Warning: Worktree lookup failed: {rich_escape(worktree_lookup_error)}[/yellow]")
    if task.log_file:
        console.print(f"[{c['label']}]Log:[/{c['label']}] [{c['value']}]{task.log_file}[/{c['value']}]")
    if task.report_file:
        console.print(f"[{c['label']}]Report:[/{c['label']}] [{c['value']}]{task.report_file}[/{c['value']}]")
        # Detect if disk file is newer than task completion (drift warning)
        if task.completed_at and task.output_content:
            report_path = config.project_dir / task.report_file
            if report_path.exists():
                file_mtime = datetime.fromtimestamp(report_path.stat().st_mtime, tz=UTC)
                if file_mtime > task.completed_at:
                    console.print("[yellow]Warning: Report on disk has been modified since task completion[/yellow]")
    task_artifacts = store.list_artifacts(task.id) if task.id is not None else []
    if task_artifacts:
        console.print(f"[{c['label']}]Artifacts:[/{c['label']}]")
        for artifact in task_artifacts:
            console.print(f"  - [{c['value']}]{_format_task_artifact_summary(artifact, config=config)}[/{c['value']}]")
    if task.task_type == "review":
        verdict = get_review_verdict(config, task)
        if verdict:
            console.print(f"[{c['label']}]Verdict:[/{c['label']}] [{c['value']}]{verdict}[/{c['value']}]")
        score = task.review_score
        if score is None and task.status == "completed":
            score = get_review_score(config, task)
        if score is not None:
            console.print(f"[{c['label']}]Score:[/{c['label']}] [{c['value']}]{score}/100[/{c['value']}]")
    _render_show_verify_section(
        task=task,
        config=config,
        store=store,
        colors=c,
        metadata_only=metadata_only,
        full_mode=full_mode,
    )
    if task.task_type == "plan_review":
        verdict, manifest_detail = _plan_review_detail(task=task, config=config, store=store)
        if verdict:
            console.print(f"[{c['label']}]Verdict:[/{c['label']}] [{c['value']}]{verdict}[/{c['value']}]")
        if manifest_detail:
            console.print(
                f"[{c['label']}]Slice Manifest:[/{c['label']}] "
                f"[{c['value']}]{manifest_detail}[/{c['value']}]"
            )
    if task.session_id:
        console.print(f"[{c['label']}]Session ID:[/{c['label']}] [{c['value']}]{task.session_id}[/{c['value']}]")

    root_task = _resolve_lineage_root_task(store, task)
    lineage_tree = _build_lineage_tree_for_root(store, root_task)
    lineage_str = _format_lineage(
        lineage_tree,
        c["task_id"],
        show_status=True,
        status_color_resolver=lambda lineage_task: _show_status_color(lineage_task, c),
    )
    if _lineage_has_descendants(lineage_tree) and lineage_str:
        console.print(f"[{c['label']}]Lineage:[/{c['label']}]")
        console.print(lineage_str)

    if not metadata_only:
        console.print()
        console.print(f"[{c['label']}]Prompt:[/{c['label']}]")
        console.print(f"[{c['section']}]{'-' * 50}[/{c['section']}]")
        console.print(f"[{c['prompt']}]{task.prompt}[/{c['prompt']}]")
        console.print(f"[{c['section']}]{'-' * 50}[/{c['section']}]")
        console.print()
    if task.id is not None:
        comments = store.get_comments(task.id)
        if comments:
            console.print(f"[{c['label']}]Comments:[/{c['label']}]")
            for comment in comments:
                state = "resolved" if comment.resolved_at is not None else "open"
                meta_parts = [
                    f"id={comment.id}",
                    f"source={comment.source}",
                    f"kind={comment.kind}",
                    f"state={state}",
                    f"created={_format_show_utc_timestamp(comment.created_at)}",
                ]
                if comment.author:
                    meta_parts.append(f"author={comment.author}")
                if comment.resolved_at is not None:
                    meta_parts.append(f"resolved={_format_show_utc_timestamp(comment.resolved_at)}")
                meta = ", ".join(meta_parts)
                console.print(f"  [{c['stats']}]({meta})[/{c['stats']}] {comment.content}")
            console.print()
    if task.created_at:
        console.print(f"[{c['label']}]Created:[/{c['label']}] [{c['value']}]{task.created_at.strftime('%Y-%m-%d %H:%M:%S')} UTC[/{c['value']}]")
    if task.started_at:
        console.print(f"[{c['label']}]Started:[/{c['label']}] [{c['value']}]{task.started_at.strftime('%Y-%m-%d %H:%M:%S')} UTC[/{c['value']}]")
    if task.completed_at:
        console.print(f"[{c['label']}]Completed:[/{c['label']}] [{c['value']}]{task.completed_at.strftime('%Y-%m-%d %H:%M:%S')} UTC[/{c['value']}]")
    stats_str = format_stats(task)
    if stats_str:
        console.print(f"[{c['label']}]Stats:[/{c['label']}] [{c['stats']}]{stats_str}[/{c['stats']}]")

    if task.id is not None:
        latest_worker = _latest_worker_for_task(WorkerRegistry(config.workers_path), task.id)
        if latest_worker:
            run_mode = "background" if latest_worker.is_background else "foreground"
            pid_part = f", PID {latest_worker.pid}" if latest_worker.pid else ""
            worker_label = f"{run_mode} ({latest_worker.worker_id}){pid_part}"
            console.print(f"[{c['label']}]Run Context:[/{c['label']}] [{c['value']}]{worker_label}[/{c['value']}]")
            if _worker_failed_during_startup(latest_worker, task):
                console.print(
                    f"[{c['label']}]Worker Failure:[/{c['label']}] "
                    f"[{c['status_failed']}]failed during startup (before main log setup)[/{c['status_failed']}]"
                )
                if latest_worker.startup_log_file:
                    console.print(
                        f"[{c['label']}]Startup Log:[/{c['label']}] "
                        f"[{c['value']}]{latest_worker.startup_log_file}[/{c['value']}]"
                    )

    if task.status == "failed":
        log_path = _resolve_task_log_path(config, task)
        diagnostics = _build_failure_diagnostics(task, log_path, config.verify_command, store=store)
        guidance_reason = diagnostics.marker_reason or diagnostics.reason
        _render_failure_diagnostics(
            diagnostics,
            label_color=c["label"],
            value_color=c["value"],
            status_failed_color=c["status_failed"],
            include_explanation=bool(log_path and log_path.exists()),
        )

        if guidance_reason in {"MAX_STEPS", "MAX_TURNS"}:
            _, _, effective_max_steps = get_effective_config_for_task(task, config)
            steps_used = task.num_steps_reported if task.num_steps_reported is not None else task.num_steps_computed
            if steps_used is not None:
                console.print(
                    f"[{c['label']}]Step Limit:[/{c['label']}] "
                    f"[{c['value']}]{steps_used} / {effective_max_steps}[/{c['value']}]"
                )
            turns_used = task.num_turns_reported if task.num_turns_reported is not None else task.num_turns_computed
            if turns_used is not None:
                console.print(
                    f"[{c['label']}]Legacy Turns:[/{c['label']}] "
                    f"[{c['value']}]{turns_used}[/{c['value']}]"
                )

        next_step_commands = _failure_next_steps(task, guidance_reason, config=config, store=store)
        if next_step_commands:
            console.print(f"[{c['label']}]Next Steps:[/{c['label']}]")
            for command in next_step_commands:
                console.print(f"[{c['value']}]  - {command}[/{c['value']}]")

    # Display output content using precedence logic (disk version when newer)
    output = _get_task_output(task, config.project_dir)
    if output and not metadata_only:
        console.print()
        console.print(f"[{c['label']}]Output:[/{c['label']}]")
        console.print(f"[{c['section']}]{'-' * 50}[/{c['section']}]")
        lines = output.splitlines()
        if not full_mode and len(lines) > 30:
            truncated = "\n".join(lines[:20])
            remainder = len(lines) - 20
            console.print(truncated)
            console.print(f"[{c['section']}](... truncated, {remainder} more lines — use `gza show {task.id} --full` to see all)[/{c['section']}]")
        else:
            console.print(output)
        console.print(f"[{c['section']}]{'-' * 50}[/{c['section']}]")

    return 0


# Providers where the human can interact (type messages, approve/deny tools)
_INTERACTIVE_PROVIDERS = {"claude"}
# Providers that run headless — attach is observe-only
_OBSERVE_ONLY_PROVIDERS = {"codex", "gemini"}


def _task_log_file_path(config: Config, task: DbTask) -> Path | None:
    if not task.log_file:
        return None
    return config.project_dir / task.log_file


def _build_resume_worker_args(*, no_docker: bool, max_turns: int | None, force: bool) -> argparse.Namespace:
    return argparse.Namespace(
        no_docker=no_docker,
        max_turns=max_turns,
        force=force,
        resume=True,
    )


def _infer_resume_overrides_from_worker(worker: WorkerMetadata) -> tuple[bool, int | None, bool]:
    """Best-effort parse of current worker CLI args for resume handoff parity.

    Uses ``ps -p <pid> -o args=`` which works on both macOS and Linux.
    """
    try:
        result = subprocess.run(
            ["ps", "-p", str(worker.pid), "-o", "args="],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return (False, None, False)
    if result.returncode != 0 or not result.stdout.strip():
        return (False, None, False)

    args = result.stdout.strip().split()
    no_docker = "--no-docker" in args
    force = "--force" in args
    max_turns: int | None = None
    for index, arg in enumerate(args):
        if arg == "--max-turns" and index + 1 < len(args):
            try:
                max_turns = int(args[index + 1])
            except ValueError:
                max_turns = None
            break
        if arg.startswith("--max-turns="):
            try:
                max_turns = int(arg.split("=", 1)[1])
            except ValueError:
                max_turns = None
            break
    return (no_docker, max_turns, force)


def _stop_worker_for_attach(task: DbTask, worker: WorkerMetadata, registry: WorkerRegistry) -> bool:
    """Stop a running worker process without marking the task failed."""
    pid = worker.pid

    def _pid_exists() -> bool:
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    registry.record_interrupt_request(
        pid,
        signal_name="SIGTERM",
        source="attach_takeover",
        task_id=str(task.id) if task.id is not None else None,
        detail="stopping worker for interactive attach takeover",
    )
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError as exc:
        print(f"✗ Failed to stop worker {worker.worker_id}: {exc}")
        return False

    deadline = time.time() + 3
    while time.time() < deadline:
        try:
            os.kill(pid, 0)
            time.sleep(0.1)
        except OSError:
            break
    else:
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError as exc:
            print(f"✗ Failed to force-stop worker {worker.worker_id}: {exc}")
            return False

    # Confirm the worker process is truly gone before mutating task/registry state.
    force_deadline = time.time() + 1
    while time.time() < force_deadline:
        if not _pid_exists():
            break
        time.sleep(0.05)
    else:
        if _pid_exists():
            print(f"✗ Worker {worker.worker_id} is still running; aborting attach handoff.")
            return False

    registry.mark_completed(
        worker.worker_id,
        exit_code=0,
        status="completed",
        completion_reason="stopped_for_attach",
    )
    task.running_pid = None
    if task.status == "in_progress":
        task.status = "pending"
        task.completed_at = None
        task.failure_reason = None
        task.completion_reason = None
    return True


def _preflight_attach_session(
    session_name: str,
    *,
    cols: int,
    rows: int,
) -> str | None:
    """Validate tmux availability and ability to create the attach session."""
    if shutil.which("tmux") is None:
        return "tmux is not installed; install tmux to use interactive attach."

    subprocess.run(["tmux", "kill-session", "-t", session_name], stderr=subprocess.DEVNULL)
    probe_result = subprocess.run(
        ["tmux", "new-session", "-d", "-s", session_name, "-x", str(cols), "-y", str(rows), "--", "sh", "-lc", "exit 0"],
        capture_output=True,
        text=True,
    )
    if probe_result.returncode != 0:
        stderr = probe_result.stderr.strip()
        return stderr or "unknown tmux error"

    subprocess.run(["tmux", "kill-session", "-t", session_name], stderr=subprocess.DEVNULL)
    return None


def cmd_attach(args: argparse.Namespace) -> int:
    """Attach to a running task."""
    config = Config.load(args.project_dir)
    registry = WorkerRegistry(config.workers_path)
    store = get_store(config)

    target = args.worker_id

    # Try as worker ID first, then as task ID string.
    worker = registry.get(target)
    if worker is None:
        # Try resolving as a task ID — WorkerMetadata.from_dict already
        # normalises task_id to str | None, so no str() cast needed here.
        resolved_target = resolve_id(config, target) if not target.startswith("w-") else None
        for w in registry.list_all(include_completed=False):
            if w.task_id == target or (resolved_target and w.task_id == resolved_target):
                worker = w
                break

    if worker is None or worker.status != "running":
        print(f"No running worker found for: {target}")
        return 1

    if worker.task_id is None:
        print(f"Worker {worker.worker_id} has no associated task ID")
        return 1

    # Determine provider to decide attach mode.
    task = store.get(worker.task_id)
    if task is None:
        print(f"Task not found: {worker.task_id}")
        return 1

    provider_name = "claude"
    provider_name = (task.provider or config.provider or "claude").lower()

    # When already inside tmux, use switch-client instead of attach-session
    # to avoid the "sessions should be nested with care" error.
    inside_tmux = bool(os.environ.get("TMUX"))

    if provider_name in _OBSERVE_ONLY_PROVIDERS:
        session_name = worker.tmux_session or f"gza-{worker.task_id}"
        result = subprocess.run(
            ["tmux", "has-session", "-t", session_name],
            capture_output=True,
        )
        if result.returncode != 0:
            print(f"No tmux session found: {session_name}")
            return 1
        if inside_tmux:
            dod_result = subprocess.run(
                ["tmux", "set-option", "-t", session_name, "detach-on-destroy", "previous"],
                capture_output=True,
            )
            if dod_result.returncode != 0:
                print(
                    "Warning: could not set detach-on-destroy on task session. "
                    "When the task ends you may be detached from tmux.",
                    file=sys.stderr,
                )
        print(f"Attaching to task {worker.task_id} (provider: {provider_name})...")
        print(
            f"Note: {provider_name.title()} runs in headless mode. You can observe"
        )
        print("output but cannot interact. Use Ctrl-B D to detach.")
        print(
            f"To intervene, stop this task (gza kill {worker.task_id}) and re-run with Claude."
        )
        print()
        if inside_tmux:
            os.execvp("tmux", ["tmux", "switch-client", "-r", "-t", session_name])
        else:
            os.execvp("tmux", ["tmux", "attach-session", "-r", "-t", session_name])

    if provider_name not in _INTERACTIVE_PROVIDERS:
        print(f"Error: Interactive attach is not supported for provider '{provider_name}'")
        return 1

    if not task.session_id:
        print(f"Error: Task {task.id} has no session ID (cannot attach interactively)")
        return 1

    session_name = f"gza-attach-{task.id}"
    cols, rows = config.tmux.terminal_size
    resume_no_docker, resume_max_turns, resume_force = _infer_resume_overrides_from_worker(worker)
    wrapper_cmd = [
        sys.executable,
        "-m",
        "gza.attach_wrapper",
        "--task-id",
        str(task.id),
        "--session-id",
        task.session_id,
        "--project",
        str(config.project_dir.absolute()),
    ]
    if resume_no_docker:
        wrapper_cmd.append("--no-docker")
    if resume_max_turns is not None:
        wrapper_cmd.extend(["--max-turns", str(resume_max_turns)])
    if resume_force:
        wrapper_cmd.append("--force")
    preflight_err = _preflight_attach_session(session_name, cols=cols, rows=rows)
    if preflight_err:
        print(f"Error: failed to create interactive tmux session: {preflight_err}")
        return 1

    if not _stop_worker_for_attach(task, worker, registry):
        return 1
    store.update(task)

    log_path = _task_log_file_path(config, task)
    if log_path is not None:
        write_log_entry(
            log_path,
            {
                "type": "gza",
                "subtype": "worker_lifecycle",
                "event": "stop",
                "worker_id": worker.worker_id,
                "message": f"Worker {worker.worker_id} stopped (interactive attach)",
                "reason": "stopped_for_attach",
            },
        )

    subprocess.run(["tmux", "kill-session", "-t", session_name], stderr=subprocess.DEVNULL)
    create_result = subprocess.run(
        ["tmux", "new-session", "-d", "-s", session_name, "-x", str(cols), "-y", str(rows), "--", *wrapper_cmd],
        capture_output=True,
        text=True,
    )
    if create_result.returncode != 0:
        create_stderr = create_result.stderr.strip()
        print(f"Error: failed to create interactive tmux session: {create_stderr}")
        recovery_args = _build_resume_worker_args(
            no_docker=resume_no_docker,
            max_turns=resume_max_turns,
            force=resume_force,
        )
        recovery_rc = _spawn_background_worker(
            recovery_args,
            config,
            task_id=task.id,
            quiet=True,
        )
        if recovery_rc == 0:
            print(f"Recovered: background worker restarted for task {task.id}.")
            return 1

        print("Recovery failed: unable to restart the background worker.")
        mark_task_failed_from_cause(
            task=task,
            config=config,
            store=store,
            log_file=task.log_file,
            branch=task.branch,
            has_commits=bool(task.has_commits),
            explicit_reason="WORKER_DIED",
            error_type=None,
            exit_code=None,
        )
        if log_path is not None:
            write_log_entry(
                log_path,
                {
                    "type": "gza",
                    "subtype": "worker_lifecycle",
                    "event": "handoff_failed",
                    "message": (
                        "Interactive attach handoff failed: tmux session creation "
                        "and background recovery both failed; task marked failed."
                    ),
                    "reason": "WORKER_DIED",
                    "tmux_error": create_stderr,
                    "recovery_exit_code": recovery_rc,
                },
            )
        return 1

    if log_path is not None:
        subprocess.run(
            [
                "tmux",
                "pipe-pane",
                "-t",
                session_name,
                f"cat >> {shlex.quote(str(log_path))}",
            ],
            capture_output=True,
        )

    subprocess.run(["tmux", "set-option", "-t", session_name, "remain-on-exit", "off"], capture_output=True)
    subprocess.run(
        ["tmux", "set-hook", "-t", session_name, "client-detached", f"kill-session -t {session_name}"],
        capture_output=True,
    )
    if inside_tmux:
        subprocess.run(
            ["tmux", "set-option", "-t", session_name, "detach-on-destroy", "previous"],
            capture_output=True,
        )

    print(f"Attaching to task {task.id} (provider: {provider_name})...")
    print("Worker stopped. Interactive Claude session is live.")
    print("Detach with Ctrl-B D or exit Claude normally to auto-resume in background.")
    print()
    if inside_tmux:
        os.execvp("tmux", ["tmux", "switch-client", "-t", session_name])
    else:
        os.execvp("tmux", ["tmux", "attach-session", "-t", session_name])

    return 0  # unreachable after execvp but satisfies the return type
