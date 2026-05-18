"""CLI commands for querying and displaying task state.

Covers: next, history, unmerged, ps, kill, delete, show, attach.
"""

import argparse
import datetime as _dt
import os
import shlex
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, Protocol, cast

from rich.console import Console
from rich.markup import escape as rich_escape

import gza.colors as _colors

from ..colors import PS_STATUS_COLORS, SHOW_COLORS_DICT, pink
from ..config import Config
from ..console import (
    MAX_PROMPT_DISPLAY,
    console,
    prompt_available_width,
    shorten_prompt,
    truncate,
)
from ..db import (
    MergeUnit,
    SqliteTaskStore,
    Task as DbTask,
    _is_readonly_snapshot_operational_error,
    task_id_numeric_key as _task_id_numeric_key,
    task_owns_merge_status,
)
from ..failure_reasons import mark_task_failed_from_cause
from ..git import Git, GitError, active_worktree_path_for_branch
from ..github import GitHub
from ..lineage import walk_based_on_descendants
from ..lineage_query import filter_display_unresolved_tasks_for_incomplete
from ..pr_ops import lookup_task_pr
from ..query import (
    _LINEAGE_REL_LABELS as _QUERY_LINEAGE_REL_LABELS,
    TaskLineageNode,
    _classify_child_relationship as _classify_lineage_child_relationship,
    _lineage_child_sort_key as _lineage_child_sort_key,
    build_lineage_tree as _build_lineage_tree_for_root,
    flatten_lineage_tree as _flatten_query_lineage_tree,
    get_code_changing_descendants_for_root as _get_code_changing_descendants_for_root_task,
    get_reviews_for_root as _get_reviews_for_root_task,
    resolve_lineage_root as _resolve_lineage_root_task,
)
from ..runner import _get_task_output, get_effective_config_for_task, write_log_entry
from ..sync_ops import (
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
    parse_csv as _parse_csv,
    projection_fields as _projection_fields,
)
from ..workers import WorkerMetadata, WorkerRegistry
from ._common import (
    TASK_COLORS,
    _build_failure_diagnostics,
    _failure_next_steps,
    _format_lineage,
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
    resolve_id,
    validate_cli_tag_values,
)
from ._queue_render import (
    QueueRenderRow as _QueueRenderRow,
    build_blocked_count_summary as _build_blocked_count_summary,
    print_queue_rows as _print_queue_rows,
    queue_render_widths as _queue_render_widths,
)
from .advance_engine import (
    classify_advance_action,
    determine_next_action,
    format_needs_attention_lifecycle,
    resolve_advance_context,
    resolve_subject_task,
)

_LINEAGE_REL_LABELS = _QUERY_LINEAGE_REL_LABELS
_QueryDateField = Literal["created", "completed", "effective"]
_PresentationMode = Literal["flat", "blocks", "grouped", "lineage", "tree", "one_line", "json", "rich"]
_stderr_console = Console(highlight=False, stderr=True)
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
)
_HISTORY_EXPLICIT_PROJECTION_FIELDS: tuple[str, ...] = _TASK_EXPLICIT_PROJECTION_FIELDS
_SEARCH_EXPLICIT_PROJECTION_FIELDS: tuple[str, ...] = _TASK_EXPLICIT_PROJECTION_FIELDS
_INCOMPLETE_PROJECTION_FIELDS: tuple[str, ...] = _projection_fields(
    _TaskProjectionSpec(preset=_TaskProjectionPreset.INCOMPLETE_SUMMARY),
    scope="lineages",
)
_INCOMPLETE_PROJECTION_FIELDS = (*_INCOMPLETE_PROJECTION_FIELDS, "trigger_source")
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

_INCOMPLETE_DEPRECATION_LINES: tuple[str, ...] = (
    "Error: `gza incomplete` is deprecated and no longer supported.",
    "",
    "Use these dedicated surfaces instead:",
    "  `uv run gza unmerged` for unmerged code work",
    "  `uv run gza advance --unimplemented` for completed plan/explore work without implementation",
    "  `uv run gza history --status failed` for factual failed-task history",
    "  `uv run gza watch --restart-failed --dry-run` for failed-task recovery decisions",
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
        )
    except (GitError, OSError, ValueError):
        return None

    if ctx.review_preserved_by_rebase is not None and ctx.review_verdict in {"APPROVED", "APPROVED_WITH_FOLLOWUPS"}:
        return f"{ctx.review_verdict} (carried across rebase {ctx.review_preserved_by_rebase.id})"
    if ctx.review_invalidated_by_rebase is not None:
        reason = "diff changed" if ctx.review_invalidated_by_rebase.changed_diff is True else "change unknown"
        return f"invalidated by rebase {ctx.review_invalidated_by_rebase.id} ({reason})"
    return None


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
) -> str:
    """Render a truthful blocked-dependency label for `gza next --all` rows."""
    if blocking_status and blocking_id:
        return f"(blocked-by-{blocking_status} {blocking_id})"
    if blocking_status:
        return f"(blocked-by-{blocking_status})"
    if blocking_id:
        return f"(blocked by {blocking_id})"
    return "(blocked by dependency)"


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


def cmd_next(args: argparse.Namespace) -> int:
    """List upcoming pending tasks in order."""
    config = Config.load(args.project_dir)
    store = get_store(config, open_mode="query_only")
    service = _TaskQueryService(store)
    try:
        tag_filters, any_tag = parse_cli_tag_filters(args)
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1

    queue_rows = [
        row
        for row in service.run(
            _TaskQueryPresets.queue_listing(limit=None, tags=tag_filters, any_tag=any_tag)
        ).rows
        if isinstance(row, _TaskRow)
    ]
    runnable_rows = [row for row in queue_rows if not bool(row.values.get("blocked"))]
    blocked_rows = [row for row in queue_rows if bool(row.values.get("blocked"))]

    # Check for orphaned/stale tasks once, regardless of whether pending tasks exist
    registry = WorkerRegistry(config.workers_path)
    orphaned = _get_orphaned_tasks(registry, store)

    if not queue_rows:
        if tag_filters:
            console.print(f"No pending tasks matching tags: {', '.join(tag_filters)}")
        else:
            console.print("No pending tasks")
        if orphaned:
            _print_orphaned_warning(orphaned)
        return 0

    # Filter blocked tasks unless --all is specified
    show_all = bool(getattr(args, "all", False))
    rendered_rows = [
        _QueueRenderRow(task=row.task, position_text=str(index))
        for index, row in enumerate(runnable_rows, 1)
    ]
    if show_all:
        rendered_rows.extend(
            _QueueRenderRow(
                task=row.task,
                position_text="-",
                blocked=True,
                blocked_by_text=_format_blocked_dependency_label(
                    cast(str | None, row.values.get("blocking_id")),
                    cast(str | None, row.values.get("blocking_status")),
                )[1:-1],
            )
            for row in blocked_rows
        )
    widths = _queue_render_widths(rendered_rows)

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

    # Show blocked tasks if --all is specified
    if show_all and blocked_rows:
        if runnable_rows:
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
    any_tag = bool(getattr(args, "any_tag", False))

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
        all_rows = tuple(row for row in service.run(query).rows if isinstance(row, _TaskRow))
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
                return unit.state
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
        merge_label = " \\[merged]" if merge_state == "merged" and task_owns_merge_status(task) else ""
        tid = c['task_id']
        if task.based_on and task.depends_on:
            parent_label = f" ← [{tid}]{task.based_on}[/{tid}] (dep [{tid}]{task.depends_on}[/{tid}])"
        elif task.based_on:
            parent_label = f" ← [{tid}]{task.based_on}[/{tid}]"
        elif task.depends_on:
            parent_label = f" ← [{tid}]{task.depends_on}[/{tid}]"
        else:
            parent_label = ""

        if compact_child and task.task_type in {"review", "improve"}:
            compact_parts = [f"{type_label}{merge_label}{parent_label}"]
            if task.task_type == "review":
                verdict = get_review_verdict(config, task)
                if verdict:
                    verdict_part = f"verdict: {verdict}"
                    if task.review_score is not None:
                        verdict_part = f"{verdict_part} ({task.review_score})"
                    compact_parts.append(verdict_part)
            stats_str = format_stats(task)
            if stats_str:
                compact_parts.append(f"stats: [{c['stats']}]{stats_str}[/{c['stats']}]")
            console.print(f"{detail_prefix}    " + " | ".join(compact_parts))
            return

        console.print(f"{detail_prefix}    {type_label}{merge_label}{parent_label}")
        show_branch = bool(task.branch) and not shares_parent_branch
        if show_branch:
            console.print(f"{detail_prefix}    branch: [{c['branch']}]{task.branch}[/{c['branch']}]")
        if task.report_file:
            console.print(f"{detail_prefix}    report: [{c['file']}]{task.report_file}[/{c['file']}]")
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
    any_tag = bool(getattr(args, "any_tag", False))

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
    result = service.run(query)
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
    if blocked_by_dropped_only:
        query = _TaskQuery(
            scope="tasks",
            limit=limit,
            statuses=("pending",),
            task_types=(task_type_filter,) if task_type_filter else None,
            dependency_state=("blocked_by_dropped_dep",),
            date_filter=date_filter,
            projection=_TaskProjectionSpec(fields=projection_fields or ("id", "prompt", "status", "task_type", "blocking_id")),
            presentation=_TaskPresentationSpec(mode="json" if getattr(args, "json", False) else "blocks"),
        )
    else:
        query = _TaskQueryPresets.incomplete(
            limit=limit,
            task_types=(task_type_filter,) if task_type_filter else None,
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

    for row in result.rows:
        if not isinstance(row, _LineageRow):
            normalized_rows.append(row)
            continue

        owner_task = _resolve_incomplete_owner_task(store, row)
        action = row.next_action_data
        if action is not None and classify_advance_action(action) == "needs_attention":
            owner_task = resolve_subject_task(store, action, row, fallback_task=owner_task)
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
        if owner_task.status == "dropped" or not unresolved_tasks:
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
            next_action_data=row.next_action_data,
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
        owner_task = _resolve_lineage_owner_task(store, candidate)
        if owner_task.task_type == "implement" and owner_task.branch:
            return owner_task
    for candidate in candidates:
        owner_task = _resolve_lineage_owner_task(store, candidate)
        if owner_task.task_type == "implement":
            return owner_task
    return row.owner_task


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
    if needs_pr_metadata:
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
        if needs_pr_metadata and gh is not None:
            if gh_available is None:
                gh_available = gh.is_available()
            pr_lookup = lookup_task_pr(
                owner_task,
                gh=gh,
                available=gh_available,
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
                if result.skipped_reason is None and result.merge_status != "merged"
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
            refresh_candidate_count = sum(len(cohort.tasks) for cohort in refresh_cohorts)
            _print_unmerged_progress(
                f"refreshing canonical merge truth for {refresh_candidate_count} candidate tasks "
                f"across {len(refresh_cohorts)} branches",
                to_stderr=True,
            )
            reconcile_results, partial = sync_branch_cohorts(
                store,
                cast(Git, git_client),
                refresh_cohorts,
                include_git=True,
                include_pr=False,
                dry_run=False,
                fetch_remote=bool(getattr(args, "fetch", False)),
                allow_cached_remote_target_ref_without_fetch=True,
            )
            if partial:
                errors = [error for result in reconcile_results for error in result.errors]
                if errors:
                    print(f"Error: failed to refresh canonical merge truth: {errors[0]}")
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

    owner_ids = tuple(
        dict.fromkeys(
            owner.id
            for owner in (_resolve_lineage_owner_task(store, task) for task in selected_tasks)
            if owner.id is not None
        )
    )
    if not owner_ids:
        _print_unmerged_empty(use_json=use_json)
        return 0

    limit = None if getattr(args, "limit", 5) == 0 else getattr(args, "limit", 5)
    merge_unit_ids_list: list[str] = []
    for task in selected_tasks:
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
        merge_unit_ids=merge_unit_ids or None,
        task_ids=tuple(task.id for task in selected_tasks if task.id is not None),
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
        rows.sort(key=_ps_sort_key)
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

    header = (
        f"{'TASK ID':<10} {'TYPE':<10} "
        f"{'STATUS':<16} {'PID':<8} {'STARTED':<24} {'STEPS':<7} {'DURATION':<10} {'TASK'}"
    )
    console.print(f"[bold]{header}[/bold]", soft_wrap=True)
    console.print("[bold]" + "─" * 106 + "[/bold]", soft_wrap=True)

    for row in rows:
        task_id_display = f"{row['task_id']}" if row["task_id"] is not None else ""
        status = row['status']
        if status == "failed" and row.get("startup_failure"):
            status = "failed(startup)"
        sc = STATUS_COLORS.get(status, "white")

        # Escape Rich markup in task display (may contain brackets from truncation)
        task_display = row['task'].replace('[', '\\[') if row['task'] else ''

        console.print(
            f"[cyan]{task_id_display:<10}[/cyan] {row['type']:<10} "
            f"[{sc}]{status:<16}[/{sc}] {row['pid']:<8} {row['started']:<24} {row['steps']:<7} {row['duration']:<10} "
            f"[{pink}]{task_display}[/{pink}]",
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
        try:
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
                )
                last_poll_at = _dt.datetime.now(_dt.UTC)
                time.sleep(poll_interval)
        except KeyboardInterrupt:
            return 0
    else:
        _print_ps_output(args, registry, store, show_all=show_all, recent_minutes=recent_minutes)

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
        elif not (task and task.running_pid):
            status = "stale"
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
    """Show the full lineage tree for a given task."""
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
        if type_str not in {"plan", "implement"}:
            return ""
        if t.slug:
            value = _strip_slug_date_prefix(t.slug)
        else:
            value = t.prompt.split("\n")[0].strip()
        return value[:60] + "…" if len(value) > 60 else value

    rows: list[tuple[TaskLineageNode, str]] = []

    def _collect_rows(
        node: TaskLineageNode,
        *,
        ancestors_last: tuple[bool, ...] = (),
    ) -> None:
        if ancestors_last:
            prefix = "".join(" " if flag else "│" for flag in ancestors_last[:-1])
            prefix += "└── " if ancestors_last[-1] else "├── "
        else:
            prefix = ""
        rows.append((node, prefix))

        for idx, child in enumerate(node.children):
            _collect_rows(
                child,
                ancestors_last=(*ancestors_last, idx == len(node.children) - 1),
            )

    _collect_rows(lineage_tree)

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

    def _format_utc_timestamp(value: datetime) -> str:
        ts = value.astimezone(UTC) if value.tzinfo is not None else value
        return f"{ts.strftime('%Y-%m-%d %H:%M:%S')} UTC"

    console.print(f"[{c['heading']}]Task {task.id}[/{c['heading']}]")
    console.print(f"[{c['section']}]{'=' * 50}[/{c['section']}]")
    console.print(f"[{c['label']}]Status:[/{c['label']}] [{status_color}]{task.status}[/{status_color}]")
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
    if task.task_type in {"rebase", "improve"}:
        console.print(
            f"[{c['label']}]Changed Diff:[/{c['label']}] "
            f"[{c['value']}]{_format_changed_diff_label(task.changed_diff)}[/{c['value']}]"
        )
    if task.merge_status and task_owns_merge_status(task):
        console.print(f"[{c['label']}]Merge Status:[/{c['label']}] [{c['value']}]{task.merge_status}[/{c['value']}]")
    console.print(f"[{c['label']}]Type:[/{c['label']}] [{c['value']}]{task.task_type}[/{c['value']}]")
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
    if task.task_type == "review":
        verdict = get_review_verdict(config, task)
        if verdict:
            console.print(f"[{c['label']}]Verdict:[/{c['label']}] [{c['value']}]{verdict}[/{c['value']}]")
        score = task.review_score
        if score is None and task.status == "completed":
            score = get_review_score(config, task)
        if score is not None:
            console.print(f"[{c['label']}]Score:[/{c['label']}] [{c['value']}]{score}/100[/{c['value']}]")
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

    metadata_only = getattr(args, "metadata_only", False)
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
                    f"state={state}",
                    f"created={_format_utc_timestamp(comment.created_at)}",
                ]
                if comment.author:
                    meta_parts.append(f"author={comment.author}")
                if comment.resolved_at is not None:
                    meta_parts.append(f"resolved={_format_utc_timestamp(comment.resolved_at)}")
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
        diagnostics = _build_failure_diagnostics(task, log_path, config.verify_command)
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

        next_step_commands = _failure_next_steps(task, guidance_reason, config=config)
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
        full_mode = getattr(args, "full", False)
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
