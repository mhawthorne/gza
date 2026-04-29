"""Parser setup, subparser definitions, dispatch, and main() entry point."""

import argparse
import sys
from pathlib import Path

from ..config import Config, ConfigError, discover_project_dir, persist_project_id_if_missing
from ..db import (
    KNOWN_EXECUTION_MODES,
    InvalidTaskIdError,
    ManualMigrationRequired,
    SchemaIntegrityError,
    SqliteTaskStore,
    check_migration_status,
    import_legacy_local_db,
    preview_v25_migration,
    preview_v26_migration,
    run_v25_migration,
    run_v26_migration,
    run_v27_migration,
)
from ..learnings import DEFAULT_LEARNINGS_WINDOW
from ._common import (
    GzaArgumentParser,
    SortingHelpFormatter,
    _add_query_filter_args,
    _add_skills_install_args,
    add_common_args,
    prune_terminal_dead_workers,
    reconcile_in_progress_tasks,
)
from .config_cmds import (
    cmd_clean,
    cmd_config,
    cmd_config_keys,
    cmd_import,
    cmd_init,
    cmd_learnings,
    cmd_skills_install,
    cmd_stats,
    cmd_sync_report,
    cmd_validate,
)
from .execution import (
    cmd_add,
    cmd_comment,
    cmd_edit,
    cmd_extract,
    cmd_fix,
    cmd_implement,
    cmd_improve,
    cmd_iterate,
    cmd_mark_completed,
    cmd_resume,
    cmd_retry,
    cmd_review,
    cmd_run,
    cmd_run_inline,
    cmd_set_status,
)
from .git_ops import (
    cmd_advance,
    cmd_checkout,
    cmd_diff,
    cmd_merge,
    cmd_pr,
    cmd_rebase,
    cmd_refresh,
)
from .log import cmd_log
from .query import (
    cmd_attach,
    cmd_delete,
    cmd_group_rename,
    cmd_groups,
    cmd_history,
    cmd_incomplete,
    cmd_kill,
    cmd_lineage,
    cmd_next,
    cmd_ps,
    cmd_search,
    cmd_show,
    cmd_status,
    cmd_unmerged,
)
from .tv import cmd_tv
from .watch import cmd_queue, cmd_watch


def _parse_search_last(value: str) -> int:
    """Parse `search --last` where 0 means unlimited and negatives are invalid."""
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--last must be an integer") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("--last must be >= 0 (use 0 for all matches)")
    return parsed


def _parse_incomplete_last(value: str) -> int:
    """Parse `incomplete --last` where 0 means unlimited and negatives are invalid."""
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--last must be an integer") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("--last must be >= 0 (use 0 for all unresolved lineages)")
    return parsed


def _parse_non_negative_int(value: str) -> int:
    """Parse integer flags that must be zero or positive."""
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("value must be an integer") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be >= 0")
    return parsed


def _parse_queue_limit(value: str) -> int:
    """Parse `queue -n/--limit` where 0 and -1 both mean unlimited."""
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--limit must be an integer") from exc
    if parsed < -1:
        raise argparse.ArgumentTypeError("--limit must be >= -1 (use 0, -1, or --all for all tasks)")
    return parsed


def main() -> int:
    parser = GzaArgumentParser(
        description="Gza - AI agent task runner",
        formatter_class=SortingHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # work command
    work_parser = subparsers.add_parser("work", help="Run the next pending task or specific tasks")
    work_parser.add_argument(
        "task_ids",
        nargs="*",
        type=str,
        help="Specific full prefixed task ID(s) to run (optional, can specify multiple)",
    )
    add_common_args(work_parser)
    work_parser.add_argument(
        "--no-docker",
        action="store_true",
        help="Run Claude directly instead of in Docker",
    )
    work_parser.add_argument(
        "--count", "-c",
        type=int,
        metavar="N",
        help="Number of tasks to run before stopping (overrides config default)",
    )
    work_parser.add_argument(
        "--background", "-b",
        action="store_true",
        help="Run worker in background (detached mode)",
    )
    work_parser.add_argument(
        "--worker-mode",
        action="store_true",
        help=argparse.SUPPRESS,  # Internal flag for background workers
    )
    work_parser.add_argument(
        "--worker-id",
        help=argparse.SUPPRESS,  # Internal: worker registry ID passed by parent
    )
    work_parser.add_argument(
        "--resume",
        action="store_true",
        help=argparse.SUPPRESS,  # Internal flag for resume mode
    )
    work_parser.add_argument(
        "--max-turns",
        type=int,
        metavar="N",
        help="Override max_turns setting from gza.yaml for this run",
    )
    work_parser.add_argument(
        "--tmux-session",
        metavar="NAME",
        help=argparse.SUPPRESS,  # Internal flag: tmux session name when running inside tmux
    )
    work_parser.add_argument(
        "--force",
        action="store_true",
        help="Skip dependency precondition checks (allows running with unmerged depends_on tasks)",
    )
    work_parser.add_argument(
        "--pr",
        action="store_true",
        dest="create_pr",
        help="Create/reuse a GitHub PR after successful code-task completion (when branch has commits)",
    )
    work_parser.add_argument(
        "--group",
        metavar="NAME",
        help="Deprecated alias for --tag (single value)",
    )
    work_parser.add_argument(
        "--tag",
        action="append",
        dest="tags",
        metavar="TAG",
        help="Only pick pending tasks matching tag filters when no task IDs are specified (repeatable)",
    )
    work_parser.add_argument(
        "--any-tag",
        action="store_true",
        dest="any_tag",
        help="With repeated --tag values, match any tag instead of all tags",
    )

    # run-inline command
    run_inline_parser = subparsers.add_parser(
        "run-inline",
        help="Run a specific task in the foreground through runner-managed execution",
    )
    run_inline_parser.add_argument(
        "task_id",
        type=str,
        help="Full prefixed task ID to run inline",
    )
    run_inline_parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from the stored provider session instead of starting fresh",
    )
    run_inline_parser.add_argument(
        "--no-docker",
        action="store_true",
        help="Run provider directly instead of in Docker",
    )
    run_inline_parser.add_argument(
        "--max-turns",
        type=int,
        metavar="N",
        help="Override max_turns setting from gza.yaml for this run",
    )
    run_inline_parser.add_argument(
        "--force",
        action="store_true",
        help="Skip dependency precondition checks when starting the inline run",
    )
    add_common_args(run_inline_parser)

    # attach command
    attach_parser = subparsers.add_parser("attach", help="Attach to a running task (interactive for Claude, observe-only for Codex/Gemini)")
    attach_parser.add_argument(
        "worker_id",
        help="Worker ID (e.g. w-20260301-1) or full prefixed task ID (e.g. gza-1234) to attach to",
    )
    add_common_args(attach_parser)

    # next command
    next_parser = subparsers.add_parser("next", help="List upcoming pending tasks")
    add_common_args(next_parser)
    next_parser.add_argument(
        "--all",
        action="store_true",
        help="Show all pending tasks including blocked ones",
    )
    next_parser.add_argument(
        "--group",
        metavar="NAME",
        help="Deprecated alias for --tag (single value)",
    )
    next_parser.add_argument(
        "--tag",
        action="append",
        dest="tags",
        metavar="TAG",
        help="Only show pending tasks matching tag filters (repeatable)",
    )
    next_parser.add_argument(
        "--any-tag",
        action="store_true",
        dest="any_tag",
        help="With repeated --tag values, match any tag instead of all tags",
    )

    # history command
    history_parser = subparsers.add_parser("history", help="List recent completed/failed tasks")
    add_common_args(history_parser)
    _add_query_filter_args(history_parser)
    history_parser.set_defaults(last=5)
    history_parser.add_argument(
        "--status",
        type=str,
        choices=["completed", "failed", "unmerged"],
        help="Filter tasks by status (e.g., completed, failed, unmerged)",
    )
    history_parser.add_argument(
        "--incomplete",
        action="store_true",
        help=(
            "Show only tasks that have not been fully resolved "
            "(failed tasks, or completed tasks with unmerged commits)"
        ),
    )
    history_parser.add_argument(
        "--lineage-depth",
        type=int,
        dest="lineage_depth",
        default=0,
        metavar="N",
        help=(
            "Render root-deduplicated lineage trees up to N levels from each "
            "resolved root"
        ),
    )
    history_parser.add_argument(
        "--date-field",
        choices=["created", "completed", "effective"],
        default="effective",
        help="Date field used by --days/--start-date/--end-date filters (default: effective)",
    )
    history_parser.add_argument(
        "--fields",
        metavar="CSV",
        help="Projection fields override (comma-separated, requires --json)",
    )
    history_parser.add_argument(
        "--preset",
        metavar="NAME",
        help="Projection preset override (requires --json)",
    )
    history_parser.add_argument(
        "--json",
        action="store_true",
        help="Output JSON rows from the unified query API",
    )

    # search command
    search_parser = subparsers.add_parser(
        "search",
        help="Search task prompts by substring",
    )
    add_common_args(search_parser)
    search_parser.add_argument(
        "term",
        type=str,
        help="Substring to match in task prompt text",
    )
    search_parser.set_defaults(last=10)
    search_parser.add_argument(
        "--last",
        "-n",
        type=_parse_search_last,
        metavar="N",
        help="Show last N matching tasks (default: 10, 0 for all)",
    )
    search_parser.add_argument(
        "--status",
        metavar="CSV",
        help="Filter statuses (comma-separated, e.g. completed,failed)",
    )
    search_parser.add_argument(
        "--type",
        metavar="CSV",
        help="Filter task types (comma-separated)",
    )
    search_parser.add_argument(
        "--days",
        type=_parse_non_negative_int,
        metavar="N",
        help="Show only matches from the last N days",
    )
    search_parser.add_argument(
        "--start-date",
        dest="start_date",
        metavar="YYYY-MM-DD",
        help="Show only matches on or after this date",
    )
    search_parser.add_argument(
        "--end-date",
        dest="end_date",
        metavar="YYYY-MM-DD",
        help="Show only matches on or before this date",
    )
    search_parser.add_argument(
        "--date-field",
        choices=["created", "completed", "effective"],
        default="created",
        help="Date field used by --days/--start-date/--end-date filters (default: created)",
    )
    search_parser.add_argument(
        "--related-to",
        metavar="TASK_ID",
        help="Limit matches to tasks in the same lineage as TASK_ID",
    )
    search_parser.add_argument(
        "--lineage-of",
        metavar="TASK_ID",
        help="Limit matches to the canonical lineage containing TASK_ID",
    )
    search_parser.add_argument(
        "--root",
        metavar="CSV",
        help="Filter by lineage root task IDs (comma-separated)",
    )
    search_parser.add_argument(
        "--fields",
        metavar="CSV",
        help="Projection fields override (comma-separated, requires --json)",
    )
    search_parser.add_argument(
        "--preset",
        metavar="NAME",
        help="Projection preset override (requires --json)",
    )
    search_parser.add_argument(
        "--json",
        action="store_true",
        help="Output JSON rows from the unified query API",
    )
    search_parser.add_argument(
        "--tag",
        action="append",
        dest="tags",
        metavar="TAG",
        help="Filter matches by tag (repeatable)",
    )
    search_parser.add_argument(
        "--any-tag",
        action="store_true",
        dest="any_tag",
        help="With repeated --tag values, match any tag instead of all tags",
    )

    # incomplete command
    incomplete_parser = subparsers.add_parser(
        "incomplete",
        help="Show unresolved task lineages that still need attention",
    )
    add_common_args(incomplete_parser)
    incomplete_parser.set_defaults(last=5)
    incomplete_parser.add_argument(
        "--last",
        "-n",
        type=_parse_incomplete_last,
        metavar="N",
        help="Show last N unresolved lineages (0 for all)",
    )
    incomplete_parser.add_argument(
        "--type",
        type=str,
        choices=["explore", "plan", "implement", "review", "improve", "fix", "rebase", "internal"],
        help="Filter tasks by task_type before lineage rollup",
    )
    incomplete_parser.add_argument(
        "--days",
        type=_parse_non_negative_int,
        metavar="N",
        help="Show only unresolved lineages with activity in the last N days",
    )
    incomplete_parser.add_argument(
        "--date-field",
        choices=["created", "completed", "effective"],
        default="effective",
        help="Date field used by --days filters (default: effective)",
    )
    incomplete_parser.add_argument(
        "--tree",
        action="store_true",
        help="Show unresolved lineages as trees instead of one-line summaries",
    )
    incomplete_parser.add_argument(
        "--blocked-by-dropped",
        action="store_true",
        help="Show pending tasks blocked by dropped dependencies",
    )
    incomplete_parser.add_argument(
        "--verbose",
        action="store_true",
        help="Include owner metadata under one-line summaries",
    )
    incomplete_parser.add_argument(
        "--json",
        action="store_true",
        help="Output JSON rows from the unified query API",
    )

    # unmerged command
    unmerged_parser = subparsers.add_parser("unmerged", help="List tasks with unmerged work")
    add_common_args(unmerged_parser)
    unmerged_parser.add_argument(
        "--commits-only",
        action="store_true",
        help="Use commit-based detection (git cherry) instead of diff-based detection",
    )
    unmerged_parser.add_argument(
        "--all",
        action="store_true",
        help="Include failed tasks and check git directly for commits instead of trusting has_commits",
    )
    unmerged_parser.add_argument(
        "-n",
        type=int,
        default=5,
        dest="limit",
        metavar="N",
        help="Show last N unmerged tasks (default: 5, 0 for all)",
    )
    unmerged_parser.add_argument(
        "--update",
        action="store_true",
        help="Reconcile unmerged tasks against live git state before listing",
    )
    target_group = unmerged_parser.add_mutually_exclusive_group()
    target_group.add_argument(
        "--into-current",
        action="store_true",
        help="List tasks unmerged relative to the current branch instead of the default branch",
    )
    target_group.add_argument(
        "--target",
        metavar="BRANCH",
        help="List tasks unmerged relative to the specified branch instead of the default branch",
    )

    # advance command
    advance_parser = subparsers.add_parser(
        "advance",
        help="Intelligently progress unmerged tasks through their lifecycle",
    )
    add_common_args(advance_parser)
    advance_parser.add_argument(
        "task_id",
        type=str,
        nargs="?",
        metavar="task_id",
        help="Specific full prefixed task ID to advance (omit to advance all eligible tasks)",
    )
    advance_parser.add_argument(
        "--dry-run",
        action="store_true",
        dest="dry_run",
        help="Preview actions without executing them",
    )
    advance_parser.add_argument(
        "--max",
        type=int,
        metavar="N",
        dest="max",
        help="Limit the number of tasks to advance",
    )
    advance_parser.add_argument(
        "--no-docker",
        action="store_true",
        help="Run workers directly instead of in Docker",
    )
    advance_parser.add_argument(
        "--force",
        action="store_true",
        help="Skip dependency precondition checks when advance starts workers",
    )
    advance_parser.add_argument(
        "--unimplemented",
        action="store_true",
        help="List completed plans/explores with no implementation task yet",
    )
    advance_parser.add_argument(
        "--plans",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    advance_parser.add_argument(
        "--create",
        action="store_true",
        help="With --unimplemented: create queued implement tasks for listed tasks",
    )
    advance_parser.add_argument(
        "--auto",
        "-y",
        action="store_true",
        dest="auto",
        help="Skip confirmation prompt and execute immediately (for scripts/cron)",
    )
    advance_parser.add_argument(
        "--batch",
        type=int,
        metavar="B",
        help="Stop after spawning B background workers. Merge actions do not count toward this limit.",
    )
    advance_parser.add_argument(
        "--no-resume-failed",
        action="store_true",
        dest="no_resume_failed",
        help="Skip auto-resume of resumable failed tasks (for example MAX_STEPS, MAX_TURNS, TIMEOUT, TERMINATED)",
    )
    advance_parser.add_argument(
        "--max-resume-attempts",
        type=int,
        metavar="N",
        dest="max_resume_attempts",
        help="Override max_resume_attempts config value for this run",
    )
    advance_parser.add_argument(
        "--max-review-cycles",
        type=int,
        metavar="N",
        dest="max_review_cycles",
        help="Override max_review_cycles config value for this run",
    )
    advance_parser.add_argument(
        "--new",
        action="store_true",
        help="Start new pending tasks to fill remaining --batch slots (requires --batch)",
    )
    advance_parser.add_argument(
        "--type",
        choices=["plan", "implement"],
        dest="advance_type",
        help="Only advance tasks of this type (plan: create+start implement tasks; implement: review/merge lifecycle)",
    )
    advance_parser.add_argument(
        "--squash-threshold",
        type=int,
        default=None,
        metavar="N",
        dest="squash_threshold",
        help=(
            "Override merge_squash_threshold for this run. "
            "Squash-merge branches with N or more commits. "
            "0 disables auto-squash. Default: from gza.yaml."
        ),
    )

    # watch command
    watch_parser = subparsers.add_parser(
        "watch",
        help="Continuously maintain a target number of running workers",
    )
    add_common_args(watch_parser)
    watch_parser.add_argument(
        "--batch",
        type=int,
        metavar="N",
        help="Target number of concurrent workers to maintain (default: watch.batch or 5)",
    )
    watch_parser.add_argument(
        "--poll",
        type=int,
        metavar="SECS",
        help="Polling interval in seconds (default: watch.poll or 300)",
    )
    watch_parser.add_argument(
        "--max-idle",
        type=int,
        metavar="SECS",
        dest="max_idle",
        help="Exit after SECS of consecutive idle cycles (default: watch.max_idle)",
    )
    watch_parser.add_argument(
        "--max-iterations",
        type=int,
        metavar="N",
        dest="max_iterations",
        help="Max iterate review/improve loops when watch launches implement tasks (default: watch.max_iterations or 10)",
    )
    watch_parser.add_argument(
        "--restart-failed",
        action="store_true",
        dest="restart_failed",
        help="Enable failed-task recovery mode (resume/retry failed tasks before pending queue work)",
    )
    watch_parser.add_argument(
        "--restart-failed-batch",
        type=int,
        metavar="N",
        dest="restart_failed_batch",
        help="Max concurrent failed-task recovery launches while --restart-failed is active (default: watch.restart_failed_batch or 1)",
    )
    watch_parser.add_argument(
        "--max-resume-attempts",
        type=int,
        metavar="N",
        dest="max_resume_attempts",
        help="Override max_resume_attempts for watch auto-resume and --restart-failed recovery decisions",
    )
    watch_parser.add_argument(
        "--dry-run",
        action="store_true",
        dest="dry_run",
        help="Show what watch would do without executing; with --restart-failed, print the failed-recovery report and exit",
    )
    watch_parser.add_argument(
        "--quiet",
        action="store_true",
        help="Write events to .gza/watch.log only",
    )
    watch_parser.add_argument(
        "-y", "--yes",
        action="store_true",
        help="Skip confirmation prompt before first cycle",
    )
    watch_parser.add_argument(
        "--group",
        metavar="NAME",
        help="Deprecated alias for --tag (single value)",
    )
    watch_parser.add_argument(
        "--tag",
        action="append",
        dest="tags",
        metavar="TAG",
        help="Only advance, resume, and start tasks matching tag filters (repeatable); use 'gza queue --tag TAG' to preview scoped pickup order",
    )
    watch_parser.add_argument(
        "--any-tag",
        action="store_true",
        dest="any_tag",
        help="With repeated --tag values, match any tag instead of all tags",
    )

    # queue command
    queue_parser = subparsers.add_parser(
        "queue",
        help="List runnable pending tasks in pickup order and manage urgent bump flags",
    )
    add_common_args(queue_parser)
    queue_parser.set_defaults(limit=10, all=False)
    queue_parser.add_argument(
        "--group",
        metavar="NAME",
        help="Deprecated alias for --tag (single value)",
    )
    queue_parser.add_argument(
        "--tag",
        action="append",
        dest="tags",
        metavar="TAG",
        help="Only list runnable tasks matching tag filters (repeatable); same scoped pickup order used by 'gza watch --tag TAG'",
    )
    queue_parser.add_argument(
        "--any-tag",
        action="store_true",
        dest="any_tag",
        help="With repeated --tag values, match any tag instead of all tags",
    )
    queue_parser.add_argument(
        "-n",
        "--limit",
        type=_parse_queue_limit,
        metavar="N",
        help="Show first N runnable tasks (default: 10; use 0, -1, or --all for all)",
    )
    queue_parser.add_argument(
        "--all",
        action="store_true",
        help="Show all runnable tasks",
    )

    def _add_queue_tag_scope_args(subparser: argparse.ArgumentParser, *, action: str) -> None:
        """Add queue management tag-scope filters for runnable status messages."""
        subparser.add_argument(
            "--group",
            metavar="NAME",
            help=f"Deprecated alias for --tag; check runnable status within this tag scope while {action}",
        )
        subparser.add_argument(
            "--tag",
            action="append",
            dest="tags",
            metavar="TAG",
            help=f"Check runnable status only within matching tag filters while {action} (repeatable)",
        )
        subparser.add_argument(
            "--any-tag",
            action="store_true",
            dest="any_tag",
            help="With repeated --tag values, match any tag instead of all tags",
        )

    queue_subparsers = queue_parser.add_subparsers(dest="queue_action")
    queue_bump = queue_subparsers.add_parser("bump", help="Move a pending task to the front of the urgent queue lane")
    queue_bump.add_argument("task_id", type=str, help="Full prefixed task ID to bump")
    add_common_args(queue_bump)
    _add_queue_tag_scope_args(queue_bump, action="bumping")
    queue_unbump = queue_subparsers.add_parser("unbump", help="Move a pending task back to the normal queue lane")
    queue_unbump.add_argument("task_id", type=str, help="Full prefixed task ID to unbump")
    add_common_args(queue_unbump)
    _add_queue_tag_scope_args(queue_unbump, action="unbumping")
    queue_move = queue_subparsers.add_parser(
        "move",
        help=(
            "Assign an explicit queue position "
            "(with --tag scope shared across matching tasks; fails if target does not match scope)"
        ),
    )
    queue_move.add_argument("task_id", type=str, help="Full prefixed task ID to reorder")
    queue_move.add_argument("position", type=_parse_non_negative_int, help="1-based queue position")
    add_common_args(queue_move)
    _add_queue_tag_scope_args(queue_move, action="reordering")
    queue_next = queue_subparsers.add_parser(
        "next",
        help=(
            "Move a pending task to explicit queue position 1 "
            "(with --tag scope shared across matching tasks; fails if target does not match scope)"
        ),
    )
    queue_next.add_argument("task_id", type=str, help="Full prefixed task ID to move next")
    add_common_args(queue_next)
    _add_queue_tag_scope_args(queue_next, action="moving next")
    queue_clear = queue_subparsers.add_parser(
        "clear",
        help=(
            "Remove a task's explicit queue position and return it to lane-based ordering "
            "(with --tag scope, fails if target does not match scope)"
        ),
    )
    queue_clear.add_argument("task_id", type=str, help="Full prefixed task ID to clear")
    add_common_args(queue_clear)
    _add_queue_tag_scope_args(queue_clear, action="clearing queue order")

    # refresh command
    refresh_parser = subparsers.add_parser("refresh", help="Refresh cached diff stats for unmerged tasks")
    add_common_args(refresh_parser)
    refresh_group = refresh_parser.add_mutually_exclusive_group()
    refresh_group.add_argument(
        "task_id",
        type=str,
        nargs="?",
        metavar="task_id",
        help="Full prefixed task ID to refresh (omit to refresh all unmerged tasks)",
    )
    refresh_group.add_argument(
        "--include-failed",
        action="store_true",
        dest="include_failed",
        help="Also refresh failed tasks that have branches (cannot be used with task_id)",
    )

    # merge command
    merge_parser = subparsers.add_parser("merge", help="Merge task branches into current branch")
    merge_parser.add_argument(
        "task_ids",
        type=str,
        nargs="*",
        metavar="task_id",
        help="Full prefixed task ID(s) to merge",
    )
    merge_parser.add_argument(
        "--all",
        action="store_true",
        help="Merge all unmerged done tasks (task_ids optional when this flag is used)",
    )
    merge_parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete the branch after successful merge",
    )
    merge_parser.add_argument(
        "--squash",
        action="store_true",
        help="Perform a squash merge instead of a regular merge",
    )
    merge_parser.add_argument(
        "--rebase",
        action="store_true",
        help="Rebase the task's branch onto current branch instead of merging",
    )
    merge_parser.add_argument(
        "--remote",
        action="store_true",
        help="Fetch from origin and rebase against origin/<target-branch> (requires --rebase)",
    )
    merge_parser.add_argument(
        "--mark-only",
        action="store_true",
        help="Mark the task as merged in the database without performing an actual git merge (branch is preserved)",
    )
    merge_parser.add_argument(
        "--resolve",
        action="store_true",
        help="Auto-resolve conflicts using AI when rebasing (requires --rebase)",
    )
    add_common_args(merge_parser)

    # rebase command
    rebase_parser = subparsers.add_parser("rebase", help="Rebase a task's branch onto a target branch")
    rebase_parser.add_argument(
        "task_id",
        type=str,
        help="Full prefixed task ID to rebase",
    )
    rebase_parser.add_argument(
        "--onto",
        help="Branch to rebase onto (defaults to current branch)",
    )
    rebase_parser.add_argument(
        "--remote",
        action="store_true",
        help="Fetch from origin and rebase against origin/<target-branch>",
    )
    rebase_parser.add_argument(
        "--force", "-f",
        action="store_true",
        help="Force remove worktree even if it has uncommitted changes",
    )
    rebase_parser.add_argument(
        "--resolve",
        action="store_true",
        help="Auto-resolve conflicts using AI (non-interactive)",
    )
    rebase_parser.add_argument(
        "--background", "-b",
        action="store_true",
        help="Run rebase in background (detached mode)",
    )
    add_common_args(rebase_parser)

    # checkout command
    checkout_parser = subparsers.add_parser("checkout", help="Checkout a task's branch, removing stale worktree if needed")
    checkout_parser.add_argument(
        "task_id_or_branch",
        help="Full prefixed task ID or branch name to checkout",
    )
    checkout_parser.add_argument(
        "--force", "-f",
        action="store_true",
        help="Force removal of worktree even if it has uncommitted changes",
    )
    add_common_args(checkout_parser)

    # diff command
    diff_parser = subparsers.add_parser("diff", help="Run git diff with colored output and pager support")
    add_common_args(diff_parser)
    diff_parser.add_argument(
        "diff_args",
        nargs="*",
        help="Arguments to pass to git diff (use -- before options like --stat)",
    )

    # pr command
    pr_parser = subparsers.add_parser("pr", help="Create GitHub PR from completed task")
    pr_parser.add_argument(
        "task_id",
        type=str,
        help="Full prefixed task ID to create PR from",
    )
    pr_parser.add_argument(
        "--title",
        help="Override auto-generated PR title",
    )
    pr_parser.add_argument(
        "--draft",
        action="store_true",
        help="Create as draft PR",
    )
    add_common_args(pr_parser)

    # stats command
    stats_parser = subparsers.add_parser(
        "stats",
        help="Review and iteration analytics",
    )
    add_common_args(stats_parser)
    stats_subs = stats_parser.add_subparsers(dest="stats_subcommand")
    stats_parser.set_defaults(_stats_parser=stats_parser)

    # stats reviews subcommand
    stats_reviews_parser = stats_subs.add_parser(
        "reviews", help="Show review count stats per implementation task"
    )
    add_common_args(stats_reviews_parser)
    stats_reviews_parser.add_argument(
        "--issues",
        action="store_true",
        help="Show per-model issue counts parsed from review content",
    )
    stats_reviews_parser.add_argument(
        "--days",
        type=int,
        metavar="N",
        default=None,
        help="Show tasks from the last N days (default: 14)",
    )
    stats_reviews_parser.add_argument(
        "--start-date",
        dest="start_date",
        metavar="YYYY-MM-DD",
        help="Show only tasks on or after this date",
    )
    stats_reviews_parser.add_argument(
        "--end-date",
        dest="end_date",
        metavar="YYYY-MM-DD",
        help="Show only tasks on or before this date",
    )
    stats_reviews_parser.add_argument(
        "--all",
        dest="all_time",
        action="store_true",
        help="Show stats across all time (ignore --days/--start-date/--end-date)",
    )
    stats_reviews_parser.add_argument(
        "--json",
        action="store_true",
        help="Output review stats and score analytics as JSON",
    )

    # stats iterations subcommand
    stats_iterations_parser = stats_subs.add_parser(
        "iterations",
        help="Show per-implementation review/improve iteration rollups",
    )
    add_common_args(stats_iterations_parser)
    stats_iterations_parser.add_argument(
        "-n",
        "--last",
        type=int,
        metavar="N",
        default=20,
        help="Limit output to the N most recent implementation tasks (default: 20)",
    )
    stats_iterations_parser.add_argument(
        "--hours",
        type=int,
        metavar="N",
        default=None,
        help="Show tasks with activity in the last N hours (cannot combine with --days/--start-date/--end-date)",
    )
    stats_iterations_parser.add_argument(
        "--days",
        type=int,
        metavar="N",
        default=None,
        help="Show tasks from the last N days (default: 14)",
    )
    stats_iterations_parser.add_argument(
        "--start-date",
        dest="start_date",
        metavar="YYYY-MM-DD",
        help="Show only tasks on or after this date",
    )
    stats_iterations_parser.add_argument(
        "--end-date",
        dest="end_date",
        metavar="YYYY-MM-DD",
        help="Show only tasks on or before this date",
    )
    stats_iterations_parser.add_argument(
        "--all",
        "--all-time",
        dest="all_time",
        action="store_true",
        help="Show stats across all time (cannot combine with --hours/--days/--start-date/--end-date)",
    )

    # validate command
    validate_parser = subparsers.add_parser("validate", help="Validate gza.yaml configuration")
    add_common_args(validate_parser)

    # config command
    config_parser = subparsers.add_parser("config", help="Show effective config with source attribution")
    config_subparsers = config_parser.add_subparsers(dest="config_action")
    config_keys_parser = config_subparsers.add_parser("keys", help="List discoverable configuration keys")
    config_keys_parser.add_argument(
        "--json",
        action="store_true",
        help="Output config key registry as JSON",
    )
    add_common_args(config_keys_parser)
    config_parser.add_argument(
        "--json",
        action="store_true",
        help="Output effective config and source attribution as JSON",
    )
    add_common_args(config_parser)

    # clean command
    clean_parser = subparsers.add_parser("clean", help="Clean up stale worktrees, old logs, worker metadata, and archives")
    clean_parser.add_argument(
        "--worktrees",
        action="store_true",
        help="Only clean up stale worktrees",
    )
    clean_parser.add_argument(
        "--workers",
        action="store_true",
        help="Only clean up stale worker metadata",
    )
    clean_parser.add_argument(
        "--logs",
        action="store_true",
        help="Only clean up old log files",
    )
    clean_parser.add_argument(
        "--backups",
        action="store_true",
        help="Only clean up old backup files",
    )
    clean_parser.add_argument(
        "--days",
        type=int,
        default=None,
        metavar="N",
        help="Remove items older than N days (default: from config cleanup_days, or 30)",
    )
    clean_parser.add_argument(
        "--keep-unmerged",
        action="store_true",
        help="Keep logs for tasks that are still unmerged",
    )
    clean_parser.add_argument(
        "--archive",
        action="store_true",
        help="Archive old log and worker files instead of deleting",
    )
    clean_parser.add_argument(
        "--purge",
        action="store_true",
        help="Delete previously archived files (default: older than 365 days)",
    )
    clean_parser.add_argument(
        "--force",
        action="store_true",
        help="Skip confirmation prompt before removing worktrees",
    )
    clean_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be cleaned without actually doing it",
    )
    add_common_args(clean_parser)

    # init command
    init_parser = subparsers.add_parser("init", help="Generate new gza.yaml with defaults")
    add_common_args(init_parser)
    init_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing gza.yaml file",
    )

    # log command
    log_parser = subparsers.add_parser("log", help="Display log for a task or worker")
    log_parser.add_argument(
        "identifier",
        help="Full prefixed task ID, slug, or worker ID",
    )
    log_parser.add_argument(
        "--slug", "-s",
        action="store_true",
        help="Look up by task slug (supports partial match)",
    )
    log_parser.add_argument(
        "--worker", "-w",
        action="store_true",
        help="Look up by worker ID",
    )
    timeline_group = log_parser.add_mutually_exclusive_group()
    timeline_group.add_argument(
        "--steps",
        dest="timeline_mode",
        action="store_const",
        const="compact",
        help="Show compact step timeline (S<n>)",
    )
    timeline_group.add_argument(
        "--steps-verbose",
        dest="timeline_mode",
        action="store_const",
        const="verbose",
        help="Show verbose step timeline with substeps (S<n>.<m>)",
    )
    timeline_group.add_argument(
        "--turns",
        dest="timeline_mode",
        action="store_const",
        const="verbose",
        help="Deprecated alias for --steps-verbose",
    )
    log_parser.add_argument(
        "--follow", "-f",
        action="store_true",
        help="Follow log in real-time when the requested task or worker is actively running",
    )
    log_parser.add_argument(
        "--tail",
        type=int,
        metavar="N",
        help="Show last N lines only (used with --follow or --raw)",
    )
    log_parser.add_argument(
        "--raw",
        action="store_true",
        help="Show raw JSON lines instead of formatted output",
    )
    log_parser.add_argument(
        "--failure", "-F",
        action="store_true",
        help="Show failure-focused diagnostics (failed tasks only): reason, summary, agent explanation, and last verify/result context",
    )
    log_parser.add_argument(
        "--page",
        action="store_true",
        help="Pipe output through $PAGER (default: less -R); skipped for --follow and --raw modes",
    )
    add_common_args(log_parser)

    # tv command
    tv_parser = subparsers.add_parser("tv", help="Live multi-task log dashboard")
    tv_parser.add_argument(
        "task_ids",
        nargs="*",
        help="Task IDs to watch (default: auto-select running tasks)",
    )
    tv_parser.add_argument(
        "--number", "-n",
        type=int,
        metavar="N",
        help="Fixed slot count (equivalent to --min N --max N)",
    )
    tv_parser.add_argument(
        "--min",
        dest="min_slots",
        type=int,
        metavar="N",
        help="Minimum slot count in auto-select mode (default: 1)",
    )
    tv_parser.add_argument(
        "--max",
        dest="max_slots",
        type=int,
        metavar="N",
        help="Maximum slot count in auto-select mode (default: 4)",
    )
    add_common_args(tv_parser)

    # add command
    add_parser = subparsers.add_parser("add", help="Add a new task")
    add_parser.add_argument(
        "prompt",
        nargs="?",
        help="Task prompt (opens $EDITOR if not provided)",
    )
    add_parser.add_argument(
        "--edit", "-e",
        action="store_true",
        help="Open $EDITOR to write the prompt",
    )
    add_parser.add_argument(
        "--type",
        choices=["explore", "plan", "implement", "review", "improve"],
        help="Set task type (default: implement)",
    )
    add_parser.add_argument(
        "--branch-type",
        metavar="TYPE",
        help="Set branch type hint for branch naming (e.g., fix, feature, chore)",
    )
    add_parser.add_argument(
        "--explore",
        action="store_true",
        help="Create an explore task (shorthand for --type explore)",
    )
    add_parser.add_argument(
        "--group",
        metavar="NAME",
        help="Deprecated alias for --tag",
    )
    add_parser.add_argument(
        "--tag",
        action="append",
        dest="tags",
        metavar="TAG",
        help="Attach one or more tags to the task (repeatable)",
    )
    add_parser.add_argument(
        "--based-on",
        type=str,
        metavar="ID",
        help="Set lineage/parent relationship (based_on field, used for branch inheritance and context)",
    )
    add_parser.add_argument(
        "--depends-on",
        type=str,
        metavar="ID",
        help="Set execution dependency (depends_on field, blocks task until dependency completes)",
    )
    add_parser.add_argument(
        "--review",
        action="store_true",
        help="Auto-create review task on completion (for implement tasks)",
    )
    add_parser.add_argument(
        "--same-branch",
        action="store_true",
        help="Continue on depends_on task's branch instead of creating new",
    )
    add_parser.add_argument(
        "--spec",
        metavar="FILE",
        help="Path to spec file for task context",
    )
    add_parser.add_argument(
        "--prompt-file",
        metavar="FILE",
        help="Read prompt from file (for non-interactive use)",
    )
    add_parser.add_argument(
        "--next",
        action="store_true",
        help="Mark the new task urgent and bump it to the front of the urgent lane (same as add + queue bump)",
    )
    add_parser.add_argument(
        "--model",
        metavar="MODEL",
        help="Override model for this task (e.g., claude-3-5-haiku-latest)",
    )
    add_parser.add_argument(
        "--provider",
        metavar="PROVIDER",
        choices=["claude", "codex", "gemini"],
        help="Override provider for this task (claude, codex, or gemini)",
    )
    add_parser.add_argument(
        "--no-learnings",
        action="store_true",
        dest="skip_learnings",
        help="Skip injecting .gza/learnings.md context into this task's prompt",
    )
    add_common_args(add_parser)

    # edit command
    edit_parser = subparsers.add_parser("edit", help="Edit a pending task's prompt or metadata")
    edit_parser.add_argument(
        "task_id",
        type=str,
        help="Full prefixed task ID to edit",
    )
    edit_parser.add_argument(
        "--group",
        dest="group_flag",
        metavar="NAME",
        help="Deprecated alias for tag edits (empty string clears all tags); mutually exclusive with other tag mutation flags",
    )
    edit_parser.add_argument(
        "--add-tag",
        action="append",
        dest="add_tags",
        metavar="TAG",
        help="Add a tag (repeatable; mutually exclusive with other tag mutation flags)",
    )
    edit_parser.add_argument(
        "--remove-tag",
        action="append",
        dest="remove_tags",
        metavar="TAG",
        help="Remove a tag (repeatable; mutually exclusive with other tag mutation flags)",
    )
    edit_parser.add_argument(
        "--clear-tags",
        action="store_true",
        dest="clear_tags",
        help="Clear all task tags (mutually exclusive with other tag mutation flags)",
    )
    edit_parser.add_argument(
        "--set-tags",
        metavar="CSV",
        dest="set_tags",
        help="Replace task tags with a comma-separated list (mutually exclusive with other tag mutation flags)",
    )
    edit_parser.add_argument(
        "--based-on",
        dest="based_on_flag",
        type=str,
        metavar="ID",
        help="Set lineage/parent relationship (based_on field, used for branch inheritance and context)",
    )
    edit_parser.add_argument(
        "--depends-on",
        dest="depends_on_flag",
        type=str,
        metavar="ID",
        help="Set execution dependency (depends_on field, blocks task until dependency completes)",
    )
    edit_parser.add_argument(
        "--explore",
        action="store_true",
        help="Convert to an explore task",
    )
    edit_parser.add_argument(
        "--task",
        action="store_true",
        help="Convert to a regular task",
    )
    edit_parser.add_argument(
        "--review",
        action="store_true",
        help="Enable automatic review task creation on completion",
    )
    edit_parser.add_argument(
        "--prompt-file",
        metavar="FILE",
        help="Read new prompt from file (for non-interactive use)",
    )
    edit_parser.add_argument(
        "--prompt",
        metavar="TEXT",
        help="Set new prompt directly, or use '-' to read from stdin",
    )
    edit_parser.add_argument(
        "--model",
        metavar="MODEL",
        help="Set model override for this task",
    )
    edit_parser.add_argument(
        "--provider",
        metavar="PROVIDER",
        choices=["claude", "codex", "gemini"],
        help="Set provider override for this task",
    )
    edit_parser.add_argument(
        "--no-learnings",
        action="store_true",
        dest="skip_learnings",
        help="Skip injecting .gza/learnings.md context into this task's prompt",
    )
    add_common_args(edit_parser)

    # comment command
    comment_parser = subparsers.add_parser("comment", help="Add a comment to a task")
    comment_parser.add_argument(
        "task_id",
        type=str,
        help="Full prefixed task ID to comment on",
    )
    comment_parser.add_argument(
        "text",
        type=str,
        help="Comment text",
    )
    comment_parser.add_argument(
        "--author",
        type=str,
        help="Optional author attribution",
    )
    add_common_args(comment_parser)

    # learnings command
    learnings_parser = subparsers.add_parser("learnings", help="Manage project learnings")
    learnings_subparsers = learnings_parser.add_subparsers(
        dest="learnings_command",
        metavar="SUBCOMMAND",
    )
    learnings_show_parser = learnings_subparsers.add_parser(
        "show",
        help="Display the current learnings file",
    )
    add_common_args(learnings_show_parser)
    learnings_update_parser = learnings_subparsers.add_parser(
        "update",
        help="Regenerate learnings from recent completed tasks",
    )
    learnings_update_parser.add_argument(
        "--window",
        type=int,
        default=DEFAULT_LEARNINGS_WINDOW,
        help=f"Number of recent completed tasks to process (default: {DEFAULT_LEARNINGS_WINDOW})",
    )
    add_common_args(learnings_update_parser)
    add_common_args(learnings_parser)

    # delete command
    delete_parser = subparsers.add_parser("delete", help="Delete a task")
    delete_parser.add_argument(
        "task_id",
        type=str,
        help="Full prefixed task ID to delete",
    )
    delete_parser.add_argument(
        "--force", "-f",
        action="store_true",
        help="Skip confirmation prompt (deprecated, use --yes/-y)",
    )
    delete_parser.add_argument(
        "--yes", "-y",
        action="store_true",
        help="Skip confirmation prompt",
    )
    add_common_args(delete_parser)

    # retry command
    retry_parser = subparsers.add_parser("retry", help="Retry a failed or completed task")
    retry_parser.add_argument(
        "task_id",
        type=str,
        help="Full prefixed task ID to retry",
    )
    retry_parser.add_argument(
        "--no-docker",
        action="store_true",
        help="Run Claude directly instead of in Docker (only with --background)",
    )
    retry_parser.add_argument(
        "--background", "-b",
        action="store_true",
        help="Run worker in background (detached mode)",
    )
    retry_parser.add_argument(
        "--queue", "-q",
        action="store_true",
        help="Add task to queue without executing immediately",
    )
    retry_parser.add_argument(
        "--max-turns",
        type=int,
        metavar="N",
        help="Override max_turns setting from gza.yaml for this run",
    )
    retry_parser.add_argument(
        "--force",
        action="store_true",
        help="Skip dependency precondition checks when running the retry task",
    )
    add_common_args(retry_parser)

    # resume command
    resume_parser = subparsers.add_parser("resume", help="Resume a failed task from where it left off")
    resume_parser.add_argument(
        "task_id",
        type=str,
        help="Full prefixed task ID to resume",
    )
    resume_parser.add_argument(
        "--no-docker",
        action="store_true",
        help="Run Claude directly instead of in Docker",
    )
    resume_parser.add_argument(
        "--background", "-b",
        action="store_true",
        help="Run worker in background (detached mode)",
    )
    resume_parser.add_argument(
        "--queue", "-q",
        action="store_true",
        help="Add task to queue without executing immediately",
    )
    resume_parser.add_argument(
        "--max-turns",
        type=int,
        metavar="N",
        help="Override max_turns setting from gza.yaml for this run",
    )
    resume_parser.add_argument(
        "--force",
        action="store_true",
        help="Skip dependency precondition checks when running the resumed task",
    )
    add_common_args(resume_parser)

    # improve command
    improve_parser = subparsers.add_parser(
        "improve",
        help="Create an improve task from review feedback and/or unresolved task comments",
    )
    improve_parser.add_argument(
        "task_id",
        type=str,
        help="Full prefixed task ID (implement, improve, review, or fix — resolves to owning implementation)",
    )
    improve_parser.add_argument(
        "--review",
        action="store_true",
        help="Auto-create review task on completion",
    )
    improve_parser.add_argument(
        "--review-id",
        type=str,
        metavar="ID",
        help="Explicit full review task ID to base the improve on (overrides auto-pick of most recent completed review)",
    )
    improve_parser.add_argument(
        "--queue", "-q",
        action="store_true",
        help="Add task to queue without executing immediately",
    )
    improve_parser.add_argument(
        "--background", "-b",
        action="store_true",
        help="Run worker in background (detached mode)",
    )
    improve_parser.add_argument(
        "--no-docker",
        action="store_true",
        help="Run Claude directly instead of in Docker (only with --background or when running immediately)",
    )
    improve_parser.add_argument(
        "--max-turns",
        type=int,
        metavar="N",
        help="Override max_turns setting from gza.yaml for this run",
    )
    improve_parser.add_argument(
        "--model",
        metavar="MODEL",
        help="Override the model for this task (e.g. 'claude-opus-4-5')",
    )
    improve_parser.add_argument(
        "--provider",
        metavar="PROVIDER",
        help="Override the provider for this task (e.g. 'claude', 'gemini', 'codex')",
    )
    improve_parser.add_argument(
        "--force",
        action="store_true",
        help="Skip dependency precondition checks when running the improve task",
    )
    add_common_args(improve_parser)

    # fix command
    fix_parser = subparsers.add_parser(
        "fix",
        help="Create and optionally run an interactive fix rescue task for a stuck implementation lifecycle",
    )
    fix_parser.add_argument(
        "task_id",
        type=str,
        help="Full prefixed task ID (implement, improve, review, or fix — resolves to owning implementation)",
    )
    fix_parser.add_argument(
        "--queue", "-q",
        action="store_true",
        help="Add task to queue without executing immediately",
    )
    fix_parser.add_argument(
        "--background", "-b",
        action="store_true",
        help="Run worker in background (detached mode)",
    )
    fix_parser.add_argument(
        "--no-docker",
        action="store_true",
        help="Run Claude directly instead of in Docker (only with --background or when running immediately)",
    )
    fix_parser.add_argument(
        "--max-turns",
        type=int,
        metavar="N",
        help="Override max_turns setting from gza.yaml for this run",
    )
    fix_parser.add_argument(
        "--model",
        metavar="MODEL",
        help="Override the model for this task (e.g. 'claude-opus-4-5')",
    )
    fix_parser.add_argument(
        "--provider",
        metavar="PROVIDER",
        help="Override the provider for this task (e.g. 'claude', 'gemini', 'codex')",
    )
    fix_parser.add_argument(
        "--force",
        action="store_true",
        help="Skip dependency precondition checks when running the fix task",
    )
    add_common_args(fix_parser)

    def _add_iterate_args(iterate_parser: argparse.ArgumentParser) -> None:
        iterate_parser.add_argument(
            "impl_task_id",
            type=str,
            help="Full prefixed implementation task ID to iterate (e.g. 'gza-1234')",
        )
        iterate_parser.add_argument(
            "-i", "--max-iterations",
            type=int,
            default=None,
            dest="max_iterations",
            metavar="N",
            help="Maximum iterate iterations (each is a code-change task [implement/improve] plus its review) (default: iterate_max_iterations or 3)",
        )
        iterate_parser.add_argument(
            "--dry-run",
            action="store_true",
            dest="dry_run",
            help="Preview what would happen without executing",
        )
        iterate_parser.add_argument(
            "--no-docker",
            action="store_true",
            help="Run Claude directly instead of in Docker",
        )
        start_action = iterate_parser.add_mutually_exclusive_group()
        start_action.add_argument(
            "--resume",
            action="store_true",
            help="Resume a failed task before iterating (picks up where it left off)",
        )
        start_action.add_argument(
            "--retry",
            action="store_true",
            help="Retry a failed task before iterating (starts fresh)",
        )
        iterate_parser.add_argument(
            "--worker-id",
            help=argparse.SUPPRESS,  # Internal: worker registry ID passed by parent
        )
        iterate_parser.add_argument(
            "--background", "-b",
            action="store_true",
            help="Run the entire iterate loop in the background",
        )
        iterate_parser.add_argument(
            "--force",
            action="store_true",
            help="Skip dependency precondition checks when iterate starts workers",
        )
        add_common_args(iterate_parser)

    # iterate command
    iterate_parser = subparsers.add_parser(
        "iterate",
        help="Run an automated implementation lifecycle loop (review/improve/resume/rebase) for an implementation task",
        description="Run an automated implementation lifecycle loop (review/improve/resume/rebase) for an implementation task",
    )
    _add_iterate_args(iterate_parser)

    # Backward-compat parser for legacy command spelling.
    cycle_parser = subparsers.add_parser(
        "cycle",
        help=argparse.SUPPRESS,
    )
    _add_iterate_args(cycle_parser)

    # implement command
    implement_parser = subparsers.add_parser(
        "implement",
        help="Create an implementation task from a completed plan task",
    )
    implement_parser.add_argument(
        "plan_task_id",
        type=str,
        help="Completed plan full prefixed task ID to implement",
    )
    implement_parser.add_argument(
        "prompt",
        nargs="?",
        help="Implementation prompt (defaults to plan-derived prompt)",
    )
    implement_parser.add_argument(
        "--review",
        action="store_true",
        help="Auto-create review task on completion",
    )
    implement_parser.add_argument(
        "--group",
        metavar="NAME",
        help="Deprecated alias for --tag",
    )
    implement_parser.add_argument(
        "--tag",
        action="append",
        dest="tags",
        metavar="TAG",
        help="Attach one or more tags to the new implementation task (repeatable)",
    )
    implement_parser.add_argument(
        "--same-branch",
        action="store_true",
        help="Continue on depends_on task's branch instead of creating new",
    )
    implement_parser.add_argument(
        "--branch-type",
        metavar="TYPE",
        help="Set branch type hint for branch naming (e.g., fix, feature, chore)",
    )
    implement_parser.add_argument(
        "--model",
        metavar="MODEL",
        help="Override model for this task (e.g., claude-3-5-haiku-latest)",
    )
    implement_parser.add_argument(
        "--provider",
        metavar="PROVIDER",
        choices=["claude", "codex", "gemini"],
        help="Override provider for this task (claude, codex, or gemini)",
    )
    implement_parser.add_argument(
        "--no-learnings",
        action="store_true",
        dest="skip_learnings",
        help="Skip injecting .gza/learnings.md context into this task's prompt",
    )
    implement_parser.add_argument(
        "--queue", "-q",
        action="store_true",
        help="Add task to queue without executing immediately",
    )
    implement_parser.add_argument(
        "--background", "-b",
        action="store_true",
        help="Run worker in background (detached mode)",
    )
    implement_parser.add_argument(
        "--no-docker",
        action="store_true",
        help="Run Claude directly instead of in Docker (only with --background or when running immediately)",
    )
    implement_parser.add_argument(
        "--max-turns",
        type=int,
        metavar="N",
        help="Override max_turns setting from gza.yaml for this run",
    )
    implement_parser.add_argument(
        "--force",
        action="store_true",
        help="Skip dependency precondition checks when running the implement task",
    )
    add_common_args(implement_parser)

    # extract command
    extract_parser = subparsers.add_parser(
        "extract",
        help="Create an implementation task from selected file changes on a source task/branch",
    )
    extract_parser.add_argument(
        "source",
        nargs="?",
        help="Source full prefixed task ID to extract from (alternative to --branch)",
    )
    extract_parser.add_argument(
        "paths",
        nargs="*",
        help="Repo-relative files to extract from the source diff",
    )
    extract_parser.add_argument(
        "--branch",
        metavar="BRANCH",
        help="Source branch to extract from (alternative to SOURCE task ID)",
    )
    extract_parser.add_argument(
        "--files-from",
        metavar="FILE",
        help="Read newline-delimited selected files from FILE",
    )
    extract_parser.add_argument(
        "--prompt",
        metavar="TEXT",
        help="Additional operator intent appended to the drafted extraction prompt",
    )
    extract_parser.add_argument(
        "--review",
        action="store_true",
        help="Auto-create review task on completion",
    )
    extract_parser.add_argument(
        "--group",
        metavar="NAME",
        help="Deprecated alias for --tag",
    )
    extract_parser.add_argument(
        "--tag",
        action="append",
        dest="tags",
        metavar="TAG",
        help="Attach one or more tags to the new implementation task (repeatable)",
    )
    extract_parser.add_argument(
        "--branch-type",
        metavar="TYPE",
        help="Set branch type hint for branch naming (e.g., fix, feature, chore)",
    )
    extract_parser.add_argument(
        "--base-branch",
        metavar="BRANCH",
        help="Override base branch for source diff calculation and target task branch creation",
    )
    extract_parser.add_argument(
        "--model",
        metavar="MODEL",
        help="Override model for this task (e.g., claude-3-5-haiku-latest)",
    )
    extract_parser.add_argument(
        "--provider",
        metavar="PROVIDER",
        choices=["claude", "codex", "gemini"],
        help="Override provider for this task (claude, codex, or gemini)",
    )
    extract_parser.add_argument(
        "--no-learnings",
        action="store_true",
        dest="skip_learnings",
        help="Skip injecting .gza/learnings.md context into this task's prompt",
    )
    extract_parser.add_argument(
        "--queue", "-q",
        action="store_true",
        help="Add task to queue without executing immediately",
    )
    extract_parser.add_argument(
        "--background", "-b",
        action="store_true",
        help="Run worker in background (detached mode)",
    )
    extract_parser.add_argument(
        "--no-docker",
        action="store_true",
        help="Run Claude directly instead of in Docker (only with --background or when running immediately)",
    )
    extract_parser.add_argument(
        "--max-turns",
        type=int,
        metavar="N",
        help="Override max_turns setting from gza.yaml for this run",
    )
    extract_parser.add_argument(
        "--force",
        action="store_true",
        help="Skip dependency precondition checks when running the extracted implement task",
    )
    add_common_args(extract_parser)

    # review command
    review_parser = subparsers.add_parser(
        "review",
        help="Create and optionally run a review task for an implementation or improve task",
    )
    review_parser.add_argument(
        "task_id",
        type=str,
        help="Full prefixed task ID (implement, improve, review, or fix — resolves to owning implementation)",
    )
    review_parser.add_argument(
        "--queue", "-q",
        action="store_true",
        help="Add task to queue without executing immediately",
    )
    review_parser.add_argument(
        "--background", "-b",
        action="store_true",
        help="Run worker in background (detached mode)",
    )
    review_parser.add_argument(
        "--no-docker",
        action="store_true",
        help="Run Claude directly instead of in Docker (only used with --background or when running immediately)",
    )
    review_parser.add_argument(
        "--no-pr",
        action="store_true",
        help="Do not post review to PR even if one exists",
    )
    review_parser.add_argument(
        "--pr",
        action="store_true",
        help="Require PR to exist (error if not found)",
    )
    review_parser.add_argument(
        "--open",
        action="store_true",
        help="Open the review file in $EDITOR after the review task completes",
    )
    review_parser.add_argument(
        "--model",
        metavar="MODEL",
        help="Override the model for this task (e.g. 'claude-opus-4-5')",
    )
    review_parser.add_argument(
        "--provider",
        metavar="PROVIDER",
        help="Override the provider for this task (e.g. 'claude', 'gemini', 'codex')",
    )
    add_common_args(review_parser)

    # lineage command
    lineage_parser = subparsers.add_parser(
        "lineage", help="Show the full lineage tree for a given task"
    )
    lineage_parser.add_argument(
        "task_id",
        type=str,
        help="Full prefixed task ID to show lineage for",
    )
    add_common_args(lineage_parser)

    # show command
    show_parser = subparsers.add_parser("show", help="Show details of a specific task")
    show_parser.add_argument(
        "task_id",
        type=str,
        help="Full prefixed task ID to show",
    )
    show_parser.add_argument(
        "--full",
        action="store_true",
        default=False,
        help="Show full output without truncation",
    )
    show_parser.add_argument(
        "--prompt",
        action="store_true",
        default=False,
        help="Print only the fully built prompt text for this task and exit",
    )
    show_parser.add_argument(
        "--output",
        action="store_true",
        default=False,
        help="Print only the raw output/report content (no metadata), suitable for piping",
    )
    show_parser.add_argument(
        "--path",
        action="store_true",
        default=False,
        help="Print only the report file path",
    )
    show_parser.add_argument(
        "--page",
        action="store_true",
        default=False,
        help="Pipe output through $PAGER (default: less -R); skipped for --prompt, --output, and --path modes",
    )
    add_common_args(show_parser)

    # sync-report command
    sync_report_parser = subparsers.add_parser(
        "sync-report", help="Sync report file content from disk into DB output_content"
    )
    sync_report_parser.add_argument(
        "task_id",
        type=str,
        nargs="?",
        default=None,
        help="Full prefixed task ID to sync (optional if --all is used)",
    )
    sync_report_parser.add_argument(
        "--all",
        action="store_true",
        help="Sync all tasks that have report files on disk",
    )
    sync_report_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be synced without making changes",
    )
    add_common_args(sync_report_parser)

    # import command
    import_parser = subparsers.add_parser("import", help="Import tasks from a YAML file")
    import_parser.add_argument(
        "file",
        nargs="?",
        help="YAML file to import tasks from",
    )
    import_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be imported without creating tasks",
    )
    import_parser.add_argument(
        "--force", "-f",
        action="store_true",
        help="Skip duplicate detection and import all tasks",
    )
    add_common_args(import_parser)

    # groups command
    groups_parser = subparsers.add_parser("groups", help="Deprecated tag-management commands")
    add_common_args(groups_parser)
    groups_subparsers = groups_parser.add_subparsers(dest="groups_action")
    groups_list_parser = groups_subparsers.add_parser("list", help="List tags with task counts")
    add_common_args(groups_list_parser)
    groups_rename_parser = groups_subparsers.add_parser("rename", help="Rename a tag across all attached tasks")
    groups_rename_parser.add_argument(
        "old_group",
        help="Current group name",
    )
    groups_rename_parser.add_argument(
        "new_group",
        help="New group name",
    )
    add_common_args(groups_rename_parser)

    # group command
    group_parser = subparsers.add_parser("group", help="Deprecated alias for tasks filtered by one tag")
    group_parser.add_argument(
        "group",
        help="Group name to show tasks for",
    )
    group_parser.add_argument(
        "--view",
        choices=["flat", "lineage", "tree", "json"],
        default="flat",
        help="Presentation mode (default: flat)",
    )
    add_common_args(group_parser)

    # ps command (status is an alias for ps)
    for ps_cmd in ("ps", "status"):
        ps_parser = subparsers.add_parser(
            ps_cmd,
            help="List active workers and startup failures" if ps_cmd == "ps" else "List active workers and startup failures (alias for ps)",
        )
        ps_parser.add_argument(
            "--all", "-a",
            action="store_true",
            help="Include all completed/failed workers (not just startup failures)",
        )
        ps_parser.add_argument(
            "--quiet", "-q",
            action="store_true",
            help="Only show worker IDs",
        )
        ps_parser.add_argument(
            "--json",
            action="store_true",
            help="Output as JSON",
        )
        ps_parser.add_argument(
            "--poll",
            nargs="?",
            const=5,
            type=int,
            metavar="SECS",
            help="Refresh output every SECS seconds (default: 5 if flag given without value)",
        )
        ps_parser.add_argument(
            "--recent-minutes",
            type=int,
            default=1,
            metavar="MINUTES",
            help="In --poll mode, include terminal rows that ended within the last MINUTES (default: 1)",
        )
        add_common_args(ps_parser)

    # kill command
    kill_parser = subparsers.add_parser("kill", help="Kill a running task")
    kill_parser.add_argument(
        "task_id",
        nargs="?",
        type=str,
        help="Full prefixed task ID to kill (optional if --all is used)",
    )
    kill_parser.add_argument(
        "--all",
        action="store_true",
        help="Kill all running tasks",
    )
    kill_parser.add_argument(
        "--force", "-9",
        action="store_true",
        help="Skip SIGTERM and send SIGKILL immediately",
    )
    add_common_args(kill_parser)

    # mark-completed command
    mark_completed_parser = subparsers.add_parser(
        "mark-completed",
        help="Mark a task as completed (defaults by task type; supports --verify-git or --force)",
    )
    mark_completed_parser.add_argument(
        "task_id",
        type=str,
        help="Full prefixed task ID to mark as completed",
    )
    mark_completed_mode_group = mark_completed_parser.add_mutually_exclusive_group()
    mark_completed_mode_group.add_argument(
        "--verify-git",
        action="store_true",
        help="Validate branch/commits against git before completion",
    )
    mark_completed_mode_group.add_argument(
        "--force",
        action="store_true",
        help="Status-only completion (for non-code tasks or infrastructure recovery)",
    )
    add_common_args(mark_completed_parser)

    # set-status command
    set_status_parser = subparsers.add_parser(
        "set-status",
        help="Manually force a task's status (pending, in_progress, completed, failed, dropped)",
    )
    set_status_parser.add_argument(
        "task_id",
        type=str,
        help="Full prefixed task ID to update",
    )
    set_status_parser.add_argument(
        "status",
        choices=["pending", "in_progress", "completed", "failed", "dropped"],
        # 'unmerged' is intentionally excluded: that transition is managed
        # exclusively by the 'advance' workflow and should not be forced manually.
        help="New status for the task",
    )
    set_status_parser.add_argument(
        "--reason",
        default=None,
        help="Failure reason (only meaningful for failed status)",
    )
    set_status_parser.add_argument(
        "--execution-mode",
        choices=sorted(KNOWN_EXECUTION_MODES),
        help="Execution provenance to persist when status is in_progress",
    )
    add_common_args(set_status_parser)

    # skills-install command
    skills_install_parser = subparsers.add_parser(
        "skills-install",
        help="Install gza skills for supported agent runtimes",
    )
    _add_skills_install_args(skills_install_parser)
    add_common_args(skills_install_parser)

    # migrate command
    migrate_parser = subparsers.add_parser(
        "migrate",
        help="Run pending manual database migrations (e.g. v25/v26/v27)",
    )
    migrate_parser.add_argument(
        "--status",
        action="store_true",
        help="Show migration status without running any migrations",
    )
    migrate_parser.add_argument(
        "--yes", "-y",
        action="store_true",
        help="Skip confirmation prompt",
    )
    migrate_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what the migration would do without writing any changes",
    )
    migrate_parser.add_argument(
        "--import-local-db",
        action="store_true",
        help="Import legacy project-local .gza/gza.db into the active shared db_path",
    )
    add_common_args(migrate_parser)

    args = parser.parse_args()

    # Validate and resolve project_dir
    project_explicit = False
    if hasattr(args, "project_dir"):
        raw_project = getattr(args, "project_dir", None)
        project_explicit = raw_project is not None
        args.project_dir = Path(raw_project or ".").resolve()
        setattr(args, "project_explicit", project_explicit)
        if not args.project_dir.is_dir():
            print(f"Error: {args.project_dir} is not a directory")
            return 1
        if not project_explicit:
            try:
                args.project_dir = discover_project_dir(args.project_dir)
            except ConfigError:
                # Let the command-specific config load surface the normal error path.
                pass

    # Commands where reconciling orphaned in-progress tasks is useful.
    _RECONCILE_COMMANDS = {
        "work", "ps", "status", "kill", "advance", "retry",
        "mark-completed", "run-inline", "set-status", "history",
    }

    try:
        if args.command in _RECONCILE_COMMANDS:
            try:
                cfg = Config.load(args.project_dir, discover=not project_explicit)
            except Exception as exc:
                print(f"Warning: Skipping in-progress reconciliation: {exc}", file=sys.stderr)
            else:
                try:
                    reconcile_in_progress_tasks(cfg)
                except Exception as exc:
                    print(f"Warning: In-progress reconciliation failed: {exc}", file=sys.stderr)
                if args.command in {"ps", "status"}:
                    try:
                        prune_terminal_dead_workers(cfg)
                    except Exception as exc:
                        print(f"Warning: Worker prune failed: {exc}", file=sys.stderr)
        if args.command == "attach":
            return cmd_attach(args)
        elif args.command == "work":
            return cmd_run(args)
        elif args.command == "run-inline":
            return cmd_run_inline(args)
        elif args.command == "next":
            return cmd_next(args)
        elif args.command == "history":
            return cmd_history(args)
        elif args.command == "search":
            return cmd_search(args)
        elif args.command == "incomplete":
            return cmd_incomplete(args)
        elif args.command == "unmerged":
            return cmd_unmerged(args)
        elif args.command == "advance":
            return cmd_advance(args)
        elif args.command == "watch":
            return cmd_watch(args)
        elif args.command == "queue":
            return cmd_queue(args)
        elif args.command == "refresh":
            return cmd_refresh(args)
        elif args.command == "merge":
            return cmd_merge(args)
        elif args.command == "rebase":
            return cmd_rebase(args)
        elif args.command == "checkout":
            return cmd_checkout(args)
        elif args.command == "diff":
            return cmd_diff(args)
        elif args.command == "pr":
            return cmd_pr(args)
        elif args.command == "stats":
            return cmd_stats(args)
        elif args.command == "validate":
            return cmd_validate(args)
        elif args.command == "config":
            if getattr(args, "config_action", None) == "keys":
                return cmd_config_keys(args)
            return cmd_config(args)
        elif args.command == "clean":
            return cmd_clean(args)
        elif args.command == "init":
            return cmd_init(args)
        elif args.command == "log":
            return cmd_log(args)
        elif args.command == "tv":
            return cmd_tv(args)
        elif args.command == "add":
            return cmd_add(args)
        elif args.command == "edit":
            return cmd_edit(args)
        elif args.command == "comment":
            return cmd_comment(args)
        elif args.command == "delete":
            return cmd_delete(args)
        elif args.command == "retry":
            return cmd_retry(args)
        elif args.command == "improve":
            return cmd_improve(args)
        elif args.command == "fix":
            return cmd_fix(args)
        elif args.command in ("iterate", "cycle"):
            return cmd_iterate(args)
        elif args.command == "implement":
            return cmd_implement(args)
        elif args.command == "extract":
            return cmd_extract(args)
        elif args.command == "review":
            return cmd_review(args)
        elif args.command == "resume":
            return cmd_resume(args)
        elif args.command == "lineage":
            return cmd_lineage(args)
        elif args.command == "show":
            return cmd_show(args)
        elif args.command == "sync-report":
            return cmd_sync_report(args)
        elif args.command == "import":
            return cmd_import(args)
        elif args.command == "groups":
            if getattr(args, "groups_action", None) == "rename":
                return cmd_group_rename(args)
            return cmd_groups(args)
        elif args.command == "group":
            return cmd_status(args)
        elif args.command in ("ps", "status"):
            return cmd_ps(args)
        elif args.command == "kill":
            return cmd_kill(args)
        elif args.command == "mark-completed":
            return cmd_mark_completed(args)
        elif args.command == "set-status":
            return cmd_set_status(args)
        elif args.command == "learnings":
            return cmd_learnings(args)
        elif args.command == "skills-install":
            return cmd_skills_install(args, default_targets=["all"])
        elif args.command == "migrate":
            return _cmd_migrate(args)
    except ManualMigrationRequired as e:
        print(f"Error: {e}", file=sys.stderr)
        print("Run 'gza migrate' to upgrade the database.", file=sys.stderr)
        return 1
    except ConfigError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except InvalidTaskIdError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except SchemaIntegrityError as e:
        print(f"Error: {e}", file=sys.stderr)
        print(
            "Run 'gza migrate' with a writable database (or restore schema artifacts), then retry.",
            file=sys.stderr,
        )
        return 1

    return 0


def _cmd_migrate(args: "argparse.Namespace") -> int:
    """Handle the 'migrate' subcommand."""
    try:
        config = Config.load(args.project_dir, discover=not bool(getattr(args, "project_explicit", False)))
    except ConfigError as e:
        print(f"Error loading config: {e}", file=sys.stderr)
        return 1

    status = check_migration_status(config.db_path)

    if args.import_local_db:
        if not args.yes and not args.dry_run:
            answer = input(
                "Import legacy local DB into active shared DB now? [y/N]: "
            ).strip().lower()
            if answer not in {"y", "yes"}:
                print("Import cancelled.")
                return 1

        project_id_source = config.source_map.get("project_id", "")
        if project_id_source == "derived":
            if args.dry_run:
                print(
                    "Dry-run: would persist missing project_id in gza.yaml "
                    f"as '{config.project_id}' before import."
                )
            else:
                try:
                    updated = persist_project_id_if_missing(config.project_dir, config.project_id)
                except (ConfigError, OSError) as exc:
                    print(f"Error: unable to persist project_id in gza.yaml: {exc}", file=sys.stderr)
                    return 1
                if updated:
                    print(f"Persisted project_id '{config.project_id}' to {config.config_path(config.project_dir)}")
                    try:
                        config = Config.load(
                            args.project_dir,
                            discover=not bool(getattr(args, "project_explicit", False)),
                        )
                    except ConfigError as e:
                        print(f"Error loading config after persisting project_id: {e}", file=sys.stderr)
                        return 1
        try:
            result = import_legacy_local_db(config, dry_run=args.dry_run)
        except ValueError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 1
        status_text = result.get("status", "unknown")
        if status_text == "no_local_db":
            print(f"No legacy local DB found at {result['local_db_path']}.")
            return 0
        if status_text == "already_imported":
            print("Legacy local DB already imported for this shared DB (idempotent no-op).")
            return 0
        if status_text == "dry_run":
            print("Dry-run: legacy local DB import preview")
            print(f"  local_db: {result['local_db_path']}")
            print(f"  shared_db: {result['shared_db_path']}")
            print(f"  project_id: {result['project_id']}")
            print(f"  local tasks: {result['local_task_count']}")
            print(f"  shared existing tasks: {result['shared_existing_task_count']}")
            return 0
        if status_text == "imported":
            print("Imported legacy local DB into shared DB.")
            print(f"  local_db: {result['local_db_path']}")
            print(f"  shared_db: {result['shared_db_path']}")
            print(f"  project_id: {result['project_id']}")
            print(f"  tasks_imported: {result['tasks_imported']}")
            return 0
        print("Error: unexpected import result", file=sys.stderr)
        return 1

    if args.status:
        current = status["current_version"]
        target = status["target_version"]
        pending_auto = status["pending_auto"]
        pending_manual = status["pending_manual"]
        print(f"Schema version: {current} / {target}")
        if not pending_auto and not pending_manual:
            print("Database is up-to-date.")
        else:
            if pending_auto:
                print(f"Pending auto migrations: {', '.join(f'v{v}' for v in pending_auto)}")
            if pending_manual:
                print(f"Pending manual migrations: {', '.join(f'v{v}' for v in pending_manual)}")
        return 0

    pending_manual = status["pending_manual"]
    if not pending_manual:
        print("No pending manual migrations.")
        return 0

    if args.dry_run:
        versions_str = ", ".join(f"v{v}" for v in pending_manual)
        print(f"Dry-run: would apply migration(s): {versions_str}")
        print(f"Database: {config.db_path}")
        v25_task_count_cache: int | None = None
        v25_samples_cache: list[tuple[int, str]] = []
        v25_random_samples_cache: list[tuple[int, str]] = []
        for version in pending_manual:
            if version == 25:
                preview_v25_data = preview_v25_migration(config.db_path, config.project_prefix)
                v25_task_count_cache = preview_v25_data["task_count"]
                v25_samples_cache = preview_v25_data["samples"]
                v25_random_samples_cache = preview_v25_data["random_samples"]
                print("\nMigration v25 preview (INTEGER PK → TEXT base36 IDs):")
                print(f"  Tasks to convert: {preview_v25_data['task_count']}")
                # Right-align old IDs so the → arrow lines up in both sample
                # sections — width is computed across first + random so both
                # blocks share the same column alignment.
                old_ids_v25 = [old for old, _ in preview_v25_data["samples"]] + [
                    old for old, _ in preview_v25_data["random_samples"]
                ]
                id_width = max((len(str(old)) for old in old_ids_v25), default=0)
                if preview_v25_data["samples"]:
                    print(f"  Sample ID conversions (first {len(preview_v25_data['samples'])}):")
                    for old_v25_id, new_id in preview_v25_data["samples"]:
                        print(f"    #{old_v25_id:>{id_width}} → {new_id}")
                if preview_v25_data["random_samples"]:
                    print(f"  Sample ID conversions (random {len(preview_v25_data['random_samples'])}):")
                    for old_v25_id, new_id in preview_v25_data["random_samples"]:
                        print(f"    #{old_v25_id:>{id_width}} → {new_id}")
                print(f"  First post-migration task ID: {preview_v25_data['first_post_migration_id']}")
            elif version == 26:
                preview_v26_data = preview_v26_migration(config.db_path)
                task_count_v26: int = preview_v26_data["task_count"]
                samples_v26: list[tuple[str, str]] = list(preview_v26_data["samples"])
                random_samples_v26: list[tuple[str, str]] = list(preview_v26_data["random_samples"])
                if (
                    not samples_v26
                    and not random_samples_v26
                    and 25 in pending_manual
                    and v25_task_count_cache is not None
                ):
                    # DB is pre-v25; synthesize v26 preview from v25 preview rows.
                    task_count_v26 = v25_task_count_cache
                    samples_v26 = [
                        (old_v25, f"{config.project_prefix}-{old_int}")
                        for old_int, old_v25 in v25_samples_cache
                    ]
                    random_samples_v26 = [
                        (old_v25, f"{config.project_prefix}-{old_int}")
                        for old_int, old_v25 in v25_random_samples_cache
                    ]
                print("\nMigration v26 preview (TEXT base36 IDs → TEXT decimal IDs):")
                print(f"  Tasks to convert: {task_count_v26}")
                old_ids_v26 = [old for old, _ in samples_v26] + [
                    old for old, _ in random_samples_v26
                ]
                id_width = max((len(old) for old in old_ids_v26), default=0)
                if samples_v26:
                    print(f"  Sample ID conversions (first {len(samples_v26)}):")
                    for old_v26_id, new_id in samples_v26:
                        print(f"    {old_v26_id:>{id_width}} → {new_id}")
                if random_samples_v26:
                    print(f"  Sample ID conversions (random {len(random_samples_v26)}):")
                    for old_v26_id, new_id in random_samples_v26:
                        print(f"    {old_v26_id:>{id_width}} → {new_id}")
            elif version == 27:
                print("\nMigration v27 preview (drop TaskCycle bookkeeping tables/columns):")
                print("  - Drop task_cycles and task_cycle_iterations tables")
                print("  - Rebuild tasks table without cycle_id/cycle_iteration_index/cycle_role")
        return 0

    versions_str = ", ".join(f"v{v}" for v in pending_manual)
    if not args.yes:
        print(f"This will run manual migration(s): {versions_str}")
        print(f"Database: {config.db_path}")
        answer = input("Continue? [y/N] ").strip().lower()
        if answer not in ("y", "yes"):
            print("Aborted.")
            return 0

    # Trigger auto-migrations (up to v24) before applying manual ones.
    # If the DB is at an older auto-migratable version, SqliteTaskStore.__init__
    # will run the auto-migrations and then raise ManualMigrationRequired.
    # We swallow that exception here since we are about to run the manual migration.
    try:
        SqliteTaskStore.from_config(config)
    except ManualMigrationRequired:
        pass  # Expected — auto-migrations ran, now proceed with the manual migration

    for version in pending_manual:
        if version == 25:
            print("Running migration v25 (INTEGER PK → TEXT base36 IDs)...")
            try:
                run_v25_migration(config.db_path, config.project_prefix)
                backup_path = config.db_path.with_suffix(".backup.pre-v25.db")
                print(f"Migration v25 complete. Backup at: {backup_path}")
            except Exception as e:
                print(f"Migration v25 failed: {e}", file=sys.stderr)
                return 1
        elif version == 26:
            print("Running migration v26 (TEXT base36 IDs → TEXT decimal IDs)...")
            try:
                run_v26_migration(config.db_path)
                backup_path = config.db_path.with_suffix(".backup.pre-v26.db")
                print(f"Migration v26 complete. Backup at: {backup_path}")
            except Exception as e:
                print(f"Migration v26 failed: {e}", file=sys.stderr)
                return 1
        elif version == 27:
            print("Running migration v27 (drop TaskCycle bookkeeping)...")
            try:
                run_v27_migration(config.db_path)
                backup_path = config.db_path.with_suffix(".backup.pre-v27.db")
                print(f"Migration v27 complete. Backup at: {backup_path}")
            except Exception as e:
                print(f"Migration v27 failed: {e}", file=sys.stderr)
                return 1
        else:
            print(f"Unknown manual migration v{version}", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
