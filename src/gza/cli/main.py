"""Parser setup, subparser definitions, dispatch, and main() entry point."""

import argparse
import sys
from pathlib import Path

from ..config import ConfigError
from ..learnings import DEFAULT_LEARNINGS_WINDOW

from ._common import (
    SortingHelpFormatter,
    _add_skills_install_args,
    add_common_args,
    _add_query_filter_args,
    prune_terminal_dead_workers,
    reconcile_in_progress_tasks,
)
from ..config import Config
from .config_cmds import (
    cmd_clean,
    cmd_cleanup,
    cmd_config,
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
    cmd_edit,
    cmd_implement,
    cmd_improve,
    cmd_iterate,
    cmd_mark_completed,
    cmd_resume,
    cmd_retry,
    cmd_review,
    cmd_run,
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
    cmd_delete,
    cmd_groups,
    cmd_history,
    cmd_lineage,
    cmd_next,
    cmd_ps,
    cmd_show,
    cmd_status,
    cmd_stop,
    cmd_unmerged,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Gza - AI agent task runner",
        formatter_class=SortingHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # work command
    work_parser = subparsers.add_parser("work", help="Run the next pending task or specific tasks")
    work_parser.add_argument(
        "task_ids",
        nargs="*",
        type=int,
        help="Specific task ID(s) to run (optional, can specify multiple)",
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

    # next command
    next_parser = subparsers.add_parser("next", help="List upcoming pending tasks")
    add_common_args(next_parser)
    next_parser.add_argument(
        "--all",
        action="store_true",
        help="Show all pending tasks including blocked ones",
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

    # advance command
    advance_parser = subparsers.add_parser(
        "advance",
        help="Intelligently progress unmerged tasks through their lifecycle",
    )
    add_common_args(advance_parser)
    advance_parser.add_argument(
        "task_id",
        type=int,
        nargs="?",
        metavar="task_id",
        help="Specific task ID to advance (omit to advance all eligible tasks)",
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
        help="Skip auto-resume of failed tasks (do not create resume tasks for MAX_STEPS/MAX_TURNS failures)",
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

    # refresh command
    refresh_parser = subparsers.add_parser("refresh", help="Refresh cached diff stats for unmerged tasks")
    add_common_args(refresh_parser)
    refresh_group = refresh_parser.add_mutually_exclusive_group()
    refresh_group.add_argument(
        "task_id",
        type=int,
        nargs="?",
        metavar="task_id",
        help="Task ID to refresh (omit to refresh all unmerged tasks)",
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
        type=int,
        nargs="*",
        metavar="task_id",
        help="Task ID(s) to merge",
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
        type=int,
        help="Task ID to rebase",
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
        help="Task ID or branch name to checkout",
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
        type=int,
        help="Task ID to create PR from",
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
    stats_parser = subparsers.add_parser("stats", help="Show cost and usage statistics")
    add_common_args(stats_parser)
    _add_query_filter_args(stats_parser)
    stats_parser.add_argument(
        "--all",
        action="store_true",
        help="Show all tasks (no limit)",
    )
    stats_parser.set_defaults(last=5)
    stats_parser.add_argument(
        "--cycles",
        action="store_true",
        help="Show cycle analytics (review/improve iteration statistics)",
    )
    stats_parser.add_argument(
        "--task",
        type=int,
        dest="cycle_task_id",
        metavar="ID",
        help="Show cycle analytics for a specific implementation task (use with --cycles)",
    )
    stats_parser.add_argument(
        "--json",
        action="store_true",
        help="Output as machine-readable JSON",
    )

    # validate command
    validate_parser = subparsers.add_parser("validate", help="Validate gza.yaml configuration")
    add_common_args(validate_parser)

    # config command
    config_parser = subparsers.add_parser("config", help="Show effective config with source attribution")
    config_parser.add_argument(
        "--json",
        action="store_true",
        help="Output effective config and source attribution as JSON",
    )
    add_common_args(config_parser)

    # cleanup command
    cleanup_parser = subparsers.add_parser("cleanup", help="Clean up stale worktrees, old logs, and worker metadata")
    cleanup_parser.add_argument(
        "--worktrees",
        action="store_true",
        help="Only clean up stale worktrees",
    )
    cleanup_parser.add_argument(
        "--logs",
        action="store_true",
        help="Only clean up old log files",
    )
    cleanup_parser.add_argument(
        "--workers",
        action="store_true",
        help="Only clean up stale worker metadata",
    )
    cleanup_parser.add_argument(
        "--days",
        type=int,
        default=None,
        metavar="N",
        help="Remove worktrees/logs older than N days (default: from config cleanup_days, or 30)",
    )
    cleanup_parser.add_argument(
        "--keep-unmerged",
        action="store_true",
        help="Keep logs for tasks that are still unmerged",
    )
    cleanup_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be cleaned without actually doing it",
    )
    cleanup_parser.add_argument(
        "--force",
        action="store_true",
        help="Skip confirmation prompt before removing worktrees",
    )
    add_common_args(cleanup_parser)

    # clean command
    clean_parser = subparsers.add_parser("clean", help="Archive or delete old log and worker files")
    clean_parser.add_argument(
        "--days",
        type=int,
        default=30,
        metavar="N",
        help="Archive files older than N days (default: 30), or delete archived files if --purge (default: 365)",
    )
    clean_parser.add_argument(
        "--purge",
        action="store_true",
        help="Delete archived files instead of archiving (requires --days, default: 365)",
    )
    clean_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be archived/deleted without actually doing it",
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
        help="Task ID (numeric), slug, or worker ID",
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
        help="Follow log in real-time when the resolved task run is actively running",
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
    add_common_args(log_parser)

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
        help="Set task group",
    )
    add_parser.add_argument(
        "--based-on",
        type=int,
        metavar="ID",
        help="Base this task on a previous task's output (sets depends_on field)",
    )
    add_parser.add_argument(
        "--depends-on",
        type=int,
        metavar="ID",
        help="Set dependency on another task",
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
        type=int,
        help="Task ID to edit",
    )
    edit_parser.add_argument(
        "--group",
        dest="group_flag",
        metavar="NAME",
        help="Move task to group (use empty string \"\" to remove from group)",
    )
    edit_parser.add_argument(
        "--based-on",
        dest="based_on_flag",
        type=int,
        metavar="ID",
        help="Set dependency on another task",
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
        type=int,
        help="Task ID to delete",
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
        type=int,
        help="Task ID to retry",
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
    add_common_args(retry_parser)

    # resume command
    resume_parser = subparsers.add_parser("resume", help="Resume a failed task from where it left off")
    resume_parser.add_argument(
        "task_id",
        type=int,
        help="Task ID to resume",
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
    add_common_args(resume_parser)

    # improve command
    improve_parser = subparsers.add_parser("improve", help="Create an improve task based on implementation and review")
    improve_parser.add_argument(
        "impl_task_id",
        type=int,
        help="Implementation task ID to improve",
    )
    improve_parser.add_argument(
        "--review",
        action="store_true",
        help="Auto-create review task on completion",
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
    add_common_args(improve_parser)

    # iterate command (formerly "cycle"; "cycle" kept as a hidden alias for backward compatibility)
    cycle_parser = subparsers.add_parser(
        "iterate",
        aliases=["cycle"],
        help="Run an automated review/improve cycle for an implementation task",
    )
    cycle_parser.add_argument(
        "impl_task_id",
        type=int,
        help="Implementation task ID to cycle",
    )
    cycle_parser.add_argument(
        "--max-iterations",
        type=int,
        default=3,
        dest="max_iterations",
        metavar="N",
        help="Maximum review/improve iterations (default: 3)",
    )
    cycle_parser.add_argument(
        "--dry-run",
        action="store_true",
        dest="dry_run",
        help="Preview what would happen without executing",
    )
    cycle_parser.add_argument(
        "--continue",
        action="store_true",
        dest="continue_cycle",
        help="Resume an existing active cycle instead of starting a new one",
    )
    cycle_parser.add_argument(
        "--no-docker",
        action="store_true",
        help="Run Claude directly instead of in Docker",
    )
    # TODO: Phase 1 deferred — add --queue (create chain without running immediately)
    # and --background flags as specified in the design plan.
    add_common_args(cycle_parser)

    # implement command
    implement_parser = subparsers.add_parser(
        "implement",
        help="Create an implementation task from a completed plan task",
    )
    implement_parser.add_argument(
        "plan_task_id",
        type=int,
        help="Completed plan task ID to implement",
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
        help="Set task group",
    )
    implement_parser.add_argument(
        "--depends-on",
        type=int,
        metavar="ID",
        help="Set dependency on another task",
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
    add_common_args(implement_parser)

    # review command
    review_parser = subparsers.add_parser(
        "review",
        help="Create and optionally run a review task for an implementation or improve task",
    )
    review_parser.add_argument(
        "task_id",
        type=int,
        help="Implementation or improve task ID to review",
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
        type=int,
        help="Task ID to show lineage for",
    )
    add_common_args(lineage_parser)

    # show command
    show_parser = subparsers.add_parser("show", help="Show details of a specific task")
    show_parser.add_argument(
        "task_id",
        type=int,
        help="Task ID to show",
    )
    show_parser.add_argument(
        "--full",
        action="store_true",
        default=False,
        help="Show full output without truncation",
    )
    add_common_args(show_parser)

    # sync-report command
    sync_report_parser = subparsers.add_parser(
        "sync-report", help="Sync report file content from disk into DB output_content"
    )
    sync_report_parser.add_argument(
        "task_id",
        type=int,
        nargs="?",
        default=None,
        help="Task ID to sync (optional if --all is used)",
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
    groups_parser = subparsers.add_parser("groups", help="List all groups with task counts")
    add_common_args(groups_parser)

    # group command
    group_parser = subparsers.add_parser("group", help="Show tasks in a group")
    group_parser.add_argument(
        "group",
        help="Group name to show tasks for",
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
        add_common_args(ps_parser)

    # stop command
    stop_parser = subparsers.add_parser("stop", help="Stop a running worker")
    stop_parser.add_argument(
        "worker_id",
        nargs="?",
        help="Worker ID to stop (optional if --all is used)",
    )
    stop_parser.add_argument(
        "--all",
        action="store_true",
        help="Stop all running workers",
    )
    stop_parser.add_argument(
        "--force",
        action="store_true",
        help="Force kill (SIGKILL instead of SIGTERM)",
    )
    add_common_args(stop_parser)

    # mark-completed command
    mark_completed_parser = subparsers.add_parser(
        "mark-completed",
        help="Mark a task as completed (defaults by task type; supports --verify-git or --force)",
    )
    mark_completed_parser.add_argument(
        "task_id",
        type=int,
        help="Task ID to mark as completed",
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
        type=int,
        help="Task ID to update",
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
    add_common_args(set_status_parser)

    # skills-install command
    skills_install_parser = subparsers.add_parser(
        "skills-install",
        help="Install gza skills for supported agent runtimes",
    )
    _add_skills_install_args(skills_install_parser)
    add_common_args(skills_install_parser)

    args = parser.parse_args()

    # Validate and resolve project_dir
    if hasattr(args, 'project_dir'):
        args.project_dir = Path(args.project_dir).resolve()
        if not args.project_dir.is_dir():
            print(f"Error: {args.project_dir} is not a directory")
            return 1

    # Commands where reconciling orphaned in-progress tasks is useful.
    _RECONCILE_COMMANDS = {
        "work", "ps", "status", "stop", "advance", "retry",
        "mark-completed", "set-status", "history",
    }

    try:
        if args.command in _RECONCILE_COMMANDS:
            try:
                cfg = Config.load(args.project_dir)
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
        if args.command == "work":
            return cmd_run(args)
        elif args.command == "next":
            return cmd_next(args)
        elif args.command == "history":
            return cmd_history(args)
        elif args.command == "unmerged":
            return cmd_unmerged(args)
        elif args.command == "advance":
            return cmd_advance(args)
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
            return cmd_config(args)
        elif args.command == "cleanup":
            return cmd_cleanup(args)
        elif args.command == "clean":
            return cmd_clean(args)
        elif args.command == "init":
            return cmd_init(args)
        elif args.command == "log":
            return cmd_log(args)
        elif args.command == "add":
            return cmd_add(args)
        elif args.command == "edit":
            return cmd_edit(args)
        elif args.command == "delete":
            return cmd_delete(args)
        elif args.command == "retry":
            return cmd_retry(args)
        elif args.command == "improve":
            return cmd_improve(args)
        elif args.command in ("iterate", "cycle"):
            return cmd_iterate(args)
        elif args.command == "implement":
            return cmd_implement(args)
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
            return cmd_groups(args)
        elif args.command == "group":
            return cmd_status(args)
        elif args.command in ("ps", "status"):
            return cmd_ps(args)
        elif args.command == "stop":
            return cmd_stop(args)
        elif args.command == "mark-completed":
            return cmd_mark_completed(args)
        elif args.command == "set-status":
            return cmd_set_status(args)
        elif args.command == "learnings":
            return cmd_learnings(args)
        elif args.command == "skills-install":
            return cmd_skills_install(args, default_targets=["all"])
    except ConfigError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
