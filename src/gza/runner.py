"""Main Gza runner orchestration."""

import json
import logging
import os
import re
import shutil
import sqlite3
import tomllib
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import gza.colors as _colors

from .branch_naming import generate_branch_name
from .commit_messages import build_task_commit_message
from .config import (
    APP_NAME,
    DEFAULT_REVIEW_CONTEXT_FILE_LIMIT,
    DEFAULT_REVIEW_DIFF_MEDIUM_THRESHOLD,
    DEFAULT_REVIEW_DIFF_SMALL_THRESHOLD,
    BranchStrategy,
    Config,
)
from .console import (
    console,
    error_message,
    task_footer,
    task_header,
)
from .db import SqliteTaskStore, Task, TaskStats, extract_failure_reason, task_id_numeric_key
from .git import Git, GitError, cleanup_worktree_for_branch, parse_diff_numstat
from .github import GitHub, GitHubError
from .learnings import maybe_auto_regenerate_learnings
from .prompts import PromptBuilder
from .providers import Provider, RunResult, get_provider
from .review_tasks import DuplicateReviewError, create_review_task
from .review_verdict import parse_review_verdict
from .task_slug import extract_task_id_suffix, get_base_task_slug

logger = logging.getLogger(__name__)

__all__ = [
    "run",
    "build_prompt",
    "write_log_entry",
    "extract_content_from_log",
    "get_effective_config_for_task",
    "post_review_to_pr",
]


def write_log_entry(log_file: "Path", entry: dict) -> None:
    """Append a JSONL entry to the task log file."""
    try:
        with open(log_file, "a") as f:
            f.write(json.dumps(entry) + "\n")
            f.flush()
    except Exception:
        logger.warning("Failed to write log entry to %s", log_file, exc_info=True)


def write_worker_start_event(log_file: "Path", *, resumed: bool) -> None:
    """Write a worker start lifecycle event when running under worker mode."""
    if os.environ.get("GZA_WORKER_MODE") != "1":
        return
    worker_id = os.environ.get("GZA_WORKER_ID")
    if not worker_id:
        return
    mode = "pipe mode, resumed" if resumed else "pipe mode"
    write_log_entry(
        log_file,
        {
            "type": "gza",
            "subtype": "worker_lifecycle",
            "event": "start",
            "worker_id": worker_id,
            "message": f"Worker {worker_id} started ({mode})",
        },
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
    1. Task-specific model (task.model)
    2. Provider-aware config resolution (Config.get_model_for_task)

    Priority order for max_steps selection:
    1. Provider-aware config resolution (Config.get_max_steps_for_task)

    Args:
        task: The task to get config for
        config: The base configuration

    Returns:
        Tuple of (model, provider, max_steps) where model can be None
    """
    provider_override = task.provider if task.provider_is_explicit and task.provider else None
    provider = provider_override if provider_override else config.get_provider_for_task(task.task_type)
    model = task.model if task.model else config.get_model_for_task(task.task_type, provider)
    max_steps = config.get_max_steps_for_task(task.task_type, provider)
    return model, provider, max_steps


DEFAULT_REPORT_DIR = f".{APP_NAME}/explorations"
PLAN_DIR = f".{APP_NAME}/plans"
REVIEW_DIR = f".{APP_NAME}/reviews"
INTERNAL_DIR = f".{APP_NAME}/internal"
SUMMARY_DIR = f".{APP_NAME}/summaries"
WIP_DIR = f".{APP_NAME}/wip"
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

    if task.task_type in ("task", "implement", "improve", "rebase"):
        summary_path = project_dir / SUMMARY_DIR / f"{task.slug}.md"
    elif task.task_type == "explore":
        report_path = project_dir / DEFAULT_REPORT_DIR / f"{task.slug}.md"
    elif task.task_type == "plan":
        report_path = project_dir / PLAN_DIR / f"{task.slug}.md"
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
REVIEW_IMPROVE_LINEAGE_LIMIT = 4
REVIEW_IMPROVE_SUMMARY_MAX_CHARS = 320
COMMIT_SUBJECT_MAX_CHARS = 72


def _extract_review_verdict(content: str | None) -> str | None:
    """Backward-compatible wrapper around the shared verdict parser."""
    return parse_review_verdict(content)


def backup_database(db_path: Path, project_dir: Path) -> None:
    """Create an hourly backup of the SQLite database if one doesn't exist yet.

    Checks if a backup for the current hour already exists. If not, creates
    a timestamped backup using SQLite's backup API (safe for concurrent access).

    Backup filename format: gza-YYYYMMDDHH.db (e.g., gza-2026021414.db)

    Args:
        db_path: Path to the source SQLite database
        project_dir: Project directory (used to locate the backups folder)
    """
    if not db_path.exists():
        return

    backup_dir = project_dir / BACKUP_DIR
    hour_stamp = datetime.now().strftime("%Y%m%d%H")
    backup_path = backup_dir / f"gza-{hour_stamp}.db"

    if backup_path.exists():
        return

    backup_dir.mkdir(parents=True, exist_ok=True)

    source = sqlite3.connect(str(db_path))
    try:
        dest = sqlite3.connect(str(backup_path))
        try:
            source.backup(dest)
        finally:
            dest.close()
    finally:
        source.close()


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
    if not _slug_exists(base_id, log_path, git, project_name, prompt, branch_strategy, explicit_type, project_prefix):
        return base_id

    # Find next available suffix
    suffix = 2
    new_id = f"{base_id}-{suffix}"
    while _slug_exists(new_id, log_path, git, project_name, prompt, branch_strategy, explicit_type, project_prefix):
        suffix += 1
        new_id = f"{base_id}-{suffix}"
    return new_id



def _compute_slug_override(task: "Task", store: "SqliteTaskStore") -> str | None:
    """Compute a slug_override for review/implement/improve tasks.

    Uses ``{task_id_suffix}-{type_prefix}-{target_slug}`` where target is the
    direct parent this task operates on:
    - review -> depends_on
    - improve -> based_on
    - implement -> based_on (fallback: depends_on)

    Returns None for other task types (slug is derived from prompt as usual).
    """
    prefix_map = {
        "review": "rev",
        "implement": "impl",
        "improve": "impr",
    }
    prefix = prefix_map.get(task.task_type)
    if prefix is None:
        return None

    if task.task_type == "review":
        anchor_id = task.depends_on
    elif task.task_type == "improve":
        anchor_id = task.based_on
    else:  # implement
        anchor_id = task.based_on or task.depends_on

    anchor_task = None
    if anchor_id is not None:
        anchor_task = store.get(anchor_id)
        if anchor_task is None:
            logger.warning(
                "Slug override anchor task missing for task #%s (%s): anchor_id=%s; "
                "falling back to the child task prompt",
                task.id,
                task.task_type,
                anchor_id,
            )

    # get_base_task_slug strips the YYYYMMDD date prefix and any trailing
    # "-N" revision suffix from the anchor task's slug. The returned override
    # has no date prefix; generate_slug re-prepends today's date later, so
    # passing a bare override keeps the final slug from gaining a double date.
    target_slug = (
        get_base_task_slug(anchor_task.slug)
        if anchor_task and anchor_task.slug
        else slugify(anchor_task.prompt if anchor_task else task.prompt)
    )
    task_id_suffix = extract_task_id_suffix(task.id)

    return "-".join(part for part in (task_id_suffix, prefix, target_slug) if part)


def _slug_exists(
    task_id: str,
    log_path: Path | None,
    git: Git | None,
    project_name: str | None,
    prompt: str = "",
    branch_strategy: "BranchStrategy | None" = None,
    explicit_type: str | None = None,
    project_prefix: str | None = None,
) -> bool:
    """Check if a slug is already in use (log file or branch exists)."""
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


def build_prompt(task: Task, config: Config, store: SqliteTaskStore, report_path: Path | None = None, summary_path: Path | None = None, git: Git | None = None) -> str:
    """Build the prompt for Claude."""
    return PromptBuilder().build(task, config, store, report_path=report_path, summary_path=summary_path, git=git)


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
    if task.slug and task.task_type in {"task", "implement", "improve"}:
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


def _default_code_task_commit_subject(task_slug: str | None, task_db_id: str | None) -> str:
    """Build deterministic fallback commit subject for code tasks."""
    if task_slug and task_slug.strip():
        return f"gza task {task_slug.strip()}"
    if task_db_id is not None:
        return f"Task {task_db_id}"
    return "gza task"


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
    n_cycles = len(prior_improves)

    # Build lineage chain (oldest first for readability)
    chronological = sorted(
        prior_improves,
        key=lambda t: (t.created_at or datetime.min.replace(tzinfo=UTC), task_id_numeric_key(t.id)),
    )
    chain_parts = []
    for improve in chronological:
        review_ref = f"Review {improve.depends_on}" if improve.depends_on else "Review ?"
        chain_parts.append(f"{review_ref} → Improve {improve.id}")
    lineage_chain = " → ".join(chain_parts)

    cycle_note = (
        f"This implementation has been through {n_cycles} prior review/improve cycle(s)"
        + (
            f" (showing {len(included)} most recent, {omitted_count} older omitted)"
            if omitted_count
            else ""
        )
        + ". Use `uv run gza show <id>` to inspect prior review findings or improve task prompts."
        " Use `cat <report_file>` to read full review reports."
    )

    lines = [
        "## Improve Lineage Context",
        "",
        cycle_note,
        f"Lineage: {lineage_chain}",
        "",
    ]

    for improve in included:
        review_ref = f"review {improve.depends_on}" if improve.depends_on else "review ?"
        content = _get_task_output(improve, project_dir)
        summary = _compact_output_summary(content) if content else ""
        if not summary:
            summary = "No summary content available."
        lines.append(f"- Improve {improve.id} ({review_ref}): {summary}")

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

    parts = [
        "## Implementation Diff Context",
        "",
        f"Implementation branch: {branch_name}",
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
) -> str:
    """Build context by walking the depends_on and based_on chain."""
    context_parts = []

    def _int_or_default(value: object, default: int) -> int:
        return value if isinstance(value, int) else default

    # For improve tasks, include review feedback and original plan
    if task.task_type == "improve":
        # Get the review we're addressing
        if task.depends_on:
            review_task = store.get(task.depends_on)
            if review_task and review_task.task_type == "review":
                review_content = _get_task_output(review_task, project_dir)
                if review_content:
                    context_parts.append("## Review feedback to address:\n")
                    context_parts.append(review_content)
                else:
                    context_parts.append(
                        "## Review feedback to address:\n"
                        f"(review task {review_task.id} exists but content unavailable on this machine - flag as blocker)"
                    )

        # Get the original plan (via based_on chain)
        if task.based_on:
            impl_task = store.get(task.based_on)
            if impl_task and impl_task.based_on:
                plan_task = _find_task_of_type_in_chain(impl_task.based_on, "plan", store)
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

    # For implement tasks, include plan from based_on chain
    if task.task_type == "implement" and task.based_on:
        plan_task = _find_task_of_type_in_chain(task.based_on, "plan", store)
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
                plan_task = None
                if impl_task.based_on:
                    plan_task = _find_task_of_type_in_chain(impl_task.based_on, "plan", store)

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
                elif impl_task.prompt:
                    context_parts.append("\n## Original request:\n")
                    context_parts.append(impl_task.prompt)

                # Get diff if we have a branch (tiered strategy based on diff size)
                if impl_task.branch and git:
                    try:
                        default_branch = git.default_branch()
                        revision_range = f"{default_branch}...{impl_task.branch}"
                        context_parts.append(
                            _build_review_diff_context(
                                git,
                                revision_range,
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


def _find_task_of_type_in_chain(task_id: str, task_type: str, store: SqliteTaskStore, visited: set[str] | None = None) -> Task | None:
    """Walk lineage links to find a task of the given type."""
    if visited is None:
        visited = set()
    stack = [task_id]

    while stack:
        current_id = stack.pop()
        if current_id in visited:
            continue  # Avoid cycles
        visited.add(current_id)

        task = store.get(current_id)
        if not task:
            continue

        if task.task_type == task_type:
            return task

        # Transitional behavior: support historical plan links on either edge
        # until all plan discovery callers/records are normalized.
        if task.based_on:
            stack.append(task.based_on)
        if task.depends_on:
            stack.append(task.depends_on)

    return None


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


def _save_wip_changes(
    task: Task,
    worktree_git: Git,
    config: Config,
    branch_name: str,
) -> None:
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
        return

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
    if task.slug and diff:
        wip_file = wip_dir / f"{task.slug}.diff"
        wip_file.write_text(diff)
        console.print(f"[yellow]Saved WIP diff to: {wip_file.relative_to(config.project_dir)}[/yellow]")

    # Commit changes with --no-verify
    try:
        worktree_git._run("commit", "--no-verify", "-m", f"WIP: gza task interrupted\n\nTask ID: {task.slug}")
        console.print(f"[yellow]Saved WIP commit on branch: {branch_name}[/yellow]")
    except GitError as e:
        # If commit fails, that's okay - we have the diff backup
        console.print(f"[yellow]Warning: Could not create WIP commit: {e}[/yellow]")


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

    # Check if the last commit is a WIP commit
    try:
        last_commit_msg = worktree_git._run("log", "-1", "--pretty=%B", check=False).stdout.strip()
        if last_commit_msg.startswith("WIP: gza task interrupted"):
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
    gh = GitHub()

    # Check gh is available
    if not gh.is_available():
        if required:
            print("Error: GitHub CLI not available, cannot post review")
            return
        else:
            print("Info: GitHub CLI not available, skipping PR comment")
            return

    # Find PR number
    pr_number = None

    # Try cached pr_number first
    if impl_task.pr_number:
        pr_number = impl_task.pr_number
        print(f"Found PR #{pr_number} (cached)")
    elif impl_task.branch:
        # Try to discover PR via branch
        pr_number = gh.get_pr_number(impl_task.branch)
        if pr_number:
            print(f"Found PR #{pr_number} for branch {impl_task.branch}")
            # Cache it for future use
            impl_task.pr_number = pr_number
            store.update(impl_task)

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


def _create_and_run_review_task(completed_task: Task, config: Config, store: SqliteTaskStore) -> int:
    """Create and immediately execute a review task for a completed implementation.

    Returns:
        Exit code from running the review task.
    """
    try:
        review_task = create_review_task(
            store, completed_task, prompt_mode="auto",
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


def _create_local_dep_symlinks(config: Config, worktree_path: Path) -> None:
    """Create symlinks for local path dependencies so uv can resolve them in worktrees.

    Parses [tool.uv.sources] from the project's pyproject.toml and creates
    symlinks in the worktree's ancestor directories so that relative path
    references resolve to the same real directories as they would from the
    original project root.
    """
    pyproject = config.project_dir / "pyproject.toml"
    if not pyproject.exists():
        return

    try:
        with open(pyproject, "rb") as f:
            data = tomllib.load(f)
    except Exception:
        logger.warning("Failed to read/parse %s; skipping local dep symlinks", pyproject)
        return

    sources = data.get("tool", {}).get("uv", {}).get("sources", {})
    if not sources:
        return

    for _dep_name, entry in sources.items():
        if not isinstance(entry, dict):
            continue
        raw_path = entry.get("path")
        if not raw_path:
            continue
        dep_rel = Path(raw_path)
        # Skip absolute paths — they work everywhere without symlinks
        if dep_rel.is_absolute():
            continue
        dep_real_path = (config.project_dir / dep_rel).resolve()
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
    """
    load_dotenv(config.project_dir)

    # Create hourly backup before running
    backup_database(config.db_path, config.project_dir)

    # Load tasks from SQLite
    store = SqliteTaskStore(config.db_path)

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
                console.print("Use 'gza retry' to start fresh instead")
                return 1
            if task.status == "pending":
                assert task.id is not None
                claimed = store.try_mark_in_progress(task.id, os.getpid())
                if claimed is None:
                    refreshed = store.get(task.id)
                    status = refreshed.status if refreshed else "unknown"
                    error_message(f"Error: Task {task_id} is no longer pending (status: {status})")
                    return 1
                task = claimed
            else:
                task.status = "in_progress"
                task.started_at = datetime.now(UTC)
                task.completed_at = None
                task.failure_reason = None
                task.running_pid = os.getpid()
                store.update(task)
        else:
            # Check if task is blocked by dependencies
            is_blocked, blocking_id, blocking_status = store.is_task_blocked(task)
            if is_blocked:
                error_message(f"Error: Task {task_id} is blocked by task {blocking_id} ({blocking_status})")
                return 1
            if task.status == "in_progress":
                task.running_pid = os.getpid()
                store.update(task)
            elif task.status != "pending":
                error_message(f"Error: Task {task_id} is no longer pending (status: {task.status})")
                return 1
            else:
                assert task.id is not None
                claimed = store.try_mark_in_progress(task.id, os.getpid())
                if claimed is None:
                    refreshed = store.get(task.id)
                    status = refreshed.status if refreshed else "unknown"
                    error_message(f"Error: Task {task_id} is no longer pending (status: {status})")
                    return 1
                task = claimed
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
            claimed = store.try_mark_in_progress(candidate.id, os.getpid())
            if claimed is None:
                continue
            task = claimed
            break

    if not task:
        console.print("No pending tasks found")
        return 0
    if on_task_claimed is not None:
        on_task_claimed(task)

    # Get effective model and provider for this task
    effective_model, effective_provider, effective_max_steps = get_effective_config_for_task(task, config)

    # Persist resolved model/provider to the task DB row immediately so analytics
    # can track which configuration actually ran, even if it crashes before completion.
    # provider_is_explicit is intentionally left unchanged so resolved provider
    # state does not become a sticky override for future executions.
    task.model = effective_model
    task.provider = effective_provider
    store.update(task)

    # Create a modified config with task-specific settings
    from copy import copy
    task_config = copy(config)
    task_config.model = effective_model or ""
    task_config.provider = effective_provider
    task_config.max_steps = effective_max_steps
    task_config.max_turns = effective_max_steps

    # Get the provider for this task
    provider = get_provider(task_config)

    if not provider.check_credentials():
        error_message(f"Error: No {provider.name} credentials found")
        console.print(f"  {provider.credential_setup_hint}")
        return 1

    # Verify credentials work before proceeding
    console.print(f"Verifying {provider.name} credentials...")
    if not provider.verify_credentials(task_config):
        return 1
    rc = _colors.RUNNER_COLORS
    console.print(f"[{rc.success}]Credentials verified ✓[/{rc.success}]")

    # Setup git on the main repo (for worktree operations)
    git = Git(config.project_dir)
    default_branch = git.default_branch()

    # Pull latest on default branch (without switching away from user's current branch)
    # We do this by fetching and then basing the worktree on origin/default_branch
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
            project_name=config.project_name,
            project_prefix=config.project_prefix,
            slug_override=slug_override,
            branch_strategy=config.branch_strategy,
            explicit_type=task.task_type_hint,
        )

    task_header(
        task.prompt,
        str(task.id) if task.id is not None else "",
        task.task_type,
        slug=task.slug,
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
    )


def _check_dependency_merge_precondition(
    task: Task,
    store: SqliteTaskStore,
    git: Git,
    *,
    default_branch: str,
) -> tuple[Task | None, str | None, str | None]:
    """Return unmet dependency merge prerequisite or a git operational error."""
    if task.same_branch or not task.depends_on:
        return (None, None, None)

    dep = store.resolve_dependency_completion(task)
    if dep is None:
        return (None, None, None)
    if not dep.branch:
        # Non-code dependencies may have no branch and are reachable via repo files.
        return (None, None, None)
    if not git.branch_exists(dep.branch):
        # A missing local branch is only safe when merge status is explicitly known.
        # Otherwise, fail closed so deleted local refs cannot bypass prerequisites.
        if dep.merge_status == "merged":
            return (None, None, None)
        return (dep, default_branch, None)

    result = git._run("merge-base", "--is-ancestor", dep.branch, default_branch, check=False)
    if result.returncode == 0:
        return (None, None, None)
    if result.returncode == 1:
        return (dep, default_branch, None)

    stderr = (result.stderr or "").strip()
    stdout = (result.stdout or "").strip()
    detail = stderr or stdout or "git merge-base failed"
    return (None, None, f"git merge-base --is-ancestor failed (exit {result.returncode}): {detail}")


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
            cleanup_worktree_for_branch(git, branch_name, force=True)
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
        base_ref = _select_worktree_base_ref(git, default_branch)
        console.print(f"Creating worktree: {worktree_path}")
        git.worktree_add(worktree_path, branch_name, base_ref)
        return True
    except GitError as e:
        error_message(f"Git error: {e}")
        return False


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
    skip_commit: bool = False,
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
        # Compute which files changed during the provider run (selective staging)
        post_run_status = worktree_git.status_porcelain()
        new_changes = post_run_status - pre_run_status
        has_uncommitted = bool(new_changes)

        if not has_uncommitted:
            # Check if branch already has commits from a previous run
            default_branch = worktree_git.default_branch()
            commits_ahead = worktree_git.count_commits_ahead(branch_name, default_branch)
            if commits_ahead == 0:
                # No uncommitted changes and no commits on branch - real failure
                # Note: No need to save WIP here since there are no changes
                failure_reason = extract_failure_reason(log_file)
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
                    },
                )
                write_log_entry(
                    log_file,
                    {
                        "type": "gza",
                        "subtype": "stats",
                        "message": f"Stats: {stats.num_steps_computed or stats.num_steps_reported or 0} steps, {stats.duration_seconds or 0.0:.1f}s, ${stats.cost_usd or 0.0:.4f}",
                        "duration_seconds": stats.duration_seconds,
                        "cost_usd": stats.cost_usd,
                        "num_steps": stats.num_steps_computed or stats.num_steps_reported or 0,
                    },
                )
                store.mark_failed(
                    task,
                    log_file=str(log_file.relative_to(config.project_dir)),
                    stats=stats,
                    branch=branch_name,
                    failure_reason=failure_reason,
                )
                return 0
            # else: branch has commits from a previous run - treat as success without committing

        if has_uncommitted:
            assert task.id is not None, "Task ID must be set before committing"
            # Squash any WIP commits before creating final commit
            _squash_wip_commits(worktree_git, task)

            # Stage only files that changed during the provider run
            files_to_stage = [filepath for _, filepath in new_changes]
            for f in files_to_stage:
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
    default_branch = worktree_git.default_branch()
    numstat_output = worktree_git.get_diff_numstat(f"{default_branch}...{branch_name}")
    diff_files, diff_added, diff_removed = parse_diff_numstat(numstat_output)

    # Write final log entries before marking completed in DB, so that
    # `gza log -f` (which checks task status) doesn't break out of the
    # follow loop before the log file is fully written.
    write_log_entry(log_file, {"type": "gza", "subtype": "outcome", "message": "Outcome: completed", "exit_code": 0})
    write_log_entry(
        log_file,
        {
            "type": "gza",
            "subtype": "stats",
            "message": f"Stats: {stats.num_steps_computed or stats.num_steps_reported or 0} steps, {stats.duration_seconds or 0.0:.1f}s, ${stats.cost_usd or 0.0:.4f}",
            "duration_seconds": stats.duration_seconds,
            "cost_usd": stats.cost_usd,
            "num_steps": stats.num_steps_computed or stats.num_steps_reported or 0,
        },
    )

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
    )
    auto_learnings = maybe_auto_regenerate_learnings(store, config)

    # Clear review state on the based_on implementation task after improve completes.
    # The improve task has addressed the review feedback, so the old review no longer
    # reflects the current code state.
    if task.task_type == "improve" and task.based_on:
        store.clear_review_state(task.based_on)
        # If parent was already merged, flip it back to unmerged — the improve
        # task added commits to the shared branch after the merge.
        parent = store.get(task.based_on)
        if parent and parent.id is not None and parent.merge_status == "merged":
            store.set_merge_status(parent.id, "unmerged")

    # Invalidate review state after rebase completes, since conflict resolution
    # may have introduced changes not covered by prior reviews.
    if task.task_type == "rebase" and task.based_on:
        store.invalidate_review_state(task.based_on)
        parent = store.get(task.based_on)
        if parent and parent.id is not None and parent.merge_status == "merged":
            store.set_merge_status(parent.id, "unmerged")

    # Rebase tasks run provider-side conflict resolution in the worktree.
    # Force-push from the host runner so SSH/auth follows host environment.
    if task.task_type == "rebase":
        worktree_git.push_force_with_lease(branch_name)

    console.print("")
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
        return _create_and_run_review_task(task, config, store)

    return 0


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
) -> int:
    """Inner task execution logic, split out to allow foreground worker cleanup."""
    # For explore, plan, review, and internal tasks, run without creating a branch.
    # Keep temporary "learn" compatibility for pre-migration rows.
    if task.task_type in ("explore", "plan", "review", "internal", "learn"):
        return _run_non_code_task(task, task_config, store, provider, git, resume=resume, open_after=open_after)

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

    # Setup logging - use task_id for naming (logs stay in main project)
    config.log_path.mkdir(parents=True, exist_ok=True)
    log_file = config.log_path / f"{task.slug}.log"
    # Persist log_file early so it's available if the process is killed before completion
    task.log_file = str(log_file.relative_to(config.project_dir))
    store.update(task)

    # Write orchestration pre-run entries
    write_worker_start_event(log_file, resumed=resume)
    write_log_entry(log_file, {"type": "gza", "subtype": "info", "message": f"Task: {task.id} {task.slug}"})
    write_log_entry(log_file, {"type": "gza", "subtype": "branch", "message": f"Branch: {branch_name}", "branch": branch_name})
    write_log_entry(log_file, {"type": "gza", "subtype": "info", "message": f"Provider: {provider.name}, Model: {task_config.model or 'default'}"})

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
            store.mark_failed(
                task,
                log_file=str(log_file.relative_to(config.project_dir)),
                branch=branch_name,
                failure_reason="GIT_ERROR",
            )
            return 1
        if blocking_dep is not None:
            assert blocking_dep.id is not None
            dep_branch = blocking_dep.branch or "<none>"
            failure_message = (
                f"Dependency {blocking_dep.id} on branch '{dep_branch}' is not merged into "
                f"'{target_branch}'. Failing without provider run."
            )
            error_message(f"Error: {failure_message}")
            write_log_entry(
                log_file,
                {
                    "type": "gza",
                    "subtype": "outcome",
                    "message": failure_message,
                    "failure_reason": "PREREQUISITE_UNMERGED",
                    "dependency_task_id": blocking_dep.id,
                    "dependency_branch": dep_branch,
                    "target_branch": target_branch,
                },
            )
            store.mark_failed(
                task,
                log_file=str(log_file.relative_to(config.project_dir)),
                branch=branch_name,
                failure_reason="PREREQUISITE_UNMERGED",
            )
            return 1

    # Setup summary directory and path for task/implement types
    _, summary_path = get_task_output_paths(task, config.project_dir)
    assert summary_path is not None, f"Code task type '{task.task_type}' must have a summary path"
    summary_dir = summary_path.parent
    summary_dir.mkdir(parents=True, exist_ok=True)

    # Create summary directory structure in worktree
    worktree_summary_dir = worktree_path / summary_dir.relative_to(config.project_dir)
    worktree_summary_dir.mkdir(parents=True, exist_ok=True)
    worktree_summary_path = worktree_path / summary_path.relative_to(config.project_dir)

    # For Docker containers, use /workspace-relative path instead of host worktree path
    # For native mode, use the actual worktree path
    if config.use_docker:
        prompt_summary_path = Path("/workspace") / summary_path.relative_to(config.project_dir)
    else:
        prompt_summary_path = worktree_summary_path

    # Run provider in the worktree
    if resume:
        prompt = PromptBuilder().resume_prompt()
    else:
        prompt = build_prompt(task, config, store, report_path=None, summary_path=prompt_summary_path, git=git)

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

    # Ensure all bundled skills are available in the worktree
    from .skills_utils import ensure_all_skills
    skills_dir = worktree_path / ".claude" / "skills"
    n_installed = ensure_all_skills(skills_dir)
    if n_installed:
        console.print(f"Installed {n_installed} skill(s) into worktree")

    # Copy learnings file into worktree so the agent can read it
    _copy_learnings_to_worktree(config, worktree_path)

    if not config.use_docker:
        _create_local_dep_symlinks(config, worktree_path)

    # Snapshot worktree state before provider runs so we can selectively stage only new changes
    pre_run_status = worktree_git.status_porcelain()

    try:
        result = provider.run(task_config, prompt, log_file, worktree_path, resume_session_id=task.session_id if resume else None, on_session_id=_on_session_id, on_step_count=_on_step_count)

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

        # Handle failures - check error_type first, then exit codes
        if result.error_type in ("max_turns", "max_steps"):
            # Save WIP changes before marking failed
            _save_wip_changes(task, worktree_git, config, branch_name)
            task_footer(
                task,
                stats,
                status=f"Failed: max steps of {task_config.max_steps} exceeded",
                branch=branch_name,
                store=store,
            )
            # Check log for agent-written marker; prefer MAX_STEPS for provider-detected over-budget failures.
            detected = extract_failure_reason(log_file)
            failure_reason = detected if detected != "UNKNOWN" else "MAX_STEPS"
            write_log_entry(log_file, {"type": "gza", "subtype": "outcome", "message": "Outcome: failed (max_steps)", "exit_code": result.exit_code, "failure_reason": failure_reason})
            write_log_entry(log_file, {"type": "gza", "subtype": "stats", "message": f"Stats: {stats.num_steps_computed or stats.num_steps_reported or 0} steps, {stats.duration_seconds or 0.0:.1f}s, ${stats.cost_usd or 0.0:.4f}", "duration_seconds": stats.duration_seconds, "cost_usd": stats.cost_usd, "num_steps": stats.num_steps_computed or stats.num_steps_reported or 0})
            store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats, branch=branch_name, failure_reason=failure_reason)
            return 0
        elif exit_code == 124:
            # Save WIP changes before marking failed
            _save_wip_changes(task, worktree_git, config, branch_name)
            task_footer(
                task,
                stats,
                status=f"Failed: {provider.name} timed out after {config.timeout_minutes} minutes",
                branch=branch_name,
                store=store,
            )
            detected = extract_failure_reason(log_file)
            failure_reason = detected if detected != "UNKNOWN" else "TIMEOUT"
            write_log_entry(log_file, {"type": "gza", "subtype": "outcome", "message": f"Outcome: failed (timeout after {config.timeout_minutes}m)", "exit_code": exit_code, "failure_reason": failure_reason})
            write_log_entry(log_file, {"type": "gza", "subtype": "stats", "message": f"Stats: {stats.num_steps_computed or stats.num_steps_reported or 0} steps, {stats.duration_seconds or 0.0:.1f}s, ${stats.cost_usd or 0.0:.4f}", "duration_seconds": stats.duration_seconds, "cost_usd": stats.cost_usd, "num_steps": stats.num_steps_computed or stats.num_steps_reported or 0})
            store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats, branch=branch_name, failure_reason=failure_reason)
            return 0
        elif exit_code != 0:
            # Save WIP changes before marking failed
            _save_wip_changes(task, worktree_git, config, branch_name)
            task_footer(
                task,
                stats,
                status=f"Failed: {provider.name} exited with code {exit_code}",
                branch=branch_name,
                store=store,
            )
            failure_reason = extract_failure_reason(log_file)
            write_log_entry(log_file, {"type": "gza", "subtype": "outcome", "message": f"Outcome: failed (exit_code={exit_code})", "exit_code": exit_code, "failure_reason": failure_reason})
            write_log_entry(log_file, {"type": "gza", "subtype": "stats", "message": f"Stats: {stats.num_steps_computed or stats.num_steps_reported or 0} steps, {stats.duration_seconds or 0.0:.1f}s, ${stats.cost_usd or 0.0:.4f}", "duration_seconds": stats.duration_seconds, "cost_usd": stats.cost_usd, "num_steps": stats.num_steps_computed or stats.num_steps_reported or 0})
            store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats, branch=branch_name, failure_reason=failure_reason)
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
            skip_commit=task.task_type == "rebase",
        )

    except GitError as e:
        error_message(f"Git error: {e}")
        store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), branch=branch_name, failure_reason="GIT_ERROR")
        return 1
    except KeyboardInterrupt:
        # Save WIP changes before returning
        _save_wip_changes(task, worktree_git, config, branch_name)
        store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), branch=branch_name, failure_reason="INTERRUPTED")
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
) -> int:
    """Run a non-code task (explore, plan, review, internal) in a worktree (no branch creation).

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

    # Setup logging
    config.log_path.mkdir(parents=True, exist_ok=True)
    log_file = config.log_path / f"{task.slug}.log"
    # Persist log_file early so it's available if the process is killed before completion
    task.log_file = str(log_file.relative_to(config.project_dir))
    store.update(task)

    # Write orchestration pre-run entries
    write_worker_start_event(log_file, resumed=resume)
    write_log_entry(log_file, {"type": "gza", "subtype": "info", "message": f"Task: {task.id} {task.slug}"})
    write_log_entry(log_file, {"type": "gza", "subtype": "info", "message": f"Provider: {provider.name}, Model: {config.model or 'default'}"})

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

        # For review tasks with depends_on, check if we should run on the implementation branch
        base_ref = None
        if task.task_type == "review" and task.depends_on:
            dep_task = store.get(task.depends_on)
            if dep_task and dep_task.branch and dep_task.status == "completed":
                # Run review on the implementation branch
                base_ref = dep_task.branch
                console.print(f"Running review on implementation branch: [blue]{base_ref}[/blue]")

        # Default to origin/default_branch or local default_branch
        if not base_ref:
            base_ref = f"origin/{default_branch}"
            if git:
                git_result = git._run("rev-parse", "--verify", base_ref, check=False)
                if git_result.returncode != 0:
                    base_ref = default_branch  # Fall back to local branch

        # Create worktree without creating a new branch (use --detach to check out HEAD)
        # This creates a worktree in detached HEAD state based on the specified ref
        console.print(f"Creating worktree: {worktree_path}")
        if git:
            git._run("worktree", "add", "--detach", str(worktree_path), base_ref)

        # Create report directory structure in worktree
        worktree_report_dir = worktree_path / report_path.parent.relative_to(config.project_dir)
        worktree_report_dir.mkdir(parents=True, exist_ok=True)
        worktree_report_path = worktree_path / report_path.relative_to(config.project_dir)

        # For Docker containers, use /workspace-relative path instead of host worktree path
        # The container only has /workspace mounted, so we need to use a path inside that
        # For native mode, use the actual worktree path
        if config.use_docker:
            prompt_report_path = Path("/workspace") / report_path.relative_to(config.project_dir)
        else:
            prompt_report_path = worktree_report_path

        # Run provider in the worktree
        if resume:
            prompt = PromptBuilder().resume_prompt(
                task_id=task.id,
                task_slug=task.slug,
                report_path=prompt_report_path,
            )
        else:
            prompt = build_prompt(task, config, store, report_path=prompt_report_path, git=git)
        # Ensure all bundled skills are available in the worktree
        from .skills_utils import ensure_all_skills
        skills_dir = worktree_path / ".claude" / "skills"
        n_installed = ensure_all_skills(skills_dir)
        if n_installed:
            console.print(f"Installed {n_installed} skill(s) into worktree")

        # Internal orchestration tasks do not implicitly consume learnings context.
        if task.task_type not in ("internal", "learn"):
            _copy_learnings_to_worktree(config, worktree_path)

        if not config.use_docker:
            _create_local_dep_symlinks(config, worktree_path)

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
            result = provider.run(config, prompt, log_file, worktree_path, resume_session_id=task.session_id if resume else None, on_session_id=_on_session_id_non_code, on_step_count=_on_step_count_non_code)
        except KeyboardInterrupt:
            store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), failure_reason="INTERRUPTED")
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

        # Handle failures - check error_type first, then exit codes
        if result.error_type in ("max_turns", "max_steps"):
            task_footer(
                task,
                stats,
                status=f"Failed: max steps of {config.max_steps} exceeded",
                worktree=worktree_path,
                store=store,
            )
            detected = extract_failure_reason(log_file)
            failure_reason = detected if detected != "UNKNOWN" else "MAX_STEPS"
            write_log_entry(log_file, {"type": "gza", "subtype": "outcome", "message": "Outcome: failed (max_steps)", "exit_code": result.exit_code, "failure_reason": failure_reason})
            write_log_entry(log_file, {"type": "gza", "subtype": "stats", "message": f"Stats: {stats.num_steps_computed or stats.num_steps_reported or 0} steps, {stats.duration_seconds or 0.0:.1f}s, ${stats.cost_usd or 0.0:.4f}", "duration_seconds": stats.duration_seconds, "cost_usd": stats.cost_usd, "num_steps": stats.num_steps_computed or stats.num_steps_reported or 0})
            store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats, failure_reason=failure_reason)
            return 0
        elif exit_code == 124:
            task_footer(
                task,
                stats,
                status=f"Failed: {provider.name} timed out after {config.timeout_minutes} minutes",
                worktree=worktree_path,
                store=store,
            )
            failure_reason = extract_failure_reason(log_file)
            write_log_entry(log_file, {"type": "gza", "subtype": "outcome", "message": f"Outcome: failed (timeout after {config.timeout_minutes}m)", "exit_code": exit_code, "failure_reason": failure_reason})
            write_log_entry(log_file, {"type": "gza", "subtype": "stats", "message": f"Stats: {stats.num_steps_computed or stats.num_steps_reported or 0} steps, {stats.duration_seconds or 0.0:.1f}s, ${stats.cost_usd or 0.0:.4f}", "duration_seconds": stats.duration_seconds, "cost_usd": stats.cost_usd, "num_steps": stats.num_steps_computed or stats.num_steps_reported or 0})
            store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats, failure_reason=failure_reason)
            return 0
        elif exit_code != 0:
            task_footer(
                task,
                stats,
                status=f"Failed: {provider.name} exited with code {exit_code}",
                worktree=worktree_path,
                store=store,
            )
            failure_reason = extract_failure_reason(log_file)
            write_log_entry(log_file, {"type": "gza", "subtype": "outcome", "message": f"Outcome: failed (exit_code={exit_code})", "exit_code": exit_code, "failure_reason": failure_reason})
            write_log_entry(log_file, {"type": "gza", "subtype": "stats", "message": f"Stats: {stats.num_steps_computed or stats.num_steps_reported or 0} steps, {stats.duration_seconds or 0.0:.1f}s, ${stats.cost_usd or 0.0:.4f}", "duration_seconds": stats.duration_seconds, "cost_usd": stats.cost_usd, "num_steps": stats.num_steps_computed or stats.num_steps_reported or 0})
            store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats, failure_reason=failure_reason)
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
                write_log_entry(
                    log_file,
                    {
                        "type": "gza",
                        "subtype": "stats",
                        "message": f"Stats: {stats.num_steps_computed or stats.num_steps_reported or 0} steps, {stats.duration_seconds or 0.0:.1f}s, ${stats.cost_usd or 0.0:.4f}",
                        "duration_seconds": stats.duration_seconds,
                        "cost_usd": stats.cost_usd,
                        "num_steps": stats.num_steps_computed or stats.num_steps_reported or 0,
                    },
                )
                store.mark_failed(
                    task,
                    log_file=str(log_file.relative_to(config.project_dir)),
                    stats=stats,
                    failure_reason="MISSING_REPORT_ARTIFACT",
                )
                return 0

        console.print(f"Report written to: {report_file_relative}")
        # Ensure target directory exists
        report_path.parent.mkdir(parents=True, exist_ok=True)
        # Copy report content from worktree to project dir
        report_path.write_text(worktree_report_path.read_text())

        # Read output content for storage in DB
        output_content = report_path.read_text()

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
                post_review_to_pr(task, impl_task, store, config.project_dir, required=False)

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
        store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), failure_reason="GIT_ERROR")
        return 1
