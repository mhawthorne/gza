"""Learning accumulation from completed tasks."""

from __future__ import annotations

import json
import re
import subprocess
from collections import OrderedDict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from .config import DEFAULT_LEARNINGS_INTERVAL, DEFAULT_LEARNINGS_MAX_ITEMS, DEFAULT_LEARNINGS_WINDOW
from .console import console
from .db import SqliteTaskStore, Task

if TYPE_CHECKING:
    from .config import Config


AUTO_LEARNINGS_INTERVAL = DEFAULT_LEARNINGS_INTERVAL
LEARNINGS_HISTORY_FILE = "learnings_history.jsonl"


@dataclass
class LearningsResult:
    """Result metadata for learnings regeneration."""

    path: Path
    tasks_used: int
    learnings_count: int
    added_count: int
    removed_count: int
    retained_count: int
    churn_percent: float


_BULLET_RE = re.compile(r"^\s*[-*]\s+(.+?)\s*$")
_TOPIC_RE = re.compile(r"^##\s+(.+?)\s*$")


def _normalize_learning(text: str) -> str:
    """Normalize candidate learning text for dedupe and display."""
    cleaned = re.sub(r"\s+", " ", text.strip())
    cleaned = cleaned.strip("-* ")
    return cleaned


# Type alias: topic -> list of bullet strings
CategorizedLearnings = dict[str, list[str]]

DEFAULT_TOPIC = "General"


def _extract_learnings_from_output(output: str) -> CategorizedLearnings:
    """Extract categorized bullet learnings from markdown-ish output.

    Expects ``## Topic`` headers followed by ``- bullet`` lines.
    Bullets before any header go under DEFAULT_TOPIC.
    """
    categories: CategorizedLearnings = {}
    current_topic = DEFAULT_TOPIC
    for line in output.splitlines():
        topic_match = _TOPIC_RE.match(line)
        if topic_match:
            current_topic = topic_match.group(1).strip()
            continue
        bullet_match = _BULLET_RE.match(line)
        if bullet_match:
            item = _normalize_learning(bullet_match.group(1))
            if 8 <= len(item) <= 160 and not item.lower().startswith("task id:"):
                categories.setdefault(current_topic, []).append(item)

    return categories


def _flatten_categorized(categorized: CategorizedLearnings) -> list[str]:
    """Flatten categorized learnings to a flat list for delta tracking."""
    items: list[str] = []
    for bullets in categorized.values():
        items.extend(bullets)
    return items


def _dedupe(items: list[str]) -> list[str]:
    """Case-insensitive stable dedupe preserving first-seen order."""
    seen: OrderedDict[str, str] = OrderedDict()
    for item in items:
        normalized = item.lower()
        if normalized not in seen:
            seen[normalized] = item
    return list(seen.values())


def _dedupe_categorized(categorized: CategorizedLearnings) -> CategorizedLearnings:
    """Case-insensitive dedupe within each topic, preserving topic order."""
    result: CategorizedLearnings = {}
    for topic, items in categorized.items():
        deduped = _dedupe(items)
        if deduped:
            result[topic] = deduped
    return result


def _merge_categorized(
    existing: CategorizedLearnings, new: CategorizedLearnings
) -> CategorizedLearnings:
    """Merge new categorized learnings into existing, deduplicating per topic."""
    merged: CategorizedLearnings = {}
    all_topics = list(existing.keys())
    for topic in new:
        if topic not in all_topics:
            all_topics.append(topic)
    for topic in all_topics:
        combined = list(existing.get(topic, [])) + list(new.get(topic, []))
        deduped = _dedupe(combined)
        if deduped:
            merged[topic] = deduped
    return merged


def _format_learnings_markdown(learnings: CategorizedLearnings, task_count: int) -> str:
    """Format categorized learnings as markdown with topic headers."""
    timestamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# Project Learnings",
        "",
        f"Last updated: {timestamp} (from {task_count} completed tasks)",
    ]
    for topic, items in learnings.items():
        lines.append("")
        lines.append(f"## {topic}")
        lines.extend(f"- {item}" for item in items)
    lines.append("")
    return "\n".join(lines)


def _extract_existing_file_learnings(path: Path) -> CategorizedLearnings:
    """Extract categorized bullet learnings from an existing learnings file."""
    if not path.exists():
        return {}
    try:
        content = path.read_text()
    except OSError:
        return {}

    return _dedupe_categorized(_extract_learnings_from_output(content))


def _build_summarization_prompt(
    tasks: list[Task],
    existing_learnings: str = "",
    max_items: int = DEFAULT_LEARNINGS_MAX_ITEMS,
) -> str:
    """Build LLM prompt for incrementally updating learnings from recent task outputs."""
    existing_section = existing_learnings.strip() if existing_learnings.strip() else "No existing learnings."

    task_sections = []
    for task in tasks:
        output = task.output_content or ""
        if len(output) > 1500:
            output = output[:1500] + "\n... [truncated]"
        prompt_text = task.prompt or ""
        if len(prompt_text) > 300:
            prompt_text = prompt_text[:300] + "... [truncated]"
        task_sections.append(
            f"### Task {task.id} ({task.task_type})\n"
            f"**Prompt**: {prompt_text}\n\n"
            f"**Output**:\n{output}"
        )
    tasks_text = "\n\n---\n\n".join(task_sections)
    n = len(tasks)

    return (
        "You are maintaining a knowledge base for a software project. Your job is to\n"
        "update the project's learnings based on recent completed tasks.\n\n"
        "## Current Learnings\n"
        f"{existing_section}\n\n"
        f"## Recent Completed Tasks (last {n})\n"
        f"{tasks_text}\n\n"
        "## Instructions\n\n"
        "Update the learnings based on the recent tasks above:\n"
        "- ADD new patterns, conventions, or pitfalls discovered in recent tasks\n"
        "- REVISE any existing learnings that are now outdated or wrong based on recent work\n"
        "- KEEP existing learnings that are still valid, even if not mentioned in recent tasks\n"
        "- REMOVE learnings only if recent tasks clearly contradict them\n\n"
        "Focus on:\n"
        "- Codebase conventions (naming, structure, idioms)\n"
        "- Architecture decisions and rationale\n"
        "- Testing patterns (frameworks, fixtures, assertions)\n"
        "- Common pitfalls specific to this project\n"
        "- Workflow preferences (tools, commands)\n\n"
        "Do NOT include:\n"
        '- Task-specific details that don\'t generalize\n'
        "- Generic software engineering advice\n"
        '- Vague platitudes ("write clean code", "test thoroughly")\n'
        "- Repetitive or near-duplicate entries\n\n"
        "Output format: organize learnings under topic headers.\n"
        'Each topic is a markdown H2 header (e.g., "## Testing Patterns").\n'
        'Under each topic, list learnings as bullet points starting with "- ".\n'
        "Choose short, descriptive topic names (2-4 words). Group related learnings together.\n"
        "Typical topics: Testing Patterns, Git Workflow, Code Style, Architecture,\n"
        "Configuration, Error Handling, CLI Commands — but use whatever fits the content.\n"
        "No numbering, no sub-lists. Max 25 words per learning.\n"
        "Each learning should be concrete and actionable — a new developer should\n"
        "be able to follow it without additional context.\n\n"
        f"IMPORTANT: Keep the total number of learnings to at most {max_items} items.\n"
        "If the combined list exceeds this limit, consolidate related items and\n"
        "drop the least generalizable or most project-specific entries.\n"
        "Prefer keeping broadly useful process knowledge over one-off bug fixes."
    )


def _run_learnings_task(
    store: SqliteTaskStore,
    config: Config,
    recent_tasks: list[Task],
    existing_learnings: str = "",
    max_items: int = DEFAULT_LEARNINGS_MAX_ITEMS,
) -> CategorizedLearnings | None:
    """Run an internal task to summarize learnings from recent task outputs.

    Creates an ``internal`` task and runs it via the standard runner (same as
    explore/plan/review tasks — worktree, provider, status transitions).
    The task is kept in the DB for observability.

    Returns categorized learnings, or None on any failure.
    """
    from . import runner as _runner_mod

    prompt = _build_summarization_prompt(recent_tasks, existing_learnings, max_items=max_items)
    learn_task = store.add(
        prompt=prompt,
        task_type="internal",
        skip_learnings=True,
    )

    learn_task_id = learn_task.id
    if learn_task_id is None:
        return None

    try:
        exit_code = _runner_mod.run(config, task_id=learn_task_id)
    except Exception as exc:
        console.print(f"[yellow]LLM learnings summarization failed: {exc}; falling back to regex extraction.[/yellow]")
        return None

    refreshed = store.get(learn_task_id)
    if exit_code != 0 or refreshed is None or refreshed.status != "completed":
        return None

    if not refreshed.output_content:
        return None

    learnings = _extract_learnings_from_output(refreshed.output_content)
    if not learnings:
        return None

    # Clean up the report file — the parsed result lives in .gza/learnings.md
    # and the raw output is preserved in task.output_content in the DB.
    from .runner import get_task_output_paths

    report_path, _ = get_task_output_paths(refreshed, config.project_dir)
    if report_path and report_path.exists():
        report_path.unlink()

    return learnings


def _spawn_background_learnings_update(config: Config, window: int) -> bool:
    """Start detached `gza learnings update` process.

    Returns True when spawning succeeds. Returns False if process creation
    fails; caller should run foreground fallback.
    """
    startup_log_path = config.workers_path / "learnings-update.startup.log"
    startup_log_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "uv",
        "run",
        "gza",
        "learnings",
        "update",
        "--window",
        str(window),
        "--project",
        str(config.project_dir.absolute()),
    ]
    with startup_log_path.open("ab") as startup_log:
        subprocess.Popen(
            cmd,
            stdout=startup_log,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
            cwd=config.project_dir,
        )
    return True


def _append_history_entry(config: Config, entry: dict) -> None:
    """Append a JSONL history record for learnings regeneration.

    Best-effort only: failures should not block task completion.
    """
    history_path = config.project_dir / ".gza" / LEARNINGS_HISTORY_FILE
    try:
        history_path.parent.mkdir(parents=True, exist_ok=True)
        with history_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, separators=(",", ":")) + "\n")
    except OSError:
        return


def regenerate_learnings(
    store: SqliteTaskStore,
    config: Config,
    window: int = DEFAULT_LEARNINGS_WINDOW,
) -> LearningsResult:
    """Regenerate `.gza/learnings.md` from recent completed tasks."""
    if window <= 0:
        raise ValueError("window must be positive")

    recent_tasks = store.get_recent_completed(limit=window)
    categorized: CategorizedLearnings = {}
    learnings_path = config.project_dir / ".gza" / "learnings.md"
    previous_categorized = _extract_existing_file_learnings(learnings_path)
    existing_learnings_text = learnings_path.read_text() if learnings_path.exists() else ""

    if recent_tasks:
        llm_learnings = _run_learnings_task(
            store, config, recent_tasks, existing_learnings_text,
            max_items=config.learnings_max_items,
        )
        if llm_learnings is not None:
            categorized = llm_learnings
        else:
            # Fallback: preserve existing learnings and append new bullets (deduplicated)
            new_categorized: CategorizedLearnings = {}
            for task in recent_tasks:
                if task.output_content:
                    task_learnings = _extract_learnings_from_output(task.output_content)
                    new_categorized = _merge_categorized(new_categorized, task_learnings)
            categorized = _merge_categorized(previous_categorized, new_categorized)

    categorized = _dedupe_categorized(categorized)
    if not categorized:
        categorized = {DEFAULT_TOPIC: ["No strong patterns extracted yet; keep tasks explicit and scoped."]}

    previous_flat = _flatten_categorized(previous_categorized)
    current_flat = _flatten_categorized(categorized)
    previous_set = {item.lower() for item in previous_flat}
    current_set = {item.lower() for item in current_flat}
    retained_count = len(previous_set & current_set)
    added_count = len(current_set - previous_set)
    removed_count = len(previous_set - current_set)
    baseline = max(len(previous_set), 1)
    churn_percent = round(((added_count + removed_count) / baseline) * 100, 1)

    content = _format_learnings_markdown(categorized, len(recent_tasks))
    learnings_path.parent.mkdir(parents=True, exist_ok=True)
    learnings_path.write_text(content)
    _append_history_entry(
        config,
        {
            "timestamp_utc": datetime.now(UTC).isoformat(),
            "window": window,
            "tasks_used": len(recent_tasks),
            "learnings_count": len(current_flat),
            "added_count": added_count,
            "removed_count": removed_count,
            "retained_count": retained_count,
            "churn_percent": churn_percent,
            "learnings_file": str(learnings_path.relative_to(config.project_dir)),
        },
    )

    return LearningsResult(
        path=learnings_path,
        tasks_used=len(recent_tasks),
        learnings_count=len(current_flat),
        added_count=added_count,
        removed_count=removed_count,
        retained_count=retained_count,
        churn_percent=churn_percent,
    )


def maybe_auto_regenerate_learnings(
    store: SqliteTaskStore,
    config: Config,
    interval: int | None = None,
    window: int | None = None,
) -> LearningsResult | None:
    """Regenerate learnings on periodic completed-task intervals."""
    if interval is None:
        interval = config.learnings_interval
    if window is None:
        window = config.learnings_window
    if interval <= 0:
        return None

    completed_count = store.get_stats().get("completed", 0)
    if completed_count <= 0 or completed_count % interval != 0:
        return None

    recent_tasks = store.get_recent_completed(limit=window)
    if not recent_tasks:
        return None

    try:
        _spawn_background_learnings_update(config, window)
    except Exception as exc:
        console.print(
            "[yellow]LLM learnings background spawn failed: "
            f"{exc}; running foreground regeneration fallback.[/yellow]"
        )
        return regenerate_learnings(store, config, window=window)
    return None
