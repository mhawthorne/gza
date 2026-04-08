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

from .config import DEFAULT_LEARNINGS_INTERVAL, DEFAULT_LEARNINGS_WINDOW
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


def _normalize_learning(text: str) -> str:
    """Normalize candidate learning text for dedupe and display."""
    cleaned = re.sub(r"\s+", " ", text.strip())
    cleaned = cleaned.strip("-* ")
    return cleaned


def _extract_learnings_from_output(output: str) -> list[str]:
    """Extract compact bullet learnings from markdown-ish output."""
    learnings: list[str] = []
    for line in output.splitlines():
        bullet_match = _BULLET_RE.match(line)
        if bullet_match:
            item = _normalize_learning(bullet_match.group(1))
            if 8 <= len(item) <= 160 and not item.lower().startswith("task id:"):
                learnings.append(item)

    return learnings



def _dedupe(items: list[str]) -> list[str]:
    """Case-insensitive stable dedupe preserving first-seen order."""
    seen: OrderedDict[str, str] = OrderedDict()
    for item in items:
        normalized = item.lower()
        if normalized not in seen:
            seen[normalized] = item
    return list(seen.values())


def _format_learnings_markdown(learnings: list[str], task_count: int) -> str:
    """Format learnings as markdown."""
    timestamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# Project Learnings",
        "",
        f"Last updated: {timestamp} (from {task_count} completed tasks)",
        "",
        "## Recent Patterns",
    ]
    lines.extend(f"- {item}" for item in learnings)
    lines.append("")
    return "\n".join(lines)


def _extract_existing_file_learnings(path: Path) -> list[str]:
    """Extract bullet learnings from an existing learnings file."""
    if not path.exists():
        return []
    try:
        content = path.read_text()
    except OSError:
        return []

    items: list[str] = []
    for line in content.splitlines():
        match = _BULLET_RE.match(line)
        if match:
            item = _normalize_learning(match.group(1))
            if item:
                items.append(item)
    return _dedupe(items)


def _build_summarization_prompt(tasks: list[Task], existing_learnings: str = "") -> str:
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
            f"### Task #{task.id} ({task.task_type})\n"
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
        'Output format: a flat bullet list, one learning per line, starting with "- ".\n'
        "No headers, no numbering, no sub-lists. Max 25 words per learning.\n"
        "Each learning should be concrete and actionable — a new developer should\n"
        "be able to follow it without additional context."
    )


def _run_learnings_task(
    store: SqliteTaskStore,
    config: Config,
    recent_tasks: list[Task],
    existing_learnings: str = "",
) -> list[str] | None:
    """Run an internal task to summarize learnings from recent task outputs.

    Creates an ``internal`` task and runs it via the standard runner (same as
    explore/plan/review tasks — worktree, provider, status transitions).
    The task is kept in the DB for observability.

    Returns extracted bullet-point learnings, or None on any failure.
    """
    from . import runner as _runner_mod

    prompt = _build_summarization_prompt(recent_tasks, existing_learnings)
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
    return learnings if learnings else None


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
    raw_learnings: list[str] = []
    learnings_path = config.project_dir / ".gza" / "learnings.md"
    previous_learnings = _extract_existing_file_learnings(learnings_path)
    existing_learnings_text = learnings_path.read_text() if learnings_path.exists() else ""

    if recent_tasks:
        llm_learnings = _run_learnings_task(store, config, recent_tasks, existing_learnings_text)
        if llm_learnings is not None:
            raw_learnings = llm_learnings
        else:
            # Fallback: preserve existing learnings and append new bullets (deduplicated)
            new_bullets: list[str] = []
            for task in recent_tasks:
                if task.output_content:
                    new_bullets.extend(_extract_learnings_from_output(task.output_content))
            raw_learnings = _dedupe(previous_learnings + new_bullets)

    learnings = _dedupe(raw_learnings)
    if not learnings:
        learnings = ["No strong patterns extracted yet; keep tasks explicit and scoped."]

    previous_set = {item.lower() for item in previous_learnings}
    current_set = {item.lower() for item in learnings}
    retained_count = len(previous_set & current_set)
    added_count = len(current_set - previous_set)
    removed_count = len(previous_set - current_set)
    baseline = max(len(previous_set), 1)
    churn_percent = round(((added_count + removed_count) / baseline) * 100, 1)

    content = _format_learnings_markdown(learnings, len(recent_tasks))
    learnings_path.parent.mkdir(parents=True, exist_ok=True)
    learnings_path.write_text(content)
    _append_history_entry(
        config,
        {
            "timestamp_utc": datetime.now(UTC).isoformat(),
            "window": window,
            "tasks_used": len(recent_tasks),
            "learnings_count": len(learnings),
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
        learnings_count=len(learnings),
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
