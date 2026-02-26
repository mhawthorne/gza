"""Learning accumulation from completed tasks."""

from __future__ import annotations

import re
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from .db import SqliteTaskStore, Task

if TYPE_CHECKING:
    from .config import Config


DEFAULT_LEARNINGS_WINDOW = 15
AUTO_LEARNINGS_INTERVAL = 5


@dataclass
class LearningsResult:
    """Result metadata for learnings regeneration."""

    path: Path
    tasks_used: int
    learnings_count: int


_BULLET_RE = re.compile(r"^\s*[-*]\s+(.+?)\s*$")
_HEADER_RE = re.compile(r"^\s{0,3}#{1,6}\s+(.+?)\s*$")


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
            continue

        header_match = _HEADER_RE.match(line)
        if header_match:
            section = _normalize_learning(header_match.group(1))
            if section and section.lower() not in {"summary", "report", "overview"}:
                learnings.append(f"Prefer following documented {section.lower()} conventions.")

    return learnings


def _fallback_learning(task: Task) -> str:
    """Generate a fallback learning from task prompt when no bullets exist."""
    first_line = task.prompt.splitlines()[0].strip()
    first_line = _normalize_learning(first_line)
    if len(first_line) > 120:
        first_line = first_line[:117].rstrip() + "..."
    return f"Reuse patterns from: {first_line}"


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
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
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


def regenerate_learnings(
    store: SqliteTaskStore,
    config: "Config",
    window: int = DEFAULT_LEARNINGS_WINDOW,
) -> LearningsResult:
    """Regenerate `.gza/learnings.md` from recent completed tasks."""
    recent_tasks = store.get_recent_completed(limit=window)
    raw_learnings: list[str] = []

    for task in recent_tasks:
        if task.output_content:
            raw_learnings.extend(_extract_learnings_from_output(task.output_content))
        if not task.output_content:
            raw_learnings.append(_fallback_learning(task))

    learnings = _dedupe(raw_learnings)
    if not learnings:
        learnings = ["No strong patterns extracted yet; keep tasks explicit and scoped."]

    content = _format_learnings_markdown(learnings, len(recent_tasks))
    learnings_path = config.project_dir / ".gza" / "learnings.md"
    learnings_path.parent.mkdir(parents=True, exist_ok=True)
    learnings_path.write_text(content)

    return LearningsResult(
        path=learnings_path,
        tasks_used=len(recent_tasks),
        learnings_count=len(learnings),
    )


def maybe_auto_regenerate_learnings(
    store: SqliteTaskStore,
    config: "Config",
    interval: int = AUTO_LEARNINGS_INTERVAL,
    window: int = DEFAULT_LEARNINGS_WINDOW,
) -> LearningsResult | None:
    """Regenerate learnings on periodic completed-task intervals."""
    if interval <= 0:
        return None

    completed_count = store.get_stats().get("completed", 0)
    if completed_count <= 0 or completed_count % interval != 0:
        return None

    return regenerate_learnings(store, config, window=window)
