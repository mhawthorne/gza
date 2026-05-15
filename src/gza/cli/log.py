"""Log display and timeline rendering for ``gza log``."""

import argparse
import json
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from rich.markup import escape as rich_escape

from ..colors import (
    LINEAGE_STATUS_COLORS,
    PS_STATUS_COLORS,
    SHOW_COLORS_DICT,
    blue,
    pink,
)
from ..config import Config
from ..console import console, format_duration
from ..db import SqliteTaskStore, Task as DbTask
from ..log_paths import ops_log_path_for
from ..providers.log_renderers import UnknownLogProviderError, get_log_renderer
from ..providers.log_rendering import (
    message_content_items as provider_message_content_items,
    result_step_count as provider_result_step_count,
)
from ..workers import WorkerMetadata, WorkerRegistry
from ._common import (
    _build_failure_diagnostics,
    _parse_iso,
    _render_failure_diagnostics,
    get_store,
    pager_context,
    resolve_id,
)


def _lc() -> str:
    """Return the themed label color for log output."""
    return SHOW_COLORS_DICT["label"]


def _result_step_count(result_entry: dict) -> int | None:
    """Resolve a result entry's step count using step-first fallback."""
    return provider_result_step_count(result_entry)


def _append_timeline_step(steps: list[dict], message_text: str | None, summary: str | None = None) -> dict:
    """Append a timeline step and return it."""
    step_index = len(steps) + 1
    step: dict = {
        "step_id": f"S{step_index}",
        "message_text": (message_text or "").strip() or None,
        "summary": summary,
        "substeps": [],
    }
    steps.append(step)
    return step


def _append_substep(step: dict, detail: str) -> None:
    """Append a substep line to a timeline step."""
    detail = detail.strip()
    if not detail:
        return
    substeps = step["substeps"]
    substeps.append(
        {
            "substep_id": f"{step['step_id']}.{len(substeps) + 1}",
            "detail": detail,
        }
    )


def _ensure_current_step(steps: list[dict], current_step: dict | None) -> dict:
    """Ensure a current step exists for pre-message tool activity."""
    if current_step is not None:
        return current_step
    return _append_timeline_step(steps, None, summary="Pre-message tool activity")


def _message_content_items(entry: dict) -> list[dict]:
    """Normalize assistant/user message content into a list of content items."""
    return provider_message_content_items(entry)


_RICH_STYLE_TAG_RE = re.compile(r"\[/?[a-zA-Z0-9_#]+\]")


def _strip_timeline_markup(line: str) -> str:
    """Remove renderer Rich style tags while preserving literal bracketed content."""
    return _RICH_STYLE_TAG_RE.sub("", line).replace("\\[", "[").replace("\\]", "]").strip()


def _timeline_detail_from_log_line(entry: dict, line: str) -> str:
    """Normalize renderer log output into timeline detail text."""
    cleaned = _strip_timeline_markup(line)
    if not cleaned:
        return ""

    if cleaned.startswith("[tool: "):
        header, _, detail = cleaned.partition("] ")
        tool_name = header.removeprefix("[tool: ").rstrip("]")
        item: dict[str, object] = {}
        if isinstance(entry.get("item"), dict):
            item = cast(dict[str, object], entry["item"])
        if entry.get("type") == "item.completed" and item.get("type") == "command_execution":
            return f"tool_call {tool_name} {detail}".strip()
        return f"tool_call {detail or tool_name}".strip()

    if entry.get("type") == "user":
        message = entry.get("message")
        if isinstance(message, dict):
            content = message.get("content")
            if isinstance(content, list):
                for item in content:
                    if not isinstance(item, dict) or item.get("type") != "tool_result":
                        continue
                    prefix = "tool_error" if item.get("is_error", False) else "tool_output"
                    return f"{prefix} {cleaned}".strip()

    if entry.get("type") == "item.completed":
        item_payload = entry.get("item")
        if isinstance(item_payload, dict) and item_payload.get("type") == "command_execution":
            exit_code = item_payload.get("exit_code")
            prefix = "tool_error" if isinstance(exit_code, int) and exit_code != 0 else "tool_output"
            return f"{prefix} {cleaned}".strip()

    if entry.get("type") in {"tool_output", "tool_error", "tool_retry"}:
        return f"{entry['type']} {cleaned}".strip()

    return cleaned


def _build_step_timeline(
    entries: list[dict],
    *,
    provider: str | None = None,
    configured_model: str | None = None,
) -> list[dict]:
    """Build a step-first timeline from provider renderer output."""
    steps, _suppressed_count = _build_step_timeline_with_metadata(
        entries,
        provider=provider,
        configured_model=configured_model,
    )
    return steps


def _build_step_timeline_with_metadata(
    entries: list[dict],
    *,
    provider: str | None = None,
    configured_model: str | None = None,
) -> tuple[list[dict], int]:
    """Build a step-first timeline from provider renderer output."""
    steps: list[dict] = []
    current_step: dict | None = None
    renderer = get_log_renderer(provider, configured_model=configured_model, verbose=False)

    for entry in entries:
        entry_type = entry.get("type")
        if entry_type == "gza" and entry.get("subtype") in {"branch", "stats", "outcome"}:
            continue

        rendered = renderer.handle_log(entry, live=False)
        details = [_timeline_detail_from_log_line(entry, line) for line in rendered.log_lines]
        details = [detail for detail in details if detail]

        if entry_type in {"gza", "raw"} and details:
            current_step = _append_timeline_step(steps, details[0])
            for detail in details[1:]:
                _append_substep(current_step, detail)
            continue

        if rendered.starts_step and details:
            message_text = details[0] if details else None
            current_step = _append_timeline_step(steps, message_text)
            for detail in details[1:]:
                _append_substep(current_step, detail)
            continue

        if not details:
            continue

        current_step = _ensure_current_step(steps, current_step)
        for detail in details:
            _append_substep(current_step, detail)

    return steps, renderer.suppressed_count


def _display_step_timeline(
    entries: list[dict],
    *,
    verbose: bool,
    provider: str | None = None,
    configured_model: str | None = None,
) -> int:
    """Render a step-first timeline in compact or verbose mode."""
    steps, suppressed_count = _build_step_timeline_with_metadata(
        entries,
        provider=provider,
        configured_model=configured_model,
    )
    if not steps:
        console.print("No step entries found.", soft_wrap=True)
        return suppressed_count

    for step in steps:
        title = f"[{_lc()}]\\[Step {step['step_id']}][/{_lc()}]"
        message_text = step.get("message_text")
        summary = step.get("summary")
        if message_text:
            console.print(f"{title} {rich_escape(message_text)}", soft_wrap=True)
        elif summary:
            console.print(f"{title} {rich_escape(summary)}", soft_wrap=True)
        else:
            console.print(title, soft_wrap=True)
        if verbose:
            for substep in step["substeps"]:
                console.print(f"  [green]\\[{substep['substep_id']}][/green] {rich_escape(substep['detail'])}", soft_wrap=True)
    return suppressed_count


class _LiveLogPrinter:
    """Thin adapter around provider-owned renderers."""

    def __init__(
        self,
        *,
        live: bool = True,
        provider: str | None = None,
        configured_model: str | None = None,
        verbose: bool = False,
    ) -> None:
        from ..providers.output_formatter import StreamOutputFormatter

        self._fmt = StreamOutputFormatter()
        self._console = self._fmt.console
        self._live = live
        self._renderer = get_log_renderer(provider, configured_model=configured_model, verbose=verbose)
        self._start_time: float | None = None

    @property
    def renderer(self):
        return self._renderer

    def process(self, entry: dict) -> bool:
        """Process a single JSON log entry and print it."""
        rendered = self._renderer.handle_log(entry, live=self._live)
        has_visible_output = bool(rendered.log_lines)
        if rendered.starts_step and has_visible_output:
            if self._start_time is None:
                self._start_time = time.time()
            if self._renderer.stats.step_count > 1:
                self._console.print()
            total_tokens = self._renderer.stats.input_tokens + self._renderer.stats.output_tokens
            if self._live:
                elapsed = int(time.time() - self._start_time)
                self._fmt.print_step_header(
                    self._renderer.stats.step_count,
                    total_tokens,
                    self._renderer.stats.cost_usd,
                    elapsed,
                    blank_line_before=False,
                )
            else:
                self._console.print(f"| Step {self._renderer.stats.step_count} |", style=blue)

        for line in rendered.log_lines:
            self._console.print(line, soft_wrap=True)
        return has_visible_output


def _format_log_entry(entry: dict) -> str | None:
    """Format a single JSON log entry for compatibility tests."""
    renderer = get_log_renderer(None, verbose=False)
    rendered = renderer.handle_log(entry, live=False)
    if not rendered.log_lines:
        return None
    return "\n".join(rendered.log_lines)


def _task_log_candidates(config: Config, task: DbTask) -> list[Path]:
    """Build ordered candidate log paths for a task."""
    candidates: list[Path] = []

    if task.log_file:
        path = Path(task.log_file)
        if not path.is_absolute():
            path = config.project_dir / path
        candidates.append(path)

    if task.slug:
        inferred = config.log_path / f"{task.slug}.log"
        candidates.append(inferred)

    deduped: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = str(path.resolve()) if path.exists() else str(path)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(path)
    return deduped


def _ops_log_candidates(conversation_candidates: list[Path]) -> list[Path]:
    """Build ordered sibling ops-log candidates for conversation log candidates."""
    candidates: list[Path] = []
    seen: set[str] = set()
    for conversation_path in conversation_candidates:
        ops_path = ops_log_path_for(conversation_path)
        key = str(ops_path.resolve()) if ops_path.exists() else str(ops_path)
        if key in seen:
            continue
        seen.add(key)
        candidates.append(ops_path)
    return candidates


def _first_existing(paths: list[Path]) -> Path | None:
    """Return the first existing path in order, if any."""
    for path in paths:
        if path.exists():
            return path
    return None


def _task_startup_log_path(config: Config, task: DbTask | None, worker: WorkerMetadata | None = None) -> Path | None:
    """Resolve deterministic startup log path from task slug, with legacy fallback."""
    if task and task.slug:
        deterministic = config.workers_path / f"{task.slug}.startup.log"
        if deterministic.exists() or ops_log_path_for(deterministic).exists():
            return deterministic
    if worker and worker.startup_log_file:
        startup_path = Path(worker.startup_log_file)
        if not startup_path.is_absolute():
            startup_path = config.project_dir / startup_path
        return startup_path
    if task and task.slug:
        return config.workers_path / f"{task.slug}.startup.log"
    return None


def _resolve_worker_log_path(
    config: Config,
    worker: WorkerMetadata,
    task: DbTask | None,
) -> tuple[Path | None, bool]:
    """Resolve log path for worker lookups, preferring main task logs then startup logs."""
    main_candidates: list[Path] = []

    if worker.log_file:
        worker_log = Path(worker.log_file)
        if not worker_log.is_absolute():
            worker_log = config.project_dir / worker_log
        main_candidates.append(worker_log)

    if task is not None:
        main_candidates.extend(_task_log_candidates(config, task))

    for candidate in main_candidates:
        if candidate.exists() or ops_log_path_for(candidate).exists():
            return candidate, False

    startup_log_path = _task_startup_log_path(config, task, worker)
    if startup_log_path and (startup_log_path.exists() or ops_log_path_for(startup_log_path).exists()):
        return startup_log_path, True

    # Prefer returning a main log candidate (even if missing) over a non-existent
    # startup log, so error messages reference the expected main log path.
    if main_candidates:
        return main_candidates[0], False
    return None, False


def _latest_worker_for_task(registry: WorkerRegistry, task_id: str) -> WorkerMetadata | None:
    """Return most recent worker metadata for a task."""
    workers = [w for w in registry.list_all(include_completed=True) if w.task_id == task_id]
    if not workers:
        return None
    workers.sort(key=lambda w: (_parse_iso(w.started_at) or datetime.min.replace(tzinfo=UTC), w.worker_id))
    return workers[-1]


def _resolve_task_log_path(
    config: Config,
    registry: WorkerRegistry,
    task: DbTask,
) -> tuple[Path | None, bool]:
    """Resolve log path for task/slug lookups with worker startup fallback."""
    main_candidates: list[Path] = []
    main_candidates.extend(_task_log_candidates(config, task))

    for candidate in main_candidates:
        if candidate.exists() or ops_log_path_for(candidate).exists():
            return candidate, False

    if task.id is not None:
        latest_worker = _latest_worker_for_task(registry, task.id)
        if latest_worker is not None:
            worker_log_path, using_startup_log = _resolve_worker_log_path(config, latest_worker, task)
            if worker_log_path is not None:
                return worker_log_path, using_startup_log

    if main_candidates:
        return main_candidates[0], False
    return None, False


def _running_worker_id_for_task(registry: WorkerRegistry, task_id: str) -> str | None:
    """Return a running worker ID for a task when available."""
    # Note: legacy worker JSON files created before the INTEGER→TEXT PK migration
    # may have task_id stored as a bare stringified integer (e.g. "123") rather than
    # the canonical prefixed form (e.g. "gza-123"). Such workers won't match here.
    # This is acceptable since worker metadata is ephemeral and old JSON files are
    # cleaned up after the worker process exits.
    workers = [w for w in registry.list_all(include_completed=True) if w.task_id == task_id]
    running = [w for w in workers if w.status == "running" and registry.is_running(w.worker_id)]
    if not running:
        return None
    running.sort(key=lambda w: (_parse_iso(w.started_at) or datetime.min.replace(tzinfo=UTC), w.worker_id))
    return running[-1].worker_id


def _load_log_file_entries(log_path: Path) -> tuple[dict | None, list[dict], str]:
    """Load log file as old JSON object or JSONL entries."""
    with open(log_path) as f:
        content = f.read().strip()

    log_data = None
    entries: list[dict] = []

    if not content:
        return None, [], content

    try:
        parsed = json.loads(content)
        if isinstance(parsed, dict):
            log_data = parsed
            if parsed.get("type"):
                entries.append(parsed)
            return log_data, entries, content
    except json.JSONDecodeError:
        pass

    for line in content.split("\n"):
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
            if isinstance(entry, dict):
                entries.append(entry)
                if entry.get("type") == "result":
                    log_data = entry
            else:
                entries.append({"type": "raw", "message": str(entry)})
        except json.JSONDecodeError:
            entries.append({"type": "raw", "message": line})

    return log_data, entries, content


def _annotate_stream(entries: list[dict], stream: str) -> list[dict]:
    """Return copies of entries annotated with their source stream."""
    annotated: list[dict] = []
    for entry in entries:
        payload = dict(entry)
        payload.setdefault("stream", stream)
        annotated.append(payload)
    return annotated


_MAX_SORT_DATETIME = datetime.max.replace(tzinfo=UTC)


def _entry_timestamp(entry: dict) -> tuple[int, datetime, int]:
    """Return a comparable merge key for timestamped and untimestamped entries."""
    raw = entry.get("timestamp")
    if isinstance(raw, str):
        parsed = _parse_iso(raw)
        if parsed is not None:
            return 0, parsed, int(entry.get("_merge_index", 0))
    return 1, _MAX_SORT_DATETIME, int(entry.get("_merge_index", 0))


def _merge_stream_entries(conversation_entries: list[dict], ops_entries: list[dict]) -> list[dict]:
    """Merge split conversation and ops entries using stable timestamp/read ordering."""
    merged_entries = _annotate_stream(conversation_entries, "conversation") + _annotate_stream(ops_entries, "ops")
    for idx, entry in enumerate(merged_entries):
        entry["_merge_index"] = idx
    merged_entries.sort(key=_entry_timestamp)
    return merged_entries


def _parse_stream_lines(raw_lines: list[str], stream: str) -> list[dict]:
    """Parse raw JSONL lines into annotated entries for one stream."""
    entries: list[dict] = []
    for raw_line in raw_lines:
        line = raw_line.strip()
        if not line or line.startswith("---"):
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            entries.append({"type": "raw", "message": line, "stream": stream})
            continue
        if isinstance(parsed, dict):
            entry = dict(parsed)
            entry.setdefault("stream", stream)
            entries.append(entry)
        else:
            entries.append({"type": "raw", "message": str(parsed), "stream": stream})
    return entries


def _load_selected_log_entries(
    conversation_log_path: Path | None,
    ops_log_path: Path | None,
    *,
    conversation_only: bool,
    ops_only: bool,
) -> tuple[dict | None, list[dict], str]:
    """Load conversation, ops, or merged entries based on CLI selection."""
    if ops_only:
        if ops_log_path is None or not ops_log_path.exists():
            return None, [], ""
        log_data, entries, content = _load_log_file_entries(ops_log_path)
        return log_data, _annotate_stream(entries, "ops"), content

    if conversation_only or ops_log_path is None or not ops_log_path.exists():
        if conversation_log_path is None or not conversation_log_path.exists():
            return None, [], ""
        log_data, entries, content = _load_log_file_entries(conversation_log_path)
        return log_data, _annotate_stream(entries, "conversation"), content

    conversation_data, conversation_entries, conversation_content = (
        _load_log_file_entries(conversation_log_path)
        if conversation_log_path is not None and conversation_log_path.exists()
        else (None, [], "")
    )
    ops_data, ops_entries, ops_content = _load_log_file_entries(ops_log_path)

    merged_entries = _merge_stream_entries(conversation_entries, ops_entries)
    return conversation_data or ops_data, merged_entries, "\n".join(
        part for part in (conversation_content, ops_content) if part
    )


def _read_follow_lines(path: Path | None) -> list[str]:
    """Return all lines from one follow target, or an empty list when absent."""
    if path is None or not path.exists():
        return []
    with open(path) as f:
        return f.readlines()


def _seed_follow_offset(
    offsets: dict[str, int],
    previous_path: Path | None,
    current_path: Path | None,
    *,
    current_line_count: int,
) -> None:
    """Initialize offsets for newly resolved paths without replaying renamed startup logs."""
    if current_path is None:
        return
    current_key = str(current_path)
    if current_key in offsets:
        return
    if previous_path is None:
        offsets[current_key] = 0
        return

    previous_key = str(previous_path)
    previous_count = offsets.get(previous_key, 0)
    if previous_path != current_path and current_line_count >= previous_count:
        offsets[current_key] = previous_count
        return
    offsets[current_key] = 0


def _resolve_follow_log_paths(
    args: argparse.Namespace,
    registry: WorkerRegistry,
    current_conversation_path: Path,
    current_ops_path: Path | None,
    *,
    task_id: str | None,
    store: SqliteTaskStore | None,
) -> tuple[Path, Path | None]:
    """Re-resolve follow targets so startup logs can promote to slug logs mid-tail."""
    if task_id is None or store is None:
        return current_conversation_path, current_ops_path

    project_dir = getattr(args, "project_dir", None)
    if project_dir is None:
        return current_conversation_path, current_ops_path

    latest_task = store.get(task_id)
    if latest_task is None:
        return current_conversation_path, current_ops_path

    config = Config.load(project_dir)
    resolved_conversation_path, _using_startup = _resolve_task_log_path(config, registry, latest_task)
    if resolved_conversation_path is None:
        return current_conversation_path, current_ops_path
    return resolved_conversation_path, ops_log_path_for(resolved_conversation_path)


def _print_log_header(
    *,
    task: DbTask | None,
    worker: WorkerMetadata | None,
    conversation_log_path: Path | None,
    ops_log_path: Path | None,
    is_running: bool,
    using_startup_log: bool,
) -> None:
    """Print the static task/worker header banner for ``gza log``."""
    _sep = f"[{_lc()}]" + "━" * 70 + f"[/{_lc()}]"
    console.print(_sep, soft_wrap=True)
    if task:
        prompt_display = task.prompt[:100] if task.prompt else "(no prompt)"
        console.print(f"[{pink}]Task: {rich_escape(prompt_display)}[/{pink}]", soft_wrap=True)
        console.print(f"[{_lc()}]ID:[/{_lc()}] {task.id} | [{_lc()}]Slug:[/{_lc()}] {rich_escape(task.slug or '')}", soft_wrap=True)
        _status_color = LINEAGE_STATUS_COLORS.get(task.status, "")
        _status_val = f"[{_status_color}]{rich_escape(task.status)}[/{_status_color}]" if _status_color else rich_escape(task.status)
        console.print(f"[{_lc()}]Status:[/{_lc()}] {_status_val}", soft_wrap=True)
        if conversation_log_path is not None:
            console.print(f"[{_lc()}]Transcript:[/{_lc()}] {rich_escape(str(conversation_log_path))}", soft_wrap=True)
        if ops_log_path is not None and (ops_log_path.exists() or not using_startup_log):
            console.print(f"[{_lc()}]Ops:[/{_lc()}] {rich_escape(str(ops_log_path))}", soft_wrap=True)
        if using_startup_log:
            console.print("[yellow]Using worker startup log (main task log not available).[/yellow]", soft_wrap=True)
        if task.branch:
            console.print(f"[{_lc()}]Branch:[/{_lc()}] {rich_escape(task.branch)}", soft_wrap=True)
    elif worker:
        console.print(f"[{_lc()}]Worker:[/{_lc()}] {rich_escape(worker.worker_id)}", soft_wrap=True)
        _w_status = worker.status if worker.status else "unknown"
        if is_running and _w_status != "running":
            # Prefer live process state when worker metadata is stale.
            _w_status = "running"
        _w_color = PS_STATUS_COLORS.get(_w_status, "white")
        console.print(f"[{_lc()}]Status:[/{_lc()}] [{_w_color}]{_w_status}[/{_w_color}]", soft_wrap=True)
        if conversation_log_path is not None:
            console.print(f"[{_lc()}]Transcript:[/{_lc()}] {rich_escape(str(conversation_log_path))}", soft_wrap=True)
        if ops_log_path is not None and ops_log_path.exists():
            console.print(f"[{_lc()}]Ops:[/{_lc()}] {rich_escape(str(ops_log_path))}", soft_wrap=True)
        if using_startup_log:
            console.print("[yellow]Using startup log (main task log not available).[/yellow]", soft_wrap=True)
    console.print(_sep, soft_wrap=True)
    console.print()


def _print_failure_focus(task: DbTask, log_path: Path, config: Config) -> None:
    """Print failure-first diagnostics for a failed task."""
    diagnostics = _build_failure_diagnostics(task, log_path, config.verify_command)
    _render_failure_diagnostics(
        diagnostics,
        label_color=_lc(),
        value_color=SHOW_COLORS_DICT["value"],
        status_failed_color=SHOW_COLORS_DICT["status_failed"],
        soft_wrap=True,
    )


def _print_unknown_log_provider_error(exc: UnknownLogProviderError) -> None:
    """Emit a stable operator-facing error for unsupported task log providers."""
    print(f"Error: {exc}")


def cmd_log(args: argparse.Namespace) -> int:
    """Display the log for a task or worker."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    registry = WorkerRegistry(config.workers_path)

    query = args.identifier
    task = None
    worker = None
    log_path = None
    ops_log_path: Path | None = None
    using_startup_log = False
    is_running = False
    worker_id_for_follow: str | None = None
    provider_name: str | None = None
    configured_model: str | None = None

    if args.worker:
        # Look up by worker ID
        worker = registry.get(query)
        if not worker:
            print(f"Error: Worker '{query}' not found")
            return 1
        is_running = registry.is_running(worker.worker_id)
        if is_running:
            worker_id_for_follow = worker.worker_id
        if worker.task_id:
            task = store.get(worker.task_id)
        log_path, using_startup_log = _resolve_worker_log_path(config, worker, task)

    elif args.slug:
        # Look up by slug (exact or partial match)
        task = store.get_by_slug(query)
        if not task:
            # Try partial match
            all_tasks = store.get_all()
            for t in all_tasks:
                if t.slug and query in t.slug:
                    task = t
                    break
        if not task:
            print(f"Error: No task found matching slug '{query}'")
            return 1

        log_path, using_startup_log = _resolve_task_log_path(config, registry, task)

        if task.id:
            worker_id_for_follow = _running_worker_id_for_task(registry, task.id)
            is_running = worker_id_for_follow is not None

    else:
        # Default: look up by task ID
        task_id: str = resolve_id(config, query)
        task = store.get(task_id)
        if not task:
            print(f"Error: Task {query} not found")
            return 1

        log_path, using_startup_log = _resolve_task_log_path(config, registry, task)

        if task.id:
            worker_id_for_follow = _running_worker_id_for_task(registry, task.id)
            is_running = worker_id_for_follow is not None

    if not log_path:
        print("Error: No log file found")
        return 1

    conversation_only = bool(getattr(args, "conversation_only", False))
    ops_only = bool(getattr(args, "ops_only", False))
    ops_log_path = ops_log_path_for(log_path)

    selected_exists = (
        (ops_log_path.exists() if ops_only and ops_log_path is not None else False)
        or (log_path.exists() if not ops_only else False)
        or (
            not conversation_only
            and not ops_only
            and ops_log_path is not None
            and ops_log_path.exists()
        )
    )
    if not selected_exists:
        if is_running and not using_startup_log:
            print(f"Log file not yet created: {log_path}")
            print("Worker is still starting up...")
        elif using_startup_log:
            print(f"Error: Startup log not found at {log_path}")
        else:
            print(f"Error: Log file not found at {log_path}")
        return 1

    # Determine mode: follow (live tail) vs static display
    follow = hasattr(args, 'follow') and args.follow
    if follow and not is_running:
        follow = False  # Can't follow a completed task

    if task is not None:
        provider_name = task.provider
        if provider_name is not None:
            configured_model = task.model or config.get_model_for_task(task.task_type, provider_name)
        else:
            configured_model = task.model

    # Check for raw mode
    raw_mode = hasattr(args, 'raw') and args.raw
    failure_only = bool(getattr(args, "failure", False))
    if failure_only and (follow or raw_mode):
        print("Error: --failure cannot be used with --follow or --raw")
        return 1

    if follow and not raw_mode:
        _print_log_header(
            task=task,
            worker=worker,
            conversation_log_path=log_path,
            ops_log_path=ops_log_path,
            is_running=is_running,
            using_startup_log=using_startup_log,
        )

    if follow or raw_mode:
        # Live streaming mode - use the formatted streaming output
        setattr(args, "_log_provider_name", provider_name)
        setattr(args, "_log_configured_model", configured_model)
        setattr(args, "_ops_log_path", ops_log_path)
        return _tail_log_file(
            log_path,
            args,
            registry,
            worker_id_for_follow if is_running else None,
            task.id if task else None,
            store if task else None,
        )

    use_page = getattr(args, 'page', False)
    with pager_context(use_page, config.project_dir):
        # Static display mode - show summary or full turns
        try:
            log_data, entries, content = _load_selected_log_entries(
                log_path,
                ops_log_path,
                conversation_only=conversation_only,
                ops_only=ops_only,
            )

            if log_data is None and not entries:
                # If we have content but couldn't parse any JSON, it's likely a startup error
                if content:
                    if using_startup_log:
                        console.print(f"[{_lc()}]Startup log:[/{_lc()}] {rich_escape(str(log_path))}", soft_wrap=True)
                        console.print("[yellow]Using startup log (main task log not available).[/yellow]", soft_wrap=True)
                    console.print("[red]Task failed during startup (no Claude session):[/red]", soft_wrap=True)
                    # Display the raw error message, indented for clarity
                    for line in content.split('\n'):
                        console.print(f"  {rich_escape(line)}", soft_wrap=True)
                    return 1
                else:
                    console.print("Error: No log entries found in log file", soft_wrap=True)
                    return 1
        except Exception as e:
            print(f"Error: Failed to read log file: {e}")
            return 1

        # Display header
        _print_log_header(
            task=task,
            worker=worker,
            conversation_log_path=log_path,
            ops_log_path=ops_log_path,
            is_running=is_running,
            using_startup_log=using_startup_log,
        )

        if failure_only:
            if task is None:
                print("Error: --failure requires a task target")
                return 1
            if task.status != "failed":
                print(f"Error: Task {task.id} is not failed")
                return 1
            _print_failure_focus(task, log_path, config)
            return 0

        _sep = f"[{_lc()}]" + "━" * 70 + f"[/{_lc()}]"

        timeline_mode = getattr(args, "timeline_mode", None)
        suppressed_count = 0
        try:
            if timeline_mode and entries:
                suppressed_count = _display_step_timeline(
                    entries,
                    verbose=timeline_mode == "verbose",
                    provider=provider_name,
                    configured_model=configured_model,
                )
            elif entries:
                printer = _LiveLogPrinter(
                    live=False,
                    provider=provider_name,
                    configured_model=configured_model,
                    verbose=bool(getattr(args, "verbose", False)),
                )
                any_printed = False
                for entry in entries:
                    any_printed = printer.process(entry) or any_printed
                if not any_printed:
                    if log_data:
                        if "result" in log_data:
                            console.print(rich_escape(log_data["result"]), soft_wrap=True)
                        else:
                            subtype = log_data.get("subtype", "unknown")
                            console.print(f"Run ended with: {rich_escape(subtype)}", soft_wrap=True)
                            if log_data.get("errors"):
                                console.print(f"[red]Errors:[/red] {rich_escape(str(log_data['errors']))}", soft_wrap=True)
                    else:
                        console.print("No displayable log entries found.", soft_wrap=True)
            elif log_data:
                # Extract and display the result field (which contains markdown)
                if "result" in log_data:
                    console.print(rich_escape(log_data["result"]), soft_wrap=True)
                else:
                    # No result - show the subtype (e.g., error_max_turns)
                    subtype = log_data.get("subtype", "unknown")
                    console.print(f"Run ended with: {rich_escape(subtype)}", soft_wrap=True)
                    if log_data.get("errors"):
                        console.print(f"[red]Errors:[/red] {rich_escape(str(log_data['errors']))}", soft_wrap=True)
            else:
                # No result entry yet - show compact step timeline
                suppressed_count = _display_step_timeline(
                    entries,
                    verbose=False,
                    provider=provider_name,
                    configured_model=configured_model,
                )
        except UnknownLogProviderError as exc:
            _print_unknown_log_provider_error(exc)
            return 1

        if entries and timeline_mode is None and "printer" in locals():
            suppressed_count = printer.renderer.suppressed_count
        if entries and suppressed_count:
            console.print(
                f"({suppressed_count} routine events suppressed; rerun with --raw to see them)",
                soft_wrap=True,
            )

        console.print()
        console.print(_sep, soft_wrap=True)

        # Display summary stats if available
        if log_data:
            if "duration_ms" in log_data:
                duration_sec = log_data["duration_ms"] / 1000
                console.print(f"[{_lc()}]Duration:[/{_lc()}] {format_duration(duration_sec, verbose=True)}", soft_wrap=True)
            step_count = _result_step_count(log_data)
            if step_count is not None:
                console.print(f"[{_lc()}]Steps:[/{_lc()}] {step_count}", soft_wrap=True)
                if "num_steps" not in log_data and "num_steps_reported" not in log_data and "num_turns" in log_data:
                    console.print(f"[{_lc()}]Legacy turns:[/{_lc()}] {log_data['num_turns']}", soft_wrap=True)
            if "total_cost_usd" in log_data:
                console.print(f"[{_lc()}]Cost:[/{_lc()}] ${log_data['total_cost_usd']:.4f}", soft_wrap=True)

        return 0


def _tail_log_file(
    log_path: Path,
    args: argparse.Namespace,
    registry: WorkerRegistry,
    worker_id: str | None,
    task_id: str | None = None,
    store: SqliteTaskStore | None = None,
) -> int:
    """Tail a log file with optional follow mode."""
    raw_mode = hasattr(args, 'raw') and args.raw
    follow = hasattr(args, 'follow') and args.follow
    conversation_only = bool(getattr(args, "conversation_only", False))
    ops_only = bool(getattr(args, "ops_only", False))
    ops_log_path: Path | None = getattr(args, "_ops_log_path", None)

    if raw_mode:
        try:
            current_conversation_path = log_path
            current_ops_path = ops_log_path
            stream_offsets: dict[str, int] = {}

            def _emit_raw_entries() -> tuple[int, int]:
                _log_data, entries, _content = _load_selected_log_entries(
                    current_conversation_path,
                    current_ops_path,
                    conversation_only=conversation_only,
                    ops_only=ops_only,
                )
                tail_lines = args.tail if hasattr(args, 'tail') and args.tail else None
                rendered_entries = entries[-tail_lines:] if tail_lines else entries
                for entry in rendered_entries:
                    print(json.dumps(entry))
                conv_count = 0
                ops_count = 0
                if current_conversation_path.exists() and not ops_only:
                    conv_count = len(current_conversation_path.read_text().splitlines())
                if current_ops_path is not None and current_ops_path.exists() and not conversation_only:
                    ops_count = len(current_ops_path.read_text().splitlines())
                return conv_count, ops_count

            conv_count, ops_count = _emit_raw_entries()
            if not ops_only:
                stream_offsets[str(current_conversation_path)] = conv_count
            if not conversation_only and current_ops_path is not None:
                stream_offsets[str(current_ops_path)] = ops_count
            if not follow:
                return 0

            while True:
                time.sleep(0.5)
                previous_conversation_path = log_path
                previous_ops_path = ops_log_path
                current_conversation_path, current_ops_path = _resolve_follow_log_paths(
                    args,
                    registry,
                    current_conversation_path,
                    current_ops_path,
                    task_id=task_id,
                    store=store,
                )
                new_conversation_entries: list[dict] = []
                new_ops_entries: list[dict] = []
                conv_lines = current_conversation_path.read_text().splitlines() if current_conversation_path.exists() else []
                _seed_follow_offset(
                    stream_offsets,
                    previous_conversation_path,
                    current_conversation_path,
                    current_line_count=len(conv_lines),
                )
                conv_count = stream_offsets.get(str(current_conversation_path), 0)
                if not ops_only and len(conv_lines) > conv_count:
                    for line in conv_lines[conv_count:]:
                        line = line.strip()
                        if not line:
                            continue
                        new_conversation_entries.extend(_parse_stream_lines([line], "conversation"))
                    stream_offsets[str(current_conversation_path)] = len(conv_lines)
                ops_lines = current_ops_path.read_text().splitlines() if current_ops_path is not None and current_ops_path.exists() else []
                _seed_follow_offset(
                    stream_offsets,
                    previous_ops_path,
                    current_ops_path,
                    current_line_count=len(ops_lines),
                )
                ops_count = stream_offsets.get(str(current_ops_path), 0) if current_ops_path is not None else 0
                if not conversation_only and current_ops_path is not None and len(ops_lines) > ops_count:
                    for line in ops_lines[ops_count:]:
                        line = line.strip()
                        if not line:
                            continue
                        new_ops_entries.extend(_parse_stream_lines([line], "ops"))
                    stream_offsets[str(current_ops_path)] = len(ops_lines)
                log_path = current_conversation_path
                ops_log_path = current_ops_path
                new_entries = (
                    new_ops_entries
                    if ops_only
                    else new_conversation_entries
                    if conversation_only
                    else _merge_stream_entries(new_conversation_entries, new_ops_entries)
                )
                if new_entries:
                    for entry in new_entries:
                        print(json.dumps(entry))
                    continue

                if worker_id and not registry.is_running(worker_id):
                    break
                if task_id is not None and store is not None and worker_id is None:
                    latest_task = store.get(task_id)
                    if latest_task is None or latest_task.status != "in_progress":
                        break
            return 0
        except KeyboardInterrupt:
            return 0
        except Exception as e:
            print(f"Error tailing log: {e}")
            return 1

    # Formatted output mode
    try:
        tail_lines = args.tail if hasattr(args, 'tail') and args.tail else None
        printer = _LiveLogPrinter(
            provider=getattr(args, "_log_provider_name", None),
            configured_model=getattr(args, "_log_configured_model", None),
            verbose=bool(getattr(args, "verbose", False)),
        )

        def _process_entries(entries: list[dict]) -> None:
            for entry in entries:
                printer.process(entry)

        def _merged_batch(conversation_batch: list[str], ops_batch: list[str]) -> list[dict]:
            if ops_only:
                return _parse_stream_lines(ops_batch, "ops")
            if conversation_only:
                return _parse_stream_lines(conversation_batch, "conversation")
            return _merge_stream_entries(
                _parse_stream_lines(conversation_batch, "conversation"),
                _parse_stream_lines(ops_batch, "ops"),
            )

        current_conversation_path = log_path
        current_ops_path = ops_log_path if not conversation_only else None
        stream_offsets = {}

        conv_lines = _read_follow_lines(current_conversation_path)
        ops_lines = _read_follow_lines(current_ops_path)
        initial_entries = _merged_batch(conv_lines, ops_lines)
        if tail_lines:
            initial_entries = initial_entries[-tail_lines:]
        _process_entries(initial_entries)

        if not follow:
            if printer.renderer.suppressed_count:
                console.print(
                    f"({printer.renderer.suppressed_count} routine events suppressed; rerun with --raw to see them)",
                    soft_wrap=True,
                )
            return 0

        if not ops_only:
            stream_offsets[str(current_conversation_path)] = len(conv_lines)
        if not conversation_only and current_ops_path is not None:
            stream_offsets[str(current_ops_path)] = len(ops_lines)

        while True:
            time.sleep(0.5)
            current_conversation_path, resolved_ops_path = _resolve_follow_log_paths(
                args,
                registry,
                current_conversation_path,
                current_ops_path,
                task_id=task_id,
                store=store,
            )
            current_ops_path = None if conversation_only else resolved_ops_path

            conv_lines_now = _read_follow_lines(current_conversation_path)
            ops_lines_now = _read_follow_lines(current_ops_path)
            _seed_follow_offset(
                stream_offsets,
                log_path,
                current_conversation_path,
                current_line_count=len(conv_lines_now),
            )
            if current_ops_path is not None:
                _seed_follow_offset(
                    stream_offsets,
                    ops_log_path,
                    current_ops_path,
                    current_line_count=len(ops_lines_now),
                )
            new_conv_lines: list[str] = []
            new_ops_lines: list[str] = []
            conv_count = stream_offsets.get(str(current_conversation_path), 0)
            ops_count = stream_offsets.get(str(current_ops_path), 0) if current_ops_path is not None else 0
            if not ops_only and len(conv_lines_now) > conv_count:
                new_conv_lines = conv_lines_now[conv_count:]
                stream_offsets[str(current_conversation_path)] = len(conv_lines_now)
            if not conversation_only and current_ops_path is not None and len(ops_lines_now) > ops_count:
                new_ops_lines = ops_lines_now[ops_count:]
                stream_offsets[str(current_ops_path)] = len(ops_lines_now)
            new_entries = _merged_batch(new_conv_lines, new_ops_lines)
            if new_entries:
                _process_entries(new_entries)
            log_path = current_conversation_path
            ops_log_path = current_ops_path

            # Check if worker is still running
            if worker_id and not registry.is_running(worker_id):
                time.sleep(0.5)
                final_new_conv_lines: list[str] = []
                final_new_ops_lines: list[str] = []
                current_conversation_path, resolved_ops_path = _resolve_follow_log_paths(
                    args,
                    registry,
                    current_conversation_path,
                    current_ops_path,
                    task_id=task_id,
                    store=store,
                )
                current_ops_path = None if conversation_only else resolved_ops_path
                conv_lines_now = _read_follow_lines(current_conversation_path)
                _seed_follow_offset(
                    stream_offsets,
                    log_path,
                    current_conversation_path,
                    current_line_count=len(conv_lines_now),
                )
                conv_count = stream_offsets.get(str(current_conversation_path), 0)
                if not ops_only and len(conv_lines_now) > conv_count:
                    final_new_conv_lines = conv_lines_now[conv_count:]
                if not conversation_only and current_ops_path is not None:
                    ops_lines_now = _read_follow_lines(current_ops_path)
                    _seed_follow_offset(
                        stream_offsets,
                        ops_log_path,
                        current_ops_path,
                        current_line_count=len(ops_lines_now),
                    )
                    ops_count = stream_offsets.get(str(current_ops_path), 0)
                    if len(ops_lines_now) > ops_count:
                        final_new_ops_lines = ops_lines_now[ops_count:]
                _process_entries(_merged_batch(final_new_conv_lines, final_new_ops_lines))
                break

            # Fallback for task-based follow without a running worker ID.
            if task_id is not None and store is not None and worker_id is None:
                latest_task = store.get(task_id)
                if latest_task is None or latest_task.status != "in_progress":
                    time.sleep(0.5)
                    task_final_new_conv_lines: list[str] = []
                    task_final_new_ops_lines: list[str] = []
                    current_conversation_path, resolved_ops_path = _resolve_follow_log_paths(
                        args,
                        registry,
                        current_conversation_path,
                        current_ops_path,
                        task_id=task_id,
                        store=store,
                    )
                    current_ops_path = None if conversation_only else resolved_ops_path
                    conv_lines_now = _read_follow_lines(current_conversation_path)
                    _seed_follow_offset(
                        stream_offsets,
                        log_path,
                        current_conversation_path,
                        current_line_count=len(conv_lines_now),
                    )
                    conv_count = stream_offsets.get(str(current_conversation_path), 0)
                    if not ops_only and len(conv_lines_now) > conv_count:
                        task_final_new_conv_lines = conv_lines_now[conv_count:]
                    if not conversation_only and current_ops_path is not None:
                        ops_lines_now = _read_follow_lines(current_ops_path)
                        _seed_follow_offset(
                            stream_offsets,
                            ops_log_path,
                            current_ops_path,
                            current_line_count=len(ops_lines_now),
                        )
                        ops_count = stream_offsets.get(str(current_ops_path), 0)
                        if len(ops_lines_now) > ops_count:
                            task_final_new_ops_lines = ops_lines_now[ops_count:]
                    _process_entries(_merged_batch(task_final_new_conv_lines, task_final_new_ops_lines))
                    break

        if printer.renderer.suppressed_count:
            console.print(
                f"({printer.renderer.suppressed_count} routine events suppressed; rerun with --raw to see them)",
                soft_wrap=True,
            )
        return 0

    except UnknownLogProviderError as exc:
        _print_unknown_log_provider_error(exc)
        return 1
    except KeyboardInterrupt:
        return 0
    except Exception as e:
        print(f"Error tailing log: {e}")
        return 1


def _display_conversation_turns(entries: list[dict]) -> None:
    """Deprecated compatibility wrapper for legacy call sites."""
    _display_step_timeline(entries, verbose=True)
