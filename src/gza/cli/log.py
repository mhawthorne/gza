"""Log display and timeline rendering for ``gza log``."""

import argparse
import json
import re
import subprocess
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


def _task_startup_log_path(config: Config, task: DbTask | None, worker: WorkerMetadata | None = None) -> Path | None:
    """Resolve deterministic startup log path from task slug, with legacy fallback."""
    if task and task.slug:
        deterministic = config.workers_path / f"{task.slug}.startup.log"
        if deterministic.exists():
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
        if candidate.exists():
            return candidate, False

    startup_log_path = _task_startup_log_path(config, task, worker)
    if startup_log_path and startup_log_path.exists():
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
        if candidate.exists():
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


def _print_log_header(
    *,
    task: DbTask | None,
    worker: WorkerMetadata | None,
    log_path: Path,
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
        console.print(f"[{_lc()}]Log:[/{_lc()}] {rich_escape(str(log_path))}", soft_wrap=True)
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
        console.print(f"[{_lc()}]Log:[/{_lc()}] {rich_escape(str(log_path))}", soft_wrap=True)
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
        is_running = registry.is_running(query)
        if worker.task_id:
            task = store.get(worker.task_id)
        log_path, using_startup_log = _resolve_worker_log_path(config, worker, task)
        worker_id_for_follow = worker.worker_id

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

        if task.id is not None:
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

        if task.id is not None:
            worker_id_for_follow = _running_worker_id_for_task(registry, task.id)
            is_running = worker_id_for_follow is not None

    if not log_path:
        print("Error: No log file found")
        return 1

    if not log_path.exists():
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
            log_path=log_path,
            is_running=is_running,
            using_startup_log=using_startup_log,
        )

    if follow or raw_mode:
        # Live streaming mode - use the formatted streaming output
        setattr(args, "_log_provider_name", provider_name)
        setattr(args, "_log_configured_model", configured_model)
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
            log_data, entries, content = _load_log_file_entries(log_path)

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
            log_path=log_path,
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

    if raw_mode:
        # Use tail directly for raw JSON output
        try:
            cmd = ["tail"]
            if hasattr(args, 'tail') and args.tail:
                cmd.extend(["-n", str(args.tail)])
            if follow:
                cmd.append("-f")
            cmd.append(str(log_path))
            subprocess.run(cmd)
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

        def _process_lines(raw_lines: list[str]) -> None:
            """Parse JSON lines and feed them to the live printer."""
            for line in raw_lines:
                line = line.strip()
                if not line:
                    continue
                if line.startswith("---"):
                    # Skip step timestamp markers written by the runner
                    continue
                try:
                    entry = json.loads(line)
                    printer.process(entry)
                except json.JSONDecodeError:
                    console.print(rich_escape(line), soft_wrap=True)

        # Initial read
        with open(log_path) as f:
            lines = f.readlines()
        if tail_lines:
            lines = lines[-tail_lines:]
        _process_lines(lines)

        if not follow:
            if printer.renderer.suppressed_count:
                console.print(
                    f"({printer.renderer.suppressed_count} routine events suppressed; rerun with --raw to see them)",
                    soft_wrap=True,
                )
            return 0

        # Follow mode - watch for new lines
        last_size = log_path.stat().st_size
        with open(log_path) as f:
            last_line_count = sum(1 for _ in f)

        while True:
            time.sleep(0.5)

            current_size = log_path.stat().st_size
            if current_size > last_size:
                with open(log_path) as f:
                    lines = f.readlines()

                new_lines = lines[last_line_count:]
                last_line_count = len(lines)
                last_size = current_size
                _process_lines(new_lines)

            # Check if worker is still running
            if worker_id and not registry.is_running(worker_id):
                time.sleep(0.5)
                with open(log_path) as f:
                    lines = f.readlines()
                _process_lines(lines[last_line_count:])
                break

            # Fallback for task-based follow without a running worker ID.
            if task_id is not None and store is not None and worker_id is None:
                latest_task = store.get(task_id)
                if latest_task is None or latest_task.status != "in_progress":
                    time.sleep(0.5)
                    with open(log_path) as f:
                        lines = f.readlines()
                    _process_lines(lines[last_line_count:])
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
