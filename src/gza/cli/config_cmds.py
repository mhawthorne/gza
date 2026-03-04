"""Configuration, stats, cleanup, init, import, and skills-install CLI commands."""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from ..config import Config
from ..console import console, format_duration, get_terminal_width
from ..db import SqliteTaskStore
from ..git import Git
from ..importer import import_tasks, parse_import_file, validate_import
from ..learnings import DEFAULT_LEARNINGS_WINDOW, regenerate_learnings
from ..tasks import YamlTaskStore
from ..workers import WorkerMetadata, WorkerRegistry

from ._common import TASK_COLORS, get_store, get_task_step_count


def _format_percentile_row(label: str, pdata: dict | None) -> str:
    """Format a percentile stats row for display."""
    if pdata is None:
        return f"  {label:<28} (no data)"
    return (
        f"  {label:<28} min={pdata['min']:.1f}  avg={pdata['avg']:.1f}  "
        f"median={pdata['median']:.1f}  p90={pdata['p90']:.1f}  max={pdata['max']:.1f}  "
        f"(n={pdata['count']})"
    )


def _cmd_stats_cycles(config: Config, store: "SqliteTaskStore", as_json: bool) -> int:
    """Show project-wide cycle analytics."""
    agg = store.get_cycle_aggregate_stats()

    if as_json:
        print(json.dumps(agg, indent=2))
        return 0

    total = agg["total_cycles"]
    approved = agg["approved_cycles"]
    print("Cycle Analytics")
    print("=" * 60)
    print(f"  Total cycles:    {total}")
    print(f"  Approved:        {approved}")
    if total > 0:
        other = total - approved
        print(f"  Other (blocked/maxed): {other}")
    print()
    if total == 0:
        print("  No cycles found. Run 'gza iterate <impl-id>' to start one.")
        return 0

    print("  Improves before approval (approved cycles only):")
    print(_format_percentile_row("improves_before_approval", agg["improves_before_approval"]))
    print()
    print("  Per-cycle review/improve counts (all closed cycles):")
    print(_format_percentile_row("reviews_per_cycle", agg["reviews_per_cycle"]))
    print(_format_percentile_row("improves_per_cycle", agg["improves_per_cycle"]))
    print()
    print("  Cycle duration (seconds, all closed cycles):")
    print(_format_percentile_row("cycle_duration_seconds", agg["cycle_duration_seconds"]))
    return 0


def _cmd_stats_cycles_task(config: Config, store: "SqliteTaskStore", impl_task_id: int, as_json: bool) -> int:
    """Show per-implementation cycle analytics."""
    impl_task = store.get(impl_task_id)
    if not impl_task:
        print(f"Error: Task #{impl_task_id} not found")
        return 1

    cycles = store.get_cycles_for_impl(impl_task_id)

    if as_json:
        result: dict = {
            "impl_task_id": impl_task_id,
            "cycle_count": len(cycles),
            "cycles": [],
        }
        for cycle in cycles:
            iters = store.get_cycle_iterations(cycle.id)
            result["cycles"].append({
                "id": cycle.id,
                "status": cycle.status,
                "stop_reason": cycle.stop_reason,
                "max_iterations": cycle.max_iterations,
                "started_at": cycle.started_at.isoformat(),
                "ended_at": cycle.ended_at.isoformat() if cycle.ended_at else None,
                "iterations": [
                    {
                        "iteration_index": it.iteration_index,
                        "review_task_id": it.review_task_id,
                        "review_verdict": it.review_verdict,
                        "improve_task_id": it.improve_task_id,
                        "state": it.state,
                    }
                    for it in iters
                ],
            })
        print(json.dumps(result, indent=2))
        return 0

    print(f"Cycle History for Implementation #{impl_task_id}")
    print(f"  Prompt: {impl_task.prompt[:80]}{'...' if len(impl_task.prompt) > 80 else ''}")
    print("=" * 60)
    if not cycles:
        print("  No cycles found.")
        return 0

    for cycle in cycles:
        iters = store.get_cycle_iterations(cycle.id)
        duration_str = ""
        if cycle.ended_at and cycle.started_at:
            duration_s = (cycle.ended_at - cycle.started_at).total_seconds()
            duration_str = f"  ({format_duration(duration_s, verbose=True)})"
        print(f"\nCycle #{cycle.id}  status={cycle.status}  stop={cycle.stop_reason or '-'}{duration_str}")
        print(f"  {'Iter':<6} {'Review':>8} {'Verdict':<22} {'Improve':>8} State")
        for it in iters:
            iter_str = str(it.iteration_index + 1)
            rev_str = f"#{it.review_task_id}" if it.review_task_id else "-"
            verdict_str = it.review_verdict or "-"
            imp_str = f"#{it.improve_task_id}" if it.improve_task_id else "-"
            print(f"  {iter_str:<6} {rev_str:>8} {verdict_str:<22} {imp_str:>8} {it.state}")
    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    """Show cost and usage statistics."""
    from gza.query import HistoryFilter, query_history

    config = Config.load(args.project_dir)
    store = get_store(config)

    show_cycles: bool = getattr(args, 'cycles', False)
    as_json: bool = getattr(args, 'json', False)
    cycle_task_id: int | None = getattr(args, 'cycle_task_id', None)

    # --cycles mode: show cycle analytics
    if show_cycles:
        if cycle_task_id is not None:
            return _cmd_stats_cycles_task(config, store, cycle_task_id, as_json)
        return _cmd_stats_cycles(config, store, as_json)

    # Build filter from shared query args
    limit: int | None = None if getattr(args, 'all', False) else getattr(args, 'last', 5)
    task_type: str | None = getattr(args, 'type', None)
    days: int | None = getattr(args, 'days', None)
    start_date: str | None = getattr(args, 'start_date', None)
    end_date: str | None = getattr(args, 'end_date', None)

    f = HistoryFilter(
        limit=limit,
        task_type=task_type,
        days=days,
        start_date=start_date,
        end_date=end_date,
    )
    tasks = query_history(store, f)

    if not tasks:
        console.print("No completed, failed, or dropped tasks")
        return 0

    if as_json:
        json_tasks = [
            {
                "id": t.id,
                "task_id": t.task_id,
                "status": t.status,
                "task_type": t.task_type,
                "prompt": t.prompt,
                "cost_usd": t.cost_usd,
                "duration_seconds": t.duration_seconds,
                "created_at": t.created_at.isoformat() if t.created_at else None,
                "completed_at": t.completed_at.isoformat() if t.completed_at else None,
            }
            for t in tasks
        ]
        print(json.dumps(json_tasks, indent=2))
        return 0

    # Compute summary from filtered task list
    c = TASK_COLORS
    n_completed = sum(1 for t in tasks if t.status == "completed")
    n_failed = sum(1 for t in tasks if t.status == "failed")
    n_dropped = sum(1 for t in tasks if t.status == "dropped")
    total_cost = sum(t.cost_usd or 0 for t in tasks)
    total_duration = sum(t.duration_seconds or 0 for t in tasks)
    total_steps = sum((get_task_step_count(t) or 0) for t in tasks)
    tasks_with_cost = n_completed + n_failed + n_dropped
    avg_cost = total_cost / tasks_with_cost if tasks_with_cost else 0

    # Section header
    console.print(f"[{c['header']}]Summary[/{c['header']}]")
    console.print("=" * 50)
    dropped_str = f", [{c['failure']}]{n_dropped} dropped[/{c['failure']}]" if n_dropped > 0 else ""
    console.print(
        f"  [{c['label']}]Tasks:[/{c['label']}]       "
        f"  [{c['success']}]{n_completed} completed[/{c['success']}]"
        f", [{c['failure']}]{n_failed} failed[/{c['failure']}]"
        f"{dropped_str}"
    )
    console.print(
        f"  [{c['label']}]Total cost:[/{c['label']}]   [{c['value']}]${total_cost:.2f}[/{c['value']}]"
    )
    console.print(
        f"  [{c['label']}]Total time:[/{c['label']}]   [{c['value']}]{format_duration(total_duration, verbose=True)}[/{c['value']}]"
    )
    console.print(
        f"  [{c['label']}]Total steps:[/{c['label']}]  [{c['value']}]{total_steps}[/{c['value']}]"
    )
    if tasks_with_cost:
        console.print(
            f"  [{c['label']}]Avg cost:[/{c['label']}]     [{c['value']}]${avg_cost:.2f}/task[/{c['value']}]"
        )
    console.print()

    # Task table
    from ..console import get_terminal_width
    terminal_width = get_terminal_width()
    table_width = int(terminal_width * 0.8)

    # Fixed column widths
    status_width = 8
    id_width = 6
    type_width = 10
    cost_width = 8
    turns_width = 6
    time_width = 8
    len_width = 5

    # Calculate remaining space for prompt column
    fixed_width = status_width + id_width + type_width + cost_width + turns_width + time_width + len_width + 7
    prompt_width = max(20, table_width - fixed_width)

    label = "All" if getattr(args, 'all', False) else f"Last {len(tasks)}"
    console.print(f"[{c['header']}]{label} Tasks[/{c['header']}]")
    console.print("=" * 50)

    # Table header
    console.print(f"{'Status':<{status_width}} {'ID':>{id_width}} {'Type':<{type_width}} {'Cost':>{cost_width}} {'Steps':>{turns_width}} {'Time':>{time_width}} {'Len':>{len_width}}  Prompt")
    console.print("-" * table_width)

    for task in tasks:
        is_ok = task.status == "completed"
        status_str = "✓" if is_ok else "✗"
        status_col = (
            f"[{c['success']}]{status_str:<{status_width}}[/{c['success']}]" if is_ok
            else f"[{c['failure']}]{status_str:<{status_width}}[/{c['failure']}]"
        )
        id_str = f"#{task.id}" if task.id is not None else "-"
        type_str = task.task_type[:type_width] if task.task_type else "-"
        cost_str = f"${task.cost_usd:.4f}" if task.cost_usd is not None else "-"
        resolved_steps = get_task_step_count(task)
        turns_str = str(resolved_steps) if resolved_steps is not None else "-"
        time_str = format_duration(task.duration_seconds, verbose=True) if task.duration_seconds else "-"
        prompt_len = len(task.prompt)
        len_str = str(prompt_len)
        prompt = task.prompt
        if len(prompt) > prompt_width:
            prompt = prompt[:prompt_width - 3] + "..."

        # Use console.print for colorized status; pad manually to preserve alignment
        id_col = f"[{c['task_id']}]{id_str:>{id_width}}[/{c['task_id']}]"
        type_col = f"[{c['stats']}]{type_str:<{type_width}}[/{c['stats']}]"
        prompt_col = f"[{c['prompt']}]{prompt}[/{c['prompt']}]"
        console.print(
            f"{status_col} {id_col} {type_col} {cost_str:>{cost_width}} {turns_str:>{turns_width}} {time_str:>{time_width}} {len_str:>{len_width}}  {prompt_col}"
        )

    console.print()
    console.print(
        f"[{c['label']}]Total for shown:[/{c['label']}] [{c['value']}]${sum(t.cost_usd or 0 for t in tasks):.2f}[/{c['value']}]"
    )

    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    """Validate the gza.yaml configuration file."""
    is_valid, errors, warnings = Config.validate(args.project_dir)

    # Print warnings first
    for warning in warnings:
        print(f"⚠ Warning: {warning}")

    if is_valid:
        print("✓ Configuration is valid")
        return 0
    else:
        print("✗ Configuration validation failed:")
        for error in errors:
            print(f"  - {error}")
        return 1


def _config_to_effective_dict(config: Config) -> dict:
    """Build an effective configuration dict from a loaded Config object."""
    return {
        "project_name": config.project_name,
        "tasks_file": config.tasks_file,
        "log_dir": config.log_dir,
        "use_docker": config.use_docker,
        "docker_image": config.docker_image,
        "docker_volumes": config.docker_volumes,
        "docker_setup_command": config.docker_setup_command,
        "timeout_minutes": config.timeout_minutes,
        "branch_mode": config.branch_mode,
        "max_steps": config.max_steps,
        "max_turns": config.max_turns,
        "worktree_dir": config.worktree_dir,
        "work_count": config.work_count,
        "provider": config.provider,
        "task_providers": config.task_providers,
        "model": config.model,
        "chat_text_display_length": config.chat_text_display_length,
        "verify_command": config.verify_command,
        "claude": {
            "fetch_auth_token_from_keychain": config.claude.fetch_auth_token_from_keychain,
            "args": config.claude.args,
        },
        "task_types": {
            task_type: {
                "model": task_cfg.model,
                "max_steps": task_cfg.max_steps,
                "max_turns": task_cfg.max_turns,
            }
            for task_type, task_cfg in config.task_types.items()
        },
        "providers": {
            provider_name: {
                "model": provider_cfg.model,
                "task_types": {
                    task_type: {
                        "model": task_cfg.model,
                        "max_steps": task_cfg.max_steps,
                        "max_turns": task_cfg.max_turns,
                    }
                    for task_type, task_cfg in provider_cfg.task_types.items()
                },
            }
            for provider_name, provider_cfg in config.providers.items()
        },
        "branch_strategy": {
            "pattern": config.branch_strategy.pattern if config.branch_strategy else None,
            "default_type": config.branch_strategy.default_type if config.branch_strategy else None,
        },
    }


def _flatten_dict(data: dict, prefix: str = "") -> list[tuple[str, object]]:
    """Flatten nested dictionaries into dotted key paths."""
    flattened: list[tuple[str, object]] = []
    for key, value in data.items():
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            flattened.extend(_flatten_dict(value, path))
        else:
            flattened.append((path, value))
    return flattened


def _resolve_source_for_effective_path(path: str, source_map: dict[str, str]) -> str:
    """Resolve source attribution for an effective config path.

    Effective output can contain normalized/derived leaf keys (for example
    ``branch_strategy.pattern``), while ``source_map`` may only track the
    originating parent key (for example ``branch_strategy``).
    """
    if path in source_map:
        return source_map[path]

    parent = path
    while "." in parent:
        parent = parent.rsplit(".", 1)[0]
        if parent in source_map:
            return source_map[parent]

    return "default"


def _project_effective_source_map(effective: dict, source_map: dict[str, str]) -> dict[str, str]:
    """Project raw source_map keys onto effective config paths."""
    projected: dict[str, str] = {}
    for path, _value in _flatten_dict(effective):
        projected[path] = _resolve_source_for_effective_path(path, source_map)
    return projected


def cmd_config(args: argparse.Namespace) -> int:
    """Show effective config with source attribution."""
    config = Config.load(args.project_dir)
    effective = _config_to_effective_dict(config)
    effective_sources = _project_effective_source_map(effective, config.source_map)

    if args.json:
        payload = {
            "effective": effective,
            "sources": effective_sources,
            "local_overrides_active": config.local_overrides_active,
            "local_override_file": (
                config.local_override_path.name if config.local_override_path else None
            ),
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    print("Effective Configuration")
    print("=" * 50)
    if config.local_overrides_active and config.local_override_path:
        print(f"Local overrides: active ({config.local_override_path.name})")
    else:
        print("Local overrides: inactive")
    print()
    for path, value in sorted(_flatten_dict(effective), key=lambda item: item[0]):
        source = effective_sources.get(path, "default")
        print(f"{path} = {json.dumps(value)} [{source}]")
    return 0


def _find_removable_workers(registry: WorkerRegistry, store: "SqliteTaskStore") -> "list[WorkerMetadata]":
    """Find worker files that can be safely removed.

    A worker is removable if:
    - Its status is completed or failed (finished normally), OR
    - Its process is no longer running (stale), OR
    - It has a task_id whose DB task is already completed/failed
      (zombie worker — PID may have been reused by another process)

    Reads worker JSON directly and only checks PID liveness when needed,
    avoiding the expensive PID checks for workers we can decide on via DB alone.
    """
    import json as json_lib
    from ..workers import WorkerMetadata
    removable = []
    for metadata_path in registry.workers_dir.glob("w-*.json"):
        try:
            with open(metadata_path) as f:
                data = json_lib.load(f)
            worker = WorkerMetadata.from_dict(data)
        except (OSError, json_lib.JSONDecodeError, KeyError):
            continue

        if worker.status in ("completed", "failed"):
            removable.append(worker)
        elif worker.task_id is not None:
            # Check DB first (cheap) — if the task is done, this is a zombie
            task = store.get(worker.task_id)
            if task and task.status in ("completed", "failed"):
                removable.append(worker)
            elif not registry.is_running(worker.worker_id):
                removable.append(worker)
        elif not registry.is_running(worker.worker_id):
            # No task_id — fall back to PID check
            removable.append(worker)
    return removable


def cmd_cleanup(args: argparse.Namespace) -> int:
    """Clean up stale worktrees, old logs, and stale worker metadata."""
    from datetime import timedelta
    import shutil

    config = Config.load(args.project_dir)
    store = get_store(config)
    git = Git(config.project_dir)
    registry = WorkerRegistry(config.workers_path)

    days = args.days if args.days is not None else config.cleanup_days
    cutoff_time = datetime.now(timezone.utc) - timedelta(days=days)
    cutoff_timestamp = cutoff_time.timestamp()

    # Track what was cleaned
    cleaned_worktrees: list[tuple[str, str]] = []
    cleaned_logs: list[str] = []
    cleaned_workers = 0
    errors: list[tuple[str, Exception]] = []

    # 1. Lineage-aware worktree cleanup
    if args.worktrees or not (args.logs or args.workers):
        from gza._query import resolve_lineage_root, build_lineage, task_time_for_lineage

        print("Scanning worktrees...")
        worktree_dir = config.worktree_path
        # Collect worktrees to remove (with reasons) before prompting
        pending_worktree_removals: list[tuple[Path, str]] = []
        if worktree_dir.exists():
            try:
                worktrees = git.worktree_list()
                worktree_paths = {Path(wt["path"]) for wt in worktrees if wt.get("path")}

                for worktree_path in worktree_dir.iterdir():
                    if not worktree_path.is_dir():
                        continue

                    if worktree_path not in worktree_paths:
                        # Orphaned directory not in git's worktree list
                        pending_worktree_removals.append((worktree_path, "orphaned"))
                        continue

                    # Git-tracked worktree — check lineage age
                    wt_name = worktree_path.name
                    task = store.get_by_task_id(wt_name)
                    if task is None:
                        # No task in DB for this worktree — treat as orphaned
                        pending_worktree_removals.append((worktree_path, "no task in DB"))
                        continue

                    # Resolve lineage and check most recent activity
                    root = resolve_lineage_root(store, task)
                    lineage = build_lineage(store, root)
                    most_recent = max(
                        (task_time_for_lineage(t) for t in lineage),
                        default=datetime.min,
                    )
                    # Make cutoff_time naive if most_recent is naive (DB timestamps may lack tz)
                    cutoff_naive = cutoff_time.replace(tzinfo=None) if most_recent.tzinfo is None else cutoff_time
                    if most_recent < cutoff_naive:
                        pending_worktree_removals.append((worktree_path, f"lineage inactive >{days}d"))
                    # else: lineage still active, skip

            except Exception as e:
                errors.append((str(worktree_dir), e))

        # Confirmation prompt before removing worktrees
        if pending_worktree_removals and not args.dry_run:
            if not args.force:
                print(f"\nWorktrees to remove ({len(pending_worktree_removals)}):")
                for wt_path, reason in pending_worktree_removals:
                    print(f"  - {wt_path.name} ({reason})")
                try:
                    answer = input(f"\nRemove {len(pending_worktree_removals)} worktree(s)? [y/N] ")
                except EOFError:
                    answer = ""
                if answer.strip().lower() != "y":
                    print("Skipped worktree removal.")
                    pending_worktree_removals = []

        # Execute removals
        for worktree_path, reason in pending_worktree_removals:
            if args.dry_run:
                cleaned_worktrees.append((worktree_path.name, reason))
            else:
                try:
                    git.worktree_remove(worktree_path, force=True)
                    # worktree_remove uses check=False, so check if dir still exists
                    if worktree_path.exists():
                        shutil.rmtree(worktree_path)
                    cleaned_worktrees.append((worktree_path.name, reason))
                except OSError as e:
                    errors.append((worktree_path.name, e))

    # 2. Clean up old log files
    if args.logs or not (args.worktrees or args.workers):
        print("Scanning logs...")
        if config.log_path.exists():
            # Get list of unmerged tasks if --keep-unmerged is set
            unmerged_task_ids = set()
            if args.keep_unmerged:
                try:
                    # Check completed tasks with branches for unmerged work
                    default_branch = git.default_branch()
                    history = store.get_history(limit=200)
                    for task in history:
                        if task.status == "completed" and task.branch and task.has_commits:
                            try:
                                if task.merge_status != "merged" and not git.is_merged(task.branch, default_branch):
                                    if task.task_id:
                                        unmerged_task_ids.add(task.task_id)
                            except Exception:
                                # Branch might not exist anymore, skip
                                pass
                except Exception as e:
                    print(f"Warning: Could not fetch unmerged tasks: {e}", file=sys.stderr)

            for log_file in config.log_path.iterdir():
                if not log_file.is_file():
                    continue

                # Check if this log is for an unmerged task
                if args.keep_unmerged:
                    # Extract task_id from log filename (format: YYYYMMDD-slug.log or task-id.log)
                    task_id = log_file.stem
                    if task_id in unmerged_task_ids:
                        continue

                # Check age
                if log_file.stat().st_mtime < cutoff_timestamp:
                    if args.dry_run:
                        cleaned_logs.append(log_file.name)
                    else:
                        try:
                            log_file.unlink()
                            cleaned_logs.append(log_file.name)
                        except OSError as e:
                            errors.append((log_file.name, e))

    # 3. Clean up worker metadata for finished/stale/zombie workers
    if args.workers or not (args.worktrees or args.logs):
        print("Scanning workers...")
        removable = _find_removable_workers(registry, store)
        if removable:
            print(f"Found {len(removable)} worker file(s) to clean up...")
        if args.dry_run:
            cleaned_workers = len(removable)
        else:
            for worker in removable:
                registry.remove(worker.worker_id)
                cleaned_workers += 1

    # Report results
    if args.dry_run:
        print(f"Dry run: would clean up resources")
        print()
    else:
        print(f"Cleanup completed")
        print()

    if args.worktrees or not (args.logs or args.workers):
        if cleaned_worktrees:
            print(f"Worktrees cleaned: {len(cleaned_worktrees)}")
            for name, reason in cleaned_worktrees:
                print(f"  - {name} ({reason})")
        else:
            print("Worktrees: nothing to clean")
        print()

    if args.logs or not (args.worktrees or args.workers):
        if cleaned_logs:
            print(f"Logs cleaned: {len(cleaned_logs)}")
            if args.keep_unmerged:
                print(f"  (kept logs for unmerged tasks)")
        else:
            print("Logs: nothing to clean")
        print()

    if args.workers or not (args.worktrees or args.logs):
        print(f"Worker files cleaned: {cleaned_workers}")
        print()

    # Report errors
    if errors:
        print(f"Errors ({len(errors)} items):")
        for item, error in errors:
            print(f"  - {item}: {error}", file=sys.stderr)
        return 1

    return 0


def cmd_clean(args: argparse.Namespace) -> int:
    """Archive or delete old log and worker files."""
    from datetime import timedelta
    import shutil

    config = Config.load(args.project_dir)

    # Determine default days based on mode
    if args.purge and args.days == 30:
        # User didn't specify --days, use purge default
        days = 365
    else:
        days = args.days

    # Calculate cutoff time
    cutoff_time = datetime.now(timezone.utc) - timedelta(days=days)
    cutoff_timestamp = cutoff_time.timestamp()

    if args.purge:
        # Purge mode: delete files from archives directory
        archives_dir = config.project_dir / ".gza" / "archives"

        # Track deleted files and errors
        deleted_logs = []
        deleted_workers = []
        errors = []

        # Delete from archives/logs
        archives_logs_dir = archives_dir / "logs"
        if archives_logs_dir.exists():
            for log_file in archives_logs_dir.iterdir():
                if log_file.is_file():
                    if log_file.stat().st_mtime < cutoff_timestamp:
                        if args.dry_run:
                            deleted_logs.append(log_file)
                        else:
                            try:
                                log_file.unlink()
                                deleted_logs.append(log_file)
                            except OSError as e:
                                errors.append((log_file, e))

        # Delete from archives/workers
        archives_workers_dir = archives_dir / "workers"
        if archives_workers_dir.exists():
            for worker_file in archives_workers_dir.iterdir():
                if worker_file.is_file():
                    if worker_file.stat().st_mtime < cutoff_timestamp:
                        if args.dry_run:
                            deleted_workers.append(worker_file)
                        else:
                            try:
                                worker_file.unlink()
                                deleted_workers.append(worker_file)
                            except OSError as e:
                                errors.append((worker_file, e))

        # Report results
        if args.dry_run:
            print(f"Dry run: would purge archived files older than {days} days")
            print()
            if deleted_logs:
                print(f"Archived logs ({len(deleted_logs)} files):")
                for log_file in deleted_logs:
                    print(f"  - {log_file.name}")
            else:
                print("Archived logs: no files to purge")

            print()
            if deleted_workers:
                print(f"Archived workers ({len(deleted_workers)} files):")
                for worker_file in deleted_workers:
                    print(f"  - {worker_file.name}")
            else:
                print("Archived workers: no files to purge")
        else:
            print(f"Purged archived files older than {days} days:")
            print(f"  - Archived logs: {len(deleted_logs)} files")
            print(f"  - Archived workers: {len(deleted_workers)} files")

            # Report any errors
            if errors:
                print()
                print(f"Errors ({len(errors)} files):")
                for file, error in errors:
                    print(f"  - {file.name}: {error}", file=sys.stderr)

    else:
        # Archive mode: move files to archives directory
        archives_dir = config.project_dir / ".gza" / "archives"

        # Track archived files and errors
        archived_logs = []
        archived_workers = []
        deleted_backups = []
        errors = []

        # Archive logs
        if config.log_path.exists():
            archives_logs_dir = archives_dir / "logs"
            for log_file in config.log_path.iterdir():
                if log_file.is_file():
                    if log_file.stat().st_mtime < cutoff_timestamp:
                        if args.dry_run:
                            archived_logs.append(log_file)
                        else:
                            try:
                                # Create archive directory if needed
                                archives_logs_dir.mkdir(parents=True, exist_ok=True)
                                # Move file to archive
                                dest = archives_logs_dir / log_file.name
                                shutil.move(str(log_file), str(dest))
                                archived_logs.append(log_file)
                            except OSError as e:
                                errors.append((log_file, e))

        # Archive workers
        if config.workers_path.exists():
            archives_workers_dir = archives_dir / "workers"
            for worker_file in config.workers_path.iterdir():
                if worker_file.is_file():
                    if worker_file.stat().st_mtime < cutoff_timestamp:
                        if args.dry_run:
                            archived_workers.append(worker_file)
                        else:
                            try:
                                # Create archive directory if needed
                                archives_workers_dir.mkdir(parents=True, exist_ok=True)
                                # Move file to archive
                                dest = archives_workers_dir / worker_file.name
                                shutil.move(str(worker_file), str(dest))
                                archived_workers.append(worker_file)
                            except OSError as e:
                                errors.append((worker_file, e))

        # Delete old backups
        backups_dir = config.project_dir / ".gza" / "backups"
        if backups_dir.exists():
            for backup_file in backups_dir.iterdir():
                if backup_file.is_file():
                    if backup_file.stat().st_mtime < cutoff_timestamp:
                        if args.dry_run:
                            deleted_backups.append(backup_file)
                        else:
                            try:
                                backup_file.unlink()
                                deleted_backups.append(backup_file)
                            except OSError as e:
                                errors.append((backup_file, e))

        # Report results
        if args.dry_run:
            print(f"Dry run: would archive files older than {days} days")
            print()
            if archived_logs:
                print(f"Logs ({len(archived_logs)} files):")
                for log_file in archived_logs:
                    print(f"  - {log_file.name}")
            else:
                print("Logs: no files to archive")

            print()
            if archived_workers:
                print(f"Workers ({len(archived_workers)} files):")
                for worker_file in archived_workers:
                    print(f"  - {worker_file.name}")
            else:
                print("Workers: no files to archive")

            print()
            if deleted_backups:
                print(f"Backups ({len(deleted_backups)} files):")
                for backup_file in deleted_backups:
                    print(f"  - {backup_file.name}")
            else:
                print("Backups: no files to delete")
        else:
            print(f"Archived files older than {days} days:")
            print(f"  - Logs: {len(archived_logs)} files")
            print(f"  - Workers: {len(archived_workers)} files")
            print(f"  - Backups deleted: {len(deleted_backups)} files")

            # Report any errors
            if errors:
                print()
                print(f"Errors ({len(errors)} files):")
                for file, error in errors:
                    print(f"  - {file.name}: {error}", file=sys.stderr)

    return 0


def cmd_init(args: argparse.Namespace) -> int:
    """Generate a new gza.yaml configuration file with defaults."""
    import importlib.resources

    from ..config import CONFIG_FILENAME, LOCAL_CONFIG_FILENAME

    # Derive project name from directory name
    default_project_name = args.project_dir.name

    config_path = args.project_dir / CONFIG_FILENAME

    if config_path.exists() and not args.force:
        print(f"Error: {CONFIG_FILENAME} already exists at {config_path}")
        print("Use --force to overwrite")
        return 1

    # Read the example template from the package
    template = importlib.resources.files("gza").joinpath("gza.yaml.example").read_text()

    # Check if running interactively (stdin is a TTY)
    is_interactive = sys.stdin.isatty()

    if is_interactive:
        # Prompt for branch strategy
        print("Branch naming strategy:")
        print("  1. monorepo    - {project}/{task_id} (e.g., myproj/20260107-add-feature)")
        print("  2. conventional - {type}/{slug} (e.g., feature/add-feature, fix/login-bug)")
        print("  3. simple      - {slug} (e.g., add-feature)")
        print("  4. custom      - Define your own pattern")

        while True:
            choice = input("Choose strategy [1-4, default=1]: ").strip() or "1"
            if choice in ("1", "2", "3", "4"):
                break
            print("Invalid choice. Please enter 1, 2, 3, or 4.")
    else:
        # Non-interactive mode: use default (monorepo)
        choice = "1"

    # Replace project name placeholder
    config_content = template.replace("project_name: my-project", f"project_name: {default_project_name}")

    # Apply branch strategy based on user's choice
    default_branch_line = "# branch_strategy: monorepo  # Default: {project}/{task_id}"
    if choice == "1":
        pass  # Keep commented-out default from template
    elif choice == "2":
        config_content = config_content.replace(
            default_branch_line,
            "branch_strategy: conventional  # {type}/{slug}",
        )
    elif choice == "3":
        config_content = config_content.replace(
            default_branch_line,
            "branch_strategy: simple  # {slug}",
        )
    else:  # custom
        print("\nCustom pattern variables:")
        print("  {project}  - Project name")
        print("  {task_id}  - Full task ID (YYYYMMDD-slug)")
        print("  {date}     - Date portion (YYYYMMDD)")
        print("  {slug}     - Slug portion")
        print("  {type}     - Inferred/default type (feature, fix, etc.)")

        while True:
            pattern = input("Enter custom pattern: ").strip()
            if pattern:
                break
            print("Pattern cannot be empty.")

        default_type = input("Default type [default=feature]: ").strip() or "feature"
        custom_strategy = f'branch_strategy:\n  pattern: "{pattern}"\n  default_type: {default_type}'
        config_content = config_content.replace(default_branch_line, custom_strategy)

    config_path.write_text(config_content)
    print(f"✓ Created {config_path}")

    local_example_path = args.project_dir / f"{LOCAL_CONFIG_FILENAME}.example"
    if not local_example_path.exists() or args.force:
        local_template = importlib.resources.files("gza").joinpath("gza.local.yaml.example").read_text()
        local_example_path.write_text(local_template)
        print(f"✓ Created {local_example_path}")

    # Initialize the database (Config.load will now work since we have project_name)
    config = Config.load(args.project_dir)
    store = get_store(config)
    print(f"✓ Initialized database at {config.db_path}")

    return 0


def cmd_sync_report(args: argparse.Namespace) -> int:
    """Sync report file content from disk into DB output_content."""
    config = Config.load(args.project_dir)
    store = get_store(config)

    task = store.get(args.task_id)
    if not task:
        console.print(f"[red]Error: Task #{args.task_id} not found[/red]")
        return 1

    if not task.report_file:
        console.print(f"[red]Error: Task #{args.task_id} has no report file[/red]")
        return 1

    report_path = config.project_dir / task.report_file
    if not report_path.exists():
        console.print(f"[red]Error: Report file not found: {task.report_file}[/red]")
        return 1

    disk_content = report_path.read_text()

    if task.output_content == disk_content:
        console.print(f"[dim]Task #{args.task_id} already in sync — no changes needed.[/dim]")
        return 0

    task.output_content = disk_content
    store.update(task)
    console.print(f"[green]Synced report for task #{args.task_id} from disk to DB.[/green]")
    return 0


def cmd_learnings(args: argparse.Namespace) -> int:
    """Handle learnings subcommands."""
    config = Config.load(args.project_dir)
    subcommand = args.learnings_command

    if not subcommand:
        print("usage: gza learnings <subcommand>")
        print()
        print("Available subcommands:")
        print("  show     Display the current learnings file")
        print("  update   Regenerate learnings from recent completed tasks")
        return 0

    if subcommand == "show":
        learnings_path = config.project_dir / ".gza" / "learnings.md"
        if not learnings_path.exists():
            print("No learnings file found at .gza/learnings.md")
            return 0
        try:
            content = learnings_path.read_text()
        except OSError as e:
            print(f"Error reading learnings file: {e}", file=sys.stderr)
            return 1
        print(content, end="")
        return 0

    if subcommand == "update":
        store = get_store(config)
        window = args.window if hasattr(args, "window") and args.window is not None else DEFAULT_LEARNINGS_WINDOW
        if window <= 0:
            print("Error: --window must be a positive integer", file=sys.stderr)
            return 1
        result = regenerate_learnings(store, config, window=window)
        print(f"Updated learnings: {result.path.relative_to(config.project_dir)}")
        print(f"  Tasks used: {result.tasks_used}")
        print(f"  Learnings: {result.learnings_count}")
        print(
            "  Delta: "
            f"+{result.added_count} / -{result.removed_count} / ={result.retained_count} "
            f"(churn {result.churn_percent:.1f}%)"
        )
        return 0

    print(f"Unknown learnings subcommand: {subcommand}", file=sys.stderr)
    return 1


def cmd_import(args: argparse.Namespace) -> int:
    """Import tasks from a YAML file."""
    # Handle legacy usage: gza import <project_dir>
    # If the file argument is a directory, treat it as project_dir
    if args.file and Path(args.file).is_dir():
        args.project_dir = Path(args.file).resolve()
        args.file = None

    config = Config.load(args.project_dir)
    store = get_store(config)

    # Determine which file to import
    if args.file:
        import_path = Path(args.file)
        if not import_path.is_absolute():
            import_path = config.project_dir / import_path
    else:
        # Legacy: import from tasks.yaml
        import_path = config.tasks_path
        if not import_path.exists():
            print(f"Error: No file specified and {import_path} not found")
            print("Usage: gza import <file> [--dry-run] [--force]")
            return 1
        return _cmd_import_legacy(config, store)

    # Parse the import file
    tasks, default_group, default_spec, parse_errors = parse_import_file(import_path)

    if parse_errors:
        print("Error: Failed to parse import file:")
        for error in parse_errors:
            if error.task_index:
                print(f"  Task {error.task_index}: {error.message}")
            else:
                print(f"  {error.message}")
        return 1

    # Validate the tasks
    validation_errors = validate_import(tasks, config.project_dir, default_spec)

    if validation_errors:
        print("Error: Validation failed:")
        for error in validation_errors:
            if error.task_index:
                print(f"  Task {error.task_index}: {error.message}")
            else:
                print(f"  {error.message}")
        return 1

    # Import the tasks
    if args.dry_run:
        print(f"Would import {len(tasks)} tasks:")
    else:
        print(f"Importing {len(tasks)} tasks...")

    results, messages = import_tasks(
        store=store,
        tasks=tasks,
        project_dir=config.project_dir,
        dry_run=args.dry_run,
        force=args.force,
    )

    for message in messages:
        print(message)

    # Summary
    if args.dry_run:
        return 0

    created = sum(1 for r in results if not r.skipped)
    skipped = sum(1 for r in results if r.skipped)

    if skipped:
        print(f"Imported {created} tasks ({skipped} skipped)")
    else:
        print(f"Imported {created} tasks")

    return 0


def _cmd_import_legacy(config: Config, store: SqliteTaskStore) -> int:
    """Legacy import from tasks.yaml (old format)."""
    yaml_store = YamlTaskStore(config.tasks_path)
    imported = 0
    skipped = 0

    for yaml_task in yaml_store._tasks:
        # Check if already imported (by task_id)
        if yaml_task.task_id:
            existing = store.get_by_task_id(yaml_task.task_id)
            if existing:
                skipped += 1
                continue

        # Create task in SQLite (Task class uses 'prompt' and 'task_type')
        task = store.add(yaml_task.prompt, task_type=yaml_task.task_type)

        # Copy over fields - need to convert TaskStatus enum to string for status
        status_value = yaml_task.status.value if hasattr(yaml_task.status, 'value') else yaml_task.status
        task.status = status_value
        task.task_id = yaml_task.task_id
        task.branch = yaml_task.branch
        task.log_file = yaml_task.log_file
        task.report_file = yaml_task.report_file
        task.has_commits = yaml_task.has_commits
        task.duration_seconds = yaml_task.duration_seconds
        task.num_turns_reported = yaml_task.num_turns_reported
        task.cost_usd = yaml_task.cost_usd
        if yaml_task.completed_at:
            if isinstance(yaml_task.completed_at, datetime):
                task.completed_at = yaml_task.completed_at
            else:
                task.completed_at = datetime.combine(yaml_task.completed_at, datetime.min.time())

        store.update(task)
        imported += 1

    print(f"✓ Imported {imported} tasks")
    if skipped:
        print(f"  Skipped {skipped} already imported tasks")

    return 0


def _resolve_skill_install_targets(
    project_dir: Path,
    requested_targets: list[str] | None,
    default_targets: list[str],
) -> list[tuple[str, Path]]:
    """Resolve skill install target names to destination directories."""
    codex_home = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))).expanduser()
    gemini_home = Path(os.environ.get("GEMINI_HOME", str(Path.home() / ".gemini"))).expanduser()
    target_map = {
        "claude": project_dir / ".claude" / "skills",
        "codex": codex_home / "skills",
        "gemini": gemini_home / "skills",
    }

    targets = requested_targets or default_targets
    if "all" in targets:
        normalized_targets = ["claude", "codex"]
    else:
        normalized_targets = []
        for target in targets:
            if target not in target_map:
                continue
            if target not in normalized_targets:
                normalized_targets.append(target)

    return [(target_name, target_map[target_name]) for target_name in normalized_targets]


def cmd_skills_install(
    args: argparse.Namespace,
    default_targets: list[str],
) -> int:
    """Install gza skills from package to one or more target directories."""
    from ..skills_utils import (
        get_available_skills,
        get_skill_description,
        get_skill_version,
        copy_skill,
    )

    public_only = not getattr(args, "dev", False)

    # Handle --list flag
    if args.list:
        available = get_available_skills(public_only=public_only)
        if not available:
            print("No skills available")
            return 0

        print("Available skills:")
        for skill in available:
            desc = get_skill_description(skill)
            version = get_skill_version(skill)
            version_str = f" (v{version})" if version else ""
            print(f"  {skill:20} - {desc}{version_str}")
        return 0

    # Determine which skills to install
    available = get_available_skills(public_only=public_only)

    if args.skills:
        # When specific skills are requested, check against public skills
        skills_to_install = []
        for skill in args.skills:
            if skill not in available:
                print(f"Error: Skill '{skill}' not found")
                print(f"Available skills: {', '.join(available)}")
                return 1
            skills_to_install.append(skill)
    else:
        # Install all public skills
        skills_to_install = available

    if not skills_to_install:
        print("No skills to install")
        return 0

    requested_targets = getattr(args, "target", None)
    install_targets = _resolve_skill_install_targets(
        project_dir=args.project_dir,
        requested_targets=requested_targets,
        default_targets=default_targets,
    )

    if not install_targets:
        print("Error: No install targets selected")
        return 1

    any_failed = False

    for target_name, target_dir in install_targets:
        # Create target directory
        target_dir.mkdir(parents=True, exist_ok=True)

        # Install skills
        print(f"Installing {len(skills_to_install)} skill(s) to {target_dir} [{target_name}]...")

        installed = 0
        skipped = 0
        failed = 0

        for skill in skills_to_install:
            success, message = copy_skill(skill, target_dir, args.force)

            if success:
                print(f"  ✓ {skill}")
                installed += 1
            elif "already exists" in message:
                print(f"  ⊘ {skill} ({message})")
                skipped += 1
            else:
                print(f"  ✗ {skill} ({message})")
                failed += 1

        # Print summary
        print()
        if failed > 0:
            print(f"Installed {installed} skill(s), {skipped} skipped, {failed} failed [{target_name}]")
            any_failed = True
        elif skipped > 0:
            print(f"Installed {installed} skill(s) ({skipped} skipped) [{target_name}]")
        else:
            print(f"Installed {installed} skill(s) [{target_name}]")
        print()

    return 1 if any_failed else 0
