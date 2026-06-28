"""CLI surface for manual parked-task clearing."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Literal

from ..concurrency import get_concurrency_snapshot
from ..config import Config
from ..console import truncate
from ..git import Git
from ..unstick import (
    SUPPORTED_PARK_REASON_CLASSES,
    UnstickOutcome,
    select_and_clear_parked_tasks,
)
from ._common import get_store, parse_cli_tag_filters
from .watch import _build_watch_cycle_plan, _dispatch_scoped_watch_once, _WatchLog


@dataclass(frozen=True)
class _UnstickRunSummary:
    started_owner_ids: frozenset[str]
    capacity_blocked_owner_ids: frozenset[str]


def _seed_zero_slot_capacity_blocked(plan, observed: dict[str, Literal["started", "direct", "capacity_blocked"]]) -> None:
    """Pre-classify runnable worker recovery owners when scoped watch has no slots at all."""
    slots = getattr(plan, "slots", None)
    analysis = getattr(plan, "analysis", None)
    if slots is None or analysis is None or slots > 0:
        return
    for row, failed, _decision, _action, worker_consuming, _action_task in getattr(analysis, "actionable_failed", ()):
        if not worker_consuming or row.owner_task.id is None or failed.id is None:
            continue
        observed[str(row.owner_task.id)] = "capacity_blocked"


def _dispatch_rearmed_owners(
    *,
    config: Config,
    store,
    owner_ids: tuple[str, ...],
    limit: int,
) -> _UnstickRunSummary:
    if not owner_ids:
        return _UnstickRunSummary(started_owner_ids=frozenset(), capacity_blocked_owner_ids=frozenset())

    snapshot = get_concurrency_snapshot(config, store, cleanup_stale=False)
    scoped_batch = min(snapshot.limit, snapshot.running + limit)
    observed: dict[str, Literal["started", "direct", "capacity_blocked"]] = {}
    priority = {"direct": 0, "capacity_blocked": 1, "started": 2}

    def _observe(owner_task_id: str, outcome: Literal["started", "direct", "capacity_blocked"], _action_type: str) -> None:
        previous = observed.get(owner_task_id)
        if previous is None or priority[outcome] >= priority[previous]:
            observed[owner_task_id] = outcome

    log = _WatchLog(config.project_dir / ".gza" / "unstick-run.log", quiet=True)
    plan = _build_watch_cycle_plan(
        config=config,
        store=store,
        batch=scoped_batch,
        tags=None,
        any_tag=False,
        recovery_slots=limit,
        recovery_mode=None,
        max_recovery_attempts=config.max_resume_attempts,
        scoped_owner_ids=owner_ids,
    )
    _seed_zero_slot_capacity_blocked(plan, observed)
    _dispatch_scoped_watch_once(
        config=config,
        store=store,
        batch=scoped_batch,
        max_iterations=10,
        dry_run=False,
        log=log,
        quiet=True,
        recovery_slots=limit,
        recovery_mode=None,
        max_recovery_attempts=config.max_resume_attempts,
        auto_restart_on_drift=False,
        precomputed_plan=plan,
        emit_cycle_header=False,
        emit_lifecycle_summary=False,
        scoped_owner_ids=owner_ids,
        dispatch_observer=_observe,
        new_worker_start_cap=limit,
    )
    return _UnstickRunSummary(
        started_owner_ids=frozenset(owner_id for owner_id, outcome in observed.items() if outcome == "started"),
        capacity_blocked_owner_ids=frozenset(
            owner_id for owner_id, outcome in observed.items() if outcome == "capacity_blocked"
        ),
    )


def _print_outcome_group(title: str, outcomes: list[UnstickOutcome]) -> None:
    if not outcomes:
        return
    print(title)
    for outcome in outcomes:
        prompt = truncate(outcome.owner_task.prompt, 80)
        reason = outcome.reason_class or "unknown"
        if outcome.status == "skipped":
            print(f"  {outcome.owner_task.id} {outcome.detail}: {prompt}")
        else:
            print(f"  {outcome.owner_task.id} [{reason}] {prompt}")


def cmd_unstick(args: argparse.Namespace) -> int:
    """Clear eligible parked owner state and optionally dispatch it through scoped watch."""
    task_ids = tuple(getattr(args, "task_ids", ()) or ())
    reason_classes = tuple(getattr(args, "reasons", ()) or ())
    select_all = bool(getattr(args, "all", False))
    run_cleared = bool(getattr(args, "run", False))
    limit_arg = getattr(args, "limit", None)
    if not task_ids and not getattr(args, "tags", None) and not reason_classes and not select_all:
        print("Error: gza unstick requires at least one selector: task ID, --tag, --reason, or --all")
        return 2

    config = Config.load(args.project_dir)
    store = get_store(config)
    git = Git(config.project_dir)
    target_branch = git.default_branch()
    try:
        tag_filters, any_tag = parse_cli_tag_filters(args)
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1

    limit = config.max_concurrent if limit_arg is None else limit_arg

    result = select_and_clear_parked_tasks(
        store,
        config=config,
        git=git,
        target_branch=target_branch,
        task_ids=task_ids,
        tags=tag_filters,
        any_tag=any_tag,
        reason_classes=reason_classes,
        select_all=select_all,
    )

    print(f"Selected {len(result.selected)} parked owner(s)")
    if result.stale_backstop_cleared:
        print(f"Cleared {result.stale_backstop_cleared} stale backstop park(s) before selection")

    rearmed = [outcome for outcome in result.outcomes if outcome.status == "rearmed"]
    skipped = [outcome for outcome in result.outcomes if outcome.status == "skipped"]
    started: list[UnstickOutcome] = []
    capacity_blocked: list[UnstickOutcome] = []
    cleared_only: list[UnstickOutcome] = rearmed

    if run_cleared and rearmed:
        run_summary = _dispatch_rearmed_owners(
            config=config,
            store=store,
            owner_ids=tuple(
                str(outcome.owner_task.id)
                for outcome in rearmed
                if outcome.owner_task.id is not None
            ),
            limit=limit,
        )
        started = [
            outcome
            for outcome in rearmed
            if outcome.owner_task.id is not None and str(outcome.owner_task.id) in run_summary.started_owner_ids
        ]
        capacity_blocked = [
            outcome
            for outcome in rearmed
            if outcome.owner_task.id is not None and str(outcome.owner_task.id) in run_summary.capacity_blocked_owner_ids
        ]
        cleared_only = [
            outcome
            for outcome in rearmed
            if outcome.owner_task.id is None
            or str(outcome.owner_task.id) not in run_summary.started_owner_ids | run_summary.capacity_blocked_owner_ids
        ]
        print(
            "Run summary: "
            f"{len(started)} started, {len(cleared_only)} cleared-only, {len(capacity_blocked)} capacity-blocked"
        )
        if limit_arg is not None:
            print(f"Dispatch limit: {limit}")
    elif run_cleared:
        print("Run summary: 0 started, 0 cleared-only, 0 capacity-blocked")

    if run_cleared:
        _print_outcome_group("Started:", started)
        _print_outcome_group("Cleared Only:", cleared_only)
        _print_outcome_group("Capacity Blocked:", capacity_blocked)
    else:
        _print_outcome_group("Rearmed:", rearmed)

    _print_outcome_group("Skipped:", skipped)

    if not rearmed and not skipped:
        reasons = ", ".join(reason_classes) if reason_classes else ", ".join(SUPPORTED_PARK_REASON_CLASSES)
        print(f"No parked owners matched the requested selectors for reasons: {reasons}")
    return 0
