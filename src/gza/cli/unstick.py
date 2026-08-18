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
class _UnstickDirectOutcome:
    action_type: str
    status: Literal["success", "blocked", "error"]
    detail: str | None = None


@dataclass(frozen=True)
class _UnstickLaunchOutcome:
    action_type: str
    detail: str | None = None


@dataclass(frozen=True)
class _UnstickRunSummary:
    started_owner_ids: frozenset[str]
    capacity_blocked_owner_ids: frozenset[str]
    direct_owner_outcomes: dict[str, _UnstickDirectOutcome]
    launch_blocked_owner_outcomes: dict[str, _UnstickLaunchOutcome]


_ObservedRunOutcome = Literal[
    "started",
    "direct",
    "direct_blocked",
    "direct_error",
    "capacity_blocked",
    "launch_blocked",
]


def _seed_zero_slot_capacity_blocked(plan, observed: dict[str, tuple[_ObservedRunOutcome, str, str | None]]) -> None:
    """Pre-classify runnable worker recovery owners when scoped watch has no slots at all."""
    slots = getattr(plan, "slots", None)
    analysis = getattr(plan, "analysis", None)
    if slots is None or analysis is None or slots > 0:
        return
    for row, failed, _decision, _action, worker_consuming, _action_task in getattr(analysis, "actionable_failed", ()):
        if not worker_consuming or row.owner_task.id is None or failed.id is None:
            continue
        observed[str(row.owner_task.id)] = ("capacity_blocked", str(_action.get("type", "recovery")), None)


def _dispatch_rearmed_owners(
    *,
    config: Config,
    store,
    owner_ids: tuple[str, ...],
    limit: int,
) -> _UnstickRunSummary:
    if not owner_ids:
        return _UnstickRunSummary(
            started_owner_ids=frozenset(),
            capacity_blocked_owner_ids=frozenset(),
            direct_owner_outcomes={},
            launch_blocked_owner_outcomes={},
        )

    snapshot = get_concurrency_snapshot(config, store, cleanup_stale=False)
    scoped_batch = min(snapshot.limit, snapshot.running + limit)
    observed: dict[str, tuple[_ObservedRunOutcome, str, str | None]] = {}
    priority = {
        "direct": 0,
        "direct_blocked": 1,
        "direct_error": 1,
        "launch_blocked": 2,
        "capacity_blocked": 3,
        "started": 4,
    }

    def _observe(
        owner_task_id: str,
        outcome: _ObservedRunOutcome,
        action_type: str,
        detail: str | None = None,
    ) -> None:
        previous = observed.get(owner_task_id)
        if previous is None or priority[outcome] >= priority[previous[0]]:
            observed[owner_task_id] = (outcome, action_type, detail)

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
        started_owner_ids=frozenset(owner_id for owner_id, (outcome, _action_type, _detail) in observed.items() if outcome == "started"),
        capacity_blocked_owner_ids=frozenset(
            owner_id for owner_id, (outcome, _action_type, _detail) in observed.items() if outcome == "capacity_blocked"
        ),
        direct_owner_outcomes={
            owner_id: _UnstickDirectOutcome(
                action_type=action_type,
                status=(
                    "success"
                    if outcome == "direct"
                    else "blocked"
                    if outcome == "direct_blocked"
                    else "error"
                ),
                detail=detail,
            )
            for owner_id, (outcome, action_type, detail) in observed.items()
            if outcome in {"direct", "direct_blocked", "direct_error"}
        },
        launch_blocked_owner_outcomes={
            owner_id: _UnstickLaunchOutcome(action_type=action_type, detail=detail)
            for owner_id, (outcome, action_type, detail) in observed.items()
            if outcome == "launch_blocked"
        },
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


def _print_direct_outcome_group(
    title: str,
    outcomes: list[tuple[UnstickOutcome, _UnstickDirectOutcome]],
) -> None:
    if not outcomes:
        return
    print(title)
    for outcome, direct in outcomes:
        prompt = truncate(outcome.owner_task.prompt, 80)
        reason = outcome.reason_class or "unknown"
        direct_detail = getattr(direct, "detail", None)
        detail = f" - {direct_detail.removeprefix('SKIP: ')}" if direct_detail else ""
        print(f"  {outcome.owner_task.id} [{reason}] {direct.action_type} {direct.status}: {prompt}{detail}")


def _print_launch_outcome_group(
    title: str,
    outcomes: list[tuple[UnstickOutcome, _UnstickLaunchOutcome]],
) -> None:
    if not outcomes:
        return
    print(title)
    for outcome, launch in outcomes:
        prompt = truncate(outcome.owner_task.prompt, 80)
        reason = outcome.reason_class or "unknown"
        detail = f" - {launch.detail.removeprefix('SKIP: ')}" if launch.detail else ""
        print(f"  {outcome.owner_task.id} [{reason}] {launch.action_type}: {prompt}{detail}")


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
    direct: list[tuple[UnstickOutcome, _UnstickDirectOutcome]] = []
    direct_blocked: list[tuple[UnstickOutcome, _UnstickDirectOutcome]] = []
    launch_blocked: list[tuple[UnstickOutcome, _UnstickLaunchOutcome]] = []
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
        direct = [
            (outcome, run_summary.direct_owner_outcomes[str(outcome.owner_task.id)])
            for outcome in rearmed
            if outcome.owner_task.id is not None
            and str(outcome.owner_task.id) in run_summary.direct_owner_outcomes
            and run_summary.direct_owner_outcomes[str(outcome.owner_task.id)].status == "success"
        ]
        direct_blocked = [
            (outcome, run_summary.direct_owner_outcomes[str(outcome.owner_task.id)])
            for outcome in rearmed
            if outcome.owner_task.id is not None
            and str(outcome.owner_task.id) in run_summary.direct_owner_outcomes
            and run_summary.direct_owner_outcomes[str(outcome.owner_task.id)].status != "success"
        ]
        launch_blocked = [
            (outcome, run_summary.launch_blocked_owner_outcomes[str(outcome.owner_task.id)])
            for outcome in rearmed
            if outcome.owner_task.id is not None
            and str(outcome.owner_task.id) in run_summary.launch_blocked_owner_outcomes
        ]
        direct_owner_ids = frozenset(run_summary.direct_owner_outcomes)
        launch_blocked_owner_ids = frozenset(run_summary.launch_blocked_owner_outcomes)
        cleared_only = [
            outcome
            for outcome in rearmed
            if outcome.owner_task.id is None
            or str(outcome.owner_task.id)
            not in (
                run_summary.started_owner_ids
                | run_summary.capacity_blocked_owner_ids
                | direct_owner_ids
                | launch_blocked_owner_ids
            )
        ]
        print(
            "Run summary: "
            f"{len(started)} started, {len(direct)} direct, {len(direct_blocked)} direct-blocked, "
            f"{len(launch_blocked)} launch-blocked, {len(cleared_only)} cleared-only, "
            f"{len(capacity_blocked)} capacity-blocked"
        )
        if limit_arg is not None:
            print(f"Dispatch limit: {limit}")
    elif run_cleared:
        print("Run summary: 0 started, 0 direct, 0 direct-blocked, 0 launch-blocked, 0 cleared-only, 0 capacity-blocked")

    if run_cleared:
        _print_outcome_group("Started:", started)
        _print_direct_outcome_group("Direct:", direct)
        _print_direct_outcome_group("Direct Blocked:", direct_blocked)
        _print_launch_outcome_group("Launch Blocked:", launch_blocked)
        _print_outcome_group("Cleared Only:", cleared_only)
        _print_outcome_group("Capacity Blocked:", capacity_blocked)
    else:
        _print_outcome_group("Rearmed:", rearmed)

    _print_outcome_group("Skipped:", skipped)

    if not rearmed and not skipped:
        reasons = ", ".join(reason_classes) if reason_classes else ", ".join(SUPPORTED_PARK_REASON_CLASSES)
        print(f"No parked owners matched the requested selectors for reasons: {reasons}")
    return 0
