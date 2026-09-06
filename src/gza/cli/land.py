"""CLI adapter for operator-triggered landing."""

from __future__ import annotations

import argparse
from typing import TYPE_CHECKING, Any

from gza.cli._common import get_store, resolve_id
from gza.config import Config
from gza.git import Git

if TYPE_CHECKING:
    from gza.landing import LandResult, LandTerminalResult


def land_terminal_state(*args: Any, **kwargs: Any) -> Any:
    from gza.landing import land_terminal_state as _land_terminal_state

    return _land_terminal_state(*args, **kwargs)


def reconcile_terminal_merge_truth(git: Git) -> Any:
    from gza.landing import reconcile_terminal_merge_truth as _reconcile_terminal_merge_truth

    return _reconcile_terminal_merge_truth(git)


def cmd_land(args: argparse.Namespace) -> int:
    """Resolve and plan an operator-triggered landing attempt."""

    from gza.landing import (
        LANDING_POLICIES,
        LandRequest,
        LandTerminalResult,
        run_production_landing,
    )

    config = Config.load(args.project_dir)
    store = get_store(config, open_mode="query_only" if args.dry_run else "readwrite")
    git = Git(config.project_dir)
    task_id = resolve_id(config, args.task_id)
    policy = args.policy
    if policy not in LANDING_POLICIES:
        print(f"Error: unknown landing policy {policy!r}")
        return 2

    request = LandRequest(task_id=task_id, policy=policy, dry_run=bool(args.dry_run))
    result = run_production_landing(config=config, store=store, git=git, request=request)
    if isinstance(result, LandTerminalResult):
        print(_format_terminal_result(result))
        return 0
    for step in result.steps:
        print(f"{step.phase}: {step.status} - {step.summary}")

    terminal_message = _format_terminal_result(result)
    if terminal_message is not None:
        print(terminal_message)
        return 0

    if result.blocked is not None:
        print(result.blocked.terminal_sentence(task_id))
        return 1
    if result.post_merge_verify_failure is not None:
        print(result.post_merge_verify_failure.terminal_sentence(task_id))
        return 1
    if result.already_merged:
        print(
            f"Already landed {task_id}: owner {result.owner_task_id} "
            f"on {result.source_ref} -> {result.target_branch}."
        )
        return 0
    if args.dry_run:
        print(
            f"Dry run for {task_id}: owner {result.owner_task_id} "
            f"on {result.source_ref} -> {result.target_branch}; "
            "later outcomes stop at the first execution-required boundary."
        )
        return 0
    if result.merged:
        print(
            f"Landed {task_id}: owner {result.owner_task_id} -> {result.target_branch} "
            f"with {_landing_usage_summary(result)}; "
            f"follow-up task IDs {_format_task_ids(result.followup_task_ids)}; "
            f"deferred task IDs {_format_task_ids(result.deferred_task_ids)}; "
            f"final provenance {result.merge_provenance} ({result.merge_provenance} provenance)."
        )
        return 0
    print(f"Cannot land {task_id}: landing stopped before a terminal result.")
    return 1


def _format_terminal_result(result: LandResult | LandTerminalResult) -> str | None:
    from gza.landing import LandResult

    if isinstance(result, LandResult):
        if result.terminal_outcome is None:
            return None
        prefix = "Dry run: " if result.request.dry_run else ""
        merge_unit_id = result.merge_unit_id or result.owner_task_id or "unknown"
        owner = result.owner_task_id or "unknown"
        source = result.source_ref or "unknown"
        target = result.target_branch or "unknown"
        outcome = result.terminal_outcome
        reconciled = result.terminal_reconciled
    else:
        prefix = "Dry run: " if result.dry_run else ""
        merge_unit_id = result.merge_unit_id
        owner = result.owner_task_id or "unknown"
        source = result.source_branch
        target = result.target_branch
        outcome = result.outcome
        reconciled = result.reconciled

    identity = f"owner {owner}, source {source}, target {target}, known outcome {outcome}"
    if outcome == "merged":
        if reconciled and prefix:
            return (
                f"{prefix}Merge unit {merge_unit_id} ({identity}) would reconcile "
                "to already merged; no landing activity was run."
            )
        if reconciled:
            return (
                f"{prefix}Merge unit {merge_unit_id} ({identity}) reconciled "
                "to already merged; no landing activity was run."
            )
        return (
            f"{prefix}Merge unit {merge_unit_id} ({identity}) is already merged; "
            "no landing activity was run."
        )
    if reconciled and prefix:
        return (
            f"{prefix}Merge unit {merge_unit_id} ({identity}) would reconcile to terminal "
            f"no-work state {outcome}; no landing activity was run."
        )
    if reconciled:
        return (
            f"{prefix}Merge unit {merge_unit_id} ({identity}) reconciled to terminal "
            f"no-work state {outcome}; no landing activity was run."
        )
    return (
        f"{prefix}Merge unit {merge_unit_id} ({identity}) is terminal no-work state "
        f"{outcome}; no landing activity was run."
    )


def _landing_usage_summary(result: LandResult) -> str:
    return (
        f"rebase {_phase_usage(result.steps, 'rebase')}, "
        f"review {_phase_usage(result.steps, 'post_rebase_review')}, "
        f"judgment {_judgment_usage(result)}"
    )


def _phase_usage(steps: tuple[Any, ...], phase: str) -> str:
    phase_steps = [step for step in steps if getattr(step, "phase", None) == phase]
    if not phase_steps:
        return "not used"
    statuses = {getattr(step, "status", None) for step in phase_steps}
    if "completed" in statuses:
        return "used"
    if "conditional" in statuses:
        return "conditional"
    if statuses <= {"skipped"}:
        return "not used"
    return "not used"


def _judgment_usage(result: LandResult) -> str:
    if result.judgment_artifact_id or result.judgment_key:
        return "used"
    return _phase_usage(result.steps, "judge")


def _format_task_ids(task_ids: tuple[str, ...]) -> str:
    return ", ".join(task_ids) if task_ids else "none"
