"""CLI adapter for operator-triggered landing."""

from __future__ import annotations

import argparse

from gza.cli._common import get_store, resolve_id
from gza.config import Config
from gza.git import Git
from gza.landing import (
    LandingCollaborators,
    LandResult,
    LandTerminalResult,
    land_terminal_state,
    reconcile_terminal_merge_truth,
)


def cmd_land(args: argparse.Namespace) -> int:
    """Resolve and plan an operator-triggered landing attempt."""

    from gza.cli._common import _create_rebase_task
    from gza.cli.git_ops import _run_task_backed_rebase
    from gza.landing import LANDING_POLICIES, LandingCoordinator, LandRequest

    config = Config.load(args.project_dir)
    store = get_store(config, open_mode="query_only" if args.dry_run else "readwrite")
    git = Git(config.project_dir)
    task_id = resolve_id(config, args.task_id)
    policy = args.policy
    if policy not in LANDING_POLICIES:
        print(f"Error: unknown landing policy {policy!r}")
        return 2

    if hasattr(store, "resolve_merge_unit_subject"):
        terminal_result = land_terminal_state(
            store,
            LandRequest(task_id=task_id, policy=policy, dry_run=bool(args.dry_run)),
            collaborators=LandingCollaborators(
                reconcile_terminal_state=reconcile_terminal_merge_truth(git),
            ),
        )
        if isinstance(terminal_result, LandTerminalResult):
            print(_format_terminal_result(terminal_result))
            return 0

    result = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        create_rebase_task=_create_rebase_task,
        rebase_executor=_run_task_backed_rebase,
    ).run(
        LandRequest(task_id=task_id, policy=policy, dry_run=bool(args.dry_run))
    )
    for step in result.steps:
        print(f"{step.phase}: {step.status} - {step.summary}")

    if result.blocked is not None:
        print(result.blocked.terminal_sentence(task_id))
        return 1
    terminal_output = _format_terminal_result(result)
    if terminal_output is not None:
        print(terminal_output)
        return 0
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
            f"with {result.merge_provenance} provenance."
        )
        return 0
    print(f"Cannot land {task_id}: landing stopped before a terminal result.")
    return 1


def _format_terminal_result(result: LandResult | LandTerminalResult) -> str | None:
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
