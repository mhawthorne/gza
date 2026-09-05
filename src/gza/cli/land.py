"""CLI adapter for operator-triggered landing."""

from __future__ import annotations

import argparse

from gza.cli._common import get_store, resolve_id
from gza.config import Config
from gza.git import Git


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
