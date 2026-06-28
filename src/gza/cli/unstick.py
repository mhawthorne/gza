"""CLI surface for manual parked-task clearing."""

from __future__ import annotations

import argparse

from ..config import Config
from ..console import truncate
from ..git import Git
from ..unstick import SUPPORTED_PARK_REASON_CLASSES, select_and_clear_parked_tasks
from ._common import get_store, parse_cli_tag_filters


def cmd_unstick(args: argparse.Namespace) -> int:
    """Clear eligible parked owner state without starting work."""
    task_ids = tuple(getattr(args, "task_ids", ()) or ())
    reason_classes = tuple(getattr(args, "reasons", ()) or ())
    select_all = bool(getattr(args, "all", False))
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

    if rearmed:
        print("Rearmed:")
        for outcome in rearmed:
            prompt = truncate(outcome.owner_task.prompt, 80)
            reason = outcome.reason_class or "unknown"
            print(f"  {outcome.owner_task.id} [{reason}] {prompt}")

    if skipped:
        print("Skipped:")
        for outcome in skipped:
            prompt = truncate(outcome.owner_task.prompt, 80)
            print(f"  {outcome.owner_task.id} {outcome.detail}: {prompt}")

    if not rearmed and not skipped:
        reasons = ", ".join(reason_classes) if reason_classes else ", ".join(SUPPORTED_PARK_REASON_CLASSES)
        print(f"No parked owners matched the requested selectors for reasons: {reasons}")
    return 0
