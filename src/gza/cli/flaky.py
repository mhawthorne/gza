"""CLI surface for bounded flaky reproduce-or-record investigations."""

from __future__ import annotations

from pathlib import Path

from ..config import Config
from ..flaky_investigations import (
    build_flaky_reproduction_plan,
    run_flaky_reproduction_plan,
)
from ..runner import _resolve_review_verify_timeout_grace_seconds, _resolve_review_verify_timeout_seconds
from ._common import get_store, resolve_id


def cmd_flaky_reproduce(args) -> int:
    """Run the reproduce-or-record harness for one flaky investigation task."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    task_id = resolve_id(config, args.task_id)
    task = store.get(task_id)
    if task is None:
        print(f"Error: Task not found: {task_id}")
        return 1

    hypotheses = tuple(str(value).strip() for value in getattr(args, "hypothesis", ()) if str(value).strip())
    try:
        plan = build_flaky_reproduction_plan(
            store,
            project_dir=Path(config.project_dir),
            task_id=task_id,
            runs=args.runs,
            enable_xdist=not bool(args.no_xdist),
            enable_randomization=not bool(args.no_randomization),
            randomization_seed_base=args.seed,
        )
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1

    result = run_flaky_reproduction_plan(
        store,
        project_dir=Path(config.project_dir),
        task_id=task_id,
        plan=plan,
        timeout_seconds=_resolve_review_verify_timeout_seconds(config),
        timeout_grace_seconds=_resolve_review_verify_timeout_grace_seconds(config),
        hypotheses=hypotheses,
    )
    print(f"Task: {task_id}")
    print(f"Harness cwd: {plan.working_directory}")
    print(f"Harness command: {plan.command}")
    print(f"Attempt budget: {plan.runs}")
    if result.reproduced:
        print(
            f"Reproduced the recorded flaky signature after {len(result.attempts)} attempt(s). "
            "Fix only after preserving this red-under-stress evidence."
        )
        return 0

    print(
        f"No matching reproduction within {len(result.attempts)} attempt(s). "
        f"Recorded inconclusive artifact id {result.inconclusive_artifact_id}."
    )
    if hypotheses:
        print("Hypotheses:")
        for hypothesis in hypotheses:
            print(f"- {hypothesis}")
    return 0
