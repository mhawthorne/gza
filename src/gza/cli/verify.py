"""Manual verify-gate CLI command."""

from __future__ import annotations

import argparse
from typing import Any, Literal

from gza.cli._common import get_store, resolve_id
from gza.cli.advance_engine import plan_manual_verify_gate_action
from gza.cli.advance_executor import AdvanceActionExecutionContext, execute_advance_action
from gza.cli.git_ops import (
    _advance_progress_console,
    _CliVerifyProgressHeartbeat,
    _merge_execution_status_error,
    _resolve_merge_subject,
    _resolve_merge_subject_query_only,
)
from gza.config import Config
from gza.git import Git
from gza.review_verify_state import (
    VerifyGateDecision,
    resolve_verify_gate_decision,
    select_current_merge_unit_verify_evidence,
)


def _unused_direct_worker(*_args: Any, **_kwargs: Any) -> Any:
    raise AssertionError("unused")


def _format_epoch(decision: VerifyGateDecision) -> str:
    epoch = decision.current_epoch
    if epoch is None:
        return "epoch: unavailable"
    return (
        f"epoch: branch={epoch.reviewed_branch or '<unknown>'} "
        f"head={epoch.reviewed_head_sha or '<unknown>'} "
        f"command={epoch.verify_command or '<none>'}"
    )


def _print_decision(
    *,
    store: Any,
    owner_task: Any,
    decision: VerifyGateDecision,
    prefix: str = "Verify gate",
) -> None:
    task_id = owner_task.id or "<unknown>"
    print(f"{prefix}: {decision.state} for {task_id}")
    print(_format_epoch(decision))
    result = decision.lookup.result
    if result is None:
        print("artifact: <none>")
        return
    print(f"verdict: {result.status} (exit {result.exit_status})")
    if result.failure:
        print(f"failure: {result.failure}")
    source_task_id = result.output_artifact_task_id or result.source_task_id
    if source_task_id and source_task_id != owner_task.id:
        print(f"evidence: {source_task_id}")
    artifact_path = result.output_artifact_path
    print(f"artifact: {artifact_path or '<none>'}")


def _state_for_current_lookup(decision: VerifyGateDecision) -> VerifyGateDecision:
    result = decision.lookup.result
    state: Literal["passed", "missing", "stale", "failed", "unavailable"]
    if result is None:
        state = "missing"
    elif not decision.lookup.is_current:
        state = "stale"
    elif result.status == "passed":
        state = "passed"
    elif result.status == "unavailable":
        state = "unavailable"
    else:
        state = "failed"
    return VerifyGateDecision(
        owner_task_id=decision.owner_task_id,
        current_epoch=decision.current_epoch,
        lookup=decision.lookup,
        state=state,
    )


def _effective_verify_gate_decision(
    *,
    store: Any,
    owner_task: Any,
    config: Config,
    git: Git,
    member_tasks: tuple[Any, ...] | None = None,
) -> VerifyGateDecision:
    owner_decision = resolve_verify_gate_decision(store, owner_task, config=config, git=git)
    selection = select_current_merge_unit_verify_evidence(
        store,
        owner_task,
        current_epoch=owner_decision.current_epoch,
        member_tasks=member_tasks,
    )
    if selection is None:
        return owner_decision
    return _state_for_current_lookup(
        VerifyGateDecision(
            owner_task_id=owner_task.id,
            current_epoch=owner_decision.current_epoch,
            lookup=selection.lookup,
            state=owner_decision.state,
        )
    )


def _verify_artifact_ids(store: Any, owner_task: Any) -> set[int]:
    if owner_task.id is None:
        return set()
    return {
        artifact.id
        for artifact in store.list_artifacts(owner_task.id, kind="verify_gate_result")
        if artifact.id is not None
    }


def _forced_run_persisted_current_result(
    *,
    decision: VerifyGateDecision,
    previous_artifact_ids: set[int],
) -> bool:
    result = decision.lookup.result
    if result is None or decision.state in {"missing", "stale"}:
        return False
    artifact_id = decision.lookup.artifact_id
    return artifact_id is not None and artifact_id not in previous_artifact_ids


def _print_forced_pre_existing_decision(
    *,
    store: Any,
    owner_task: Any,
    decision: VerifyGateDecision,
) -> None:
    _print_decision(
        store=store,
        owner_task=owner_task,
        decision=decision,
        prefix="Pre-existing verify gate evidence",
    )
    print("Forced verify rerun did not produce new current green evidence.")


def _make_verify_context(*, config: Config, store: Any, git: Git) -> AdvanceActionExecutionContext:
    return AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=getattr(config, "max_resume_attempts", 0),
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=_unused_direct_worker,
        create_resume_task=_unused_direct_worker,
        create_rebase_task=_unused_direct_worker,
        create_implement_task=_unused_direct_worker,
        spawn_worker=_unused_direct_worker,
        spawn_resume_worker=_unused_direct_worker,
        spawn_iterate_worker=_unused_direct_worker,
        config=config,
        git=git,
        heartbeat_for_lifecycle_phase=lambda phase, _task: _CliVerifyProgressHeartbeat(
            _advance_progress_console,
            "Verify gate" if phase == "verify" else f"Verify gate ({phase})",
        ),
    )


def cmd_verify(args: argparse.Namespace) -> int:
    """Rerun or inspect a task merge unit's lifecycle verify gate."""
    config = Config.load(args.project_dir)
    store = get_store(config, open_mode="query_only" if args.dry_run else "readwrite")
    git = Git(config.project_dir)
    task_id = resolve_id(config, args.task_id)
    target_branch = git.default_branch()

    resolved = _resolve_merge_subject_query_only(store, git, task_id, target_branch=target_branch)
    if resolved is None:
        print(f"Error: Task {task_id} not found")
        return 1
    if resolved.merge_resolution_warning:
        print(f"Error: {resolved.merge_resolution_warning}")
        return 1
    if resolved.merge_source_warning:
        print(f"Error: {resolved.merge_source_warning}")
        return 1
    if not args.dry_run and _resolve_merge_subject(store, git, task_id, target_branch=target_branch) is None:
        print(f"Error: Task {task_id} not found")
        return 1
    execution_task = resolved.execution_task
    owner_task = resolved.merge_subject
    if owner_task.id is None:
        print(f"Error: Task {task_id} has no resolvable verify owner")
        return 1

    status_error = _merge_execution_status_error(owner_task.id, execution_task)
    if status_error is not None:
        print(f"Error: {status_error}")
        return 1
    owner_current_decision = resolve_verify_gate_decision(store, owner_task, config=config, git=git)
    resolved_members = resolved.merge_member_tasks or None
    current_decision = _effective_verify_gate_decision(
        store=store,
        owner_task=owner_task,
        config=config,
        git=git,
        member_tasks=resolved_members,
    )
    if args.dry_run:
        _print_decision(store=store, owner_task=owner_task, decision=current_decision, prefix="[dry-run] Verify gate")
        return 0 if current_decision.state == "passed" else 1

    if current_decision.state == "passed" and owner_current_decision.state == "passed" and not args.force:
        _print_decision(store=store, owner_task=owner_task, decision=current_decision)
        print("Verify gate already passed for the current epoch; use --force to rerun.")
        return 0

    action = plan_manual_verify_gate_action(
        config,
        store,
        git,
        execution_task,
        target_branch,
        verify_owner_task=owner_task,
        member_tasks=resolved_members,
        selected_for_merge=True,
    )
    reconciled = False
    if action is not None and action.get("type") == "reconcile_verify_gate_evidence":
        result = execute_advance_action(
            task=execution_task,
            action=action,
            context=_make_verify_context(config=config, store=store, git=git),
        )
        message = result.success_message or result.message or result.error_message
        if message:
            print(message)
        if result.status != "success":
            refreshed_owner = store.get(owner_task.id) or owner_task
            refreshed_decision = _effective_verify_gate_decision(
                store=store,
                owner_task=refreshed_owner,
                config=config,
                git=git,
                member_tasks=resolved_members,
            )
            if args.force:
                _print_forced_pre_existing_decision(
                    store=store,
                    owner_task=refreshed_owner,
                    decision=refreshed_decision,
                )
                return 1
            _print_decision(store=store, owner_task=refreshed_owner, decision=refreshed_decision)
            return 1
        reconciled = True
        refreshed_owner = store.get(owner_task.id) or owner_task
        owner_task = refreshed_owner
        current_decision = _effective_verify_gate_decision(
            store=store,
            owner_task=owner_task,
            config=config,
            git=git,
            member_tasks=resolved_members,
        )
        action = plan_manual_verify_gate_action(
            config,
            store,
            git,
            execution_task,
            target_branch,
            verify_owner_task=owner_task,
            member_tasks=resolved_members,
            selected_for_merge=True,
        )

    if reconciled and current_decision.state == "passed" and not args.force:
        _print_decision(store=store, owner_task=owner_task, decision=current_decision)
        print("Verify gate already passed for the current epoch; use --force to rerun.")
        return 0

    verify_owner_task = owner_task
    if action is None or action.get("type") != "verify_gate":
        action = plan_manual_verify_gate_action(
            config,
            store,
            git,
            execution_task,
            target_branch,
            verify_owner_task=verify_owner_task,
            member_tasks=resolved_members,
            selected_for_merge=True,
        )
    previous_artifact_ids = _verify_artifact_ids(store, verify_owner_task) if args.force else set()
    result = execute_advance_action(
        task=execution_task,
        action=action,
        context=_make_verify_context(config=config, store=store, git=git),
    )
    message = result.success_message or result.message or result.error_message
    if message:
        print(message)

    owner_task_id = result.handled_task_id or verify_owner_task.id
    if owner_task_id is None:
        print(f"Error: Task {task_id} has no resolvable verify owner")
        return 1
    refreshed_owner = store.get(owner_task_id) or verify_owner_task
    refreshed_decision = _effective_verify_gate_decision(
        store=store,
        owner_task=refreshed_owner,
        config=config,
        git=git,
        member_tasks=resolved_members,
    )
    forced_run_persisted_current_result = _forced_run_persisted_current_result(
        decision=refreshed_decision,
        previous_artifact_ids=previous_artifact_ids,
    )
    if args.force and not forced_run_persisted_current_result:
        _print_forced_pre_existing_decision(
            store=store,
            owner_task=refreshed_owner,
            decision=refreshed_decision,
        )
        return 1
    _print_decision(store=store, owner_task=refreshed_owner, decision=refreshed_decision)
    if args.force:
        return (
            0
            if result.status == "success" and result.work_done and refreshed_decision.state == "passed"
            else 1
        )
    return 0 if refreshed_decision.state == "passed" else 1
