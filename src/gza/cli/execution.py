"""Task execution commands: run, add, edit, retry, resume, review, improve, fix, iterate."""

import argparse
import json
import os
import shutil
import signal
import sys
import tempfile
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from ..advance_engine import (
    _resolve_and_persist_post_merge_rebase_state,
    _resolve_current_merge_source,
    _resolve_latest_plan_source,
    count_completed_review_cycles,
)
from ..concurrency import (
    LaunchPermit,
    MaxConcurrentTasksError,
    launch_permit,
    launch_permits,
    release_task_launch_permit,
    reserve_task_launch_permit,
)
from ..config import (
    DEFAULT_MAX_FAILED_CLOSING_REVIEW_RETRIES,
    DEFAULT_MAX_RESUME_ATTEMPTS,
    Config,
    is_model_compatible_with_provider,
    provider_model_mismatch_error,
)
from ..console import format_duration
from ..db import (
    TASK_COMMENT_KIND_FEEDBACK,
    InvalidTaskIdError,
    SqliteTaskStore,
    Task as DbTask,
    _launch_editor,
    _normalize_tags,
    add_task_interactive,
    edit_task_interactive,
    task_id_numeric_key,
    validate_prompt,
)
from ..dependency_preconditions import plan_dependency_awaits_review
from ..derived_tags import resolve_derived_task_tags
from ..extractions import (
    ExtractionDraft,
    ExtractionError,
    SourceSelection,
    infer_selected_paths,
    normalize_selected_paths,
    plan_extraction,
    read_paths_file,
    resolve_source_selection,
    write_extraction_bundle,
)
from ..failure_reasons import mark_task_failed_from_cause
from ..git import Git
from ..lifecycle_completion import merge_state_is_terminal_for_lifecycle
from ..lineage import resolve_impl_task
from ..log_paths import ops_log_path_for
from ..merge_state import resolve_task_merge_state_for_target
from ..operator_state import blocked_dependency_error_message
from ..plan_review_verdict import get_plan_review_outcome, validate_plan_review_manifest
from ..prompts import PromptBuilder
from ..query import (
    get_base_task_slug as _get_base_task_slug,
    get_code_changing_descendants_for_root,
)
from ..recovery_engine import (
    FailedRecoveryDecision,
    _classify_empty_task_recovery_state,
    decide_failed_task_recovery,
    empty_task_requires_recovery,
    get_failed_recovery_needs_attention_reason,
    get_manual_resume_override_descendant,
    resolve_pending_recovery_execution_mode,
    resolve_recovery_planning_task,
)
from ..review_tasks import build_review_blocker_dispute_metadata
from ..review_verdict import get_review_report
from ..runner import RunInvocationContext, generate_slug, remove_task_startup_artifacts
from ..status_ops import apply_manual_task_status
from ..task_types import CLI_ADD_TASK_TYPES
from ..workers import WorkerRegistry
from ._common import (
    _REUSE_WORKER_OWNER_ENV,
    _REUSE_WORKER_OWNER_OUTER,
    _REUSE_WORKER_REENTRY_ENV,
    _REUSE_WORKER_SESSION_ENV,
    DuplicateReviewError,
    PlanReviewMaterializationResult,
    _allow_pr_required_retry,
    _create_implementation_task_from_source,
    _create_improve_task,
    _create_or_reuse_followup_tasks,
    _create_plan_improve_task,
    _create_plan_review_task,
    _create_rebase_task,
    _create_resume_task,
    _create_retry_task,
    _create_review_adjudication_task,
    _create_review_task,
    _materialize_plan_review_slices,
    _plan_review_timeout_budget_minutes,
    _prepare_task_for_immediate_execution,
    _prepare_task_for_reserved_launch,
    _record_preclaim_startup_failure,
    _resolved_review_scope_metadata,
    _run_as_worker,
    _run_foreground,
    _spawn_background_iterate_worker,
    _spawn_background_resume_worker,
    _spawn_background_worker,
    _spawn_background_workers,
    format_no_runnable_message_for_tags,
    format_review_outcome,
    get_review_verdict,
    get_store,
    get_task_step_count,
    parse_cli_tag_filters,
    persist_plan_review_override_manifest,
    phase1_error,
    print_phase1_message,
    resolve_comments_improve_action,
    resolve_effective_plan_review_manifest_state,
    resolve_id,
    resolve_improve_action,
    run_with_recovery,
    set_task_urgency,
)
from ._recovery_lane import collect_recovery_lane_entries
from .advance_engine import (
    NEEDS_ATTENTION_LABEL,
    WORKER_CONSUMING_ACTIONS,
    classify_advance_action,
    determine_next_action,
    format_needs_attention_entry_for_display,
    needs_attention_recommends_fix,
    resolve_closing_review_action,
    resolve_subject_task,
)
from .advance_executor import (
    build_failed_recovery_needs_attention_result,
    build_improve_needs_attention_result,
    resolve_execution_needs_attention,
)
from .log import _latest_worker_for_task, _running_worker_id_for_task
from .query import _get_orphaned_tasks, _print_orphaned_warning

_ITERATE_TERMINAL_NO_WORK_REASON_CODES = frozenset({"merge_unit_empty", "merge_unit_redundant"})

ExecutionMode = Literal["queue", "run", "background"]


def _execution_mode(args: argparse.Namespace) -> ExecutionMode:
    if getattr(args, "background", False):
        return "background"
    if getattr(args, "run", False):
        return "run"
    return "queue"


def _find_held_plan_in_based_on_lineage(
    store: SqliteTaskStore,
    *,
    based_on_id: str | None,
) -> DbTask | None:
    """Return the held plan found in a based_on lineage, if any."""
    current_id = based_on_id
    seen: set[str] = set()
    while current_id is not None and current_id not in seen:
        seen.add(current_id)
        current = store.get(current_id)
        if current is None:
            return None
        if plan_dependency_awaits_review(current):
            return current
        current_id = current.based_on
    return None


def _held_plan_implement_creation_error(plan_id: str) -> str:
    """Return the operator-facing refusal for implement tasks sourced from a held plan."""
    return (
        f"Error: plan {plan_id} is held for review; release it with "
        f"uv run gza implement {plan_id} or uv run gza edit {plan_id} --no-hold-for-review "
        "before creating implement dependents."
    )


def _validate_new_implement_source_not_held_for_review(
    store: SqliteTaskStore,
    *,
    based_on_id: str | None,
    depends_on_id: str | None,
) -> str | None:
    """Return a refusal message when a new implement task would source from a held plan."""
    if depends_on_id is not None:
        dep_task = store.get(depends_on_id)
        if dep_task is not None and plan_dependency_awaits_review(dep_task):
            assert dep_task.id is not None
            return _held_plan_implement_creation_error(dep_task.id)

    held_plan = _find_held_plan_in_based_on_lineage(store, based_on_id=based_on_id)
    if held_plan is None:
        return None
    assert held_plan.id is not None
    return _held_plan_implement_creation_error(held_plan.id)


def _foreground_command_invocation(command: str) -> RunInvocationContext:
    """Build command-specific invocation metadata for foreground runner calls."""
    return RunInvocationContext(
        command=command,
        execution_mode="foreground_worker",
    )


@dataclass(frozen=True)
class IterateSummaryRow:
    iteration_index: int
    task_type: str
    task_id: str | None
    verdict: str | None
    duration_seconds: float | None
    steps: int | None
    cost_usd: float | None
    status: str
    failure_reason: str | None
    completion_reason: str | None


def _format_compact_duration(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    return format_duration(seconds).replace(" ", "")


def _format_summary_status(row: IterateSummaryRow) -> str:
    if row.failure_reason:
        return f"{row.status} ({row.failure_reason})"
    if row.completion_reason:
        return f"{row.status} ({row.completion_reason})"
    return row.status


def _append_iterate_summary_row(
    store: SqliteTaskStore,
    rows: list[IterateSummaryRow],
    *,
    iteration_index: int,
    task_type: str,
    task: DbTask | None,
    verdict: str | None = None,
    status: str | None = None,
    failure_reason: str | None = None,
) -> None:
    refreshed_task = task
    if task is not None and task.id is not None:
        refreshed_task = store.get(task.id) or task

    row_status = status or (refreshed_task.status if refreshed_task else "failed")
    row_failure_reason = (refreshed_task.failure_reason if refreshed_task else None) or failure_reason
    row_completion_reason = refreshed_task.completion_reason if refreshed_task else None

    rows.append(
        IterateSummaryRow(
            iteration_index=iteration_index,
            task_type=task_type,
            task_id=refreshed_task.id if refreshed_task else None,
            verdict=verdict,
            duration_seconds=refreshed_task.duration_seconds if refreshed_task else None,
            steps=get_task_step_count(refreshed_task) if refreshed_task else None,
            cost_usd=refreshed_task.cost_usd if refreshed_task else None,
            status=row_status,
            failure_reason=row_failure_reason,
            completion_reason=row_completion_reason,
        )
    )


def _run_iterate_task_with_recovery(
    *,
    args: argparse.Namespace,
    config: Config,
    store: SqliteTaskStore,
    task_to_run: DbTask,
    max_resume_attempts: int,
    initial_resume: bool = False,
    invocation_name: str = "iterate",
    on_recovery: Callable[[DbTask, DbTask, FailedRecoveryDecision], None] | None = None,
    on_terminal_skip: Callable[[DbTask, FailedRecoveryDecision, int], None] | None = None,
) -> tuple[DbTask, int, FailedRecoveryDecision | None]:
    terminal_skip_decision: FailedRecoveryDecision | None = None

    def _run_one(task: DbTask, resume_flag: bool) -> int:
        assert task.id is not None
        force = getattr(args, "force", False)
        if resume_flag or initial_resume:
            return _run_foreground(
                config,
                task_id=task.id,
                resume=True,
                force=force,
                invocation=_foreground_command_invocation(invocation_name),
            )
        return _run_foreground(
            config,
            task_id=task.id,
            force=force,
            invocation=_foreground_command_invocation(invocation_name),
        )

    def _default_on_recovery(
        failed_task: DbTask,
        recovery_task: DbTask,
        decision: FailedRecoveryDecision,
    ) -> None:
        assert failed_task.id is not None
        assert recovery_task.id is not None
        reason = failed_task.failure_reason or "UNKNOWN"
        print(
            f"  Auto-{decision.action}: {failed_task.id} failed with {reason}; "
            f"created {recovery_task.id} (attempt {decision.attempt_index}/{decision.attempt_limit})."
        )

    def _capture_terminal_skip(
        failed_task: DbTask,
        decision: FailedRecoveryDecision,
        failure_rc: int,
    ) -> None:
        nonlocal terminal_skip_decision
        terminal_skip_decision = decision
        if on_terminal_skip is not None:
            on_terminal_skip(failed_task, decision, failure_rc)

    final_task, rc = run_with_recovery(
        config,
        store,
        task_to_run,
        run_task=_run_one,
        max_resume_attempts=max_resume_attempts,
        on_recovery=on_recovery or _default_on_recovery,
        on_terminal_skip=_capture_terminal_skip,
    )
    return final_task, rc, terminal_skip_decision


@dataclass(frozen=True)
class _IterateBackgroundPreflightContext:
    """Prepared git context for iterate background preflight."""

    git_runtime: Git
    target_branch: str


def _finalize_immediate_execution_task(
    *,
    args: argparse.Namespace,
    config: Config,
    task: DbTask,
    rollback_on_failure: bool,
    emit_created: Callable[[], None],
    rollback_cleanup: Callable[[], None] | None = None,
    reserved_permit: LaunchPermit | None = None,
) -> tuple[DbTask | None, LaunchPermit | None]:
    """Print task creation only after immediate-execution preparation succeeds."""
    if _execution_mode(args) == "queue":
        emit_created()
        return task, None

    store = get_store(config)
    if reserved_permit is not None:
        permit = reserved_permit
    else:
        try:
            permit = launch_permit(config, store)
        except MaxConcurrentTasksError as exc:
            print(f"Error: {exc}")
            return None, None

    prepared_task = _prepare_task_for_reserved_launch(
        config,
        task,
        permit=permit,
        rollback_on_failure=rollback_on_failure,
        rollback_cleanup=rollback_cleanup,
    )
    if prepared_task is None:
        return None, None

    emit_created()
    return prepared_task, permit


def _reserve_immediate_execution_permit(
    *,
    args: argparse.Namespace,
    config: Config,
    store: SqliteTaskStore,
) -> LaunchPermit | Literal[False] | None:
    if _execution_mode(args) == "queue":
        return None
    if not isinstance(getattr(config, "max_concurrent", None), int):
        return None
    try:
        return launch_permit(config, store)
    except MaxConcurrentTasksError as exc:
        print(f"Error: {exc}")
        return False


@contextmanager
def _release_reserved_launch_unless_transferred(
    reserved_launch: LaunchPermit | Literal[False] | None,
) -> Iterator[Callable[[], None]]:
    transferred = False

    def _mark_transferred() -> None:
        nonlocal transferred
        transferred = True

    try:
        yield _mark_transferred
    finally:
        if isinstance(reserved_launch, LaunchPermit) and not transferred:
            reserved_launch.release()


def _resolve_iterate_merge_state_for_current_target(
    *,
    store: SqliteTaskStore,
    impl_task: DbTask,
    git_runtime: Git,
    target_branch: str,
) -> str | None:
    """Resolve iterate merge suppression state for the current target branch.

    Stored merge-unit state remains authoritative for its recorded target, but
    iterate must suppress when current-target branch reachability proves the
    source ref is already merged elsewhere. Missing refs remain unproven.
    """
    return resolve_task_merge_state_for_target(
        store=store,
        task=impl_task,
        git=git_runtime,
        target_branch=target_branch,
    )


def _format_iterate_terminal_merge_state_message(
    *,
    store: SqliteTaskStore,
    requested_impl_task: DbTask,
    iterate_task: DbTask,
    resolved_from_failed_ancestor: bool,
    merge_state: str | None,
) -> str | None:
    """Return the operator-facing noop message for terminal iterate merge states."""
    if not merge_state_is_terminal_for_lifecycle(merge_state):
        return None

    if merge_state in {"empty", "redundant"}:
        if resolve_pending_recovery_execution_mode(iterate_task) is not None:
            return None
        empty_recovery_state = _classify_empty_task_recovery_state(store, iterate_task, merge_state=merge_state)
        if empty_recovery_state == "requires_recovery":
            return None
        if empty_recovery_state == "resolved" and iterate_task.status == "failed":
            return (
                "No remaining iterate action: "
                f"failed implementation {iterate_task.id} was already resolved by landed lineage or completed "
                "recovery work."
            )
        if merge_state == "redundant":
            if resolved_from_failed_ancestor:
                return (
                    "No remaining iterate action: "
                    f"failed implementation {requested_impl_task.id} was fully recovered by descendant "
                    f"{iterate_task.id}; commits are already present on target."
                )
            return (
                "No remaining iterate action: "
                f"implementation {iterate_task.id}'s commits are already present on target."
            )
        if resolved_from_failed_ancestor:
            return (
                "No remaining iterate action: "
                f"failed implementation {requested_impl_task.id} was fully recovered by descendant "
                f"{iterate_task.id} with no remaining commits to land."
            )
        return (
            "No remaining iterate action: "
            f"implementation {iterate_task.id} has no remaining commits to land."
        )

    if resolved_from_failed_ancestor:
        return (
            "No remaining iterate action: "
            f"failed implementation {requested_impl_task.id} was fully recovered by merged descendant "
            f"{iterate_task.id}."
        )
    return f"No remaining iterate action: implementation {iterate_task.id} is already merged."


def _reconcile_iterate_already_merged(
    *,
    store: SqliteTaskStore,
    impl_task: DbTask,
    git_runtime: Git,
    target_branch: str,
) -> None:
    """Persist merged state when iterate proves the implementation is already landed.

    Iterate may prove current-target reachability even when stored merge truth is stale.
    Reconcile only when the canonical merge-state target matches the proven target, or
    when a merge-unit-backed store would create the same target by default.
    """
    if impl_task.id is None:
        return

    merge_unit = store.resolve_merge_unit_for_task(impl_task.id)
    if impl_task.merge_status == "merged" and merge_unit is None:
        return
    if merge_unit is not None:
        if merge_unit.target_branch != target_branch or merge_unit.state == "merged":
            return
    elif store.supports_merge_units():
        default_target = git_runtime.default_branch()
        if default_target != target_branch:
            return

    store.set_merge_status(impl_task.id, "merged")

    refreshed_unit = store.resolve_merge_unit_for_task(impl_task.id)
    if refreshed_unit is not None:
        if refreshed_unit.target_branch == target_branch and refreshed_unit.state == "merged":
            return
        raise RuntimeError(
            f"stored merge unit remained {refreshed_unit.state!r} for target {refreshed_unit.target_branch!r}"
        )

    refreshed_task = store.get(impl_task.id)
    if refreshed_task is not None and refreshed_task.merge_status == "merged":
        return
    raise RuntimeError("stored task merge status remained unmerged")


def _iterate_rebase_target_already_merged(
    *,
    store: SqliteTaskStore,
    git_runtime: Git,
    task: DbTask,
    target_branch: str,
) -> bool:
    """Return True when local state proves a scheduled rebase target is already merged."""
    return _resolve_and_persist_post_merge_rebase_state(
        store,
        git_runtime,
        task,
        target_branch,
        merge_source=_resolve_current_merge_source(git_runtime, task.branch) if task.branch else None,
    ).already_merged


def _run_with_registered_worker(
    *,
    config: Config,
    worker_id: str | None,
    run_command: Any,
    allow_same_pid_reentry: bool = True,
) -> int:
    """Run a command inside a worker session without pre-consuming capacity.

    The outer session wrapper sets worker ownership env so nested foreground
    launches can reuse a single registry entry and slot serially. It must not
    publish an idle anonymous worker before the first task launch, or
    ``max_concurrent: 1`` self-blocks foreground ``gza work`` sessions.
    """
    if not worker_id:
        return run_command()

    registry = WorkerRegistry(config.workers_path)
    previous_worker_id = os.environ.get("GZA_WORKER_ID")
    previous_worker_mode = os.environ.get("GZA_WORKER_MODE")
    previous_reuse_worker_owner = os.environ.get(_REUSE_WORKER_OWNER_ENV)
    previous_reuse_worker_reentry = os.environ.get(_REUSE_WORKER_REENTRY_ENV)
    previous_reuse_worker_session = os.environ.get(_REUSE_WORKER_SESSION_ENV)
    original_sigint = signal.getsignal(signal.SIGINT)
    original_sigterm = signal.getsignal(signal.SIGTERM)

    def _signal_handler(signum, frame):
        del signum, frame
        if registry.get(worker_id) is not None:
            registry.mark_completed(worker_id, exit_code=1, status="failed")
        sys.exit(1)

    os.environ["GZA_WORKER_ID"] = worker_id
    os.environ["GZA_WORKER_MODE"] = "1"
    os.environ[_REUSE_WORKER_OWNER_ENV] = _REUSE_WORKER_OWNER_OUTER
    os.environ[_REUSE_WORKER_REENTRY_ENV] = "1" if allow_same_pid_reentry else "0"
    os.environ[_REUSE_WORKER_SESSION_ENV] = "1"
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    def _finalize_idle_wrapper_registration() -> bool:
        worker = registry.get(worker_id)
        if worker is None:
            return True
        if worker.task_id is not None:
            return False
        registry.remove(worker_id)
        return True

    try:
        rc = run_command()
        startup_failed_without_running = False
        worker = registry.get(worker_id)
        if rc != 0 and worker is not None and worker.task_id is not None:
            startup_task = get_store(config).get(worker.task_id)
            startup_failed_without_running = startup_task is not None and startup_task.status == "pending"
            if startup_failed_without_running:
                _record_preclaim_startup_failure(
                    config=config,
                    registry=registry,
                    worker_id=worker_id,
                    task=startup_task,
                    exit_code=rc,
                )
        if _finalize_idle_wrapper_registration():
            return rc
        registry.mark_completed(
            worker_id,
            exit_code=rc,
            status="completed" if rc == 0 else "failed",
            completion_reason="startup failure before task claim" if rc != 0 and startup_failed_without_running else None,
        )
        return rc
    except Exception:
        if not _finalize_idle_wrapper_registration():
            registry.mark_completed(worker_id, exit_code=1, status="failed")
        raise
    finally:
        signal.signal(signal.SIGINT, original_sigint)
        signal.signal(signal.SIGTERM, original_sigterm)
        if previous_worker_id is None:
            os.environ.pop("GZA_WORKER_ID", None)
        else:
            os.environ["GZA_WORKER_ID"] = previous_worker_id
        if previous_worker_mode is None:
            os.environ.pop("GZA_WORKER_MODE", None)
        else:
            os.environ["GZA_WORKER_MODE"] = previous_worker_mode
        if previous_reuse_worker_owner is None:
            os.environ.pop(_REUSE_WORKER_OWNER_ENV, None)
        else:
            os.environ[_REUSE_WORKER_OWNER_ENV] = previous_reuse_worker_owner
        if previous_reuse_worker_reentry is None:
            os.environ.pop(_REUSE_WORKER_REENTRY_ENV, None)
        else:
            os.environ[_REUSE_WORKER_REENTRY_ENV] = previous_reuse_worker_reentry
        if previous_reuse_worker_session is None:
            os.environ.pop(_REUSE_WORKER_SESSION_ENV, None)
        else:
            os.environ[_REUSE_WORKER_SESSION_ENV] = previous_reuse_worker_session


def _selected_tag_filters(args: argparse.Namespace) -> tuple[tuple[str, ...] | None, bool]:
    return parse_cli_tag_filters(args)


def _selected_tags_for_new_task(args: argparse.Namespace) -> tuple[str, ...]:
    tags, _any_tag = parse_cli_tag_filters(args)
    return tags or ()


def _selected_tag_override_for_derived_task(
    args: argparse.Namespace,
) -> tuple[str, ...] | None:
    """Return explicit derived-task tag override, preserving omission as ``None``."""
    return parse_cli_tag_filters(args)[0]


def _extract_run_args(args: argparse.Namespace, task_ids: list[str]) -> argparse.Namespace:
    """Clone extract args and seed defaults expected by shared run helpers."""
    worker_args = argparse.Namespace(**vars(args))
    worker_args.task_ids = task_ids
    if not hasattr(worker_args, "worker_mode"):
        worker_args.worker_mode = False
    if not hasattr(worker_args, "count"):
        worker_args.count = None
    return worker_args


@dataclass
class _CreatedImmediateExecutionTask:
    """Task plus rollback hooks for delayed immediate execution."""

    task: DbTask
    emit_created: Callable[[], None]
    rollback_cleanup: Callable[[], None]


def _rollback_created_immediate_execution_tasks(
    *,
    config: Config,
    created_tasks: list[_CreatedImmediateExecutionTask],
) -> None:
    """Best-effort rollback for a command-scoped immediate-execution batch."""
    store = get_store(config)
    for created in created_tasks:
        if created.task.id is not None:
            remove_task_startup_artifacts(config, created.task)
        created.rollback_cleanup()
        if created.task.id is not None:
            store.delete(created.task.id)


def _rollback_created_extract_task(
    *,
    config: Config,
    store: SqliteTaskStore,
    task: DbTask,
    bundle_dir: Path | None,
    failure_reason: str,
) -> None:
    """Best-effort rollback for extract creator-phase failures before worker handoff."""
    if task.id is None:
        return
    remove_task_startup_artifacts(config, task)
    if bundle_dir is not None:
        shutil.rmtree(bundle_dir, ignore_errors=True)
    if store.delete(task.id):
        return
    created_task = store.get(task.id)
    if created_task is not None:
        mark_task_failed_from_cause(
            task=created_task,
            config=config,
            store=store,
            log_file=None,
            explicit_reason=failure_reason,
        )


def _make_extract_created_emitter(
    *,
    config: Config,
    draft: ExtractionDraft,
    source: SourceSelection,
    bundle_dir: Path,
    impl_task: DbTask,
) -> Callable[[], None]:
    def _emit_created() -> None:
        _print_extraction_plan_summary(
            draft=draft,
            source_label=_describe_extract_source_label(source),
            heading=f"✓ Created extract implement task {impl_task.id}",
            bundle_path=bundle_dir.relative_to(config.project_dir),
        )

    return _emit_created


def _make_extract_rollback_cleanup(bundle_dir: Path) -> Callable[[], None]:
    def _cleanup() -> None:
        shutil.rmtree(bundle_dir, ignore_errors=True)

    return _cleanup


def _release_launch_permits(permits: list[LaunchPermit]) -> None:
    for permit in reversed(permits):
        permit.release()


def _create_reserved_extract_task(
    *,
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    draft: ExtractionDraft,
    source: SourceSelection,
    tags: tuple[str, ...],
    create_review: bool,
    create_pr: bool,
    branch_type: str | None,
    model: str | None,
    provider: str | None,
    skip_learnings: bool,
    base_branch: str | None,
    permit: LaunchPermit,
    prepare_for_launch: bool,
) -> tuple[_CreatedImmediateExecutionTask, DbTask] | None:
    impl_task, bundle_dir = _create_extract_task(
        config=config,
        store=store,
        git=git,
        draft=draft,
        tags=tags,
        create_review=create_review,
        create_pr=create_pr,
        branch_type=branch_type,
        model=model,
        provider=provider,
        skip_learnings=skip_learnings,
        base_branch=base_branch,
    )
    created_task = _CreatedImmediateExecutionTask(
        task=impl_task,
        emit_created=_make_extract_created_emitter(
            config=config,
            draft=draft,
            source=source,
            bundle_dir=bundle_dir,
            impl_task=impl_task,
        ),
        rollback_cleanup=_make_extract_rollback_cleanup(bundle_dir),
    )
    if impl_task.id is None:
        permit.release()
        return None
    if prepare_for_launch:
        prepared_task = _prepare_task_for_reserved_launch(
            config,
            impl_task,
            permit=permit,
            rollback_on_failure=True,
            rollback_cleanup=created_task.rollback_cleanup,
        )
        if prepared_task is None:
            return None
        return created_task, prepared_task
    reserve_task_launch_permit(str(impl_task.id), permit)
    return created_task, impl_task


def _maybe_reinterpret_extract_source_as_path(
    config: Config,
    source_task_id_raw: str | None,
    source_branch: str | None,
    source_commits: tuple[str, ...],
    selected_raw: list[str],
    store: SqliteTaskStore | None,
) -> tuple[str | None, list[str], SqliteTaskStore | None]:
    """Treat the optional SOURCE positional as a path for branch/commit selectors."""
    if not source_task_id_raw or (not source_branch and not source_commits):
        return source_task_id_raw, selected_raw, store

    try:
        source_id_candidate = resolve_id(config, source_task_id_raw)
    except InvalidTaskIdError:
        selected_raw.insert(0, source_task_id_raw)
        return None, selected_raw, store

    if store is None:
        store = get_store(config)
    if store.get(source_id_candidate) is None:
        selected_raw.insert(0, source_task_id_raw)
        return None, selected_raw, store
    return source_task_id_raw, selected_raw, store


def _format_extraction_diff_summary(draft: ExtractionDraft) -> list[str]:
    """Render concise per-file diff metadata for extract command summaries."""
    lines: list[str] = []
    for summary in draft.file_summaries:
        path_repr = summary.selected_path
        if summary.status in {"R", "C"} and summary.old_path and summary.new_path:
            path_repr = f"{summary.old_path} -> {summary.new_path}"

        stat_suffix = ""
        if summary.binary:
            stat_suffix = " [binary]"
        elif summary.additions is not None and summary.deletions is not None:
            stat_suffix = f" (+{summary.additions}/-{summary.deletions})"

        lines.append(f"    - {summary.status}: {path_repr}{stat_suffix}")
    return lines


def _print_extraction_plan_summary(
    *,
    draft: ExtractionDraft,
    source_label: str,
    heading: str,
    bundle_path: Path | None = None,
    dry_run: bool = False,
) -> None:
    """Print a concise summary of the extraction plan."""
    print(heading)
    print(f"  Source: {source_label}")
    print(f"  Selected files: {len(draft.selected_paths)}")
    for path in draft.selected_paths:
        print(f"    - {path}")
    print("  Diff summary:")
    for line in _format_extraction_diff_summary(draft):
        print(line)
    if bundle_path is not None:
        print(f"  Bundle: {bundle_path}")
    if dry_run:
        print("  No task created; no extraction bundle written.")


def _describe_extract_source_label(source: SourceSelection) -> str:
    if source.source_task_id:
        return f"task {source.source_task_id}"
    if source.source_commits:
        if len(source.source_commits) == 1:
            return f"commit {source.source_commits[0][:12]}"
        return f"{len(source.source_commits)} commits"
    assert source.source_branch is not None
    return f"branch {source.source_branch}"


def _create_extract_task(
    *,
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    draft: ExtractionDraft,
    tags: tuple[str, ...],
    create_review: bool,
    create_pr: bool,
    branch_type: str | None,
    model: str | None,
    provider: str | None,
    skip_learnings: bool,
    base_branch: str | None,
) -> tuple[DbTask, Path]:
    impl_task = store.add(
        draft.prompt,
        task_type="implement",
        tags=tags,
        create_review=create_review,
        create_pr=create_pr,
        same_branch=False,
        base_branch=base_branch,
        task_type_hint=branch_type,
        model=model,
        provider=provider,
        skip_learnings=skip_learnings,
        trigger_source="manual",
    )

    bundle_dir: Path | None = None
    bundle_write_started = False
    try:
        if impl_task.slug is None:
            slug_prompt = next(
                (line.strip() for line in draft.prompt.splitlines() if line.strip()),
                draft.prompt,
            )
            impl_task.slug = generate_slug(
                slug_prompt,
                existing_id=None,
                log_path=config.log_path,
                git=git,
                store=store,
                exclude_task_id=impl_task.id,
                project_name=config.project_name,
                project_prefix=config.project_prefix,
                branch_strategy=config.branch_strategy,
                explicit_type=impl_task.task_type_hint,
            )
            store.update(impl_task)

        bundle_write_started = True
        bundle_dir = write_extraction_bundle(
            project_dir=config.project_dir,
            task=impl_task,
            draft=draft,
        )
    except Exception:
        _rollback_created_extract_task(
            config=config,
            store=store,
            task=impl_task,
            bundle_dir=bundle_dir,
            failure_reason=(
                "EXTRACTION_BUNDLE_WRITE_FAILED"
                if bundle_write_started
                else "EXTRACTION_TASK_CREATE_FAILED"
            ),
        )
        raise

    return impl_task, bundle_dir


def cmd_run(args: argparse.Namespace) -> int:
    """Run the next pending task(s) or specific tasks."""
    config = Config.load(args.project_dir)
    if args.no_docker:
        config.use_docker = False

    # Override max_turns if specified
    if hasattr(args, 'max_turns') and args.max_turns is not None:
        config.max_steps = args.max_turns
        config.max_turns = args.max_turns
    try:
        selected_tags, any_tag = _selected_tag_filters(args)
    except ValueError as exc:
        return phase1_error(args, str(exc))

    # Handle background mode
    if args.background:
        return _spawn_background_workers(args, config)

    # Handle worker mode (internal)
    if args.worker_mode:
        return _run_as_worker(args, config)

    # Get task info for registration
    store = get_store(config)
    registry = WorkerRegistry(config.workers_path)

    # Warn about orphaned tasks before starting new work (skip in resume mode)
    is_resume = getattr(args, 'resume', False)
    if not is_resume:
        orphaned = _get_orphaned_tasks(registry, store)
        if orphaned:
            _print_orphaned_warning(orphaned)
            print()
    # Check if specific task IDs were provided
    if hasattr(args, 'task_ids') and args.task_ids:
        # Resolve and validate all task IDs first
        args.task_ids = [resolve_id(config, tid) for tid in args.task_ids]
        for task_id in args.task_ids:
            task = store.get(task_id)
            if not task:
                return phase1_error(args, f"Task {task_id} not found")

            allow_pr_retry = _allow_pr_required_retry(args, task)
            if task.status != "pending" and not allow_pr_retry:
                return phase1_error(args, f"Task {task_id} is not pending (status: {task.status})")

            # Check if task is blocked by a dependency
            is_blocked, blocking_id, blocking_status = store.is_task_blocked(task)
            if is_blocked:
                del blocking_id, blocking_status
                return phase1_error(args, blocked_dependency_error_message(store, task))
    else:
        git = Git(config.project_dir)
        target_branch = git.default_branch()
        recovery_entries = collect_recovery_lane_entries(
            store,
            tags=selected_tags,
            any_tag=any_tag,
            max_recovery_attempts=config.max_resume_attempts,
            git=git,
            target_branch=target_branch,
        )
        if recovery_entries:
            plural = "candidate is" if len(recovery_entries) == 1 else "candidates are"
            print(
                f"Note: {len(recovery_entries)} recovery {plural} waiting on `gza advance` / `gza watch`; "
                "`gza work` only starts pending tasks."
            )
            print()
    # Track elapsed time for the work session
    start_time = time.time()

    worker_id = WorkerRegistry(config.workers_path).generate_worker_id()

    def _run_session() -> int:
        create_pr = bool(getattr(args, "create_pr", False))
        session_store = store

        # Run the task(s)
        if hasattr(args, 'task_ids') and args.task_ids:
            # Run the specific tasks
            tasks_completed = 0
            task_separator = "\n" + "-" * 32 + "\n"
            for task_id in args.task_ids:
                if tasks_completed > 0:
                    print(task_separator)
                result = _run_foreground(
                    config,
                    task_id=task_id,
                    force=getattr(args, "force", False),
                    create_pr=create_pr,
                    phase1_args=args,
                )
                if result != 0:
                    if tasks_completed == 0:
                        return result
                    print(f"\nCompleted {tasks_completed} task(s) before task {task_id} failed")
                    return result
                tasks_completed += 1

            if tasks_completed > 1:
                elapsed = format_duration(time.time() - start_time)
                print(f"\n=== Completed {tasks_completed} tasks in {elapsed} ===")
            return 0

        count = args.count if args.count is not None else config.work_count

        tasks_completed = 0
        task_separator = "\n" + "-" * 32 + "\n"
        for i in range(count):
            if tasks_completed > 0:
                print(task_separator)
            if selected_tags:
                next_task = session_store.get_next_pending(tags=selected_tags, any_tag=any_tag)
                if not next_task:
                    if tasks_completed == 0:
                        print(
                            format_no_runnable_message_for_tags(
                                session_store,
                                selected_tags,
                                any_tag=any_tag,
                            )
                        )
                    else:
                        elapsed = format_duration(time.time() - start_time)
                        print(
                            f"\nCompleted {tasks_completed} task(s) in {elapsed}. "
                            + format_no_runnable_message_for_tags(
                                session_store,
                                selected_tags,
                                any_tag=any_tag,
                                exhausted=True,
                            )
                        )
                    break
                result = _run_foreground(
                    config,
                    task_id=next_task.id,
                    force=getattr(args, "force", False),
                    create_pr=create_pr,
                    phase1_args=args,
                )
            else:
                result = _run_foreground(
                    config,
                    task_id=None,
                    force=getattr(args, "force", False),
                    create_pr=create_pr,
                    phase1_args=args,
                )

            if result != 0:
                if tasks_completed == 0:
                    return result
                print(f"\nCompleted {tasks_completed} task(s) before a task failed")
                return result

            tasks_completed += 1

            if i < count - 1:  # Not the last iteration
                from ..db import SqliteTaskStore
                session_store = SqliteTaskStore.from_config(config)
                next_task = session_store.get_next_pending(tags=selected_tags, any_tag=any_tag)
                if not next_task:
                    elapsed = format_duration(time.time() - start_time)
                    if selected_tags:
                        print(
                            f"\nCompleted {tasks_completed} task(s) in {elapsed}. "
                            + format_no_runnable_message_for_tags(
                                session_store,
                                selected_tags,
                                any_tag=any_tag,
                                exhausted=True,
                            )
                        )
                    else:
                        print(f"\nCompleted {tasks_completed} task(s) in {elapsed}. No more pending tasks.")
                    break

        if tasks_completed > 1:
            elapsed = format_duration(time.time() - start_time)
            print(f"\n=== Completed {tasks_completed} tasks in {elapsed} ===")
        return 0

    return _run_with_registered_worker(
        config=config,
        worker_id=worker_id,
        run_command=_run_session,
        allow_same_pid_reentry=True,
    )


def cmd_run_inline(args: argparse.Namespace) -> int:
    """Run a specific task inline via the real runner orchestration path."""
    config = Config.load(args.project_dir)
    if args.no_docker:
        config.use_docker = False

    if getattr(args, "max_turns", None) is not None:
        config.max_steps = args.max_turns
        config.max_turns = args.max_turns

    task_id = resolve_id(config, args.task_id)
    invocation = RunInvocationContext(
        command="run-inline",
        execution_mode="foreground_inline",
        interaction_mode="auto",
    )
    return _run_foreground(
        config,
        task_id=task_id,
        resume=bool(getattr(args, "resume", False)),
        force=getattr(args, "force", False),
        invocation=invocation,
    )


def cmd_plan_review(args: argparse.Namespace) -> int:
    """Create or reuse a plan-review task for a completed plan source and optionally run it."""
    config = Config.load(args.project_dir)
    if hasattr(args, "no_docker") and args.no_docker:
        config.use_docker = False

    if hasattr(args, "max_turns") and args.max_turns is not None:
        config.max_steps = args.max_turns
        config.max_turns = args.max_turns
    execution_mode = _execution_mode(args)

    store = get_store(config)
    plan_source_id = resolve_id(config, args.task_id)
    plan_source_task = store.get(plan_source_id)
    if plan_source_task is None:
        return phase1_error(args, f"Task {plan_source_id} not found")
    if getattr(args, "edit_slices", False) or getattr(args, "materialize", False):
        return _cmd_plan_review_manifest_action(args, config=config, store=store, review_task=plan_source_task)
    if plan_source_task.task_type not in {"plan", "plan_improve"}:
        return phase1_error(
            args,
            f"Task {plan_source_task.id} is a {plan_source_task.task_type} task. "
            "Expected a completed plan or plan_improve task.",
        )
    if plan_source_task.status != "completed":
        return phase1_error(
            args,
            f"Task {plan_source_task.id} is {plan_source_task.status}. "
            "Plan review requires a completed plan source.",
        )

    reserved_launch = _reserve_immediate_execution_permit(args=args, config=config, store=store)
    if reserved_launch is False:
        return 1

    with _release_reserved_launch_unless_transferred(reserved_launch) as mark_launch_transferred:
        existing_review = None if getattr(args, "rerun", False) else _latest_non_dropped_plan_review_for_source(
            store,
            plan_source_task,
        )
        if existing_review is not None and existing_review.status == "completed":
            print_phase1_message(
                args,
                f"Plan review {existing_review.id} already completed for {plan_source_task.id}. "
                "Use --rerun to create a new review attempt.",
            )
            return 0

        if existing_review is not None:
            plan_review_task = existing_review
            created_new_review = False
            action_message = f"Reusing {existing_review.status} plan review task {existing_review.id}"
        else:
            model = args.model if hasattr(args, "model") and args.model else None
            provider = args.provider if hasattr(args, "provider") and args.provider else None
            plan_review_task = _create_plan_review_task(
                store,
                plan_source_task,
                trigger_source="manual",
                model=model,
                provider=provider,
            )
            created_new_review = True
            action_message = None
        assert plan_review_task.id is not None

        def _emit_plan_review_created() -> None:
            if action_message is not None:
                print(f"✓ {action_message}")
            else:
                print(f"✓ Created plan review task {plan_review_task.id}")
            print(f"  Plan source: {plan_source_task.id}")

        prepared_plan_review_task, _launch_permit = _finalize_immediate_execution_task(
            args=args,
            config=config,
            rollback_on_failure=created_new_review,
            task=plan_review_task,
            emit_created=_emit_plan_review_created,
            reserved_permit=reserved_launch if isinstance(reserved_launch, LaunchPermit) else None,
        )
        if prepared_plan_review_task is None:
            return 1
        plan_review_task = prepared_plan_review_task
        mark_launch_transferred()

    if execution_mode == "queue":
        return 0

    if execution_mode == "background":
        worker_args = argparse.Namespace(**vars(args))
        worker_args.task_ids = [plan_review_task.id]
        rc = _spawn_background_worker(
            worker_args,
            config,
            task_id=plan_review_task.id,
            prepared_task=plan_review_task,
        )
        release_task_launch_permit(str(plan_review_task.id))
        return rc

    print(f"\nRunning plan review task {plan_review_task.id}...")
    rc = _run_foreground(
        config,
        task_id=plan_review_task.id,
        force=getattr(args, "force", False),
        invocation=_foreground_command_invocation("plan-review"),
    )
    release_task_launch_permit(str(plan_review_task.id))
    return rc


def _drop_new_tasks(store: SqliteTaskStore, tasks: list[DbTask]) -> None:
    for task in tasks:
        if task.id is None:
            continue
        fresh = store.get(task.id)
        if fresh is None or fresh.status != "pending":
            continue
        fresh.status = "dropped"
        store.update(fresh)


def _emit_plan_slice_materialization(
    materialization: PlanReviewMaterializationResult,
    *,
    plan_source_id: str,
    plan_review_id: str,
) -> None:
    verb = "Created" if materialization.created else "Reused"
    for task in materialization.tasks:
        print(f"✓ {verb} implement task {task.id}")
        print(f"  Plan source: {plan_source_id}")
        print(f"  Plan review: {plan_review_id}")


def _edit_plan_review_manifest_file(
    *,
    review_task: DbTask,
    manifest: dict[str, Any],
) -> dict[str, Any] | None:
    editor = os.environ.get("VISUAL") or os.environ.get("EDITOR") or "vi"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temp_path = handle.name

    try:
        result = _launch_editor([editor, temp_path])
        if result.returncode != 0:
            print(f"Error: editor exited with status {result.returncode} for review {review_task.id}")
            return None
        with open(temp_path, encoding="utf-8") as handle:
            edited = json.load(handle)
    except json.JSONDecodeError as exc:
        print(f"Error: edited manifest is not valid JSON: {exc}")
        return None
    finally:
        os.unlink(temp_path)

    if not isinstance(edited, dict):
        print("Error: edited manifest must be a JSON object")
        return None
    return edited


def _cmd_plan_review_manifest_action(
    args: argparse.Namespace,
    *,
    config: Config,
    store: SqliteTaskStore,
    review_task: DbTask,
) -> int:
    if getattr(args, "rerun", False):
        return phase1_error(args, "--rerun cannot be combined with --edit-slices or --materialize")
    if _execution_mode(args) != "run":
        return phase1_error(args, "--queue/--background are not supported with --edit-slices or --materialize")
    if review_task.task_type != "plan_review":
        return phase1_error(
            args,
            f"Task {review_task.id} is a {review_task.task_type} task. "
            "Expected a completed plan_review task for manifest override/materialization.",
        )
    if review_task.status != "completed":
        return phase1_error(args, f"Task {review_task.id} is {review_task.status}. Plan review must be completed.")
    if review_task.depends_on is None:
        return phase1_error(args, f"Plan review {review_task.id} is missing its plan-source dependency.")

    plan_source_task = store.get(review_task.depends_on)
    if plan_source_task is None:
        return phase1_error(args, f"Plan source {review_task.depends_on} for review {review_task.id} was not found.")
    if plan_source_task.task_type not in {"plan", "plan_improve"}:
        return phase1_error(
            args,
            f"Plan review {review_task.id} depends on {plan_source_task.id} "
            f"({plan_source_task.task_type}), not a plan source.",
        )

    manifest_state = resolve_effective_plan_review_manifest_state(
        store,
        config,
        review_task=review_task,
        plan_source_task=plan_source_task,
    )
    manifest = manifest_state.manifest
    manifest_source = manifest_state.source
    if manifest is None:
        if manifest_state.source == "override":
            detail = manifest_state.validation_error or "missing approved override manifest"
            return phase1_error(
                args,
                f"Plan review {review_task.id} has an invalid override manifest: {detail}",
            )
        if manifest_state.verdict == "APPROVED":
            detail = manifest_state.validation_error or "missing approved slice manifest"
            return phase1_error(
                args,
                f"Plan review {review_task.id} is APPROVED but its slice manifest is invalid: {detail}",
            )
        return phase1_error(
            args,
            f"Plan review {review_task.id} does not have a valid approved slice manifest to override or materialize.",
        )

    if getattr(args, "edit_slices", False):
        edited_raw = _edit_plan_review_manifest_file(review_task=review_task, manifest=asdict(manifest))
        if edited_raw is None:
            return 1
        try:
            validated = validate_plan_review_manifest(
                edited_raw,
                markdown_verdict="APPROVED",
                source_task_id=plan_source_task.id or "",
                source_task_type=plan_source_task.task_type,
                max_slice_timeout_minutes=_plan_review_timeout_budget_minutes(config),
                max_plan_slices=getattr(config, "max_plan_slices", None),
            )
        except ValueError as exc:
            print(f"Error: edited manifest is invalid: {exc}")
            return 1
        persist_plan_review_override_manifest(
            store,
            config,
            review_task=review_task,
            plan_source_task=plan_source_task,
            manifest=validated,
        )
        print(f"✓ Saved validated slice override for plan review {review_task.id}")
        print(f"  Plan source: {plan_source_task.id}")
        return 0

    materialization = _materialize_plan_review_slices(
        config,
        store,
        plan_source_task,
        review_task,
        manifest,
        trigger_source="manual",
        require_review_before_merge=getattr(config, "require_review_before_merge", True),
    )
    created_ids = ", ".join(task.id or "unknown" for task in materialization.tasks)
    print(f"✓ Materialized implementation slices for plan review {review_task.id}")
    print(f"  Manifest source: {manifest_source}")
    print(f"  Tasks: {created_ids}")
    return 0


def cmd_plan_improve(args: argparse.Namespace) -> int:
    """Create or reuse a revised-plan task for a plan review and optionally run it."""
    config = Config.load(args.project_dir)
    if hasattr(args, "no_docker") and args.no_docker:
        config.use_docker = False

    if hasattr(args, "max_turns") and args.max_turns is not None:
        config.max_steps = args.max_turns
        config.max_turns = args.max_turns
    execution_mode = _execution_mode(args)

    store = get_store(config)
    review_task_id = resolve_id(config, args.task_id)
    review_task = store.get(review_task_id)
    if review_task is None:
        return phase1_error(args, f"Task {review_task_id} not found")
    if review_task.task_type != "plan_review":
        return phase1_error(
            args,
            f"Task {review_task.id} is a {review_task.task_type} task. Expected a plan_review task.",
        )
    if review_task.depends_on is None:
        return phase1_error(args, f"Plan review {review_task.id} is missing its plan-source dependency.")

    plan_source_task = store.get(review_task.depends_on)
    if plan_source_task is None:
        return phase1_error(args, f"Plan source {review_task.depends_on} for review {review_task.id} was not found.")
    if plan_source_task.task_type not in {"plan", "plan_improve"}:
        return phase1_error(
            args,
            f"Plan review {review_task.id} depends on {plan_source_task.id} "
            f"({plan_source_task.task_type}), not a plan source.",
        )
    if review_task.status != "completed":
        return phase1_error(
            args,
            f"Task {review_task.id} is {review_task.status}. plan-improve requires a completed CHANGES_REQUESTED plan_review.",
        )

    review_outcome = get_plan_review_outcome(
        Path(config.project_dir),
        review_task,
        source_task_id=plan_source_task.id or "",
        source_task_type=plan_source_task.task_type,
        max_slice_timeout_minutes=_plan_review_timeout_budget_minutes(config),
        max_plan_slices=getattr(config, "max_plan_slices", None),
    )
    if review_outcome.verdict != "CHANGES_REQUESTED":
        verdict_label = review_outcome.verdict or "unavailable"
        return phase1_error(
            args,
            f"Plan review {review_task.id} has verdict {verdict_label}. "
            "plan-improve requires a completed CHANGES_REQUESTED plan_review.",
        )

    reserved_launch = _reserve_immediate_execution_permit(args=args, config=config, store=store)
    if reserved_launch is False:
        return 1

    with _release_reserved_launch_unless_transferred(reserved_launch) as mark_launch_transferred:
        existing_improve = _matching_plan_improve_for_review(store, plan_source_task, review_task)
        if existing_improve is not None and existing_improve.status == "completed":
            print_phase1_message(
                args,
                f"Plan improve {existing_improve.id} already completed for review {review_task.id}.",
            )
            return 0

        if existing_improve is not None:
            plan_improve_task = existing_improve
            created_new_improve = False
            action_message = f"Reusing {existing_improve.status} plan improve task {existing_improve.id}"
        else:
            model = args.model if hasattr(args, "model") and args.model else None
            provider = args.provider if hasattr(args, "provider") and args.provider else None
            plan_improve_task = _create_plan_improve_task(
                store,
                plan_source_task,
                review_task,
                trigger_source="manual",
                model=model,
                provider=provider,
            )
            created_new_improve = True
            action_message = None
        assert plan_improve_task.id is not None

        def _emit_plan_improve_created() -> None:
            if action_message is not None:
                print(f"✓ {action_message}")
            else:
                print(f"✓ Created plan improve task {plan_improve_task.id}")
            print(f"  Plan source: {plan_source_task.id}")
            print(f"  Review: {review_task.id}")

        prepared_plan_improve_task, _launch_permit = _finalize_immediate_execution_task(
            args=args,
            config=config,
            rollback_on_failure=created_new_improve,
            task=plan_improve_task,
            emit_created=_emit_plan_improve_created,
            reserved_permit=reserved_launch if isinstance(reserved_launch, LaunchPermit) else None,
        )
        if prepared_plan_improve_task is None:
            return 1
        plan_improve_task = prepared_plan_improve_task
        mark_launch_transferred()

    if execution_mode == "queue":
        return 0

    if execution_mode == "background":
        worker_args = argparse.Namespace(**vars(args))
        worker_args.task_ids = [plan_improve_task.id]
        rc = _spawn_background_worker(
            worker_args,
            config,
            task_id=plan_improve_task.id,
            prepared_task=plan_improve_task,
        )
        release_task_launch_permit(str(plan_improve_task.id))
        return rc

    print(f"\nRunning plan improve task {plan_improve_task.id}...")
    rc = _run_foreground(
        config,
        task_id=plan_improve_task.id,
        force=getattr(args, "force", False),
        invocation=_foreground_command_invocation("plan-improve"),
    )
    release_task_launch_permit(str(plan_improve_task.id))
    return rc


def cmd_implement(args: argparse.Namespace) -> int:
    """Create an implementation task from a completed plan task and run it."""
    config = Config.load(args.project_dir)
    if hasattr(args, 'no_docker') and args.no_docker:
        config.use_docker = False

    # Override max_turns if specified
    if hasattr(args, 'max_turns') and args.max_turns is not None:
        config.max_steps = args.max_turns
        config.max_turns = args.max_turns
    execution_mode = _execution_mode(args)

    store = get_store(config)

    plan_task_id = resolve_id(config, args.plan_task_id)
    plan_task = store.get(plan_task_id)
    if not plan_task:
        return phase1_error(args, f"Task {plan_task_id} not found")
    if plan_task.task_type != "plan":
        return phase1_error(args, f"Task {plan_task.id} is a {plan_task.task_type} task. Expected a completed plan task.")
    if plan_task.status != "completed":
        return phase1_error(args, f"Task {plan_task.id} is {plan_task.status}. Plan task must be completed.")

    try:
        tags = _selected_tag_override_for_derived_task(args)
    except ValueError as exc:
        return phase1_error(args, str(exc))
    create_review = args.review if hasattr(args, 'review') and args.review else False
    create_pr = bool(getattr(args, "create_pr", False))
    same_branch = args.same_branch if hasattr(args, 'same_branch') and args.same_branch else False
    branch_type = args.branch_type if hasattr(args, 'branch_type') and args.branch_type else None
    model = args.model if hasattr(args, 'model') and args.model else None
    provider = args.provider if hasattr(args, 'provider') and args.provider else None
    skip_learnings = args.skip_learnings if hasattr(args, 'skip_learnings') and args.skip_learnings else False
    review_scope = args.review_scope if hasattr(args, "review_scope") and args.review_scope else None
    reserved_launch = _reserve_immediate_execution_permit(args=args, config=config, store=store)
    if reserved_launch is False:
        return 1

    latest_plan_source = _resolve_latest_plan_source(store, plan_task)
    latest_plan_review = _latest_non_dropped_plan_review_for_source(store, latest_plan_source)
    latest_completed_plan_review = _latest_completed_non_dropped_plan_review_for_source(
        store,
        latest_plan_source,
    )
    approved_review = latest_completed_plan_review
    approved_manifest = None
    fallback_warning: str | None = None
    if approved_review is not None and latest_plan_source.id is not None:
        manifest_state = resolve_effective_plan_review_manifest_state(
            store,
            config,
            review_task=approved_review,
            plan_source_task=latest_plan_source,
        )
        approved_manifest = manifest_state.manifest if manifest_state.verdict == "APPROVED" else None
        if manifest_state.verdict == "APPROVED" and approved_manifest is None:
            manifest_source_label = (
                "override manifest" if manifest_state.source == "override" else "slice manifest"
            )
            detail = manifest_state.validation_error or "missing approved slice manifest"
            return phase1_error(
                args,
                f"Plan review {approved_review.id} is APPROVED but its {manifest_source_label} is invalid: {detail}",
            )
        if approved_manifest is None and manifest_state.verdict not in {None, "APPROVED"}:
            fallback_warning = (
                f"Warning: latest completed plan review {approved_review.id} has verdict "
                f"{manifest_state.verdict}; falling back to legacy single implement task."
            )
        elif approved_manifest is None and manifest_state.verdict is None:
            detail = manifest_state.validation_error or "missing verdict"
            fallback_warning = (
                f"Warning: latest completed plan review {approved_review.id} is not usable "
                f"({detail}); falling back to legacy single implement task."
            )
    elif latest_plan_review is not None:
        fallback_warning = (
            f"Warning: latest plan review {latest_plan_review.id} is {latest_plan_review.status}; "
            "falling back to legacy single implement task."
        )

    with _release_reserved_launch_unless_transferred(reserved_launch) as mark_launch_transferred:
        if approved_manifest is not None and approved_review is not None:
            assert latest_plan_source.id is not None
            assert approved_review.id is not None
            plan_source_id = latest_plan_source.id
            plan_review_id = approved_review.id
            materialization = _materialize_plan_review_slices(
                config,
                store,
                latest_plan_source,
                approved_review,
                approved_manifest,
                trigger_source="manual",
                require_review_before_merge=create_review or getattr(config, "require_review_before_merge", True),
            )
            if not materialization.tasks:
                return phase1_error(args, f"Plan review {approved_review.id} did not materialize any implementation slices.")
            plan_task.auto_implement = True
            store.update(plan_task)

            if execution_mode == "queue":
                _emit_plan_slice_materialization(
                    materialization,
                    plan_source_id=plan_source_id,
                    plan_review_id=plan_review_id,
                )
                return 0

            first_task = materialization.tasks[0]
            assert first_task.id is not None

            def _emit_created() -> None:
                _emit_plan_slice_materialization(
                    materialization,
                    plan_source_id=plan_source_id,
                    plan_review_id=plan_review_id,
                )

            prepared_first_task, _launch_permit = _finalize_immediate_execution_task(
                args=args,
                config=config,
                rollback_on_failure=materialization.created,
                rollback_cleanup=(
                    (lambda: _drop_new_tasks(store, materialization.tasks))
                    if materialization.created
                    else None
                ),
                task=first_task,
                emit_created=_emit_created,
                reserved_permit=reserved_launch if isinstance(reserved_launch, LaunchPermit) else None,
            )
            if prepared_first_task is None:
                return 1
            mark_launch_transferred()

            if execution_mode == "background":
                worker_args = argparse.Namespace(**vars(args))
                prepared_first_task_id = prepared_first_task.id
                assert prepared_first_task_id is not None
                task_ids = [prepared_first_task_id]
                worker_args.task_ids = task_ids
                rc = _spawn_background_workers(
                    worker_args,
                    config,
                    prepared_tasks={prepared_first_task_id: prepared_first_task},
                )
                for task_id in task_ids:
                    release_task_launch_permit(str(task_id))
                return rc

            prepared_first_task_id = prepared_first_task.id
            assert prepared_first_task_id is not None
            print(f"\nRunning implement task {prepared_first_task_id}...")
            rc = _run_foreground(
                config,
                task_id=prepared_first_task_id,
                force=getattr(args, "force", False),
                invocation=_foreground_command_invocation("implement"),
            )
            release_task_launch_permit(prepared_first_task_id)
            return rc

        if fallback_warning is not None:
            print(fallback_warning)

        prompt = args.prompt
        if not prompt:
            slug = _get_base_task_slug(plan_task)
            if slug:
                prompt = f"Implement plan from task {plan_task.id}: {slug}"
            else:
                prompt = f"Implement plan from task {plan_task.id}"

        impl_task = _create_implementation_task_from_source(
            store,
            plan_task,
            prompt=prompt,
            trigger_source="manual",
            tags=tags,
            review_scope=review_scope,
            create_review=create_review,
            create_pr=create_pr,
            same_branch=same_branch,
            task_type_hint=branch_type,
            model=model,
            provider=provider,
            skip_learnings=skip_learnings,
        )
        assert impl_task.id is not None

        def _emit_impl_created() -> None:
            print(f"✓ Created implement task {impl_task.id}")
            print(f"  Depends on: plan {plan_task.id}")

        prepared_impl_task, _impl_launch_permit = _finalize_immediate_execution_task(
            args=args,
            config=config,
            rollback_on_failure=True,
            task=impl_task,
            emit_created=_emit_impl_created,
            reserved_permit=reserved_launch if isinstance(reserved_launch, LaunchPermit) else None,
        )
        if prepared_impl_task is None:
            return 1
        impl_task = prepared_impl_task
        mark_launch_transferred()

        plan_task.auto_implement = True
        store.update(plan_task)

        # Handle queue mode - add to queue without executing
        if execution_mode == "queue":
            return 0

        # Handle background mode - spawn worker to run the implement task
        if execution_mode == "background":
            assert impl_task.id is not None
            worker_args = argparse.Namespace(**vars(args))
            worker_args.task_ids = [impl_task.id]
            rc = _spawn_background_worker(
                worker_args,
                config,
                task_id=impl_task.id,
                prepared_task=impl_task,
            )
            release_task_launch_permit(str(impl_task.id))
            return rc

        # Default: run the implement task immediately
        print(f"\nRunning implement task {impl_task.id}...")
        rc = _run_foreground(
            config,
            task_id=impl_task.id,
            force=getattr(args, "force", False),
            invocation=_foreground_command_invocation("implement"),
        )
        release_task_launch_permit(str(impl_task.id))
        return rc


def cmd_extract(args: argparse.Namespace) -> int:
    """Create an implementation task from selected source-branch file changes."""
    config = Config.load(args.project_dir)
    if hasattr(args, "no_docker") and args.no_docker:
        config.use_docker = False

    if hasattr(args, "max_turns") and args.max_turns is not None:
        config.max_steps = args.max_turns
        config.max_turns = args.max_turns
    execution_mode = _execution_mode(args)

    source_task_id_raw = args.source if hasattr(args, "source") else None
    source_branch = args.branch if hasattr(args, "branch") else None
    source_commits = tuple(getattr(args, "commits", ()) or ())
    selected_raw: list[str] = list(getattr(args, "paths", ()) or ())
    store: SqliteTaskStore | None = None

    source_task_id_raw, selected_raw, store = _maybe_reinterpret_extract_source_as_path(
        config,
        source_task_id_raw,
        source_branch,
        source_commits,
        selected_raw,
        store,
    )

    files_from = getattr(args, "files_from", None)
    if files_from:
        try:
            files_from_path = Path(files_from)
            if not files_from_path.is_absolute():
                files_from_path = config.project_dir / files_from_path
            selected_raw.extend(read_paths_file(files_from_path))
        except ExtractionError as exc:
            return phase1_error(args, str(exc))

    try:
        tags = _selected_tags_for_new_task(args)
    except ValueError as exc:
        return phase1_error(args, str(exc))

    git = Git(config.project_dir)
    if store is None:
        store = get_store(config)

    source_task_id: str | None = None
    if source_task_id_raw:
        try:
            source_task_id = resolve_id(config, source_task_id_raw)
        except InvalidTaskIdError as exc:
            if source_branch or source_commits:
                return phase1_error(args, str(exc))
            selected_raw.insert(0, source_task_id_raw)
            source_task_id_raw = None
        else:
            if store.get(source_task_id) is None:
                if source_branch or source_commits:
                    return phase1_error(args, f"Task {source_task_id} not found")
                selected_raw.insert(0, source_task_id_raw)
                source_task_id = None
                source_task_id_raw = None

    if getattr(args, "per_commit", False) and not source_commits:
        return phase1_error(args, "--per-commit requires one or more --commit values")

    if not source_task_id and not source_branch and not source_commits:
        try:
            source_branch = git.current_branch()
        except Exception as exc:
            return phase1_error(args, f"failed to determine current branch for extract: {exc}")

    selector_count = int(bool(source_task_id)) + int(bool(source_branch)) + int(bool(source_commits))
    if selector_count != 1:
        return phase1_error(args, "Specify exactly one source selector: SOURCE task ID, --branch, or --commit")

    try:
        resolved_source = resolve_source_selection(
            store,
            git,
            source_task_id=source_task_id,
            source_branch=source_branch,
            source_commits=source_commits,
            base_branch_override=getattr(args, "base_branch", None),
        )
        normalized_selected_paths = normalize_selected_paths(selected_raw) if selected_raw else None
    except ExtractionError as exc:
        return phase1_error(args, str(exc))

    per_commit = bool(getattr(args, "per_commit", False))
    if per_commit:
        commit_subjects = resolved_source.source_commit_subjects
        sources = [
            SourceSelection(
                source_task_id=None,
                source_commits=(commit,),
                source_commit_subjects=((commit_subjects[index],) if index < len(commit_subjects) else ()),
            )
            for index, commit in enumerate(resolved_source.source_commits)
        ]
    else:
        sources = [resolved_source]

    drafts: list[tuple[SourceSelection, ExtractionDraft]] = []
    try:
        for source in sources:
            if normalized_selected_paths is not None:
                selected_paths = normalized_selected_paths
            else:
                selected_paths = infer_selected_paths(git, source)
            draft = plan_extraction(
                git,
                source,
                selected_paths,
                operator_prompt=getattr(args, "prompt", None),
            )
            drafts.append((source, draft))
    except ExtractionError as exc:
        return phase1_error(args, str(exc))

    if getattr(args, "dry_run", False):
        heading = "✓ Dry run: extraction plan preview"
        if per_commit:
            heading = f"✓ Dry run: {len(drafts)} per-commit extraction plans"
        for index, (source, draft) in enumerate(drafts, start=1):
            _print_extraction_plan_summary(
                draft=draft,
                source_label=_describe_extract_source_label(source),
                heading=heading if len(drafts) == 1 else f"{heading} [{index}/{len(drafts)}]",
                dry_run=True,
            )
        return 0

    create_review = bool(getattr(args, "review", False))
    create_pr = bool(getattr(args, "create_pr", False))
    branch_type = args.branch_type if hasattr(args, "branch_type") and args.branch_type else None
    model = args.model if hasattr(args, "model") and args.model else None
    provider = args.provider if hasattr(args, "provider") and args.provider else None
    skip_learnings = bool(getattr(args, "skip_learnings", False))
    base_branch = args.base_branch if hasattr(args, "base_branch") and args.base_branch else None
    created_task_summaries: list[_CreatedImmediateExecutionTask] = []

    if execution_mode == "queue":
        for source, draft in drafts:
            try:
                impl_task, bundle_dir = _create_extract_task(
                    config=config,
                    store=store,
                    git=git,
                    draft=draft,
                    tags=tags,
                    create_review=create_review,
                    create_pr=create_pr,
                    branch_type=branch_type,
                    model=model,
                    provider=provider,
                    skip_learnings=skip_learnings,
                    base_branch=base_branch,
                )
            except Exception as exc:
                _rollback_created_immediate_execution_tasks(
                    config=config,
                    created_tasks=created_task_summaries,
                )
                return phase1_error(args, str(exc))
            created_task_summaries.append(
                _CreatedImmediateExecutionTask(
                    task=impl_task,
                    emit_created=_make_extract_created_emitter(
                        config=config,
                        draft=draft,
                        source=source,
                        bundle_dir=bundle_dir,
                        impl_task=impl_task,
                    ),
                    rollback_cleanup=_make_extract_rollback_cleanup(bundle_dir),
                )
            )
        for created_task in created_task_summaries:
            created_task.emit_created()
        return 0

    if execution_mode == "background":
        try:
            reserved_permits = launch_permits(config, store, count=len(drafts))
        except MaxConcurrentTasksError as exc:
            return phase1_error(args, str(exc))

        prepared_tasks: list[DbTask] = []
        unused_permits = list(reserved_permits)
        for source, draft in drafts:
            permit = unused_permits.pop(0)
            try:
                created = _create_reserved_extract_task(
                    config=config,
                    store=store,
                    git=git,
                    draft=draft,
                    source=source,
                    tags=tags,
                    create_review=create_review,
                    create_pr=create_pr,
                    branch_type=branch_type,
                    model=model,
                    provider=provider,
                    skip_learnings=skip_learnings,
                    base_branch=base_branch,
                    permit=permit,
                    prepare_for_launch=True,
                )
            except Exception as exc:
                permit.release()
                for reserved_task in prepared_tasks:
                    release_task_launch_permit(str(reserved_task.id) if reserved_task.id is not None else None)
                _release_launch_permits(unused_permits)
                _rollback_created_immediate_execution_tasks(
                    config=config,
                    created_tasks=created_task_summaries,
                )
                return phase1_error(args, str(exc))
            if created is None:
                for reserved_task in prepared_tasks:
                    release_task_launch_permit(str(reserved_task.id) if reserved_task.id is not None else None)
                _release_launch_permits(unused_permits)
                _rollback_created_immediate_execution_tasks(
                    config=config,
                    created_tasks=created_task_summaries,
                )
                return 1
            created_task, prepared_task = created
            created_task_summaries.append(created_task)
            prepared_tasks.append(prepared_task)
        worker_args = _extract_run_args(args, [task.id for task in prepared_tasks if task.id is not None])
        for created_task in created_task_summaries:
            created_task.emit_created()
        if len(worker_args.task_ids) == 1:
            prepared_task = next(task for task in prepared_tasks if task.id == worker_args.task_ids[0])
            return _spawn_background_worker(
                worker_args,
                config,
                task_id=worker_args.task_ids[0],
                prepared_task=prepared_task,
            )
        prepared_task_map = {
            str(task.id): task
            for task in prepared_tasks
            if task.id is not None
        }
        return _spawn_background_workers(worker_args, config, prepared_tasks=prepared_task_map)

    tasks_completed = 0
    start_time = time.time()
    task_separator = "\n" + "-" * 32 + "\n"
    for source, draft in drafts:
        if tasks_completed > 0:
            print(task_separator)
        try:
            permit = launch_permit(config, store)
        except MaxConcurrentTasksError as exc:
            return phase1_error(args, str(exc))
        try:
            created = _create_reserved_extract_task(
                config=config,
                store=store,
                git=git,
                draft=draft,
                source=source,
                tags=tags,
                create_review=create_review,
                create_pr=create_pr,
                branch_type=branch_type,
                model=model,
                provider=provider,
                skip_learnings=skip_learnings,
                base_branch=base_branch,
                permit=permit,
                prepare_for_launch=True,
            )
        except Exception as exc:
            permit.release()
            return phase1_error(args, str(exc))
        if created is None:
            return 1
        created_task, prepared_task = created
        created_task.emit_created()
        if len(drafts) == 1:
            print(f"\nRunning implement task {prepared_task.id}...")
        result = _run_foreground(
            config,
            task_id=str(prepared_task.id) if prepared_task.id is not None else None,
            force=getattr(args, "force", False),
            invocation=_foreground_command_invocation("extract"),
            prepared_task=prepared_task,
        )
        if result != 0:
            if tasks_completed == 0:
                return result
            print(f"\nCompleted {tasks_completed} task(s) before a task failed")
            return result
        tasks_completed += 1

    if tasks_completed > 1:
        elapsed = format_duration(time.time() - start_time)
        print(f"\n=== Completed {tasks_completed} tasks in {elapsed} ===")
    return 0


def cmd_add(args: argparse.Namespace) -> int:
    """Add a new task."""
    config = Config.load(args.project_dir)
    store = get_store(config)

    # Determine task type
    if args.type:
        task_type = args.type
    elif args.explore:
        task_type = "explore"
    else:
        task_type = "implement"

    # Validate task type
    valid_types = [task_type for task_type in CLI_ADD_TASK_TYPES if task_type != "improve"]
    if task_type == "improve":
        print("Error: Cannot create improve tasks directly. Use 'gza improve <task_id>' instead.")
        return 1
    if task_type not in valid_types:
        print(f"Error: Invalid task type '{task_type}'. Must be one of: {', '.join(valid_types)}")
        return 1

    # Get optional parameters
    try:
        tags = _selected_tags_for_new_task(args)
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1
    depends_on = resolve_id(config, args.depends_on) if hasattr(args, 'depends_on') and args.depends_on else None
    based_on = resolve_id(config, args.based_on) if hasattr(args, 'based_on') and args.based_on else None
    create_review = args.review if hasattr(args, 'review') and args.review else False
    hold_for_review = bool(getattr(args, "hold_for_review", False))
    create_pr = bool(getattr(args, "create_pr", False))
    same_branch = args.same_branch if hasattr(args, 'same_branch') and args.same_branch else False
    spec = args.spec if hasattr(args, 'spec') and args.spec else None
    review_scope = args.review_scope if hasattr(args, 'review_scope') and args.review_scope else None
    branch_type = args.branch_type if hasattr(args, 'branch_type') and args.branch_type else None
    model = args.model if hasattr(args, 'model') and args.model else None
    provider = args.provider if hasattr(args, 'provider') and args.provider else None
    skip_learnings = args.skip_learnings if hasattr(args, 'skip_learnings') and args.skip_learnings else False
    mark_next = bool(getattr(args, "next", False))
    recovery_origin = "manual" if based_on else None

    # Validation: reject incompatible provider/model pairs at creation time
    if provider and model and not is_model_compatible_with_provider(provider, model):
        print(f"Error: {provider_model_mismatch_error('--model', provider, model)}")
        return 1

    # Validation: --spec must reference an existing file
    if spec:
        spec_path = config.project_dir / spec
        if not spec_path.exists():
            print(f"Error: Spec file not found: {spec}")
            return 1

    # Validation: --same-branch requires --based-on or --depends-on
    if same_branch and not based_on and not depends_on:
        print("Error: --same-branch requires --based-on or --depends-on")
        return 1

    if hold_for_review and task_type != "plan":
        print("Error: --hold-for-review is only valid with --type plan")
        return 1
    if review_scope and task_type != "implement":
        print("Error: --review-scope is only valid for implement tasks")
        return 1

    # Validation: --based-on must reference an existing task
    if based_on:
        dep_task = store.get(based_on)
        if not dep_task:
            print(f"Error: Task {based_on} not found")
            return 1

    # Validation: --depends-on must reference an existing task
    if depends_on:
        dep_task = store.get(depends_on)
        if not dep_task:
            print(f"Error: Task {depends_on} not found")
            return 1
    else:
        dep_task = None

    if task_type == "plan_review":
        if depends_on is None:
            print("Error: plan_review tasks require --depends-on <plan-or-plan-improve-id>")
            return 1
        if based_on is not None:
            print("Error: plan_review tasks cannot set --based-on; use --depends-on for the reviewed plan source")
            return 1
        assert dep_task is not None
        if dep_task.task_type not in {"plan", "plan_improve"}:
            print("Error: plan_review --depends-on must reference a plan or plan_improve task")
            return 1

    if task_type == "plan_improve":
        if based_on is None:
            print("Error: plan_improve tasks require --based-on <plan-or-plan-improve-id>")
            return 1
        if depends_on is None:
            print("Error: plan_improve tasks require --depends-on <plan-review-id>")
            return 1
        assert dep_task is not None
        if dep_task.task_type != "plan_review":
            print("Error: plan_improve --depends-on must reference a plan_review task")
            return 1
        based_on_task = store.get(based_on)
        assert based_on_task is not None
        if based_on_task.task_type not in {"plan", "plan_improve"}:
            print("Error: plan_improve --based-on must reference a plan or plan_improve task")
            return 1
        if dep_task.depends_on != based_on_task.id:
            print("Error: plan_improve must be based on the same plan source reviewed by its plan_review dependency")
            return 1

    if task_type == "implement":
        held_plan_error = _validate_new_implement_source_not_held_for_review(
            store,
            based_on_id=based_on,
            depends_on_id=depends_on,
        )
        if held_plan_error is not None:
            print(held_plan_error)
            return 1

    # Handle --prompt-file argument
    if hasattr(args, 'prompt_file') and args.prompt_file is not None:
        if args.prompt:
            print("Error: Cannot use both --prompt-file and prompt argument")
            return 1
        if args.edit:
            print("Error: Cannot use both --prompt-file and --edit")
            return 1
        try:
            with open(args.prompt_file) as f:
                prompt_text = f.read().strip()
        except FileNotFoundError:
            print(f"Error: File not found: {args.prompt_file}")
            return 1
        except Exception as e:
            print(f"Error reading file: {e}")
            return 1

        # Create task with prompt from file
        task = store.add(
            prompt_text,
            task_type=task_type,
            based_on=based_on,
            tags=tags,
            depends_on=depends_on,
            create_review=create_review,
            review_scope=review_scope,
            auto_implement=not hold_for_review,
            create_pr=create_pr,
            same_branch=same_branch,
            spec=spec,
            task_type_hint=branch_type,
            model=model,
            provider=provider,
            recovery_origin=recovery_origin,
            skip_learnings=skip_learnings,
            trigger_source="manual",
        )
        if mark_next:
            assert task.id is not None
            set_task_urgency(store, task.id, urgent=True)
        print(f"✓ Added task {task.id}")
        return 0

    if args.edit or not args.prompt:
        # Interactive mode with $EDITOR
        new_task = add_task_interactive(
            store,
            task_type=task_type,
            based_on=based_on,
            spec=spec,
            tags=tags,
            depends_on=depends_on,
            create_review=create_review,
            review_scope=review_scope,
            auto_implement=not hold_for_review,
            create_pr=create_pr,
            same_branch=same_branch,
            task_type_hint=branch_type,
            model=model,
            provider=provider,
            recovery_origin=recovery_origin,
            skip_learnings=skip_learnings,
            trigger_source="manual",
        )
        if not new_task:
            return 1
        if mark_next:
            assert new_task.id is not None
            set_task_urgency(store, new_task.id, urgent=True)
        print(f"✓ Added task {new_task.id}")
        return 0
    else:
        # Inline prompt
        task = store.add(
            args.prompt,
            task_type=task_type,
            based_on=based_on,
            tags=tags,
            depends_on=depends_on,
            create_review=create_review,
            review_scope=review_scope,
            auto_implement=not hold_for_review,
            create_pr=create_pr,
            same_branch=same_branch,
            spec=spec,
            task_type_hint=branch_type,
            model=model,
            provider=provider,
            recovery_origin=recovery_origin,
            skip_learnings=skip_learnings,
            trigger_source="manual",
        )
        if mark_next:
            assert task.id is not None
            set_task_urgency(store, task.id, urgent=True)
        print(f"✓ Added task {task.id}")
        return 0


def cmd_edit(args: argparse.Namespace) -> int:
    """Edit a task's prompt or metadata."""
    config = Config.load(args.project_dir)
    store = get_store(config)

    task_id = resolve_id(config, args.task_id)
    task = store.get(task_id)
    if not task:
        return phase1_error(args, f"Task {task_id} not found")

    tag_mutation_flags: list[str] = []
    if getattr(args, "clear_tags", False):
        tag_mutation_flags.append("--clear-tags")
    if getattr(args, "set_tags", None) is not None:
        tag_mutation_flags.append("--set-tags")
    if getattr(args, "add_tags", None):
        tag_mutation_flags.append("--add-tag")
    if getattr(args, "remove_tags", None):
        tag_mutation_flags.append("--remove-tag")

    if len(tag_mutation_flags) > 1:
        print(
            "Error: Tag mutation flags are mutually exclusive; "
            "choose exactly one of --clear-tags, --set-tags, --add-tag, or --remove-tag.",
        )
        return 1

    prompt_file_arg = getattr(args, "prompt_file", None) if hasattr(args, "prompt_file") else None
    prompt_arg = getattr(args, "prompt", None) if hasattr(args, "prompt") else None
    pending_only_flags: list[str] = []
    if prompt_file_arg is not None:
        pending_only_flags.append("--prompt-file")
    if prompt_arg is not None:
        pending_only_flags.append("--prompt")
    if getattr(args, "based_on_flag", None) is not None:
        pending_only_flags.append("--based-on")
    if getattr(args, "depends_on_flag", None) is not None:
        pending_only_flags.append("--depends-on")
    if getattr(args, "clear_depends_on", False):
        pending_only_flags.append("--clear-depends-on")
    if getattr(args, "explore", False):
        pending_only_flags.append("--explore")
    if getattr(args, "task", False):
        pending_only_flags.append("--task")
    if getattr(args, "review", False):
        pending_only_flags.append("--review")
    if getattr(args, "create_pr", False):
        pending_only_flags.append("--pr")
    if getattr(args, "model", None) is not None:
        pending_only_flags.append("--model")
    if getattr(args, "provider", None) is not None:
        pending_only_flags.append("--provider")
    if getattr(args, "skip_learnings", False):
        pending_only_flags.append("--no-learnings")

    hold_for_review_requested = getattr(args, "hold_for_review", None)
    hold_for_review_flags = tuple(getattr(args, "hold_for_review_flags", ()))
    hold_flag_requested = hold_for_review_requested is not None
    release_hold_requested = hold_for_review_requested is False
    add_hold_requested = hold_for_review_requested is True

    if hold_flag_requested and task.task_type != "plan":
        if "--auto-implement" in hold_for_review_flags:
            print("Error: --auto-implement is only valid for plan tasks.")
        else:
            print("Error: hold-for-review flags are only valid for plan tasks.")
        return 1

    non_pending_hold_edit_allowed = (
        release_hold_requested
        and task.task_type == "plan"
        and task.status == "completed"
    )

    non_pending_hold_error: str | None = None
    if hold_flag_requested and task.status != "pending" and not non_pending_hold_edit_allowed:
        if add_hold_requested and task.task_type == "plan" and task.status == "completed":
            non_pending_hold_error = "Error: --hold-for-review is only allowed for pending plan tasks."
        else:
            non_pending_hold_error = (
                "Error: hold-for-review edits are only allowed for pending plan tasks, "
                "except completed plans may use --no-hold-for-review or --auto-implement."
            )

    if task.status != "pending" and (
        pending_only_flags
        or non_pending_hold_error is not None
        or (not tag_mutation_flags and not non_pending_hold_edit_allowed)
    ):
        if pending_only_flags or (not tag_mutation_flags and not non_pending_hold_edit_allowed):
            print(
                f"Error: Task {task_id} is {task.status}; non-pending tasks only allow "
                "tag edits via --set-tags, --add-tag, --remove-tag, or --clear-tags.",
            )
        if non_pending_hold_error is not None:
            print(non_pending_hold_error)
        if pending_only_flags:
            print(f"Error: Pending-only edit flags requested: {', '.join(pending_only_flags)}")
        return 1

    assert task.id is not None
    task_row_id = task.id

    if args.explore and args.task:
        print("Error: Cannot use both --explore and --task")
        return 1

    if prompt_file_arg is not None and prompt_arg is not None:
        print("Error: Cannot use both --prompt-file and --prompt")
        return 1

    if getattr(args, "clear_depends_on", False) and getattr(args, "depends_on_flag", None) is not None:
        print("Error: Cannot use both --depends-on and --clear-depends-on")
        return 1

    based_on_id: str | None = None
    if hasattr(args, "based_on_flag") and args.based_on_flag is not None:
        based_on_id = resolve_id(config, args.based_on_flag)
        parent_task = store.get(based_on_id)
        if not parent_task:
            print(f"Error: Task {based_on_id} not found")
            return 1

    depends_on_id: str | None = None
    if hasattr(args, "depends_on_flag") and args.depends_on_flag is not None:
        depends_on_id = resolve_id(config, args.depends_on_flag)
        dep_task = store.get(depends_on_id)
        if not dep_task:
            print(f"Error: Task {depends_on_id} not found")
            return 1

    if task.task_type == "implement" and (
        based_on_id is not None
        or depends_on_id is not None
        or getattr(args, "clear_depends_on", False)
    ):
        effective_based_on_id = based_on_id if based_on_id is not None else task.based_on
        effective_depends_on_id = (
            None
            if getattr(args, "clear_depends_on", False)
            else depends_on_id if depends_on_id is not None else task.depends_on
        )
        held_plan_error = _validate_new_implement_source_not_held_for_review(
            store,
            based_on_id=effective_based_on_id,
            depends_on_id=effective_depends_on_id,
        )
        if held_plan_error is not None:
            print(held_plan_error)
            return 1

    prompt_requested = False
    new_prompt: str | None = None
    if prompt_file_arg is not None:
        try:
            with open(prompt_file_arg) as f:
                new_prompt = f.read().strip()
        except FileNotFoundError:
            print(f"Error: File not found: {prompt_file_arg}")
            return 1
        except Exception as e:
            print(f"Error reading file: {e}")
            return 1
        prompt_requested = True

    if prompt_arg is not None:
        prompt_requested = True
        if prompt_arg == "-":
            new_prompt = sys.stdin.read().strip()
        else:
            new_prompt = prompt_arg

    if prompt_requested:
        assert new_prompt is not None
        errors = validate_prompt(new_prompt)
        if errors:
            print("Validation errors:")
            for error in errors:
                print(f"  - {error}")
            return 1

    tag_action: str | None = None
    tag_values: tuple[str, ...] = ()
    if getattr(args, "clear_tags", False):
        tag_action = "clear"
    elif getattr(args, "set_tags", None) is not None:
        tag_action = "set"
        tag_values = tuple(part.strip() for part in str(args.set_tags).split(",") if part.strip())
    elif getattr(args, "add_tags", None):
        tag_action = "add"
        tag_values = tuple(args.add_tags)
    elif getattr(args, "remove_tags", None):
        tag_action = "remove"
        tag_values = tuple(args.remove_tags)

    update_messages: list[str] = []
    info_messages: list[str] = []
    changed = False

    # Handle --based-on flag (lineage/parent relationship)
    if based_on_id is not None:
        task.based_on = based_on_id
        task.recovery_origin = "manual"
        update_messages.append(f"✓ Set task {task.id} based_on task {based_on_id}")
        changed = True

    # Handle --depends-on flag (execution blocking dependency)
    if depends_on_id is not None:
        task.depends_on = depends_on_id
        update_messages.append(f"✓ Set task {task.id} to depend on task {depends_on_id}")
        changed = True
    elif getattr(args, "clear_depends_on", False):
        if task.depends_on is None:
            info_messages.append(f"Task {task.id} already has no execution dependency")
        else:
            previous_depends_on = task.depends_on
            task.depends_on = None
            update_messages.append(
                f"✓ Cleared execution dependency for task {task.id} (was {previous_depends_on})"
            )
            changed = True

    # Handle --review flag
    if hasattr(args, "review") and args.review:
        task.create_review = True
        update_messages.append(f"✓ Enabled automatic review task creation for task {task.id}")
        changed = True

    # Handle --pr flag
    if getattr(args, "create_pr", False):
        task.create_pr = True
        update_messages.append(f"✓ Enabled PR creation/reuse request for successful completion of task {task.id}")
        changed = True

    if hold_flag_requested:
        task.auto_implement = not hold_for_review_requested
        if hold_for_review_requested:
            update_messages.append(
                f"✓ Enabled hold-for-review for plan task {task.id}; automatic implementation follow-up is disabled"
            )
        else:
            update_messages.append(f"✓ Enabled automatic implementation follow-up for plan task {task.id}")
        changed = True

    # Handle --model flag
    if hasattr(args, "model") and args.model is not None:
        task.model = args.model
        task.model_is_explicit = True
        update_messages.append(f"✓ Set model override to '{args.model}' for task {task.id}")
        changed = True

    # Handle --provider flag
    if hasattr(args, "provider") and args.provider is not None:
        task.provider = args.provider
        task.provider_is_explicit = True
        update_messages.append(f"✓ Set provider override to '{args.provider}' for task {task.id}")
        changed = True

    # Handle --no-learnings flag
    if hasattr(args, "skip_learnings") and args.skip_learnings:
        task.skip_learnings = True
        update_messages.append(f"✓ Set skip_learnings for task {task.id}")
        changed = True

    # Handle type conversion without opening editor
    if args.explore or args.task:
        new_type = "explore" if args.explore else "implement"
        if task.task_type == new_type:
            info_messages.append(f"Task {task.id} is already a {new_type}")
        else:
            task.task_type = new_type
            task.last_edited_at = datetime.now(UTC)
            update_messages.append(f"✓ Converted task {task.id} to {new_type}")
            changed = True

    # Handle non-interactive prompt editing
    if prompt_requested:
        assert new_prompt is not None
        if task.prompt == new_prompt:
            info_messages.append(f"Task {task.id} prompt unchanged")
        else:
            task.prompt = new_prompt
            task.last_edited_at = datetime.now(UTC)
            update_messages.append(f"✓ Updated task {task.id}")
            changed = True

    tag_message: str | None = None
    if tag_action is not None:
        # Let store.update derive the legacy group mirror from the final tag set.
        task.group = None

    if tag_action == "clear":
        task.tags = ()
        tag_message = f"✓ Cleared tags for task {task_row_id}"
    elif tag_action == "set":
        try:
            final_tags = _normalize_tags(tag_values)
        except ValueError as exc:
            print(f"Error: {exc}")
            return 1
        task.tags = final_tags
        tag_message = f"✓ Set tags for task {task_row_id}: {', '.join(final_tags) if final_tags else '(none)'}"
    elif tag_action == "add":
        try:
            final_tags = _normalize_tags((*task.tags, *tag_values))
        except ValueError as exc:
            print(f"Error: {exc}")
            return 1
        task.tags = final_tags
        tag_message = f"✓ Added tags for task {task_row_id}: {', '.join(final_tags)}"
    elif tag_action == "remove":
        try:
            removed_tags = set(_normalize_tags(tag_values))
        except ValueError as exc:
            print(f"Error: {exc}")
            return 1
        final_tags = tuple(tag for tag in task.tags if tag not in removed_tags)
        task.tags = final_tags
        tag_message = f"✓ Updated tags for task {task_row_id}: {', '.join(final_tags) if final_tags else '(none)'}"

    if changed or tag_action is not None:
        store.update(task)
        if tag_message is not None:
            update_messages.append(tag_message)

    if changed or tag_action is not None:
        for message in update_messages:
            print(message)
        for message in info_messages:
            print(message)
        return 0

    if info_messages:
        for message in info_messages:
            print(message)
        return 0

    if edit_task_interactive(store, task):
        print(f"✓ Updated task {task.id}")
        return 0
    return 1


def cmd_retry(args: argparse.Namespace) -> int:
    """Retry a failed or completed task by creating a new pending task."""
    config = Config.load(args.project_dir)
    if hasattr(args, 'no_docker') and args.no_docker:
        config.use_docker = False

    # Override max_turns if specified
    if hasattr(args, 'max_turns') and args.max_turns is not None:
        config.max_steps = args.max_turns
        config.max_turns = args.max_turns
    execution_mode = _execution_mode(args)

    store = get_store(config)

    # Get the original task
    task_id = resolve_id(config, args.task_id)
    task = store.get(task_id)
    if not task:
        return phase1_error(args, f"Task {task_id} not found")

    # Validate status
    if task.status not in ("completed", "failed"):
        return phase1_error(args, f"Can only retry completed or failed tasks (task is {task.status})")

    # Check if task already has a successful retry
    children = store.get_based_on_children(task_id)
    successful_retry = next((c for c in children if c.status == "completed"), None)
    if successful_retry:
        return phase1_error(args, f"Task {task_id} already has a successful retry ({successful_retry.id}).")

    reserved_launch = _reserve_immediate_execution_permit(args=args, config=config, store=store)
    if reserved_launch is False:
        return 1
    new_task = _create_retry_task(store, task, trigger_source="manual")
    assert new_task.id is not None

    def _emit_retry_created() -> None:
        print(f"✓ Created task {new_task.id} (retry of {task_id})")

    prepared_retry_task, _retry_launch_permit = _finalize_immediate_execution_task(
        args=args,
        config=config,
        rollback_on_failure=True,
        task=new_task,
        emit_created=_emit_retry_created,
        reserved_permit=reserved_launch if isinstance(reserved_launch, LaunchPermit) else None,
    )
    if prepared_retry_task is None:
        return 1
    new_task = prepared_retry_task

    # Handle queue mode - add to queue without executing
    if execution_mode == "queue":
        return 0

    # Handle background mode - spawn worker to run the new task
    if execution_mode == "background":
        # Create a temporary args object for the worker with the new task_id
        assert new_task.id is not None
        worker_args = argparse.Namespace(**vars(args))
        worker_args.task_ids = [new_task.id]
        rc = _spawn_background_worker(
            worker_args,
            config,
            task_id=new_task.id,
            prepared_task=new_task,
        )
        release_task_launch_permit(str(new_task.id))
        return rc

    # Default: run the new task immediately
    print(f"\nRunning task {new_task.id}...")
    rc = _run_foreground(
        config,
        task_id=new_task.id,
        force=getattr(args, "force", False),
        invocation=_foreground_command_invocation("retry"),
    )
    release_task_launch_permit(str(new_task.id))
    return rc


def _default_mark_completed_mode(task_type: str) -> str:
    """Choose default completion mode based on task type."""
    if task_type in {"task", "implement", "improve"}:
        return "verify-git"
    return "force"


def _log_indicates_inline_skill(task: DbTask, config: Config) -> tuple[bool, str | None]:
    """Check synthetic inline-skill provenance in task logs.

    Returns ``(found, warning)`` where ``warning`` is set when log provenance
    could not be fully evaluated due to read or parse issues.
    """
    if not task.log_file:
        return False, None
    conversation_path = config.project_dir / Path(task.log_file)
    ops_path = ops_log_path_for(conversation_path)

    def _scan(path: Path, display_path: str) -> tuple[bool, str | None]:
        malformed_lines = 0
        try:
            with path.open(encoding="utf-8", errors="replace") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError:
                        malformed_lines += 1
                        continue
                    if entry.get("type") != "gza" or entry.get("subtype") != "provenance":
                        continue
                    if entry.get("inline") is True and entry.get("skill"):
                        return True, None
        except OSError as exc:
            return (
                False,
                (
                    f"Warning: Could not read task log '{display_path}' while checking inline provenance: "
                    f"{exc.__class__.__name__}: {exc}"
                ),
            )
        if malformed_lines:
            return (
                False,
                (
                    f"Warning: Found {malformed_lines} malformed JSON line(s) in task log "
                    f"'{display_path}' while checking inline provenance; execution mode was not promoted."
                ),
            )
        return False, None

    warnings: list[str] = []
    if ops_path.exists():
        found, warning = _scan(ops_path, str(ops_path.relative_to(config.project_dir)))
        if found:
            return True, None
        if warning:
            warnings.append(warning)

    if conversation_path.exists():
        found, warning = _scan(conversation_path, task.log_file)
        if found:
            return True, None
        if warning:
            warnings.append(warning)

    if warnings:
        return False, " ".join(warnings)
    return False, None


def cmd_mark_completed(args: argparse.Namespace) -> int:
    """Mark a task as completed with either git verification or status-only mode."""
    config = Config.load(args.project_dir)
    store = get_store(config)

    task_id = resolve_id(config, args.task_id)
    task = store.get(task_id)
    if not task:
        print(f"Error: Task {task_id} not found")
        return 1

    if task.status == "completed":
        print(f"Error: Task {task_id} is already completed")
        return 1

    if args.verify_git and args.force:
        print("Error: Cannot use --verify-git and --force together")
        return 1

    mode = "verify-git" if args.verify_git else ("force" if args.force else _default_mark_completed_mode(task.task_type))

    if task.execution_mode is None:
        inline_skill_detected, log_warning = _log_indicates_inline_skill(task, config)
        if log_warning:
            print(log_warning)
        if inline_skill_detected:
            task.execution_mode = "skill_inline"
            store.update(task)

    # Warn if task wasn't failed (but still proceed)
    if task.status != "failed":
        print(f"Warning: Task {task_id} is not in failed status (current status: {task.status}), proceeding anyway")

    if mode == "force":
        old_status = task.status
        store.mark_completed(
            task,
            branch=task.branch if task.branch else None,
            completion_reason=args.reason,
        )
        _cleanup_worker_registry(config, task_id)
        print(f"✓ Task {task_id} status changed: {old_status} → completed (status-only)")
        return 0

    # verify-git mode: validate branch and commit state
    if not task.branch:
        print(f"Error: Task {task_id} has no branch set. Use --force for status-only completion.")
        return 1

    git = Git(config.project_dir)
    if not git.branch_exists(task.branch):
        print(f"Error: Branch '{task.branch}' does not exist. Use --force for status-only completion.")
        return 1

    default_branch = git.default_branch()
    commit_count = git.count_commits_ahead(task.branch, default_branch)
    if commit_count <= 0:
        print(f"Note: No commits found on branch '{task.branch}' compared to '{default_branch}'")
        store.mark_completed(
            task,
            branch=task.branch,
            has_commits=False,
            completion_reason=args.reason,
        )
        _cleanup_worker_registry(config, task_id)
        print(f"✓ Task {task_id} marked as completed")
        return 0

    store.mark_completed(
        task,
        branch=task.branch,
        has_commits=True,
        head_sha=git.rev_parse_if_exists(task.branch),
        base_sha=git.rev_parse_if_exists(default_branch),
        completion_reason=args.reason,
    )
    _cleanup_worker_registry(config, task_id)
    print(f"✓ Task {task_id} marked as completed (unmerged, {commit_count} commit(s) on branch '{task.branch}')")

    return 0


def cmd_set_status(args: argparse.Namespace) -> int:
    """Manually force a task's status to an operator-assertable value."""
    if args.status == "in_progress":
        print(
            "Error: 'in_progress' is set by a running worker, not by manual operator action.\n"
            "       To start work on a task, run `gza work <id>` for pending tasks,\n"
            "       `gza resume <id>` to reattach to running work, `gza retry <id>`\n"
            "       for failed tasks, or let `gza watch` pick up a pending task."
        )
        return 1
    if args.status == "completed":
        print(
            "Error: 'completed' cannot be set via set-status. Use `gza mark-completed <id>` "
            "(supports --verify-git and --force)."
        )
        return 1
    if args.status not in {"pending", "failed", "dropped"}:
        print(
            f"Error: Invalid status '{args.status}'. "
            "Valid statuses: pending, failed, dropped."
        )
        return 1

    config = Config.load(args.project_dir)
    store = get_store(config)

    task_id = resolve_id(config, args.task_id)
    task = store.get(task_id)
    if not task:
        print(f"Error: Task {task_id} not found")
        return 1

    old_status = task.status
    if args.status == old_status:
        print(f"Task {task_id} is already in status '{args.status}'; no change.")
        return 0

    if args.status == "pending":
        if old_status == "failed":
            print(
                "Error: Cannot reset a failed task to pending via set-status.\n"
                "Use `gza retry <id>` to re-run (proper worker registry reset and\n"
                "failure_reason clearing), or `gza set-status <id> dropped` to\n"
                "abandon."
            )
            return 1
        if old_status == "completed":
            print(
                "Error: Completed tasks cannot be reset to pending; the branch and\n"
                "logs would be at risk of being clobbered by a new run.\n"
                "To run similar work, create a new task with `gza add`.\n"
                "To revert a falsely-completed task, use\n"
                "`gza set-status <id> failed --reason '...'`."
            )
            return 1
        if old_status != "dropped":
            print(
                f"Error: Tasks in status '{old_status}' cannot be reset to pending via set-status.\n"
                "Pending is reserved for reviving a dropped task.\n"
                "If work is still active, use `gza resume <id>` to reattach to the running task.\n"
                "If the worker is gone, settle it with `gza set-status <id> failed --reason '...'`\n"
                "or `gza set-status <id> dropped` instead."
            )
            return 1

    if args.reason and args.status != "failed":
        print(
            "Warning: --reason is only meaningful for 'failed' status "
            f"(current target: '{args.status}')"
        )

    apply_manual_task_status(
        config=config,
        store=store,
        task=task,
        status=args.status,
        reason=args.reason,
    )
    _cleanup_worker_registry(config, task_id)

    print(f"Task {task_id} status: {old_status} → {args.status}")
    return 0


def _cleanup_worker_registry(config: "Config", task_id: str) -> None:
    """Mark any running worker for a task as completed in the worker registry.

    Looks up the most recent worker associated with the task and calls
    registry.mark_completed() to update worker metadata and remove the PID file.
    If no worker exists for the task, this is a no-op.
    """
    registry = WorkerRegistry(config.workers_path)
    worker = _latest_worker_for_task(registry, task_id)
    if worker is None:
        return
    if worker.status in ("running", "stale"):
        registry.mark_completed(worker.worker_id, exit_code=0, status="completed")


def _latest_completed_review_for_impl(store: SqliteTaskStore, impl_task_id: str) -> DbTask | None:
    reviews = [r for r in store.get_reviews_for_task(impl_task_id) if r.status == "completed"]
    return reviews[0] if reviews else None


def _review_targets_implementation(review_task: DbTask, impl_task_id: str) -> bool:
    """Return whether a review is canonically linked to the implementation."""
    if review_task.based_on is not None:
        return review_task.based_on == impl_task_id
    return review_task.depends_on == impl_task_id


def _latest_non_dropped_plan_review_for_source(
    store: SqliteTaskStore,
    plan_source_task: DbTask,
) -> DbTask | None:
    assert plan_source_task.id is not None
    candidates = [
        child
        for child in store.get_lineage_children(plan_source_task.id)
        if child.task_type == "plan_review"
        and child.depends_on == plan_source_task.id
        and child.status != "dropped"
    ]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda task: (
            task.completed_at or task.created_at or datetime.min.replace(tzinfo=UTC),
            task_id_numeric_key(task.id),
        ),
    )


def _latest_completed_non_dropped_plan_review_for_source(
    store: SqliteTaskStore,
    plan_source_task: DbTask,
) -> DbTask | None:
    assert plan_source_task.id is not None
    candidates = [
        child
        for child in store.get_lineage_children(plan_source_task.id)
        if child.task_type == "plan_review"
        and child.depends_on == plan_source_task.id
        and child.status == "completed"
    ]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda task: (
            task.completed_at or task.created_at or datetime.min.replace(tzinfo=UTC),
            task_id_numeric_key(task.id),
        ),
    )


def _matching_plan_improve_for_review(
    store: SqliteTaskStore,
    plan_source_task: DbTask,
    review_task: DbTask,
) -> DbTask | None:
    assert plan_source_task.id is not None
    assert review_task.id is not None
    candidates = [
        child
        for child in store.get_lineage_children(plan_source_task.id)
        if child.task_type == "plan_improve"
        and child.based_on == plan_source_task.id
        and child.depends_on == review_task.id
        and child.status != "dropped"
    ]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda task: (
            task.completed_at or task.created_at or datetime.min.replace(tzinfo=UTC),
            task_id_numeric_key(task.id),
        ),
    )


def cmd_improve(args: argparse.Namespace) -> int:
    """Create an improve task based on an implementation task and its most recent review."""
    config = Config.load(args.project_dir)
    if hasattr(args, 'no_docker') and args.no_docker:
        config.use_docker = False

    # Override max_turns if specified
    if hasattr(args, 'max_turns') and args.max_turns is not None:
        config.max_steps = args.max_turns
        config.max_turns = args.max_turns
    execution_mode = _execution_mode(args)

    store = get_store(config)

    impl_task, err = resolve_impl_task(store, resolve_id(config, args.task_id))
    if err:
        return phase1_error(args, err)
    assert impl_task is not None
    assert impl_task.id is not None

    unresolved_comments = store.get_comments(
        impl_task.id,
        unresolved_only=True,
        kinds=(TASK_COMMENT_KIND_FEEDBACK,),
    )

    review_id_override = getattr(args, "review_id", None)
    review_task: DbTask | None = None
    if review_id_override is not None:
        resolved_review_id = resolve_id(config, review_id_override)
        review_task = store.get(resolved_review_id)
        if review_task is None:
            return phase1_error(args, f"Review task {resolved_review_id} not found.")
        if review_task.task_type != "review":
            return phase1_error(args, f"Task {resolved_review_id} is a {review_task.task_type} task, not a review.")
        if not _review_targets_implementation(review_task, impl_task.id):
            return phase1_error(
                args,
                f"Review {resolved_review_id} reviews task "
                f"{review_task.based_on or review_task.depends_on}, "
                f"not implementation {impl_task.id}.",
            )
        if review_task.status in ("failed", "dropped"):
            # Terminal review statuses never produce feedback. Binding an
            # improve to one creates a permanently blocked task, so reject
            # rather than warn.
            print_phase1_message(
                args,
                f"Error: Review {review_task.id} is {review_task.status}; "
                "terminal reviews cannot produce feedback.",
            )
            if unresolved_comments:
                print_phase1_message(
                    args,
                    f"Omit --review-id to run a comments-only improve from the "
                    f"{len(unresolved_comments)} unresolved comment(s).",
                )
            else:
                print_phase1_message(
                    args,
                    "Run a new review, or add comments with "
                    f"`gza comment {impl_task.id} <text>`.",
                )
            return 1
        if review_task.status != "completed":
            print(
                f"Warning: Review {review_task.id} is {review_task.status}. "
                "The improve task will be blocked until it completes."
            )
    else:
        # Auto-pick considers only completed reviews. Non-completed reviews
        # (pending/in_progress/failed/dropped) are not eligible: terminal
        # statuses never produce feedback, and a pending/in_progress review
        # has no report yet. When no completed review exists but unresolved
        # comments do, fall back to a comments-only improve. To target an
        # incomplete review explicitly, use --review-id.
        review_tasks = store.get_reviews_for_task(impl_task.id)
        completed_reviews = [r for r in review_tasks if r.status == "completed"]

        if completed_reviews:
            review_task = completed_reviews[0]
        elif unresolved_comments:
            print(
                f"Note: Task {impl_task.id} has no completed review; "
                "continuing from unresolved comments only."
            )
        elif review_tasks:
            statuses = ", ".join(
                f"{r.id} ({r.status})" for r in review_tasks
            )
            print_phase1_message(
                args,
                f"Error: Task {impl_task.id} has no completed review "
                f"(existing reviews: {statuses}).",
            )
            print_phase1_message(
                args,
                "Wait for a review to complete, add comments via "
                f"`gza comment {impl_task.id} <text>`, or pass --review-id <id> "
                "to target a specific review.",
            )
            return 1
        else:
            print_phase1_message(args, f"Error: Task {impl_task.id} has no review. Run a review first:")
            print_phase1_message(args, f"  gza add --type review --depends-on {impl_task.id}")
            return 1

    create_review = args.review if hasattr(args, 'review') and args.review else False
    create_pr = bool(getattr(args, "create_pr", False))
    model = args.model if hasattr(args, 'model') and args.model else None
    provider = args.provider if hasattr(args, 'provider') and args.provider else None
    reserved_launch = _reserve_immediate_execution_permit(args=args, config=config, store=store)
    if reserved_launch is False:
        return 1

    with _release_reserved_launch_unless_transferred(reserved_launch) as mark_launch_transferred:
        improve_task: DbTask
        action_message: str | None = None

        def _apply_comments_only_invocation_overrides(task: DbTask) -> DbTask:
            """Reset comments-only improve reuse/restart state to current CLI intent."""
            task.create_review = create_review
            task.create_pr = create_pr
            task.model = model
            task.model_is_explicit = model is not None
            task.provider = provider
            task.provider_is_explicit = provider is not None
            store.update(task)
            return task

        if review_task is None:
            comments_action, existing_comments_improve, comments_decision = resolve_comments_improve_action(
                store,
                impl_task.id,
                max_resume_attempts=config.max_resume_attempts,
            )
            if comments_action == "wait_in_progress":
                assert existing_comments_improve is not None and existing_comments_improve.id is not None
                return phase1_error(
                    args,
                    f"Comments-only improve {existing_comments_improve.id} is already in progress. "
                    "Wait for it to finish.",
                )
            if comments_action == "reuse_pending":
                assert existing_comments_improve is not None and existing_comments_improve.id is not None
                improve_task = _apply_comments_only_invocation_overrides(existing_comments_improve)
                action_message = f"Reusing pending improve task {improve_task.id}"
            elif comments_action == "give_up":
                assert existing_comments_improve is not None and existing_comments_improve.id is not None
                return phase1_error(
                    args,
                    "Comments-only improve automatic recovery is disabled "
                    f"(max_resume_attempts={config.max_resume_attempts}); "
                    f"latest failure: {existing_comments_improve.id}",
                )
            elif comments_action == "manual_review":
                assert existing_comments_improve is not None and existing_comments_improve.id is not None
                assert comments_decision is not None
                return phase1_error(
                    args,
                    f"Latest comments-only improve failure {existing_comments_improve.id} "
                    f"requires manual review ({comments_decision.reason_text})",
                )
            elif comments_action == "resume":
                assert existing_comments_improve is not None and existing_comments_improve.id is not None
                improve_task = _apply_comments_only_invocation_overrides(
                    _create_resume_task(store, existing_comments_improve, trigger_source="manual")
                )
                action_message = f"Created improve task {improve_task.id} (resume of {existing_comments_improve.id})"
            elif comments_action == "retry":
                assert existing_comments_improve is not None and existing_comments_improve.id is not None
                # Comments-only improve restarts keep the shared retry/resume creators,
                # but preserve the historical cmd_improve contract: omitted CLI flags
                # reset to the current invocation defaults instead of inheriting
                # stale values from the failed improve task.
                improve_task = _apply_comments_only_invocation_overrides(
                    _create_retry_task(store, existing_comments_improve, trigger_source="manual")
                )
                action_message = f"Created improve task {improve_task.id} (retry of {existing_comments_improve.id})"
            else:
                try:
                    improve_task = _create_improve_task(
                        store,
                        impl_task,
                        None,
                        trigger_source="manual",
                        create_review=create_review,
                        create_pr=create_pr,
                        model=model,
                        provider=provider,
                    )
                except ValueError as e:
                    return phase1_error(args, str(e))
        else:
            # Create improve task (using shared helper)
            try:
                improve_task = _create_improve_task(
                    store,
                    impl_task,
                    review_task,
                    trigger_source="manual",
                    create_review=create_review,
                    create_pr=create_pr,
                    model=model,
                    provider=provider,
                )
            except ValueError as e:
                return phase1_error(args, str(e))

        created_new_improve = not (review_task is None and comments_action == "reuse_pending")
        assert improve_task.id is not None

        def _emit_improve_created() -> None:
            if action_message is not None:
                print(f"✓ {action_message}")
            else:
                print(f"✓ Created improve task {improve_task.id}")
            print(f"  Based on: implementation {impl_task.id}")
            if review_task is not None:
                print(f"  Review: {review_task.id}")
            elif unresolved_comments:
                print(f"  Comments: {len(unresolved_comments)} unresolved")
            print(f"  Branch: {impl_task.branch or '(will use implementation branch)'}")

        prepared_improve_task, _improve_launch_permit = _finalize_immediate_execution_task(
            args=args,
            config=config,
            rollback_on_failure=created_new_improve,
            task=improve_task,
            emit_created=_emit_improve_created,
            reserved_permit=reserved_launch if isinstance(reserved_launch, LaunchPermit) else None,
        )
        if prepared_improve_task is None:
            return 1
        improve_task = prepared_improve_task
        mark_launch_transferred()

    # Handle queue mode - add to queue without executing
    if execution_mode == "queue":
        return 0

    # Handle background mode - spawn worker to run the improve task
    if execution_mode == "background":
        assert improve_task.id is not None
        worker_args = argparse.Namespace(**vars(args))
        worker_args.task_ids = [improve_task.id]
        rc = _spawn_background_worker(
            worker_args,
            config,
            task_id=improve_task.id,
            prepared_task=improve_task,
        )
        release_task_launch_permit(str(improve_task.id))
        return rc

    # Default: run the improve task immediately
    print(f"\nRunning improve task {improve_task.id}...")
    rc = _run_foreground(
        config,
        task_id=improve_task.id,
        force=getattr(args, "force", False),
        invocation=_foreground_command_invocation("improve"),
    )
    release_task_launch_permit(str(improve_task.id))
    return rc


def cmd_fix(args: argparse.Namespace) -> int:
    """Create and run a fix task for a stuck implementation workflow."""
    config = Config.load(args.project_dir)
    if hasattr(args, 'no_docker') and args.no_docker:
        config.use_docker = False

    if hasattr(args, 'max_turns') and args.max_turns is not None:
        config.max_steps = args.max_turns
        config.max_turns = args.max_turns
    execution_mode = _execution_mode(args)

    store = get_store(config)
    impl_task, err = resolve_impl_task(store, resolve_id(config, args.task_id))
    if err:
        return phase1_error(args, err)
    assert impl_task is not None
    assert impl_task.id is not None

    if impl_task.status in {"pending", "in_progress"}:
        return phase1_error(
            args,
            f"Task {impl_task.id} is {impl_task.status}. "
            "Run/finish the implementation first, then run fix for stuck review/improve churn.",
        )
    reserved_launch = _reserve_immediate_execution_permit(args=args, config=config, store=store)
    if reserved_launch is False:
        return 1

    latest_review = _latest_completed_review_for_impl(store, impl_task.id)
    review_id = latest_review.id if latest_review is not None else None
    fix_prompt = PromptBuilder().fix_task_prompt(impl_task.id, review_id)
    create_review = args.review if hasattr(args, "review") and args.review else False
    fix_task = store.add(
        fix_prompt,
        task_type="fix",
        based_on=impl_task.id,
        depends_on=review_id,
        same_branch=True,
        review_scope=_resolved_review_scope_metadata(impl_task),
        create_review=create_review,
        tags=resolve_derived_task_tags(impl_task),
        model=args.model if hasattr(args, "model") and args.model else None,
        provider=args.provider if hasattr(args, "provider") and args.provider else None,
        trigger_source="manual",
    )
    assert fix_task.id is not None

    def _emit_fix_created() -> None:
        print(f"✓ Created fix task {fix_task.id}")
        print(f"  Implementation: {impl_task.id}")
        if review_id:
            print(f"  Latest completed review: {review_id}")
        else:
            print("  Latest completed review: (none found)")
        print("  Handoff policy: changed code requires a fresh independent review")

    prepared_fix_task, _fix_launch_permit = _finalize_immediate_execution_task(
        args=args,
        config=config,
        rollback_on_failure=True,
        task=fix_task,
        emit_created=_emit_fix_created,
        reserved_permit=reserved_launch if isinstance(reserved_launch, LaunchPermit) else None,
    )
    if prepared_fix_task is None:
        return 1
    fix_task = prepared_fix_task

    if execution_mode == "queue":
        return 0

    if execution_mode == "background":
        worker_args = argparse.Namespace(**vars(args))
        worker_args.task_ids = [fix_task.id]
        rc = _spawn_background_worker(
            worker_args,
            config,
            task_id=fix_task.id,
            prepared_task=fix_task,
        )
        release_task_launch_permit(str(fix_task.id))
        return rc

    print(f"\nRunning fix task {fix_task.id}...")
    rc = _run_foreground(
        config,
        task_id=fix_task.id,
        force=getattr(args, "force", False),
        invocation=_foreground_command_invocation("fix"),
    )
    release_task_launch_permit(str(fix_task.id))
    return rc


def cmd_comment(args: argparse.Namespace) -> int:
    """Add a direct comment to a task."""
    config = Config.load(args.project_dir)
    store = get_store(config)

    task_id = resolve_id(config, args.task_id)
    task = store.get(task_id)
    if task is None:
        print(f"Error: Task {task_id} not found")
        return 1

    author = getattr(args, "author", None)
    kind = getattr(args, "kind", "feedback")
    try:
        comment = store.add_comment(task_id, args.text, source="direct", author=author, kind=kind)
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1

    print(f"✓ Added {comment.kind} comment {comment.id} to task {task_id}")
    return 0


def cmd_review(args: argparse.Namespace) -> int:
    """Create a review task for an implementation/improve task and optionally run it."""
    config = Config.load(args.project_dir)
    if args.no_docker:
        config.use_docker = False
    execution_mode = _execution_mode(args)

    store = get_store(config)

    # Resolve target implementation from provided task (accepts implement, improve, or review)
    impl_task, err = resolve_impl_task(store, resolve_id(config, args.task_id))
    if err:
        return phase1_error(args, err)
    assert impl_task is not None

    # Check if task is completed
    if impl_task.status != "completed":
        return phase1_error(args, f"Task {impl_task.id} is {impl_task.status}. Can only review completed tasks.")

    # Create review task (using shared helper)
    model = args.model if hasattr(args, 'model') and args.model else None
    provider = args.provider if hasattr(args, 'provider') and args.provider else None
    reserved_launch = _reserve_immediate_execution_permit(args=args, config=config, store=store)
    if reserved_launch is False:
        return 1
    with _release_reserved_launch_unless_transferred(reserved_launch) as mark_launch_transferred:
        try:
            review_task = _create_review_task(
                store,
                impl_task,
                trigger_source="manual",
                model=model,
                provider=provider,
            )
        except DuplicateReviewError as e:
            review = e.active_review
            print_phase1_message(args, f"Warning: A review task already exists for implementation {impl_task.id}")
            print_phase1_message(args, f"  Existing review: {review.id} (status: {review.status})")
            print_phase1_message(args, f"  Use 'gza work' to run it, or 'gza review {impl_task.id}' after it completes.")
            return 1
        except ValueError as e:
            return phase1_error(args, str(e))
        assert review_task.id is not None

        def _emit_review_created() -> None:
            print(f"✓ Created review task {review_task.id}")
            print(f"  Implementation: {impl_task.id}")
            if len(impl_task.tags) == 1:
                print(f"  Group: {impl_task.tags[0]}")
            if impl_task.tags:
                print(f"  Tags: {', '.join(impl_task.tags)}")

        prepared_review_task, _review_launch_permit = _finalize_immediate_execution_task(
            args=args,
            config=config,
            rollback_on_failure=True,
            task=review_task,
            emit_created=_emit_review_created,
            reserved_permit=reserved_launch if isinstance(reserved_launch, LaunchPermit) else None,
        )
        if prepared_review_task is None:
            return 1
        review_task = prepared_review_task
        mark_launch_transferred()

    # Handle queue mode - add to queue without executing
    if execution_mode == "queue":
        return 0

    # Handle background mode - spawn worker to run the review task
    if execution_mode == "background":
        assert review_task.id is not None
        worker_args = argparse.Namespace(**vars(args))
        worker_args.task_ids = [review_task.id]
        rc = _spawn_background_worker(
            worker_args,
            config,
            task_id=review_task.id,
            prepared_task=review_task,
        )
        release_task_launch_permit(str(review_task.id))
        return rc

    # Default: run the review task immediately
    # Note: PR posting happens in _run_non_code_task, no need to do it here
    print(f"\nRunning review task {review_task.id}...")
    open_after = hasattr(args, 'open') and args.open
    rc = _run_foreground(
        config,
        task_id=review_task.id,
        open_after=open_after,
        force=getattr(args, "force", False),
        invocation=_foreground_command_invocation("review"),
    )
    release_task_launch_permit(str(review_task.id))
    if rc == 0:
        assert review_task.id is not None
        refreshed_review = store.get(review_task.id)
        if refreshed_review is not None and refreshed_review.status == "completed":
            print(f"Review {refreshed_review.id}: {format_review_outcome(config, refreshed_review)}")
    return rc


def _spawn_background_iterate(
    args: argparse.Namespace,
    config: Config,
    impl_task: DbTask,
    *,
    max_iterations: int | None = None,
    dry_run: bool = False,
    prepared_task_id: str | None = None,
    prepared_resume: bool = False,
    prepared_phase: str | None = None,
    prepared_action_type: str | None = None,
    prepared_review_task_id: str | None = None,
    startup_quiet: bool = False,
) -> int:
    """Spawn the iterate loop as a detached background process."""
    effective_max_iterations = max_iterations
    if effective_max_iterations is None:
        arg_value = getattr(args, "max_iterations", None)
        effective_max_iterations = arg_value if isinstance(arg_value, int) else config.iterate_max_iterations

    return _spawn_background_iterate_worker(
        args,
        config,
        impl_task,
        max_iterations=effective_max_iterations,
        resume=getattr(args, "resume", False),
        retry=getattr(args, "retry", False),
        auto_iterate=bool(getattr(args, "auto_iterate", False)),
        dry_run=dry_run,
        prepared_task_id=prepared_task_id,
        prepared_resume=prepared_resume,
        prepared_phase=prepared_phase,
        prepared_action_type=prepared_action_type,
        prepared_review_task_id=prepared_review_task_id,
        startup_quiet=startup_quiet,
    )


@dataclass(frozen=True)
class _AdvanceEngineConfigAdapter:
    """Minimal config surface required by determine_next_action()."""

    project_dir: Any
    require_review_before_merge: bool
    advance_create_reviews: bool
    max_review_cycles: int
    max_resume_attempts: int
    max_noop_improve_cycles: int = 1
    max_failed_closing_review_retries: int = DEFAULT_MAX_FAILED_CLOSING_REVIEW_RETRIES


def _iterate_action_description(action: dict[str, Any]) -> str:
    """Return a user-facing description for an iterate action."""
    description = action.get("description")
    if isinstance(description, str) and description:
        return description.removeprefix("SKIP: ").strip()
    return str(action.get("type", "skip"))


@dataclass(frozen=True)
class _PreparedIterateStart:
    task: DbTask
    initial_resume: bool
    phase: str
    action_type: str | None = None
    review_task_id: str | None = None


def _cmd_iterate_impl(args: argparse.Namespace, config: Config) -> int:
    """Run an automated lifecycle loop for an implementation task."""
    store = get_store(config)
    def _int_config(value: object, default: int) -> int:
        return value if isinstance(value, int) else default

    max_iterations_arg = getattr(args, "max_iterations", None)
    max_iterations = max_iterations_arg if max_iterations_arg is not None else config.iterate_max_iterations
    if max_iterations <= 0:
        return phase1_error(args, "--max-iterations must be a positive integer.")
    dry_run: bool = getattr(args, 'dry_run', False)
    use_resume: bool = getattr(args, 'resume', False)
    use_retry: bool = getattr(args, 'retry', False)
    background: bool = getattr(args, 'background', False)

    # cmd_iterate intentionally only accepts implement task IDs (not improve/review);
    # it manages the full review/improve iteration lifecycle and requires the root impl task.
    impl_task_id = resolve_id(config, args.impl_task_id)
    impl_task = store.get(impl_task_id)
    if not impl_task:
        return phase1_error(args, f"Task {impl_task_id} not found")
    if impl_task.task_type != "implement":
        return phase1_error(args, f"Task {impl_task.id} is a {impl_task.task_type} task. Expected an implement task.")

    requested_impl_task = impl_task
    resolved_impl_task = resolve_recovery_planning_task(store, impl_task)
    resolved_from_failed_ancestor = requested_impl_task is not resolved_impl_task
    if resolved_from_failed_ancestor:
        impl_task = resolved_impl_task

    allowed_statuses = {"completed", "pending", "failed"}
    if impl_task.status not in allowed_statuses:
        return phase1_error(
            args,
            f"Task {impl_task.id} is {impl_task.status}. Can only iterate completed, pending, or failed tasks.",
        )

    effective_max_resume_attempts = _int_config(
        getattr(config, "max_resume_attempts", None),
        DEFAULT_MAX_RESUME_ATTEMPTS,
    )

    def _resolve_prepared_iterate_seed() -> tuple[DbTask | None, str | None, int | None]:
        prepared_task_id = getattr(args, "prepared_task_id", None)
        if not prepared_task_id:
            return None, None, None
        prepared_phase = getattr(args, "prepared_phase", None) or "preloop"
        if prepared_phase not in {"preloop", "iteration"}:
            print(f"Error: unsupported iterate prepared startup phase {prepared_phase!r}.")
            return None, None, 1
        prepared_action_type = getattr(args, "prepared_action_type", None)
        if prepared_phase == "iteration" and not isinstance(prepared_action_type, str):
            print("Error: prepared iterate iteration start is missing an action type.")
            return None, None, 1
        prepared_task = store.get(prepared_task_id)
        if prepared_task is None:
            print(f"Error: prepared iterate task {prepared_task_id} not found.")
            return None, None, 1
        return prepared_task, prepared_phase, None

    prepared_validation_task, prepared_validation_phase, prepared_validation_rc = _resolve_prepared_iterate_seed()
    if prepared_validation_rc is not None:
        return prepared_validation_rc
    prepared_preloop_task = prepared_validation_task if prepared_validation_phase == "preloop" else None
    resolved_unit = store.resolve_merge_unit_for_task(impl_task.id) if impl_task.id is not None else None
    failed_start_decision = (
        decide_failed_task_recovery(
            store,
            impl_task,
            max_recovery_attempts=max(1, effective_max_resume_attempts),
        )
        if impl_task.status == "failed"
        else None
    )
    failed_task_is_lifecycle_moot = bool(
        impl_task.status == "failed"
        and resolved_unit is not None
        and merge_state_is_terminal_for_lifecycle(resolved_unit.state)
        and not empty_task_requires_recovery(store, impl_task, merge_state=resolved_unit.state)
    )
    failed_task_is_lifecycle_moot = failed_task_is_lifecycle_moot or bool(
        failed_start_decision is not None
        and failed_start_decision.action == "skip"
        and failed_start_decision.reason_code in _ITERATE_TERMINAL_NO_WORK_REASON_CODES
    )

    if (
        prepared_preloop_task is None
        and impl_task.status == "failed"
        and not use_resume
        and not use_retry
        and not failed_task_is_lifecycle_moot
    ):
        return phase1_error(
            args,
            f"Task {impl_task.id} is failed. Use --resume or --retry to specify how to restart it.",
        )

    if (
        prepared_preloop_task is None
        and (use_resume or use_retry)
        and (impl_task.status != "failed" or failed_task_is_lifecycle_moot)
    ):
        if (resolved_from_failed_ancestor and requested_impl_task.status == "failed") or (
            impl_task.status == "failed" and failed_task_is_lifecycle_moot
        ):
            use_resume = False
            use_retry = False
        else:
            flag = "--resume" if use_resume else "--retry"
            return phase1_error(
                args,
                f"{flag} is only valid for failed tasks (task {impl_task.id} is {impl_task.status}).",
            )

    if (
        impl_task.status == "failed"
        and not use_resume
        and not use_retry
        and failed_start_decision is not None
        and failed_start_decision.action == "skip"
        and failed_start_decision.reason_code in _ITERATE_TERMINAL_NO_WORK_REASON_CODES
    ):
        terminal_message = _format_iterate_terminal_merge_state_message(
            store=store,
            requested_impl_task=requested_impl_task,
            iterate_task=impl_task,
            resolved_from_failed_ancestor=resolved_from_failed_ancestor,
            merge_state="redundant" if failed_start_decision.reason_code == "merge_unit_redundant" else "empty",
        )
        if terminal_message is not None:
            print(terminal_message)
            return 0

    assert impl_task.id is not None
    active_impl_task_id = impl_task.id

    if prepared_preloop_task is None and impl_task.status == "failed" and use_resume and not impl_task.session_id:
        return phase1_error(args, f"Task {impl_task.id} has no session ID (cannot resume). Use --retry instead.")

    manual_iterate = not bool(getattr(args, "auto_iterate", False))

    def _decision_hits_max_auto_resume_cap(
        failed_task: DbTask,
        decision: FailedRecoveryDecision,
    ) -> bool:
        return (
            get_failed_recovery_needs_attention_reason(
                store,
                failed_task,
                decision=decision,
                max_recovery_attempts=max(1, effective_max_resume_attempts),
            )
            == "retry-limit-reached"
        )

    def _resolve_manual_resume_override(
        failed_task: DbTask,
        decision: FailedRecoveryDecision,
    ) -> tuple[DbTask, FailedRecoveryDecision, list[str]] | None:
        if not manual_iterate or decision.action == "resume":
            return None
        attention_reason = get_failed_recovery_needs_attention_reason(
            store,
            failed_task,
            decision=decision,
            max_recovery_attempts=max(1, effective_max_resume_attempts),
        )
        if (
            attention_reason != "newer-recovery-descendant-needs-attention"
            and decision.reason_code != "retry_limit_reached"
        ):
            return None
        descendant = get_manual_resume_override_descendant(
            store,
            failed_task,
            decision=decision,
            max_recovery_attempts=max(1, effective_max_resume_attempts),
        )
        if descendant is None or descendant.id is None or descendant.id == failed_task.id:
            if _decision_hits_max_auto_resume_cap(failed_task, decision):
                assert failed_task.id is not None
                return (
                    failed_task,
                    decision,
                    [
                        "warning: task "
                        f"{failed_task.id} has hit max auto-resume attempts; proceeding because this resume is manual"
                    ],
                )
            return None
        descendant_decision = _decide_failed_iterate_resume_start(descendant)
        nested_override = _resolve_manual_resume_override(descendant, descendant_decision)
        route_warning = (
            "warning: task "
            f"{failed_task.id} is blocked by newer failed recovery descendant {descendant.id}; "
            f"proceeding with manual resume from {descendant.id}"
        )
        if nested_override is not None:
            target_task, target_decision, warnings = nested_override
            return target_task, target_decision, [route_warning, *warnings]
        if descendant_decision.action != "resume":
            return None
        return descendant, descendant_decision, [route_warning]

    def _emit_manual_resume_override_warnings(
        failed_task: DbTask,
        decision: FailedRecoveryDecision,
    ) -> tuple[DbTask, FailedRecoveryDecision] | None:
        override = _resolve_manual_resume_override(failed_task, decision)
        if override is None:
            return None
        target_task, target_decision, warnings = override
        for warning in warnings:
            print(warning, file=sys.stderr)
        return target_task, target_decision

    def _decide_failed_iterate_resume_start(failed_task: DbTask) -> FailedRecoveryDecision:
        if failed_task.id is not None and failed_task.id == active_impl_task_id and failed_start_decision is not None:
            return failed_start_decision
        return decide_failed_task_recovery(
            store,
            failed_task,
            # Explicit iterate --resume is still manual intent, but the failed-start
            # selector must use shared recovery edge classification/guardrails.
            max_recovery_attempts=max(1, effective_max_resume_attempts),
        )

    def _resolve_failed_iterate_resume_start(
        failed_task: DbTask,
        *,
        emit_override_warnings: bool = True,
    ) -> tuple[
        tuple[DbTask, FailedRecoveryDecision] | None,
        tuple[DbTask, FailedRecoveryDecision] | None,
    ]:
        decision = _decide_failed_iterate_resume_start(failed_task)
        override: tuple[DbTask, FailedRecoveryDecision] | None
        if emit_override_warnings:
            override = _emit_manual_resume_override_warnings(failed_task, decision)
        else:
            manual_override = _resolve_manual_resume_override(failed_task, decision)
            override = (
                None
                if manual_override is None
                else (manual_override[0], manual_override[1])
            )
        if override is not None:
            target_failed_task, target_decision = override
            if target_decision.action == "resume":
                if target_decision.reuse_existing and target_decision.recovery_task_id is not None:
                    existing_resume = store.get(target_decision.recovery_task_id)
                    if existing_resume is None:
                        print_phase1_message(
                            args,
                            f"Error: pending resume child {target_decision.recovery_task_id} "
                            "selected by recovery policy was not found.",
                        )
                        return None, None
                    return (existing_resume, target_decision), None
                return (_create_resume_task(store, target_failed_task, trigger_source="manual"), target_decision), None
            return (_create_resume_task(store, target_failed_task, trigger_source="manual"), target_decision), None
        if decision.action != "resume":
            return None, (failed_task, decision)
        if decision.reuse_existing and decision.recovery_task_id is not None:
            existing_resume = store.get(decision.recovery_task_id)
            if existing_resume is None:
                print_phase1_message(
                    args,
                    f"Error: pending resume child {decision.recovery_task_id} "
                    "selected by recovery policy was not found.",
                )
                return None, None
            return (existing_resume, decision), None
        return (_create_resume_task(store, failed_task, trigger_source="manual"), decision), None

    engine_config = _AdvanceEngineConfigAdapter(
        project_dir=config.project_dir,
        require_review_before_merge=bool(getattr(config, "require_review_before_merge", True)),
        advance_create_reviews=bool(getattr(config, "advance_create_reviews", True)),
        max_review_cycles=_int_config(getattr(config, "max_review_cycles", None), 3),
        max_noop_improve_cycles=_int_config(getattr(config, "max_noop_improve_cycles", None), 1),
        max_resume_attempts=effective_max_resume_attempts,
        max_failed_closing_review_retries=_int_config(
            getattr(config, "max_failed_closing_review_retries", None),
            DEFAULT_MAX_FAILED_CLOSING_REVIEW_RETRIES,
        ),
    )

    def _prepare_iterate_background_preflight_context(
        iterate_task: DbTask,
    ) -> tuple[_IterateBackgroundPreflightContext | None, int | None]:
        if iterate_task.status == "pending":
            return None, None
        try:
            git_runtime: Any = Git(config.project_dir)
            target_branch = git_runtime.current_branch()
        except Exception as exc:
            task_label = iterate_task.id or "<unknown>"
            print_phase1_message(
                args,
                f"Error: failed to initialize iterate background preflight for task {task_label}: {exc}",
            )
            return None, 1
        return (
            _IterateBackgroundPreflightContext(
                git_runtime=git_runtime,
                target_branch=target_branch,
            ),
            None,
        )

    def _warn_manual_background_iterate_override_if_needed(
        iterate_task: DbTask,
        preflight_context: _IterateBackgroundPreflightContext | None,
    ) -> dict[str, Any] | None:
        if manual_iterate and iterate_task.status == "failed" and use_resume:
            _emit_manual_resume_override_warnings(
                iterate_task,
                _decide_failed_iterate_resume_start(iterate_task),
            )
            return None
        if iterate_task.status != "completed" or preflight_context is None:
            return None
        initial_action = determine_next_action(
            engine_config,
            store,
            preflight_context.git_runtime,
            iterate_task,
            preflight_context.target_branch,
            max_resume_attempts=effective_max_resume_attempts,
        )
        if initial_action.get("type") != "improve":
            return initial_action
        review_task = initial_action.get("review_task")
        if not isinstance(review_task, DbTask):
            return initial_action
        assert iterate_task.id is not None
        assert review_task.id is not None
        improve_action, failed_improve, improve_decision = resolve_improve_action(
            store,
            iterate_task.id,
            review_task.id,
            max_resume_attempts=effective_max_resume_attempts,
        )
        if (
            manual_iterate
            and improve_action == "manual_review"
            and failed_improve is not None
            and improve_decision is not None
        ):
            manual_override = _emit_manual_resume_override_warnings(failed_improve, improve_decision)
            if manual_override is not None:
                failed_improve, improve_decision = manual_override
                improve_action = "resume"
        attention_result = build_improve_needs_attention_result(
            store=store,
            impl_task=iterate_task,
            review_task=review_task,
            improve_mode=improve_action,
            failed_improve=failed_improve,
            improve_decision=improve_decision,
            max_resume_attempts=effective_max_resume_attempts,
        )
        if attention_result is not None and failed_improve is not None:
            initial_action = dict(initial_action)
            initial_action["_improve_attention_result"] = attention_result
            initial_action["_improve_failed_task"] = failed_improve
            initial_action["_improve_decision"] = improve_decision
            return initial_action
        return initial_action

    def _maybe_surface_background_iterate_preflight_decision(
        iterate_task: DbTask,
        preflight_context: _IterateBackgroundPreflightContext | None,
    ) -> int | None:
        if iterate_task.status != "pending" and preflight_context is not None:
            resolved_merge_state = _resolve_iterate_merge_state_for_current_target(
                store=store,
                impl_task=iterate_task,
                git_runtime=preflight_context.git_runtime,
                target_branch=preflight_context.target_branch,
            )
            if resolved_merge_state == "merged":
                try:
                    _reconcile_iterate_already_merged(
                        store=store,
                        impl_task=iterate_task,
                        git_runtime=preflight_context.git_runtime,
                        target_branch=preflight_context.target_branch,
                    )
                except Exception as exc:
                    task_label = iterate_task.id or "<unknown>"
                    print_phase1_message(
                        args,
                        f"Error: failed to reconcile already-merged implementation {task_label}: {exc}",
                    )
                    return 1
            terminal_message = _format_iterate_terminal_merge_state_message(
                store=store,
                requested_impl_task=requested_impl_task,
                iterate_task=iterate_task,
                resolved_from_failed_ancestor=resolved_from_failed_ancestor,
                merge_state=resolved_merge_state,
            )
            if terminal_message is not None:
                print(terminal_message)
                return 0
        try:
            initial_action = _warn_manual_background_iterate_override_if_needed(
                iterate_task,
                preflight_context,
            )
        except Exception as exc:
            task_label = iterate_task.id or "<unknown>"
            print_phase1_message(
                args,
                f"Error: failed to determine iterate background start for task {task_label}: {exc}",
            )
            return 1
        if iterate_task.status != "completed":
            return None
        assert preflight_context is not None
        if initial_action is None:
            try:
                initial_action = determine_next_action(
                    engine_config,
                    store,
                    preflight_context.git_runtime,
                    iterate_task,
                    preflight_context.target_branch,
                    max_resume_attempts=effective_max_resume_attempts,
                )
            except Exception as exc:
                task_label = iterate_task.id or "<unknown>"
                print_phase1_message(
                    args,
                    f"Error: failed to determine iterate background start for task {task_label}: {exc}",
                )
                return 1

        action_type = initial_action["type"]
        if action_type == "improve":
            attention_result = initial_action.get("_improve_attention_result")
            if attention_result is None:
                return None
            failed_improve = initial_action.get("_improve_failed_task")
            if not isinstance(failed_improve, DbTask):
                return None
            print("Next action: improve")
            attention = resolve_execution_needs_attention(iterate_task, attention_result)
            if attention is not None:
                print(
                    f"{NEEDS_ATTENTION_LABEL}: "
                    f"{format_needs_attention_entry_for_display(attention.task, action=attention.action, prefix=len(attention.task.id or '') + 4)}"
                )
                if needs_attention_recommends_fix(attention.action):
                    print(f"Recommended next step: uv run gza fix {iterate_task.id}")
            else:
                print(f"Iterate blocked: {attention_result.message.removeprefix('SKIP: ')}")
            return 3
        if action_type in WORKER_CONSUMING_ACTIONS:
            return None

        if action_type == "merge_with_followups":
            return None

        print(f"Next action: {action_type}")
        if action_type == "merge":
            print(f"No remaining iterate action: implementation {iterate_task.id} is ready to merge.")
            return 0
        if action_type == "wait_review":
            print("Iterate waiting: review_in_progress. Existing task is already in progress.")
            return 3
        if action_type == "wait_improve":
            print("Iterate waiting: improve_in_progress. Existing task is already in progress.")
            return 3
        if action_type == "max_cycles_reached":
            subject_task = resolve_subject_task(store, initial_action, fallback_task=iterate_task)
            print(
                f"{NEEDS_ATTENTION_LABEL}: "
                f"{format_needs_attention_entry_for_display(subject_task, action=initial_action, prefix=len(subject_task.id or '') + 4)}"
            )
            assert iterate_task.id is not None
            completed_review_cycles = count_completed_review_cycles(store, iterate_task.id)
            print(
                "Review-iteration accounting: "
                f"completed={completed_review_cycles}, "
                f"max_review_cycles={engine_config.max_review_cycles}, "
                "consumed_this_invocation=0"
            )
            print(f"Recommended next step: uv run gza fix {iterate_task.id}")
            return 3
        if classify_advance_action(initial_action) == "needs_attention":
            subject_task = resolve_subject_task(store, initial_action, fallback_task=iterate_task)
            print(
                f"{NEEDS_ATTENTION_LABEL}: "
                f"{format_needs_attention_entry_for_display(subject_task, action=initial_action, prefix=len(subject_task.id or '') + 4)}"
            )
            if needs_attention_recommends_fix(initial_action):
                print(f"Recommended next step: uv run gza fix {iterate_task.id}")
            return 3
        print(f"Iterate blocked: {_iterate_action_description(initial_action)}")
        return 3

    def _print_failed_recovery_attention_and_return(
        failed_task: DbTask,
        decision: FailedRecoveryDecision,
        *,
        fix_task_id: str,
    ) -> int | None:
        attention_result = build_failed_recovery_needs_attention_result(
            store=store,
            failed_task=failed_task,
            recovery_decision=decision,
            max_resume_attempts=effective_max_resume_attempts,
        )
        if attention_result is None:
            return None
        attention = resolve_execution_needs_attention(failed_task, attention_result)
        if attention is None:
            return None
        print(
            f"{NEEDS_ATTENTION_LABEL}: "
            f"{format_needs_attention_entry_for_display(attention.task, action=attention.action, prefix=len(attention.task.id or '') + 4)}"
        )
        if needs_attention_recommends_fix(attention.action):
            print(f"Recommended next step: uv run gza fix {fix_task_id}")
        return 3

    def _resolve_prepared_iterate_start() -> tuple[_PreparedIterateStart | None, int | None]:
        prepared_task, prepared_phase, prepared_seed_rc = _resolve_prepared_iterate_seed()
        if prepared_seed_rc is not None:
            return None, prepared_seed_rc
        if prepared_task is None:
            return None, None
        assert prepared_phase is not None
        prepared_action_type = getattr(args, "prepared_action_type", None)
        prepared_review_task_id = getattr(args, "prepared_review_task_id", None)
        return (
            _PreparedIterateStart(
                task=prepared_task,
                initial_resume=bool(getattr(args, "prepared_resume", False)),
                phase=prepared_phase,
                action_type=prepared_action_type if isinstance(prepared_action_type, str) else None,
                review_task_id=prepared_review_task_id if isinstance(prepared_review_task_id, str) else None,
            ),
            None,
        )

    def _reserve_iterate_launch(*, emit_error: bool = True) -> LaunchPermit | Literal[False] | None:
        if not isinstance(getattr(config, "max_concurrent", None), int):
            return None
        try:
            return launch_permit(config, store)
        except MaxConcurrentTasksError as exc:
            if emit_error:
                print_phase1_message(args, f"Error: {exc}")
            return False

    def _prepare_reserved_iterate_task(
        task: DbTask,
        *,
        permit: LaunchPermit | None,
        rollback_on_failure: bool,
    ) -> DbTask | None:
        prepared_task = _prepare_task_for_immediate_execution(
            config,
            task,
            rollback_on_failure=rollback_on_failure,
        )
        if prepared_task is None:
            if permit is not None:
                permit.release()
            return None
        if permit is not None and prepared_task.id is not None:
            reserve_task_launch_permit(str(prepared_task.id), permit)
        return prepared_task

    def _prepare_background_iterate_start(
        iterate_task: DbTask,
        preflight_context: _IterateBackgroundPreflightContext | None,
    ) -> tuple[_PreparedIterateStart | None, int | None]:

        if dry_run:
            return None, None
        if iterate_task.status == "pending":
            pending_recovery_mode = resolve_pending_recovery_execution_mode(iterate_task)
            permit = _reserve_iterate_launch()
            if permit is False:
                return None, 1
            prepared_task = _prepare_reserved_iterate_task(iterate_task, permit=permit, rollback_on_failure=False)
            if prepared_task is None:
                return None, 1
            return (
                _PreparedIterateStart(
                    task=prepared_task,
                    initial_resume=pending_recovery_mode == "resume",
                    phase="preloop",
                ),
                None,
            )
        if iterate_task.status == "completed":
            if preflight_context is None:
                task_label = iterate_task.id or "<unknown>"
                print_phase1_message(
                    args,
                    f"Error: missing iterate background preflight context for task {task_label}.",
                )
                return None, 1

            try:
                initial_action = determine_next_action(
                    config,
                    store,
                    preflight_context.git_runtime,
                    iterate_task,
                    preflight_context.target_branch,
                    max_resume_attempts=effective_max_resume_attempts,
                )
            except Exception as exc:
                task_label = iterate_task.id or "<unknown>"
                print_phase1_message(
                    args,
                    f"Error: failed to determine iterate background start for task {task_label}: {exc}",
                )
                return None, 1
            action_type = initial_action["type"]

            if action_type == "create_review":
                rollback_on_failure = True
                permit = _reserve_iterate_launch()
                if permit is False:
                    return None, 1
                try:
                    action_task = _create_review_task(store, iterate_task, trigger_source="auto-recovery")
                except DuplicateReviewError as exc:
                    action_task = exc.active_review
                    if action_task.status != "pending":
                        if isinstance(permit, LaunchPermit):
                            permit.release()
                        return None, None
                    rollback_on_failure = False
                except ValueError as exc:
                    if isinstance(permit, LaunchPermit):
                        permit.release()
                    print_phase1_message(args, f"  Error creating review: {exc}")
                    return None, 1
                prepared_task = _prepare_reserved_iterate_task(
                    action_task,
                    permit=permit,
                    rollback_on_failure=rollback_on_failure,
                )
                if prepared_task is None:
                    return None, 1
                return (
                    _PreparedIterateStart(
                        task=prepared_task,
                        initial_resume=False,
                        phase="iteration",
                        action_type="create_review",
                    ),
                    None,
                )

            if action_type == "run_review":
                review_task = initial_action.get("review_task")
                if not isinstance(review_task, DbTask):
                    return None, None
                permit = _reserve_iterate_launch()
                if permit is False:
                    return None, 1
                prepared_task = _prepare_reserved_iterate_task(review_task, permit=permit, rollback_on_failure=False)
                if prepared_task is None:
                    return None, 1
                return (
                    _PreparedIterateStart(
                        task=prepared_task,
                        initial_resume=False,
                        phase="iteration",
                        action_type="run_review",
                    ),
                    None,
                )

            if action_type == "create_review_adjudication":
                review_task = initial_action.get("review_task")
                candidate = initial_action.get("review_blocker_adjudication_candidate")
                if not isinstance(review_task, DbTask) or candidate is None:
                    return None, None
                permit = _reserve_iterate_launch()
                if permit is False:
                    return None, 1
                action_task = _create_review_adjudication_task(
                    store,
                    iterate_task,
                    review_task,
                    candidate.finding,
                    dispute_metadata=dict(candidate.dispute_metadata),
                    trigger_source="auto-recovery",
                )
                prepared_task = _prepare_reserved_iterate_task(
                    action_task,
                    permit=permit,
                    rollback_on_failure=True,
                )
                if prepared_task is None:
                    return None, 1
                return (
                    _PreparedIterateStart(
                        task=prepared_task,
                        initial_resume=False,
                        phase="iteration",
                        action_type="create_review_adjudication",
                    ),
                    None,
                )

            if action_type == "run_review_adjudication":
                adjudication_task = initial_action.get("review_adjudication_task")
                if not isinstance(adjudication_task, DbTask):
                    return None, None
                permit = _reserve_iterate_launch()
                if permit is False:
                    return None, 1
                prepared_task = _prepare_reserved_iterate_task(
                    adjudication_task,
                    permit=permit,
                    rollback_on_failure=False,
                )
                if prepared_task is None:
                    return None, 1
                return (
                    _PreparedIterateStart(
                        task=prepared_task,
                        initial_resume=False,
                        phase="iteration",
                        action_type="run_review_adjudication",
                    ),
                    None,
                )

            if action_type == "needs_rebase":
                if not iterate_task.branch:
                    return None, None
                if _iterate_rebase_target_already_merged(
                    store=store,
                    git_runtime=preflight_context.git_runtime,
                    task=iterate_task,
                    target_branch=preflight_context.target_branch,
                ):
                    return None, None
                assert iterate_task.id is not None
                permit = _reserve_iterate_launch()
                if permit is False:
                    return None, 1
                action_task = _create_rebase_task(
                    store,
                    iterate_task.id,
                    iterate_task.branch,
                    preflight_context.target_branch,
                    trigger_source="auto-recovery",
                )
                prepared_task = _prepare_reserved_iterate_task(action_task, permit=permit, rollback_on_failure=True)
                if prepared_task is None:
                    return None, 1
                return (
                    _PreparedIterateStart(
                        task=prepared_task,
                        initial_resume=False,
                        phase="iteration",
                        action_type="needs_rebase",
                    ),
                    None,
                )

            if action_type == "improve":
                review_task = initial_action.get("review_task")
                if not isinstance(review_task, DbTask):
                    return None, None
                assert iterate_task.id is not None
                assert review_task.id is not None
                improve_action, failed_improve, improve_decision = resolve_improve_action(
                    store,
                    iterate_task.id,
                    review_task.id,
                    max_resume_attempts=effective_max_resume_attempts,
                )
                if (
                    manual_iterate
                    and improve_action == "manual_review"
                    and failed_improve is not None
                    and improve_decision is not None
                ):
                    manual_override = _emit_manual_resume_override_warnings(
                        failed_improve,
                        improve_decision,
                    )
                    if manual_override is not None:
                        failed_improve, improve_decision = manual_override
                        improve_action = "resume"
                attention_result = build_improve_needs_attention_result(
                    store=store,
                    impl_task=iterate_task,
                    review_task=review_task,
                    improve_mode=improve_action,
                    failed_improve=failed_improve,
                    improve_decision=improve_decision,
                    max_resume_attempts=effective_max_resume_attempts,
                )
                if attention_result is not None:
                    return None, None
                initial_resume = False
                permit = _reserve_iterate_launch()
                if permit is False:
                    return None, 1
                if improve_action == "resume" and failed_improve is not None:
                    action_task = _create_resume_task(store, failed_improve, trigger_source="auto-recovery")
                    rollback_on_failure = True
                    initial_resume = True
                elif improve_action == "retry" and failed_improve is not None:
                    action_task = _create_retry_task(store, failed_improve, trigger_source="auto-recovery")
                    rollback_on_failure = True
                else:
                    try:
                        action_task = _create_improve_task(
                            store,
                            iterate_task,
                            review_task,
                            trigger_source="auto-recovery",
                        )
                    except ValueError as exc:
                        if isinstance(permit, LaunchPermit):
                            permit.release()
                        print_phase1_message(args, f"  Error creating improve task: {exc}")
                        return None, 1
                    rollback_on_failure = True
                prepared_task = _prepare_reserved_iterate_task(
                    action_task,
                    permit=permit,
                    rollback_on_failure=rollback_on_failure,
                )
                if prepared_task is None:
                    return None, 1
                return (
                    _PreparedIterateStart(
                        task=prepared_task,
                        initial_resume=initial_resume,
                        phase="iteration",
                        action_type="improve",
                        review_task_id=review_task.id,
                    ),
                    None,
                )

            if action_type == "run_improve":
                improve_task = initial_action.get("improve_task")
                if not isinstance(improve_task, DbTask):
                    return None, None
                review_task_id = None
                if improve_task.depends_on is not None:
                    review_task_id = str(improve_task.depends_on)
                permit = _reserve_iterate_launch()
                if permit is False:
                    return None, 1
                prepared_task = _prepare_reserved_iterate_task(improve_task, permit=permit, rollback_on_failure=False)
                if prepared_task is None:
                    return None, 1
                return (
                    _PreparedIterateStart(
                        task=prepared_task,
                        initial_resume=False,
                        phase="iteration",
                        action_type="run_improve",
                        review_task_id=review_task_id,
                    ),
                    None,
                )

            return None, None
        if iterate_task.status != "failed":
            return None, None
        if use_resume:
            resume_start, resume_blocked = _resolve_failed_iterate_resume_start(
                iterate_task,
                emit_override_warnings=False,
            )
            if resume_start is None:
                if resume_blocked is not None:
                    blocked_task, resume_blocked_decision = resume_blocked
                    exit_code = _print_failed_recovery_attention_and_return(
                        blocked_task,
                        resume_blocked_decision,
                        fix_task_id=impl_task_id,
                    )
                    if exit_code is not None:
                        return None, exit_code
                    print_phase1_message(
                        args,
                        f"Error: Cannot resume failed implementation {iterate_task.id}: "
                        f"{resume_blocked_decision.reason_text}.",
                    )
                return None, 1
            run_start_task, decision = resume_start
            permit = _reserve_iterate_launch()
            if permit is False:
                return None, 1
            prepared_task = _prepare_reserved_iterate_task(
                run_start_task,
                permit=permit,
                rollback_on_failure=not decision.reuse_existing,
            )
            if prepared_task is None:
                return None, 1
            return (
                _PreparedIterateStart(
                    task=prepared_task,
                    initial_resume=True,
                    phase="preloop",
                ),
                None,
            )
        if use_retry:
            permit = _reserve_iterate_launch()
            if permit is False:
                return None, 1
            run_start_task = _create_retry_task(store, iterate_task, trigger_source="manual")
            prepared_task = _prepare_reserved_iterate_task(
                run_start_task,
                permit=permit,
                rollback_on_failure=True,
            )
            if prepared_task is None:
                return None, 1
            return (
                _PreparedIterateStart(
                    task=prepared_task,
                    initial_resume=False,
                    phase="preloop",
                ),
                None,
            )
        return None, None

    def _print_iterate_failed_start_message(
        failed_task: DbTask,
        run_start_task: DbTask,
        *,
        resume_start: bool,
    ) -> None:
        assert failed_task.id is not None
        assert run_start_task.id is not None
        if resume_start:
            if run_start_task.based_on is not None and str(run_start_task.based_on) != str(failed_task.id):
                print(
                    f"Resuming failed implementation {failed_task.id} via newer recovery descendant "
                    f"{run_start_task.based_on} as {run_start_task.id}..."
                )
                return
            print(f"Resuming failed implementation {failed_task.id} as {run_start_task.id}...")
            return
        print(f"Retrying failed implementation {failed_task.id} as {run_start_task.id}...")

    prepared_start, prepared_start_rc = _resolve_prepared_iterate_start()
    if prepared_start_rc is not None:
        return prepared_start_rc

    # Handle background mode: re-exec this command as a detached process.
    if background:
        background_preflight_context, background_preflight_rc = (
            _prepare_iterate_background_preflight_context(impl_task)
        )
        if background_preflight_rc is not None:
            return background_preflight_rc
        preflight_result = _maybe_surface_background_iterate_preflight_decision(
            impl_task,
            background_preflight_context,
        )
        if preflight_result is not None:
            return preflight_result
        prepared_background_start, background_prepare_rc = _prepare_background_iterate_start(
            impl_task,
            background_preflight_context,
        )
        if background_prepare_rc is not None:
            return background_prepare_rc
        return _spawn_background_iterate(
            args,
            config,
            impl_task,
            max_iterations=max_iterations,
            dry_run=dry_run,
            prepared_task_id=prepared_background_start.task.id if prepared_background_start is not None else None,
            prepared_resume=(
                prepared_background_start.initial_resume if prepared_background_start is not None else False
            ),
            prepared_phase=prepared_background_start.phase if prepared_background_start is not None else None,
            prepared_action_type=(
                prepared_background_start.action_type if prepared_background_start is not None else None
            ),
            prepared_review_task_id=(
                prepared_background_start.review_task_id if prepared_background_start is not None else None
            ),
        )

    try:
        git_runtime: Any = Git(config.project_dir)
        target_branch = git_runtime.current_branch()
    except Exception as exc:
        print(f"Error: failed to initialize git runtime for iterate: {exc}")
        return 1

    resolved_merge_state = _resolve_iterate_merge_state_for_current_target(
        store=store,
        impl_task=impl_task,
        git_runtime=git_runtime,
        target_branch=target_branch,
    )
    if resolved_merge_state == "merged":
        try:
            _reconcile_iterate_already_merged(
                store=store,
                impl_task=impl_task,
                git_runtime=git_runtime,
                target_branch=target_branch,
            )
        except Exception as exc:
            task_label = impl_task.id or "<unknown>"
            print(f"Error: failed to reconcile already-merged implementation {task_label}: {exc}")
            return 1
    terminal_message = _format_iterate_terminal_merge_state_message(
        store=store,
        requested_impl_task=requested_impl_task,
        iterate_task=impl_task,
        resolved_from_failed_ancestor=resolved_from_failed_ancestor,
        merge_state=resolved_merge_state,
    )
    if terminal_message is not None:
        print(terminal_message)
        return 0

    if prepared_start is not None and impl_task.status == "pending" and prepared_start.task.id == impl_task.id:
        impl_task = prepared_start.task

    # If the task is pending, run it first before entering the loop.
    if impl_task.status == "pending":
        if dry_run:
            print(f"[dry-run] Would run pending implementation {impl_task.id} then iterate (max {max_iterations} iterations)")
            return 0

        print(f"Running pending implementation {impl_task.id}...")
        pending_recovery_mode = resolve_pending_recovery_execution_mode(impl_task)
        impl_task, rc, terminal_skip_decision = _run_iterate_task_with_recovery(
            args=args,
            config=config,
            store=store,
            task_to_run=impl_task,
            max_resume_attempts=effective_max_resume_attempts,
            initial_resume=pending_recovery_mode == "resume",
        )
        if rc != 0:
            if terminal_skip_decision is not None:
                exit_code = _print_failed_recovery_attention_and_return(
                    impl_task,
                    terminal_skip_decision,
                    fix_task_id=impl_task_id,
                )
                if exit_code is not None:
                    return exit_code
            print(f"Implementation {impl_task.id} failed (exit code {rc})")
            return 1
        assert impl_task.id is not None
        if impl_task.status == "failed":
            print(f"Implementation {impl_task.id} failed, cannot continue iteration.")
            return 1

    # If the task is failed, resume or retry it first.
    if impl_task.status == "failed":
        if prepared_start is not None:
            run_start_task = prepared_start.task
            _print_iterate_failed_start_message(
                impl_task,
                run_start_task,
                resume_start=prepared_start.initial_resume,
            )
            impl_task, rc, terminal_skip_decision = _run_iterate_task_with_recovery(
                args=args,
                config=config,
                store=store,
                task_to_run=run_start_task,
                max_resume_attempts=effective_max_resume_attempts,
                initial_resume=prepared_start.initial_resume,
            )
        elif use_resume:
            if dry_run:
                print(f"[dry-run] Would resume failed implementation {impl_task.id} then iterate (max {max_iterations} iterations)")
                return 0
            resume_start, resume_blocked = _resolve_failed_iterate_resume_start(impl_task)
            if resume_start is None:
                if resume_blocked is not None:
                    blocked_task, resume_blocked_decision = resume_blocked
                    exit_code = _print_failed_recovery_attention_and_return(
                        blocked_task,
                        resume_blocked_decision,
                        fix_task_id=impl_task_id,
                    )
                    if exit_code is not None:
                        return exit_code
                    print(
                        f"Error: Cannot resume failed implementation {impl_task.id}: "
                        f"{resume_blocked_decision.reason_text}."
                    )
                return 1
            run_start_task, decision = resume_start
            permit: LaunchPermit | None = None
            if isinstance(getattr(config, "max_concurrent", None), int):
                try:
                    permit = launch_permit(config, store)
                except MaxConcurrentTasksError as exc:
                    print(f"Error: {exc}")
                    return 1
            prepared_run_start = (
                _prepare_task_for_reserved_launch(
                    config,
                    run_start_task,
                    permit=permit,
                    rollback_on_failure=not decision.reuse_existing,
                )
                if permit is not None
                else run_start_task
            )
            if prepared_run_start is None:
                return 1
            run_start_task = prepared_run_start
            _print_iterate_failed_start_message(
                impl_task,
                run_start_task,
                resume_start=True,
            )
            impl_task, rc, terminal_skip_decision = _run_iterate_task_with_recovery(
                args=args,
                config=config,
                store=store,
                task_to_run=run_start_task,
                max_resume_attempts=effective_max_resume_attempts,
                initial_resume=True,
            )
        else:
            # --retry
            if dry_run:
                print(f"[dry-run] Would retry failed implementation {impl_task.id} then iterate (max {max_iterations} iterations)")
                return 0
            permit = None
            if isinstance(getattr(config, "max_concurrent", None), int):
                try:
                    permit = launch_permit(config, store)
                except MaxConcurrentTasksError as exc:
                    print(f"Error: {exc}")
                    return 1
            run_start_task = _create_retry_task(store, impl_task, trigger_source="manual")
            prepared_run_start = (
                _prepare_task_for_reserved_launch(
                    config,
                    run_start_task,
                    permit=permit,
                    rollback_on_failure=True,
                )
                if permit is not None
                else run_start_task
            )
            if prepared_run_start is None:
                return 1
            run_start_task = prepared_run_start
            _print_iterate_failed_start_message(
                impl_task,
                run_start_task,
                resume_start=False,
            )
            impl_task, rc, terminal_skip_decision = _run_iterate_task_with_recovery(
                args=args,
                config=config,
                store=store,
                task_to_run=run_start_task,
                max_resume_attempts=effective_max_resume_attempts,
            )

        if rc != 0:
            if terminal_skip_decision is not None:
                exit_code = _print_failed_recovery_attention_and_return(
                    impl_task,
                    terminal_skip_decision,
                    fix_task_id=impl_task_id,
                )
                if exit_code is not None:
                    return exit_code
            action_label = "Resume" if use_resume else "Retry"
            print(f"{action_label} of {impl_task_id} failed (exit code {rc})")
            return 1

        # The new task is now the impl task for the loop
        assert impl_task.id is not None
        if impl_task.status == "failed":
            action_label = "Resume" if use_resume else "Retry"
            print(f"{action_label} of {impl_task_id} failed, cannot continue iteration.")
            return 1

    assert impl_task.id is not None
    prepared_iteration_start = prepared_start if prepared_start is not None and prepared_start.phase == "iteration" else None

    max_resume_attempts = _int_config(
        getattr(config, "max_resume_attempts", None),
        DEFAULT_MAX_RESUME_ATTEMPTS,
    )
    engine_config = _AdvanceEngineConfigAdapter(
        project_dir=config.project_dir,
        require_review_before_merge=bool(getattr(config, "require_review_before_merge", True)),
        advance_create_reviews=bool(getattr(config, "advance_create_reviews", True)),
        max_review_cycles=_int_config(getattr(config, "max_review_cycles", None), 3),
        max_noop_improve_cycles=_int_config(getattr(config, "max_noop_improve_cycles", None), 1),
        max_resume_attempts=max_resume_attempts,
        max_failed_closing_review_retries=_int_config(
            getattr(config, "max_failed_closing_review_retries", None),
            DEFAULT_MAX_FAILED_CLOSING_REVIEW_RETRIES,
        ),
    )
    if prepared_iteration_start is not None:
        initial_action = {"type": prepared_iteration_start.action_type or "iteration"}
    else:
        initial_action = determine_next_action(
            engine_config,
            store,
            git_runtime,
            impl_task,
            target_branch,
            max_resume_attempts=max_resume_attempts,
        )
    initial_action_type = initial_action["type"]
    initial_action_description = initial_action.get("description")
    if not isinstance(initial_action_description, str) or not initial_action_description:
        initial_action_description = initial_action_type

    iteration_actions = {
        "create_review",
        "run_review",
        "create_review_adjudication",
        "run_review_adjudication",
        "improve",
        "run_improve",
    }

    if dry_run:
        print(f"[dry-run] Would iterate implementation {impl_task.id} (max {max_iterations} iterations)")
        if initial_action_type in iteration_actions:
            print(f"[dry-run] First iteration 1/{max_iterations} action: {initial_action_type} - {initial_action_description}")
        else:
            print(f"[dry-run] First next action: {initial_action_type} - {initial_action_description}")
        return 0
    print(f"Iterating implementation {impl_task.id} (max {max_iterations} iterations)...")
    impl_task_key = impl_task.id
    assert impl_task_key is not None

    def _append_summary_row(
        rows: list[IterateSummaryRow],
        *,
        iteration_index: int,
        task_type: str,
        task: DbTask | None,
        verdict: str | None = None,
        status: str | None = None,
        failure_reason: str | None = None,
    ) -> None:
        _append_iterate_summary_row(
            store,
            rows,
            iteration_index=iteration_index,
            task_type=task_type,
            task=task,
            verdict=verdict,
            status=status,
            failure_reason=failure_reason,
        )

    def _task_sort_key(task: DbTask) -> tuple[datetime, int]:
        return (task.created_at or datetime.min, task_id_numeric_key(task.id))

    def _latest_with_status(tasks: list[DbTask], status: str) -> DbTask | None:
        matching = [task for task in tasks if task.status == status]
        if not matching:
            return None
        return max(matching, key=_task_sort_key)

    def _latest_completed_review() -> DbTask | None:
        reviews = [r for r in store.get_reviews_for_task(impl_task_key) if r.status == "completed"]
        if not reviews:
            return None
        return max(
            reviews,
            key=lambda review: (
                review.completed_at or datetime.min,
                review.created_at or datetime.min,
                review.id or "",
            ),
        )

    def _materialize_followup_tasks(
        review_task: DbTask,
        *,
        iteration_index: int,
        findings: tuple[Any, ...] | None = None,
    ) -> bool:
        followup_findings = findings
        if followup_findings is None:
            parsed_report = get_review_report(config.project_dir, review_task)
            followup_findings = tuple(
                finding for finding in parsed_report.findings if finding.severity == "FOLLOWUP"
            )
        if not followup_findings:
            return False

        assert impl_task is not None
        created_tasks, reused_tasks = _create_or_reuse_followup_tasks(
            store,
            review_task=review_task,
            impl_task=impl_task,
            findings=followup_findings,
            trigger_source="auto-recovery",
        )
        for followup_task in created_tasks:
            _append_summary_row(
                summary_rows,
                iteration_index=iteration_index,
                task_type="followup",
                task=followup_task,
                status="created",
            )
        for followup_task in reused_tasks:
            _append_summary_row(
                summary_rows,
                iteration_index=iteration_index,
                task_type="followup",
                task=followup_task,
                status="reused",
            )
        return True

    iterate_started_at = time.monotonic()
    summary_rows: list[IterateSummaryRow] = []
    final_status = "maxed_out"
    final_stop_reason = "max_iterations"
    final_attention_action: dict[str, Any] | None = None
    final_attention_task: DbTask | None = None
    final_non_attention_stop_message: str | None = None
    iteration = 0
    starting_completed_review_cycles = count_completed_review_cycles(store, impl_task_key)
    max_resume_attempts = _int_config(
        getattr(config, "max_resume_attempts", None),
        DEFAULT_MAX_RESUME_ATTEMPTS,
    )
    engine_config = _AdvanceEngineConfigAdapter(
        project_dir=config.project_dir,
        require_review_before_merge=bool(getattr(config, "require_review_before_merge", True)),
        advance_create_reviews=bool(getattr(config, "advance_create_reviews", True)),
        max_review_cycles=_int_config(getattr(config, "max_review_cycles", None), 3),
        max_noop_improve_cycles=_int_config(getattr(config, "max_noop_improve_cycles", None), 1),
        max_resume_attempts=max_resume_attempts,
        max_failed_closing_review_retries=_int_config(
            getattr(config, "max_failed_closing_review_retries", None),
            DEFAULT_MAX_FAILED_CLOSING_REVIEW_RETRIES,
        ),
    )

    def _current_impl_task() -> DbTask:
        assert impl_task is not None
        return impl_task

    def _latest_completed_code_change() -> DbTask | None:
        current_impl_task = _current_impl_task()
        if _latest_completed_review() is None:
            return current_impl_task if current_impl_task.status == "completed" else None
        completed_changes = [
            candidate
            for candidate in get_code_changing_descendants_for_root(store, current_impl_task)
            if candidate.status == "completed"
        ]
        if not completed_changes:
            return None
        return max(
            completed_changes,
            key=lambda candidate: (
                candidate.completed_at or candidate.created_at or datetime.min,
                task_id_numeric_key(candidate.id),
            ),
        )

    def _resolve_forced_closing_review_action() -> dict[str, Any] | None:
        current_impl_task = _current_impl_task()
        return resolve_closing_review_action(
            task=current_impl_task,
            reviews=store.get_reviews_for_task(impl_task_key),
            latest_completed_review=_latest_completed_review(),
            latest_completed_code_change=_latest_completed_code_change(),
        )

    def _run_forced_closing_review(iteration_index: int) -> bool:
        nonlocal final_status, final_stop_reason, final_attention_action, final_attention_task
        closing_action = _resolve_forced_closing_review_action()
        if closing_action is None:
            return False

        action_type = closing_action["type"]
        if action_type == "wait_review":
            review_task = closing_action.get("review_task")
            print("\nClosing review already in progress before termination.")
            final_status = "blocked"
            final_stop_reason = "review_in_progress"
            if isinstance(review_task, DbTask):
                _append_summary_row(
                    summary_rows,
                    iteration_index=iteration_index,
                    task_type="review",
                    task=review_task,
                    status="in_progress",
                )
            else:
                _append_summary_row(
                    summary_rows,
                    iteration_index=iteration_index,
                    task_type="review",
                    task=None,
                    status="in_progress",
                )
            return True

        if action_type not in {"create_review", "run_review"}:
            return False

        print("\nClosing review required before termination.")
        current_impl_task = _current_impl_task()
        action_task: DbTask | None = None
        if action_type == "create_review":
            try:
                action_task = _create_review_task(store, current_impl_task, trigger_source="auto-recovery")
            except DuplicateReviewError as e:
                action_task = e.active_review
                assert action_task.id is not None
                if action_task.status == "in_progress":
                    final_status = "blocked"
                    final_stop_reason = "review_in_progress"
                    _append_summary_row(
                        summary_rows,
                        iteration_index=iteration_index,
                        task_type="review",
                        task=action_task,
                        status="in_progress",
                    )
                    return True
                if action_task.status != "pending":
                    final_status = "blocked"
                    final_stop_reason = "review_failed"
                    _append_summary_row(
                        summary_rows,
                        iteration_index=iteration_index,
                        task_type="review",
                        task=action_task,
                        status="failed",
                    )
                    return True
            except ValueError as e:
                final_status = "blocked"
                final_stop_reason = "review_failed"
                _append_summary_row(
                    summary_rows,
                    iteration_index=iteration_index,
                    task_type="review",
                    task=None,
                    status="failed",
                    failure_reason=str(e),
                )
                return True
        else:
            maybe_review_task = closing_action.get("review_task")
            if isinstance(maybe_review_task, DbTask):
                action_task = maybe_review_task

        if action_task is None:
            return False

        assert action_task.id is not None
        action_task, rc, terminal_skip_decision = _run_iterate_task_with_recovery(
            args=args,
            config=config,
            store=store,
            task_to_run=action_task,
            max_resume_attempts=effective_max_resume_attempts,
        )
        if rc != 0:
            final_status = "blocked"
            attention_result = None
            if terminal_skip_decision is not None:
                attention_result = build_failed_recovery_needs_attention_result(
                    store=store,
                    failed_task=action_task,
                    recovery_decision=terminal_skip_decision,
                    max_resume_attempts=effective_max_resume_attempts,
                )
            if attention_result is not None:
                attention = resolve_execution_needs_attention(action_task, attention_result)
                if attention is not None:
                    final_attention_action = attention.action
                    final_attention_task = attention.task
            final_stop_reason = "review_failed"
            _append_summary_row(
                summary_rows,
                iteration_index=iteration_index,
                task_type="review",
                task=action_task,
                status="failed",
                failure_reason=f"exit code {rc}",
            )
            return True

        assert action_task.id is not None
        action_task = store.get(action_task.id) or action_task
        verdict = get_review_verdict(config, action_task)
        _append_summary_row(
            summary_rows,
            iteration_index=iteration_index,
            task_type="review",
            task=action_task,
            verdict=verdict,
        )
        if verdict == "APPROVED_WITH_FOLLOWUPS":
            materialized = _materialize_followup_tasks(action_task, iteration_index=iteration_index)
            if not materialized:
                final_status = "blocked"
                final_stop_reason = "needs_discussion"
                return True
            final_status = "approved"
            final_stop_reason = "approved_with_followups"
            return True
        if verdict == "APPROVED":
            final_status = "approved"
            final_stop_reason = "approved"
            return True
        if verdict in {"NEEDS_DISCUSSION", None}:
            final_status = "blocked"
            final_stop_reason = "needs_discussion" if verdict == "NEEDS_DISCUSSION" else "no_verdict"
            return True
        final_status = "blocked"
        final_stop_reason = "closing_review_completed"
        return True

    while iteration < max_iterations:
        if prepared_iteration_start is not None:
            action = {
                "type": prepared_iteration_start.action_type,
                "_prepared_task": prepared_iteration_start.task,
                "_prepared_initial_resume": prepared_iteration_start.initial_resume,
                "_prepared_review_task_id": prepared_iteration_start.review_task_id,
            }
            prepared_iteration_start = None
        else:
            action = determine_next_action(
                engine_config,
                store,
                git_runtime,
                impl_task,
                target_branch,
                max_resume_attempts=max_resume_attempts,
            )
        action_type = action["type"]
        assert isinstance(action_type, str)
        if action_type in iteration_actions:
            print(f"\nIteration {iteration + 1}/{max_iterations}: {action_type}")
        else:
            print(f"\nNext action: {action_type}")

        if action_type in {"merge", "merge_with_followups"}:
            final_status = "merge_ready"
            final_stop_reason = "merge_ready"
            maybe_review_verdict: str | None = None
            maybe_review = action.get("review_task")
            if action_type == "merge_with_followups" and isinstance(maybe_review, DbTask):
                followup_findings = action.get("followup_findings")
                materialized = _materialize_followup_tasks(
                    maybe_review,
                    iteration_index=iteration,
                    findings=followup_findings if isinstance(followup_findings, tuple) else None,
                )
                if not materialized:
                    maybe_review_verdict = "APPROVED_WITH_FOLLOWUPS"
                    _append_summary_row(
                        summary_rows,
                        iteration_index=iteration,
                        task_type="review",
                        task=maybe_review,
                        verdict=maybe_review_verdict,
                    )
                    final_status = "blocked"
                    final_stop_reason = "needs_discussion"
                    break
            if isinstance(maybe_review, DbTask):
                maybe_review_verdict = get_review_verdict(config, maybe_review) if maybe_review.status == "completed" else None
                _append_summary_row(
                    summary_rows,
                    iteration_index=iteration,
                    task_type="review",
                    task=maybe_review,
                    verdict=maybe_review_verdict,
                )
            if maybe_review_verdict in {"APPROVED", "APPROVED_WITH_FOLLOWUPS"}:
                final_status = "approved"
                final_stop_reason = "approved" if maybe_review_verdict == "APPROVED" else "approved_with_followups"
            else:
                merge_desc = action.get("description")
                if isinstance(merge_desc, str) and merge_desc:
                    final_stop_reason = merge_desc
            break

        if action_type in {"needs_discussion", "max_cycles_reached", "skip"}:
            final_status = "blocked"
            final_stop_reason = action_type
            if classify_advance_action(action) == "needs_attention":
                final_attention_action = action
                final_attention_task = resolve_subject_task(store, action, fallback_task=impl_task)
            maybe_review = action.get("review_task")
            if isinstance(maybe_review, DbTask):
                maybe_verdict = get_review_verdict(config, maybe_review) if maybe_review.status == "completed" else None
                _append_summary_row(summary_rows, iteration_index=iteration, task_type="review", task=maybe_review, verdict=maybe_verdict)
            break

        if action_type == "wait_review":
            final_status = "blocked"
            final_stop_reason = "review_in_progress"
            review_task = action.get("review_task")
            if isinstance(review_task, DbTask):
                _append_summary_row(
                    summary_rows,
                    iteration_index=iteration,
                    task_type="review",
                    task=review_task,
                    status="in_progress",
                )
            else:
                _append_summary_row(summary_rows, iteration_index=iteration, task_type="review", task=None, status="in_progress")
            break

        if action_type == "wait_improve":
            final_status = "blocked"
            final_stop_reason = "improve_in_progress"
            latest_review = _latest_completed_review()
            if latest_review is not None:
                review_verdict = get_review_verdict(config, latest_review)
                _append_summary_row(
                    summary_rows,
                    iteration_index=iteration,
                    task_type="review",
                    task=latest_review,
                    verdict=review_verdict,
                )
                assert latest_review.id is not None
                improves = store.get_improve_tasks_for(impl_task_key, latest_review.id)
                running_improve = _latest_with_status(improves, "in_progress")
                if running_improve is None:
                    running_improve = _latest_with_status(improves, "pending")
                _append_summary_row(
                    summary_rows,
                    iteration_index=iteration,
                    task_type="improve",
                    task=running_improve,
                    status="in_progress",
                )
            else:
                _append_summary_row(summary_rows, iteration_index=iteration, task_type="improve", task=None, status="in_progress")
            break

        action_task: DbTask | None = None
        verdict: str | None = None
        initial_resume = False
        review_row_task: DbTask | None = None
        review_row_verdict: str | None = None
        prepared_action_task = action.get("_prepared_task")
        prepared_review_task_id = action.get("_prepared_review_task_id")

        if action_type == "resume":
            action_task = _create_resume_task(store, impl_task, trigger_source="manual")
            assert action_task.id is not None
            initial_resume = True
            print(f"  Resuming implementation as {action_task.id}...")
        elif action_type == "needs_rebase":
            if isinstance(prepared_action_task, DbTask):
                action_task = prepared_action_task
                assert action_task.id is not None
                print(f"  Running rebase {action_task.id}...")
            elif not impl_task.branch:
                print(f"  Cannot rebase {impl_task.id}: no branch")
                final_status = "blocked"
                final_stop_reason = "needs_rebase"
                _append_summary_row(summary_rows, iteration_index=iteration, task_type="rebase", task=None, status="failed")
                break
            elif _iterate_rebase_target_already_merged(
                store=store,
                git_runtime=git_runtime,
                task=impl_task,
                target_branch=target_branch,
            ):
                print("  Skipping rebase: target implementation already merged.")
                continue
            else:
                permit_candidate = _reserve_iterate_launch()
                if permit_candidate is False:
                    final_status = "blocked"
                    final_stop_reason = "needs_rebase"
                    break
                permit_for_rebase: LaunchPermit | None = permit_candidate
                created_rebase_task = _create_rebase_task(
                    store,
                    impl_task.id,
                    impl_task.branch,
                    target_branch,
                    trigger_source="manual",
                )
                if isinstance(permit_for_rebase, LaunchPermit):
                    prepared_rebase_task = _prepare_reserved_iterate_task(
                        created_rebase_task,
                        permit=permit_for_rebase,
                        rollback_on_failure=True,
                    )
                    if prepared_rebase_task is None:
                        final_status = "blocked"
                        final_stop_reason = "needs_rebase"
                        _append_summary_row(summary_rows, iteration_index=iteration, task_type="rebase", task=None, status="failed")
                        break
                    action_task = prepared_rebase_task
                else:
                    action_task = created_rebase_task
                assert action_task.id is not None
                print(f"  Created rebase task {action_task.id}...")
        elif action_type == "create_review":
            if isinstance(prepared_action_task, DbTask):
                action_task = prepared_action_task
                assert action_task.id is not None
                print(f"  Running review {action_task.id}...")
            else:
                permit_candidate = _reserve_iterate_launch()
                if permit_candidate is False:
                    final_status = "blocked"
                    final_stop_reason = "review_failed"
                    break
                permit_for_review: LaunchPermit | None = permit_candidate
                try:
                    created_review_task = _create_review_task(store, impl_task, trigger_source="manual")
                except DuplicateReviewError as e:
                    if isinstance(permit_for_review, LaunchPermit):
                        permit_for_review.release()
                    action_task = e.active_review
                    assert action_task.id is not None
                    if action_task.status == "in_progress":
                        print(f"  Waiting for review {action_task.id}: already in progress.")
                        final_status = "blocked"
                        final_stop_reason = "review_in_progress"
                        _append_summary_row(
                            summary_rows,
                            iteration_index=iteration,
                            task_type="review",
                            task=action_task,
                            status="in_progress",
                        )
                        break
                    if action_task.status != "pending":
                        print(f"  Error creating review: duplicate review {action_task.id} has unexpected status {action_task.status}.")
                        final_status = "blocked"
                        final_stop_reason = "review_failed"
                        _append_summary_row(summary_rows, iteration_index=iteration, task_type="review", task=action_task, status="failed")
                        break
                    print(f"  Reusing pending review {action_task.id}...")
                except ValueError as e:
                    if isinstance(permit_for_review, LaunchPermit):
                        permit_for_review.release()
                    print(f"  Error creating review: {e}")
                    final_status = "blocked"
                    final_stop_reason = "review_failed"
                    _append_summary_row(
                        summary_rows,
                        iteration_index=iteration,
                        task_type="review",
                        task=None,
                        status="failed",
                        failure_reason=str(e),
                    )
                    break
                else:
                    if isinstance(permit_for_review, LaunchPermit):
                        prepared_review_task = _prepare_reserved_iterate_task(
                            created_review_task,
                            permit=permit_for_review,
                            rollback_on_failure=True,
                        )
                        if prepared_review_task is None:
                            final_status = "blocked"
                            final_stop_reason = "review_failed"
                            _append_summary_row(summary_rows, iteration_index=iteration, task_type="review", task=None, status="failed")
                            break
                        action_task = prepared_review_task
                    else:
                        action_task = created_review_task
                assert action_task.id is not None
                print(f"  Running review {action_task.id}...")
        elif action_type == "run_review":
            if isinstance(prepared_action_task, DbTask):
                action_task = prepared_action_task
            else:
                maybe_action_task = action.get("review_task")
                assert isinstance(maybe_action_task, DbTask)
                action_task = maybe_action_task
            assert action_task.id is not None
            print(f"  Running pending review {action_task.id}...")
        elif action_type == "create_review_adjudication":
            if isinstance(prepared_action_task, DbTask):
                action_task = prepared_action_task
            else:
                maybe_review_task = action.get("review_task")
                candidate = action.get("review_blocker_adjudication_candidate")
                assert isinstance(maybe_review_task, DbTask)
                finding = getattr(candidate, "finding", None)
                dispute_artifact = getattr(candidate, "dispute_artifact", None)
                assert finding is not None
                assert dispute_artifact is not None
                permit_candidate = _reserve_iterate_launch()
                if permit_candidate is False:
                    final_status = "blocked"
                    final_stop_reason = "review_adjudication_failed"
                    break
                created_task = _create_review_adjudication_task(
                    store,
                    impl_task,
                    maybe_review_task,
                    finding,
                    dispute_metadata=build_review_blocker_dispute_metadata(
                        dispute_artifact
                    ),
                    trigger_source="manual",
                )
                if isinstance(permit_candidate, LaunchPermit):
                    prepared_internal_task = _prepare_reserved_iterate_task(
                        created_task,
                        permit=permit_candidate,
                        rollback_on_failure=True,
                    )
                    if prepared_internal_task is None:
                        final_status = "blocked"
                        final_stop_reason = "review_adjudication_failed"
                        _append_summary_row(
                            summary_rows,
                            iteration_index=iteration,
                            task_type="review_adjudication",
                            task=None,
                            status="failed",
                        )
                        break
                    action_task = prepared_internal_task
                else:
                    action_task = created_task
            assert action_task.id is not None
            print(f"  Running adjudication {action_task.id}...")
        elif action_type == "run_review_adjudication":
            if isinstance(prepared_action_task, DbTask):
                action_task = prepared_action_task
            else:
                maybe_action_task = action.get("review_adjudication_task")
                assert isinstance(maybe_action_task, DbTask)
                action_task = maybe_action_task
            assert action_task.id is not None
            print(f"  Running pending adjudication {action_task.id}...")
        elif action_type == "improve":
            if isinstance(prepared_action_task, DbTask):
                action_task = prepared_action_task
                initial_resume = bool(action.get("_prepared_initial_resume", False))
                review_task = None
                if isinstance(prepared_review_task_id, str):
                    maybe_review = store.get(prepared_review_task_id)
                    if maybe_review is not None and maybe_review.task_type == "review":
                        review_task = maybe_review
                if review_task is None and action_task.depends_on:
                    maybe_review = store.get(action_task.depends_on)
                    if maybe_review is not None and maybe_review.task_type == "review":
                        review_task = maybe_review
                if review_task is not None:
                    review_row_task = review_task
                    review_row_verdict = get_review_verdict(config, review_task)
                print(f"  Running improve {action_task.id}...")
            else:
                maybe_review_task = action.get("review_task")
                assert isinstance(maybe_review_task, DbTask)
                review_task = maybe_review_task
                review_row_task = review_task
                review_row_verdict = get_review_verdict(config, review_task)
                assert impl_task.id is not None
                assert review_task.id is not None

                # Use shared logic to decide resume/retry/new for this impl+review pair
                improve_action, failed_improve, improve_decision = resolve_improve_action(
                    store, impl_task.id, review_task.id, max_resume_attempts=max_resume_attempts
                )
                if (
                    improve_action == "manual_review"
                    and failed_improve is not None
                    and improve_decision is not None
                ):
                    manual_override = _emit_manual_resume_override_warnings(failed_improve, improve_decision)
                    if manual_override is not None:
                        failed_improve, improve_decision = manual_override
                        improve_action = "resume"
                attention_result = build_improve_needs_attention_result(
                    store=store,
                    impl_task=impl_task,
                    review_task=review_task,
                    improve_mode=improve_action,
                    failed_improve=failed_improve,
                    improve_decision=improve_decision,
                    max_resume_attempts=max_resume_attempts,
                )
                if attention_result is not None:
                    attention = resolve_execution_needs_attention(impl_task, attention_result)
                    assert failed_improve is not None
                    if attention is not None:
                        final_attention_action = attention.action
                        final_attention_task = attention.task
                        if attention_result.attention_type == "automatic_recovery_disabled":
                            print(
                                "  Improve automatic recovery is disabled "
                                f"(max_resume_attempts={max_resume_attempts}); "
                                f"latest failure: {failed_improve.id}"
                            )
                            final_stop_reason = "automatic_recovery_disabled"
                        else:
                            assert improve_decision is not None
                            print(
                                f"  Latest failed improve {failed_improve.id} requires manual review "
                                f"({improve_decision.reason_text})"
                            )
                            final_stop_reason = "manual_review_required"
                            if review_row_task is not None:
                                _append_summary_row(
                                    summary_rows,
                                    iteration_index=iteration,
                                    task_type="review",
                                    task=review_row_task,
                                    verdict=review_row_verdict,
                                )
                    else:
                        assert improve_decision is not None
                        final_non_attention_stop_message = attention_result.message.removeprefix("SKIP: ")
                        print(f"  {final_non_attention_stop_message}")
                        final_stop_reason = improve_decision.reason_code
                        if review_row_task is not None:
                            _append_summary_row(
                                summary_rows,
                                iteration_index=iteration,
                                task_type="review",
                                task=review_row_task,
                                verdict=review_row_verdict,
                            )
                    final_status = "blocked"
                    _append_summary_row(summary_rows, iteration_index=iteration, task_type="improve", task=failed_improve, status="failed")
                    break
                permit_candidate = _reserve_iterate_launch()
                if permit_candidate is False:
                    final_status = "blocked"
                    final_stop_reason = "improve_failed"
                    break
                permit_for_improve: LaunchPermit | None = permit_candidate
                if improve_action == "resume" and failed_improve is not None:
                    assert failed_improve.id is not None
                    created_improve_task = _create_resume_task(store, failed_improve, trigger_source="manual")
                    initial_resume = True
                elif improve_action == "retry" and failed_improve is not None:
                    assert failed_improve.id is not None
                    created_improve_task = _create_retry_task(store, failed_improve, trigger_source="manual")
                else:
                    try:
                        created_improve_task = _create_improve_task(
                            store,
                            impl_task,
                            review_task,
                            trigger_source="manual",
                        )
                    except ValueError as e:
                        if isinstance(permit_for_improve, LaunchPermit):
                            permit_for_improve.release()
                        print(f"  Error creating improve task: {e}")
                        final_status = "blocked"
                        final_stop_reason = "improve_failed"
                        if review_row_task is not None:
                            _append_summary_row(
                                summary_rows,
                                iteration_index=iteration,
                                task_type="review",
                                task=review_row_task,
                                verdict=review_row_verdict,
                            )
                        _append_summary_row(
                            summary_rows,
                            iteration_index=iteration,
                            task_type="improve",
                            task=None,
                            status="failed",
                            failure_reason=str(e),
                        )
                        break
                if isinstance(permit_for_improve, LaunchPermit):
                    prepared_improve_task = _prepare_reserved_iterate_task(
                        created_improve_task,
                        permit=permit_for_improve,
                        rollback_on_failure=True,
                    )
                    if prepared_improve_task is None:
                        final_status = "blocked"
                        final_stop_reason = "improve_failed"
                        if review_row_task is not None:
                            _append_summary_row(
                                summary_rows,
                                iteration_index=iteration,
                                task_type="review",
                                task=review_row_task,
                                verdict=review_row_verdict,
                            )
                        _append_summary_row(summary_rows, iteration_index=iteration, task_type="improve", task=None, status="failed")
                        break
                    action_task = prepared_improve_task
                else:
                    action_task = created_improve_task
                assert action_task.id is not None
                if improve_action == "resume" and failed_improve is not None:
                    print(f"  Created improve task {action_task.id} (resume of {failed_improve.id})")
                elif improve_action == "retry" and failed_improve is not None:
                    print(f"  Created improve task {action_task.id} (retry of {failed_improve.id})")
                print(f"  Running improve {action_task.id}...")
        elif action_type == "run_improve":
            if isinstance(prepared_action_task, DbTask):
                action_task = prepared_action_task
            else:
                maybe_action_task = action.get("improve_task")
                assert isinstance(maybe_action_task, DbTask)
                action_task = maybe_action_task
            assert action_task.id is not None
            if action_task.depends_on:
                maybe_review = store.get(action_task.depends_on)
                if maybe_review is not None and maybe_review.task_type == "review":
                    review_row_task = maybe_review
                    review_row_verdict = get_review_verdict(config, maybe_review)
            print(f"  Running pending improve {action_task.id}...")
        else:
            final_status = "blocked"
            final_stop_reason = f"unsupported_action:{action_type}"
            _append_summary_row(summary_rows, iteration_index=iteration, task_type=action_type, task=None, status="failed")
            break

        if review_row_task is not None:
            _append_summary_row(
                summary_rows,
                iteration_index=iteration,
                task_type="review",
                task=review_row_task,
                verdict=review_row_verdict,
            )

        assert action_task is not None
        action_task, rc, terminal_skip_decision = _run_iterate_task_with_recovery(
            args=args,
            config=config,
            store=store,
            task_to_run=action_task,
            max_resume_attempts=effective_max_resume_attempts,
            initial_resume=initial_resume,
        )
        if rc != 0:
            final_status = "blocked"
            task_type = (
                "review"
                if action_type in {"create_review", "run_review"}
                else "review_adjudication"
                if action_type in {"create_review_adjudication", "run_review_adjudication"}
                else "improve" if action_type in {"improve", "run_improve"} else action_type
            )
            attention_result = None
            if terminal_skip_decision is not None:
                attention_result = build_failed_recovery_needs_attention_result(
                    store=store,
                    failed_task=action_task,
                    recovery_decision=terminal_skip_decision,
                    max_resume_attempts=effective_max_resume_attempts,
                )
            if attention_result is not None:
                attention = resolve_execution_needs_attention(action_task, attention_result)
                if attention is not None:
                    final_attention_action = attention.action
                    final_attention_task = attention.task
                    assert terminal_skip_decision is not None
                    terminal_reason_code = terminal_skip_decision.reason_code
                    final_stop_reason = attention_result.attention_reason or terminal_reason_code
                else:
                    final_stop_reason = f"{action_type}_failed"
            else:
                final_stop_reason = f"{action_type}_failed"
            _append_summary_row(
                summary_rows,
                iteration_index=iteration,
                task_type=task_type,
                task=action_task,
                status="failed",
                failure_reason=f"exit code {rc}",
            )
            break

        if action_task.id is not None:
            action_task = store.get(action_task.id) or action_task

        if action_type in {"create_review", "run_review"}:
            verdict = get_review_verdict(config, action_task)
            print(
                f"  Review {action_task.id}: "
                f"{format_review_outcome(config, action_task, unknown_label='(none)')}"
            )
            _append_summary_row(
                summary_rows,
                iteration_index=iteration,
                task_type="review",
                task=action_task,
                verdict=verdict,
            )
            if verdict == "APPROVED_WITH_FOLLOWUPS":
                materialized = _materialize_followup_tasks(action_task, iteration_index=iteration)
                if not materialized:
                    final_status = "blocked"
                    final_stop_reason = "needs_discussion"
                    break
                final_status = "approved"
                final_stop_reason = "approved_with_followups"
                break
            if verdict == "APPROVED":
                final_status = "approved"
                final_stop_reason = "approved"
                break
            if verdict in {"NEEDS_DISCUSSION", None}:
                final_status = "blocked"
                final_stop_reason = "needs_discussion" if verdict == "NEEDS_DISCUSSION" else "no_verdict"
                break
        else:
            verdict = None
            task_type = (
                "improve"
                if action_type in {"improve", "run_improve"}
                else "review_adjudication"
                if action_type in {"create_review_adjudication", "run_review_adjudication"}
                else action_type
            )
            _append_summary_row(
                summary_rows,
                iteration_index=iteration,
                task_type=task_type,
                task=action_task,
                verdict=verdict,
            )
        if action_type in {"create_review", "run_review"}:
            # Count full change+review cycles by completed review actions.
            iteration += 1
        impl_task = store.get(impl_task.id) or impl_task

    if final_status in {"approved", "merge_ready", "maxed_out"}:
        _run_forced_closing_review(iteration)

    iterate_wall_seconds = time.monotonic() - iterate_started_at
    total_steps = sum(row.steps or 0 for row in summary_rows)
    total_cost = sum(row.cost_usd or 0.0 for row in summary_rows)

    print(f"\n{'=' * 60}")
    print(f"Iterate complete: {final_status.upper()} ({final_stop_reason})")
    print(f"{'=' * 60}")
    print(f"{'Iter':<5} {'Type':<8} {'Task':<10} {'Verdict':<18} {'Duration':>8} {'Steps':>5} {'Cost':>8} Status")
    print(f"{'-' * 5} {'-' * 8} {'-' * 10} {'-' * 18} {'-' * 8} {'-' * 5} {'-' * 8} {'-' * 12}")
    for row in summary_rows:
        iter_str = str(row.iteration_index + 1)
        verdict_str = row.verdict or "-"
        duration_str = _format_compact_duration(row.duration_seconds)
        steps_str = str(row.steps) if row.steps is not None else "-"
        cost_str = f"${row.cost_usd:.2f}" if row.cost_usd is not None else "-"
        status_str = _format_summary_status(row)
        task_str = row.task_id or "-"
        print(
            f"{iter_str:<5} {row.task_type:<8} {task_str:<10} {verdict_str:<18} "
            f"{duration_str:>8} {steps_str:>5} {cost_str:>8} {status_str}"
        )
    print(f"Totals: {_format_compact_duration(iterate_wall_seconds)} wall | {total_steps} steps | ${total_cost:.2f}")
    print()

    if final_status in {"approved", "merge_ready"}:
        return 0
    if final_status == "maxed_out":
        print(f"Max iterations ({max_iterations}) reached.")
        return 2
    if final_stop_reason in {"review_in_progress", "improve_in_progress"}:
        print(f"Iterate waiting: {final_stop_reason}. Existing task is already in progress.")
        return 3
    if final_stop_reason == "max_cycles_reached":
        completed_review_cycles = count_completed_review_cycles(store, impl_task_key)
        consumed_this_invocation = max(0, completed_review_cycles - starting_completed_review_cycles)
        if final_attention_action is not None and final_attention_task is not None:
            print(
                f"{NEEDS_ATTENTION_LABEL}: "
                f"{format_needs_attention_entry_for_display(final_attention_task, action=final_attention_action, prefix=len(final_attention_task.id or '') + 4)}"
            )
        else:
            print(f"Iterate blocked: {final_stop_reason}.")
        print(
            "Review-iteration accounting: "
            f"completed={completed_review_cycles}, "
            f"max_review_cycles={engine_config.max_review_cycles}, "
            f"consumed_this_invocation={consumed_this_invocation}"
        )
        print(f"Recommended next step: uv run gza fix {impl_task_key}")
        return 3
    if final_attention_action is not None and final_attention_task is not None:
        print(
            f"{NEEDS_ATTENTION_LABEL}: "
            f"{format_needs_attention_entry_for_display(final_attention_task, action=final_attention_action, prefix=len(final_attention_task.id or '') + 4)}"
        )
        if needs_attention_recommends_fix(final_attention_action):
            print(f"Recommended next step: uv run gza fix {impl_task_key}")
        return 3
    if final_non_attention_stop_message is not None:
        print(f"Iterate blocked: {final_non_attention_stop_message}")
        return 3
    print(f"Iterate blocked: {final_stop_reason}. Manual review required.")
    return 3


def cmd_iterate(args: argparse.Namespace) -> int:
    """Run an automated lifecycle loop for an implementation task."""
    config = Config.load(args.project_dir)
    if hasattr(args, 'no_docker') and args.no_docker:
        config.use_docker = False

    return _run_with_registered_worker(
        config=config,
        worker_id=getattr(args, "worker_id", None),
        run_command=lambda: _cmd_iterate_impl(args, config),
    )


def cmd_resume(args: argparse.Namespace) -> int:
    """Resume a failed or orphaned task from where it left off."""
    config = Config.load(args.project_dir)
    if args.no_docker:
        config.use_docker = False

    # Override max_turns if specified
    if hasattr(args, 'max_turns') and args.max_turns is not None:
        config.max_steps = args.max_turns
        config.max_turns = args.max_turns
    execution_mode = _execution_mode(args)

    store = get_store(config)

    task_id = resolve_id(config, args.task_id)
    task = store.get(task_id)
    if not task:
        return phase1_error(args, f"Task {task_id} not found")

    if task.status not in ("failed", "in_progress"):
        return phase1_error(args, f"Can only resume failed or orphaned tasks (task is {task.status})")

    if task.status == "in_progress":
        # Allow resume only if the task is orphaned (no live worker)
        assert task.id is not None
        registry = WorkerRegistry(config.workers_path)
        running_worker = _running_worker_id_for_task(registry, task.id)
        if running_worker is not None:
            print_phase1_message(args, f"Error: Task {task_id} is still running (worker {running_worker})")
            print_phase1_message(args, "Use 'gza cancel' to stop it first, or wait for it to finish")
            return 1
        print(f"Note: Task {task_id} appears orphaned (in_progress but no live worker), resuming...")
    elif task.status == "failed" and task.failure_reason == "WORKER_DIED":
        print(f"Note: Task {task_id} appears orphaned (worker died), resuming...")

    if not task.session_id:
        print_phase1_message(args, f"Error: Task {task_id} has no session ID (cannot resume)")
        print_phase1_message(
            args,
            "Use 'gza retry' to create a new retry attempt with a fresh conversation instead"
            " (implement retries may fork fresh; same-branch follow-ups stay on the shared branch)",
        )
        return 1

    # Create a new task (like retry) to track this resumed run.
    # The original task stays failed with its stats preserved.
    reserved_launch = _reserve_immediate_execution_permit(args=args, config=config, store=store)
    if reserved_launch is False:
        return 1
    new_task = _create_resume_task(store, task, trigger_source="manual")
    assert new_task.id is not None

    def _emit_resume_created() -> None:
        print(f"✓ Created task {new_task.id} (resume of {task_id})")

    prepared_resume_task, _resume_launch_permit = _finalize_immediate_execution_task(
        args=args,
        config=config,
        rollback_on_failure=True,
        task=new_task,
        emit_created=_emit_resume_created,
        reserved_permit=reserved_launch if isinstance(reserved_launch, LaunchPermit) else None,
    )
    if prepared_resume_task is None:
        return 1
    new_task = prepared_resume_task

    # Handle queue mode - add to queue without executing
    if execution_mode == "queue":
        return 0

    # Handle background mode
    if execution_mode == "background":
        assert new_task.id is not None
        rc = _spawn_background_resume_worker(
            args,
            config,
            new_task.id,
            prepared_task=new_task,
        )
        release_task_launch_permit(str(new_task.id))
        return rc

    # Default: run the new resume task immediately
    rc = _run_foreground(
        config,
        task_id=new_task.id,
        resume=True,
        force=getattr(args, "force", False),
        invocation=_foreground_command_invocation("resume"),
    )
    release_task_launch_permit(str(new_task.id))
    return rc
