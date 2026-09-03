"""Git-related CLI commands: merge, rebase, checkout, diff, PR, advance."""

import argparse
import inspect
import json
import logging
import os
import shutil
import subprocess
import sys
import traceback
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import nullcontext
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, NoReturn, TypeVar, cast

from rich.console import Console

import gza.colors as _colors
from gza.query import (
    get_base_task_slug as _get_base_task_slug,
    get_implementation_review_evidence,
)

from ..advance_engine import (
    IMPROVE_ACTION_REASON_REVIEW_CHANGES_REQUESTED,
    _resolve_and_persist_post_merge_rebase_state,
    _resolve_current_merge_source,
    is_current_red_verify_gate_action,
    is_red_verify_gate_family_action,
    pending_merge_finalization_action,
    resolve_post_merge_rebase_state,
)
from ..branch_publication import load_branch_publication_state
from ..colors import pink
from ..commit_messages import build_task_commit_message
from ..concurrency import (
    MaxConcurrentTasksError,
    format_max_concurrent_message,
    get_concurrency_snapshot,
    launch_permit,
    reserve_task_launch_permit,
)
from ..config import Config, ConfigError
from ..console import (
    console,
    prompt_available_width,
    shorten_prompt,
)
from ..db import (
    DB_UNSET,
    MERGE_SOURCE_ADVANCE,
    MERGE_SOURCE_MANUAL,
    MERGE_SOURCE_MAX_CYCLES_DEFERRED,
    DuplicateActiveChildError,
    MergeTargetResolutionError,
    SqliteTaskStore,
    Task as DbTask,
    TaskStats,
    merge_unit_is_active,
    task_id_numeric_key,
)
from ..dependency_preconditions import task_is_merged
from ..derived_tags import resolve_derived_task_tags
from ..dispatch_preview import build_dispatch_preview
from ..failure_reasons import is_readonly_db_failure, mark_task_failed_from_cause
from ..git import (
    Git,
    GitError,
    ResolvedGitRef,
    ResolvedMergeSourceRef,
    active_worktree_path_for_branch,
    cleanup_worktree_for_branch,
    git_error_indicates_containerized_worktree_metadata_failure,
    is_rebase_in_progress,
    prime_advance_planning_refs,
    remove_worktree_registration_for_path,
    resolve_ref_if_possible,
)
from ..lineage_query import (
    LineageOwnerQuery,
    LineageOwnerRow,
    apply_deferred_lineage_query_reconciliations,
    query_lineage_owner_rows,
)
from ..log_paths import resolve_ops_log_path
from ..main_integration_verify import (
    MAIN_INTEGRATION_VERIFY_REASON,
    CandidateIntegrationVerifyCheck,
    check_candidate_integration_verify,
    check_main_integration_verify,
    inspect_main_integration_verify_checkpoint,
    promote_candidate_integration_verify_evidence,
    verify_gate_enabled,
)
from ..merge_finalization_proof import (
    MergeFinalizationFamily,
    MergeFinalizationPromotionKind,
    get_merge_finalization_proof,
    merge_finalization_child_task_ids,
    merge_finalization_finding_ids,
    persist_merge_finalization_attempt_proof,
    persist_merge_finalization_prepared_attempt,
)
from ..merge_services import (
    ManualMergeExecutionHooks,
    ManualMergeExecutionRequest,
    ManualMergeExecutionResult,
    MergeDeferredBlockerDecision as _MergeDeferredBlockerDecision,
    ResolvedMergeSubject as _ResolvedMergeSubject,
    classify_manual_merge_blockers,
    execute_manual_merge,
    latest_completed_review_for_merge_subject,
    manual_force_merge_source,
    mark_merge_subject_merged,
    materialize_merge_deferred_blockers,
    materialize_merge_followups,
    resolve_fresh_merge_source,
    resolve_merge_subject,
    resolve_merge_subject_query_only,
    resolve_merge_target_task,
)
from ..merge_state import resolve_task_merge_state_for_target
from ..pickup import (
    count_worker_consuming_actions,
    get_runnable_pending_tasks,
    is_worker_consuming_advance_action,
)
from ..pr_ops import build_task_pr_content, ensure_task_pr
from ..providers.base import provider_home_from_env
from ..rebase_checkout import (
    StaleRebaseImportError,
    build_immutable_rebase_provider_prompt,
    import_isolated_rebase_tip,
    isolated_rebase_checkout,
)
from ..rebase_diff import (
    build_rebase_diff_provenance,
    capture_rebase_diff_baseline,
    compute_rebase_changed_diff,
    prove_rebase_changed_diff_boundary,
)
from ..rebase_publish import (
    REBASE_SUPERSEDED_COMPLETION_REASON,
    branch_contains_rebase_target,
    publish_rebased_branch,
)
from ..rebase_service import (
    RebaseExecutionOutcome,
    RebaseExecutionStatus,
    RebaseServiceRequest,
    execute_task_backed_rebase_service,
)
from ..recovery_engine import (
    _MergeContext,
    _resolve_merged_target_task,
    build_merge_context_from_git,
    list_failed_tasks_for_recovery,
    resolve_pending_recovery_execution_mode,
    resolve_recovery_planning_task,
)
from ..recovery_read_context import RecoveryReadContext
from ..review_scope import declares_resolution_review_mode, declares_spec_coherence_review_mode
from ..review_tasks import (
    DEFERRED_REVIEW_BLOCKER_TAG,
    CappedReviewBlockerMaterializationError,
    FollowupMaterializationError,
    build_capped_review_blocker_prompt_prefix,
    build_followup_prompt,
    capped_review_blocker_task_identity_mismatches,
    format_followup_finding_context,
    validate_capped_review_blocker_action,
)
from ..review_verdict import (
    ReviewFinding,
    get_review_content,
    get_review_report,
    parse_review_report,
    summarize_review_blockers,
)
from ..review_verify_state import (
    VerifyEpoch,
    refresh_preserved_rebase_review_verify_heads,
    resolve_verify_gate_decision,
    verify_epoch_matches,
)
from ..runner import (
    WIP_INTERRUPTED_COMMIT_SUBJECT,
    LongPhaseHeartbeat,
    LongPhaseProgress,
    TaskExecutionLogger,
    _call_provider_run,
    _complete_failed_code_task_after_pr_publication,
    _compute_tree_fingerprint,
    _LongPhaseHeartbeatState,
    _resolve_impl_ancestor,
    _resolve_root_implementation_for_fix,
    ensure_task_log_path,
    get_effective_config_for_task,
    task_log_storage_path,
    write_log_entry,
)
from ..runtime_context import RuntimeExecutionContext
from ..source_followup import (
    SourceFollowupState,
    collect_non_dropped_implement_source_ids,
    resolve_source_followup_state,
    source_task_needs_implementation_followup,
)
from ..sync_ops import (
    DEFAULT_SYNC_CACHE_SECONDS,
    build_branch_cohorts_for_task_ids,
    build_default_branch_cohorts,
    reconcile_task_branch_merge_truth,
    sync_branch_cohorts,
)
from ..task_query import normalize_tag_filters
from ..workers import WorkerMetadata, WorkerRegistry
from ..worktree_roots import managed_worktree_root_paths
from ._common import (
    _REUSE_WORKER_OWNER_ENV,
    _REUSE_WORKER_OWNER_OUTER,
    _REUSE_WORKER_REENTRY_ENV,
    _REUSE_WORKER_SESSION_ENV,
    DuplicateReviewError,
    _create_implementation_task_from_source,
    _create_or_reuse_capped_review_blocker_tasks,
    _create_or_reuse_deferred_blocker_tasks,
    _create_or_reuse_followup_tasks,
    _create_plan_improve_task,
    _create_plan_review_task,
    _create_rebase_task,
    _create_resume_task,
    _create_retry_task,
    _create_review_adjudication_task,
    _create_review_task,
    _get_pager,
    _looks_like_task_id,
    _materialize_plan_review_slices,
    _prepare_task_for_immediate_execution,
    _repair_plan_review_slice_materialization,
    _run_foreground,
    _spawn_background_iterate_worker,
    _spawn_background_resume_worker,
    _spawn_background_worker,
    format_duplicate_rebase_message,
    get_review_verdict,  # noqa: F401  # re-exported for test patching
    get_store,
    parse_cli_tag_filters,
    phase1_error,
    resolve_id,
)
from ._lifecycle_actions import (
    LifecycleActionEntry,
    lifecycle_action_execution_sort_key,
    plan_lifecycle_execution,
    print_lifecycle_action_entries,
    reproject_selected_merge_actions,
)
from .advance_engine import (
    NEEDS_ATTENTION_LABEL,
    classify_advance_action,
    determine_next_action,
    failed_recovery_decision_to_action,
    failed_recovery_decision_to_attention_action,
    format_needs_attention_entry_for_display,
    needs_attention_recommended_next_step,
    resolve_subject_task,
)
from .advance_executor import (
    AdvanceActionExecutionContext,
    BranchDivergenceReconcileResult,
    execute_advance_action,
    resolve_execution_needs_attention,
)

logger = logging.getLogger(__name__)


def _owned_git_env(git: Git) -> Mapping[str, str] | None:
    env = getattr(git, "env", None)
    return env if isinstance(env, Mapping) else None


def _git_accepts_env() -> bool:
    try:
        parameters = inspect.signature(Git).parameters.values()
    except (TypeError, ValueError):
        return True
    return any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD or parameter.name == "env"
        for parameter in parameters
    )


def _git_from_runtime_context(repo_dir: Path, runtime_context: RuntimeExecutionContext) -> Git:
    if _git_accepts_env():
        return Git(repo_dir, env=runtime_context.env)
    return Git(repo_dir)


def _git_with_env(repo_dir: Path, env: Mapping[str, str] | None) -> Git:
    if env is not None and _git_accepts_env():
        return Git(repo_dir, env=env)
    return Git(repo_dir)


def _build_advance_recovery_merge_context(git: Git, target_branch: str | None) -> _MergeContext:
    local_branch_names = getattr(git, "local_branch_names", None)
    if local_branch_names is None or not callable(local_branch_names):
        console.print(
            "[yellow]Warning: advance recovery preview is using a degraded git context; "
            "the active git object does not expose local_branch_names()[/yellow]"
        )
        return _MergeContext(git=git, default_branch=target_branch)

    try:
        preview_branch_names = local_branch_names()
    except (GitError, OSError, ValueError):
        return build_merge_context_from_git(git, target_branch)
    if not isinstance(preview_branch_names, Iterable):
        console.print(
            "[yellow]Warning: advance recovery preview is using a degraded git context; "
            "local_branch_names() did not return an iterable branch list[/yellow]"
        )
        return _MergeContext(git=git, default_branch=target_branch)

    return build_merge_context_from_git(git, target_branch)


def _classify_rebase_git_failure(error: BaseException) -> str:
    if git_error_indicates_containerized_worktree_metadata_failure(error) or is_readonly_db_failure(error):
        return "INFRASTRUCTURE_ERROR"
    return "GIT_ERROR"


_T = TypeVar("_T")


def _print_mixed_recovery_preview_entries(
    *,
    store: SqliteTaskStore,
    preview: object,
    max_recovery_attempts: int,
) -> None:
    mixed_entries = [
        entry
        for entry in getattr(preview, "recovery_entries", ())
        if (
            entry.owner_task is not None
            and entry.task.id is not None
            and entry.owner_task.id is not None
            and entry.task.id != entry.owner_task.id
            and entry.decision is not None
        )
    ]
    if not mixed_entries:
        return

    print("Recovery subset (shared preview):\n")
    for entry in mixed_entries:
        task = entry.task
        prompt_display = shorten_prompt(task.prompt, 100)
        console.print(f"{task.id} {prompt_display}")
        if entry.runnable:
            action = failed_recovery_decision_to_action(
                task,
                entry.decision,
                subject_task_id=task.id,
            )
            action_color = _advance_action_color(str(action["type"]))
            console.print(f"[{action_color}]→ {action['description']}[/{action_color}]")
        else:
            attention_action = failed_recovery_decision_to_attention_action(
                store,
                task,
                entry.decision,
                max_recovery_attempts=max_recovery_attempts,
                read_context=getattr(preview, "read_context", None),
            )
            if attention_action is not None:
                detail = format_needs_attention_entry_for_display(task, action=attention_action)
                console.print(f"{detail}")
            else:
                console.print(f"SKIP: {entry.decision.reason_text}")
        console.print()


@dataclass(frozen=True)
class _CandidateVerifyPromotionProof:
    blocked_status: Literal["blocked_candidate_verify", "blocked_candidate_verify_unavailable"]
    block_reason: str
    verified_head_sha: str | None = None
    verified_tree_fingerprint: str | None = None

    @property
    def exact_match(self) -> bool:
        return self.verified_head_sha is not None and self.verified_tree_fingerprint is not None


class _IsolatedPromotionRollbackFailed(GitError):
    """Target promotion failed after the target ref moved and rollback failed."""

    def __init__(
        self,
        message: str,
        *,
        target_branch: str,
        previous_target_oid: str,
        candidate_oid: str,
    ) -> None:
        super().__init__(message)
        self.target_branch = target_branch
        self.previous_target_oid = previous_target_oid
        self.candidate_oid = candidate_oid


@dataclass(frozen=True)
class _IsolatedPromotionRollbackClassification:
    status: Literal[
        "isolated_post_promotion_rollback_failed",
        "isolated_merge_failed",
        "isolated_promotion_rollback_failed_target_uncertain",
    ]
    block_reason: str
    target_oid: str | None
    target_at_candidate: bool = False


def _materialize_merge_followups(
    store: SqliteTaskStore,
    config: Config,
    merge_subject: DbTask,
) -> tuple[list[DbTask], list[DbTask]]:
    """Create or reuse FOLLOWUP tasks for the latest completed review on a merged task."""
    return materialize_merge_followups(
        store,
        config,
        merge_subject,
        create_followups=_create_or_reuse_followup_tasks,
    )


def _merge_single_side_effect_failure_result(exc: Exception) -> "_MergeSingleTaskResult":
    result = _mandatory_merge_side_effects_failed_result(exc=exc)
    return _MergeSingleTaskResult(
        rc=result.rc,
        status=result.status,
        block_reason=result.block_reason,
        created_followups=tuple(result.created_followups),
        reused_followups=tuple(result.reused_followups),
        created_deferred_blockers=tuple(result.created_deferred_blockers),
        reused_deferred_blockers=tuple(result.reused_deferred_blockers),
    )


def _print_followup_tasks(merge_subject: DbTask, created: Iterable[DbTask], reused: Iterable[DbTask]) -> None:
    for followup_task in created:
        print(f"FOLLOW {followup_task.id} created from {merge_subject.id}")
    for followup_task in reused:
        print(f"FOLLOW {followup_task.id} reused from {merge_subject.id}")


def _latest_completed_review_for_merge_subject(
    store: SqliteTaskStore,
    merge_subject: DbTask,
) -> DbTask | None:
    return latest_completed_review_for_merge_subject(store, merge_subject)


def _classify_manual_merge_blockers(
    *,
    store: SqliteTaskStore,
    config: Config,
    merge_subject: DbTask,
    defer_blockers: bool,
) -> _MergeDeferredBlockerDecision:
    return classify_manual_merge_blockers(
        store=store,
        config=config,
        merge_subject=merge_subject,
        defer_blockers=defer_blockers,
        load_review_report=get_review_report,
        load_review_content=get_review_content,
        summarize_blockers=summarize_review_blockers,
    )


def _materialize_merge_deferred_blockers(
    store: SqliteTaskStore,
    config: Config,
    merge_subject: DbTask,
    *,
    defer_blockers: bool,
) -> tuple[list[DbTask], list[DbTask]] | None:
    materialization = materialize_merge_deferred_blockers(
        store,
        config,
        merge_subject,
        defer_blockers=defer_blockers,
        create_deferred_blockers=_create_or_reuse_deferred_blocker_tasks,
        load_review_report=get_review_report,
        load_review_content=get_review_content,
        summarize_blockers=summarize_review_blockers,
    )
    if materialization.decision.refusal_message is not None:
        print(materialization.decision.refusal_message)
        return None
    return materialization.tasks


def _print_deferred_blocker_tasks(
    merge_subject: DbTask,
    deferred_blockers: tuple[list[DbTask], list[DbTask]],
) -> None:
    created_deferred_blockers, reused_deferred_blockers = deferred_blockers
    for blocker_task in created_deferred_blockers:
        print(f"DEFERRED-BLOCKER {blocker_task.id} created from {merge_subject.id}")
    for blocker_task in reused_deferred_blockers:
        print(f"DEFERRED-BLOCKER {blocker_task.id} reused from {merge_subject.id}")


def _is_review_changes_requested_improve_action(
    action: Mapping[str, Any],
    *,
    store: SqliteTaskStore,
    merge_subject: DbTask,
) -> bool:
    if action.get("type") != "improve":
        return False
    if action.get("improve_reason") != IMPROVE_ACTION_REASON_REVIEW_CHANGES_REQUESTED:
        return False
    review_task = action.get("review_task")
    if not isinstance(review_task, DbTask):
        return False
    if review_task.status != "completed":
        return False
    if review_task.id is None:
        return False
    latest_review = _latest_completed_review_for_merge_subject(store, merge_subject)
    if latest_review is None or latest_review.id != review_task.id:
        return False
    review_mode = action.get("review_mode")
    if review_mode is None:
        if declares_spec_coherence_review_mode(review_task.review_scope):
            review_mode = "spec_coherence"
        elif declares_resolution_review_mode(review_task.review_scope):
            review_mode = "resolution"
        else:
            review_mode = "plain_full"
    return review_mode in {"plain_full", "resolution"}


def _merge_execution_status_error(
    merge_subject_id: str,
    execution_task: DbTask,
) -> str | None:
    if execution_task.status in {"completed", "unmerged"}:
        return None
    return f"Task {merge_subject_id} is not completed or unmerged (execution status: {execution_task.status})"


@dataclass(frozen=True)
class SquashBranchReconcileResult:
    status: str
    branch: str
    remote: str = "origin"
    reason: str | None = None
    manual_source_ref: str | None = None
    expected_remote_oid: str | None = None


def _should_retry_pr_publication_after_reconcile(task: DbTask) -> bool:
    """Return whether reconcile should rerun shared PR publication work."""
    if not task.create_pr:
        return False
    return not (task.pr_state == "open" and task.pr_number is not None)


def complete_branch_unpushable_after_reconcile(
    *,
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    task: DbTask,
) -> int:
    """Re-publish PR state and complete a failed branch-publication task after reconcile."""
    if task.id is None or not task.branch:
        return 1
    if task.status != "failed" or task.failure_reason not in {"BRANCH_UNPUSHABLE", "PR_REQUIRED"}:
        return 1

    log_path = None
    if task.log_file:
        log_path = config.project_dir / Path(task.log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
    task_logger = (
        TaskExecutionLogger(resolve_ops_log_path(config, log_path), echo=True) if log_path is not None else None
    )
    default_branch = git.default_branch()
    publication_state = load_branch_publication_state(store, task.id)
    publication_retry_task = (
        task if _should_retry_pr_publication_after_reconcile(task) else replace(task, create_pr=False)
    )
    verify_fix_worktree_path = None
    if task.task_type == "verify_fix" and task.slug:
        configured_worktree_root = getattr(config, "worktree_path", None)
        if isinstance(configured_worktree_root, Path):
            verify_fix_worktree_path = configured_worktree_root / task.slug
    return _complete_failed_code_task_after_pr_publication(
        task=publication_retry_task,
        config=config,
        store=store,
        git=git,
        branch_name=task.branch,
        stats=TaskStats(
            duration_seconds=task.duration_seconds,
            num_steps_reported=task.num_steps_reported,
            num_steps_computed=task.num_steps_computed,
            num_turns_reported=task.num_turns_reported,
            num_turns_computed=task.num_turns_computed,
            cost_usd=task.cost_usd,
            input_tokens=task.input_tokens,
            output_tokens=task.output_tokens,
        ),
        log_file=log_path,
        output_content=task.output_content,
        diff_files=task.diff_files_changed or 0,
        diff_added=task.diff_lines_added or 0,
        diff_removed=task.diff_lines_removed or 0,
        head_sha=git.rev_parse_if_exists(task.branch) if task.has_commits else None,
        base_sha=git.rev_parse_if_exists(default_branch) if task.has_commits else None,
        task_logger=task_logger,
        target_branch=default_branch,
        fix_commits_ahead_before_run=publication_state.fix_commits_ahead_before_run,
        fix_default_branch=publication_state.fix_default_branch,
        fix_was_merged_before_run=publication_state.fix_was_merged_before_run,
        record_reconcile_attempt=True,
        worktree_path=verify_fix_worktree_path,
    )


def _reconcile_diverged_branch_with_origin(
    config: Config,
    git: Git,
    task: DbTask,
    *,
    target_branch: str,
    remote: str = "origin",
) -> BranchDivergenceReconcileResult:
    """Reconcile a diverged local/origin branch without consuming a worker slot."""
    if not task.branch:
        return BranchDivergenceReconcileResult(
            status="error",
            message=f"Cannot reconcile divergence for task {task.id}: branch is missing",
        )
    if not target_branch:
        return BranchDivergenceReconcileResult(
            status="error",
            message="Cannot reconcile divergence: target branch is missing",
        )
    if not git.branch_exists(target_branch):
        return BranchDivergenceReconcileResult(
            status="error",
            message=(f"Cannot reconcile divergence for '{task.branch}': missing local target branch '{target_branch}'"),
        )

    branch = task.branch
    remote_ref = f"{remote}/{branch}"
    rebase_target = target_branch
    remote_sha_before_push = git.rev_parse_if_exists(remote_ref)
    if not remote_sha_before_push:
        return BranchDivergenceReconcileResult(
            status="error",
            message=f"Cannot reconcile divergence for '{branch}': missing '{remote_ref}'",
        )
    local_sha = git.rev_parse_if_exists(branch)
    if not local_sha:
        return BranchDivergenceReconcileResult(
            status="error",
            message=f"Cannot reconcile divergence for '{branch}': missing local branch",
        )

    resolved_merge_source = git.resolve_fresh_merge_source(branch, remote=remote)
    needs_mechanical_rebase = False
    fetched_remote_for_rebase = False
    local_ahead = git.count_commits_ahead(branch, remote_ref)
    remote_ahead = git.count_commits_ahead(remote_ref, branch)
    if local_sha == remote_sha_before_push:
        return BranchDivergenceReconcileResult(
            status="reconciled",
            message=f"'{branch}' is already aligned with '{remote_ref}'",
        )
    if resolved_merge_source.ref == branch:
        needs_mechanical_rebase = False
    elif resolved_merge_source.ref == remote_ref:
        needs_mechanical_rebase = True
    elif local_ahead > 0 and remote_ahead > 0:
        needs_mechanical_rebase = not _is_benign_gza_rewrite_divergence(
            git,
            branch=branch,
            remote_ref=remote_ref,
            local_ahead=local_ahead,
            remote_ahead=remote_ahead,
        )
    else:
        message = resolved_merge_source.warning or (
            f"Unable to determine how to reconcile '{branch}' against '{remote_ref}'"
        )
        return BranchDivergenceReconcileResult(
            status="error",
            message=message,
        )

    if not needs_mechanical_rebase:
        try:
            git.push_ref_force_with_lease(
                branch,
                branch,
                remote=remote,
                expected_remote_oid=remote_sha_before_push,
            )
            return BranchDivergenceReconcileResult(
                status="reconciled",
                message=f"Reconciled '{branch}' with --force-with-lease",
            )
        except GitError as push_error:
            try:
                git.fetch(remote)
            except GitError as fetch_error:
                return BranchDivergenceReconcileResult(
                    status="error",
                    message=f"Failed to fetch {remote} after force-with-lease rejection: {fetch_error}",
                )

            remote_sha_after_fetch = git.rev_parse_if_exists(remote_ref)
            if not remote_sha_after_fetch:
                return BranchDivergenceReconcileResult(
                    status="error",
                    message=f"Fetch completed but '{remote_ref}' is still unavailable",
                )
            if remote_sha_after_fetch == remote_sha_before_push:
                return BranchDivergenceReconcileResult(
                    status="error",
                    message=(f"Force-with-lease push failed for '{branch}' without a remote ref change: {push_error}"),
                )
            remote_sha_before_push = remote_sha_after_fetch
            needs_mechanical_rebase = True
            fetched_remote_for_rebase = True

    if needs_mechanical_rebase and not fetched_remote_for_rebase:
        try:
            git.fetch(remote)
        except GitError as fetch_error:
            return BranchDivergenceReconcileResult(
                status="error",
                message=(
                    f"Failed to fetch {remote} before reconciling '{branch}' for rebase onto "
                    f"'{rebase_target}': {fetch_error}"
                ),
            )
        remote_sha_after_fetch = git.rev_parse_if_exists(remote_ref)
        if not remote_sha_after_fetch:
            return BranchDivergenceReconcileResult(
                status="error",
                message=f"Fetch completed but '{remote_ref}' is still unavailable",
            )
        remote_sha_before_push = remote_sha_after_fetch

    worktree_suffix = task.id or branch.replace("/", "-")
    worktree_path = config.worktree_path / f"advance-reconcile-{worktree_suffix}"
    try:
        cleanup_worktree_for_branch(
            git,
            branch,
            force=True,
            permitted_root_paths=managed_worktree_root_paths(config),
        )
        if worktree_path.exists():
            git.worktree_remove(worktree_path, force=True)
            if worktree_path.exists():
                shutil.rmtree(worktree_path, ignore_errors=True)
        worktree_path.parent.mkdir(parents=True, exist_ok=True)
        git.worktree_add_existing(worktree_path, branch)
        parent_env = _owned_git_env(git)
        worktree_git = Git(worktree_path, env=parent_env) if parent_env is not None else Git(worktree_path)
        baseline = capture_rebase_diff_baseline(
            worktree_git,
            branch=branch,
            target=rebase_target,
        )
        rebase_target_for_execution = baseline.target_at_start or rebase_target
        try:
            worktree_git.rebase(rebase_target_for_execution)
        except GitError as rebase_error:
            try:
                worktree_git.rebase_abort()
            except GitError:
                pass
            return BranchDivergenceReconcileResult(
                status="needs_attention",
                message=(
                    f"SKIP: mechanical rebase onto local target '{rebase_target}' hit conflicts: "
                    f"{rebase_error}. Resolve the local-target rebase manually before continuing."
                ),
                attention_reason="reconcile-needs-manual-resolution",
            )

        publish_result = publish_rebased_branch(
            worktree_git,
            branch=branch,
            baseline=baseline,
            remote=remote,
        )
        publish_detail = (
            "and pushed with --force-with-lease" if publish_result.pushed else "and verified origin was already aligned"
        )
        return BranchDivergenceReconcileResult(
            status="reconciled",
            message=f"Rebased '{branch}' onto local target '{rebase_target}' {publish_detail}",
        )
    except (GitError, ValueError) as exc:
        return BranchDivergenceReconcileResult(
            status="error",
            message=f"Failed to reconcile divergence for '{branch}': {exc}",
        )
    finally:
        if worktree_path.exists():
            try:
                git.worktree_remove(worktree_path, force=True)
            except GitError:
                shutil.rmtree(worktree_path, ignore_errors=True)


def _is_benign_gza_rewrite_divergence(
    git: Git,
    *,
    branch: str,
    remote_ref: str,
    local_ahead: int,
    remote_ahead: int,
) -> bool:
    """Recognize rewrite-only divergence that is safe to publish directly."""
    if local_ahead <= 0 or remote_ahead <= 0:
        return False

    # Rewritten task branches keep the same patch content while changing commit IDs
    # and often their base ancestry. Publish directly when we can prove either
    # symmetric patch-equivalence or that the remote-only commits are gza-authored
    # dead WIP savepoints superseded by newer local work.
    if git.is_merged(branch, into=remote_ref, use_cherry=True) and git.is_merged(
        remote_ref,
        into=branch,
        use_cherry=True,
    ):
        return True
    return _remote_unique_commits_are_all_wip_savepoints(
        git,
        branch=branch,
        remote_ref=remote_ref,
    )


def _remote_unique_commits_are_all_wip_savepoints(
    git: Git,
    *,
    branch: str,
    remote_ref: str,
) -> bool:
    """Return True when the remote-only side is entirely stale gza WIP savepoints."""
    merge_base_result = git._run("merge-base", branch, remote_ref, check=False)
    if merge_base_result.returncode != 0:
        return False
    merge_base = merge_base_result.stdout.strip()
    if not merge_base:
        return False

    remote_unique_subjects_result = git._run(
        "log",
        "--format=%s",
        f"{merge_base}..{remote_ref}",
        "--not",
        branch,
        check=False,
    )
    if remote_unique_subjects_result.returncode != 0:
        return False
    subjects = [line.strip() for line in remote_unique_subjects_result.stdout.splitlines() if line.strip()]
    return bool(subjects) and all(subject.startswith(WIP_INTERRUPTED_COMMIT_SUBJECT) for subject in subjects)


def _tracking_ref_refresh_command(*, remote: str, branch: str) -> str:
    remote_branch_ref = f"refs/heads/{branch}"
    tracking_ref = f"refs/remotes/{remote}/{branch}"
    return f"git fetch {remote} +{remote_branch_ref}:{tracking_ref}"


@dataclass(frozen=True)
class _PendingSquashBranchReconcile:
    branch: str
    pre_squash_local_oid: str | None
    pre_squash_remote_oid: str | None
    remote: str = "origin"


@dataclass(frozen=True)
class _MergeSingleTaskResult:
    rc: int
    status: str = "merged"
    block_reason: str | None = None
    pending_squash_reconcile: _PendingSquashBranchReconcile | None = None
    created_followups: tuple[DbTask, ...] = ()
    reused_followups: tuple[DbTask, ...] = ()
    created_deferred_blockers: tuple[DbTask, ...] = ()
    reused_deferred_blockers: tuple[DbTask, ...] = ()
    authorized_merge_action: dict[str, Any] | None = None
    authorized_source_ref_sha: str | None = None


class _CappedReviewLifecycleAuthorityChanged(GitError):
    """Raised when live lifecycle planning no longer authorizes a capped merge."""


class _AlreadyMergedLifecycleProofGit:
    """Proxy used to prove capped lifecycle state for mark-only reconciliation."""

    def __init__(self, wrapped: Git) -> None:
        self._wrapped = wrapped

    def __getattr__(self, name: str) -> Any:
        return getattr(self._wrapped, name)

    def is_merged(self, *_args: object, **_kwargs: object) -> bool:
        return False


def _coerce_merge_single_task_result(result: int | _MergeSingleTaskResult) -> _MergeSingleTaskResult:
    if isinstance(result, _MergeSingleTaskResult):
        return result
    return _MergeSingleTaskResult(rc=result)


def _coerce_manual_merge_execution_result(result: ManualMergeExecutionResult) -> _MergeSingleTaskResult:
    return _MergeSingleTaskResult(
        rc=result.rc,
        status=result.status,
        block_reason=result.block_reason,
        pending_squash_reconcile=cast(
            _PendingSquashBranchReconcile | None,
            result.pending_squash_reconcile,
        ),
        created_followups=tuple(result.created_followups or ()),
        reused_followups=tuple(result.reused_followups or ()),
        created_deferred_blockers=tuple(result.created_deferred_blockers or ()),
        reused_deferred_blockers=tuple(result.reused_deferred_blockers or ()),
    )


def _call_git_rebase_with_optional_monitor(
    git: Git,
    branch: str,
    process_monitor_factory: Callable[[subprocess.Popen[str], float], _LongPhaseHeartbeatState] | None,
) -> None:
    if process_monitor_factory is None:
        git.rebase(branch)
        return
    try:
        git.rebase(branch, process_monitor_factory=process_monitor_factory)
    except TypeError as exc:
        if "process_monitor_factory" not in str(exc):
            raise
        git.rebase(branch)


def _call_git_merge_with_optional_monitor(
    git: Git,
    branch: str,
    *,
    squash: bool,
    commit_message: str | None = None,
    process_monitor_factory: Callable[[subprocess.Popen[str], float], _LongPhaseHeartbeatState] | None,
) -> None:
    if process_monitor_factory is None:
        git.merge(branch, squash=squash, commit_message=commit_message)
        return
    try:
        git.merge(
            branch,
            squash=squash,
            commit_message=commit_message,
            process_monitor_factory=process_monitor_factory,
        )
    except TypeError as exc:
        if "process_monitor_factory" not in str(exc):
            raise
        git.merge(branch, squash=squash, commit_message=commit_message)


def _resolve_fresh_merge_source(git: Git, branch: str | None) -> ResolvedMergeSourceRef:
    """Return the local-only merge source ref used by lifecycle merge execution."""
    return resolve_fresh_merge_source(git, branch)


def _task_is_already_merged(store: SqliteTaskStore, task: DbTask) -> bool:
    """Return whether the selected task is already merged."""
    return task_is_merged(store, task)


def _format_needs_attention_line(task: DbTask, action: dict[str, Any]) -> str:
    return format_needs_attention_entry_for_display(
        task,
        action=action,
        prefix=len(task.id or "") + 4,
    )


def _paths_match(left: str | Path, right: Path) -> bool:
    try:
        return Path(left).resolve() == right.resolve()
    except OSError:
        return Path(left) == right


def _find_worktree_entry_for_path(git: Git, path: Path) -> dict | None:
    for entry in git.worktree_list():
        wt_path = entry.get("path")
        if isinstance(wt_path, str) and wt_path and _paths_match(wt_path, path):
            return entry
    return None


def _remove_watch_merge_checkout(git: Git, checkout_path: Path) -> None:
    git.worktree_remove(checkout_path, force=True)
    if checkout_path.exists():
        shutil.rmtree(checkout_path, ignore_errors=True)
    remove_worktree_registration_for_path(git, checkout_path)

    if _find_worktree_entry_for_path(git, checkout_path) is not None:
        raise GitError(f"isolated watch checkout is still registered at '{checkout_path}' after cleanup")


def ensure_watch_main_checkout(
    config: Config,
    git: Git,
    target_branch: str,
    *,
    rebuild: bool = False,
) -> Git:
    """Ensure and refresh the dedicated watch-time merge checkout.

    The checkout is kept on a detached HEAD reset to ``target_branch`` so
    watch-time merges do not move the shared ``refs/heads/<target_branch>``
    ref underneath another worktree.
    """
    checkout_path = config.main_checkout_integration_path

    if rebuild:
        _remove_watch_merge_checkout(git, checkout_path)

    entry = _find_worktree_entry_for_path(git, checkout_path)
    if entry is not None and entry.get("prunable"):
        remove_worktree_registration_for_path(git, checkout_path)
        entry = _find_worktree_entry_for_path(git, checkout_path)

    if entry is None and checkout_path.exists():
        shutil.rmtree(checkout_path, ignore_errors=True)

    if entry is None:
        checkout_path.parent.mkdir(parents=True, exist_ok=True)
        git.worktree_add_existing(checkout_path, target_branch, detach=True)

    parent_env = _owned_git_env(git)
    workspace_git = Git(checkout_path, env=parent_env) if parent_env is not None else Git(checkout_path)
    workspace_git.checkout_detached(target_branch)
    workspace_git.reset_hard(target_branch)
    workspace_git.clean_force()

    current_branch = workspace_git.current_branch()
    if current_branch != "HEAD":
        raise GitError(f"isolated watch checkout expected detached HEAD at '{target_branch}', found '{current_branch}'")
    entry = _find_worktree_entry_for_path(git, checkout_path)
    if entry is None:
        raise GitError(f"isolated watch checkout is not registered at '{checkout_path}'")
    if not entry.get("detached"):
        raise GitError("isolated watch checkout must remain detached from shared branch refs")
    if entry.get("branch") == f"refs/heads/{target_branch}":
        raise GitError(f"isolated watch checkout must not directly check out shared branch '{target_branch}'")
    if workspace_git.has_changes(include_untracked=True):
        raise GitError("isolated watch checkout is dirty after refresh")

    return workspace_git


def cleanup_failed_merge_checkout(workspace_git: Git) -> None:
    """Best-effort cleanup of a conflicted merge checkout."""
    try:
        workspace_git.merge_abort()
    except GitError:
        pass
    workspace_git.reset_hard_head()
    workspace_git.clean_force()
    if workspace_git.has_changes(include_untracked=True):
        raise GitError("merge checkout remains dirty after cleanup")


def _promote_isolated_merge_to_target_branch(
    repo_git: Git,
    merge_git: Git,
    target_branch: str,
    *,
    expected_previous_target_sha: str | None = None,
) -> tuple[str, ...]:
    """Advance the real target-branch ref to the detached isolated merge result.

    Successful watch-time merges are staged in a detached integration checkout,
    but they only count as merged once the shared target branch itself points at
    the detached merge commit. If a real checkout currently has ``target_branch``
    attached, it is hard-reset to the new tip so that checkout stays clean.
    """
    target_ref = f"refs/heads/{target_branch}"
    observed_previous_target_oid = repo_git.rev_parse(target_ref)
    if expected_previous_target_sha is not None and observed_previous_target_oid != expected_previous_target_sha:
        raise GitError(
            f"isolated merge promotion refused because target '{target_branch}' changed after authorization; "
            f"expected {expected_previous_target_sha}, got {observed_previous_target_oid}"
        )
    previous_target_oid = expected_previous_target_sha or observed_previous_target_oid
    merged_head_oid = merge_git.rev_parse("HEAD")
    attached_target_checkout = active_worktree_path_for_branch(repo_git, target_branch)
    parent_env = _owned_git_env(repo_git)
    attached_target_git = (
        Git(attached_target_checkout, env=parent_env)
        if attached_target_checkout is not None and parent_env is not None
        else Git(attached_target_checkout)
        if attached_target_checkout is not None
        else None
    )
    attached_stash_ref: str | None = None
    attached_stash_parked = False
    attached_stash_restored_cleanly = False
    promotion_warnings: list[str] = []

    if attached_target_git is not None and attached_target_git.has_changes(include_untracked=False):
        attached_stash_ref = attached_target_git.stash_push(f"gza isolated merge promotion for {target_branch}")
        attached_stash_parked = attached_stash_ref is not None

    target_ref_updated = False
    try:
        repo_git.update_ref(target_ref, merged_head_oid, previous_target_oid)
        target_ref_updated = True
        if attached_target_git is not None:
            attached_target_git.reset_hard(target_ref)
            if attached_stash_ref is not None:
                if attached_target_git.stash_pop_if_clean(attached_stash_ref):
                    attached_stash_parked = False
                    attached_stash_restored_cleanly = True
                    warning = (
                        f"Isolated merge promotion advanced '{target_branch}' while shared checkout "
                        f"'{attached_target_checkout}' had tracked changes; stashed them as "
                        f"{attached_stash_ref} and restored them onto the new tip"
                    )
                    promotion_warnings.append(warning)
                    logger.warning(
                        "Isolated merge promotion advanced '%s' while shared checkout '%s' "
                        "had tracked changes; stashed them as %s and restored them onto the new tip",
                        target_branch,
                        attached_target_checkout,
                        attached_stash_ref,
                    )
                else:
                    warning = (
                        f"Isolated merge promotion advanced '{target_branch}' while shared checkout "
                        f"'{attached_target_checkout}' had tracked changes; stash {attached_stash_ref} "
                        "could not be restored cleanly and was left parked"
                    )
                    promotion_warnings.append(warning)
                    logger.warning(
                        "Isolated merge promotion advanced '%s' while shared checkout '%s' "
                        "had tracked changes; stash %s could not be restored cleanly and was left parked",
                        target_branch,
                        attached_target_checkout,
                        attached_stash_ref,
                    )
            if not attached_stash_restored_cleanly and attached_target_git.has_changes(include_untracked=False):
                raise GitError(f"shared checkout '{attached_target_checkout}' for '{target_branch}' remained dirty")
        merge_git.reset_hard(target_ref)
    except GitError as exc:
        cleanup_failures: list[str] = []
        if target_ref_updated:
            try:
                repo_git.update_ref(target_ref, previous_target_oid, merged_head_oid)
            except GitError as rollback_error:
                raise _IsolatedPromotionRollbackFailed(
                    f"failed to advance '{target_branch}' after target moved from {previous_target_oid} "
                    f"to {merged_head_oid} and rollback also failed: {rollback_error}",
                    target_branch=target_branch,
                    previous_target_oid=previous_target_oid,
                    candidate_oid=merged_head_oid,
                ) from exc
            if attached_target_git is not None:
                try:
                    attached_target_git.reset_hard(target_ref)
                except GitError as reset_error:
                    checkout_label = attached_target_checkout or str(attached_target_git.repo_dir)
                    cleanup_failures.append(
                        f"shared checkout '{checkout_label}' could not be reset to rolled-back tip: {reset_error}"
                    )
        if attached_target_git is not None and attached_stash_ref is not None and attached_stash_parked:
            try:
                if attached_target_git.stash_pop_if_clean(attached_stash_ref):
                    attached_stash_parked = False
                else:
                    cleanup_failures.append(
                        f"stash {attached_stash_ref} could not be restored after rollback and remains parked"
                    )
            except GitError as stash_error:
                cleanup_failures.append(
                    f"stash {attached_stash_ref} could not be restored after rollback: {stash_error}"
                )
        try:
            merge_git.reset_hard(target_ref)
        except GitError as reset_error:
            cleanup_failures.append(
                f"isolated merge checkout could not be reset after promotion failure: {reset_error}"
            )
        message = f"failed to advance shared branch '{target_branch}' from isolated merge: {exc}"
        if cleanup_failures:
            message = f"{message}; cleanup issues: {'; '.join(cleanup_failures)}"
        raise GitError(message) from exc
    return tuple(promotion_warnings)


def _classify_isolated_promotion_rollback_failed(
    repo_git: Git,
    exc: _IsolatedPromotionRollbackFailed,
) -> _IsolatedPromotionRollbackClassification:
    target_ref = f"refs/heads/{exc.target_branch}"
    target_oid = _rev_parse_if_exists_if_supported(repo_git, target_ref)
    if target_oid is None:
        try:
            target_oid = repo_git.rev_parse(target_ref)
        except GitError:
            target_oid = None
    if target_oid == exc.candidate_oid:
        return _IsolatedPromotionRollbackClassification(
            status="isolated_post_promotion_rollback_failed",
            block_reason=(
                f"isolated promotion rollback failed after target advanced to verified candidate "
                f"{exc.candidate_oid}: {exc}"
            ),
            target_oid=target_oid,
            target_at_candidate=True,
        )
    if target_oid == exc.previous_target_oid:
        return _IsolatedPromotionRollbackClassification(
            status="isolated_merge_failed",
            block_reason=(
                f"isolated promotion failed and target returned to previous tip {exc.previous_target_oid}: {exc}"
            ),
            target_oid=target_oid,
        )
    observed = target_oid or "unknown"
    return _IsolatedPromotionRollbackClassification(
        status="isolated_promotion_rollback_failed_target_uncertain",
        block_reason=(
            f"isolated promotion rollback failed and target '{exc.target_branch}' is at {observed}; "
            f"expected previous tip {exc.previous_target_oid} or candidate {exc.candidate_oid}: {exc}"
        ),
        target_oid=target_oid,
    )


def _advance_uses_iterate(config: Config) -> bool:
    """Whether advance should launch implement work through the iterate loop."""
    return getattr(config, "advance_mode", "default") == "iterate"


def _classify_squash_reconcile_push_failure(exc: GitError) -> str:
    message = str(exc).lower()
    if "stale info" in message or "fetch first" in message:
        return "failed_push_rejected"
    return "failed_push_unavailable"


def _rev_parse_if_exists_if_supported(git: Git, ref: str) -> str | None:
    rev_parse_if_exists = getattr(git, "rev_parse_if_exists", None)
    if callable(rev_parse_if_exists):
        return rev_parse_if_exists(ref)
    return None


def _rev_parse_if_supported(git: Git, ref: str) -> str | None:
    rev_parse = getattr(git, "rev_parse", None)
    if callable(rev_parse):
        return rev_parse(ref)
    return None


def _candidate_verify_promotion_proof(
    git: Git,
    candidate_verify: CandidateIntegrationVerifyCheck,
) -> _CandidateVerifyPromotionProof:
    verified_head_sha = _rev_parse_if_exists_if_supported(git, "HEAD") or _rev_parse_if_supported(git, "HEAD")
    verified_tree_fingerprint = candidate_verify.evidence.tree_fingerprint
    live_tree_fingerprint = _compute_tree_fingerprint(git) if verified_tree_fingerprint is not None else None

    if (
        candidate_verify.evidence.verify_status == "passed"
        and candidate_verify.evidence.head_sha
        and verified_head_sha == candidate_verify.evidence.head_sha
        and verified_tree_fingerprint
        and live_tree_fingerprint == verified_tree_fingerprint
    ):
        return _CandidateVerifyPromotionProof(
            blocked_status="blocked_candidate_verify",
            block_reason="candidate verify blocked isolated promotion",
            verified_head_sha=verified_head_sha,
            verified_tree_fingerprint=verified_tree_fingerprint,
        )

    message = "candidate verify blocked isolated promotion"
    blocked_status: Literal["blocked_candidate_verify", "blocked_candidate_verify_unavailable"] = (
        "blocked_candidate_verify"
    )
    if candidate_verify.classification == "unavailable":
        message = "candidate verify unavailable; refusing to promote without exact host proof"
        blocked_status = "blocked_candidate_verify_unavailable"
    elif candidate_verify.evidence.failing_phase:
        message = (
            "candidate verify red; refusing to promote "
            f"while phase `{candidate_verify.evidence.failing_phase}` is failing"
        )
    elif candidate_verify.evidence.failure:
        message = f"candidate verify blocked isolated promotion: {candidate_verify.evidence.failure}"
    elif not candidate_verify.evidence.head_sha:
        message = "candidate verify blocked isolated promotion: missing verified head proof"
    elif verified_head_sha != candidate_verify.evidence.head_sha:
        message = "candidate verify blocked isolated promotion: verified head did not match isolated checkout"
    elif not verified_tree_fingerprint:
        message = "candidate verify blocked isolated promotion: missing verified tree fingerprint"
    elif live_tree_fingerprint != verified_tree_fingerprint:
        message = "candidate verify blocked isolated promotion: verified tree did not match isolated checkout"

    return _CandidateVerifyPromotionProof(
        blocked_status=blocked_status,
        block_reason=message,
    )


def _capture_pre_squash_reconcile_state(
    git: Git,
    *,
    branch: str,
    remote: str = "origin",
) -> _PendingSquashBranchReconcile:
    return _PendingSquashBranchReconcile(
        branch=branch,
        pre_squash_local_oid=_rev_parse_if_exists_if_supported(git, f"refs/heads/{branch}"),
        pre_squash_remote_oid=_rev_parse_if_exists_if_supported(git, f"refs/remotes/{remote}/{branch}"),
        remote=remote,
    )


def _reconcile_squash_merged_branch_with_origin(
    git: Git,
    *,
    branch: str,
    squash_oid: str,
    pre_squash_local_oid: str | None,
    pre_squash_remote_oid: str | None,
    remote: str = "origin",
) -> SquashBranchReconcileResult:
    if pre_squash_remote_oid is None:
        return SquashBranchReconcileResult(
            status="skipped_no_remote_tracking_ref",
            branch=branch,
            remote=remote,
        )

    source_ref = "HEAD"
    if pre_squash_local_oid is not None:
        try:
            git.update_ref(f"refs/heads/{branch}", squash_oid, pre_squash_local_oid)
        except GitError as exc:
            return SquashBranchReconcileResult(
                status="failed_local_ref_update",
                branch=branch,
                remote=remote,
                reason=str(exc),
                expected_remote_oid=pre_squash_remote_oid,
            )
        source_ref = f"refs/heads/{branch}"

    try:
        git.push_ref_force_with_lease(
            source_ref,
            branch,
            remote=remote,
            expected_remote_oid=pre_squash_remote_oid,
        )
    except GitError as exc:
        return SquashBranchReconcileResult(
            status=_classify_squash_reconcile_push_failure(exc),
            branch=branch,
            remote=remote,
            reason=str(exc),
            manual_source_ref=source_ref,
            expected_remote_oid=pre_squash_remote_oid,
        )

    try:
        git.update_ref(f"refs/remotes/{remote}/{branch}", squash_oid)
    except GitError as exc:
        return SquashBranchReconcileResult(
            status="failed_remote_tracking_ref_update",
            branch=branch,
            remote=remote,
            reason=str(exc),
            manual_source_ref=source_ref,
            expected_remote_oid=pre_squash_remote_oid,
        )

    return SquashBranchReconcileResult(
        status="updated",
        branch=branch,
        remote=remote,
    )


def _print_squash_reconcile_result(
    result: SquashBranchReconcileResult,
    *,
    suppress_success: bool = False,
) -> None:
    if result.status == "skipped_no_remote_tracking_ref":
        return
    if result.status == "updated":
        if suppress_success:
            return
        print(f"✓ Reconciled {result.remote}/{result.branch} to the squash merge commit")
        return

    reason = result.reason or "unknown error"
    if result.status == "failed_remote_tracking_ref_update":
        tracking_ref = f"refs/remotes/{result.remote}/{result.branch}"
        print(
            "Warning: Squash merge landed and the remote push succeeded, "
            f"but the local tracking ref '{tracking_ref}' could not be updated: {reason}"
        )
        print(
            f"Refresh the local tracking ref with: {_tracking_ref_refresh_command(remote=result.remote, branch=result.branch)}"
        )
        return

    print(f"Warning: Squash merge landed, but {result.remote}/{result.branch} could not be reconciled: {reason}")
    if result.status == "failed_push_rejected":
        print(
            f"{result.remote}/{result.branch} changed since it was last observed; "
            "reconcile it manually before relying on watch."
        )
    if result.status == "failed_local_ref_update":
        print(
            f"Reconcile the local branch '{result.branch}' first, or push a ref that is "
            "known to point at the squash merge commit before repairing origin."
        )
    if result.manual_source_ref and result.expected_remote_oid:
        remote_branch_ref = f"refs/heads/{result.branch}"
        print(
            "Manual repair: "
            f"git push --force-with-lease={remote_branch_ref}:{result.expected_remote_oid} "
            f"{result.remote} {result.manual_source_ref}:{remote_branch_ref}"
        )


def _spawn_prepared_background_iterate(
    args: argparse.Namespace,
    config: Config,
    impl_task: DbTask,
    *,
    max_iterations: int,
    auto_iterate: bool = False,
    quiet: bool = False,
) -> int:
    pending_recovery_mode = resolve_pending_recovery_execution_mode(impl_task)
    try:
        permit = launch_permit(config, get_store(config))
    except MaxConcurrentTasksError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    prepared_task = _prepare_task_for_immediate_execution(
        config,
        impl_task,
        rollback_on_failure=False,
    )
    if prepared_task is None:
        permit.release()
        return 1
    if prepared_task.id is not None:
        reserve_task_launch_permit(str(prepared_task.id), permit)
    return _spawn_background_iterate_worker(
        args,
        config,
        prepared_task,
        max_iterations=max_iterations,
        auto_iterate=auto_iterate,
        quiet=quiet,
        prepared_task_id=prepared_task.id,
        prepared_resume=pending_recovery_mode == "resume",
        prepared_phase="preloop",
    )


def _collect_advance_completed_tasks(
    store: SqliteTaskStore,
    *,
    advance_type: str | None = None,
    target_branch: str | None = None,
) -> tuple[list[DbTask], set[str]]:
    """Collect completed tasks eligible for advance-style action planning.

    Returns completed unmerged tasks and also completed plan tasks without
    implement children (except when filtering to implement-only mode).
    """
    impl_based_on_ids = collect_non_dropped_implement_source_ids(store.get_all())
    if store.supports_merge_units():
        tasks = []
        seen_unit_ids: set[str] = set()
        seen_task_ids: set[str] = set()
        for unit in store._get_unmerged_merge_units_with_legacy_fallback():
            if unit.id in seen_unit_ids:
                continue
            seen_unit_ids.add(unit.id)
            if isinstance(target_branch, str) and unit.target_branch != target_branch:
                continue
            owner = store.resolve_merge_unit_owner_task(unit, require_actionable=True)
            if owner is None or owner.status != "completed" or owner.id is None or owner.id in seen_task_ids:
                continue
            tasks.append(owner)
            seen_task_ids.add(owner.id)
    else:
        all_unmerged = store.get_unmerged()
        tasks = [t for t in all_unmerged if t.status == "completed"]
        if isinstance(target_branch, str):
            filtered_tasks: list[DbTask] = []
            for task in tasks:
                unit_for_task = store.resolve_merge_unit_for_task(task.id) if task.id is not None else None
                if unit_for_task is None:
                    unit_for_task = store.get_or_create_merge_unit_for_task(task)
                if unit_for_task is not None and unit_for_task.target_branch != target_branch:
                    continue
                filtered_tasks.append(task)
            tasks = filtered_tasks

    if advance_type != "implement":
        completed_plans = store.get_history(limit=None, status="completed", task_type="plan")
        existing_ids = {t.id for t in tasks}
        for plan_task in completed_plans:
            if plan_task.id in impl_based_on_ids:
                continue
            if plan_task.id in existing_ids:
                continue
            tasks.append(plan_task)

    if advance_type == "plan":
        tasks = [t for t in tasks if t.task_type == "plan"]
    elif advance_type == "implement":
        tasks = [t for t in tasks if t.task_type == "implement"]

    return tasks, impl_based_on_ids


def _require_default_branch(
    git: Git,
    current_branch: str,
    command: str,
    *,
    to_stderr: bool = False,
) -> bool:
    """Enforce that a command is being run from the repo's default branch.

    Returns True if on default branch; prints an error and returns False otherwise.
    """
    default = git.default_branch()
    if current_branch != default:
        print(
            f"Error: `gza {command}` must be run from the default branch "
            f"'{default}' (currently on '{current_branch}').",
            file=sys.stderr if to_stderr else sys.stdout,
        )
        return False
    return True


def _auto_squash_commit_count(
    config: Config,
    git: Git,
    source_ref: str | None,
    target_branch: str,
) -> int | None:
    """Return commit count when task should auto-squash, otherwise None."""
    if config.merge_squash_threshold <= 0 or not source_ref:
        return None
    commit_count = git.count_commits_ahead(source_ref, target_branch)
    if commit_count < config.merge_squash_threshold:
        return None
    return commit_count


def _build_auto_merge_args(
    config: Config,
    git: Git,
    source_ref: str | None,
    target_branch: str,
) -> argparse.Namespace:
    """Build merge args with auto-squash behavior aligned across entrypoints."""
    should_squash = _auto_squash_commit_count(config, git, source_ref, target_branch) is not None
    return argparse.Namespace(
        rebase=False,
        squash=should_squash,
        delete=False,
        mark_only=False,
        no_followups=True,
        remote=False,
        resolve=False,
    )


def _task_merge_unit_state(store: SqliteTaskStore, task: DbTask, *, target_branch: str | None) -> str | None:
    if task.id is not None:
        unit = store.resolve_merge_unit_for_task(task.id)
        if unit is not None:
            return unit.state
    return task.merge_status


def _resolve_advance_target_branch(
    store: SqliteTaskStore,
    git: Git,
    *,
    task: DbTask | None,
) -> str:
    if task is not None and task.id is not None:
        unit = store.resolve_merge_unit_for_task(task.id)
        if unit is not None and unit.target_branch:
            return unit.target_branch
        return store.default_merge_target(strict=True)
    return git.current_branch()


def _resolve_merge_target_task(
    store: SqliteTaskStore,
    task_id: str,
    target_branch: str,
) -> DbTask | None:
    return resolve_merge_target_task(store, task_id, target_branch)


def _resolve_merge_subject(
    store: SqliteTaskStore,
    git: Git,
    task_id: str,
    *,
    target_branch: str,
) -> _ResolvedMergeSubject | None:
    return resolve_merge_subject(store, git, task_id, target_branch=target_branch)


def _resolve_merge_subject_query_only(
    store: SqliteTaskStore,
    git: Git,
    task_id: str,
    *,
    target_branch: str,
) -> _ResolvedMergeSubject | None:
    """Resolve a merge subject without creating or backfilling merge-unit state."""
    return resolve_merge_subject_query_only(store, git, task_id, target_branch=target_branch)


def _merge_option_relationship_error(args: argparse.Namespace) -> str | None:
    removed_flags = [flag for flag in ("rebase", "remote", "resolve") if bool(getattr(args, flag, False))]
    if removed_flags:
        rendered = ", ".join(f"--{flag}" for flag in removed_flags)
        return (
            f"Error: removed merge option(s) {rendered}; use `gza rebase <task-id> --run` "
            "for standalone rebases or `gza land <task-id>` for full landing orchestration"
        )
    if getattr(args, "ignore_verify_gate", False) and not getattr(args, "force", False):
        return "Error: --ignore-verify-gate requires --force"
    return None


def _merge_single_task(
    task_id: str,
    config: Config,
    store,
    git: Git,
    args: argparse.Namespace,
    current_branch: str,
    *,
    merge_preflight_ref: str | None = None,
    merge_source: str = MERGE_SOURCE_MANUAL,
    quiet_mechanics: bool = False,
    materialize_side_effects: bool = True,
    heartbeat_threshold_seconds: int | None = None,
    heartbeat_interval_seconds: int | None = None,
    on_heartbeat: LongPhaseHeartbeat | None = None,
    before_irreversible_side_effect: Callable[[Mapping[str, Any] | None], "_MergeActionResult | None"] | None = None,
    resolved_subject: _ResolvedMergeSubject | None = None,
) -> _MergeSingleTaskResult:
    """Merge a single task's branch."""
    option_error = _merge_option_relationship_error(args)
    if option_error is not None:
        print(option_error)
        return _MergeSingleTaskResult(rc=1)

    def _process_monitor_factory(process: subprocess.Popen[str], started_at: float) -> _LongPhaseHeartbeatState:
        return _LongPhaseHeartbeatState(
            process=process,
            started_at=started_at,
            threshold_seconds=max(0, int(heartbeat_threshold_seconds or 0)),
            interval_seconds=max(1, int(heartbeat_interval_seconds or 1)),
            on_heartbeat=on_heartbeat,
        )

    process_monitor_factory = _process_monitor_factory if on_heartbeat is not None else None
    target_branch = git.default_branch()
    resolved = resolved_subject or _resolve_merge_subject(store, git, task_id, target_branch=target_branch)
    if resolved is None:
        print(f"Error: Task {task_id} not found")
        return _MergeSingleTaskResult(rc=1)
    execution_task = resolved.execution_task
    merge_subject = resolved.merge_subject
    assert merge_subject.id is not None
    merge_branch = resolved.merge_branch or execution_task.branch
    merge_source_ref = resolved.merge_source_ref
    merge_unit_id = resolved.merge_unit_id
    created_followups: list[DbTask] = []
    reused_followups: list[DbTask] = []

    # Validate task state
    status_error = _merge_execution_status_error(merge_subject.id, execution_task)
    if status_error is not None:
        print(f"Error: {status_error}")
        return _MergeSingleTaskResult(rc=1)
    if resolved.merge_source_warning:
        print(f"Error: {resolved.merge_source_warning}")
        return _MergeSingleTaskResult(rc=1)

    if not merge_branch or not merge_source_ref:
        print(f"Error: Task {merge_subject.id} has no resolvable merge source")
        return _MergeSingleTaskResult(rc=1)
    if resolved.merge_source_warning:
        print(f"Error: {resolved.merge_source_warning}")
        return _MergeSingleTaskResult(rc=1)

    def _materialize_manual_followups_before_state() -> _MergeSingleTaskResult | None:
        nonlocal created_followups, reused_followups
        if not materialize_side_effects or getattr(args, "no_followups", False):
            return None
        try:
            created_followups, reused_followups = _materialize_merge_followups(store, config, merge_subject)
        except Exception as exc:
            return _merge_single_side_effect_failure_result(exc)
        _print_followup_tasks(merge_subject, created_followups, reused_followups)
        return None

    # Handle --mark-only flag
    if args.mark_only:
        # Check for conflicting flags
        if getattr(args, "squash", False) or getattr(args, "delete", False) or getattr(args, "force", False):
            print("Error: --mark-only cannot be used with --squash, --delete, or --force")
            return _MergeSingleTaskResult(rc=1)

        if materialize_side_effects:
            deferred_blockers = _materialize_merge_deferred_blockers(
                store,
                config,
                merge_subject,
                defer_blockers=getattr(args, "defer_blockers", False),
            )
            if deferred_blockers is None:
                return _MergeSingleTaskResult(rc=1)
            _print_deferred_blocker_tasks(merge_subject, deferred_blockers)
            if deferred_blockers[0] or deferred_blockers[1]:
                merge_source = manual_force_merge_source(merge_source)

        if before_irreversible_side_effect is not None:
            side_effect_result = before_irreversible_side_effect(None)
            if side_effect_result is not None:
                return _MergeSingleTaskResult(
                    rc=side_effect_result.rc,
                    status=side_effect_result.status,
                    block_reason=side_effect_result.block_reason,
                    created_followups=tuple(side_effect_result.created_followups),
                    reused_followups=tuple(side_effect_result.reused_followups),
                    created_deferred_blockers=tuple(side_effect_result.created_deferred_blockers),
                    reused_deferred_blockers=tuple(side_effect_result.reused_deferred_blockers),
                )

        followup_error = _materialize_manual_followups_before_state()
        if followup_error is not None:
            return followup_error

        mark_merge_subject_merged(
            store,
            merge_subject=merge_subject,
            merge_unit_id=merge_unit_id,
            merge_source=merge_source,
        )
        print(f"✓ Marked task {merge_subject.id} as merged (branch '{merge_branch}' preserved)")
        return _MergeSingleTaskResult(
            rc=0,
            created_followups=tuple(created_followups),
            reused_followups=tuple(reused_followups),
        )

    direct_prerequisite_types = {"verify_gate", "reconcile_verify_gate_evidence"}
    planned_action = determine_next_action(
        config,
        store,
        git,
        execution_task,
        target_branch,
        selected_for_merge=True,
    )
    for _prerequisite_attempt in range(len(direct_prerequisite_types) + 1):
        if planned_action.get("type") not in direct_prerequisite_types:
            break
        prerequisite_context = AdvanceActionExecutionContext(
            store=store,
            trigger_source="manual",
            dry_run=False,
            max_resume_attempts=getattr(config, "max_resume_attempts", 0),
            use_iterate_for_create_implement=False,
            use_iterate_for_needs_rebase=False,
            prepare_task_for_background_start=lambda task, _rollback: task,
            prepare_create_review=lambda _task: (_ for _ in ()).throw(AssertionError("unused")),
            create_resume_task=lambda _task: (_ for _ in ()).throw(AssertionError("unused")),
            create_rebase_task=lambda _task: (_ for _ in ()).throw(AssertionError("unused")),
            create_implement_task=lambda _task: (_ for _ in ()).throw(AssertionError("unused")),
            spawn_worker=lambda _task, _kind: (_ for _ in ()).throw(AssertionError("unused")),
            spawn_resume_worker=lambda _task, _kind: (_ for _ in ()).throw(AssertionError("unused")),
            spawn_iterate_worker=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unused")),
            config=config,
            git=git,
        )
        prerequisite_result = execute_advance_action(
            task=execution_task,
            action=planned_action,
            context=prerequisite_context,
        )
        message = (
            prerequisite_result.success_message or prerequisite_result.message or planned_action.get("description", "")
        )
        if message:
            print(message)
        if prerequisite_result.status != "success":
            return _MergeSingleTaskResult(rc=1)
        planned_action = determine_next_action(
            config,
            store,
            git,
            execution_task,
            target_branch,
            selected_for_merge=True,
        )
    else:
        print("Error: merge prerequisite loop did not converge")
        return _MergeSingleTaskResult(rc=1)
    effective_merge_source = merge_source
    authorized_merge_action: dict[str, Any] | None = None
    pregate_deferred_blockers: tuple[list[DbTask], list[DbTask]] | None = None
    pregate_deferred_blockers_printed = False

    if planned_action.get("type") not in {"merge", "merge_with_followups"}:
        if is_current_red_verify_gate_action(planned_action):
            if not (getattr(args, "force", False) and getattr(args, "ignore_verify_gate", False)):
                description = str(planned_action.get("description") or "verify gate is red")
                print(f"Error: {description}. Red verify gates require --force --ignore-verify-gate.")
                return _MergeSingleTaskResult(rc=1)
            effective_merge_source = manual_force_merge_source(merge_source)
            proof = planned_action["red_verify_gate_proof"]
            failing_head = str(proof["reviewed_head_sha"])
            verify_command = str(proof["verify_command"])
            description = str(planned_action.get("description") or "verify gate is red")
            print(
                "Warning: Forcing merge despite red verify gate: "
                f"{description}; failing epoch head={failing_head}; verify command={verify_command!r}"
            )
        elif (
            getattr(args, "defer_blockers", False)
            and materialize_side_effects
            and _is_review_changes_requested_improve_action(
                planned_action,
                store=store,
                merge_subject=merge_subject,
            )
        ):
            pregate_deferred_blockers = _materialize_merge_deferred_blockers(
                store,
                config,
                merge_subject,
                defer_blockers=True,
            )
            if pregate_deferred_blockers is None:
                return _MergeSingleTaskResult(rc=1)
            if not (pregate_deferred_blockers[0] or pregate_deferred_blockers[1]):
                print(
                    "Error: --defer-blockers could not create or reuse any deferred blocker tasks "
                    "for the selected review gate. Refusing to guess."
                )
                return _MergeSingleTaskResult(rc=1)
            _print_deferred_blocker_tasks(merge_subject, pregate_deferred_blockers)
            pregate_deferred_blockers_printed = True
            effective_merge_source = manual_force_merge_source(merge_source)
            description = str(planned_action.get("description") or "merge is blocked")
            print(f"Warning: Forcing merge despite lifecycle gate: {description}")
        elif is_red_verify_gate_family_action(planned_action):
            description = str(planned_action.get("description") or "verify gate proof is unavailable")
            action_type = str(planned_action.get("type") or "")
            if action_type in {"run_verify_fix", "wait_verify_fix"}:
                print(
                    "Error: "
                    f"{description}. Live verify-fix tasks cannot be bypassed; wait for the existing "
                    "same-epoch verify_fix task to complete before forcing merge."
                )
                return _MergeSingleTaskResult(rc=1)
            proof = planned_action.get("red_verify_gate_proof")
            if (
                isinstance(proof, dict)
                and proof.get("phase") == "pre_merge"
                and isinstance(proof.get("reviewed_head_sha"), str)
                and proof.get("reviewed_head_sha")
                and isinstance(proof.get("verify_command"), str)
                and proof.get("verify_command")
            ):
                print(
                    "Error: "
                    f"{description}. This current red verify-gate recovery state cannot be bypassed; "
                    "follow the remediation above before forcing merge."
                )
                return _MergeSingleTaskResult(rc=1)
            print(
                "Error: "
                f"{description}. Red verify gate bypass requires current failed pre-merge proof; "
                "rerun or repair the verify gate evidence before forcing merge."
            )
            return _MergeSingleTaskResult(rc=1)
        elif getattr(args, "force", False) and classify_advance_action(planned_action) == "needs_attention":
            effective_merge_source = manual_force_merge_source(merge_source)
            description = str(planned_action.get("description") or "merge is blocked")
            print(f"Warning: Forcing merge despite lifecycle gate: {description}")
        else:
            description = str(planned_action.get("description") or "merge is blocked")
            print(f"Error: {description}")
            return _MergeSingleTaskResult(rc=1)

    if planned_action.get("type") in {"merge", "merge_with_followups"}:
        authorized_merge_action = dict(planned_action)
    authorized_source_ref_sha: str | None = None
    if authorized_merge_action is not None and _merge_finalization_family_for_action(authorized_merge_action) is not None:
        try:
            authorized_source_ref_sha = _require_ref_sha_for_merge_finalization_proof(
                git,
                merge_source_ref,
                label="authorized source",
            )
        except Exception as exc:
            block_reason = _pre_promotion_merge_refusal_reason(
                phase="pre-merge authorized source proof",
                exc=exc,
            )
            print(f"Error: {block_reason}")
            return _MergeSingleTaskResult(
                rc=1,
                status="merge_source_ref_unavailable",
                block_reason=block_reason,
            )
    merge_preflight_target = merge_preflight_ref or current_branch
    expected_preflight_target_sha: str | None = None
    if authorized_merge_action is not None and _merge_finalization_family_for_action(authorized_merge_action) is not None:
        try:
            expected_preflight_target_sha = _require_ref_sha_for_merge_finalization_proof(
                git,
                merge_preflight_target,
                label="preflight target",
            )
        except Exception as exc:
            block_reason = _pre_promotion_merge_refusal_reason(
                phase="pre-merge target proof",
                exc=exc,
            )
            print(f"Error: {block_reason}")
            return _MergeSingleTaskResult(
                rc=1,
                status="merge_target_ref_unavailable",
                block_reason=block_reason,
            )

    def _build_commit_message(subject: DbTask) -> str:
        assert subject.id is not None, "Task ID must be set before squash merge commit"
        return build_task_commit_message(
            subject.prompt,
            task_id=subject.id,
            task_slug=subject.slug,
            subject_prefix="Squash merge: ",
        )

    def _reconcile_squash(git_to_reconcile: Git, branch: str, squash_oid: str, state: Any) -> SquashBranchReconcileResult:
        pending = state if isinstance(state, _PendingSquashBranchReconcile) else None
        return _reconcile_squash_merged_branch_with_origin(
            git_to_reconcile,
            branch=branch,
            squash_oid=squash_oid,
            pre_squash_local_oid=pending.pre_squash_local_oid if pending is not None else None,
            pre_squash_remote_oid=pending.pre_squash_remote_oid if pending is not None else None,
            remote=pending.remote if pending is not None else "origin",
        )

    def _print_followups(subject: DbTask, followups: tuple[list[DbTask], list[DbTask]]) -> None:
        created_followups, reused_followups = followups
        for followup_task in created_followups:
            print(f"FOLLOW {followup_task.id} created from {subject.id}")
        for followup_task in reused_followups:
            print(f"FOLLOW {followup_task.id} reused from {subject.id}")

    def _run_before_irreversible_side_effect(_subject: DbTask) -> ManualMergeExecutionResult | None:
        if before_irreversible_side_effect is None:
            return None
        side_effect_result = before_irreversible_side_effect(authorized_merge_action)
        if side_effect_result is None:
            return None
        return ManualMergeExecutionResult(
            rc=side_effect_result.rc,
            status=side_effect_result.status,
            block_reason=side_effect_result.block_reason,
            created_followups=list(side_effect_result.created_followups),
            reused_followups=list(side_effect_result.reused_followups),
            created_deferred_blockers=list(side_effect_result.created_deferred_blockers),
            reused_deferred_blockers=list(side_effect_result.reused_deferred_blockers),
        )

    result = execute_manual_merge(
        ManualMergeExecutionRequest(
            store=store,
            config=config,
            git=git,
            merge_subject=merge_subject,
            merge_unit_id=merge_unit_id,
            merge_branch=merge_branch,
            merge_source_ref=merge_source_ref,
            current_branch=current_branch,
            merge_source=effective_merge_source,
            merge_preflight_target=merge_preflight_target,
            squash=getattr(args, "squash", False),
            delete_branch=getattr(args, "delete", False),
            no_followups=getattr(args, "no_followups", False),
            quiet_mechanics=quiet_mechanics,
            materialize_side_effects=materialize_side_effects,
            authorized_source_ref_sha=authorized_source_ref_sha,
            expected_preflight_target_sha=expected_preflight_target_sha,
            pre_materialized_deferred_blockers=pregate_deferred_blockers,
            pre_materialized_deferred_blockers_printed=pregate_deferred_blockers_printed,
            process_monitor_factory=process_monitor_factory,
        ),
        ManualMergeExecutionHooks(
            build_commit_message=_build_commit_message,
            capture_pre_squash_reconcile_state=lambda git_to_capture, branch: _capture_pre_squash_reconcile_state(
                git_to_capture,
                branch=branch,
            ),
            reconcile_squash_merge=_reconcile_squash,
            print_squash_reconcile_result=lambda reconcile_result, suppress_success: _print_squash_reconcile_result(
                reconcile_result,
                suppress_success=suppress_success,
            ),
            rev_parse_head=lambda git_to_parse: _rev_parse_if_supported(git_to_parse, "HEAD"),
            materialize_deferred_blockers=lambda subject: _materialize_merge_deferred_blockers(
                store,
                config,
                subject,
                defer_blockers=getattr(args, "defer_blockers", False),
            ),
            print_deferred_blockers=_print_deferred_blocker_tasks,
            materialize_followups=lambda subject: _materialize_merge_followups(store, config, subject),
            print_followups=_print_followups,
            before_irreversible_side_effect=_run_before_irreversible_side_effect,
        ),
    )
    coerced = _coerce_manual_merge_execution_result(result)
    return replace(
        coerced,
        authorized_merge_action=authorized_merge_action,
        authorized_source_ref_sha=authorized_source_ref_sha,
    )


def cmd_merge(args: argparse.Namespace) -> int:
    """Merge task branches into the current branch."""
    option_error = _merge_option_relationship_error(args)
    if option_error is not None:
        print(option_error)
        return 1

    config = Config.load(args.project_dir)
    runtime_context = RuntimeExecutionContext.from_config(config)
    store = get_store(config)
    git = _git_from_runtime_context(config.project_dir, runtime_context)

    # Get current branch once
    current_branch = git.current_branch()
    default = git.default_branch()
    print(f"On branch {current_branch}")

    # --mark-only is a DB-only escape hatch for users who merge manually;
    # it does not run git operations so the default-branch rule does not apply.
    if getattr(args, "mark_only", False):
        if current_branch != default:
            print(f"Note: --mark-only on non-default branch '{current_branch}' (default is '{default}')")
    else:
        if not _require_default_branch(git, current_branch, "merge"):
            return 1

    # Determine the list of task IDs to merge
    task_ids = [resolve_id(config, tid) for tid in args.task_ids]

    use_all = getattr(args, "all", False)
    if use_all:
        seen_ids = set(task_ids)
        for task in reversed(store.get_unmerged()):
            if task.id is None or task.id in seen_ids or not task.branch:
                continue
            if task.status not in ("completed", "unmerged"):
                continue
            if git.is_merged(task.branch, current_branch):
                continue
            task_ids.append(task.id)
            seen_ids.add(task.id)
        if not task_ids:
            print("No unmerged done tasks found")
            return 0
    elif not task_ids:
        print("Error: either provide task_id(s) or use --all to merge all unmerged done tasks")
        return 1

    # Deduplicate selected task rows by active merge unit/branch owner.
    deduped_task_ids: list[str] = []
    seen_units: set[str] = set()
    seen_tasks: set[str] = set()
    for raw_task_id in task_ids:
        resolved = _resolve_merge_target_task(store, raw_task_id, default)
        if resolved is None or resolved.id is None:
            print(f"Error: Task {raw_task_id} not found")
            return 1
        resolved_id = resolved.id
        resolved_unit = store.resolve_merge_unit_for_task(resolved_id)
        if resolved_unit is not None:
            if resolved_unit.id in seen_units:
                continue
            seen_units.add(resolved_unit.id)
        if resolved_id in seen_tasks:
            continue
        seen_tasks.add(resolved_id)
        deduped_task_ids.append(resolved_id)
    task_ids = deduped_task_ids

    # Track success/failure
    merged_tasks = []
    failed_task_id = None

    # Merge each task in sequence
    for task_id in task_ids:
        if use_all:
            print(f"Merging task {task_id}...")
        result = _coerce_merge_single_task_result(_merge_single_task(task_id, config, store, git, args, current_branch))

        if result.rc != 0:
            # Merge failed, stop processing
            failed_task_id = task_id
            break

        merged_tasks.append(task_id)
        if use_all:
            print()

    # Report results
    if merged_tasks:
        print(f"\n✓ Successfully merged {len(merged_tasks)} task(s): {', '.join(str(tid) for tid in merged_tasks)}")

    if failed_task_id is not None:
        remaining = [tid for tid in task_ids if tid not in merged_tasks and tid != failed_task_id]
        if remaining:
            print(
                f"⚠ Stopped at task {failed_task_id}. Remaining tasks not processed: {', '.join(str(tid) for tid in remaining)}"
            )
        return 1

    return 0


def _resolve_runtime_skill_dir(
    project_dir: Path,
    provider: str,
    *,
    env: Mapping[str, str] | None = None,
) -> tuple[str, Path] | None:
    """Resolve runtime skill directory for a provider."""
    runtime_env = os.environ if env is None else env
    target_map = {
        "claude": ("claude", project_dir / ".claude" / "skills"),
        "codex": ("codex", provider_home_from_env("codex", env=runtime_env) / "skills"),
        "gemini": ("gemini", provider_home_from_env("gemini", env=runtime_env) / "skills"),
    }
    return target_map.get(provider)


def ensure_skill(
    skill_name: str,
    provider: str,
    project_dir: Path,
    *,
    env: Mapping[str, str] | None = None,
) -> bool:
    """Ensure a skill is available for the provider runtime, installing if missing.

    Resolves the runtime skill directory for the provider, checks whether the
    skill file exists, and if not attempts to auto-install it from the bundled
    package via skills_utils.copy_skill.

    Args:
        skill_name: Name of the skill to ensure (e.g. 'gza-rebase').
        provider: Provider name ('claude', 'codex', or 'gemini').
        project_dir: Project directory used to resolve the runtime skill path.

    Returns:
        True if the skill is available after the check/install, False otherwise.
    """
    from ..skills_utils import copy_skill

    runtime = _resolve_runtime_skill_dir(project_dir, provider, env=env)
    if not runtime:
        return False
    _, runtime_dir = runtime
    skill_path = runtime_dir / skill_name / "SKILL.md"
    if skill_path.exists():
        return True
    # Skill missing — attempt auto-install from bundled package.
    runtime_dir.mkdir(parents=True, exist_ok=True)
    ok, _ = copy_skill(skill_name, runtime_dir)
    return ok and skill_path.exists()


def _is_rebase_in_progress(worktree_path: Path) -> bool:
    """Backward-compatible wrapper for shared rebase-state detection."""
    return is_rebase_in_progress(worktree_path)


def _git_runtime_env(git: Any | None) -> Mapping[str, str] | None:
    env = getattr(git, "env", None)
    return env if isinstance(env, Mapping) else None


def _check_main_integration_verify_with_git_env(
    config: Config,
    store: SqliteTaskStore,
    git: Any,
    **kwargs: Any,
) -> Any:
    runtime_env = _git_runtime_env(git)
    if runtime_env is not None:
        kwargs["env"] = runtime_env
    return check_main_integration_verify(config, store, git, **kwargs)


def _branch_has_commits(config: Config, branch: str | None, *, env: Mapping[str, str] | None = None) -> bool:
    """Return whether a branch is ahead of the default branch."""
    if not branch:
        return False
    try:
        git = _git_with_env(config.project_dir, env)
        default_branch = git.default_branch()
        return git.count_commits_ahead(branch, default_branch) > 0
    except (GitError, OSError, ValueError):
        return False


def invoke_provider_resolve(
    task: DbTask,
    branch: str,
    target: str,
    config: Config,
    *,
    log_file: Path,
    logger: TaskExecutionLogger | None = None,
    worktree_path: Path | None = None,
    runtime_context: RuntimeExecutionContext | None = None,
    provider_target_ref: str | None = None,
    provider_target_sha: str | None = None,
) -> bool:
    """Invoke active provider runtime to resolve rebase conflicts via /gza-rebase.

    Provider output is appended to ``log_file`` owned by the caller's task row.
    """
    from dataclasses import replace

    from ..providers import get_provider

    log_file.parent.mkdir(parents=True, exist_ok=True)
    if not log_file.exists():
        log_file.touch()
    task_logger = logger or TaskExecutionLogger(resolve_ops_log_path(config, log_file), echo=True)
    task_id_label = getattr(task, "id", None)
    task_ref = f"{task_id_label}" if task_id_label is not None else "<unknown>"

    effective_model, effective_provider, effective_max_steps = get_effective_config_for_task(task, config)

    runtime_context = runtime_context or RuntimeExecutionContext.from_config(config)

    runtime = _resolve_runtime_skill_dir(config.project_dir, effective_provider, env=runtime_context.env)
    if not runtime:
        task_logger.error(f"Error: Provider '{effective_provider}' does not support runtime skills for auto-resolve.")
        return False

    target_name, _runtime_dir = runtime
    if not ensure_skill("gza-rebase", effective_provider, config.project_dir, env=runtime_context.env):
        task_logger.error(
            f"Error: Missing required 'gza-rebase' skill for provider '{effective_provider}'."
        )
        task_logger.error(
            "Install it with: "
            f"uv run gza skills-install --target {target_name} gza-rebase --project {config.project_dir}"
        )
        return False

    # When running in a worktree, install the skill there so the provider finds it.
    if worktree_path is not None:
        from ..skills_utils import copy_skill

        worktree_skills_dir = worktree_path / ".claude" / "skills"
        worktree_skills_dir.mkdir(parents=True, exist_ok=True)
        ok, msg = copy_skill("gza-rebase", worktree_skills_dir)
        if not ok:
            task_logger.warning(f"Warning: Failed to copy gza-rebase skill to worktree: {msg}")

    resolve_config = replace(
        config,
        provider=effective_provider,
        model=effective_model or "",
        reasoning_effort=config.get_reasoning_effort_for_task(task.task_type, effective_provider) or "",
        max_steps=effective_max_steps,
        max_turns=effective_max_steps,
    )

    provider = get_provider(resolve_config)
    work_dir = worktree_path if worktree_path is not None else config.project_dir

    if provider_target_ref is not None and provider_target_sha is not None:
        skill_cmd = build_immutable_rebase_provider_prompt(
            auto_continue=worktree_path is None,
            target_ref=provider_target_ref,
            target_sha=provider_target_sha,
        )
    elif worktree_path is not None:
        skill_cmd = "/gza-rebase --auto"
    else:
        skill_cmd = "/gza-rebase --auto --continue"

    task_logger.phase(
        f"Provider fallback: resolving conflicts for task {task_ref} branch '{branch}' onto '{target}'.",
        extra={"provider": effective_provider, "model": effective_model or "default"},
    )
    task_logger.command(
        f"Running provider command: {skill_cmd}",
        extra={"provider": effective_provider, "command": skill_cmd},
    )
    try:
        run_result = _call_provider_run(
            provider,
            resolve_config,
            skill_cmd,
            log_file,
            work_dir,
            provider_run_kwargs={"ops_log_file": resolve_ops_log_path(config, log_file)},
            runtime_env=runtime_context.env,
        )
    except Exception as exc:
        traceback_text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        task_logger.error(
            (
                "Internal gza error during provider resolve: "
                f"provider={effective_provider}, model={effective_model or 'default'}, "
                f"command={skill_cmd}, work_dir={work_dir}, exception={exc}"
            ),
            extra={
                "provider": effective_provider,
                "model": effective_model or "default",
                "command": skill_cmd,
                "work_dir": str(work_dir),
                "exception_type": type(exc).__name__,
                "exception_message": str(exc),
                "traceback": traceback_text,
            },
        )
        return False

    if run_result.exit_code != 0:
        task_logger.error(f"Provider resolve failed with exit code {run_result.exit_code}.")
        return False

    rebase_in_progress = _is_rebase_in_progress(worktree_path or config.project_dir)
    if rebase_in_progress:
        task_logger.error(f"Rebase still in progress after {skill_cmd}.")
        return False

    task_logger.info("Provider resolve completed successfully.")
    return True


def _run_task_backed_rebase(
    *,
    config: Config,
    store: SqliteTaskStore,
    rebase_task: DbTask,
    branch: str,
    target_branch: str,
    remote: bool = False,
    parent_task_id: str | None = None,
    failure_hint_lines: list[str] | None = None,
    runtime_context: RuntimeExecutionContext | None = None,
    outcome_callback: Callable[[RebaseExecutionOutcome], None] | None = None,
) -> int:
    """Execute a foreground rebase flow with single-task log/state ownership."""
    runtime_context = runtime_context or RuntimeExecutionContext.from_config(config)
    git = _git_from_runtime_context(config.project_dir, runtime_context)
    log_file = ensure_task_log_path(config, store, rebase_task)
    logger = TaskExecutionLogger(resolve_ops_log_path(config, log_file), echo=True)
    log_file_storage = task_log_storage_path(config, log_file)

    if rebase_task.status != "in_progress":
        store.mark_in_progress(rebase_task)

    rebase_target = target_branch
    logger.info(f"Rebasing task {rebase_task.id}...")
    logger.phase(f"Current branch: {git.current_branch()}")
    logger.phase(f"Target branch: {target_branch}")
    write_log_entry(
        log_file,
        {
            "type": "gza",
            "subtype": "branch",
            "message": f"Branch: {branch}",
            "branch": branch,
            "target_branch": target_branch,
        },
    )

    if remote:
        logger.command("Fetching from origin...")
        try:
            git.fetch("origin")
        except GitError as e:
            logger.error(f"Error fetching from origin: {e}")
            mark_task_failed_from_cause(
                task=rebase_task,
                config=config,
                store=store,
                log_file=log_file,
                branch=branch,
                explicit_reason=_classify_rebase_git_failure(e),
            )
            return 1
        logger.info("✓ Fetched from origin")
        rebase_target = f"origin/{target_branch}"
        logger.phase(f"Resolved remote target: {rebase_target}")

    worktree_path = config.worktree_path / str(rebase_task.id)
    try:
        stale_path = cleanup_worktree_for_branch(
            git,
            branch,
            force=True,
            permitted_root_paths=managed_worktree_root_paths(config),
        )
        if stale_path:
            logger.phase(f"Removing stale worktree at {stale_path}...")
            logger.info("✓ Removed worktree")
        if worktree_path.exists():
            logger.phase(f"Removing existing worktree path {worktree_path}...")
            git.worktree_remove(worktree_path, force=True)
            if worktree_path.exists():
                shutil.rmtree(worktree_path, ignore_errors=True)
        worktree_path.parent.mkdir(parents=True, exist_ok=True)
        logger.phase(f"Creating worktree at {worktree_path}...")
        git.worktree_add_existing(worktree_path, branch)
    except GitError as e:
        logger.error(f"Error setting up worktree: {e}")
        mark_task_failed_from_cause(
            task=rebase_task,
            config=config,
            store=store,
            log_file=log_file,
            branch=branch,
            explicit_reason=_classify_rebase_git_failure(e),
        )
        return 1

    worktree_git = _git_from_runtime_context(worktree_path, runtime_context)
    rebase_diff_baseline = capture_rebase_diff_baseline(
        worktree_git,
        branch=branch,
        target=rebase_target,
    )
    rebase_target_for_execution = rebase_diff_baseline.target_at_start or rebase_target
    rebase_exec_git = worktree_git

    try:
        logger.command(f"Rebasing '{branch}' onto '{rebase_target}'...")
        resolved_by_provider = False
        superseded_by_concurrent_rebase = False
        supersession_proof_target: str | None = None
        try:
            rebase_exec_git.rebase(rebase_target_for_execution)
        except GitError as e:
            logger.warning(f"Conflicts detected: {e}")
            try:
                rebase_exec_git.rebase_abort()
                logger.phase("Aborted conflicted mechanical rebase before provider fallback.")
            except GitError as abort_error:
                logger.warning(f"Warning: Could not abort rebase cleanly: {abort_error}")

            logger.phase("Invoking provider to resolve via /gza-rebase --auto...")
            with isolated_rebase_checkout(
                config=config,
                source_git=git,
                branch=branch,
                target_ref=rebase_target_for_execution,
                checkout_name=str(rebase_task.slug or rebase_task.id or branch),
            ) as checkout:
                resolved = invoke_provider_resolve(
                    rebase_task,
                    branch,
                    rebase_target_for_execution,
                    config,
                    log_file=log_file,
                    logger=logger,
                    worktree_path=checkout.path,
                    runtime_context=runtime_context,
                    provider_target_ref=getattr(checkout, "provider_target_ref", None),
                    provider_target_sha=rebase_diff_baseline.target_at_start,
                )
                if resolved:
                    try:
                        import_isolated_rebase_tip(
                            destination_git=git,
                            checkout=checkout,
                            branch=branch,
                            expected_old_sha=rebase_diff_baseline.old_tip,
                            temp_ref_name=str(rebase_task.slug or rebase_task.id or branch),
                        )
                    except StaleRebaseImportError:
                        supersession_target = rebase_diff_baseline.target_at_start
                        if not branch_contains_rebase_target(
                            git,
                            branch=branch,
                            target=supersession_target,
                        ):
                            raise
                        logger.warning(
                            "Rebased tip lost the import race, but the current branch already "
                            f"contains {supersession_target}; completing this rebase as superseded."
                        )
                        superseded_by_concurrent_rebase = True
                        supersession_proof_target = supersession_target
            if not resolved:
                logger.error("Could not resolve conflicts automatically.")
                if failure_hint_lines:
                    for line in failure_hint_lines:
                        logger.error(line)
                mark_task_failed_from_cause(
                    task=rebase_task,
                    config=config,
                    store=store,
                    log_file=log_file,
                    branch=branch,
                    explicit_reason="REBASE_CONFLICT",
                )
                print()
                return 1

            resolved_by_provider = True
            rebase_exec_git = git
        try:
            if superseded_by_concurrent_rebase:
                publish_result = publish_rebased_branch(
                    rebase_exec_git,
                    branch=branch,
                    baseline=rebase_diff_baseline,
                    logger=logger,
                    supersession_proof_target=supersession_proof_target,
                )
            else:
                publish_result = publish_rebased_branch(
                    rebase_exec_git,
                    branch=branch,
                    baseline=rebase_diff_baseline,
                    logger=logger,
                )
        except GitError as e:
            mark_task_failed_from_cause(
                task=rebase_task,
                config=config,
                store=store,
                log_file=log_file,
                branch=branch,
                explicit_reason=_classify_rebase_git_failure(e),
            )
            print()
            return 1
        outcome_status: RebaseExecutionStatus
        if superseded_by_concurrent_rebase:
            output_content = (
                f"Superseded/no-op: '{branch}' already contains '{supersession_proof_target}' "
                "after a concurrent rebase; this task's isolated rebased tip was not imported."
            )
            outcome_status = "completed_no_op"
        elif resolved_by_provider:
            output_content = f"Resolved conflicts and rebased '{branch}' onto '{rebase_target}'."
            outcome_status = "provider_conflict_resolved"
        else:
            output_content = f"Rebased '{branch}' onto '{rebase_target}'."
            outcome_status = "completed_mechanical"

        has_commits = _branch_has_commits(config, branch, env=_git_runtime_env(rebase_exec_git))
        head_ref = resolve_ref_if_possible(rebase_exec_git, branch)
        publish_result_local_sha = getattr(publish_result, "local_sha", None)
        if isinstance(publish_result_local_sha, str) and publish_result_local_sha:
            head_ref = ResolvedGitRef(publish_result_local_sha, head_ref.warning)
        base_ref = resolve_ref_if_possible(rebase_exec_git, rebase_target_for_execution)
        for warning in (head_ref.warning, base_ref.warning):
            if warning:
                logger.warning(warning)
        comparison = compute_rebase_changed_diff(
            rebase_exec_git,
            baseline=rebase_diff_baseline,
            branch=head_ref.sha or branch,
            target=rebase_target_for_execution,
        )
        if comparison.warning:
            logger.warning(comparison.warning)
        compared_head_sha = getattr(comparison, "compared_head_sha", None) or head_ref.sha
        compared_target_sha = getattr(comparison, "compared_target_sha", None) or base_ref.sha
        comparison_boundary_proven = bool(getattr(comparison, "changed_diff_boundary_proven", False))
        should_probe_changed_diff_boundary = comparison_boundary_proven and not superseded_by_concurrent_rebase
        changed_diff_boundary_proof = (
            prove_rebase_changed_diff_boundary(
                rebase_exec_git,
                baseline=rebase_diff_baseline,
                resolved_head_sha=compared_head_sha,
                resolved_target_sha=compared_target_sha,
            )
            if should_probe_changed_diff_boundary
            else None
        )
        changed_diff_boundary_warning = (
            changed_diff_boundary_proof.warning if changed_diff_boundary_proof is not None else None
        )
        if changed_diff_boundary_warning:
            logger.warning(changed_diff_boundary_warning)
        rebase_task.review_scope = build_rebase_diff_provenance(
            baseline=rebase_diff_baseline,
            resolved_head_sha=compared_head_sha,
            resolved_target_sha=compared_target_sha,
            changed_diff_boundary_proven=(
                False
                if not should_probe_changed_diff_boundary or changed_diff_boundary_proof is None
                else changed_diff_boundary_proof.proven
            ),
        )
        store.mark_completed(
            rebase_task,
            branch=branch,
            log_file=log_file_storage,
            output_content=output_content,
            has_commits=has_commits,
            changed_diff=comparison.changed_diff,
            head_sha=compared_head_sha if compared_head_sha is not None else DB_UNSET,
            base_sha=compared_target_sha if compared_target_sha is not None else DB_UNSET,
            completion_reason=(REBASE_SUPERSEDED_COMPLETION_REASON if superseded_by_concurrent_rebase else None),
        )
        if outcome_callback is not None:
            outcome_callback(
                RebaseExecutionOutcome(
                    status=outcome_status,
                    source_head_before=rebase_diff_baseline.old_tip,
                    target_head_before=rebase_diff_baseline.target_at_start,
                    source_head_after=head_ref.sha,
                    target_head_after=base_ref.sha,
                    changed_diff=comparison.changed_diff,
                    completion_reason=(
                        REBASE_SUPERSEDED_COMPLETION_REASON
                        if superseded_by_concurrent_rebase
                        else None
                    ),
                    provider_conflict_resolved=resolved_by_provider,
                    superseded=superseded_by_concurrent_rebase,
                )
            )

        target_parent_id = parent_task_id or rebase_task.based_on
        if target_parent_id and comparison.changed_diff:
            store.invalidate_review_state(target_parent_id)
            parent = store.get(target_parent_id)
            if (
                parent
                and parent.id is not None
                and _task_merge_unit_state(
                    store,
                    parent,
                    target_branch=rebase_target,
                )
                == "merged"
            ):
                store.set_merge_status(parent.id, "unmerged")
        elif target_parent_id:
            refresh_preserved_rebase_review_verify_heads(
                store,
                store.get(target_parent_id),
                branch=branch,
                old_head_sha=rebase_diff_baseline.old_tip,
                new_head_sha=head_ref.sha,
            )

        if target_parent_id:
            reconciliation = reconcile_task_branch_merge_truth(
                store,
                rebase_exec_git,
                target_parent_id,
                target_branch=target_branch,
                include_diff_stats=True,
            )
            for warning in reconciliation.warnings:
                logger.warning(warning)
            if reconciliation.skipped_reason is not None:
                logger.warning(
                    "Skipped parent merge-status reconciliation for "
                    f"{target_parent_id}: {reconciliation.skipped_reason}"
                )
            for error in reconciliation.errors:
                logger.warning(f"Parent merge-status reconciliation for {target_parent_id} failed: {error}")

        logger.info(f"Changed Diff: {comparison.detail}")

        if superseded_by_concurrent_rebase:
            logger.info(f"✓ Superseded/no-op rebase for {branch}; branch already contains {supersession_proof_target}")
        elif resolved_by_provider:
            logger.info(f"✓ Successfully rebased {branch} with provider assistance")
        else:
            logger.info(f"✓ Successfully rebased {branch} onto {rebase_target}")
        print()
        return 0

    except GitError as e:
        logger.error(f"Error during rebase: {e}")
        mark_task_failed_from_cause(
            task=rebase_task,
            config=config,
            store=store,
            log_file=log_file,
            branch=branch,
            explicit_reason=_classify_rebase_git_failure(e),
        )
        print()
        return 1
    finally:
        try:
            logger.phase(f"Cleaning up worktree at {worktree_path}...")
            git.worktree_remove(worktree_path, force=True)
            if worktree_path.exists():
                shutil.rmtree(worktree_path, ignore_errors=True)
            logger.phase("Worktree cleanup complete.")
        except Exception:
            logger.warning(f"Warning: Failed to remove rebase worktree at {worktree_path}")


def _execution_mode(args: argparse.Namespace) -> Literal["queue", "run", "background"]:
    if getattr(args, "background", False):
        return "background"
    if getattr(args, "run", False):
        return "run"
    return "queue"


def cmd_rebase(args: argparse.Namespace) -> int:
    """Rebase a task's branch onto a target branch."""
    config = Config.load(args.project_dir)
    runtime_context = RuntimeExecutionContext.from_config(config)
    task_id = resolve_id(config, args.task_id)
    git = _git_from_runtime_context(config.project_dir, runtime_context)
    execution_mode = _execution_mode(args)

    current_branch = git.current_branch()
    if not _require_default_branch(
        git,
        current_branch,
        "rebase",
        to_stderr=execution_mode == "background",
    ):
        return 1

    store = get_store(config)

    # Get the task
    task = store.get(task_id)
    if not task:
        return phase1_error(args, f"Task {task_id} not found")

    # Validate task state
    if task.status not in ("completed", "unmerged", "running"):
        return phase1_error(
            args,
            f"Task {task.id} is not completed, unmerged, or running (status: {task.status})",
        )

    if not task.branch:
        return phase1_error(args, f"Task {task.id} has no branch")

    # Check if branch exists
    if not git.branch_exists(task.branch):
        return phase1_error(args, f"Branch '{task.branch}' does not exist")

    print(f"On branch {current_branch}")

    # Determine rebase target: use --onto if provided, else current branch
    rebase_target = getattr(args, "onto", None) or current_branch
    task_target = f"origin/{rebase_target}" if getattr(args, "remote", False) else rebase_target

    if execution_mode == "background":
        try:
            permit = launch_permit(config, store)
        except MaxConcurrentTasksError as exc:
            return phase1_error(args, str(exc))
        try:
            rebase_task = _create_rebase_task(
                store,
                task_id,
                task.branch,
                task_target,
                config=config,
                trigger_source="manual",
            )
        except DuplicateActiveChildError as exc:
            permit.release()
            return phase1_error(args, format_duplicate_rebase_message(exc, parent_task_id=task_id))
        except ConfigError as exc:
            permit.release()
            return phase1_error(args, str(exc))
        prepared_rebase_task = _prepare_task_for_immediate_execution(
            config,
            rebase_task,
            rollback_on_failure=True,
            runtime_context=runtime_context,
        )
        if prepared_rebase_task is None:
            permit.release()
            return 1
        if prepared_rebase_task.id is not None:
            reserve_task_launch_permit(str(prepared_rebase_task.id), permit)
        assert prepared_rebase_task.id is not None
        worker_args = argparse.Namespace(
            no_docker=getattr(args, "no_docker", False),
            max_turns=None,
        )
        return _spawn_background_worker(
            worker_args,
            config,
            task_id=prepared_rebase_task.id,
            prepared_task=prepared_rebase_task,
            runtime_context=runtime_context,
        )

    try:
        rebase_result = execute_task_backed_rebase_service(
            config=config,
            store=store,
            git=git,
            request=RebaseServiceRequest(
                parent_task_id=task_id,
                branch=task.branch,
                target_branch=rebase_target,
                remote=bool(getattr(args, "remote", False)),
                trigger_source="manual",
                run=execution_mode == "run",
                skip_if_target_contained=False,
                reuse_completed=False,
                duplicate_as_result=False,
            ),
            create_rebase_task=_create_rebase_task,
            executor=_run_task_backed_rebase if execution_mode == "run" else None,
            runtime_context=runtime_context,
        )
    except DuplicateActiveChildError as exc:
        return phase1_error(args, format_duplicate_rebase_message(exc, parent_task_id=task_id))
    except ConfigError as exc:
        return phase1_error(args, str(exc))
    if execution_mode == "queue":
        print(f"✓ Created rebase task {rebase_result.rebase_task_id}")
        print(f"  Parent: {task.id}")
        print(f"  Branch: {task.branch}")
        print(f"  Target: {task_target}")
        return 0

    return rebase_result.exit_code


def cmd_checkout(args: argparse.Namespace) -> int:
    """Checkout a task's branch, removing any stale worktree if needed."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    git = Git(config.project_dir)

    # Resolve task ID or branch name
    task = None
    branch = None

    arg = args.task_id_or_branch
    if _looks_like_task_id(arg):
        resolved_task_id = resolve_id(config, arg)
        task = store.get(resolved_task_id)
        if task is not None:
            if not task.branch:
                print(f"Error: Task {task.id} has no branch")
                return 1
            branch = task.branch
        else:
            # Not found as a task ID — fall back to treating it as a branch name
            branch = arg
    else:
        # It's a branch name
        branch = arg

    # Check if branch exists
    if not git.branch_exists(branch):
        print(f"Error: Branch '{branch}' does not exist locally")
        return 1

    # Clean up worktree if branch is checked out in one
    try:
        worktree_path = cleanup_worktree_for_branch(
            git,
            branch,
            force=args.force,
            permitted_root_paths=managed_worktree_root_paths(config),
        )
        if worktree_path:
            print(f"Removing stale worktree at {worktree_path}...")
            print("✓ Removed worktree")
    except (ValueError, GitError) as e:
        print(f"Error: {e}")
        return 1

    # Checkout the branch
    try:
        git.checkout(branch)
        print(f"✓ Checked out '{branch}'")
        return 0
    except GitError as e:
        print(f"Error checking out branch: {e}")
        return 1


def cmd_diff(args: argparse.Namespace) -> int:
    """Run git diff with colored output and pager support."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    git = Git(config.project_dir)

    # Build git diff command
    git_cmd = ["git", "diff"]

    # Add --color=always to force colored output
    git_cmd.append("--color=always")

    # Process arguments - check if first arg is a task ID
    diff_args = args.diff_args if hasattr(args, "diff_args") and args.diff_args else []

    if diff_args and not diff_args[0].startswith("-") and _looks_like_task_id(diff_args[0]):
        # First argument is a full prefixed decimal task ID ("prefix-decimal").
        task_id: str = resolve_id(config, diff_args[0])
        task = store.get(task_id)

        if not task:
            # Not found as a task ID — fall back to treating arg as a branch/ref, same
            # as cmd_checkout does.
            pass
        elif not task.branch:
            print(f"Error: Task {task_id} has no branch")
            return 1
        else:
            # Replace task ID with branch diff range
            default_branch = git.default_branch()
            diff_args = [f"{default_branch}...{task.branch}"] + diff_args[1:]

    # Add any additional arguments passed to gza diff
    if diff_args:
        git_cmd.extend(diff_args)

    # Check if stdout is a TTY (not redirected/piped)
    use_pager = sys.stdout.isatty()

    try:
        if use_pager:
            # Determine which pager to use
            pager = _get_pager(config.project_dir)

            # Run git diff and pipe to pager
            git_proc = subprocess.Popen(
                git_cmd,
                cwd=config.project_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            pager_proc = subprocess.Popen(
                pager,
                stdin=git_proc.stdout,
                cwd=config.project_dir,
                shell=True,
            )

            # Close git's stdout in parent to allow git_proc to receive SIGPIPE
            if git_proc.stdout:
                git_proc.stdout.close()

            # Wait for pager to finish
            pager_proc.wait()
            git_proc.wait()

            # Return git's exit code if it failed, otherwise pager's
            if git_proc.returncode != 0:
                # Print any stderr from git
                if git_proc.stderr:
                    stderr = git_proc.stderr.read().decode()
                    if stderr:
                        print(stderr, file=sys.stderr, end="")
                return git_proc.returncode
            return pager_proc.returncode
        else:
            # No pager - output directly (for redirection/piping)
            result = subprocess.run(
                git_cmd,
                cwd=config.project_dir,
                check=False,
            )
            return result.returncode

    except Exception as e:
        print(f"Error running git diff: {e}", file=sys.stderr)
        return 1


def cmd_pr(args: argparse.Namespace) -> int:
    """Create a GitHub PR from a completed task."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    git = Git(config.project_dir)

    # Get the task first (validate task exists and state before checking gh)
    task_id = resolve_id(config, args.task_id)
    task = store.get(task_id)
    if not task:
        print(f"Error: Task {task_id} not found")
        return 1

    # Validate task state
    if task.status not in ("completed", "unmerged"):
        print(f"Error: Task {task.id} is not completed (status: {task.status})")
        return 1

    if not task.branch:
        print(f"Error: Task {task.id} has no branch")
        return 1

    if not task.has_commits:
        print(f"Error: Task {task.id} has no commits")
        return 1

    # Check merge_status before requiring gh (local DB check, no external dependencies)
    default_branch = git.default_branch()
    if _task_merge_unit_state(store, task, target_branch=default_branch) == "merged":
        print(f"Error: Task {task.id} is already marked as merged")
        return 1

    result = ensure_task_pr(
        task,
        store,
        git,
        pr_integration=config.pr_integration,
        content_builder=lambda: _build_pr_content_for_cmd_pr(task, git, config, store, title_override=args.title),
        draft=args.draft,
        merged_behavior="error",
    )
    if result.ok and result.status == "created":
        print(f"✓ Created PR: {result.pr_url}")
        return 0
    if result.ok and result.status == "existing":
        print(f"PR already exists: {result.pr_url}")
        return 0
    if result.ok and result.status == "cached" and result.pr_number:
        print(f"PR already exists: #{result.pr_number}")
        return 0
    if result.status == "gh_unavailable":
        print("Error: GitHub CLI (gh) is not installed or not authenticated")
        print("Install: https://cli.github.com/")
        print("Auth: gh auth login")
        return 1
    if result.status == "disabled":
        print("Error: PR integration is disabled by project config (`pr_integration: false`)")
        return 1
    if result.status == "unsupported":
        print("Error: Project has no GitHub-capable remote")
        return 1
    if result.status == "lookup_failed":
        print(f"Error looking up PR:\n{result.error}")
        return 1
    if result.status == "push_failed":
        print(f"Error pushing branch: {result.error}")
        return 1
    if result.status == "merged":
        print(f"Error: Branch '{task.branch}' is already merged into {default_branch}")
        return 1
    if result.status == "create_failed":
        print(f"Error creating PR:\n{result.error}")
        return 1
    print("Error creating PR")
    return 1


def _build_pr_content_for_cmd_pr(
    task,
    git: Git,
    config: Config,
    store,
    *,
    title_override: str | None,
) -> tuple[str, str]:
    """Build PR content lazily so reused/skip paths avoid provider work."""
    if title_override is None:
        print("Generating PR description...")
    return build_task_pr_content(task, git, config, store, title_override=title_override)


def cmd_sync(args: argparse.Namespace) -> int:
    """Explicitly reconcile branch state across git and GitHub."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    git = Git(config.project_dir)

    include_git = not getattr(args, "pr_only", False)
    include_pr = not getattr(args, "git_only", False)

    preliminary_results: list = []
    if args.task_ids:
        resolved_ids = [resolve_id(config, task_id) for task_id in args.task_ids]
        cohorts, preliminary_results = build_branch_cohorts_for_task_ids(
            store,
            resolved_ids,
        )
    else:
        cohorts = build_default_branch_cohorts(store)

    if not cohorts and not preliminary_results:
        if not args.task_ids and store.get_sync_candidates(recent_days=30, cooldown_seconds=0):
            cache_minutes = max(DEFAULT_SYNC_CACHE_SECONDS // 60, 1)
            print(f"No sync candidates: default sync cache is still warm ({cache_minutes}m cooldown).")
        else:
            print("No sync candidates")
        return 0

    results = list(preliminary_results)
    partial_failure = False
    if cohorts:

        def _progress(message: str) -> None:
            print(f"[sync] {message}")

        cohort_results, partial_failure = sync_branch_cohorts(
            store,
            git,
            cohorts,
            include_git=include_git,
            include_pr=include_pr,
            pr_integration=config.pr_integration,
            dry_run=bool(getattr(args, "dry_run", False)),
            fetch_remote=not bool(getattr(args, "no_fetch", False)),
            progress=_progress,
        )
        results.extend(cohort_results)

    synced = 0
    skipped = 0
    errors = 0
    for result in results:
        if result.errors:
            errors += 1
        if result.skipped_reason is not None:
            skipped += 1
            task_label = result.task_ids[0] if result.task_ids else result.branch
            print(f"{task_label}: skipped ({result.skipped_reason})")
            continue

        if result.reconciled:
            synced += 1
        parts = [result.branch]
        if result.merge_status is not None:
            parts.append(f"merge={result.merge_status}")
        if result.diff_files_changed is not None:
            parts.append(
                f"diff=+{result.diff_lines_added}/-{result.diff_lines_removed} {result.diff_files_changed} files"
            )
        if result.pr_number is not None or result.pr_state is not None:
            pr_num = f"#{result.pr_number}" if result.pr_number is not None else "#?"
            parts.append(f"pr={pr_num}:{result.pr_state or 'unknown'}")
        if result.actions:
            parts.append(", ".join(result.actions))
        if result.warnings:
            parts.append(f"warnings: {'; '.join(result.warnings)}")
        if result.errors:
            parts.append(f"errors: {'; '.join(result.errors)}")
        print(" | ".join(parts))

    print(f"\nSynced {synced} branch(es), skipped {skipped}, errors {errors}.")
    return 1 if partial_failure or errors else 0


def _unimplemented_implement_prompt(task: DbTask) -> str:
    """Build the default implement prompt for an upstream source task."""
    assert task.id is not None
    slug = _get_base_task_slug(task)
    if task.task_type == "plan":
        return f"Implement plan from task {task.id}: {slug}" if slug else f"Implement plan from task {task.id}"
    return f"Implement findings from task {task.id}: {slug}" if slug else f"Implement findings from task {task.id}"


def _normalize_task_created_at(value: datetime | None) -> datetime:
    if not isinstance(value, datetime):
        return datetime.min
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _unimplemented_target_sort_key(task: DbTask) -> tuple[datetime, int]:
    return (_normalize_task_created_at(task.created_at), task_id_numeric_key(task.id))


def _is_directly_implementable_plan(task: DbTask) -> bool:
    """Return True when the row can be handed directly to `gza implement`."""
    return task.task_type == "plan" and task.status == "completed"


def _unimplemented_status_label(task: DbTask) -> str:
    """Render task type and status for the unimplemented source list."""
    status = task.status or "pending"
    return f"[{task.task_type}] ({status})"


def _unimplemented_followup_command(task: DbTask) -> str:
    """Return truthful operator guidance for one listed source row."""
    assert task.id is not None
    if _is_directly_implementable_plan(task):
        return f"gza implement {task.id}"
    return "gza advance --unimplemented --create"


def _get_unimplemented_lineage_root(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    task_cache: dict[str, DbTask],
) -> DbTask:
    """Walk to the absolute based_on root so each lineage tree is processed once."""
    assert task.id is not None

    current = task
    while current.based_on:
        parent = task_cache.get(current.based_on)
        if parent is None:
            parent = store.get(current.based_on)
            if parent is None or parent.id is None:
                break
            task_cache[parent.id] = parent
        current = parent

    return current


def _resolve_unimplemented_source_targets(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    task_types: tuple[str, ...],
    task_cache: dict[str, DbTask],
    children_cache: dict[str, list[DbTask]],
    frontier_cache: dict[str, list[DbTask]],
    followup_state_cache: dict[str, SourceFollowupState],
    non_dropped_implement_source_ids: set[str],
) -> list[DbTask]:
    """Resolve the newest unimplemented plan/explore source rows for each lineage branch."""

    def _walk(current: DbTask) -> list[DbTask]:
        assert current.id is not None
        cached = frontier_cache.get(current.id)
        if cached is not None:
            return cached

        task_cache[current.id] = current
        children = children_cache.get(current.id)
        if children is None:
            children = store.get_based_on_children(current.id)
            children_cache[current.id] = children
        for child in children:
            if child.id is not None:
                task_cache[child.id] = child

        child_targets: list[DbTask] = []
        for child in children:
            if child.id is None:
                continue
            branch_targets = _walk(child)
            child_targets.extend(branch_targets)

        if child_targets:
            result = child_targets
        elif current.task_type in task_types and source_task_needs_implementation_followup(
            current,
            followup_state_cache.setdefault(
                current.id,
                resolve_source_followup_state(current, get_children=store.get_based_on_children),
            ),
            non_dropped_implement_source_ids=non_dropped_implement_source_ids,
        ):
            result = [current]
        else:
            result = []

        frontier_cache[current.id] = result
        return result

    return _walk(task)


def _cmd_advance_unimplemented(
    config: "Config",
    store: SqliteTaskStore,
    dry_run: bool = False,
    create: bool = False,
    task_types: tuple[str, ...] = ("plan", "explore"),
) -> int:
    """List plan/explore lineages that do not yet have an implementation task.

    With --create, queues implement tasks for each listed plan/explore source row.
    """
    all_completed: list[DbTask] = []
    for task_type in task_types:
        all_completed.extend(store.get_history(limit=None, status="completed", task_type=task_type))

    # Find the current unimplemented source frontier for each lineage tree. A newer
    # descendant source row can replace its own ancestors, but sibling branches stay
    # independently eligible and implement tasks are never shown directly.
    task_cache = {task.id: task for task in all_completed if task.id is not None}
    children_cache: dict[str, list[DbTask]] = {}
    frontier_cache: dict[str, list[DbTask]] = {}
    followup_state_cache: dict[str, SourceFollowupState] = {}
    non_dropped_implement_source_ids = collect_non_dropped_implement_source_ids(store.get_all())
    covered_root_ids: set[str] = set()
    pending_tasks: list[DbTask] = []

    for task in all_completed:
        assert task.id is not None
        root = _get_unimplemented_lineage_root(store, task, task_cache=task_cache)
        assert root.id is not None
        if root.id in covered_root_ids:
            continue
        covered_root_ids.add(root.id)
        pending_tasks.extend(
            _resolve_unimplemented_source_targets(
                store,
                root,
                task_types=task_types,
                task_cache=task_cache,
                children_cache=children_cache,
                frontier_cache=frontier_cache,
                followup_state_cache=followup_state_cache,
                non_dropped_implement_source_ids=non_dropped_implement_source_ids,
            )
        )

    if not pending_tasks:
        task_label = "/".join(task_types)
        print(f"No {task_label} lineages without implementation tasks.")
        return 0

    task_label = "/".join(task_types)
    print(f"{task_label.capitalize()} lineages without implementation ({len(pending_tasks)}):")
    print()
    for task in pending_tasks:
        assert task.id is not None
        status_label = _unimplemented_status_label(task)
        prefix_len = len(f"  {task.id}  {status_label} ")
        prompt_display = shorten_prompt(task.prompt, prompt_available_width(prefix=prefix_len))
        print(f"  {task.id}  {status_label} {prompt_display}")
        print(f"       → {_unimplemented_followup_command(task)}")
    print()

    if not create:
        if any(_is_directly_implementable_plan(task) for task in pending_tasks):
            print(
                "Completed plan rows can be run directly with 'gza implement <task_id>' or auto-started with 'gza advance'."
            )
        if any(not _is_directly_implementable_plan(task) for task in pending_tasks):
            print("Use 'gza advance --unimplemented --create' to queue implement tasks for listed explore rows.")
        return 0

    # Create queued implement tasks
    created_count = 0
    for task in pending_tasks:
        assert task.id is not None
        if dry_run:
            print(f"[dry-run] Would create implement task for {task.task_type} {task.id}")
            continue
        config.require_model_for_task("implement")
        prompt_text = _unimplemented_implement_prompt(task)
        impl_task = store.add(
            prompt=prompt_text,
            task_type="implement",
            depends_on=task.id,
            tags=resolve_derived_task_tags(task),
            trigger_source="manual",
        )
        print(f"✓ Created implement task {impl_task.id} for {task.task_type} {task.id}")
        created_count += 1

    if not dry_run:
        print(f"\nCreated {created_count} implement task(s). Run 'gza work' to execute them.")
    return 0


@dataclass
class _MergeActionResult:
    rc: int
    created_followups: list[DbTask]
    reused_followups: list[DbTask]
    created_investigation_task_ids: list[str]
    reused_investigation_task_ids: list[str]
    created_deferred_blockers: list[DbTask] = field(default_factory=list)
    reused_deferred_blockers: list[DbTask] = field(default_factory=list)
    status: str = "merged"
    block_reason: str | None = None
    promotion_warnings: tuple[str, ...] = ()
    candidate_verify: CandidateIntegrationVerifyCheck | None = None


PRE_PROMOTION_MERGE_REFUSAL_STATUSES = frozenset(
    {
        "pre_merge_proof_persistence_failed",
        "merge_source_ref_unavailable",
        "merge_source_ref_changed",
    }
)
PENDING_MERGE_FINALIZATION_REFUSAL_STATUSES = frozenset(
    {
        "pending_merge_finalization_proof_stale",
    }
)


def is_pre_promotion_merge_refusal_status(status: object) -> bool:
    return isinstance(status, str) and status in PRE_PROMOTION_MERGE_REFUSAL_STATUSES


def is_pending_merge_finalization_refusal_status(status: object) -> bool:
    return isinstance(status, str) and status in PENDING_MERGE_FINALIZATION_REFUSAL_STATUSES


def _pre_promotion_merge_refusal_reason(*, phase: str, exc: Exception | str) -> str:
    return (
        f"{phase} failed before target promotion: {exc}; "
        "target is unchanged; retry after refreshing merge authorization"
    )


@dataclass(frozen=True)
class _StagedIsolatedMergeAction:
    merge_subject: DbTask
    merge_unit_id: str | None
    merge_branch: str | None
    pending_squash_reconcile: _PendingSquashBranchReconcile | None
    review_task: DbTask | None
    followup_findings: tuple[ReviewFinding, ...]
    created_followups: tuple[DbTask, ...] = ()
    reused_followups: tuple[DbTask, ...] = ()
    created_investigation_task_ids: tuple[str, ...] = ()
    reused_investigation_task_ids: tuple[str, ...] = ()
    created_deferred_blockers: tuple[DbTask, ...] = ()
    reused_deferred_blockers: tuple[DbTask, ...] = ()
    merge_action_metadata: Mapping[str, Any] = field(default_factory=dict)
    source_ref: str | None = None
    source_ref_sha: str | None = None
    previous_target_sha: str | None = None
    promoted_target_sha: str | None = None
    promotion_kind: MergeFinalizationPromotionKind = "normal"
    target_branch: str = "main"


@dataclass
class _CreateReviewActionResult:
    status: str
    review_task: DbTask | None
    message: str


def _prepare_create_review_action(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    config: Config | None = None,
    trigger_source: str,
) -> _CreateReviewActionResult:
    """Create or resolve the review task for an advance-style create_review action."""
    review_target = task
    if task.task_type in {"improve", "rebase"}:
        resolved_impl = _resolve_merged_target_task(store, task)
        if resolved_impl is not None:
            review_target = resolved_impl
    elif task.task_type == "fix":
        resolved_impl = _resolve_root_implementation_for_fix(task, store)
        if resolved_impl is not None:
            review_target = resolved_impl
    elif task.task_type != "implement":
        resolved_impl = _resolve_impl_ancestor(store, task)
        if resolved_impl is not None:
            review_target = resolved_impl

    try:
        if config is not None:
            config.require_model_for_task("review")
            review_task = _create_review_task(
                store,
                review_target,
                config=config,
                trigger_source=trigger_source,
            )
        else:
            review_task = _create_review_task(store, review_target, trigger_source=trigger_source)
    except DuplicateReviewError as exc:
        review_task = exc.active_review
        return _CreateReviewActionResult(
            status="skip",
            review_task=review_task,
            message=f"SKIP: review {review_task.id} is already {review_task.status}",
        )
    except ValueError as exc:
        return _CreateReviewActionResult(
            status="skip",
            review_task=None,
            message=f"SKIP: {exc}",
        )

    return _CreateReviewActionResult(
        status="created",
        review_task=review_task,
        message=f"Created review task {review_task.id}",
    )


def _blocked_candidate_verify_attention_key(task_id: str, check: CandidateIntegrationVerifyCheck | None) -> str:
    if check is None:
        return f"merge-candidate-verify:{task_id}:unavailable"
    remediation = getattr(check, "remediation", None)
    if remediation is not None:
        fingerprint = remediation.tree_fingerprint or "unavailable"
        return f"merge-candidate-verify:{task_id}:{remediation.signature}:{fingerprint}"
    evidence = check.evidence
    phase = evidence.failing_phase or evidence.verify_exit_status or evidence.verify_status or "unknown"
    fingerprint = evidence.tree_fingerprint or "unavailable"
    return f"merge-candidate-verify:{task_id}:{phase}:{fingerprint}"


def format_blocked_candidate_verify_message(task_id: str, merge_result: Any) -> str:
    block_reason = getattr(merge_result, "block_reason", None) or "candidate verify blocked promotion"
    check = getattr(merge_result, "candidate_verify", None)
    if not isinstance(check, CandidateIntegrationVerifyCheck):
        return f"{task_id}: {block_reason}"
    evidence = check.evidence
    fingerprint = evidence.tree_fingerprint or "unavailable"
    if check.classification == "unavailable":
        return f"{task_id}: candidate verify unavailable on {fingerprint}; local main was left unchanged"
    if evidence.failing_phase:
        return (
            f"{task_id}: candidate verify blocked promotion on {fingerprint}; "
            f"phase `{evidence.failing_phase}` failed before main changed"
        )
    if evidence.failure:
        return f"{task_id}: candidate verify blocked promotion on {fingerprint}; {evidence.failure}"
    return f"{task_id}: {block_reason}"


def _isolated_merge_checkout_unavailable_result() -> _MergeActionResult:
    return _MergeActionResult(
        rc=1,
        created_followups=[],
        reused_followups=[],
        created_investigation_task_ids=[],
        reused_investigation_task_ids=[],
        status="blocked_candidate_verify_unavailable",
        block_reason="isolated host merge checkout unavailable; local main was left unchanged",
    )


def _materialize_merge_followup_side_effects(
    store: SqliteTaskStore,
    *,
    config: Config,
    merge_subject: DbTask,
    review_task: DbTask | None,
    followup_findings: tuple[ReviewFinding, ...],
    action: Mapping[str, Any],
) -> tuple[list[DbTask], list[DbTask]]:
    if review_task is None or not followup_findings:
        return [], []
    proven_tasks = action.get("proven_followup_tasks")
    if action.get("pending_merge_finalization") is True and proven_tasks is not None:
        _validate_proven_followup_replay_tasks(
            store,
            review_task=review_task,
            impl_task=merge_subject,
            findings=followup_findings,
            proven_tasks=proven_tasks,
        )
        created, reused = _create_or_reuse_followup_tasks(
            store,
            config=config,
            review_task=review_task,
            impl_task=merge_subject,
            findings=followup_findings,
            trigger_source="manual",
        )
        _require_reused_proven_task_ids(
            reused,
            proven_tasks,
            kind="ordinary follow-up",
        )
        return created, reused
    return _create_or_reuse_followup_tasks(
        store,
        config=config,
        review_task=review_task,
        impl_task=merge_subject,
        findings=followup_findings,
        trigger_source="manual",
    )


def _validate_proven_followup_replay_tasks(
    store: SqliteTaskStore,
    *,
    review_task: DbTask,
    impl_task: DbTask,
    findings: tuple[ReviewFinding, ...],
    proven_tasks: object,
) -> None:
    if review_task.id is None or impl_task.id is None:
        raise ValueError("pending follow-up finalization replay requires persisted review and implementation tasks")
    if not isinstance(proven_tasks, (list, tuple)) or not all(isinstance(task, DbTask) for task in proven_tasks):
        raise ValueError("pending follow-up finalization replay contains malformed proven follow-up tasks")
    typed_proven_tasks = cast("tuple[DbTask, ...]", tuple(proven_tasks))
    expected_by_id: dict[str, DbTask] = {}
    if len(typed_proven_tasks) != len(findings):
        raise ValueError("pending follow-up finalization replay has the wrong number of proven follow-up tasks")
    seen_ids: set[str] = set()
    for finding, proven_task in zip(findings, typed_proven_tasks, strict=True):
        if proven_task.id is None:
            raise ValueError("pending follow-up finalization replay contains a proven task without an ID")
        if proven_task.id in seen_ids:
            raise ValueError(f"pending follow-up finalization replay contains duplicate task {proven_task.id}")
        seen_ids.add(proven_task.id)
        expected_prompt = build_followup_prompt(review_task.id, impl_task.id, finding)
        current = store.get(proven_task.id)
        if current is None:
            raise ValueError(f"proven follow-up task {proven_task.id} disappeared before merge finalization")
        mismatches: list[str] = []
        if current.prompt != expected_prompt:
            mismatches.append("prompt")
        if current.based_on != review_task.id:
            mismatches.append("based_on")
        if current.depends_on != impl_task.id:
            mismatches.append("depends_on")
        if current.task_type != "implement":
            mismatches.append("task_type")
        if current.urgent:
            mismatches.append("urgent")
        if current.create_pr:
            mismatches.append("create_pr")
        if current.review_scope != format_followup_finding_context(finding):
            mismatches.append("review_scope")
        if mismatches:
            raise ValueError(
                f"proven follow-up task {proven_task.id} does not match required replay identity: "
                f"{', '.join(mismatches)}"
            )
        expected_by_id[proven_task.id] = current
    if set(expected_by_id) != seen_ids:
        raise ValueError("pending follow-up finalization replay contains ambiguous proven follow-up tasks")


def _require_reused_proven_task_ids(
    reused: Iterable[DbTask],
    proven_tasks: object,
    *,
    kind: str,
) -> None:
    assert isinstance(proven_tasks, (list, tuple))
    proven_ids = {task.id for task in proven_tasks if isinstance(task, DbTask) and task.id is not None}
    reused_ids = {task.id for task in reused if task.id is not None}
    if reused_ids != proven_ids:
        raise ValueError(
            f"pending {kind} finalization replay reused {sorted(reused_ids)!r}; "
            f"expected exactly {sorted(proven_ids)!r}"
        )


def _validated_merge_followup_action_metadata(
    store: SqliteTaskStore,
    action: Mapping[str, Any],
) -> tuple[DbTask | None, tuple[ReviewFinding, ...]]:
    raw_findings = action.get("followup_findings")
    if action.get("type") != "merge_with_followups" and not raw_findings:
        return None, ()

    raw_review = action.get("review_task")
    if not isinstance(raw_review, DbTask):
        raise ValueError("merge_with_followups action requires a persisted review_task")
    if raw_review.id is None or store.get(raw_review.id) is None:
        raise ValueError("merge_with_followups action requires a persisted review_task")
    if raw_review.task_type != "review":
        raise ValueError("merge_with_followups action review_task must be a review task")

    if not isinstance(raw_findings, (list, tuple)) or not raw_findings:
        raise ValueError("merge_with_followups action requires nonempty followup_findings")

    findings: list[ReviewFinding] = []
    seen_ids: set[str] = set()
    for finding in raw_findings:
        if not isinstance(finding, ReviewFinding):
            raise ValueError("merge_with_followups action contains malformed followup_findings")
        if finding.severity != "FOLLOWUP":
            raise ValueError("merge_with_followups action requires FOLLOWUP severity findings")
        finding_id = finding.id.strip() if isinstance(finding.id, str) else ""
        if not finding_id or finding_id != finding.id:
            raise ValueError("merge_with_followups action contains noncanonical finding identities")
        if finding_id in seen_ids:
            raise ValueError("merge_with_followups action contains duplicate finding identities")
        seen_ids.add(finding_id)
        findings.append(finding)

    return raw_review, tuple(findings)


def _followup_findings_for_proof(action: Mapping[str, Any]) -> tuple[ReviewFinding, ...]:
    raw_findings = action.get("followup_findings")
    if not isinstance(raw_findings, (list, tuple)) or not all(
        isinstance(finding, ReviewFinding) for finding in raw_findings
    ):
        raise ValueError("ordinary merge finalization proof requires follow-up findings")
    return tuple(raw_findings)


def _mandatory_merge_side_effects_failed_result(
    *,
    exc: Exception,
    created_followups: list[DbTask] | tuple[DbTask, ...] | None = None,
    reused_followups: list[DbTask] | tuple[DbTask, ...] | None = None,
    created_investigation_task_ids: list[str] | tuple[str, ...] | None = None,
    reused_investigation_task_ids: list[str] | tuple[str, ...] | None = None,
    created_deferred_blockers: list[DbTask] | tuple[DbTask, ...] | None = None,
    reused_deferred_blockers: list[DbTask] | tuple[DbTask, ...] | None = None,
) -> _MergeActionResult:
    partial_created = tuple(getattr(exc, "created", ()))
    partial_reused = tuple(getattr(exc, "reused", ()))
    if isinstance(exc, FollowupMaterializationError):
        created_followups = (*tuple(created_followups or ()), *partial_created)
        reused_followups = (*tuple(reused_followups or ()), *partial_reused)
        block_reason = f"ordinary follow-up materialization failed: {exc}; source remains unmerged"
    elif isinstance(exc, CappedReviewBlockerMaterializationError):
        created_deferred_blockers = (*tuple(created_deferred_blockers or ()), *partial_created)
        reused_deferred_blockers = (*tuple(reused_deferred_blockers or ()), *partial_reused)
        block_reason = f"max-cycle deferred blocker materialization failed: {exc}; source remains unmerged"
    else:
        block_reason = f"mandatory merge side-effect materialization failed: {exc}; source remains unmerged"
    print(f"Error: {block_reason}")
    return _MergeActionResult(
        rc=1,
        created_followups=list(created_followups or ()),
        reused_followups=list(reused_followups or ()),
        created_investigation_task_ids=list(created_investigation_task_ids or ()),
        reused_investigation_task_ids=list(reused_investigation_task_ids or ()),
        created_deferred_blockers=list(created_deferred_blockers or ()),
        reused_deferred_blockers=list(reused_deferred_blockers or ()),
        status="merge_side_effect_materialization_failed",
        block_reason=block_reason,
    )


def _post_merge_state_persistence_failed_result(
    *,
    exc: Exception,
    status: str,
    phase: str,
    created_followups: list[DbTask] | tuple[DbTask, ...] | None = None,
    reused_followups: list[DbTask] | tuple[DbTask, ...] | None = None,
    created_investigation_task_ids: list[str] | tuple[str, ...] | None = None,
    reused_investigation_task_ids: list[str] | tuple[str, ...] | None = None,
    created_deferred_blockers: list[DbTask] | tuple[DbTask, ...] | None = None,
    reused_deferred_blockers: list[DbTask] | tuple[DbTask, ...] | None = None,
) -> _MergeActionResult:
    block_reason = (
        f"{phase} failed after target was already changed: {exc}; "
        "replay will finalize promoted target state"
    )
    print(f"Error: {block_reason}")
    return _MergeActionResult(
        rc=1,
        created_followups=list(created_followups or ()),
        reused_followups=list(reused_followups or ()),
        created_investigation_task_ids=list(created_investigation_task_ids or ()),
        reused_investigation_task_ids=list(reused_investigation_task_ids or ()),
        created_deferred_blockers=list(created_deferred_blockers or ()),
        reused_deferred_blockers=list(reused_deferred_blockers or ()),
        status=status,
        block_reason=block_reason,
    )


def _post_merge_proof_persistence_failed_result(
    *,
    exc: Exception,
    status: str,
    phase: str,
    created_followups: list[DbTask] | tuple[DbTask, ...] | None = None,
    reused_followups: list[DbTask] | tuple[DbTask, ...] | None = None,
    created_investigation_task_ids: list[str] | tuple[str, ...] | None = None,
    reused_investigation_task_ids: list[str] | tuple[str, ...] | None = None,
    created_deferred_blockers: list[DbTask] | tuple[DbTask, ...] | None = None,
    reused_deferred_blockers: list[DbTask] | tuple[DbTask, ...] | None = None,
) -> _MergeActionResult:
    block_reason = (
        f"{phase} failed after target was already changed: {exc}; "
        "merge finalization proof was not stored, so automatic replay cannot be promised; "
        "operator attention required before retry or manual recovery"
    )
    print(f"Error: {block_reason}")
    return _MergeActionResult(
        rc=1,
        created_followups=list(created_followups or ()),
        reused_followups=list(reused_followups or ()),
        created_investigation_task_ids=list(created_investigation_task_ids or ()),
        reused_investigation_task_ids=list(reused_investigation_task_ids or ()),
        created_deferred_blockers=list(created_deferred_blockers or ()),
        reused_deferred_blockers=list(reused_deferred_blockers or ()),
        status=status,
        block_reason=block_reason,
    )


def _pre_promotion_merge_refusal_result(
    *,
    exc: Exception | str,
    status: str,
    phase: str,
    created_followups: list[DbTask] | tuple[DbTask, ...] | None = None,
    reused_followups: list[DbTask] | tuple[DbTask, ...] | None = None,
    created_investigation_task_ids: list[str] | tuple[str, ...] | None = None,
    reused_investigation_task_ids: list[str] | tuple[str, ...] | None = None,
    created_deferred_blockers: list[DbTask] | tuple[DbTask, ...] | None = None,
    reused_deferred_blockers: list[DbTask] | tuple[DbTask, ...] | None = None,
) -> _MergeActionResult:
    block_reason = _pre_promotion_merge_refusal_reason(phase=phase, exc=exc)
    print(f"Error: {block_reason}")
    return _MergeActionResult(
        rc=1,
        created_followups=list(created_followups or ()),
        reused_followups=list(reused_followups or ()),
        created_investigation_task_ids=list(created_investigation_task_ids or ()),
        reused_investigation_task_ids=list(reused_investigation_task_ids or ()),
        created_deferred_blockers=list(created_deferred_blockers or ()),
        reused_deferred_blockers=list(reused_deferred_blockers or ()),
        status=status,
        block_reason=block_reason,
    )


def _pending_merge_finalization_refusal_result(
    *,
    exc: Exception | str,
    created_followups: list[DbTask] | tuple[DbTask, ...] | None = None,
    reused_followups: list[DbTask] | tuple[DbTask, ...] | None = None,
    created_investigation_task_ids: list[str] | tuple[str, ...] | None = None,
    reused_investigation_task_ids: list[str] | tuple[str, ...] | None = None,
    created_deferred_blockers: list[DbTask] | tuple[DbTask, ...] | None = None,
    reused_deferred_blockers: list[DbTask] | tuple[DbTask, ...] | None = None,
) -> _MergeActionResult:
    block_reason = (
        f"pending merge finalization refused before state finalization: {exc}; "
        "merge state and provenance were left unchanged"
    )
    print(f"Error: {block_reason}")
    return _MergeActionResult(
        rc=1,
        created_followups=list(created_followups or ()),
        reused_followups=list(reused_followups or ()),
        created_investigation_task_ids=list(created_investigation_task_ids or ()),
        reused_investigation_task_ids=list(reused_investigation_task_ids or ()),
        created_deferred_blockers=list(created_deferred_blockers or ()),
        reused_deferred_blockers=list(reused_deferred_blockers or ()),
        status="pending_merge_finalization_proof_stale",
        block_reason=block_reason,
    )


def _authoritative_merge_unit_still_unmerged(store: SqliteTaskStore, task: DbTask) -> bool:
    if task.id is None:
        return False
    merge_unit = store.resolve_merge_unit_for_task(task.id)
    if merge_unit is None or merge_unit.state == "merged":
        return False
    return any(
        child.task_type == "implement" and DEFERRED_REVIEW_BLOCKER_TAG in set(child.tags or ())
        for child in store.get_based_on_children(task.id)
    )


def _pending_merge_finalization_action_for_advance(
    config: Config,
    store: SqliteTaskStore,
    task: DbTask,
    *,
    target_branch: str | None,
    resolved_merge_state: str | None,
    live_target_sha: str | None,
) -> dict[str, Any] | None:
    return pending_merge_finalization_action(
        config,
        store,
        task,
        target_branch=target_branch,
        require_already_merged=True,
        resolved_merge_state=resolved_merge_state,
        live_target_sha=live_target_sha,
    )


def _already_merged_behavior_for_advance_action(action: Mapping[str, Any]) -> str:
    if action.get("pending_merge_finalization") is True and action.get("type") in {"merge", "merge_with_followups"}:
        return "mark_merged"
    if action.get("type") == "merge" and action.get("max_cycles_merge_and_defer") is True:
        return "mark_merged"
    return "error"


def _capped_review_blocker_findings_for_action(action: Mapping[str, Any]) -> tuple[ReviewFinding, ...]:
    present_keys = [
        key
        for key in (
            "deferred_blocker_findings",
            "blocker_findings",
            "max_cycles_blocker_findings",
            "findings",
        )
        if action.get(key) is not None
    ]
    if len(present_keys) > 1:
        raise ValueError(
            "max-cycle merge-and-defer action has ambiguous blocker finding metadata: "
            f"{', '.join(present_keys)}"
        )
    if action.get("followup_findings") is not None:
        raise ValueError("max-cycle merge-and-defer action cannot include ordinary follow-up metadata")
    for key in (
        "deferred_blocker_findings",
        "blocker_findings",
        "max_cycles_blocker_findings",
        "findings",
    ):
        raw_findings = action.get(key)
        if raw_findings is None:
            continue
        if not isinstance(raw_findings, (list, tuple)):
            raise ValueError(f"max-cycle merge-and-defer action requires {key} to be a list or tuple")
        if not all(isinstance(finding, ReviewFinding) for finding in raw_findings):
            raise ValueError(f"max-cycle merge-and-defer action contains malformed blocker findings in {key}")
        return tuple(raw_findings)
    return ()


def _validated_capped_review_action_metadata(
    store: SqliteTaskStore,
    *,
    config: Config,
    action: Mapping[str, Any],
) -> tuple[DbTask, tuple[ReviewFinding, ...], str] | None:
    if action.get("max_cycles_merge_and_defer") is True and action.get("type") != "merge":
        raise ValueError("max-cycle merge-and-defer metadata is only valid on merge actions")
    if not (action.get("type") == "merge" and action.get("max_cycles_merge_and_defer") is True):
        return None
    review_task = action.get("review_task")
    if not isinstance(review_task, DbTask):
        raise ValueError("max-cycle merge-and-defer action requires a review_task")
    persisted_review_output = action.get("persisted_review_output")
    review_output_alias = action.get("review_output")
    if not isinstance(persisted_review_output, str):
        raise ValueError("max-cycle merge-and-defer action requires persisted_review_output")
    if review_output_alias is not None and review_output_alias != persisted_review_output:
        raise ValueError("max-cycle merge-and-defer action has conflicting review output aliases")
    audit = action.get("max_cycles_audit")
    if not isinstance(audit, Mapping):
        raise ValueError("max-cycle merge-and-defer action requires max_cycles_audit")
    if audit.get("reason") != "review-max-cycles":
        raise ValueError("max-cycle merge-and-defer action requires review-max-cycles audit reason")
    if audit.get("policy") != "merge_and_defer":
        raise ValueError("max-cycle merge-and-defer action requires merge_and_defer audit policy")
    completed_review_cycles = audit.get("completed_review_cycles")
    max_review_cycles = audit.get("max_review_cycles")
    if (
        isinstance(completed_review_cycles, bool)
        or not isinstance(completed_review_cycles, int)
        or isinstance(max_review_cycles, bool)
        or not isinstance(max_review_cycles, int)
    ):
        raise ValueError("max-cycle merge-and-defer action requires integer review cycle audit fields")
    if completed_review_cycles < max_review_cycles:
        raise ValueError("max-cycle merge-and-defer action completed cycles are below the configured cap")
    if action.get("pending_merge_finalization") is not True:
        latest_review_task_id = action.get("latest_review_task_id")
        if not isinstance(latest_review_task_id, str) or not latest_review_task_id:
            raise ValueError("max-cycle merge-and-defer action requires latest_review_task_id")
        if review_task.id != latest_review_task_id:
            raise ValueError("max-cycle merge-and-defer review_task does not match latest_review_task_id")
        current_review_task = store.get(latest_review_task_id)
        if current_review_task is None:
            raise ValueError(f"max-cycle merge-and-defer review {latest_review_task_id} is missing")
        latest_review_completed_at = action.get("latest_review_completed_at")
        if not isinstance(latest_review_completed_at, str) or not latest_review_completed_at:
            raise ValueError("max-cycle merge-and-defer action requires latest_review_completed_at")
        try:
            annotated_completed_at = _normalize_action_completed_at(latest_review_completed_at)
        except ValueError as exc:
            raise ValueError("max-cycle merge-and-defer action has invalid latest_review_completed_at") from exc
        if current_review_task.completed_at != annotated_completed_at:
            raise ValueError("max-cycle merge-and-defer latest_review_completed_at does not match review row")
        latest_review_mode = action.get("latest_review_mode")
        if latest_review_mode not in {"plain_full", "resolution"}:
            raise ValueError("max-cycle merge-and-defer action requires latest_review_mode")
        if _capped_review_authority_mode(current_review_task) != latest_review_mode:
            raise ValueError("max-cycle merge-and-defer latest_review_mode does not match review row")
        latest_review_head_sha = action.get("latest_review_head_sha")
        current_review_head_sha = action.get("current_review_head_sha")
        if not isinstance(latest_review_head_sha, str) or not latest_review_head_sha:
            raise ValueError("max-cycle merge-and-defer action requires latest_review_head_sha")
        if not isinstance(current_review_head_sha, str) or not current_review_head_sha:
            raise ValueError("max-cycle merge-and-defer action requires current_review_head_sha")
        if current_review_head_sha != latest_review_head_sha:
            raise ValueError("max-cycle merge-and-defer action reviewed head does not match current review head")
        if current_review_task.review_verify_head_sha != latest_review_head_sha:
            raise ValueError("max-cycle merge-and-defer latest_review_head_sha does not match review row")
        verify_gate_state = audit.get("verify_gate_state") if isinstance(audit, Mapping) else None
        verify_epoch = audit.get("verify_epoch") if isinstance(audit, Mapping) else None
        if verify_gate_state != "passed" or not isinstance(verify_epoch, VerifyEpoch):
            raise ValueError("max-cycle merge-and-defer action requires passing verify epoch metadata")
        if verify_epoch.reviewed_head_sha != current_review_head_sha:
            raise ValueError("max-cycle merge-and-defer verify epoch does not match reviewed head")
    blocker_findings = _capped_review_blocker_findings_for_action(action)
    if not blocker_findings:
        raise ValueError("max-cycle merge-and-defer action requires blocker findings")
    parsed_blockers = validate_capped_review_blocker_action(
        store,
        config=config,
        review_task=review_task,
        findings=blocker_findings,
        persisted_review_output=persisted_review_output,
    )
    expected_blocker_ids = tuple(finding.id for finding in parsed_blockers)
    declared_blocker_ids = action.get("deferred_blocker_ids")
    if not isinstance(declared_blocker_ids, (list, tuple)) or not all(
        isinstance(finding_id, str) for finding_id in declared_blocker_ids
    ):
        raise ValueError("max-cycle merge-and-defer action requires deferred_blocker_ids")
    if tuple(declared_blocker_ids) != expected_blocker_ids:
        raise ValueError(
            "max-cycle merge-and-defer deferred_blocker_ids do not match parsed blocker findings; "
            f"expected {expected_blocker_ids!r}, got {tuple(declared_blocker_ids)!r}"
        )
    return review_task, parsed_blockers, persisted_review_output


def _capped_review_authority_mode(review_task: DbTask) -> str:
    if declares_resolution_review_mode(review_task.review_scope):
        return "resolution"
    if declares_spec_coherence_review_mode(review_task.review_scope):
        return "spec_coherence"
    return "plain_full"


def _normalize_action_completed_at(value: str) -> datetime:
    completed_at = datetime.fromisoformat(value)
    if completed_at.tzinfo is None:
        completed_at = completed_at.replace(tzinfo=UTC)
    return completed_at


def _latest_applicable_capped_review(
    reviews: Iterable[DbTask],
    *,
    config: Config,
    reviewed_head_sha: str,
) -> DbTask | None:
    candidates: list[DbTask] = []
    seen_review_ids: set[str] = set()
    for review in reviews:
        if review.id is None or review.id in seen_review_ids:
            continue
        seen_review_ids.add(review.id)
        if review.status != "completed" or review.task_type != "review":
            continue
        if review.review_verify_head_sha != reviewed_head_sha:
            continue
        review_mode = _capped_review_authority_mode(review)
        if review_mode in {"plain_full", "resolution"}:
            candidates.append(review)
            continue
        if review_mode == "spec_coherence":
            authoritative_output = review.output_content
            if not authoritative_output:
                try:
                    authoritative_output = get_review_content(config.project_dir, review)
                except Exception:
                    authoritative_output = None
            if authoritative_output is None or parse_review_report(authoritative_output).verdict != "APPROVED":
                candidates.append(review)
    if not candidates:
        return None
    return max(candidates, key=lambda review: (review.completed_at or review.created_at, review.created_at))


def _latest_canonical_capped_authority_review(
    reviews: Iterable[DbTask],
    *,
    config: Config,
) -> DbTask | None:
    candidates: list[DbTask] = []
    seen_review_ids: set[str] = set()
    for review in reviews:
        if review.id is None or review.id in seen_review_ids:
            continue
        seen_review_ids.add(review.id)
        if review.status != "completed" or review.task_type != "review":
            continue
        review_mode = _capped_review_authority_mode(review)
        if review_mode in {"plain_full", "resolution"}:
            candidates.append(review)
            continue
        if review_mode == "spec_coherence":
            authoritative_output = review.output_content
            if not authoritative_output:
                try:
                    authoritative_output = get_review_content(config.project_dir, review)
                except Exception:
                    authoritative_output = None
            if authoritative_output is None or parse_review_report(authoritative_output).verdict != "APPROVED":
                candidates.append(review)
    if not candidates:
        return None
    return max(candidates, key=lambda review: (review.completed_at or review.created_at, review.created_at))


def _require_latest_capped_review_action_authority(
    store: SqliteTaskStore,
    *,
    config: Config,
    merge_subject: DbTask,
    action: Mapping[str, Any],
) -> tuple[DbTask, tuple[ReviewFinding, ...], str]:
    validated = _validated_capped_review_action_metadata(
        store,
        config=config,
        action=action,
    )
    if validated is None:
        raise ValueError("max-cycle merge-and-defer action requires capped review authority")
    review_task, blocker_findings, persisted_review_output = validated
    reviewed_head_sha = action.get("current_review_head_sha")
    if not isinstance(reviewed_head_sha, str) or not reviewed_head_sha:
        raise ValueError("max-cycle merge-and-defer action requires current_review_head_sha")
    canonical_reviews = tuple(get_implementation_review_evidence(store, merge_subject))
    canonical_review_ids = {review.id for review in canonical_reviews if review.id is not None}
    if review_task.id not in canonical_review_ids:
        raise ValueError("max-cycle merge-and-defer review is not canonical evidence for the merge subject")
    latest = _latest_canonical_capped_authority_review(
        canonical_reviews,
        config=config,
    )
    if latest is None:
        raise ValueError(
            "max-cycle merge-and-defer latest applicable completed review is no longer backed by "
            "canonical completed review evidence"
        )
    latest_mode = _capped_review_authority_mode(latest)
    if latest_mode == "spec_coherence":
        _raise_spec_coherence_capped_review_preemption(config, latest)
    if latest.review_verify_head_sha != reviewed_head_sha:
        observed = latest.review_verify_head_sha or "unavailable"
        raise ValueError(
            "max-cycle merge-and-defer latest canonical completed review has ambiguous reviewed head; "
            f"expected {reviewed_head_sha}, got {observed}"
        )
    if latest.id != action.get("latest_review_task_id"):
        raise ValueError(
            "max-cycle merge-and-defer review is no longer the latest applicable completed review "
            "from canonical evidence"
        )
    if latest.id != review_task.id:
        raise ValueError(
            "max-cycle merge-and-defer review_task no longer matches latest applicable completed review "
            "from canonical evidence"
        )
    latest_completed_at = latest.completed_at
    if latest_completed_at is None:
        raise ValueError("max-cycle merge-and-defer latest review has no completion time")
    action_completed_at = _normalize_action_completed_at(str(action["latest_review_completed_at"]))
    if latest_completed_at != action_completed_at:
        raise ValueError("max-cycle merge-and-defer latest review completion identity changed")
    if latest_mode != action.get("latest_review_mode"):
        raise ValueError("max-cycle merge-and-defer latest review mode changed")
    if latest.review_verify_head_sha != reviewed_head_sha:
        raise ValueError("max-cycle merge-and-defer latest review head changed")
    authoritative_output = latest.output_content
    if not authoritative_output:
        authoritative_output = get_review_content(config.project_dir, latest)
    if authoritative_output != persisted_review_output:
        raise ValueError("max-cycle merge-and-defer latest review content changed")
    validate_capped_review_blocker_action(
        store,
        config=config,
        review_task=latest,
        findings=blocker_findings,
        persisted_review_output=persisted_review_output,
        authoritative_review_output=authoritative_output,
    )
    return latest, blocker_findings, persisted_review_output


def _require_fresh_capped_review_lifecycle_authority(
    store: SqliteTaskStore,
    *,
    config: Config,
    git: Git,
    merge_subject: DbTask,
    action: Mapping[str, Any],
    target_branch: str,
    allow_already_merged_reconciliation: bool = False,
) -> None:
    """Require the live lifecycle planner to still emit the same capped merge action."""
    proof_git: Any = _AlreadyMergedLifecycleProofGit(git) if allow_already_merged_reconciliation else git
    live_action = determine_next_action(
        config,
        store,
        proof_git,
        merge_subject,
        target_branch,
        selected_for_merge=True,
    )
    if not (live_action.get("type") == "merge" and live_action.get("max_cycles_merge_and_defer") is True):
        action_type = live_action.get("type") or "unknown"
        description = live_action.get("description") or "capped merge is no longer lifecycle-eligible"
        raise _CappedReviewLifecycleAuthorityChanged(
            "max-cycle merge-and-defer lifecycle authority changed before promotion; "
            f"live action is {action_type}: {description}"
        )

    live_review_task, live_blockers, live_review_output = _validated_capped_review_action_metadata(
        store,
        config=config,
        action=live_action,
    ) or (None, (), None)
    action_review_task, action_blockers, action_review_output = _validated_capped_review_action_metadata(
        store,
        config=config,
        action=action,
    ) or (None, (), None)
    if (
        live_review_task is None
        or action_review_task is None
        or live_review_task.id != action_review_task.id
        or live_review_output != action_review_output
        or tuple(finding.id for finding in live_blockers) != tuple(finding.id for finding in action_blockers)
        or live_action.get("current_review_head_sha") != action.get("current_review_head_sha")
        or live_action.get("latest_review_completed_at") != action.get("latest_review_completed_at")
        or live_action.get("latest_review_mode") != action.get("latest_review_mode")
    ):
        raise _CappedReviewLifecycleAuthorityChanged(
            "max-cycle merge-and-defer lifecycle authority no longer matches the planned action"
        )


def _raise_spec_coherence_capped_review_preemption(config: Config, review: DbTask) -> NoReturn:
    authoritative_output = review.output_content
    if not authoritative_output:
        try:
            authoritative_output = get_review_content(config.project_dir, review)
        except Exception as exc:
            raise ValueError(
                "max-cycle merge-and-defer latest applicable completed review is preempted by "
                "a newer unreadable spec-coherence review"
            ) from exc
    verdict = parse_review_report(authoritative_output).verdict
    if verdict == "CHANGES_REQUESTED":
        raise ValueError(
            "max-cycle merge-and-defer latest applicable completed review is preempted by "
            "a newer CHANGES_REQUESTED spec-coherence review"
        )
    if verdict == "NEEDS_DISCUSSION":
        raise ValueError(
            "max-cycle merge-and-defer latest applicable completed review is preempted by "
            "a newer NEEDS_DISCUSSION spec-coherence review"
        )
    raise ValueError(
        "max-cycle merge-and-defer latest applicable completed review is preempted by "
        "a newer unknown or malformed spec-coherence review verdict"
    )


def _require_live_capped_review_merge_authorization(
    store: SqliteTaskStore,
    *,
    config: Config,
    git: Git,
    merge_subject: DbTask,
    action: Mapping[str, Any],
    source_ref: str | None,
    target_branch: str,
    source_ref_sha: str | None = None,
    allow_already_merged_reconciliation: bool = False,
) -> str | None:
    """Revalidate non-replay max-cycle merge authority against the live source."""
    if not (action.get("type") == "merge" and action.get("max_cycles_merge_and_defer") is True):
        return source_ref_sha
    if action.get("pending_merge_finalization") is True:
        return source_ref_sha

    _require_fresh_capped_review_lifecycle_authority(
        store,
        config=config,
        git=git,
        merge_subject=merge_subject,
        action=action,
        target_branch=target_branch,
        allow_already_merged_reconciliation=allow_already_merged_reconciliation,
    )
    _require_latest_capped_review_action_authority(
        store,
        config=config,
        merge_subject=merge_subject,
        action=action,
    )
    if not source_ref:
        raise GitError("max-cycle merge-and-defer action cannot prove live source ref")
    resolved_source_sha = _require_ref_sha_for_merge_finalization_proof(
        git,
        source_ref,
        label="authorized source",
    )
    if source_ref_sha is not None and resolved_source_sha != source_ref_sha:
        raise GitError(
            "max-cycle merge-and-defer source changed after staging; "
            f"expected {source_ref_sha}, got {resolved_source_sha}"
        )
    reviewed_head_sha = action.get("current_review_head_sha")
    if resolved_source_sha != reviewed_head_sha:
        raise GitError(
            "max-cycle merge-and-defer source no longer matches reviewed head; "
            f"expected {reviewed_head_sha}, got {resolved_source_sha}"
        )
    if not isinstance(getattr(config, "verify_command", None), str) or not config.verify_command.strip():
        raise GitError("max-cycle merge-and-defer verify_command is not configured")

    audit = action.get("max_cycles_audit")
    annotated_epoch = audit.get("verify_epoch") if isinstance(audit, Mapping) else None
    if not isinstance(annotated_epoch, VerifyEpoch):
        raise GitError("max-cycle merge-and-defer action is missing verify epoch proof")
    decision = resolve_verify_gate_decision(store, merge_subject, config=config, git=git)
    if decision.state != "passed" or decision.current_epoch is None:
        raise GitError(
            "max-cycle merge-and-defer verify evidence is no longer current and passing "
            f"(state={decision.state})"
        )
    if not verify_epoch_matches(expected=annotated_epoch, candidate=decision.current_epoch):
        raise GitError("max-cycle merge-and-defer verify epoch no longer matches current source head")
    result = decision.lookup.result
    if result is None or result.reviewed_head_sha != resolved_source_sha or result.status != "passed":
        raise GitError("max-cycle merge-and-defer verify result does not authorize the live source head")
    return resolved_source_sha


def _require_post_materialization_capped_review_merge_authorization(
    store: SqliteTaskStore,
    *,
    config: Config,
    git: Git,
    merge_subject: DbTask,
    action: Mapping[str, Any],
    source_ref: str | None,
    source_ref_sha: str | None,
    target_branch: str,
    allow_already_merged_reconciliation: bool = False,
) -> str | None:
    """Revalidate capped authority at the final side-effect boundary after child materialization."""
    if not (action.get("type") == "merge" and action.get("max_cycles_merge_and_defer") is True):
        if _merge_finalization_family_for_action(action) is not None:
            if not source_ref:
                raise GitError("could not prove merge finalization source ref after materialization")
            if not source_ref_sha:
                raise GitError("could not prove merge finalization source after materialization")
            return _require_authorized_source_ref_unchanged(git, source_ref, source_ref_sha)
        return source_ref_sha
    return _require_live_capped_review_merge_authorization(
        store,
        config=config,
        git=git,
        merge_subject=merge_subject,
        action=action,
        source_ref=source_ref,
        target_branch=target_branch,
        source_ref_sha=source_ref_sha,
        allow_already_merged_reconciliation=allow_already_merged_reconciliation,
    )


def _require_staged_authorized_source_ref_unchanged_after_materialization(
    store: SqliteTaskStore,
    *,
    config: Config,
    git: Git,
    staged: "_StagedIsolatedMergeAction",
) -> _StagedIsolatedMergeAction:
    """Recheck proof-bound staged source immediately before prepared-attempt writes."""
    action = staged.merge_action_metadata
    if action.get("pending_merge_finalization") is True or _merge_finalization_family_for_action(action) is None:
        return staged
    source_ref = staged.source_ref or staged.merge_branch
    if not source_ref:
        raise GitError("could not prove isolated merge finalization source ref before promotion")
    if not staged.source_ref_sha:
        raise GitError("could not prove isolated merge finalization source before promotion")
    source_ref_sha = _require_post_materialization_capped_review_merge_authorization(
        store,
        config=config,
        git=git,
        merge_subject=staged.merge_subject,
        action=action,
        source_ref=source_ref,
        source_ref_sha=staged.source_ref_sha,
        target_branch=staged.target_branch,
    )
    return replace(staged, source_ref=source_ref, source_ref_sha=source_ref_sha)


def _materialize_max_cycle_deferred_blockers_for_action(
    store: SqliteTaskStore,
    *,
    config: Config,
    merge_subject: DbTask,
    action: Mapping[str, Any],
    active_scope_tags: tuple[str, ...] | None,
    trigger_source: str,
) -> tuple[list[DbTask], list[DbTask]]:
    if not (action.get("type") == "merge" and action.get("max_cycles_merge_and_defer") is True):
        return [], []

    proven_tasks = action.get("proven_deferred_blocker_tasks")
    if action.get("pending_merge_finalization") is True and proven_tasks is not None:
        if not isinstance(proven_tasks, (list, tuple)) or not all(
            isinstance(task, DbTask) for task in proven_tasks
        ):
            raise ValueError("pending max-cycle finalization replay contains malformed proven blocker tasks")
        review_task = action.get("review_task")
        if not isinstance(review_task, DbTask):
            raise ValueError("pending max-cycle finalization replay requires a review_task")
        persisted_review_output = action.get("persisted_review_output")
        if not isinstance(persisted_review_output, str):
            persisted_review_output = action.get("review_output")
        if not isinstance(persisted_review_output, str):
            raise ValueError("pending max-cycle finalization replay requires persisted review output")
        blocker_findings = _capped_review_blocker_findings_for_action(action)
        if not blocker_findings:
            raise ValueError("pending max-cycle finalization replay requires blocker findings")
        blocker_findings = validate_capped_review_blocker_action(
            store,
            config=config,
            review_task=review_task,
            findings=blocker_findings,
            persisted_review_output=persisted_review_output,
        )
        reused = _validate_proven_capped_replay_tasks(
            store,
            review_task=review_task,
            impl_task=merge_subject,
            findings=blocker_findings,
            persisted_review_output=persisted_review_output,
            proven_tasks=proven_tasks,
        )
        _created, reconciled_reused = _create_or_reuse_capped_review_blocker_tasks(
            store,
            config=config,
            review_task=review_task,
            impl_task=merge_subject,
            findings=blocker_findings,
            persisted_review_output=persisted_review_output,
            active_scope_tags=active_scope_tags,
            trigger_source=trigger_source,
        )
        _require_reused_proven_task_ids(
            reconciled_reused,
            reused,
            kind="max-cycle deferred blocker",
        )
        return [], list(reconciled_reused)

    review_task = action.get("review_task")
    if not isinstance(review_task, DbTask):
        raise ValueError("max-cycle merge-and-defer action requires a review_task")
    persisted_review_output = action.get("persisted_review_output")
    if not isinstance(persisted_review_output, str):
        persisted_review_output = action.get("review_output")
    if not isinstance(persisted_review_output, str):
        raise ValueError("max-cycle merge-and-defer action requires persisted review output")
    blocker_findings = _capped_review_blocker_findings_for_action(action)
    if not blocker_findings:
        raise ValueError("max-cycle merge-and-defer action requires blocker findings")
    blocker_findings = validate_capped_review_blocker_action(
        store,
        config=config,
        review_task=review_task,
        findings=blocker_findings,
        persisted_review_output=persisted_review_output,
    )

    return _create_or_reuse_capped_review_blocker_tasks(
        store,
        config=config,
        review_task=review_task,
        impl_task=merge_subject,
        findings=blocker_findings,
        persisted_review_output=persisted_review_output,
        active_scope_tags=active_scope_tags,
        trigger_source=trigger_source,
    )


def _validate_proven_capped_replay_tasks(
    store: SqliteTaskStore,
    *,
    review_task: DbTask,
    impl_task: DbTask,
    findings: tuple[ReviewFinding, ...],
    persisted_review_output: str,
    proven_tasks: object,
) -> tuple[DbTask, ...]:
    if review_task.id is None or impl_task.id is None:
        raise ValueError("pending max-cycle finalization replay requires persisted review and implementation tasks")
    assert isinstance(proven_tasks, (list, tuple))
    if len(proven_tasks) != len(findings):
        raise ValueError("pending max-cycle finalization replay has the wrong number of proven blocker tasks")
    seen_ids: set[str] = set()
    current_tasks: list[DbTask] = []
    for finding, proven_task in zip(findings, proven_tasks, strict=True):
        assert isinstance(proven_task, DbTask)
        if proven_task.id is None:
            raise ValueError("pending max-cycle finalization replay contains a proven task without an ID")
        if proven_task.id in seen_ids:
            raise ValueError(f"pending max-cycle finalization replay contains duplicate task {proven_task.id}")
        seen_ids.add(proven_task.id)
        current = store.get(proven_task.id)
        if current is None:
            raise ValueError(f"proven max-cycle deferred blocker task {proven_task.id} disappeared before finalization")
        prompt_prefix = build_capped_review_blocker_prompt_prefix(review_task.id, impl_task.id, finding.id)
        matching_children = [
            child
            for child in store.get_based_on_children(impl_task.id)
            if child.task_type == "implement" and child.prompt.strip().startswith(prompt_prefix)
        ]
        if len(matching_children) != 1 or matching_children[0].id != proven_task.id:
            match_ids = ", ".join(str(child.id) for child in matching_children)
            raise ValueError(
                f"Ambiguous capped review blocker task identity for {impl_task.id}: "
                f"{len(matching_children)} children match {prompt_prefix!r} ({match_ids})."
            )
        mismatches = list(
            capped_review_blocker_task_identity_mismatches(
                current,
                review_task_id=review_task.id,
                impl_task_id=impl_task.id,
                finding=finding,
                persisted_review_output=persisted_review_output,
            )
        )
        if task_is_merged(store, current) or current.status in {"dropped", "superseded"}:
            mismatches.append("cannot be reused")
        if mismatches:
            raise ValueError(
                f"proven max-cycle deferred blocker task {proven_task.id} does not match required replay identity: "
                f"{', '.join(mismatches)}"
            )
        current_tasks.append(current)
    return tuple(current_tasks)


def _deferred_blocker_materialization_failed_result(
    *,
    exc: Exception,
    created_followups: list[DbTask] | None = None,
    reused_followups: list[DbTask] | None = None,
    created_investigation_task_ids: list[str] | tuple[str, ...] | None = None,
    reused_investigation_task_ids: list[str] | tuple[str, ...] | None = None,
    created_deferred_blockers: list[DbTask] | tuple[DbTask, ...] | None = None,
    reused_deferred_blockers: list[DbTask] | tuple[DbTask, ...] | None = None,
) -> _MergeActionResult:
    partial_created = getattr(exc, "created", ())
    partial_reused = getattr(exc, "reused", ())
    if partial_created or partial_reused:
        created_deferred_blockers = (*tuple(created_deferred_blockers or ()), *tuple(partial_created))
        reused_deferred_blockers = (*tuple(reused_deferred_blockers or ()), *tuple(partial_reused))
    block_reason = f"max-cycle deferred blocker materialization failed: {exc}"
    print(f"Error: {block_reason}")
    return _MergeActionResult(
        rc=1,
        created_followups=list(created_followups or ()),
        reused_followups=list(reused_followups or ()),
        created_investigation_task_ids=list(created_investigation_task_ids or ()),
        reused_investigation_task_ids=list(reused_investigation_task_ids or ()),
        created_deferred_blockers=list(created_deferred_blockers or ()),
        reused_deferred_blockers=list(reused_deferred_blockers or ()),
        status="deferred_blocker_materialization_failed",
        block_reason=block_reason,
    )


def _format_task_ids(tasks: Iterable[DbTask]) -> str:
    return ", ".join(str(task.id) for task in tasks if task.id is not None)


def _print_advance_deferred_blocker_lines(
    console: Console,
    merge_result: object,
    *,
    indent: str = "",
) -> None:
    ac = _colors.WORK_COLORS
    created_ids = _format_task_ids(getattr(merge_result, "created_deferred_blockers", ()))
    if created_ids:
        console.print(f"{indent}[{ac.merge}]✓ Created deferred blocker task(s): {created_ids}[/{ac.merge}]")
    reused_ids = _format_task_ids(getattr(merge_result, "reused_deferred_blockers", ()))
    if reused_ids:
        console.print(f"{indent}[{ac.waiting}]↺ Reused deferred blocker task(s): {reused_ids}[/{ac.waiting}]")


def _materialize_mandatory_merge_side_effects_for_staged_action(
    store: SqliteTaskStore,
    *,
    config: Config,
    staged: _StagedIsolatedMergeAction,
    active_scope_tags: tuple[str, ...] | None,
    trigger_source: str,
) -> _StagedIsolatedMergeAction:
    created_followups, reused_followups = _materialize_merge_followup_side_effects(
        store,
        config=config,
        merge_subject=staged.merge_subject,
        review_task=staged.review_task,
        followup_findings=staged.followup_findings,
        action=staged.merge_action_metadata,
    )
    created_blockers, reused_blockers = _materialize_max_cycle_deferred_blockers_for_action(
        store,
        config=config,
        merge_subject=staged.merge_subject,
        action=staged.merge_action_metadata,
        active_scope_tags=active_scope_tags,
        trigger_source=trigger_source,
    )
    return replace(
        staged,
        created_followups=tuple(created_followups),
        reused_followups=tuple(reused_followups),
        created_deferred_blockers=tuple(created_blockers),
        reused_deferred_blockers=tuple(reused_blockers),
    )


def _authorize_staged_merge_finalization_before_materialization(
    store: SqliteTaskStore,
    *,
    config: Config,
    live_git: Git,
    staged_git: Git,
    staged: _StagedIsolatedMergeAction,
    target_branch: str,
    allow_already_merged_reconciliation: bool = False,
) -> _StagedIsolatedMergeAction:
    """Prove live source and target identity for staged mandatory side effects."""
    action = staged.merge_action_metadata
    if action.get("pending_merge_finalization") is True or _merge_finalization_family_for_action(action) is None:
        return staged
    source_ref = staged.source_ref or staged.merge_branch
    if not source_ref:
        raise GitError("could not prove isolated merge finalization source ref before promotion")
    staged_source_sha = staged.source_ref_sha or _ref_sha_for_merge_finalization_proof(staged_git, source_ref)
    source_ref_sha = _require_live_capped_review_merge_authorization(
        store,
        config=config,
        git=live_git,
        merge_subject=staged.merge_subject,
        action=action,
        source_ref=source_ref,
        source_ref_sha=staged_source_sha,
        target_branch=staged.target_branch,
        allow_already_merged_reconciliation=allow_already_merged_reconciliation,
    )
    if source_ref_sha is None:
        source_ref_sha = _require_ref_sha_for_merge_finalization_proof(
            live_git,
            source_ref,
            label="authorized source",
        )
    previous_target_sha = _ref_sha_for_merge_finalization_proof(
        live_git,
        target_branch,
    ) or _require_ref_sha_for_merge_finalization_proof(
        staged_git,
        target_branch,
        label="previous target",
    )
    return replace(
        staged,
        source_ref=source_ref,
        source_ref_sha=source_ref_sha,
        previous_target_sha=previous_target_sha,
    )


def _materialize_max_cycle_deferred_blockers_for_staged_action(
    store: SqliteTaskStore,
    *,
    config: Config,
    staged: _StagedIsolatedMergeAction,
    active_scope_tags: tuple[str, ...] | None,
    trigger_source: str,
) -> _StagedIsolatedMergeAction:
    return _materialize_mandatory_merge_side_effects_for_staged_action(
        store,
        config=config,
        staged=staged,
        active_scope_tags=active_scope_tags,
        trigger_source=trigger_source,
    )


def merge_source_for_action(action: Mapping[str, Any], default_merge_source: str) -> str:
    """Return persisted merge provenance for an action execution."""
    if action.get("type") == "merge" and action.get("max_cycles_merge_and_defer") is True:
        return MERGE_SOURCE_MAX_CYCLES_DEFERRED
    return default_merge_source


def _merge_finalization_family_for_action(action: Mapping[str, Any]) -> MergeFinalizationFamily | None:
    if action.get("type") == "merge" and action.get("max_cycles_merge_and_defer") is True:
        return "max_cycles_deferred"
    if action.get("type") == "merge_with_followups":
        return "ordinary_followup"
    return None


def _require_ref_sha_for_merge_finalization_proof(git: Git, ref: str, *, label: str) -> str:
    rev_parse_if_exists = getattr(git, "rev_parse_if_exists", None)
    value = rev_parse_if_exists(ref) if callable(rev_parse_if_exists) else None
    if not isinstance(value, str) or not value:
        raise GitError(f"could not prove {label} ref {ref!r} for merge finalization replay")
    return value


def _require_authorized_source_ref_unchanged(git: Git, ref: str, authorized_sha: str) -> str:
    current_sha = _require_ref_sha_for_merge_finalization_proof(git, ref, label="current source")
    if current_sha != authorized_sha:
        raise GitError(
            "merge source ref changed after lifecycle authorization; "
            f"expected {authorized_sha}, got {current_sha}"
        )
    return current_sha


def _require_authorized_source_contained_in_target(
    git: Git,
    *,
    source_sha: str,
    target_sha: str,
    target_branch: str,
) -> None:
    is_ancestor = getattr(git, "is_ancestor", None)
    if not callable(is_ancestor) or not is_ancestor(source_sha, target_sha):
        raise GitError(
            "could not prove already-merged finalization contains the authorized source "
            f"{source_sha} in target {target_branch} at {target_sha}"
        )


def _ref_tree_sha_for_merge_finalization_proof(git: Git, ref: str | None) -> str | None:
    if not ref:
        return None
    resolve_refs = getattr(git, "resolve_refs", None)
    if callable(resolve_refs):
        resolved = resolve_refs((ref,), peel="tree")
        value = resolved.get(ref) if isinstance(resolved, Mapping) else None
        return value if isinstance(value, str) and value else None
    return None


def _require_ref_tree_sha_for_merge_finalization_proof(git: Git, ref: str, *, label: str) -> str:
    value = _ref_tree_sha_for_merge_finalization_proof(git, ref)
    if not value:
        raise GitError(f"could not prove {label} tree {ref!r} for merge finalization replay")
    return value


def _validate_pending_merge_finalization_proof_for_state(
    store: SqliteTaskStore,
    git: Git,
    *,
    merge_subject: DbTask,
    action: Mapping[str, Any],
    target_branch: str,
    merge_unit_id: str | None,
    created_followups: Iterable[DbTask],
    reused_followups: Iterable[DbTask],
    created_deferred_blockers: Iterable[DbTask],
    reused_deferred_blockers: Iterable[DbTask],
) -> None:
    """Re-read and validate replay proof immediately before merge-state writes."""
    if action.get("pending_merge_finalization") is not True:
        return
    family = _merge_finalization_family_for_action(action)
    if family is None:
        return
    if merge_subject.id is None:
        raise GitError("pending merge finalization proof requires a persisted implementation task")
    raw_proof_id = action.get("merge_finalization_proof_id")
    raw_proof_sha = action.get("merge_finalization_proof_sha")
    if not isinstance(raw_proof_id, int) or not isinstance(raw_proof_sha, str) or not raw_proof_sha:
        raise GitError("pending merge finalization replay is missing immutable proof identity")
    proof = get_merge_finalization_proof(
        store,
        artifact_id=raw_proof_id,
        impl_task_id=merge_subject.id,
    )
    if proof is None:
        raise GitError(f"pending merge finalization proof artifact {raw_proof_id} disappeared or is malformed")
    if proof.artifact.sha256 != raw_proof_sha:
        raise GitError(
            f"pending merge finalization proof artifact {raw_proof_id} changed after planning"
        )
    identity_family, review_task, findings, child_tasks = _merge_finalization_identity_for_action(
        merge_subject=merge_subject,
        action=action,
        created_followups=created_followups,
        reused_followups=reused_followups,
        created_deferred_blockers=created_deferred_blockers,
        reused_deferred_blockers=reused_deferred_blockers,
    )
    assert review_task.id is not None
    expected_finding_ids = merge_finalization_finding_ids(findings)
    expected_child_task_ids = merge_finalization_child_task_ids(child_tasks)
    mismatches: list[str] = []
    if proof.action_family != identity_family or proof.action_family != family:
        mismatches.append("action_family")
    if proof.impl_task_id != merge_subject.id:
        mismatches.append("impl_task_id")
    if proof.review_task_id != review_task.id:
        mismatches.append("review_task_id")
    if proof.finding_ids != expected_finding_ids:
        mismatches.append("finding_ids")
    if proof.child_task_ids != expected_child_task_ids:
        mismatches.append("child_task_ids")
    if merge_subject.branch and proof.source_branch != merge_subject.branch:
        mismatches.append("source_branch")
    if proof.target_branch != target_branch:
        mismatches.append("target_branch")
    if merge_unit_id is not None and proof.merge_unit_id != merge_unit_id:
        mismatches.append("merge_unit_id")
    live_target_sha = _require_ref_sha_for_merge_finalization_proof(
        git,
        target_branch,
        label="pending replay target",
    )
    if live_target_sha != proof.promoted_target_sha:
        mismatches.append("promoted_target_sha")
    if not mismatches:
        if proof.promotion_kind == "squash":
            if not proof.promoted_target_tree_sha:
                raise GitError("pending squash merge finalization proof is missing promoted target tree identity")
            live_target_tree_sha = _require_ref_tree_sha_for_merge_finalization_proof(
                git,
                live_target_sha,
                label="pending replay target",
            )
            if live_target_tree_sha != proof.promoted_target_tree_sha:
                raise GitError(
                    "pending squash merge finalization proof no longer matches live target tree: "
                    f"expected {proof.promoted_target_tree_sha}, got {live_target_tree_sha}"
                )
        else:
            current_source_sha = _require_ref_sha_for_merge_finalization_proof(
                git,
                proof.source_ref,
                label="pending replay source",
            )
            if current_source_sha != proof.source_ref_sha:
                raise GitError(
                    "pending merge finalization proof no longer matches live finalization identity: source_ref_sha"
                )
            _require_authorized_source_contained_in_target(
                git,
                source_sha=proof.source_ref_sha,
                target_sha=live_target_sha,
                target_branch=target_branch,
            )
        return
    raise GitError(
        "pending merge finalization proof no longer matches live finalization identity: "
        f"{', '.join(mismatches)}"
    )


def _ref_sha_for_merge_finalization_proof(git: Git, ref: str | None) -> str | None:
    if not ref:
        return None
    rev_parse_if_exists = getattr(git, "rev_parse_if_exists", None)
    value = rev_parse_if_exists(ref) if callable(rev_parse_if_exists) else None
    return value if isinstance(value, str) and value else None


def _persist_merge_finalization_attempt_proof_for_action(
    store: SqliteTaskStore,
    git: Git,
    *,
    merge_subject: DbTask,
    action: Mapping[str, Any],
    target_branch: str,
    previous_target_sha: str,
    promoted_target_sha: str,
    promotion_kind: MergeFinalizationPromotionKind = "normal",
    promoted_target_tree_sha: str | None = None,
    source_ref: str,
    source_ref_sha: str | None = None,
    merge_unit_id: str | None,
    created_followups: Iterable[DbTask],
    reused_followups: Iterable[DbTask],
    created_deferred_blockers: Iterable[DbTask],
    reused_deferred_blockers: Iterable[DbTask],
) -> None:
    family = _merge_finalization_family_for_action(action)
    if family is None:
        return
    if merge_subject.id is None or not merge_subject.branch:
        raise ValueError("merge finalization proof requires a persisted implementation task and source branch")
    review_task = action.get("review_task")
    if not isinstance(review_task, DbTask) or review_task.id is None:
        raise ValueError("merge finalization proof requires a persisted review task")
    if family == "ordinary_followup":
        findings = _followup_findings_for_proof(action)
        child_tasks = (*tuple(created_followups), *tuple(reused_followups))
    else:
        findings = _capped_review_blocker_findings_for_action(action)
        child_tasks = (*tuple(created_deferred_blockers), *tuple(reused_deferred_blockers))
    finding_ids = merge_finalization_finding_ids(cast("Iterable[ReviewFinding]", findings))
    child_task_ids = merge_finalization_child_task_ids(child_tasks)
    if not finding_ids or len(child_task_ids) != len(finding_ids):
        raise ValueError("merge finalization proof requires exact finding and child task identities")
    if source_ref_sha is None:
        source_ref_sha = _require_ref_sha_for_merge_finalization_proof(git, source_ref, label="source")
    persist_merge_finalization_attempt_proof(
        store,
        action_family=family,
        impl_task_id=merge_subject.id,
        review_task_id=review_task.id,
        finding_ids=finding_ids,
        child_task_ids=child_task_ids,
        source_branch=merge_subject.branch,
        source_ref=source_ref,
        source_ref_sha=source_ref_sha,
        target_branch=target_branch,
        previous_target_sha=previous_target_sha,
        promoted_target_sha=promoted_target_sha,
        promotion_kind=promotion_kind,
        promoted_target_tree_sha=promoted_target_tree_sha,
        merge_unit_id=merge_unit_id,
    )


def _merge_finalization_identity_for_action(
    *,
    merge_subject: DbTask,
    action: Mapping[str, Any],
    created_followups: Iterable[DbTask],
    reused_followups: Iterable[DbTask],
    created_deferred_blockers: Iterable[DbTask],
    reused_deferred_blockers: Iterable[DbTask],
) -> tuple[MergeFinalizationFamily, DbTask, tuple[ReviewFinding, ...], tuple[DbTask, ...]]:
    family = _merge_finalization_family_for_action(action)
    if family is None:
        raise ValueError("merge finalization identity requires a finalization action")
    if merge_subject.id is None or not merge_subject.branch:
        raise ValueError("merge finalization identity requires a persisted implementation task and source branch")
    review_task = action.get("review_task")
    if not isinstance(review_task, DbTask) or review_task.id is None:
        raise ValueError("merge finalization identity requires a persisted review task")
    if family == "ordinary_followup":
        findings = tuple(_followup_findings_for_proof(action))
        child_tasks = (*tuple(created_followups), *tuple(reused_followups))
    else:
        findings = tuple(_capped_review_blocker_findings_for_action(action))
        child_tasks = (*tuple(created_deferred_blockers), *tuple(reused_deferred_blockers))
    finding_ids = merge_finalization_finding_ids(findings)
    child_task_ids = merge_finalization_child_task_ids(child_tasks)
    if not finding_ids or len(child_task_ids) != len(finding_ids):
        raise ValueError("merge finalization identity requires exact finding and child task identities")
    return family, review_task, findings, tuple(child_tasks)


def _persist_merge_finalization_prepared_attempt_for_action(
    store: SqliteTaskStore,
    *,
    merge_subject: DbTask,
    action: Mapping[str, Any],
    target_branch: str,
    previous_target_sha: str,
    source_ref: str,
    source_ref_sha: str,
    merge_unit_id: str | None,
    promotion_observed: bool = False,
    promotion_kind: MergeFinalizationPromotionKind = "normal",
    created_followups: Iterable[DbTask],
    reused_followups: Iterable[DbTask],
    created_deferred_blockers: Iterable[DbTask],
    reused_deferred_blockers: Iterable[DbTask],
) -> None:
    family = _merge_finalization_family_for_action(action)
    if family is None:
        return
    family, review_task, findings, child_tasks = _merge_finalization_identity_for_action(
        merge_subject=merge_subject,
        action=action,
        created_followups=created_followups,
        reused_followups=reused_followups,
        created_deferred_blockers=created_deferred_blockers,
        reused_deferred_blockers=reused_deferred_blockers,
    )
    assert merge_subject.id is not None
    assert merge_subject.branch is not None
    assert review_task.id is not None
    persist_merge_finalization_prepared_attempt(
        store,
        action_family=family,
        impl_task_id=merge_subject.id,
        review_task_id=review_task.id,
        finding_ids=merge_finalization_finding_ids(findings),
        child_task_ids=merge_finalization_child_task_ids(child_tasks),
        source_branch=merge_subject.branch,
        source_ref=source_ref,
        source_ref_sha=source_ref_sha,
        target_branch=target_branch,
        previous_target_sha=previous_target_sha,
        merge_unit_id=merge_unit_id,
        promotion_observed=promotion_observed,
        promotion_kind=promotion_kind,
    )


def _finalize_staged_isolated_merge_action(
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    *,
    staged: _StagedIsolatedMergeAction,
    merge_source: str,
    quiet_mechanics: bool,
) -> _MergeActionResult:
    assert staged.merge_subject.id is not None
    if staged.created_followups or staged.reused_followups:
        created_followups = list(staged.created_followups)
        reused_followups = list(staged.reused_followups)
    else:
        created_followups, reused_followups = _materialize_merge_followup_side_effects(
            store,
            config=config,
            merge_subject=staged.merge_subject,
            review_task=staged.review_task,
            followup_findings=staged.followup_findings,
            action=staged.merge_action_metadata,
        )
    pending = staged.pending_squash_reconcile
    if pending is not None:
        _print_squash_reconcile_result(
            _reconcile_squash_merged_branch_with_origin(
                git,
                branch=pending.branch,
                squash_oid=git.rev_parse(f"refs/heads/{git.default_branch()}"),
                pre_squash_local_oid=pending.pre_squash_local_oid,
                pre_squash_remote_oid=pending.pre_squash_remote_oid,
                remote=pending.remote,
            ),
            suppress_success=quiet_mechanics,
    )
    effective_merge_source = merge_source_for_action(staged.merge_action_metadata, merge_source)
    if (
        staged.merge_action_metadata.get("pending_merge_finalization") is not True
        and _merge_finalization_family_for_action(staged.merge_action_metadata) is not None
    ):
        try:
            source_ref = staged.source_ref or staged.merge_branch
            if not source_ref or not staged.previous_target_sha:
                raise GitError("could not prove isolated merge finalization source or previous target")
            promoted_target_sha = staged.promoted_target_sha or _require_ref_sha_for_merge_finalization_proof(
                git,
                staged.target_branch,
                label="promoted target",
            )
            _persist_merge_finalization_attempt_proof_for_action(
                store,
                git,
                merge_subject=staged.merge_subject,
                action=staged.merge_action_metadata,
                target_branch=staged.target_branch,
                previous_target_sha=staged.previous_target_sha,
                promoted_target_sha=promoted_target_sha,
                promotion_kind=staged.promotion_kind,
                promoted_target_tree_sha=(
                    _require_ref_tree_sha_for_merge_finalization_proof(
                        git,
                        promoted_target_sha,
                        label="promoted target",
                    )
                    if staged.promotion_kind == "squash"
                    else None
                ),
                source_ref=source_ref,
                source_ref_sha=staged.source_ref_sha,
                merge_unit_id=staged.merge_unit_id,
                created_followups=created_followups,
                reused_followups=reused_followups,
                created_deferred_blockers=staged.created_deferred_blockers,
                reused_deferred_blockers=staged.reused_deferred_blockers,
            )
        except Exception as exc:
            return _post_merge_proof_persistence_failed_result(
                exc=exc,
                status="isolated_post_promotion_proof_persistence_failed",
                phase="isolated merge-finalization proof persistence",
                created_followups=created_followups,
                reused_followups=reused_followups,
                created_investigation_task_ids=staged.created_investigation_task_ids,
                reused_investigation_task_ids=staged.reused_investigation_task_ids,
                created_deferred_blockers=staged.created_deferred_blockers,
                reused_deferred_blockers=staged.reused_deferred_blockers,
            )
    try:
        try:
            _validate_pending_merge_finalization_proof_for_state(
                store,
                git,
                merge_subject=staged.merge_subject,
                action=staged.merge_action_metadata,
                target_branch=staged.target_branch,
                merge_unit_id=staged.merge_unit_id,
                created_followups=created_followups,
                reused_followups=reused_followups,
                created_deferred_blockers=staged.created_deferred_blockers,
                reused_deferred_blockers=staged.reused_deferred_blockers,
            )
        except Exception as exc:
            return _pending_merge_finalization_refusal_result(
                exc=exc,
                created_followups=created_followups,
                reused_followups=reused_followups,
                created_investigation_task_ids=staged.created_investigation_task_ids,
                reused_investigation_task_ids=staged.reused_investigation_task_ids,
                created_deferred_blockers=staged.created_deferred_blockers,
                reused_deferred_blockers=staged.reused_deferred_blockers,
            )
        mark_merge_subject_merged(
            store,
            merge_subject=staged.merge_subject,
            merge_unit_id=staged.merge_unit_id,
            merge_source=effective_merge_source,
        )
    except Exception as exc:
        return _post_merge_state_persistence_failed_result(
            exc=exc,
            status="isolated_post_promotion_merge_state_finalization_failed",
            phase="isolated merge-state finalization",
            created_followups=created_followups,
            reused_followups=reused_followups,
            created_investigation_task_ids=staged.created_investigation_task_ids,
            reused_investigation_task_ids=staged.reused_investigation_task_ids,
            created_deferred_blockers=staged.created_deferred_blockers,
            reused_deferred_blockers=staged.reused_deferred_blockers,
        )
    return _MergeActionResult(
        rc=0,
        created_followups=created_followups,
        reused_followups=reused_followups,
        created_investigation_task_ids=list(staged.created_investigation_task_ids),
        reused_investigation_task_ids=list(staged.reused_investigation_task_ids),
        created_deferred_blockers=list(staged.created_deferred_blockers),
        reused_deferred_blockers=list(staged.reused_deferred_blockers),
    )


def _stage_isolated_merge_action(
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    task: DbTask,
    action: dict,
    *,
    target_branch: str,
    current_branch: str,
    merge_git: Git,
    merge_current_branch: str,
    merge_preflight_ref: str | None = None,
    already_merged_behavior: str = "error",
    merge_source: str = MERGE_SOURCE_MANUAL,
    quiet_mechanics: bool = False,
    heartbeat_threshold_seconds: int | None = None,
    heartbeat_interval_seconds: int | None = None,
    on_heartbeat: LongPhaseHeartbeat | None = None,
    active_scope_tags: tuple[str, ...] | None = None,
) -> _StagedIsolatedMergeAction | _MergeActionResult:
    created_investigation_task_ids = tuple(
        task_id for task_id in action.get("created_investigation_task_ids", ()) if isinstance(task_id, str) and task_id
    )
    reused_investigation_task_ids = tuple(
        task_id for task_id in action.get("reused_investigation_task_ids", ()) if isinstance(task_id, str) and task_id
    )
    resolved_subject = (
        _resolve_merge_subject(store, merge_git, task.id or "", target_branch=target_branch) if task.id else None
    )
    merge_subject = resolved_subject.merge_subject if resolved_subject is not None else task
    assert merge_subject.id is not None
    if resolved_subject is not None:
        status_error = _merge_execution_status_error(merge_subject.id, resolved_subject.execution_task)
        if status_error is not None:
            print(f"Error: {status_error}")
            return _MergeActionResult(
                rc=1,
                created_followups=[],
                reused_followups=[],
                created_investigation_task_ids=list(created_investigation_task_ids),
                reused_investigation_task_ids=list(reused_investigation_task_ids),
            )
        if resolved_subject.merge_source_warning:
            print(f"Error: {resolved_subject.merge_source_warning}")
            return _MergeActionResult(
                rc=1,
                created_followups=[],
                reused_followups=[],
                created_investigation_task_ids=list(created_investigation_task_ids),
                reused_investigation_task_ids=list(reused_investigation_task_ids),
            )
    try:
        _validated_capped_review_action_metadata(store, config=config, action=action)
        review_task, followup_findings = _validated_merge_followup_action_metadata(store, action)
    except Exception as exc:
        if action.get("max_cycles_merge_and_defer") is True:
            return _deferred_blocker_materialization_failed_result(
                exc=exc,
                created_investigation_task_ids=created_investigation_task_ids,
                reused_investigation_task_ids=reused_investigation_task_ids,
            )
        return _mandatory_merge_side_effects_failed_result(
            exc=exc,
            created_investigation_task_ids=created_investigation_task_ids,
            reused_investigation_task_ids=reused_investigation_task_ids,
        )
    if merge_current_branch != target_branch:
        print(
            f"Error: Advance merge for task {merge_subject.id} targets '{target_branch}', "
            f"but the active checkout is '{merge_current_branch}'. Switch to '{target_branch}' and rerun."
        )
        return _MergeActionResult(
            rc=1,
            created_followups=[],
            reused_followups=[],
            created_investigation_task_ids=list(created_investigation_task_ids),
            reused_investigation_task_ids=list(reused_investigation_task_ids),
        )
    candidate_source_ref = resolved_subject.merge_source_ref if resolved_subject is not None else None
    candidate_contains_source = (
        already_merged_behavior == "mark_merged"
        and candidate_source_ref is not None
        and merge_git.is_merged(candidate_source_ref, merge_current_branch)
    )
    live_is_merged = getattr(git, "is_merged", None)
    live_target_contains_source = (
        candidate_contains_source
        and candidate_source_ref is not None
        and callable(live_is_merged)
        and live_is_merged(candidate_source_ref, current_branch)
    )
    if live_target_contains_source:
        if resolved_subject is None or candidate_source_ref is None:
            raise GitError("could not prove already-merged finalization source ref")
        proof_source_ref = candidate_source_ref
        proof_source_ref_sha: str | None = None
        if _merge_finalization_family_for_action(action) is not None:
            try:
                proof_source_ref_sha = _require_ref_sha_for_merge_finalization_proof(
                    git,
                    proof_source_ref,
                    label="authorized source",
                )
            except Exception as exc:
                return _pre_promotion_merge_refusal_result(
                    exc=exc,
                    status="pre_merge_proof_persistence_failed",
                    phase="pre-merge finalization source proof",
                    created_investigation_task_ids=created_investigation_task_ids,
                    reused_investigation_task_ids=reused_investigation_task_ids,
                )
        created_followups: list[DbTask] = []
        reused_followups: list[DbTask] = []
        created_deferred_blockers: list[DbTask] = []
        reused_deferred_blockers: list[DbTask] = []
        try:
            authorized_already_merged = _authorize_staged_merge_finalization_before_materialization(
                store,
                config=config,
                live_git=git,
                staged_git=merge_git,
                staged=_StagedIsolatedMergeAction(
                    merge_subject=merge_subject,
                    merge_unit_id=resolved_subject.merge_unit_id,
                    merge_branch=resolved_subject.merge_branch,
                    pending_squash_reconcile=None,
                    review_task=review_task,
                    followup_findings=followup_findings,
                    merge_action_metadata=action,
                    source_ref=proof_source_ref,
                    source_ref_sha=proof_source_ref_sha,
                    target_branch=target_branch,
                ),
                target_branch=target_branch,
                allow_already_merged_reconciliation=True,
            )
            if authorized_already_merged.source_ref is None:
                raise GitError("could not prove already-merged finalization source ref")
            proof_source_ref = authorized_already_merged.source_ref
            proof_source_ref_sha = authorized_already_merged.source_ref_sha
            created_followups, reused_followups = _materialize_merge_followup_side_effects(
                store,
                config=config,
                merge_subject=merge_subject,
                review_task=review_task,
                followup_findings=followup_findings,
                action=action,
            )
            created_deferred_blockers, reused_deferred_blockers = _materialize_max_cycle_deferred_blockers_for_action(
                store,
                config=config,
                merge_subject=merge_subject,
                action=action,
                active_scope_tags=active_scope_tags,
                trigger_source=merge_source,
            )
            if _merge_finalization_family_for_action(action) is not None:
                if not proof_source_ref or not proof_source_ref_sha:
                    raise GitError("could not prove already-merged finalization source ref")
                proof_source_ref_sha = _require_post_materialization_capped_review_merge_authorization(
                    store,
                    config=config,
                    git=git,
                    merge_subject=merge_subject,
                    action=action,
                    source_ref=proof_source_ref,
                    source_ref_sha=proof_source_ref_sha,
                    target_branch=target_branch,
                    allow_already_merged_reconciliation=True,
                )
                if proof_source_ref_sha is None:
                    raise GitError("could not prove already-merged finalization source ref")
                target_sha = _require_ref_sha_for_merge_finalization_proof(
                    git,
                    target_branch,
                    label="target",
                )
                _require_authorized_source_contained_in_target(
                    git,
                    source_sha=proof_source_ref_sha,
                    target_sha=target_sha,
                    target_branch=target_branch,
                )
        except Exception as exc:
            if action.get("max_cycles_merge_and_defer") is True and not followup_findings:
                return _deferred_blocker_materialization_failed_result(
                    exc=exc,
                    created_followups=created_followups,
                    reused_followups=reused_followups,
                    created_investigation_task_ids=created_investigation_task_ids,
                    reused_investigation_task_ids=reused_investigation_task_ids,
                    created_deferred_blockers=created_deferred_blockers,
                    reused_deferred_blockers=reused_deferred_blockers,
                )
            return _mandatory_merge_side_effects_failed_result(
                exc=exc,
                created_followups=created_followups,
                reused_followups=reused_followups,
                created_investigation_task_ids=created_investigation_task_ids,
                reused_investigation_task_ids=reused_investigation_task_ids,
                created_deferred_blockers=created_deferred_blockers,
                reused_deferred_blockers=reused_deferred_blockers,
            )
        effective_merge_source = merge_source_for_action(action, merge_source)
        try:
            try:
                _validate_pending_merge_finalization_proof_for_state(
                    store,
                    git,
                    merge_subject=merge_subject,
                    action=action,
                    target_branch=target_branch,
                    merge_unit_id=resolved_subject.merge_unit_id,
                    created_followups=created_followups,
                    reused_followups=reused_followups,
                    created_deferred_blockers=created_deferred_blockers,
                    reused_deferred_blockers=reused_deferred_blockers,
                )
            except Exception as exc:
                return _pending_merge_finalization_refusal_result(
                    exc=exc,
                    created_followups=created_followups,
                    reused_followups=reused_followups,
                    created_investigation_task_ids=created_investigation_task_ids,
                    reused_investigation_task_ids=reused_investigation_task_ids,
                    created_deferred_blockers=created_deferred_blockers,
                    reused_deferred_blockers=reused_deferred_blockers,
                )
            try:
                proof_source_ref_sha = _require_post_materialization_capped_review_merge_authorization(
                    store,
                    config=config,
                    git=git,
                    merge_subject=merge_subject,
                    action=action,
                    source_ref=proof_source_ref,
                    source_ref_sha=proof_source_ref_sha,
                    target_branch=target_branch,
                    allow_already_merged_reconciliation=True,
                )
            except Exception as exc:
                return _post_merge_state_persistence_failed_result(
                    exc=exc,
                    status="already_merged_state_persistence_failed",
                    phase="already-merged state finalization",
                    created_followups=created_followups,
                    reused_followups=reused_followups,
                    created_investigation_task_ids=created_investigation_task_ids,
                    reused_investigation_task_ids=reused_investigation_task_ids,
                    created_deferred_blockers=created_deferred_blockers,
                    reused_deferred_blockers=reused_deferred_blockers,
                )
            mark_merge_subject_merged(
                store,
                merge_subject=merge_subject,
                merge_unit_id=resolved_subject.merge_unit_id,
                merge_source=effective_merge_source,
            )
        except Exception as exc:
            return _post_merge_state_persistence_failed_result(
                exc=exc,
                status="already_merged_state_persistence_failed",
                phase="already-merged state finalization",
                created_followups=created_followups,
                reused_followups=reused_followups,
                created_investigation_task_ids=created_investigation_task_ids,
                reused_investigation_task_ids=reused_investigation_task_ids,
                created_deferred_blockers=created_deferred_blockers,
                reused_deferred_blockers=reused_deferred_blockers,
            )
        return _MergeActionResult(
            rc=0,
            created_followups=created_followups,
            reused_followups=reused_followups,
            created_investigation_task_ids=list(created_investigation_task_ids),
            reused_investigation_task_ids=list(reused_investigation_task_ids),
            created_deferred_blockers=created_deferred_blockers,
            reused_deferred_blockers=reused_deferred_blockers,
            status="already_merged",
        )
    if candidate_contains_source and not live_target_contains_source:
        if resolved_subject is None or candidate_source_ref is None:
            raise GitError("could not prove candidate-only finalization source ref")
        staged = _StagedIsolatedMergeAction(
            merge_subject=merge_subject,
            merge_unit_id=resolved_subject.merge_unit_id,
            merge_branch=resolved_subject.merge_branch,
            pending_squash_reconcile=None,
            review_task=review_task,
            followup_findings=followup_findings,
            created_investigation_task_ids=created_investigation_task_ids,
            reused_investigation_task_ids=reused_investigation_task_ids,
            merge_action_metadata=dict(action),
            source_ref=candidate_source_ref,
            target_branch=target_branch,
        )
        try:
            return _authorize_staged_merge_finalization_before_materialization(
                store,
                config=config,
                live_git=git,
                staged_git=merge_git,
                staged=staged,
                target_branch=target_branch,
            )
        except Exception as exc:
            return _pre_promotion_merge_refusal_result(
                exc=exc,
                status="pre_merge_proof_persistence_failed",
                phase="candidate-only merge finalization proof",
                created_investigation_task_ids=created_investigation_task_ids,
                reused_investigation_task_ids=reused_investigation_task_ids,
            )
    merge_args = _build_auto_merge_args(
        config,
        merge_git,
        resolved_subject.merge_source_ref if resolved_subject is not None else task.branch,
        target_branch,
    )
    pending_squash_reconcile: _PendingSquashBranchReconcile | None = None
    if getattr(merge_args, "squash", False) and resolved_subject is not None and resolved_subject.merge_branch:
        pending_squash_reconcile = _capture_pre_squash_reconcile_state(
            git,
            branch=resolved_subject.merge_branch,
        )
    assert task.id is not None
    effective_merge_source = merge_source_for_action(action, merge_source)
    merge_result = _coerce_merge_single_task_result(
        _merge_single_task(
            task.id,
            config,
            store,
            merge_git,
            merge_args,
            merge_current_branch,
            merge_preflight_ref=merge_preflight_ref,
            merge_source=effective_merge_source,
            quiet_mechanics=quiet_mechanics,
            materialize_side_effects=False,
            heartbeat_threshold_seconds=heartbeat_threshold_seconds,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            on_heartbeat=on_heartbeat,
            resolved_subject=resolved_subject,
        )
    )
    if merge_result.rc != 0:
        return _MergeActionResult(
            rc=merge_result.rc,
            created_followups=[],
            reused_followups=[],
            created_investigation_task_ids=list(created_investigation_task_ids),
            reused_investigation_task_ids=list(reused_investigation_task_ids),
            status=merge_result.status,
            block_reason=merge_result.block_reason,
        )
    authorized_action = merge_result.authorized_merge_action or dict(action)
    try:
        _validated_capped_review_action_metadata(store, config=config, action=authorized_action)
        review_task, followup_findings = _validated_merge_followup_action_metadata(store, authorized_action)
    except Exception as exc:
        return _mandatory_merge_side_effects_failed_result(
            exc=exc,
            created_investigation_task_ids=created_investigation_task_ids,
            reused_investigation_task_ids=reused_investigation_task_ids,
        )
    return _StagedIsolatedMergeAction(
        merge_subject=merge_subject,
        merge_unit_id=resolved_subject.merge_unit_id if resolved_subject is not None else None,
        merge_branch=resolved_subject.merge_branch if resolved_subject is not None else task.branch,
        target_branch=target_branch,
        pending_squash_reconcile=pending_squash_reconcile or merge_result.pending_squash_reconcile,
        review_task=review_task,
        followup_findings=followup_findings,
        created_investigation_task_ids=created_investigation_task_ids,
        reused_investigation_task_ids=reused_investigation_task_ids,
        merge_action_metadata=dict(authorized_action),
        source_ref=resolved_subject.merge_source_ref if resolved_subject is not None else task.branch,
        source_ref_sha=merge_result.authorized_source_ref_sha,
        promotion_kind="squash" if getattr(merge_args, "squash", False) else "normal",
    )


CLI_VERIFY_HEARTBEAT_THRESHOLD_SECONDS = 15
CLI_VERIFY_HEARTBEAT_INTERVAL_SECONDS = 30


def _format_elapsed(seconds: float) -> str:
    total = int(max(0.0, seconds))
    minutes, secs = divmod(total, 60)
    return f"{minutes}m{secs:02d}s" if minutes else f"{secs}s"


class _CliVerifyProgressHeartbeat:
    """Print periodic pre-merge verify progress to stdout for interactive CLI runs."""

    def __init__(self, console: Console, label: str) -> None:
        self._console = console
        self._label = label

    def __call__(self, progress: LongPhaseProgress) -> None:
        elapsed = _format_elapsed(progress.elapsed_seconds)
        if progress.no_progress:
            detail = "no output yet"
        else:
            detail = f"+{progress.output_lines_delta} line(s)"
        self._console.print(f"{self._label} running - {elapsed} elapsed, {detail}")


_advance_progress_console = Console()


def _drain_pending_stdin() -> None:
    """Discard buffered typeahead so a stray keystroke cannot answer a prompt.

    Long silent phases (verify gates, post-merge side effects) tempt the operator
    into pressing keys to check for life. Without this drain those keystrokes sit
    in the tty buffer and are consumed by the next confirm prompt, auto-approving
    an action nobody read.
    """
    try:
        if not sys.stdin.isatty():
            return
        import termios

        termios.tcflush(sys.stdin.fileno(), termios.TCIFLUSH)
    except (OSError, ImportError, ValueError):
        return


def _execute_merge_action(
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    task: DbTask,
    action: dict,
    *,
    target_branch: str,
    current_branch: str,
    merge_git: Git | None = None,
    merge_current_branch: str | None = None,
    already_merged_behavior: str = "error",
    merge_source: str = MERGE_SOURCE_MANUAL,
    quiet_mechanics: bool = False,
    heartbeat_threshold_seconds: int | None = None,
    heartbeat_interval_seconds: int | None = None,
    on_heartbeat: LongPhaseHeartbeat | None = None,
    active_scope_tags: tuple[str, ...] | None = None,
) -> _MergeActionResult:
    """Execute a merge-style advance action and materialize follow-up tasks if needed."""
    created_followups: list[DbTask] = []
    reused_followups: list[DbTask] = []
    created_deferred_blockers: list[DbTask] = []
    reused_deferred_blockers: list[DbTask] = []
    created_investigation_task_ids = [
        task_id for task_id in action.get("created_investigation_task_ids", ()) if isinstance(task_id, str) and task_id
    ]
    reused_investigation_task_ids = [
        task_id for task_id in action.get("reused_investigation_task_ids", ()) if isinstance(task_id, str) and task_id
    ]
    execution_git = merge_git or git
    execution_branch = merge_current_branch or current_branch
    isolated_promotion = merge_git is not None and merge_git.repo_dir != git.repo_dir
    canonical_live_proof_available = all(
        callable(getattr(git, method_name, None))
        for method_name in ("is_merged", "is_ancestor", "rev_parse_if_exists")
    )
    live_proof_git = git if canonical_live_proof_available else execution_git
    resolved_subject = (
        _resolve_merge_subject(store, execution_git, task.id or "", target_branch=target_branch) if task.id else None
    )
    merge_subject = resolved_subject.merge_subject if resolved_subject is not None else task
    assert merge_subject.id is not None

    if resolved_subject is not None:
        status_error = _merge_execution_status_error(merge_subject.id, resolved_subject.execution_task)
        if status_error is not None:
            print(f"Error: {status_error}")
            return _MergeActionResult(
                rc=1,
                created_followups=created_followups,
                reused_followups=reused_followups,
                created_investigation_task_ids=created_investigation_task_ids,
                reused_investigation_task_ids=reused_investigation_task_ids,
            )

    if execution_branch != target_branch:
        print(
            f"Error: Advance merge for task {merge_subject.id} targets '{target_branch}', "
            f"but the active checkout is '{execution_branch}'. Switch to '{target_branch}' and rerun."
        )
        return _MergeActionResult(
            rc=1,
            created_followups=created_followups,
            reused_followups=reused_followups,
            created_investigation_task_ids=created_investigation_task_ids,
            reused_investigation_task_ids=reused_investigation_task_ids,
        )

    if resolved_subject is not None and resolved_subject.merge_source_warning:
        print(f"Error: {resolved_subject.merge_source_warning}")
        return _MergeActionResult(
            rc=1,
            created_followups=created_followups,
            reused_followups=reused_followups,
            created_investigation_task_ids=created_investigation_task_ids,
            reused_investigation_task_ids=reused_investigation_task_ids,
        )

    try:
        _validated_capped_review_action_metadata(store, config=config, action=action)
        review_task, followup_findings = _validated_merge_followup_action_metadata(store, action)
    except Exception as exc:
        if action.get("max_cycles_merge_and_defer") is True:
            return _deferred_blocker_materialization_failed_result(
                exc=exc,
                created_followups=created_followups,
                reused_followups=reused_followups,
                created_investigation_task_ids=created_investigation_task_ids,
                reused_investigation_task_ids=reused_investigation_task_ids,
                created_deferred_blockers=created_deferred_blockers,
                reused_deferred_blockers=reused_deferred_blockers,
            )
        return _mandatory_merge_side_effects_failed_result(
            exc=exc,
            created_followups=created_followups,
            reused_followups=reused_followups,
            created_investigation_task_ids=created_investigation_task_ids,
            reused_investigation_task_ids=reused_investigation_task_ids,
            created_deferred_blockers=created_deferred_blockers,
            reused_deferred_blockers=reused_deferred_blockers,
        )

    assert task.id is not None
    effective_merge_source = merge_source_for_action(action, merge_source)
    allow_already_merged_reconciliation = already_merged_behavior == "mark_merged"
    mandatory_side_effects_materialized = False
    proof_source_ref = (resolved_subject.merge_source_ref if resolved_subject is not None else None) or task.branch
    proof_source_ref_sha: str | None = None
    promotion_kind: MergeFinalizationPromotionKind = "normal"
    if (
        action.get("type") == "merge"
        and action.get("max_cycles_merge_and_defer") is True
        and action.get("pending_merge_finalization") is not True
    ):
        if not proof_source_ref:
            return _pre_promotion_merge_refusal_result(
                exc=GitError("could not prove merge finalization source ref before materialization"),
                status="pre_merge_proof_persistence_failed",
                phase="pre-merge finalization source proof",
                created_followups=created_followups,
                reused_followups=reused_followups,
                created_investigation_task_ids=created_investigation_task_ids,
                reused_investigation_task_ids=reused_investigation_task_ids,
                created_deferred_blockers=created_deferred_blockers,
                reused_deferred_blockers=reused_deferred_blockers,
            )
        try:
            proof_source_ref_sha = _require_ref_sha_for_merge_finalization_proof(
                live_proof_git,
                proof_source_ref,
                label="authorized source",
            )
            proof_source_ref_sha = _require_live_capped_review_merge_authorization(
                store,
                config=config,
                git=live_proof_git,
                merge_subject=merge_subject,
                action=action,
                source_ref=proof_source_ref,
                source_ref_sha=proof_source_ref_sha,
                target_branch=target_branch,
                allow_already_merged_reconciliation=allow_already_merged_reconciliation,
            )
        except Exception as exc:
            return _pre_promotion_merge_refusal_result(
                exc=exc,
                status="pre_merge_proof_persistence_failed",
                phase="pre-merge finalization source proof",
                created_followups=created_followups,
                reused_followups=reused_followups,
                created_investigation_task_ids=created_investigation_task_ids,
                reused_investigation_task_ids=reused_investigation_task_ids,
                created_deferred_blockers=created_deferred_blockers,
                reused_deferred_blockers=reused_deferred_blockers,
            )
    previous_target_sha: str | None = None

    def _materialize_mandatory_side_effects_before_state(
        authorized_action: Mapping[str, Any] | None = None,
    ) -> _MergeActionResult | None:
        nonlocal mandatory_side_effects_materialized
        nonlocal created_followups, reused_followups, created_deferred_blockers, reused_deferred_blockers
        nonlocal previous_target_sha, proof_source_ref_sha
        if mandatory_side_effects_materialized:
            return None
        effective_action = authorized_action or action
        try:
            _validated_capped_review_action_metadata(
                store,
                config=config,
                action=effective_action,
            )
            effective_review_task, effective_followup_findings = _validated_merge_followup_action_metadata(
                store,
                effective_action,
            )
            if (
                effective_action.get("type") == "merge"
                and effective_action.get("max_cycles_merge_and_defer") is True
                and effective_action.get("pending_merge_finalization") is not True
            ):
                _require_fresh_capped_review_lifecycle_authority(
                    store,
                    config=config,
                    git=live_proof_git,
                    merge_subject=merge_subject,
                    action=effective_action,
                    target_branch=target_branch,
                    allow_already_merged_reconciliation=allow_already_merged_reconciliation,
                )
            if (
                _merge_finalization_family_for_action(effective_action) is not None
                and proof_source_ref_sha is None
                and proof_source_ref
            ):
                proof_source_ref_sha = _require_ref_sha_for_merge_finalization_proof(
                    live_proof_git,
                    proof_source_ref,
                    label="authorized source",
                )
                proof_source_ref_sha = _require_live_capped_review_merge_authorization(
                    store,
                    config=config,
                    git=live_proof_git,
                    merge_subject=merge_subject,
                    action=effective_action,
                    source_ref=proof_source_ref,
                    source_ref_sha=proof_source_ref_sha,
                    target_branch=target_branch,
                    allow_already_merged_reconciliation=allow_already_merged_reconciliation,
                )
            created_followups, reused_followups = _materialize_merge_followup_side_effects(
                store,
                config=config,
                merge_subject=merge_subject,
                review_task=effective_review_task,
                followup_findings=effective_followup_findings,
                action=effective_action,
            )
            created_deferred_blockers, reused_deferred_blockers = _materialize_max_cycle_deferred_blockers_for_action(
                store,
                config=config,
                merge_subject=merge_subject,
                action=effective_action,
                active_scope_tags=active_scope_tags,
                trigger_source=merge_source,
            )
        except Exception as exc:
            if effective_action.get("max_cycles_merge_and_defer") is True:
                if isinstance(exc, _CappedReviewLifecycleAuthorityChanged):
                    return _pre_promotion_merge_refusal_result(
                        exc=exc,
                        status="max_cycle_lifecycle_authority_changed",
                        phase="pre-materialization lifecycle authority proof",
                        created_followups=created_followups,
                        reused_followups=reused_followups,
                        created_investigation_task_ids=created_investigation_task_ids,
                        reused_investigation_task_ids=reused_investigation_task_ids,
                        created_deferred_blockers=created_deferred_blockers,
                        reused_deferred_blockers=reused_deferred_blockers,
                    )
                return _deferred_blocker_materialization_failed_result(
                    exc=exc,
                    created_followups=created_followups,
                    reused_followups=reused_followups,
                    created_investigation_task_ids=created_investigation_task_ids,
                    reused_investigation_task_ids=reused_investigation_task_ids,
                    created_deferred_blockers=created_deferred_blockers,
                    reused_deferred_blockers=reused_deferred_blockers,
                )
            return _mandatory_merge_side_effects_failed_result(
                exc=exc,
                created_followups=created_followups,
                reused_followups=reused_followups,
                created_investigation_task_ids=created_investigation_task_ids,
                reused_investigation_task_ids=reused_investigation_task_ids,
                created_deferred_blockers=created_deferred_blockers,
                reused_deferred_blockers=reused_deferred_blockers,
            )
        if (
            _merge_finalization_family_for_action(effective_action) is not None
            and effective_action.get("pending_merge_finalization") is not True
            and not isolated_promotion
        ):
            try:
                if not proof_source_ref or not proof_source_ref_sha:
                    raise GitError("could not prove merge finalization source ref before promotion")
                proof_source_ref_sha = _require_post_materialization_capped_review_merge_authorization(
                    store,
                    config=config,
                    git=live_proof_git,
                    merge_subject=merge_subject,
                    action=effective_action,
                    source_ref=proof_source_ref,
                    source_ref_sha=proof_source_ref_sha,
                    target_branch=target_branch,
                    allow_already_merged_reconciliation=allow_already_merged_reconciliation,
                )
                if proof_source_ref_sha is None:
                    raise GitError("could not prove merge finalization source ref before promotion")
            except Exception as exc:
                return _pre_promotion_merge_refusal_result(
                    exc=exc,
                    status="merge_source_ref_changed",
                    phase="pre-merge authorized source proof",
                    created_followups=created_followups,
                    reused_followups=reused_followups,
                    created_investigation_task_ids=created_investigation_task_ids,
                    reused_investigation_task_ids=reused_investigation_task_ids,
                    created_deferred_blockers=created_deferred_blockers,
                    reused_deferred_blockers=reused_deferred_blockers,
                )
            try:
                previous_target_sha = _require_ref_sha_for_merge_finalization_proof(
                    live_proof_git,
                    target_branch,
                    label="previous target",
                )
                _persist_merge_finalization_prepared_attempt_for_action(
                    store,
                    merge_subject=merge_subject,
                    action=effective_action,
                    target_branch=target_branch,
                    previous_target_sha=previous_target_sha,
                    source_ref=proof_source_ref,
                    source_ref_sha=proof_source_ref_sha,
                    merge_unit_id=resolved_subject.merge_unit_id if resolved_subject is not None else None,
                    promotion_kind=promotion_kind,
                    created_followups=created_followups,
                    reused_followups=reused_followups,
                    created_deferred_blockers=created_deferred_blockers,
                    reused_deferred_blockers=reused_deferred_blockers,
                )
            except Exception as exc:
                return _pre_promotion_merge_refusal_result(
                    exc=exc,
                    status="pre_merge_proof_persistence_failed",
                    phase="pre-merge target proof",
                    created_followups=created_followups,
                    reused_followups=reused_followups,
                    created_investigation_task_ids=created_investigation_task_ids,
                    reused_investigation_task_ids=reused_investigation_task_ids,
                    created_deferred_blockers=created_deferred_blockers,
                    reused_deferred_blockers=reused_deferred_blockers,
                )
        mandatory_side_effects_materialized = True
        return None

    already_merged_source_ref: str | None = (
        resolved_subject.merge_source_ref if resolved_subject is not None else None
    )
    execution_contains_source = (
        already_merged_behavior == "mark_merged"
        and already_merged_source_ref is not None
        and execution_git.is_merged(already_merged_source_ref, execution_branch)
    )
    live_contains_source = (
        execution_contains_source
        and already_merged_source_ref is not None
        and live_proof_git.is_merged(already_merged_source_ref, current_branch)
    )
    if execution_contains_source and (not isolated_promotion or live_contains_source):
        if resolved_subject is None or already_merged_source_ref is None:
            raise GitError("could not prove already-merged finalization source ref")
        materialization_error = _materialize_mandatory_side_effects_before_state(action)
        if materialization_error is not None:
            return materialization_error
        if (
            action.get("pending_merge_finalization") is not True
            and _merge_finalization_family_for_action(action) is not None
        ):
            try:
                if not proof_source_ref or not proof_source_ref_sha:
                    raise GitError("could not prove already-merged finalization source ref")
                proof_source_ref_sha = _require_post_materialization_capped_review_merge_authorization(
                    store,
                    config=config,
                    git=live_proof_git,
                    merge_subject=merge_subject,
                    action=action,
                    source_ref=proof_source_ref,
                    source_ref_sha=proof_source_ref_sha,
                    target_branch=target_branch,
                    allow_already_merged_reconciliation=allow_already_merged_reconciliation,
                )
                if proof_source_ref_sha is None:
                    raise GitError("could not prove already-merged finalization source ref")
                target_sha = _require_ref_sha_for_merge_finalization_proof(
                    live_proof_git,
                    target_branch,
                    label="target",
                )
                _require_authorized_source_contained_in_target(
                    live_proof_git,
                    source_sha=proof_source_ref_sha,
                    target_sha=target_sha,
                    target_branch=target_branch,
                )
                _persist_merge_finalization_prepared_attempt_for_action(
                    store,
                    merge_subject=merge_subject,
                    action=action,
                    target_branch=target_branch,
                    previous_target_sha=target_sha,
                    source_ref=already_merged_source_ref,
                    source_ref_sha=proof_source_ref_sha,
                    merge_unit_id=resolved_subject.merge_unit_id,
                    promotion_observed=True,
                    created_followups=created_followups,
                    reused_followups=reused_followups,
                    created_deferred_blockers=created_deferred_blockers,
                    reused_deferred_blockers=reused_deferred_blockers,
                )
                _persist_merge_finalization_attempt_proof_for_action(
                    store,
                    live_proof_git,
                    merge_subject=merge_subject,
                    action=action,
                    target_branch=target_branch,
                    previous_target_sha=target_sha,
                    promoted_target_sha=target_sha,
                    source_ref=already_merged_source_ref,
                    source_ref_sha=proof_source_ref_sha,
                    merge_unit_id=resolved_subject.merge_unit_id,
                    created_followups=created_followups,
                    reused_followups=reused_followups,
                    created_deferred_blockers=created_deferred_blockers,
                    reused_deferred_blockers=reused_deferred_blockers,
                )
            except Exception as exc:
                return _post_merge_proof_persistence_failed_result(
                    exc=exc,
                    status="already_merged_proof_persistence_failed",
                    phase="already-merged finalization proof persistence",
                    created_followups=created_followups,
                    reused_followups=reused_followups,
                    created_investigation_task_ids=created_investigation_task_ids,
                    reused_investigation_task_ids=reused_investigation_task_ids,
                    created_deferred_blockers=created_deferred_blockers,
                    reused_deferred_blockers=reused_deferred_blockers,
                )
        try:
            try:
                _validate_pending_merge_finalization_proof_for_state(
                    store,
                    live_proof_git,
                    merge_subject=merge_subject,
                    action=action,
                    target_branch=target_branch,
                    merge_unit_id=resolved_subject.merge_unit_id,
                    created_followups=created_followups,
                    reused_followups=reused_followups,
                    created_deferred_blockers=created_deferred_blockers,
                    reused_deferred_blockers=reused_deferred_blockers,
                )
            except Exception as exc:
                return _pending_merge_finalization_refusal_result(
                    exc=exc,
                    created_followups=created_followups,
                    reused_followups=reused_followups,
                    created_investigation_task_ids=created_investigation_task_ids,
                    reused_investigation_task_ids=reused_investigation_task_ids,
                    created_deferred_blockers=created_deferred_blockers,
                    reused_deferred_blockers=reused_deferred_blockers,
                )
            try:
                proof_source_ref_sha = _require_post_materialization_capped_review_merge_authorization(
                    store,
                    config=config,
                    git=live_proof_git,
                    merge_subject=merge_subject,
                    action=action,
                    source_ref=proof_source_ref,
                    source_ref_sha=proof_source_ref_sha,
                    target_branch=target_branch,
                    allow_already_merged_reconciliation=allow_already_merged_reconciliation,
                )
            except Exception as exc:
                return _post_merge_state_persistence_failed_result(
                    exc=exc,
                    status="already_merged_state_persistence_failed",
                    phase="already-merged state finalization",
                    created_followups=created_followups,
                    reused_followups=reused_followups,
                    created_investigation_task_ids=created_investigation_task_ids,
                    reused_investigation_task_ids=reused_investigation_task_ids,
                    created_deferred_blockers=created_deferred_blockers,
                    reused_deferred_blockers=reused_deferred_blockers,
                )
            mark_merge_subject_merged(
                store,
                merge_subject=merge_subject,
                merge_unit_id=resolved_subject.merge_unit_id,
                merge_source=effective_merge_source,
            )
        except Exception as exc:
            return _post_merge_state_persistence_failed_result(
                exc=exc,
                status="already_merged_state_persistence_failed",
                phase="already-merged state finalization",
                created_followups=created_followups,
                reused_followups=reused_followups,
                created_investigation_task_ids=created_investigation_task_ids,
                reused_investigation_task_ids=reused_investigation_task_ids,
                created_deferred_blockers=created_deferred_blockers,
                reused_deferred_blockers=reused_deferred_blockers,
            )
        return _MergeActionResult(
            rc=0,
            created_followups=created_followups,
            reused_followups=reused_followups,
            created_investigation_task_ids=created_investigation_task_ids,
            reused_investigation_task_ids=reused_investigation_task_ids,
            created_deferred_blockers=created_deferred_blockers,
            reused_deferred_blockers=reused_deferred_blockers,
            status="already_merged",
        )

    candidate_verify_required = bool(config.main_checkout_isolate and verify_gate_enabled(config))
    if candidate_verify_required and not isolated_promotion:
        print(
            "Error: pre-promotion candidate verify requires the isolated host merge checkout; "
            "refusing to promote without it."
        )
        return _MergeActionResult(
            rc=1,
            created_followups=created_followups,
            reused_followups=reused_followups,
            created_investigation_task_ids=created_investigation_task_ids,
            reused_investigation_task_ids=reused_investigation_task_ids,
            status="blocked_candidate_verify_unavailable",
            block_reason="isolated host merge checkout unavailable for pre-promotion candidate verify",
        )

    merge_args = _build_auto_merge_args(
        config,
        execution_git,
        resolved_subject.merge_source_ref if resolved_subject is not None else task.branch,
        target_branch,
    )
    promotion_kind = "squash" if getattr(merge_args, "squash", False) else "normal"
    staged_isolated: _StagedIsolatedMergeAction | None = None
    if isolated_promotion:
        staged_result = _stage_isolated_merge_action(
            config,
            store,
            git,
            task,
            action,
            target_branch=target_branch,
            current_branch=current_branch,
            merge_git=execution_git,
            merge_current_branch=execution_branch,
            already_merged_behavior=already_merged_behavior,
            merge_source=merge_source,
            quiet_mechanics=quiet_mechanics,
            heartbeat_threshold_seconds=heartbeat_threshold_seconds,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            on_heartbeat=on_heartbeat,
            active_scope_tags=active_scope_tags,
        )
        if isinstance(staged_result, _MergeActionResult):
            return staged_result
        staged_isolated = staged_result
        merge_result = _MergeSingleTaskResult(rc=0)
    else:
        merge_result = _coerce_merge_single_task_result(
            _merge_single_task(
                task.id,
                config,
                store,
                execution_git,
                merge_args,
                execution_branch,
                merge_source=effective_merge_source,
                quiet_mechanics=quiet_mechanics,
                heartbeat_threshold_seconds=heartbeat_threshold_seconds,
                heartbeat_interval_seconds=heartbeat_interval_seconds,
                on_heartbeat=on_heartbeat,
                materialize_side_effects=False,
                before_irreversible_side_effect=_materialize_mandatory_side_effects_before_state,
                resolved_subject=resolved_subject,
            )
        )
    rc = merge_result.rc
    result_status = merge_result.status
    result_block_reason = merge_result.block_reason
    final_authorized_action = merge_result.authorized_merge_action or action
    effective_merge_source = merge_source_for_action(final_authorized_action, merge_source)
    if merge_result.created_followups:
        created_followups = list(merge_result.created_followups)
    if merge_result.reused_followups:
        reused_followups = list(merge_result.reused_followups)
    if merge_result.created_deferred_blockers:
        created_deferred_blockers = list(merge_result.created_deferred_blockers)
    if merge_result.reused_deferred_blockers:
        reused_deferred_blockers = list(merge_result.reused_deferred_blockers)
    promotion_warnings: tuple[str, ...] = ()
    candidate_verify = None
    verified_head_sha: str | None = None
    verified_tree_fingerprint: str | None = None
    if rc == 0 and candidate_verify_required:
        _verify_console = Console()
        _verify_command_label = getattr(config, "verify_command", None) or "verify"
        if not quiet_mechanics:
            _verify_console.print(f"Running pre-merge verify ({_verify_command_label}) on the merge candidate...")
        _cli_heartbeat = (
            None if quiet_mechanics else _CliVerifyProgressHeartbeat(_verify_console, "Pre-merge verify")
        )
        _verify_heartbeat = on_heartbeat or _cli_heartbeat

        def _announce_red_rerun(attempt: int, total: int, _evidence: object) -> None:
            if quiet_mechanics:
                return
            _verify_console.print(f"Pre-merge verify came back red; re-running to confirm (attempt {attempt} of {total})")

        candidate_verify = check_candidate_integration_verify(
            config,
            execution_git,
            reason="merge-executor-pre-promotion",
            red_reruns=2,
            env=_owned_git_env(execution_git),
            on_red_rerun_start=_announce_red_rerun,
            heartbeat_threshold_seconds=(
                heartbeat_threshold_seconds
                if heartbeat_threshold_seconds is not None
                else CLI_VERIFY_HEARTBEAT_THRESHOLD_SECONDS
            ),
            heartbeat_interval_seconds=(
                heartbeat_interval_seconds
                if heartbeat_interval_seconds is not None
                else CLI_VERIFY_HEARTBEAT_INTERVAL_SECONDS
            ),
            heartbeat_for_attempt=(lambda _attempt: _verify_heartbeat) if _verify_heartbeat is not None else None,
        )
        if not quiet_mechanics:
            _verify_status = getattr(getattr(candidate_verify, "evidence", None), "verify_status", None)
            _verify_console.print(f"Pre-merge verify {_verify_status or 'finished'}")
        proof = _candidate_verify_promotion_proof(execution_git, candidate_verify)
        if not proof.exact_match:
            print(f"Error: {proof.block_reason}")
            return _MergeActionResult(
                rc=1,
                created_followups=created_followups,
                reused_followups=reused_followups,
                created_investigation_task_ids=created_investigation_task_ids,
                reused_investigation_task_ids=reused_investigation_task_ids,
                status=proof.blocked_status,
                block_reason=proof.block_reason,
                candidate_verify=candidate_verify,
            )
        verified_head_sha = proof.verified_head_sha
        verified_tree_fingerprint = proof.verified_tree_fingerprint
    if rc == 0 and merge_git is not None and merge_git.repo_dir != git.repo_dir:
        assert staged_isolated is not None
        if not quiet_mechanics:
            _advance_progress_console.print("Finalizing merge (materializing follow-ups and promoting)...")
        try:
            if (
                staged_isolated.merge_action_metadata.get("pending_merge_finalization") is not True
                and _merge_finalization_family_for_action(staged_isolated.merge_action_metadata) is not None
            ):
                staged_isolated = _authorize_staged_merge_finalization_before_materialization(
                    store,
                    config=config,
                    live_git=git,
                    staged_git=execution_git,
                    staged=staged_isolated,
                    target_branch=target_branch,
                )
                source_ref = staged_isolated.source_ref
                source_ref_sha = staged_isolated.source_ref_sha
                assert staged_isolated.previous_target_sha is not None
            staged_isolated = _materialize_mandatory_merge_side_effects_for_staged_action(
                store,
                config=config,
                staged=staged_isolated,
                active_scope_tags=active_scope_tags,
                trigger_source=merge_source,
            )
            created_followups = list(staged_isolated.created_followups)
            reused_followups = list(staged_isolated.reused_followups)
            created_deferred_blockers = list(staged_isolated.created_deferred_blockers)
            reused_deferred_blockers = list(staged_isolated.reused_deferred_blockers)
            if (
                staged_isolated.merge_action_metadata.get("pending_merge_finalization") is not True
                and _merge_finalization_family_for_action(staged_isolated.merge_action_metadata) is not None
            ):
                staged_isolated = _require_staged_authorized_source_ref_unchanged_after_materialization(
                    store,
                    config=config,
                    git=git,
                    staged=staged_isolated,
                )
                source_ref = staged_isolated.source_ref or staged_isolated.merge_branch
                source_ref_sha = staged_isolated.source_ref_sha
                if staged_isolated.previous_target_sha is None:
                    raise GitError("could not prove isolated merge finalization target before promotion")
                if source_ref is None:
                    raise GitError("could not prove isolated merge finalization source ref before promotion")
                if source_ref_sha is None:
                    raise GitError("could not prove isolated merge finalization source before promotion")
                _persist_merge_finalization_prepared_attempt_for_action(
                    store,
                    merge_subject=staged_isolated.merge_subject,
                    action=staged_isolated.merge_action_metadata,
                    target_branch=target_branch,
                    previous_target_sha=staged_isolated.previous_target_sha,
                    source_ref=source_ref,
                    source_ref_sha=source_ref_sha,
                    merge_unit_id=staged_isolated.merge_unit_id,
                    promotion_kind=staged_isolated.promotion_kind,
                    created_followups=created_followups,
                    reused_followups=reused_followups,
                    created_deferred_blockers=created_deferred_blockers,
                    reused_deferred_blockers=reused_deferred_blockers,
                )
        except Exception as exc:
            if action.get("max_cycles_merge_and_defer") is True and not staged_isolated.followup_findings:
                return _deferred_blocker_materialization_failed_result(
                    exc=exc,
                    created_followups=created_followups,
                    reused_followups=reused_followups,
                    created_investigation_task_ids=created_investigation_task_ids,
                    reused_investigation_task_ids=reused_investigation_task_ids,
                    created_deferred_blockers=created_deferred_blockers,
                    reused_deferred_blockers=reused_deferred_blockers,
                )
            return _mandatory_merge_side_effects_failed_result(
                exc=exc,
                created_followups=created_followups,
                reused_followups=reused_followups,
                created_investigation_task_ids=created_investigation_task_ids,
                reused_investigation_task_ids=reused_investigation_task_ids,
                created_deferred_blockers=created_deferred_blockers,
                reused_deferred_blockers=reused_deferred_blockers,
            )
        try:
            promotion_warnings = _promote_isolated_merge_to_target_branch(
                git,
                execution_git,
                target_branch,
                expected_previous_target_sha=staged_isolated.previous_target_sha,
            )
        except _IsolatedPromotionRollbackFailed as exc:
            classification = _classify_isolated_promotion_rollback_failed(git, exc)
            print(f"Error: {classification.block_reason}")
            if classification.target_at_candidate:
                promotion_warnings = (classification.block_reason,)
            else:
                rc = 1
                result_status = classification.status
                result_block_reason = classification.block_reason
        except GitError as exc:
            print(f"Error: isolated merge failed: {exc}")
            rc = 1
            result_status = "isolated_merge_failed"
            result_block_reason = str(exc)
        if rc == 0:
            if _merge_finalization_family_for_action(staged_isolated.merge_action_metadata) is not None:
                promoted_target_sha = _ref_sha_for_merge_finalization_proof(git, target_branch) or _ref_sha_for_merge_finalization_proof(
                    execution_git,
                    target_branch,
                )
                if not promoted_target_sha:
                    rc = 1
                    result_status = "isolated_post_promotion_finalization_failed"
                    result_block_reason = (
                        "target was promoted but merge finalization proof failed: "
                        f"could not prove promoted target ref {target_branch!r}"
                    )
                else:
                    staged_isolated = replace(staged_isolated, promoted_target_sha=promoted_target_sha)
            if rc == 0 and candidate_verify is not None:
                try:
                    if not verified_head_sha or not verified_tree_fingerprint:
                        raise GitError(
                            "promoted target could not prove exact candidate identity for canonical checkpoint update"
                        )
                    persisted_checkpoint = promote_candidate_integration_verify_evidence(
                        store,
                        evidence=candidate_verify.evidence,
                        promoted_head_sha=verified_head_sha,
                        promoted_tree_fingerprint=verified_tree_fingerprint,
                    )
                    if persisted_checkpoint is None:
                        raise GitError(
                            "promoted target did not exactly match the verified candidate tree; "
                            "canonical checkpoint was not updated"
                        )
                except Exception as exc:
                    checkpoint_failure = _post_merge_state_persistence_failed_result(
                        exc=exc,
                        status="isolated_post_promotion_checkpoint_persistence_failed",
                        phase="isolated checkpoint persistence",
                        created_followups=created_followups,
                        reused_followups=reused_followups,
                        created_investigation_task_ids=created_investigation_task_ids,
                        reused_investigation_task_ids=reused_investigation_task_ids,
                        created_deferred_blockers=created_deferred_blockers,
                        reused_deferred_blockers=reused_deferred_blockers,
                    )
                    rc = checkpoint_failure.rc
                    result_status = checkpoint_failure.status
                    result_block_reason = checkpoint_failure.block_reason
            if rc == 0:
                finalized = _finalize_staged_isolated_merge_action(
                    config,
                    store,
                    git,
                    staged=staged_isolated,
                    merge_source=merge_source,
                    quiet_mechanics=quiet_mechanics,
                )
                created_followups = finalized.created_followups
                reused_followups = finalized.reused_followups
                created_deferred_blockers = finalized.created_deferred_blockers
                reused_deferred_blockers = finalized.reused_deferred_blockers
                if finalized.rc != 0:
                    rc = finalized.rc
                    result_status = finalized.status
                    result_block_reason = finalized.block_reason
    elif rc == 0:
        resolved_merge_unit_id = resolved_subject.merge_unit_id if resolved_subject is not None else None
        materialization_error = _materialize_mandatory_side_effects_before_state(final_authorized_action)
        if materialization_error is not None:
            return materialization_error
        if (
            final_authorized_action.get("pending_merge_finalization") is not True
            and _merge_finalization_family_for_action(final_authorized_action) is not None
        ):
            try:
                if previous_target_sha is None or not proof_source_ref or not proof_source_ref_sha:
                    raise GitError("could not prove merge finalization source or previous target")
                promoted_target_sha = _require_ref_sha_for_merge_finalization_proof(
                    execution_git,
                    target_branch,
                    label="promoted target",
                )
                _persist_merge_finalization_attempt_proof_for_action(
                    store,
                    execution_git,
                    merge_subject=merge_subject,
                    action=final_authorized_action,
                    target_branch=target_branch,
                    previous_target_sha=previous_target_sha,
                    promoted_target_sha=promoted_target_sha,
                    promotion_kind=promotion_kind,
                    promoted_target_tree_sha=(
                        _require_ref_tree_sha_for_merge_finalization_proof(
                            execution_git,
                            promoted_target_sha,
                            label="promoted target",
                        )
                        if promotion_kind == "squash"
                        else None
                    ),
                    source_ref=proof_source_ref,
                    source_ref_sha=proof_source_ref_sha,
                    merge_unit_id=resolved_merge_unit_id,
                    created_followups=created_followups,
                    reused_followups=reused_followups,
                    created_deferred_blockers=created_deferred_blockers,
                    reused_deferred_blockers=reused_deferred_blockers,
                )
            except Exception as exc:
                return _post_merge_proof_persistence_failed_result(
                    exc=exc,
                    status="post_merge_proof_persistence_failed",
                    phase="post-merge finalization proof persistence",
                    created_followups=created_followups,
                    reused_followups=reused_followups,
                    created_investigation_task_ids=created_investigation_task_ids,
                    reused_investigation_task_ids=reused_investigation_task_ids,
                    created_deferred_blockers=created_deferred_blockers,
                    reused_deferred_blockers=reused_deferred_blockers,
                )
        try:
            try:
                _validate_pending_merge_finalization_proof_for_state(
                    store,
                    execution_git,
                    merge_subject=merge_subject,
                    action=final_authorized_action,
                    target_branch=target_branch,
                    merge_unit_id=resolved_merge_unit_id,
                    created_followups=created_followups,
                    reused_followups=reused_followups,
                    created_deferred_blockers=created_deferred_blockers,
                    reused_deferred_blockers=reused_deferred_blockers,
                )
            except Exception as exc:
                return _pending_merge_finalization_refusal_result(
                    exc=exc,
                    created_followups=created_followups,
                    reused_followups=reused_followups,
                    created_investigation_task_ids=created_investigation_task_ids,
                    reused_investigation_task_ids=reused_investigation_task_ids,
                    created_deferred_blockers=created_deferred_blockers,
                    reused_deferred_blockers=reused_deferred_blockers,
                )
            mark_merge_subject_merged(
                store,
                merge_subject=merge_subject,
                merge_unit_id=resolved_merge_unit_id,
                merge_source=effective_merge_source,
            )
        except Exception as exc:
            return _post_merge_state_persistence_failed_result(
                exc=exc,
                status="post_merge_state_persistence_failed",
                phase="post-merge state persistence",
                created_followups=created_followups,
                reused_followups=reused_followups,
                created_investigation_task_ids=created_investigation_task_ids,
                reused_investigation_task_ids=reused_investigation_task_ids,
                created_deferred_blockers=created_deferred_blockers,
                reused_deferred_blockers=reused_deferred_blockers,
            )
    return _MergeActionResult(
        rc=rc,
        created_followups=created_followups,
        reused_followups=reused_followups,
        created_investigation_task_ids=created_investigation_task_ids,
        reused_investigation_task_ids=reused_investigation_task_ids,
        created_deferred_blockers=created_deferred_blockers,
        reused_deferred_blockers=reused_deferred_blockers,
        status=result_status,
        block_reason=result_block_reason,
        promotion_warnings=promotion_warnings,
        candidate_verify=candidate_verify,
    )


def _advance_action_color(action_type: str) -> str:
    """Return a Rich color for an advance action type."""
    ac = _colors.WORK_COLORS
    if action_type in {"merge", "merge_with_followups"}:
        return ac.merge
    if action_type in (
        "needs_rebase",
        "reconcile_branch_divergence",
        "awaiting_human",
        "needs_discussion",
        "max_cycles_reached",
        "max_improve_attempts",
        "automatic_recovery_disabled",
    ):
        return ac.error
    if action_type in ("skip", "wait_review", "wait_improve"):
        return ac.waiting
    return ac.default


def _run_advance_owner_row_read_session(
    store: SqliteTaskStore,
    query_fn: Callable[[], _T],
    *,
    apply_deferred_reconciliations: bool = True,
) -> _T:
    """Run one or more advance owner-row queries in one read snapshot.

    `cmd_advance()` sometimes needs multiple `query_lineage_owner_rows(...)`
    calls against the same read-session snapshot, so it cannot always use the
    one-shot `query_lineage_owner_rows_in_read_session(...)` wrapper. Those
    manual queries may queue deferred lineage reconciliations while the read
    session is open; apply them only after the read session closes.
    """

    with store.read_session():
        result = query_fn()
    if apply_deferred_reconciliations:
        apply_deferred_lineage_query_reconciliations(store)
    return result


def _build_advance_recovery_read_context(
    store: SqliteTaskStore,
    *,
    allow_reconcile_mutation: bool,
) -> RecoveryReadContext:
    tasks = tuple(store.get_all())
    task_by_id = {task.id: task for task in tasks if task.id is not None}
    based_on_children: dict[str, list[DbTask]] = {}
    depends_on_children: dict[str, list[DbTask]] = {}
    for task in tasks:
        if task.id is None:
            continue
        if task.based_on is not None:
            based_on_children.setdefault(task.based_on, []).append(task)
        if task.depends_on is not None:
            depends_on_children.setdefault(task.depends_on, []).append(task)

    merge_units_by_task_id = {}
    historical_merge_units_by_task_id = {}
    if store.supports_merge_units():
        task_ids = [task.id for task in tasks if task.id is not None]
        for task_id, units in store.list_merge_units_for_tasks(task_ids).items():
            if not units:
                continue
            historical_merge_units_by_task_id[task_id] = units
            active_unit = next((unit for unit in units if merge_unit_is_active(unit)), None)
            if active_unit is not None:
                merge_units_by_task_id[task_id] = active_unit

    return RecoveryReadContext(
        tasks=tasks,
        task_by_id=task_by_id,
        based_on_children=based_on_children,
        depends_on_children=depends_on_children,
        merge_units_by_task_id=merge_units_by_task_id,
        historical_merge_units_by_task_id=historical_merge_units_by_task_id,
        allow_reconcile_mutation=allow_reconcile_mutation,
    )


def cmd_advance(args: argparse.Namespace) -> int:
    """Intelligently progress unmerged tasks through their lifecycle."""
    try:
        tag_filters, any_tag = parse_cli_tag_filters(args)
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1
    config = Config.load(args.project_dir)
    runtime_context = RuntimeExecutionContext.from_config(config)
    store = get_store(config)

    # Themed work/advance colors — resolved once after Config.load() applies the theme.
    _ac = _colors.WORK_COLORS
    _c_tid = _colors.TASK_COLORS.task_id
    _c_ok = _ac.merge
    _c_err = _ac.error
    _c_warn = _ac.waiting
    _c_default = _ac.default

    # Prefix for advance lines: "  #NNN " — compute available prompt width per task.
    def _prompt_avail(task_id: str | None) -> int:
        return prompt_available_width(prefix=len(task_id or "") + 4)  # "  #NNN "
    git = _git_from_runtime_context(config.project_dir, runtime_context)

    dry_run: bool = args.dry_run
    auto: bool = getattr(args, "auto", False)
    max_tasks: int | None = getattr(args, "max", None)
    batch_limit: int | None = getattr(args, "batch", None)
    force: bool = getattr(args, "force", False)
    task_id: str | None = resolve_id(config, args.task_id) if getattr(args, "task_id", None) is not None else None
    plans_mode: bool = getattr(args, "plans", False)
    unimplemented_mode: bool = getattr(args, "unimplemented", False)
    create_mode: bool = getattr(args, "create", False)
    no_resume_failed: bool = getattr(args, "no_resume_failed", False)
    max_resume_attempts_override: int | None = getattr(args, "max_resume_attempts", None)
    advance_type: str | None = getattr(args, "advance_type", None)

    # Determine effective max_resume_attempts
    max_resume_attempts = (
        max_resume_attempts_override if max_resume_attempts_override is not None else config.max_resume_attempts
    )

    new_mode: bool = getattr(args, "new", False)

    max_review_cycles_override: int | None = getattr(args, "max_review_cycles", None)

    if max_review_cycles_override is not None:
        config.max_review_cycles = max_review_cycles_override

    squash_threshold_override: int | None = getattr(args, "squash_threshold", None)
    if squash_threshold_override is not None:
        config.merge_squash_threshold = squash_threshold_override

    repeat_mode: bool = getattr(args, "repeat", False)
    repeat_max_iterations_arg: int | None = getattr(args, "max_iterations", None)

    if repeat_mode and task_id is None:
        return phase1_error(args, "--repeat requires an explicit task_id")
    if repeat_mode and max_tasks is not None:
        return phase1_error(args, "--repeat cannot be combined with --max")
    if repeat_mode and new_mode:
        return phase1_error(args, "--repeat cannot be combined with --new")
    if repeat_mode and (unimplemented_mode or plans_mode):
        return phase1_error(args, "--repeat cannot be combined with --unimplemented")
    repeat_max_iterations = (
        repeat_max_iterations_arg if repeat_max_iterations_arg is not None else config.iterate_max_iterations
    )
    if repeat_mode and repeat_max_iterations < 1:
        return phase1_error(args, "--max-iterations must be a positive integer")

    if new_mode and batch_limit is None:
        return phase1_error(args, "--new requires --batch")

    if batch_limit is not None and batch_limit < 1:
        return phase1_error(args, "--batch must be a positive integer")
    concurrency_snapshot = get_concurrency_snapshot(config, store)
    concurrency_budget = concurrency_snapshot.available
    effective_start_budget = concurrency_budget if batch_limit is None else min(batch_limit, concurrency_budget)
    capacity_message = format_max_concurrent_message(
        running=concurrency_snapshot.running,
        limit=concurrency_snapshot.limit,
    )

    # --unimplemented mode: list completed plans/explores without implementations
    # Legacy --plans is supported as an alias scoped to plans only.
    if unimplemented_mode or plans_mode:
        unimplemented_types: tuple[str, ...] = (
            ("plan",) if plans_mode and not unimplemented_mode else ("plan", "explore")
        )
        if plans_mode:
            print("Warning: --plans is deprecated. Use --unimplemented instead.", file=sys.stderr)
        return _cmd_advance_unimplemented(
            config,
            store,
            dry_run=dry_run,
            create=create_mode,
            task_types=unimplemented_types,
        )

    owner_rows: list[LineageOwnerRow] = []
    failed_task_recovery_warnings: list[str] = []
    target_branch: str | None = None

    # Cache planning-only git reads. Execution runs outside this scope so
    # mutating actions always operate on fresh subprocess state.
    planning_cache = nullcontext()
    cached = getattr(git, "cached", None)
    if callable(cached):
        candidate = cached()
        if hasattr(candidate, "__enter__") and hasattr(candidate, "__exit__"):
            planning_cache = candidate
    if repeat_mode:
        planning_cache = nullcontext()

    def _print_needs_attention_section(items: list[tuple[DbTask, dict]]) -> None:
        if not items:
            return
        console.print(
            f"\n[{_c_err}]{NEEDS_ATTENTION_LABEL} ({len(items)} task{'s' if len(items) != 1 else ''}):[/{_c_err}]"
        )
        for atask, aaction in items:
            _color = _advance_action_color(aaction["type"])
            console.print(f"[{_color}]{_format_needs_attention_line(atask, aaction)}[/{_color}]")
            next_step = needs_attention_recommended_next_step(store, atask, aaction)
            if next_step is not None:
                console.print(f"[{_color}]{next_step}[/{_color}]")

    def _append_attention_once(
        items: list[tuple[DbTask, dict[str, Any]]],
        task: DbTask,
        action: dict[str, Any],
    ) -> None:
        task_id = task.id
        reason = action.get("needs_attention_reason")
        for existing_task, existing_action in items:
            if existing_task.id == task_id and existing_action.get("needs_attention_reason") == reason:
                return
        items.append((task, action))

    def _main_verify_attention_item() -> tuple[DbTask, dict[str, Any]] | None:
        if target_branch != actual_current_branch:
            return None
        if not any(item_action["type"] in {"merge", "merge_with_followups"} for _, _, item_action in plan):
            return None
        if dry_run:
            rev_parse_if_exists = getattr(git, "rev_parse_if_exists", None)
            resolved_head_sha = rev_parse_if_exists("HEAD") if callable(rev_parse_if_exists) else None
            main_verify = inspect_main_integration_verify_checkpoint(
                config,
                store,
                git,
                resolved_head_sha=resolved_head_sha,
            )
        else:
            main_verify = _check_main_integration_verify_with_git_env(
                config,
                store,
                git,
                reason="advance-pre-merge",
            )
        if (
            not main_verify.merges_halted
            or main_verify.state is None
            or main_verify.state.task.id is None
        ):
            return None
        return (
            main_verify.state.task,
            {
                "type": "needs_discussion",
                "description": f"SKIP: {main_verify.state.alert_message or 'main verify is red; merges halted'}",
                "needs_attention_reason": MAIN_INTEGRATION_VERIFY_REASON,
                "subject_task_id": main_verify.state.task.id,
            },
        )

    plan: list[tuple[LineageOwnerRow, DbTask, dict[str, Any]]] = []
    preview_actionable_rows: list[tuple[LineageOwnerRow, DbTask, dict[str, Any], str]] = []
    preview_gated_rows: list[tuple[LineageOwnerRow, DbTask, dict[str, Any], str]] = []
    new_pending_tasks: list = []
    recovery_preview_task_ids: set[str] = set()
    persist_planning_state = not dry_run
    recovery_planning_read_contexts_by_task_id: dict[str, RecoveryReadContext] = {}

    def _remember_recovery_planning_read_context(
        read_context: RecoveryReadContext,
        *tasks: DbTask | None,
    ) -> None:
        for candidate in tasks:
            if candidate is not None and candidate.id is not None:
                recovery_planning_read_contexts_by_task_id[candidate.id] = read_context

    def _remember_recovery_planning_read_context_for_rows(
        read_context: RecoveryReadContext,
        rows: Sequence[LineageOwnerRow],
    ) -> None:
        for row in rows:
            _remember_recovery_planning_read_context(
                read_context,
                row.recovery_leaf_task,
                row.recovery_action_task,
                row.lifecycle_action_task,
                row.owner_task,
                *row.unresolved_tasks,
                *row.members,
            )

    def _build_recovery_planning_read_context_for_rows(
        rows: Sequence[LineageOwnerRow],
        *,
        allow_reconcile_mutation: bool,
    ) -> RecoveryReadContext | None:
        if not any(
            candidate is not None and candidate.status == "failed"
            for row in rows
            for candidate in (
                row.recovery_leaf_task,
                row.recovery_action_task,
                row.owner_task,
                *row.unresolved_tasks,
            )
        ):
            return None
        read_context = _build_advance_recovery_read_context(
            store,
            allow_reconcile_mutation=allow_reconcile_mutation,
        )
        if git is not None:
            read_context.merge_context = _build_advance_recovery_merge_context(
                git,
                target_branch,
            )
        _remember_recovery_planning_read_context_for_rows(read_context, rows)
        return read_context

    def _recovery_planning_read_context_for_row(
        row: LineageOwnerRow,
        action_task: DbTask,
    ) -> RecoveryReadContext | None:
        for candidate in (row.recovery_leaf_task, row.recovery_action_task, action_task):
            if candidate is not None and candidate.id is not None:
                read_context = recovery_planning_read_contexts_by_task_id.get(candidate.id)
                if read_context is not None:
                    return read_context
        return None

    def _advance_scope_mismatch_message(explicit_task_id: str) -> str:
        scoped_filters: list[str] = []
        if advance_type is not None:
            scoped_filters.append(f"--type {advance_type}")
        if tag_filters:
            scope_mode = "--any-tag" if any_tag else "--all-tags"
            scoped_filters.append(" ".join([*(f"--tag {tag}" for tag in tag_filters), scope_mode]))
        scope_text = ", ".join(scoped_filters) if scoped_filters else "the requested scope"
        return f"Task {explicit_task_id} does not match the requested advance scope ({scope_text})"

    with planning_cache:
        # Determine which tasks to advance
        if task_id is not None:
            task = store.get(task_id)
            if not task:
                return phase1_error(args, f"Task {task_id} not found")
            explicit_task = task
            if task.status == "failed":
                if no_resume_failed:
                    return phase1_error(args, f"Task {task_id} is not completed (status: {task.status})")
            else:
                if task.status != "completed":
                    return phase1_error(args, f"Task {task_id} is not completed (status: {task.status})")
            try:
                target_branch = _resolve_advance_target_branch(store, git, task=task)
            except MergeTargetResolutionError as exc:
                print(f"Error: {exc}", file=sys.stderr)
                return 1
            prime_advance_planning_refs(
                git,
                branch_names=[task.branch] if task.branch else [],
                target_branch=target_branch,
                warning_logger=logger,
            )
            if target_branch is not None:
                resolved_merge_state = resolve_task_merge_state_for_target(
                    store=store,
                    task=task,
                    git=git,
                    target_branch=target_branch,
                )
                pending_finalization_action = _pending_merge_finalization_action_for_advance(
                    config,
                    store,
                    task,
                    target_branch=target_branch,
                    resolved_merge_state=resolved_merge_state,
                    live_target_sha=_ref_sha_for_merge_finalization_proof(git, target_branch),
                )
                if (
                    resolved_merge_state == "merged"
                    and pending_finalization_action is None
                ):
                    print(f"Task {task_id} is already merged")
                    return 0

            def _load_explicit_owner_rows() -> tuple[list[LineageOwnerRow], bool]:
                owner_rows = list(
                    query_lineage_owner_rows(
                        store,
                        LineageOwnerQuery(
                            limit=None,
                            task_types=(advance_type,) if advance_type else None,
                            tags=tag_filters,
                            any_tag=any_tag,
                            include_skipped=True,
                            exclude_dropped_from_planning=True,
                            max_recovery_attempts=max_resume_attempts,
                            task_ids=(explicit_task.id,) if explicit_task.id is not None else None,
                        ),
                        config=config,
                        git=git,
                        target_branch=target_branch,
                        persist_post_merge_rebase_state=persist_planning_state,
                        persist_review_clearance=persist_planning_state,
                        reuse_recovery_merge_context=True,
                    )
                )
                dropped_owner_lineage = False
                if not owner_rows and explicit_task.status != "dropped":
                    dropped_owner_rows = [
                        row
                        for row in query_lineage_owner_rows(
                            store,
                            LineageOwnerQuery(
                                limit=None,
                                task_types=(advance_type,) if advance_type else None,
                                tags=tag_filters,
                                any_tag=any_tag,
                                include_skipped=True,
                                max_recovery_attempts=max_resume_attempts,
                                task_ids=(explicit_task.id,) if explicit_task.id is not None else None,
                            ),
                            config=config,
                            git=git,
                            target_branch=target_branch,
                            persist_post_merge_rebase_state=persist_planning_state,
                            persist_review_clearance=persist_planning_state,
                            reuse_recovery_merge_context=True,
                        )
                        if row.owner_task.status == "dropped"
                    ]
                    if dropped_owner_rows:
                        dropped_owner_lineage = True
                return owner_rows, dropped_owner_lineage

            owner_rows, dropped_owner_lineage = _run_advance_owner_row_read_session(
                store,
                _load_explicit_owner_rows,
                apply_deferred_reconciliations=persist_planning_state,
            )
            _build_recovery_planning_read_context_for_rows(
                owner_rows,
                allow_reconcile_mutation=persist_planning_state,
            )
            if not owner_rows and task.status != "dropped" and not dropped_owner_lineage:
                if advance_type is not None or tag_filters is not None:
                    return phase1_error(args, _advance_scope_mismatch_message(task_id))
                recovery_fallback_read_context = _build_advance_recovery_read_context(
                    store,
                    allow_reconcile_mutation=persist_planning_state,
                )
                recovery_fallback_merge_context = None
                if git is not None:
                    recovery_fallback_merge_context = _build_advance_recovery_merge_context(
                        git,
                        target_branch,
                    )
                    recovery_fallback_read_context.merge_context = recovery_fallback_merge_context
                planning_task = (
                    resolve_recovery_planning_task(
                        store,
                        task,
                        merge_context=recovery_fallback_merge_context,
                        read_context=recovery_fallback_read_context,
                    )
                    if task.status == "failed"
                    else task
                )
                _remember_recovery_planning_read_context(
                    recovery_fallback_read_context,
                    task,
                    planning_task,
                )
                owner_rows = [
                    LineageOwnerRow(
                        owner_task=task,
                        members=(planning_task,),
                        tree=None,
                        lineage_status="skipped",
                        next_action={"type": "unknown", "description": "pending command evaluation"},
                        next_action_reason="pending command evaluation",
                        unresolved_tasks=(planning_task,),
                        unresolved_leaf_summary=(),
                        lifecycle_action_task=planning_task if planning_task.status != "failed" else None,
                        recovery_action_task=planning_task if planning_task.status == "failed" else None,
                        recovery_leaf_task=task if task.status == "failed" else None,
                    )
                ]
        else:
            target_branch = _resolve_advance_target_branch(store, git, task=None)

            def _load_all_owner_rows() -> list[LineageOwnerRow]:
                branch_names = [
                    task.branch
                    for task in store.get_all()
                    if task.branch and task.status in {"completed", "failed", "unmerged", "dropped"}
                ]
                prime_advance_planning_refs(
                    git,
                    branch_names=branch_names,
                    target_branch=target_branch,
                    warning_logger=logger,
                )
                owner_rows = list(
                    query_lineage_owner_rows(
                        store,
                        LineageOwnerQuery(
                            limit=None,
                            task_types=(advance_type,) if advance_type else None,
                            tags=tag_filters,
                            any_tag=any_tag,
                            include_skipped=True,
                            exclude_dropped_from_planning=True,
                            max_recovery_attempts=max_resume_attempts,
                        ),
                        config=config,
                        git=git,
                        target_branch=target_branch,
                        persist_post_merge_rebase_state=persist_planning_state,
                        persist_review_clearance=persist_planning_state,
                        reuse_recovery_merge_context=True,
                    )
                )
                return owner_rows

            owner_rows = _run_advance_owner_row_read_session(
                store,
                _load_all_owner_rows,
                apply_deferred_reconciliations=persist_planning_state,
            )
            if not no_resume_failed:
                failed_inventory_read_context = _build_advance_recovery_read_context(
                    store,
                    allow_reconcile_mutation=persist_planning_state,
                )
                failed_inventory_read_context.merge_context = _build_advance_recovery_merge_context(
                    git,
                    target_branch,
                )
                failed_inventory_tasks = list_failed_tasks_for_recovery(
                    store,
                    warnings=failed_task_recovery_warnings,
                    read_context=failed_inventory_read_context,
                    git=git,
                    target_branch=target_branch,
                )
                _remember_recovery_planning_read_context(
                    failed_inventory_read_context,
                    *failed_inventory_tasks,
                )
                _remember_recovery_planning_read_context_for_rows(
                    failed_inventory_read_context,
                    owner_rows,
                )
            if no_resume_failed:
                owner_rows = [
                    row
                    for row in owner_rows
                    if row.lifecycle_action_task is not None or row.recovery_action_task is None
                ]

        if not owner_rows and not new_mode:
            print("No eligible tasks to advance")
            return 0

        # Apply --max limit
        if max_tasks is not None:
            owner_rows = owner_rows[:max_tasks]

        if owner_rows:
            recovery_read_context = next(
                (
                    context
                    for row in owner_rows
                    for candidate in (row.recovery_leaf_task, row.recovery_action_task)
                    if candidate is not None
                    and candidate.id is not None
                    and (context := recovery_planning_read_contexts_by_task_id.get(candidate.id)) is not None
                ),
                None,
            ) or RecoveryReadContext(allow_reconcile_mutation=persist_planning_state)
            if git is not None:
                recovery_read_context.merge_context = _build_advance_recovery_merge_context(
                    git,
                    target_branch,
                )
            recovery_preview = build_dispatch_preview(
                store,
                config=config,
                git=git,
                target_branch=target_branch,
                owner_rows=tuple(owner_rows),
                read_context=recovery_read_context,
                tags=tag_filters,
                any_tag=any_tag,
                max_recovery_attempts=max_resume_attempts,
                selection_mode="recovery_only",
                include_pending=False,
            )
            recovery_preview_task_ids = {
                entry.task.id for entry in recovery_preview.recovery_entries if entry.task.id is not None
            }

        # Use the currently checked-out branch as the target for conflict checks,
        # merge execution, and rebase task creation.
        actual_current_branch = git.current_branch()
        if target_branch is None:
            target_branch = actual_current_branch
        use_iterate_mode = _advance_uses_iterate(config)

        def _worker_args() -> argparse.Namespace:
            return argparse.Namespace(
                no_docker=getattr(args, "no_docker", False),
                max_turns=None,
                force=force,
            )

        def _build_action_context(*, dry_run_mode: bool) -> AdvanceActionExecutionContext:
            def _create_rebase_from_task(parent_task: DbTask) -> DbTask:
                assert parent_task.id is not None
                assert parent_task.branch is not None
                config.require_model_for_task("rebase")
                return _create_rebase_task(
                    store,
                    parent_task.id,
                    parent_task.branch,
                    target_branch,
                    trigger_source="manual",
                )

            def _create_targeted_rebase_from_task(parent_task: DbTask, rebase_target: str) -> DbTask:
                assert parent_task.id is not None
                assert parent_task.branch is not None
                config.require_model_for_task("rebase")
                return _create_rebase_task(
                    store,
                    parent_task.id,
                    parent_task.branch,
                    rebase_target,
                    trigger_source="manual",
                )

            def _create_implement_from_task(parent_task: DbTask) -> DbTask:
                return _create_implementation_task_from_source(
                    store,
                    parent_task,
                    config=config,
                    prompt=_unimplemented_implement_prompt(parent_task),
                    trigger_source="manual",
                )

            def _create_plan_review_from_task(parent_task: DbTask) -> DbTask:
                return _create_plan_review_task(store, parent_task, config=config, trigger_source="manual")

            def _create_plan_improve_from_task(parent_task: DbTask, review_task: DbTask) -> DbTask:
                return _create_plan_improve_task(
                    store, parent_task, review_task, config=config, trigger_source="manual"
                )

            def _create_review_adjudication_from_task(
                impl_task: DbTask,
                review_task: DbTask,
                finding: Any,
                dispute_metadata: dict[str, Any],
            ) -> DbTask:
                return _create_review_adjudication_task(
                    store,
                    impl_task,
                    review_task,
                    finding,
                    config=config,
                    dispute_metadata=dispute_metadata,
                    trigger_source="manual",
                )

            return AdvanceActionExecutionContext(
                store=store,
                trigger_source="manual",
                dry_run=dry_run_mode,
                max_resume_attempts=max_resume_attempts,
                use_iterate_for_create_implement=use_iterate_mode,
                use_iterate_for_needs_rebase=use_iterate_mode,
                can_spawn_worker=lambda _kind: workers_started < effective_start_budget,
                no_worker_capacity_message=lambda worker_label: (
                    f"SKIP: batch limit reached ({workers_started}/{batch_limit}), cannot start {worker_label} worker"
                    if batch_limit is not None and workers_started >= batch_limit
                    else f"SKIP: {capacity_message}"
                ),
                prepare_task_for_background_start=lambda task, rollback_on_failure: _prepare_task_for_immediate_execution(
                    config,
                    task,
                    rollback_on_failure=rollback_on_failure,
                    store=store,
                    runtime_context=runtime_context,
                ),
                prepare_create_review=lambda t: _prepare_create_review_action(
                    store,
                    t,
                    config=config,
                    trigger_source="manual",
                ),
                create_resume_task=lambda t: _create_resume_task(store, t, config=config, trigger_source="manual"),
                create_retry_task=lambda t: _create_retry_task(store, t, config=config, trigger_source="manual"),
                create_rebase_task=_create_rebase_from_task,
                create_implement_task=_create_implement_from_task,
                create_plan_review_task=_create_plan_review_from_task,
                create_plan_improve_task=_create_plan_improve_from_task,
                create_review_adjudication_task=_create_review_adjudication_from_task,
                materialize_plan_slices=lambda plan_task, review_task, manifest: _materialize_plan_review_slices(
                    config,
                    store,
                    plan_task,
                    review_task,
                    manifest,
                    trigger_source="plan-review",
                    require_review_before_merge=config.require_review_before_merge,
                ),
                repair_plan_slice_materialization=lambda plan_task, review_task, manifest, partial_task_ids, repair_trigger_source: (
                    _repair_plan_review_slice_materialization(
                        config,
                        store,
                        plan_task,
                        review_task,
                        manifest,
                        partial_task_ids=partial_task_ids,
                        trigger_source=repair_trigger_source,
                        require_review_before_merge=config.require_review_before_merge,
                    )
                ),
                create_targeted_rebase_task=_create_targeted_rebase_from_task,
                spawn_worker=lambda task_obj, _kind: _spawn_background_worker(
                    _worker_args(),
                    config,
                    task_id=str(task_obj.id),
                    quiet=True,
                    prepared_task=task_obj,
                    runtime_context=runtime_context,
                ),
                spawn_resume_worker=lambda task_obj, _kind: _spawn_background_resume_worker(
                    _worker_args(),
                    config,
                    str(task_obj.id),
                    quiet=True,
                    prepared_task=task_obj,
                    runtime_context=runtime_context,
                ),
                is_rebase_target_already_merged=(
                    lambda t: (
                        resolve_post_merge_rebase_state(
                            store,
                            git,
                            t,
                            target_branch,
                            merge_source=_resolve_current_merge_source(git, t.branch) if t.branch else None,
                        ).already_merged
                        if dry_run_mode
                        else _resolve_and_persist_post_merge_rebase_state(
                            store,
                            git,
                            t,
                            target_branch,
                            config=config,
                            merge_source=_resolve_current_merge_source(git, t.branch) if t.branch else None,
                        ).already_merged
                    )
                ),
                config=config,
                git=git,
                spawn_iterate_worker=lambda task_obj, _kind, *, prepared_task=None, prepared_phase=None, prepared_action_type=None: _spawn_background_iterate_worker(
                    argparse.Namespace(
                        no_docker=getattr(args, 'no_docker', False),
                        force=force,
                    ),
                    config,
                    task_obj,
                    max_iterations=config.iterate_max_iterations,
                    auto_iterate=True,
                    quiet=True,
                    prepared_task_id=str(prepared_task.id) if prepared_task is not None and prepared_task.id is not None else None,
                    prepared_phase=prepared_phase,
                    prepared_action_type=prepared_action_type,
                    runtime_context=runtime_context,
                ),
                spawn_iterate_recovery=lambda task_obj, mode, prepared_task: _spawn_background_iterate_worker(
                    argparse.Namespace(
                        no_docker=getattr(args, "no_docker", False),
                        force=force,
                    ),
                    config,
                    prepared_task,
                    max_iterations=config.iterate_max_iterations,
                    resume=False,
                    retry=False,
                    auto_iterate=True,
                    quiet=True,
                    prepared_task_id=str(prepared_task.id),
                    prepared_resume=mode == "resume",
                    prepared_phase="preloop",
                    runtime_context=runtime_context,
                ),
                reconcile_diverged_branch=lambda t: _reconcile_diverged_branch_with_origin(
                    config,
                    git,
                    t,
                    target_branch=target_branch,
                ),
                heartbeat_for_lifecycle_phase=(
                    None
                    if dry_run_mode
                    else (
                        lambda phase, _task: _CliVerifyProgressHeartbeat(
                            _advance_progress_console,
                            "Verify gate" if phase == "verify" else f"Verify gate ({phase})",
                        )
                    )
                ),
                runtime_context=runtime_context,
            )

        def _build_repeat_action_context(*, dry_run_mode: bool) -> AdvanceActionExecutionContext:
            context = _build_action_context(dry_run_mode=dry_run_mode)
            if dry_run_mode:
                return context

            def _run_task_foreground(task_obj: DbTask, _kind: str) -> int:
                assert task_obj.id is not None
                return _run_foreground(
                    config,
                    str(task_obj.id),
                    force=force,
                    phase1_args=args,
                    prepared_task=task_obj,
                    runtime_context=context.runtime_context,
                )

            def _resume_task_foreground(task_obj: DbTask, _kind: str) -> int:
                assert task_obj.id is not None
                return _run_foreground(
                    config,
                    str(task_obj.id),
                    resume=True,
                    force=force,
                    phase1_args=args,
                    prepared_task=task_obj,
                    runtime_context=context.runtime_context,
                )

            return replace(
                context,
                use_iterate_for_create_implement=False,
                use_iterate_for_needs_rebase=False,
                can_spawn_worker=lambda _kind: effective_start_budget > 0,
                no_worker_capacity_message=lambda worker_label: (
                    f"SKIP: {capacity_message}" if effective_start_budget <= 0 else f"SKIP: {capacity_message}"
                ),
                spawn_worker=_run_task_foreground,
                spawn_resume_worker=_resume_task_foreground,
                spawn_iterate_worker=lambda *_args, **_kwargs: 1,
                spawn_iterate_recovery=lambda *_args, **_kwargs: 1,
                same_process_launch_pid=os.getpid(),
            )

        def _repeat_lineage_ids(subject_task_id: str) -> set[str]:
            all_tasks = [candidate for candidate in store.get_all() if candidate.id is not None]
            lineage_ids: set[str] = {subject_task_id}
            subject_unit = store.resolve_merge_unit_for_task(subject_task_id)
            subject_unit_id = subject_unit.id if subject_unit is not None else None
            if subject_unit_id is not None:
                for member in store.list_tasks_for_merge_unit(subject_unit_id):
                    if member.id is not None:
                        lineage_ids.add(str(member.id))

            def _belongs_to_other_merge_unit(candidate_id: str) -> bool:
                if subject_unit_id is None:
                    return False
                candidate_unit = store.resolve_merge_unit_for_task(candidate_id)
                return candidate_unit is not None and candidate_unit.id != subject_unit_id

            changed = True
            while changed:
                changed = False
                for candidate in all_tasks:
                    assert candidate.id is not None
                    candidate_id = str(candidate.id)
                    if candidate_id in lineage_ids or _belongs_to_other_merge_unit(candidate_id):
                        continue
                    linked_ids = {candidate.based_on, candidate.depends_on}
                    if any(linked_id in lineage_ids for linked_id in linked_ids if linked_id):
                        lineage_ids.add(candidate_id)
                        changed = True
            return lineage_ids

        def _repeat_state_signature(subject_task_id: str) -> tuple[Any, ...]:
            rows: list[tuple[Any, ...]] = []
            for task_id_for_sig in sorted(_repeat_lineage_ids(subject_task_id), key=task_id_numeric_key):
                signature_task = store.get(task_id_for_sig)
                if signature_task is None:
                    continue
                artifact_rows = tuple(
                    (
                        artifact.id,
                        artifact.kind,
                        artifact.label,
                        artifact.status,
                        artifact.exit_status,
                        artifact.head_sha,
                        artifact.sha256,
                        json.dumps(artifact.metadata or {}, sort_keys=True, default=str),
                    )
                    for artifact in store.list_artifacts(str(signature_task.id))
                )
                rows.append(
                    (
                        str(signature_task.id),
                        signature_task.status,
                        signature_task.task_type,
                        signature_task.branch,
                        signature_task.based_on,
                        signature_task.depends_on,
                        signature_task.merge_status,
                        signature_task.merged_at,
                        signature_task.completed_at,
                        signature_task.updated_at,
                        signature_task.report_file,
                        signature_task.output_content,
                        signature_task.review_scope,
                        signature_task.review_cleared_at,
                        signature_task.review_score,
                        signature_task.review_verify_status,
                        signature_task.review_verify_head_sha,
                        signature_task.review_verify_base_sha,
                        signature_task.review_verify_artifact_file,
                        signature_task.verify_fix_completion_outcome_json,
                        artifact_rows,
                    )
                )
            merge_unit_values_by_id = {}
            for task_id_for_sig in _repeat_lineage_ids(subject_task_id):
                unit = store.resolve_merge_unit_for_task(task_id_for_sig)
                if unit is not None and unit.id is not None:
                    merge_unit_values_by_id[unit.id] = unit
            merge_units = tuple(
                (
                    unit.id,
                    unit.owner_task_id,
                    unit.source_branch,
                    unit.target_branch,
                    unit.state,
                    unit.merged_at,
                    unit.updated_at,
                )
                for unit in sorted(merge_unit_values_by_id.values(), key=lambda unit: unit.id or "")
            )
            return (tuple(rows), merge_units)

        def _repeat_main_verify_attention(
            action: dict[str, Any],
            *,
            dry_run_mode: bool,
        ) -> tuple[DbTask, dict[str, Any]] | None:
            if str(action.get("type")) not in {"merge", "merge_with_followups"}:
                return None
            if target_branch != actual_current_branch:
                return None
            if dry_run_mode:
                main_verify_inspection = inspect_main_integration_verify_checkpoint(config, store, git)
                merges_halted = main_verify_inspection.merges_halted
                state = main_verify_inspection.state
            else:
                main_verify_check = _check_main_integration_verify_with_git_env(
                    config,
                    store,
                    git,
                    reason="advance-repeat-pre-merge",
                )
                merges_halted = main_verify_check.merges_halted
                state = main_verify_check.state
            if not merges_halted or state is None or state.task.id is None:
                return None
            return (
                state.task,
                {
                    "type": "needs_discussion",
                    "description": f"SKIP: {state.alert_message or 'main verify is red; merges halted'}",
                    "needs_attention_reason": MAIN_INTEGRATION_VERIFY_REASON,
                    "subject_task_id": state.task.id,
                },
            )

        def _repeat_execution_attention(action_task: DbTask, exec_result: Any):
            if not hasattr(exec_result, "attention_type") or not hasattr(exec_result, "attention_reason"):
                return None
            return resolve_execution_needs_attention(action_task, exec_result)

        def _resolve_repeat_cycle() -> tuple[LineageOwnerRow | None, DbTask | None, dict[str, Any]]:
            assert task_id is not None
            task = store.get(task_id)
            if task is None:
                return None, None, {"type": "needs_discussion", "description": f"SKIP: Task {task_id} disappeared"}
            resolved_merge_state = (
                resolve_task_merge_state_for_target(
                    store=store,
                    task=task,
                    git=git,
                    target_branch=target_branch,
                )
                if target_branch is not None
                else None
            )
            pending_finalization_action = (
                _pending_merge_finalization_action_for_advance(
                    config,
                    store,
                    task,
                    target_branch=target_branch,
                    resolved_merge_state=resolved_merge_state,
                    live_target_sha=_ref_sha_for_merge_finalization_proof(git, target_branch),
                )
                if target_branch is not None
                else None
            )
            if target_branch is not None:
                if (
                    resolved_merge_state == "merged"
                    and pending_finalization_action is None
                ):
                    return None, task, {"type": "merged", "description": "Merged"}

            def _load_rows() -> list[LineageOwnerRow]:
                return list(
                    query_lineage_owner_rows(
                        store,
                        LineageOwnerQuery(
                            limit=None,
                            task_types=(advance_type,) if advance_type else None,
                            tags=tag_filters,
                            any_tag=any_tag,
                            include_skipped=True,
                            exclude_dropped_from_planning=True,
                            max_recovery_attempts=max_resume_attempts,
                            task_ids=(task.id,) if task.id is not None else None,
                        ),
                        config=config,
                        git=git,
                        target_branch=target_branch,
                        persist_post_merge_rebase_state=not dry_run,
                        persist_review_clearance=not dry_run,
                        reuse_recovery_merge_context=True,
                    )
                )

            rows = _run_advance_owner_row_read_session(
                store,
                _load_rows,
                apply_deferred_reconciliations=not dry_run,
            )
            _build_recovery_planning_read_context_for_rows(
                rows,
                allow_reconcile_mutation=not dry_run,
            )
            if not rows and target_branch is not None:
                if (
                    resolved_merge_state == "merged"
                    and pending_finalization_action is None
                ):
                    return None, task, {"type": "merged", "description": "Merged"}
            if not rows:
                if target_branch is not None and pending_finalization_action is not None:
                    rows = [
                        LineageOwnerRow(
                            owner_task=task,
                            members=(task,),
                            tree=None,
                            lineage_status="skipped",
                            next_action=pending_finalization_action,
                            next_action_reason="pending merge finalization",
                            unresolved_tasks=(task,),
                            unresolved_leaf_summary=(),
                            lifecycle_action_task=None,
                            recovery_action_task=task if task.status == "failed" else None,
                            recovery_leaf_task=task if task.status == "failed" else None,
                        )
                    ]
                else:
                    return None, task, {"type": "skip", "description": "SKIP: no eligible owner row remains"}
            row = rows[0]
            action_task = row.lifecycle_action_task or row.recovery_action_task or row.owner_task
            action = (
                row.next_action
                if (
                    row.next_action is not None
                    and str(row.next_action.get("type", "")) != "unknown"
                    and row.lifecycle_action_task is None
                    and row.recovery_action_task is None
                )
                else determine_next_action(
                    config,
                    store,
                    git,
                    action_task,
                    target_branch,
                    max_resume_attempts=max_resume_attempts,
                    persist_post_merge_rebase_state=not dry_run,
                    persist_review_clearance=not dry_run,
                    read_context=_recovery_planning_read_context_for_row(row, action_task),
                )
            )
            decision = plan_lifecycle_execution(
                [(row, action_task, action)],
                free_worker_slots=repeat_worker_budget,
                get_action=lambda item: item[2],
            )[0]
            decision = reproject_selected_merge_actions(
                [decision],
                reproject_action=lambda item: dict(item[2])
                if item[2].get("pending_merge_finalization") is True
                else determine_next_action(
                    config,
                    store,
                    git,
                    item[1],
                    target_branch,
                    max_resume_attempts=max_resume_attempts,
                    persist_post_merge_rebase_state=not dry_run,
                    persist_review_clearance=not dry_run,
                    read_context=_recovery_planning_read_context_for_row(item[0], item[1]),
                    selected_for_merge=True,
                ),
            )[0]
            if classify_advance_action(decision.action) == "actionable" and not decision.selected:
                selected_workers = max(0, effective_start_budget - decision.free_worker_slots)
                if (
                    batch_limit is not None
                    and effective_start_budget == batch_limit
                    and selected_workers >= batch_limit
                ):
                    gated_description = f"batch limit reached ({selected_workers}/{batch_limit}), skipping"
                else:
                    gated_description = f"{capacity_message}, skipping"
                action = {
                    "type": "skip",
                    "description": gated_description,
                }
            else:
                action = dict(decision.action)
            return row, action_task, action

        def _execute_repeat_merge(
            task: DbTask,
            action: dict[str, Any],
        ) -> tuple[str, str, dict[str, Any] | None, _MergeActionResult | None]:
            prepared_merge_git = None
            prepared_merge_branch = None
            if isolated_merge_enabled:
                prepared_merge_git = _prepare_repeat_advance_isolated_merge_checkout()
                prepared_merge_branch = target_branch if prepared_merge_git is not None else None
            if isolated_merge_enabled and prepared_merge_git is None:
                merge_result = _isolated_merge_checkout_unavailable_result()
            else:
                merge_result = _execute_merge_action(
                    config,
                    store,
                    git,
                    task,
                    action,
                    target_branch=target_branch,
                    current_branch=actual_current_branch,
                    merge_git=prepared_merge_git,
                    merge_current_branch=prepared_merge_branch,
                    already_merged_behavior=_already_merged_behavior_for_advance_action(action),
                    merge_source=MERGE_SOURCE_ADVANCE,
                    quiet_mechanics=True,
                    active_scope_tags=normalize_tag_filters(tag_filters),
                )
            if merge_result.rc == 0:
                if target_branch == actual_current_branch:
                    main_verify = _check_main_integration_verify_with_git_env(
                        config,
                        store,
                        git,
                        reason="advance-post-merge",
                    )
                    if main_verify.merges_halted and main_verify.state.task.id is not None:
                        message = main_verify.state.alert_message or "main verify is red; merges halted"
                        return (
                            "parked",
                            f"merged; {message}",
                            {
                                "type": "needs_discussion",
                                "description": f"SKIP: {message}",
                                "needs_attention_reason": MAIN_INTEGRATION_VERIFY_REASON,
                                "subject_task_id": main_verify.state.task.id,
                            },
                            merge_result,
                        )
                return "success", "merged", None, merge_result
            if getattr(merge_result, "status", None) in {
                "blocked_candidate_verify",
                "blocked_candidate_verify_unavailable",
            }:
                candidate_message = format_blocked_candidate_verify_message(str(task.id), merge_result)
                return (
                    "parked",
                    candidate_message,
                    {
                        "type": "needs_discussion",
                        "description": f"SKIP: {candidate_message}",
                        "needs_attention_reason": "blocked-candidate-verify",
                        "subject_task_id": task.id,
                    },
                    None,
                )
            if getattr(merge_result, "status", None) in {
                "deferred_blocker_materialization_failed",
                "merge_side_effect_materialization_failed",
                "isolated_merge_failed",
                "isolated_post_promotion_finalization_failed",
                "isolated_post_promotion_rollback_failed",
                "isolated_promotion_rollback_failed_target_uncertain",
                "isolated_post_promotion_checkpoint_persistence_failed",
                "isolated_post_promotion_proof_persistence_failed",
                "isolated_post_promotion_merge_state_finalization_failed",
                "post_merge_proof_persistence_failed",
                "post_merge_state_persistence_failed",
                "already_merged_proof_persistence_failed",
                "already_merged_state_persistence_failed",
            }:
                if getattr(merge_result, "status", None) == "isolated_merge_failed":
                    return (
                        "error",
                        (
                            "isolated merge promotion failed: "
                            f"{getattr(merge_result, 'block_reason', None) or 'promotion failed'}"
                        ),
                        None,
                        merge_result,
                    )
                return "error", getattr(merge_result, "block_reason", None) or "merge finalization failed", None, merge_result
            if is_pre_promotion_merge_refusal_status(getattr(merge_result, "status", None)):
                return "error", getattr(merge_result, "block_reason", None) or "merge refused before target promotion", None, merge_result
            if is_pending_merge_finalization_refusal_status(getattr(merge_result, "status", None)):
                return (
                    "error",
                    getattr(merge_result, "block_reason", None)
                    or "pending merge finalization refused before state finalization",
                    None,
                    merge_result,
                )
            resolved_subject = (
                _resolve_merge_subject(store, git, task.id, target_branch=target_branch)
                if task.id is not None
                else None
            )
            conflict_ref = resolved_subject.merge_source_ref if resolved_subject is not None else task.branch
            conflict_detected = conflict_ref is not None and not git.can_merge(conflict_ref, target_branch)
            if conflict_detected:
                try:
                    if prepared_merge_git is not None:
                        cleanup_failed_merge_checkout(prepared_merge_git)
                    else:
                        git.reset_hard_head()
                except GitError as cleanup_error:
                    return (
                        "error",
                        (f"cleanup failed after merge conflict: {cleanup_error}. Manual intervention required."),
                        None,
                        None,
                    )
                return "needs_rebase", "merge conflict routed to rebase", None, None
            return "error", getattr(merge_result, "block_reason", None) or "merge failed", None, merge_result

        def _cmd_advance_repeat() -> int:
            assert task_id is not None
            print(f"Repeating advance for {task_id} (max {repeat_max_iterations} cycles)...")
            if not auto and not dry_run:
                try:
                    _drain_pending_stdin()
                    answer = input("Proceed? [Y/n] ").strip().lower()
                except (EOFError, KeyboardInterrupt):
                    print()
                    return 0
                if answer not in ("", "y", "yes"):
                    print("Aborted.")
                    return 0
            last_action_type: str | None = None
            unchanged_streak = 0
            for cycle in range(1, repeat_max_iterations + 1):
                row, action_task, action = _resolve_repeat_cycle()
                action_type = str(action.get("type", "skip"))
                description = str(action.get("description", "")).strip()
                classification = classify_advance_action(action)
                signature_before = _repeat_state_signature(task_id)

                if action_type == "merged":
                    print(f"cycle {cycle}: merged -> success")
                    print(f"Advance repeat completed: {task_id} merged")
                    return 0
                if classification == "needs_attention":
                    print(f"cycle {cycle}: {action_type} -> parked: {description}")
                    print(f"Advance repeat parked: {description}")
                    return 0
                if classification == "skip":
                    print(f"cycle {cycle}: {action_type} -> skip: {description}")
                    print(f"Advance repeat stopped on skip: {description}")
                    return 0
                if action_task is None:
                    print(f"cycle {cycle}: {action_type} -> error: missing action task")
                    return 1
                if dry_run:
                    if action_type in {"merge", "merge_with_followups"}:
                        attention = _repeat_main_verify_attention(action, dry_run_mode=True)
                        if attention is not None:
                            _, attention_action = attention
                            message = attention_action["description"]
                            print(f"cycle {cycle}: {action_type} -> parked: {message}")
                            print(f"Advance repeat parked: {message}")
                            return 0
                    else:
                        preview_result = execute_advance_action(
                            task=action_task,
                            action=action,
                            context=_build_repeat_action_context(dry_run_mode=True),
                        )
                        attention = _repeat_execution_attention(action_task, preview_result)
                        if attention is not None:
                            message = attention.action["description"]
                            print(f"cycle {cycle}: {action_type} -> parked: {message}")
                            print(f"Advance repeat parked: {message}")
                            return 0
                    print(f"cycle {cycle}: {action_type} -> dry-run: {description}")
                    print(f"Advance repeat dry-run stopped: next action requires executing {action_type}")
                    return 0
                elif action_type in {"merge", "merge_with_followups"}:
                    attention = _repeat_main_verify_attention(action, dry_run_mode=False)
                    if attention is not None:
                        _, attention_action = attention
                        message = attention_action["description"]
                        print(f"cycle {cycle}: {action_type} -> parked: {message}")
                        print(f"Advance repeat parked: {message}")
                        return 0
                    outcome, message, merge_attention_action, merge_result = _execute_repeat_merge(action_task, action)
                    print(f"cycle {cycle}: {action_type} -> {outcome}: {message}")
                    if merge_result is not None:
                        _print_advance_deferred_blocker_lines(console, merge_result)
                    if merge_attention_action is not None:
                        print(f"Advance repeat parked: {merge_attention_action['description']}")
                        return 0
                    if outcome == "error":
                        return 1
                    signature_after = _repeat_state_signature(task_id)
                    refreshed_subject = store.get(task_id)
                    if refreshed_subject is not None and target_branch is not None:
                        if (
                            resolve_task_merge_state_for_target(
                                store=store,
                                task=refreshed_subject,
                                git=git,
                                target_branch=target_branch,
                            )
                            == "merged"
                        ):
                            print(f"Advance repeat completed: {task_id} merged")
                            return 0
                else:
                    exec_result = execute_advance_action(
                        task=action_task,
                        action=action,
                        context=_build_repeat_action_context(dry_run_mode=False),
                    )
                    outcome = exec_result.status
                    message = exec_result.success_message or exec_result.message or exec_result.error_message
                    print(f"cycle {cycle}: {action_type} -> {outcome}: {message}")
                    attention = _repeat_execution_attention(action_task, exec_result)
                    if attention is not None:
                        print(f"Advance repeat parked: {attention.action['description']}")
                        return 0
                    if exec_result.status == "error":
                        return 1
                    signature_after = _repeat_state_signature(task_id)
                    if exec_result.status == "skip" and signature_after == signature_before:
                        print(f"Advance repeat stopped on skip: {message}")
                        return 0
                    refreshed_subject = store.get(task_id)
                    if refreshed_subject is not None and target_branch is not None:
                        if (
                            resolve_task_merge_state_for_target(
                                store=store,
                                task=refreshed_subject,
                                git=git,
                                target_branch=target_branch,
                            )
                            == "merged"
                        ):
                            print(f"Advance repeat completed: {task_id} merged")
                            return 0

                cycle_unchanged = signature_after == signature_before
                if cycle_unchanged and last_action_type == action_type:
                    unchanged_streak += 1
                else:
                    unchanged_streak = 1 if cycle_unchanged else 0
                if unchanged_streak >= 2:
                    print(f"Advance repeat stopped: no progress after repeated {action_type}")
                    return 0
                last_action_type = action_type
            print(f"Advance repeat stopped: max iterations ({repeat_max_iterations}) reached")
            return 0

        if repeat_mode:
            repeat_worker_budget = 1 if effective_start_budget > 0 or dry_run else 0
            isolated_merge_enabled = bool(config.main_checkout_isolate)
            repeat_advance_merge_git: Git | None = None

            def _prepare_repeat_advance_isolated_merge_checkout() -> Git | None:
                nonlocal repeat_advance_merge_git
                try:
                    repeat_advance_merge_git = ensure_watch_main_checkout(config, git, target_branch)
                    return repeat_advance_merge_git
                except GitError:
                    try:
                        repeat_advance_merge_git = ensure_watch_main_checkout(config, git, target_branch, rebuild=True)
                        return repeat_advance_merge_git
                    except GitError:
                        repeat_advance_merge_git = None
                        return None

            if dry_run:
                return _cmd_advance_repeat()

            try:
                repeat_permit = launch_permit(config, store)
            except MaxConcurrentTasksError as exc:
                print(f"Repeating advance for {task_id} (max {repeat_max_iterations} cycles)...")
                print(f"cycle 1: skip -> skip: {exc}")
                print(f"Advance repeat stopped on skip: {exc}")
                return 0

            repeat_registry = WorkerRegistry(config.workers_path)
            repeat_worker_id = repeat_registry.generate_worker_id()
            repeat_task = store.add(
                f"Internal advance repeat session for {task_id}",
                task_type="internal",
                depends_on=task_id,
                tags=("system-advance-repeat",),
                skip_learnings=True,
            )
            assert repeat_task.id is not None
            repeat_task.status = "in_progress"
            repeat_task.running_pid = os.getpid()
            repeat_task.started_at = datetime.now(UTC)
            store.update(repeat_task)
            try:
                repeat_registry.register(
                    WorkerMetadata(
                        worker_id=repeat_worker_id,
                        task_id=str(repeat_task.id),
                        pid=os.getpid(),
                        started_at=datetime.now(UTC).isoformat(),
                        status="running",
                        is_background=False,
                    )
                )
            except BaseException:
                repeat_permit.release()
                repeat_task.status = "failed"
                repeat_task.completed_at = datetime.now(UTC)
                repeat_task.running_pid = None
                repeat_task.completion_reason = "advance repeat failed before worker registration"
                store.update(repeat_task)
                raise
            repeat_permit.release()

            previous_worker_id = os.environ.get("GZA_WORKER_ID")
            previous_worker_mode = os.environ.get("GZA_WORKER_MODE")
            previous_reuse_worker_owner = os.environ.get(_REUSE_WORKER_OWNER_ENV)
            previous_reuse_worker_reentry = os.environ.get(_REUSE_WORKER_REENTRY_ENV)
            previous_reuse_worker_session = os.environ.get(_REUSE_WORKER_SESSION_ENV)
            os.environ["GZA_WORKER_ID"] = repeat_worker_id
            os.environ["GZA_WORKER_MODE"] = "1"
            os.environ[_REUSE_WORKER_OWNER_ENV] = _REUSE_WORKER_OWNER_OUTER
            os.environ[_REUSE_WORKER_REENTRY_ENV] = "1"
            os.environ[_REUSE_WORKER_SESSION_ENV] = "1"
            try:
                rc = _cmd_advance_repeat()
            except BaseException:
                repeat_registry.mark_completed(repeat_worker_id, exit_code=1, status="failed")
                refreshed_repeat = store.get(str(repeat_task.id))
                if refreshed_repeat is not None:
                    refreshed_repeat.status = "failed"
                    refreshed_repeat.completed_at = datetime.now(UTC)
                    refreshed_repeat.running_pid = None
                    refreshed_repeat.completion_reason = "advance repeat interrupted before cleanup"
                    store.update(refreshed_repeat)
                raise
            finally:
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
            repeat_registry.mark_completed(
                repeat_worker_id,
                exit_code=rc,
                status="completed" if rc == 0 else "failed",
            )
            refreshed_repeat = store.get(str(repeat_task.id))
            if refreshed_repeat is not None:
                refreshed_repeat.status = "completed" if rc == 0 else "failed"
                refreshed_repeat.completed_at = datetime.now(UTC)
                refreshed_repeat.running_pid = None
                store.update(refreshed_repeat)
            return rc

        for row in owner_rows:
            if (
                row.lifecycle_action_task is None
                and row.recovery_action_task is not None
                and row.recovery_leaf_task is not None
                and row.recovery_leaf_task.id not in recovery_preview_task_ids
            ):
                continue
            action_task = row.lifecycle_action_task or row.recovery_action_task or row.owner_task
            precomputed_action = row.next_action
            action = (
                precomputed_action
                if (
                    precomputed_action is not None
                    and str(precomputed_action.get("type", "")) != "unknown"
                    and row.lifecycle_action_task is None
                    and row.recovery_action_task is None
                )
                else determine_next_action(
                    config,
                    store,
                    git,
                    action_task,
                    target_branch,
                    max_resume_attempts=max_resume_attempts,
                    persist_post_merge_rebase_state=persist_planning_state,
                    persist_review_clearance=persist_planning_state,
                    read_context=_recovery_planning_read_context_for_row(row, action_task),
                )
            )
            plan.append((row, action_task, action))

        plan.sort(key=lambda item: lifecycle_action_execution_sort_key(item[1], item[2]))

        attention_plan = [
            (
                resolve_subject_task(store, action, row, fallback_task=row.owner_task),
                action,
            )
            for row, _task, action in plan
            if classify_advance_action(action) == "needs_attention"
        ]

        preview_context = _build_action_context(dry_run_mode=True)
        preview_attention_plan = list(attention_plan)
        main_verify_attention = _main_verify_attention_item()
        if main_verify_attention is not None:
            _append_attention_once(preview_attention_plan, *main_verify_attention)
        execution_decisions = plan_lifecycle_execution(
            plan,
            free_worker_slots=effective_start_budget,
            get_action=lambda item: item[2],
        )

        def _gated_lifecycle_skip_message(*, free_worker_slots: int) -> str:
            selected_workers = max(0, effective_start_budget - free_worker_slots)
            if batch_limit is not None and effective_start_budget == batch_limit and selected_workers >= batch_limit:
                return f"batch limit reached ({selected_workers}/{batch_limit}), skipping"
            return f"{capacity_message}, skipping"

        if main_verify_attention is None:
            execution_decisions = reproject_selected_merge_actions(
                execution_decisions,
                reproject_action=lambda item: dict(item[2])
                if item[2].get("pending_merge_finalization") is True
                else determine_next_action(
                    config,
                    store,
                    git,
                    item[1],
                    target_branch,
                    max_resume_attempts=max_resume_attempts,
                    persist_post_merge_rebase_state=persist_planning_state,
                    persist_review_clearance=persist_planning_state,
                    read_context=_recovery_planning_read_context_for_row(item[0], item[1]),
                    selected_for_merge=True,
                ),
            )

        for decision in execution_decisions:
            row, task, action = decision.item
            action = dict(decision.action)
            if classify_advance_action(action) != "actionable":
                continue
            if not decision.selected:
                preview_gated_rows.append(
                    (row, task, action, _gated_lifecycle_skip_message(free_worker_slots=decision.free_worker_slots))
                )
                continue
            if main_verify_attention is not None and action["type"] in {"merge", "merge_with_followups"}:
                continue
            description = action["description"]
            if action["type"] in {"merge", "merge_with_followups"} and dry_run:
                resolved_subject = (
                    _resolve_merge_subject(store, git, task.id, target_branch=target_branch)
                    if task.id is not None
                    else None
                )
                commit_count = _auto_squash_commit_count(
                    config,
                    git,
                    resolved_subject.merge_source_ref if resolved_subject is not None else task.branch,
                    target_branch,
                )
                if commit_count is not None:
                    description = f"{description} (auto-squash, {commit_count} commits)"
            elif is_worker_consuming_advance_action(action["type"]):
                preview_result = execute_advance_action(task=task, action=action, context=preview_context)
                attention = resolve_execution_needs_attention(task, preview_result)
                if attention is not None:
                    _append_attention_once(
                        preview_attention_plan,
                        getattr(attention, "task", row.owner_task),
                        attention.action,
                    )
                    continue
                if preview_result.status == "dry_run" and preview_result.message:
                    description = preview_result.message
            preview_actionable_rows.append((row, task, action, description))

        if not preview_actionable_rows and not dry_run:
            if not new_mode:
                print("No eligible tasks to advance")
                _print_needs_attention_section(preview_attention_plan)
                if plan:
                    print()
                    for row, _task, action in plan:
                        if classify_advance_action(action) != "skip":
                            continue
                        display_task = row.owner_task
                        prompt_display = shorten_prompt(display_task.prompt, _prompt_avail(display_task.id))
                        console.print(f"[{_c_tid}]{display_task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
                        _color = _advance_action_color(action["type"])
                        console.print(f"[{_color}]→ {action['description']}[/{_color}]")
                    for row, _task, _action, description in preview_gated_rows:
                        display_task = row.owner_task
                        prompt_display = shorten_prompt(display_task.prompt, _prompt_avail(display_task.id))
                        console.print(f"[{_c_tid}]{display_task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
                        console.print(f"[{_c_warn}]— {description}[/{_c_warn}]")
                    print()
                return 0
            if preview_attention_plan:
                _print_needs_attention_section(preview_attention_plan)
                print()
            if plan:
                for row, _task, action in plan:
                    if classify_advance_action(action) != "skip":
                        continue
                    display_task = row.owner_task
                    prompt_display = shorten_prompt(display_task.prompt, _prompt_avail(display_task.id))
                    console.print(f"[{_c_tid}]{display_task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
                    _color = _advance_action_color(action["type"])
                    console.print(f"[{_color}]→ {action['description']}[/{_color}]")
                for row, _task, _action, description in preview_gated_rows:
                    display_task = row.owner_task
                    prompt_display = shorten_prompt(display_task.prompt, _prompt_avail(display_task.id))
                    console.print(f"[{_c_tid}]{display_task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
                    console.print(f"[{_c_warn}]— {description}[/{_c_warn}]")
                print()

        if dry_run:
            for warning in failed_task_recovery_warnings:
                print(f"Warning: {warning}", file=sys.stderr)
            if not no_resume_failed and owner_rows:
                _print_mixed_recovery_preview_entries(
                    store=store,
                    preview=recovery_preview,
                    max_recovery_attempts=max_resume_attempts,
                )
            if preview_actionable_rows:
                print(f"Would advance {len(preview_actionable_rows)} task(s):\n")
                print_lifecycle_action_entries(
                    console,
                    [
                        LifecycleActionEntry(
                            owner_task=row.owner_task,
                            action_task=task,
                            action=action,
                            description=description,
                        )
                        for row, task, action, description in preview_actionable_rows
                    ],
                )
            elif not preview_attention_plan:
                print("No eligible tasks to advance")
            _print_needs_attention_section(preview_attention_plan)
            if plan:
                skip_rows_printed = False
                for row, _task, action in plan:
                    if classify_advance_action(action) != "skip":
                        continue
                    if not skip_rows_printed:
                        print()
                        skip_rows_printed = True
                    display_task = row.owner_task
                    prompt_display = shorten_prompt(display_task.prompt, _prompt_avail(display_task.id))
                    console.print(f"[{_c_tid}]{display_task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
                    _color = _advance_action_color(action["type"])
                    console.print(f"[{_color}]→ {action['description']}[/{_color}]")
                    print()
                for row, _task, _action, description in preview_gated_rows:
                    if not skip_rows_printed:
                        print()
                        skip_rows_printed = True
                    display_task = row.owner_task
                    prompt_display = shorten_prompt(display_task.prompt, _prompt_avail(display_task.id))
                    console.print(f"[{_c_tid}]{display_task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
                    console.print(f"[{_c_warn}]— {description}[/{_c_warn}]")
                    print()
            if new_mode and batch_limit is not None:
                planned_workers = count_worker_consuming_actions(
                    [action for _, _, action, _ in preview_actionable_rows]
                )
                remaining = max(0, effective_start_budget - planned_workers)
                if remaining > 0:
                    pending_tasks = get_runnable_pending_tasks(
                        store,
                        limit=remaining,
                        tags=normalize_tag_filters(tag_filters),
                        any_tag=any_tag,
                        quiet_seconds=config.quiet_period_seconds,
                    )
                    if pending_tasks:
                        print(f"Would start {len(pending_tasks)} new pending task(s):\n")
                        for pt in pending_tasks:
                            prompt_display = shorten_prompt(pt.prompt, _prompt_avail(pt.id))
                            console.print(f"[{_c_tid}]{pt.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
                            console.print(f"[{_c_default}]→ Start new worker[/{_c_default}]")
                            print()
                    else:
                        print("No pending tasks available to fill batch\n")
            return 0

        if preview_actionable_rows:
            print(f"Will advance {len(preview_actionable_rows)} task(s):\n")
            print_lifecycle_action_entries(
                console,
                [
                    LifecycleActionEntry(
                        owner_task=row.owner_task,
                        action_task=task,
                        action=action,
                        description=description,
                    )
                    for row, task, action, description in preview_actionable_rows
                ],
            )
            if preview_attention_plan:
                _print_needs_attention_section(preview_attention_plan)
                print()
        elif preview_attention_plan:
            _print_needs_attention_section(preview_attention_plan)
            print()

        if new_mode and batch_limit is not None:
            planned_workers = count_worker_consuming_actions([action for _, _, action, _ in preview_actionable_rows])
            remaining = max(0, effective_start_budget - planned_workers)
            if remaining > 0:
                new_pending_tasks = get_runnable_pending_tasks(
                    store,
                    limit=remaining,
                    tags=normalize_tag_filters(tag_filters),
                    any_tag=any_tag,
                    quiet_seconds=config.quiet_period_seconds,
                )
                if new_pending_tasks:
                    print(f"Will start {len(new_pending_tasks)} new pending task(s):\n")
                    for pt in new_pending_tasks:
                        prompt_display = shorten_prompt(pt.prompt, _prompt_avail(pt.id))
                        console.print(f"[{_c_tid}]{pt.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
                        console.print(f"[{_c_default}]→ Start new worker[/{_c_default}]")
                        print()

        if not auto and (preview_actionable_rows or new_mode):
            try:
                _drain_pending_stdin()
                answer = input("Proceed? [Y/n] ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                print()
                return 0
            if answer not in ("", "y", "yes"):
                print("Aborted.")
                return 0

    # Execute actions
    success_count = 0
    skip_count = 0
    error_count = 0
    workers_started = 0
    attention_tasks: list[tuple[DbTask, dict]] = []
    action_context = _build_action_context(dry_run_mode=False)
    merge_halt_attention = main_verify_attention[1] if main_verify_attention is not None else None
    isolated_merge_enabled = bool(config.main_checkout_isolate)
    advance_merge_git: Git | None = None

    def _prepare_advance_isolated_merge_checkout() -> Git | None:
        nonlocal advance_merge_git
        try:
            advance_merge_git = ensure_watch_main_checkout(config, git, target_branch)
            return advance_merge_git
        except GitError:
            try:
                advance_merge_git = ensure_watch_main_checkout(config, git, target_branch, rebuild=True)
                return advance_merge_git
            except GitError:
                advance_merge_git = None
                return None

    if main_verify_attention is not None:
        _append_attention_once(attention_tasks, *main_verify_attention)

    def _render_worker_action_result(action_task: DbTask, display_task: DbTask, action_type: str, exec_result) -> None:
        nonlocal workers_started, success_count, skip_count, error_count

        if exec_result.attempted_spawn:
            workers_started += 1

        if exec_result.status == "skip":
            console.print(f"[{_c_warn}]{exec_result.message}[/{_c_warn}]")
            skip_count += 1
            attention = resolve_execution_needs_attention(action_task, exec_result)
            if attention is not None:
                _append_attention_once(
                    attention_tasks,
                    getattr(attention, "task", display_task),
                    attention.action,
                )
            return

        if exec_result.status == "error":
            if exec_result.success_message:
                console.print(f"[{_c_ok}]✓ {exec_result.success_message}[/{_c_ok}]")
            err_message = exec_result.error_message or exec_result.message or f"Failed to execute {action_type}"
            console.print(f"[{_c_err}]✗ {err_message}[/{_c_err}]")
            error_count += 1
            return

        success_message = exec_result.success_message or exec_result.message
        if success_message:
            console.print(f"[{_c_ok}]✓ {success_message}[/{_c_ok}]")

        if exec_result.worker_started or (exec_result.work_done and not exec_result.worker_consuming):
            success_count += 1
        elif exec_result.worker_consuming:
            error_count += 1

    for decision in execution_decisions:
        row, task, action = decision.item
        action = dict(decision.action)
        assert task.id is not None
        display_task = row.owner_task
        prompt_display = shorten_prompt(display_task.prompt, _prompt_avail(display_task.id))
        action_type = action["type"]

        if classify_advance_action(action) != "actionable":
            console.print(f"[{_c_tid}]{display_task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
            _color = _advance_action_color(action_type)
            console.print(f"[{_color}]{action['description']}[/{_color}]")
            skip_count += 1
            if classify_advance_action(action) == "needs_attention":
                _append_attention_once(
                    attention_tasks,
                    resolve_subject_task(store, action, row, fallback_task=display_task),
                    action,
                )
            continue

        if not decision.selected:
            console.print(f"[{_c_tid}]{display_task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
            message = _gated_lifecycle_skip_message(free_worker_slots=decision.free_worker_slots)
            console.print(f"[{_c_warn}]— {message}[/{_c_warn}]")
            print()
            skip_count += 1
            continue

        console.print(f"[{_c_tid}]{display_task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
        _color = _advance_action_color(action_type)
        console.print(f"[{_color}]→ {action['description']}[/{_color}]")

        if action_type in {"merge", "merge_with_followups"}:
            if merge_halt_attention is not None:
                console.print(f"[{_c_warn}]SKIP: {merge_halt_attention['description'][6:]}[/{_c_warn}]")
                skip_count += 1
                print()
                continue
            prepared_merge_git = None
            prepared_merge_branch = None
            if isolated_merge_enabled:
                prepared_merge_git = _prepare_advance_isolated_merge_checkout()
                prepared_merge_branch = target_branch if prepared_merge_git is not None else None
            if isolated_merge_enabled and prepared_merge_git is None:
                merge_result = _isolated_merge_checkout_unavailable_result()
            else:
                merge_result = _execute_merge_action(
                    config,
                    store,
                    git,
                    task,
                    action,
                    target_branch=target_branch,
                    current_branch=actual_current_branch,
                    merge_git=prepared_merge_git,
                    merge_current_branch=prepared_merge_branch,
                    already_merged_behavior=_already_merged_behavior_for_advance_action(action),
                    merge_source=MERGE_SOURCE_ADVANCE,
                    active_scope_tags=normalize_tag_filters(tag_filters),
                )
            if merge_result.created_followups:
                created_ids = ", ".join(str(t.id) for t in merge_result.created_followups if t.id is not None)
                console.print(f"[{_c_ok}]✓ Created follow-up task(s): {created_ids}[/{_c_ok}]")
            if merge_result.reused_followups:
                reused_ids = ", ".join(str(t.id) for t in merge_result.reused_followups if t.id is not None)
                console.print(f"[{_c_warn}]↺ Reused follow-up task(s): {reused_ids}[/{_c_warn}]")
            _print_advance_deferred_blocker_lines(console, merge_result)
            created_investigation_task_ids = getattr(merge_result, "created_investigation_task_ids", ())
            reused_investigation_task_ids = getattr(merge_result, "reused_investigation_task_ids", ())
            if created_investigation_task_ids:
                created_ids = ", ".join(created_investigation_task_ids)
                console.print(f"[{_c_ok}]✓ Created investigation task(s): {created_ids}[/{_c_ok}]")
            if reused_investigation_task_ids:
                reused_ids = ", ".join(reused_investigation_task_ids)
                console.print(f"[{_c_warn}]↺ Reused investigation task(s): {reused_ids}[/{_c_warn}]")
            for warning in getattr(merge_result, "promotion_warnings", ()):
                console.print(f"[{_c_warn}]WARN: {warning}[/{_c_warn}]")
            rc = merge_result.rc
            if rc == 0:
                console.print(f"[{_c_ok}]✓ Merged[/{_c_ok}]")
                success_count += 1
                main_verify = _check_main_integration_verify_with_git_env(
                    config,
                    store,
                    git,
                    reason="advance-post-merge",
                )
                if main_verify.merges_halted and main_verify.state.task.id is not None:
                    merge_halt_attention = {
                        "type": "needs_discussion",
                        "description": f"SKIP: {main_verify.state.alert_message or 'main verify is red; merges halted'}",
                        "needs_attention_reason": MAIN_INTEGRATION_VERIFY_REASON,
                        "subject_task_id": main_verify.state.task.id,
                    }
                    _append_attention_once(attention_tasks, main_verify.state.task, merge_halt_attention)
            else:
                if getattr(merge_result, "status", None) in {
                    "blocked_candidate_verify",
                    "blocked_candidate_verify_unavailable",
                }:
                    candidate_message = format_blocked_candidate_verify_message(str(display_task.id), merge_result)
                    console.print(f"[{_c_warn}]! {candidate_message}[/{_c_warn}]")
                    _append_attention_once(
                        attention_tasks,
                        display_task,
                        {
                            "type": "needs_discussion",
                            "description": f"SKIP: {candidate_message}",
                            "needs_attention_reason": "blocked-candidate-verify",
                            "subject_task_id": display_task.id,
                        },
                    )
                    skip_count += 1
                    print()
                    continue
                if getattr(merge_result, "status", None) in {
                    "deferred_blocker_materialization_failed",
                    "merge_side_effect_materialization_failed",
                    "isolated_merge_failed",
                    "isolated_post_promotion_finalization_failed",
                    "isolated_post_promotion_rollback_failed",
                    "isolated_promotion_rollback_failed_target_uncertain",
                    "isolated_post_promotion_checkpoint_persistence_failed",
                    "isolated_post_promotion_proof_persistence_failed",
                    "isolated_post_promotion_merge_state_finalization_failed",
                    "post_merge_proof_persistence_failed",
                    "post_merge_state_persistence_failed",
                    "already_merged_proof_persistence_failed",
                    "already_merged_state_persistence_failed",
                }:
                    block_reason = getattr(merge_result, "block_reason", None) or "merge finalization failed"
                    if getattr(merge_result, "status", None) == "isolated_merge_failed":
                        block_reason = f"isolated merge promotion failed: {block_reason}"
                    console.print(
                        f"      [{_c_err}]✗ {block_reason}[/{_c_err}]"
                    )
                    error_count += 1
                    continue
                if is_pre_promotion_merge_refusal_status(getattr(merge_result, "status", None)):
                    block_reason = getattr(merge_result, "block_reason", None) or "merge refused before target promotion"
                    console.print(f"[{_c_err}]✗ {block_reason}[/{_c_err}]")
                    error_count += 1
                    continue
                if is_pending_merge_finalization_refusal_status(getattr(merge_result, "status", None)):
                    block_reason = (
                        getattr(merge_result, "block_reason", None)
                        or "pending merge finalization refused before state finalization"
                    )
                    console.print(f"[{_c_err}]✗ {block_reason}[/{_c_err}]")
                    error_count += 1
                    continue
                resolved_subject = (
                    _resolve_merge_subject(store, git, task.id, target_branch=target_branch)
                    if task.id is not None
                    else None
                )
                conflict_ref = resolved_subject.merge_source_ref if resolved_subject is not None else task.branch
                conflict_detected = conflict_ref is not None and not git.can_merge(conflict_ref, target_branch)
                if conflict_detected:
                    console.print(f"[{_c_warn}]! Merge had conflicts against '{target_branch}'[/{_c_warn}]")
                    try:
                        # _merge_single_task already attempts merge --abort.
                        # For failed squash merges, MERGE_HEAD may be absent, so
                        # force cleanup as a final fallback.
                        git.reset_hard_head()
                        console.print(f"[{_c_ok}]✓ Restored clean git state[/{_c_ok}]")
                    except GitError as cleanup_error:
                        console.print(
                            f"      [{_c_err}]✗ Cleanup failed after merge conflict: {cleanup_error}. "
                            f"Manual intervention required.[/{_c_err}]"
                        )
                        error_count += 1
                        continue
                    exec_result = execute_advance_action(
                        task=task,
                        action={"type": "needs_rebase", "description": "Create rebase task"},
                        context=action_context,
                    )
                    if exec_result.success_message:
                        exec_result.success_message = f"{exec_result.success_message} (target: {target_branch})"
                    _render_worker_action_result(task, display_task, action_type, exec_result)
                else:
                    console.print(f"[{_c_err}]✗ Merge failed[/{_c_err}]")
                    error_count += 1

        else:
            exec_result = execute_advance_action(task=task, action=action, context=action_context)
            _render_worker_action_result(task, display_task, action_type, exec_result)

        print()

    # --new: start pending tasks to fill remaining batch slots
    new_started = 0
    if new_mode and batch_limit is not None and workers_started < effective_start_budget:
        # Use the pre-fetched new_pending_tasks list so each worker gets a
        # distinct task.  If we didn't pre-fetch (e.g. no confirmation prompt
        # was shown), fetch now.
        if not new_pending_tasks:
            remaining = effective_start_budget - workers_started
            new_pending_tasks = get_runnable_pending_tasks(
                store,
                limit=remaining,
                tags=normalize_tag_filters(tag_filters),
                any_tag=any_tag,
                quiet_seconds=config.quiet_period_seconds,
            )
        for pt in new_pending_tasks:
            if workers_started >= effective_start_budget:
                break
            if _advance_uses_iterate(config) and pt.task_type == "implement":
                iterate_args = argparse.Namespace(
                    no_docker=getattr(args, "no_docker", False),
                    force=force,
                )
                rc = _spawn_prepared_background_iterate(
                    iterate_args,
                    config,
                    pt,
                    max_iterations=config.iterate_max_iterations,
                    auto_iterate=True,
                    quiet=True,
                )
            else:
                worker_args = _worker_args()
                rc = _spawn_background_worker(worker_args, config, task_id=pt.id, quiet=True)
            if rc != 0:
                error_count += 1
                break  # error spawning
            new_started += 1
            workers_started += 1

    parts = []
    if success_count:
        parts.append(f"[{_c_ok}]{success_count} advanced[/{_c_ok}]")
    if new_started > 0:
        parts.append(f"[{_c_ok}]{new_started} new[/{_c_ok}]")
    if skip_count:
        parts.append(f"[{_c_warn}]{skip_count} skipped[/{_c_warn}]")
    if error_count:
        parts.append(f"[{_c_err}]{error_count} errors[/{_c_err}]")
    console.print(", ".join(parts) if parts else "Nothing to do")

    if attention_tasks:
        _print_needs_attention_section(attention_tasks)

    return 0 if error_count == 0 else 1
