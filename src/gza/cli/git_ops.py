"""Git-related CLI commands: merge, rebase, checkout, diff, PR, advance."""

import argparse
import logging
import os
import shutil
import subprocess
import sys
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypeVar

import gza.colors as _colors
from gza.query import (
    get_base_task_slug as _get_base_task_slug,
    get_reviews_for_root as _get_reviews_for_root,
)

from ..advance_engine import _resolve_and_persist_post_merge_rebase_state, _resolve_current_merge_source
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
from ..config import Config
from ..console import (
    console,
    prompt_available_width,
    shorten_prompt,
)
from ..db import (
    DB_UNSET,
    MERGE_SOURCE_ADVANCE,
    MERGE_SOURCE_MANUAL,
    MergeTargetResolutionError,
    SqliteTaskStore,
    Task as DbTask,
    TaskStats,
    task_id_numeric_key,
)
from ..dependency_preconditions import task_is_merged
from ..derived_tags import resolve_derived_task_tags
from ..failure_reasons import mark_task_failed_from_cause
from ..git import (
    Git,
    GitError,
    ResolvedMergeSourceRef,
    active_worktree_path_for_branch,
    cleanup_worktree_for_branch,
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
    check_main_integration_verify,
)
from ..merge_state import resolve_task_merge_state_for_target
from ..pickup import (
    count_worker_consuming_actions,
    get_runnable_pending_tasks,
    is_worker_consuming_advance_action,
)
from ..pr_ops import build_task_pr_content, ensure_task_pr
from ..rebase_diff import capture_rebase_diff_baseline, compute_rebase_changed_diff
from ..rebase_publish import publish_rebased_branch
from ..recovery_engine import (
    list_failed_tasks_for_recovery,
    resolve_pending_recovery_execution_mode,
    resolve_recovery_planning_task,
)
from ..review_verdict import (
    ReviewFinding,
    get_review_content,
    get_review_report,
    is_verify_blocked_only_review,
    summarize_review_blockers,
)
from ..review_verify_state import refresh_preserved_rebase_review_verify_heads
from ..runner import (
    WIP_INTERRUPTED_COMMIT_SUBJECT,
    TaskExecutionLogger,
    _complete_failed_code_task_after_pr_publication,
    ensure_task_log_path,
    get_effective_config_for_task,
    load_dotenv,
    task_log_storage_path,
    write_log_entry,
)
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
from ..worktree_roots import managed_worktree_root_paths
from ._common import (
    DuplicateReviewError,
    _create_implementation_task_from_source,
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
    _spawn_background_iterate_worker,
    _spawn_background_resume_worker,
    _spawn_background_worker,
    get_review_verdict,  # noqa: F401  # re-exported for test patching
    get_store,
    phase1_error,
    resolve_id,
)
from ._lifecycle_actions import (
    LifecycleActionEntry,
    lifecycle_action_execution_sort_key,
    plan_lifecycle_execution,
    print_lifecycle_action_entries,
)
from .advance_engine import (
    NEEDS_ATTENTION_LABEL,
    classify_advance_action,
    determine_next_action,
    format_needs_attention_entry_for_display,
    needs_attention_recommends_fix,
    resolve_subject_task,
)
from .advance_executor import (
    AdvanceActionExecutionContext,
    BranchDivergenceReconcileResult,
    execute_advance_action,
    resolve_execution_needs_attention,
)

logger = logging.getLogger(__name__)

_T = TypeVar("_T")


@dataclass(frozen=True)
class _ResolvedMergeSubject:
    trigger_task: DbTask
    execution_task: DbTask
    merge_subject: DbTask
    merge_unit_id: str | None
    merge_branch: str | None
    merge_source_ref: str | None
    merge_source_warning: str | None


@dataclass(frozen=True)
class _MergeDeferredBlockerDecision:
    review_task: DbTask | None
    blockers: tuple[ReviewFinding, ...]
    should_materialize: bool
    refusal_message: str | None = None


def _materialize_merge_followups(
    store: SqliteTaskStore,
    config: Config,
    merge_subject: DbTask,
) -> tuple[list[DbTask], list[DbTask]]:
    """Create or reuse FOLLOWUP tasks for the latest completed review on a merged task."""
    review_task = _latest_completed_review_for_merge_subject(store, merge_subject)
    if review_task is None:
        return ([], [])
    report = get_review_report(config.project_dir, review_task)
    findings = tuple(finding for finding in report.findings if finding.severity == "FOLLOWUP")
    if not findings:
        return ([], [])
    return _create_or_reuse_followup_tasks(
        store,
        review_task=review_task,
        impl_task=merge_subject,
        findings=findings,
        trigger_source="manual",
    )


def _latest_completed_review_for_merge_subject(
    store: SqliteTaskStore,
    merge_subject: DbTask,
) -> DbTask | None:
    if merge_subject.id is None:
        return None
    return next(
        (
            review
            for review in _get_reviews_for_root(store, merge_subject)
            if review.status == "completed" and review.completed_at is not None
        ),
        None,
    )


def _classify_manual_merge_blockers(
    *,
    store: SqliteTaskStore,
    config: Config,
    merge_subject: DbTask,
    defer_blockers: bool,
) -> _MergeDeferredBlockerDecision:
    review_task = _latest_completed_review_for_merge_subject(store, merge_subject)
    if review_task is None:
        return _MergeDeferredBlockerDecision(
            review_task=None,
            blockers=(),
            should_materialize=False,
        )

    report = get_review_report(config.project_dir, review_task)
    review_content = get_review_content(config.project_dir, review_task)
    if report.verdict != "CHANGES_REQUESTED":
        return _MergeDeferredBlockerDecision(
            review_task=review_task,
            blockers=(),
            should_materialize=False,
        )

    blockers = tuple(finding for finding in report.findings if finding.severity == "BLOCKER")
    if not blockers:
        assert merge_subject.id is not None
        assert review_task.id is not None
        return _MergeDeferredBlockerDecision(
            review_task=review_task,
            blockers=(),
            should_materialize=False,
            refusal_message=(
                f"Error: Task {merge_subject.id} has CHANGES_REQUESTED review {review_task.id}, "
                "but no parsed BLOCKER findings were available to defer. Refusing to guess."
            ),
        )

    if defer_blockers:
        return _MergeDeferredBlockerDecision(
            review_task=review_task,
            blockers=blockers,
            should_materialize=True,
        )

    if is_verify_blocked_only_review(review_content):
        return _MergeDeferredBlockerDecision(
            review_task=review_task,
            blockers=blockers,
            should_materialize=True,
        )

    assert merge_subject.id is not None
    assert review_task.id is not None
    summary = summarize_review_blockers(review_content)
    if summary.blocker_count != len(blockers):
        return _MergeDeferredBlockerDecision(
            review_task=review_task,
            blockers=(),
            should_materialize=False,
            refusal_message=(
                f"Error: Task {merge_subject.id} has CHANGES_REQUESTED review {review_task.id}, "
                "but blocker classification did not match the parsed blocker set. Refusing to guess."
            ),
        )
    return _MergeDeferredBlockerDecision(
        review_task=review_task,
        blockers=blockers,
        should_materialize=False,
        refusal_message=(
            f"Error: Task {merge_subject.id} has open BLOCKER findings in review {review_task.id}.\n"
            "Use --defer-blockers to merge anyway and create urgent PR-required follow-up tasks."
        ),
    )


def _materialize_merge_deferred_blockers(
    store: SqliteTaskStore,
    config: Config,
    merge_subject: DbTask,
    *,
    defer_blockers: bool,
) -> tuple[list[DbTask], list[DbTask]] | None:
    decision = _classify_manual_merge_blockers(
        store=store,
        config=config,
        merge_subject=merge_subject,
        defer_blockers=defer_blockers,
    )
    if decision.refusal_message is not None:
        print(decision.refusal_message)
        return None
    if not decision.should_materialize or decision.review_task is None or not decision.blockers:
        return ([], [])
    return _create_or_reuse_deferred_blocker_tasks(
        store,
        review_task=decision.review_task,
        impl_task=merge_subject,
        findings=decision.blockers,
        trigger_source="manual",
    )


def _merge_execution_status_error(
    merge_subject_id: str,
    execution_task: DbTask,
) -> str | None:
    if execution_task.status in {"completed", "unmerged"}:
        return None
    return (
        f"Task {merge_subject_id} is not completed or unmerged "
        f"(execution status: {execution_task.status})"
    )


@dataclass(frozen=True)
class SquashBranchReconcileResult:
    status: str
    branch: str
    remote: str = "origin"
    reason: str | None = None
    manual_source_ref: str | None = None
    expected_remote_oid: str | None = None

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
    task_logger = TaskExecutionLogger(resolve_ops_log_path(config, log_path), echo=True) if log_path is not None else None
    default_branch = git.default_branch()
    publication_state = load_branch_publication_state(store, task.id)
    return _complete_failed_code_task_after_pr_publication(
        task=task,
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
    )


def _reconcile_diverged_branch_with_origin(
    config: Config,
    git: Git,
    task: DbTask,
    *,
    remote: str = "origin",
) -> BranchDivergenceReconcileResult:
    """Reconcile a diverged local/origin branch without consuming a worker slot."""
    if not task.branch:
        return BranchDivergenceReconcileResult(
            status="error",
            message=f"Cannot reconcile divergence for task {task.id}: branch is missing",
        )

    branch = task.branch
    remote_ref = f"{remote}/{branch}"
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
                    message=(
                        f"Force-with-lease push failed for '{branch}' without a remote ref change: {push_error}"
                    ),
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
                message=f"Failed to fetch {remote} before rebasing '{branch}' onto '{remote_ref}': {fetch_error}",
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
        worktree_git = Git(worktree_path)
        baseline = capture_rebase_diff_baseline(
            worktree_git,
            branch=branch,
            target=remote_ref,
        )
        try:
            worktree_git.rebase(remote_ref)
        except GitError as rebase_error:
            try:
                worktree_git.rebase_abort()
            except GitError:
                pass
            return BranchDivergenceReconcileResult(
                status="needs_attention",
                message=(
                    f"SKIP: mechanical rebase onto '{remote_ref}' hit conflicts: {rebase_error}. "
                    "Resolve the origin divergence manually; the sandboxed rebase worker cannot access "
                    "that remote-tracking ref."
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
            "and pushed with --force-with-lease"
            if publish_result.pushed
            else "and verified origin was already aligned"
        )
        return BranchDivergenceReconcileResult(
            status="reconciled",
            message=f"Rebased '{branch}' onto '{remote_ref}' {publish_detail}",
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


def _coerce_merge_single_task_result(result: int | _MergeSingleTaskResult) -> _MergeSingleTaskResult:
    if isinstance(result, _MergeSingleTaskResult):
        return result
    return _MergeSingleTaskResult(rc=result)


def _resolve_fresh_merge_source(git: Git, branch: str | None) -> ResolvedMergeSourceRef:
    """Return the freshest merge source ref supported by this git runtime."""
    if not branch:
        return ResolvedMergeSourceRef(None)

    resolve_fresh = getattr(git, "resolve_fresh_merge_source", None)
    if callable(resolve_fresh):
        resolved = resolve_fresh(branch)
        if isinstance(resolved, ResolvedMergeSourceRef):
            return resolved
        if isinstance(resolved, tuple) and len(resolved) == 2:
            return ResolvedMergeSourceRef(resolved[0], resolved[1])
        if isinstance(resolved, str):
            return ResolvedMergeSourceRef(resolved)
        if resolved is None:
            return ResolvedMergeSourceRef(None)

    resolve_fresh_ref = getattr(git, "resolve_fresh_merge_source_ref", None)
    if callable(resolve_fresh_ref):
        return ResolvedMergeSourceRef(resolve_fresh_ref(branch))

    ref_exists = getattr(git, "ref_exists", None)
    if callable(ref_exists):
        remote_ref = f"origin/{branch}"
        if ref_exists(remote_ref):
            return ResolvedMergeSourceRef(remote_ref)

    branch_exists = getattr(git, "branch_exists", None)
    if callable(branch_exists):
        if branch_exists(branch):
            return ResolvedMergeSourceRef(branch)
        return ResolvedMergeSourceRef(None)

    return ResolvedMergeSourceRef(branch)


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
        raise GitError(
            f"isolated watch checkout is still registered at '{checkout_path}' after cleanup"
        )


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

    workspace_git = Git(checkout_path)
    workspace_git.checkout_detached(target_branch)
    workspace_git.reset_hard(target_branch)
    workspace_git.clean_force()

    current_branch = workspace_git.current_branch()
    if current_branch != "HEAD":
        raise GitError(
            f"isolated watch checkout expected detached HEAD at '{target_branch}', found '{current_branch}'"
        )
    entry = _find_worktree_entry_for_path(git, checkout_path)
    if entry is None:
        raise GitError(f"isolated watch checkout is not registered at '{checkout_path}'")
    if not entry.get("detached"):
        raise GitError("isolated watch checkout must remain detached from shared branch refs")
    if entry.get("branch") == f"refs/heads/{target_branch}":
        raise GitError(
            f"isolated watch checkout must not directly check out shared branch '{target_branch}'"
        )
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
) -> None:
    """Advance the real target-branch ref to the detached isolated merge result.

    Successful watch-time merges are staged in a detached integration checkout,
    but they only count as merged once the shared target branch itself points at
    the detached merge commit. If a real checkout currently has ``target_branch``
    attached, it is hard-reset to the new tip so that checkout stays clean.
    """
    target_ref = f"refs/heads/{target_branch}"
    previous_target_oid = repo_git.rev_parse(target_ref)
    merged_head_oid = merge_git.rev_parse("HEAD")
    attached_target_checkout = active_worktree_path_for_branch(repo_git, target_branch)
    attached_target_git = Git(attached_target_checkout) if attached_target_checkout is not None else None

    if attached_target_git is not None and attached_target_git.has_changes(include_untracked=False):
        raise GitError(
            f"shared checkout '{attached_target_checkout}' for '{target_branch}' has tracked changes"
        )

    target_ref_updated = False
    try:
        repo_git.update_ref(target_ref, merged_head_oid, previous_target_oid)
        target_ref_updated = True
        if attached_target_git is not None:
            attached_target_git.reset_hard(target_ref)
            if attached_target_git.has_changes(include_untracked=False):
                raise GitError(
                    f"shared checkout '{attached_target_checkout}' for '{target_branch}' remained dirty"
                )
        merge_git.reset_hard(target_ref)
    except GitError as exc:
        if target_ref_updated:
            try:
                repo_git.update_ref(target_ref, previous_target_oid, merged_head_oid)
            except GitError as rollback_error:
                raise GitError(
                    f"failed to advance '{target_branch}' and rollback also failed: {rollback_error}"
                ) from exc
            if attached_target_git is not None:
                try:
                    attached_target_git.reset_hard(target_ref)
                except GitError:
                    pass
        try:
            merge_git.reset_hard(target_ref)
        except GitError:
            pass
        raise GitError(
            f"failed to advance shared branch '{target_branch}' from isolated merge: {exc}"
        ) from exc


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
        print(f"Refresh the local tracking ref with: {_tracking_ref_refresh_command(remote=result.remote, branch=result.branch)}")
        return

    print(
        f"Warning: Squash merge landed, but {result.remote}/{result.branch} "
        f"could not be reconciled: {reason}"
    )
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
        tasks = [t for t in all_unmerged if t.status == 'completed']
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

    if advance_type != 'implement':
        completed_plans = store.get_history(limit=None, status='completed', task_type='plan')
        existing_ids = {t.id for t in tasks}
        for plan_task in completed_plans:
            if plan_task.id in impl_based_on_ids:
                continue
            if plan_task.id in existing_ids:
                continue
            tasks.append(plan_task)

    if advance_type == 'plan':
        tasks = [t for t in tasks if t.task_type == 'plan']
    elif advance_type == 'implement':
        tasks = [t for t in tasks if t.task_type == 'implement']

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
    task = store.get(task_id)
    if task is None:
        return None
    if task.id is None:
        return task
    unit = store.resolve_merge_unit_for_task(task.id)
    if unit is None:
        unit = store.get_or_create_merge_unit_for_task(task)
    if unit is None:
        return task
    representative = store.resolve_merge_unit_representative_task(
        unit,
        preferred_task_id=task.id,
        require_actionable=True,
    )
    if representative is not None:
        return representative
    owner = store.resolve_merge_unit_owner_task(unit)
    return owner or task


def _resolve_merge_subject(
    store: SqliteTaskStore,
    git: Git,
    task_id: str,
    *,
    target_branch: str,
) -> _ResolvedMergeSubject | None:
    trigger_task = store.get(task_id)
    if trigger_task is None:
        return None
    trigger_source = _resolve_fresh_merge_source(git, trigger_task.branch)
    if trigger_task.id is None:
        return _ResolvedMergeSubject(
            trigger_task=trigger_task,
            execution_task=trigger_task,
            merge_subject=trigger_task,
            merge_unit_id=None,
            merge_branch=trigger_task.branch,
            merge_source_ref=trigger_source.ref,
            merge_source_warning=trigger_source.warning,
        )

    unit = store.resolve_merge_unit_for_task(trigger_task.id)
    if unit is None and trigger_task.branch:
        unit = store.get_or_create_merge_unit_for_task(trigger_task)
    if unit is None:
        return _ResolvedMergeSubject(
            trigger_task=trigger_task,
            execution_task=trigger_task,
            merge_subject=trigger_task,
            merge_unit_id=None,
            merge_branch=trigger_task.branch,
            merge_source_ref=trigger_source.ref,
            merge_source_warning=trigger_source.warning,
        )

    merge_subject = store.resolve_merge_unit_owner_task(unit) or trigger_task
    execution_task = store.resolve_merge_unit_representative_task(
        unit,
        preferred_task_id=trigger_task.id,
        require_actionable=True,
    )
    if execution_task is None:
        execution_task = trigger_task if trigger_task.branch == unit.source_branch else merge_subject
    merge_source = _resolve_fresh_merge_source(git, unit.source_branch)
    return _ResolvedMergeSubject(
        trigger_task=trigger_task,
        execution_task=execution_task,
        merge_subject=merge_subject,
        merge_unit_id=unit.id,
        merge_branch=unit.source_branch,
        merge_source_ref=merge_source.ref,
        merge_source_warning=merge_source.warning,
    )


def _merge_single_task(
    task_id: str,
    config: Config,
    store,
    git: Git,
    args: argparse.Namespace,
    current_branch: str,
    *,
    merge_source: str = MERGE_SOURCE_MANUAL,
    quiet_mechanics: bool = False,
) -> _MergeSingleTaskResult:
    """Merge a single task's branch."""
    target_branch = git.default_branch()
    resolved = _resolve_merge_subject(store, git, task_id, target_branch=target_branch)
    if resolved is None:
        print(f"Error: Task {task_id} not found")
        return _MergeSingleTaskResult(rc=1)
    execution_task = resolved.execution_task
    merge_subject = resolved.merge_subject
    assert merge_subject.id is not None
    merge_branch = resolved.merge_branch or execution_task.branch
    merge_source_ref = resolved.merge_source_ref
    merge_unit_id = resolved.merge_unit_id

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

    # Handle --mark-only flag
    if args.mark_only:
        # Check for conflicting flags
        if args.rebase or args.squash or args.delete:
            print("Error: --mark-only cannot be used with --rebase, --squash, or --delete")
            return _MergeSingleTaskResult(rc=1)

        deferred_blockers = _materialize_merge_deferred_blockers(
            store,
            config,
            merge_subject,
            defer_blockers=getattr(args, "defer_blockers", False),
        )
        if deferred_blockers is None:
            return _MergeSingleTaskResult(rc=1)
        created_deferred_blockers, reused_deferred_blockers = deferred_blockers
        for blocker_task in created_deferred_blockers:
            print(f"DEFERRED-BLOCKER {blocker_task.id} created from {merge_subject.id}")
        for blocker_task in reused_deferred_blockers:
            print(f"DEFERRED-BLOCKER {blocker_task.id} reused from {merge_subject.id}")

        if merge_unit_id is not None:
            store.set_merge_unit_state(
                merge_unit_id,
                "merged",
                merged_by_task_id=merge_subject.id,
                merge_source=merge_source,
            )
        else:
            store.set_merge_status(merge_subject.id, "merged")
        if not getattr(args, "no_followups", False):
            created_followups, reused_followups = _materialize_merge_followups(store, config, merge_subject)
            for followup_task in created_followups:
                print(f"FOLLOW {followup_task.id} created from {merge_subject.id}")
            for followup_task in reused_followups:
                print(f"FOLLOW {followup_task.id} reused from {merge_subject.id}")
        print(f"✓ Marked task {merge_subject.id} as merged (branch '{merge_branch}' preserved)")
        return _MergeSingleTaskResult(rc=0)

    # Check if branch already merged
    if git.is_merged(merge_source_ref, current_branch):
        default_branch = git.default_branch()
        if current_branch != default_branch and not git.is_merged(merge_source_ref, default_branch):
            print(
                f"Error: Branch '{merge_source_ref}' is already merged into current branch "
                f"'{current_branch}', but still unmerged from default branch '{default_branch}'"
            )
        else:
            print(f"Error: Branch '{merge_source_ref}' is already merged into {current_branch}")
        return _MergeSingleTaskResult(rc=1)

    # Check for uncommitted changes (untracked files are OK, they won't conflict with merge)
    if git.has_changes(include_untracked=False):
        print("Error: You have uncommitted changes. Please commit or stash them first.")
        return _MergeSingleTaskResult(
            rc=1,
            status="blocked_dirty_checkout",
            block_reason="main checkout has uncommitted changes",
        )

    # Check for conflicting flags
    if args.rebase and args.squash:
        print("Error: Cannot use --rebase and --squash together")
        return _MergeSingleTaskResult(rc=1)

    # Validate --remote flag
    if hasattr(args, 'remote') and args.remote and not args.rebase:
        print("Error: --remote requires --rebase")
        return _MergeSingleTaskResult(rc=1)

    # Validate --resolve flag
    if getattr(args, 'resolve', False) and not args.rebase:
        print("Error: --resolve requires --rebase")
        return _MergeSingleTaskResult(rc=1)

    if not args.rebase and not git.can_merge(merge_source_ref, current_branch):
        print(
            f"Error: Branch '{merge_source_ref}' has conflicts against '{current_branch}' "
            "and cannot be merged cleanly."
        )
        print(f"Run: uv run gza rebase {merge_subject.id} --resolve")
        print(f"Or preview the lifecycle action with: uv run gza advance {merge_subject.id} --dry-run")
        return _MergeSingleTaskResult(rc=1)

    deferred_blockers = _materialize_merge_deferred_blockers(
        store,
        config,
        merge_subject,
        defer_blockers=getattr(args, "defer_blockers", False),
    )
    if deferred_blockers is None:
        return _MergeSingleTaskResult(rc=1)
    created_deferred_blockers, reused_deferred_blockers = deferred_blockers
    for blocker_task in created_deferred_blockers:
        print(f"DEFERRED-BLOCKER {blocker_task.id} created from {merge_subject.id}")
    for blocker_task in reused_deferred_blockers:
        print(f"DEFERRED-BLOCKER {blocker_task.id} reused from {merge_subject.id}")

    # Perform the merge or rebase
    try:
        pending_squash_reconcile: _PendingSquashBranchReconcile | None = None
        if args.rebase:
            # Determine the target branch to rebase onto
            rebase_target = current_branch
            if hasattr(args, 'remote') and args.remote:
                # Fetch from origin first
                print("Fetching from origin...")
                git.fetch("origin")
                print("✓ Fetched from origin")
                rebase_target = f"origin/{current_branch}"

            # For rebase: checkout the task branch, rebase onto target, then fast-forward merge
            print(f"Rebasing '{merge_branch}' onto '{rebase_target}'...")
            git.checkout(merge_branch)
            git.rebase(rebase_target)
            print(f"✓ Successfully rebased {merge_branch}")

            # Switch back and fast-forward merge
            git.checkout(current_branch)
            git.merge(merge_branch, squash=False)
            print(f"✓ Fast-forwarded {current_branch} to {merge_branch}")
        else:
            # Regular merge or squash merge
            if not quiet_mechanics:
                print(f"Merging '{merge_source_ref}' into '{current_branch}'...")

            # For squash merge, create a commit message from the task
            commit_message = None
            if args.squash:
                assert merge_subject.id is not None, "Task ID must be set before squash merge commit"
                commit_message = build_task_commit_message(
                    merge_subject.prompt,
                    task_id=merge_subject.id,
                    task_slug=merge_subject.slug,
                    subject_prefix="Squash merge: ",
                )

            pre_squash_local_oid = None
            pre_squash_remote_oid = None
            if args.squash:
                pre_squash_local_oid = _rev_parse_if_exists_if_supported(git, f"refs/heads/{merge_branch}")
                pre_squash_remote_oid = _rev_parse_if_exists_if_supported(git, f"refs/remotes/origin/{merge_branch}")

            git.merge(merge_source_ref, squash=args.squash, commit_message=commit_message)

            if args.squash:
                squash_oid = _rev_parse_if_supported(git, "HEAD")
                if squash_oid is not None and git.repo_dir == config.project_dir:
                    _print_squash_reconcile_result(
                        _reconcile_squash_merged_branch_with_origin(
                            git,
                            branch=merge_branch,
                            squash_oid=squash_oid,
                            pre_squash_local_oid=pre_squash_local_oid,
                            pre_squash_remote_oid=pre_squash_remote_oid,
                        ),
                        suppress_success=quiet_mechanics,
                    )
                elif squash_oid is not None:
                    pending_squash_reconcile = _PendingSquashBranchReconcile(
                        branch=merge_branch,
                        pre_squash_local_oid=pre_squash_local_oid,
                        pre_squash_remote_oid=pre_squash_remote_oid,
                    )
                if not quiet_mechanics:
                    print(f"✓ Successfully squash merged {merge_source_ref} and created commit")
            else:
                if not quiet_mechanics:
                    print(f"✓ Successfully merged {merge_source_ref}")

        # Delete branch if requested
        if args.delete:
            try:
                git.delete_branch(merge_branch)
                print(f"✓ Deleted branch {merge_branch}")
            except GitError as e:
                print(f"Warning: Could not delete branch: {e}")

        if git.repo_dir == config.project_dir:
            if merge_unit_id is not None:
                store.set_merge_unit_state(
                    merge_unit_id,
                    "merged",
                    merged_by_task_id=merge_subject.id,
                    merge_source=merge_source,
                )
            else:
                store.set_merge_status(merge_subject.id, "merged")
            if not getattr(args, "no_followups", False):
                created_followups, reused_followups = _materialize_merge_followups(store, config, merge_subject)
                for followup_task in created_followups:
                    print(f"FOLLOW {followup_task.id} created from {merge_subject.id}")
                for followup_task in reused_followups:
                    print(f"FOLLOW {followup_task.id} reused from {merge_subject.id}")
        return _MergeSingleTaskResult(rc=0, pending_squash_reconcile=pending_squash_reconcile)

    except GitError as e:
        operation = "rebase" if args.rebase else "merge"

        if args.rebase and getattr(args, 'resolve', False):
            # --resolve: invoke Claude to fix conflicts
            print("Conflicts detected. Invoking provider to resolve...")
            resolve_log = ensure_task_log_path(config, store, execution_task)
            conflicts_resolved = invoke_provider_resolve(
                execution_task,
                merge_branch,
                rebase_target,
                config,
                log_file=resolve_log,
                logger=TaskExecutionLogger(resolve_ops_log_path(config, resolve_log), echo=True),
            )

            if not conflicts_resolved:
                print("Could not resolve conflicts automatically.")
                try:
                    git.rebase_abort()
                    try:
                        git.checkout(current_branch)
                    except GitError:
                        pass
                except GitError as abort_error:
                    print(f"Warning: Could not abort rebase: {abort_error}")
                return _MergeSingleTaskResult(rc=1)

            # Switch back and fast-forward merge
            git.checkout(current_branch)
            git.merge(merge_branch, squash=False)
            print(f"✓ Fast-forwarded {current_branch} to {merge_branch}")

            # Delete branch if requested
            if args.delete:
                try:
                    git.delete_branch(merge_branch)
                    print(f"✓ Deleted branch {merge_branch}")
                except GitError as del_error:
                    print(f"Warning: Could not delete branch: {del_error}")

            if git.repo_dir == config.project_dir:
                if merge_unit_id is not None:
                    store.set_merge_unit_state(
                        merge_unit_id,
                        "merged",
                        merged_by_task_id=merge_subject.id,
                        merge_source=merge_source,
                    )
                else:
                    store.set_merge_status(merge_subject.id, "merged")
                if not getattr(args, "no_followups", False):
                    created_followups, reused_followups = _materialize_merge_followups(store, config, merge_subject)
                    for followup_task in created_followups:
                        print(f"FOLLOW {followup_task.id} created from {merge_subject.id}")
                    for followup_task in reused_followups:
                        print(f"FOLLOW {followup_task.id} reused from {merge_subject.id}")
            return _MergeSingleTaskResult(rc=0)

        print(f"Error during {operation}: {e}")
        print(f"\nAborting {operation} and restoring clean state...")
        try:
            if args.rebase:
                git.rebase_abort()
                # Try to switch back to original branch
                try:
                    git.checkout(current_branch)
                except GitError:
                    pass  # Best effort to return to original branch
                print("✓ Rebase aborted, working directory restored")
            else:
                git.merge_abort()
                print("✓ Merge aborted, working directory restored")
        except GitError as abort_error:
            print(f"Warning: Could not abort {operation}: {abort_error}")
        return _MergeSingleTaskResult(rc=1)


def cmd_merge(args: argparse.Namespace) -> int:
    """Merge task branches into the current branch."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    git = Git(config.project_dir)

    # Get current branch once
    current_branch = git.current_branch()
    default = git.default_branch()
    print(f"On branch {current_branch}")

    # --mark-only is a DB-only escape hatch for users who merge manually;
    # it does not run git operations so the default-branch rule does not apply.
    if getattr(args, 'mark_only', False):
        if current_branch != default:
            print(
                f"Note: --mark-only on non-default branch "
                f"'{current_branch}' (default is '{default}')"
            )
    else:
        if not _require_default_branch(git, current_branch, "merge"):
            return 1

    # Determine the list of task IDs to merge
    task_ids = [resolve_id(config, tid) for tid in args.task_ids]

    use_all = getattr(args, 'all', False)
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
            print(f"⚠ Stopped at task {failed_task_id}. Remaining tasks not processed: {', '.join(str(tid) for tid in remaining)}")
        return 1

    return 0


def _resolve_runtime_skill_dir(project_dir: Path, provider: str) -> tuple[str, Path] | None:
    """Resolve runtime skill directory for a provider."""
    codex_home = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))).expanduser()
    gemini_home = Path(os.environ.get("GEMINI_HOME", str(Path.home() / ".gemini"))).expanduser()
    target_map = {
        "claude": ("claude", project_dir / ".claude" / "skills"),
        "codex": ("codex", codex_home / "skills"),
        "gemini": ("gemini", gemini_home / "skills"),
    }
    return target_map.get(provider)


def ensure_skill(skill_name: str, provider: str, project_dir: Path) -> bool:
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

    runtime = _resolve_runtime_skill_dir(project_dir, provider)
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


def _branch_has_commits(config: Config, branch: str | None) -> bool:
    """Return whether a branch is ahead of the default branch."""
    if not branch:
        return False
    try:
        git = Git(config.project_dir)
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

    runtime = _resolve_runtime_skill_dir(config.project_dir, effective_provider)
    if not runtime:
        task_logger.error(
            f"Error: Provider '{effective_provider}' does not support runtime skills for auto-resolve."
        )
        return False

    target_name, _runtime_dir = runtime
    if not ensure_skill("gza-rebase", effective_provider, config.project_dir):
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

    load_dotenv(config.project_dir)
    provider = get_provider(resolve_config)
    work_dir = worktree_path if worktree_path is not None else config.project_dir

    if worktree_path is not None:
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
        run_result = provider.run(
            resolve_config,
            skill_cmd,
            log_file,
            work_dir,
            ops_log_file=resolve_ops_log_path(config, log_file),
        )
    except Exception as exc:
        task_logger.error(f"Provider resolve failed with exception: {exc}")
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
) -> int:
    """Execute a foreground rebase flow with single-task log/state ownership."""
    git = Git(config.project_dir)
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
                explicit_reason="GIT_ERROR",
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
        git._run("worktree", "add", str(worktree_path), branch)
    except GitError as e:
        logger.error(f"Error setting up worktree: {e}")
        mark_task_failed_from_cause(
            task=rebase_task,
            config=config,
            store=store,
            log_file=log_file,
            branch=branch,
            explicit_reason="GIT_ERROR",
        )
        return 1

    worktree_git = Git(worktree_path)
    rebase_diff_baseline = capture_rebase_diff_baseline(
        worktree_git,
        branch=branch,
        target=rebase_target,
    )

    try:
        logger.command(f"Rebasing '{branch}' onto '{rebase_target}'...")
        resolved_by_provider = False
        try:
            worktree_git.rebase(rebase_target)
        except GitError as e:
            logger.warning(f"Conflicts detected: {e}")
            try:
                worktree_git.rebase_abort()
                logger.phase("Aborted conflicted mechanical rebase before provider fallback.")
            except GitError as abort_error:
                logger.warning(f"Warning: Could not abort rebase cleanly: {abort_error}")

            logger.phase("Invoking provider to resolve via /gza-rebase --auto...")
            resolved = invoke_provider_resolve(
                rebase_task,
                branch,
                rebase_target,
                config,
                log_file=log_file,
                logger=logger,
                worktree_path=worktree_path,
            )
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
                    explicit_reason="TEST_FAILURE",
                )
                print()
                return 1

            resolved_by_provider = True
        try:
            publish_rebased_branch(
                worktree_git,
                branch=branch,
                baseline=rebase_diff_baseline,
                logger=logger,
            )
        except GitError:
            mark_task_failed_from_cause(
                task=rebase_task,
                config=config,
                store=store,
                log_file=log_file,
                branch=branch,
                explicit_reason="GIT_ERROR",
            )
            print()
            return 1
        output_content = (
            f"Resolved conflicts and rebased '{branch}' onto '{rebase_target}'."
            if resolved_by_provider
            else f"Rebased '{branch}' onto '{rebase_target}'."
        )

        has_commits = _branch_has_commits(config, branch)
        head_ref = resolve_ref_if_possible(worktree_git, branch)
        base_ref = resolve_ref_if_possible(worktree_git, rebase_target)
        for warning in (head_ref.warning, base_ref.warning):
            if warning:
                logger.warning(warning)
        comparison = compute_rebase_changed_diff(
            worktree_git,
            baseline=rebase_diff_baseline,
            branch=branch,
            target=rebase_target,
        )
        if comparison.warning:
            logger.warning(comparison.warning)
        store.mark_completed(
            rebase_task,
            branch=branch,
            log_file=log_file_storage,
            output_content=output_content,
            has_commits=has_commits,
            changed_diff=comparison.changed_diff,
            head_sha=head_ref.sha if head_ref.sha is not None else DB_UNSET,
            base_sha=base_ref.sha if base_ref.sha is not None else DB_UNSET,
        )

        target_parent_id = parent_task_id or rebase_task.based_on
        if target_parent_id and comparison.changed_diff:
            store.invalidate_review_state(target_parent_id)
            parent = store.get(target_parent_id)
            if parent and parent.id is not None and _task_merge_unit_state(
                store,
                parent,
                target_branch=rebase_target,
            ) == "merged":
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
                worktree_git,
                target_parent_id,
                target_branch=target_branch,
                include_diff_stats=True,
                remote_target_ref=rebase_target if remote else None,
            )
            for warning in reconciliation.warnings:
                logger.warning(warning)
            if reconciliation.skipped_reason is not None:
                logger.warning(
                    "Skipped parent merge-status reconciliation for "
                    f"{target_parent_id}: {reconciliation.skipped_reason}"
                )
            for error in reconciliation.errors:
                logger.warning(
                    "Parent merge-status reconciliation for "
                    f"{target_parent_id} failed: {error}"
                )

        logger.info(f"Changed Diff: {comparison.detail}")

        if resolved_by_provider:
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
            explicit_reason="GIT_ERROR",
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


def cmd_rebase(args: argparse.Namespace) -> int:
    """Rebase a task's branch onto a target branch."""
    config = Config.load(args.project_dir)
    task_id = resolve_id(config, args.task_id)
    git = Git(config.project_dir)

    current_branch = git.current_branch()
    if not _require_default_branch(
        git,
        current_branch,
        "rebase",
        to_stderr=bool(getattr(args, "background", False)),
    ):
        return 1

    # Handle background mode - create a rebase task and run through the standard runner
    if getattr(args, 'background', False):
        store = get_store(config)
        task = store.get(task_id)
        if not task:
            return phase1_error(args, f"Task {task_id} not found")
        if not task.branch:
            return phase1_error(args, f"Task {task_id} has no branch")
        target = getattr(args, 'onto', None) or git.default_branch()
        if getattr(args, 'remote', False):
            target = f"origin/{target}"
        try:
            permit = launch_permit(config, store)
        except MaxConcurrentTasksError as exc:
            return phase1_error(args, str(exc))
        rebase_task = _create_rebase_task(
            store,
            task_id,
            task.branch,
            target,
            trigger_source="manual",
        )
        prepared_rebase_task = _prepare_task_for_immediate_execution(
            config,
            rebase_task,
            rollback_on_failure=True,
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
        )

    store = get_store(config)

    # Get the task
    task = store.get(task_id)
    if not task:
        print(f"Error: Task {task_id} not found")
        return 1

    # Validate task state
    if task.status not in ("completed", "unmerged", "running"):
        print(f"Error: Task {task.id} is not completed, unmerged, or running (status: {task.status})")
        return 1

    if not task.branch:
        print(f"Error: Task {task.id} has no branch")
        return 1

    # Check if branch exists
    if not git.branch_exists(task.branch):
        print(f"Error: Branch '{task.branch}' does not exist")
        return 1

    print(f"On branch {current_branch}")

    # Determine rebase target: use --onto if provided, else current branch
    rebase_target = getattr(args, 'onto', None) or current_branch

    rebase_task = _create_rebase_task(
        store,
        task_id,
        task.branch,
        rebase_target,
        trigger_source="manual",
    )
    assert rebase_task.id is not None
    rebase_task.branch = task.branch
    store.update(rebase_task)

    return _run_task_backed_rebase(
        config=config,
        store=store,
        rebase_task=rebase_task,
        branch=task.branch,
        target_branch=rebase_target,
        remote=bool(getattr(args, "remote", False)),
        parent_task_id=task.id,
    )


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
    diff_args = args.diff_args if hasattr(args, 'diff_args') and args.diff_args else []

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
                        print(stderr, file=sys.stderr, end='')
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
            print("Completed plan rows can be run directly with 'gza implement <task_id>' or auto-started with 'gza advance'.")
        if any(not _is_directly_implementable_plan(task) for task in pending_tasks):
            print(
                "Use 'gza advance --unimplemented --create' to queue implement tasks "
                "for listed explore rows."
            )
        return 0

    # Create queued implement tasks
    created_count = 0
    for task in pending_tasks:
        assert task.id is not None
        if dry_run:
            print(f"[dry-run] Would create implement task for {task.task_type} {task.id}")
            continue
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
    status: str = "merged"
    block_reason: str | None = None


@dataclass
class _CreateReviewActionResult:
    status: str
    review_task: DbTask | None
    message: str


def _prepare_create_review_action(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    trigger_source: str,
) -> _CreateReviewActionResult:
    """Create or resolve the review task for an advance-style create_review action."""
    try:
        review_task = _create_review_task(store, task, trigger_source=trigger_source)
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
) -> _MergeActionResult:
    """Execute a merge-style advance action and materialize follow-up tasks if needed."""
    created_followups: list[DbTask] = []
    reused_followups: list[DbTask] = []
    execution_git = merge_git or git
    execution_branch = merge_current_branch or current_branch
    resolved_subject = _resolve_merge_subject(store, execution_git, task.id or "", target_branch=target_branch) if task.id else None
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
        )

    if resolved_subject is not None and resolved_subject.merge_source_warning:
        print(f"Error: {resolved_subject.merge_source_warning}")
        return _MergeActionResult(
            rc=1,
            created_followups=created_followups,
            reused_followups=reused_followups,
        )

    if action.get("type") == "merge_with_followups":
        review_task = action.get("review_task")
        followup_findings = action.get("followup_findings")
        if isinstance(review_task, DbTask) and isinstance(followup_findings, tuple):
            created_followups, reused_followups = _create_or_reuse_followup_tasks(
                store,
                review_task=review_task,
                impl_task=merge_subject,
                findings=followup_findings,
                trigger_source="manual",
            )

    assert task.id is not None
    if (
        already_merged_behavior == "mark_merged"
        and resolved_subject is not None
        and resolved_subject.merge_source_ref
        and execution_git.is_merged(resolved_subject.merge_source_ref, execution_branch)
    ):
        if resolved_subject.merge_unit_id is not None:
            store.set_merge_unit_state(
                resolved_subject.merge_unit_id,
                "merged",
                merged_by_task_id=merge_subject.id,
                merge_source=merge_source,
            )
        else:
            store.set_merge_status(merge_subject.id, "merged")
        return _MergeActionResult(
            rc=0,
            created_followups=created_followups,
            reused_followups=reused_followups,
            status="already_merged",
        )

    merge_args = _build_auto_merge_args(
        config,
        execution_git,
        resolved_subject.merge_source_ref if resolved_subject is not None else task.branch,
        target_branch,
    )
    real_pending_squash_reconcile: _PendingSquashBranchReconcile | None = None
    if (
        getattr(merge_args, "squash", False)
        and merge_git is not None
        and merge_git.repo_dir != git.repo_dir
        and resolved_subject is not None
        and resolved_subject.merge_branch
    ):
        real_pending_squash_reconcile = _capture_pre_squash_reconcile_state(
            git,
            branch=resolved_subject.merge_branch,
        )
    merge_result = _coerce_merge_single_task_result(
        _merge_single_task(
            task.id,
            config,
            store,
            execution_git,
            merge_args,
            execution_branch,
            merge_source=merge_source,
            quiet_mechanics=quiet_mechanics,
        )
    )
    rc = merge_result.rc
    if rc == 0 and merge_git is not None and merge_git.repo_dir != git.repo_dir:
        try:
            _promote_isolated_merge_to_target_branch(git, execution_git, target_branch)
            pending = real_pending_squash_reconcile or merge_result.pending_squash_reconcile
            if pending is not None:
                _print_squash_reconcile_result(
                    _reconcile_squash_merged_branch_with_origin(
                        git,
                        branch=pending.branch,
                        squash_oid=git.rev_parse(f"refs/heads/{target_branch}"),
                        pre_squash_local_oid=pending.pre_squash_local_oid,
                        pre_squash_remote_oid=pending.pre_squash_remote_oid,
                        remote=pending.remote,
                    ),
                    suppress_success=quiet_mechanics,
                )
            if resolved_subject is not None and resolved_subject.merge_unit_id is not None:
                store.set_merge_unit_state(
                    resolved_subject.merge_unit_id,
                    "merged",
                    merged_by_task_id=merge_subject.id,
                    merge_source=merge_source,
                )
            else:
                store.set_merge_status(merge_subject.id, "merged")
        except GitError as exc:
            print(f"Error finalizing isolated merge success: {exc}")
            rc = 1
    return _MergeActionResult(
        rc=rc,
        created_followups=created_followups,
        reused_followups=reused_followups,
        status=merge_result.status,
        block_reason=merge_result.block_reason,
    )


def _advance_action_color(action_type: str) -> str:
    """Return a Rich color for an advance action type."""
    ac = _colors.WORK_COLORS
    if action_type in {'merge', 'merge_with_followups'}:
        return ac.merge
    if action_type in (
        'needs_rebase',
        'reconcile_branch_divergence',
        'awaiting_human',
        'needs_discussion',
        'max_cycles_reached',
        'max_improve_attempts',
        'automatic_recovery_disabled',
    ):
        return ac.error
    if action_type in ('skip', 'wait_review', 'wait_improve'):
        return ac.waiting
    return ac.default


def _run_advance_owner_row_read_session(
    store: SqliteTaskStore,
    query_fn: Callable[[], _T],
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
    apply_deferred_lineage_query_reconciliations(store)
    return result


def cmd_advance(args: argparse.Namespace) -> int:
    """Intelligently progress unmerged tasks through their lifecycle."""
    config = Config.load(args.project_dir)
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
    git = Git(config.project_dir)

    dry_run: bool = args.dry_run
    auto: bool = getattr(args, 'auto', False)
    max_tasks: int | None = getattr(args, 'max', None)
    batch_limit: int | None = getattr(args, 'batch', None)
    force: bool = getattr(args, 'force', False)
    task_id: str | None = resolve_id(config, args.task_id) if getattr(args, 'task_id', None) is not None else None
    plans_mode: bool = getattr(args, 'plans', False)
    unimplemented_mode: bool = getattr(args, 'unimplemented', False)
    create_mode: bool = getattr(args, 'create', False)
    no_resume_failed: bool = getattr(args, 'no_resume_failed', False)
    max_resume_attempts_override: int | None = getattr(args, 'max_resume_attempts', None)
    advance_type: str | None = getattr(args, 'advance_type', None)

    # Determine effective max_resume_attempts
    max_resume_attempts = max_resume_attempts_override if max_resume_attempts_override is not None else config.max_resume_attempts

    new_mode: bool = getattr(args, 'new', False)

    max_review_cycles_override: int | None = getattr(args, 'max_review_cycles', None)

    if max_review_cycles_override is not None:
        config.max_review_cycles = max_review_cycles_override

    squash_threshold_override: int | None = getattr(args, 'squash_threshold', None)
    if squash_threshold_override is not None:
        config.merge_squash_threshold = squash_threshold_override

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
        unimplemented_types: tuple[str, ...] = ("plan",) if plans_mode and not unimplemented_mode else ("plan", "explore")
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

    def _print_needs_attention_section(items: list[tuple[DbTask, dict]]) -> None:
        if not items:
            return
        console.print(
            f"\n[{_c_err}]{NEEDS_ATTENTION_LABEL} ({len(items)} task{'s' if len(items) != 1 else ''}):[/{_c_err}]"
        )
        for atask, aaction in items:
            _color = _advance_action_color(aaction["type"])
            console.print(f"  [{_color}]{_format_needs_attention_line(atask, aaction)}[/{_color}]")
            if needs_attention_recommends_fix(aaction):
                console.print(f"  [{_color}]Recommended next step: uv run gza fix {atask.id}[/{_color}]")

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
        main_verify = check_main_integration_verify(
            config,
            store,
            git,
            reason="advance-pre-merge",
        )
        if not main_verify.merges_halted or main_verify.state.task.id is None:
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

    with planning_cache:
        # Determine which tasks to advance
        if task_id is not None:
            task = store.get(task_id)
            if not task:
                return phase1_error(args, f"Task {task_id} not found")
            explicit_task = task
            if task.status == 'failed':
                if no_resume_failed:
                    return phase1_error(args, f"Task {task_id} is not completed (status: {task.status})")
            else:
                if task.status != 'completed':
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
            if task.status != "failed" and target_branch is not None:
                if resolve_task_merge_state_for_target(
                    store=store,
                    task=task,
                    git=git,
                    target_branch=target_branch,
                ) == "merged":
                    print(f"Task {task_id} is already merged")
                    return 0
            def _load_explicit_owner_rows() -> tuple[list[LineageOwnerRow], bool]:
                owner_rows = list(
                    query_lineage_owner_rows(
                        store,
                        LineageOwnerQuery(
                            limit=None,
                            task_types=(advance_type,) if advance_type else None,
                            include_skipped=True,
                            exclude_dropped_from_planning=True,
                            max_recovery_attempts=max_resume_attempts,
                            task_ids=(explicit_task.id,) if explicit_task.id is not None else None,
                        ),
                        config=config,
                        git=git,
                        target_branch=target_branch,
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
                                include_skipped=True,
                                max_recovery_attempts=max_resume_attempts,
                                task_ids=(explicit_task.id,) if explicit_task.id is not None else None,
                            ),
                            config=config,
                            git=git,
                            target_branch=target_branch,
                        )
                        if row.owner_task.status == "dropped"
                    ]
                    if dropped_owner_rows:
                        dropped_owner_lineage = True
                return owner_rows, dropped_owner_lineage

            owner_rows, dropped_owner_lineage = _run_advance_owner_row_read_session(
                store,
                _load_explicit_owner_rows,
            )
            if not owner_rows and task.status != "dropped" and not dropped_owner_lineage:
                planning_task = resolve_recovery_planning_task(store, task) if task.status == "failed" else task
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
                            include_skipped=True,
                            exclude_dropped_from_planning=True,
                            max_recovery_attempts=max_resume_attempts,
                        ),
                        config=config,
                        git=git,
                        target_branch=target_branch,
                    )
                )
                return owner_rows

            owner_rows = _run_advance_owner_row_read_session(store, _load_all_owner_rows)
            if not no_resume_failed:
                list_failed_tasks_for_recovery(store, warnings=failed_task_recovery_warnings, git=git, target_branch=target_branch)
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

        # Use the currently checked-out branch as the target for conflict checks,
        # merge execution, and rebase task creation.
        actual_current_branch = git.current_branch()
        if target_branch is None:
            target_branch = actual_current_branch
        use_iterate_mode = _advance_uses_iterate(config)

        def _worker_args() -> argparse.Namespace:
            return argparse.Namespace(
                no_docker=getattr(args, 'no_docker', False),
                max_turns=None,
                force=force,
            )

        def _build_action_context(*, dry_run_mode: bool) -> AdvanceActionExecutionContext:
            def _create_rebase_from_task(parent_task: DbTask) -> DbTask:
                assert parent_task.id is not None
                assert parent_task.branch is not None
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
                    prompt=_unimplemented_implement_prompt(parent_task),
                    trigger_source="manual",
                )

            def _create_plan_review_from_task(parent_task: DbTask) -> DbTask:
                return _create_plan_review_task(store, parent_task, trigger_source="manual")

            def _create_plan_improve_from_task(parent_task: DbTask, review_task: DbTask) -> DbTask:
                return _create_plan_improve_task(store, parent_task, review_task, trigger_source="manual")

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
                ),
                prepare_create_review=lambda t: _prepare_create_review_action(store, t, trigger_source="manual"),
                create_resume_task=lambda t: _create_resume_task(store, t, trigger_source="manual"),
                create_retry_task=lambda t: _create_retry_task(store, t, trigger_source="manual"),
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
                create_targeted_rebase_task=_create_targeted_rebase_from_task,
                spawn_worker=lambda task_obj, _kind: _spawn_background_worker(
                    _worker_args(), config, task_id=str(task_obj.id), quiet=True, prepared_task=task_obj
                ),
                spawn_resume_worker=lambda task_obj, _kind: _spawn_background_resume_worker(
                    _worker_args(), config, str(task_obj.id), quiet=True, prepared_task=task_obj
                ),
                is_rebase_target_already_merged=lambda t: _resolve_and_persist_post_merge_rebase_state(
                    store,
                    git,
                    t,
                    target_branch,
                    merge_source=_resolve_current_merge_source(git, t.branch) if t.branch else None,
                ).already_merged,
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
                ),
                spawn_iterate_recovery=lambda task_obj, mode, prepared_task: _spawn_background_iterate_worker(
                    argparse.Namespace(
                        no_docker=getattr(args, 'no_docker', False),
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
                ),
                reconcile_diverged_branch=lambda t: _reconcile_diverged_branch_with_origin(config, git, t),
            )

        for row in owner_rows:
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

        for decision in execution_decisions:
            row, task, action = decision.item
            if classify_advance_action(action) != "actionable":
                continue
            if not decision.selected:
                preview_gated_rows.append(
                    (row, task, action, _gated_lifecycle_skip_message(free_worker_slots=decision.free_worker_slots))
                )
                continue
            if (
                main_verify_attention is not None
                and action["type"] in {"merge", "merge_with_followups"}
            ):
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
                        console.print(f"  [{_c_tid}]{display_task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
                        _color = _advance_action_color(action['type'])
                        console.print(f"      [{_color}]→ {action['description']}[/{_color}]")
                    for row, _task, _action, description in preview_gated_rows:
                        display_task = row.owner_task
                        prompt_display = shorten_prompt(display_task.prompt, _prompt_avail(display_task.id))
                        console.print(f"  [{_c_tid}]{display_task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
                        console.print(f"      [{_c_warn}]— {description}[/{_c_warn}]")
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
                    console.print(f"  [{_c_tid}]{display_task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
                    _color = _advance_action_color(action['type'])
                    console.print(f"      [{_color}]→ {action['description']}[/{_color}]")
                for row, _task, _action, description in preview_gated_rows:
                    display_task = row.owner_task
                    prompt_display = shorten_prompt(display_task.prompt, _prompt_avail(display_task.id))
                    console.print(f"  [{_c_tid}]{display_task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
                    console.print(f"      [{_c_warn}]— {description}[/{_c_warn}]")
                print()

        if dry_run:
            for warning in failed_task_recovery_warnings:
                print(f"Warning: {warning}", file=sys.stderr)
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
                    console.print(f"  [{_c_tid}]{display_task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
                    _color = _advance_action_color(action['type'])
                    console.print(f"      [{_color}]→ {action['description']}[/{_color}]")
                    print()
                for row, _task, _action, description in preview_gated_rows:
                    if not skip_rows_printed:
                        print()
                        skip_rows_printed = True
                    display_task = row.owner_task
                    prompt_display = shorten_prompt(display_task.prompt, _prompt_avail(display_task.id))
                    console.print(f"  [{_c_tid}]{display_task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
                    console.print(f"      [{_c_warn}]— {description}[/{_c_warn}]")
                    print()
            if new_mode and batch_limit is not None:
                planned_workers = count_worker_consuming_actions([action for _, _, action, _ in preview_actionable_rows])
                remaining = max(0, effective_start_budget - planned_workers)
                if remaining > 0:
                    pending_tasks = get_runnable_pending_tasks(store, limit=remaining)
                    if pending_tasks:
                        print(f"Would start {len(pending_tasks)} new pending task(s):\n")
                        for pt in pending_tasks:
                            prompt_display = shorten_prompt(pt.prompt, _prompt_avail(pt.id))
                            console.print(f"  [{_c_tid}]{pt.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
                            console.print(f"      [{_c_default}]→ Start new worker[/{_c_default}]")
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
                new_pending_tasks = get_runnable_pending_tasks(store, limit=remaining)
                if new_pending_tasks:
                    print(f"Will start {len(new_pending_tasks)} new pending task(s):\n")
                    for pt in new_pending_tasks:
                        prompt_display = shorten_prompt(pt.prompt, _prompt_avail(pt.id))
                        console.print(f"  [{_c_tid}]{pt.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
                        console.print(f"      [{_c_default}]→ Start new worker[/{_c_default}]")
                        print()

        if not auto and (preview_actionable_rows or new_mode):
            try:
                answer = input("Proceed? [Y/n] ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                print()
                return 0
            if answer not in ('', 'y', 'yes'):
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

    if main_verify_attention is not None:
        _append_attention_once(attention_tasks, *main_verify_attention)

    def _render_worker_action_result(action_task: DbTask, display_task: DbTask, action_type: str, exec_result) -> None:
        nonlocal workers_started, success_count, skip_count, error_count

        if exec_result.attempted_spawn:
            workers_started += 1

        if exec_result.status == "skip":
            console.print(f"      [{_c_warn}]{exec_result.message}[/{_c_warn}]")
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
                console.print(f"      [{_c_ok}]✓ {exec_result.success_message}[/{_c_ok}]")
            err_message = exec_result.error_message or exec_result.message or f"Failed to execute {action_type}"
            console.print(f"      [{_c_err}]✗ {err_message}[/{_c_err}]")
            error_count += 1
            return

        success_message = exec_result.success_message or exec_result.message
        if success_message:
            console.print(f"      [{_c_ok}]✓ {success_message}[/{_c_ok}]")

        if exec_result.worker_started or (exec_result.work_done and not exec_result.worker_consuming):
            success_count += 1
        elif exec_result.worker_consuming:
            error_count += 1

    for decision in execution_decisions:
        row, task, action = decision.item
        assert task.id is not None
        display_task = row.owner_task
        prompt_display = shorten_prompt(display_task.prompt, _prompt_avail(display_task.id))
        action_type = action['type']

        if classify_advance_action(action) != "actionable":
            console.print(f"  [{_c_tid}]{display_task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
            _color = _advance_action_color(action_type)
            console.print(f"      [{_color}]{action['description']}[/{_color}]")
            skip_count += 1
            if classify_advance_action(action) == "needs_attention":
                _append_attention_once(
                    attention_tasks,
                    resolve_subject_task(store, action, row, fallback_task=display_task),
                    action,
                )
            continue

        if not decision.selected:
            console.print(f"  [{_c_tid}]{display_task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
            message = _gated_lifecycle_skip_message(free_worker_slots=decision.free_worker_slots)
            console.print(f"      [{_c_warn}]— {message}[/{_c_warn}]")
            print()
            skip_count += 1
            continue

        console.print(f"  [{_c_tid}]{display_task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
        _color = _advance_action_color(action_type)
        console.print(f"      [{_color}]→ {action['description']}[/{_color}]")

        if action_type in {'merge', 'merge_with_followups'}:
            if merge_halt_attention is not None:
                console.print(
                    f"      [{_c_warn}]SKIP: {merge_halt_attention['description'][6:]}[/{_c_warn}]"
                )
                skip_count += 1
                print()
                continue
            merge_result = _execute_merge_action(
                config,
                store,
                git,
                task,
                action,
                target_branch=target_branch,
                current_branch=actual_current_branch,
                merge_source=MERGE_SOURCE_ADVANCE,
            )
            if merge_result.created_followups:
                created_ids = ", ".join(str(t.id) for t in merge_result.created_followups if t.id is not None)
                console.print(f"      [{_c_ok}]✓ Created follow-up task(s): {created_ids}[/{_c_ok}]")
            if merge_result.reused_followups:
                reused_ids = ", ".join(str(t.id) for t in merge_result.reused_followups if t.id is not None)
                console.print(f"      [{_c_warn}]↺ Reused follow-up task(s): {reused_ids}[/{_c_warn}]")
            rc = merge_result.rc
            if rc == 0:
                console.print(f"      [{_c_ok}]✓ Merged[/{_c_ok}]")
                success_count += 1
                main_verify = check_main_integration_verify(
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
                resolved_subject = (
                    _resolve_merge_subject(store, git, task.id, target_branch=target_branch)
                    if task.id is not None
                    else None
                )
                conflict_ref = resolved_subject.merge_source_ref if resolved_subject is not None else task.branch
                conflict_detected = (
                    conflict_ref is not None and not git.can_merge(conflict_ref, target_branch)
                )
                if conflict_detected:
                    console.print(f"      [{_c_warn}]! Merge had conflicts against '{target_branch}'[/{_c_warn}]")
                    try:
                        # _merge_single_task already attempts merge --abort.
                        # For failed squash merges, MERGE_HEAD may be absent, so
                        # force cleanup as a final fallback.
                        git.reset_hard_head()
                        console.print(f"      [{_c_ok}]✓ Restored clean git state[/{_c_ok}]")
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
                        exec_result.success_message = (
                            f"{exec_result.success_message} (target: {target_branch})"
                        )
                    _render_worker_action_result(task, display_task, action_type, exec_result)
                else:
                    console.print(f"      [{_c_err}]✗ Merge failed[/{_c_err}]")
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
            new_pending_tasks = get_runnable_pending_tasks(store, limit=remaining)
        for pt in new_pending_tasks:
            if workers_started >= effective_start_budget:
                break
            if _advance_uses_iterate(config) and pt.task_type == "implement":
                iterate_args = argparse.Namespace(
                    no_docker=getattr(args, 'no_docker', False),
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
