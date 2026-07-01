"""Shared git/GitHub reconciliation for branch-scoped task state."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal, cast

from .db import (
    DB_UNSET,
    MERGE_SOURCE_EXTERNAL,
    MERGE_SOURCE_GITHUB_PR,
    MergeUnit,
    SqliteTaskStore,
    Task,
    task_owns_merge_status,
)
from .git import Git, GitError, parse_diff_numstat, resolve_ref_if_possible
from .github import GitHub, GitHubError, PullRequestDetails, is_github_repo_unsupported_error
from .merge_state import classify_branch_merge_state_for_target, classify_proven_merged_state

_UNSET = object()
DEFAULT_SYNC_CACHE_SECONDS = 300

PrLookupSource = Literal["cached", "discovered"]
SyncProgressCallback = Callable[[str], None]


@dataclass(frozen=True)
class ResolvedBranchPr:
    """Resolved PR metadata for a branch."""

    details: PullRequestDetails | None
    source: PrLookupSource | None
    clear_cached_number: bool = False


@dataclass(frozen=True)
class BranchCohort:
    """All task rows that share one branch."""

    branch: str
    tasks: tuple[Task, ...]
    merge_unit_id: str | None = None
    merge_unit_state: str | None = None
    merge_unit_head_sha: str | None = None

    @property
    def code_tasks(self) -> tuple[Task, ...]:
        return tuple(task for task in self.tasks if task.branch == self.branch and task.has_commits)

    @property
    def merge_status_owner_tasks(self) -> tuple[Task, ...]:
        return tuple(task for task in self.code_tasks if task_owns_merge_status(task))

    @property
    def has_non_owner_merge_status_rows(self) -> bool:
        return any(
            task.merge_status is not None and not task_owns_merge_status(task)
            for task in self.code_tasks
        )

    @property
    def representative_task(self) -> Task:
        ordered = sorted(
            self.tasks,
            key=lambda task: (
                task.created_at or datetime.min.replace(tzinfo=UTC),
                task.id or "",
            ),
        )
        return ordered[0]


@dataclass
class BranchSyncResult:
    """Operator-facing outcome for one synced branch."""

    branch: str
    task_ids: tuple[str, ...]
    skipped_reason: str | None = None
    merge_status: str | None = None
    merge_source: str | None = None
    diff_files_changed: int | None = None
    diff_lines_added: int | None = None
    diff_lines_removed: int | None = None
    pr_number: int | None = None
    pr_state: str | None = None
    head_sha: str | None = None
    base_sha: str | None = None
    fetch_attempted: bool = False
    fetch_succeeded: bool = False
    reconciled: bool = False
    actions: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def resolve_branch_pr(
    gh: GitHub,
    branch: str,
    *,
    cached_pr_numbers: tuple[int, ...] = (),
    allow_discovery: bool = True,
) -> ResolvedBranchPr:
    """Resolve the most relevant PR for a branch."""
    had_cached = False
    seen_numbers: set[int] = set()
    cached_non_open: PullRequestDetails | None = None
    for pr_number in cached_pr_numbers:
        if pr_number in seen_numbers:
            continue
        seen_numbers.add(pr_number)
        had_cached = True
        try:
            details = gh.get_pr_details(pr_number)
        except GitHubError as exc:
            raise GitHubError(
                f"failed to look up cached PR #{pr_number} for branch '{branch}': {exc}"
            ) from exc
        if details is None:
            continue
        if details.state == "open":
            return ResolvedBranchPr(details=details, source="cached")
        if cached_non_open is None:
            cached_non_open = details

    if allow_discovery:
        try:
            details = gh.discover_pr_by_branch(branch)
        except GitHubError as exc:
            raise GitHubError(f"failed to discover PR for branch '{branch}': {exc}") from exc
        if details is not None:
            if cached_non_open is None:
                return ResolvedBranchPr(details=details, source="discovered")
            if details.state == "open" or details.number != cached_non_open.number:
                return ResolvedBranchPr(details=details, source="discovered")
        if cached_non_open is not None:
            return ResolvedBranchPr(details=cached_non_open, source="cached")
    elif cached_non_open is not None:
        return ResolvedBranchPr(details=cached_non_open, source="cached")

    return ResolvedBranchPr(details=None, source=None, clear_cached_number=had_cached and allow_discovery)


def _mark_merged(result: BranchSyncResult) -> None:
    """Record a merge normalization action once."""
    if "marked merged" not in result.actions:
        result.actions.append("marked merged")


def _merge_status_transition_time(
    previous_merge_status: str | None,
    previous_merged_at: datetime | None,
    merge_status: str | None,
) -> datetime | None:
    """Return the merged_at value for the requested merge-status transition."""
    if merge_status != "merged":
        return None
    if previous_merge_status == "merged" and previous_merged_at is not None:
        return previous_merged_at
    return datetime.now(UTC)


def _emit_progress(progress: SyncProgressCallback | None, message: str) -> None:
    """Emit a sync progress message when a callback is configured."""
    if progress is not None:
        progress(message)


def build_branch_cohorts_for_task_ids(
    store: SqliteTaskStore,
    task_ids: list[str],
) -> tuple[list[BranchCohort], list[BranchSyncResult]]:
    """Expand explicit task IDs into branch cohorts and skip/error rows."""
    seen_keys: set[tuple[str, str]] = set()
    cohorts: list[BranchCohort] = []
    prelim_results: list[BranchSyncResult] = []

    for task_id in task_ids:
        task = store.get(task_id)
        if task is None:
            prelim_results.append(
                BranchSyncResult(
                    branch=f"<missing:{task_id}>",
                    task_ids=(task_id,),
                    errors=[f"Task {task_id} not found"],
                )
            )
            continue
        if not task.branch:
            prelim_results.append(
                BranchSyncResult(
                    branch=f"<no-branch:{task.id}>",
                    task_ids=(task.id,) if task.id else (),
                    skipped_reason="no branch",
                )
            )
            continue
        if not task.has_commits:
            prelim_results.append(
                BranchSyncResult(
                    branch=task.branch,
                    task_ids=(task.id,) if task.id else (),
                    skipped_reason="no commits",
                )
            )
            continue
        unit = (
            store.resolve_merge_unit_for_task(task.id)
            if store.supports_merge_units() and task.id is not None
            else None
        )
        if unit is not None:
            cohort_key = ("unit", unit.id)
            if cohort_key in seen_keys:
                continue
            seen_keys.add(cohort_key)
            cohorts.append(
                BranchCohort(
                    branch=unit.source_branch,
                    tasks=tuple(store.list_tasks_for_merge_unit(unit.id)),
                    merge_unit_id=unit.id,
                    merge_unit_state=unit.state,
                    merge_unit_head_sha=unit.head_sha,
                )
            )
            continue
        cohort_key = ("branch", task.branch)
        if cohort_key in seen_keys:
            continue
        seen_keys.add(cohort_key)
        cohorts.append(BranchCohort(branch=task.branch, tasks=tuple(store.get_tasks_for_branch(task.branch))))

    return cohorts, prelim_results


def build_branch_cohorts_for_tasks(
    store: SqliteTaskStore,
    tasks: list[Task],
) -> list[BranchCohort]:
    """Collapse branch-bearing task rows into one cohort per branch."""
    seen_keys: set[tuple[str, str]] = set()
    cohorts: list[BranchCohort] = []
    for task in tasks:
        if not task.branch:
            continue
        unit = (
            store.resolve_merge_unit_for_task(task.id)
            if store.supports_merge_units() and task.id is not None
            else None
        )
        if unit is not None:
            cohort_key = ("unit", unit.id)
            if cohort_key in seen_keys:
                continue
            seen_keys.add(cohort_key)
            cohorts.append(
                BranchCohort(
                    branch=unit.source_branch,
                    tasks=tuple(store.list_tasks_for_merge_unit(unit.id)),
                    merge_unit_id=unit.id,
                    merge_unit_state=unit.state,
                    merge_unit_head_sha=unit.head_sha,
                )
            )
            continue
        cohort_key = ("branch", task.branch)
        if cohort_key in seen_keys:
            continue
        seen_keys.add(cohort_key)
        cohorts.append(BranchCohort(branch=task.branch, tasks=tuple(store.get_tasks_for_branch(task.branch))))
    return cohorts


def build_task_branch_cohort(
    store: SqliteTaskStore,
    task_id: str,
) -> tuple[BranchCohort | None, BranchSyncResult | None]:
    """Expand one task ID into its branch cohort for task-scoped callers."""
    cohorts, preliminary = build_branch_cohorts_for_task_ids(store, [task_id])
    if preliminary:
        return None, preliminary[0]
    if not cohorts:
        return None, None
    return cohorts[0], None


def build_default_branch_cohorts(
    store: SqliteTaskStore,
    *,
    recent_days: int = 30,
    cooldown_seconds: int = DEFAULT_SYNC_CACHE_SECONDS,
) -> list[BranchCohort]:
    """Build the bounded default sync cohort set."""
    return build_branch_cohorts_for_tasks(
        store,
        store.get_sync_candidates(
            recent_days=recent_days,
            cooldown_seconds=cooldown_seconds,
        ),
    )


def build_unmerged_branch_cohorts(store: SqliteTaskStore) -> list[BranchCohort]:
    """Build the canonical branch cohort set for daily default-branch unmerged reconciliation."""
    return build_branch_cohorts_for_tasks(
        store,
        store.get_canonical_unmerged_candidates(),
    )


def _remote_branch_ref_for_reconcile(
    git: Git,
    branch: str,
    *,
    remote_target_ref: str | None,
) -> str | None:
    """Return a surviving remote feature ref that can prove merge truth."""
    if remote_target_ref is None or not remote_target_ref.startswith("origin/"):
        return None
    remote_branch_ref = f"origin/{branch}"
    if git.ref_exists(remote_branch_ref):
        return remote_branch_ref
    return None


def reconcile_branch_merge_truth(
    git: Git,
    cohorts: list[BranchCohort],
    *,
    target_branch: str,
    include_diff_stats: bool,
    remote_target_ref: str | None = None,
    preserve_recorded_merged: bool = True,
) -> list[BranchSyncResult]:
    """Compute branch-scoped merge truth and optional diff stats without persistence.

    Missing local branches do not imply merged. Reconciliation requires explicit
    target-branch proof from a surviving branch ref, typically the local feature
    branch or a fetched ``origin/<feature>`` ref for canonical default-branch syncs.
    A lagging remote proof target must not revoke an already-recorded merged state
    unless the source branch itself has disappeared and no surviving ref can prove
    merge truth.
    """
    results: list[BranchSyncResult] = []

    for cohort in cohorts:
        result = BranchSyncResult(
            branch=cohort.branch,
            task_ids=tuple(task.id for task in cohort.code_tasks if task.id is not None),
            reconciled=True,
        )
        results.append(result)
        code_tasks = cohort.code_tasks
        if not code_tasks:
            result.skipped_reason = "no code-bearing task rows"
            continue

        owner_tasks = cohort.merge_status_owner_tasks
        source_owner_task = owner_tasks[0] if owner_tasks else code_tasks[0]
        source_has_commits = source_owner_task.has_commits
        desired_merge_status = owner_tasks[0].merge_status if owner_tasks else code_tasks[0].merge_status
        proof_target_ref = remote_target_ref or target_branch
        try:
            local_branch_exists = git.branch_exists(cohort.branch)
            reconcile_ref = cohort.branch if local_branch_exists else _remote_branch_ref_for_reconcile(
                git,
                cohort.branch,
                remote_target_ref=remote_target_ref,
            )
            if reconcile_ref is None:
                target_merged = None
            else:
                target_merged = git.is_merged(reconcile_ref, into=proof_target_ref)
        except GitError as exc:
            result.errors.append(str(exc))
            result.merge_status = desired_merge_status
            continue

        head_resolution = resolve_ref_if_possible(git, reconcile_ref)
        base_resolution = resolve_ref_if_possible(git, remote_target_ref or target_branch)
        result.head_sha = head_resolution.sha
        result.base_sha = base_resolution.sha
        if head_resolution.warning is not None:
            result.warnings.append(head_resolution.warning)
        if base_resolution.warning is not None:
            result.warnings.append(base_resolution.warning)
        if (result.head_sha is None) ^ (result.base_sha is None):
            if result.head_sha is None:
                result.warnings.append(
                    f"degraded merge-unit provenance: could not resolve head SHA for '{cohort.branch}'; "
                    "preserving any stored head_sha"
                )
            else:
                target_ref = remote_target_ref or target_branch
                result.warnings.append(
                    f"degraded merge-unit provenance: could not resolve base SHA for '{target_ref}'; "
                    "preserving any stored base_sha"
                )

        if target_merged is True and reconcile_ref is not None:
            desired_merge_status = classify_proven_merged_state(
                git=git,
                source_ref=reconcile_ref,
                target_branch=proof_target_ref,
                source_has_commits=source_has_commits,
                recorded_head_sha=cohort.merge_unit_head_sha,
                on_warning=result.warnings.append,
            )
            if desired_merge_status == "merged":
                _mark_merged(result)
                result.merge_source = MERGE_SOURCE_EXTERNAL
        elif reconcile_ref is None:
            # No surviving ref: cannot inspect the branch to prove merge truth.
            # Fail-closed: preserve a previously-proven "empty" state rather than
            # overwriting it with "unmerged". Emit a warning so operators know
            # reconciliation was incomplete.
            if cohort.merge_unit_state in {"empty", "redundant"}:
                desired_merge_status = cohort.merge_unit_state
                result.warnings.append(
                    f"branch '{cohort.branch}': ref unavailable; preserved prior "
                    f"'{cohort.merge_unit_state}' state "
                    "(branch previously confirmed to have no unique commits)"
                )
            else:
                desired_merge_status = "unmerged"
        else:
            # Branch is inspectable but not proven merged against the target.
            # Use the F-A1 classifier to detect zero-commit empty branches.
            # Fail-closed to the existing state when commit count is unavailable.
            try:
                classification = classify_branch_merge_state_for_target(
                    git=git,
                    source_branch=reconcile_ref,
                    target_branch=proof_target_ref,
                    persisted_state=cohort.merge_unit_state,
                    merged_proof=False,
                    source_has_commits=source_has_commits,
                    recorded_head_sha=cohort.merge_unit_head_sha,
                    on_warning=result.warnings.append,
                )
                classify_state = classification.state
            except Exception:
                classify_state = "unknown"
            if classify_state in {"empty", "redundant"}:
                desired_merge_status = classify_state
            elif classify_state == "unknown":
                result.warnings.append(
                    f"branch '{cohort.branch}': could not determine unique commit count "
                    f"against '{proof_target_ref}'; preserving existing merge state"
                )
                # desired_merge_status remains as initialized (from task.merge_status) — fail-closed
            else:
                if not preserve_recorded_merged or desired_merge_status != "merged":
                    desired_merge_status = "unmerged"
            if include_diff_stats and desired_merge_status not in {"empty", "redundant"}:
                try:
                    diff_output = git.get_diff_numstat(f"{target_branch}...{reconcile_ref}")
                except GitError as exc:
                    result.errors.append(str(exc))
                else:
                    diff_stats = parse_diff_numstat(diff_output)
                    result.diff_files_changed, result.diff_lines_added, result.diff_lines_removed = diff_stats
                    result.actions.append("refreshed diff stats")

        result.merge_status = desired_merge_status

    return results


@dataclass
class _BranchPersistenceUpdate:
    merge_status: str | None | object = _UNSET
    merge_source: str | None | object = _UNSET
    diff_stats: tuple[int | None, int | None, int | None] | object = _UNSET
    pr_number: int | None | object = _UNSET
    pr_state: str | None | object = _UNSET
    pr_last_synced_at: datetime | None | object = _UNSET
    head_sha: str | None | object = _UNSET
    base_sha: str | None | object = _UNSET


def _cohort_baseline_merge_state(cohort: BranchCohort) -> str | None:
    """Return the persisted merge state that update overlays resolve against."""
    if cohort.merge_unit_state is not None:
        return cohort.merge_unit_state
    owner_tasks = cohort.merge_status_owner_tasks
    if owner_tasks:
        return owner_tasks[0].merge_status
    code_tasks = cohort.code_tasks
    if code_tasks:
        return code_tasks[0].merge_status
    return None


def _resolved_persisted_merge_state(
    merge_status: str | None | object,
    *,
    baseline_state: str | None,
) -> str | None:
    """Resolve an optional merge-status override to the state that will be persisted."""
    if merge_status is _UNSET:
        return baseline_state
    return cast("str | None", merge_status) or "stale"


def _drop_non_merged_provenance(
    update: _BranchPersistenceUpdate,
    *,
    baseline_state: str | None,
) -> _BranchPersistenceUpdate:
    """Keep update objects internally consistent with merged-only provenance."""
    if _resolved_persisted_merge_state(update.merge_status, baseline_state=baseline_state) != "merged":
        update.merge_source = _UNSET
    return update


def _is_readonly_persist_error(exc: Exception) -> bool:
    """Return whether a persist failure should still hard-fail the caller."""
    return isinstance(exc, sqlite3.OperationalError) and "readonly" in str(exc).lower()


def _provenance_persistence_update(
    *,
    head_sha: str | None,
    base_sha: str | None,
) -> _BranchPersistenceUpdate:
    """Persist only resolved provenance fields so degraded refreshes preserve stored SHAs."""
    update = _BranchPersistenceUpdate()
    if head_sha is not None:
        update.head_sha = head_sha
    if base_sha is not None:
        update.base_sha = base_sha
    return update


def _git_reconcile_update(result: BranchSyncResult) -> _BranchPersistenceUpdate:
    """Translate a git-reconcile result into branch-state persistence fields."""
    update = _provenance_persistence_update(
        head_sha=result.head_sha,
        base_sha=result.base_sha,
    )
    if result.merge_status is not None:
        update.merge_status = result.merge_status
    if result.merge_status == "merged" and result.merge_source is not None:
        update.merge_source = result.merge_source
    if "refreshed diff stats" in result.actions:
        update.diff_stats = (
            result.diff_files_changed,
            result.diff_lines_added,
            result.diff_lines_removed,
        )
    return update


def _merge_persistence_update(
    base: _BranchPersistenceUpdate,
    overlay: _BranchPersistenceUpdate,
    *,
    baseline_state: str | None,
) -> _BranchPersistenceUpdate:
    """Overlay non-UNSET branch-state persistence fields."""
    if overlay.merge_status is not _UNSET:
        base.merge_status = overlay.merge_status
    if overlay.merge_source is not _UNSET:
        base.merge_source = overlay.merge_source
    if overlay.diff_stats is not _UNSET:
        base.diff_stats = overlay.diff_stats
    if overlay.pr_number is not _UNSET:
        base.pr_number = overlay.pr_number
    if overlay.pr_state is not _UNSET:
        base.pr_state = overlay.pr_state
    if overlay.pr_last_synced_at is not _UNSET:
        base.pr_last_synced_at = overlay.pr_last_synced_at
    if overlay.head_sha is not _UNSET:
        base.head_sha = overlay.head_sha
    if overlay.base_sha is not _UNSET:
        base.base_sha = overlay.base_sha
    return _drop_non_merged_provenance(base, baseline_state=baseline_state)


def _enrich_branch_pr_state(
    git: Git,
    cohort: BranchCohort,
    result: BranchSyncResult,
    *,
    default_branch: str,
    remote_default_ref: str | None,
    gh: GitHub,
    dry_run: bool,
    fetched_this_run: bool,
) -> _BranchPersistenceUpdate:
    """Layer PR/GitHub reconciliation onto an existing branch result."""
    code_tasks = cohort.code_tasks
    if not code_tasks:
        return _BranchPersistenceUpdate()

    branch = cohort.branch
    representative = cohort.representative_task
    baseline_merge_status = _cohort_baseline_merge_state(cohort)
    desired_merge_status = result.merge_status if result.merge_status is not None else baseline_merge_status
    branch_exists = git.branch_exists(branch)
    remote_merged = (
        git.is_merged(branch, into=remote_default_ref)
        if fetched_this_run and remote_default_ref is not None and branch_exists
        else None
    )
    cached_numbers = tuple(
        pr_number
        for pr_number in (
            task.pr_number
            for task in sorted(
                code_tasks,
                key=lambda task: task.created_at or datetime.min.replace(tzinfo=UTC),
                reverse=True,
            )
        )
        if pr_number is not None
    )
    try:
        resolved_pr = resolve_branch_pr(gh, branch, cached_pr_numbers=cached_numbers, allow_discovery=True)
    except GitHubError as exc:
        if is_github_repo_unsupported_error(exc):
            return _BranchPersistenceUpdate()
        result.errors.append(str(exc))
        return _BranchPersistenceUpdate()

    pr_lookup_time = datetime.now(UTC)
    if resolved_pr.details is not None:
        details = resolved_pr.details
        result.pr_number = details.number
        result.pr_state = details.state
        result.actions.append(f"{resolved_pr.source} PR #{details.number} ({details.state})")
        if details.state == "merged" and details.base_ref_name == default_branch:
            desired_merge_status = "merged"
            _mark_merged(result)
            result.merge_source = MERGE_SOURCE_GITHUB_PR
        if (
            details.state == "open"
            and fetched_this_run
            and remote_default_ref is not None
            and details.base_ref_name == default_branch
            and branch_exists
            and remote_merged is True
        ):
            desired_merge_status = "merged"
            _mark_merged(result)
            result.merge_source = MERGE_SOURCE_GITHUB_PR
            comment_body = (
                f"Closing automatically via `gza sync`: the changes from task {representative.id} "
                f"on branch `{branch}` are already present on `origin/{default_branch}`, "
                "so this PR is stale after a manual or squash merge outside GitHub."
            )
            if dry_run:
                result.actions.append(f"would comment and close PR #{details.number}")
            else:
                try:
                    gh.add_pr_comment(details.number, comment_body)
                except GitHubError as exc:
                    result.errors.append(f"failed to comment on PR #{details.number}: {exc}")
                else:
                    try:
                        gh.close_pr(details.number)
                    except GitHubError as exc:
                        result.errors.append(f"failed to close PR #{details.number}: {exc}")
                    else:
                        result.actions.append(f"closed stale PR #{details.number}")
                        result.pr_state = "closed"
    elif resolved_pr.clear_cached_number:
        result.actions.append("cleared stale cached PR")

    result.reconciled = True
    result.merge_status = desired_merge_status
    if desired_merge_status != "merged":
        result.merge_source = None
    return _drop_non_merged_provenance(
        _BranchPersistenceUpdate(
            merge_status=(
                desired_merge_status
                if desired_merge_status != baseline_merge_status or cohort.has_non_owner_merge_status_rows
                else _UNSET
            ),
            merge_source=(
                result.merge_source
                if desired_merge_status == "merged" and result.merge_source is not None
                else _UNSET
            ),
            pr_number=(
                resolved_pr.details.number
                if resolved_pr.details is not None
                else None if resolved_pr.clear_cached_number else _UNSET
            ),
            pr_state=result.pr_state,
            pr_last_synced_at=pr_lookup_time,
        ),
        baseline_state=baseline_merge_status,
    )


def _persist_branch_updates(
    store: SqliteTaskStore,
    cohorts: list[BranchCohort],
    results: list[BranchSyncResult],
    updates: list[_BranchPersistenceUpdate],
    target_branch: str,
    *,
    sync_completed_at: datetime | None = None,
) -> None:
    """Write combined branch-state updates once per error-free cohort."""
    for cohort, result, update in zip(cohorts, results, updates, strict=True):
        if not result.ok:
            continue
        if result.skipped_reason is not None:
            continue
        if not cohort.code_tasks:
            continue
        try:
            _persist_branch_state(
                store,
                cohort.code_tasks,
                target_branch,
                merge_unit_id=cohort.merge_unit_id,
                merge_status=update.merge_status,
                merge_source=update.merge_source,
                diff_stats=update.diff_stats,
                pr_number=update.pr_number,
                pr_state=update.pr_state,
                pr_last_synced_at=update.pr_last_synced_at,
                head_sha=update.head_sha,
                base_sha=update.base_sha,
                sync_last_synced_at=(
                    sync_completed_at if sync_completed_at is not None else _UNSET
                ),
            )
        except Exception as exc:
            if _is_readonly_persist_error(exc):
                raise
            merge_unit_detail = f" (merge unit {cohort.merge_unit_id})" if cohort.merge_unit_id is not None else ""
            result.errors.append(
                f"failed to persist sync state for branch '{cohort.branch}'{merge_unit_detail}: {exc}"
            )


def _resolve_persist_merge_unit(
    store: SqliteTaskStore,
    tasks: tuple[Task, ...],
    *,
    merge_unit_id: str | None,
    allow_create: bool,
) -> tuple[MergeUnit | None, bool]:
    """Resolve the merge unit that would receive canonical branch-state updates."""
    had_existing_unit = False
    unit = store.get_merge_unit(merge_unit_id) if merge_unit_id is not None else None
    if unit is not None:
        had_existing_unit = True
    if unit is None:
        owner_task = next((task for task in tasks if task.id is not None and task_owns_merge_status(task)), None)
        if owner_task is not None:
            owner_task_id = owner_task.id
            assert owner_task_id is not None
            existing_unit = store.resolve_merge_unit_for_task(owner_task_id)
            if existing_unit is not None:
                had_existing_unit = True
                unit = existing_unit
            elif allow_create:
                unit = store.get_or_create_merge_unit_for_task(owner_task)
    return unit, had_existing_unit


def _target_branch_mismatch_result(
    store: SqliteTaskStore,
    cohort: BranchCohort,
    *,
    target_branch: str,
) -> BranchSyncResult | None:
    """Return a skipped result when canonical merge state targets a different branch."""
    if not store.supports_merge_units():
        return None
    unit, had_existing_unit = _resolve_persist_merge_unit(
        store,
        cohort.code_tasks,
        merge_unit_id=cohort.merge_unit_id,
        allow_create=False,
    )
    if unit is None or not had_existing_unit or unit.target_branch == target_branch:
        return None
    return BranchSyncResult(
        branch=cohort.branch,
        task_ids=tuple(task.id for task in cohort.code_tasks if task.id is not None),
        skipped_reason=(
            f"merge unit targets '{unit.target_branch}', not requested target '{target_branch}'"
        ),
    )


def _partition_target_mismatch_cohorts(
    store: SqliteTaskStore,
    cohorts: list[BranchCohort],
    *,
    target_branch: str,
) -> tuple[dict[int, BranchSyncResult], list[int], list[BranchCohort]]:
    """Split cohorts into skipped off-target results and eligible cohorts."""
    results_by_index: dict[int, BranchSyncResult] = {}
    eligible_indices: list[int] = []
    eligible_cohorts: list[BranchCohort] = []
    for idx, cohort in enumerate(cohorts):
        mismatch = _target_branch_mismatch_result(store, cohort, target_branch=target_branch)
        if mismatch is not None:
            results_by_index[idx] = mismatch
            continue
        eligible_indices.append(idx)
        eligible_cohorts.append(cohort)
    return results_by_index, eligible_indices, eligible_cohorts


def reconcile_task_branch_merge_truth(
    store: SqliteTaskStore,
    git: Git,
    task_id: str,
    *,
    target_branch: str,
    include_diff_stats: bool,
    remote_target_ref: str | None = None,
    persist: bool = True,
) -> BranchSyncResult:
    """Task-scoped wrapper that expands to a cohort and reconciles git merge truth."""
    cohort, preliminary = build_task_branch_cohort(store, task_id)
    if preliminary is not None:
        return preliminary
    if cohort is None:
        return BranchSyncResult(branch=f"<missing:{task_id}>", task_ids=(task_id,), skipped_reason="no branch cohort")
    mismatch = _target_branch_mismatch_result(store, cohort, target_branch=target_branch)
    if mismatch is not None:
        return mismatch

    result = reconcile_branch_merge_truth(
        git,
        [cohort],
        target_branch=target_branch,
        include_diff_stats=include_diff_stats,
        remote_target_ref=remote_target_ref,
    )[0]
    if persist and result.skipped_reason is None:
        _persist_branch_updates(store, [cohort], [result], [_git_reconcile_update(result)], target_branch)
    return result


def sync_branch_cohorts(
    store: SqliteTaskStore,
    git: Git,
    cohorts: list[BranchCohort],
    *,
    include_git: bool,
    include_pr: bool,
    include_diff_stats: bool = True,
    pr_integration: bool = True,
    dry_run: bool = False,
    fetch_remote: bool = True,
    allow_cached_remote_target_ref_without_fetch: bool = False,
    progress: SyncProgressCallback | None = None,
) -> tuple[list[BranchSyncResult], bool]:
    """Reconcile branch cohorts against git and optional GitHub state."""
    partial_failure = False
    default_branch = git.default_branch()
    results_by_index, eligible_indices, eligible_cohorts = _partition_target_mismatch_cohorts(
        store,
        cohorts,
        target_branch=default_branch,
    )

    _emit_progress(progress, f"Syncing {len(cohorts)} branch cohort(s)")
    if not eligible_cohorts:
        return [results_by_index[idx] for idx in range(len(cohorts))], False

    remote_default_ref: str | None = None
    remote_default_candidate = f"origin/{default_branch}"
    fetched_this_run = False
    fetch_error: str | None = None

    has_origin_remote = True
    remote_exists = getattr(git, "remote_exists", None)
    if callable(remote_exists):
        remote_present = remote_exists("origin")
        if isinstance(remote_present, bool):
            has_origin_remote = remote_present

    if allow_cached_remote_target_ref_without_fetch and git.ref_exists(remote_default_candidate):
        remote_default_ref = remote_default_candidate

    if fetch_remote and has_origin_remote:
        _emit_progress(progress, "Fetching origin")
        try:
            git.fetch("origin")
            fetched_this_run = True
            _emit_progress(progress, "Fetched origin")
            if git.ref_exists(remote_default_candidate):
                remote_default_ref = remote_default_candidate
        except GitError as exc:
            partial_failure = True
            fetch_error = f"git fetch origin failed: {exc}"
            _emit_progress(progress, f"Fetch failed: {exc}")

    if include_git:
        results = reconcile_branch_merge_truth(
            git,
            eligible_cohorts,
            target_branch=default_branch,
            include_diff_stats=include_diff_stats,
            remote_target_ref=remote_default_ref,
        )
    else:
        results = [
            BranchSyncResult(
                branch=cohort.branch,
                task_ids=tuple(task.id for task in cohort.code_tasks if task.id is not None),
            )
            for cohort in eligible_cohorts
        ]

    for result in results:
        result.fetch_attempted = fetch_remote and has_origin_remote
        result.fetch_succeeded = fetched_this_run
        if fetch_error is not None:
            result.errors.append(fetch_error)

    updates = [
        _git_reconcile_update(result) if include_git else _BranchPersistenceUpdate()
        for result in results
    ]

    gh: GitHub | None = None
    gh_available = False
    if include_pr and pr_integration:
        _emit_progress(progress, "Checking GitHub CLI auth")
        gh = GitHub()
        if gh.cached_pr_support() is False:
            _emit_progress(progress, "GitHub PR integration unavailable for this repo")
        else:
            gh_available = gh.is_available()
            _emit_progress(progress, "GitHub CLI auth OK" if gh_available else "GitHub CLI unavailable")
        if gh.cached_pr_support() is False:
            gh_available = False
        elif not gh_available:
            partial_failure = True
            for result in results:
                result.errors.append("GitHub CLI (gh) is not installed or not authenticated")
    elif include_pr:
        _emit_progress(progress, "PR integration disabled by project config")
        gh_available = False
        gh = None

    total = len(eligible_cohorts)
    for idx, (cohort, result) in enumerate(zip(eligible_cohorts, results, strict=True)):
        _emit_progress(progress, f"[{idx + 1}/{total}] {cohort.branch}")
        if include_pr and gh_available and gh is not None:
            if gh.cached_pr_support() is False:
                gh_available = False
                continue
            if result.skipped_reason is not None:
                continue
            pr_update = _enrich_branch_pr_state(
                git,
                cohort,
                result,
                default_branch=default_branch,
                remote_default_ref=remote_default_ref,
                gh=gh,
                dry_run=dry_run,
                fetched_this_run=fetched_this_run,
            )
            updates[idx] = _merge_persistence_update(
                updates[idx],
                pr_update,
                baseline_state=_cohort_baseline_merge_state(cohort),
            )

    if not dry_run:
        _persist_branch_updates(
            store,
            eligible_cohorts,
            results,
            updates,
            default_branch,
            sync_completed_at=datetime.now(UTC),
        )

    for idx, result in zip(eligible_indices, results, strict=True):
        results_by_index[idx] = result
    ordered_results = [results_by_index[idx] for idx in range(len(cohorts))]

    for result in ordered_results:
        partial_failure = partial_failure or not result.ok

    return ordered_results, partial_failure


def summarize_git_reconcile(results: list[BranchSyncResult]) -> tuple[int, int]:
    """Return merged/refreshed counts for git reconciliation summaries."""
    merged_count = 0
    refreshed_count = 0
    for result in results:
        if result.skipped_reason is not None:
            continue
        if "marked merged" in result.actions:
            merged_count += 1
        if "refreshed diff stats" in result.actions:
            refreshed_count += 1
    return merged_count, refreshed_count


def _persist_branch_state(
    store: SqliteTaskStore,
    tasks: tuple[Task, ...],
    target_branch: str,
    *,
    merge_unit_id: str | None = None,
    merge_status: str | None | object = _UNSET,
    merge_source: str | None | object = _UNSET,
    diff_stats: tuple[int | None, int | None, int | None] | object = _UNSET,
    pr_number: int | None | object = _UNSET,
    pr_state: str | None | object = _UNSET,
    pr_last_synced_at: datetime | None | object = _UNSET,
    head_sha: str | None | object = _UNSET,
    base_sha: str | None | object = _UNSET,
    sync_last_synced_at: datetime | None | object = _UNSET,
) -> None:
    """Write normalized branch-scoped sync state back to each code-bearing row.

    Merge truth is stored only on rows that own merge status; other branch-scoped
    sync fields continue to fan out across the cohort.
    """
    if store.supports_merge_units():
        unit, had_existing_unit = _resolve_persist_merge_unit(
            store,
            tasks,
            merge_unit_id=merge_unit_id,
            allow_create=True,
        )
        if unit is not None:
            if had_existing_unit and unit.target_branch != target_branch:
                return
            if head_sha is not _UNSET or base_sha is not _UNSET:
                store.refresh_merge_unit_head(
                    unit.id,
                    DB_UNSET if head_sha is _UNSET else cast("str | None", head_sha),
                    DB_UNSET if base_sha is _UNSET else cast("str | None", base_sha),
                )
            diff_tuple = (
                cast("tuple[int | None, int | None, int | None]", diff_stats)
                if diff_stats is not _UNSET
                else None
            )
            resolved_state = _resolved_persisted_merge_state(merge_status, baseline_state=unit.state) or "stale"
            resolved_merge_source: str | None | object = merge_source
            if resolved_state != "merged":
                resolved_merge_source = DB_UNSET
            store.set_merge_unit_state(
                unit.id,
                resolved_state,
                merged_by_task_id=DB_UNSET,
                merge_source=cast(
                    Any,
                    cast("str | None", resolved_merge_source) if resolved_merge_source is not _UNSET else DB_UNSET,
                ),
                merged_at=DB_UNSET,
                pr_number=cast(Any, cast("int | None", pr_number) if pr_number is not _UNSET else DB_UNSET),
                pr_state=cast(Any, cast("str | None", pr_state) if pr_state is not _UNSET else DB_UNSET),
                pr_last_synced_at=cast(
                    Any,
                    cast("datetime | None", pr_last_synced_at) if pr_last_synced_at is not _UNSET else DB_UNSET,
                ),
                sync_last_synced_at=cast(
                    Any,
                    cast("datetime | None", sync_last_synced_at)
                    if sync_last_synced_at is not _UNSET
                    else DB_UNSET,
                ),
                diff_stats=diff_tuple,
            )
            return
    for original in tasks:
        task = store.get(original.id) if original.id is not None else None
        if task is None:
            continue
        if merge_status is not _UNSET:
            typed_merge_status = cast("str | None", merge_status) if task_owns_merge_status(task) else None
            previous_merge_status = task.merge_status
            previous_merged_at = task.merged_at
            task.merge_status = typed_merge_status
            task.merged_at = _merge_status_transition_time(
                previous_merge_status,
                previous_merged_at,
                typed_merge_status,
            )
        if diff_stats is not _UNSET:
            files_changed, lines_added, lines_removed = cast(
                "tuple[int | None, int | None, int | None]",
                diff_stats,
            )
            task.diff_files_changed = files_changed
            task.diff_lines_added = lines_added
            task.diff_lines_removed = lines_removed
        if pr_number is not _UNSET:
            task.pr_number = cast("int | None", pr_number)
        if pr_state is not _UNSET:
            task.pr_state = cast("str | None", pr_state)
        if pr_last_synced_at is not _UNSET:
            task.pr_last_synced_at = cast("datetime | None", pr_last_synced_at)
        if sync_last_synced_at is not _UNSET:
            task.sync_last_synced_at = cast("datetime | None", sync_last_synced_at)
        store.update(task)
