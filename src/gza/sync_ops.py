"""Shared git/GitHub reconciliation for branch-scoped task state."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal, cast

from .db import SqliteTaskStore, Task
from .git import Git, GitError, parse_diff_numstat
from .github import GitHub, GitHubError, PullRequestDetails

_UNSET = object()

PrLookupSource = Literal["cached", "discovered"]


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

    @property
    def code_tasks(self) -> tuple[Task, ...]:
        return tuple(task for task in self.tasks if task.branch == self.branch and task.has_commits)

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
    diff_files_changed: int | None = None
    diff_lines_added: int | None = None
    diff_lines_removed: int | None = None
    pr_number: int | None = None
    pr_state: str | None = None
    fetch_attempted: bool = False
    fetch_succeeded: bool = False
    reconciled: bool = False
    actions: list[str] = field(default_factory=list)
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


def build_branch_cohorts_for_task_ids(
    store: SqliteTaskStore,
    task_ids: list[str],
) -> tuple[list[BranchCohort], list[BranchSyncResult]]:
    """Expand explicit task IDs into branch cohorts and skip/error rows."""
    branch_names: set[str] = set()
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
        if task.branch in branch_names:
            continue
        branch_names.add(task.branch)
        cohorts.append(BranchCohort(branch=task.branch, tasks=tuple(store.get_tasks_for_branch(task.branch))))

    return cohorts, prelim_results


def build_default_branch_cohorts(
    store: SqliteTaskStore,
    *,
    recent_days: int = 30,
) -> list[BranchCohort]:
    """Build the bounded default sync cohort set."""
    branch_names: set[str] = set()
    cohorts: list[BranchCohort] = []
    for task in store.get_sync_candidates(recent_days=recent_days):
        if not task.branch or task.branch in branch_names:
            continue
        branch_names.add(task.branch)
        cohorts.append(BranchCohort(branch=task.branch, tasks=tuple(store.get_tasks_for_branch(task.branch))))
    return cohorts


def sync_branch_cohorts(
    store: SqliteTaskStore,
    git: Git,
    cohorts: list[BranchCohort],
    *,
    include_git: bool,
    include_pr: bool,
    dry_run: bool = False,
    fetch_remote: bool = True,
) -> tuple[list[BranchSyncResult], bool]:
    """Reconcile branch cohorts against git and optional GitHub state."""
    results: list[BranchSyncResult] = []
    partial_failure = False
    default_branch = git.default_branch()
    remote_default_ref: str | None = None
    fetched_this_run = False

    if fetch_remote:
        for cohort in cohorts:
            results.append(
                BranchSyncResult(
                    branch=cohort.branch,
                    task_ids=tuple(task.id for task in cohort.code_tasks if task.id is not None),
                    fetch_attempted=True,
                )
            )
        try:
            git.fetch("origin")
            fetched_this_run = True
            for result in results:
                result.fetch_succeeded = True
            remote_default_candidate = f"origin/{default_branch}"
            if git.ref_exists(remote_default_candidate):
                remote_default_ref = remote_default_candidate
        except GitError as exc:
            partial_failure = True
            for result in results:
                result.errors.append(f"git fetch origin failed: {exc}")
    else:
        results = [
            BranchSyncResult(
                branch=cohort.branch,
                task_ids=tuple(task.id for task in cohort.code_tasks if task.id is not None),
            )
            for cohort in cohorts
        ]

    gh: GitHub | None = None
    gh_available = False
    if include_pr:
        gh = GitHub()
        gh_available = gh.is_available()
        if not gh_available:
            partial_failure = True
            for result in results:
                result.errors.append("GitHub CLI (gh) is not installed or not authenticated")

    for cohort, result in zip(cohorts, results, strict=True):
        if not cohort.code_tasks:
            result.skipped_reason = "no code-bearing task rows"
            continue
        _sync_single_branch(
            store,
            git,
            cohort,
            result,
            default_branch=default_branch,
            remote_default_ref=remote_default_ref,
            include_git=include_git,
            include_pr=include_pr and gh_available,
            dry_run=dry_run,
            gh=gh,
            fetched_this_run=fetched_this_run,
        )
        partial_failure = partial_failure or not result.ok

    return results, partial_failure


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


def refresh_branch_diff_stats(
    store: SqliteTaskStore,
    git: Git,
    tasks: list[Task],
) -> tuple[list[BranchSyncResult], int]:
    """Refresh diff stats for explicit task/branch selections via the shared path."""
    branch_names: set[str] = set()
    cohorts: list[BranchCohort] = []
    results: list[BranchSyncResult] = []
    for task in tasks:
        task_id = task.id
        if not task.branch:
            results.append(
                BranchSyncResult(
                    branch=f"<no-branch:{task_id}>",
                    task_ids=(task_id,) if task_id else (),
                    skipped_reason="no branch",
                )
            )
            continue
        if not git.branch_exists(task.branch):
            results.append(
                BranchSyncResult(
                    branch=task.branch,
                    task_ids=(task_id,) if task_id else (),
                    skipped_reason="branch no longer exists",
                )
            )
            continue
        if task.branch in branch_names:
            continue
        branch_names.add(task.branch)
        cohorts.append(BranchCohort(branch=task.branch, tasks=tuple(store.get_tasks_for_branch(task.branch))))

    default_branch = git.default_branch()
    for cohort in cohorts:
        result = BranchSyncResult(
            branch=cohort.branch,
            task_ids=tuple(task.id for task in cohort.code_tasks if task.id is not None),
            reconciled=True,
        )
        diff_output = git.get_diff_numstat(f"{default_branch}...{cohort.branch}")
        files_changed, lines_added, lines_removed = parse_diff_numstat(diff_output)
        result.diff_files_changed = files_changed
        result.diff_lines_added = lines_added
        result.diff_lines_removed = lines_removed
        result.actions.append("refreshed diff stats")
        _persist_branch_state(
            store,
            cohort.code_tasks,
            diff_stats=(files_changed, lines_added, lines_removed),
        )
        results.append(result)
    skipped = sum(1 for result in results if result.skipped_reason is not None)
    return results, skipped


def _sync_single_branch(
    store: SqliteTaskStore,
    git: Git,
    cohort: BranchCohort,
    result: BranchSyncResult,
    *,
    default_branch: str,
    remote_default_ref: str | None,
    include_git: bool,
    include_pr: bool,
    dry_run: bool,
    gh: GitHub | None,
    fetched_this_run: bool,
) -> None:
    branch = cohort.branch
    code_tasks = cohort.code_tasks
    representative = cohort.representative_task
    branch_exists = git.branch_exists(branch)
    result.reconciled = include_git or include_pr

    desired_merge_status = code_tasks[0].merge_status
    diff_stats: tuple[int | None, int | None, int | None] | None = None
    remote_merged: bool | None = None
    local_merged: bool | None = None

    if include_git or include_pr:
        if remote_default_ref is not None:
            remote_merged = git.is_merged(branch, into=remote_default_ref)
        if include_git:
            local_merged = git.is_merged(branch, into=default_branch)

    if include_git:
        if remote_merged is True or local_merged is True:
            desired_merge_status = "merged"
            _mark_merged(result)
        elif branch_exists:
            desired_merge_status = "unmerged"
            diff_output = git.get_diff_numstat(f"{default_branch}...{branch}")
            diff_stats = parse_diff_numstat(diff_output)
            result.actions.append("refreshed diff stats")

    pr_lookup_time: datetime | None = None
    resolved_pr: ResolvedBranchPr | None = None
    if include_pr and gh is not None:
        cached_numbers = tuple(
            pr_number
            for pr_number in (
                task.pr_number for task in sorted(code_tasks, key=lambda task: task.created_at or datetime.min.replace(tzinfo=UTC), reverse=True)
            )
            if pr_number is not None
        )
        try:
            resolved_pr = resolve_branch_pr(gh, branch, cached_pr_numbers=cached_numbers, allow_discovery=True)
        except GitHubError as exc:
            result.errors.append(str(exc))
        else:
            pr_lookup_time = datetime.now(UTC)
        if resolved_pr is not None and resolved_pr.details is not None:
            details = resolved_pr.details
            result.pr_number = details.number
            result.pr_state = details.state
            result.actions.append(f"{resolved_pr.source} PR #{details.number} ({details.state})")
            if details.state == "merged" and details.base_ref_name == default_branch:
                desired_merge_status = "merged"
                _mark_merged(result)
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
        elif resolved_pr is not None and resolved_pr.clear_cached_number:
            result.actions.append("cleared stale cached PR")

    result.merge_status = desired_merge_status
    if diff_stats is not None:
        result.diff_files_changed, result.diff_lines_added, result.diff_lines_removed = diff_stats

    if not dry_run:
        persisted_merge_status: str | None | object = _UNSET
        if include_git:
            persisted_merge_status = desired_merge_status
        elif include_pr and desired_merge_status != code_tasks[0].merge_status:
            persisted_merge_status = desired_merge_status
        _persist_branch_state(
            store,
            code_tasks,
            merge_status=persisted_merge_status,
            diff_stats=diff_stats if diff_stats is not None else _UNSET,
            pr_number=(
                resolved_pr.details.number
                if resolved_pr is not None and resolved_pr.details is not None
                else None if resolved_pr is not None and resolved_pr.clear_cached_number else _UNSET
            ),
            pr_state=(
                result.pr_state
                if pr_lookup_time is not None
                else _UNSET
            ),
            pr_last_synced_at=pr_lookup_time if pr_lookup_time is not None else _UNSET,
        )


def _persist_branch_state(
    store: SqliteTaskStore,
    tasks: tuple[Task, ...],
    *,
    merge_status: str | None | object = _UNSET,
    diff_stats: tuple[int | None, int | None, int | None] | object = _UNSET,
    pr_number: int | None | object = _UNSET,
    pr_state: str | None | object = _UNSET,
    pr_last_synced_at: datetime | None | object = _UNSET,
) -> None:
    """Write normalized branch-scoped sync state back to every code-bearing row."""
    for original in tasks:
        task = store.get(original.id) if original.id is not None else None
        if task is None:
            continue
        if merge_status is not _UNSET:
            typed_merge_status = cast("str | None", merge_status)
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
        store.update(task)
