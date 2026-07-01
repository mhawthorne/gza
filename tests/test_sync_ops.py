"""Tests for branch-scoped sync operations."""

from datetime import UTC, datetime, timedelta
from contextlib import nullcontext
from unittest.mock import Mock, patch

from gza.db import SqliteTaskStore
from gza.git import GitError
from gza.github import GitHub, GitHubError, PullRequestDetails
from gza.sync_ops import (
    _UNSET,
    BranchCohort,
    BranchSyncResult,
    _BranchPersistenceUpdate,
    _git_reconcile_update,
    _merge_persistence_update,
    _persist_branch_state,
    _persist_branch_updates,
    build_branch_cohorts_for_task_ids,
    build_branch_cohorts_for_tasks,
    build_default_branch_cohorts,
    build_task_branch_cohort,
    build_unmerged_branch_cohorts,
    reconcile_branch_merge_truth,
    reconcile_task_branch_merge_truth,
    revalidate_terminal_no_work_merge_units,
    sync_branch_cohorts,
)


def _completed_branch_task(store: SqliteTaskStore, prompt: str, branch: str):
    task = store.add(prompt, task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = branch
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    return task


def test_build_branch_cohorts_for_task_ids_expands_same_branch_chains(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    parent = _completed_branch_task(store, "Parent task", "feature/shared")
    child = store.add("Improve task", task_type="improve")
    child.status = "completed"
    child.completed_at = datetime.now(UTC)
    child.branch = "feature/shared"
    child.has_commits = True
    child.merge_status = "unmerged"
    child.based_on = parent.id
    child.same_branch = True
    store.update(child)

    cohorts, prelim = build_branch_cohorts_for_task_ids(store, [parent.id])

    assert prelim == []
    assert len(cohorts) == 1
    assert cohorts[0].branch == "feature/shared"
    assert {task.id for task in cohorts[0].tasks} == {parent.id, child.id}


def test_build_unmerged_branch_cohorts_uses_canonical_branch_deduping(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    parent = _completed_branch_task(store, "Parent task", "feature/shared")
    child = store.add("Fix task", task_type="fix", based_on=parent.id)
    child.status = "completed"
    child.completed_at = datetime.now(UTC)
    child.branch = "feature/shared"
    child.has_commits = True
    child.merge_status = "unmerged"
    child.same_branch = True
    store.update(child)

    cohorts = build_unmerged_branch_cohorts(store)

    assert len(cohorts) == 1
    assert cohorts[0].branch == "feature/shared"
    assert {task.id for task in cohorts[0].tasks} == {parent.id, child.id}


def test_build_task_branch_cohort_returns_cohort_for_task_scoped_callers(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/scoped")

    cohort, preliminary = build_task_branch_cohort(store, task.id)

    assert preliminary is None
    assert cohort is not None
    assert cohort.branch == "feature/scoped"
    assert {row.id for row in cohort.tasks} == {task.id}


def test_build_task_branch_cohort_carries_merge_unit_head_sha(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/scoped-head")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    store.refresh_merge_unit_head(unit.id, head_sha="recorded-head-sha")

    cohort, preliminary = build_task_branch_cohort(store, task.id)

    assert preliminary is None
    assert cohort is not None
    assert cohort.merge_unit_id == unit.id
    assert cohort.merge_unit_head_sha == "recorded-head-sha"


def test_reconcile_branch_merge_truth_marks_merged_without_persisting(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/merged")
    cohort = BranchCohort(branch=task.branch, tasks=(task,))

    git = Mock()
    git.branch_exists.return_value = True
    git.is_merged.return_value = True

    results = reconcile_branch_merge_truth(
        git,
        [cohort],
        target_branch="main",
        include_diff_stats=True,
    )

    assert results[0].merge_status == "merged"
    assert "marked merged" in results[0].actions
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "unmerged"


def test_reconcile_branch_merge_truth_marks_proven_merged_zero_ahead_task_branch_redundant(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/already-present")
    cohort = BranchCohort(branch=task.branch, tasks=(task,))

    git = Mock()
    git.branch_exists.return_value = True
    git.is_merged.return_value = True
    git.count_commits_ahead_checked.return_value = 0
    git.is_on_first_parent_history.return_value = True
    git.resolve_refs.side_effect = lambda refs, peel="commit": {
        "feature/already-present": "shared-tree-sha" if peel == "tree" else "old-main-sha",
        "main": "shared-tree-sha" if peel == "tree" else "advanced-main-sha",
    }
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/already-present": "old-main-sha",
        "main": "advanced-main-sha",
    }.get(ref)

    results = reconcile_branch_merge_truth(
        git,
        [cohort],
        target_branch="main",
        include_diff_stats=True,
    )

    assert results[0].merge_status == "redundant"
    assert "marked merged" not in results[0].actions


def test_reconcile_branch_merge_truth_stale_source_missing_multicommit_recorded_head_stays_unmerged(
    tmp_path,
):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/false-redundant")
    cohort = BranchCohort(
        branch=task.branch,
        tasks=(task,),
        merge_unit_id="gza-mu-563",
        merge_unit_state="redundant",
        merge_unit_head_sha="recorded-head-tip-sha",
    )

    git = Mock()
    git.branch_exists.return_value = False
    git.ref_exists.return_value = True
    git.is_merged.return_value = True
    git.count_commits_ahead_checked.return_value = 0
    git.is_on_first_parent_history.return_value = True
    git.is_ancestor.return_value = False
    git.has_non_empty_source_diff_against_target.return_value = False
    # The recorded head covers multiple commits; target only has the tip patch,
    # so the shared helper reports the range as not fully represented.
    git.is_patch_equivalent_commit_present_on_target.return_value = False
    git.resolve_refs.side_effect = lambda refs, peel="commit": {
        "origin/feature/false-redundant": "shared-tree-sha" if peel == "tree" else "base-sha",
        "main": "shared-tree-sha" if peel == "tree" else "target-sha",
        "origin/main": "shared-tree-sha" if peel == "tree" else "target-sha",
    }
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "origin/feature/false-redundant": "base-sha",
        "main": "target-sha",
        "origin/main": "target-sha",
    }.get(ref)

    results = reconcile_branch_merge_truth(
        git,
        [cohort],
        target_branch="main",
        include_diff_stats=True,
        remote_target_ref="origin/main",
    )

    assert results[0].merge_status == "unmerged"
    assert "marked merged" not in results[0].actions


def test_reconcile_branch_merge_truth_stale_source_missing_multicommit_recorded_head_stays_redundant_when_all_patches_are_already_on_target(
    tmp_path,
):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/redundant")
    cohort = BranchCohort(
        branch=task.branch,
        tasks=(task,),
        merge_unit_id="gza-mu-564",
        merge_unit_state="redundant",
        merge_unit_head_sha="recorded-head-tip-sha",
    )

    git = Mock()
    git.branch_exists.return_value = False
    git.ref_exists.return_value = True
    git.is_merged.return_value = True
    git.count_commits_ahead_checked.return_value = 0
    git.is_on_first_parent_history.return_value = True
    git.is_ancestor.return_value = False
    git.has_non_empty_source_diff_against_target.return_value = False
    # The stale source ref points at an ancestor, but every recorded-head patch
    # is already present on target via patch equivalence.
    git.is_patch_equivalent_commit_present_on_target.return_value = True
    git.resolve_refs.side_effect = lambda refs, peel="commit": {
        "origin/feature/redundant": "shared-tree-sha" if peel == "tree" else "base-sha",
        "main": "shared-tree-sha" if peel == "tree" else "target-sha",
        "origin/main": "shared-tree-sha" if peel == "tree" else "target-sha",
    }
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "origin/feature/redundant": "base-sha",
        "main": "target-sha",
        "origin/main": "target-sha",
    }.get(ref)

    results = reconcile_branch_merge_truth(
        git,
        [cohort],
        target_branch="main",
        include_diff_stats=True,
        remote_target_ref="origin/main",
    )

    assert results[0].merge_status == "redundant"
    assert "marked merged" not in results[0].actions


def test_reconcile_branch_merge_truth_marks_redundant_when_side_branch_probe_fails(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/probe-failure")
    cohort = BranchCohort(branch=task.branch, tasks=(task,))

    git = Mock()
    git.branch_exists.return_value = True
    git.is_merged.return_value = True
    git.count_commits_ahead_checked.return_value = 0
    git.is_on_first_parent_history.side_effect = RuntimeError("probe exploded")
    git.resolve_refs.side_effect = lambda refs, peel="commit": {
        "feature/probe-failure": "shared-tree-sha" if peel == "tree" else "branch-tip-sha",
        "main": "shared-tree-sha" if peel == "tree" else "merge-commit-sha",
    }
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/probe-failure": "branch-tip-sha",
        "main": "merge-commit-sha",
    }.get(ref)

    results = reconcile_branch_merge_truth(
        git,
        [cohort],
        target_branch="main",
        include_diff_stats=True,
    )

    assert results[0].merge_status == "redundant"
    assert "marked merged" not in results[0].actions
    assert any("Could not probe first-parent membership" in warning for warning in results[0].warnings)


def test_reconcile_branch_merge_truth_marks_redundant_when_first_parent_probe_missing(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/probe-missing")
    cohort = BranchCohort(branch=task.branch, tasks=(task,))

    class _GitWithoutFirstParentProbe:
        def branch_exists(self, branch: str) -> bool:
            return branch == "feature/probe-missing"

        def is_merged(self, source: str, into: str = "main") -> bool:
            return source == "feature/probe-missing" and into == "main"

        def count_commits_ahead_checked(self, source: str, target: str) -> int:
            assert (source, target) == ("feature/probe-missing", "main")
            return 0

        def rev_parse_if_exists(self, ref: str) -> str | None:
            return {
                "feature/probe-missing": "branch-tip-sha",
                "main": "merge-commit-sha",
            }.get(ref)

        def resolve_refs(self, refs, peel: str = "commit") -> dict[str, str | None]:
            return {
                "feature/probe-missing": "shared-tree-sha" if peel == "tree" else "branch-tip-sha",
                "main": "shared-tree-sha" if peel == "tree" else "merge-commit-sha",
            }

    results = reconcile_branch_merge_truth(
        _GitWithoutFirstParentProbe(),
        [cohort],
        target_branch="main",
        include_diff_stats=True,
    )

    assert results[0].merge_status == "redundant"
    assert "marked merged" not in results[0].actions
    assert any("Could not probe first-parent membership" in warning for warning in results[0].warnings)


def test_reconcile_branch_merge_truth_keeps_zero_ahead_branch_with_live_net_diff_unmerged(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/false-moot")
    cohort = BranchCohort(branch=task.branch, tasks=(task,))

    git = Mock()
    git.branch_exists.return_value = True
    git.is_merged.return_value = False
    git.has_non_empty_source_diff_against_target.return_value = True
    git.count_commits_ahead_checked.return_value = 0
    git.resolve_refs.side_effect = lambda refs, peel="commit": {
        "feature/false-moot": "branch-tree-sha" if peel == "tree" else "branch-tip-sha",
        "main": "target-tree-sha" if peel == "tree" else "target-tip-sha",
    }
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/false-moot": "branch-tip-sha",
        "main": "target-tip-sha",
    }.get(ref)
    git.get_diff_numstat.return_value = "2\t1\tfeature.txt\n"

    results = reconcile_branch_merge_truth(
        git,
        [cohort],
        target_branch="main",
        include_diff_stats=True,
    )

    assert results[0].merge_status == "unmerged"
    assert "marked merged" not in results[0].actions


def test_reconcile_task_branch_merge_truth_skips_no_commit_task_before_merged_persistence(
    tmp_path,
):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/no-commit-probe-failure")
    task.has_commits = False
    store.update(task)

    git = Mock()

    result = reconcile_task_branch_merge_truth(
        store,
        git,
        task.id,
        target_branch="main",
        include_diff_stats=True,
        persist=True,
    )

    assert result.skipped_reason == "no commits"
    assert result.merge_status is None
    assert "marked merged" not in result.actions
    assert store.resolve_merge_unit_for_task(task.id) is None
    git.is_merged.assert_not_called()


def test_reconcile_task_branch_merge_truth_persists_no_ff_side_branch_as_merged(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/no-ff-merged")

    git = Mock()
    git.branch_exists.return_value = True
    git.is_merged.return_value = True
    git.count_commits_ahead_checked.return_value = 0
    git.is_on_first_parent_history.return_value = False
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/no-ff-merged": "branch-tip-sha",
        "main": "merge-commit-sha",
    }.get(ref)

    result = reconcile_task_branch_merge_truth(
        store,
        git,
        task.id,
        target_branch="main",
        include_diff_stats=True,
        persist=True,
    )

    assert result.merge_status == "merged"
    assert "marked merged" in result.actions
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "merged"
    assert refreshed.merged_at is not None
    unit = store.resolve_merge_unit_for_task(task.id)
    assert unit is not None
    assert unit.state == "merged"
    assert unit.merged_at == refreshed.merged_at
    assert unit.merged_by_task_id == task.id
    assert unit.merge_source == "external"


def test_reconcile_branch_merge_truth_missing_local_branch_without_remote_proof_stays_unmerged(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/deleted")
    task.merge_status = "merged"
    store.update(task)
    cohort = BranchCohort(branch=task.branch, tasks=(task,))

    git = Mock()
    git.branch_exists.return_value = False
    git.ref_exists.return_value = False

    results = reconcile_branch_merge_truth(
        git,
        [cohort],
        target_branch="main",
        include_diff_stats=True,
    )

    assert results[0].merge_status == "unmerged"
    assert "marked merged" not in results[0].actions
    git.is_merged.assert_not_called()


def test_reconcile_branch_merge_truth_preserves_recorded_merge_when_remote_target_lags(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/local-merged-remote-lag")
    task.merge_status = "merged"
    task.merged_at = datetime.now(UTC)
    store.update(task)
    cohort = BranchCohort(branch=task.branch, tasks=(task,))

    git = Mock()
    git.branch_exists.return_value = True
    git.is_merged.return_value = False
    git.get_diff_numstat.return_value = "2\t1\tfeature.txt\n"

    results = reconcile_branch_merge_truth(
        git,
        [cohort],
        target_branch="main",
        remote_target_ref="origin/main",
        include_diff_stats=True,
    )

    assert results[0].merge_status == "merged"
    assert "marked merged" not in results[0].actions
    assert results[0].diff_files_changed == 1
    git.is_merged.assert_called_once_with("feature/local-merged-remote-lag", into="origin/main")


def test_reconcile_task_branch_merge_truth_persists_branch_state(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/scoped-sync")

    git = Mock()
    git.branch_exists.return_value = True
    git.is_merged.return_value = False
    git.get_diff_numstat.return_value = "2\t1\tfeature.txt\n"
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/scoped-sync": "head-sync-123",
        "main": "base-sync-456",
    }.get(ref)

    result = reconcile_task_branch_merge_truth(
        store,
        git,
        task.id,
        target_branch="main",
        include_diff_stats=True,
        persist=True,
    )

    assert result.merge_status == "unmerged"
    assert result.diff_files_changed == 1
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.diff_files_changed == 1
    unit = store.resolve_merge_unit_for_task(task.id)
    assert unit is not None
    assert unit.head_sha == "head-sync-123"
    assert unit.base_sha == "base-sync-456"


def test_reconcile_task_branch_merge_truth_marks_merged_and_preserves_unit_projection(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/scoped-merged")

    git = Mock()
    git.branch_exists.return_value = True
    git.is_merged.return_value = True
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/scoped-merged": "head-sync-merged",
        "main": "base-sync-merged",
    }.get(ref)

    result = reconcile_task_branch_merge_truth(
        store,
        git,
        task.id,
        target_branch="main",
        include_diff_stats=True,
        persist=True,
    )

    assert result.merge_status == "merged"
    assert "marked merged" in result.actions
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "merged"
    assert refreshed.merged_at is not None
    unit = store.resolve_merge_unit_for_task(task.id)
    assert unit is not None
    assert unit.state == "merged"
    assert unit.merged_at == refreshed.merged_at
    assert unit.merge_source == "external"
    assert unit.head_sha == "head-sync-merged"
    assert unit.base_sha == "base-sync-merged"


def test_reconcile_task_branch_merge_truth_by_same_branch_improve_marks_owner_unit_merged(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    owner = _completed_branch_task(store, "Owner task", "feature/shared-followup-merged")
    follow_up = store.add("Improve task", task_type="improve", based_on=owner.id, same_branch=True)
    follow_up.status = "completed"
    follow_up.completed_at = datetime.now(UTC)
    follow_up.branch = owner.branch
    follow_up.has_commits = True
    follow_up.merge_status = "unmerged"
    store.update(follow_up)

    git = Mock()
    git.branch_exists.return_value = True
    git.is_merged.return_value = True
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/shared-followup-merged": "head-followup-merged",
        "main": "base-followup-merged",
    }.get(ref)

    result = reconcile_task_branch_merge_truth(
        store,
        git,
        follow_up.id,
        target_branch="main",
        include_diff_stats=True,
        persist=True,
    )

    assert result.merge_status == "merged"
    owner_unit = store.resolve_merge_unit_for_task(owner.id)
    follow_up_unit = store.resolve_merge_unit_for_task(follow_up.id)
    assert owner_unit is not None
    assert follow_up_unit is not None
    assert follow_up_unit.id == owner_unit.id
    assert owner_unit.state == "merged"
    assert owner_unit.merged_by_task_id == owner.id
    refreshed_owner = store.get(owner.id)
    refreshed_follow_up = store.get(follow_up.id)
    assert refreshed_owner is not None
    assert refreshed_follow_up is not None
    assert refreshed_owner.merge_status == "merged"
    assert refreshed_follow_up.merge_status is None


def test_sync_branch_cohorts_records_github_pr_merge_source(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/pr-merged")

    git = Mock()
    git.default_branch.return_value = "main"
    git.branch_exists.return_value = True
    git.is_merged.side_effect = [False]
    git.get_diff_numstat.return_value = "2\t1\tfeature.txt\n"
    git.ref_exists.return_value = False

    gh = Mock(spec=GitHub)
    gh.cached_pr_support.return_value = True
    gh.is_available.return_value = True
    gh.get_pr_details.return_value = None
    gh.discover_pr_by_branch.return_value = PullRequestDetails(
        url="https://example.invalid/pr/17",
        number=17,
        state="merged",
        base_ref_name="main",
    )

    with patch("gza.sync_ops.GitHub", return_value=gh):
        results, partial = sync_branch_cohorts(
            store,
            git,
            build_branch_cohorts_for_tasks(store, [task]),
            include_git=True,
            include_pr=True,
            dry_run=False,
            fetch_remote=False,
        )

    assert partial is False
    assert results[0].merge_source == "github_pr"
    unit = store.resolve_merge_unit_for_task(task.id)
    assert unit is not None
    assert unit.merge_source == "github_pr"


def test_reconcile_task_branch_merge_truth_persists_redundant_for_zero_commit_task_branch(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/scoped-empty")

    git = Mock()
    git.branch_exists.return_value = True
    git.is_merged.return_value = True
    git.count_commits_ahead_checked.return_value = 0
    git.is_on_first_parent_history.return_value = True
    git.resolve_refs.side_effect = lambda refs, peel="commit": {
        "feature/scoped-empty": "shared-tree-sha" if peel == "tree" else "head-sync-empty",
        "main": "shared-tree-sha" if peel == "tree" else "base-sync-empty",
    }
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/scoped-empty": "head-sync-empty",
        "main": "base-sync-empty",
    }.get(ref)

    result = reconcile_task_branch_merge_truth(
        store,
        git,
        task.id,
        target_branch="main",
        include_diff_stats=True,
        persist=True,
    )

    assert result.merge_status == "redundant"
    assert "marked merged" not in result.actions
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status is None
    assert refreshed.merged_at is None
    unit = store.resolve_merge_unit_for_task(task.id)
    assert unit is not None
    assert unit.state == "redundant"
    assert unit.merged_at is None
    assert unit.head_sha == "head-sync-empty"
    assert unit.base_sha == "base-sync-empty"


def test_reconcile_task_branch_merge_truth_uses_remote_target_ref_for_merge_proof_but_persists_canonical_target(
    tmp_path,
):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/scoped-remote-proof")
    assert task.id is not None

    git = Mock()
    git.branch_exists.return_value = True
    git.get_diff_numstat.return_value = "2\t1\tfeature.txt\n"

    def _is_merged(branch, into):
        return into == "origin/main"

    git.is_merged.side_effect = _is_merged
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/scoped-remote-proof": "head-remote-proof",
        "origin/main": "base-origin-proof",
        "main": "base-local-stale",
    }.get(ref)

    result = reconcile_task_branch_merge_truth(
        store,
        git,
        task.id,
        target_branch="main",
        remote_target_ref="origin/main",
        include_diff_stats=True,
        persist=True,
    )

    assert result.merge_status == "merged"
    git.is_merged.assert_called_once_with("feature/scoped-remote-proof", into="origin/main")

    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "merged"

    unit = store.resolve_merge_unit_for_task(task.id)
    assert unit is not None
    assert unit.state == "merged"
    assert unit.target_branch == "main"
    assert unit.base_sha == "base-origin-proof"


def test_reconcile_task_branch_merge_truth_remote_proof_does_not_accept_stale_local_target(
    tmp_path,
):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/scoped-remote-false-positive")
    assert task.id is not None

    git = Mock()
    git.branch_exists.return_value = True
    git.get_diff_numstat.return_value = "2\t1\tfeature.txt\n"

    def _is_merged(branch, into):
        return into == "main"

    git.is_merged.side_effect = _is_merged
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/scoped-remote-false-positive": "head-remote-false-positive",
        "origin/main": "base-origin-proof",
        "main": "base-local-stale",
    }.get(ref)

    result = reconcile_task_branch_merge_truth(
        store,
        git,
        task.id,
        target_branch="main",
        remote_target_ref="origin/main",
        include_diff_stats=True,
        persist=True,
    )

    assert result.merge_status == "unmerged"
    git.is_merged.assert_called_once_with("feature/scoped-remote-false-positive", into="origin/main")

    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "unmerged"

    unit = store.resolve_merge_unit_for_task(task.id)
    assert unit is not None
    assert unit.state == "unmerged"
    assert unit.target_branch == "main"
    assert unit.base_sha == "base-origin-proof"


def test_reconcile_task_branch_merge_truth_persisted_local_proof_does_not_downgrade_merged(
    tmp_path,
):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/local-proof-sticky-merged")
    assert task.id is not None
    task.merge_status = "merged"
    task.merged_at = datetime.now(UTC)
    store.update(task)

    git = Mock()
    git.branch_exists.return_value = True
    git.is_merged.return_value = False
    git.get_diff_numstat.return_value = "2\t1\tfeature.txt\n"
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/local-proof-sticky-merged": "head-local-proof",
        "main": "base-local-proof",
    }.get(ref)

    result = reconcile_task_branch_merge_truth(
        store,
        git,
        task.id,
        target_branch="main",
        include_diff_stats=True,
        persist=True,
    )

    assert result.merge_status == "merged"
    assert "marked merged" not in result.actions
    git.is_merged.assert_called_once_with("feature/local-proof-sticky-merged", into="main")

    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "merged"
    assert refreshed.merged_at is not None

    unit = store.resolve_merge_unit_for_task(task.id)
    assert unit is not None
    assert unit.state == "merged"
    assert unit.merged_at == refreshed.merged_at
    assert unit.head_sha == "head-local-proof"
    assert unit.base_sha == "base-local-proof"


def test_sync_branch_cohorts_normalizes_same_branch_rows(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    parent = _completed_branch_task(store, "Parent task", "feature/shared")
    child = store.add("Improve task", task_type="improve")
    child.status = "completed"
    child.completed_at = datetime.now(UTC)
    child.branch = "feature/shared"
    child.has_commits = True
    child.merge_status = "merged"
    child.merged_at = datetime.now(UTC) - timedelta(days=1)
    child.based_on = parent.id
    child.same_branch = True
    store.update(child)

    git = Mock()
    git.default_branch.return_value = "main"
    git.branch_exists.return_value = True
    git.is_merged.return_value = False
    git.get_diff_numstat.return_value = "2\t1\tfeature.txt\n"

    results, partial = sync_branch_cohorts(
        store,
        git,
        [BranchCohort(branch="feature/shared", tasks=tuple(store.get_tasks_for_branch("feature/shared")))],
        include_git=True,
        include_pr=False,
        dry_run=False,
        fetch_remote=False,
    )

    assert partial is False
    assert results[0].diff_files_changed == 1
    refreshed_parent = store.get(parent.id)
    refreshed_child = store.get(child.id)
    assert refreshed_parent is not None
    assert refreshed_child is not None
    assert refreshed_parent.diff_files_changed == 1
    assert refreshed_child.diff_files_changed == 1
    assert refreshed_parent.merge_status == "unmerged"
    assert refreshed_child.merge_status is None
    assert refreshed_child.merged_at is None
    assert refreshed_parent.sync_last_synced_at is not None
    assert refreshed_child.sync_last_synced_at is not None


def test_sync_branch_cohorts_marks_only_owner_row_merged_for_same_branch_improve(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    parent = _completed_branch_task(store, "Parent task", "feature/shared-merged")
    child = store.add("Improve task", task_type="improve", based_on=parent.id)
    child.status = "completed"
    child.completed_at = datetime.now(UTC)
    child.branch = "feature/shared-merged"
    child.has_commits = True
    child.merge_status = "unmerged"
    child.same_branch = True
    store.update(child)

    git = Mock()
    git.default_branch.return_value = "main"
    git.ref_exists.return_value = True
    git.branch_exists.return_value = True
    git.get_diff_numstat.return_value = ""
    git.is_merged.side_effect = lambda branch, into: into == "origin/main"

    results, partial = sync_branch_cohorts(
        store,
        git,
        [BranchCohort(branch="feature/shared-merged", tasks=tuple(store.get_tasks_for_branch("feature/shared-merged")))],
        include_git=True,
        include_pr=False,
        dry_run=False,
        fetch_remote=True,
    )

    assert partial is False
    assert results[0].merge_status == "merged"
    refreshed_parent = store.get(parent.id)
    refreshed_child = store.get(child.id)
    assert refreshed_parent is not None
    assert refreshed_child is not None
    assert refreshed_parent.merge_status == "merged"
    assert refreshed_parent.merged_at is not None
    assert refreshed_child.merge_status is None
    assert refreshed_child.merged_at is None


def test_sync_branch_cohorts_marks_merged_when_origin_default_ref_proves_remote_merge(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task with remote-only merge", "feature/remote-only-merge")

    git = Mock()
    git.default_branch.return_value = "main"
    git.ref_exists.return_value = True
    git.branch_exists.return_value = True
    git.get_diff_numstat.return_value = "2\t1\tfeature.txt\n"

    def _is_merged(branch, into):
        return into == "origin/main"

    git.is_merged.side_effect = _is_merged

    results, partial = sync_branch_cohorts(
        store,
        git,
        [BranchCohort(branch="feature/remote-only-merge", tasks=(task,))],
        include_git=True,
        include_pr=False,
        dry_run=False,
        fetch_remote=True,
    )

    assert partial is False
    assert results[0].merge_status == "merged"
    assert "marked merged" in results[0].actions
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "merged"


def test_sync_branch_cohorts_does_not_downgrade_merged_when_origin_default_ref_lags(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task with local-only merge", "feature/local-only-merge")
    task.merge_status = "merged"
    task.merged_at = datetime.now(UTC)
    store.update(task)

    git = Mock()
    git.default_branch.return_value = "main"
    git.ref_exists.return_value = True
    git.branch_exists.return_value = True
    git.get_diff_numstat.return_value = "2\t1\tfeature.txt\n"
    git.is_merged.return_value = False

    results, partial = sync_branch_cohorts(
        store,
        git,
        [BranchCohort(branch="feature/local-only-merge", tasks=(task,))],
        include_git=True,
        include_pr=False,
        dry_run=False,
        fetch_remote=True,
    )

    assert partial is False
    assert results[0].merge_status == "merged"
    assert "marked merged" not in results[0].actions
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "merged"
    assert refreshed.merged_at is not None


def test_sync_branch_cohorts_persists_merge_units(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/master-target-sync")

    git = Mock()
    git.default_branch.return_value = "master"
    git.branch_exists.return_value = True
    git.is_merged.return_value = False
    git.get_diff_numstat.return_value = "2\t1\tfeature.txt\n"
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/master-target-sync": "head-master-123",
        "master": "base-master-456",
    }.get(ref)

    results, partial = sync_branch_cohorts(
        store,
        git,
        [BranchCohort(branch=task.branch, tasks=(task,))],
        include_git=True,
        include_pr=False,
        dry_run=False,
        fetch_remote=False,
    )

    assert partial is False
    assert results[0].merge_status == "unmerged"
    assert task.id is not None
    unit = store.resolve_merge_unit_for_task(task.id)
    assert unit is not None
    assert unit.state == "unmerged"
    assert unit.head_sha == "head-master-123"
    assert unit.base_sha == "base-master-456"


def test_reconcile_task_branch_merge_truth_preserves_existing_base_sha_on_partial_resolution(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/partial-provenance")
    assert task.id is not None

    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    store.refresh_merge_unit_head(unit.id, "head-old-123", "base-old-456")

    git = Mock()
    git.branch_exists.return_value = True
    git.is_merged.return_value = False
    git.get_diff_numstat.return_value = "2\t1\tfeature.txt\n"
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/partial-provenance": "head-new-789",
        "main": None,
    }.get(ref)

    result = reconcile_task_branch_merge_truth(
        store,
        git,
        task.id,
        target_branch="main",
        include_diff_stats=True,
        persist=True,
    )

    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.head_sha == "head-new-789"
    assert refreshed_unit.base_sha == "base-old-456"
    assert result.warnings == [
        "degraded merge-unit provenance: could not resolve base SHA for 'main'; preserving any stored base_sha"
    ]


def test_sync_branch_cohorts_skips_persisting_mismatched_target_branch_merge_unit(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/retargeted-default")
    assert task.id is not None

    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    assert unit.target_branch == "main"
    store.set_merge_unit_state(unit.id, "merged")

    before_task = store.get(task.id)
    before_unit = store.get_merge_unit(unit.id)
    assert before_task is not None
    assert before_unit is not None
    assert before_task.merge_status == "merged"
    assert before_unit.state == "merged"

    git = Mock()
    git.default_branch.return_value = "release"
    git.branch_exists.return_value = True
    git.is_merged.return_value = False
    git.get_diff_numstat.return_value = "2\t1\tfeature.txt\n"

    results, partial = sync_branch_cohorts(
        store,
        git,
        [BranchCohort(branch=task.branch, tasks=(task,), merge_unit_id=unit.id)],
        include_git=True,
        include_pr=False,
        dry_run=False,
        fetch_remote=False,
    )

    assert partial is False
    assert results[0].skipped_reason == "merge unit targets 'main', not requested target 'release'"
    assert results[0].reconciled is False
    assert results[0].merge_status is None

    refreshed_task = store.get(task.id)
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_task is not None
    assert refreshed_unit is not None
    assert refreshed_task.merge_status == "merged"
    assert refreshed_task.diff_files_changed is None
    assert refreshed_task.sync_last_synced_at is None
    assert refreshed_unit.state == "merged"
    assert refreshed_unit.diff_files_changed is None
    assert refreshed_unit.sync_last_synced_at is None


def test_sync_branch_cohorts_all_mismatched_targets_skip_without_fetch_or_github(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/retargeted-default")
    assert task.id is not None

    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    assert unit.target_branch == "main"

    git = Mock()
    git.default_branch.return_value = "release"

    with patch("gza.sync_ops.GitHub") as github_cls:
        results, partial = sync_branch_cohorts(
            store,
            git,
            [BranchCohort(branch=task.branch, tasks=(task,), merge_unit_id=unit.id)],
            include_git=True,
            include_pr=True,
            dry_run=False,
            fetch_remote=True,
        )

    assert partial is False
    assert results[0].skipped_reason == "merge unit targets 'main', not requested target 'release'"
    git.fetch.assert_not_called()
    github_cls.assert_not_called()


def test_reconcile_task_branch_merge_truth_skips_mismatched_target_branch_merge_unit(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/scoped-retarget")
    assert task.id is not None

    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    assert unit.target_branch == "main"
    store.set_merge_unit_state(unit.id, "merged")

    git = Mock()

    result = reconcile_task_branch_merge_truth(
        store,
        git,
        task.id,
        target_branch="release",
        include_diff_stats=True,
        persist=True,
    )

    assert result.skipped_reason == "merge unit targets 'main', not requested target 'release'"
    assert result.reconciled is False
    git.branch_exists.assert_not_called()

    refreshed_task = store.get(task.id)
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_task is not None
    assert refreshed_unit is not None


def test_build_default_branch_cohorts_unions_merge_units_and_legacy_branches_without_duplicates(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    unit_task = _completed_branch_task(store, "Unit task", "feature/unit")
    unit = store.get_or_create_merge_unit_for_task(unit_task)
    assert unit is not None

    unit_follow_up = store.add("Fix task", task_type="fix", based_on=unit_task.id)
    unit_follow_up.status = "completed"
    unit_follow_up.completed_at = datetime.now(UTC)
    unit_follow_up.branch = "feature/unit"
    unit_follow_up.has_commits = True
    unit_follow_up.same_branch = True
    store.update(unit_follow_up)
    assert unit_task.id is not None
    assert unit_follow_up.id is not None
    store.get_or_create_merge_unit_for_task(unit_follow_up)

    legacy_task = _completed_branch_task(store, "Legacy task", "feature/legacy")

    cohorts = build_default_branch_cohorts(store, recent_days=30, cooldown_seconds=0)

    assert {(cohort.branch, cohort.merge_unit_id) for cohort in cohorts} == {
        ("feature/unit", unit.id),
        ("feature/legacy", None),
    }
    assert {task.id for cohort in cohorts for task in cohort.tasks} == {
        unit_task.id,
        unit_follow_up.id,
        legacy_task.id,
    }


def test_sync_branch_cohorts_keeps_historical_reused_branch_unit_merged(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    historical = _completed_branch_task(store, "Historical task", "feature/reused")
    assert historical.id is not None
    historical_unit = store.get_or_create_merge_unit_for_task(historical)
    assert historical_unit is not None
    store.set_merge_unit_state(historical_unit.id, "merged")

    unrelated = _completed_branch_task(store, "Unrelated task", "feature/reused")
    assert unrelated.id is not None
    unrelated_unit = store.get_or_create_merge_unit_for_task(unrelated)
    assert unrelated_unit is not None
    assert unrelated_unit.id != historical_unit.id

    cohorts = build_unmerged_branch_cohorts(store)
    assert len(cohorts) == 1
    assert cohorts[0].merge_unit_id == unrelated_unit.id
    assert {task.id for task in cohorts[0].code_tasks} == {unrelated.id}

    git = Mock()
    git.default_branch.return_value = "main"
    git.branch_exists.return_value = True
    git.is_merged.return_value = False
    git.get_diff_numstat.return_value = "2\t1\tfeature.txt\n"

    results, partial = sync_branch_cohorts(
        store,
        git,
        cohorts,
        include_git=True,
        include_pr=False,
        dry_run=False,
        fetch_remote=False,
    )

    assert partial is False
    assert results[0].merge_status == "unmerged"

    refreshed_historical = store.get(historical.id)
    refreshed_unrelated = store.get(unrelated.id)
    refreshed_historical_unit = store.resolve_merge_unit_for_task(historical.id)
    refreshed_unrelated_unit = store.resolve_merge_unit_for_task(unrelated.id)
    assert refreshed_historical is not None
    assert refreshed_unrelated is not None
    assert refreshed_historical_unit is not None
    assert refreshed_unrelated_unit is not None
    assert refreshed_historical_unit.state == "merged"
    assert refreshed_historical.merge_status == "merged"
    assert refreshed_unrelated_unit.state == "unmerged"
    assert refreshed_unrelated.merge_status == "unmerged"
    assert (
        refreshed_unrelated_unit.diff_files_changed,
        refreshed_unrelated_unit.diff_lines_added,
        refreshed_unrelated_unit.diff_lines_removed,
    ) == (1, 2, 1)


def test_sync_branch_cohorts_skips_when_git_default_branch_differs_from_canonical_unit(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/target-specific-sync")
    assert task.id is not None

    main_unit = store.get_or_create_merge_unit_for_task(task)
    assert main_unit is not None
    store.set_merge_unit_state(main_unit.id, "merged", diff_stats=(99, 999, 111))

    git = Mock()
    git.default_branch.return_value = "master"
    git.branch_exists.return_value = True
    git.is_merged.return_value = False
    git.get_diff_numstat.return_value = "2\t1\tfeature.txt\n"

    results, partial = sync_branch_cohorts(
        store,
        git,
        [BranchCohort(branch=task.branch, tasks=(task,))],
        include_git=True,
        include_pr=False,
        dry_run=False,
        fetch_remote=False,
    )

    assert partial is False
    assert results[0].merge_status is None
    assert results[0].skipped_reason == "merge unit targets 'main', not requested target 'master'"

    refreshed_main_unit = store.resolve_merge_unit_for_task(task.id)
    assert refreshed_main_unit is not None
    assert refreshed_main_unit.target_branch == "main"
    assert refreshed_main_unit.state == "merged"
    assert (
        refreshed_main_unit.diff_files_changed,
        refreshed_main_unit.diff_lines_added,
        refreshed_main_unit.diff_lines_removed,
    ) == (99, 999, 111)


def test_sync_branch_cohorts_no_fetch_ignores_cached_origin_default_ref_by_default(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task with stale cached origin ref", "feature/stale-origin-proof")

    git = Mock()
    git.default_branch.return_value = "main"
    git.ref_exists.return_value = True
    git.branch_exists.return_value = True
    git.get_diff_numstat.return_value = "2\t1\tfeature.txt\n"
    git.is_merged.side_effect = lambda branch, into: into == "origin/main"

    results, partial = sync_branch_cohorts(
        store,
        git,
        [BranchCohort(branch="feature/stale-origin-proof", tasks=(task,))],
        include_git=True,
        include_pr=False,
        dry_run=False,
        fetch_remote=False,
    )

    assert partial is False
    assert results[0].merge_status == "unmerged"
    assert "marked merged" not in results[0].actions
    assert not any(
        call.kwargs.get("into") == "origin/main"
        for call in git.is_merged.call_args_list
    )
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "unmerged"


def test_sync_branch_cohorts_missing_local_branch_uses_remote_feature_ref_before_marking_merged(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task with deleted local branch", "feature/remote-survivor")

    git = Mock()
    git.default_branch.return_value = "main"
    git.ref_exists.side_effect = lambda ref: ref in {"origin/main", "origin/feature/remote-survivor"}
    git.branch_exists.return_value = False
    git.get_diff_numstat.return_value = "2\t1\tfeature.txt\n"

    def _is_merged(branch, into):
        return False

    git.is_merged.side_effect = _is_merged

    results, partial = sync_branch_cohorts(
        store,
        git,
        [BranchCohort(branch="feature/remote-survivor", tasks=(task,))],
        include_git=True,
        include_pr=False,
        dry_run=False,
        fetch_remote=True,
    )

    assert partial is False
    assert results[0].merge_status == "unmerged"
    assert "marked merged" not in results[0].actions
    git.is_merged.assert_called_once_with("origin/feature/remote-survivor", into="origin/main")
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "unmerged"


def test_sync_branch_cohorts_skips_persisting_errored_cohorts(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task with fetch failure", "feature/fetch-failure")
    task.diff_files_changed = 99
    task.diff_lines_added = 999
    task.diff_lines_removed = 111
    store.update(task)

    git = Mock()
    git.default_branch.return_value = "main"
    git.remote_exists.return_value = True
    git.fetch.side_effect = GitError("network down")
    git.branch_exists.return_value = True
    git.is_merged.return_value = False
    git.get_diff_numstat.return_value = "2\t1\tfeature.txt\n"

    results, partial = sync_branch_cohorts(
        store,
        git,
        [BranchCohort(branch="feature/fetch-failure", tasks=(task,))],
        include_git=True,
        include_pr=False,
        dry_run=False,
        fetch_remote=True,
    )

    assert partial is True
    assert results[0].errors == ["git fetch origin failed: network down"]
    assert "refreshed diff stats" in results[0].actions
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "unmerged"
    assert refreshed.diff_files_changed == 99
    assert refreshed.diff_lines_added == 999
    assert refreshed.diff_lines_removed == 111


def test_sync_branch_cohorts_missing_local_branch_open_pr_does_not_override_to_merged(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task with open PR", "feature/open-pr-deleted-local")
    task.pr_number = 88
    store.update(task)

    git = Mock()
    git.default_branch.return_value = "main"
    git.branch_exists.return_value = False
    git.ref_exists.return_value = False

    gh = Mock()
    gh.is_available.return_value = True
    gh.get_pr_details.return_value = PullRequestDetails(
        url="https://github.com/o/r/pull/88",
        number=88,
        state="open",
        base_ref_name="main",
    )
    gh.discover_pr_by_branch.return_value = None

    with patch("gza.sync_ops.GitHub", return_value=gh):
        results, partial = sync_branch_cohorts(
            store,
            git,
            [BranchCohort(branch="feature/open-pr-deleted-local", tasks=(task,))],
            include_git=True,
            include_pr=True,
            dry_run=False,
            fetch_remote=False,
        )

    assert partial is False
    assert results[0].merge_status == "unmerged"
    assert results[0].pr_state == "open"
    assert "marked merged" not in results[0].actions
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "unmerged"
    assert refreshed.pr_state == "open"


def test_sync_branch_cohorts_pr_merged_marks_merge_status_without_git_phase(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task with PR", "feature/pr-merged")
    task.pr_number = 12
    store.update(task)

    git = Mock()
    git.default_branch.return_value = "main"

    gh = Mock()
    gh.is_available.return_value = True
    gh.get_pr_details.return_value = PullRequestDetails(
        url="https://github.com/o/r/pull/12",
        number=12,
        state="merged",
        base_ref_name="main",
    )
    gh.discover_pr_by_branch.return_value = None

    with patch("gza.sync_ops.GitHub", return_value=gh):
        results, partial = sync_branch_cohorts(
            store,
            git,
            [BranchCohort(branch="feature/pr-merged", tasks=(task,))],
            include_git=False,
            include_pr=True,
            dry_run=False,
            fetch_remote=False,
        )

    assert partial is False
    assert results[0].pr_state == "merged"
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "merged"
    assert refreshed.pr_state == "merged"


def test_sync_branch_cohorts_pr_only_clears_stale_non_owner_merge_status_when_owner_baseline_matches(
    tmp_path,
):
    store = SqliteTaskStore(tmp_path / "test.db")
    parent = _completed_branch_task(store, "Parent task", "feature/pr-only-normalize")
    parent.merge_status = "merged"
    parent.merged_at = datetime.now(UTC) - timedelta(days=7)
    parent.pr_number = 112
    store.update(parent)

    child = store.add("Improve task", task_type="improve", based_on=parent.id)
    child.status = "completed"
    child.completed_at = datetime.now(UTC)
    child.branch = "feature/pr-only-normalize"
    child.has_commits = True
    child.merge_status = "unmerged"
    child.merged_at = datetime.now(UTC) - timedelta(days=2)
    child.same_branch = True
    store.update(child)

    git = Mock()
    git.default_branch.return_value = "main"
    git.branch_exists.return_value = True
    git.is_merged.return_value = False

    gh = Mock()
    gh.is_available.return_value = True
    gh.get_pr_details.return_value = PullRequestDetails(
        url="https://github.com/o/r/pull/112",
        number=112,
        state="merged",
        base_ref_name="main",
    )
    gh.discover_pr_by_branch.return_value = None

    with patch("gza.sync_ops.GitHub", return_value=gh):
        results, partial = sync_branch_cohorts(
            store,
            git,
            [
                BranchCohort(
                    branch="feature/pr-only-normalize",
                    tasks=tuple(store.get_tasks_for_branch("feature/pr-only-normalize")),
                )
            ],
            include_git=False,
            include_pr=True,
            dry_run=False,
            fetch_remote=False,
        )

    assert partial is False
    assert results[0].merge_status == "merged"
    refreshed_parent = store.get(parent.id)
    refreshed_child = store.get(child.id)
    assert refreshed_parent is not None
    assert refreshed_child is not None
    assert refreshed_parent.merge_status == "merged"
    assert refreshed_child.merge_status is None
    assert refreshed_child.merged_at is None


def test_sync_branch_cohorts_prefers_discovered_open_pr_over_closed_cached_pr(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task with replaced PR", "feature/reused-pr")
    task.pr_number = 12
    store.update(task)

    git = Mock()
    git.default_branch.return_value = "main"
    git.branch_exists.return_value = True
    git.is_merged.return_value = False

    gh = Mock()
    gh.is_available.return_value = True
    gh.get_pr_details.return_value = PullRequestDetails(
        url="https://github.com/o/r/pull/12",
        number=12,
        state="closed",
        base_ref_name="main",
    )
    gh.discover_pr_by_branch.return_value = PullRequestDetails(
        url="https://github.com/o/r/pull/13",
        number=13,
        state="open",
        base_ref_name="main",
    )

    with patch("gza.sync_ops.GitHub", return_value=gh):
        results, partial = sync_branch_cohorts(
            store,
            git,
            [BranchCohort(branch="feature/reused-pr", tasks=(task,))],
            include_git=False,
            include_pr=True,
            dry_run=False,
            fetch_remote=False,
        )

    assert partial is False
    assert results[0].pr_number == 13
    assert results[0].pr_state == "open"
    assert "discovered PR #13 (open)" in results[0].actions
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.pr_number == 13
    assert refreshed.pr_state == "open"


def test_sync_branch_cohorts_closes_stale_open_pr_when_origin_proves_merge(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task with stale PR", "feature/stale-pr")
    task.pr_number = 21
    store.update(task)

    git = Mock()
    git.default_branch.return_value = "main"
    git.ref_exists.return_value = True
    git.branch_exists.return_value = True
    git.get_diff_numstat.return_value = ""

    def _is_merged(branch, into):
        return into == "origin/main"

    git.is_merged.side_effect = _is_merged

    gh = Mock()
    gh.is_available.return_value = True
    gh.get_pr_details.return_value = PullRequestDetails(
        url="https://github.com/o/r/pull/21",
        number=21,
        state="open",
        base_ref_name="main",
    )

    with patch("gza.sync_ops.GitHub", return_value=gh):
        results, partial = sync_branch_cohorts(
            store,
            git,
            [BranchCohort(branch="feature/stale-pr", tasks=(task,))],
            include_git=True,
            include_pr=True,
            dry_run=False,
            fetch_remote=True,
        )

    assert partial is False
    gh.add_pr_comment.assert_called_once()
    gh.close_pr.assert_called_once_with(21)
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "merged"
    assert refreshed.pr_state == "closed"
    assert "closed stale PR #21" in results[0].actions


def test_sync_branch_cohorts_pr_only_closes_stale_open_pr_when_origin_proves_merge(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task with stale PR", "feature/pr-only-close")
    task.pr_number = 31
    store.update(task)

    git = Mock()
    git.default_branch.return_value = "main"
    git.ref_exists.return_value = True
    git.branch_exists.return_value = True

    def _is_merged(branch, into):
        return into == "origin/main"

    git.is_merged.side_effect = _is_merged

    gh = Mock()
    gh.is_available.return_value = True
    gh.get_pr_details.return_value = PullRequestDetails(
        url="https://github.com/o/r/pull/31",
        number=31,
        state="open",
        base_ref_name="main",
    )

    with patch("gza.sync_ops.GitHub", return_value=gh):
        results, partial = sync_branch_cohorts(
            store,
            git,
            [BranchCohort(branch="feature/pr-only-close", tasks=(task,))],
            include_git=False,
            include_pr=True,
            dry_run=False,
            fetch_remote=True,
        )

    assert partial is False
    gh.add_pr_comment.assert_called_once()
    gh.close_pr.assert_called_once_with(31)
    git.get_diff_numstat.assert_not_called()
    assert results[0].diff_files_changed is None
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "merged"
    assert refreshed.pr_state == "closed"


def test_sync_branch_cohorts_pr_only_does_not_refresh_diff_stats(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task with cached diff stats", "feature/pr-only-no-diff")
    task.pr_number = 41
    task.diff_files_changed = 7
    task.diff_lines_added = 20
    task.diff_lines_removed = 4
    store.update(task)

    git = Mock()
    git.default_branch.return_value = "main"
    git.branch_exists.return_value = True
    git.is_merged.return_value = False

    gh = Mock()
    gh.is_available.return_value = True
    gh.get_pr_details.return_value = PullRequestDetails(
        url="https://github.com/o/r/pull/41",
        number=41,
        state="merged",
        base_ref_name="main",
    )
    gh.discover_pr_by_branch.return_value = None

    with patch("gza.sync_ops.GitHub", return_value=gh):
        results, partial = sync_branch_cohorts(
            store,
            git,
            [BranchCohort(branch="feature/pr-only-no-diff", tasks=(task,))],
            include_git=False,
            include_pr=True,
            dry_run=False,
            fetch_remote=False,
        )

    assert partial is False
    git.get_diff_numstat.assert_not_called()
    assert results[0].diff_files_changed is None
    assert results[0].diff_lines_added is None
    assert results[0].diff_lines_removed is None
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.diff_files_changed == 7
    assert refreshed.diff_lines_added == 20
    assert refreshed.diff_lines_removed == 4


def test_sync_branch_cohorts_preserves_cached_pr_state_on_lookup_failure(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task with cached PR", "feature/pr-lookup-failure")
    old_synced_at = datetime.now(UTC) - timedelta(days=2)
    task.pr_number = 41
    task.pr_state = "open"
    task.pr_last_synced_at = old_synced_at
    store.update(task)

    git = Mock()
    git.default_branch.return_value = "main"
    git.branch_exists.return_value = True

    gh = Mock()
    gh.is_available.return_value = True
    gh.get_pr_details.side_effect = GitHubError("gh pr view 41 failed: authentication failed")

    with patch("gza.sync_ops.GitHub", return_value=gh):
        results, partial = sync_branch_cohorts(
            store,
            git,
            [BranchCohort(branch="feature/pr-lookup-failure", tasks=(task,))],
            include_git=False,
            include_pr=True,
            dry_run=False,
            fetch_remote=False,
        )

    assert partial is True
    assert results[0].errors == [
        "failed to look up cached PR #41 for branch 'feature/pr-lookup-failure': gh pr view 41 failed: authentication failed"
    ]
    assert "cleared stale cached PR" not in results[0].actions
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.pr_number == 41
    assert refreshed.pr_state == "open"
    assert refreshed.pr_last_synced_at == old_synced_at


def test_sync_branch_cohorts_treats_repo_unsupported_pr_lookup_as_skip_and_stops_later_lookups(tmp_path):
    GitHub.clear_pr_support_cache()
    try:
        store = SqliteTaskStore(tmp_path / "test.db")
        first = _completed_branch_task(store, "First task", "feature/pr-unsupported-first")
        second = _completed_branch_task(store, "Second task", "feature/pr-unsupported-second")

        git = Mock()
        git.default_branch.return_value = "main"
        git.branch_exists.return_value = True

        unsupported = (
            "gh pr list --head feature/pr-unsupported-first failed: "
            "none of the git remotes configured for this repository point to a known github host"
        )

        gh = Mock(spec=GitHub)
        gh.is_available.return_value = True
        gh.cached_pr_support.side_effect = GitHub.cached_pr_support
        gh.get_pr_details.return_value = None

        def _raise_unsupported(branch: str):
            GitHub._mark_pr_unsupported()
            raise GitHubError(
                unsupported.replace("feature/pr-unsupported-first", branch)
            )

        gh.discover_pr_by_branch.side_effect = _raise_unsupported

        with patch("gza.sync_ops.GitHub", return_value=gh):
            results, partial = sync_branch_cohorts(
                store,
                git,
                [
                    BranchCohort(branch="feature/pr-unsupported-first", tasks=(first,)),
                    BranchCohort(branch="feature/pr-unsupported-second", tasks=(second,)),
                ],
                include_git=False,
                include_pr=True,
                dry_run=False,
                fetch_remote=False,
            )

        assert partial is False
        assert [result.errors for result in results] == [[], []]
        assert all(
            "known github host" not in action.lower()
            for result in results
            for action in (*result.actions, *result.warnings, *result.errors)
        )
        gh.discover_pr_by_branch.assert_called_once_with("feature/pr-unsupported-first")
        assert GitHub.cached_pr_support() is False
    finally:
        GitHub.clear_pr_support_cache()


def test_sync_branch_cohorts_pr_only_does_not_mark_merged_from_local_git_heuristic(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task with local merge heuristic", "feature/pr-only-local-merge")
    task.pr_number = 51
    store.update(task)

    git = Mock()
    git.default_branch.return_value = "main"
    git.branch_exists.return_value = True
    git.is_merged.return_value = True

    gh = Mock()
    gh.is_available.return_value = True
    gh.get_pr_details.return_value = PullRequestDetails(
        url="https://github.com/o/r/pull/51",
        number=51,
        state="open",
        base_ref_name="main",
    )
    gh.discover_pr_by_branch.return_value = None

    with patch("gza.sync_ops.GitHub", return_value=gh):
        results, partial = sync_branch_cohorts(
            store,
            git,
            [BranchCohort(branch="feature/pr-only-local-merge", tasks=(task,))],
            include_git=False,
            include_pr=True,
            dry_run=False,
            fetch_remote=False,
        )

    assert partial is False
    assert results[0].merge_status == "unmerged"
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "unmerged"


def test_sync_branch_cohorts_pr_only_does_not_mark_merged_when_branch_missing_without_pr_proof(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task with missing local branch", "feature/pr-only-missing-branch")
    task.pr_number = 61
    store.update(task)

    git = Mock()
    git.default_branch.return_value = "main"
    git.branch_exists.return_value = False
    git.is_merged.return_value = True

    gh = Mock()
    gh.is_available.return_value = True
    gh.get_pr_details.return_value = PullRequestDetails(
        url="https://github.com/o/r/pull/61",
        number=61,
        state="open",
        base_ref_name="main",
    )
    gh.discover_pr_by_branch.return_value = None

    with patch("gza.sync_ops.GitHub", return_value=gh):
        results, partial = sync_branch_cohorts(
            store,
            git,
            [BranchCohort(branch="feature/pr-only-missing-branch", tasks=(task,))],
            include_git=False,
            include_pr=True,
            dry_run=False,
            fetch_remote=False,
        )

    assert partial is False
    assert results[0].merge_status == "unmerged"
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "unmerged"


def test_sync_branch_cohorts_does_not_close_pr_when_branch_missing_locally(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task with missing branch", "feature/missing-branch")
    task.pr_number = 34
    store.update(task)

    git = Mock()
    git.default_branch.return_value = "main"
    git.ref_exists.return_value = True
    git.branch_exists.return_value = False
    git.is_merged.return_value = True
    git.get_diff_numstat.return_value = ""

    gh = Mock()
    gh.is_available.return_value = True
    gh.get_pr_details.return_value = PullRequestDetails(
        url="https://github.com/o/r/pull/34",
        number=34,
        state="open",
        base_ref_name="main",
    )

    with patch("gza.sync_ops.GitHub", return_value=gh):
        results, partial = sync_branch_cohorts(
            store,
            git,
            [BranchCohort(branch="feature/missing-branch", tasks=(task,))],
            include_git=True,
            include_pr=True,
            dry_run=False,
            fetch_remote=True,
        )

    assert partial is False
    gh.add_pr_comment.assert_not_called()
    gh.close_pr.assert_not_called()
    assert "closed stale PR #34" not in results[0].actions


def test_sync_branch_cohorts_preserves_existing_merged_at_for_already_merged_branch(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Merged task", "feature/already-merged")
    old_merged_at = datetime.now(UTC) - timedelta(days=45)
    task.merge_status = "merged"
    task.merged_at = old_merged_at
    store.update(task)

    git = Mock()
    git.default_branch.return_value = "main"
    git.branch_exists.return_value = True
    git.is_merged.return_value = True

    results, partial = sync_branch_cohorts(
        store,
        git,
        [BranchCohort(branch="feature/already-merged", tasks=(task,))],
        include_git=True,
        include_pr=False,
        dry_run=False,
        fetch_remote=False,
    )

    assert partial is False
    assert "marked merged" in results[0].actions
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merged_at == old_merged_at


def test_sync_branch_cohorts_preserves_existing_merged_by_task_id_on_routine_persistence(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Merged task", "feature/merged-by")
    assert task.id is not None
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "merged", merged_by_task_id=task.id)

    git = Mock()
    git.default_branch.return_value = "main"
    git.branch_exists.return_value = True
    git.is_merged.return_value = True
    git.get_diff_numstat.return_value = "2\t1\tfeature.txt\n"

    results, partial = sync_branch_cohorts(
        store,
        git,
        [BranchCohort(branch="feature/merged-by", tasks=(task,), merge_unit_id=unit.id)],
        include_git=True,
        include_pr=False,
        dry_run=False,
        fetch_remote=False,
    )

    assert partial is False
    assert "marked merged" in results[0].actions
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.merged_by_task_id == task.id


# ---------------------------------------------------------------------------
# F-A3: reconcile_branch_merge_truth empty emission and fail-closed paths
# ---------------------------------------------------------------------------


def test_reconcile_branch_merge_truth_emits_redundant_for_zero_commit_task_branch(tmp_path):
    """Classifier detects task commits with zero unique commits and emits 'redundant'."""
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/zero-commit")
    cohort = BranchCohort(branch=task.branch, tasks=(task,))

    git = Mock()
    git.branch_exists.return_value = True
    git.is_merged.return_value = False
    # Disable origin/ ref preference so classifier uses the local branch ref.
    git.ref_exists.return_value = False
    git.resolve_refs.side_effect = lambda refs, peel="commit": {
        "feature/zero-commit": "shared-tree-sha" if peel == "tree" else "sha-abc123",
        "main": "shared-tree-sha" if peel == "tree" else "sha-def456",
    }
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/zero-commit": "sha-abc123",
        "main": "sha-def456",
    }.get(ref)
    git.count_commits_ahead.return_value = 0

    results = reconcile_branch_merge_truth(
        git,
        [cohort],
        target_branch="main",
        include_diff_stats=False,
    )

    assert results[0].merge_status == "redundant"
    assert "marked merged" not in results[0].actions


def test_reconcile_branch_merge_truth_preserves_state_when_zero_ahead_diff_proof_is_unavailable(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/no-tree-proof")
    cohort = BranchCohort(branch=task.branch, tasks=(task,))

    class _GitWithoutTreeResolveRefs:
        def branch_exists(self, branch: str) -> bool:
            return branch == "feature/no-tree-proof"

        def is_merged(self, source: str, into: str = "main") -> bool:
            return False

        def ref_exists(self, ref: str) -> bool:
            return False

        def rev_parse_if_exists(self, ref: str) -> str | None:
            return {
                "feature/no-tree-proof": "sha-abc123",
                "main": "sha-def456",
            }.get(ref)

        def count_commits_ahead(self, source: str, target: str) -> int:
            assert (source, target) == ("feature/no-tree-proof", "main")
            return 0

    results = reconcile_branch_merge_truth(
        _GitWithoutTreeResolveRefs(),
        [cohort],
        target_branch="main",
        include_diff_stats=False,
    )

    assert results[0].merge_status == "unmerged"
    assert any("diff proof unavailable" in warning for warning in results[0].warnings)
    assert "marked merged" not in results[0].actions


def test_reconcile_branch_merge_truth_preserves_empty_state_when_ref_becomes_unavailable(tmp_path):
    """Previously-proven empty merge unit stays 'empty' even after the branch ref disappears."""
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/was-empty")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "empty")

    # Branch and remote ref are both gone.
    git = Mock()
    git.branch_exists.return_value = False
    git.ref_exists.return_value = False

    cohort = BranchCohort(
        branch=task.branch,
        tasks=(task,),
        merge_unit_id=unit.id,
        merge_unit_state="empty",
    )
    results = reconcile_branch_merge_truth(
        git,
        [cohort],
        target_branch="main",
        include_diff_stats=False,
    )

    assert results[0].merge_status == "empty"
    assert results[0].warnings  # warning about unavailable ref should be present
    assert "empty" in results[0].warnings[0]
    git.is_merged.assert_not_called()


def test_reconcile_branch_merge_truth_preserves_redundant_state_when_ref_becomes_unavailable(tmp_path):
    """Previously-proven redundant merge unit stays 'redundant' when the branch ref disappears."""
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/was-redundant")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "redundant")

    git = Mock()
    git.branch_exists.return_value = False
    git.ref_exists.return_value = False

    cohort = BranchCohort(
        branch=task.branch,
        tasks=(task,),
        merge_unit_id=unit.id,
        merge_unit_state="redundant",
    )
    results = reconcile_branch_merge_truth(
        git,
        [cohort],
        target_branch="main",
        include_diff_stats=False,
    )

    assert results[0].merge_status == "redundant"
    assert results[0].warnings
    assert "redundant" in results[0].warnings[0]
    git.is_merged.assert_not_called()


def test_reconcile_task_branch_merge_truth_persists_preserved_empty_when_ref_unavailable(tmp_path):
    """Preserved 'empty' state is written through persistence so the merge unit stays 'empty'."""
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/persisted-empty")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "empty")

    git = Mock()
    git.branch_exists.return_value = False
    git.ref_exists.return_value = False
    git.rev_parse_if_exists.return_value = None

    result = reconcile_task_branch_merge_truth(
        store,
        git,
        task.id,
        target_branch="main",
        include_diff_stats=False,
        persist=True,
    )

    assert result.merge_status == "empty"
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "empty"
    assert refreshed_unit.merged_at is None


def test_revalidate_terminal_no_work_merge_units_restores_recorded_head_diff_to_unmerged(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/false-redundant-heal")
    assert task.id is not None
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    store.refresh_merge_unit_head(unit.id, head_sha="recorded-head-sha")
    store.set_merge_unit_state(unit.id, "redundant")

    git = Mock()
    git.cached.return_value = nullcontext()
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "recorded-head-sha": "recorded-head-sha",
    }.get(ref)
    git.is_patch_equivalent_commit_present_on_target.return_value = False

    results = revalidate_terminal_no_work_merge_units(store, git)

    assert len(results) == 1
    assert results[0].merge_status == "unmerged"
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    refreshed_task = store.get(task.id)
    assert refreshed_task is not None
    assert refreshed_task.merge_status == "unmerged"


def test_revalidate_terminal_no_work_merge_units_leaves_unresolvable_recorded_head_unchanged(
    tmp_path,
    caplog,
):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/gced-head")
    assert task.id is not None
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    store.refresh_merge_unit_head(unit.id, head_sha="missing-recorded-head")
    store.set_merge_unit_state(unit.id, "empty")

    git = Mock()
    git.cached.return_value = nullcontext()
    git.rev_parse_if_exists.return_value = None

    with caplog.at_level("WARNING"):
        results = revalidate_terminal_no_work_merge_units(store, git)

    assert len(results) == 1
    assert results[0].merge_status == "empty"
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "empty"
    assert "recorded head 'missing-recorded-head' is unavailable" in caplog.text


def test_git_reconcile_update_drops_merge_source_for_empty_result() -> None:
    update = _git_reconcile_update(
        BranchSyncResult(
            branch="feature/empty-source",
            task_ids=("task-1",),
            merge_status="empty",
            merge_source="github_pr",
        )
    )

    assert update.merge_status == "empty"
    assert update.merge_source is _UNSET


def test_sync_branch_cohorts_runs_terminal_no_work_revalidation_before_reconciling_requested_cohorts(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    healthy = _completed_branch_task(store, "Healthy task", "feature/healthy")
    stranded = _completed_branch_task(store, "Stranded task", "feature/stranded")
    assert stranded.id is not None
    stranded_unit = store.get_or_create_merge_unit_for_task(stranded)
    assert stranded_unit is not None
    store.refresh_merge_unit_head(stranded_unit.id, head_sha="recorded-head-sha")
    store.set_merge_unit_state(stranded_unit.id, "redundant")

    git = Mock()
    git.cached.return_value = nullcontext()
    git.default_branch.return_value = "main"
    git.branch_exists.side_effect = lambda ref: ref == "feature/healthy"
    git.is_merged.return_value = False
    git.get_diff_numstat.return_value = "1\t1\thealthy.txt\n"
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "recorded-head-sha": "recorded-head-sha",
        "feature/healthy": "healthy-head",
        "main": "main-head",
    }.get(ref)
    git.is_patch_equivalent_commit_present_on_target.side_effect = lambda commit, target: (
        False if (commit, target) == ("recorded-head-sha", "main") else None
    )

    results, partial = sync_branch_cohorts(
        store,
        git,
        [BranchCohort(branch=healthy.branch, tasks=(healthy,))],
        include_git=True,
        include_pr=False,
        dry_run=False,
        fetch_remote=False,
    )

    assert partial is False
    assert results[0].merge_status == "unmerged"
    refreshed_unit = store.get_merge_unit(stranded_unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"


def test_git_reconcile_update_drops_merge_source_for_redundant_result() -> None:
    update = _git_reconcile_update(
        BranchSyncResult(
            branch="feature/redundant-source",
            task_ids=("task-1",),
            merge_status="redundant",
            merge_source="github_pr",
        )
    )

    assert update.merge_status == "redundant"
    assert update.merge_source is _UNSET


def test_merge_persistence_update_drops_merge_source_when_resolved_state_is_not_merged() -> None:
    update = _merge_persistence_update(
        _BranchPersistenceUpdate(merge_status="empty"),
        _BranchPersistenceUpdate(merge_source="github_pr"),
        baseline_state="empty",
    )

    assert update.merge_status == "empty"
    assert update.merge_source is _UNSET


def test_persist_branch_updates_drops_merge_source_for_empty_unit_and_advances_sync_cooldown(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/persist-empty-source")
    assert task.id is not None
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "empty")

    sync_completed_at = datetime.now(UTC)
    cohort = BranchCohort(
        branch=task.branch,
        tasks=(task,),
        merge_unit_id=unit.id,
        merge_unit_state="empty",
    )
    result = BranchSyncResult(
        branch=task.branch,
        task_ids=(task.id,),
        merge_status="empty",
    )

    _persist_branch_updates(
        store,
        [cohort],
        [result],
        [_BranchPersistenceUpdate(merge_source="github_pr")],
        "main",
        sync_completed_at=sync_completed_at,
    )

    refreshed_unit = store.get_merge_unit(unit.id)
    refreshed_task = store.get(task.id)
    assert refreshed_unit is not None
    assert refreshed_task is not None
    assert refreshed_unit.state == "empty"
    assert refreshed_unit.merge_source is None
    assert refreshed_unit.merged_at is None
    assert refreshed_unit.sync_last_synced_at == sync_completed_at
    assert refreshed_task.sync_last_synced_at == sync_completed_at
    assert result.errors == []
    assert build_default_branch_cohorts(store, recent_days=30, cooldown_seconds=300) == []


def test_persist_branch_updates_persists_redundant_without_merged_provenance(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/persist-redundant")
    assert task.id is not None
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "unmerged")

    cohort = BranchCohort(
        branch=task.branch,
        tasks=(task,),
        merge_unit_id=unit.id,
        merge_unit_state="unmerged",
    )
    result = BranchSyncResult(
        branch=task.branch,
        task_ids=(task.id,),
        merge_status="redundant",
    )

    _persist_branch_updates(
        store,
        [cohort],
        [result],
        [_BranchPersistenceUpdate(merge_status="redundant", merge_source="github_pr")],
        "main",
    )

    refreshed_unit = store.get_merge_unit(unit.id)
    refreshed_task = store.get(task.id)
    assert refreshed_unit is not None
    assert refreshed_task is not None
    assert refreshed_unit.state == "redundant"
    assert refreshed_unit.merged_at is None
    assert refreshed_unit.merged_by_task_id is None
    assert refreshed_unit.merge_source is None
    assert refreshed_task.merge_status is None


def test_sync_branch_cohorts_persist_failure_marks_only_that_cohort_as_error(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    failing = _completed_branch_task(store, "Failing persist", "feature/persist-fails")
    succeeding = _completed_branch_task(store, "Successful persist", "feature/persist-ok")

    git = Mock()
    git.default_branch.return_value = "main"

    real_persist_branch_state = _persist_branch_state

    def _failing_first_persist(*args, **kwargs):
        tasks = args[1]
        if tasks[0].branch == "feature/persist-fails":
            raise RuntimeError("boom")
        return real_persist_branch_state(*args, **kwargs)

    with patch("gza.sync_ops._persist_branch_state", side_effect=_failing_first_persist):
        results, partial = sync_branch_cohorts(
            store,
            git,
            [
                BranchCohort(branch=failing.branch, tasks=(failing,)),
                BranchCohort(branch=succeeding.branch, tasks=(succeeding,)),
            ],
            include_git=False,
            include_pr=False,
            dry_run=False,
            fetch_remote=False,
        )

    assert partial is True
    assert len(results) == 2
    assert results[0].errors == ["failed to persist sync state for branch 'feature/persist-fails': boom"]
    assert results[1].errors == []
    refreshed_failing = store.get(failing.id)
    refreshed_succeeding = store.get(succeeding.id)
    assert refreshed_failing is not None
    assert refreshed_succeeding is not None
    assert refreshed_failing.sync_last_synced_at is None
    assert refreshed_succeeding.sync_last_synced_at is not None


def test_reconcile_branch_merge_truth_warns_and_fails_closed_when_commit_count_unavailable(tmp_path):
    """When refs cannot be resolved to SHAs, emit a warning and preserve the existing state."""
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/no-sha")
    cohort = BranchCohort(branch=task.branch, tasks=(task,))

    git = Mock()
    git.branch_exists.return_value = True
    git.is_merged.return_value = False
    # No origin/ ref and rev_parse always returns None → source_sha = None → "unknown".
    git.ref_exists.return_value = False
    git.rev_parse_if_exists.return_value = None

    results = reconcile_branch_merge_truth(
        git,
        [cohort],
        target_branch="main",
        include_diff_stats=False,
    )

    # Fail-closed: preserves "unmerged" (from task.merge_status) rather than guessing "empty".
    assert results[0].merge_status == "unmerged"
    assert any("could not determine unique commit count" in w for w in results[0].warnings)
