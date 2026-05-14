from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from gza.cli.git_ops import _collect_advance_completed_tasks
from tests.cli.conftest import make_store, setup_config
from tests.helpers.cli import run_gza


class _MergeGit:
    def __init__(self, project_dir: Path, *, default_branch: str = "main") -> None:
        self.repo_dir = project_dir
        self._default_branch = default_branch
        self.merged: list[tuple[str, bool]] = []
        self.commit_messages: list[str | None] = []

    def current_branch(self) -> str:
        return self._default_branch

    def default_branch(self) -> str:
        return self._default_branch

    def is_merged(self, branch: str, into: str | None = None, use_cherry: bool = False) -> bool:
        return False

    def branch_exists(self, branch: str) -> bool:
        return True

    def ref_exists(self, ref: str) -> bool:
        return False

    def can_merge(self, branch: str, into: str | None = None) -> bool:
        return True

    def get_diff_numstat(self, revision_range: str) -> str:
        return "1\t0\tfeature.txt\n"

    def get_diff_stat_parsed(self, revision_range: str) -> tuple[int, int, int]:
        return (1, 1, 0)

    def count_commits_ahead(self, branch: str, target: str) -> int:
        return 1

    def has_changes(self, include_untracked: bool = False) -> bool:
        return False

    def merge(self, branch: str, squash: bool = False, commit_message: str | None = None) -> None:
        self.merged.append((branch, squash))
        self.commit_messages.append(commit_message)

    def delete_branch(self, branch: str) -> None:
        return None

    def checkout(self, branch: str) -> None:
        return None

    def rebase(self, target: str) -> None:
        return None

    def fetch(self, remote: str = "origin") -> None:
        return None


class _AdvanceGit:
    def __init__(self, *, default_branch: str = "main", current_branch: str | None = None) -> None:
        self.repo_dir = Path.cwd()
        self._default_branch = default_branch
        self._current_branch = current_branch or default_branch

    def current_branch(self) -> str:
        return self._current_branch

    def default_branch(self) -> str:
        return self._default_branch

    def can_merge(self, branch: str, into: str | None = None) -> bool:
        return True

    def is_merged(self, branch: str, into: str | None = None, use_cherry: bool = False) -> bool:
        return False

    def branch_exists(self, branch: str) -> bool:
        return True

    def ref_exists(self, ref: str) -> bool:
        return False

    def count_commits_ahead(self, branch: str, target: str) -> int:
        return 1

    def get_diff_stat_parsed(self, revision_range: str) -> tuple[int, int, int]:
        return (1, 1, 0)


def _add_completed_legacy_impl(store, prompt: str, branch: str):
    task = store.add(prompt, task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = branch
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    return task


def test_merge_all_deduplicates_same_branch_merge_unit(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement shared branch", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/shared")

    improve = store.add("Improve shared branch", task_type="improve", based_on=impl.id, same_branch=True)
    store.mark_completed(improve, has_commits=True, branch="feature/shared")

    fake_git = _MergeGit(tmp_path)
    with patch("gza.cli.git_ops.Git", lambda project_dir: fake_git):
        result = run_gza("merge", "--all", "--project", str(tmp_path), cwd=tmp_path)

    assert result.returncode == 0
    assert fake_git.merged == [("feature/shared", False)]
    refreshed_impl = store.get(impl.id)
    refreshed_improve = store.get(improve.id)
    assert refreshed_impl is not None
    assert refreshed_improve is not None
    assert refreshed_impl.merge_status == "merged"
    assert refreshed_improve.merge_status is None


def test_collect_advance_completed_tasks_backfills_legacy_unmerged_owner(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    legacy = _add_completed_legacy_impl(store, "Legacy shared branch", "feature/legacy-advance")

    assert legacy.id is not None
    assert store.resolve_merge_unit_for_task(legacy.id) is None

    tasks, impl_based_on_ids = _collect_advance_completed_tasks(store, target_branch="main")

    assert legacy.id not in impl_based_on_ids
    assert [task.id for task in tasks if task.task_type == "implement"] == [legacy.id]
    unit = store.resolve_merge_unit_for_task(legacy.id)
    assert unit is not None
    assert unit.state == "unmerged"


def test_collect_advance_completed_tasks_returns_owner_once_for_same_unit_descendants(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement shared branch", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/owner-only-advance")
    assert impl.id is not None

    improve = store.add("Improve shared branch", task_type="improve", based_on=impl.id, same_branch=True)
    store.mark_completed(improve, has_commits=True, branch="feature/owner-only-advance")
    assert improve.id is not None

    tasks, _ = _collect_advance_completed_tasks(store, target_branch="main")

    assert [task.id for task in tasks if task.task_type == "implement"] == [impl.id]
    assert improve.id not in [task.id for task in tasks]


def test_collect_advance_completed_tasks_filters_unmerged_tasks_by_target_branch(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    main_task = _add_completed_legacy_impl(store, "Main-target work", "feature/main-target")
    release_task = _add_completed_legacy_impl(store, "Release-target work", "feature/release-target")
    assert main_task.id is not None
    assert release_task.id is not None

    main_unit = store.create_merge_unit(
        source_branch="feature/main-target",
        target_branch="main",
        owner_task_id=main_task.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(main_task.id, main_unit.id, "owner")
    store.dual_write_legacy_merge_status(main_unit.id)

    release_unit = store.create_merge_unit(
        source_branch="feature/release-target",
        target_branch="release",
        owner_task_id=release_task.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(release_task.id, release_unit.id, "owner")
    store.dual_write_legacy_merge_status(release_unit.id)

    main_tasks, _ = _collect_advance_completed_tasks(store, target_branch="main")
    release_tasks, _ = _collect_advance_completed_tasks(store, target_branch="release")

    assert [task.id for task in main_tasks if task.task_type == "implement"] == [main_task.id]
    assert [task.id for task in release_tasks if task.task_type == "implement"] == [release_task.id]


def test_advance_explicit_task_uses_default_target_merge_unit_over_stale_legacy_row(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Advance explicit task", task_type="implement")
    store.mark_completed(task, has_commits=True, branch="feature/advance-explicit")
    assert task.id is not None

    refreshed = store.get(task.id)
    assert refreshed is not None
    refreshed.merge_status = "merged"
    store.update(refreshed)

    calls: list[str] = []

    def _fake_determine_next_action(*args, **kwargs):
        selected_task = args[3]
        assert selected_task.id is not None
        calls.append(selected_task.id)
        return {"type": "skip", "description": "still actionable via merge unit"}

    with (
        patch("gza.cli.git_ops.Git", lambda _project_dir: _AdvanceGit()),
        patch("gza.cli.git_ops.determine_next_action", side_effect=_fake_determine_next_action),
    ):
        result = run_gza("advance", task.id, "--dry-run", "--project", str(tmp_path), cwd=tmp_path)

    assert result.returncode == 0
    assert f"Task {task.id} is already merged" not in result.stdout
    assert calls == [task.id]


def test_advance_failed_task_recovery_planning_uses_merge_unit_over_stale_legacy_row(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.branch = "feature/advance-recovery"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    recovery = store.add(failed.prompt, task_type="implement", based_on=failed.id)
    store.mark_completed(recovery, has_commits=True, branch="feature/advance-recovery")
    assert recovery.id is not None

    refreshed_recovery = store.get(recovery.id)
    assert refreshed_recovery is not None
    refreshed_recovery.merge_status = "merged"
    store.update(refreshed_recovery)

    calls: list[str] = []

    def _fake_determine_next_action(*args, **kwargs):
        selected_task = args[3]
        assert selected_task.id is not None
        calls.append(selected_task.id)
        return {"type": "skip", "description": "recovery descendant still actionable via merge unit"}

    with (
        patch("gza.cli.git_ops.Git", lambda _project_dir: _AdvanceGit()),
        patch("gza.cli.git_ops.determine_next_action", side_effect=_fake_determine_next_action),
    ):
        result = run_gza("advance", failed.id, "--dry-run", "--project", str(tmp_path), cwd=tmp_path)

    assert result.returncode == 0
    assert f"Task {failed.id} is already merged" not in result.stdout
    assert calls == [recovery.id]


def test_advance_dry_run_uses_current_branch_for_merge_unit_target_collection(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    release_task = _add_completed_legacy_impl(store, "Release-target work", "feature/release-advance")
    assert release_task.id is not None

    release_unit = store.create_merge_unit(
        source_branch="feature/release-advance",
        target_branch="release",
        owner_task_id=release_task.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(release_task.id, release_unit.id, "owner")
    store.dual_write_legacy_merge_status(release_unit.id)

    calls: list[str] = []

    def _fake_determine_next_action(*args, **kwargs):
        selected_task = args[3]
        assert selected_task.id is not None
        calls.append(selected_task.id)
        return {"type": "skip", "description": "eligible on current release branch"}

    fake_git = _AdvanceGit(default_branch="main", current_branch="release")

    with (
        patch("gza.cli.git_ops.Git", lambda _project_dir: fake_git),
        patch("gza.cli.git_ops.determine_next_action", side_effect=_fake_determine_next_action),
    ):
        result = run_gza("advance", "--dry-run", "--project", str(tmp_path), cwd=tmp_path)

    assert result.returncode == 0
    assert release_task.id in result.stdout
    assert "eligible on current release branch" in result.stdout
    assert calls == [release_task.id]


def test_advance_dry_run_filters_owner_rows_by_target_branch_and_keeps_legacy_fallback(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    main_task = _add_completed_legacy_impl(store, "Main-target owner", "feature/main-owner")
    release_task = _add_completed_legacy_impl(store, "Release-target owner", "feature/release-owner")
    legacy_task = _add_completed_legacy_impl(store, "Legacy fallback owner", "feature/legacy-owner")
    assert main_task.id is not None
    assert release_task.id is not None
    assert legacy_task.id is not None

    main_unit = store.create_merge_unit(
        source_branch="feature/main-owner",
        target_branch="main",
        owner_task_id=main_task.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(main_task.id, main_unit.id, "owner")
    store.dual_write_legacy_merge_status(main_unit.id)

    release_unit = store.create_merge_unit(
        source_branch="feature/release-owner",
        target_branch="release",
        owner_task_id=release_task.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(release_task.id, release_unit.id, "owner")
    store.dual_write_legacy_merge_status(release_unit.id)

    calls: list[str] = []

    def _fake_determine_next_action(*args, **kwargs):
        selected_task = args[3]
        assert selected_task.id is not None
        calls.append(selected_task.id)
        return {"type": "merge", "description": f"merge {selected_task.id}"}

    with (
        patch("gza.cli.git_ops.Git", lambda _project_dir: _AdvanceGit(current_branch="main")),
        patch("gza.cli.git_ops.determine_next_action", side_effect=_fake_determine_next_action),
    ):
        result = run_gza("advance", "--dry-run", "--project", str(tmp_path), cwd=tmp_path)

    assert result.returncode == 0
    assert set(calls) == {main_task.id, legacy_task.id}
    assert release_task.id not in calls
    assert main_task.id in result.stdout
    assert legacy_task.id in result.stdout
    assert release_task.id not in result.stdout


def test_merge_review_task_id_resolves_branchless_review_to_implementation_unit(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement shared branch", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/shared")
    assert impl.id is not None

    create_result = run_gza("review", str(impl.id), "--queue", "--project", str(tmp_path), cwd=tmp_path)
    assert create_result.returncode == 0
    review = next(task for task in store.get_all() if task.task_type == "review")
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    store.update(review)
    assert review.id is not None

    fake_git = _MergeGit(tmp_path)
    with patch("gza.cli.git_ops.Git", lambda project_dir: fake_git):
        result = run_gza("merge", str(review.id), "--project", str(tmp_path), cwd=tmp_path)

    assert result.returncode == 0
    assert fake_git.merged == [("feature/shared", False)]
    assert store.resolve_merge_unit_for_task(review.id).id == store.resolve_merge_unit_for_task(impl.id).id


def test_unmerged_lists_merge_unit_owner(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/master-target")
    assert impl.id is not None

    fake_git = _MergeGit(tmp_path, default_branch="master")
    with (
        patch("gza.cli.query.Git", lambda project_dir: fake_git),
        patch("gza.github.GitHub.is_available", return_value=False),
    ):
        result = run_gza("unmerged", "--project", str(tmp_path), cwd=tmp_path)

    assert result.returncode == 0
    assert impl.id in result.stdout
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    assert unit.state == "unmerged"


def test_pr_blocks_when_task_merge_unit_is_merged_even_if_git_default_branch_differs(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement release-target branch", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/release-target")
    assert impl.id is not None

    unit = store.get_or_create_merge_unit_for_task(impl)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "merged")

    fake_git = _MergeGit(tmp_path, default_branch="release")
    with patch("gza.cli.git_ops.Git", lambda project_dir: fake_git):
        result = run_gza("pr", str(impl.id), "--project", str(tmp_path), cwd=tmp_path)

    assert "already marked as merged" in result.stdout


def test_merge_missing_explicit_task_id_fails_closed(tmp_path: Path) -> None:
    setup_config(tmp_path)
    make_store(tmp_path)

    fake_git = _MergeGit(tmp_path)
    with patch("gza.cli.git_ops.Git", lambda project_dir: fake_git):
        result = run_gza("merge", "testproject-9999", "--project", str(tmp_path), cwd=tmp_path)

    assert result.returncode == 1
    assert "Error: Task testproject-9999 not found" in result.stdout
    assert fake_git.merged == []


def test_merge_all_backfills_legacy_unmerged_owner_when_units_exist(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    legacy = store.add("Legacy merge-all branch", task_type="implement")
    legacy.status = "completed"
    legacy.completed_at = datetime.now(UTC)
    legacy.branch = "feature/legacy-merge-all"
    legacy.has_commits = True
    legacy.merge_status = "unmerged"
    store.update(legacy)

    fake_git = _MergeGit(tmp_path)
    with patch("gza.cli.git_ops.Git", lambda project_dir: fake_git):
        result = run_gza("merge", "--all", "--project", str(tmp_path), cwd=tmp_path)

    assert result.returncode == 0
    assert "No unmerged done tasks found" not in result.stdout
    assert fake_git.merged == [("feature/legacy-merge-all", False)]
    assert legacy.id is not None
    unit = store.resolve_merge_unit_for_task(legacy.id)
    assert unit is not None
    assert unit.state == "merged"


def test_merge_all_uses_completed_retry_when_merge_unit_owner_failed(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.completed_at = datetime.now(UTC)
    failed.branch = "feature/merge-retry"
    failed.has_commits = True
    failed.merge_status = "unmerged"
    store.update(failed)

    retry = store.add("Completed retry", task_type="implement", based_on=failed.id)
    store.mark_completed(retry, has_commits=True, branch="feature/merge-retry")
    assert retry.id is not None

    fake_git = _MergeGit(tmp_path)
    with patch("gza.cli.git_ops.Git", lambda project_dir: fake_git):
        result = run_gza("merge", "--all", "--project", str(tmp_path), cwd=tmp_path)

    assert result.returncode == 0
    assert fake_git.merged == [("feature/merge-retry", False)]


def test_merge_explicit_retry_task_id_uses_actionable_member_when_owner_failed(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.completed_at = datetime.now(UTC)
    failed.branch = "feature/explicit-retry"
    failed.has_commits = True
    failed.merge_status = "unmerged"
    store.update(failed)

    retry = store.add("Completed retry", task_type="implement", based_on=failed.id)
    store.mark_completed(retry, has_commits=True, branch="feature/explicit-retry")
    assert retry.id is not None

    fake_git = _MergeGit(tmp_path)
    with patch("gza.cli.git_ops.Git", lambda project_dir: fake_git):
        result = run_gza("merge", str(retry.id), "--project", str(tmp_path), cwd=tmp_path)

    assert result.returncode == 0
    assert fake_git.merged == [("feature/explicit-retry", False)]
    unit = store.resolve_merge_unit_for_task(retry.id)
    assert unit is not None
    assert unit.merged_by_task_id == failed.id


def test_merge_explicit_improve_task_uses_owner_for_provenance_and_squash_subject(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement shared branch", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/explicit-improve")
    assert impl.id is not None

    improve = store.add("Improve shared branch", task_type="improve", based_on=impl.id, same_branch=True)
    store.mark_completed(improve, has_commits=True, branch="feature/explicit-improve")
    assert improve.id is not None

    fake_git = _MergeGit(tmp_path)
    with patch("gza.cli.git_ops.Git", lambda project_dir: fake_git):
        result = run_gza("merge", str(improve.id), "--squash", "--project", str(tmp_path), cwd=tmp_path)

    assert result.returncode == 0
    assert fake_git.merged == [("feature/explicit-improve", True)]
    assert fake_git.commit_messages and fake_git.commit_messages[0] is not None
    assert impl.id in fake_git.commit_messages[0]
    assert "Implement shared branch" in fake_git.commit_messages[0]
    unit = store.resolve_merge_unit_for_task(improve.id)
    assert unit is not None
    assert unit.merged_by_task_id == impl.id


def test_merge_valid_and_missing_explicit_task_ids_report_missing_without_partial_merge(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement shared branch", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/shared")
    assert impl.id is not None

    fake_git = _MergeGit(tmp_path)
    with patch("gza.cli.git_ops.Git", lambda project_dir: fake_git):
        result = run_gza(
            "merge",
            str(impl.id),
            "testproject-9999",
            "--project",
            str(tmp_path),
            cwd=tmp_path,
        )

    assert result.returncode == 1
    assert "Error: Task testproject-9999 not found" in result.stdout
    assert fake_git.merged == []
