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

    def delete_branch(self, branch: str) -> None:
        return None

    def checkout(self, branch: str) -> None:
        return None

    def rebase(self, target: str) -> None:
        return None

    def fetch(self, remote: str = "origin") -> None:
        return None


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

    legacy = store.add("Legacy shared branch", task_type="implement")
    legacy.status = "completed"
    legacy.completed_at = datetime.now(UTC)
    legacy.branch = "feature/legacy-advance"
    legacy.has_commits = True
    legacy.merge_status = "unmerged"
    store.update(legacy)

    assert legacy.id is not None
    assert store.resolve_merge_unit_for_task(legacy.id, "main") is None

    tasks, impl_based_on_ids = _collect_advance_completed_tasks(store, target_branch="main")

    assert legacy.id not in impl_based_on_ids
    assert [task.id for task in tasks if task.task_type == "implement"] == [legacy.id]
    unit = store.resolve_merge_unit_for_task(legacy.id, "main")
    assert unit is not None
    assert unit.state == "unmerged"


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


def test_unmerged_uses_real_default_branch_for_merge_units(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement master-target branch", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/master-target", target_branch="master")
    assert impl.id is not None

    fake_git = _MergeGit(tmp_path, default_branch="master")
    with patch("gza.cli.query.Git", lambda project_dir: fake_git):
        result = run_gza("unmerged", "--project", str(tmp_path), cwd=tmp_path)

    assert result.returncode == 0
    assert impl.id in result.stdout
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    assert unit.target_branch == "master"


def test_pr_uses_requested_default_branch_merge_unit_state(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement release-target branch", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/release-target", target_branch="main")
    assert impl.id is not None

    main_unit = store.get_or_create_merge_unit_for_task(impl, "main")
    release_unit = store.get_or_create_merge_unit_for_task(impl, "release")
    assert main_unit is not None
    assert release_unit is not None
    store.set_merge_unit_state(main_unit.id, "merged")
    store.set_merge_unit_state(release_unit.id, "unmerged")

    fake_git = _MergeGit(tmp_path, default_branch="release")
    with patch("gza.cli.git_ops.Git", lambda project_dir: fake_git):
        result = run_gza("pr", str(impl.id), "--project", str(tmp_path), cwd=tmp_path)

    assert "already marked as merged" not in result.stdout


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
    unit = store.resolve_merge_unit_for_task(legacy.id, "main")
    assert unit is not None
    assert unit.state == "merged"


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
