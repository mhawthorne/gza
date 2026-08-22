import inspect
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from gza.cli.git_ops import _collect_advance_completed_tasks
from gza.advance_engine import PARK_REASON_VERIFY_FIX_FAILED
from gza.config import Config
from gza.db import MERGE_SOURCE_MANUAL_FORCE
from gza.git import GitError
from gza.review_tasks import build_verify_fix_prompt
from gza.review_verify_state import VerifyEpoch, persist_verify_gate_artifact
from gza.verify_fix_outcome import persist_verify_fix_completion_outcome
from tests.cli.conftest import make_store, setup_config
from tests.helpers.cli import invoke_gza


@pytest.fixture(autouse=True)
def _patch_ambient_real_git():
    with (
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
    ):
        yield


@pytest.fixture(autouse=True)
def _stub_main_integration_verify():
    with patch(
        "gza.cli.git_ops.check_main_integration_verify",
        return_value=SimpleNamespace(
            merges_halted=False,
            state=SimpleNamespace(task=SimpleNamespace(id=None), alert_message=None),
        ),
    ):
        yield


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

    def rev_parse_if_exists(self, ref: str) -> str | None:
        if ref == self._default_branch:
            return "base-head"
        if ref.startswith("feature/") or ref.startswith("origin/feature/"):
            return "same-head"
        return None

    def can_merge(self, branch: str, into: str | None = None) -> bool:
        return True

    def get_diff_numstat(self, revision_range: str) -> str:
        return "1\t0\tfeature.txt\n"

    def get_diff_name_status(
        self,
        revision_range: str,
        paths: tuple[str, ...] | list[str] = (),
        *,
        check: bool = False,
    ) -> str:
        del revision_range, paths, check
        return ""

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


def _persist_current_verify(
    tmp_path: Path,
    store,
    *,
    owner_task,
    source_task,
    status: str = "passed",
    command: str = "./bin/tests",
    exit_status: str = "0",
    head_sha: str = "same-head",
    base_sha: str = "base-head",
) -> None:
    config_path = tmp_path / "gza.yaml"
    config_text = config_path.read_text(encoding="utf-8")
    if "verify_command:" not in config_text:
        config_path.write_text(config_text + f"verify_command: {command}\n", encoding="utf-8")
    config = Config.load(tmp_path)
    persist_verify_gate_artifact(
        store,
        config,
        owner_task=owner_task,
        source_task=source_task,
        result=SimpleNamespace(
            command=command,
            status=status,
            exit_status=exit_status,
            captured_at=datetime(2026, 6, 29, 12, 5, tzinfo=UTC),
            reviewed_branch=owner_task.branch,
            reviewed_head_sha=head_sha,
            reviewed_base_sha=base_sha,
            working_directory="/tmp/merge-unit-verify",
            failure=None if status == "passed" else f"verify gate {status}",
        ),
        verify_timeout_seconds=config.autonomous_verify_timeout_seconds,
        verify_timeout_grace_seconds=config.review_verify_timeout_grace_seconds,
        producer="review_verify",
    )


def _persist_current_green_verify(
    tmp_path: Path,
    store,
    *,
    owner_task,
    source_task,
    head_sha: str = "same-head",
    base_sha: str = "base-head",
) -> None:
    _persist_current_verify(
        tmp_path,
        store,
        owner_task=owner_task,
        source_task=source_task,
        head_sha=head_sha,
        base_sha=base_sha,
    )


def _add_verify_fix_for_current_epoch(
    store,
    *,
    impl,
    status: str,
    command: str = "./bin/tests",
    head_sha: str = "same-head",
):
    assert impl.id is not None
    epoch = VerifyEpoch(
        reviewed_branch=impl.branch,
        reviewed_head_sha=head_sha,
        verify_command=command,
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
    )
    verify_fix = store.add(
        build_verify_fix_prompt(impl.id, epoch),
        task_type="verify_fix",
        based_on=impl.id,
        same_branch=True,
    )
    verify_fix.status = status
    verify_fix.branch = impl.branch
    verify_fix.has_commits = False
    store.update(verify_fix)
    return verify_fix


def _add_completed_no_source_verify_fix(
    store,
    *,
    impl,
    recovery_rerun_attempted: bool,
    head_sha: str = "same-head",
):
    verify_fix = _add_verify_fix_for_current_epoch(store, impl=impl, status="completed", head_sha=head_sha)
    verify_fix.completed_at = datetime.now(UTC)
    store.update(verify_fix)
    persist_verify_fix_completion_outcome(
        store,
        verify_fix,
        no_source_changes=True,
        completion_head_sha=head_sha,
        recovery_rerun_attempted=recovery_rerun_attempted,
    )
    return verify_fix


def _assert_verify_family_merge_refused(
    tmp_path: Path,
    store,
    *,
    impl,
    expected_text: str,
    fake_git_factory=None,
    expects_proof_requirement: bool = False,
) -> None:
    flag_sets = [
        ("--force",),
        ("--force", "--ignore-verify-gate"),
    ]
    for flags in flag_sets:
        git = fake_git_factory() if fake_git_factory is not None else _MergeGit(tmp_path)
        with patch("gza.cli.git_ops.Git", lambda project_dir, git=git: git):
            result = invoke_gza(
                "merge",
                str(impl.id),
                *flags,
                "--project",
                str(tmp_path),
                cwd=tmp_path,
            )

        assert result.returncode == 1
        assert expected_text in result.stdout
        if expects_proof_requirement:
            assert "Red verify gate bypass requires current failed pre-merge proof" in result.stdout
        else:
            assert "Red verify gate bypass requires current failed pre-merge proof" not in result.stdout
        assert "Warning: Forcing merge despite lifecycle gate" not in result.stdout
        assert "Warning: Forcing merge despite red verify gate" not in result.stdout
        assert git.merged == []
        unit = store.resolve_merge_unit_for_task(impl.id)
        assert unit is not None
        assert unit.state == "unmerged"
        assert unit.merge_source is None


def _add_completed_approved_review(
    store,
    *,
    based_on_task,
    depends_on_task,
):
    review = store.add(
        f"Review {depends_on_task.id}",
        task_type="review",
        based_on=based_on_task.id,
        depends_on=depends_on_task.id,
    )
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: APPROVED**"
    store.update(review)
    return review


def _snapshot_merge_refusal_state(store, task_id: str) -> dict[str, object]:
    return {
        "tasks": [(task.id, task.status, task.branch, task.merge_status) for task in store.get_all()],
        "merge_unit": store.resolve_merge_unit_for_task(task_id),
        "artifacts": [(artifact.id, artifact.kind, artifact.path) for artifact in store.list_artifacts(task_id)],
    }


def test_merge_all_deduplicates_same_branch_merge_unit(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement shared branch", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/shared")
    assert impl.id is not None

    improve = store.add("Improve shared branch", task_type="improve", based_on=impl.id, same_branch=True)
    store.mark_completed(improve, has_commits=True, branch="feature/shared")
    assert improve.id is not None
    review = _add_completed_approved_review(store, based_on_task=impl, depends_on_task=improve)
    _persist_current_green_verify(tmp_path, store, owner_task=impl, source_task=review)

    fake_git = _MergeGit(tmp_path)
    with patch("gza.cli.git_ops.Git", lambda project_dir: fake_git):
        result = invoke_gza("merge", "--all", "--project", str(tmp_path), cwd=tmp_path)

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
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops.determine_next_action", side_effect=_fake_determine_next_action),
    ):
        result = invoke_gza("advance", task.id, "--dry-run", "--project", str(tmp_path), cwd=tmp_path)

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
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops.determine_next_action", side_effect=_fake_determine_next_action),
    ):
        result = invoke_gza("advance", failed.id, "--dry-run", "--project", str(tmp_path), cwd=tmp_path)

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
        result = invoke_gza("advance", "--dry-run", "--project", str(tmp_path), cwd=tmp_path)

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
        result = invoke_gza("advance", "--dry-run", "--project", str(tmp_path), cwd=tmp_path)

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

    create_result = invoke_gza("review", str(impl.id), "--queue", "--project", str(tmp_path), cwd=tmp_path)
    assert create_result.returncode == 0
    review = next(task for task in store.get_all() if task.task_type == "review")
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: APPROVED**"
    store.update(review)
    assert review.id is not None
    _persist_current_green_verify(tmp_path, store, owner_task=impl, source_task=review)

    fake_git = _MergeGit(tmp_path)
    with patch("gza.cli.git_ops.Git", lambda project_dir: fake_git):
        result = invoke_gza("merge", str(review.id), "--project", str(tmp_path), cwd=tmp_path)

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
        result = invoke_gza("unmerged", "--project", str(tmp_path), cwd=tmp_path)

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
        result = invoke_gza("pr", str(impl.id), "--project", str(tmp_path), cwd=tmp_path)

    assert "already marked as merged" in result.stdout


def test_merge_missing_explicit_task_id_fails_closed(tmp_path: Path) -> None:
    setup_config(tmp_path)
    make_store(tmp_path)

    fake_git = _MergeGit(tmp_path)
    with patch("gza.cli.git_ops.Git", lambda project_dir: fake_git):
        result = invoke_gza("merge", "testproject-9999", "--project", str(tmp_path), cwd=tmp_path)

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
    assert legacy.id is not None
    review = _add_completed_approved_review(store, based_on_task=legacy, depends_on_task=legacy)
    _persist_current_green_verify(tmp_path, store, owner_task=legacy, source_task=review)

    fake_git = _MergeGit(tmp_path)
    with patch("gza.cli.git_ops.Git", lambda project_dir: fake_git):
        result = invoke_gza("merge", "--all", "--project", str(tmp_path), cwd=tmp_path)

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
    review = _add_completed_approved_review(store, based_on_task=retry, depends_on_task=retry)
    _persist_current_green_verify(tmp_path, store, owner_task=retry, source_task=review)

    fake_git = _MergeGit(tmp_path)
    with patch("gza.cli.git_ops.Git", lambda project_dir: fake_git):
        result = invoke_gza("merge", "--all", "--project", str(tmp_path), cwd=tmp_path)

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
    review = _add_completed_approved_review(store, based_on_task=retry, depends_on_task=retry)
    _persist_current_green_verify(tmp_path, store, owner_task=retry, source_task=review)

    fake_git = _MergeGit(tmp_path)
    with patch("gza.cli.git_ops.Git", lambda project_dir: fake_git):
        result = invoke_gza("merge", str(retry.id), "--project", str(tmp_path), cwd=tmp_path)

    assert result.returncode == 0
    assert fake_git.merged == [("feature/explicit-retry", False)]
    unit = store.resolve_merge_unit_for_task(retry.id)
    assert unit is not None
    assert unit.merged_by_task_id == retry.id


def test_merge_explicit_improve_task_uses_owner_for_provenance_and_squash_subject(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement shared branch", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/explicit-improve")
    assert impl.id is not None

    improve = store.add("Improve shared branch", task_type="improve", based_on=impl.id, same_branch=True)
    store.mark_completed(improve, has_commits=True, branch="feature/explicit-improve")
    assert improve.id is not None
    review = _add_completed_approved_review(store, based_on_task=impl, depends_on_task=improve)
    _persist_current_green_verify(tmp_path, store, owner_task=impl, source_task=review)

    fake_git = _MergeGit(tmp_path)
    with patch("gza.cli.git_ops.Git", lambda project_dir: fake_git):
        result = invoke_gza("merge", str(improve.id), "--squash", "--project", str(tmp_path), cwd=tmp_path)

    assert result.returncode == 0
    assert fake_git.merged == [("feature/explicit-improve", True)]
    assert fake_git.commit_messages and fake_git.commit_messages[0] is not None
    assert impl.id in fake_git.commit_messages[0]
    assert "Implement shared branch" in fake_git.commit_messages[0]
    unit = store.resolve_merge_unit_for_task(improve.id)
    assert unit is not None
    assert unit.merged_by_task_id == impl.id


def test_merge_force_bypasses_lifecycle_gate_and_records_manual_force_provenance(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement forced merge path", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/force-lifecycle-gate")
    assert impl.id is not None

    review = _add_completed_approved_review(store, based_on_task=impl, depends_on_task=impl)
    _persist_current_green_verify(tmp_path, store, owner_task=impl, source_task=review)

    fake_git = _MergeGit(tmp_path)
    blocked_action = {
        "type": "needs_discussion",
        "description": "SKIP: required resolution-review metadata is missing or malformed",
        "needs_attention_reason": "resolution-review-metadata-invalid",
    }

    with (
        patch("gza.cli.git_ops.Git", lambda project_dir: fake_git),
        patch("gza.cli.git_ops.determine_next_action", return_value=blocked_action),
    ):
        blocked = invoke_gza("merge", str(impl.id), "--squash", "--project", str(tmp_path), cwd=tmp_path)

    assert blocked.returncode == 1
    assert "required resolution-review metadata is missing or malformed" in blocked.stdout
    assert fake_git.merged == []

    with (
        patch("gza.cli.git_ops.Git", lambda project_dir: fake_git),
        patch("gza.cli.git_ops.determine_next_action", return_value=blocked_action),
    ):
        forced = invoke_gza(
            "merge",
            str(impl.id),
            "--squash",
            "--force",
            "--project",
            str(tmp_path),
            cwd=tmp_path,
        )

    assert forced.returncode == 0
    assert "Warning: Forcing merge despite lifecycle gate" in forced.stdout
    assert fake_git.merged == [("feature/force-lifecycle-gate", True)]
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    assert unit.merge_source == MERGE_SOURCE_MANUAL_FORCE


def test_merge_force_alone_refuses_actionable_verify_gate(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement red verify gate path", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/red-verify-gate")
    assert impl.id is not None

    review = _add_completed_approved_review(store, based_on_task=impl, depends_on_task=impl)
    _persist_current_verify(
        tmp_path,
        store,
        owner_task=impl,
        source_task=impl,
        status="failed",
        exit_status="1",
    )

    fake_git = _MergeGit(tmp_path)
    with patch("gza.cli.git_ops.Git", lambda project_dir: fake_git):
        result = invoke_gza(
            "merge",
            str(impl.id),
            "--force",
            "--project",
            str(tmp_path),
            cwd=tmp_path,
        )

    assert result.returncode == 1
    assert "Create verify_fix task for verify epoch" in result.stdout
    assert "Red verify gates require --force --ignore-verify-gate" in result.stdout
    assert "Warning: Forcing merge despite red verify gate" not in result.stdout
    assert fake_git.merged == []
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    assert unit.state == "unmerged"
    assert unit.merge_source is None
    verify_fix_tasks = [task for task in store.get_all() if task.task_type == "verify_fix"]
    assert verify_fix_tasks == []


def test_merge_force_ignore_verify_gate_accepts_production_create_verify_fix_action(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement red verify gate bypass path", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/red-verify-gate-bypass")
    assert impl.id is not None

    _add_completed_approved_review(store, based_on_task=impl, depends_on_task=impl)
    _persist_current_verify(
        tmp_path,
        store,
        owner_task=impl,
        source_task=impl,
        status="failed",
        exit_status="1",
    )

    fake_git = _MergeGit(tmp_path)
    with patch("gza.cli.git_ops.Git", lambda project_dir: fake_git):
        result = invoke_gza(
            "merge",
            str(impl.id),
            "--force",
            "--ignore-verify-gate",
            "--project",
            str(tmp_path),
            cwd=tmp_path,
        )

    assert result.returncode == 0
    assert "Warning: Forcing merge despite red verify gate" in result.stdout
    assert "failing epoch head=same-head" in result.stdout
    assert "verify command='./bin/tests'" in result.stdout
    assert fake_git.merged == [("feature/red-verify-gate-bypass", False)]
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    assert unit.merge_source == MERGE_SOURCE_MANUAL_FORCE
    verify_fix_tasks = [task for task in store.get_all() if task.task_type == "verify_fix"]
    assert verify_fix_tasks == []


def test_merge_force_alone_refuses_red_verify_needs_discussion_after_completed_verify_fix(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement red verify discussion path", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/red-verify-discussion")
    assert impl.id is not None

    review = _add_completed_approved_review(store, based_on_task=impl, depends_on_task=impl)
    _persist_current_green_verify(tmp_path, store, owner_task=impl, source_task=review)

    fake_git = _MergeGit(tmp_path)
    verify_action = {
        "type": "needs_discussion",
        "description": "SKIP: verify gate is still red after completed verify_fix testproject-77",
        "verify_epoch": SimpleNamespace(reviewed_head_sha="bad-head", verify_command="./bin/tests"),
        "red_verify_gate_proof": {
            "phase": "pre_merge",
            "reviewed_head_sha": "bad-head",
            "verify_command": "./bin/tests",
        },
        "needs_attention_reason": PARK_REASON_VERIFY_FIX_FAILED,
    }

    with (
        patch("gza.cli.git_ops.Git", lambda project_dir: fake_git),
        patch("gza.cli.git_ops.determine_next_action", return_value=verify_action),
    ):
        result = invoke_gza(
            "merge",
            str(impl.id),
            "--force",
            "--project",
            str(tmp_path),
            cwd=tmp_path,
        )

    assert result.returncode == 1
    assert "Red verify gates require --force --ignore-verify-gate" in result.stdout
    assert "Warning: Forcing merge despite lifecycle gate" not in result.stdout
    assert "Warning: Forcing merge despite red verify gate" not in result.stdout
    assert fake_git.merged == []
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    assert unit.state == "unmerged"
    assert unit.merge_source is None


@pytest.mark.parametrize(
    "action_type,description",
    [
        ("create_verify_fix", "Create verify_fix task for verify epoch at head bad-head"),
        (
            "rerun_completed_verify_fix",
            "Rerun exact-head verify for completed no-source verify_fix testproject-77",
        ),
        (
            "needs_discussion",
            "SKIP: verify gate is still red after completed verify_fix testproject-77",
        ),
    ],
)
def test_merge_force_ignore_verify_gate_warns_and_records_manual_force_provenance(
    tmp_path: Path,
    action_type: str,
    description: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add(f"Implement ignored red verify gate path {action_type}", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch=f"feature/ignore-red-verify-gate-{action_type}")
    assert impl.id is not None

    review = _add_completed_approved_review(store, based_on_task=impl, depends_on_task=impl)
    _persist_current_green_verify(tmp_path, store, owner_task=impl, source_task=review)

    fake_git = _MergeGit(tmp_path)
    verify_action = {
        "type": action_type,
        "description": description,
        "verify_epoch": SimpleNamespace(reviewed_head_sha="bad-head", verify_command="./bin/tests"),
        "red_verify_gate_proof": {
            "phase": "pre_merge",
            "reviewed_head_sha": "bad-head",
            "verify_command": "./bin/tests",
        },
    }
    if action_type == "needs_discussion":
        verify_action["needs_attention_reason"] = PARK_REASON_VERIFY_FIX_FAILED

    with (
        patch("gza.cli.git_ops.Git", lambda project_dir: fake_git),
        patch("gza.cli.git_ops.determine_next_action", return_value=verify_action),
    ):
        result = invoke_gza(
            "merge",
            str(impl.id),
            "--force",
            "--ignore-verify-gate",
            "--project",
            str(tmp_path),
            cwd=tmp_path,
        )

    assert result.returncode == 0
    assert "Warning: Forcing merge despite red verify gate" in result.stdout
    assert "failing epoch head=bad-head" in result.stdout
    assert "verify command='./bin/tests'" in result.stdout
    assert fake_git.merged == [(f"feature/ignore-red-verify-gate-{action_type}", False)]
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    assert unit.merge_source == MERGE_SOURCE_MANUAL_FORCE


@pytest.mark.parametrize(
    ("case_name", "recovery_rerun_attempted", "evidence_source"),
    [
        ("consumed-rerun", True, "impl"),
        ("verify-fix-sourced-non-green", False, "verify_fix"),
    ],
)
def test_merge_force_ignore_verify_gate_accepts_production_completed_verify_fix_red_branches(
    tmp_path: Path,
    case_name: str,
    recovery_rerun_attempted: bool,
    evidence_source: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add(f"Implement completed verify_fix red branch {case_name}", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch=f"feature/completed-verify-fix-red-{case_name}")
    assert impl.id is not None
    review = _add_completed_approved_review(store, based_on_task=impl, depends_on_task=impl)
    del review
    verify_fix = _add_completed_no_source_verify_fix(
        store,
        impl=impl,
        recovery_rerun_attempted=recovery_rerun_attempted,
    )
    source_task = verify_fix if evidence_source == "verify_fix" else impl
    _persist_current_verify(
        tmp_path,
        store,
        owner_task=impl,
        source_task=source_task,
        status="failed",
        command="./bin/timeout-tests",
        exit_status="timed out",
    )

    force_only_git = _MergeGit(tmp_path)
    with patch("gza.cli.git_ops.Git", lambda project_dir: force_only_git):
        force_only = invoke_gza(
            "merge",
            str(impl.id),
            "--force",
            "--project",
            str(tmp_path),
            cwd=tmp_path,
        )

    assert force_only.returncode == 1
    assert "Red verify gates require --force --ignore-verify-gate" in force_only.stdout
    assert "Warning: Forcing merge despite lifecycle gate" not in force_only.stdout
    assert force_only_git.merged == []
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    assert unit.state == "unmerged"
    assert unit.merge_source is None

    forced_git = _MergeGit(tmp_path)
    with patch("gza.cli.git_ops.Git", lambda project_dir: forced_git):
        forced = invoke_gza(
            "merge",
            str(impl.id),
            "--force",
            "--ignore-verify-gate",
            "--project",
            str(tmp_path),
            cwd=tmp_path,
        )

    assert forced.returncode == 0
    assert "Warning: Forcing merge despite red verify gate" in forced.stdout
    assert "failing epoch head=same-head" in forced.stdout
    assert "verify command='./bin/timeout-tests'" in forced.stdout
    assert forced_git.merged == [(f"feature/completed-verify-fix-red-{case_name}", False)]
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    assert unit.merge_source == MERGE_SOURCE_MANUAL_FORCE


@pytest.mark.parametrize("verify_fix_status", ["failed", "stopped"])
def test_merge_force_refuses_failed_or_stopped_pre_merge_verify_fix_task(
    tmp_path: Path,
    verify_fix_status: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add(f"Implement {verify_fix_status} verify_fix refusal", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch=f"feature/{verify_fix_status}-verify-fix-refusal")
    assert impl.id is not None
    _add_completed_approved_review(store, based_on_task=impl, depends_on_task=impl)
    _persist_current_verify(tmp_path, store, owner_task=impl, source_task=impl, status="failed", exit_status="1")
    verify_fix = _add_verify_fix_for_current_epoch(store, impl=impl, status=verify_fix_status)

    _assert_verify_family_merge_refused(
        tmp_path,
        store,
        impl=impl,
        expected_text=f"verify_fix task {verify_fix.id} is {verify_fix_status}",
    )
    refreshed_fix = store.get(verify_fix.id)
    assert refreshed_fix is not None
    assert refreshed_fix.status == verify_fix_status


@pytest.mark.parametrize(
    ("proof_kind", "canonical_outcome", "legacy_scope", "expected_text"),
    [
        ("canonical", "{not-json", None, "invalid canonical completion proof"),
        (
            "legacy",
            None,
            json.dumps(
                {
                    "kind": "verify_fix_completion_outcome",
                    "schema_version": 1,
                    "no_source_changes": True,
                    "completion_head_sha": None,
                    "recovery_rerun_attempted": False,
                }
            ),
            "invalid legacy completion proof",
        ),
    ],
)
def test_merge_force_refuses_invalid_pre_merge_verify_fix_completion_proof(
    tmp_path: Path,
    proof_kind: str,
    canonical_outcome: str | None,
    legacy_scope: str | None,
    expected_text: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add(f"Implement invalid {proof_kind} verify_fix proof", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch=f"feature/invalid-{proof_kind}-verify-fix-proof")
    assert impl.id is not None
    _add_completed_approved_review(store, based_on_task=impl, depends_on_task=impl)
    verify_fix = _add_verify_fix_for_current_epoch(store, impl=impl, status="completed")
    verify_fix.completed_at = datetime.now(UTC)
    verify_fix.has_commits = False
    verify_fix.changed_diff = False
    verify_fix.review_verify_head_sha = "same-head"
    if canonical_outcome is not None:
        verify_fix.verify_fix_completion_outcome_json = canonical_outcome
    if legacy_scope is not None:
        verify_fix.verify_fix_completion_outcome_json = None
        verify_fix.review_scope = legacy_scope
        verify_fix.changed_diff = None
        verify_fix.review_verify_head_sha = None
    store.update(verify_fix)
    _persist_current_verify(
        tmp_path,
        store,
        owner_task=impl,
        source_task=impl,
        status="failed",
        command="./bin/timeout-tests",
        exit_status="timed out",
    )

    _assert_verify_family_merge_refused(
        tmp_path,
        store,
        impl=impl,
        expected_text=expected_text,
    )
    refreshed_fix = store.get(verify_fix.id)
    assert refreshed_fix is not None
    assert refreshed_fix.status == "completed"


def test_merge_force_refuses_unavailable_pre_merge_verify_fix_exact_head_proof(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement unavailable exact-head proof refusal", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/unavailable-exact-head-proof")
    assert impl.id is not None
    _add_completed_approved_review(store, based_on_task=impl, depends_on_task=impl)
    verify_fix = _add_verify_fix_for_current_epoch(store, impl=impl, status="completed")
    verify_fix.completed_at = datetime.now(UTC)
    verify_fix.has_commits = False
    verify_fix.changed_diff = None
    verify_fix.review_verify_head_sha = None
    store.update(verify_fix)
    _persist_current_verify(
        tmp_path,
        store,
        owner_task=impl,
        source_task=impl,
        status="failed",
        command="./bin/timeout-tests",
        exit_status="timed out",
    )

    def _fake_git() -> _MergeGit:
        fake_git = _MergeGit(tmp_path)

        def _rev_parse_if_exists(ref: str) -> str | None:
            if ref == impl.branch:
                in_legacy_completion_repair = any(
                    frame.function == "_prove_legacy_verify_fix_completion_repair" for frame in inspect.stack()
                )
                if in_legacy_completion_repair:
                    raise GitError("branch probe unavailable")
                return "same-head"
            return _MergeGit.rev_parse_if_exists(fake_git, ref)

        fake_git.rev_parse_if_exists = _rev_parse_if_exists  # type: ignore[method-assign]
        return fake_git

    _assert_verify_family_merge_refused(
        tmp_path,
        store,
        impl=impl,
        expected_text="exact-head proof is unavailable",
        fake_git_factory=_fake_git,
    )
    refreshed_fix = store.get(verify_fix.id)
    assert refreshed_fix is not None
    assert refreshed_fix.status == "completed"


def test_merge_force_refuses_pre_merge_verify_fix_representative_resolution_failure(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement representative resolution refusal", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/representative-resolution-refusal")
    assert impl.id is not None
    unrelated = store.add("Unrelated evidence source", task_type="implement")
    store.mark_completed(unrelated, has_commits=True, branch="feature/unrelated-evidence")
    assert unrelated.id is not None
    _add_completed_approved_review(store, based_on_task=impl, depends_on_task=impl)
    _persist_current_verify(
        tmp_path,
        store,
        owner_task=impl,
        source_task=unrelated,
        status="failed",
        exit_status="1",
    )

    _assert_verify_family_merge_refused(
        tmp_path,
        store,
        impl=impl,
        expected_text="could not resolve verify_fix representative task",
    )


def test_merge_force_refuses_malformed_red_verify_gate_proof_with_proof_requirement(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement malformed red proof refusal", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/malformed-red-proof-refusal")
    assert impl.id is not None
    review = _add_completed_approved_review(store, based_on_task=impl, depends_on_task=impl)
    _persist_current_green_verify(tmp_path, store, owner_task=impl, source_task=review)

    fake_git = _MergeGit(tmp_path)
    malformed_action = {
        "type": "needs_discussion",
        "description": "SKIP: verify gate proof is malformed",
        "verify_gate_phase": "pre_merge",
        "verify_gate_family": "verify_fix_routing",
        "red_verify_gate_proof": {
            "phase": "pre_merge",
            "reviewed_head_sha": "",
            "verify_command": "./bin/tests",
        },
        "needs_attention_reason": "verify-fix-proof-unavailable",
    }

    with (
        patch("gza.cli.git_ops.Git", lambda project_dir: fake_git),
        patch("gza.cli.git_ops.determine_next_action", return_value=malformed_action),
    ):
        result = invoke_gza(
            "merge",
            str(impl.id),
            "--force",
            "--ignore-verify-gate",
            "--project",
            str(tmp_path),
            cwd=tmp_path,
        )

    assert result.returncode == 1
    assert "verify gate proof is malformed" in result.stdout
    assert "Red verify gate bypass requires current failed pre-merge proof" in result.stdout
    assert "This current red verify-gate recovery state cannot be bypassed" not in result.stdout
    assert fake_git.merged == []
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    assert unit.state == "unmerged"
    assert unit.merge_source is None


def test_merge_ignore_verify_gate_without_force_refuses_before_resolution_or_git(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = _add_completed_legacy_impl(
        store,
        "Implement ignored verify gate option refusal",
        "feature/ignore-without-force",
    )
    assert impl.id is not None
    before = _snapshot_merge_refusal_state(store, impl.id)

    with (
        patch("gza.cli.git_ops.Config.load", side_effect=AssertionError("config must not load")),
        patch("gza.cli.git_ops.get_store", side_effect=AssertionError("store must not open")),
        patch("gza.cli.git_ops.Git", side_effect=AssertionError("git must not initialize")),
        patch("gza.cli.git_ops.determine_next_action", side_effect=AssertionError("planner must not run")),
    ):
        result = invoke_gza(
            "merge",
            str(impl.id),
            "--ignore-verify-gate",
            "--project",
            str(tmp_path),
            cwd=tmp_path,
        )

    assert result.returncode == 1
    assert "--ignore-verify-gate requires --force" in result.stdout
    assert _snapshot_merge_refusal_state(store, impl.id) == before
    assert store.resolve_merge_unit_for_task(impl.id) is None


def test_merge_force_rebase_resolve_refuses_before_terminal_planning_can_persist_merge_truth(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = _add_completed_legacy_impl(
        store,
        "Implement forced rebase resolve early refusal",
        "feature/force-rebase-resolve-early",
    )
    assert impl.id is not None
    before = _snapshot_merge_refusal_state(store, impl.id)

    with (
        patch("gza.cli.git_ops.Config.load", side_effect=AssertionError("config must not load")),
        patch("gza.cli.git_ops.get_store", side_effect=AssertionError("store must not open")),
        patch("gza.cli.git_ops.Git", side_effect=AssertionError("git must not initialize")),
        patch("gza.cli.git_ops.determine_next_action", side_effect=AssertionError("planner must not run")),
        patch("gza.cli.git_ops.invoke_provider_resolve", side_effect=AssertionError("provider must not run")),
    ):
        result = invoke_gza(
            "merge",
            str(impl.id),
            "--force",
            "--rebase",
            "--resolve",
            "--project",
            str(tmp_path),
            cwd=tmp_path,
        )

    assert result.returncode == 1
    assert "--force cannot be combined with --rebase --resolve" in result.stdout
    assert _snapshot_merge_refusal_state(store, impl.id) == before
    assert store.resolve_merge_unit_for_task(impl.id) is None


@pytest.mark.parametrize("verify_fix_shape", ["timeout-no-source", "ordinary-completed"])
@pytest.mark.parametrize(
    ("executor_status", "executor_message"),
    [
        ("success", "Verify gate passed for the current tip before merge."),
        ("skip", "SKIP: verify gate remained failed; merge is blocked."),
        ("skip", "SKIP: could not run or persist the verify gate for owner testproject-1; merge is blocked."),
    ],
)
def test_manual_merge_after_verify_fix_rearm_refreshes_pre_merge_gate(
    tmp_path: Path,
    verify_fix_shape: str,
    executor_status: str,
    executor_message: str,
) -> None:
    from gza.cli.advance_executor import AdvanceActionExecutionResult

    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add(f"Implement manual rearm {verify_fix_shape}", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch=f"feature/manual-rearm-{verify_fix_shape}")
    assert impl.id is not None
    _add_completed_approved_review(store, based_on_task=impl, depends_on_task=impl)
    command = "./bin/timeout-tests" if verify_fix_shape == "timeout-no-source" else "./bin/tests"

    if verify_fix_shape == "timeout-no-source":
        _add_completed_no_source_verify_fix(store, impl=impl, recovery_rerun_attempted=False)
        _persist_current_verify(
            tmp_path,
            store,
            owner_task=impl,
            source_task=impl,
            status="failed",
            command=command,
            exit_status="timed out",
        )
    else:
        verify_fix = _add_verify_fix_for_current_epoch(store, impl=impl, status="completed")
        verify_fix.completed_at = datetime.now(UTC)
        store.update(verify_fix)
        _persist_current_verify(
            tmp_path,
            store,
            owner_task=impl,
            source_task=impl,
            status="failed",
            command=command,
            exit_status="1",
        )

    store.record_parked_task_manual_rearm(
        subject_kind="task",
        subject_id=impl.id,
        attention_reason=PARK_REASON_VERIFY_FIX_FAILED,
        subject_task_id=impl.id,
    )

    fake_git = _MergeGit(tmp_path)
    captured_actions: list[dict] = []

    def fake_execute_advance_action(*, task, action, context):
        del task, context
        captured_actions.append(dict(action))
        if executor_status == "success":
            _persist_current_verify(
                tmp_path,
                store,
                owner_task=impl,
                source_task=impl,
                status="passed",
                command=command,
                exit_status="0",
            )
            return AdvanceActionExecutionResult(
                action_type="verify_gate",
                status="success",
                success_message=executor_message,
                handled_task_id=impl.id,
            )
        return AdvanceActionExecutionResult(
            action_type="verify_gate",
            status="skip",
            message=executor_message.replace("testproject-1", str(impl.id)),
            handled_task_id=impl.id,
        )

    with (
        patch("gza.cli.git_ops.Git", lambda project_dir: fake_git),
        patch("gza.cli.git_ops.execute_advance_action", side_effect=fake_execute_advance_action),
    ):
        result = invoke_gza(
            "merge",
            str(impl.id),
            "--project",
            str(tmp_path),
            cwd=tmp_path,
        )

    assert captured_actions
    assert captured_actions[0]["type"] == "verify_gate"
    assert captured_actions[0]["verify_gate_phase"] == "pre_merge"
    assert captured_actions[0]["verify_gate_explicit_refresh"] is True
    assert captured_actions[0]["description"] == "Run verify gate before merge"
    assert "before review" not in result.stdout
    assert "review is blocked" not in result.stdout
    if executor_status == "success":
        assert result.returncode == 0
        assert "Verify gate passed for the current tip before merge." in result.stdout
        assert fake_git.merged == [(f"feature/manual-rearm-{verify_fix_shape}", False)]
    else:
        assert result.returncode == 1
        assert "merge is blocked" in result.stdout
        assert fake_git.merged == []


class _ConflictingRebaseMergeGit(_MergeGit):
    def __init__(self, project_dir: Path, *, default_branch: str = "main") -> None:
        super().__init__(project_dir, default_branch=default_branch)
        self.checked_out: list[str] = []
        self.rebased: list[str] = []
        self.rebase_aborted = 0

    def checkout(self, branch: str) -> None:
        self.checked_out.append(branch)

    def rebase(self, target: str) -> None:
        self.rebased.append(target)
        raise GitError("rebase conflict")

    def rebase_abort(self) -> None:
        self.rebase_aborted += 1


@pytest.mark.parametrize(
    ("planned_action", "extra_flags"),
    [
        (
            {
                "type": "merge",
                "description": "Merge task",
            },
            (),
        ),
        (
            {
                "type": "needs_discussion",
                "description": "SKIP: verify gate is still red after completed verify_fix testproject-77",
                "needs_attention_reason": PARK_REASON_VERIFY_FIX_FAILED,
                "red_verify_gate_proof": {
                    "phase": "pre_merge",
                    "reviewed_head_sha": "bad-head",
                    "verify_command": "./bin/tests",
                },
            },
            ("--ignore-verify-gate",),
        ),
        (
            {
                "type": "needs_discussion",
                "description": "SKIP: required resolution-review metadata is missing or malformed",
                "needs_attention_reason": "resolution-review-metadata-invalid",
            },
            (),
        ),
    ],
)
def test_merge_force_rebase_resolve_refuses_before_conflict_resolution(
    tmp_path: Path,
    planned_action: dict,
    extra_flags: tuple[str, ...],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = _add_completed_legacy_impl(
        store,
        "Implement forced rebase resolve refusal",
        "feature/forced-rebase-resolve-refusal",
    )
    assert impl.id is not None
    review = _add_completed_approved_review(store, based_on_task=impl, depends_on_task=impl)
    _persist_current_green_verify(tmp_path, store, owner_task=impl, source_task=review)

    fake_git = _ConflictingRebaseMergeGit(tmp_path)
    with (
        patch("gza.cli.git_ops.Git", lambda project_dir: fake_git),
        patch("gza.cli.git_ops.determine_next_action", return_value=planned_action),
        patch("gza.cli.git_ops.invoke_provider_resolve") as invoke_provider_resolve,
    ):
        result = invoke_gza(
            "merge",
            str(impl.id),
            "--force",
            "--rebase",
            "--resolve",
            *extra_flags,
            "--project",
            str(tmp_path),
            cwd=tmp_path,
        )

    assert result.returncode == 1
    assert "--force cannot be combined with --rebase --resolve" in result.stdout
    assert "Warning: Forcing merge despite" not in result.stdout
    invoke_provider_resolve.assert_not_called()
    assert fake_git.checked_out == []
    assert fake_git.rebased == []
    assert fake_git.rebase_aborted == 0
    assert fake_git.merged == []
    assert store.resolve_merge_unit_for_task(impl.id) is None


def test_merge_rebase_resolve_without_force_still_resolves_conflicts(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement ordinary rebase resolve", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/ordinary-rebase-resolve")
    assert impl.id is not None
    review = _add_completed_approved_review(store, based_on_task=impl, depends_on_task=impl)
    _persist_current_green_verify(tmp_path, store, owner_task=impl, source_task=review)

    fake_git = _ConflictingRebaseMergeGit(tmp_path)
    with (
        patch("gza.cli.git_ops.Git", lambda project_dir: fake_git),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "merge"}),
        patch("gza.cli.git_ops.invoke_provider_resolve", return_value=True) as invoke_provider_resolve,
    ):
        result = invoke_gza(
            "merge",
            str(impl.id),
            "--rebase",
            "--resolve",
            "--project",
            str(tmp_path),
            cwd=tmp_path,
        )

    assert result.returncode == 0
    assert "Conflicts detected. Invoking provider to resolve" in result.stdout
    invoke_provider_resolve.assert_called_once()
    assert fake_git.checked_out == ["feature/ordinary-rebase-resolve", "main"]
    assert fake_git.rebased == ["main"]
    assert fake_git.merged == [("feature/ordinary-rebase-resolve", False)]
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    assert unit.state == "merged"
    assert unit.merge_source != MERGE_SOURCE_MANUAL_FORCE


@pytest.mark.parametrize(
    ("verify_fix_status", "expected_text"),
    [
        ("pending", "Spawn worker for pending verify_fix"),
        ("in_progress", "is in_progress"),
    ],
)
def test_merge_force_ignore_verify_gate_refuses_unavailable_pre_review_verify_fix_actions(
    tmp_path: Path,
    verify_fix_status: str,
    expected_text: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add(f"Implement unavailable verify evidence {verify_fix_status}", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch=f"feature/unavailable-verify-{verify_fix_status}")
    assert impl.id is not None
    _persist_current_verify(tmp_path, store, owner_task=impl, source_task=impl, status="unavailable", exit_status="unavailable")
    verify_fix = _add_verify_fix_for_current_epoch(store, impl=impl, status=verify_fix_status)

    fake_git = _MergeGit(tmp_path)
    with patch("gza.cli.git_ops.Git", lambda project_dir: fake_git):
        result = invoke_gza(
            "merge",
            str(impl.id),
            "--force",
            "--ignore-verify-gate",
            "--project",
            str(tmp_path),
            cwd=tmp_path,
        )

    assert result.returncode == 1
    assert expected_text in result.stdout
    assert "Live verify-fix tasks cannot be bypassed" in result.stdout
    assert "Warning: Forcing merge despite red verify gate" not in result.stdout
    assert "Warning: Forcing merge despite lifecycle gate" not in result.stdout
    assert fake_git.merged == []
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    assert unit.state == "unmerged"
    assert unit.merge_source is None
    refreshed_fix = store.get(verify_fix.id)
    assert refreshed_fix is not None
    assert refreshed_fix.status == verify_fix_status


@pytest.mark.parametrize(
    ("verify_fix_status", "expected_text"),
    [
        ("pending", "Live verify-fix tasks cannot be bypassed"),
        ("in_progress", "Live verify-fix tasks cannot be bypassed"),
    ],
)
def test_merge_force_ignore_verify_gate_refuses_live_pre_merge_verify_fix_tasks(
    tmp_path: Path,
    verify_fix_status: str,
    expected_text: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add(f"Implement live verify evidence {verify_fix_status}", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch=f"feature/live-verify-{verify_fix_status}")
    assert impl.id is not None
    _add_completed_approved_review(store, based_on_task=impl, depends_on_task=impl)
    _persist_current_verify(
        tmp_path,
        store,
        owner_task=impl,
        source_task=impl,
        status="failed",
        exit_status="1",
    )
    verify_fix = _add_verify_fix_for_current_epoch(store, impl=impl, status=verify_fix_status)

    fake_git = _MergeGit(tmp_path)
    with (
        patch("gza.cli.git_ops.Git", lambda project_dir: fake_git),
        patch("gza.cli.git_ops.invoke_provider_resolve") as invoke_provider_resolve,
    ):
        result = invoke_gza(
            "merge",
            str(impl.id),
            "--force",
            "--ignore-verify-gate",
            "--project",
            str(tmp_path),
            cwd=tmp_path,
        )

    assert result.returncode == 1
    assert expected_text in result.stdout
    assert "Warning: Forcing merge despite red verify gate" not in result.stdout
    assert "Warning: Forcing merge despite lifecycle gate" not in result.stdout
    invoke_provider_resolve.assert_not_called()
    assert fake_git.merged == []
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    assert unit.state == "unmerged"
    assert unit.merge_source is None
    refreshed_fix = store.get(verify_fix.id)
    assert refreshed_fix is not None
    assert refreshed_fix.status == verify_fix_status


def test_merge_force_ignore_verify_gate_refuses_pre_review_red_action_when_review_is_stale(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement stale review with current red verify", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/stale-review-current-red")
    assert impl.id is not None
    review = _add_completed_approved_review(store, based_on_task=impl, depends_on_task=impl)
    review.review_verify_head_sha = "old-head"
    store.update(review)
    improve = store.add("Improve after stale review", task_type="improve", based_on=impl.id, depends_on=review.id)
    improve.status = "completed"
    improve.completed_at = datetime.now(UTC)
    improve.branch = impl.branch
    improve.has_commits = True
    store.update(improve)
    _persist_current_verify(tmp_path, store, owner_task=impl, source_task=impl, status="failed", exit_status="1")
    _add_verify_fix_for_current_epoch(store, impl=impl, status="pending")

    fake_git = _MergeGit(tmp_path)
    with patch("gza.cli.git_ops.Git", lambda project_dir: fake_git):
        result = invoke_gza(
            "merge",
            str(impl.id),
            "--force",
            "--ignore-verify-gate",
            "--project",
            str(tmp_path),
            cwd=tmp_path,
        )

    assert result.returncode == 1
    assert "Spawn worker for pending verify_fix" in result.stdout
    assert "Live verify-fix tasks cannot be bypassed" in result.stdout
    assert "Warning: Forcing merge despite red verify gate" not in result.stdout
    assert fake_git.merged == []
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    assert unit.state == "unmerged"
    assert unit.merge_source is None


def test_merge_force_malformed_verify_fix_failed_does_not_fall_through_to_generic_force(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement malformed verify-fix-failed action", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/malformed-verify-fix-failed")
    assert impl.id is not None
    review = _add_completed_approved_review(store, based_on_task=impl, depends_on_task=impl)
    _persist_current_green_verify(tmp_path, store, owner_task=impl, source_task=review)

    fake_git = _MergeGit(tmp_path)
    malformed_action = {
        "type": "needs_discussion",
        "description": "SKIP: verify gate is still red after completed verify_fix testproject-77",
        "needs_attention_reason": PARK_REASON_VERIFY_FIX_FAILED,
    }

    with (
        patch("gza.cli.git_ops.Git", lambda project_dir: fake_git),
        patch("gza.cli.git_ops.determine_next_action", return_value=malformed_action),
    ):
        result = invoke_gza(
            "merge",
            str(impl.id),
            "--force",
            "--project",
            str(tmp_path),
            cwd=tmp_path,
        )

    assert result.returncode == 1
    assert "Red verify gate bypass requires current failed pre-merge proof" in result.stdout
    assert "Warning: Forcing merge despite lifecycle gate" not in result.stdout
    assert "Warning: Forcing merge despite red verify gate" not in result.stdout
    assert fake_git.merged == []
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    assert unit.merge_source is None


def test_merge_force_ignore_verify_gate_warns_with_stored_failed_command_when_config_changed(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement changed verify command red gate", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/changed-verify-command")
    assert impl.id is not None
    review = _add_completed_approved_review(store, based_on_task=impl, depends_on_task=impl)
    _persist_current_verify(
        tmp_path,
        store,
        owner_task=impl,
        source_task=impl,
        status="failed",
        command="./bin/old-tests",
        exit_status="1",
    )
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            "verify_command: ./bin/old-tests",
            "verify_command: ./bin/new-tests",
        ),
        encoding="utf-8",
    )

    fake_git = _MergeGit(tmp_path)
    with patch("gza.cli.git_ops.Git", lambda project_dir: fake_git):
        result = invoke_gza(
            "merge",
            str(impl.id),
            "--force",
            "--ignore-verify-gate",
            "--project",
            str(tmp_path),
            cwd=tmp_path,
        )

    assert result.returncode == 0
    assert "Warning: Forcing merge despite red verify gate" in result.stdout
    assert "failing epoch head=same-head" in result.stdout
    assert "verify command='./bin/old-tests'" in result.stdout
    assert "./bin/new-tests" not in result.stdout
    assert fake_git.merged == [("feature/changed-verify-command", False)]
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    assert unit.merge_source == MERGE_SOURCE_MANUAL_FORCE


def test_merge_force_ignore_verify_gate_still_refuses_git_conflicts(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement conflicted ignored verify gate path", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/conflicted-red-verify-gate")
    assert impl.id is not None

    review = _add_completed_approved_review(store, based_on_task=impl, depends_on_task=impl)
    _persist_current_green_verify(tmp_path, store, owner_task=impl, source_task=review)

    fake_git = _MergeGit(tmp_path)
    fake_git.can_merge = lambda branch, into=None: False  # type: ignore[method-assign]
    verify_action = {
        "type": "create_verify_fix",
        "description": "Create verify_fix task for verify epoch at head bad-head",
        "verify_epoch": SimpleNamespace(reviewed_head_sha="bad-head", verify_command="./bin/tests"),
        "red_verify_gate_proof": {
            "phase": "pre_merge",
            "reviewed_head_sha": "bad-head",
            "verify_command": "./bin/tests",
        },
    }

    with (
        patch("gza.cli.git_ops.Git", lambda project_dir: fake_git),
        patch("gza.cli.git_ops.determine_next_action", return_value=verify_action),
    ):
        result = invoke_gza(
            "merge",
            str(impl.id),
            "--force",
            "--ignore-verify-gate",
            "--project",
            str(tmp_path),
            cwd=tmp_path,
        )

    assert result.returncode == 1
    assert "Warning: Forcing merge despite red verify gate" in result.stdout
    assert "has conflicts against 'main' and cannot be merged cleanly" in result.stdout
    assert fake_git.merged == []
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    assert unit.state == "unmerged"
    assert unit.merge_source is None


def test_merge_force_ignore_verify_gate_still_refuses_open_review_blockers(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement blocked ignored verify gate path", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/blocked-red-verify-gate")
    assert impl.id is not None

    review = _add_completed_approved_review(store, based_on_task=impl, depends_on_task=impl)
    _persist_current_green_verify(tmp_path, store, owner_task=impl, source_task=review)

    fake_git = _MergeGit(tmp_path)
    blocker = SimpleNamespace(severity="BLOCKER")
    verify_action = {
        "type": "create_verify_fix",
        "description": "Create verify_fix task for verify epoch at head bad-head",
        "verify_epoch": SimpleNamespace(reviewed_head_sha="bad-head", verify_command="./bin/tests"),
        "red_verify_gate_proof": {
            "phase": "pre_merge",
            "reviewed_head_sha": "bad-head",
            "verify_command": "./bin/tests",
        },
    }

    with (
        patch("gza.cli.git_ops.Git", lambda project_dir: fake_git),
        patch("gza.cli.git_ops.determine_next_action", return_value=verify_action),
        patch(
            "gza.cli.git_ops.get_review_report",
            return_value=SimpleNamespace(verdict="CHANGES_REQUESTED", findings=(blocker,), format_version="v2"),
        ),
        patch("gza.cli.git_ops.get_review_content", return_value="review with one blocker"),
        patch("gza.cli.git_ops.summarize_review_blockers", return_value=SimpleNamespace(blocker_count=1)),
    ):
        result = invoke_gza(
            "merge",
            str(impl.id),
            "--force",
            "--ignore-verify-gate",
            "--project",
            str(tmp_path),
            cwd=tmp_path,
        )

    assert result.returncode == 1
    assert "Warning: Forcing merge despite red verify gate" in result.stdout
    assert f"Error: Task {impl.id} has open BLOCKER findings in review {review.id}." in result.stdout
    assert "Use --defer-blockers" in result.stdout
    assert fake_git.merged == []
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    assert unit.state == "unmerged"
    assert unit.merge_source is None


def test_merge_valid_and_missing_explicit_task_ids_report_missing_without_partial_merge(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement shared branch", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/shared")
    assert impl.id is not None

    fake_git = _MergeGit(tmp_path)
    with patch("gza.cli.git_ops.Git", lambda project_dir: fake_git):
        result = invoke_gza(
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
