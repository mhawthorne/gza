"""Tests for auto-advancing completed plans via `gza advance`."""

import argparse
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from gza.cli.git_ops import cmd_advance
from gza.git import GitError
from gza.recovery_engine import _MergeContext
from tests.cli.conftest import make_store, setup_config


def _advance_args(tmp_path: Path, **overrides) -> argparse.Namespace:
    args = argparse.Namespace(
        project_dir=tmp_path,
        task_id=None,
        dry_run=False,
        auto=True,
        max=None,
        batch=None,
        no_docker=True,
        force=False,
        plans=False,
        unimplemented=False,
        create=False,
        no_resume_failed=False,
        max_resume_attempts=None,
        advance_type=None,
        new=False,
        max_review_cycles=None,
        squash_threshold=None,
    )
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


def _mock_git(*, current_branch: str = "main", can_merge: bool = True) -> Mock:
    git = Mock()
    git.current_branch.return_value = current_branch
    git.can_merge.return_value = can_merge
    return git


def _create_completed_plan(store, prompt="Design the feature"):
    plan = store.add(prompt, task_type="plan")
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)
    return plan


def _create_completed_implement(store, prompt="Implement feature", based_on=None):
    task = store.add(prompt, task_type="implement", based_on=based_on)
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = f"feature/{task.id}"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)
    assert task.id is not None
    unit = store.create_merge_unit(
        source_branch=task.branch,
        target_branch="main",
        owner_task_id=task.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(task.id, unit.id, "owner")
    store.dual_write_legacy_merge_status(unit.id)
    return task


def _create_completed_review(store, impl, *, verdict: str = "APPROVED"):
    review = store.add(f"Review {impl.id}", task_type="review", depends_on=impl.id, based_on=impl.id)
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = f"**Verdict: {verdict}**"
    store.update(review)
    return review


def _merge_context_without_repo_state(*, default_branch: str = "main") -> _MergeContext:
    class _MergeGit:
        def branch_exists(self, branch: str) -> bool:
            return False

        def ref_exists(self, ref: str) -> bool:
            return False

        def is_merged(self, branch: str, into: str) -> bool:
            return False

    return _MergeContext(git=_MergeGit(), default_branch=default_branch)


@pytest.fixture(autouse=True)
def _stub_accidental_real_git_probes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("gza.git.Git.default_branch", lambda _self: "main")
    monkeypatch.setattr("gza.git.Git.local_branch_names", lambda _self: frozenset())
    monkeypatch.setattr("gza.git.Git.branch_exists", lambda _self, _branch: False)
    monkeypatch.setattr("gza.git.Git.ref_exists", lambda _self, _ref: False)
    monkeypatch.setattr("gza.git.Git.remote_branch_exists", lambda _self, _branch, remote="origin": False)
    monkeypatch.setattr("gza.git.Git.is_merged", lambda _self, _source, _into="main": False)


def test_advance_creates_plan_review_for_completed_plan(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    plan = _create_completed_plan(store, "Design auth system")
    plan.slug = "20260305-design-auth-system-2"
    store.update(plan)

    spawn_calls: list[str | None] = []

    def fake_spawn(_worker_args, _config, task_id=None, **_kw):
        spawn_calls.append(task_id)
        return 0

    with (
        patch("gza.cli.git_ops.Git", return_value=_mock_git()),
        patch("gza.cli.git_ops.list_failed_tasks_for_recovery", return_value=[]),
        patch("gza.recovery_engine.list_failed_tasks_for_recovery", return_value=[]),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.cli.git_ops._prepare_task_for_immediate_execution", side_effect=lambda _c, task, **_k: task),
        patch("gza.cli.advance_executor._prepare_task_for_reserved_launch", side_effect=lambda _c, task, **_k: task),
        patch("gza.cli.git_ops._spawn_background_worker", side_effect=fake_spawn),
    ):
        rc = cmd_advance(_advance_args(tmp_path))

    assert rc == 0
    assert "Created plan review task" in capsys.readouterr().out
    plan_review_tasks = [task for task in store.get_all() if task.task_type == "plan_review"]
    assert len(plan_review_tasks) == 1
    assert plan_review_tasks[0].depends_on == plan.id
    assert plan_review_tasks[0].based_on is None
    assert spawn_calls == [plan_review_tasks[0].id]


def test_advance_create_plan_review_inherits_tags_from_completed_plan(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    plan = _create_completed_plan(store, "Design auth slice")
    plan.tags = ("lifecycle", "planner")
    store.update(plan)

    with (
        patch("gza.cli.git_ops.Git", return_value=_mock_git()),
        patch("gza.cli.git_ops.list_failed_tasks_for_recovery", return_value=[]),
        patch("gza.recovery_engine.list_failed_tasks_for_recovery", return_value=[]),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.cli.git_ops._prepare_task_for_immediate_execution", side_effect=lambda _c, task, **_k: task),
        patch("gza.cli.advance_executor._prepare_task_for_reserved_launch", side_effect=lambda _c, task, **_k: task),
        patch("gza.cli.git_ops._spawn_background_worker", return_value=0),
    ):
        rc = cmd_advance(_advance_args(tmp_path))

    assert rc == 0
    assert "Created plan review task" in capsys.readouterr().out
    plan_review_tasks = [task for task in store.get_all() if task.task_type == "plan_review"]
    assert len(plan_review_tasks) == 1
    assert plan_review_tasks[0].tags == plan.tags


def test_advance_auto_implement_inherits_all_parent_tags(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "require_plan_review_before_implement: false\n")
    store = make_store(tmp_path)
    plan = _create_completed_plan(store, "Design recovery slice")
    plan.tags = ("202606-recovery", "v0.5.0")
    store.update(plan)

    with (
        patch("gza.cli.git_ops.Git", return_value=_mock_git()),
        patch("gza.cli.git_ops.list_failed_tasks_for_recovery", return_value=[]),
        patch("gza.recovery_engine.list_failed_tasks_for_recovery", return_value=[]),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.cli.git_ops._prepare_task_for_immediate_execution", side_effect=lambda _c, task, **_k: task),
        patch("gza.cli.advance_executor._prepare_task_for_reserved_launch", side_effect=lambda _c, task, **_k: task),
        patch("gza.cli.git_ops._spawn_background_worker", return_value=0),
    ):
        rc = cmd_advance(_advance_args(tmp_path))

    assert rc == 0
    assert "Created implement task" in capsys.readouterr().out
    implement_tasks = [task for task in store.get_all() if task.task_type == "implement"]
    assert len(implement_tasks) == 1
    assert implement_tasks[0].depends_on == plan.id
    assert implement_tasks[0].tags == plan.tags


def test_advance_create_plan_review_startup_failure_rolls_back_child_and_skips_spawn(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _create_completed_plan(store, "Design auth system")

    with (
        patch("gza.cli.git_ops.Git", return_value=_mock_git()),
        patch("gza.cli.git_ops.list_failed_tasks_for_recovery", return_value=[]),
        patch("gza.recovery_engine.list_failed_tasks_for_recovery", return_value=[]),
        patch("gza.cli._common.prepare_task_startup_phase", side_effect=RuntimeError("creator boom")),
        patch(
            "gza.cli.git_ops._spawn_background_worker",
            side_effect=AssertionError("worker should not spawn"),
        ),
    ):
        rc = cmd_advance(_advance_args(tmp_path))

    assert rc == 1
    output = capsys.readouterr()
    assert "creator boom" in output.err
    assert "Created plan review task" not in output.out
    plan_review_tasks = [task for task in store.get_all() if task.task_type == "plan_review"]
    assert plan_review_tasks == []
    logs_dir = tmp_path / ".gza" / "logs"
    if logs_dir.exists():
        assert list(logs_dir.iterdir()) == []
    workers_dir = tmp_path / ".gza" / "workers"
    if workers_dir.exists():
        assert list(workers_dir.iterdir()) == []


def test_advance_skips_plan_with_existing_implement(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    plan = _create_completed_plan(store, "Design auth system")
    store.add("Implement auth", task_type="implement", based_on=plan.id)

    with patch("gza.cli.git_ops.Git", return_value=_mock_git()):
        rc = cmd_advance(_advance_args(tmp_path, task_id=plan.id, dry_run=True))

    assert rc == 0
    assert "implement task already exists" in capsys.readouterr().out


def test_advance_does_not_create_implement_for_held_plan(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    plan = _create_completed_plan(store, "Design auth system")
    plan.auto_implement = False
    store.update(plan)

    with (
        patch("gza.cli.git_ops.Git", return_value=_mock_git()),
        patch(
            "gza.cli.git_ops._spawn_background_worker",
            side_effect=AssertionError("held plan should not spawn an implement worker"),
        ),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=plan.id, dry_run=True))

    output = capsys.readouterr().out
    assert rc == 0
    assert "Awaiting human review" in output
    assert f"uv run gza implement {plan.id}" in output
    assert [task for task in store.get_all() if task.task_type == "implement"] == []


def test_advance_type_plan_filters_to_plans_only(tmp_path: Path, capsys) -> None:
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "require_review_before_merge: false\n"
    )
    store = make_store(tmp_path)
    plan = _create_completed_plan(store, "Design feature X")
    _create_completed_implement(store)

    with patch("gza.cli.git_ops.Git", return_value=_mock_git()):
        rc = cmd_advance(_advance_args(tmp_path, dry_run=True, advance_type="plan"))

    output = capsys.readouterr().out
    assert rc == 0
    assert str(plan.id) in output
    assert "Create and start plan review" in output
    assert "Merge" not in output


def test_advance_type_implement_filters_to_implements_only(tmp_path: Path, capsys) -> None:
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "require_review_before_merge: false\n"
    )
    store = make_store(tmp_path)
    _create_completed_plan(store, "Design feature X")
    impl = _create_completed_implement(store)

    with patch("gza.cli.git_ops.Git", return_value=_mock_git()):
        rc = cmd_advance(_advance_args(tmp_path, dry_run=True, advance_type="implement"))

    output = capsys.readouterr().out
    assert rc == 0
    assert str(impl.id) in output
    assert "Merge task (no review yet)" in output
    assert "Create and start implement" not in output


def test_advance_dry_run_warns_once_when_failed_task_branch_reachability_is_unavailable(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Recover failed work", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.branch = "feature/recovery-warning"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    class _BrokenMergeGit:
        def branch_exists(self, branch: str) -> bool:
            return bool(branch)

        def is_merged(self, branch: str, into: str) -> bool:
            raise GitError("simulated reachability failure")

    with (
        patch("gza.cli.git_ops.Git", return_value=_mock_git()),
        patch(
            "gza.recovery_engine._load_merge_context",
            lambda _project_dir=None: _MergeContext(git=_BrokenMergeGit(), default_branch="main"),
        ),
    ):
        rc = cmd_advance(_advance_args(tmp_path, dry_run=True))

    captured = capsys.readouterr()
    assert rc == 0
    assert "Would advance 1 task(s):" in captured.out
    assert str(failed.id) in captured.out
    assert "Retry failed task (INFRASTRUCTURE_ERROR)" in captured.out
    assert captured.err.count("Warning: Failed-task recovery could not inspect repository branch reachability;") == 1
    assert "git branch reachability suppression is unavailable for this run" in captured.err
    assert "metadata-based same-lineage merged-task suppression may still apply" in captured.err
    assert "simulated reachability failure" in captured.err


def test_advance_no_resume_failed_keeps_lifecycle_merge_rows_and_filters_recovery_only_rows(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = _create_completed_implement(store, "Implement mergeable owner")
    assert impl.id is not None
    _create_completed_review(store, impl)

    failed_rebase = store.add(
        "Failed rebase descendant",
        task_type="rebase",
        based_on=impl.id,
        same_branch=True,
    )
    assert failed_rebase.id is not None
    failed_rebase.status = "failed"
    failed_rebase.failure_reason = "MERGE_CONFLICT"
    failed_rebase.completed_at = datetime.now(UTC)
    failed_rebase.branch = impl.branch
    failed_rebase.has_commits = True
    store.update(failed_rebase)
    store.get_or_create_merge_unit_for_task(failed_rebase)

    failed_impl = store.add("Recover failed work", task_type="implement")
    assert failed_impl.id is not None
    failed_impl.status = "failed"
    failed_impl.failure_reason = "MAX_TURNS"
    failed_impl.session_id = "sess-failed"
    failed_impl.branch = "feature/recovery-only"
    failed_impl.completed_at = datetime.now(UTC)
    store.update(failed_impl)

    with (
        patch("gza.cli.git_ops.Git", return_value=_mock_git()),
        patch(
            "gza.recovery_engine._load_merge_context",
            lambda _project_dir=None: _merge_context_without_repo_state(),
        ),
    ):
        rc = cmd_advance(_advance_args(tmp_path, dry_run=True))
    captured = capsys.readouterr()

    assert rc == 0
    assert "Would advance 1 task(s):" in captured.out
    assert str(impl.id) in captured.out
    assert "reason=rebase-failed-needs-manual-resolution" in captured.out
    assert str(failed_impl.id) in captured.out
    assert "Resume failed task (MAX_TURNS)" in captured.out

    with (
        patch("gza.cli.git_ops.Git", return_value=_mock_git()),
        patch(
            "gza.recovery_engine._load_merge_context",
            lambda _project_dir=None: _merge_context_without_repo_state(),
        ),
    ):
        rc = cmd_advance(_advance_args(tmp_path, dry_run=True, no_resume_failed=True))
    captured = capsys.readouterr()

    assert rc == 0
    assert "Would advance 1 task(s):" not in captured.out
    assert str(impl.id) in captured.out
    assert "reason=rebase-failed-needs-manual-resolution" in captured.out
    assert str(failed_impl.id) not in captured.out
    assert "Resume failed task (MAX_TURNS)" not in captured.out
    assert "No eligible tasks to advance" not in captured.out


def test_advance_create_implement_respects_batch_limit(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _create_completed_plan(store, "Plan A")
    _create_completed_plan(store, "Plan B")

    spawn_calls: list[str | None] = []

    def fake_spawn(_worker_args, _config, task_id=None, **_kw):
        spawn_calls.append(task_id)
        return 0

    with (
        patch("gza.cli.git_ops.Git", return_value=_mock_git()),
        patch("gza.cli.git_ops.list_failed_tasks_for_recovery", return_value=[]),
        patch("gza.recovery_engine.list_failed_tasks_for_recovery", return_value=[]),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.cli.git_ops._prepare_task_for_immediate_execution", side_effect=lambda _c, task, **_k: task),
        patch("gza.cli.advance_executor._prepare_task_for_reserved_launch", side_effect=lambda _c, task, **_k: task),
        patch("gza.cli.git_ops._spawn_background_worker", side_effect=fake_spawn),
    ):
        rc = cmd_advance(_advance_args(tmp_path, batch=1))

    output = capsys.readouterr().out
    assert rc == 0
    assert len(spawn_calls) == 1
    assert "batch limit reached" in output


def test_advance_dry_run_uses_post_rebase_review_after_later_completed_rebase(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _create_completed_implement(store, "Implement feature with recovery")
    review = _create_completed_review(store, impl)
    review.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    store.update(review)

    failed_rebase = store.add(
        "Failed rebase descendant",
        task_type="rebase",
        based_on=impl.id,
        same_branch=True,
    )
    assert failed_rebase.id is not None
    failed_rebase.status = "failed"
    failed_rebase.failure_reason = "MERGE_CONFLICT"
    failed_rebase.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    failed_rebase.branch = impl.branch
    failed_rebase.has_commits = True
    store.update(failed_rebase)

    completed_rebase = store.add(
        "Recovered rebase descendant",
        task_type="rebase",
        based_on=impl.id,
        same_branch=True,
    )
    assert completed_rebase.id is not None
    completed_rebase.status = "completed"
    completed_rebase.completed_at = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    completed_rebase.branch = impl.branch
    completed_rebase.has_commits = True
    store.update(completed_rebase)

    with (
        patch("gza.cli.git_ops.Git", return_value=_mock_git()),
        patch(
            "gza.recovery_engine._load_merge_context",
            lambda _project_dir=None: _merge_context_without_repo_state(),
        ),
        patch(
            "gza.advance_engine.get_review_report",
            return_value=SimpleNamespace(
                verdict="APPROVED",
                findings=(),
                format_version="legacy",
            ),
        ),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, dry_run=True))

    captured = capsys.readouterr()
    assert rc == 0
    assert "Would advance 1 task(s):" in captured.out
    assert str(impl.id) in captured.out
    assert "Create review (rebase" in captured.out
    assert "change unknown" in captured.out
    assert "reason=rebase-failed-needs-manual-resolution" not in captured.out


def test_advance_explicit_impl_uses_canonical_target_and_skips_orphan_rebase_branch(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = _create_completed_implement(store, "Implement feature")
    assert impl.id is not None
    _create_completed_review(store, impl, verdict="APPROVED")

    orphan = store.add("Completed orphan rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    assert orphan.id is not None
    orphan.status = "completed"
    orphan.completed_at = datetime.now(UTC)
    orphan.branch = "feature/orphan"
    orphan.merge_status = "unmerged"
    orphan.has_commits = True
    store.update(orphan)

    orphan_unit = store.create_merge_unit(
        source_branch=orphan.branch,
        target_branch="main",
        owner_task_id=orphan.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(orphan.id, orphan_unit.id, "owner")
    store.dual_write_legacy_merge_status(orphan_unit.id)

    outputs: list[str] = []
    for current_branch in ("main", impl.branch):
        with patch("gza.cli.git_ops.Git", return_value=_mock_git(current_branch=current_branch)):
            rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, dry_run=True))
        assert rc == 0
        captured = capsys.readouterr()
        outputs.append(captured.out)
        assert "Would advance 1 task(s):" in captured.out
        assert str(impl.id) in captured.out
        assert "Merge (review APPROVED)" in captured.out
        assert str(orphan.id) not in captured.out
        assert "Merge task (no review yet)" not in captured.out

    assert outputs[0] == outputs[1]


def test_advance_explicit_impl_reports_already_merged_when_branch_is_reachable_but_merge_state_is_stale(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = _create_completed_implement(store, "Implement feature")
    assert impl.id is not None
    _create_completed_review(store, impl, verdict="APPROVED")

    rebase = store.add("Completed rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    assert rebase.id is not None
    rebase.status = "completed"
    rebase.completed_at = datetime.now(UTC)
    rebase.branch = impl.branch
    rebase.has_commits = True
    rebase.changed_diff = True
    store.update(rebase)

    git = _mock_git(can_merge=False)
    git.default_branch.return_value = "main"
    git.branch_exists.return_value = True
    git.ref_exists.return_value = False
    git.is_merged.return_value = True

    with patch("gza.cli.git_ops.Git", return_value=git):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, dry_run=True))

    captured = capsys.readouterr()
    assert rc == 0
    assert f"Task {impl.id} is already merged" in captured.out
    assert "Would advance" not in captured.out


def test_advance_explicit_impl_prefers_fresh_remote_over_stale_legacy_local_ref(
    tmp_path: Path,
    capsys,
) -> None:
    from gza.git import ResolvedMergeSourceRef

    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = _create_completed_implement(store, "Implement feature")
    assert impl.id is not None
    _create_completed_review(store, impl, verdict="APPROVED")

    rebase = store.add("Completed rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    assert rebase.id is not None
    rebase.status = "completed"
    rebase.completed_at = datetime.now(UTC)
    rebase.branch = impl.branch
    rebase.has_commits = True
    rebase.changed_diff = True
    store.update(rebase)

    git = _mock_git(can_merge=False)
    git.default_branch.return_value = "main"
    git.branch_exists.return_value = True
    git.ref_exists.return_value = True
    git.resolve_merge_source_ref.return_value = impl.branch
    git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(f"origin/{impl.branch}")
    git.is_merged.side_effect = lambda source_ref, target_branch: (
        source_ref == f"origin/{impl.branch}" and target_branch == "main"
    )

    with patch("gza.cli.git_ops.Git", return_value=git):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, dry_run=True))

    captured = capsys.readouterr()
    assert rc == 0
    assert f"Task {impl.id} is already merged" in captured.out
    assert "Would advance" not in captured.out


def test_advance_explicit_impl_conflict_plan_skips_orphan_rebase_branch_for_non_merge_action(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = _create_completed_implement(store, "Implement feature")
    assert impl.id is not None

    orphan = store.add("Completed orphan rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    assert orphan.id is not None
    orphan.status = "completed"
    orphan.completed_at = datetime.now(UTC)
    orphan.branch = "feature/orphan"
    orphan.merge_status = "unmerged"
    orphan.has_commits = True
    store.update(orphan)

    orphan_unit = store.create_merge_unit(
        source_branch=orphan.branch,
        target_branch="main",
        owner_task_id=orphan.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(orphan.id, orphan_unit.id, "owner")
    store.dual_write_legacy_merge_status(orphan_unit.id)

    with patch("gza.cli.git_ops.Git", return_value=_mock_git(can_merge=False)):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, dry_run=True))

    captured = capsys.readouterr()
    assert rc == 0
    assert "Would advance 1 task(s):" in captured.out
    assert str(impl.id) in captured.out
    assert "rebase --resolve (conflicts detected)" in captured.out
    assert str(orphan.id) not in captured.out


def test_advance_explicit_task_without_merge_unit_uses_strict_non_main_default_target(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/no-merge-unit"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    called_targets: list[str] = []

    def _record_target(*args, **kwargs):
        called_targets.append(args[4])
        return {"type": "skip", "description": "no-op for target capture"}

    with (
        patch("gza.cli.git_ops.Git", return_value=_mock_git(current_branch="feature/local")),
        patch("gza.git.Git.default_branch", return_value="release"),
        patch("gza.cli.git_ops.determine_next_action", side_effect=_record_target),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, dry_run=True))

    assert rc == 0
    assert called_targets == ["release"]


def test_advance_explicit_task_errors_when_default_target_cannot_be_resolved(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/no-default-target"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    with patch("gza.git.Git.default_branch", side_effect=RuntimeError("git failure")):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, dry_run=True))

    captured = capsys.readouterr()
    assert rc == 1
    assert "Could not determine default merge target" in captured.err
    assert "git failure" in captured.err
    assert "Would advance" not in captured.out


def test_advance_dry_run_shows_attention_for_orphan_owned_merge_unit_without_noop_banner(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "in_progress"
    impl.branch = "feature/canonical"
    impl.has_commits = True
    store.update(impl)

    orphan = store.add("Completed orphan rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    assert orphan.id is not None
    orphan.status = "completed"
    orphan.completed_at = datetime.now(UTC)
    orphan.branch = "feature/orphan"
    orphan.merge_status = "unmerged"
    orphan.has_commits = True
    store.update(orphan)

    orphan_unit = store.create_merge_unit(
        source_branch=orphan.branch,
        target_branch="main",
        owner_task_id=orphan.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(orphan.id, orphan_unit.id, "owner")
    store.dual_write_legacy_merge_status(orphan_unit.id)

    with patch("gza.cli.git_ops.Git", return_value=_mock_git()):
        rc = cmd_advance(_advance_args(tmp_path, dry_run=True))

    captured = capsys.readouterr()
    assert rc == 0
    assert "Would advance" not in captured.out
    assert "No eligible tasks to advance" not in captured.out
    assert "Needs attention" in captured.out
    assert str(impl.id) in captured.out
    assert "no descendant on the impl branch" in captured.out


def test_advance_new_pending_implement_iterate_spawn_marks_auto_iterate(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    pending_impl = store.add("Implement queued task", task_type="implement")

    iterate_calls: list[dict[str, object]] = []

    def fake_spawn_iterate(_args, _config, impl_task, **kwargs):
        iterate_calls.append(
            {
                "task_id": impl_task.id,
                "auto_iterate": kwargs.get("auto_iterate"),
                "max_iterations": kwargs.get("max_iterations"),
                "prepared_task_id": kwargs.get("prepared_task_id"),
                "prepared_resume": kwargs.get("prepared_resume"),
                "prepared_phase": kwargs.get("prepared_phase"),
            }
        )
        return 0

    with (
        patch("gza.cli.git_ops.Git", return_value=_mock_git()),
        patch("gza.cli.git_ops.list_failed_tasks_for_recovery", return_value=[]),
        patch("gza.recovery_engine.list_failed_tasks_for_recovery", return_value=[]),
        patch("gza.cli.git_ops._advance_uses_iterate", return_value=True),
        patch("gza.cli.git_ops._prepare_task_for_immediate_execution", side_effect=lambda _c, task, **_k: task),
        patch("gza.cli.git_ops._spawn_background_iterate_worker", side_effect=fake_spawn_iterate),
    ):
        rc = cmd_advance(_advance_args(tmp_path, batch=1, new=True))

    assert rc == 0
    assert iterate_calls == [
        {
            "task_id": pending_impl.id,
            "auto_iterate": True,
            "max_iterations": 3,
            "prepared_task_id": pending_impl.id,
            "prepared_resume": False,
            "prepared_phase": "preloop",
        }
    ]


def test_advance_new_pending_resume_row_on_empty_branch_preserves_resume_startup(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    pending_impl = store.add("Implement queued resume task", task_type="implement", recovery_origin="resume")
    assert pending_impl.id is not None
    pending_impl.status = "pending"
    pending_impl.session_id = "sess-advance-pending"
    pending_impl.branch = "feature/advance-pending-empty-resume"
    store.update(pending_impl)

    unit = store.create_merge_unit(
        source_branch=pending_impl.branch,
        target_branch="main",
        owner_task_id=pending_impl.id,
        state="empty",
    )
    store.attach_task_to_merge_unit(pending_impl.id, unit.id, "owner")

    iterate_calls: list[dict[str, object]] = []

    def fake_spawn_iterate(_args, _config, impl_task, **kwargs):
        iterate_calls.append(
            {
                "task_id": impl_task.id,
                "prepared_task_id": kwargs.get("prepared_task_id"),
                "prepared_resume": kwargs.get("prepared_resume"),
                "prepared_phase": kwargs.get("prepared_phase"),
            }
        )
        return 0

    with (
        patch("gza.cli.git_ops.Git", return_value=_mock_git()),
        patch("gza.cli.git_ops.list_failed_tasks_for_recovery", return_value=[]),
        patch("gza.recovery_engine.list_failed_tasks_for_recovery", return_value=[]),
        patch("gza.cli.git_ops._advance_uses_iterate", return_value=True),
        patch("gza.cli.git_ops._prepare_task_for_immediate_execution", side_effect=lambda _c, task, **_k: task),
        patch("gza.cli.git_ops._spawn_background_iterate_worker", side_effect=fake_spawn_iterate),
    ):
        rc = cmd_advance(_advance_args(tmp_path, batch=1, new=True))

    assert rc == 0
    assert iterate_calls == [
        {
            "task_id": pending_impl.id,
            "prepared_task_id": pending_impl.id,
            "prepared_resume": True,
            "prepared_phase": "preloop",
        }
    ]


def test_advance_new_pending_implement_iterate_startup_failure_surfaces_and_skips_spawn(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    pending_impl = store.add("Implement queued task", task_type="implement")

    with (
        patch("gza.cli.git_ops.Git", return_value=_mock_git()),
        patch("gza.cli.git_ops.list_failed_tasks_for_recovery", return_value=[]),
        patch("gza.recovery_engine.list_failed_tasks_for_recovery", return_value=[]),
        patch("gza.cli.git_ops._advance_uses_iterate", return_value=True),
        patch("gza.cli._common.prepare_task_startup_phase", side_effect=RuntimeError("creator boom")),
        patch(
            "gza.cli.git_ops._spawn_background_iterate_worker",
            side_effect=AssertionError("iterate worker should not spawn"),
        ),
    ):
        rc = cmd_advance(_advance_args(tmp_path, batch=1, new=True))

    output = capsys.readouterr()
    assert rc == 1
    assert "creator boom" in output.err
    refreshed = store.get(pending_impl.id)
    assert refreshed is not None
    assert refreshed.slug is None
    assert refreshed.log_file is None
    logs_dir = tmp_path / ".gza" / "logs"
    if logs_dir.exists():
        assert list(logs_dir.iterdir()) == []
    workers_dir = tmp_path / ".gza" / "workers"
    if workers_dir.exists():
        assert list(workers_dir.iterdir()) == []


def test_advance_creates_exactly_one_closing_review_after_completed_improve(
    tmp_path: Path, capsys
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = _create_completed_implement(store)
    stale_review = store.add("Old review", task_type="review", depends_on=impl.id)
    assert stale_review.id is not None
    stale_review.status = "completed"
    stale_review.output_content = "**Verdict: CHANGES_REQUESTED**"
    stale_review.completed_at = datetime(2026, 1, 2, tzinfo=UTC)
    store.update(stale_review)

    improve = store.add(
        "Improve feature",
        task_type="improve",
        based_on=impl.id,
        depends_on=stale_review.id,
        same_branch=True,
    )
    improve.status = "completed"
    improve.completed_at = datetime(2026, 1, 3, tzinfo=UTC)
    store.update(improve)

    impl.review_cleared_at = datetime(2026, 1, 3, tzinfo=UTC)
    store.update(impl)

    closing_review = SimpleNamespace(id="testproject-closing-review")

    with (
        patch("gza.cli.git_ops.Git", return_value=_mock_git()),
        patch("gza.cli.git_ops.list_failed_tasks_for_recovery", return_value=[]),
        patch("gza.recovery_engine.list_failed_tasks_for_recovery", return_value=[]),
        patch(
            "gza.cli.git_ops._prepare_create_review_action",
            return_value=SimpleNamespace(
                status="created",
                review_task=closing_review,
                message=f"Created review task {closing_review.id}",
            ),
        ) as create_review,
        patch("gza.cli.git_ops.launch_permit"),
        patch("gza.cli.advance_executor._prepare_task_for_reserved_launch", side_effect=lambda _c, task, **_k: task),
        patch("gza.cli.git_ops._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        rc = cmd_advance(_advance_args(tmp_path))

    output = capsys.readouterr().out
    assert rc == 0
    assert create_review.call_count == 1
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == closing_review.id
    assert f"Created review task {closing_review.id}" in output


def test_advance_dry_run_surfaces_improve_noop_attention_reason(tmp_path: Path, capsys, monkeypatch) -> None:
    from gza import advance_engine as advance_engine_module
    from gza.review_verdict import ParsedReviewReport

    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = _create_completed_implement(store)
    review = _create_completed_review(store, impl, verdict="CHANGES_REQUESTED")
    review.report_file = "reviews/fake.md"
    store.update(review)

    for hour in (11, 12):
        improve = store.add(
            f"Improve {hour}",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
        )
        improve.status = "completed"
        improve.completed_at = datetime(2026, 1, 3, hour, 0, tzinfo=UTC)
        improve.branch = impl.branch
        improve.changed_diff = False
        store.update(improve)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review: ParsedReviewReport(
            verdict="CHANGES_REQUESTED",
            findings=(),
            format_version="legacy",
        ),
    )

    with patch("gza.cli.git_ops.Git", return_value=_mock_git()):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, dry_run=True))

    output = capsys.readouterr().out
    assert rc == 0
    assert "reason=improve-no-op" in output
    assert "consecutive no-op improves" in output
