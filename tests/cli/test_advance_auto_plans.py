"""Tests for auto-advancing completed plans via `gza advance`."""

import argparse
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

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


def test_advance_creates_implement_for_completed_plan(tmp_path: Path, capsys) -> None:
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
        patch("gza.cli.git_ops._spawn_background_worker", side_effect=fake_spawn),
    ):
        rc = cmd_advance(_advance_args(tmp_path))

    assert rc == 0
    assert "Created implement task" in capsys.readouterr().out
    impl_tasks = [task for task in store.get_all() if task.task_type == "implement"]
    assert len(impl_tasks) == 1
    assert impl_tasks[0].depends_on == plan.id
    assert impl_tasks[0].based_on is None
    assert impl_tasks[0].prompt == f"Implement plan from task {plan.id}: design-auth-system"
    assert spawn_calls == [impl_tasks[0].id]


def test_advance_skips_plan_with_existing_implement(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    plan = _create_completed_plan(store, "Design auth system")
    store.add("Implement auth", task_type="implement", based_on=plan.id)

    with patch("gza.cli.git_ops.Git", return_value=_mock_git()):
        rc = cmd_advance(_advance_args(tmp_path, task_id=plan.id, dry_run=True))

    assert rc == 0
    assert "implement task already exists" in capsys.readouterr().out


def test_advance_type_plan_filters_to_plans_only(tmp_path: Path, capsys) -> None:
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "advance_requires_review: false\n"
    )
    store = make_store(tmp_path)
    plan = _create_completed_plan(store, "Design feature X")
    _create_completed_implement(store)

    with patch("gza.cli.git_ops.Git", return_value=_mock_git()):
        rc = cmd_advance(_advance_args(tmp_path, dry_run=True, advance_type="plan"))

    output = capsys.readouterr().out
    assert rc == 0
    assert str(plan.id) in output
    assert "Create and start implement" in output
    assert "Merge" not in output


def test_advance_type_implement_filters_to_implements_only(tmp_path: Path, capsys) -> None:
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "advance_requires_review: false\n"
    )
    store = make_store(tmp_path)
    _create_completed_plan(store, "Design feature X")
    impl = _create_completed_implement(store)

    with patch("gza.cli.git_ops.Git", return_value=_mock_git()):
        rc = cmd_advance(_advance_args(tmp_path, dry_run=True, advance_type="implement"))

    output = capsys.readouterr().out
    assert rc == 0
    assert str(impl.id) in output
    assert "Create closing review" in output
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
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-failed"
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
    assert "Resume failed task (MAX_TURNS)" in captured.out
    assert captured.err.count("Warning: Failed-task recovery could not inspect repository branch reachability;") == 1
    assert "simulated reachability failure" in captured.err


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
        patch("gza.cli.git_ops._spawn_background_worker", side_effect=fake_spawn),
    ):
        rc = cmd_advance(_advance_args(tmp_path, batch=1))

    output = capsys.readouterr().out
    assert rc == 0
    assert len(spawn_calls) == 1
    assert "batch limit reached" in output


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
        patch(
            "gza.cli.git_ops._prepare_create_review_action",
            return_value=SimpleNamespace(
                status="created",
                review_task=closing_review,
                message=f"Created review task {closing_review.id}",
            ),
        ) as create_review,
        patch("gza.cli.git_ops._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        rc = cmd_advance(_advance_args(tmp_path))

    output = capsys.readouterr().out
    assert rc == 0
    assert create_review.call_count == 1
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == closing_review.id
    assert f"Created review task {closing_review.id}" in output
