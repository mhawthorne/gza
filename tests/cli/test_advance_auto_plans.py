"""Tests for auto-advancing completed plans via `gza advance`."""

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from gza.cli.advance_executor import AdvanceActionExecutionResult
from gza.cli._common import reconcile_in_progress_tasks
from gza.cli.git_ops import cmd_advance
from gza.concurrency import MaxConcurrentTasksError, get_concurrency_snapshot, launch_permit, take_task_launch_permit
from gza.config import Config
from gza.dispatch_preview import build_dispatch_preview
from gza.git import GitError
from gza.main_integration_verify import MAIN_INTEGRATION_VERIFY_PROMPT
from gza.recovery_engine import _MergeContext
from gza.workers import WorkerMetadata, WorkerRegistry
from tests.cli.conftest import make_store, setup_config


def _advance_args(tmp_path: Path, **overrides) -> argparse.Namespace:
    args = argparse.Namespace(
        project_dir=tmp_path,
        task_id=None,
        dry_run=False,
        auto=True,
        max=None,
        repeat=False,
        max_iterations=None,
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
        tags=None,
        all_tags=False,
    )
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


def _mock_git(*, current_branch: str = "main", can_merge: bool = True) -> Mock:
    git = Mock()
    git.current_branch.return_value = current_branch
    git.local_branch_names.return_value = ()
    git.branch_exists.return_value = True
    git.ref_exists.return_value = False
    git.can_merge.return_value = can_merge
    return git


def _create_completed_plan(store, prompt="Design the feature"):
    plan = store.add(prompt, task_type="plan")
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)
    return plan


def _create_mixed_owner_recovery_fixture(store):
    plan = _create_completed_plan(store, "Scoped completed plan")
    plan.tags = ("release",)
    store.update(plan)
    assert plan.id is not None

    failed_review = store.add(
        "Scoped failed review leaf",
        task_type="review",
        based_on=plan.id,
        tags=("release",),
    )
    assert failed_review.id is not None
    failed_review.status = "failed"
    failed_review.failure_reason = "MAX_TURNS"
    failed_review.session_id = "sess-mixed-review"
    failed_review.completed_at = datetime(2026, 6, 24, 9, 5, tzinfo=UTC)
    failed_review.branch = "feature/scoped-failed-review"
    failed_review.has_commits = True
    store.update(failed_review)
    return plan, failed_review


def _create_completed_implement(store, prompt="Implement feature", based_on=None, *, target_branch: str = "main"):
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
        target_branch=target_branch,
        owner_task_id=task.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(task.id, unit.id, "owner")
    store.dual_write_legacy_merge_status(unit.id)
    return task


def _durable_repeat_snapshot(store, task_id: str) -> tuple:
    tasks = tuple(
        (
            task.id,
            task.status,
            task.task_type,
            task.branch,
            task.based_on,
            task.depends_on,
            task.merge_status,
            task.merged_at,
            task.review_cleared_at,
            task.verify_fix_completion_outcome_json,
        )
        for task in sorted(store.get_all(), key=lambda task: task.id or "")
    )
    artifacts = tuple(
        (
            task.id,
            tuple(
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
                for artifact in store.list_artifacts(str(task.id))
            ),
        )
        for task in sorted(store.get_all(), key=lambda task: task.id or "")
        if task.id is not None
    )
    units = tuple(
        (
            unit.id,
            unit.owner_task_id,
            unit.source_branch,
            unit.target_branch,
            unit.state,
            unit.merged_at,
        )
        for unit in sorted(
            (store.resolve_merge_unit_for_task(task.id) for task in store.get_all() if task.id is not None),
            key=lambda unit: unit.id if unit is not None else "",
        )
        if unit is not None
    )
    return tasks, artifacts, units


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

    with (
        patch("gza.cli.git_ops.Git", return_value=_mock_git()),
        patch("gza.cli.git_ops.list_failed_tasks_for_recovery", return_value=[]),
        patch("gza.recovery_engine.list_failed_tasks_for_recovery", return_value=[]),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.cli.git_ops._prepare_task_for_immediate_execution", side_effect=lambda _c, task, **_k: task),
        patch("gza.cli.advance_executor._prepare_task_for_reserved_launch", side_effect=lambda _c, task, **_k: task),
        patch("gza.cli.git_ops._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        rc = cmd_advance(_advance_args(tmp_path))

    assert rc == 0
    assert "Created plan review task" in capsys.readouterr().out
    plan_review_tasks = [task for task in store.get_all() if task.task_type == "plan_review"]
    assert len(plan_review_tasks) == 1
    assert plan_review_tasks[0].depends_on == plan.id
    assert plan_review_tasks[0].based_on is None
    assert spawn_worker.call_args.kwargs["task_id"] == plan_review_tasks[0].id


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


def test_advance_materializes_approved_revised_plan_into_implement_tasks(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    plan = _create_completed_plan(store, "Design auth system")
    assert plan.id is not None

    first_review = store.add("Review original auth plan", task_type="plan_review", depends_on=plan.id)
    assert first_review.id is not None
    first_review.status = "completed"
    first_review.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    first_review.output_content = "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    store.update(first_review)

    revised_plan = store.add(
        "Revise auth plan",
        task_type="plan_improve",
        based_on=plan.id,
        depends_on=first_review.id,
    )
    assert revised_plan.id is not None
    revised_plan.status = "completed"
    revised_plan.completed_at = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    store.update(revised_plan)

    manifest = {
        "schema_version": 1,
        "source_task_id": revised_plan.id,
        "source_task_type": "plan_improve",
        "verdict": "APPROVED",
        "slice_quality": {
            "fits_single_task_budget": True,
            "timeout_budget_minutes": 30,
            "max_expected_files_changed_per_slice": 8,
            "rationale": "Bounded slices.",
        },
        "slices": [
            {
                "slice_id": "S1",
                "title": "Foundation",
                "prompt": "Implement revised auth foundation.",
                "scope": ["Planner"],
                "out_of_scope": [],
                "acceptance_criteria": ["Foundation exists"],
                "depends_on_slices": [],
                "based_on_slice": None,
                "review_scope": "Foundation only.",
                "estimated_complexity": "small",
                "expected_timeout_minutes": 30,
                "requires_code_review": True,
                "tags": ["planner"],
            }
        ],
    }
    revised_review = store.add("Review revised auth plan", task_type="plan_review", depends_on=revised_plan.id)
    assert revised_review.id is not None
    revised_review.status = "completed"
    revised_review.completed_at = datetime(2026, 5, 10, 13, 0, tzinfo=UTC)
    revised_review.output_content = (
        "## Verdict\nVerdict: APPROVED\n\n## Slice Manifest\n```json\n"
        + json.dumps(manifest)
        + "\n```\n"
    )
    store.update(revised_review)

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
    output = capsys.readouterr().out
    assert "Materialized implementation slices" in output
    implement_tasks = [task for task in store.get_all() if task.task_type == "implement"]
    assert len(implement_tasks) == 1
    assert implement_tasks[0].based_on == revised_plan.id


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
        "project_name: test-project\nprovider: codex\nmodel: gpt-5.5\n"
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
        "project_name: test-project\nprovider: codex\nmodel: gpt-5.5\n"
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
    assert "Run verify gate before merge" in output
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
    assert "Rebase before failed-task recovery" not in captured.out
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
    assert "Rebase before failed-task recovery" not in captured.out

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
    assert "Rebase before failed-task recovery" not in captured.out
    assert "No eligible tasks to advance" not in captured.out


def test_advance_dry_run_tag_scope_matches_shared_recovery_preview_ids(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001
    store._project_root = None  # noqa: SLF001

    release_retry = store.add("Release retry", task_type="plan", tags=("release",))
    assert release_retry.id is not None
    release_retry.status = "failed"
    release_retry.failure_reason = "INFRASTRUCTURE_ERROR"
    release_retry.completed_at = datetime(2026, 6, 24, 10, 0, tzinfo=UTC)
    store.update(release_retry)

    ops_retry = store.add("Ops retry", task_type="plan", tags=("ops",))
    assert ops_retry.id is not None
    ops_retry.status = "failed"
    ops_retry.failure_reason = "INFRASTRUCTURE_ERROR"
    ops_retry.completed_at = datetime(2026, 6, 24, 10, 5, tzinfo=UTC)
    store.update(ops_retry)

    release_manual = store.add("Release manual", task_type="plan", tags=("release", "ops"))
    assert release_manual.id is not None
    release_manual.status = "failed"
    release_manual.failure_reason = "TEST_FAILURE"
    release_manual.completed_at = datetime(2026, 6, 24, 10, 10, tzinfo=UTC)
    store.update(release_manual)

    with patch(
        "gza.recovery_engine._load_merge_context",
        return_value=_MergeContext(git=None, default_branch="main"),
    ):
        preview = build_dispatch_preview(
            store,
            tags=("release", "missing"),
            any_tag=True,
            max_recovery_attempts=1,
            selection_mode="recovery_only",
            include_pending=False,
        )

    preview_ids = [entry.task.id for entry in preview.recovery_entries]

    with (
        patch("gza.cli.git_ops.Git", return_value=_mock_git()),
        patch(
            "gza.recovery_engine._load_merge_context",
            return_value=_MergeContext(git=None, default_branch="main"),
        ),
    ):
        rc = cmd_advance(
            _advance_args(
                tmp_path,
                dry_run=True,
                tags=["release", "missing"],
                all_tags=False,
            )
        )

    captured = capsys.readouterr()
    assert rc == 0
    assert preview_ids == [release_retry.id, release_manual.id]
    for task_id in preview_ids:
        assert str(task_id) in captured.out
    assert str(ops_retry.id) not in captured.out


def test_advance_dry_run_mixed_owner_recovery_row_shows_recovery_leaf_and_lifecycle_owner(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001
    store._project_root = None  # noqa: SLF001
    plan, failed_review = _create_mixed_owner_recovery_fixture(store)

    with patch(
        "gza.recovery_engine._load_merge_context",
        return_value=_MergeContext(git=None, default_branch="main"),
    ):
        preview = build_dispatch_preview(
            store,
            tags=("release",),
            any_tag=True,
            max_recovery_attempts=1,
            selection_mode="recovery_only",
            include_pending=False,
        )

    with (
        patch("gza.cli.git_ops.Git", return_value=_mock_git()),
        patch(
            "gza.recovery_engine._load_merge_context",
            return_value=_MergeContext(git=None, default_branch="main"),
        ),
    ):
        rc = cmd_advance(
            _advance_args(
                tmp_path,
                dry_run=True,
                tags=["release"],
                all_tags=False,
            )
        )

    captured = capsys.readouterr()
    assert rc == 0
    assert [entry.task.id for entry in preview.recovery_entries] == [failed_review.id]
    assert "Recovery subset (shared preview):" in captured.out
    assert failed_review.id in captured.out
    assert "Resume failed task (MAX_TURNS)" in captured.out
    assert "Would advance 1 task(s):" in captured.out
    assert plan.id in captured.out
    assert "Create and start plan review task" in captured.out


def test_advance_explicit_task_fails_closed_when_tag_scope_excludes_it(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Ops-only task", task_type="plan", tags=("ops",))
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    store.update(task)
    assert task.id is not None

    with patch("gza.cli.git_ops.Git", return_value=_mock_git()):
        rc = cmd_advance(
            _advance_args(
                tmp_path,
                task_id=task.id,
                dry_run=True,
                tags=["release"],
                all_tags=False,
            )
        )

    captured = capsys.readouterr()
    assert rc == 1
    assert f"Task {task.id} does not match the requested advance scope" in captured.out


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


def test_advance_repeat_requires_explicit_task_id(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)

    rc = cmd_advance(_advance_args(tmp_path, repeat=True))

    captured = capsys.readouterr()
    assert rc == 1
    assert "--repeat requires an explicit task_id" in captured.out


def test_advance_repeat_stops_when_merged(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _create_completed_implement(store)
    git = _mock_git()
    merge_state = {"value": "unmerged"}

    def fake_merge(*_args, **_kwargs):
        merge_state["value"] = "merged"
        store.set_merge_status(impl.id, "merged")
        return SimpleNamespace(rc=0)

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", side_effect=lambda **_kwargs: merge_state["value"]),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "merge", "description": "Merge task"}),
        patch("gza.cli.git_ops._execute_merge_action", side_effect=fake_merge),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, max_iterations=2))

    captured = capsys.readouterr()
    assert rc == 0
    assert "cycle 1: merge -> success: merged" in captured.out
    assert f"Advance repeat completed: {impl.id} merged" in captured.out


def test_advance_repeat_stops_when_parked(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _create_completed_implement(store)
    git = _mock_git()
    action = {
        "type": "needs_discussion",
        "description": "SKIP: needs human",
        "needs_attention_reason": "manual-check",
        "subject_task_id": impl.id,
    }

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.determine_next_action", return_value=action),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, max_iterations=2))

    captured = capsys.readouterr()
    assert rc == 0
    assert "cycle 1: needs_discussion -> parked: SKIP: needs human" in captured.out
    assert "Advance repeat parked: SKIP: needs human" in captured.out


def test_advance_repeat_stops_at_iteration_cap(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _create_completed_implement(store)
    git = _mock_git()
    actions = iter(
        [
            {"type": "create_review", "description": "Create review"},
            {"type": "run_review", "description": "Run review"},
        ]
    )

    def fake_execute(*_args, **_kwargs):
        return SimpleNamespace(status="success", message="ok", success_message="", error_message="")

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.determine_next_action", side_effect=lambda *_a, **_k: next(actions)),
        patch("gza.cli.git_ops.execute_advance_action", side_effect=fake_execute),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, max_iterations=2))

    captured = capsys.readouterr()
    assert rc == 0
    assert "cycle 1: create_review -> success: ok" in captured.out
    assert "cycle 2: run_review -> success: ok" in captured.out
    assert "Advance repeat stopped: max iterations (2) reached" in captured.out


def test_advance_repeat_stops_on_no_progress_backstop(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _create_completed_implement(store)
    git = _mock_git()

    def fake_execute(*_args, **_kwargs):
        return SimpleNamespace(status="success", message="noop", success_message="", error_message="")

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "create_review", "description": "Create review"}),
        patch("gza.cli.git_ops.execute_advance_action", side_effect=fake_execute),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, max_iterations=5))

    captured = capsys.readouterr()
    assert rc == 0
    assert "cycle 2: create_review -> success: noop" in captured.out
    assert "Advance repeat stopped: no progress after repeated create_review" in captured.out


def test_advance_repeat_reaches_merge_after_rebase_review_chain(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _create_completed_implement(store)
    git = _mock_git()
    merge_state = {"value": "unmerged"}
    actions = ["needs_rebase", "create_review", "run_review", "merge"]
    index = {"value": 0}

    def fake_determine(*_args, **kwargs):
        if kwargs.get("selected_for_merge"):
            return {"type": "merge", "description": "Merge task"}
        action_type = actions[index["value"]]
        index["value"] = min(index["value"] + 1, len(actions) - 1)
        return {"type": action_type, "description": action_type.replace("_", " ")}

    def fake_execute(*_args, **_kwargs):
        return SimpleNamespace(status="success", message="ok", success_message="", error_message="")

    def fake_merge(*_args, **_kwargs):
        merge_state["value"] = "merged"
        store.set_merge_status(impl.id, "merged")
        return SimpleNamespace(rc=0)

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", side_effect=lambda **_kwargs: merge_state["value"]),
        patch("gza.cli.git_ops.determine_next_action", side_effect=fake_determine),
        patch("gza.cli.git_ops.execute_advance_action", side_effect=fake_execute),
        patch("gza.cli.git_ops._execute_merge_action", side_effect=fake_merge),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, max_iterations=4))

    captured = capsys.readouterr()
    assert rc == 0
    assert "cycle 1: needs_rebase -> success: ok" in captured.out
    assert "cycle 2: create_review -> success: ok" in captured.out
    assert "cycle 3: run_review -> success: ok" in captured.out
    assert "cycle 4: merge -> success: merged" in captured.out
    assert f"Advance repeat completed: {impl.id} merged" in captured.out


def test_advance_repeat_honors_launch_permit_cap(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    (tmp_path / "gza.yaml").write_text((tmp_path / "gza.yaml").read_text() + "max_concurrent: 1\n")
    store = make_store(tmp_path)
    impl = _create_completed_implement(store)
    other = store.add("Already running task", task_type="implement")
    assert other.id is not None
    other.status = "in_progress"
    other.running_pid = os.getpid()
    other.started_at = datetime.now(UTC)
    store.update(other)
    git = _mock_git()
    WorkerRegistry(tmp_path / ".gza" / "workers").register(
        WorkerMetadata(
            worker_id="saturating-worker",
            task_id=other.id,
            pid=os.getpid(),
            started_at=datetime.now(UTC).isoformat(),
            status="running",
        )
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "create_review", "description": "Create review"}),
        patch("gza.cli.git_ops.execute_advance_action") as execute_mock,
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, max_iterations=2))

    captured = capsys.readouterr()
    assert rc == 0
    assert "already at max concurrent tasks: 1 running, limit is 1" in captured.out
    execute_mock.assert_not_called()


def test_advance_repeat_saturated_cap_does_not_enter_direct_merge_gate(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    (tmp_path / "gza.yaml").write_text((tmp_path / "gza.yaml").read_text() + "max_concurrent: 1\n")
    store = make_store(tmp_path)
    impl = _create_completed_implement(store)
    other = store.add("Already running task", task_type="implement")
    assert other.id is not None
    other.status = "in_progress"
    other.running_pid = os.getpid()
    other.started_at = datetime.now(UTC)
    store.update(other)
    WorkerRegistry(tmp_path / ".gza" / "workers").register(
        WorkerMetadata(
            worker_id="saturating-merge-worker",
            task_id=other.id,
            pid=os.getpid(),
            started_at=datetime.now(UTC).isoformat(),
            status="running",
        )
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=_mock_git()),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "merge", "description": "Merge task"}),
        patch("gza.cli.git_ops.check_main_integration_verify") as main_verify,
        patch("gza.cli.git_ops.check_candidate_integration_verify") as candidate_verify,
        patch("gza.cli.git_ops._execute_merge_action") as execute_merge,
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, max_iterations=2))

    captured = capsys.readouterr()
    assert rc == 0
    assert "already at max concurrent tasks: 1 running, limit is 1" in captured.out
    main_verify.assert_not_called()
    candidate_verify.assert_not_called()
    execute_merge.assert_not_called()


def test_advance_repeat_saturated_cap_does_not_execute_direct_verify_gate(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    (tmp_path / "gza.yaml").write_text((tmp_path / "gza.yaml").read_text() + "max_concurrent: 1\n")
    store = make_store(tmp_path)
    impl = _create_completed_implement(store)
    other = store.add("Already running task", task_type="implement")
    assert other.id is not None
    other.status = "in_progress"
    other.running_pid = os.getpid()
    other.started_at = datetime.now(UTC)
    store.update(other)
    WorkerRegistry(tmp_path / ".gza" / "workers").register(
        WorkerMetadata(
            worker_id="saturating-verify-gate-worker",
            task_id=other.id,
            pid=os.getpid(),
            started_at=datetime.now(UTC).isoformat(),
            status="running",
        )
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=_mock_git()),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "verify_gate", "description": "Run verify gate"}),
        patch("gza.cli.git_ops.execute_advance_action") as execute_action,
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, max_iterations=2))

    captured = capsys.readouterr()
    assert rc == 0
    assert "already at max concurrent tasks: 1 running, limit is 1" in captured.out
    execute_action.assert_not_called()


def test_advance_repeat_does_not_enter_git_planning_cache_between_cycles(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _create_completed_implement(store)
    git = _mock_git()
    observed_heads: list[str] = []
    head = {"value": "before-rebase"}

    class _ForbiddenCache:
        def __enter__(self):
            raise AssertionError("repeat must not enter planning cache")

        def __exit__(self, exc_type, exc, tb):
            return None

    git.cached.return_value = _ForbiddenCache()

    def fake_determine(*_args, **kwargs):
        if kwargs.get("selected_for_merge"):
            return {"type": "merge", "description": "Merge task"}
        observed_heads.append(head["value"])
        return {
            "type": "needs_rebase" if len(observed_heads) == 1 else "create_review",
            "description": "repeat action",
        }

    def fake_execute(*_args, **_kwargs):
        head["value"] = "after-rebase"
        return AdvanceActionExecutionResult(
            action_type="needs_rebase",
            status="success",
            message="rebased",
            success_message="rebased",
            worker_started=True,
            worker_consuming=True,
        )

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.determine_next_action", side_effect=fake_determine),
        patch("gza.cli.git_ops.execute_advance_action", side_effect=fake_execute),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, max_iterations=2))

    captured = capsys.readouterr()
    assert rc == 0
    assert observed_heads == ["before-rebase", "after-rebase"]
    assert "cycle 2: create_review" in captured.out


def test_advance_repeat_red_main_verify_parks_before_merge(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _create_completed_implement(store)
    git = _mock_git()
    main_verify_task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    store.update(main_verify_task)
    red = SimpleNamespace(
        merges_halted=True,
        state=SimpleNamespace(
            task=main_verify_task,
            alert_message="main verify RED at `repeatmain` - merges halted; phase `unit` failing",
        ),
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "merge", "description": "Merge task"}),
        patch("gza.cli.git_ops.check_main_integration_verify", return_value=red),
        patch("gza.cli.git_ops._execute_merge_action") as execute_merge,
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, max_iterations=2))

    captured = capsys.readouterr()
    assert rc == 0
    execute_merge.assert_not_called()
    assert "cycle 1: merge -> parked: SKIP: main verify RED at `repeatmain`" in captured.out
    assert "Advance repeat parked: SKIP: main verify RED at `repeatmain`" in captured.out


def test_advance_repeat_executor_attention_skip_parks_immediately(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _create_completed_implement(store)
    git = _mock_git()

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "create_review", "description": "Create review"}),
        patch(
            "gza.cli.git_ops.execute_advance_action",
            return_value=AdvanceActionExecutionResult(
                action_type="create_review",
                status="skip",
                message="SKIP: needs human",
                attention_type="needs_discussion",
                attention_reason="manual-check",
            ),
        ),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, max_iterations=3))

    captured = capsys.readouterr()
    assert rc == 0
    assert "cycle 1: create_review -> skip: SKIP: needs human" in captured.out
    assert "Advance repeat parked: SKIP: needs human" in captured.out
    assert "cycle 2:" not in captured.out


def test_advance_repeat_plain_executor_skip_without_progress_stops_after_one_attempt(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _create_completed_implement(store)
    git = _mock_git()
    execute_calls = 0

    def fake_execute(*_args, **_kwargs):
        nonlocal execute_calls
        execute_calls += 1
        return AdvanceActionExecutionResult(
            action_type="create_review",
            status="skip",
            message="SKIP: ordinary executor skip",
        )

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "create_review", "description": "Create review"}),
        patch("gza.cli.git_ops.execute_advance_action", side_effect=fake_execute),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, max_iterations=3))

    captured = capsys.readouterr()
    assert rc == 0
    assert execute_calls == 1
    assert "cycle 1: create_review -> skip: SKIP: ordinary executor skip" in captured.out
    assert "Advance repeat stopped on skip: SKIP: ordinary executor skip" in captured.out
    assert "cycle 2:" not in captured.out


def test_advance_repeat_capacity_loss_at_permit_acquisition_stops_after_one_attempt(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    (tmp_path / "gza.yaml").write_text((tmp_path / "gza.yaml").read_text() + "max_concurrent: 1\n")
    store = make_store(tmp_path)
    impl = _create_completed_implement(store)
    git = _mock_git()
    launch_attempts = 0

    def fake_launch_permit(*_args, **_kwargs):
        nonlocal launch_attempts
        launch_attempts += 1
        raise MaxConcurrentTasksError("already at max concurrent tasks: 1 running, limit is 1")

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "create_review", "description": "Create review"}),
        patch("gza.cli.advance_executor.launch_permit", side_effect=fake_launch_permit),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, max_iterations=3))

    captured = capsys.readouterr()
    assert rc == 0
    assert launch_attempts == 1
    assert "cycle 1: create_review -> skip: SKIP: already at max concurrent tasks: 1 running, limit is 1" in captured.out
    assert "Advance repeat stopped on skip: SKIP: already at max concurrent tasks: 1 running, limit is 1" in captured.out
    assert "cycle 2:" not in captured.out


def test_advance_repeat_failed_owner_reports_merged_after_recovery_descendant_lands(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    owner = _create_completed_implement(store)
    owner.status = "failed"
    owner.failure_reason = "MAX_TURNS"
    owner.completed_at = datetime.now(UTC)
    store.update(owner)
    recovery = store.add("Recovered implementation", task_type="implement", based_on=owner.id)
    assert recovery.id is not None
    recovery.status = "completed"
    recovery.branch = owner.branch
    recovery.has_commits = True
    recovery.completed_at = datetime.now(UTC)
    store.update(recovery)
    unit = store.resolve_merge_unit_for_task(owner.id)
    assert unit is not None
    store.attach_task_to_merge_unit(recovery.id, unit.id, "recovery")
    store.set_merge_unit_state(unit.id, "merged", merged_by_task_id=owner.id)

    with patch("gza.cli.git_ops.Git", return_value=_mock_git()):
        rc = cmd_advance(_advance_args(tmp_path, task_id=owner.id, repeat=True, max_iterations=3))

    captured = capsys.readouterr()
    assert rc == 0
    assert store.get(owner.id).status == "failed"
    assert f"Task {owner.id} is already merged" in captured.out
    assert "stopped on skip" not in captured.out


def test_advance_repeat_candidate_verify_block_parks_not_error(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _create_completed_implement(store)
    git = _mock_git()
    green = SimpleNamespace(merges_halted=False, state=SimpleNamespace(task=impl, alert_message=None))
    blocked_result = SimpleNamespace(
        rc=1,
        status="blocked_candidate_verify",
        block_reason="candidate verify red",
        candidate_verify=SimpleNamespace(
            evidence=SimpleNamespace(
                tree_fingerprint="fp-repeat-candidate",
                failing_phase="unit",
                verify_status="failed",
            )
        ),
        created_followups=[],
        reused_followups=[],
        created_investigation_task_ids=[],
        reused_investigation_task_ids=[],
        promotion_warnings=(),
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "merge", "description": "Merge task"}),
        patch("gza.cli.git_ops.check_main_integration_verify", return_value=green),
        patch("gza.cli.git_ops._execute_merge_action", return_value=blocked_result),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, max_iterations=2))

    captured = capsys.readouterr()
    assert rc == 0
    assert "cycle 1: merge -> parked:" in captured.out
    assert "candidate verify red" in captured.out
    assert "Advance repeat parked: SKIP:" in captured.out
    assert "-> error:" not in captured.out


def test_advance_repeat_uses_single_available_permit_for_foreground_action(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    (tmp_path / "gza.yaml").write_text((tmp_path / "gza.yaml").read_text() + "max_concurrent: 1\n")
    store = make_store(tmp_path)
    impl = _create_completed_implement(store)
    git = _mock_git()
    foreground_task_ids: list[str] = []

    def fake_run_foreground(_config, task_id, **kwargs):
        foreground_task_ids.append(task_id)
        prepared_task = kwargs["prepared_task"]
        permit = take_task_launch_permit(str(prepared_task.id))
        assert permit is not None
        permit.release()
        return 0

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "create_review", "description": "Create review"}),
        patch("gza.cli.advance_executor._prepare_task_for_reserved_launch", side_effect=lambda _c, task, **_k: task),
        patch("gza.cli.git_ops._run_foreground", side_effect=fake_run_foreground),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, max_iterations=1))

    captured = capsys.readouterr()
    assert rc == 0
    assert foreground_task_ids
    assert "cycle 1: create_review -> success" in captured.out
    config = Config.load(tmp_path)
    snapshot = get_concurrency_snapshot(config, store, cleanup_stale=False)
    assert snapshot.available == 1
    assert not WorkerRegistry(config.workers_path).list_all(include_completed=False)


def test_advance_repeat_launch_lock_released_when_foreground_action_visible(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    (tmp_path / "gza.yaml").write_text((tmp_path / "gza.yaml").read_text() + "max_concurrent: 1\n")
    store = make_store(tmp_path)
    impl = _create_completed_implement(store)
    git = _mock_git()
    registry = WorkerRegistry(tmp_path / ".gza" / "workers")
    observed_capacity_block = {"value": False}

    def fake_run_foreground(config, task_id, **kwargs):
        prepared_task = kwargs["prepared_task"]
        permit = take_task_launch_permit(str(prepared_task.id))
        assert permit is not None
        prepared_task.status = "in_progress"
        prepared_task.running_pid = os.getpid()
        prepared_task.started_at = datetime.now(UTC)
        store.update(prepared_task)
        registry.register(
            WorkerMetadata(
                worker_id="visible-repeat-worker",
                task_id=task_id,
                pid=os.getpid(),
                started_at=datetime.now(UTC).isoformat(),
                status="running",
            )
        )
        permit.release()
        try:
            launch_permit(config, store)
        except MaxConcurrentTasksError:
            observed_capacity_block["value"] = True
        finally:
            registry.mark_completed("visible-repeat-worker", exit_code=0, status="completed")
            prepared_task.status = "completed"
            prepared_task.completed_at = datetime.now(UTC)
            store.update(prepared_task)
        return 0

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "create_review", "description": "Create review"}),
        patch("gza.cli.advance_executor._prepare_task_for_reserved_launch", side_effect=lambda _c, task, **_k: task),
        patch("gza.cli.git_ops._run_foreground", side_effect=fake_run_foreground),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, max_iterations=1))

    assert rc == 0
    assert observed_capacity_block["value"] is True


def test_advance_repeat_dry_run_projects_action_chain_without_side_effects(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _create_completed_implement(store)
    git = _mock_git()
    before = _durable_repeat_snapshot(store, impl.id)
    workers_dir = tmp_path / ".gza" / "workers"
    before_workers = sorted(path.name for path in workers_dir.iterdir()) if workers_dir.exists() else []

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "needs_rebase", "description": "Create rebase task"}),
        patch("gza.cli.git_ops.check_main_integration_verify", return_value=SimpleNamespace(merges_halted=False, state=SimpleNamespace(task=impl, alert_message=None))),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, dry_run=True, max_iterations=4))

    captured = capsys.readouterr()
    after = _durable_repeat_snapshot(store, impl.id)
    after_workers = sorted(path.name for path in workers_dir.iterdir()) if workers_dir.exists() else []
    assert rc == 0
    assert "cycle 1: needs_rebase -> dry-run" in captured.out
    assert "Advance repeat dry-run stopped: next action requires executing needs_rebase" in captured.out
    assert "cycle 2:" not in captured.out
    assert after == before
    assert after_workers == before_workers


def test_advance_repeat_dry_run_merge_without_main_checkpoint_has_no_side_effects(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _create_completed_implement(store)
    git = _mock_git()
    before = _durable_repeat_snapshot(store, impl.id)
    workers_dir = tmp_path / ".gza" / "workers"
    before_workers = sorted(path.name for path in workers_dir.iterdir()) if workers_dir.exists() else []

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "merge", "description": "Merge task"}),
        patch("gza.cli.git_ops.check_main_integration_verify") as check_main,
        patch("gza.cli.git_ops._execute_merge_action") as execute_merge,
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, dry_run=True, max_iterations=2))

    captured = capsys.readouterr()
    after_workers = sorted(path.name for path in workers_dir.iterdir()) if workers_dir.exists() else []
    assert rc == 0
    assert "cycle 1: merge -> dry-run: Merge task" in captured.out
    assert "Advance repeat dry-run stopped: next action requires executing merge" in captured.out
    check_main.assert_not_called()
    execute_merge.assert_not_called()
    assert _durable_repeat_snapshot(store, impl.id) == before
    assert after_workers == before_workers


def test_advance_repeat_dry_run_merge_with_stale_main_checkpoint_has_no_side_effects(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _create_completed_implement(store)
    checkpoint = store.add(MAIN_INTEGRATION_VERIFY_PROMPT, task_type="internal", skip_learnings=True)
    assert checkpoint.id is not None
    checkpoint.status = "completed"
    checkpoint.completed_at = datetime.now(UTC)
    checkpoint.review_verify_status = "failed"
    checkpoint.review_verify_exit_status = "1"
    checkpoint.review_verify_captured_at = datetime.now(UTC)
    checkpoint.output_content = json.dumps(
        {
            "gate_enabled": True,
            "verify_command": "./bin/tests",
            "tree_fingerprint": "stale-tree",
            "head_sha": "stale-head",
            "alert_message": "main verify RED - merges halted",
            "captured_at": datetime.now(UTC).isoformat(),
        }
    )
    store.update(checkpoint)
    git = _mock_git()
    before = _durable_repeat_snapshot(store, impl.id)
    workers_dir = tmp_path / ".gza" / "workers"
    before_workers = sorted(path.name for path in workers_dir.iterdir()) if workers_dir.exists() else []

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "merge", "description": "Merge task"}),
        patch("gza.cli.git_ops.check_main_integration_verify") as check_main,
        patch("gza.cli.git_ops._execute_merge_action") as execute_merge,
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, dry_run=True, max_iterations=2))

    captured = capsys.readouterr()
    after_workers = sorted(path.name for path in workers_dir.iterdir()) if workers_dir.exists() else []
    assert rc == 0
    assert "cycle 1: merge -> dry-run: Merge task" in captured.out
    assert "Advance repeat dry-run stopped: next action requires executing merge" in captured.out
    assert "Advance repeat parked" not in captured.out
    check_main.assert_not_called()
    execute_merge.assert_not_called()
    assert _durable_repeat_snapshot(store, impl.id) == before
    assert after_workers == before_workers


def test_advance_repeat_dry_run_uses_read_only_planning_and_cycle_resolution(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _create_completed_implement(store)
    git = _mock_git()
    query_flags: list[tuple[bool, bool]] = []
    determine_flags: list[tuple[bool, bool]] = []

    from gza.cli import git_ops

    original_query = git_ops.query_lineage_owner_rows

    def spy_query(*args, **kwargs):
        query_flags.append(
            (
                kwargs.get("persist_post_merge_rebase_state"),
                kwargs.get("persist_review_clearance"),
            )
        )
        return original_query(*args, **kwargs)

    def fake_determine(*_args, **kwargs):
        determine_flags.append(
            (
                kwargs.get("persist_post_merge_rebase_state"),
                kwargs.get("persist_review_clearance"),
            )
        )
        return {"type": "create_review", "description": "Create review"}

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", side_effect=spy_query),
        patch("gza.cli.git_ops.determine_next_action", side_effect=fake_determine),
        patch("gza.cli.git_ops.apply_deferred_lineage_query_reconciliations") as apply_deferred,
        patch(
            "gza.cli.git_ops.execute_advance_action",
            return_value=AdvanceActionExecutionResult(
                action_type="create_review",
                status="dry_run",
                message="would create review",
            ),
        ),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, dry_run=True, max_iterations=2))

    assert rc == 0
    assert query_flags
    assert all(flags == (False, False) for flags in query_flags)
    assert determine_flags
    assert all(flags == (False, False) for flags in determine_flags)
    apply_deferred.assert_not_called()


@pytest.mark.parametrize(
    ("action", "expected_context_key"),
    [
        ({"type": "create_review", "description": "Create review", "review_root_task_id": "root-context"}, "review_root_task_id"),
        (
            {
                "type": "create_verify_fix",
                "description": "Create verify fix",
                "verify_owner_task_id": "owner-context",
                "verify_epoch": {"reviewed_head_sha": "head-context"},
            },
            "verify_epoch",
        ),
        (
            {
                "type": "run_verify_fix",
                "description": "Run verify fix",
                "verify_fix_task_id": "fix-context",
                "verify_owner_task_id": "owner-context",
            },
            "verify_fix_task_id",
        ),
        (
            {
                "type": "improve",
                "description": "Improve task",
                "review_task_id": "review-context",
                "improve_task_id": "improve-context",
            },
            "review_task_id",
        ),
        (
            {
                "type": "reconcile_branch_divergence",
                "description": "Reconcile branch divergence",
                "reconcile_task_id": "reconcile-context",
            },
            "reconcile_task_id",
        ),
    ],
)
def test_advance_repeat_dry_run_reports_engine_action_without_context_substitution(
    tmp_path: Path,
    capsys,
    action: dict,
    expected_context_key: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _create_completed_implement(store)
    git = _mock_git()
    before = _durable_repeat_snapshot(store, impl.id)
    observed_actions: list[dict] = []

    def fake_execute(*, task, action, context):
        observed_actions.append(dict(action))
        assert context.dry_run is True
        return AdvanceActionExecutionResult(
            action_type=str(action["type"]),
            status="dry_run",
            message=f"would execute {action['type']}",
        )

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.determine_next_action", return_value=action),
        patch("gza.cli.git_ops.execute_advance_action", side_effect=fake_execute),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, dry_run=True, max_iterations=4))

    captured = capsys.readouterr()
    assert rc == 0
    assert observed_actions == [action]
    assert expected_context_key in observed_actions[0]
    assert f"cycle 1: {action['type']} -> dry-run: {action['description']}" in captured.out
    assert f"Advance repeat dry-run stopped: next action requires executing {action['type']}" in captured.out
    assert "cycle 2:" not in captured.out
    assert _durable_repeat_snapshot(store, impl.id) == before


def test_advance_repeat_non_current_target_ignores_red_main_verify_for_merge(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _create_completed_implement(store, target_branch="release")
    git = _mock_git(current_branch="main")
    red = SimpleNamespace(
        merges_halted=True,
        state=SimpleNamespace(task=impl, alert_message="main verify RED"),
    )
    merge_result = SimpleNamespace(rc=0)

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "merge", "description": "Merge task"}),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", side_effect=["unmerged", "unmerged", "merged"]),
        patch("gza.cli.git_ops.check_main_integration_verify", return_value=red),
        patch("gza.cli.git_ops._execute_merge_action", return_value=merge_result) as execute_merge,
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, max_iterations=1))

    captured = capsys.readouterr()
    assert rc == 0
    execute_merge.assert_called_once()
    assert execute_merge.call_args.kwargs["target_branch"] == "release"
    assert "cycle 1: merge -> success: merged" in captured.out
    assert "Advance repeat parked" not in captured.out


def test_advance_repeat_successful_merge_runs_post_merge_main_verify(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _create_completed_implement(store)
    git = _mock_git()
    merge_result = SimpleNamespace(rc=0)
    reasons: list[str] = []

    def fake_check_main(_config, _store, check_git, *, reason):
        assert check_git is git
        reasons.append(reason)
        checkpoint = store.add(MAIN_INTEGRATION_VERIFY_PROMPT, task_type="internal", skip_learnings=True)
        assert checkpoint.id is not None
        checkpoint.status = "completed"
        checkpoint.completed_at = datetime.now(UTC)
        checkpoint.review_verify_status = "passed"
        checkpoint.review_verify_exit_status = "0"
        checkpoint.output_content = json.dumps({"reason": reason, "tree_fingerprint": "green-target"})
        store.update(checkpoint)
        return SimpleNamespace(merges_halted=False, state=SimpleNamespace(task=checkpoint, alert_message=None))

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "merge", "description": "Merge task"}),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", side_effect=["unmerged", "unmerged", "merged"]),
        patch("gza.cli.git_ops.check_main_integration_verify", side_effect=fake_check_main),
        patch("gza.cli.git_ops._execute_merge_action", return_value=merge_result),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, max_iterations=1))

    captured = capsys.readouterr()
    checkpoints = [
        task
        for task in store.get_all()
        if task.prompt == MAIN_INTEGRATION_VERIFY_PROMPT and task.review_verify_status == "passed"
    ]
    assert rc == 0
    assert reasons == ["advance-repeat-pre-merge", "advance-post-merge"]
    assert len(checkpoints) == 2
    assert "cycle 1: merge -> success: merged" in captured.out
    assert f"Advance repeat completed: {impl.id} merged" in captured.out


def test_advance_repeat_successful_merge_surfaces_red_post_merge_verify(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _create_completed_implement(store)
    git = _mock_git()
    merge_result = SimpleNamespace(rc=0)
    reasons: list[str] = []

    def fake_check_main(_config, _store, _git, *, reason):
        reasons.append(reason)
        checkpoint = store.add(MAIN_INTEGRATION_VERIFY_PROMPT, task_type="internal", skip_learnings=True)
        assert checkpoint.id is not None
        checkpoint.status = "completed"
        checkpoint.completed_at = datetime.now(UTC)
        post_merge = reason == "advance-post-merge"
        checkpoint.review_verify_status = "failed" if post_merge else "passed"
        checkpoint.review_verify_exit_status = "1" if post_merge else "0"
        checkpoint.output_content = json.dumps({"reason": reason, "tree_fingerprint": "target"})
        store.update(checkpoint)
        return SimpleNamespace(
            merges_halted=post_merge,
            state=SimpleNamespace(task=checkpoint, alert_message="main verify RED after repeat merge"),
        )

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "merge", "description": "Merge task"}),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.check_main_integration_verify", side_effect=fake_check_main),
        patch("gza.cli.git_ops._execute_merge_action", return_value=merge_result),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, max_iterations=1))

    captured = capsys.readouterr()
    red_checkpoints = [
        task
        for task in store.get_all()
        if task.prompt == MAIN_INTEGRATION_VERIFY_PROMPT and task.review_verify_status == "failed"
    ]
    assert rc == 0
    assert reasons == ["advance-repeat-pre-merge", "advance-post-merge"]
    assert len(red_checkpoints) == 1
    assert "cycle 1: merge -> parked: merged; main verify RED after repeat merge" in captured.out
    assert "Advance repeat parked: SKIP: main verify RED after repeat merge" in captured.out
    assert "Advance repeat completed" not in captured.out


def test_advance_repeat_merge_conflict_routes_to_rebase_and_reresolves_to_merged(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _create_completed_implement(store)
    git = _mock_git(can_merge=False)
    git.reset_hard_head.return_value = None
    merge_state = {"value": "unmerged"}
    conflict_seen = {"value": False}
    execute_calls: list[str] = []
    resolver_conflict_states: list[bool] = []

    def fake_determine(*_args, **_kwargs):
        resolver_conflict_states.append(conflict_seen["value"])
        if conflict_seen["value"]:
            return {"type": "needs_rebase", "description": "Create rebase task"}
        return {"type": "merge", "description": "Merge task"}

    def fake_execute(*, task, action, context):
        execute_calls.append(action["type"])
        rebase = store.add(f"Rebase {task.id}", task_type="rebase", based_on=task.id, depends_on=task.id)
        assert rebase.id is not None
        rebase.status = "completed"
        rebase.branch = task.branch
        rebase.has_commits = True
        rebase.completed_at = datetime.now(UTC)
        store.update(rebase)
        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None
        store.attach_task_to_merge_unit(rebase.id, unit.id, "rebase")
        merge_state["value"] = "merged"
        store.set_merge_unit_state(unit.id, "merged", merged_by_task_id=task.id)
        return AdvanceActionExecutionResult(
            action_type="needs_rebase",
            status="success",
            message="created and ran rebase",
            success_message="created and ran rebase",
            worker_started=True,
            worker_consuming=True,
        )

    failed_merge = SimpleNamespace(
        rc=1,
        status="failed",
        block_reason="merge conflict",
        created_followups=[],
        reused_followups=[],
        created_investigation_task_ids=[],
        reused_investigation_task_ids=[],
        promotion_warnings=(),
    )

    def fake_merge_action(*_args, **_kwargs):
        conflict_seen["value"] = True
        return failed_merge

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.determine_next_action", side_effect=fake_determine),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", side_effect=lambda **_kwargs: merge_state["value"]),
        patch("gza.cli.git_ops.check_main_integration_verify", return_value=SimpleNamespace(merges_halted=False, state=SimpleNamespace(task=impl, alert_message=None))),
        patch("gza.cli.git_ops._execute_merge_action", side_effect=fake_merge_action),
        patch("gza.cli.git_ops.execute_advance_action", side_effect=fake_execute),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, max_iterations=3))

    captured = capsys.readouterr()
    cycle_lines = [line for line in captured.out.splitlines() if line.startswith("cycle ")]
    assert rc == 0
    assert execute_calls == ["needs_rebase"]
    assert "cycle 1: merge -> needs_rebase: merge conflict routed to rebase" in captured.out
    assert "cycle 2: needs_rebase -> success: created and ran rebase" in captured.out
    assert "cycle 1: needs_rebase" not in captured.out
    assert [line.split(":", 1)[0] for line in cycle_lines] == ["cycle 1", "cycle 2"]
    assert False in resolver_conflict_states and True in resolver_conflict_states
    assert f"Advance repeat completed: {impl.id} merged" in captured.out


def test_advance_repeat_no_progress_requires_two_consecutive_unchanged_cycles(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _create_completed_implement(store)
    git = _mock_git()
    calls = {"value": 0}

    def fake_execute(*_args, **_kwargs):
        calls["value"] += 1
        if calls["value"] == 1:
            impl.output_content = "progress from first repeated action"
            store.update(impl)
        return AdvanceActionExecutionResult(action_type="create_review", status="success", message="noop")

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "create_review", "description": "Create review"}),
        patch("gza.cli.git_ops.execute_advance_action", side_effect=fake_execute),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, max_iterations=5))

    captured = capsys.readouterr()
    assert rc == 0
    assert calls["value"] == 3
    assert "cycle 3: create_review -> success: noop" in captured.out
    assert "Advance repeat stopped: no progress after repeated create_review" in captured.out


def test_advance_repeat_transitive_descendant_artifact_counts_as_progress(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _create_completed_implement(store)
    child = store.add("Review child", task_type="review", based_on=impl.id, depends_on=impl.id)
    assert child.id is not None
    child.status = "completed"
    child.completed_at = datetime.now(UTC)
    store.update(child)
    grandchild = store.add("Improve grandchild", task_type="improve", based_on=child.id, depends_on=child.id)
    assert grandchild.id is not None
    grandchild.status = "completed"
    grandchild.completed_at = datetime.now(UTC)
    store.update(grandchild)
    git = _mock_git()
    calls = {"value": 0}

    def fake_execute(*_args, **_kwargs):
        calls["value"] += 1
        if calls["value"] == 2:
            store.add_artifact(
                grandchild.id,
                kind="repeat-progress",
                label="progress",
                path=".gza/artifacts/repeat-progress.txt",
                content_type="text/plain",
                byte_size=1,
                sha256="abc",
                producer="test",
                status="created",
            )
        return AdvanceActionExecutionResult(action_type="create_review", status="success", message="noop")

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "create_review", "description": "Create review"}),
        patch("gza.cli.git_ops.execute_advance_action", side_effect=fake_execute),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=impl.id, repeat=True, max_iterations=5))

    assert rc == 0
    assert calls["value"] == 4


def test_advance_repeat_no_progress_ignores_sibling_merge_unit_churn(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    plan = _create_completed_plan(store)
    selected = _create_completed_implement(store, based_on=plan.id)
    sibling = _create_completed_implement(store, based_on=plan.id)
    git = _mock_git()
    calls = {"value": 0}

    def fake_execute(*_args, **_kwargs):
        calls["value"] += 1
        sibling.output_content = f"sibling-only progress {calls['value']}"
        store.update(sibling)
        return AdvanceActionExecutionResult(action_type="create_review", status="success", message="noop")

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "create_review", "description": "Create review"}),
        patch("gza.cli.git_ops.execute_advance_action", side_effect=fake_execute),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task_id=selected.id, repeat=True, max_iterations=5))

    captured = capsys.readouterr()
    assert rc == 0
    assert calls["value"] == 2
    assert "cycle 2: create_review -> success: noop" in captured.out
    assert "Advance repeat stopped: no progress after repeated create_review" in captured.out


def test_advance_repeat_supervised_session_survives_reconciliation_after_timeout(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    (tmp_path / "gza.yaml").write_text((tmp_path / "gza.yaml").read_text() + "max_concurrent: 1\n")
    config = Config.load(tmp_path)
    config.watch.no_activity_timeout = 1
    store = make_store(tmp_path)
    repeat = store.add(
        "Internal advance repeat session for test-1",
        task_type="internal",
        depends_on="test-1",
        tags=("system-advance-repeat",),
        skip_learnings=True,
    )
    assert repeat.id is not None
    repeat.status = "in_progress"
    repeat.running_pid = os.getpid()
    repeat.started_at = datetime(2026, 1, 1, tzinfo=UTC)
    store.update(repeat)
    registry = WorkerRegistry(config.workers_path)
    registry.register(
        WorkerMetadata(
            worker_id="w-repeat-session-worker",
            task_id=repeat.id,
            pid=os.getpid(),
            started_at=datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
            status="running",
            is_background=False,
        )
    )
    signaled: list[tuple[int, int]] = []

    def fake_kill(pid: int, sig: int) -> None:
        if sig != 0:
            signaled.append((pid, sig))

    with patch("gza.cli._common.os.kill", side_effect=fake_kill):
        reconcile_in_progress_tasks(config)
        snapshot = get_concurrency_snapshot(config, store)
        permit = launch_permit(config, store, current_pid=os.getpid())
        permit.release()

    refreshed = store.get(repeat.id)
    assert refreshed is not None
    assert refreshed.status == "in_progress"
    assert refreshed.running_pid == os.getpid()
    assert signaled == []
    assert snapshot.running == 1
    assert snapshot.available == 0
    assert snapshot.running_task_ids == (repeat.id,)


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
    completed_rebase.review_scope = (
        "Rebase diff provenance: yes\n"
        "Pre-rebase head SHA: old-head\n"
        "Pre-rebase target SHA: target-before\n"
        "Pre-rebase merge-base SHA: old-base\n"
        "Resolved head SHA: rebased-sha\n"
        "Resolved target SHA: target-sha\n"
        "Recovered baseline: no"
    )
    store.update(completed_rebase)

    git = Mock()
    git.current_branch.return_value = "main"
    git.can_merge.return_value = True
    git.branch_exists.side_effect = lambda branch: branch == impl.branch
    git.ref_exists.return_value = False
    git.resolve_merge_source_ref.side_effect = lambda branch: branch if branch == impl.branch else None
    git.resolve_fresh_merge_source_ref.side_effect = lambda branch: branch if branch == impl.branch else None
    git.resolve_fresh_merge_source.side_effect = (
        lambda branch: SimpleNamespace(ref=branch if branch == impl.branch else None, warning=None)
    )
    git.rev_parse_if_exists.side_effect = lambda ref: {
        impl.branch: "rebased-sha",
        "main": "target-sha",
    }.get(ref)
    git.is_ancestor.return_value = False
    git.is_merged.return_value = False

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
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
    assert "Run verify gate before review" in captured.out
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
        assert "Run verify gate before merge" in captured.out
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


def test_advance_explicit_impl_remote_only_fresh_ref_no_longer_proves_merged(
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
    assert "Needs attention (1 task):" in captured.out
    assert f'{impl.id} implement "Implement feature"' in captured.out
    assert "reason=review-freshness-unverified" in captured.out
    assert "latest review freshness could not be verified" in captured.out
    assert "Run verify gate before review" not in captured.out
    assert "resolution-review-metadata-invalid" not in captured.out


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
    assert "Run verify gate before review" in captured.out
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
    assert create_review.call_count == 0
    assert spawn_worker.call_count == 0
    assert f"Created review task {closing_review.id}" not in output


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
