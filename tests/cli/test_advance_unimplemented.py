"""Tests for `gza advance --unimplemented`."""

import argparse
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from gza.cli.git_ops import _cmd_advance_unimplemented, cmd_advance
from gza.config import Config
from gza.db import SqliteTaskStore

from tests.cli.conftest import make_store, setup_config


def _set_task_times(
    store: SqliteTaskStore,
    task,
    *,
    created_at: datetime | None = None,
    completed_at: datetime | None = None,
    status: str | None = None,
) -> None:
    if created_at is not None:
        task.created_at = created_at
    if completed_at is not None:
        task.completed_at = completed_at
    if status is not None:
        task.status = status
    store.update(task)


def _run_unimplemented(
    tmp_path: Path,
    store: SqliteTaskStore,
    *,
    dry_run: bool = False,
    create: bool = False,
    task_types: tuple[str, ...] = ("plan", "explore"),
) -> int:
    return _cmd_advance_unimplemented(
        Config.load(tmp_path),
        store,
        dry_run=dry_run,
        create=create,
        task_types=task_types,
    )


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


class TestAdvanceUnimplementedCommand:
    def test_advance_unimplemented_lists_completed_plan_and_explore_without_impl(self, tmp_path: Path, capsys) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Design the authentication system", task_type="plan")
        explore = store.add("Explore auth provider options", task_type="explore")
        plan.status = "completed"
        explore.status = "completed"
        now = datetime.now(UTC)
        plan.completed_at = now
        explore.completed_at = now
        store.update(plan)
        store.update(explore)

        rc = _run_unimplemented(tmp_path, store)

        output = capsys.readouterr().out
        assert rc == 0
        assert str(plan.id) in output
        assert str(explore.id) in output
        assert "[plan]" in output
        assert "[explore]" in output
        assert "(completed)" in output
        assert f"gza implement {plan.id}" in output
        assert f"gza implement {explore.id}" not in output
        assert "gza advance --unimplemented --create" in output

    def test_advance_unimplemented_excludes_tasks_with_impl(self, tmp_path: Path, capsys) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("A plan", task_type="plan")
        explore = store.add("An explore", task_type="explore")
        plan.status = "completed"
        explore.status = "completed"
        now = datetime.now(UTC)
        plan.completed_at = now
        explore.completed_at = now
        store.update(plan)
        store.update(explore)

        store.add("Implement plan", task_type="implement", based_on=plan.id)
        store.add("Implement explore", task_type="implement", based_on=explore.id)

        rc = _run_unimplemented(tmp_path, store)

        assert rc == 0
        assert "No plan/explore lineages without implementation tasks." in capsys.readouterr().out

    def test_advance_unimplemented_create_inherits_source_tags(self, tmp_path: Path, capsys) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        explore = store.add(
            "Explore recovery rollout",
            task_type="explore",
            tags=("202606-recovery", "v0.5.0"),
        )
        _set_task_times(
            store,
            explore,
            created_at=datetime(2026, 3, 1, tzinfo=UTC),
            completed_at=datetime(2026, 3, 2, tzinfo=UTC),
            status="completed",
        )

        rc = _run_unimplemented(tmp_path, store, create=True, task_types=("explore",))

        output = capsys.readouterr().out
        assert rc == 0
        assert f"Created implement task" in output
        implement_tasks = [task for task in store.get_all() if task.task_type == "implement"]
        assert len(implement_tasks) == 1
        assert implement_tasks[0].depends_on == explore.id
        assert implement_tasks[0].tags == explore.tags

    def test_advance_unimplemented_skips_explore_lineage_handed_off_to_pending_plan_descendant(
        self, tmp_path: Path, capsys
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        explore = store.add("Explore the ingestion approach", task_type="explore")
        _set_task_times(
            store,
            explore,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            completed_at=datetime(2026, 1, 2, tzinfo=UTC),
            status="completed",
        )

        plan = store.add("Plan the ingestion implementation", task_type="plan", based_on=explore.id)
        _set_task_times(store, plan, created_at=datetime(2026, 1, 3, tzinfo=UTC))

        rc = _run_unimplemented(tmp_path, store)
        output = capsys.readouterr().out

        assert rc == 0
        assert "No plan/explore lineages without implementation tasks." in output
        assert str(plan.id) not in output
        assert "Plan the ingestion implementation" not in output
        assert str(explore.id) not in output
        assert "Explore the ingestion approach" not in output

        create_rc = _run_unimplemented(tmp_path, store, create=True)

        assert create_rc == 0
        implement_tasks = [task for task in store.get_all() if task.task_type == "implement"]
        assert implement_tasks == []

    def test_advance_unimplemented_still_lists_completed_plan_without_descendants(self, tmp_path: Path, capsys) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan the scheduler rewrite", task_type="plan")
        _set_task_times(
            store,
            plan,
            created_at=datetime(2026, 2, 1, tzinfo=UTC),
            completed_at=datetime(2026, 2, 2, tzinfo=UTC),
            status="completed",
        )

        rc = _run_unimplemented(tmp_path, store)
        output = capsys.readouterr().out

        assert rc == 0
        assert str(plan.id) in output
        assert "Plan the scheduler rewrite" in output
        assert "(completed)" in output
        assert f"gza implement {plan.id}" in output

    def test_advance_unimplemented_excludes_lineage_with_descendant_implement(self, tmp_path: Path, capsys) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        explore = store.add("Explore cache invalidation options", task_type="explore")
        _set_task_times(
            store,
            explore,
            created_at=datetime(2026, 3, 1, tzinfo=UTC),
            completed_at=datetime(2026, 3, 2, tzinfo=UTC),
            status="completed",
        )

        plan = store.add("Plan cache invalidation rollout", task_type="plan", based_on=explore.id)
        _set_task_times(
            store,
            plan,
            created_at=datetime(2026, 3, 3, tzinfo=UTC),
            completed_at=datetime(2026, 3, 4, tzinfo=UTC),
            status="completed",
        )

        store.add("Implement cache invalidation rollout", task_type="implement", based_on=plan.id)

        rc = _run_unimplemented(tmp_path, store)
        output = capsys.readouterr().out

        assert rc == 0
        assert "No plan/explore lineages without implementation tasks." in output
        assert str(explore.id) not in output
        assert str(plan.id) not in output

    def test_advance_unimplemented_excludes_lineage_with_depends_on_linked_descendant_implement(
        self, tmp_path: Path, capsys
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        explore = store.add("Explore cache invalidation options", task_type="explore")
        _set_task_times(
            store,
            explore,
            created_at=datetime(2026, 3, 1, tzinfo=UTC),
            completed_at=datetime(2026, 3, 2, tzinfo=UTC),
            status="completed",
        )

        plan = store.add("Plan cache invalidation rollout", task_type="plan", based_on=explore.id)
        _set_task_times(
            store,
            plan,
            created_at=datetime(2026, 3, 3, tzinfo=UTC),
            completed_at=datetime(2026, 3, 4, tzinfo=UTC),
            status="completed",
        )

        store.add("Implement cache invalidation rollout", task_type="implement", depends_on=plan.id)

        rc = _run_unimplemented(tmp_path, store)
        output = capsys.readouterr().out

        assert rc == 0
        assert "No plan/explore lineages without implementation tasks." in output
        assert str(explore.id) not in output
        assert str(plan.id) not in output

    def test_advance_unimplemented_prefers_newest_completed_explore_descendant_deterministically(
        self, tmp_path: Path, capsys
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("Explore reporting architecture", task_type="explore")
        _set_task_times(
            store,
            root,
            created_at=datetime(2026, 4, 1, tzinfo=UTC),
            completed_at=datetime(2026, 4, 2, tzinfo=UTC),
            status="completed",
        )

        first_descendant = store.add("Explore reporting storage choices", task_type="explore", based_on=root.id)
        _set_task_times(
            store,
            first_descendant,
            created_at=datetime(2026, 4, 3, tzinfo=UTC),
            completed_at=datetime(2026, 4, 4, tzinfo=UTC),
            status="completed",
        )

        latest_descendant = store.add(
            "Explore reporting delivery tradeoffs",
            task_type="explore",
            based_on=first_descendant.id,
        )
        _set_task_times(
            store,
            latest_descendant,
            created_at=datetime(2026, 4, 5, tzinfo=UTC),
        )

        rc = _run_unimplemented(tmp_path, store)
        output = capsys.readouterr().out

        assert rc == 0
        assert str(first_descendant.id) in output
        assert "Explore reporting storage choices" in output
        assert "(completed)" in output
        assert str(root.id) not in output
        assert "Explore reporting architecture" not in output
        assert str(latest_descendant.id) not in output
        assert "Explore reporting delivery tradeoffs" not in output
        assert f"gza implement {first_descendant.id}" not in output
        assert "gza advance --unimplemented --create" in output

        create_rc = _run_unimplemented(tmp_path, store, create=True)

        assert create_rc == 0
        implement_tasks = [task for task in store.get_all() if task.task_type == "implement"]
        assert len(implement_tasks) == 1
        assert implement_tasks[0].depends_on == first_descendant.id
        assert implement_tasks[0].prompt.startswith(f"Implement findings from task {first_descendant.id}")

    def test_advance_unimplemented_skips_failed_sibling_when_other_sibling_implemented(
        self, tmp_path: Path, capsys
    ) -> None:
        """Failed plan sibling should not surface when a sibling resume reached implementation.

        Mirrors gza-2520: root failed plan with two children — a failed resume (no descendants)
        and a completed resume whose subtree carries the implement work. The failed sibling is a
        dead-end; the implementation already exists via the successful sibling.
        """
        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("Plan the merge truth replacement", task_type="plan")
        _set_task_times(
            store,
            root,
            created_at=datetime(2026, 5, 8, 7, 11, tzinfo=UTC),
            status="failed",
        )

        failed_resume = store.add("Plan the merge truth replacement", task_type="plan", based_on=root.id)
        _set_task_times(
            store,
            failed_resume,
            created_at=datetime(2026, 5, 8, 7, 17, tzinfo=UTC),
            status="failed",
        )

        completed_resume = store.add("Plan the merge truth replacement", task_type="plan", based_on=root.id)
        _set_task_times(
            store,
            completed_resume,
            created_at=datetime(2026, 5, 8, 16, 47, tzinfo=UTC),
            completed_at=datetime(2026, 5, 8, 17, 0, tzinfo=UTC),
            status="completed",
        )

        impl = store.add("Implement merge truth", task_type="implement", based_on=completed_resume.id)
        _set_task_times(
            store,
            impl,
            created_at=datetime(2026, 5, 8, 17, 46, tzinfo=UTC),
            completed_at=datetime(2026, 5, 8, 18, 0, tzinfo=UTC),
            status="completed",
        )

        rc = _run_unimplemented(tmp_path, store)
        output = capsys.readouterr().out

        assert rc == 0
        assert str(failed_resume.id) not in output
        assert str(root.id) not in output
        assert str(completed_resume.id) not in output
        assert "No plan/explore lineages without implementation tasks." in output

    def test_advance_unimplemented_skips_dropped_sibling_when_other_sibling_implemented(
        self, tmp_path: Path, capsys
    ) -> None:
        """Dropped plan sibling means abandoned — never surface even with no implement nearby.

        Mirrors gza-719: a dropped retry exists alongside a sibling chain that did reach
        implementation. The dropped row is explicitly abandoned and should not be listed.
        """
        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("Design symlink creation", task_type="plan")
        _set_task_times(
            store,
            root,
            created_at=datetime(2026, 4, 8, 21, 45, tzinfo=UTC),
            status="failed",
        )

        failed_retry = store.add("Design symlink creation", task_type="plan", based_on=root.id)
        _set_task_times(
            store,
            failed_retry,
            created_at=datetime(2026, 4, 8, 22, 3, tzinfo=UTC),
            status="failed",
        )

        completed_retry = store.add("Design symlink creation", task_type="plan", based_on=failed_retry.id)
        _set_task_times(
            store,
            completed_retry,
            created_at=datetime(2026, 4, 8, 22, 7, tzinfo=UTC),
            completed_at=datetime(2026, 4, 8, 22, 30, tzinfo=UTC),
            status="completed",
        )

        impl = store.add("Implement symlinks", task_type="implement", based_on=completed_retry.id)
        _set_task_times(
            store,
            impl,
            created_at=datetime(2026, 4, 9, 3, 48, tzinfo=UTC),
            completed_at=datetime(2026, 4, 9, 4, 0, tzinfo=UTC),
            status="completed",
        )

        dropped_retry = store.add("Design symlink creation", task_type="plan", based_on=root.id)
        _set_task_times(
            store,
            dropped_retry,
            created_at=datetime(2026, 4, 8, 22, 41, tzinfo=UTC),
            status="dropped",
        )

        rc = _run_unimplemented(tmp_path, store)
        output = capsys.readouterr().out

        assert rc == 0
        assert str(dropped_retry.id) not in output
        assert "No plan/explore lineages without implementation tasks." in output

    def test_advance_unimplemented_lists_completed_explore_with_only_dropped_plan_descendant(
        self, tmp_path: Path, capsys
    ) -> None:
        """A dropped plan descendant does not suppress the completed explore frontier row."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        completed_ancestor = store.add("Explore the thing", task_type="explore")
        _set_task_times(
            store,
            completed_ancestor,
            created_at=datetime(2026, 4, 1, tzinfo=UTC),
            completed_at=datetime(2026, 4, 1, 1, tzinfo=UTC),
            status="completed",
        )

        dropped_descendant = store.add(
            "Plan the thing",
            task_type="plan",
            based_on=completed_ancestor.id,
        )
        _set_task_times(
            store,
            dropped_descendant,
            created_at=datetime(2026, 4, 2, tzinfo=UTC),
            status="dropped",
        )

        rc = _run_unimplemented(tmp_path, store)
        output = capsys.readouterr().out

        assert rc == 0
        assert str(dropped_descendant.id) not in output
        assert str(completed_ancestor.id) in output
        assert "Explore the thing" in output

        create_rc = _run_unimplemented(tmp_path, store, create=True)

        assert create_rc == 0
        implement_tasks = [task for task in store.get_all() if task.task_type == "implement"]
        assert len(implement_tasks) == 1
        assert (
            implement_tasks[0].based_on == completed_ancestor.id
            or implement_tasks[0].depends_on == completed_ancestor.id
        )

    def test_advance_unimplemented_keeps_sibling_source_branches(self, tmp_path: Path, capsys) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        explore = store.add("Explore rollout options", task_type="explore")
        _set_task_times(
            store,
            explore,
            created_at=datetime(2026, 4, 1, tzinfo=UTC),
            completed_at=datetime(2026, 4, 2, tzinfo=UTC),
            status="completed",
        )

        first_plan = store.add("Plan one", task_type="plan", based_on=explore.id)
        _set_task_times(
            store,
            first_plan,
            created_at=datetime(2026, 4, 3, tzinfo=UTC),
            completed_at=datetime(2026, 4, 4, tzinfo=UTC),
            status="completed",
        )

        second_plan = store.add("Plan two", task_type="plan", based_on=explore.id)
        _set_task_times(
            store,
            second_plan,
            created_at=datetime(2026, 4, 5, tzinfo=UTC),
            completed_at=datetime(2026, 4, 6, tzinfo=UTC),
            status="completed",
        )

        rc = _run_unimplemented(tmp_path, store)
        output = capsys.readouterr().out

        assert rc == 0
        assert "Plan one" in output
        assert "Plan two" in output
        assert str(first_plan.id) in output
        assert str(second_plan.id) in output
        assert "Explore rollout options" not in output
        assert str(explore.id) not in output

    def test_advance_unimplemented_create_queues_one_implement_per_sibling_source_branch(
        self, tmp_path: Path
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        explore = store.add("Explore rollout options", task_type="explore")
        _set_task_times(
            store,
            explore,
            created_at=datetime(2026, 4, 1, tzinfo=UTC),
            completed_at=datetime(2026, 4, 2, tzinfo=UTC),
            status="completed",
        )

        first_plan = store.add("Plan one", task_type="plan", based_on=explore.id)
        _set_task_times(
            store,
            first_plan,
            created_at=datetime(2026, 4, 3, tzinfo=UTC),
            completed_at=datetime(2026, 4, 4, tzinfo=UTC),
            status="completed",
        )

        second_plan = store.add("Plan two", task_type="plan", based_on=explore.id)
        _set_task_times(
            store,
            second_plan,
            created_at=datetime(2026, 4, 5, tzinfo=UTC),
            completed_at=datetime(2026, 4, 6, tzinfo=UTC),
            status="completed",
        )

        rc = _run_unimplemented(tmp_path, store, create=True)

        assert rc == 0
        implement_tasks = [task for task in store.get_all() if task.task_type == "implement"]
        assert len(implement_tasks) == 2
        assert {task.depends_on for task in implement_tasks} == {first_plan.id, second_plan.id}
        assert all(task.based_on is None for task in implement_tasks)

    def test_advance_unimplemented_create_rerun_excludes_ancestor_lineage(self, tmp_path: Path, capsys) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        explore = store.add("Explore queue visibility options", task_type="explore")
        _set_task_times(
            store,
            explore,
            created_at=datetime(2026, 4, 1, tzinfo=UTC),
            completed_at=datetime(2026, 4, 2, tzinfo=UTC),
            status="completed",
        )

        plan = store.add("Plan queue visibility rollout", task_type="plan", based_on=explore.id)
        _set_task_times(
            store,
            plan,
            created_at=datetime(2026, 4, 3, tzinfo=UTC),
            completed_at=datetime(2026, 4, 4, tzinfo=UTC),
            status="completed",
        )

        create_rc = _run_unimplemented(tmp_path, store, create=True)

        assert create_rc == 0
        capsys.readouterr()
        implement_tasks = [task for task in store.get_all() if task.task_type == "implement"]
        assert len(implement_tasks) == 1
        assert implement_tasks[0].depends_on == plan.id

        rerun_rc = _run_unimplemented(tmp_path, store)
        output = capsys.readouterr().out

        assert rerun_rc == 0
        assert "No plan/explore lineages without implementation tasks." in output
        assert str(explore.id) not in output
        assert str(plan.id) not in output

    def test_advance_unimplemented_guidance_distinguishes_completed_plan_vs_explore(self, tmp_path: Path, capsys) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan E", task_type="plan")
        explore = store.add("Explore E", task_type="explore")
        plan.status = "completed"
        explore.status = "completed"
        now = datetime.now(UTC)
        plan.completed_at = now
        explore.completed_at = now
        store.update(plan)
        store.update(explore)

        rc = _run_unimplemented(tmp_path, store)
        output = capsys.readouterr().out

        assert rc == 0
        assert f"gza implement {plan.id}" in output
        assert f"gza implement {explore.id}" not in output
        assert (
            "Completed plan rows can be run directly with 'gza implement <task_id>' "
            "or auto-started with 'gza advance'."
        ) in output
        assert (
            "Use 'gza advance --unimplemented --create' to queue implement tasks "
            "for listed explore rows."
        ) in output

    def test_advance_unimplemented_create_queues_implement_tasks(self, tmp_path: Path, capsys) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan A", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        explore = store.add("Explore B", task_type="explore")
        explore.status = "completed"
        explore.completed_at = datetime.now(UTC)
        store.update(explore)

        rc = _run_unimplemented(tmp_path, store, create=True)
        output = capsys.readouterr().out

        assert rc == 0
        assert "Created" in output

        all_tasks = store.get_all()
        impl_tasks = [task for task in all_tasks if task.task_type == "implement"]
        assert len(impl_tasks) == 2
        by_depends_on = {task.depends_on: task for task in impl_tasks}
        assert plan.id in by_depends_on
        assert explore.id in by_depends_on
        assert all(task.based_on is None for task in impl_tasks)
        assert by_depends_on[plan.id].prompt.startswith(f"Implement plan from task {plan.id}")
        assert by_depends_on[explore.id].prompt.startswith(f"Implement findings from task {explore.id}")

    def test_advance_unimplemented_dry_run_no_create(self, tmp_path: Path, capsys) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan C", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        rc = _run_unimplemented(tmp_path, store, create=True, dry_run=True)
        output = capsys.readouterr().out

        assert rc == 0
        assert "dry-run" in output.lower() or "Would create" in output

        impl_tasks = [task for task in store.get_all() if task.task_type == "implement"]
        assert len(impl_tasks) == 0

    def test_advance_unimplemented_targeted_query_ignores_non_source_tasks(self, tmp_path: Path, capsys) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan_with_impl = store.add("Plan with impl", task_type="plan")
        plan_with_impl.status = "completed"
        plan_with_impl.completed_at = datetime.now(UTC)
        store.update(plan_with_impl)

        explore_without_impl = store.add("Explore without impl", task_type="explore")
        explore_without_impl.status = "completed"
        explore_without_impl.completed_at = datetime.now(UTC)
        store.update(explore_without_impl)

        assert plan_with_impl.id is not None and explore_without_impl.id is not None

        store.add("Impl 1", task_type="implement", based_on=plan_with_impl.id)

        for i in range(20):
            task = store.add(f"Task {i}", task_type="review")
            task.based_on = plan_with_impl.id
            store.update(task)

        rc = _run_unimplemented(tmp_path, store)
        output = capsys.readouterr().out

        assert rc == 0
        assert "Explore without impl" in output
        assert "Plan with impl" not in output

    def test_advance_plans_alias_keeps_plan_only_behavior(self, tmp_path: Path, capsys) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan D", task_type="plan")
        explore = store.add("Explore D", task_type="explore")
        plan.status = "completed"
        explore.status = "completed"
        now = datetime.now(UTC)
        plan.completed_at = datetime.now(UTC)
        explore.completed_at = now
        store.update(plan)
        store.update(explore)

        with patch("gza.cli.git_ops.Git"):
            rc = cmd_advance(_advance_args(tmp_path, plans=True))

        captured = capsys.readouterr()
        assert rc == 0
        assert "deprecated" in captured.err.lower()
        assert str(plan.id) in captured.out
        assert str(explore.id) not in captured.out
