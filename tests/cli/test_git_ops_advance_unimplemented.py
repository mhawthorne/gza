"""Tests for git operations CLI commands."""


import argparse
import io
import os
import shutil
import time
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from gza.cli import cmd_advance
from gza.config import Config
from gza.db import SqliteTaskStore

from .conftest import (
    make_store,
    run_gza,
    setup_config,
    setup_db_with_tasks,
    setup_git_repo_with_task_branch,
)


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


class TestAdvanceUnimplementedCommand:
    """Tests for 'gza advance --unimplemented' command."""

    def test_advance_unimplemented_lists_completed_plan_and_explore_without_impl(self, tmp_path: Path):
        """advance --unimplemented lists completed plans/explores with no implement task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Design the authentication system", task_type="plan")
        explore = store.add("Explore auth provider options", task_type="explore")
        plan.status = "completed"
        explore.status = "completed"
        from datetime import datetime
        now = datetime.now(UTC)
        plan.completed_at = now
        explore.completed_at = now
        store.update(plan)
        store.update(explore)

        result = run_gza("advance", "--unimplemented", "--project", str(tmp_path))

        assert result.returncode == 0
        assert str(plan.id) in result.stdout
        assert str(explore.id) in result.stdout
        assert "[plan]" in result.stdout
        assert "[explore]" in result.stdout
        assert "gza implement" in result.stdout

    def test_advance_unimplemented_excludes_tasks_with_impl(self, tmp_path: Path):
        """advance --unimplemented excludes tasks that already have an implement task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        from datetime import datetime

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

        result = run_gza("advance", "--unimplemented", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No plan/explore lineages without implementation tasks." in result.stdout

    def test_advance_unimplemented_prefers_pending_plan_descendant_and_create_targets_it(self, tmp_path: Path):
        """advance --unimplemented should surface the newer plan descendant, not the completed explore ancestor."""
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

        result = run_gza("advance", "--unimplemented", "--project", str(tmp_path))

        assert result.returncode == 0
        assert str(plan.id) in result.stdout
        assert "Plan the ingestion implementation" in result.stdout
        assert str(explore.id) not in result.stdout
        assert "Explore the ingestion approach" not in result.stdout
        assert f"gza implement {plan.id}" in result.stdout

        create_result = run_gza("advance", "--unimplemented", "--create", "--project", str(tmp_path))

        assert create_result.returncode == 0
        implement_tasks = [task for task in store.get_all() if task.task_type == "implement"]
        assert len(implement_tasks) == 1
        assert implement_tasks[0].depends_on == plan.id
        assert implement_tasks[0].depends_on != explore.id
        assert implement_tasks[0].prompt.startswith(f"Implement plan from task {plan.id}")

    def test_advance_unimplemented_still_lists_completed_plan_without_descendants(self, tmp_path: Path):
        """advance --unimplemented should keep showing a completed plan with no more specific source descendant."""
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

        result = run_gza("advance", "--unimplemented", "--project", str(tmp_path))

        assert result.returncode == 0
        assert str(plan.id) in result.stdout
        assert "Plan the scheduler rewrite" in result.stdout
        assert f"gza implement {plan.id}" in result.stdout

    def test_advance_unimplemented_excludes_lineage_with_descendant_implement(self, tmp_path: Path):
        """advance --unimplemented should drop the lineage when a descendant implement already exists."""
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

        result = run_gza("advance", "--unimplemented", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No plan/explore lineages without implementation tasks." in result.stdout
        assert str(explore.id) not in result.stdout
        assert str(plan.id) not in result.stdout

    def test_advance_unimplemented_prefers_newest_explore_descendant_deterministically(self, tmp_path: Path):
        """advance --unimplemented should choose the newest explore descendant when no implement exists."""
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

        result = run_gza("advance", "--unimplemented", "--project", str(tmp_path))

        assert result.returncode == 0
        assert str(latest_descendant.id) in result.stdout
        assert "Explore reporting delivery tradeoffs" in result.stdout
        assert str(root.id) not in result.stdout
        assert str(first_descendant.id) not in result.stdout
        assert "Explore reporting architecture" not in result.stdout
        assert "Explore reporting storage choices" not in result.stdout

    def test_advance_unimplemented_guidance_distinguishes_plan_vs_explore(self, tmp_path: Path):
        """advance --unimplemented guidance is accurate for explores in list mode."""
        from datetime import datetime

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

        result = run_gza("advance", "--unimplemented", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Run 'gza advance' to create and start implement tasks for completed plan tasks." in result.stdout
        assert "Run 'gza advance --unimplemented --create' to create implement tasks for completed explore tasks." in result.stdout
        assert "Run 'gza advance' to create and start implement tasks." not in result.stdout

    def test_advance_unimplemented_create_queues_implement_tasks(self, tmp_path: Path):
        """advance --unimplemented --create creates implement tasks for each listed task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        from datetime import datetime

        plan = store.add("Plan A", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        explore = store.add("Explore B", task_type="explore")
        explore.status = "completed"
        explore.completed_at = datetime.now(UTC)
        store.update(explore)

        result = run_gza("advance", "--unimplemented", "--create", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created" in result.stdout

        all_tasks = store.get_all()
        impl_tasks = [t for t in all_tasks if t.task_type == "implement"]
        assert len(impl_tasks) == 2
        by_depends_on = {t.depends_on: t for t in impl_tasks}
        assert plan.id in by_depends_on
        assert explore.id in by_depends_on
        assert all(t.based_on is None for t in impl_tasks)
        assert by_depends_on[plan.id].prompt.startswith(f"Implement plan from task {plan.id}")
        assert by_depends_on[explore.id].prompt.startswith(f"Implement findings from task {explore.id}")

    def test_advance_unimplemented_dry_run_no_create(self, tmp_path: Path):
        """advance --unimplemented --create --dry-run shows preview but creates nothing."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        from datetime import datetime

        plan = store.add("Plan C", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        result = run_gza("advance", "--unimplemented", "--create", "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "dry-run" in result.stdout.lower() or "Would create" in result.stdout

        all_tasks = store.get_all()
        impl_tasks = [t for t in all_tasks if t.task_type == "implement"]
        assert len(impl_tasks) == 0

    def test_advance_unimplemented_targeted_query_ignores_non_source_tasks(self, tmp_path: Path):
        """advance --unimplemented filters by implement based_on regardless of task noise."""
        from datetime import datetime

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
            t = store.add(f"Task {i}", task_type="review")
            t.based_on = plan_with_impl.id
            store.update(t)

        result = run_gza("advance", "--unimplemented", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Explore without impl" in result.stdout
        assert "Plan with impl" not in result.stdout

    def test_advance_plans_alias_keeps_plan_only_behavior(self, tmp_path: Path):
        """legacy --plans remains supported and only targets plan tasks."""
        from datetime import datetime

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

        result = run_gza("advance", "--plans", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "deprecated" in result.stderr.lower()
        assert str(plan.id) in result.stdout
        assert str(explore.id) not in result.stdout
