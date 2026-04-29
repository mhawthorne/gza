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


class TestAdvanceAutoPlans:
    """Tests for auto-advancing completed plans via 'gza advance'."""

    def _setup_git_repo(self, tmp_path: Path):
        """Initialize a git repo in tmp_path with an initial commit on main."""
        from gza.git import Git
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")
        (tmp_path / "README.md").write_text("initial")
        git._run("add", "README.md")
        git._run("commit", "-m", "Initial commit")
        return git

    def _create_completed_plan(self, store, prompt="Design the feature"):
        """Create a completed plan task (no branch)."""
        from datetime import datetime
        plan = store.add(prompt, task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)
        return plan

    def _create_implement_task_with_branch(self, store, git, tmp_path, prompt="Implement feature", based_on=None):
        """Create a completed implement task with a real git branch."""
        from datetime import datetime
        task = store.add(prompt, task_type="implement", based_on=based_on)
        branch = f"feat/task-{task.id}"
        git._run("checkout", "-b", branch)
        (tmp_path / f"feat_{task.id}.txt").write_text("feature")
        git._run("add", f"feat_{task.id}.txt")
        git._run("commit", "-m", f"Add feature for task {task.id}")
        git._run("checkout", "main")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)
        return task

    def test_advance_creates_implement_for_completed_plan(self, tmp_path: Path):
        """advance creates and starts an implement task for a completed plan with no implement child."""
        import io
        from unittest.mock import patch

        from gza.cli import cmd_advance

        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        plan = self._create_completed_plan(store, "Design auth system")
        plan.slug = "20260305-design-auth-system-2"
        store.update(plan)

        spawn_calls = []

        def fake_spawn(worker_args, config, task_id=None, **_kw):
            spawn_calls.append(task_id)
            return 0

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            batch=None,
            no_docker=True,
        )

        with patch("gza.cli._spawn_background_worker", side_effect=fake_spawn):
            with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
                rc = cmd_advance(args)
                output = mock_stdout.getvalue()

        assert rc == 0
        assert "Created implement task" in output

        # Verify the implement task was created with plan dependency.
        all_tasks = store.get_all()
        impl_tasks = [t for t in all_tasks if t.task_type == "implement"]
        assert len(impl_tasks) == 1
        assert impl_tasks[0].depends_on == plan.id
        assert impl_tasks[0].based_on is None
        assert impl_tasks[0].prompt == f"Implement plan from task {plan.id}: design-auth-system"

    def test_advance_skips_plan_with_existing_implement(self, tmp_path: Path):
        """advance skips a completed plan that already has an implement child.

        When targeted by task_id, the skip message is shown. In batch mode
        the plan is simply excluded from the candidate list.
        """
        import io

        from gza.cli import cmd_advance

        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        plan = self._create_completed_plan(store, "Design auth system")
        # Create an implement task based on this plan
        store.add("Implement auth", task_type="implement", based_on=plan.id)

        # Target the plan by task_id to see the skip message
        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=plan.id,
            dry_run=True,
            auto=True,
            max=None,
            batch=None,
            no_docker=True,
        )

        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            rc = cmd_advance(args)
            output = mock_stdout.getvalue()

        assert rc == 0
        assert "implement task already exists" in output

    def test_advance_type_plan_filters_to_plans_only(self, tmp_path: Path):
        """--type plan only processes plan tasks, not implement tasks."""
        import io

        from gza.cli import cmd_advance

        # Disable review requirement so implement would normally merge
        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\n"
            "db_path: .gza/gza.db\n"
            "advance_requires_review: false\n"
        )

        store = make_store(tmp_path)
        git = self._setup_git_repo(tmp_path)

        plan = self._create_completed_plan(store, "Design feature X")
        self._create_implement_task_with_branch(store, git, tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=True,
            auto=True,
            max=None,
            batch=None,
            no_docker=True,
            advance_type="plan",
        )

        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            rc = cmd_advance(args)
            output = mock_stdout.getvalue()

        assert rc == 0
        # Plan should be in the output
        assert str(plan.id) in output
        assert "Create and start implement" in output
        # Implement task should NOT be in the output
        assert "Merge" not in output

    def test_advance_type_implement_filters_to_implements_only(self, tmp_path: Path):
        """--type implement only processes implement tasks, not plans."""
        import io

        from gza.cli import cmd_advance

        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\n"
            "db_path: .gza/gza.db\n"
            "advance_requires_review: false\n"
        )

        store = make_store(tmp_path)
        git = self._setup_git_repo(tmp_path)

        self._create_completed_plan(store, "Design feature X")
        impl = self._create_implement_task_with_branch(store, git, tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=True,
            auto=True,
            max=None,
            batch=None,
            no_docker=True,
            advance_type="implement",
        )

        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            rc = cmd_advance(args)
            output = mock_stdout.getvalue()

        assert rc == 0
        # Implement task should be in the output
        assert str(impl.id) in output
        assert "Merge" in output
        # Plan should NOT appear (no "Create and start implement")
        assert "Create and start implement" not in output

    def test_advance_create_implement_respects_batch_limit(self, tmp_path: Path):
        """batch limit applies to plan->implement worker spawns."""
        import io
        from unittest.mock import patch

        from gza.cli import cmd_advance

        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        # Create two completed plans
        self._create_completed_plan(store, "Plan A")
        self._create_completed_plan(store, "Plan B")

        spawn_calls = []

        def fake_spawn(worker_args, config, task_id=None, **_kw):
            spawn_calls.append(task_id)
            return 0

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            batch=1,
            no_docker=True,
        )

        with patch("gza.cli._spawn_background_worker", side_effect=fake_spawn):
            with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
                rc = cmd_advance(args)
                output = mock_stdout.getvalue()

        assert rc == 0
        # Only one worker should have been spawned due to batch limit
        assert len(spawn_calls) == 1
        assert "batch limit reached" in output
