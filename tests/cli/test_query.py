"""Tests for task query and display CLI commands."""


import argparse
import json
import os
import re
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from rich.console import Console
from rich.text import Text

import gza.colors as colors
from gza.artifacts import store_command_output_artifact
from gza import dependency_preconditions as dependency_preconditions_module
from gza.cli import _queue_render as queue_render_cli, query as query_cli, watch as watch_cli
from gza.config import Config
from gza.console import truncate
from gza.db import Task
from gza.git import GitError
from gza.pr_ops import LookupTaskPrResult
from gza.review_verdict import ParsedReviewReport
from gza.sync_ops import BranchSyncResult

from .conftest import (
    make_store,
    mark_orphaned,
    run_gza,
    setup_config,
    setup_db_with_tasks,
)


def _projection_list_stdout(command_name: str, *, blocked_by_dropped: bool = False) -> str:
    return query_cli._format_projection_fields(  # noqa: SLF001
        query_cli._projection_field_choices(command_name, blocked_by_dropped=blocked_by_dropped)  # noqa: SLF001
    )


def _mock_unmerged_git() -> MagicMock:
    git = MagicMock()
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "main"
    git.branch_exists.return_value = True
    git.count_commits_ahead.return_value = 1
    git.get_diff_stat_parsed.return_value = (1, 1, 0)
    git.can_merge.return_value = True
    git.is_merged.return_value = False
    return git


class _FastUnmergedGit:
    def __init__(self) -> None:
        self._current_branch = "main"
        self._branches: set[str] = set()
        self._refs: set[str] = set()
        self._merged: dict[tuple[str, str | None], bool] = {}
        self._ahead_counts: dict[tuple[str, str], int | None] = {}
        self._can_merge: dict[tuple[str, str | None], bool] = {}
        self._numstat = "1\t0\tfeature.txt\n"
        self._fetch_error: GitError | None = None
        self.fetch_calls: list[str] = []

    def default_branch(self) -> str:
        return "main"

    def current_branch(self) -> str:
        return self._current_branch

    def branch_exists(self, branch: str) -> bool:
        return bool(branch) and (not self._branches or branch in self._branches)

    def ref_exists(self, ref: str) -> bool:
        return ref in self._refs

    def is_merged(self, branch: str, into: str | None = None, use_cherry: bool = False) -> bool:
        return self._merged.get((branch, into), False)

    def count_commits_ahead(self, branch: str, target: str) -> int:
        return 1

    def count_commits_ahead_checked(self, branch: str, target: str) -> int | None:
        return self._ahead_counts.get((branch, target), 1)

    def get_diff_stat_parsed(self, revision_range: str) -> tuple[int, int, int]:
        return (1, 1, 0)

    def get_diff_numstat(self, revision_range: str) -> str:
        return self._numstat

    def can_merge(self, branch: str, into: str | None = None) -> bool:
        return self._can_merge.get((branch, into), True)

    def fetch(self, remote: str = "origin") -> None:
        self.fetch_calls.append(remote)
        if self._fetch_error is not None:
            raise self._fetch_error
        return None

    def run_forbidden_subprocess(self, *args: str) -> None:
        raise AssertionError("fast unmerged git double does not run subprocesses")


class _UnavailableGitHub:
    def is_available(self) -> bool:
        return False


class _RecordingConsole:
    def __init__(self) -> None:
        self.outputs: list[object] = []

    def print(self, renderable: object = "", *args, **kwargs) -> None:
        self.outputs.append(renderable)


def _styled_substrings(text: Text) -> dict[str, str | None]:
    styled: dict[str, str | None] = {}
    for span in text.spans:
        styled[text.plain[span.start:span.end]] = span.style
    return styled


def _span_styles(text: Text) -> list[str | None]:
    return [span.style for span in text.spans]


def _setup_unmerged_env_fast(
    tmp_path: Path,
    *,
    task_prompt: str = "Add feature",
    task_type: str = "implement",
    task_id: str = "20260212-add-feature",
    branch: str = "feature/test",
    merge_status: str | None = "unmerged",
    status: str = "completed",
    has_commits: bool = True,
):
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add(task_prompt, task_type=task_type)
    task.status = status
    if status in ("completed", "failed", "dropped"):
        task.completed_at = datetime.now(UTC)
    task.branch = branch
    task.has_commits = has_commits
    task.merge_status = merge_status
    task.slug = task_id
    store.update(task)

    return store, task, _FastUnmergedGit()


setup_unmerged_env = _setup_unmerged_env_fast


def _setup_task_with_worktree_metadata(
    tmp_path: Path,
    *,
    task_prompt: str,
    branch_name: str,
    worktree_name: str | None,
) -> tuple[Task, Path | None]:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add(task_prompt)
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = branch_name
    store.update(task)
    worktree_path = None
    if worktree_name is not None:
        worktree_path = tmp_path / "worktrees" / worktree_name
        worktree_path.parent.mkdir(parents=True, exist_ok=True)
    assert task.id is not None
    return task, worktree_path


def _create_failed_recovery_candidate(
    store,
    *,
    prompt: str = "Failed task",
    task_type: str = "implement",
    failure_reason: str = "MAX_TURNS",
    session_id: str | None = "sess-1",
) -> Task:
    task = store.add(prompt, task_type=task_type)
    assert task.id is not None
    task.status = "failed"
    task.failure_reason = failure_reason
    task.session_id = session_id
    task.completed_at = datetime.now(UTC)
    store.update(task)
    return task


def _first_nonempty_output_line(output: str) -> str:
    return next(line for line in output.splitlines() if line.strip())


def _lineage_root_id(output: str) -> str:
    match = re.search(r"\b[a-z0-9]{1,12}-\d+\b", _first_nonempty_output_line(output))
    assert match is not None
    return match.group(0)


def _seed_same_branch_merge_owner_and_improve(tmp_path: Path) -> tuple[Task, Task]:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    owner = store.add("Owning implement", task_type="implement")
    assert owner.id is not None
    owner.status = "completed"
    owner.has_commits = True
    owner.branch = "feature/shared-branch"
    owner.merge_status = "unmerged"
    owner.completed_at = datetime(2026, 5, 4, 11, 0, 0, tzinfo=UTC)
    store.update(owner)

    improve = store.add("Shared-branch improve", task_type="improve", based_on=owner.id)
    assert improve.id is not None
    improve.status = "completed"
    improve.has_commits = True
    improve.branch = owner.branch
    improve.merge_status = "unmerged"
    improve.completed_at = datetime(2026, 5, 4, 11, 10, 0, tzinfo=UTC)
    store.update(improve)

    return owner, improve


def _setup_lineage_owner_parity_fixture(tmp_path: Path) -> dict[str, Task]:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    plan = store.add("Parity plan context", task_type="plan")
    plan.status = "completed"
    plan.completed_at = datetime(2026, 5, 14, 9, 0, tzinfo=UTC)
    store.update(plan)
    assert plan.id is not None

    impl = store.add("Parity implement owner", task_type="implement", based_on=plan.id)
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
    impl.branch = "feature/parity-owner"
    impl.has_commits = True
    impl.merge_status = "unmerged"
    store.update(impl)
    assert impl.id is not None

    unit = store.create_merge_unit(
        source_branch=impl.branch,
        target_branch="main",
        owner_task_id=impl.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(impl.id, unit.id, "owner")

    review = store.add(
        "Parity review context",
        task_type="review",
        based_on=impl.id,
        depends_on=impl.id,
    )
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 14, 11, 0, tzinfo=UTC)
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    store.update(review)
    assert review.id is not None
    store.attach_task_to_merge_unit(review.id, unit.id, "review")

    improve = store.add(
        "Parity improve follow-up",
        task_type="improve",
        based_on=impl.id,
        depends_on=review.id,
        same_branch=True,
    )
    improve.status = "in_progress"
    improve.branch = impl.branch
    improve.has_commits = True
    store.update(improve)
    assert improve.id is not None
    store.attach_task_to_merge_unit(improve.id, unit.id, "improve")

    dropped_one = store.add(
        "Parity dropped rebase one",
        task_type="rebase",
        based_on=impl.id,
        same_branch=True,
    )
    dropped_one.status = "dropped"
    dropped_one.completed_at = datetime(2026, 5, 14, 12, 0, tzinfo=UTC)
    dropped_one.branch = impl.branch
    dropped_one.has_commits = True
    store.update(dropped_one)
    assert dropped_one.id is not None
    store.attach_task_to_merge_unit(dropped_one.id, unit.id, "rebase")

    dropped_two = store.add(
        "Parity dropped rebase two",
        task_type="rebase",
        based_on=dropped_one.id,
        same_branch=True,
    )
    dropped_two.status = "dropped"
    dropped_two.completed_at = datetime(2026, 5, 14, 13, 0, tzinfo=UTC)
    dropped_two.branch = impl.branch
    dropped_two.has_commits = True
    store.update(dropped_two)
    assert dropped_two.id is not None
    store.attach_task_to_merge_unit(dropped_two.id, unit.id, "rebase")

    return {
        "plan": plan,
        "impl": impl,
        "review": review,
        "improve": improve,
        "dropped_one": dropped_one,
        "dropped_two": dropped_two,
    }


def _setup_branchless_rebase_owner_fixture(tmp_path: Path) -> dict[str, Task]:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Branch owner implementation", task_type="implement")
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
    impl.branch = "feature/branchless-rebase-owner"
    impl.has_commits = True
    impl.merge_status = "unmerged"
    store.update(impl)
    assert impl.id is not None

    rebase = store.add(
        "Branchless rebase child",
        task_type="rebase",
        based_on=impl.id,
        same_branch=True,
    )
    rebase.status = "completed"
    rebase.completed_at = datetime(2026, 5, 14, 11, 0, tzinfo=UTC)
    rebase.has_commits = True
    store.update(rebase)
    assert rebase.id is not None

    return {"impl": impl, "rebase": rebase}


def _setup_retry_review_owner_fixture(tmp_path: Path) -> dict[str, Task]:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    root = store.add("Retry owner root implementation", task_type="implement")
    root.status = "completed"
    root.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
    root.branch = "feature/retry-owner"
    root.has_commits = True
    root.merge_status = "unmerged"
    store.update(root)
    assert root.id is not None

    retry = store.add(
        "Retry implementation on same branch",
        task_type="implement",
        based_on=root.id,
        same_branch=True,
    )
    retry.status = "completed"
    retry.completed_at = datetime(2026, 5, 14, 11, 0, tzinfo=UTC)
    retry.branch = root.branch
    retry.has_commits = True
    retry.merge_status = "unmerged"
    store.update(retry)
    assert retry.id is not None

    review = store.add(
        "Review of same-branch retry",
        task_type="review",
        based_on=retry.id,
        depends_on=retry.id,
    )
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 14, 12, 0, tzinfo=UTC)
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    store.update(review)
    assert review.id is not None

    return {"root": root, "retry": retry, "review": review}


def _setup_plan_fanout_fixture(tmp_path: Path) -> dict[str, Task]:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    plan = store.add("Fanout plan root", task_type="plan")
    plan.status = "completed"
    plan.completed_at = datetime(2026, 5, 14, 9, 0, tzinfo=UTC)
    store.update(plan)
    assert plan.id is not None

    impl_a = store.add("Fanout implement alpha", task_type="implement", based_on=plan.id)
    impl_a.status = "completed"
    impl_a.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
    impl_a.branch = "feature/fanout-alpha"
    impl_a.has_commits = True
    impl_a.merge_status = "unmerged"
    store.update(impl_a)
    assert impl_a.id is not None

    unit_a = store.create_merge_unit(
        source_branch=impl_a.branch,
        target_branch="main",
        owner_task_id=impl_a.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(impl_a.id, unit_a.id, "owner")

    impl_b = store.add("Fanout implement beta", task_type="implement", based_on=plan.id)
    impl_b.status = "completed"
    impl_b.completed_at = datetime(2026, 5, 14, 11, 0, tzinfo=UTC)
    impl_b.branch = "feature/fanout-beta"
    impl_b.has_commits = True
    impl_b.merge_status = "unmerged"
    store.update(impl_b)
    assert impl_b.id is not None

    unit_b = store.create_merge_unit(
        source_branch=impl_b.branch,
        target_branch="main",
        owner_task_id=impl_b.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(impl_b.id, unit_b.id, "owner")

    return {"plan": plan, "impl_a": impl_a, "impl_b": impl_b}


def _unmerged_branch_block(output: str, branch: str) -> str:
    """Return the rendered unmerged output block for a specific branch."""
    for block in output.split("-" * 32):
        if f"branch: {branch}" in block:
            return block
    raise AssertionError(f"Branch block not found for {branch}")


def _drop_task_comments_column(db_path: Path, column_name: str) -> None:
    """Rebuild task_comments without a specific column."""
    import sqlite3

    conn = sqlite3.connect(db_path)
    conn.execute("ALTER TABLE task_comments RENAME TO task_comments_old")
    cols = [row[1] for row in conn.execute("PRAGMA table_info(task_comments_old)")]
    kept_cols = [col for col in cols if col != column_name]
    cols_str = ", ".join(kept_cols)
    col_defs = []
    for row in conn.execute("PRAGMA table_info(task_comments_old)"):
        if row[1] == column_name:
            continue
        name, typ, notnull, dflt, pk = row[1], row[2], row[3], row[4], row[5]
        parts = [name, typ]
        if pk:
            parts.append("PRIMARY KEY")
        if notnull and not pk:
            parts.append("NOT NULL")
        if dflt is not None:
            parts.append(f"DEFAULT {dflt}")
        col_defs.append(" ".join(parts))
    conn.execute(f"CREATE TABLE task_comments ({', '.join(col_defs)})")
    conn.execute(f"INSERT INTO task_comments ({cols_str}) SELECT {cols_str} FROM task_comments_old")
    conn.execute("DROP TABLE task_comments_old")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_task_comments_task_created ON task_comments(task_id, created_at ASC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_task_comments_task_unresolved ON task_comments(task_id, resolved_at)")
    conn.commit()
    conn.close()


def _drop_tasks_column(db_path: Path, column_name: str) -> None:
    """Rebuild tasks without a specific column."""
    import sqlite3

    def _quote(column: str) -> str:
        return f'"{column}"' if column in ("group",) else column

    conn = sqlite3.connect(db_path)
    conn.execute("ALTER TABLE tasks RENAME TO tasks_old")
    cols = [row[1] for row in conn.execute("PRAGMA table_info(tasks_old)")]
    kept_cols = [col for col in cols if col != column_name]
    cols_str = ", ".join(_quote(col) for col in kept_cols)
    col_defs = []
    pragma_rows = list(conn.execute("PRAGMA table_info(tasks_old)"))
    pk_cols = [(row[5], row[1]) for row in pragma_rows if row[1] != column_name and row[5]]
    has_composite_pk = len(pk_cols) > 1
    for row in pragma_rows:
        if row[1] == column_name:
            continue
        name, typ, notnull, dflt, pk = row[1], row[2], row[3], row[4], row[5]
        quoted_name = _quote(name)
        parts = [quoted_name, typ]
        if pk and not has_composite_pk:
            parts.append("PRIMARY KEY")
        if notnull and not pk:
            parts.append("NOT NULL")
        if dflt is not None:
            parts.append(f"DEFAULT {dflt}")
        col_defs.append(" ".join(parts))
    if has_composite_pk:
        ordered_pk = ", ".join(_quote(name) for _, name in sorted(pk_cols, key=lambda item: item[0]))
        col_defs.append(f"PRIMARY KEY({ordered_pk})")
    conn.execute(f"CREATE TABLE tasks ({', '.join(col_defs)})")
    conn.execute(f"INSERT INTO tasks ({cols_str}) SELECT {cols_str} FROM tasks_old")
    conn.execute("DROP TABLE tasks_old")
    conn.commit()
    conn.close()


class TestHistoryCommand:
    """Tests for 'gza history' command."""

    def test_history_with_tasks(self, tmp_path: Path):
        """History command works with SQLite tasks."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Test task 1", "status": "completed"},
            {"prompt": "Test task 2", "status": "failed"},
            {"prompt": "Test task 3", "status": "pending"},
        ])

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Test task 1" in result.stdout
        assert "Test task 2" in result.stdout
        assert "Test task 3" not in result.stdout  # pending tasks not shown

    def test_history_with_no_tasks(self, tmp_path: Path):
        """History command handles missing database gracefully."""
        setup_config(tmp_path)
        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No completed or failed tasks" in result.stdout

    def test_history_with_empty_tasks(self, tmp_path: Path):
        """History command handles empty tasks list."""
        # Create empty database
        setup_db_with_tasks(tmp_path, [])

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No completed or failed tasks" in result.stdout

    def test_history_query_only_pre_v40_missing_completion_reason_reads_without_traceback(
        self, tmp_path: Path
    ):
        """History should read a frozen v39 snapshot without forcing the v40 additive migration."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Completed before v40")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        db_path = tmp_path / ".gza" / "gza.db"
        _drop_tasks_column(db_path, "completion_reason")
        with sqlite3.connect(db_path) as conn:
            conn.execute("UPDATE schema_version SET version = 39")
            conn.commit()

        original_mode = db_path.stat().st_mode
        os.chmod(db_path, 0o444)
        try:
            result = run_gza("history", "--project", str(tmp_path))
        finally:
            os.chmod(db_path, original_mode)

        assert result.returncode == 0
        assert "Completed before v40" in result.stdout
        assert "Traceback" not in result.stdout
        assert "Traceback" not in result.stderr
        assert "tasks.completion_reason" in result.stderr

    def test_history_query_only_pre_v41_missing_recovery_origin_reads_without_traceback(
        self, tmp_path: Path
    ):
        """History should read a frozen v40 snapshot without forcing the v41 additive migration."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Completed before v41")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        db_path = tmp_path / ".gza" / "gza.db"
        _drop_tasks_column(db_path, "recovery_origin")
        with sqlite3.connect(db_path) as conn:
            conn.execute("UPDATE schema_version SET version = 40")
            conn.commit()

        original_mode = db_path.stat().st_mode
        os.chmod(db_path, 0o444)
        try:
            result = run_gza("history", "--project", str(tmp_path))
        finally:
            os.chmod(db_path, original_mode)

        assert result.returncode == 0
        assert "Completed before v41" in result.stdout
        assert "Traceback" not in result.stdout
        assert "Traceback" not in result.stderr
        assert "tasks.recovery_origin" in result.stderr

    def test_history_reads_frozen_snapshot_without_startup_write_error(self, tmp_path: Path):
        """History should inspect a read-only snapshot without triggering startup writes."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Frozen history task")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        db_path = tmp_path / ".gza" / "gza.db"
        original_mode = db_path.stat().st_mode
        os.chmod(db_path, 0o444)
        try:
            result = run_gza("history", "--project", str(tmp_path))
        finally:
            os.chmod(db_path, original_mode)

        assert result.returncode == 0
        assert "Frozen history task" in result.stdout
        assert "readonly" not in result.stderr.lower()

    def test_history_filter_by_completed_status(self, tmp_path: Path):
        """History command filters by completed status."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Completed task 1", "status": "completed"},
            {"prompt": "Completed task 2", "status": "completed"},
            {"prompt": "Failed task", "status": "failed"},
            {"prompt": "Unmerged task", "status": "unmerged"},
        ])

        result = run_gza("history", "--status", "completed", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Completed task 1" in result.stdout
        assert "Completed task 2" in result.stdout
        assert "Failed task" not in result.stdout
        assert "Unmerged task" not in result.stdout

    def test_history_filter_by_failed_status(self, tmp_path: Path):
        """History command filters by failed status."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Completed task", "status": "completed"},
            {"prompt": "Failed task 1", "status": "failed"},
            {"prompt": "Failed task 2", "status": "failed"},
            {"prompt": "Unmerged task", "status": "unmerged"},
        ])

        result = run_gza("history", "--status", "failed", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Failed task 1" in result.stdout
        assert "Failed task 2" in result.stdout
        assert "Completed task" not in result.stdout
        assert "Unmerged task" not in result.stdout

    def test_history_filter_by_unmerged_status(self, tmp_path: Path):
        """History command filters by unmerged status."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Completed task", "status": "completed"},
            {"prompt": "Failed task", "status": "failed"},
            {"prompt": "Unmerged task 1", "status": "unmerged"},
            {"prompt": "Unmerged task 2", "status": "unmerged"},
        ])

        result = run_gza("history", "--status", "unmerged", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Unmerged task 1" in result.stdout
        assert "Unmerged task 2" in result.stdout
        assert "Completed task" not in result.stdout
        assert "Failed task" not in result.stdout

    def test_history_filter_with_no_matching_tasks(self, tmp_path: Path):
        """History command handles no tasks matching filter."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Completed task", "status": "completed"},
        ])

        result = run_gza("history", "--status", "failed", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No completed or failed tasks with status 'failed'" in result.stdout

    def test_history_filter_by_task_type(self, tmp_path: Path):
        """History command filters by task_type."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Regular task", "status": "completed", "task_type": "plan"},
            {"prompt": "Explore task", "status": "completed", "task_type": "explore"},
            {"prompt": "Plan task", "status": "completed", "task_type": "plan"},
            {"prompt": "Implement task", "status": "completed", "task_type": "implement"},
            {"prompt": "Review task", "status": "completed", "task_type": "review"},
            {"prompt": "Improve task", "status": "completed", "task_type": "improve"},
        ])

        result = run_gza("history", "--type", "implement", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Implement task" in result.stdout
        assert "Regular task" not in result.stdout
        assert "Explore task" not in result.stdout
        assert "Plan task" not in result.stdout
        assert "Review task" not in result.stdout
        assert "Improve task" not in result.stdout

    def test_history_filter_by_multiple_types(self, tmp_path: Path):
        """History command filters by task_type for different types."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Regular task", "status": "completed", "task_type": "implement"},
            {"prompt": "Explore task 1", "status": "completed", "task_type": "explore"},
            {"prompt": "Explore task 2", "status": "completed", "task_type": "explore"},
            {"prompt": "Plan task", "status": "completed", "task_type": "plan"},
        ])

        result = run_gza("history", "--type", "explore", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Explore task 1" in result.stdout
        assert "Explore task 2" in result.stdout
        assert "Regular task" not in result.stdout
        assert "Plan task" not in result.stdout

    def test_history_filter_by_status_and_type(self, tmp_path: Path):
        """History command filters by both status and task_type."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Completed implement", "status": "completed", "task_type": "implement"},
            {"prompt": "Failed implement", "status": "failed", "task_type": "implement"},
            {"prompt": "Completed plan", "status": "completed", "task_type": "plan"},
        ])

        result = run_gza("history", "--status", "completed", "--type", "implement", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Completed implement" in result.stdout
        assert "Failed implement" not in result.stdout
        assert "Completed plan" not in result.stdout

    def test_history_filter_by_type_no_matching_tasks(self, tmp_path: Path):
        """History command handles no tasks matching type filter."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Regular task", "status": "completed", "task_type": "implement"},
        ])

        result = run_gza("history", "--type", "review", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No completed or failed tasks with type 'review'" in result.stdout

    def test_history_excludes_internal_tasks_by_default(self, tmp_path: Path):
        """Default history output omits internal tasks."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Implement task", "status": "completed", "task_type": "implement"},
            {"prompt": "Internal task", "status": "completed", "task_type": "internal"},
        ])

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Implement task" in result.stdout
        assert "Internal task" not in result.stdout

    def test_history_internal_type_includes_internal_tasks(self, tmp_path: Path):
        """Explicit --type internal includes internal tasks."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Implement task", "status": "completed", "task_type": "implement"},
            {"prompt": "Internal task", "status": "completed", "task_type": "internal"},
        ])

        result = run_gza("history", "--type", "internal", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Internal task" in result.stdout
        assert "Implement task" not in result.stdout

    def test_history_json_matches_default_internal_filtering(self, tmp_path: Path):
        """history and history --json should select the same default task IDs."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        public_task = store.add("public history task", task_type="implement")
        public_task.status = "completed"
        public_task.completed_at = datetime.now(UTC)
        store.update(public_task)

        internal_task = store.add("internal history task", task_type="internal")
        internal_task.status = "completed"
        internal_task.completed_at = datetime.now(UTC)
        store.update(internal_task)

        text_result = run_gza("history", "--project", str(tmp_path))
        json_result = run_gza("history", "--json", "--project", str(tmp_path))

        assert text_result.returncode == 0
        assert json_result.returncode == 0
        assert "public history task" in text_result.stdout
        assert "internal history task" not in text_result.stdout

        payload = json.loads(json_result.stdout)
        ids = {row["id"] for row in payload}
        assert public_task.id in ids
        assert internal_task.id not in ids

    def test_history_internal_type_json_includes_internal_rows(self, tmp_path: Path):
        """history --type internal --json should include internal tasks."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        public_task = store.add("public history task", task_type="implement")
        public_task.status = "completed"
        public_task.completed_at = datetime.now(UTC)
        store.update(public_task)

        internal_task = store.add("internal history task", task_type="internal")
        internal_task.status = "completed"
        internal_task.completed_at = datetime.now(UTC)
        store.update(internal_task)

        result = run_gza("history", "--type", "internal", "--json", "--project", str(tmp_path))

        assert result.returncode == 0
        payload = json.loads(result.stdout)
        ids = {row["id"] for row in payload}
        assert internal_task.id in ids
        assert public_task.id not in ids

    def test_history_text_fields_multi_field_uses_generic_blocks(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("history projection test", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = run_gza(
            "history",
            "--fields",
            "id,prompt",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert f"id: {task.id}" in result.stdout
        assert "prompt: history projection test" in result.stdout

    def test_history_text_fields_single_field_prints_bare_values(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("history bare value", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = run_gza("history", "--fields", "id", "--project", str(tmp_path))

        assert result.returncode == 0
        assert result.stdout.strip() == task.id

    def test_history_json_fields_override_limits_projection(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("history json projection", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = run_gza(
            "history",
            "--json",
            "--fields",
            "id,status",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        payload = json.loads(result.stdout)
        assert payload == [{"id": task.id, "status": "completed"}]

    def test_history_unknown_fields_list_valid_choices(self, tmp_path: Path):
        setup_config(tmp_path)
        make_store(tmp_path).add("history unknown field", task_type="implement")

        result = run_gza("history", "--fields", "id,nope", "--project", str(tmp_path))

        assert result.returncode == 2
        assert "unknown field for gza history: nope" in result.stderr
        assert "valid fields:" in result.stderr
        assert "id" in result.stderr
        assert "Run uv run gza history --list-fields to list valid fields." in result.stderr

    def test_history_list_fields_prints_valid_projection_choices(self, tmp_path: Path):
        setup_config(tmp_path)

        result = run_gza("history", "--list-fields", "--project", str(tmp_path))

        assert result.returncode == 0
        assert result.stdout.strip() == _projection_list_stdout("history")
        assert "group" not in result.stdout.split()
        assert result.stderr == ""

    def test_history_rejects_group_projection_field(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("history retired group field", task_type="implement", tags=("release",))
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = run_gza("history", "--fields", "group", "--project", str(tmp_path))

        assert result.returncode == 2
        assert "unknown field for gza history: group" in result.stderr
        assert "Run uv run gza history --list-fields to list valid fields." in result.stderr

    def test_history_rejects_next_action_projection_field(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("history next action unsupported", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = run_gza("history", "--fields", "next_action", "--project", str(tmp_path))

        assert result.returncode == 2
        assert "unknown field for gza history: next_action" in result.stderr
        assert "valid fields:" in result.stderr
        assert "id" in result.stderr
        assert "next_action" not in result.stderr.split("valid fields:", 1)[1]

    def test_history_shows_task_type_labels(self, tmp_path: Path):
        """History command displays task type labels for all task types."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Implement task", "status": "completed", "task_type": "implement"},
            {"prompt": "Plan task", "status": "completed", "task_type": "plan"},
        ])

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        # Type labels are now on a separate line
        assert "Implement task" in result.stdout
        assert "[implement]" in result.stdout
        assert "Plan task" in result.stdout
        assert "[plan]" in result.stdout

    def test_history_uses_default_target_merge_unit_state_for_status_label(self, tmp_path: Path):
        """History should show unit-backed unmerged status even when the task row is stale."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = query_cli.get_store(config, open_mode="readwrite")

        task = store.add("Stale merged row", task_type="implement")
        store.mark_completed(task, has_commits=True, branch="feature/history-status")
        assert task.id is not None

        target_branch = store.default_merge_target()
        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None
        assert unit.target_branch == target_branch
        store.set_merge_unit_state(unit.id, "unmerged")

        stale_row = store.get(task.id)
        assert stale_row is not None
        stale_row.merge_status = "merged"
        store.update(stale_row)

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        normalized = " ".join(result.stdout.split())
        assert "unmerged" in normalized
        assert "[merged]" not in normalized

    def test_history_uses_default_target_merge_unit_state_for_merge_label(self, tmp_path: Path):
        """History should render the merged badge from the default-target merge unit."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = query_cli.get_store(config, open_mode="readwrite")

        task = store.add("Unit merged row", task_type="implement")
        store.mark_completed(task, has_commits=True, branch="feature/history-merged")
        assert task.id is not None

        target_branch = store.default_merge_target()
        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None
        assert unit.target_branch == target_branch
        store.set_merge_unit_state(unit.id, "merged")

        stale_row = store.get(task.id)
        assert stale_row is not None
        stale_row.merge_status = "unmerged"
        store.update(stale_row)

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        normalized = " ".join(result.stdout.split())
        assert "Unit merged row [implement] [merged]" in normalized
        assert "unmerged" not in result.stdout

    def test_history_renders_empty_merge_unit_as_moot(self, tmp_path: Path):
        """History should visibly surface empty merge units as moot/empty rows."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = query_cli.get_store(config, open_mode="readwrite")

        task = store.add("Historical empty row", task_type="implement")
        store.mark_completed(task, has_commits=True, branch="feature/history-empty")
        assert task.id is not None

        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None
        store.set_merge_unit_state(unit.id, "empty")

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        normalized = " ".join(result.stdout.split())
        assert "Historical empty row [implement] [empty]" in normalized
        assert "lifecycle: moot (no unique commits vs target)" in result.stdout

    def test_history_shows_orphaned_tasks_at_top(self, tmp_path: Path):
        """History command includes orphaned in-progress tasks at the top."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        from gza.config import Config
        config = Config.load(tmp_path)
        store = SqliteTaskStore(db_path, prefix=config.project_prefix)

        # Create an orphaned (in-progress, no worker) task
        orphaned_task = store.add("Orphaned task needing attention")
        mark_orphaned(store, orphaned_task)

        # Create a completed task
        completed_task = store.add("Completed task")
        completed_task.status = "completed"
        completed_task.completed_at = datetime.now(UTC)
        store.update(completed_task)

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "orphaned" in result.stdout
        # Prompt may be truncated — assert a prefix short enough to survive
        # layout changes as task IDs widen (e.g. the v25 padding to width 6).
        assert "Orphaned task" in result.stdout
        assert "Completed task" in result.stdout
        # Orphaned should appear before completed in output
        orphaned_pos = result.stdout.find("Orphaned task")
        completed_pos = result.stdout.find("Completed task")
        assert orphaned_pos < completed_pos, "Orphaned task should appear before completed task"

    def test_history_orphaned_shows_resume_suggestion(self, tmp_path: Path):
        """History command shows resume suggestion for orphaned tasks."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        orphaned_task = store.add("Orphaned task")
        mark_orphaned(store, orphaned_task)

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"gza work {orphaned_task.id}" in result.stdout

    def test_history_no_orphaned_when_status_filter_set(self, tmp_path: Path):
        """History command does not show orphaned tasks when --status filter is active."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create an orphaned task
        orphaned_task = store.add("Orphaned task")
        mark_orphaned(store, orphaned_task)

        # Create a completed task
        completed_task = store.add("Completed task")
        completed_task.status = "completed"
        completed_task.completed_at = datetime.now(UTC)
        store.update(completed_task)

        result = run_gza("history", "--status", "completed", "--project", str(tmp_path))

        assert result.returncode == 0
        # Orphaned should NOT appear when a status filter is specified
        assert "orphaned" not in result.stdout
        assert "Completed task" in result.stdout

    def test_history_lookback_days(self, tmp_path: Path):
        """--days excludes old tasks and includes recent ones."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        now = datetime.now(UTC)

        old = store.add("Old task")
        old.status = "completed"
        old.completed_at = now - timedelta(days=30)
        store.update(old)

        recent = store.add("Recent task")
        recent.status = "completed"
        recent.completed_at = now - timedelta(days=1)
        store.update(recent)

        result = run_gza("history", "--days", "7", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Recent task" in result.stdout
        assert "Old task" not in result.stdout

    def test_history_lineage_depth(self, tmp_path: Path):
        """--lineage-depth shows a branch-rendered tree."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        parent = store.add("Parent task")
        parent.status = "completed"
        parent.completed_at = datetime.now(UTC)
        store.update(parent)

        child = store.add("Child task", based_on=parent.id)
        child.status = "completed"
        child.completed_at = datetime.now(UTC)
        store.update(child)

        result = run_gza("history", "--lineage-depth", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Parent task" in result.stdout
        assert "Child task" in result.stdout
        assert "└──" in result.stdout or "├──" in result.stdout

    def test_history_lineage_depth_two(self, tmp_path: Path):
        """--lineage-depth 2 renders all three levels of a grandparent→parent→child chain."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        grandparent = store.add("Grandparent task")
        grandparent.status = "completed"
        grandparent.completed_at = datetime.now(UTC)
        store.update(grandparent)

        parent = store.add("Parent task", based_on=grandparent.id)
        parent.status = "completed"
        parent.completed_at = datetime.now(UTC)
        store.update(parent)

        child = store.add("Child task", based_on=parent.id)
        child.status = "completed"
        child.completed_at = datetime.now(UTC)
        store.update(child)

        result = run_gza("history", "--lineage-depth", "2", "--project", str(tmp_path))

        assert result.returncode == 0
        # All three levels of the chain must appear in the output
        assert "Grandparent task" in result.stdout
        assert "Parent task" in result.stdout
        assert "Child task" in result.stdout

    def test_history_lineage_orders_completed_root_before_pending_descendants(self, tmp_path: Path):
        """Lineage rendering keeps ancestor-first order even when descendants are pending."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("Root done", task_type="implement")
        root.status = "completed"
        root.completed_at = datetime(2026, 3, 1, tzinfo=UTC)
        store.update(root)

        child = store.add("Child pend", task_type="implement", based_on=root.id)
        grandchild = store.add("Gchild pend", task_type="implement", based_on=child.id)
        assert child.id is not None
        assert grandchild.id is not None

        result = run_gza("history", "--lineage-depth", "2", "--project", str(tmp_path))

        assert result.returncode == 0
        root_idx = result.stdout.index("Root done")
        child_idx = result.stdout.index("Child pend")
        grandchild_idx = result.stdout.index("Gchild pend")
        assert root_idx < child_idx < grandchild_idx

    def test_history_lineage_same_branch_children_render_compact_without_repeated_connectors(
        self,
        tmp_path: Path,
    ):
        """Same-branch review/improve children render compactly and avoid connector/status/branch bugs."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement root", task_type="implement")
        impl.status = "completed"
        impl.completed_at = datetime.now(UTC)
        impl.branch = "20260412-impl-history-lineage"
        impl.merge_status = "merged"
        store.update(impl)
        assert impl.id is not None

        review = store.add(
            "Review root",
            task_type="review",
            based_on=impl.id,
            depends_on=impl.id,
            same_branch=True,
        )
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.branch = impl.branch
        review.merge_status = "unmerged"
        review.report_file = "reviews/review.md"
        review.output_content = "Verdict: CHANGES_REQUESTED\n\nNeeds revisions."
        review.duration_seconds = 99
        review.cost_usd = 0.25
        store.update(review)
        assert review.id is not None

        improve = store.add(
            "Improve root",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
        )
        improve.status = "completed"
        improve.completed_at = datetime.now(UTC)
        improve.branch = impl.branch
        improve.merge_status = "unmerged"
        improve.report_file = "reviews/improve.md"
        improve.duration_seconds = 111
        improve.cost_usd = 0.33
        store.update(improve)
        assert improve.id is not None

        result = run_gza("history", "--lineage-depth", "2", "--project", str(tmp_path))

        assert result.returncode == 0
        assert re.search(rf"completed\s+{re.escape(review.id)}", result.stdout)
        assert re.search(rf"completed\s+{re.escape(improve.id)}", result.stdout)
        assert f"unmerged  {review.id}" not in result.stdout
        assert f"unmerged  {improve.id}" not in result.stdout
        assert "verdict:" in result.stdout
        assert "CHANGES_REQUESTED" in result.stdout
        assert "| stats:" in result.stdout
        assert result.stdout.count("branch: ") == 1
        assert "└──     [review]" not in result.stdout
        assert "├──     [review]" not in result.stdout
        assert "└──     [improve]" not in result.stdout
        assert "├──     [improve]" not in result.stdout
        assert "report:" not in result.stdout

    def test_history_last_flag(self, tmp_path: Path):
        """--last limits the number of tasks shown."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Task 1", "status": "completed"},
            {"prompt": "Task 2", "status": "completed"},
            {"prompt": "Task 3", "status": "completed"},
        ])

        result = run_gza("history", "--last", "2", "--project", str(tmp_path))

        assert result.returncode == 0
        # Exactly 2 tasks shown (the 2 most recent)
        assert result.stdout.count("Task ") == 2

    def test_history_n_shorthand(self, tmp_path: Path):
        """-n is shorthand for --last."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Task A", "status": "completed"},
            {"prompt": "Task B", "status": "completed"},
            {"prompt": "Task C", "status": "completed"},
        ])

        result = run_gza("history", "-n", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert result.stdout.count("Task ") == 1

    def test_history_start_date(self, tmp_path: Path):
        """--start-date excludes tasks before the given date."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        now = datetime.now(UTC)

        old = store.add("Old task")
        old.status = "completed"
        old.completed_at = now - timedelta(days=30)
        store.update(old)

        recent = store.add("Recent task")
        recent.status = "completed"
        recent.completed_at = now - timedelta(days=1)
        store.update(recent)

        # Use a date 7 days ago as start date
        start = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        result = run_gza("history", "--start-date", start, "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Recent task" in result.stdout
        assert "Old task" not in result.stdout

    def test_history_end_date(self, tmp_path: Path):
        """--end-date excludes tasks after the given date."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        now = datetime.now(UTC)

        old = store.add("Old task")
        old.status = "completed"
        old.completed_at = now - timedelta(days=30)
        store.update(old)

        recent = store.add("Recent task")
        recent.status = "completed"
        recent.completed_at = now - timedelta(days=1)
        store.update(recent)

        # Use a date 7 days ago as end date — only old task should appear
        end = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        result = run_gza("history", "--end-date", end, "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Old task" in result.stdout
        assert "Recent task" not in result.stdout

    def test_history_shows_text_status_labels(self, tmp_path: Path):
        """History command shows text labels instead of icons for status."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        completed = store.add("Completed task")
        completed.status = "completed"
        completed.completed_at = datetime.now(UTC)
        store.update(completed)

        failed = store.add("Failed task")
        failed.status = "failed"
        failed.completed_at = datetime.now(UTC)
        store.update(failed)

        dropped = store.add("Dropped task")
        dropped.status = "dropped"
        dropped.completed_at = datetime.now(UTC)
        store.update(dropped)

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "completed" in result.stdout
        assert "failed" in result.stdout
        assert "dropped" in result.stdout

    def test_history_shows_failure_reason_including_unknown(self, tmp_path: Path):
        """History shows failure_reason for all failed tasks, including UNKNOWN."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed_unknown = store.add("Failed unknown reason")
        failed_unknown.status = "failed"
        failed_unknown.failure_reason = "UNKNOWN"
        failed_unknown.completed_at = datetime.now(UTC)
        store.update(failed_unknown)

        failed_known = store.add("Failed known reason")
        failed_known.status = "failed"
        failed_known.failure_reason = "MAX_STEPS"
        failed_known.completed_at = datetime.now(UTC)
        store.update(failed_known)

        failed_none = store.add("Failed no reason")
        failed_none.status = "failed"
        failed_none.completed_at = datetime.now(UTC)
        store.update(failed_none)

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        # Failure reason is now on a separate line
        assert result.stdout.count("reason: UNKNOWN") >= 2  # explicit UNKNOWN and None both map to UNKNOWN
        assert "reason: MAX_STEPS" in result.stdout

    def test_history_annotates_failed_task_with_retry_outcome(self, tmp_path: Path):
        """Failed tasks show a retry annotation with final retry outcome."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        original = store.add("Original failed", task_type="implement")
        original.status = "failed"
        original.failure_reason = "MAX_STEPS"
        original.completed_at = datetime.now(UTC)
        store.update(original)

        assert original.id is not None
        retry = store.add("Retry succeeded", task_type="implement", based_on=original.id)
        retry.status = "completed"
        retry.completed_at = datetime.now(UTC)
        store.update(retry)
        assert retry.id is not None

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"→ retried as {retry.id} ✓" in result.stdout

    def test_history_status_failed_keeps_resolved_retry_in_factual_history(self, tmp_path: Path):
        """`history --status failed` remains historical and still includes retried rows."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        original = store.add("Original failed", task_type="implement")
        original.status = "failed"
        original.failure_reason = "MAX_STEPS"
        original.completed_at = datetime.now(UTC)
        store.update(original)

        assert original.id is not None
        retry = store.add("Retry succeeded", task_type="implement", based_on=original.id)
        retry.status = "completed"
        retry.completed_at = datetime.now(UTC)
        store.update(retry)
        assert retry.id is not None

        result = run_gza("history", "--status", "failed", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Original failed" in result.stdout
        assert f"→ retried as {retry.id} ✓" in result.stdout
        assert "Retry succeeded" not in result.stdout

    def test_history_annotates_failed_task_with_resume_outcome(self, tmp_path: Path):
        """Failed tasks show 'resumed' when the next attempt reused session + branch."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        original = store.add("Original failed", task_type="implement")
        original.status = "failed"
        original.failure_reason = "MAX_STEPS"
        original.branch = "20260415-impl-resume-test"
        original.session_id = "session-123"
        original.completed_at = datetime.now(UTC)
        store.update(original)

        assert original.id is not None
        resumed = store.add("Resumed failed again", task_type="implement", based_on=original.id)
        resumed.status = "failed"
        resumed.failure_reason = "TEST_FAILURE"
        resumed.branch = "20260415-impl-resume-test"
        resumed.session_id = "session-123"
        resumed.completed_at = datetime.now(UTC)
        store.update(resumed)
        assert resumed.id is not None

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"→ resumed as {resumed.id} ✗" in result.stdout

    def test_history_annotates_failed_task_with_queued_retry_without_outcome_marker(self, tmp_path: Path):
        """Queued retries are annotated without a terminal outcome marker."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        original = store.add("Original failed", task_type="implement")
        original.status = "failed"
        original.failure_reason = "MAX_STEPS"
        original.completed_at = datetime.now(UTC)
        store.update(original)
        assert original.id is not None

        queued_retry = store.add("Queued retry", task_type="implement", based_on=original.id)
        queued_retry.status = "pending"
        store.update(queued_retry)
        assert queued_retry.id is not None

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"→ retried as {queued_retry.id}" in result.stdout
        assert f"→ retried as {queued_retry.id} ✗" not in result.stdout
        assert f"→ retried as {queued_retry.id} ✓" not in result.stdout

    def test_history_annotates_failed_task_with_queued_resume_without_outcome_marker(self, tmp_path: Path):
        """Queued resume attempts are annotated without a terminal outcome marker."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        original = store.add("Original failed", task_type="implement")
        original.status = "failed"
        original.failure_reason = "MAX_STEPS"
        original.branch = "20260415-impl-resume-queued"
        original.session_id = "session-queued"
        original.completed_at = datetime.now(UTC)
        store.update(original)
        assert original.id is not None

        queued_resumed = store.add("Queued resumed", task_type="implement", based_on=original.id)
        queued_resumed.status = "pending"
        queued_resumed.branch = "20260415-impl-resume-queued"
        queued_resumed.session_id = "session-queued"
        store.update(queued_resumed)
        assert queued_resumed.id is not None

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"→ resumed as {queued_resumed.id}" in result.stdout
        assert f"→ resumed as {queued_resumed.id} ✗" not in result.stdout
        assert f"→ resumed as {queued_resumed.id} ✓" not in result.stdout

    def test_history_retry_annotation_follows_chain_and_ignores_other_task_types(self, tmp_path: Path):
        """Retry annotation follows same-type based_on chains to the final attempt."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        original = store.add("Original failed", task_type="implement")
        original.status = "failed"
        original.failure_reason = "MAX_STEPS"
        original.completed_at = datetime.now(UTC)
        store.update(original)
        assert original.id is not None

        # Different task type child should not be considered a retry/resume.
        review = store.add("Review child", task_type="review", based_on=original.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        assert review.id is not None

        retry_1 = store.add("Retry one", task_type="implement", based_on=original.id)
        retry_1.status = "failed"
        retry_1.failure_reason = "MAX_TURNS"
        retry_1.completed_at = datetime.now(UTC)
        store.update(retry_1)
        assert retry_1.id is not None

        retry_2 = store.add("Retry two", task_type="implement", based_on=retry_1.id)
        retry_2.status = "completed"
        retry_2.completed_at = datetime.now(UTC)
        store.update(retry_2)
        assert retry_2.id is not None

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"→ retried as {retry_2.id} ✓" in result.stdout
        assert f"→ retried as {review.id}" not in result.stdout

    def test_history_retry_annotation_resolves_latest_descendant_across_sibling_branches(self, tmp_path: Path):
        """Retry annotation resolves from the full same-type descendant tree, not one direct-child branch."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        original = store.add("Original failed", task_type="implement")
        original.status = "failed"
        original.failure_reason = "MAX_STEPS"
        original.completed_at = datetime.now(UTC)
        store.update(original)
        assert original.id is not None

        older_branch = store.add("Older retry branch", task_type="implement", based_on=original.id)
        older_branch.status = "failed"
        older_branch.failure_reason = "MAX_TURNS"
        older_branch.completed_at = datetime.now(UTC)
        store.update(older_branch)
        assert older_branch.id is not None

        newer_direct_child = store.add("Newest direct child", task_type="implement", based_on=original.id)
        newer_direct_child.status = "pending"
        store.update(newer_direct_child)
        assert newer_direct_child.id is not None

        final_attempt = store.add("Final success on older branch", task_type="implement", based_on=older_branch.id)
        final_attempt.status = "completed"
        final_attempt.completed_at = datetime.now(UTC)
        store.update(final_attempt)
        assert final_attempt.id is not None

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"→ retried as {final_attempt.id} ✓" in result.stdout
        assert f"→ retried as {newer_direct_child.id}" not in result.stdout

    def test_history_retry_annotation_keeps_resume_label_in_mixed_sibling_branches(self, tmp_path: Path):
        """Mixed retry/resume siblings should keep action label from the resolved descendant path."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        original = store.add("Original failed", task_type="implement")
        original.status = "failed"
        original.failure_reason = "MAX_STEPS"
        original.branch = "20260415-impl-branching"
        original.session_id = "session-branching"
        original.completed_at = datetime.now(UTC)
        store.update(original)
        assert original.id is not None

        resume_child = store.add("Resume sibling", task_type="implement", based_on=original.id)
        resume_child.status = "failed"
        resume_child.failure_reason = "MAX_TURNS"
        resume_child.branch = "20260415-impl-branching"
        resume_child.session_id = "session-branching"
        resume_child.completed_at = datetime.now(UTC)
        store.update(resume_child)
        assert resume_child.id is not None

        retry_child = store.add("Retry sibling", task_type="implement", based_on=original.id)
        retry_child.status = "pending"
        retry_child.branch = "20260415-impl-other-branch"
        retry_child.session_id = "session-other"
        store.update(retry_child)
        assert retry_child.id is not None

        resumed_terminal = store.add(
            "Terminal resumed attempt",
            task_type="implement",
            based_on=resume_child.id,
        )
        resumed_terminal.status = "completed"
        resumed_terminal.completed_at = datetime.now(UTC)
        resumed_terminal.branch = "20260415-impl-branching"
        resumed_terminal.session_id = "session-branching"
        store.update(resumed_terminal)
        assert resumed_terminal.id is not None

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"→ resumed as {resumed_terminal.id} ✓" in result.stdout
        assert f"→ retried as {resumed_terminal.id}" not in result.stdout

    def test_history_shows_parent_task_id(self, tmp_path: Path):
        """History shows parent task ID when based_on or depends_on is set."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        parent = store.add("Parent task")
        parent.status = "completed"
        parent.completed_at = datetime.now(UTC)
        store.update(parent)
        assert parent.id is not None

        child = store.add("Child task", based_on=parent.id)
        child.status = "completed"
        child.completed_at = datetime.now(UTC)
        store.update(child)

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"← {parent.id}" in result.stdout

    def test_history_shows_comment_count_indicator_when_present(self, tmp_path: Path):
        """History detail lines should include a comment count when task comments exist."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Task with comments", task_type="implement")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)
        store.add_comment(task.id, "Follow-up 1")
        store.add_comment(task.id, "Follow-up 2")

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "comments: 2" in result.stdout

    def test_history_shows_both_based_on_and_depends_on(self, tmp_path: Path):
        """History shows both based_on and depends_on when a task has both set."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan task")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        blocker = store.add("Blocker task")
        blocker.status = "completed"
        blocker.completed_at = datetime.now(UTC)
        store.update(blocker)

        assert plan.id is not None
        assert blocker.id is not None

        child = store.add("Child task", based_on=plan.id, depends_on=blocker.id)
        child.status = "completed"
        child.completed_at = datetime.now(UTC)
        store.update(child)

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"← {plan.id} (dep {blocker.id})" in result.stdout

    def test_history_shows_orphaned_in_progress_tasks_without_reconciling(self, tmp_path: Path):
        """History should display orphaned tasks directly instead of mutating DB state."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a task and mark it in_progress with a non-existent PID so reconciliation triggers
        orphaned = store.add("Orphaned in_progress task")
        orphaned.status = "in_progress"
        orphaned.started_at = datetime.now(UTC)
        orphaned.running_pid = 999999999  # non-existent PID
        store.update(orphaned)

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "orphaned" in result.stdout.lower()
        assert "Orphaned in_progress" in result.stdout
        reloaded = store.get(orphaned.id)
        assert reloaded is not None
        assert reloaded.status == "in_progress"

    def test_history_tag_filter_is_case_insensitive_with_json_text_parity(self, tmp_path: Path):
        """history --tag should match canonical lowercase tags regardless of filter case."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        tagged = store.add("Tagged completed history", tags=("release-1.2",))
        assert tagged.id is not None
        tagged.status = "completed"
        tagged.completed_at = datetime.now(UTC)
        store.update(tagged)

        other = store.add("Other completed history", tags=("backlog",))
        assert other.id is not None
        other.status = "completed"
        other.completed_at = datetime.now(UTC)
        store.update(other)

        text_result = run_gza("history", "--tag", "Release-1.2", "--project", str(tmp_path))
        json_result = run_gza("history", "--tag", "Release-1.2", "--json", "--project", str(tmp_path))

        assert text_result.returncode == 0
        assert "Tagged completed history" in text_result.stdout
        assert "Other completed history" not in text_result.stdout

        assert json_result.returncode == 0
        rows = json.loads(json_result.stdout)
        prompts = [row["prompt"] for row in rows]
        assert prompts == ["Tagged completed history"]

    def test_history_negative_filters_cover_status_type_and_tag(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        keep = store.add("keep completed history", task_type="implement", tags=("release",))
        keep.status = "completed"
        keep.completed_at = datetime.now(UTC)
        store.update(keep)

        failed = store.add("failed history", task_type="implement", tags=("release",))
        failed.status = "failed"
        failed.completed_at = datetime.now(UTC)
        store.update(failed)

        plan = store.add("plan history", task_type="plan", tags=("release",))
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        blocked = store.add("blocked history", task_type="implement", tags=("release", "blocked"))
        blocked.status = "completed"
        blocked.completed_at = datetime.now(UTC)
        store.update(blocked)

        result = run_gza(
            "history",
            "--status",
            "completed",
            "--status-not",
            "failed",
            "--type-not",
            "plan",
            "--tag",
            "release",
            "--tag-not",
            "blocked",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "keep completed history" in result.stdout
        assert "failed history" not in result.stdout
        assert "plan history" not in result.stdout
        assert "blocked history" not in result.stdout


class TestSearchCommand:
    """Tests for 'gza search' command."""

    def test_search_matches_pending_and_history_excludes_pending(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        pending = store.add("needle pending task")
        assert pending.id is not None

        completed = store.add("needle completed task")
        completed.status = "completed"
        completed.completed_at = datetime.now(UTC)
        store.update(completed)

        search_result = run_gza("search", "needle", "--project", str(tmp_path))
        history_result = run_gza("history", "--project", str(tmp_path))

        assert search_result.returncode == 0
        assert "needle pending task" in search_result.stdout
        assert "needle completed task" in search_result.stdout
        assert history_result.returncode == 0
        assert "needle pending task" not in history_result.stdout

    def test_search_shows_actual_status_labels_for_in_progress_and_pending(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        pending = store.add("label pending needle")
        assert pending.id is not None

        in_progress = store.add("label in progress needle")
        store.mark_in_progress(in_progress)

        result = run_gza("search", "needle", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "pending" in result.stdout
        assert "in_progress" in result.stdout

    def test_search_aligns_status_column_for_pending_and_in_progress(self, tmp_path: Path):
        """Search rows should keep task IDs in the same column across status label lengths."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        pending = store.add("align pending needle")
        assert pending.id is not None

        in_progress = store.add("align in progress needle")
        assert in_progress.id is not None
        store.mark_in_progress(in_progress)

        result = run_gza("search", "align", "--last", "0", "--project", str(tmp_path))

        assert result.returncode == 0
        pending_line = next(line for line in result.stdout.splitlines() if "align pending needle" in line)
        in_progress_line = next(line for line in result.stdout.splitlines() if "align in progress needle" in line)
        assert pending_line.index(pending.id) == in_progress_line.index(in_progress.id)

    def test_search_empty_state_message(self, tmp_path: Path):
        setup_db_with_tasks(tmp_path, [
            {"prompt": "alpha task", "status": "completed"},
        ])

        result = run_gza("search", "missing", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No tasks found matching 'missing'" in result.stdout
        assert "Showing results 0-0 out of 0" in result.stdout

    def test_search_json_empty_results_returns_empty_array_without_human_message(self, tmp_path: Path):
        setup_db_with_tasks(tmp_path, [
            {"prompt": "alpha task", "status": "completed"},
        ])

        result = run_gza("search", "missing", "--json", "--project", str(tmp_path))

        assert result.returncode == 0
        assert json.loads(result.stdout) == []
        assert "No tasks found matching 'missing'" not in result.stdout

    def test_search_last_limit(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        store.add("limit needle one")
        store.add("limit needle two")
        store.add("limit needle three")

        result = run_gza("search", "needle", "--last", "2", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "limit needle three" in result.stdout
        assert "limit needle two" in result.stdout
        assert "limit needle one" not in result.stdout
        assert "Showing results 1-2 out of 3" in result.stdout

    def test_search_last_zero_shows_all_matches(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        store.add("all needle one")
        store.add("all needle two")
        store.add("all needle three")

        result = run_gza("search", "needle", "--last", "0", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "all needle three" in result.stdout
        assert "all needle two" in result.stdout
        assert "all needle one" in result.stdout
        assert "Showing results 1-3 out of 3" in result.stdout

    def test_search_last_rejects_negative_values(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        store.add("negative needle one")

        result = run_gza("search", "needle", "--last", "-1", "--project", str(tmp_path))

        assert result.returncode != 0
        assert "--last must be >= 0 (use 0 for all matches)" in result.stderr

    def test_search_uses_history_style_row_and_detail_rendering(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("style needle task", task_type="implement")
        task.branch = "feat/search-style"
        store.update(task)

        result = run_gza("search", "style needle", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "style needle task" in result.stdout
        assert "[implement]" in result.stdout
        assert "branch: feat/search-style" in result.stdout

    def test_search_json_fields_override_limits_projection(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        first = store.add("needle alpha", task_type="implement")
        first.status = "completed"
        first.completed_at = datetime.now(UTC)
        store.update(first)

        second = store.add("needle beta", task_type="plan")
        second.status = "failed"
        second.completed_at = datetime.now(UTC)
        store.update(second)

        result = run_gza(
            "search",
            "needle",
            "--json",
            "--fields",
            "id,status",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        payload = json.loads(result.stdout)
        assert payload
        for row in payload:
            assert set(row.keys()) == {"id", "status"}

    def test_search_text_fields_multi_field_uses_generic_blocks(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("needle projection test", task_type="implement")

        result = run_gza(
            "search",
            "needle",
            "--fields",
            "id,prompt",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert f"id: {task.id}" in result.stdout
        assert "prompt: needle projection test" in result.stdout

    def test_search_text_fields_single_field_prints_bare_values(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("needle bare value", task_type="implement")

        result = run_gza("search", "needle", "--fields", "id", "--project", str(tmp_path))

        assert result.returncode == 0
        assert result.stdout.strip() == task.id

    def test_search_unknown_fields_list_valid_choices(self, tmp_path: Path):
        setup_config(tmp_path)
        make_store(tmp_path).add("needle unknown field", task_type="implement")

        result = run_gza("search", "needle", "--fields", "id,nope", "--project", str(tmp_path))

        assert result.returncode == 2
        assert "unknown field for gza search: nope" in result.stderr
        assert "valid fields:" in result.stderr
        assert "id" in result.stderr
        assert "Run uv run gza search --list-fields to list valid fields." in result.stderr

    def test_search_list_fields_prints_valid_projection_choices(self, tmp_path: Path):
        setup_config(tmp_path)

        result = run_gza("search", "needle", "--list-fields", "--project", str(tmp_path))

        assert result.returncode == 0
        assert result.stdout.strip() == _projection_list_stdout("search")
        assert "group" not in result.stdout.split()
        assert result.stderr == ""

    def test_search_rejects_group_projection_field(self, tmp_path: Path):
        setup_config(tmp_path)
        make_store(tmp_path).add("needle retired group field", task_type="implement", tags=("release",))

        result = run_gza("search", "needle", "--fields", "group", "--project", str(tmp_path))

        assert result.returncode == 2
        assert "unknown field for gza search: group" in result.stderr
        assert "Run uv run gza search --list-fields to list valid fields." in result.stderr

    def test_search_rejects_next_action_projection_field(self, tmp_path: Path):
        setup_config(tmp_path)
        make_store(tmp_path).add("needle next action unsupported", task_type="implement")

        result = run_gza("search", "needle", "--fields", "next_action", "--project", str(tmp_path))

        assert result.returncode == 2
        assert "unknown field for gza search: next_action" in result.stderr
        assert "valid fields:" in result.stderr
        assert "id" in result.stderr
        assert "next_action" not in result.stderr.split("valid fields:", 1)[1]

    def test_search_tag_filter_is_case_insensitive_with_json_text_parity(self, tmp_path: Path):
        """search --tag should match regardless of filter case in text and JSON outputs."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        store.add("Tagged search task", tags=("release-1.2",))
        store.add("Backlog search task", tags=("backlog",))

        text_result = run_gza("search", "search task", "--tag", "Release-1.2", "--project", str(tmp_path))
        json_result = run_gza(
            "search",
            "search task",
            "--tag",
            "Release-1.2",
            "--json",
            "--project",
            str(tmp_path),
        )

        assert text_result.returncode == 0
        assert "Tagged search task" in text_result.stdout
        assert "Backlog search task" not in text_result.stdout

        assert json_result.returncode == 0
        rows = json.loads(json_result.stdout)
        prompts = [row["prompt"] for row in rows]
        assert prompts == ["Tagged search task"]

    def test_search_negative_filters_cover_status_type_tag_and_lineage(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        root_keep = store.add("needle keep task", task_type="implement", tags=("release",))
        root_keep.status = "completed"
        root_keep.completed_at = datetime.now(UTC)
        store.update(root_keep)
        assert root_keep.id is not None

        failed = store.add("needle failed task", task_type="implement", tags=("release",))
        failed.status = "failed"
        failed.completed_at = datetime.now(UTC)
        store.update(failed)

        plan = store.add("needle plan task", task_type="plan", tags=("release",))
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        blocked = store.add("needle blocked task", task_type="implement", tags=("release", "blocked"))
        blocked.status = "completed"
        blocked.completed_at = datetime.now(UTC)
        store.update(blocked)

        root_excluded = store.add("needle root excluded", task_type="implement", tags=("release",))
        root_excluded.status = "completed"
        root_excluded.completed_at = datetime.now(UTC)
        store.update(root_excluded)
        assert root_excluded.id is not None

        child_excluded = store.add(
            "needle child excluded",
            task_type="review",
            tags=("release",),
            based_on=root_excluded.id,
            same_branch=True,
        )
        child_excluded.status = "completed"
        child_excluded.completed_at = datetime.now(UTC)
        store.update(child_excluded)
        assert child_excluded.id is not None

        result = run_gza(
            "search",
            "needle",
            "--status",
            "completed,failed",
            "--status-not",
            "failed",
            "--type",
            "implement,plan,review",
            "--type-not",
            "plan,review",
            "--tag",
            "release",
            "--tag-not",
            "blocked",
            "--root",
            f"{root_keep.id},{root_excluded.id}",
            "--lineage-of-not",
            child_excluded.id,
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "needle keep task" in result.stdout
        assert "needle failed task" not in result.stdout
        assert "needle plan task" not in result.stdout
        assert "needle blocked task" not in result.stdout
        assert "needle root excluded" not in result.stdout
        assert "needle child excluded" not in result.stdout

    def test_search_related_to_alias_warns_and_matches_lineage_of(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("needle root", task_type="implement")
        assert root.id is not None
        child = store.add("needle child", task_type="review", based_on=root.id, same_branch=True)
        store.add("needle other root", task_type="implement")

        deprecated_result = run_gza(
            "search",
            "needle",
            "--related-to",
            child.id,
            "--project",
            str(tmp_path),
        )
        canonical_result = run_gza(
            "search",
            "needle",
            "--lineage-of",
            child.id,
            "--project",
            str(tmp_path),
        )

        assert deprecated_result.returncode == 0
        assert canonical_result.returncode == 0
        assert deprecated_result.stdout == canonical_result.stdout
        assert "needle root" in deprecated_result.stdout
        assert "needle child" in deprecated_result.stdout
        assert "needle other root" not in deprecated_result.stdout
        assert "Warning: --related-to is deprecated; use --lineage-of instead." in deprecated_result.stderr
        assert canonical_result.stderr == ""

    def test_search_related_to_not_alias_warns_and_matches_lineage_of_not(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("needle root", task_type="implement")
        assert root.id is not None
        child = store.add("needle child", task_type="review", based_on=root.id, same_branch=True)
        store.add("needle keep", task_type="implement")

        deprecated_result = run_gza(
            "search",
            "needle",
            "--related-to-not",
            child.id,
            "--project",
            str(tmp_path),
        )
        canonical_result = run_gza(
            "search",
            "needle",
            "--lineage-of-not",
            child.id,
            "--project",
            str(tmp_path),
        )

        assert deprecated_result.returncode == 0
        assert canonical_result.returncode == 0
        assert deprecated_result.stdout == canonical_result.stdout
        assert "needle root" not in deprecated_result.stdout
        assert "needle child" not in deprecated_result.stdout
        assert "needle keep" in deprecated_result.stdout
        assert "Warning: --related-to-not is deprecated; use --lineage-of-not instead." in deprecated_result.stderr
        assert canonical_result.stderr == ""

    def test_search_lineage_of_not_and_related_to_not_union_exclusions(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        root_a = store.add("needle root a", task_type="implement")
        assert root_a.id is not None
        child_a = store.add("needle child a", task_type="review", based_on=root_a.id, same_branch=True)
        assert child_a.id is not None

        root_b = store.add("needle root b", task_type="implement")
        assert root_b.id is not None
        child_b = store.add("needle child b", task_type="review", based_on=root_b.id, same_branch=True)
        assert child_b.id is not None

        root_keep = store.add("needle keep", task_type="implement")
        assert root_keep.id is not None

        result = run_gza(
            "search",
            "needle",
            "--lineage-of-not",
            child_a.id,
            "--related-to-not",
            child_b.id,
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "needle root a" not in result.stdout
        assert "needle child a" not in result.stdout
        assert "needle root b" not in result.stdout
        assert "needle child b" not in result.stdout
        assert "needle keep" in result.stdout
        assert "Warning: --related-to-not is deprecated; use --lineage-of-not instead." in result.stderr

    def test_search_lineage_of_not_keeps_valid_exclusion_when_related_to_not_missing(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        root_excluded = store.add("needle excluded", task_type="implement")
        assert root_excluded.id is not None
        child_excluded = store.add(
            "needle excluded child",
            task_type="review",
            based_on=root_excluded.id,
            same_branch=True,
        )
        assert child_excluded.id is not None
        root_keep = store.add("needle keep", task_type="implement")
        assert root_keep.id is not None

        result = run_gza(
            "search",
            "needle",
            "--lineage-of-not",
            child_excluded.id,
            "--related-to-not",
            "gza-9999",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "needle excluded" not in result.stdout
        assert "needle excluded child" not in result.stdout
        assert "needle keep" in result.stdout
        assert "Warning: --related-to-not is deprecated; use --lineage-of-not instead." in result.stderr

    def test_search_lineage_of_missing_id_returns_no_matches(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        store.add("needle keep", task_type="implement")

        result = run_gza(
            "search",
            "needle",
            "--lineage-of",
            "gza-9999",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "needle keep" not in result.stdout

    def test_search_related_to_missing_id_warns_and_returns_no_matches(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        store.add("needle keep", task_type="implement")

        result = run_gza(
            "search",
            "needle",
            "--related-to",
            "gza-9999",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "needle keep" not in result.stdout
        assert "Warning: --related-to is deprecated; use --lineage-of instead." in result.stderr

    def test_search_root_and_lineage_of_different_roots_intersect_to_no_matches(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        root_a = store.add("needle root a", task_type="implement")
        assert root_a.id is not None
        child_a = store.add("needle child a", task_type="review", based_on=root_a.id, same_branch=True)
        assert child_a.id is not None
        root_b = store.add("needle root b", task_type="implement")
        assert root_b.id is not None

        result = run_gza(
            "search",
            "needle",
            "--root",
            root_b.id,
            "--lineage-of",
            child_a.id,
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "needle root a" not in result.stdout
        assert "needle child a" not in result.stdout
        assert "needle root b" not in result.stdout

    def test_search_root_and_related_to_different_roots_intersect_to_no_matches(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        root_a = store.add("needle root a", task_type="implement")
        assert root_a.id is not None
        child_a = store.add("needle child a", task_type="review", based_on=root_a.id, same_branch=True)
        assert child_a.id is not None
        root_b = store.add("needle root b", task_type="implement")
        assert root_b.id is not None

        result = run_gza(
            "search",
            "needle",
            "--root",
            root_b.id,
            "--related-to",
            child_a.id,
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "needle root a" not in result.stdout
        assert "needle child a" not in result.stdout
        assert "needle root b" not in result.stdout
        assert "Warning: --related-to is deprecated; use --lineage-of instead." in result.stderr

    def test_search_negative_filters_override_positive_matches_for_same_field(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("needle overlap task", task_type="implement", tags=("release",))
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = run_gza(
            "search",
            "needle",
            "--status",
            "completed",
            "--status-not",
            "completed",
            "--type",
            "implement",
            "--type-not",
            "implement",
            "--tag",
            "release",
            "--tag-not",
            "release",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "No tasks found matching 'needle'" in result.stdout
        assert "Showing results 0-0 out of 0" in result.stdout


class TestNextCommand:
    """Tests for 'gza next' command."""

    def test_next_shows_pending_tasks(self, tmp_path: Path):
        """Next command shows pending tasks."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "First pending task", "status": "pending"},
            {"prompt": "Second pending task", "status": "pending"},
            {"prompt": "Completed task", "status": "completed"},
        ])

        result = run_gza("next", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "First pending task" in result.stdout
        assert "Second pending task" in result.stdout
        assert "Completed task" not in result.stdout

    def test_next_shows_recovery_lane_before_pending_lane(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        failed = _create_failed_recovery_candidate(store, prompt="Resume me")
        pending = store.add("Pending work")

        result = run_gza("next", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Recovery lane:" in result.stdout
        assert "Pending lane:" in result.stdout
        assert f"resume {failed.id}" in " ".join(result.stdout.split())
        recovery_idx = result.stdout.index("Recovery lane:")
        pending_header_idx = result.stdout.index("Pending lane:")
        pending_task_idx = result.stdout.index("Pending work")
        assert recovery_idx < pending_header_idx < pending_task_idx

    def test_next_pending_lane_header_is_preview_only_not_lane_owner(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        store.add("Pending work")

        result = run_gza("next", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Pending lane: `gza next` preview only. `gza work` / `watch` start from this lane." in result.stdout
        assert "Pending lane: `gza next` / `watch`." not in result.stdout

    def test_next_with_no_pending_tasks(self, tmp_path: Path):
        """Next command handles no pending tasks."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Completed task", "status": "completed"},
        ])

        result = run_gza("next", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No pending tasks" in result.stdout

    def test_next_reads_frozen_snapshot_without_startup_write_error(self, tmp_path: Path):
        """Next should inspect a read-only snapshot without triggering startup writes."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        store.add("Frozen next task")

        db_path = tmp_path / ".gza" / "gza.db"
        original_mode = db_path.stat().st_mode
        os.chmod(db_path, 0o444)
        try:
            result = run_gza("next", "--project", str(tmp_path))
        finally:
            os.chmod(db_path, original_mode)

        assert result.returncode == 0
        assert "Frozen next task" in result.stdout
        assert "readonly" not in result.stderr.lower()

    def test_next_query_only_missing_task_comments_id_warns_without_traceback(self, tmp_path: Path):
        """Next should degrade comment counts cleanly when task_comments.id is missing."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Pending task with missing comment ids")
        assert task.id is not None
        store.add_comment(task.id, "Existing comment", source="direct")

        db_path = tmp_path / ".gza" / "gza.db"
        _drop_task_comments_column(db_path, "id")

        original_mode = db_path.stat().st_mode
        os.chmod(db_path, 0o444)
        try:
            result = run_gza("next", "--project", str(tmp_path))
        finally:
            os.chmod(db_path, original_mode)

        assert result.returncode == 0
        assert "Pending task with missing comment ids" in result.stdout
        assert "comments:" not in result.stdout
        assert "Traceback" not in result.stdout
        assert "Traceback" not in result.stderr
        assert "Warning: Query-only DB open detected incomplete task_comments schema" in result.stderr

    def test_next_rejects_empty_tag_without_traceback(self, tmp_path: Path):
        """next --tag '' should fail with user-facing validation, not traceback."""
        setup_config(tmp_path)

        result = run_gza("next", "--tag", "", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Error: tag must not be empty" in result.stdout
        assert "Traceback" not in result.stdout
        assert "Traceback" not in result.stderr

    def test_next_warns_about_orphaned_tasks(self, tmp_path: Path):
        """Next command warns about orphaned in-progress tasks."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a pending task
        store.add("Pending task")

        # Create an orphaned (in-progress, no active worker) task
        orphaned_task = store.add("Orphaned task that needs attention")
        mark_orphaned(store, orphaned_task)

        result = run_gza("next", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Pending task" in result.stdout
        assert "orphaned" in result.stdout
        assert "Orphaned task that needs attention" in result.stdout
        assert "gza work" in result.stdout

    def test_next_warns_orphaned_when_no_pending(self, tmp_path: Path):
        """Next command shows orphaned warning even when there are no pending tasks."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Only an orphaned task, no pending tasks
        orphaned_task = store.add("Stuck orphaned task")
        mark_orphaned(store, orphaned_task)

        result = run_gza("next", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No pending tasks" in result.stdout
        assert "orphaned" in result.stdout
        assert "Stuck orphaned task" in result.stdout

    def test_next_orphaned_hint_requires_full_task_id(self, tmp_path: Path):
        """Orphaned-task hint should tell users to pass full prefixed task IDs."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        orphaned_task = store.add("Orphaned task")
        mark_orphaned(store, orphaned_task)

        result = run_gza("next", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "gza work <full-task-id>" in result.stdout
        assert "gza mark-completed --force" in result.stdout
        assert "<full-task-id>" in result.stdout


class TestQueueCommand:
    """Tests for `gza queue` ordering and urgent-lane controls."""

    def test_queue_defaults_to_first_ten_runnable_tasks(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        for i in range(12):
            store.add(f"Task {i + 1}")

        result = run_gza("queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Task 1" in result.stdout
        assert "Task 10" in result.stdout
        assert "Task 11" not in result.stdout
        assert "Task 12" not in result.stdout
        assert "2 more runnable tasks" in result.stdout

    def test_queue_shows_recovery_lane_separately_from_pending_lane(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        failed = _create_failed_recovery_candidate(store, prompt="Retry me", task_type="plan", session_id=None, failure_reason="INFRASTRUCTURE_ERROR")
        store.add("Pending queue task")

        result = run_gza("queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Recovery lane:" in result.stdout
        assert "Pending lane:" in result.stdout
        assert f"retry {failed.id}" in " ".join(result.stdout.split())
        assert "Pending queue task" in result.stdout

    def test_queue_lists_pending_in_urgent_then_fifo_order(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        normal_1 = store.add("Normal 1")
        normal_2 = store.add("Normal 2")
        urgent = store.add("Urgent")
        assert urgent.id is not None
        store.set_urgent(urgent.id, True)

        result = run_gza("queue", "--project", str(tmp_path))

        assert result.returncode == 0
        lines = result.stdout.splitlines()
        assert any(line.strip() == "[urgent]" for line in lines)
        urgent_line = next(i for i, line in enumerate(lines) if "Urgent" in line)
        normal_1_line = next(i for i, line in enumerate(lines) if "Normal 1" in line)
        normal_2_line = next(i for i, line in enumerate(lines) if "Normal 2" in line)
        assert urgent_line < normal_1_line < normal_2_line
        assert str(normal_1.id) in lines[normal_1_line]
        assert str(normal_2.id) in lines[normal_2_line]
        assert "[urgent]" not in lines[urgent_line]

    @pytest.mark.parametrize("all_value", ["0", "-1"])
    def test_queue_limit_zero_or_minus_one_shows_all(self, tmp_path: Path, all_value: str):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        for i in range(12):
            store.add(f"Task {i + 1}")

        result = run_gza("queue", "-n", all_value, "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Task 12" in result.stdout
        assert "more runnable tasks" not in result.stdout

    def test_queue_all_shows_all(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        for i in range(12):
            store.add(f"Task {i + 1}")

        result = run_gza("queue", "--all", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Task 12" in result.stdout
        assert "more runnable tasks" not in result.stdout

    def test_queue_limit_restricts_output(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        for i in range(5):
            store.add(f"Task {i + 1}")

        result = run_gza("queue", "-n", "2", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Task 1" in result.stdout
        assert "Task 2" in result.stdout
        assert "Task 3" not in result.stdout
        assert "3 more runnable tasks" in result.stdout

    def test_queue_bump_and_unbump(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Need soon")
        assert task.id is not None
        assert task.urgent is False

        bump = run_gza("queue", "bump", task.id, "--project", str(tmp_path))
        assert bump.returncode == 0
        assert "Bumped task" in bump.stdout
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.urgent is True

        unbump = run_gza("queue", "unbump", task.id, "--project", str(tmp_path))
        assert unbump.returncode == 0
        assert "Removed task" in unbump.stdout
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.urgent is False

    def test_queue_bump_rejects_internal_pending_task(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        internal = store.add("Internal pending", task_type="internal")
        assert internal.id is not None

        result = run_gza("queue", "bump", internal.id, "--project", str(tmp_path))

        assert result.returncode == 1
        assert "is internal and not part of the runnable queue" in result.stdout

    def test_queue_bump_blocked_pending_task_clarifies_non_runnable_status(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        blocker = store.add("Blocking task")
        blocked = store.add("Blocked pending", depends_on=blocker.id)
        assert blocked.id is not None

        result = run_gza("queue", "bump", blocked.id, "--project", str(tmp_path))

        assert result.returncode == 0
        assert "not currently runnable" in result.stdout
        refreshed = store.get(blocked.id)
        assert refreshed is not None
        assert refreshed.urgent is True

    def test_queue_bump_moves_task_to_front_of_urgent_lane(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        older_urgent = store.add("Older urgent", urgent=True)
        newer_urgent = store.add("Newer urgent", urgent=True)
        bumped = store.add("Bumped now")
        assert older_urgent.id is not None
        assert newer_urgent.id is not None
        assert bumped.id is not None

        bump = run_gza("queue", "bump", bumped.id, "--project", str(tmp_path))
        assert bump.returncode == 0

        queue = run_gza("queue", "--project", str(tmp_path))
        assert queue.returncode == 0
        lines = queue.stdout.splitlines()
        bumped_line = next(i for i, line in enumerate(lines) if "Bumped now" in line)
        older_line = next(i for i, line in enumerate(lines) if "Older urgent" in line)
        newer_line = next(i for i, line in enumerate(lines) if "Newer urgent" in line)
        assert bumped_line < older_line < newer_line

    @pytest.mark.parametrize(
        ("action", "extra_args"),
        [
            ("bump", []),
            ("unbump", []),
            ("move", ["1"]),
            ("next", []),
            ("clear", []),
        ],
    )
    def test_queue_management_subcommands_accept_tag_after_subcommand(
        self,
        tmp_path: Path,
        action: str,
        extra_args: list[str],
    ):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Release task", tags=("release-1.2",))
        assert task.id is not None

        result = run_gza(
            "queue",
            action,
            task.id,
            *extra_args,
            "--tag",
            "release-1.2",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "unrecognized arguments" not in result.stderr

    def test_queue_move_assigns_explicit_positions(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        first = store.add("First")
        second = store.add("Second")
        third = store.add("Third")
        assert first.id is not None
        assert second.id is not None
        assert third.id is not None

        move = run_gza("queue", "move", third.id, "1", "--project", str(tmp_path))
        assert move.returncode == 0
        assert "queue position 1" in move.stdout

        queue = run_gza("queue", "--project", str(tmp_path))
        assert queue.returncode == 0
        lines = queue.stdout.splitlines()
        third_line = next(i for i, line in enumerate(lines) if "Third" in line)
        first_line = next(i for i, line in enumerate(lines) if "First" in line)
        second_line = next(i for i, line in enumerate(lines) if "Second" in line)
        assert third_line < first_line < second_line
        assert lines[third_line + 1].strip() == "[#1]"

    def test_queue_next_and_clear_manage_explicit_order(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        first = store.add("First", group="release")
        second = store.add("Second", group="release")
        assert first.id is not None
        assert second.id is not None

        move_next = run_gza("queue", "next", second.id, "--project", str(tmp_path))
        assert move_next.returncode == 0
        assert "queue position 1" in move_next.stdout

        queue = run_gza("queue", "--tag", "release", "--project", str(tmp_path))
        assert queue.returncode == 0
        lines = queue.stdout.splitlines()
        second_line = next(i for i, line in enumerate(lines) if "Second" in line)
        assert "Second" in lines[second_line]
        assert lines[second_line + 1].strip() == "[#1]"

        clear = run_gza("queue", "clear", second.id, "--project", str(tmp_path))
        assert clear.returncode == 0
        assert "Cleared explicit queue order" in clear.stdout

        refreshed = store.get(second.id)
        assert refreshed is not None
        assert refreshed.queue_position is None

    @pytest.mark.parametrize(
        ("action", "extra_args"),
        [
            ("move", ["1"]),
            ("next", []),
        ],
    )
    def test_queue_tag_scoped_move_does_not_mutate_disjoint_multi_tag_bucket(
        self,
        tmp_path: Path,
        action: str,
        extra_args: list[str],
    ):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        release_plain = store.add("Release plain", tags=("release",))
        release_backend = store.add("Release backend", tags=("release", "backend"))
        release_docs = store.add("Release docs", tags=("release", "docs"))
        ops_first = store.add("Ops first", tags=("ops", "infra"))
        ops_second = store.add("Ops second", tags=("ops", "infra"))
        assert release_plain.id is not None
        assert release_backend.id is not None
        assert release_docs.id is not None
        assert ops_first.id is not None
        assert ops_second.id is not None

        init_release_plain = run_gza(
            "queue",
            "move",
            release_plain.id,
            "1",
            "--tag",
            "release",
            "--project",
            str(tmp_path),
        )
        init_release_backend = run_gza(
            "queue",
            "move",
            release_backend.id,
            "2",
            "--tag",
            "release",
            "--project",
            str(tmp_path),
        )
        init_release_docs = run_gza(
            "queue",
            "move",
            release_docs.id,
            "3",
            "--tag",
            "release",
            "--project",
            str(tmp_path),
        )
        init_ops_first = run_gza("queue", "move", ops_first.id, "1", "--project", str(tmp_path))
        init_ops_second = run_gza("queue", "move", ops_second.id, "2", "--project", str(tmp_path))
        assert init_release_plain.returncode == 0
        assert init_release_backend.returncode == 0
        assert init_release_docs.returncode == 0
        assert init_ops_first.returncode == 0
        assert init_ops_second.returncode == 0

        result = run_gza(
            "queue",
            action,
            release_backend.id,
            *extra_args,
            "--tag",
            "release",
            "--project",
            str(tmp_path),
        )
        assert result.returncode == 0

        refreshed_release_plain = store.get(release_plain.id)
        refreshed_release_backend = store.get(release_backend.id)
        refreshed_release_docs = store.get(release_docs.id)
        refreshed_ops_first = store.get(ops_first.id)
        refreshed_ops_second = store.get(ops_second.id)
        assert refreshed_release_plain is not None
        assert refreshed_release_backend is not None
        assert refreshed_release_docs is not None
        assert refreshed_ops_first is not None
        assert refreshed_ops_second is not None

        assert refreshed_release_backend.queue_position == 1
        assert refreshed_release_plain.queue_position == 2
        assert refreshed_release_docs.queue_position == 3
        assert refreshed_ops_first.queue_position == 1
        assert refreshed_ops_second.queue_position == 2

    @pytest.mark.parametrize(
        ("action", "extra_args"),
        [
            ("move", ["1"]),
            ("next", []),
            ("clear", []),
        ],
    )
    def test_queue_tag_scoped_ordering_fails_closed_when_target_misses_scope(
        self,
        tmp_path: Path,
        action: str,
        extra_args: list[str],
    ):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        release_first = store.add("Release first", tags=("release",))
        release_second = store.add("Release second", tags=("release",))
        ops_first = store.add("Ops first", tags=("ops",))
        ops_second = store.add("Ops second", tags=("ops",))
        assert release_first.id is not None
        assert release_second.id is not None
        assert ops_first.id is not None
        assert ops_second.id is not None

        assert run_gza("queue", "move", release_first.id, "1", "--project", str(tmp_path)).returncode == 0
        assert run_gza("queue", "move", release_second.id, "2", "--project", str(tmp_path)).returncode == 0
        assert run_gza("queue", "move", ops_first.id, "1", "--project", str(tmp_path)).returncode == 0
        assert run_gza("queue", "move", ops_second.id, "2", "--project", str(tmp_path)).returncode == 0

        result = run_gza(
            "queue",
            action,
            ops_second.id,
            *extra_args,
            "--tag",
            "release",
            "--project",
            str(tmp_path),
        )
        assert result.returncode == 1
        assert "does not match tag scope" in result.stdout
        assert "queue ordering was not changed" in result.stdout

        refreshed_release_first = store.get(release_first.id)
        refreshed_release_second = store.get(release_second.id)
        refreshed_ops_first = store.get(ops_first.id)
        refreshed_ops_second = store.get(ops_second.id)
        assert refreshed_release_first is not None
        assert refreshed_release_second is not None
        assert refreshed_ops_first is not None
        assert refreshed_ops_second is not None

        assert refreshed_release_first.queue_position == 1
        assert refreshed_release_second.queue_position == 2
        assert refreshed_ops_first.queue_position == 1
        assert refreshed_ops_second.queue_position == 2

    def test_queue_tag_scoped_ordering_fails_closed_for_repeated_tags_with_any_tag_mismatch(
        self,
        tmp_path: Path,
    ):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        release_first = store.add("Release first", tags=("release",))
        release_second = store.add("Release second", tags=("release",))
        ops_first = store.add("Ops first", tags=("ops",))
        ops_second = store.add("Ops second", tags=("ops",))
        assert release_first.id is not None
        assert release_second.id is not None
        assert ops_first.id is not None
        assert ops_second.id is not None

        assert run_gza("queue", "move", release_first.id, "1", "--project", str(tmp_path)).returncode == 0
        assert run_gza("queue", "move", release_second.id, "2", "--project", str(tmp_path)).returncode == 0
        assert run_gza("queue", "move", ops_first.id, "1", "--project", str(tmp_path)).returncode == 0
        assert run_gza("queue", "move", ops_second.id, "2", "--project", str(tmp_path)).returncode == 0

        result = run_gza(
            "queue",
            "move",
            ops_second.id,
            "1",
            "--tag",
            "release",
            "--tag",
            "release",
            "--any-tag",
            "--project",
            str(tmp_path),
        )
        assert result.returncode == 1
        assert "does not match tag scope" in result.stdout
        assert "queue ordering was not changed" in result.stdout

        refreshed_release_first = store.get(release_first.id)
        refreshed_release_second = store.get(release_second.id)
        refreshed_ops_first = store.get(ops_first.id)
        refreshed_ops_second = store.get(ops_second.id)
        assert refreshed_release_first is not None
        assert refreshed_release_second is not None
        assert refreshed_ops_first is not None
        assert refreshed_ops_second is not None

        assert refreshed_release_first.queue_position == 1
        assert refreshed_release_second.queue_position == 2
        assert refreshed_ops_first.queue_position == 1
        assert refreshed_ops_second.queue_position == 2

    def test_queue_tag_scope_reports_out_of_scope_runnable_child_when_scoped_pending_lane_is_empty(
        self,
        tmp_path: Path,
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add(
            "Scoped recovery owner",
            task_type="plan",
            tags=("202606-recovery", "v0.5.0"),
            auto_implement=False,
        )
        assert plan.id is not None
        plan.status = "completed"
        plan.completed_at = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
        store.update(plan)

        child = store.add(
            "Out of scope implement child",
            task_type="implement",
            based_on=plan.id,
            tags=("v0.5.0",),
        )
        assert child.id is not None

        result = run_gza(
            "queue",
            "--tag",
            "202606-recovery",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "No pending tasks matching tags: 202606-recovery" in result.stdout
        assert "Scope gap:" in result.stdout
        assert plan.id in result.stdout
        assert child.id in result.stdout
        assert "out-of-scope child" in result.stdout

    def test_queue_tag_scope_reports_depends_on_only_out_of_scope_runnable_child(
        self,
        tmp_path: Path,
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        explore = store.add(
            "Scoped recovery owner",
            task_type="explore",
            tags=("202606-recovery", "v0.5.0"),
        )
        assert explore.id is not None
        explore.status = "completed"
        explore.completed_at = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
        store.update(explore)

        child = store.add(
            "Out of scope implement child",
            task_type="implement",
            depends_on=explore.id,
            tags=("v0.5.0",),
        )
        assert child.id is not None

        result = run_gza(
            "queue",
            "--tag",
            "202606-recovery",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "No pending tasks matching tags: 202606-recovery" in result.stdout
        assert "Scope gap:" in result.stdout
        assert explore.id in result.stdout
        assert child.id in result.stdout
        assert "out-of-scope child" in result.stdout

    def test_queue_tag_scope_reports_gap_alongside_visible_scoped_pending_rows(
        self,
        tmp_path: Path,
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        visible_pending = store.add(
            "Visible scoped pending task",
            tags=("202606-recovery",),
        )
        assert visible_pending.id is not None

        plan = store.add(
            "Scoped recovery owner",
            task_type="plan",
            tags=("202606-recovery", "v0.5.0"),
            auto_implement=False,
        )
        assert plan.id is not None
        plan.status = "completed"
        plan.completed_at = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
        store.update(plan)

        child = store.add(
            "Out of scope implement child",
            task_type="implement",
            based_on=plan.id,
            tags=("v0.5.0",),
        )
        assert child.id is not None

        result = run_gza(
            "queue",
            "--tag",
            "202606-recovery",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert visible_pending.id in result.stdout
        assert "Visible scoped pending task" in result.stdout
        assert "Scope gap:" in result.stdout
        assert plan.id in result.stdout
        assert child.id in result.stdout
        assert "out-of-scope child" in result.stdout

    def test_queue_tag_scope_reports_owner_missing_from_lineage_projection(
        self,
        tmp_path: Path,
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        unrelated = store.add(
            "Visible scoped pending task",
            task_type="implement",
            tags=("202606-recovery",),
        )
        assert unrelated.id is not None

        plan = store.add(
            "Scoped owner hidden from owner rows",
            task_type="plan",
            tags=("202606-recovery", "v0.5.0"),
            auto_implement=False,
        )
        assert plan.id is not None
        plan.status = "completed"
        plan.completed_at = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
        store.update(plan)

        child = store.add(
            "Out of scope implement child",
            task_type="implement",
            based_on=plan.id,
            tags=("v0.5.0",),
        )
        assert child.id is not None

        result = run_gza(
            "queue",
            "--tag",
            "202606-recovery",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert unrelated.id in result.stdout
        assert "Scope gap:" in result.stdout
        assert plan.id in result.stdout
        assert child.id in result.stdout
        assert "out-of-scope child" in result.stdout

    def test_queue_tag_scope_reports_blocked_out_of_scope_pending_child(
        self,
        tmp_path: Path,
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        blocked_by = store.add("Blocking dependency")
        assert blocked_by.id is not None

        plan = store.add(
            "Scoped recovery owner",
            task_type="plan",
            tags=("202606-recovery", "v0.5.0"),
            auto_implement=False,
        )
        assert plan.id is not None
        plan.status = "completed"
        plan.completed_at = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
        store.update(plan)

        child = store.add(
            "Blocked out of scope implement child",
            task_type="implement",
            based_on=plan.id,
            depends_on=blocked_by.id,
            tags=("v0.5.0",),
        )
        assert child.id is not None

        result = run_gza(
            "queue",
            "--tag",
            "202606-recovery",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "No pending tasks matching tags: 202606-recovery" in result.stdout
        assert "Scope gap:" in result.stdout
        assert plan.id in result.stdout
        assert child.id in result.stdout
        assert "out-of-scope child" in result.stdout
        assert "blocked implement" in result.stdout

    def test_queue_tag_scope_reports_independent_out_of_scope_child_even_with_runnable_in_scope_sibling(
        self,
        tmp_path: Path,
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add(
            "Scoped owner",
            task_type="plan",
            tags=("202606-recovery", "v0.5.0"),
            auto_implement=False,
        )
        assert plan.id is not None
        plan.status = "completed"
        plan.completed_at = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
        store.update(plan)

        in_scope_child = store.add(
            "Runnable in-scope child",
            task_type="implement",
            based_on=plan.id,
            tags=("202606-recovery", "v0.5.0"),
        )
        assert in_scope_child.id is not None

        out_of_scope_child = store.add(
            "Independent out-of-scope child",
            task_type="review",
            based_on=plan.id,
            tags=("v0.5.0",),
        )
        assert out_of_scope_child.id is not None

        result = run_gza(
            "queue",
            "--tag",
            "202606-recovery",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert in_scope_child.id in result.stdout
        assert "Scope gap:" in result.stdout
        assert plan.id in result.stdout
        assert out_of_scope_child.id in result.stdout
        assert "out-of-scope child" in result.stdout

    def test_queue_tag_scope_reports_failed_owner_with_out_of_scope_retry_child(
        self,
        tmp_path: Path,
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed = store.add(
            "Scoped failed implementation",
            task_type="implement",
            tags=("202606-recovery", "v0.5.0"),
        )
        assert failed.id is not None
        store.mark_failed(failed, "verify failed")

        retry_child = store.add(
            "Pre-existing out of scope retry child",
            task_type="implement",
            based_on=failed.id,
            recovery_origin="retry",
            tags=("v0.5.0",),
        )
        assert retry_child.id is not None

        result = run_gza(
            "queue",
            "--tag",
            "202606-recovery",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "Recovery lane:" in result.stdout
        assert "Scope gap:" in result.stdout
        assert failed.id in result.stdout
        assert retry_child.id in result.stdout
        assert "out-of-scope child" in result.stdout
        assert "runnable implement" in result.stdout

    def test_queue_tag_scope_all_mode_hint_lists_all_missing_tags(
        self,
        tmp_path: Path,
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add(
            "Scoped recovery owner",
            task_type="plan",
            tags=("202606-recovery", "v0.5.0"),
            auto_implement=False,
        )
        assert plan.id is not None
        plan.status = "completed"
        plan.completed_at = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
        store.update(plan)

        child = store.add(
            "Out of scope implement child",
            task_type="implement",
            based_on=plan.id,
            tags=("ops",),
        )
        assert child.id is not None

        result = run_gza(
            "queue",
            "--tag",
            "202606-recovery",
            "--tag",
            "v0.5.0",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "Scope gap:" in result.stdout
        assert child.id in result.stdout
        assert "missing_scope=202606-recovery,v0.5.0" in result.stdout
        assert (
            f"hint: `uv run gza edit {child.id} --add-tag 202606-recovery --add-tag v0.5.0`"
            in result.stdout
        )

    def test_queue_tag_scope_does_not_warn_when_child_inherits_scope_tag(
        self,
        tmp_path: Path,
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add(
            "Scoped recovery owner",
            task_type="plan",
            tags=("202606-recovery", "v0.5.0"),
            auto_implement=False,
        )
        assert plan.id is not None
        plan.status = "completed"
        plan.completed_at = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
        store.update(plan)

        child = store.add(
            "Inherited scope implement child",
            task_type="implement",
            based_on=plan.id,
            tags=("202606-recovery", "v0.5.0"),
        )
        assert child.id is not None

        result = run_gza(
            "queue",
            "--tag",
            "202606-recovery",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "Scope gap:" not in result.stdout
        assert child.id in result.stdout

    def test_queue_tag_scope_does_not_warn_when_depends_on_only_child_inherits_scope_tag(
        self,
        tmp_path: Path,
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        explore = store.add(
            "Scoped recovery owner",
            task_type="explore",
            tags=("202606-recovery", "v0.5.0"),
        )
        assert explore.id is not None
        explore.status = "completed"
        explore.completed_at = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
        store.update(explore)

        child = store.add(
            "Inherited scope implement child",
            task_type="implement",
            depends_on=explore.id,
            tags=("202606-recovery", "v0.5.0"),
        )
        assert child.id is not None

        result = run_gza(
            "queue",
            "--tag",
            "202606-recovery",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "Scope gap:" not in result.stdout
        assert child.id in result.stdout

    def test_queue_tag_scope_does_not_warn_when_runnable_scoped_owner_has_out_of_scope_dependent_child(
        self,
        tmp_path: Path,
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        owner = store.add(
            "Scoped runnable owner",
            task_type="implement",
            tags=("202606-recovery",),
        )
        assert owner.id is not None

        child = store.add(
            "Out of scope dependent child",
            task_type="review",
            depends_on=owner.id,
            tags=("v0.5.0",),
        )
        assert child.id is not None

        result = run_gza(
            "queue",
            "--tag",
            "202606-recovery",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "Scope gap:" not in result.stdout
        assert owner.id in result.stdout
        assert "Scoped runnable owner" in result.stdout
        assert child.id not in result.stdout

    def test_queue_tag_scope_uses_any_tag_matching_for_scope_gap_detection(
        self,
        tmp_path: Path,
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add(
            "Scoped recovery owner",
            task_type="plan",
            tags=("202606-recovery", "ops"),
            auto_implement=False,
        )
        assert plan.id is not None
        plan.status = "completed"
        plan.completed_at = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
        store.update(plan)

        matching_child = store.add(
            "Any-tag matching child",
            task_type="implement",
            based_on=plan.id,
            tags=("ops",),
        )
        assert matching_child.id is not None

        matching_result = run_gza(
            "queue",
            "--tag",
            "202606-recovery",
            "--tag",
            "ops",
            "--any-tag",
            "--project",
            str(tmp_path),
        )
        assert matching_result.returncode == 0
        assert "Scope gap:" not in matching_result.stdout
        assert matching_child.id in matching_result.stdout

        store.delete(matching_child.id)
        out_of_scope_child = store.add(
            "Any-tag out of scope child",
            task_type="implement",
            based_on=plan.id,
            tags=("v0.5.0",),
        )
        assert out_of_scope_child.id is not None

        gap_result = run_gza(
            "queue",
            "--tag",
            "202606-recovery",
            "--tag",
            "ops",
            "--any-tag",
            "--project",
            str(tmp_path),
        )
        assert gap_result.returncode == 0
        assert "Scope gap:" in gap_result.stdout
        assert out_of_scope_child.id in gap_result.stdout
        assert (
            f"hint: `uv run gza edit {out_of_scope_child.id} --add-tag 202606-recovery`"
            in gap_result.stdout
        )
        assert f"--add-tag ops`" not in gap_result.stdout

    def test_next_shows_bumped_task_first(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        store.add("Older urgent", urgent=True)
        store.add("Newer urgent", urgent=True)
        bumped = store.add("Bumped now")
        assert bumped.id is not None
        run_gza("queue", "bump", bumped.id, "--project", str(tmp_path))

        result = run_gza("next", "--project", str(tmp_path))
        assert result.returncode == 0
        lines = [line for line in result.stdout.splitlines() if line.strip()]
        bumped_line = next(i for i, line in enumerate(lines) if "Bumped now" in line)
        older_line = next(i for i, line in enumerate(lines) if "Older urgent" in line)
        newer_line = next(i for i, line in enumerate(lines) if "Newer urgent" in line)
        assert bumped_line < older_line < newer_line

    def test_queue_tag_view_shares_one_order_across_tasks_with_extra_tags(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        release_plain = store.add("Release plain", tags=("release",))
        release_backend = store.add("Release backend", tags=("release", "backend"))
        release_docs = store.add("Release docs", tags=("release", "docs"))
        assert release_plain.id is not None
        assert release_backend.id is not None
        assert release_docs.id is not None

        assert run_gza("queue", "move", release_plain.id, "1", "--tag", "release", "--project", str(tmp_path)).returncode == 0
        assert run_gza("queue", "move", release_backend.id, "2", "--tag", "release", "--project", str(tmp_path)).returncode == 0
        assert run_gza("queue", "move", release_docs.id, "3", "--tag", "release", "--project", str(tmp_path)).returncode == 0

        queue = run_gza("queue", "--tag", "release", "--project", str(tmp_path))
        assert queue.returncode == 0

        lines = queue.stdout.splitlines()
        first_idx = next(i for i, line in enumerate(lines) if "Release plain" in line)
        second_idx = next(i for i, line in enumerate(lines) if "Release backend" in line)
        third_idx = next(i for i, line in enumerate(lines) if "Release docs" in line)
        assert first_idx < second_idx < third_idx
        assert lines[first_idx + 1].strip() == "[#1]"
        assert lines[second_idx + 1].strip() == "[#2]"
        assert lines[third_idx + 1].strip() == "[#3]"

    def test_queue_tag_scoped_clear_shares_order_across_tasks_with_extra_tags(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        release_plain = store.add("Release plain", tags=("release",))
        release_backend = store.add("Release backend", tags=("release", "backend"))
        release_docs = store.add("Release docs", tags=("release", "docs"))
        ops_first = store.add("Ops first", tags=("ops", "infra"))
        ops_second = store.add("Ops second", tags=("ops", "infra"))
        assert release_plain.id is not None
        assert release_backend.id is not None
        assert release_docs.id is not None
        assert ops_first.id is not None
        assert ops_second.id is not None

        assert run_gza("queue", "move", release_plain.id, "1", "--tag", "release", "--project", str(tmp_path)).returncode == 0
        assert run_gza("queue", "move", release_backend.id, "2", "--tag", "release", "--project", str(tmp_path)).returncode == 0
        assert run_gza("queue", "move", release_docs.id, "3", "--tag", "release", "--project", str(tmp_path)).returncode == 0
        assert run_gza("queue", "move", ops_first.id, "1", "--project", str(tmp_path)).returncode == 0
        assert run_gza("queue", "move", ops_second.id, "2", "--project", str(tmp_path)).returncode == 0

        clear = run_gza(
            "queue",
            "clear",
            release_backend.id,
            "--tag",
            "release",
            "--project",
            str(tmp_path),
        )
        assert clear.returncode == 0
        assert "Cleared explicit queue order" in clear.stdout

        refreshed_release_plain = store.get(release_plain.id)
        refreshed_release_backend = store.get(release_backend.id)
        refreshed_release_docs = store.get(release_docs.id)
        refreshed_ops_first = store.get(ops_first.id)
        refreshed_ops_second = store.get(ops_second.id)
        assert refreshed_release_plain is not None
        assert refreshed_release_backend is not None
        assert refreshed_release_docs is not None
        assert refreshed_ops_first is not None
        assert refreshed_ops_second is not None

        assert refreshed_release_plain.queue_position == 1
        assert refreshed_release_backend.queue_position is None
        assert refreshed_release_docs.queue_position == 2
        assert refreshed_ops_first.queue_position == 1
        assert refreshed_ops_second.queue_position == 2

    def test_next_shows_explicitly_ordered_task_first(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        store.add("Older urgent", urgent=True)
        ordered = store.add("Ordered")
        assert ordered.id is not None
        run_gza("queue", "move", ordered.id, "1", "--project", str(tmp_path))

        result = run_gza("next", "--project", str(tmp_path))
        assert result.returncode == 0
        lines = [line for line in result.stdout.splitlines() if line.strip()]
        ordered_line = next(i for i, line in enumerate(lines) if "Ordered" in line)
        urgent_line = next(i for i, line in enumerate(lines) if "Older urgent" in line)
        assert ordered_line < urgent_line

    def test_queue_lists_blocked_pending_tasks_after_runnable_rows(self, tmp_path: Path):
        """Queue should show blocked pending tasks at the bottom with direct blocker metadata."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        runnable = store.add("Runnable")
        store.add("Internal pending", task_type="internal")
        blocker = store.add("Dependency blocker")
        blocked = store.add("Blocked pending", depends_on=blocker.id)
        assert runnable.id is not None
        assert blocker.id is not None
        assert blocked.id is not None

        result = run_gza("queue", "--all", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Runnable" in result.stdout
        assert "Internal pending" not in result.stdout
        assert "Blocked pending" in result.stdout
        lines = result.stdout.splitlines()
        runnable_line = next(i for i, line in enumerate(lines) if "Runnable" in line)
        blocker_line = next(i for i, line in enumerate(lines) if "Dependency blocker" in line)
        blocked_line = next(i for i, line in enumerate(lines) if "Blocked pending" in line)
        assert runnable_line < blocker_line < blocked_line
        assert lines[blocked_line].split()[0] == "-"
        assert lines[blocked_line + 1].strip() == f"blocked by {blocker.id}"

    def test_queue_tag_filters_runnable_tasks_to_tag(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        release_task = store.add("Release task", group="release-1")
        other_task = store.add("Other task", group="backlog")
        blocked_parent = store.add("Release blocker", group="release-1")
        store.add("Blocked release task", group="release-1", depends_on=blocked_parent.id)
        assert release_task.id is not None
        assert other_task.id is not None

        result = run_gza("queue", "--tag", "release-1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Release task" in result.stdout
        assert "Release blocker" in result.stdout
        assert "Blocked release task" in result.stdout
        assert "Other task" not in result.stdout

    def test_queue_shows_blocked_pending_tasks_when_no_runnable_tasks_exist(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        blocker = store.add("Internal blocker", task_type="internal")
        blocked = store.add("Blocked pending", depends_on=blocker.id)
        assert blocker.id is not None
        assert blocked.id is not None

        result = run_gza("queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No pending tasks" not in result.stdout
        assert "Blocked pending" in result.stdout
        lines = result.stdout.splitlines()
        blocked_line = next(i for i, line in enumerate(lines) if "Blocked pending" in line)
        assert lines[blocked_line].split()[0] == "-"
        assert lines[blocked_line + 1].strip() == f"blocked by {blocker.id}"

    def test_queue_empty_prerequisite_surfaces_release_valve_by_default(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        dep = store.add("Empty dependency")
        assert dep.id is not None
        dep.status = "completed"
        dep.completed_at = datetime.now(UTC)
        dep.branch = "feature/queue-empty-prereq"
        dep.has_commits = True
        store.update(dep)
        unit = store.create_merge_unit(
            source_branch=dep.branch,
            target_branch="main",
            owner_task_id=dep.id,
            state="empty",
        )
        store.attach_task_to_merge_unit(dep.id, unit.id, "owner")

        downstream = store.add("Held downstream", depends_on=dep.id)
        assert downstream.id is not None

        result = run_gza("queue", "--all", "--project", str(tmp_path))

        assert result.returncode == 0
        normalized = " ".join(result.stdout.split())
        assert "Held downstream" in normalized
        assert "empty prerequisite" in normalized
        assert "gza-4072" in normalized

    def test_queue_failed_empty_prerequisite_surfaces_release_valve_by_default(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        dep = store.add("Failed empty dependency", task_type="implement")
        assert dep.id is not None
        dep.status = "failed"
        dep.completed_at = datetime.now(UTC)
        dep.failure_reason = "PREREQUISITE_UNMERGED"
        dep.branch = "feature/queue-failed-empty-prereq"
        dep.has_commits = False
        store.update(dep)
        unit = store.create_merge_unit(
            source_branch=dep.branch,
            target_branch="main",
            owner_task_id=dep.id,
            state="empty",
        )
        store.attach_task_to_merge_unit(dep.id, unit.id, "owner")

        downstream = store.add("Held downstream", task_type="implement", depends_on=dep.id)
        assert downstream.id is not None

        result = run_gza("queue", "--all", "--project", str(tmp_path))

        assert result.returncode == 0
        normalized = " ".join(result.stdout.split())
        assert "Held downstream" in normalized
        assert "empty prerequisite" in normalized
        assert "gza-4072" in normalized

    def test_queue_layout_keeps_first_line_columns_stable_and_second_line_aligned(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plain = store.add("Plain task")
        urgent = store.add("Urgent task", urgent=True)
        blocker = store.add("Blocking task")
        blocked = store.add("Blocked task", depends_on=blocker.id)
        assert plain.id is not None
        assert urgent.id is not None
        assert blocker.id is not None
        assert blocked.id is not None

        result = run_gza("queue", "--all", "--project", str(tmp_path))

        assert result.returncode == 0
        lines = result.stdout.splitlines()
        plain_line = next(line for line in lines if "Plain task" in line)
        urgent_line_index = next(i for i, line in enumerate(lines) if "Urgent task" in line)
        blocked_line_index = next(i for i, line in enumerate(lines) if "Blocked task" in line)
        urgent_line = lines[urgent_line_index]
        blocked_line = lines[blocked_line_index]

        plain_id_col = plain_line.index(str(plain.id))
        plain_type_col = plain_line.index("[implement]")
        plain_prompt_col = plain_line.index("Plain task")
        assert urgent_line.index(str(urgent.id)) == plain_id_col
        assert urgent_line.index("[implement]") == plain_type_col
        assert urgent_line.index("Urgent task") == plain_prompt_col
        assert blocked_line.index(str(blocked.id)) == plain_id_col
        assert blocked_line.index("[implement]") == plain_type_col
        assert blocked_line.index("Blocked task") == plain_prompt_col
        assert blocked_line.split()[0] == "-"
        assert lines[urgent_line_index + 1].startswith(" " * plain_prompt_col)
        assert lines[blocked_line_index + 1].startswith(" " * plain_prompt_col)

    def test_queue_vanilla_runnable_row_stays_single_line(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Vanilla task")
        assert task.id is not None

        result = run_gza("queue", "--project", str(tmp_path))

        assert result.returncode == 0
        lines = result.stdout.splitlines()
        task_line = next(i for i, line in enumerate(lines) if "Vanilla task" in line)
        assert task_line == len(lines) - 1

    def test_queue_shows_urgent_and_blocked_metadata_on_same_second_line(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        blocker = store.add("Blocking task")
        blocked_urgent = store.add("Blocked urgent", depends_on=blocker.id, urgent=True)
        assert blocker.id is not None
        assert blocked_urgent.id is not None

        result = run_gza("queue", "--project", str(tmp_path))

        assert result.returncode == 0
        lines = result.stdout.splitlines()
        blocked_line = next(i for i, line in enumerate(lines) if "Blocked urgent" in line)
        assert lines[blocked_line + 1].strip() == f"[urgent]  blocked by {blocker.id}"

    def test_queue_command_uses_shared_queue_theme_renderables(self, tmp_path: Path) -> None:
        from gza.colors import QueueColors

        setup_config(tmp_path)
        store = make_store(tmp_path)
        blocker = store.add("Queue blocker")
        urgent = store.add("Urgent ordered queue task", urgent=True)
        blocked = store.add("Blocked queue task", depends_on=blocker.id)
        assert urgent.id is not None
        assert blocked.id is not None
        store.set_queue_position(urgent.id, 1)

        recording_console = _RecordingConsole()
        config = Config.load(tmp_path)
        queue_colors = QueueColors(
            position="red",
            blocked_marker="green",
            task_id="cyan",
            task_type="magenta",
            prompt="white",
            urgent="yellow",
            blocked_by="blue",
            explicit_position="bright_black",
            summary="bright_white",
        )
        with (
            patch.object(queue_render_cli._colors, "QUEUE_COLORS", queue_colors),  # noqa: SLF001
            patch.object(watch_cli, "console", recording_console),
            patch.object(watch_cli.Config, "load", return_value=config),
        ):
            exit_code = watch_cli.cmd_queue(
                argparse.Namespace(
                    project_dir=tmp_path,
                    queue_action=None,
                    limit=10,
                    all=True,
                    tags=None,
                    any_tag=False,
                )
            )

        text_outputs = [output for output in recording_console.outputs if isinstance(output, Text)]
        assert exit_code == 0
        urgent_line = next(text for text in text_outputs if "Urgent ordered queue task" in text.plain)
        urgent_meta = next(text for text in text_outputs if "[urgent]" in text.plain)
        blocked_line = next(text for text in text_outputs if "Blocked queue task" in text.plain)
        blocked_meta = next(text for text in text_outputs if f"blocked by {blocker.id}" in text.plain)

        assert _span_styles(urgent_line) == ["red", "cyan", "magenta", "white"]
        assert _styled_substrings(urgent_meta)["[urgent]"] == "yellow"
        assert _styled_substrings(urgent_meta)["[#1]"] == "bright_black"
        assert _span_styles(blocked_line) == ["green", "cyan", "magenta", "white"]
        assert _span_styles(blocked_meta) == ["blue"]

    def test_next_tag_filters_pending_and_blocked_counts(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        store.add("Release runnable", group="release-1")
        blocker = store.add("Release blocker", group="release-1")
        store.add("Release blocked", group="release-1", depends_on=blocker.id)
        store.add("Backlog runnable", group="backlog")

        result = run_gza("next", "--tag", "release-1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Release runnable" in result.stdout
        assert "Release blocked" not in result.stdout
        assert "Backlog runnable" not in result.stdout
        assert "1 task blocked by dependencies" in result.stdout

    def test_next_command_uses_shared_queue_theme_renderables(self, tmp_path: Path) -> None:
        from gza.colors import QueueColors

        setup_config(tmp_path)
        store = make_store(tmp_path)
        blocker = store.add("Next blocker")
        urgent = store.add("Urgent next task", urgent=True)
        blocked = store.add("Blocked next task", depends_on=blocker.id)
        assert urgent.id is not None
        assert blocked.id is not None
        store.set_queue_position(urgent.id, 1)

        recording_console = _RecordingConsole()
        config = Config.load(tmp_path)
        queue_colors = QueueColors(
            position="red",
            blocked_marker="green",
            task_id="cyan",
            task_type="magenta",
            prompt="white",
            urgent="yellow",
            blocked_by="blue",
            explicit_position="bright_black",
            summary="bright_white",
        )
        with (
            patch.object(queue_render_cli._colors, "QUEUE_COLORS", queue_colors),  # noqa: SLF001
            patch.object(query_cli, "console", recording_console),
            patch.object(query_cli.Config, "load", return_value=config),
        ):
            exit_code = query_cli.cmd_next(
                argparse.Namespace(
                    project_dir=tmp_path,
                    all=True,
                    tags=None,
                    any_tag=False,
                )
            )

        text_outputs = [output for output in recording_console.outputs if isinstance(output, Text)]
        assert exit_code == 0
        urgent_line = next(text for text in text_outputs if "Urgent next task" in text.plain)
        urgent_meta = next(text for text in text_outputs if "[urgent]" in text.plain)
        blocked_line = next(text for text in text_outputs if "Blocked next task" in text.plain)
        blocked_meta = next(
            text
            for text in text_outputs
            if str(blocker.id) in text.plain and "blocked" in text.plain
        )

        assert _span_styles(urgent_line) == ["red", "cyan", "magenta", "white"]
        assert _styled_substrings(urgent_meta)["[urgent]"] == "yellow"
        assert _styled_substrings(urgent_meta)["[#1]"] == "bright_black"
        assert _span_styles(blocked_line) == ["green", "cyan", "magenta", "white"]
        assert _span_styles(blocked_meta) == ["blue"]

    def test_queue_and_next_tag_filters_are_case_insensitive(self, tmp_path: Path):
        """queue/next should match lowercase-stored tags for mixed-case --tag values."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        store.add("Release runnable", tags=("release-1.2",))
        store.add("Backlog runnable", tags=("backlog",))

        queue_result = run_gza("queue", "--tag", "Release-1.2", "--project", str(tmp_path))
        next_result = run_gza("next", "--tag", "Release-1.2", "--project", str(tmp_path))

        assert queue_result.returncode == 0
        assert "Release runnable" in queue_result.stdout
        assert "Backlog runnable" not in queue_result.stdout

        assert next_result.returncode == 0
        assert "Release runnable" in next_result.stdout
        assert "Backlog runnable" not in next_result.stdout


class TestShowCommand:
    """Tests for 'gza show' command."""

    def test_show_existing_task(self, tmp_path: Path):
        """Show command displays task details."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A detailed task prompt", "status": "pending"},
        ])

        result = run_gza("show", "testproject-1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Task " in result.stdout
        assert "A detailed task prompt" in result.stdout
        assert "Status: pending" in result.stdout

    def test_show_output_reads_legacy_fix_summary_fallback(self, tmp_path: Path):
        """--output should surface legacy fix summaries stored only on disk."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Fix legacy output", task_type="fix")
        assert task.id is not None
        task.status = "completed"
        task.slug = "20260422-fix-show-output"
        task.output_content = None
        task.report_file = None
        store.update(task)

        summary_path = tmp_path / ".gza" / "summaries" / f"{task.slug}.md"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("- Restored legacy fix output\n")

        result = run_gza("show", str(task.id), "--output", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "- Restored legacy fix output" in result.stdout

    def test_show_prompt_prints_only_built_prompt_text(self, tmp_path: Path):
        """--prompt should emit only prompt text, not a JSON metadata envelope."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Prompt only task")
        assert task.id is not None

        result = run_gza("show", str(task.id), "--prompt", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Prompt only task" in result.stdout
        assert '"task_id"' not in result.stdout
        assert '"prompt"' not in result.stdout
        assert '"verify_command"' not in result.stdout

    def test_show_metadata_only_hides_prompt_and_output_blocks(self, tmp_path: Path):
        """--metadata-only should keep the detail view while omitting prompt and output/report content."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Prompt hidden in metadata-only mode")
        assert task.id is not None
        task.status = "completed"
        task.output_content = "report line 1\nreport line 2"
        store.update(task)
        store.add_comment(task.id, "Operator note", source="direct")

        result = run_gza("show", str(task.id), "--metadata-only", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Task " in result.stdout
        assert "Status: completed" in result.stdout
        assert "Comments:" in result.stdout
        assert "Operator note" in result.stdout
        assert "Prompt:" not in result.stdout
        assert "Output:" not in result.stdout
        assert "Prompt hidden in metadata-only mode" not in result.stdout
        assert "report line 1" not in result.stdout

    @pytest.mark.parametrize("flag", ["--prompt", "--output", "--path", "--full"])
    def test_show_metadata_only_rejects_incompatible_flags(self, tmp_path: Path, flag: str):
        """--metadata-only should fail closed when combined with other show-only output modes."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Task for metadata-only conflict")
        assert task.id is not None

        result = run_gza("show", str(task.id), "--metadata-only", flag, "--project", str(tmp_path))

        assert result.returncode == 1
        assert f"Error: --metadata-only cannot be used with {flag}." in result.stdout

    def test_show_metadata_only_rejects_multiple_incompatible_flags_in_stable_order(self, tmp_path: Path):
        """--metadata-only should report all incompatible flags in a predictable order."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Task for multi-flag metadata-only conflict")
        assert task.id is not None

        result = run_gza(
            "show",
            str(task.id),
            "--metadata-only",
            "--prompt",
            "--output",
            "--path",
            "--full",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 1
        assert (
            "Error: --metadata-only cannot be used with --prompt, --output, --path, --full."
            in result.stdout
        )

    def test_show_displays_execution_mode_when_set(self, tmp_path: Path):
        """Show command includes execution provenance mode when present."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Task with execution mode")
        assert task.id is not None
        task.execution_mode = "skill_inline"
        store.update(task)

        result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Execution Mode: skill_inline" in result.stdout

    def test_show_displays_trigger_source_and_unknown_for_legacy_rows(self, tmp_path: Path):
        """Show command should surface trigger source and label legacy NULL rows as unknown."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        manual = store.add("Task with trigger source", trigger_source="manual")
        legacy = store.add("Legacy task without trigger source")
        assert manual.id is not None
        assert legacy.id is not None

        manual_result = run_gza("show", str(manual.id), "--project", str(tmp_path))
        legacy_result = run_gza("show", str(legacy.id), "--project", str(tmp_path))

        assert manual_result.returncode == 0
        assert "Trigger Source: manual" in manual_result.stdout
        assert legacy_result.returncode == 0
        assert "Trigger Source: unknown" in legacy_result.stdout

    def test_show_review_displays_verdict_and_score(self, tmp_path: Path):
        """Show command includes derived review verdict and score metadata."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        review = store.add("Review output", task_type="review")
        assert review.id is not None
        review.status = "completed"
        review.output_content = "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        review.review_score = 34
        store.update(review)

        result = run_gza("show", str(review.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Verdict: CHANGES_REQUESTED" in result.stdout
        assert "Score: 34/100" in result.stdout

    def test_show_current_format_review_displays_stored_score(self, tmp_path: Path):
        """Show command should display persisted score for current review template format."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        review = store.add("Current-format review output", task_type="review")
        assert review.id is not None
        review.status = "completed"
        review.output_content = (
            "## Summary\n\n"
            "- No - Missing edge case coverage\n\n"
            "## Blockers\n\n"
            "### B1 Missing guard\n"
            "Required fix: add guard for empty input\n"
            "Required tests: add empty-input regression\n\n"
            "## Follow-Ups\n\n"
            "### F1 Improve docs\n"
            "Recommended follow-up: document empty input behavior\n"
            "Recommended tests: docs snippet check\n\n"
            "## Verdict\n\n"
            "Verdict: CHANGES_REQUESTED\n"
        )
        review.review_score = 67
        store.update(review)

        result = run_gza("show", str(review.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Verdict: CHANGES_REQUESTED" in result.stdout
        assert "Score: 67/100" in result.stdout

    def test_show_review_hides_score_when_source_unavailable(self, tmp_path: Path):
        """Show command must not render score when no derivation source exists."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        review = store.add("Review output", task_type="review")
        assert review.id is not None
        review.status = "completed"
        review.output_content = None
        review.report_file = None
        review.review_score = None
        store.update(review)

        result = run_gza("show", str(review.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Score:" not in result.stdout

    def test_show_review_displays_zero_score_for_malformed_present_content(self, tmp_path: Path):
        """Show command should render 0/100 when malformed review content is present."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        review = store.add("Malformed review output", task_type="review")
        assert review.id is not None
        review.status = "completed"
        review.output_content = "not a structured review"
        review.report_file = None
        review.review_score = None
        store.update(review)

        result = run_gza("show", str(review.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Score: 0/100" in result.stdout

    def test_show_displays_task_comments(self, tmp_path: Path):
        """Show command prints full comment metadata, including audit timestamps/state."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Task with comments")
        assert task.id is not None

        resolved_seed = store.add_comment(task.id, "First note", source="direct", author="alice")
        store.resolve_comments(task.id, created_on_or_before=resolved_seed.created_at)
        store.add_comment(task.id, "Second note", source="github")

        comments = store.get_comments(task.id)
        assert len(comments) == 2
        resolved_comment, open_comment = comments
        assert resolved_comment.resolved_at is not None
        assert open_comment.resolved_at is None

        resolved_created = f"{resolved_comment.created_at.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S')} UTC"
        resolved_at = f"{resolved_comment.resolved_at.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S')} UTC"
        open_created = f"{open_comment.created_at.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S')} UTC"

        result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Comments:" in result.stdout
        assert f"id={resolved_comment.id}" in result.stdout
        assert "source=direct" in result.stdout
        assert "author=alice" in result.stdout
        assert "state=resolved" in result.stdout
        assert f"created={resolved_created}" in result.stdout
        assert f"resolved={resolved_at}" in result.stdout
        assert f"id={open_comment.id}" in result.stdout
        assert "source=github" in result.stdout
        assert "state=open" in result.stdout
        assert f"created={open_created}" in result.stdout
        assert "First note" in result.stdout
        assert "Second note" in result.stdout

    def test_show_displays_review_verify_evidence(self, tmp_path: Path):
        """Show should surface persisted review verify audit fields and stored markdown."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        impl.status = "completed"
        impl.branch = "gza/20260605-implement-feature"
        store.update(impl)

        task = store.add("Review feature", task_type="review", depends_on=impl.id)
        task.slug = "20260605-review-feature"
        task.status = "completed"
        task.review_verify_status = "failed"
        task.review_verify_exit_status = "7"
        task.review_verify_branch = impl.branch
        task.review_verify_head_sha = "deadbeef"
        task.review_verify_base_sha = "cafebabe"
        task.review_verify_cwd = "/tmp/worktrees/20260605-review-feature-review"
        task.review_verify_markdown = (
            "## verify_command result\n\n"
            "- Command: `./bin/tests`\n"
            "- Status: failed\n"
            "- Exit status: 7\n"
            "- Working directory: `/tmp/worktrees/20260605-review-feature-review`\n"
            "- Failure: verify failed\n\n"
            "Failing output (trimmed):\n"
            "```text\nmypy failed\n```"
        )
        store.update(task)
        config = Config.load(tmp_path)
        stored = store_command_output_artifact(
            store,
            task,
            config,
            kind="verify_command_output",
            producer="review_verify",
            label="verify_command",
            output="mypy failed\n",
            command="./bin/tests",
            status="failed",
            exit_status="7",
            head_sha="deadbeef",
            metadata={
                "reviewed_branch": impl.branch,
                "reviewed_base_sha": "cafebabe",
                "working_directory": task.review_verify_cwd,
            },
        )
        task.review_verify_artifact_file = stored.path
        store.update(task)

        result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Review Verify Status:" in result.stdout
        assert "failed" in result.stdout
        assert "Review Verify Exit:" in result.stdout
        assert "Review Verify Branch:" in result.stdout
        assert "Review Verify Head:" in result.stdout
        assert "Review Verify Base:" in result.stdout
        assert "Review Verify Cwd:" in result.stdout
        assert "Review Verify Artifact:" in result.stdout
        assert "Artifacts:" in result.stdout
        assert stored.path in result.stdout
        assert "Review Verify Result:" in result.stdout
        assert "## verify_command result" in result.stdout
        assert "mypy failed" in result.stdout

    def test_show_metadata_only_marks_missing_artifacts(self, tmp_path: Path) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Task with missing artifact", task_type="review")
        task.status = "completed"
        store.update(task)
        config = Config.load(tmp_path)
        stored = store_command_output_artifact(
            store,
            task,
            config,
            kind="verify_command_output",
            producer="review_verify",
            label="verify_command",
            output="full output\n",
            status="failed",
            exit_status="1",
        )
        (tmp_path / stored.path).unlink()

        result = run_gza("show", str(task.id), "--metadata-only", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Artifacts:" in result.stdout
        assert stored.path in result.stdout
        assert "missing" in result.stdout

    def test_artifact_command_prints_latest_content_and_path(self, tmp_path: Path) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Task with artifacts", task_type="review")
        store.update(task)
        config = Config.load(tmp_path)
        older = store_command_output_artifact(
            store,
            task,
            config,
            kind="verify_command_output",
            producer="review_verify",
            label="verify_command",
            output="older output\n",
            created_at=datetime(2026, 6, 1, tzinfo=UTC),
        )
        newer = store_command_output_artifact(
            store,
            task,
            config,
            kind="verify_command_output",
            producer="review_verify",
            label="verify_command",
            output="newer output\n",
            created_at=datetime(2026, 6, 2, tzinfo=UTC),
        )

        content_result = run_gza("artifact", str(task.id), "--project", str(tmp_path))
        path_result = run_gza("artifact", str(task.id), "--path", "--project", str(tmp_path))

        assert content_result.returncode == 0
        assert content_result.stdout == "newer output\n\n"
        assert path_result.returncode == 0
        assert path_result.stdout.strip() == str(tmp_path / newer.path)

    def test_artifact_command_does_not_fall_back_to_older_content_when_latest_row_is_metadata_only(
        self, tmp_path: Path
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Task with mixed verify artifacts", task_type="review")
        task.status = "completed"
        store.update(task)
        config = Config.load(tmp_path)
        older = store_command_output_artifact(
            store,
            task,
            config,
            kind="verify_command_output",
            producer="review_verify",
            label="verify_command",
            output="older output\n",
            status="failed",
            exit_status="1",
            created_at=datetime(2026, 6, 1, tzinfo=UTC),
        )
        newer = store_command_output_artifact(
            store,
            task,
            config,
            kind="verify_command_output",
            producer="review_verify",
            label="verify_command",
            output=None,
            status="unavailable",
            created_at=datetime(2026, 6, 2, tzinfo=UTC),
        )
        task.review_verify_artifact_file = newer.path
        store.update(task)

        content_result = run_gza("artifact", str(task.id), "--project", str(tmp_path))
        path_result = run_gza("artifact", str(task.id), "--path", "--project", str(tmp_path))
        show_result = run_gza("show", str(task.id), "--metadata-only", "--project", str(tmp_path))

        assert content_result.returncode == 1
        assert "has no content file" in content_result.stdout
        assert "older output" not in content_result.stdout

        assert path_result.returncode == 1
        assert "has no content file" in path_result.stdout
        assert str(tmp_path / newer.path) not in path_result.stdout

        assert show_result.returncode == 0
        assert f"Review Verify Artifact:" in show_result.stdout
        assert newer.path in show_result.stdout
        assert older.path in show_result.stdout

    def test_artifact_command_path_fails_when_latest_content_file_is_missing(self, tmp_path: Path) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Task with missing latest artifact file", task_type="review")
        store.update(task)
        config = Config.load(tmp_path)
        stored = store_command_output_artifact(
            store,
            task,
            config,
            kind="verify_command_output",
            producer="review_verify",
            label="verify_command",
            output="verify output\n",
            created_at=datetime(2026, 6, 2, tzinfo=UTC),
        )
        (tmp_path / stored.path).unlink()

        content_result = run_gza("artifact", str(task.id), "--project", str(tmp_path))
        path_result = run_gza("artifact", str(task.id), "--path", "--project", str(tmp_path))

        assert content_result.returncode == 1
        assert f"missing on disk: {stored.path}" in content_result.stdout

        assert path_result.returncode == 1
        assert f"missing on disk: {stored.path}" in path_result.stdout

    def test_show_metadata_only_lists_skipped_cross_project_metadata_only_artifact(self, tmp_path: Path) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Cross-project review with skipped verify artifact", task_type="review")
        task.status = "completed"
        store.update(task)
        config = Config.load(tmp_path)
        older = store_command_output_artifact(
            store,
            task,
            config,
            kind="verify_command_output",
            producer="review_verify",
            label="verify_command",
            output="services/foo passed\n",
            status="passed",
            exit_status="0",
            scope="services/foo",
            metadata={
                "scope": "services/foo",
                "working_directory": "services/foo",
                "skip_reason": None,
            },
            created_at=datetime(2026, 6, 1, tzinfo=UTC),
        )
        newer = store_command_output_artifact(
            store,
            task,
            config,
            kind="verify_command_output",
            producer="review_verify",
            label="verify_command",
            output=None,
            status="skipped",
            exit_status="skipped",
            scope="unknown paths",
            metadata={
                "scope": "unknown paths",
                "working_directory": "unknown paths",
                "skip_reason": "affected paths fell outside all discovered project roots",
            },
            created_at=datetime(2026, 6, 2, tzinfo=UTC),
        )
        task.review_verify_artifact_file = older.path
        store.update(task)

        result = run_gza("show", str(task.id), "--metadata-only", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Artifacts:" in result.stdout
        assert older.path in result.stdout
        assert newer.path in result.stdout
        assert "skipped" in result.stdout
        assert "missing" in result.stdout
        assert "Review Verify Artifact:" in result.stdout
        assert newer.path in result.stdout

    def test_show_warns_and_reads_when_readonly_db_is_missing_task_comments(self, tmp_path: Path):
        """Show should warn instead of trying to repair task_comments on a frozen DB."""
        import sqlite3

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Task with damaged schema")
        assert task.id is not None

        db_path = tmp_path / ".gza" / "gza.db"
        conn = sqlite3.connect(db_path)
        conn.execute("DROP TABLE task_comments")
        conn.commit()
        conn.close()

        original_mode = db_path.stat().st_mode
        os.chmod(db_path, 0o444)
        try:
            result = run_gza("show", str(task.id), "--project", str(tmp_path))
        finally:
            os.chmod(db_path, original_mode)

        assert result.returncode == 0
        assert "Task with damaged schema" in result.stdout
        assert "Warning: Query-only DB open detected incomplete task_comments schema" in result.stderr
    def test_show_query_only_missing_task_comments_id_warns_without_traceback(self, tmp_path: Path):
        """Show should degrade comments cleanly when task_comments.id is missing on a frozen DB."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Task with missing comment ids")
        assert task.id is not None
        store.add_comment(task.id, "Existing comment", source="direct")

        db_path = tmp_path / ".gza" / "gza.db"
        _drop_task_comments_column(db_path, "id")

        original_mode = db_path.stat().st_mode
        os.chmod(db_path, 0o444)
        try:
            result = run_gza("show", str(task.id), "--project", str(tmp_path))
        finally:
            os.chmod(db_path, original_mode)

        assert result.returncode == 0
        assert "Task with missing comment ids" in result.stdout
        assert "Comments:" not in result.stdout
        assert "Existing comment" not in result.stdout
        assert "Traceback" not in result.stdout
        assert "Traceback" not in result.stderr
        assert "Warning: Query-only DB open detected incomplete task_comments schema" in result.stderr

    def test_show_query_only_pre_v40_missing_completion_reason_reads_without_traceback(
        self, tmp_path: Path
    ):
        """Show should read a frozen v39 snapshot without forcing the v40 additive migration."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Completed before completion reasons")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        db_path = tmp_path / ".gza" / "gza.db"
        _drop_tasks_column(db_path, "completion_reason")
        with sqlite3.connect(db_path) as conn:
            conn.execute("UPDATE schema_version SET version = 39")
            conn.commit()

        original_mode = db_path.stat().st_mode
        os.chmod(db_path, 0o444)
        try:
            result = run_gza("show", str(task.id), "--project", str(tmp_path))
        finally:
            os.chmod(db_path, original_mode)

        assert result.returncode == 0
        assert "Completed before completion reasons" in result.stdout
        assert "Completion Reason:" not in result.stdout
        assert "Traceback" not in result.stdout
        assert "Traceback" not in result.stderr
        assert "tasks.completion_reason" in result.stderr

    def test_show_query_only_missing_tasks_project_id_surfaces_controlled_error_without_traceback(
        self,
        tmp_path: Path,
    ):
        """Show should fail closed with a schema error when tasks.project_id is missing."""
        import sqlite3

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Task with missing project_id")
        assert task.id is not None

        db_path = tmp_path / ".gza" / "gza.db"
        conn = sqlite3.connect(db_path)
        conn.execute("ALTER TABLE tasks RENAME TO tasks_old")
        conn.execute(
            """
            CREATE TABLE tasks AS
            SELECT
                id,
                prompt,
                status,
                task_type,
                slug,
                branch,
                log_file,
                report_file,
                based_on,
                has_commits,
                duration_seconds,
                num_steps_reported,
                num_steps_computed,
                num_turns,
                num_turns_reported,
                num_turns_computed,
                attach_count,
                attach_duration_seconds,
                cost_usd,
                created_at,
                started_at,
                running_pid,
                completed_at,
                "group",
                depends_on,
                spec,
                create_review,
                same_branch,
                task_type_hint,
                output_content,
                session_id,
                pr_number,
                model,
                provider,
                provider_is_explicit,
                urgent,
                urgent_bumped_at,
                queue_position,
                input_tokens,
                output_tokens,
                merge_status,
                merged_at,
                failure_reason,
                skip_learnings,
                diff_files_changed,
                diff_lines_added,
                diff_lines_removed,
                review_cleared_at,
                review_score,
                log_schema_version,
                execution_mode,
                base_branch
            FROM tasks_old
            """
        )
        conn.execute("DROP TABLE tasks_old")
        conn.commit()
        conn.close()

        original_mode = db_path.stat().st_mode
        os.chmod(db_path, 0o444)
        try:
            result = run_gza("show", str(task.id), "--project", str(tmp_path))
        finally:
            os.chmod(db_path, original_mode)

        assert result.returncode == 1
        assert "Error: Query-only DB open detected missing required column tasks.project_id" in result.stderr
        assert "Traceback" not in result.stdout
        assert "Traceback" not in result.stderr

    def test_show_query_only_missing_tasks_table_surfaces_controlled_error_without_traceback(
        self,
        tmp_path: Path,
    ):
        """Show should fail closed with a schema error when the tasks table is missing."""
        import sqlite3

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Task before dropping tasks table")
        assert task.id is not None

        db_path = tmp_path / ".gza" / "gza.db"
        conn = sqlite3.connect(db_path)
        conn.execute("DROP TABLE tasks")
        conn.commit()
        conn.close()

        original_mode = db_path.stat().st_mode
        os.chmod(db_path, 0o444)
        try:
            result = run_gza("show", str(task.id), "--project", str(tmp_path))
        finally:
            os.chmod(db_path, original_mode)

        assert result.returncode == 1
        assert "Error: Query-only DB open detected missing required table tasks" in result.stderr
        assert "Traceback" not in result.stdout
        assert "Traceback" not in result.stderr

    def test_show_nonexistent_task(self, tmp_path: Path):
        """Show command handles nonexistent task."""
        setup_db_with_tasks(tmp_path, [])

        result = run_gza("show", "testproject-999999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_show_displays_lineage_for_review_task(self, tmp_path: Path):
        """Show command displays lineage using implementation/review chain."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement feature", task_type="implement")
        assert impl.id is not None
        review = store.add("Review feature", task_type="review", depends_on=impl.id)
        assert review.id is not None

        result = run_gza("show", str(review.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Lineage:" in result.stdout
        assert f"{impl.id}" in result.stdout
        assert f"{review.id}" in result.stdout

    def test_show_displays_active_worktree_path_for_task_branch(self, tmp_path: Path):
        """Show command includes active worktree path when task branch is checked out in a worktree."""
        task, worktree_path = _setup_task_with_worktree_metadata(
            tmp_path,
            task_prompt="Task with worktree",
            branch_name="feature/show-worktree",
            worktree_name="show-worktree",
        )
        assert task.id is not None
        assert worktree_path is not None

        with patch("gza.cli.query.Git.worktree_list", return_value=[
            {
                "path": str(worktree_path),
                "branch": f"refs/heads/{task.branch}",
                "prunable": False,
            }
        ]):
            result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Branch: feature/show-worktree" in result.stdout
        compact_output = "".join(result.stdout.split())
        assert f"Worktree: {worktree_path}".replace(" ", "") in compact_output

    def test_show_omits_worktree_path_when_branch_has_no_active_worktree(self, tmp_path: Path):
        """Show command omits worktree line when no active worktree is registered for the task branch."""
        task, worktree_path = _setup_task_with_worktree_metadata(
            tmp_path,
            task_prompt="Task without active worktree",
            branch_name="feature/no-worktree",
            worktree_name=None,
        )
        assert task.id is not None
        assert worktree_path is None

        with patch("gza.cli.query.Git.worktree_list", return_value=[]):
            result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Branch: feature/no-worktree" in result.stdout
        assert "Worktree:" not in result.stdout

    def test_show_warns_when_worktree_lookup_raises_git_error(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """Show command emits a warning when worktree lookup fails with GitError."""
        from gza.cli.query import cmd_show
        from gza.git import GitError

        task, worktree_path = _setup_task_with_worktree_metadata(
            tmp_path,
            task_prompt="Task with lookup failure",
            branch_name="feature/worktree-lookup-giterror",
            worktree_name=None,
        )
        assert task.id is not None
        assert worktree_path is None

        with patch("gza.cli.query.Git.worktree_list", side_effect=GitError("simulated worktree list failure")):
            args = argparse.Namespace(
                project_dir=tmp_path,
                task_id=str(task.id),
                prompt=False,
                path=False,
                output=False,
                page=False,
                full=False,
            )
            exit_code = cmd_show(args)
        output = capsys.readouterr().out

        assert exit_code == 0
        assert "Branch: feature/worktree-lookup-giterror" in output
        assert "Warning: Worktree lookup failed:" in output
        assert "simulated worktree list failure" in output

    def test_show_warns_when_worktree_lookup_raises_os_error(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """Show command emits a warning when worktree lookup fails with OSError."""
        from gza.cli.query import cmd_show

        task, worktree_path = _setup_task_with_worktree_metadata(
            tmp_path,
            task_prompt="Task with lookup os error",
            branch_name="feature/worktree-lookup-oserror",
            worktree_name=None,
        )
        assert task.id is not None
        assert worktree_path is None

        with patch("gza.cli.query.Git.worktree_list", side_effect=OSError("simulated os error")):
            args = argparse.Namespace(
                project_dir=tmp_path,
                task_id=str(task.id),
                prompt=False,
                path=False,
                output=False,
                page=False,
                full=False,
            )
            exit_code = cmd_show(args)
        output = capsys.readouterr().out

        assert exit_code == 0
        assert "Branch: feature/worktree-lookup-oserror" in output
        assert "Warning: Worktree lookup failed:" in output
        assert "simulated os error" in output

    def test_show_recovered_parent_reports_lifecycle_and_lineage_statuses(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Show should answer recovery + review state without extra commands."""
        from gza.cli.query import cmd_show

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed_root = store.add("Recovered failed implement", task_type="implement")
        assert failed_root.id is not None
        failed_root.status = "failed"
        failed_root.failure_reason = "TIMEOUT"
        failed_root.session_id = "sess-root"
        failed_root.branch = "feature/root"
        failed_root.completed_at = datetime(2026, 5, 4, 10, 0, 0, tzinfo=UTC)
        store.update(failed_root)

        resumed = store.add(failed_root.prompt, task_type="implement", based_on=failed_root.id)
        assert resumed.id is not None
        resumed.status = "completed"
        resumed.session_id = failed_root.session_id
        resumed.branch = failed_root.branch
        resumed.has_commits = True
        resumed.merge_status = "unmerged"
        resumed.completed_at = datetime(2026, 5, 4, 10, 10, 0, tzinfo=UTC)
        store.update(resumed)

        review = store.add(f"Review {resumed.id}", task_type="review", based_on=resumed.id, depends_on=resumed.id)
        assert review.id is not None
        review.status = "in_progress"
        review.created_at = datetime(2026, 5, 4, 10, 15, 0, tzinfo=UTC)
        store.update(review)

        git = MagicMock()
        git.default_branch.return_value = "main"
        git.can_merge.return_value = True
        git.worktree_list.return_value = []

        with patch("gza.cli.query.Git", return_value=git):
            exit_code = cmd_show(
                argparse.Namespace(
                    project_dir=tmp_path,
                    task_id=str(failed_root.id),
                    prompt=False,
                    path=False,
                    output=False,
                    page=False,
                    full=False,
                    metadata_only=True,
                )
            )

        output = capsys.readouterr().out
        assert exit_code == 0
        assert f"Lifecycle: recovered, review in_progress ({review.id})" in output
        assert "Lineage:" in output
        assert f"{failed_root.id}[implement] failed (TIMEOUT)" in output
        assert f"{resumed.id}[implement] [resume] completed (unmerged)" in output
        assert f"{review.id}[review] in_progress" in output

    def test_show_recovered_parent_reports_completed_and_merged_for_merged_resume(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Recovered parents should not re-plan already merged resume descendants."""
        from gza.cli.query import cmd_show

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed_root = store.add("Recovered merged implement", task_type="implement")
        assert failed_root.id is not None
        failed_root.status = "failed"
        failed_root.failure_reason = "TIMEOUT"
        failed_root.session_id = "sess-root"
        failed_root.branch = "feature/root"
        failed_root.completed_at = datetime(2026, 5, 4, 10, 0, 0, tzinfo=UTC)
        store.update(failed_root)

        resumed = store.add(failed_root.prompt, task_type="implement", based_on=failed_root.id)
        assert resumed.id is not None
        resumed.status = "completed"
        resumed.session_id = failed_root.session_id
        resumed.branch = failed_root.branch
        resumed.has_commits = True
        resumed.merge_status = "merged"
        resumed.completed_at = datetime(2026, 5, 4, 10, 10, 0, tzinfo=UTC)
        store.update(resumed)

        git = MagicMock()
        git.default_branch.return_value = "main"
        git.can_merge.return_value = True
        git.worktree_list.return_value = []

        with patch("gza.cli.query.Git", return_value=git):
            exit_code = cmd_show(
                argparse.Namespace(
                    project_dir=tmp_path,
                    task_id=str(failed_root.id),
                    prompt=False,
                    path=False,
                    output=False,
                    page=False,
                    full=False,
                    metadata_only=True,
                )
            )

        output = capsys.readouterr().out
        assert exit_code == 0
        assert "Lifecycle: recovered, completed and merged" in output
        assert "ready for review" not in output
        assert "Lineage:" in output
        assert f"{failed_root.id}[implement] failed (TIMEOUT)" in output
        assert f"{resumed.id}[implement] [resume] completed (merged)" in output

    def test_show_completed_plan_reports_terminal_lifecycle_from_merged_implement_descendant(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Completed plans should summarize landed implement descendants as terminal work."""
        from gza.cli.query import cmd_show

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan landed feature", task_type="plan")
        assert plan.id is not None
        plan.status = "completed"
        plan.completed_at = datetime(2026, 5, 4, 10, 0, 0, tzinfo=UTC)
        store.update(plan)

        implement = store.add("Implement landed feature", task_type="implement", based_on=plan.id)
        assert implement.id is not None
        implement.status = "completed"
        implement.branch = "feature/landed-plan"
        implement.has_commits = True
        implement.merge_status = "merged"
        implement.completed_at = datetime(2026, 5, 4, 10, 10, 0, tzinfo=UTC)
        store.update(implement)

        git = MagicMock()
        git.default_branch.return_value = "main"
        git.can_merge.return_value = True
        git.worktree_list.return_value = []

        with patch("gza.cli.query.Git", return_value=git):
            exit_code = cmd_show(
                argparse.Namespace(
                    project_dir=tmp_path,
                    task_id=str(plan.id),
                    prompt=False,
                    path=False,
                    output=False,
                    page=False,
                    full=False,
                    metadata_only=True,
                )
            )

        output = capsys.readouterr().out
        assert exit_code == 0
        assert "Lifecycle: completed and merged" in output
        assert "Lifecycle: recovered, completed and merged" not in output
        assert "implement task already exists for this plan" not in output
        assert "lifecycle unavailable" not in output

    def test_show_completed_plan_reports_review_state_from_implement_descendant(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Completed plans should summarize the active implement descendant lifecycle."""
        from gza.cli.query import cmd_show

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan reviewed feature", task_type="plan")
        assert plan.id is not None
        plan.status = "completed"
        plan.completed_at = datetime(2026, 5, 4, 10, 0, 0, tzinfo=UTC)
        store.update(plan)

        implement = store.add("Implement reviewed feature", task_type="implement", based_on=plan.id)
        assert implement.id is not None
        implement.status = "completed"
        implement.branch = "feature/reviewed-plan"
        implement.has_commits = True
        implement.merge_status = "unmerged"
        implement.completed_at = datetime(2026, 5, 4, 10, 10, 0, tzinfo=UTC)
        store.update(implement)

        review = store.add(f"Review {implement.id}", task_type="review", based_on=implement.id, depends_on=implement.id)
        assert review.id is not None
        review.status = "in_progress"
        review.created_at = datetime(2026, 5, 4, 10, 15, 0, tzinfo=UTC)
        store.update(review)

        git = MagicMock()
        git.default_branch.return_value = "main"
        git.can_merge.return_value = True
        git.worktree_list.return_value = []

        with patch("gza.cli.query.Git", return_value=git):
            exit_code = cmd_show(
                argparse.Namespace(
                    project_dir=tmp_path,
                    task_id=str(plan.id),
                    prompt=False,
                    path=False,
                    output=False,
                    page=False,
                    full=False,
                    metadata_only=True,
                )
            )

        output = capsys.readouterr().out
        assert exit_code == 0
        assert f"Lifecycle: review in_progress ({review.id})" in output
        assert f"Lifecycle: recovered, review in_progress ({review.id})" not in output
        assert "implement task already exists for this plan" not in output
        assert "lifecycle unavailable" not in output

    def test_show_plan_review_displays_verdict_and_manifest_state(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        from gza.cli.query import cmd_show

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan reviewed feature", task_type="plan")
        assert plan.id is not None
        plan.status = "completed"
        plan.completed_at = datetime(2026, 5, 4, 10, 0, 0, tzinfo=UTC)
        store.update(plan)

        review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
        assert review.id is not None
        review.status = "completed"
        review.completed_at = datetime(2026, 5, 4, 10, 10, 0, tzinfo=UTC)
        review.output_content = (
            "## Verdict\n\n"
            "Verdict: APPROVED\n\n"
            "## Slice Manifest\n"
            "```json\n"
            "{\n"
            '  "schema_version": 1,\n'
            f'  "source_task_id": "{plan.id}",\n'
            '  "source_task_type": "plan",\n'
            '  "verdict": "APPROVED",\n'
            '  "slice_quality": {\n'
            '    "fits_single_task_budget": true,\n'
            '    "timeout_budget_minutes": 45,\n'
            '    "max_expected_files_changed_per_slice": 8,\n'
            '    "rationale": "Each slice has one clear verification boundary."\n'
            "  },\n"
            '  "slices": [\n'
            "    {\n"
            '      "slice_id": "S1",\n'
            '      "title": "Implement slice one",\n'
            '      "prompt": "Implement slice one",\n'
            '      "scope": ["Add the first slice"],\n'
            '      "out_of_scope": [],\n'
            '      "acceptance_criteria": ["Slice one works"],\n'
            '      "depends_on_slices": [],\n'
            '      "based_on_slice": null,\n'
            '      "review_scope": "Slice one only.",\n'
            '      "estimated_complexity": "medium",\n'
            '      "expected_timeout_minutes": 30,\n'
            '      "requires_code_review": true,\n'
            '      "tags": ["auth"]\n'
            "    }\n"
            "  ],\n"
            '  "manual_override": {"commands": ["uv run gza plan-review <review-id> --edit-slices"]}\n'
            "}\n"
            "```\n"
        )
        store.update(review)

        git = MagicMock()
        git.default_branch.return_value = "main"
        git.can_merge.return_value = True
        git.worktree_list.return_value = []

        with patch("gza.cli.query.Git", return_value=git):
            exit_code = cmd_show(
                argparse.Namespace(
                    project_dir=tmp_path,
                    task_id=str(review.id),
                    prompt=False,
                    path=False,
                    output=False,
                    page=False,
                    full=False,
                    metadata_only=True,
                )
            )

        output = capsys.readouterr().out
        assert exit_code == 0
        assert "Verdict: APPROVED" in output
        assert "Slice Manifest: review manifest valid (1 slices)" in output

    def test_lineage_shows_plan_review_verdict_as_compact_child_metadata(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        from gza.cli.query import cmd_lineage

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan reviewed feature", task_type="plan")
        assert plan.id is not None
        plan.status = "completed"
        plan.completed_at = datetime(2026, 5, 4, 10, 0, 0, tzinfo=UTC)
        store.update(plan)

        review = store.add("Review the plan", task_type="plan_review", depends_on=plan.id)
        assert review.id is not None
        review.status = "completed"
        review.completed_at = datetime(2026, 5, 4, 10, 10, 0, tzinfo=UTC)
        review.output_content = "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        store.update(review)

        exit_code = cmd_lineage(argparse.Namespace(project_dir=tmp_path, task_id=str(plan.id)))
        output = capsys.readouterr().out

        assert exit_code == 0
        normalized = " ".join(output.split())
        assert "plan_review [depends]" in normalized or "plan_review [plan_review]" in normalized
        assert "verdict: CHANGES_REQUESTED" in normalized

    def test_show_held_plan_displays_auto_implement_hold_guidance(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        from gza.cli.query import cmd_show

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan held feature", task_type="plan", auto_implement=False)
        assert plan.id is not None
        plan.status = "completed"
        plan.completed_at = datetime(2026, 5, 4, 10, 0, 0, tzinfo=UTC)
        store.update(plan)

        git = MagicMock()
        git.default_branch.return_value = "main"
        git.can_merge.return_value = True
        git.worktree_list.return_value = []

        with patch("gza.cli.query.Git", return_value=git):
            exit_code = cmd_show(
                argparse.Namespace(
                    project_dir=tmp_path,
                    task_id=str(plan.id),
                    prompt=False,
                    path=False,
                    output=False,
                    page=False,
                    full=False,
                    metadata_only=True,
                )
            )

        output = capsys.readouterr().out
        assert exit_code == 0
        assert "Auto Implement: no (hold for review; run uv run gza implement" in output

    def test_show_displays_create_pr_flag(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        from gza.cli.query import cmd_show

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Implement with PR intent", task_type="implement", create_pr=True)
        assert task.id is not None

        git = MagicMock()
        git.default_branch.return_value = "main"
        git.can_merge.return_value = True
        git.worktree_list.return_value = []

        with patch("gza.cli.query.Git", return_value=git):
            exit_code = cmd_show(
                argparse.Namespace(
                    project_dir=tmp_path,
                    task_id=str(task.id),
                    prompt=False,
                    path=False,
                    output=False,
                    page=False,
                    full=False,
                    metadata_only=True,
                )
            )

        output = capsys.readouterr().out
        assert exit_code == 0
        assert "Create PR: yes" in output

    def test_show_displays_cached_pr_details(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        from gza.cli.query import cmd_show

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Implement with cached PR", task_type="implement")
        assert task.id is not None
        task.pr_number = 123
        task.pr_state = "open"
        store.update(task)

        git = MagicMock()
        git.default_branch.return_value = "main"
        git.can_merge.return_value = True
        git.worktree_list.return_value = []

        with patch("gza.cli.query.Git", return_value=git):
            exit_code = cmd_show(
                argparse.Namespace(
                    project_dir=tmp_path,
                    task_id=str(task.id),
                    prompt=False,
                    path=False,
                    output=False,
                    page=False,
                    full=False,
                    metadata_only=True,
                )
            )

        output = capsys.readouterr().out
        assert exit_code == 0
        assert "Create PR: no" in output
        assert "PR Number: 123" in output
        assert "PR State: open" in output

    @pytest.mark.parametrize(
        ("changed_diff", "expected"),
        [
            (False, "Changed Diff: no"),
            (True, "Changed Diff: yes"),
            (None, "Changed Diff: unknown (treated as yes)"),
        ],
    )
    def test_show_rebase_renders_changed_diff_states(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        changed_diff: bool | None,
        expected: str,
    ) -> None:
        from gza.cli.query import cmd_show

        setup_config(tmp_path)
        store = make_store(tmp_path)

        rebase = store.add("Rebase feature", task_type="rebase")
        assert rebase.id is not None
        rebase.status = "completed"
        rebase.completed_at = datetime(2026, 5, 4, 10, 0, 0, tzinfo=UTC)
        rebase.branch = "feature/rebase-show"
        rebase.changed_diff = changed_diff
        store.update(rebase)

        git = MagicMock()
        git.default_branch.return_value = "main"
        git.can_merge.return_value = True
        git.worktree_list.return_value = []

        with patch("gza.cli.query.Git", return_value=git):
            exit_code = cmd_show(
                argparse.Namespace(
                    project_dir=tmp_path,
                    task_id=str(rebase.id),
                    prompt=False,
                    path=False,
                    output=False,
                    page=False,
                    full=False,
                    metadata_only=True,
                )
            )

        output = capsys.readouterr().out
        assert exit_code == 0
        assert expected in output

    def test_show_implement_renders_review_carried_across_rebase(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from gza import advance_engine as advance_engine_module
        from gza.cli.query import cmd_show

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Implement feature", task_type="implement")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime(2026, 5, 4, 10, 0, 0, tzinfo=UTC)
        task.branch = "feature/review-preserved-show"
        task.has_commits = True
        task.merge_status = "unmerged"
        store.update(task)

        review = store.add(f"Review {task.id}", task_type="review", based_on=task.id, depends_on=task.id)
        review.status = "completed"
        review.completed_at = datetime(2026, 5, 4, 10, 10, 0, tzinfo=UTC)
        store.update(review)

        rebase = store.add(f"Rebase {task.id}", task_type="rebase", based_on=task.id, same_branch=True)
        rebase.status = "completed"
        rebase.completed_at = datetime(2026, 5, 4, 10, 20, 0, tzinfo=UTC)
        rebase.branch = task.branch
        rebase.changed_diff = False
        store.update(rebase)

        monkeypatch.setattr(
            advance_engine_module,
            "get_review_report",
            lambda project_dir, r: ParsedReviewReport(verdict="APPROVED", findings=(), format_version="legacy"),
        )

        git = MagicMock()
        git.default_branch.return_value = "main"
        git.can_merge.return_value = True
        git.worktree_list.return_value = []

        with patch("gza.cli.query.Git", return_value=git):
            exit_code = cmd_show(
                argparse.Namespace(
                    project_dir=tmp_path,
                    task_id=str(task.id),
                    prompt=False,
                    path=False,
                    output=False,
                    page=False,
                    full=False,
                    metadata_only=True,
                )
            )

        output = capsys.readouterr().out
        assert exit_code == 0
        assert f"Review: APPROVED (carried across rebase {rebase.id})" in output

    def test_show_in_progress_implement_does_not_invoke_db_write_paths(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from gza import advance_engine as advance_engine_module
        from gza.cli.query import cmd_show

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Implement feature", task_type="implement")
        assert task.id is not None
        task.status = "in_progress"
        task.branch = "feature/show-readonly"
        task.merge_status = "unmerged"
        task.has_commits = False
        store.update(task)

        review = store.add(f"Review {task.id}", task_type="review", based_on=task.id, depends_on=task.id)
        review.status = "completed"
        review.completed_at = datetime(2026, 5, 4, 10, 10, 0, tzinfo=UTC)
        store.update(review)

        rebase = store.add(f"Rebase {task.id}", task_type="rebase", based_on=task.id, same_branch=True)
        rebase.status = "completed"
        rebase.completed_at = datetime(2026, 5, 4, 10, 20, 0, tzinfo=UTC)
        rebase.branch = task.branch
        rebase.changed_diff = False
        store.update(rebase)

        monkeypatch.setattr(
            advance_engine_module,
            "get_review_report",
            lambda project_dir, r: ParsedReviewReport(verdict="APPROVED", findings=(), format_version="legacy"),
        )

        git = MagicMock()
        git.default_branch.return_value = "main"
        git.can_merge.return_value = True
        git.worktree_list.return_value = []
        git.resolve_fresh_merge_source.return_value = advance_engine_module.ResolvedMergeSourceRef(task.branch)
        git.rev_parse_if_exists.side_effect = lambda ref: "same-sha" if ref in {task.branch, "main"} else None
        git.is_ancestor.return_value = True

        with (
            patch.object(
                query_cli.SqliteTaskStore,
                "set_merge_status",
                side_effect=AssertionError("show must not set merge_status"),
            ),
            patch.object(
                query_cli.SqliteTaskStore,
                "set_merge_unit_state",
                side_effect=AssertionError("show must not set merge unit state"),
            ),
            patch.object(
                query_cli.SqliteTaskStore,
                "create_merge_unit",
                side_effect=AssertionError("show must not create merge units"),
            ),
            patch("gza.cli.query.Git", return_value=git),
        ):
            exit_code = cmd_show(
                argparse.Namespace(
                    project_dir=tmp_path,
                    task_id=str(task.id),
                    prompt=False,
                    path=False,
                    output=False,
                    page=False,
                    full=False,
                    metadata_only=True,
                )
            )

        output = capsys.readouterr().out
        assert exit_code == 0
        assert f"Review: APPROVED (carried across rebase {rebase.id})" in output

    def test_show_merged_implement_still_renders_review_carried_across_rebase(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from gza import advance_engine as advance_engine_module
        from gza.cli.query import cmd_show

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Implement feature", task_type="implement")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime(2026, 5, 4, 10, 0, 0, tzinfo=UTC)
        task.branch = "feature/review-preserved-merged-show"
        task.has_commits = True
        task.merge_status = "merged"
        store.update(task)
        unit = store.get_or_create_merge_unit_for_task(task)
        assert unit is not None
        store.set_merge_unit_state(unit.id, "merged", merged_by_task_id=task.id)

        review = store.add(f"Review {task.id}", task_type="review", based_on=task.id, depends_on=task.id)
        review.status = "completed"
        review.completed_at = datetime(2026, 5, 4, 10, 10, 0, tzinfo=UTC)
        store.update(review)

        rebase = store.add(f"Rebase {task.id}", task_type="rebase", based_on=task.id, same_branch=True)
        rebase.status = "completed"
        rebase.completed_at = datetime(2026, 5, 4, 10, 20, 0, tzinfo=UTC)
        rebase.branch = task.branch
        rebase.changed_diff = False
        store.update(rebase)

        monkeypatch.setattr(
            advance_engine_module,
            "get_review_report",
            lambda project_dir, r: ParsedReviewReport(verdict="APPROVED", findings=(), format_version="legacy"),
        )

        git = MagicMock()
        git.default_branch.return_value = "main"
        git.can_merge.return_value = True
        git.worktree_list.return_value = []

        with patch("gza.cli.query.Git", return_value=git):
            exit_code = cmd_show(
                argparse.Namespace(
                    project_dir=tmp_path,
                    task_id=str(task.id),
                    prompt=False,
                    path=False,
                    output=False,
                    page=False,
                    full=False,
                    metadata_only=True,
                )
            )

        output = capsys.readouterr().out
        assert exit_code == 0
        assert "Merge Status: merged" in output
        assert f"Review: APPROVED (carried across rebase {rebase.id})" in output

    def test_show_implement_renders_review_invalidated_by_rebase(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from gza import advance_engine as advance_engine_module
        from gza.cli.query import cmd_show

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Implement feature", task_type="implement")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime(2026, 5, 4, 10, 0, 0, tzinfo=UTC)
        task.branch = "feature/review-invalidated-show"
        task.has_commits = True
        task.merge_status = "unmerged"
        store.update(task)

        review = store.add(f"Review {task.id}", task_type="review", based_on=task.id, depends_on=task.id)
        review.status = "completed"
        review.completed_at = datetime(2026, 5, 4, 10, 10, 0, tzinfo=UTC)
        store.update(review)

        rebase = store.add(f"Rebase {task.id}", task_type="rebase", based_on=task.id, same_branch=True)
        rebase.status = "completed"
        rebase.completed_at = datetime(2026, 5, 4, 10, 20, 0, tzinfo=UTC)
        rebase.branch = task.branch
        rebase.changed_diff = True
        store.update(rebase)

        monkeypatch.setattr(
            advance_engine_module,
            "get_review_report",
            lambda project_dir, r: ParsedReviewReport(verdict="APPROVED", findings=(), format_version="legacy"),
        )

        git = MagicMock()
        git.default_branch.return_value = "main"
        git.can_merge.return_value = True
        git.worktree_list.return_value = []

        with patch("gza.cli.query.Git", return_value=git):
            exit_code = cmd_show(
                argparse.Namespace(
                    project_dir=tmp_path,
                    task_id=str(task.id),
                    prompt=False,
                    path=False,
                    output=False,
                    page=False,
                    full=False,
                    metadata_only=True,
                )
            )

        output = capsys.readouterr().out
        assert exit_code == 0
        assert f"Review: invalidated by rebase {rebase.id} (diff changed)" in output

    def test_show_changes_requested_lifecycle_reports_active_improve_id(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Show should surface the concrete active improve task for CHANGES_REQUESTED reviews."""
        from gza.cli.query import cmd_show

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Implement feature", task_type="implement")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime(2026, 5, 4, 10, 0, 0, tzinfo=UTC)
        task.branch = "feature/changes-requested"
        task.has_commits = True
        task.merge_status = "unmerged"
        store.update(task)

        review = store.add(f"Review {task.id}", task_type="review", based_on=task.id, depends_on=task.id)
        assert review.id is not None
        review.status = "completed"
        review.output_content = "Verdict: CHANGES_REQUESTED"
        review.completed_at = datetime(2026, 5, 4, 10, 10, 0, tzinfo=UTC)
        store.update(review)

        improve = store.add("Improve feature", task_type="improve", based_on=task.id, depends_on=review.id)
        assert improve.id is not None
        improve.status = "in_progress"
        improve.created_at = datetime(2026, 5, 4, 10, 15, 0, tzinfo=UTC)
        store.update(improve)

        git = MagicMock()
        git.default_branch.return_value = "main"
        git.can_merge.return_value = True
        git.worktree_list.return_value = []

        with patch("gza.cli.query.Git", return_value=git):
            exit_code = cmd_show(
                argparse.Namespace(
                    project_dir=tmp_path,
                    task_id=str(task.id),
                    prompt=False,
                    path=False,
                    output=False,
                    page=False,
                    full=False,
                    metadata_only=True,
                )
            )

        output = capsys.readouterr().out
        assert exit_code == 0
        assert f"Lifecycle: improve in_progress ({improve.id})" in output
        assert "(unknown)" not in output

    def test_show_fresh_comments_lifecycle_reports_active_improve_id(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Show should surface the concrete active improve task for fresh unresolved comments."""
        from gza.cli.query import cmd_show

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Implement feature", task_type="implement")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime(2026, 5, 4, 10, 0, 0, tzinfo=UTC)
        task.branch = "feature/fresh-comments"
        task.has_commits = True
        task.merge_status = "unmerged"
        store.update(task)

        review = store.add(f"Review {task.id}", task_type="review", based_on=task.id, depends_on=task.id)
        assert review.id is not None
        review.status = "completed"
        review.output_content = "Verdict: APPROVED"
        review.completed_at = datetime(2026, 5, 4, 10, 10, 0, tzinfo=UTC)
        store.update(review)

        improve = store.add("Improve fresh comments", task_type="improve", based_on=task.id, depends_on=review.id)
        assert improve.id is not None
        improve.status = "in_progress"
        improve.created_at = datetime(2026, 5, 4, 10, 20, 0, tzinfo=UTC)
        store.update(improve)

        store.add_comment(task.id, "Need one more tweak from newer unresolved feedback.")

        git = MagicMock()
        git.default_branch.return_value = "main"
        git.can_merge.return_value = True
        git.worktree_list.return_value = []

        with patch("gza.cli.query.Git", return_value=git):
            exit_code = cmd_show(
                argparse.Namespace(
                    project_dir=tmp_path,
                    task_id=str(task.id),
                    prompt=False,
                    path=False,
                    output=False,
                    page=False,
                    full=False,
                    metadata_only=True,
                )
            )

        output = capsys.readouterr().out
        assert exit_code == 0
        assert f"Lifecycle: improve in_progress ({improve.id})" in output
        assert "(unknown)" not in output

    def test_show_lineage_omits_merge_badge_for_failed_code_task_with_stale_merge_status(
        self, tmp_path: Path
    ) -> None:
        """Failed code-task lineage rows should not show merge badges from stale metadata."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed = store.add("Failed implement with stale merge status", task_type="implement")
        assert failed.id is not None
        failed.status = "failed"
        failed.failure_reason = "TIMEOUT"
        failed.has_commits = True
        failed.branch = "feature/failed-stale-merge-status"
        failed.merge_status = "unmerged"
        failed.completed_at = datetime(2026, 5, 4, 10, 0, 0, tzinfo=UTC)
        store.update(failed)

        child = store.add("Follow-up lineage child", task_type="review", based_on=failed.id, depends_on=failed.id)
        assert child.id is not None

        result = run_gza("show", str(failed.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Lineage:" in result.stdout
        assert f"{failed.id}[implement] failed (TIMEOUT)" in result.stdout
        assert f"{failed.id}[implement] failed (TIMEOUT) (unmerged)" not in result.stdout

    def test_show_completed_merged_task_omits_trivial_lifecycle_and_lineage(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Self-only completed+merged tasks should keep show output compact."""
        from gza.cli.query import cmd_show

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Merged implement", task_type="implement")
        assert task.id is not None
        task.status = "completed"
        task.has_commits = True
        task.merge_status = "merged"
        task.completed_at = datetime(2026, 5, 4, 11, 0, 0, tzinfo=UTC)
        store.update(task)

        exit_code = cmd_show(
            argparse.Namespace(
                project_dir=tmp_path,
                task_id=str(task.id),
                prompt=False,
                path=False,
                output=False,
                page=False,
                full=False,
                metadata_only=True,
            )
        )

        output = capsys.readouterr().out
        assert exit_code == 0
        assert "Lifecycle:" not in output
        assert "Lineage:" not in output

    def test_show_prints_merge_status_for_merge_owner_rows(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Show should render merge status for the branch-owning row."""
        from gza.cli.query import cmd_show

        owner, _improve = _seed_same_branch_merge_owner_and_improve(tmp_path)

        exit_code = cmd_show(
            argparse.Namespace(
                project_dir=tmp_path,
                task_id=str(owner.id),
                prompt=False,
                path=False,
                output=False,
                page=False,
                full=False,
                metadata_only=True,
            )
        )
        output = capsys.readouterr().out

        assert exit_code == 0
        assert "Merge Status: unmerged" in output

    def test_show_suppresses_merge_status_for_same_branch_follow_up_rows(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Show should suppress stale row-local merge metadata on same-branch follow-up tasks."""
        from gza.cli.query import cmd_show

        _owner, improve = _seed_same_branch_merge_owner_and_improve(tmp_path)

        exit_code = cmd_show(
            argparse.Namespace(
                project_dir=tmp_path,
                task_id=str(improve.id),
                prompt=False,
                path=False,
                output=False,
                page=False,
                full=False,
                metadata_only=True,
            )
        )
        output = capsys.readouterr().out

        assert exit_code == 0
        assert "Type: improve" in output
        assert "Merge Status:" not in output

    def test_show_failed_recovery_chain_needs_attention_uses_shared_wording(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Exhausted recovery chains should surface the shared needs-attention verdict."""
        from gza.advance_engine import format_needs_attention_lifecycle
        from gza.cli.query import cmd_show

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed_root = store.add("Exhausted failed implement", task_type="implement")
        assert failed_root.id is not None
        failed_root.status = "failed"
        failed_root.failure_reason = "TIMEOUT"
        failed_root.session_id = "sess-root"
        failed_root.branch = "feature/root"
        failed_root.completed_at = datetime(2026, 5, 4, 12, 0, 0, tzinfo=UTC)
        store.update(failed_root)

        failed_resume = store.add(failed_root.prompt, task_type="implement", based_on=failed_root.id)
        assert failed_resume.id is not None
        failed_resume.status = "failed"
        failed_resume.failure_reason = "TIMEOUT"
        failed_resume.session_id = failed_root.session_id
        failed_resume.branch = failed_root.branch
        failed_resume.completed_at = datetime(2026, 5, 4, 12, 10, 0, tzinfo=UTC)
        store.update(failed_resume)

        failed_resume_retry_limit = store.add(failed_root.prompt, task_type="implement", based_on=failed_root.id)
        assert failed_resume_retry_limit.id is not None
        failed_resume_retry_limit.status = "failed"
        failed_resume_retry_limit.failure_reason = "TIMEOUT"
        failed_resume_retry_limit.session_id = failed_root.session_id
        failed_resume_retry_limit.branch = failed_root.branch
        failed_resume_retry_limit.completed_at = datetime(2026, 5, 4, 12, 20, 0, tzinfo=UTC)
        store.update(failed_resume_retry_limit)

        git = MagicMock()
        git.default_branch.return_value = "main"
        git.can_merge.return_value = True
        git.worktree_list.return_value = []

        with patch("gza.cli.query.Git", return_value=git):
            exit_code = cmd_show(
                argparse.Namespace(
                    project_dir=tmp_path,
                    task_id=str(failed_root.id),
                    prompt=False,
                    path=False,
                    output=False,
                    page=False,
                    full=False,
                    metadata_only=True,
                )
            )

        output = capsys.readouterr().out
        assert exit_code == 0
        expected = format_needs_attention_lifecycle(
            {
                "type": "skip",
                "needs_attention_reason": "retry-limit-reached",
                "description": "SKIP: automatic recovery stops here; retry limit reached",
            }
        )
        assert f"Lifecycle: {expected}" in output
        assert f"{failed_root.id}[implement] failed (TIMEOUT)" in output
        assert f"{failed_resume.id}[implement] [resume] failed (TIMEOUT)" in output
        assert f"{failed_resume_retry_limit.id}[implement] [resume] failed (TIMEOUT)" in output

    def test_show_recovered_needs_attention_lifecycle_uses_failed_color(
        self, tmp_path: Path
    ) -> None:
        """Recovered needs-attention lifecycles must render with attention severity, not recovered green."""
        from gza.advance_engine import with_needs_attention
        from gza.cli.query import cmd_show

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed_root = store.add("Recovered attention implement", task_type="implement")
        assert failed_root.id is not None
        failed_root.status = "failed"
        failed_root.failure_reason = "TIMEOUT"
        failed_root.session_id = "sess-root"
        failed_root.branch = "feature/root"
        failed_root.completed_at = datetime(2026, 5, 4, 12, 0, 0, tzinfo=UTC)
        store.update(failed_root)

        resumed = store.add(failed_root.prompt, task_type="implement", based_on=failed_root.id)
        assert resumed.id is not None
        resumed.status = "completed"
        resumed.session_id = failed_root.session_id
        resumed.branch = failed_root.branch
        resumed.has_commits = True
        resumed.merge_status = "unmerged"
        resumed.completed_at = datetime(2026, 5, 4, 12, 10, 0, tzinfo=UTC)
        store.update(resumed)

        git = MagicMock()
        git.default_branch.return_value = "main"
        git.can_merge.return_value = True
        git.worktree_list.return_value = []

        attention_action = with_needs_attention(
            {"type": "skip", "description": "SKIP: automatic recovery exhausted"},
            reason="retry-limit-reached",
            subject_task_id=failed_root.id,
        )
        console = Console(record=True, force_terminal=True, color_system="standard", width=300)
        show_colors = dict(query_cli.SHOW_COLORS_DICT)
        show_colors.update(
            {
                "heading": "white",
                "section": "white",
                "label": "white",
                "value": "white",
                "status_running": "blue",
                "status_completed": "green",
                "status_failed": "red",
                "status_default": "white",
            }
        )

        with (
            patch("gza.cli.query.Git", return_value=git),
            patch("gza.cli.query.determine_next_action", return_value=attention_action),
            patch.object(query_cli, "console", console),
            patch.object(query_cli, "SHOW_COLORS_DICT", show_colors),
        ):
            exit_code = cmd_show(
                argparse.Namespace(
                    project_dir=tmp_path,
                    task_id=str(failed_root.id),
                    prompt=False,
                    path=False,
                    output=False,
                    page=False,
                    full=False,
                    metadata_only=True,
                )
            )

        plain = console.export_text(clear=False)
        assert exit_code == 0
        assert "Lifecycle: recovered, needs attention" in plain

    def test_show_mergeable_behind_branch_uses_review_lifecycle(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        from gza.cli.query import cmd_show

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement stale show", task_type="implement")
        assert impl.id is not None
        impl.status = "completed"
        impl.completed_at = datetime.now(UTC)
        impl.branch = "feature/stale-show"
        impl.merge_status = "unmerged"
        impl.has_commits = True
        store.update(impl)

        review = store.add("Pending review", task_type="review", depends_on=impl.id, based_on=impl.id)
        review.status = "pending"
        store.update(review)

        git = MagicMock()
        git.default_branch.return_value = "main"
        git.can_merge.return_value = True
        git.resolve_fresh_merge_source.return_value = ("origin/feature/stale-show", None)
        git.count_commits_behind.return_value = 1
        git.worktree_list.return_value = []

        with patch("gza.cli.query.Git", return_value=git):
            exit_code = cmd_show(
                argparse.Namespace(
                    project_dir=tmp_path,
                    task_id=str(impl.id),
                    prompt=False,
                    path=False,
                    output=False,
                    page=False,
                    full=False,
                    metadata_only=True,
                )
            )

        output = capsys.readouterr().out
        assert exit_code == 0
        assert "Lifecycle: review pending" in output

    def test_show_lineage_statuses_reuse_top_level_show_status_colors(self, tmp_path: Path) -> None:
        """Show lineage status labels should use the same status palette as the top-level Status field."""
        from gza.cli.query import cmd_show

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed_root = store.add("Failed root", task_type="implement")
        assert failed_root.id is not None
        failed_root.status = "failed"
        failed_root.failure_reason = "TIMEOUT"
        failed_root.completed_at = datetime(2026, 5, 4, 12, 0, 0, tzinfo=UTC)
        store.update(failed_root)

        in_progress_child = store.add("Active review", task_type="review", depends_on=failed_root.id)
        assert in_progress_child.id is not None
        in_progress_child.status = "in_progress"
        store.update(in_progress_child)

        completed_child = store.add("Completed resume", task_type="implement", based_on=failed_root.id)
        assert completed_child.id is not None
        completed_child.status = "completed"
        completed_child.has_commits = True
        completed_child.merge_status = "merged"
        completed_child.completed_at = datetime(2026, 5, 4, 12, 10, 0, tzinfo=UTC)
        store.update(completed_child)

        console = Console(record=True, force_terminal=True, color_system="standard", width=300)
        show_colors = dict(query_cli.SHOW_COLORS_DICT)
        show_colors.update(
            {
                "heading": "white",
                "section": "white",
                "label": "white",
                "value": "white",
                "task_id": "white",
                "branch": "white",
                "status_running": "blue",
                "status_completed": "green",
                "status_failed": "red",
                "status_default": "white",
            }
        )
        lineage_status_colors = {
            "pending": "white",
            "in_progress": "magenta",
            "completed": "cyan",
            "failed": "yellow",
            "unknown": "white",
        }

        with (
            patch.object(query_cli, "console", console),
            patch.object(query_cli, "SHOW_COLORS_DICT", show_colors),
            patch.dict("gza.cli._common._colors.LINEAGE_STATUS_COLORS", lineage_status_colors, clear=True),
        ):
            exit_code = cmd_show(
                argparse.Namespace(
                    project_dir=tmp_path,
                    task_id=str(failed_root.id),
                    prompt=False,
                    path=False,
                    output=False,
                    page=False,
                    full=False,
                    metadata_only=True,
                )
            )

        rendered = console.export_text(styles=True, clear=False)
        plain = console.export_text(clear=False)
        assert exit_code == 0
        assert "Status: failed" in plain
        assert "Lineage:" in plain
        assert "\x1b[31mfailed\x1b[0m" in rendered
        assert "\x1b[31mTIMEOUT\x1b[0m" in rendered
        assert "\x1b[34min_progress\x1b[0m" in rendered
        assert "\x1b[32mcompleted\x1b[0m" in rendered
        assert "\x1b[33mTIMEOUT\x1b[0m" not in rendered
        assert "\x1b[35min_progress\x1b[0m" not in rendered
        assert "\x1b[36mcompleted\x1b[0m" not in rendered

    def test_show_reports_lifecycle_unavailable_when_default_branch_resolution_fails(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Show should surface lifecycle classification failures instead of silently degrading."""
        from gza.cli.query import cmd_show

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed_root = store.add("Recovered failed implement", task_type="implement")
        assert failed_root.id is not None
        failed_root.status = "failed"
        failed_root.failure_reason = "TIMEOUT"
        failed_root.session_id = "sess-root"
        failed_root.branch = "feature/root"
        failed_root.completed_at = datetime(2026, 5, 4, 10, 0, 0, tzinfo=UTC)
        store.update(failed_root)

        resumed = store.add(failed_root.prompt, task_type="implement", based_on=failed_root.id)
        assert resumed.id is not None
        resumed.status = "completed"
        resumed.session_id = failed_root.session_id
        resumed.branch = failed_root.branch
        resumed.has_commits = True
        resumed.merge_status = "unmerged"
        resumed.completed_at = datetime(2026, 5, 4, 10, 10, 0, tzinfo=UTC)
        store.update(resumed)

        git = MagicMock()
        git.default_branch.side_effect = GitError("simulated default branch failure")
        git.worktree_list.return_value = []

        with patch("gza.cli.query.Git", return_value=git):
            exit_code = cmd_show(
                argparse.Namespace(
                    project_dir=tmp_path,
                    task_id=str(failed_root.id),
                    prompt=False,
                    path=False,
                    output=False,
                    page=False,
                    full=False,
                    metadata_only=True,
                )
            )

        output = capsys.readouterr().out
        assert exit_code == 0
        assert "Lifecycle: recovered, lifecycle unavailable - failed to resolve default branch: simulated default branch failure" in output
        assert "Lifecycle: recovered\n" not in output
        assert "Lineage:" in output
        assert f"{failed_root.id}[implement] failed (TIMEOUT)" in output
        assert f"{resumed.id}[implement] [resume] completed (unmerged)" in output

    def test_show_reports_lifecycle_unavailable_when_lifecycle_classification_fails(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Show should keep rendering lineage when shared lifecycle classification raises."""
        from gza.cli.query import cmd_show

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed_root = store.add("Recovered failed implement", task_type="implement")
        assert failed_root.id is not None
        failed_root.status = "failed"
        failed_root.failure_reason = "TIMEOUT"
        failed_root.session_id = "sess-root"
        failed_root.branch = "feature/root"
        failed_root.completed_at = datetime(2026, 5, 4, 10, 0, 0, tzinfo=UTC)
        store.update(failed_root)

        resumed = store.add(failed_root.prompt, task_type="implement", based_on=failed_root.id)
        assert resumed.id is not None
        resumed.status = "completed"
        resumed.session_id = failed_root.session_id
        resumed.branch = failed_root.branch
        resumed.has_commits = True
        resumed.merge_status = "unmerged"
        resumed.completed_at = datetime(2026, 5, 4, 10, 10, 0, tzinfo=UTC)
        store.update(resumed)

        git = MagicMock()
        git.default_branch.return_value = "main"
        git.worktree_list.return_value = []

        with (
            patch("gza.cli.query.Git", return_value=git),
            patch("gza.cli.query.determine_next_action", side_effect=GitError("simulated lifecycle classification failure")),
        ):
            exit_code = cmd_show(
                argparse.Namespace(
                    project_dir=tmp_path,
                    task_id=str(failed_root.id),
                    prompt=False,
                    path=False,
                    output=False,
                    page=False,
                    full=False,
                    metadata_only=True,
                )
            )

        output = capsys.readouterr().out
        assert exit_code == 0
        assert "Lifecycle: recovered, lifecycle unavailable - failed to classify lifecycle: simulated lifecycle classification failure" in output
        assert "Lineage:" in output
        assert f"{failed_root.id}[implement] failed (TIMEOUT)" in output
        assert f"{resumed.id}[implement] [resume] completed (unmerged)" in output

    def test_show_omits_prunable_worktree_path_for_task_branch(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        """Show command should not treat prunable worktrees as active paths."""
        from gza.cli.query import cmd_show

        task, worktree_path = _setup_task_with_worktree_metadata(
            tmp_path,
            task_prompt="Task with stale worktree registration",
            branch_name="feature/prunable-worktree",
            worktree_name=None,
        )
        assert task.id is not None
        assert worktree_path is None

        with patch("gza.cli.query.Git.worktree_list", return_value=[
            {
                "path": "/tmp/stale-worktree",
                "branch": "refs/heads/feature/prunable-worktree",
                "prunable": "gone",
            }
        ]):
            args = argparse.Namespace(
                project_dir=tmp_path,
                task_id=str(task.id),
                prompt=False,
                path=False,
                output=False,
                page=False,
                full=False,
            )
            exit_code = cmd_show(args)
        output = capsys.readouterr().out

        assert exit_code == 0
        assert "Branch: feature/prunable-worktree" in output
        assert "Worktree:" not in output

    def test_show_failed_task_displays_failure_diagnostics(self, tmp_path: Path):
        """Failed task output keeps AGENT_FORFEIT guidance even with MAX_TURNS fallback state."""
        import json

        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\n"
            "db_path: .gza/gza.db\n"
            "max_steps: 50\n"
            "verify_command: uv run pytest tests/ -q\n"
        )

        store = make_store(tmp_path)
        task = store.add("Failed task for show diagnostics")
        assert task.id is not None
        task.status = "failed"
        task.failure_reason = "MAX_TURNS"
        task.session_id = "sess-show-failed"
        task.log_file = ".gza/logs/fail.log"
        task.num_steps_reported = 55
        task.num_turns_reported = 55
        store.update(task)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        registry.register(
            WorkerMetadata(
                worker_id="w-20260227-000001",
                pid=12345,
                task_id=task.id,
                task_slug=task.slug,
                started_at="2026-02-27T00:00:00+00:00",
                status="failed",
                log_file=task.log_file,
                worktree=None,
                is_background=True,
            )
        )

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        lines = [
            {
                "type": "item.completed",
                "item": {
                    "type": "agent_message",
                    "text": "Initial attempt note",
                },
            },
            {
                "type": "item.completed",
                "item": {
                    "type": "agent_message",
                    "text": "[GZA_FAILURE:AGENT_FORFEIT]\nBlocked by ordering prerequisite; implementation not started.",
                },
            },
            {
                "type": "assistant",
                "message": {
                    "role": "assistant",
                    "content": [
                        {"type": "tool_use", "id": "tool_1", "name": "Bash", "input": {"command": "uv run pytest tests/ -q"}},
                        {"type": "text", "text": "Running verification"},
                    ],
                },
            },
            {
                "type": "user",
                "message": {
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": "tool_1",
                            "is_error": True,
                            "content": "FAILED tests/test_cli.py::test_case - AssertionError",
                        }
                    ],
                },
            },
            {"type": "result", "subtype": "error_max_turns", "result": "Stopped at limit", "num_steps": 55},
        ]
        (log_dir / "fail.log").write_text("\n".join(json.dumps(line) for line in lines))

        result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Failure Reason: MAX_TURNS" in result.stdout
        assert "[GZA_FAILURE:AGENT_FORFEIT]" in result.stdout
        assert result.stdout.count("[GZA_FAILURE:AGENT_FORFEIT]") == 1
        assert "Failure Summary: Stopped due to max turns limit." in result.stdout
        assert "Agent Explanation:" in result.stdout
        assert "Blocked by ordering prerequisite; implementation not started." in result.stdout
        assert "Last Verify Failure:" in result.stdout
        assert "uv run pytest tests/ -q" in result.stdout
        assert "Last Result Context: error_max_turns" in result.stdout
        assert "Run Context: background (w-20260227-000001)" in result.stdout
        assert "Step Limit:" not in result.stdout
        assert f"gza retry {task.id}" in result.stdout
        assert f"gza resume {task.id}" not in result.stdout

    def test_show_failed_task_renders_termination_source(self, tmp_path: Path):
        """Failed-task diagnostics should include structured termination source metadata."""
        import json

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Interrupted task")
        assert task.id is not None
        task.status = "failed"
        task.failure_reason = "TERMINATED"
        task.log_file = ".gza/logs/interrupted.log"
        store.update(task)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        lines = [
            {
                "type": "gza",
                "subtype": "interrupt",
                "message": "Task interrupted by signal",
                "failure_reason": "TERMINATED",
                "signal": "SIGTERM",
                "source": "watch_reconcile_no_activity",
                "detail": "watch reconciliation detected no recent task log activity",
            }
        ]
        (log_dir / "interrupted.log").write_text("\n".join(json.dumps(line) for line in lines))

        result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Failure Reason: TERMINATED" in result.stdout
        assert "Termination Source:" in result.stdout
        assert "watch_reconcile_no_activity" in result.stdout

    def test_show_failed_task_extracts_verify_failure_from_tool_error_entries(self, tmp_path: Path):
        """Failed-task diagnostics should detect verify failures in non-Claude tool_* entry shapes."""
        import json


        setup_config(tmp_path)
        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\n"
            "db_path: .gza/gza.db\n"
            "verify_command: uv run pytest tests/ -q\n"
        )

        store = make_store(tmp_path)
        task = store.add("Failed task with non-Claude logs")
        assert task.id is not None
        task.status = "failed"
        task.failure_reason = "TEST_FAILURE"
        task.log_file = ".gza/logs/non-claude.log"
        store.update(task)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        lines = [
            {
                "type": "tool_use",
                "id": "call_1",
                "tool_name": "Bash",
                "tool_input": {"command": "uv run pytest tests/ -q"},
            },
            {
                "type": "tool_error",
                "tool_use_id": "call_1",
                "content": "FAILED tests/test_cli.py::test_case - AssertionError",
            },
            {"type": "result", "subtype": "error_test_failure", "result": "verification failed"},
        ]
        (log_dir / "non-claude.log").write_text("\n".join(json.dumps(line) for line in lines))

        result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Last Verify Failure:" in result.stdout
        assert "uv run pytest tests/ -q" in result.stdout
        assert "AssertionError" in result.stdout

    def test_show_failed_test_failure_excludes_resume_next_step(self, tmp_path: Path):
        """TEST_FAILURE guidance should not advertise gza resume."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Failed verification")
        assert task.id is not None
        task.status = "failed"
        task.failure_reason = "TEST_FAILURE"
        task.session_id = "sess-test-failure"
        store.update(task)

        result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"gza retry {task.id}" in result.stdout
        assert f"gza resume {task.id}" not in result.stdout

    def test_show_failed_task_prerequisite_unmerged_next_steps(self, tmp_path: Path):
        """PREREQUISITE_UNMERGED should show merge-only guidance for legacy parked rows."""
        setup_config(tmp_path)

        store = make_store(tmp_path)
        dep = store.add("Upstream dependency")
        task = store.add("Failed downstream", depends_on=dep.id)
        assert task.id is not None
        task.status = "failed"
        task.failure_reason = "PREREQUISITE_UNMERGED"
        store.update(task)

        result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Failure Reason: PREREQUISITE_UNMERGED" in result.stdout
        assert "Failure Summary: Dependency is not yet merged to main." in result.stdout
        assert f"gza merge {dep.id}" in result.stdout
        assert f"gza retry {task.id}" not in result.stdout

    def test_show_failed_task_prerequisite_unmerged_empty_row_is_moot_without_retry_guidance(self, tmp_path: Path):
        setup_config(tmp_path)

        store = make_store(tmp_path)
        dep = store.add("Merged dependency")
        task = store.add("Historical blocked downstream", depends_on=dep.id)
        assert task.id is not None
        task.status = "failed"
        task.failure_reason = "PREREQUISITE_UNMERGED"
        task.completed_at = datetime.now(UTC)
        task.branch = "feature/empty-show"
        task.has_commits = False
        store.update(task)

        unit = store.create_merge_unit(
            source_branch=task.branch,
            target_branch="main",
            owner_task_id=task.id,
            state="empty",
        )
        store.attach_task_to_merge_unit(task.id, unit.id, "owner")

        result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Failure Reason: PREREQUISITE_UNMERGED" in result.stdout
        assert "Failure Summary: Historical dependency-ordering failure produced no work; task is moot." in result.stdout
        assert "Merge Status: empty" in result.stdout
        assert f"gza merge {dep.id}" not in result.stdout
        assert f"gza retry {task.id}" not in result.stdout
        assert "gza-4072 (`gza edit --clear-depends-on`)" in result.stdout

    def test_show_prerequisite_unmerged_prefers_resolved_dependency_from_log(self, tmp_path: Path):
        """PREREQUISITE_UNMERGED next steps should use resolved dependency_task_id from outcome log."""
        setup_config(tmp_path)

        store = make_store(tmp_path)
        direct_dep = store.add("Original failed dependency")
        retry_dep = store.add("Completed retry dependency", based_on=direct_dep.id)
        task = store.add("Failed downstream", depends_on=direct_dep.id)
        assert task.id is not None
        assert retry_dep.id is not None
        task.status = "failed"
        task.failure_reason = "PREREQUISITE_UNMERGED"
        task.log_file = ".gza/logs/prereq-unmerged.log"
        store.update(task)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "prereq-unmerged.log").write_text(
            json.dumps(
                {
                    "type": "gza",
                    "subtype": "outcome",
                    "failure_reason": "PREREQUISITE_UNMERGED",
                    "dependency_task_id": retry_dep.id,
                }
            )
            + "\n"
        )

        result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"gza merge {retry_dep.id}" in result.stdout
        assert f"gza merge {direct_dep.id}" not in result.stdout

    def test_show_indicates_worker_startup_failure(self, tmp_path: Path):
        """Show surfaces startup failure when worker failed before main log existed."""
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Task with startup failure")
        assert task.id is not None
        task.status = "failed"
        store.update(task)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        registry.register(
            WorkerMetadata(
                worker_id="w-20260318-startup-failure",
                pid=12345,
                task_id=task.id,
                task_slug=task.slug,
                started_at="2026-03-18T00:00:00+00:00",
                status="failed",
                log_file=None,
                worktree=None,
                is_background=True,
                startup_log_file=".gza/workers/w-20260318-startup-failure-startup.log",
            )
        )

        result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Worker Failure: failed during startup" in result.stdout
        assert "Startup Log: .gza/workers/w-20260318-startup-failure-startup.log" in result.stdout

    def test_show_completed_task_omits_failure_diagnostics(self, tmp_path: Path):
        """Completed task output should not include failed-task diagnostics block."""

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Completed task")
        task.status = "completed"
        store.update(task)

        result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Failure Reason:" not in result.stdout
        assert "Failure Summary:" not in result.stdout

    def test_show_plan_lineage_includes_downstream_implement(self, tmp_path: Path):
        """Show for a plan task includes downstream implement task in lineage."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Design the feature", task_type="plan")
        assert plan.id is not None
        impl = store.add("Implement the feature", task_type="implement", based_on=plan.id)
        assert impl.id is not None

        result = run_gza("show", str(plan.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Lineage:" in result.stdout
        assert f"{plan.id}" in result.stdout
        assert f"{impl.id}" in result.stdout

    def test_show_implement_lineage_includes_plan_and_review_improve_chain(self, tmp_path: Path):
        """Show for an implement task (based on a plan) includes plan, review, and improve."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Design the feature", task_type="plan")
        assert plan.id is not None
        impl = store.add("Implement the feature", task_type="implement", based_on=plan.id)
        assert impl.id is not None
        review = store.add("Review the feature", task_type="review", depends_on=impl.id)
        assert review.id is not None
        improve = store.add("Fix review issues", task_type="improve", based_on=impl.id)
        assert improve.id is not None

        result = run_gza("show", str(impl.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Lineage:" in result.stdout
        assert f"{plan.id}" in result.stdout
        assert f"{impl.id}" in result.stdout
        assert f"{review.id}" in result.stdout
        assert f"{improve.id}" in result.stdout

    def test_show_multi_level_dependency_lineage(self, tmp_path: Path):
        """Lineage traverses multi-level dependency chains (plan->impl->sub-impl)."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Top-level plan", task_type="plan")
        assert plan.id is not None
        impl1 = store.add("First implement", task_type="implement", based_on=plan.id)
        assert impl1.id is not None
        impl2 = store.add("Second implement based on first", task_type="implement", based_on=impl1.id)
        assert impl2.id is not None

        result = run_gza("show", str(plan.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Lineage:" in result.stdout
        assert f"{plan.id}" in result.stdout
        assert f"{impl1.id}" in result.stdout
        assert f"{impl2.id}" in result.stdout

    def test_show_lineage_orders_completed_root_before_pending_descendants(self, tmp_path: Path):
        """Show lineage keeps root first even when downstream tasks are still pending."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("Root task", task_type="implement")
        root.status = "completed"
        root.completed_at = datetime(2026, 3, 1, tzinfo=UTC)
        store.update(root)

        child = store.add("Child task", task_type="implement", based_on=root.id)
        grandchild = store.add("Grandchild task", task_type="implement", based_on=child.id)

        result = run_gza("show", str(root.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Lineage:" in result.stdout
        root_idx = result.stdout.index(f"{root.id}")
        child_idx = result.stdout.index(f"{child.id}")
        grandchild_idx = result.stdout.index(f"{grandchild.id}")
        assert root_idx < child_idx < grandchild_idx

    def test_show_depended_on_by_field(self, tmp_path: Path):
        """Show displays 'Depended on by' listing tasks that reference the displayed task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Design the feature", task_type="plan")
        assert plan.id is not None
        impl = store.add("Implement the feature", task_type="implement", based_on=plan.id)
        assert impl.id is not None
        review = store.add("Review the feature", task_type="review", depends_on=impl.id)
        assert review.id is not None

        # Show the plan: it should list impl as "Depended on by"
        result_plan = run_gza("show", str(plan.id), "--project", str(tmp_path))
        assert result_plan.returncode == 0
        assert "Depended on by:" in result_plan.stdout
        assert f"{impl.id}[implement]" in result_plan.stdout

        # Show the impl: it should list review as "Depended on by"
        result_impl = run_gza("show", str(impl.id), "--project", str(tmp_path))
        assert result_impl.returncode == 0
        assert "Depended on by:" in result_impl.stdout
        assert f"{review.id}[review]" in result_impl.stdout

    def test_show_truncates_long_output(self, tmp_path: Path):
        """gza show truncates output >30 lines to 20 with a remainder hint."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        long_content = "\n".join(f"line {i}" for i in range(1, 52))  # 51 lines
        task = store.add("Plan with long output", task_type="plan")
        assert task.id is not None
        task.status = "completed"
        task.output_content = long_content
        store.update(task)

        result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "line 20" in result.stdout
        assert "line 21" not in result.stdout
        assert "truncated" in result.stdout
        assert "31 more lines" in result.stdout
        assert f"gza show {task.id} --full" in result.stdout

    def test_show_full_flag_shows_complete_output(self, tmp_path: Path):
        """gza show --full bypasses truncation and displays all lines."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        long_content = "\n".join(f"line {i}" for i in range(1, 52))  # 51 lines
        task = store.add("Plan with long output", task_type="plan")
        assert task.id is not None
        task.status = "completed"
        task.output_content = long_content
        store.update(task)

        result = run_gza("show", str(task.id), "--full", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "line 51" in result.stdout
        assert "truncated" not in result.stdout

    def test_show_short_output_not_truncated(self, tmp_path: Path):
        """gza show does not truncate output with exactly 30 lines."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        content_30_lines = "\n".join(f"line {i}" for i in range(1, 31))  # exactly 30 lines
        task = store.add("Plan with 30-line output", task_type="plan")
        assert task.id is not None
        task.status = "completed"
        task.output_content = content_30_lines
        store.update(task)

        result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "line 30" in result.stdout
        assert "truncated" not in result.stdout


class TestPsCommand:
    """Tests for 'gza ps' command."""

    def test_ps_shows_task_id(self, tmp_path: Path):
        """PS command should display task ID for running workers."""
        from gza.workers import WorkerMetadata, WorkerRegistry

        # Setup config and database
        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a task
        task = store.add("Test task for ps command")

        # Create workers directory and register a worker
        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)

        worker = WorkerMetadata(
            worker_id="w-test-ps",
            pid=99999,  # Fake PID
            task_id=task.id,
            task_slug=None,
            started_at=datetime.now(UTC).isoformat(),
            status="running",
            log_file=None,
            worktree=None,
        )
        registry.register(worker)

        # Run ps command
        result = run_gza("ps", "--project", str(tmp_path))

        # Verify task ID is in output
        assert result.returncode == 0
        assert "TASK ID" in result.stdout, "Header should contain 'TASK ID' column"
        assert "STARTED" in result.stdout, "Header should contain 'STARTED' column"
        assert f"{task.id}" in result.stdout, f"Output should contain task ID {task.id}"

        # Cleanup
        registry.remove("w-test-ps")

    def test_print_ps_output_uses_themed_task_id_color(self, tmp_path: Path) -> None:
        """PS rows should render task IDs with the shared themed task-id color."""
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Theme-aware ps row")

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-ps-theme",
                pid=99998,
                task_id=task.id,
                task_slug=None,
                started_at=datetime.now(UTC).isoformat(),
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        recording_console = _RecordingConsole()
        args = argparse.Namespace(quiet=False, json=False)

        try:
            colors.set_theme(None, {
                "task_id": "#123456",
                "prompt": "#654321",
                "header": "bold #abcdef",
            })
            expected_header_color = colors.TASK_COLORS.header
            expected_task_id_color = colors.TASK_COLORS.task_id
            expected_prompt_color = colors.TASK_COLORS.prompt
            with patch.object(query_cli, "console", recording_console):
                query_cli._print_ps_output(args, registry, store)
        finally:
            colors.set_theme(None)
            registry.remove("w-test-ps-theme")

        assert len(recording_console.outputs) == 3
        header, separator, row = recording_console.outputs
        assert header == (
            f"[{expected_header_color}]"
            f"{'TASK ID':<10} {'TYPE':<10} {'STATUS':<16} {'PID':<8} "
            f"{'STARTED':<24} {'STEPS':<7} {'DURATION':<10} {'TASK'}"
            f"[/{expected_header_color}]"
        )
        assert separator == f"[{expected_header_color}]" + "─" * 106 + f"[/{expected_header_color}]"
        assert (
            f"[{expected_task_id_color}]{task.id:<10}[/{expected_task_id_color}]"
            in row
        )
        assert (
            f"[{expected_prompt_color}]Theme-aware ps row[/{expected_prompt_color}]"
            in row
        )
        assert "[cyan]" not in row

    def test_ps_reconciles_db_and_worker_with_source_both(self, tmp_path: Path):
        """PS dedupes by task_id and marks row source as both."""
        import json
        import os

        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Reconciled task")
        store.mark_in_progress(task)
        task = store.get(task.id)
        assert task is not None
        task.started_at = datetime(2026, 1, 8, 0, 1, tzinfo=UTC)
        store.update(task)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-both",
                pid=os.getpid(),
                task_id=task.id,
                task_slug=None,
                started_at="2026-01-08T00:00:00+00:00",
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        result = run_gza("ps", "--json", "--project", str(tmp_path))
        assert result.returncode == 0
        rows = json.loads(result.stdout)
        assert len(rows) == 1
        assert rows[0]["task_id"] == task.id
        assert rows[0]["source"] == "both"
        assert rows[0]["is_orphaned"] is False
        assert rows[0]["started_at"] == "2026-01-08T00:01:00+00:00"

        registry.remove("w-test-both")

    def test_ps_prunes_dead_worker_for_terminal_task(self, tmp_path: Path):
        """ps/status should prune stale worker entries once their task is terminal."""
        import subprocess

        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Terminal task with dead worker")
        task.status = "failed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        proc = subprocess.Popen(["true"])
        proc.wait()
        dead_pid = proc.pid

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-prune-on-ps",
                pid=dead_pid,
                task_id=task.id,
                task_slug=None,
                started_at=datetime.now(UTC).isoformat(),
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        result = run_gza("ps", "--all", "--json", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "No in-progress tasks" in result.stdout
        assert registry.get("w-prune-on-ps") is None

    def test_ps_no_id_background_claim_reconciles_single_active_row(self, tmp_path: Path):
        """No-id background claim should reconcile into one active non-orphaned task row."""
        import os

        from gza.cli.query import _build_ps_rows
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("No-id claimed task")
        store.mark_in_progress(task)
        task = store.get(task.id)
        assert task is not None
        task.running_pid = os.getpid()
        task.slug = "20260319-claim-no-id"
        store.update(task)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-no-id-claim",
                pid=os.getpid(),
                task_id=task.id,
                task_slug=None,
                started_at=datetime.now(UTC).isoformat(),
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        rows, _ = _build_ps_rows(registry, store, include_completed=False)
        assert len(rows) == 1
        assert rows[0]["source"] == "both"
        assert rows[0]["task_id"] == task.id
        assert rows[0]["status"] == "in_progress"
        assert rows[0]["is_orphaned"] is False

    def test_ps_build_rows_prefer_task_started_at_over_older_worker(self, tmp_path: Path, monkeypatch):
        """Task-backed ps rows should use task timing, not worker process age."""
        import os

        from gza.cli.query import _build_ps_rows
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Claimed task with newer task start")
        store.mark_in_progress(task)
        task = store.get(task.id)
        assert task is not None
        task.started_at = datetime(2026, 1, 8, 0, 1, tzinfo=UTC)
        task.running_pid = os.getpid()
        store.update(task)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-task-start-precedence",
                pid=os.getpid(),
                task_id=task.id,
                task_slug=None,
                started_at="2026-01-08T00:00:00+00:00",
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        fixed_now = datetime(2026, 1, 8, 0, 2, 30, tzinfo=UTC)

        class _FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                if tz is None:
                    return fixed_now.replace(tzinfo=None)
                return fixed_now.astimezone(tz)

        monkeypatch.setattr(query_cli, "datetime", _FixedDateTime)

        rows, _ = _build_ps_rows(registry, store, include_completed=False)
        assert len(rows) == 1
        assert rows[0]["task_id"] == task.id
        assert rows[0]["started"] == "2026-01-08 00:01:00 UTC"
        assert rows[0]["started_at"] == "2026-01-08T00:01:00+00:00"
        assert rows[0]["sort_timestamp"] == "2026-01-08T00:01:00+00:00"
        assert rows[0]["duration"] == "1m 30s"

    def test_no_orphan_warning_for_healthy_no_id_background_claim(self, tmp_path: Path):
        """Healthy claimed no-id background runs should not be classified as orphaned."""
        import os

        from gza.cli.query import _get_orphaned_tasks
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Healthy running task")
        store.mark_in_progress(task)
        task = store.get(task.id)
        assert task is not None
        task.running_pid = os.getpid()
        task.slug = "20260319-healthy-no-id"
        store.update(task)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-no-id-healthy",
                pid=os.getpid(),
                task_id=task.id,
                task_slug=None,
                started_at=datetime.now(UTC).isoformat(),
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        orphaned = _get_orphaned_tasks(registry, store)
        assert orphaned == []

    def test_ps_task_label_maps_to_claimed_task_for_no_id_background_claim(self, tmp_path: Path):
        """No-id claimed worker rows should render the claimed task label, not a worker placeholder."""
        import os

        from gza.cli.query import _build_ps_rows
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Claimed label task")
        store.mark_in_progress(task)
        task = store.get(task.id)
        assert task is not None
        task.running_pid = os.getpid()
        task.slug = "20260319-claimed-label"
        store.update(task)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-no-id-label",
                pid=os.getpid(),
                task_id=task.id,
                task_slug=None,
                started_at=datetime.now(UTC).isoformat(),
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        rows, _ = _build_ps_rows(registry, store, include_completed=False)
        assert len(rows) == 1
        assert rows[0]["task_id"] == task.id
        assert rows[0]["task"] == task.slug
        assert rows[0]["task"] != ""
        assert not rows[0]["task"].startswith("task ")

    def test_ps_duration_prefers_terminal_task_completed_at_over_dangling_worker(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Completed tasks should not inherit runaway runtime from stale worker rows."""
        import os

        from gza.cli.query import _build_ps_rows
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Completed task with stale worker")
        task.status = "completed"
        task.started_at = datetime(2026, 1, 8, 0, 0, tzinfo=UTC)
        task.completed_at = datetime(2026, 1, 8, 0, 14, 5, tzinfo=UTC)
        store.update(task)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-stale-terminal-runtime",
                pid=os.getpid(),
                task_id=task.id,
                task_slug=None,
                started_at="2026-01-08T00:00:00+00:00",
                status="running",
                completed_at=None,
                log_file=None,
                worktree=None,
            )
        )

        fixed_now = datetime(2026, 1, 10, 12, 0, tzinfo=UTC)

        class _FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                if tz is None:
                    return fixed_now.replace(tzinfo=None)
                return fixed_now.astimezone(tz)

        monkeypatch.setattr(query_cli, "datetime", _FixedDateTime)

        rows, _ = _build_ps_rows(registry, store, include_completed=True)
        assert len(rows) == 1
        assert rows[0]["task_id"] == task.id
        assert rows[0]["ended_at"] == "2026-01-08T00:14:05+00:00"
        assert rows[0]["duration"] == "14m 5s"

    def test_ps_does_not_flag_foreground_task_with_live_pid_as_orphaned(self, tmp_path: Path):
        """A foreground in_progress task (running_pid alive, no worker) is not orphaned.

        Foreground rebase flows set
        running_pid via mark_in_progress without registering a worker. As long as
        the PID is alive, the task should not be classified as orphaned.
        """
        import json

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Foreground rebase task", task_type="rebase")
        store.mark_in_progress(task)  # sets running_pid to the live test process

        result = run_gza("ps", "--json", "--project", str(tmp_path))
        assert result.returncode == 0
        rows = json.loads(result.stdout)
        assert len(rows) == 1
        assert rows[0]["task_id"] == task.id
        assert rows[0]["is_orphaned"] is False
        assert "orphaned" not in rows[0]["flags"]

    def test_ps_includes_db_only_in_progress_and_flags_orphaned(self, tmp_path: Path):
        """PS includes in-progress DB rows even when no worker exists."""
        import json


        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("DB-only in-progress task")
        mark_orphaned(store, task)

        result = run_gza("ps", "--json", "--project", str(tmp_path))
        assert result.returncode == 0
        rows = json.loads(result.stdout)
        assert len(rows) == 1
        assert rows[0]["task_id"] == task.id
        assert rows[0]["source"] == "db"
        assert rows[0]["status"] == "in_progress"
        assert rows[0]["is_orphaned"] is True
        assert "orphaned" in rows[0]["flags"]
        assert rows[0]["started_at"] is not None

    def test_ps_formats_started_timestamp_in_table_output(self, tmp_path: Path):
        """PS table output renders start timestamps in UTC with clear formatting."""
        import os

        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Formatted start time")
        store.mark_in_progress(task)
        task = store.get(task.id)
        assert task is not None
        task.started_at = datetime(2026, 1, 8, 0, 1, tzinfo=UTC)
        store.update(task)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-start-format",
                pid=os.getpid(),
                task_id=task.id,
                task_slug=None,
                started_at="2026-01-08T00:00:00+00:00",
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        result = run_gza("ps", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "2026-01-08 00:01:00 UTC" in result.stdout
        assert "2026-01-08 00:00:00 UTC" not in result.stdout

        registry.remove("w-test-start-format")

    def test_ps_quiet_shows_only_task_ids(self, tmp_path: Path):
        """PS quiet output should include task IDs (not worker IDs)."""
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Task with an associated worker.
        task = store.add("Task with worker")
        store.mark_in_progress(task)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-quiet",
                pid=os.getpid(),
                task_id=task.id,
                task_slug=None,
                started_at=datetime.now(UTC).isoformat(),
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        result = run_gza("ps", "--quiet", "--project", str(tmp_path))
        assert result.returncode == 0
        lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        assert lines == [str(task.id)]

        registry.remove("w-test-quiet")

    def test_ps_flags_stale_and_orphaned_for_stale_worker_in_progress_task(self, tmp_path: Path):
        """PS flags stale worker + orphaned in-progress task in reconciled row."""
        import json

        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Stale worker task")
        mark_orphaned(store, task)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-stale-ps",
                pid=999999,
                task_id=task.id,
                task_slug=None,
                started_at=datetime.now(UTC).isoformat(),
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        result = run_gza("ps", "--json", "--project", str(tmp_path))
        assert result.returncode == 0
        rows = json.loads(result.stdout)
        assert len(rows) == 1
        assert rows[0]["source"] == "both"
        assert rows[0]["is_stale"] is True
        assert rows[0]["is_orphaned"] is True
        assert "stale" in rows[0]["flags"]
        assert "orphaned" in rows[0]["flags"]

        registry.remove("w-test-stale-ps")

    def test_ps_marks_startup_failure_for_failed_worker_without_main_log(self, tmp_path: Path):
        """PS marks startup failures in table and JSON output."""
        import json

        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-startup-ps",
                pid=99999,
                task_id=None,
                task_slug="startup-failed-worker",
                started_at=datetime.now(UTC).isoformat(),
                status="failed",
                log_file=None,
                worktree=None,
                startup_log_file=".gza/workers/w-test-startup-ps-startup.log",
            )
        )

        table_result = run_gza("ps", "--project", str(tmp_path))
        assert table_result.returncode == 0
        assert "failed(startup)" in table_result.stdout

        json_result = run_gza("ps", "--json", "--project", str(tmp_path))
        assert json_result.returncode == 0
        rows = json.loads(json_result.stdout)
        assert len(rows) == 1
        assert rows[0]["startup_failure"] is True
        assert rows[0]["startup_log_file"] == ".gza/workers/w-test-startup-ps-startup.log"

        registry.remove("w-test-startup-ps")

    def test_ps_default_includes_startup_failure_but_filters_other_terminal_rows(self, tmp_path: Path):
        """Default ps keeps startup failures visible while filtering other terminal rows."""
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)

        registry.register(
            WorkerMetadata(
                worker_id="w-test-ps-running",
                pid=99998,
                task_id=None,
                task_slug="running-worker",
                started_at=datetime.now(UTC).isoformat(),
                status="running",
                log_file=None,
                worktree=None,
            )
        )
        registry.register(
            WorkerMetadata(
                worker_id="w-test-ps-startup-failed",
                pid=99997,
                task_id=None,
                task_slug="startup-failed-worker",
                started_at=datetime.now(UTC).isoformat(),
                status="failed",
                log_file=None,
                worktree=None,
                startup_log_file=".gza/workers/w-test-ps-startup-failed-startup.log",
            )
        )
        registry.register(
            WorkerMetadata(
                worker_id="w-test-ps-failed",
                pid=99996,
                task_id=None,
                task_slug="ordinary-failed-worker",
                started_at=datetime.now(UTC).isoformat(),
                status="failed",
                log_file=None,
                worktree=None,
            )
        )
        registry.register(
            WorkerMetadata(
                worker_id="w-test-ps-completed",
                pid=99995,
                task_id=None,
                task_slug="completed-worker",
                started_at=datetime.now(UTC).isoformat(),
                status="completed",
                log_file=None,
                worktree=None,
                exit_code=0,
                completed_at=datetime.now(UTC).isoformat(),
            )
        )

        result = run_gza("ps", "--project", str(tmp_path))
        assert result.returncode == 0
        # Stale running worker (dead PID, no task_id) is pruned automatically
        assert "running-worker" not in result.stdout
        assert "failed(startup)" in result.stdout
        assert "ordinary-failed-worker" not in result.stdout
        assert "completed-worker" not in result.stdout

        registry.remove("w-test-ps-startup-failed")
        registry.remove("w-test-ps-failed")
        registry.remove("w-test-ps-completed")

    def test_ps_all_flag_includes_completed_and_failed_rows(self, tmp_path: Path):
        """ps --all includes ordinary completed/failed rows that default ps filters out."""
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)

        registry.register(
            WorkerMetadata(
                worker_id="w-all-running",
                pid=99998,
                task_id=None,
                task_slug="running-worker",
                started_at=datetime.now(UTC).isoformat(),
                status="running",
                log_file=None,
                worktree=None,
            )
        )
        registry.register(
            WorkerMetadata(
                worker_id="w-all-failed",
                pid=99996,
                task_id=None,
                task_slug="ordinary-failed-worker",
                started_at=datetime.now(UTC).isoformat(),
                status="failed",
                log_file=None,
                worktree=None,
            )
        )
        registry.register(
            WorkerMetadata(
                worker_id="w-all-completed",
                pid=99995,
                task_id=None,
                task_slug="completed-worker",
                started_at=datetime.now(UTC).isoformat(),
                status="completed",
                log_file=None,
                worktree=None,
                exit_code=0,
                completed_at=datetime.now(UTC).isoformat(),
            )
        )

        # Default ps: stale running worker (dead PID, no task_id) is pruned;
        # ordinary completed/failed are filtered out
        result = run_gza("ps", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "running-worker" not in result.stdout
        assert "ordinary-failed-worker" not in result.stdout
        assert "completed-worker" not in result.stdout

        # ps --all: includes completed/failed (running was already pruned)
        result_all = run_gza("ps", "--all", "--project", str(tmp_path))
        assert result_all.returncode == 0
        assert "ordinary-failed-worker" in result_all.stdout
        assert "completed-worker" in result_all.stdout

        registry.remove("w-all-failed")
        registry.remove("w-all-completed")

    def test_ps_all_json_includes_terminal_rows(self, tmp_path: Path):
        """ps --all --json includes completed/failed workers in JSON output."""
        import json as json_lib

        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)

        registry.register(
            WorkerMetadata(
                worker_id="w-json-completed",
                pid=99994,
                task_id=None,
                task_slug="json-completed-worker",
                started_at=datetime.now(UTC).isoformat(),
                status="completed",
                log_file=None,
                worktree=None,
                exit_code=0,
                completed_at=datetime.now(UTC).isoformat(),
            )
        )

        result = run_gza("ps", "--all", "--json", "--project", str(tmp_path))
        assert result.returncode == 0
        data = json_lib.loads(result.stdout)
        slugs = [r["task"] for r in data]
        assert any("json-completed-worker" in s for s in slugs)

        registry.remove("w-json-completed")

    def test_print_ps_output_poll_adopts_first_seen_startup_failure(self, tmp_path: Path, capsys):
        """Poll path keeps startup-failed workers visible on first observation."""
        import argparse

        from gza.cli import _print_ps_output
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-startup-poll",
                pid=99999,
                task_id=None,
                task_slug="startup-failed-worker",
                started_at=datetime.now(UTC).isoformat(),
                status="failed",
                log_file=None,
                worktree=None,
                startup_log_file=".gza/workers/w-test-startup-poll-startup.log",
            )
        )

        args = argparse.Namespace(quiet=False, json=True)
        seen_tasks: dict[str, dict] = {}
        _print_ps_output(args, registry, store, seen_tasks=seen_tasks)

        captured = capsys.readouterr()
        assert '"worker_id": "w-test-startup-poll"' in captured.out
        assert '"status": "failed"' in captured.out
        assert '"startup_failure": true' in captured.out
        assert "w-test-startup-poll" in seen_tasks

        registry.remove("w-test-startup-poll")

    def test_print_ps_output_poll_adopts_recently_ended_terminal_row(self, tmp_path: Path, capsys):
        """Poll path adopts first-seen terminal rows ended within the recent window."""
        import argparse

        from gza.cli import _print_ps_output
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        now = datetime.now(UTC)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-recent-terminal",
                pid=99998,
                task_id=None,
                task_slug="recent-terminal-worker",
                started_at=(now - timedelta(minutes=2)).isoformat(),
                status="completed",
                log_file=None,
                worktree=None,
                completed_at=(now - timedelta(seconds=30)).isoformat(),
            )
        )

        args = argparse.Namespace(quiet=False, json=True)
        seen_tasks: dict[str, dict] = {}
        _print_ps_output(args, registry, store, seen_tasks=seen_tasks, recent_minutes=1)

        captured = capsys.readouterr()
        assert '"worker_id": "w-test-recent-terminal"' in captured.out
        assert '"status": "completed"' in captured.out
        assert "w-test-recent-terminal" in seen_tasks

        registry.remove("w-test-recent-terminal")

    def test_print_ps_output_poll_does_not_adopt_old_terminal_row(self, tmp_path: Path, capsys):
        """Poll path does not adopt terminal rows outside the recent window."""
        import argparse

        from gza.cli import _print_ps_output
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        now = datetime.now(UTC)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-old-terminal",
                pid=99997,
                task_id=None,
                task_slug="old-terminal-worker",
                started_at=(now - timedelta(minutes=10)).isoformat(),
                status="completed",
                log_file=None,
                worktree=None,
                completed_at=(now - timedelta(minutes=5)).isoformat(),
            )
        )

        args = argparse.Namespace(quiet=False, json=True)
        seen_tasks: dict[str, dict] = {}
        _print_ps_output(args, registry, store, seen_tasks=seen_tasks, recent_minutes=1)

        captured = capsys.readouterr()
        assert "No in-progress tasks (use --poll to monitor)" in captured.out
        assert "w-test-old-terminal" not in seen_tasks

        registry.remove("w-test-old-terminal")

    def test_print_ps_output_poll_recent_minutes_zero_disables_recent_terminal_adoption(self, tmp_path: Path, capsys):
        """A 0-minute recent window preserves old behavior for first-seen terminal rows."""
        import argparse

        from gza.cli import _print_ps_output
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        now = datetime.now(UTC)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-zero-window",
                pid=99996,
                task_id=None,
                task_slug="zero-window-worker",
                started_at=(now - timedelta(minutes=2)).isoformat(),
                status="completed",
                log_file=None,
                worktree=None,
                completed_at=(now - timedelta(seconds=15)).isoformat(),
            )
        )

        args = argparse.Namespace(quiet=False, json=True)
        seen_tasks: dict[str, dict] = {}
        _print_ps_output(args, registry, store, seen_tasks=seen_tasks, recent_minutes=0)

        captured = capsys.readouterr()
        assert "No in-progress tasks (use --poll to monitor)" in captured.out
        assert "w-test-zero-window" not in seen_tasks

        registry.remove("w-test-zero-window")

    def test_print_ps_output_poll_respects_custom_recent_minutes_window(self, tmp_path: Path, capsys):
        """Custom recent window widens first-seen terminal adoption in poll mode."""
        import argparse

        from gza.cli import _print_ps_output
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        now = datetime.now(UTC)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-wide-window",
                pid=99995,
                task_id=None,
                task_slug="wide-window-worker",
                started_at=(now - timedelta(minutes=7)).isoformat(),
                status="failed",
                log_file=None,
                worktree=None,
                completed_at=(now - timedelta(minutes=5)).isoformat(),
            )
        )

        args = argparse.Namespace(quiet=False, json=True)
        seen_tasks: dict[str, dict] = {}
        _print_ps_output(args, registry, store, seen_tasks=seen_tasks, recent_minutes=10)

        captured = capsys.readouterr()
        assert '"worker_id": "w-test-wide-window"' in captured.out
        assert '"status": "failed"' in captured.out
        assert "w-test-wide-window" in seen_tasks

        registry.remove("w-test-wide-window")

    def test_ps_handles_missing_started_timestamp(self, tmp_path: Path):
        """PS should gracefully handle invalid/missing start timestamps."""
        import json
        import os as _os

        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-no-start",
                pid=_os.getpid(),  # use real PID so prune doesn't remove it
                task_id=None,
                task_slug="standalone-worker",
                started_at="not-a-timestamp",
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        table_result = run_gza("ps", "--project", str(tmp_path))
        assert table_result.returncode == 0
        assert "standalone-worker" in table_result.stdout
        assert " - " in table_result.stdout

        json_result = run_gza("ps", "--json", "--project", str(tmp_path))
        assert json_result.returncode == 0
        rows = json.loads(json_result.stdout)
        assert len(rows) == 1
        assert rows[0]["worker_id"] == "w-test-no-start"
        assert rows[0]["started"] == "-"
        assert rows[0]["started_at"] is None

        registry.remove("w-test-no-start")

    def test_ps_json_order_stable_when_started_timestamps_missing(self, tmp_path: Path):
        """PS JSON ordering is deterministic when start times are unavailable."""
        import json
        import os as _os

        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)

        # Register in reverse lexical order to assert sort stability by worker_id.
        for worker_id in ["w-test-order-b", "w-test-order-a"]:
            registry.register(
                WorkerMetadata(
                    worker_id=worker_id,
                    pid=_os.getpid(),  # use real PID so prune doesn't remove it
                    task_id=None,
                    task_slug=None,
                    started_at="invalid",
                    status="running",
                    log_file=None,
                    worktree=None,
                )
            )

        result = run_gza("ps", "--json", "--project", str(tmp_path))
        assert result.returncode == 0
        rows = json.loads(result.stdout)
        assert [row["worker_id"] for row in rows] == ["w-test-order-a", "w-test-order-b"]

        registry.remove("w-test-order-a")
        registry.remove("w-test-order-b")

    def test_ps_poll_default_interval(self, tmp_path: Path):
        """--poll without a value uses 5-second default interval."""
        import argparse

        from gza.cli import cmd_ps

        setup_config(tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            quiet=False,
            json=False,
            poll=5,  # const value when --poll given without argument
        )

        call_count = 0
        sleep_calls: list[float] = []

        def fake_sleep(n: float) -> None:
            nonlocal call_count
            sleep_calls.append(n)
            call_count += 1
            if call_count >= 2:
                raise KeyboardInterrupt

        import unittest.mock as mock
        with mock.patch("time.sleep", side_effect=fake_sleep):
            result = cmd_ps(args)

        assert result == 0
        assert all(s == 5 for s in sleep_calls)

    def test_ps_poll_custom_interval(self, tmp_path: Path):
        """--poll N uses the specified interval."""
        import argparse

        from gza.cli import cmd_ps

        setup_config(tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            quiet=False,
            json=False,
            poll=10,
        )

        sleep_calls: list[float] = []

        def fake_sleep(n: float) -> None:
            sleep_calls.append(n)
            raise KeyboardInterrupt

        import unittest.mock as mock
        with mock.patch("time.sleep", side_effect=fake_sleep):
            result = cmd_ps(args)

        assert result == 0
        assert sleep_calls == [10]

    def test_ps_poll_shows_timestamp_header(self, tmp_path: Path, capsys):
        """Poll mode prints the refresh interval and timestamp in the header."""
        import argparse

        from gza.cli import cmd_ps

        setup_config(tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            quiet=False,
            json=False,
            poll=3,
        )

        import unittest.mock as mock
        with mock.patch("time.sleep", side_effect=KeyboardInterrupt):
            result = cmd_ps(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Refreshing every 3s" in captured.out
        assert "last updated:" in captured.out
        assert "Ctrl+C to exit" in captured.out

    def test_ps_no_poll_behaves_as_before(self, tmp_path: Path, capsys):
        """Without --poll the command runs once and exits immediately."""
        import argparse

        from gza.cli import cmd_ps

        setup_config(tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            quiet=False,
            json=False,
            poll=None,
        )

        import unittest.mock as mock
        with mock.patch("time.sleep") as mock_sleep:
            result = cmd_ps(args)

        assert result == 0
        mock_sleep.assert_not_called()

    def test_ps_poll_negative_value_returns_error(self, tmp_path: Path, capsys):
        """Negative --poll value returns exit code 1 with an error message."""
        import argparse

        from gza.cli import cmd_ps

        setup_config(tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            quiet=False,
            json=False,
            poll=-1,
        )

        result = cmd_ps(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "error" in captured.err
        assert "--poll" in captured.err
        assert "-1" in captured.err

    def test_ps_poll_zero_value_returns_error(self, tmp_path: Path, capsys):
        """Zero --poll value returns exit code 1 with an error message."""
        import argparse

        from gza.cli import cmd_ps

        setup_config(tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            quiet=False,
            json=False,
            poll=0,
        )

        result = cmd_ps(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "error" in captured.err

    def test_ps_recent_minutes_negative_value_returns_error(self, tmp_path: Path, capsys):
        """Negative --recent-minutes value returns exit code 1 with an error message."""
        import argparse

        from gza.cli import cmd_ps

        setup_config(tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            quiet=False,
            json=False,
            poll=None,
            recent_minutes=-1,
        )

        result = cmd_ps(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "error" in captured.err
        assert "--recent-minutes" in captured.err
        assert "-1" in captured.err

    def test_ps_recent_minutes_non_poll_has_no_effect(self, tmp_path: Path):
        """Without --poll, recent terminal adoption is still not applied."""
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        now = datetime.now(UTC)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-non-poll-recent",
                pid=99993,
                task_id=None,
                task_slug="non-poll-recent-worker",
                started_at=(now - timedelta(minutes=2)).isoformat(),
                status="completed",
                log_file=None,
                worktree=None,
                completed_at=(now - timedelta(seconds=20)).isoformat(),
            )
        )

        result = run_gza("ps", "--json", "--recent-minutes", "10", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "No in-progress tasks (use --poll to monitor)" in result.stdout
        assert "w-test-non-poll-recent" not in result.stdout

        registry.remove("w-test-non-poll-recent")

    def test_ps_poll_no_ansi_codes_when_not_tty(self, tmp_path: Path, capsys):
        """ANSI escape codes are not emitted when stdout is not a TTY."""
        import argparse

        from gza.cli import cmd_ps

        setup_config(tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            quiet=False,
            json=False,
            poll=5,
        )

        import unittest.mock as mock
        with mock.patch("time.sleep", side_effect=KeyboardInterrupt):
            result = cmd_ps(args)

        assert result == 0
        captured = capsys.readouterr()
        # capsys captures a non-TTY stream, so ANSI codes must be absent
        assert "\033[2J" not in captured.out
        assert "\033[H" not in captured.out

    def test_ps_poll_json_prefers_task_started_at_over_older_worker(self, tmp_path: Path, capsys, monkeypatch):
        """Poll-mode task rows should keep using task timing in JSON snapshots."""
        import argparse
        import os

        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Polled claimed task")
        store.mark_in_progress(task)
        task = store.get(task.id)
        assert task is not None
        task.started_at = datetime(2026, 1, 8, 0, 1, tzinfo=UTC)
        task.running_pid = os.getpid()
        store.update(task)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-poll-task-start-precedence",
                pid=os.getpid(),
                task_id=task.id,
                task_slug=None,
                started_at="2026-01-08T00:00:00+00:00",
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        fixed_now = datetime(2026, 1, 8, 0, 2, 30, tzinfo=UTC)

        class _FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                if tz is None:
                    return fixed_now.replace(tzinfo=None)
                return fixed_now.astimezone(tz)

        monkeypatch.setattr(query_cli, "datetime", _FixedDateTime)

        args = argparse.Namespace(project_dir=tmp_path, quiet=False, json=True, poll=None)
        query_cli._print_ps_output(
            args,
            registry,
            store,
            poll_interval=2,
            seen_tasks={},
            recent_minutes=10,
        )

        captured = capsys.readouterr()
        json_start = captured.out.index("[")
        json_output = captured.out[json_start:]
        rows = json.loads(json_output)
        assert len(rows) == 1
        assert rows[0]["task_id"] == task.id
        assert rows[0]["started_at"] == "2026-01-08T00:01:00+00:00"
        assert rows[0]["duration"] == "1m 30s"

    def test_ps_poll_keeps_completed_tasks_visible(self, tmp_path: Path):
        """Poll mode keeps completed tasks visible so users see transitions.

        Workers that transition to completed remain in the display (via
        seen_tasks) instead of vanishing. The poll loop continues until
        interrupted with Ctrl+C.
        """
        import argparse
        import json
        import os
        import unittest.mock as mock

        from gza.cli import cmd_ps
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)

        worker = WorkerMetadata(
            worker_id="w-test-transition",
            pid=os.getpid(),
            task_id=None,
            task_slug=None,
            started_at=datetime.now(UTC).isoformat(),
            status="running",
            log_file=None,
            worktree=None,
        )
        registry.register(worker)

        sleep_count = 0

        def fake_sleep(n: float) -> None:
            nonlocal sleep_count
            sleep_count += 1
            if sleep_count == 1:
                # Transition worker to completed during the sleep after poll 1.
                worker.status = "completed"
                registry.update(worker)
            elif sleep_count >= 2:
                # Stop after seeing the completed state
                raise KeyboardInterrupt

        args = argparse.Namespace(
            project_dir=tmp_path,
            quiet=False,
            json=True,
            poll=2,
        )

        captured_outputs: list[str] = []
        original_print = print

        def capturing_print(*a, **kw):
            if a:
                captured_outputs.append(str(a[0]))
            original_print(*a, **kw)

        with mock.patch("builtins.print", side_effect=capturing_print):
            with mock.patch("time.sleep", side_effect=fake_sleep):
                result = cmd_ps(args)

        assert result == 0

        # Find JSON outputs (lines that start with '[') and parse them.
        json_outputs = [o for o in captured_outputs if o.startswith("[")]
        assert len(json_outputs) >= 2, f"Expected at least 2 JSON snapshots, got: {json_outputs}"

        first_snapshot = json.loads(json_outputs[0])
        assert len(first_snapshot) == 1
        assert first_snapshot[0]["status"] == "in_progress"

        # Second poll: completed worker remains visible (not filtered out).
        second_snapshot = json.loads(json_outputs[1])
        assert len(second_snapshot) == 1
        assert second_snapshot[0]["status"] == "completed"

        registry.remove("w-test-transition")

    def test_ps_poll_shows_tasks_from_start_to_end(self, tmp_path: Path):
        """Poll mode tracks task lifecycle via DB status alone (workerless tasks).

        Scenario:
        - 3 tasks: pending (#1), in_progress (#2), completed (#3)
        - Poll 1: only #2 (in_progress) is visible
        - Transition #1 to in_progress during sleep
        - Poll 2: #1 and #2 visible (2 in_progress tasks)
        - Transition #2 to completed during sleep
        - Poll 3: #1 (in_progress) and #2 (completed, still visible)
        - Transition #1 to completed during sleep
        - Poll 4: both completed, poll continues until Ctrl+C
        """
        import argparse
        import json
        import unittest.mock as mock

        from gza.cli import cmd_ps

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create 3 tasks with different initial states
        task_pending = store.add("Pending task")
        task_running = store.add("Running task")
        task_completed = store.add("Already completed task")

        store.mark_in_progress(task_running)
        store.mark_in_progress(task_completed)
        store.mark_completed(task_completed)

        # Workers dir must exist for WorkerRegistry even though we don't use workers
        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)

        sleep_count = 0

        def fake_sleep(n: float) -> None:
            nonlocal sleep_count
            sleep_count += 1
            if sleep_count == 1:
                # After poll 1: start the pending task
                store.mark_in_progress(task_pending)
            elif sleep_count == 2:
                # After poll 2: complete the originally running task
                store.mark_completed(task_running)
            elif sleep_count == 3:
                # After poll 3: complete the other task too
                store.mark_completed(task_pending)
            elif sleep_count >= 4:
                # All transitions done — stop the poll loop
                raise KeyboardInterrupt

        args = argparse.Namespace(
            project_dir=tmp_path,
            quiet=False,
            json=True,
            poll=2,
        )

        captured_outputs: list[str] = []
        original_print = print

        def capturing_print(*a, **kw):
            if a:
                captured_outputs.append(str(a[0]))
            original_print(*a, **kw)

        with mock.patch("builtins.print", side_effect=capturing_print):
            with mock.patch("time.sleep", side_effect=fake_sleep):
                result = cmd_ps(args)

        assert result == 0

        json_outputs = [o for o in captured_outputs if o.startswith("[")]
        assert len(json_outputs) >= 4, f"Expected at least 4 JSON snapshots, got {len(json_outputs)}: {json_outputs}"

        # Poll 1: only the in_progress task
        snap1 = json.loads(json_outputs[0])
        assert len(snap1) == 1, f"Poll 1: expected 1 task, got {len(snap1)}: {snap1}"
        assert snap1[0]["status"] == "in_progress"

        # Poll 2: 2 in_progress tasks (original + newly started)
        snap2 = json.loads(json_outputs[1])
        assert len(snap2) == 2, f"Poll 2: expected 2 tasks, got {len(snap2)}: {snap2}"
        statuses2 = {r["status"] for r in snap2}
        assert statuses2 == {"in_progress"}, f"Poll 2: expected all in_progress, got {statuses2}"

        # Poll 3: 1 in_progress + 1 completed (completed stays visible)
        snap3 = json.loads(json_outputs[2])
        assert len(snap3) == 2, f"Poll 3: expected 2 tasks, got {len(snap3)}: {snap3}"
        statuses3 = sorted(r["status"] for r in snap3)
        assert statuses3 == ["completed", "in_progress"], f"Poll 3: expected completed+in_progress, got {statuses3}"

        # Poll 4: both completed, poll continues until interrupted
        snap4 = json.loads(json_outputs[3])
        assert len(snap4) == 2, f"Poll 4: expected 2 tasks, got {len(snap4)}: {snap4}"
        statuses4 = {r["status"] for r in snap4}
        assert statuses4 == {"completed"}, f"Poll 4: expected all completed, got {statuses4}"

    def test_ps_poll_detects_completion_with_workers(self, tmp_path: Path):
        """Poll mode detects task completion even when workers are present.

        When a task has a worker registered, it appears in live_rows via the
        worker loop (not just get_in_progress). The poll must still detect
        when the DB task status transitions to completed, even though the
        task never "vanishes" from live_rows.

        Scenario:
        - 2 tasks running with workers (task_id set on worker)
        - Poll 1: both show as running
        - Both tasks complete in DB during sleep
        - Poll 2: both show as completed (not running)
        """
        import argparse
        import json
        import os
        import unittest.mock as mock

        from gza.cli import cmd_ps
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create 2 in_progress tasks
        task1 = store.add("Internal task 1", task_type="internal")
        task2 = store.add("Internal task 2", task_type="internal")
        store.mark_in_progress(task1)
        store.mark_in_progress(task2)

        # Register workers for both tasks (simulates gza work running them)
        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)

        w1 = WorkerMetadata(
            worker_id="w-learn-1",
            pid=os.getpid(),
            task_id=task1.id,
            task_slug=None,
            started_at=datetime.now(UTC).isoformat(),
            status="running",
            log_file=None,
            worktree=None,
        )
        w2 = WorkerMetadata(
            worker_id="w-learn-2",
            pid=os.getpid(),
            task_id=task2.id,
            task_slug=None,
            started_at=datetime.now(UTC).isoformat(),
            status="running",
            log_file=None,
            worktree=None,
        )
        registry.register(w1)
        registry.register(w2)

        sleep_count = 0

        def fake_sleep(n: float) -> None:
            nonlocal sleep_count
            sleep_count += 1
            if sleep_count == 1:
                # After poll 1: both tasks complete in DB
                store.mark_completed(task1)
                store.mark_completed(task2)
            elif sleep_count >= 2:
                raise KeyboardInterrupt

        args = argparse.Namespace(
            project_dir=tmp_path,
            quiet=False,
            json=True,
            poll=2,
        )

        captured_outputs: list[str] = []
        original_print = print

        def capturing_print(*a, **kw):
            if a:
                captured_outputs.append(str(a[0]))
            original_print(*a, **kw)

        with mock.patch("builtins.print", side_effect=capturing_print):
            with mock.patch("time.sleep", side_effect=fake_sleep):
                result = cmd_ps(args)

        assert result == 0

        json_outputs = [o for o in captured_outputs if o.startswith("[")]
        assert len(json_outputs) >= 2, f"Expected at least 2 JSON snapshots, got {len(json_outputs)}"

        # Poll 1: both tasks in_progress
        snap1 = json.loads(json_outputs[0])
        assert len(snap1) == 2, f"Poll 1: expected 2 tasks, got {len(snap1)}: {snap1}"
        statuses1 = {r["status"] for r in snap1}
        assert statuses1 == {"in_progress"}, f"Poll 1: expected all in_progress, got {statuses1}"

        # Poll 2: both tasks completed (DB is source of truth)
        snap2 = json.loads(json_outputs[1])
        assert len(snap2) == 2, f"Poll 2: expected 2 tasks, got {len(snap2)}: {snap2}"
        statuses2 = {r["status"] for r in snap2}
        assert statuses2 == {"completed"}, f"Poll 2: expected all completed, got {statuses2}"

        # Cleanup
        registry.remove("w-learn-1")
        registry.remove("w-learn-2")

    def test_ps_poll_shows_steps_for_completed_task(self, tmp_path: Path):
        """STEPS column shows num_steps_computed for a completed task in poll mode."""
        import argparse
        import json
        import os
        import unittest.mock as mock

        from gza.cli import cmd_ps
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Task with steps")
        store.mark_in_progress(task)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-steps-poll",
                pid=os.getpid(),
                task_id=task.id,
                task_slug=None,
                started_at=datetime.now(UTC).isoformat(),
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        sleep_count = 0

        def fake_sleep(n: float) -> None:
            nonlocal sleep_count
            sleep_count += 1
            if sleep_count == 1:
                # Complete the task with num_steps_computed
                task.num_steps_computed = 7
                store.mark_completed(task)
                w = registry.get("w-test-steps-poll")
                w.status = "completed"
                registry.update(w)
            else:
                raise KeyboardInterrupt

        args = argparse.Namespace(
            project_dir=tmp_path,
            quiet=False,
            json=True,
            poll=2,
        )

        captured_outputs: list[str] = []
        original_print = print

        def capturing_print(*a, **kw):
            if a:
                captured_outputs.append(str(a[0]))
            original_print(*a, **kw)

        with mock.patch("builtins.print", side_effect=capturing_print):
            with mock.patch("time.sleep", side_effect=fake_sleep):
                result = cmd_ps(args)

        assert result == 0

        json_outputs = [o for o in captured_outputs if o.startswith("[")]
        assert len(json_outputs) >= 2, f"Expected at least 2 snapshots, got {len(json_outputs)}"

        # Second poll: task is completed and should show num_steps_computed
        snap2 = json.loads(json_outputs[1])
        assert len(snap2) == 1
        assert snap2[0]["status"] == "completed"
        assert snap2[0]["steps"] == "7"

        registry.remove("w-test-steps-poll")

    def test_ps_steps_column_uses_live_count_for_in_progress_task(self, tmp_path: Path):
        """STEPS column shows live DB row count for an in-progress task."""
        import json
        import os

        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("In-progress task with live steps")
        store.mark_in_progress(task)
        assert task.id is not None

        # Emit 3 run_steps rows for this task.
        for i in range(3):
            store.emit_step(task.id, f"Step {i + 1}", provider="claude")

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-steps-live",
                pid=os.getpid(),
                task_id=task.id,
                task_slug=None,
                started_at=datetime.now(UTC).isoformat(),
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        result = run_gza("ps", "--json", "--project", str(tmp_path))
        assert result.returncode == 0
        rows = json.loads(result.stdout)
        assert len(rows) == 1
        assert rows[0]["steps"] == "3"

        registry.remove("w-test-steps-live")

    def test_ps_query_only_missing_run_steps_project_id_warns_without_traceback(self, tmp_path: Path):
        """Frozen ps snapshots should degrade damaged run_steps columns behind a warning."""
        import json
        import os
        import sqlite3

        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("In-progress task with damaged run_steps")
        store.mark_in_progress(task)
        assert task.id is not None

        for i in range(2):
            store.emit_step(task.id, f"Step {i + 1}", provider="claude")

        db_path = tmp_path / ".gza" / "gza.db"
        conn = sqlite3.connect(db_path)
        conn.execute("ALTER TABLE run_steps RENAME TO run_steps_old")
        conn.execute(
            """
            CREATE TABLE run_steps AS
            SELECT
                id,
                run_id,
                step_index,
                step_id,
                provider,
                message_role,
                message_text,
                started_at,
                completed_at,
                outcome,
                summary,
                legacy_turn_id,
                legacy_event_id
            FROM run_steps_old
            """
        )
        conn.execute("DROP TABLE run_steps_old")
        conn.commit()
        conn.close()

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-steps-damaged",
                pid=os.getpid(),
                task_id=task.id,
                task_slug=None,
                started_at=datetime.now(UTC).isoformat(),
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        original_mode = db_path.stat().st_mode
        os.chmod(db_path, 0o444)
        try:
            result = run_gza("ps", "--json", "--project", str(tmp_path))
        finally:
            os.chmod(db_path, original_mode)
            registry.remove("w-test-steps-damaged")

        assert result.returncode == 0
        rows = json.loads(result.stdout)
        assert len(rows) == 1
        assert rows[0]["steps"] == "-"
        assert "Warning: Query-only DB open detected missing required column run_steps.project_id" in result.stderr
        assert "Traceback" not in result.stdout
        assert "Traceback" not in result.stderr


class TestDeleteCommand:
    """Tests for 'gza delete' command."""

    def test_delete_with_force(self, tmp_path: Path):
        """Delete command with --force removes task without confirmation."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Task to delete", "status": "pending"},
        ])

        result = run_gza("delete", "testproject-1", "--force", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Deleted task" in result.stdout

        # Verify task was deleted
        result = run_gza("next", "--project", str(tmp_path))
        assert "No pending tasks" in result.stdout

    def test_delete_nonexistent_task(self, tmp_path: Path):
        """Delete command handles nonexistent task."""
        setup_db_with_tasks(tmp_path, [])

        result = run_gza("delete", "testproject-999999", "--force", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_delete_with_yes_flag(self, tmp_path: Path):
        """Delete command with --yes removes task without confirmation."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Task to delete", "status": "pending"},
        ])

        result = run_gza("delete", "testproject-1", "--yes", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Deleted task" in result.stdout

        # Verify task was deleted
        result = run_gza("next", "--project", str(tmp_path))
        assert "No pending tasks" in result.stdout

    def test_delete_with_y_flag(self, tmp_path: Path):
        """Delete command with -y removes task without confirmation."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Task to delete", "status": "pending"},
        ])

        result = run_gza("delete", "testproject-1", "-y", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Deleted task" in result.stdout

        # Verify task was deleted
        result = run_gza("next", "--project", str(tmp_path))
        assert "No pending tasks" in result.stdout


class TestUnmergedReviewStatus:
    """Tests for review status display in 'gza unmerged' command."""

    _REAL_GIT_TESTS = {
        "test_unmerged_shows_deleted_branch_if_merge_status_unmerged",
        "test_unmerged_keeps_deleted_local_branch_visible_when_remote_feature_still_exists",
        "test_unmerged_migrates_deleted_local_branch_with_remote_survivor_via_shared_reconcile",
        "test_unmerged_backfills_merge_status",
        "test_unmerged_marks_stale_merged_task_as_merged",
        "test_unmerged_uses_existing_origin_default_branch_without_fetch_by_default",
        "test_unmerged_fetch_flag_fetches_origin_default_branch_before_canonical_reconcile",
        "test_unmerged_update_refreshes_diff_stats_for_live_branch",
        "test_unmerged_into_current_uses_current_branch",
        "test_unmerged_into_current_keeps_deleted_branch_visible_when_not_merged_into_current",
        "test_unmerged_target_uses_specified_branch",
        "test_unmerged_target_keeps_deleted_branch_visible_when_not_merged_into_target",
        "test_unmerged_live_target_does_not_persist_reconciliation",
        "test_unmerged_live_target_excludes_same_branch_fix_descendants",
    }

    @pytest.fixture(autouse=True)
    def _use_fast_unmerged_env(self, request, monkeypatch):
        test_name = getattr(request.node, "originalname", None) or request.node.name
        if test_name in self._REAL_GIT_TESTS:
            return

        fake_git = _FastUnmergedGit()
        monkeypatch.setitem(globals(), "setup_unmerged_env", _setup_unmerged_env_fast)
        monkeypatch.setattr(query_cli, "Git", lambda _project_dir: fake_git)
        monkeypatch.setattr(query_cli, "GitHub", _UnavailableGitHub)

    @pytest.mark.parametrize(
        "review_output, expected_text",
        [
            ("# Review\n\nCode looks good!\n\n**Verdict: APPROVED**", "✓ approved"),
            ("# Review\n\nShip now; track follow-ups.\n\nVerdict: APPROVED_WITH_FOLLOWUPS", "↺ approved with follow-ups"),
            ("# Review\n\nNeeds some fixes.\n\nVerdict: CHANGES_REQUESTED", "⚠ changes requested"),
            ("# Review\n\nThis requires team discussion.\n\n**Verdict: NEEDS_DISCUSSION**", "💬 needs discussion"),
        ],
        ids=["approved", "approved_with_followups", "changes_requested", "needs_discussion"],
    )
    def test_unmerged_shows_review_verdict(self, tmp_path: Path, review_output, expected_text):
        """Unmerged output shows the correct review verdict."""
        store, task, git = setup_unmerged_env(tmp_path)

        review = store.add("Review implementation", task_type="review")
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.depends_on = task.id
        review.slug = "20260212-review-implementation"
        review.output_content = review_output
        store.update(review)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert expected_text in result.stdout

    def test_unmerged_appends_review_score_when_present(self, tmp_path: Path):
        """Unmerged verdict badge should append stored review score."""
        store, task, git = setup_unmerged_env(tmp_path)

        review = store.add("Review implementation", task_type="review")
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.depends_on = task.id
        review.output_content = "Verdict: CHANGES_REQUESTED"
        review.review_score = 34
        store.update(review)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "⚠ changes requested (34)" in result.stdout

    def test_unmerged_current_format_review_displays_stored_score(self, tmp_path: Path):
        """Unmerged should display persisted score for current review template format."""
        store, task, git = setup_unmerged_env(tmp_path)

        review = store.add("Review implementation", task_type="review")
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.depends_on = task.id
        review.output_content = (
            "## Summary\n\n"
            "- No - Missing edge case coverage\n\n"
            "## Blockers\n\n"
            "### B1 Missing guard\n"
            "Required fix: add guard for empty input\n"
            "Required tests: add empty-input regression\n\n"
            "## Follow-Ups\n\n"
            "### F1 Improve docs\n"
            "Recommended follow-up: document empty input behavior\n"
            "Recommended tests: docs snippet check\n\n"
            "## Verdict\n\n"
            "Verdict: CHANGES_REQUESTED\n"
        )
        review.review_score = 67
        store.update(review)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "⚠ changes requested (67)" in result.stdout

    def test_unmerged_without_review_shows_no_status(self, tmp_path: Path):
        """Unmerged output shows no review status when no review exists."""
        store, task, git = setup_unmerged_env(tmp_path)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "review: no review" in result.stdout
        assert "approved" not in result.stdout
        assert "changes requested" not in result.stdout
        assert "needs discussion" not in result.stdout

    def test_unmerged_failed_reviews_do_not_count_as_reviewed(self, tmp_path: Path):
        """Failed reviews with completed_at set should not mark an impl as reviewed."""
        store, task, git = setup_unmerged_env(tmp_path)

        failed_review_1 = store.add("Failed review 1", task_type="review")
        failed_review_1.status = "failed"
        failed_review_1.completed_at = datetime.now(UTC)
        failed_review_1.depends_on = task.id
        failed_review_1.output_content = "Verdict: APPROVED"
        store.update(failed_review_1)

        failed_review_2 = store.add("Failed review 2", task_type="review")
        failed_review_2.status = "failed"
        failed_review_2.completed_at = datetime.now(UTC)
        failed_review_2.depends_on = task.id
        failed_review_2.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(failed_review_2)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "review: no review" in result.stdout
        assert "review: reviewed" not in result.stdout
        assert "✓ approved" not in result.stdout
        assert "⚠ changes requested" not in result.stdout

    def test_unmerged_ignores_failed_review_when_completed_review_exists(self, tmp_path: Path):
        """Only completed reviews should drive unmerged review classification and verdict."""
        store, task, git = setup_unmerged_env(tmp_path)

        completed_review = store.add("Completed review", task_type="review")
        completed_review.status = "completed"
        completed_review.completed_at = datetime.now(UTC)
        completed_review.depends_on = task.id
        completed_review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(completed_review)

        failed_review = store.add("Failed review", task_type="review")
        failed_review.status = "failed"
        failed_review.completed_at = datetime.now(UTC)
        failed_review.depends_on = task.id
        failed_review.output_content = "Verdict: APPROVED"
        store.update(failed_review)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "review: reviewed" in result.stdout
        assert "⚠ changes requested" in result.stdout
        assert "✓ approved" not in result.stdout

    def test_unmerged_uses_most_recent_review(self, tmp_path: Path):
        """Unmerged output shows status from most recent review."""
        import time

        store, task, git = setup_unmerged_env(tmp_path)

        # Create first review (changes requested)
        review1 = store.add("First review", task_type="review")
        review1.status = "completed"
        review1.completed_at = datetime.now(UTC)
        review1.depends_on = task.id
        review1.slug = "20260212-first-review"
        review1.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(review1)

        # Wait a bit to ensure different timestamps
        time.sleep(0.1)

        # Create second review (approved)
        review2 = store.add("Second review", task_type="review")
        review2.status = "completed"
        review2.completed_at = datetime.now(UTC)
        review2.depends_on = task.id
        review2.slug = "20260212-second-review"
        review2.output_content = "**Verdict: APPROVED**"
        store.update(review2)

        # Run unmerged command - should show approved (most recent)
        with patch("gza.cli.Git", return_value=_mock_unmerged_git()):
            result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "✓ approved" in result.stdout

    def test_unmerged_uses_older_verdict_when_latest_review_has_no_output(self, tmp_path: Path):
        """Unmerged scans newest-to-oldest and uses first parseable review verdict."""
        import time

        store, task, git = setup_unmerged_env(tmp_path)

        older_review = store.add("Older review", task_type="review")
        older_review.status = "completed"
        older_review.completed_at = datetime.now(UTC)
        older_review.depends_on = task.id
        older_review.slug = "20260212-older-review"
        older_review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(older_review)

        time.sleep(0.01)

        latest_review = store.add("Latest review", task_type="review")
        latest_review.status = "completed"
        latest_review.completed_at = datetime.now(UTC)
        latest_review.depends_on = task.id
        latest_review.slug = "20260212-latest-review"
        latest_review.output_content = None
        latest_review.report_file = None
        store.update(latest_review)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "⚠ changes requested" in result.stdout

    def test_unmerged_does_not_use_older_stale_verdict_when_latest_review_has_no_output(self, tmp_path: Path):
        """Staleness via review_cleared_at still suppresses older verdicts."""
        import time

        store, task, git = setup_unmerged_env(tmp_path)

        older_review = store.add("Older review", task_type="review")
        older_review.status = "completed"
        older_review.completed_at = datetime.now(UTC)
        older_review.depends_on = task.id
        older_review.slug = "20260212-older-review"
        older_review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(older_review)

        time.sleep(0.01)
        assert task.id is not None
        store.clear_review_state(task.id)

        time.sleep(0.01)
        latest_review = store.add("Latest review", task_type="review")
        latest_review.status = "completed"
        latest_review.completed_at = datetime.now(UTC)
        latest_review.depends_on = task.id
        latest_review.slug = "20260212-latest-review"
        latest_review.output_content = None
        latest_review.report_file = None
        store.update(latest_review)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "⚠ changes requested" not in result.stdout
        assert "review: reviewed" in result.stdout

    def test_unmerged_falls_back_to_unlinked_review_slug_match(self, tmp_path: Path):
        """Unmerged should infer review status from unlinked 'review <slug>' tasks."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Simplify mixer",
            task_id="20260225-simplify-mixer-by-removing-the-people-strategy",
        )

        # Simulate a retry-created review that lost depends_on but kept review slug.
        review = store.add("review simplify-mixer-by-removing-the-people-strategy", task_type="review")
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.slug = "20260225-review-simplify-mixer-by-removing-the-people-strategy-2"
        review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(review)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "⚠ changes requested" in result.stdout

    def test_unmerged_marks_review_stale_after_improve_clears_it(self, tmp_path: Path):
        """After improve clears review state, unmerged marks review as stale."""
        import time

        store, task, git = setup_unmerged_env(tmp_path)

        # Create review task with changes requested
        review = store.add("Review implementation", task_type="review")
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.depends_on = task.id
        review.slug = "20260212-review-implementation"
        review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(review)

        # Before improve: should show changes requested
        with patch("gza.cli.Git", return_value=_mock_unmerged_git()):
            result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "⚠ changes requested" in result.stdout

        # Simulate improve task completing (clear review state)
        time.sleep(0.01)
        assert task.id is not None
        store.clear_review_state(task.id)

        # After improve: status should explicitly show stale review
        with patch("gza.cli.Git", return_value=_mock_unmerged_git()):
            result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "review stale" in result.stdout
        assert "last review" in result.stdout

    def test_unmerged_marks_review_stale_after_completed_fix(self, tmp_path: Path):
        """A completed same-branch fix after review marks review stale in unmerged output."""
        store, task, git = setup_unmerged_env(tmp_path)

        review = store.add("Review implementation", task_type="review")
        review.status = "completed"
        review.completed_at = datetime(2026, 2, 12, 11, 0, tzinfo=UTC)
        review.depends_on = task.id
        review.output_content = "Verdict: APPROVED"
        store.update(review)

        fix_task = store.add("Fix follow-up", task_type="fix")
        fix_task.status = "completed"
        fix_task.completed_at = datetime(2026, 2, 12, 12, 0, tzinfo=UTC)
        fix_task.based_on = task.id
        fix_task.depends_on = review.id
        fix_task.branch = "feature/test"
        fix_task.same_branch = True
        store.update(fix_task)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        normalized = " ".join(result.stdout.split())
        assert "review: review stale" in normalized
        assert f"latest fix {fix_task.id}" in normalized
        assert "review: reviewed [✓ approved]" not in normalized

    def test_unmerged_handles_mixed_naive_and_aware_review_timestamps(self, tmp_path: Path):
        """Legacy naive review timestamps should not crash unmerged ordering or verdict selection."""
        import sqlite3

        store, task, git = setup_unmerged_env(tmp_path)

        legacy_review = store.add("Legacy review", task_type="review")
        legacy_review.status = "completed"
        legacy_review.completed_at = datetime(2026, 2, 12, 11, 0, tzinfo=UTC)
        legacy_review.depends_on = task.id
        legacy_review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(legacy_review)
        assert legacy_review.id is not None

        db_path = tmp_path / ".gza" / "gza.db"
        conn = sqlite3.connect(db_path)
        conn.execute(
            "UPDATE tasks SET completed_at = ? WHERE id = ?",
            ("2026-02-12T11:00:00", legacy_review.id),
        )
        conn.commit()
        conn.close()

        current_review = store.add("Current review", task_type="review")
        current_review.status = "completed"
        current_review.completed_at = datetime(2026, 2, 12, 12, 0, tzinfo=UTC)
        current_review.depends_on = task.id
        current_review.output_content = "Verdict: APPROVED"
        store.update(current_review)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0

        normalized = " ".join(result.stdout.split())
        assert "review: reviewed [✓ approved]" in normalized
        assert "⚠ changes requested" not in normalized

    def test_unmerged_uses_review_attached_to_same_branch_improve(self, tmp_path: Path):
        """A same-branch improve review should drive the branch review status."""
        store, task, git = setup_unmerged_env(tmp_path)

        improve = store.add("Improve implementation", task_type="improve")
        improve.status = "completed"
        improve.completed_at = datetime(2026, 2, 12, 12, 0, tzinfo=UTC)
        improve.based_on = task.id
        improve.branch = "feature/test"
        improve.same_branch = True
        store.update(improve)

        review = store.add("Review improve", task_type="review")
        review.status = "completed"
        review.completed_at = datetime(2026, 2, 12, 13, 0, tzinfo=UTC)
        review.depends_on = improve.id
        review.output_content = "Verdict: APPROVED"
        store.update(review)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0

        normalized = " ".join(result.stdout.split())
        assert "review: reviewed [✓ approved]" in normalized
        assert review.id in normalized
        assert "review: no review" not in normalized
        assert "review stale" not in normalized

    def test_unmerged_uses_review_attached_to_same_branch_fix(self, tmp_path: Path):
        """A same-branch fix review should drive the branch review status."""
        store, task, git = setup_unmerged_env(tmp_path)

        fix_task = store.add("Fix implementation", task_type="fix")
        fix_task.status = "completed"
        fix_task.completed_at = datetime(2026, 2, 12, 12, 0, tzinfo=UTC)
        fix_task.based_on = task.id
        fix_task.branch = "feature/test"
        fix_task.same_branch = True
        store.update(fix_task)

        review = store.add("Review fix", task_type="review")
        review.status = "completed"
        review.completed_at = datetime(2026, 2, 12, 13, 0, tzinfo=UTC)
        review.depends_on = fix_task.id
        review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(review)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0

        normalized = " ".join(result.stdout.split())
        assert "review: reviewed [⚠ changes requested]" in normalized
        assert review.id in normalized
        assert "review: no review" not in normalized
        assert "review stale" not in normalized

    def test_unmerged_stale_detail_from_review_cleared_state_is_truthful(self, tmp_path: Path):
        """review_cleared_at stale state should not claim an improve task exists."""
        store, task, git = setup_unmerged_env(tmp_path)

        review = store.add("Review implementation", task_type="review")
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.depends_on = task.id
        review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(review)

        assert task.id is not None
        store.clear_review_state(task.id)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        normalized = " ".join(result.stdout.split())
        assert "review: review stale" in normalized
        assert "latest improve" not in normalized
        assert "review state cleared after last review" in normalized
    def test_unmerged_shows_new_review_status_after_improve_and_re_review(self, tmp_path: Path):
        """After improve clears review state, a newer review's verdict is shown."""
        import time

        store, task, git = setup_unmerged_env(tmp_path)

        # Create first review (changes requested)
        review1 = store.add("Review", task_type="review")
        review1.status = "completed"
        review1.completed_at = datetime.now(UTC)
        review1.depends_on = task.id
        review1.slug = "20260212-review"
        review1.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(review1)

        # Improve task runs, clearing the review state
        time.sleep(0.01)
        assert task.id is not None
        store.clear_review_state(task.id)

        # A new review runs after the improve, resulting in approved
        time.sleep(0.01)
        review2 = store.add("Second review", task_type="review")
        review2.status = "completed"
        review2.completed_at = datetime.now(UTC)
        review2.depends_on = task.id
        review2.slug = "20260212-second-review"
        review2.output_content = "**Verdict: APPROVED**"
        store.update(review2)

        # The new review's verdict should be shown (it's newer than review_cleared_at)
        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "reviewed" in result.stdout
        assert "✓ approved" in result.stdout
        assert "⚠ changes requested" not in result.stdout

    def test_unmerged_shows_lineage_for_review_improve_chain(self, tmp_path: Path):
        """Unmerged output includes related review/improve lineage for implementation."""
        import time

        store, impl, git = setup_unmerged_env(tmp_path)

        review = store.add("Review", task_type="review")
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.depends_on = impl.id
        review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(review)

        time.sleep(0.01)
        improve = store.add("Address review feedback", task_type="improve")
        improve.status = "completed"
        improve.completed_at = datetime.now(UTC)
        improve.based_on = impl.id
        improve.depends_on = review.id
        improve.branch = "feature/test"
        improve.same_branch = True
        store.update(improve)

        # Simulate improve completion clearing the review state.
        assert impl.id is not None
        store.clear_review_state(impl.id)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "lineage:" in result.stdout
        assert f"{impl.id}" in result.stdout
        assert f"{review.id}" in result.stdout
        assert f"{improve.id}" in result.stdout
        assert "[implement]" in result.stdout
        assert "[review]" in result.stdout
        assert "[improve]" in result.stdout
        assert "review stale" in result.stdout

    def test_unmerged_lineage_shows_full_canonical_tree_and_annotations(self, tmp_path: Path):
        """Unmerged lineage uses the canonical tree and keeps node annotations."""
        store, impl, git = setup_unmerged_env(tmp_path)

        review = store.add("Review", task_type="review")
        review.status = "completed"
        review.completed_at = datetime(2026, 2, 12, 11, 0, tzinfo=UTC)
        review.depends_on = impl.id
        review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(review)

        improve = store.add("Improve", task_type="improve")
        improve.status = "completed"
        improve.completed_at = datetime(2026, 2, 12, 12, 0, tzinfo=UTC)
        improve.based_on = impl.id
        improve.depends_on = review.id
        improve.branch = "feature/test"
        improve.same_branch = True
        store.update(improve)

        downstream_impl = store.add("Downstream implement noise", task_type="implement")
        downstream_impl.status = "completed"
        downstream_impl.completed_at = datetime(2026, 2, 12, 13, 0, tzinfo=UTC)
        downstream_impl.based_on = impl.id
        downstream_impl.branch = "feature/test"
        downstream_impl.same_branch = True
        store.update(downstream_impl)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "lineage:" in result.stdout
        assert f"{impl.id}" in result.stdout
        assert f"{review.id}" in result.stdout
        assert f"{improve.id}" in result.stdout
        assert f"{downstream_impl.id}" in result.stdout
        assert "| completed |" in result.stdout
        assert "changes_requested" in result.stdout
        # The "← latest" annotation may wrap across lines at narrow terminal widths
        assert "latest" in " ".join(result.stdout.split())

    def test_unmerged_keeps_owner_identity_while_using_latest_branch_review_summary(self, tmp_path: Path):
        """Unmerged keeps the branch owner row while summarizing the latest branch review state."""
        store, root_impl, git = setup_unmerged_env(tmp_path)

        retry_impl = store.add("Retry implementation", task_type="implement")
        retry_impl.status = "completed"
        retry_impl.completed_at = datetime(2026, 2, 12, 11, 0, tzinfo=UTC)
        retry_impl.based_on = root_impl.id
        retry_impl.branch = "feature/test"
        retry_impl.same_branch = True
        retry_impl.has_commits = True
        retry_impl.merge_status = "unmerged"
        store.update(retry_impl)

        review = store.add("Review retry", task_type="review")
        review.status = "completed"
        review.completed_at = datetime(2026, 2, 12, 12, 0, tzinfo=UTC)
        review.depends_on = retry_impl.id
        review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(review)

        improve = store.add("Improve retry", task_type="improve")
        improve.status = "failed"
        improve.completed_at = datetime(2026, 2, 12, 13, 0, tzinfo=UTC)
        improve.based_on = retry_impl.id
        improve.depends_on = review.id
        improve.branch = "feature/test"
        improve.same_branch = True
        store.update(improve)

        sibling_impl = store.add("Sibling retry", task_type="implement")
        sibling_impl.status = "completed"
        sibling_impl.completed_at = datetime(2026, 2, 12, 14, 0, tzinfo=UTC)
        sibling_impl.based_on = root_impl.id
        sibling_impl.branch = "feature/test"
        sibling_impl.same_branch = True
        sibling_impl.has_commits = True
        sibling_impl.merge_status = "unmerged"
        store.update(sibling_impl)

        sibling_review = store.add("Review sibling retry", task_type="review")
        sibling_review.status = "completed"
        sibling_review.completed_at = datetime(2026, 2, 12, 15, 0, tzinfo=UTC)
        sibling_review.depends_on = sibling_impl.id
        sibling_review.output_content = "Verdict: APPROVED"
        store.update(sibling_review)

        unmerged_result = run_gza("unmerged", "--project", str(tmp_path))
        assert unmerged_result.returncode == 0

        unmerged_output = " ".join(unmerged_result.stdout.split())
        assert f"⚡ {root_impl.id}" in unmerged_output
        assert "review: reviewed [✓ approved]" in unmerged_output
        assert root_impl.id in unmerged_output
        assert retry_impl.id in unmerged_output
        assert sibling_impl.id in unmerged_output
        assert sibling_review.id in unmerged_output

    def test_unmerged_lineage_matches_lineage_command_root_for_retry_chain(self, tmp_path: Path):
        """Unmerged lineage keeps the same canonical root as `gza lineage` for retried implementations."""
        store, root_impl, git = setup_unmerged_env(tmp_path)

        retry_impl = store.add("Retry implementation", task_type="implement")
        retry_impl.status = "completed"
        retry_impl.completed_at = datetime(2026, 2, 12, 11, 0, tzinfo=UTC)
        retry_impl.based_on = root_impl.id
        retry_impl.branch = "feature/test"
        retry_impl.same_branch = True
        retry_impl.has_commits = True
        retry_impl.merge_status = "unmerged"
        store.update(retry_impl)

        review = store.add("Review retry", task_type="review")
        review.status = "completed"
        review.completed_at = datetime(2026, 2, 12, 12, 0, tzinfo=UTC)
        review.depends_on = retry_impl.id
        review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(review)

        improve = store.add("Improve retry", task_type="improve")
        improve.status = "failed"
        improve.completed_at = datetime(2026, 2, 12, 13, 0, tzinfo=UTC)
        improve.based_on = retry_impl.id
        improve.depends_on = review.id
        improve.branch = "feature/test"
        improve.same_branch = True
        store.update(improve)

        sibling_impl = store.add("Sibling retry", task_type="implement")
        sibling_impl.status = "completed"
        sibling_impl.completed_at = datetime(2026, 2, 12, 14, 0, tzinfo=UTC)
        sibling_impl.based_on = root_impl.id
        sibling_impl.branch = "feature/test"
        sibling_impl.same_branch = True
        store.update(sibling_impl)

        lineage_result = run_gza("lineage", retry_impl.id, "--project", str(tmp_path))
        assert lineage_result.returncode == 0

        unmerged_result = run_gza("unmerged", "--project", str(tmp_path))
        assert unmerged_result.returncode == 0

        lineage_output = " ".join(lineage_result.stdout.split())
        unmerged_output = " ".join(unmerged_result.stdout.split())
        assert root_impl.id in lineage_output
        assert root_impl.id in unmerged_output
        assert retry_impl.id in lineage_output
        assert retry_impl.id in unmerged_output
        assert sibling_impl.id in lineage_output
        assert sibling_impl.id in unmerged_output

    def test_unmerged_keeps_owner_identity_for_retry_resume_branch_summary(self, tmp_path: Path):
        """Shared-branch retry/resume chains keep the owner row while summarizing the newest review."""
        store, root_impl, git = setup_unmerged_env(tmp_path)

        retry_impl = store.add("Retry implementation", task_type="implement")
        retry_impl.status = "completed"
        retry_impl.completed_at = datetime(2026, 2, 12, 11, 0, tzinfo=UTC)
        retry_impl.based_on = root_impl.id
        retry_impl.branch = "feature/test"
        retry_impl.same_branch = True
        retry_impl.has_commits = True
        retry_impl.merge_status = "unmerged"
        retry_impl.session_id = "sess-retry"
        store.update(retry_impl)

        retry_review = store.add("Review retry", task_type="review")
        retry_review.status = "completed"
        retry_review.completed_at = datetime(2026, 2, 12, 12, 0, tzinfo=UTC)
        retry_review.depends_on = retry_impl.id
        retry_review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(retry_review)

        resumed_impl = store.add("Resume retry implementation", task_type="implement")
        resumed_impl.status = "completed"
        resumed_impl.completed_at = datetime(2026, 2, 12, 13, 0, tzinfo=UTC)
        resumed_impl.based_on = retry_impl.id
        resumed_impl.branch = "feature/test"
        resumed_impl.same_branch = True
        resumed_impl.has_commits = True
        resumed_impl.merge_status = "unmerged"
        resumed_impl.session_id = "sess-retry"
        store.update(resumed_impl)

        resumed_review = store.add("Review resumed implementation", task_type="review")
        resumed_review.status = "completed"
        resumed_review.completed_at = datetime(2026, 2, 12, 14, 0, tzinfo=UTC)
        resumed_review.depends_on = resumed_impl.id
        resumed_review.output_content = "Verdict: APPROVED"
        store.update(resumed_review)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0

        normalized = " ".join(result.stdout.split())
        assert f"⚡ {root_impl.id}" in normalized
        assert "review: reviewed [✓ approved]" in normalized
        assert root_impl.id in normalized
        assert retry_impl.id in normalized
        assert resumed_impl.id in normalized
        assert resumed_review.id in normalized
        assert "⚠ changes requested" not in normalized

    def test_unmerged_marks_review_stale_when_newer_same_branch_implement_has_no_review(self, tmp_path: Path):
        """A newer same-branch implementation retry must stale prior review verdicts."""
        store, root_impl, git = setup_unmerged_env(tmp_path)
        root_impl.completed_at = datetime(2026, 2, 12, 10, 0, tzinfo=UTC)
        store.update(root_impl)

        review_a = store.add("Review first implementation", task_type="review")
        review_a.status = "completed"
        review_a.completed_at = datetime(2026, 2, 12, 11, 0, tzinfo=UTC)
        review_a.depends_on = root_impl.id
        review_a.output_content = "Verdict: APPROVED"
        store.update(review_a)

        impl_retry = store.add("Retry implementation without review", task_type="implement")
        impl_retry.status = "completed"
        impl_retry.completed_at = datetime(2026, 2, 12, 12, 0, tzinfo=UTC)
        impl_retry.based_on = root_impl.id
        impl_retry.branch = "feature/test"
        impl_retry.same_branch = True
        impl_retry.has_commits = True
        impl_retry.merge_status = "unmerged"
        store.update(impl_retry)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0

        normalized = " ".join(result.stdout.split())
        assert f"⚡ {root_impl.id}" in normalized
        assert "review: review stale" in normalized
        assert f"latest implement {impl_retry.id}" in normalized
        assert "review: reviewed [✓ approved]" not in normalized

    def test_unmerged_prefers_newer_same_branch_descendant_review_over_older_implementation_review(
        self, tmp_path: Path
    ):
        """A newer review on same-branch descendant work should override older impl review state."""
        store, root_impl, git = setup_unmerged_env(tmp_path)
        root_impl.completed_at = datetime(2026, 2, 12, 10, 0, tzinfo=UTC)
        store.update(root_impl)

        older_review = store.add("Review root implementation", task_type="review")
        older_review.status = "completed"
        older_review.completed_at = datetime(2026, 2, 12, 11, 0, tzinfo=UTC)
        older_review.depends_on = root_impl.id
        older_review.output_content = "Verdict: APPROVED"
        store.update(older_review)

        improve = store.add("Improve implementation", task_type="improve")
        improve.status = "completed"
        improve.completed_at = datetime(2026, 2, 12, 12, 0, tzinfo=UTC)
        improve.based_on = root_impl.id
        improve.branch = "feature/test"
        improve.same_branch = True
        store.update(improve)

        newer_review = store.add("Review improve", task_type="review")
        newer_review.status = "completed"
        newer_review.completed_at = datetime(2026, 2, 12, 13, 0, tzinfo=UTC)
        newer_review.depends_on = improve.id
        newer_review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(newer_review)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0

        normalized = " ".join(result.stdout.split())
        assert "review: reviewed [⚠ changes requested]" in normalized
        assert "review: reviewed [✓ approved]" not in normalized
        assert newer_review.id in normalized

    def test_unmerged_retry_resume_uses_root_review_clear_state_for_staleness(self, tmp_path: Path):
        """Shared-branch representative should still show stale review after root clear."""
        store, root_impl, git = setup_unmerged_env(tmp_path)
        root_impl.completed_at = datetime(2026, 2, 12, 10, 0, tzinfo=UTC)
        store.update(root_impl)

        retry_impl = store.add("Retry implementation", task_type="implement")
        retry_impl.status = "completed"
        retry_impl.completed_at = datetime(2026, 2, 12, 11, 0, tzinfo=UTC)
        retry_impl.based_on = root_impl.id
        retry_impl.branch = "feature/test"
        retry_impl.same_branch = True
        retry_impl.has_commits = True
        retry_impl.merge_status = "unmerged"
        retry_impl.session_id = "sess-retry"
        store.update(retry_impl)

        retry_review = store.add("Review retry", task_type="review")
        retry_review.status = "completed"
        retry_review.completed_at = datetime(2026, 2, 12, 12, 0, tzinfo=UTC)
        retry_review.depends_on = retry_impl.id
        retry_review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(retry_review)

        assert root_impl.id is not None
        store.clear_review_state(root_impl.id)

        resumed_impl = store.add("Resume retry implementation", task_type="implement")
        resumed_impl.status = "completed"
        resumed_impl.completed_at = datetime(2026, 2, 12, 13, 0, tzinfo=UTC)
        resumed_impl.based_on = retry_impl.id
        resumed_impl.branch = "feature/test"
        resumed_impl.same_branch = True
        resumed_impl.has_commits = True
        resumed_impl.merge_status = "unmerged"
        resumed_impl.session_id = "sess-retry"
        store.update(resumed_impl)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0

        normalized = " ".join(result.stdout.split())
        assert f"⚡ {root_impl.id}" in normalized
        assert "review: review stale" in normalized
        assert f"review state cleared after last review {retry_review.id}" in normalized
        assert "review: no review" not in normalized
        assert "⚠ changes requested" not in normalized

    def test_unmerged_does_not_inherit_root_review_across_branches(self, tmp_path: Path):
        """A descendant branch row must not inherit review verdict from root branch."""
        store, root_impl, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Root implementation",
            branch="feature/root",
        )
        root_impl.completed_at = datetime(2026, 2, 12, 10, 0, tzinfo=UTC)
        store.update(root_impl)

        root_review = store.add("Root branch review", task_type="review")
        root_review.status = "completed"
        root_review.completed_at = datetime(2026, 2, 12, 11, 0, tzinfo=UTC)
        root_review.depends_on = root_impl.id
        root_review.output_content = "Verdict: APPROVED"
        store.update(root_review)

        retry_impl = store.add("Retry on new branch", task_type="implement")
        retry_impl.status = "completed"
        retry_impl.completed_at = datetime(2026, 2, 12, 12, 0, tzinfo=UTC)
        retry_impl.based_on = root_impl.id
        retry_impl.branch = "feature/retry"
        retry_impl.same_branch = False
        retry_impl.has_commits = True
        retry_impl.merge_status = "unmerged"
        store.update(retry_impl)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0

        retry_block = _unmerged_branch_block(result.stdout, "feature/retry")
        root_block = _unmerged_branch_block(result.stdout, "feature/root")
        assert "review: no review" in retry_block
        assert "✓ approved" not in retry_block
        assert "review: reviewed [✓ approved]" in root_block

    def test_unmerged_branch_review_uses_branch_specific_review_source(self, tmp_path: Path):
        """When branch has its own review, root-branch review must not override it."""
        store, root_impl, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Root implementation",
            branch="feature/root",
        )
        root_impl.completed_at = datetime(2026, 2, 12, 10, 0, tzinfo=UTC)
        store.update(root_impl)

        retry_impl = store.add("Retry on new branch", task_type="implement")
        retry_impl.status = "completed"
        retry_impl.completed_at = datetime(2026, 2, 12, 11, 0, tzinfo=UTC)
        retry_impl.based_on = root_impl.id
        retry_impl.branch = "feature/retry"
        retry_impl.same_branch = False
        retry_impl.has_commits = True
        retry_impl.merge_status = "unmerged"
        store.update(retry_impl)

        retry_review = store.add("Retry review", task_type="review")
        retry_review.status = "completed"
        retry_review.completed_at = datetime(2026, 2, 12, 12, 0, tzinfo=UTC)
        retry_review.depends_on = retry_impl.id
        retry_review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(retry_review)

        root_review = store.add("Root review", task_type="review")
        root_review.status = "completed"
        root_review.completed_at = datetime(2026, 2, 12, 13, 0, tzinfo=UTC)
        root_review.depends_on = root_impl.id
        root_review.output_content = "Verdict: APPROVED"
        store.update(root_review)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0

        retry_block = _unmerged_branch_block(result.stdout, "feature/retry")
        root_block = _unmerged_branch_block(result.stdout, "feature/root")
        assert "review: reviewed [⚠ changes requested]" in retry_block
        assert "✓ approved" not in retry_block
        assert "review: reviewed [✓ approved]" in root_block

    def test_unmerged_split_branch_keeps_branch_local_review_after_root_review_clear(self, tmp_path: Path):
        """Root review-clear state must not stale a split descendant branch row."""
        store, root_impl, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Root implementation",
            branch="feature/root",
        )
        root_impl.completed_at = datetime(2026, 2, 12, 10, 0, tzinfo=UTC)
        store.update(root_impl)

        root_review = store.add("Root review", task_type="review")
        root_review.status = "completed"
        root_review.completed_at = datetime(2026, 2, 12, 11, 0, tzinfo=UTC)
        root_review.depends_on = root_impl.id
        root_review.output_content = "Verdict: APPROVED"
        store.update(root_review)

        retry_impl = store.add("Retry on new branch", task_type="implement")
        retry_impl.status = "completed"
        retry_impl.completed_at = datetime(2026, 2, 12, 12, 0, tzinfo=UTC)
        retry_impl.based_on = root_impl.id
        retry_impl.branch = "feature/retry"
        retry_impl.same_branch = False
        retry_impl.has_commits = True
        retry_impl.merge_status = "unmerged"
        store.update(retry_impl)

        retry_review = store.add("Retry review", task_type="review")
        retry_review.status = "completed"
        retry_review.completed_at = datetime(2026, 2, 12, 13, 0, tzinfo=UTC)
        retry_review.depends_on = retry_impl.id
        retry_review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(retry_review)

        assert root_impl.id is not None
        store.clear_review_state(root_impl.id)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0

        retry_block = _unmerged_branch_block(result.stdout, "feature/retry")
        root_block = _unmerged_branch_block(result.stdout, "feature/root")
        assert "review: reviewed [⚠ changes requested]" in retry_block
        assert "review stale" not in retry_block
        assert "review state cleared after last review" not in retry_block
        assert "review: review stale" in root_block
    def test_unmerged_lineage_marks_only_latest_review_node(self, tmp_path: Path):
        """The most recent review node is annotated with the latest marker."""
        store, impl, git = setup_unmerged_env(tmp_path)

        older_review = store.add("Older review", task_type="review")
        older_review.status = "completed"
        older_review.completed_at = datetime(2026, 2, 12, 9, 0, tzinfo=UTC)
        older_review.depends_on = impl.id
        older_review.output_content = "Verdict: APPROVED"
        store.update(older_review)

        latest_review = store.add("Latest review", task_type="review")
        latest_review.status = "completed"
        latest_review.completed_at = datetime(2026, 2, 12, 10, 0, tzinfo=UTC)
        latest_review.depends_on = impl.id
        latest_review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(latest_review)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        # Normalize output since "← latest" may wrap across lines at narrow terminal widths
        normalized = " ".join(result.stdout.split())
        older_idx = normalized.index(f"{older_review.id}")
        latest_idx = normalized.index(f"{latest_review.id}")
        marker_idx = normalized.index("latest")
        assert marker_idx > latest_idx
        assert not (older_idx < marker_idx < latest_idx)

    def test_unmerged_prefers_latest_based_on_only_review_for_badge_and_lineage(self, tmp_path: Path):
        """Latest imported review should drive both the summary badge and lineage marker."""
        store, impl, git = setup_unmerged_env(tmp_path)

        older_review = store.add("Older review", task_type="review")
        older_review.status = "completed"
        older_review.completed_at = datetime(2026, 2, 12, 9, 0, tzinfo=UTC)
        older_review.depends_on = impl.id
        older_review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(older_review)

        latest_review = store.add("Imported latest review", task_type="review")
        latest_review.status = "completed"
        latest_review.completed_at = datetime(2026, 2, 12, 10, 0, tzinfo=UTC)
        latest_review.based_on = impl.id
        latest_review.depends_on = None
        latest_review.output_content = "Verdict: APPROVED_WITH_FOLLOWUPS"
        store.update(latest_review)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0

        normalized = " ".join(result.stdout.split())
        assert "review: reviewed [↺ approved with follow-ups]" in normalized
        assert "⚠ changes requested" not in normalized

        latest_idx = normalized.index(f"{latest_review.id}")
        latest_marker_idx = normalized.index("approved_with_followups ← latest")
        assert latest_marker_idx > latest_idx


class TestUnmergedSelectionBehavior:
    """Tests for `gza unmerged` task selection behavior."""

    @pytest.fixture(autouse=True)
    def _use_fast_unmerged_env(self, monkeypatch):
        fake_git = _FastUnmergedGit()
        fake_git._branches.add("main")

        def setup_fast(*args, **kwargs):
            store, task, _git = _setup_unmerged_env_fast(*args, **kwargs)
            if task.branch:
                fake_git._branches.add(task.branch)
            fake_git._branches.add("main")
            return store, task, fake_git

        monkeypatch.setitem(globals(), "setup_unmerged_env", setup_fast)
        monkeypatch.setattr(query_cli, "Git", lambda _project_dir: fake_git)
        monkeypatch.setattr(query_cli, "GitHub", _UnavailableGitHub)

    def test_unmerged_excludes_failed_tasks(self, tmp_path: Path):
        """Failed tasks are excluded from gza unmerged (only completed tasks shown)."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Failed but useful task",
            task_id="20260220-failed-task",
            branch="feature/failed-branch",
            status="failed",
            has_commits=False,
        )

        # Failed task is excluded from unmerged output
        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Failed but useful task" not in result.stdout
        assert "No unmerged tasks" in result.stdout

    def test_unmerged_shows_only_unmerged_rows(self, tmp_path: Path):
        """Only rows with canonical unmerged merge state should appear."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Unmerged task",
            task_id="20260220-unmerged",
            branch="feature/unmerged",
        )

        # Task without merge_status='unmerged' doesn't show up
        task2 = store.add("Merged task", task_type="implement")
        task2.status = "completed"
        task2.branch = "feature/merged"
        task2.has_commits = True
        task2.merge_status = "merged"
        task2.slug = "20260220-merged"
        task2.completed_at = datetime.now(UTC)
        store.update(task2)

        # Without --all
        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Unmerged task" in result.stdout
        assert "Merged task" not in result.stdout

    def test_unmerged_excludes_tasks_without_merge_status(self, tmp_path: Path):
        """Tasks without merge_status='unmerged' are not shown."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="No commits task",
            task_id="20260220-no-commits",
            branch="feature/no-commits",
            has_commits=False,
            merge_status=None,
        )

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "No commits task" not in result.stdout

    def test_unmerged_shows_deleted_branch_if_merge_status_unmerged(self, tmp_path: Path):
        """Plain unmerged keeps deleted local branches visible without merge proof."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Task with merge_status='unmerged' but branch doesn't exist anywhere
        task = store.add("Deleted branch task", task_type="implement")
        task.status = "completed"
        task.branch = "feature/nonexistent-branch"
        task.has_commits = True
        task.merge_status = "unmerged"
        task.slug = "20260220-deleted-branch"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Deleted branch task" in result.stdout
        assert "branch deleted" in result.stdout
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.merge_status == "unmerged"

    def test_unmerged_keeps_deleted_local_branch_visible_when_remote_feature_still_exists(self, tmp_path: Path):
        """Plain canonical unmerged must not persist merged when only the local branch is gone."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Deleted local remote survivor task",
            task_id="20260220-deleted-local-remote-survivor",
            branch="feature/deleted-local-remote-survivor",
        )

        git._branches.discard("feature/deleted-local-remote-survivor")

        result = run_gza("unmerged", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Deleted local remote survivor task" in result.stdout
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.merge_status == "unmerged"

    def test_unmerged_migrates_deleted_local_branch_with_remote_survivor_via_shared_reconcile(self, tmp_path: Path):
        """Legacy merge_status=None rows keep deleted local branches visible when origin/<branch> survives."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Legacy deleted local remote survivor task",
            task_id="20260220-legacy-deleted-local-remote-survivor",
            branch="feature/legacy-deleted-local-remote-survivor",
            merge_status=None,
        )

        git._branches.discard("feature/legacy-deleted-local-remote-survivor")

        result = run_gza("unmerged", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Migrating merge status" not in result.stdout
        assert "Legacy deleted local remote survivor task" in result.stdout
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.merge_status == "unmerged"
        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None
        assert unit.state == "unmerged"

    def test_unmerged_backfills_merge_status(self, tmp_path: Path):
        """Plain unmerged backfills canonical merge_status."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Old task needing migration",
            task_id="20260220-old-task",
            branch="feature/old-task",
            merge_status=None,
        )

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Migrating merge status" not in result.stdout
        assert "Old task needing migration" in result.stdout

        migrated_task = store.get(task.id)
        assert migrated_task.merge_status == "unmerged"

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Migrating merge status" not in result.stdout

    def test_unmerged_marks_stale_merged_task_as_merged(self, tmp_path: Path):
        """Plain unmerged repairs stale merged rows in the canonical DB view."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Stale unmerged task",
            task_id="20260220-stale-unmerged",
            branch="feature/stale-unmerged",
        )

        git._merged[("feature/stale-unmerged", "main")] = True

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Stale unmerged task" not in result.stdout
        assert "No unmerged tasks" in result.stdout

        stale_task = store.get(task.id)
        assert stale_task.merge_status == "merged"

    def test_unmerged_same_branch_improve_without_merge_status_does_not_print_migration_banner(
        self,
        tmp_path: Path,
    ):
        """Valid non-owner improve rows must not trigger legacy merge-status migration output."""
        store, impl_task, _git = setup_unmerged_env(
            tmp_path,
            task_prompt="Implementation owner task",
            branch="feature/same-branch-improve",
            merge_status="unmerged",
        )

        improve_task = store.add(
            "Improve on shared branch",
            task_type="improve",
            same_branch=True,
            based_on=impl_task.id,
        )
        store.mark_completed(improve_task, has_commits=True, branch="feature/same-branch-improve")

        result = run_gza("unmerged", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Migrating merge status" not in result.stdout
        assert "Implementation owner task" in result.stdout

        refreshed_improve = store.get(improve_task.id)
        assert refreshed_improve is not None
        assert refreshed_improve.merge_status is None

    def test_unmerged_branch_owner_stops_before_branchless_plan_dependency(
        self,
        tmp_path: Path,
    ) -> None:
        """A plan dependency must not become the owner for a same-branch rebase."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan ancestor", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime(2026, 5, 9, 9, 0, tzinfo=UTC)
        store.update(plan)

        owner = store.add("Implement owner", task_type="implement")
        owner.status = "completed"
        owner.completed_at = datetime(2026, 5, 9, 10, 0, tzinfo=UTC)
        owner.branch = "feature/branch-owner"
        owner.has_commits = True
        owner.merge_status = "unmerged"
        owner.depends_on = plan.id
        store.update(owner)

        rebase = store.add("Rebase descendant", task_type="rebase")
        rebase.status = "completed"
        rebase.completed_at = datetime(2026, 5, 9, 11, 0, tzinfo=UTC)
        rebase.branch = "feature/branch-owner"
        rebase.has_commits = True
        rebase.same_branch = True
        rebase.based_on = owner.id
        store.update(rebase)

        assert query_cli._resolve_unmerged_branch_owner(store, rebase).id == owner.id  # noqa: SLF001

    @pytest.mark.parametrize("relation_field", ["based_on", "depends_on"])
    def test_unmerged_branch_owner_stops_before_branchless_explore_ancestor(
        self,
        tmp_path: Path,
        relation_field: str,
    ) -> None:
        """A branchless explore ancestor must not replace the branch-bearing implement owner."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        explore = store.add("Explore ancestor", task_type="explore")
        explore.status = "completed"
        explore.completed_at = datetime(2026, 5, 9, 9, 0, tzinfo=UTC)
        store.update(explore)

        implement = store.add("Implement owner", task_type="implement")
        implement.status = "completed"
        implement.completed_at = datetime(2026, 5, 9, 10, 0, tzinfo=UTC)
        implement.branch = "feature/explore-boundary"
        implement.has_commits = True
        implement.merge_status = "unmerged"
        setattr(implement, relation_field, explore.id)
        store.update(implement)

        assert query_cli._resolve_unmerged_branch_owner(store, implement).id == implement.id  # noqa: SLF001

    def test_unmerged_different_branch_implement_chain_keeps_separate_branch_owners(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Different-branch implement dependencies should surface one owner row per branch."""
        store, branch_a, _git = _setup_unmerged_env_fast(
            tmp_path,
            task_prompt="Branch A owner",
            branch="feature/branch-a",
        )
        branch_a.completed_at = datetime(2026, 5, 9, 10, 0, tzinfo=UTC)
        store.update(branch_a)

        branch_b = store.add("Branch B owner", task_type="implement")
        branch_b.status = "completed"
        branch_b.completed_at = datetime(2026, 5, 9, 11, 0, tzinfo=UTC)
        branch_b.based_on = branch_a.id
        branch_b.branch = "feature/branch-b"
        branch_b.has_commits = True
        branch_b.merge_status = "unmerged"
        store.update(branch_b)

        assert branch_a.id is not None
        assert branch_b.id is not None
        assert query_cli._resolve_unmerged_branch_owner(store, branch_a).id == branch_a.id  # noqa: SLF001
        assert query_cli._resolve_unmerged_branch_owner(store, branch_b).id == branch_b.id  # noqa: SLF001

        branch_a_unit = store.get_or_create_merge_unit_for_task(branch_a)
        branch_b_unit = store.get_or_create_merge_unit_for_task(branch_b)
        assert branch_a_unit is not None
        assert branch_b_unit is not None
        assert branch_a_unit.id != branch_b_unit.id

        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=5,
            json=True,
            fields="id,prompt,branch",
        )

        result = query_cli.cmd_unmerged(args, git=_FastUnmergedGit())
        captured = capsys.readouterr()
        assert result == 0
        payload = json.loads(captured.out)
        assert {(row["id"], row["prompt"], row["branch"]) for row in payload} == {
            (branch_a.id, "Branch A owner", "feature/branch-a"),
            (branch_b.id, "Branch B owner", "feature/branch-b"),
        }

    @pytest.mark.parametrize("descendant_type", ["rebase", "improve", "fix"])
    def test_unmerged_same_branch_followups_still_surface_implement_owner(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        descendant_type: str,
    ) -> None:
        """Same-branch follow-up task types should still resolve to the root implement owner."""
        store, owner, _git = _setup_unmerged_env_fast(
            tmp_path,
            task_prompt="Implement owner",
            branch="feature/same-branch-owner",
        )
        owner.completed_at = datetime(2026, 5, 9, 10, 0, tzinfo=UTC)
        store.update(owner)

        review = store.add("Review context", task_type="review")
        review.status = "completed"
        review.completed_at = datetime(2026, 5, 9, 10, 30, tzinfo=UTC)
        review.depends_on = owner.id
        review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(review)

        descendant = store.add(f"{descendant_type.title()} descendant", task_type=descendant_type)
        descendant.status = "completed"
        descendant.completed_at = datetime(2026, 5, 9, 11, 0, tzinfo=UTC)
        descendant.branch = "feature/same-branch-owner"
        descendant.has_commits = True
        descendant.same_branch = True
        descendant.based_on = owner.id
        descendant.depends_on = review.id
        store.update(descendant)

        assert query_cli._resolve_unmerged_branch_owner(store, descendant).id == owner.id  # noqa: SLF001

        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=5,
            json=True,
            fields="id,prompt",
        )

        result = query_cli.cmd_unmerged(args, git=_FastUnmergedGit())
        captured = capsys.readouterr()
        assert result == 0
        assert json.loads(captured.out) == [{"id": owner.id, "prompt": "Implement owner"}]

    def test_unmerged_uses_existing_origin_default_branch_without_fetch_by_default(
        self,
        tmp_path: Path,
    ):
        """Plain canonical unmerged should reuse an existing origin/default ref without fetching."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Remote-only merged task",
            task_id="20260220-remote-only-merged-task",
            branch="feature/remote-only-merged-task",
        )

        git._refs.add("origin/main")
        git._merged[("feature/remote-only-merged-task", "origin/main")] = True

        result = run_gza("unmerged", "--project", str(tmp_path))

        assert git.fetch_calls == []
        assert result.returncode == 0
        assert "Remote-only merged task" not in result.stdout
        assert "No unmerged tasks" in result.stdout
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.merge_status == "merged"

    def test_unmerged_fetch_flag_fetches_origin_default_branch_before_canonical_reconcile(self, tmp_path: Path):
        """`--fetch` should refresh origin/default merge evidence before canonical reconcile."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Remote-only merged task",
            task_id="20260220-remote-only-merged-task",
            branch="feature/remote-only-merged-task",
        )

        git._refs.add("origin/main")
        git._merged[("feature/remote-only-merged-task", "origin/main")] = True

        result = run_gza("unmerged", "--fetch", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Remote-only merged task" not in result.stdout
        assert "No unmerged tasks" in result.stdout
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.merge_status == "merged"

    def test_unmerged_fetch_failure_fails_closed_without_rendering_stale_rows(self, tmp_path: Path):
        """`--fetch` should stop instead of showing stale rows after fetch failures."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Fetch failure task",
            task_id="20260220-fetch-failure-task",
            branch="feature/fetch-failure-task",
        )
        task.diff_files_changed = 99
        task.diff_lines_added = 999
        task.diff_lines_removed = 111
        task.pr_number = 123
        task.pr_state = "open"
        store.update(task)

        git._fetch_error = GitError("network down")
        result = run_gza("unmerged", "--fetch", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "failed to refresh canonical merge truth" in result.stdout
        assert "git fetch origin failed: network down" in result.stdout
        assert "Fetch failure task" not in result.stdout
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.merge_status == "unmerged"
        assert refreshed.diff_files_changed == 99
        assert refreshed.diff_lines_added == 999
        assert refreshed.diff_lines_removed == 111
        assert refreshed.pr_number == 123
        assert refreshed.pr_state == "open"

    def test_unmerged_update_refreshes_diff_stats_for_live_branch(self, tmp_path: Path):
        """Plain unmerged refreshes canonical diff stats for branches still unmerged."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Refresh diff stats task",
            task_id="20260220-refresh-diff-stats",
            branch="feature/refresh-diff-stats",
        )

        task.diff_files_changed = 99
        task.diff_lines_added = 999
        task.diff_lines_removed = 111
        store.update(task)

        git._numstat = "2\t0\tfeature.txt\n"

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "+2/-0 LOC, 1 files" in result.stdout

        updated_task = store.get(task.id)
        assert updated_task.merge_status == "unmerged"
        assert updated_task.diff_files_changed == 1
        assert updated_task.diff_lines_added == 2
        assert updated_task.diff_lines_removed == 0

    def test_unmerged_into_current_uses_current_branch(self, tmp_path: Path):
        """--into-current uses live git state against the current branch."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Branch local task",
            task_id="20260220-branch-local-task",
            branch="feature/branch-local-task",
        )

        git._current_branch = "integration"
        git._branches.add("integration")
        git._merged[("feature/branch-local-task", "integration")] = True

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Branch local task" in result.stdout

        result = run_gza("unmerged", "--into-current", "--project", str(tmp_path), cwd=tmp_path)
        assert result.returncode == 0
        assert "Showing tasks unmerged relative to integration" in result.stdout
        assert "No unmerged tasks" in result.stdout

    def test_unmerged_into_current_keeps_deleted_branch_visible_when_not_merged_into_current(
        self,
        tmp_path: Path,
    ):
        """Deleted local branches stay visible for read-only current-branch comparisons."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Deleted branch current-target task",
            task_id="20260220-deleted-current-target-task",
            branch="feature/deleted-current-target-task",
        )

        git._current_branch = "integration"
        git._branches.add("integration")
        git._branches.discard("feature/deleted-current-target-task")

        result = run_gza("unmerged", "--into-current", "--project", str(tmp_path), cwd=tmp_path)

        assert result.returncode == 0
        assert "Showing tasks unmerged relative to integration" in result.stdout
        assert "Deleted branch current-target task" in result.stdout
        assert "branch deleted" in result.stdout
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.merge_status == "unmerged"

    def test_unmerged_target_uses_specified_branch(self, tmp_path: Path):
        """--target uses live git state against the specified branch."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Target branch task",
            task_id="20260220-target-branch-task",
            branch="feature/target-branch-task",
        )

        git._branches.add("integration")
        git._merged[("feature/target-branch-task", "integration")] = True

        result = run_gza("unmerged", "--target", "integration", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Showing tasks unmerged relative to integration" in result.stdout
        assert "No unmerged tasks" in result.stdout

    def test_unmerged_target_keeps_deleted_branch_visible_when_not_merged_into_target(
        self,
        tmp_path: Path,
    ):
        """Deleted local branches stay visible for live targets even with cached default-branch merges."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Deleted branch explicit-target task",
            task_id="20260220-deleted-explicit-target-task",
            branch="feature/deleted-explicit-target-task",
            merge_status="merged",
        )

        git._branches.add("integration")
        git._branches.discard("feature/deleted-explicit-target-task")

        result = run_gza("unmerged", "--target", "integration", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Showing tasks unmerged relative to integration" in result.stdout
        assert "Deleted branch explicit-target task" in result.stdout
        assert "branch deleted" in result.stdout
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.merge_status == "merged"

    def test_unmerged_live_target_does_not_persist_reconciliation(self, tmp_path: Path):
        """Live target comparisons stay query-only and do not backfill merge_status."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Live target no-op task",
            task_id="20260220-live-target-noop",
            branch="feature/live-target-noop",
            merge_status=None,
        )

        git._branches.add("integration")
        git._merged[("feature/live-target-noop", "integration")] = True

        result = run_gza(
            "unmerged",
            "--target",
            "integration",
            "--project",
            str(tmp_path),
        )
        assert result.returncode == 0
        assert "Migrating merge status" not in result.stdout
        assert "No unmerged tasks" in result.stdout

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.merge_status is None

    def test_unmerged_live_target_excludes_same_branch_fix_descendants(self, tmp_path: Path):
        """Live-target unmerged (--into-current/--target) must not double-list a fix task
        that shares a branch with its implementation — otherwise the same branch appears
        twice. See .gza/learnings.md (exclude completed same-branch fix descendants)."""
        from datetime import UTC, datetime

        store, impl_task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Implement widget",
            task_id="20260220-impl-widget",
            branch="feature/widget",
        )

        fix_task = store.add(
            "Fix widget churn",
            task_type="fix",
        )
        fix_task.status = "completed"
        fix_task.completed_at = datetime.now(UTC)
        fix_task.branch = "feature/widget"
        fix_task.has_commits = True
        fix_task.merge_status = "unmerged"
        fix_task.slug = "20260220-fix-widget-churn"
        fix_task.based_on = impl_task.id
        fix_task.same_branch = True
        store.update(fix_task)

        result = run_gza("unmerged", "--into-current", "--project", str(tmp_path), cwd=tmp_path)
        assert result.returncode == 0
        assert "Implement widget" in result.stdout
        # The fix must not appear as its own branch row — the impl row already represents the branch.
        assert "Fix widget churn" not in result.stdout


class TestUnmergedImprovedDisplay:
    """Tests for improved unmerged display (diff stats, review prominence, completed-only)."""

    @pytest.fixture(autouse=True)
    def _use_fast_unmerged_env(self, monkeypatch):
        fake_git = _FastUnmergedGit()
        monkeypatch.setitem(globals(), "setup_unmerged_env", _setup_unmerged_env_fast)
        monkeypatch.setattr(query_cli, "Git", lambda _project_dir: fake_git)
        monkeypatch.setattr(query_cli, "GitHub", _UnavailableGitHub)

    def test_unmerged_excludes_failed_tasks(self, tmp_path: Path):
        """Failed tasks with merge_status='unmerged' are excluded from unmerged output."""

        # Use setup_unmerged_env for the completed task (creates config, git, store)
        store, completed_task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Completed task",
            branch="feature/completed",
        )

        # Add a failed task to the same store
        failed_task = store.add("Failed task", task_type="implement")
        failed_task.status = "failed"
        failed_task.branch = "feature/failed"
        failed_task.merge_status = "unmerged"
        failed_task.completed_at = datetime.now(UTC)
        store.update(failed_task)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Completed task" in result.stdout
        assert "Failed task" not in result.stdout

    def test_unmerged_reads_frozen_snapshot_without_merge_status_backfill(self, tmp_path: Path):
        """Plain canonical unmerged should fail clearly when the DB snapshot is read-only."""
        import os

        store, task, _git = setup_unmerged_env(
            tmp_path,
            task_prompt="Legacy merge-status task",
            branch="feature/legacy-merge-status",
            merge_status=None,
        )
        task.merge_status = None
        store.update(task)

        db_path = tmp_path / ".gza" / "gza.db"
        original_mode = db_path.stat().st_mode
        os.chmod(db_path, 0o444)
        try:
            result = run_gza("unmerged", "--project", str(tmp_path))
        finally:
            os.chmod(db_path, original_mode)

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.merge_status is None
        assert result.returncode == 1
        assert "writable task DB" in result.stdout
        assert "read-only" in result.stdout

    def test_unmerged_canonical_disk_io_snapshot_error_shows_targeted_message(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Canonical unmerged normalizes disk I/O snapshot write errors without traceback."""
        setup_unmerged_env(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        original_mode = db_path.stat().st_mode
        os.chmod(db_path, 0o444)
        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            update=False,
            limit=5,
            commits_only=False,
            all=False,
        )

        with patch(
            "gza.cli.query.sync_branch_cohorts",
            side_effect=sqlite3.OperationalError("disk I/O error"),
        ):
            try:
                result = query_cli.cmd_unmerged(args)
            finally:
                os.chmod(db_path, original_mode)

        captured = capsys.readouterr()
        assert result == 1
        assert "writable task DB" in captured.out
        assert "read-only" in captured.out
        assert "disk I/O error" not in captured.out
        assert "Traceback" not in captured.err

    def test_unmerged_canonical_writable_disk_io_error_is_not_rewritten_as_read_only(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Writable canonical unmerged should surface real storage failures unchanged."""
        setup_unmerged_env(tmp_path)
        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            update=False,
            limit=5,
            commits_only=False,
            all=False,
        )

        with patch(
            "gza.cli.query.sync_branch_cohorts",
            side_effect=sqlite3.OperationalError("disk I/O error"),
        ):
            with pytest.raises(sqlite3.OperationalError, match=r"disk I/O error"):
                query_cli.cmd_unmerged(args)

        captured = capsys.readouterr()
        assert "writable task DB" not in captured.out
        assert "read-only" not in captured.out

    def test_unmerged_target_fails_closed_on_live_compare_error(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Live-target `--target` must not render stale fallback rows after compare errors."""
        _store, task, _git = setup_unmerged_env(
            tmp_path,
            task_prompt="Live compare failure task",
            branch="feature/live-compare-failure",
        )
        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target="integration",
            fetch=False,
            update=False,
            limit=5,
            commits_only=False,
            all=False,
        )

        with patch(
            "gza.cli.query.reconcile_branch_merge_truth",
            return_value=[
                BranchSyncResult(
                    branch="feature/live-compare-failure",
                    task_ids=(task.id,) if task.id is not None else (),
                    merge_status="unmerged",
                    reconciled=True,
                    errors=["git compare failed"],
                )
            ],
        ):
            result = query_cli.cmd_unmerged(args)

        captured = capsys.readouterr()
        assert result == 1
        assert "failed to reconcile unmerged branches relative to integration" in captured.out
        assert "feature/live-compare-failure: git compare failed" in captured.out
        assert "Showing tasks unmerged relative to integration" not in captured.out
        assert "Live compare failure task" not in captured.out

    def test_unmerged_into_current_fails_closed_on_live_diff_stat_error(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Live-target `--into-current` must stop on diff-stat refresh errors."""
        _store, task, _git = setup_unmerged_env(
            tmp_path,
            task_prompt="Live diff-stat failure task",
            branch="feature/live-diff-stat-failure",
        )
        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=True,
            target=None,
            fetch=False,
            update=False,
            limit=5,
            commits_only=False,
            all=False,
        )

        with patch(
            "gza.cli.query.reconcile_branch_merge_truth",
            return_value=[
                BranchSyncResult(
                    branch="feature/live-diff-stat-failure",
                    task_ids=(task.id,) if task.id is not None else (),
                    merge_status="unmerged",
                    reconciled=True,
                    errors=["diff-stat refresh failed"],
                )
            ],
        ):
            result = query_cli.cmd_unmerged(args)

        captured = capsys.readouterr()
        assert result == 1
        assert "failed to reconcile unmerged branches relative to main" in captured.out
        assert "feature/live-diff-stat-failure: diff-stat refresh failed" in captured.out
        assert "Showing tasks unmerged relative to main" not in captured.out
        assert "Live diff-stat failure task" not in captured.out

    def test_unmerged_persists_merge_status_during_default_refresh(self, tmp_path: Path):
        """Default unmerged persists canonical merge truth through merge units."""
        store, task, _git = setup_unmerged_env(
            tmp_path,
            task_prompt="Legacy merge-status update task",
            branch="feature/legacy-merge-status-update",
            merge_status=None,
        )
        task.merge_status = None
        store.update(task)
        assert task.id is not None

        read_only_view = run_gza("unmerged", "--project", str(tmp_path))
        assert read_only_view.returncode == 0
        assert "Migrating merge status" not in read_only_view.stdout
        migrated = store.get(task.id)
        assert migrated is not None
        assert migrated.merge_status == "unmerged"
        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None
        assert unit.state == "unmerged"

    def test_unmerged_default_refresh_persists_empty_branch_and_hides_it(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Default unmerged refresh should persist zero-commit merged branches as empty."""
        store, task, git = _setup_unmerged_env_fast(
            tmp_path,
            task_prompt="Empty merge-unit task",
            branch="feature/empty-merge-unit",
        )
        git._merged[(task.branch, "main")] = True
        git._ahead_counts[(task.branch, "main")] = 0

        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=5,
            json=False,
            fields=None,
            list_fields=False,
        )

        result = query_cli.cmd_unmerged(args, git=git)

        captured = capsys.readouterr()
        assert result == 0
        assert "No unmerged tasks" in captured.out
        assert "Empty merge-unit task" not in captured.out

        assert task.id is not None
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.merge_status is None
        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None
        assert unit.state == "empty"

    def test_unmerged_live_target_hides_zero_commit_empty_branch(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Live-target unmerged should treat zero-commit merged branches as terminal."""
        _store, task, git = _setup_unmerged_env_fast(
            tmp_path,
            task_prompt="Live empty branch task",
            branch="feature/live-empty-merge-unit",
        )
        git._current_branch = "integration"
        git._merged[(task.branch, "integration")] = True
        git._ahead_counts[(task.branch, "integration")] = 0

        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target="integration",
            fetch=False,
            limit=5,
            json=False,
            fields=None,
            list_fields=False,
        )

        result = query_cli.cmd_unmerged(args, git=git)

        captured = capsys.readouterr()
        assert result == 0
        assert "No unmerged tasks" in captured.out
        assert "Live empty branch task" not in captured.out

    def test_unmerged_shows_diff_stats(self, tmp_path: Path):
        """Unmerged output shows diff stats (files, LOC added/removed)."""
        store, task, git = setup_unmerged_env(tmp_path)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        # Diff stats should be shown in branch line
        assert "LOC" in result.stdout
        assert "files" in result.stdout

    def test_unmerged_uses_cached_diff_stats(self, tmp_path: Path):
        """gza unmerged refreshes cached diff stats before showing the canonical view."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Cached stats task",
            branch="feature/cached",
        )

        # Pre-populate cached diff stats
        task.diff_files_changed = 5
        task.diff_lines_added = 42
        task.diff_lines_removed = 7
        store.update(task)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "+42/-7 LOC, 5 files" not in result.stdout
        assert "+1/-0 LOC, 1 files" in result.stdout

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.diff_files_changed == 1
        assert refreshed.diff_lines_added == 1
        assert refreshed.diff_lines_removed == 0

    def test_unmerged_target_branch_ignores_default_branch_cached_stats(self, tmp_path: Path):
        """When `--target` is used, unmerged recomputes diff stats for that merge target."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Target-aware stats task",
            branch="feature/target-aware",
        )

        # Seed cached stats (computed against default branch) with a sentinel value.
        task.diff_files_changed = 999
        task.diff_lines_added = 999
        task.diff_lines_removed = 999
        store.update(task)

        default_result = run_gza("unmerged", "--project", str(tmp_path))
        assert default_result.returncode == 0
        assert "+999/-999 LOC, 999 files" not in default_result.stdout
        assert "+1/-0 LOC, 1 files" in default_result.stdout

        target_result = run_gza("unmerged", "--target", "target/base", "--project", str(tmp_path))
        assert target_result.returncode == 0
        assert "+999/-999 LOC, 999 files" not in target_result.stdout

    def test_unmerged_review_shown_on_own_line(self, tmp_path: Path):
        """Review status appears on its own 'review:' line."""
        store, task, git = setup_unmerged_env(tmp_path)

        review = store.add("Review feature", task_type="review")
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.depends_on = task.id
        review.output_content = "**Verdict: APPROVED**"
        store.update(review)

        with patch("gza.cli.Git", return_value=_mock_unmerged_git()):
            result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        # Review should be on its own line starting with "review:"
        assert "review:" in result.stdout
        assert "✓ approved" in result.stdout

    def test_unmerged_shows_open_pr_url_on_its_own_line(self, tmp_path: Path):
        """Open PR metadata should be surfaced directly in unmerged output."""
        setup_unmerged_env(tmp_path)

        with patch(
            "gza.cli.lookup_task_pr",
            return_value=LookupTaskPrResult(
                found=True,
                status="existing",
                pr_url="https://github.com/o/r/pull/123",
            ),
        ):
            result = run_gza("unmerged", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "pr:" in result.stdout
        assert "https://github.com/o/r/pull/123" in result.stdout

    def test_unmerged_shows_no_review_when_missing(self, tmp_path: Path):
        """Unmerged output shows 'no review' when no review exists."""
        store, task, git = setup_unmerged_env(tmp_path)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "review:" in result.stdout
        assert "no review" in result.stdout

    def test_unmerged_always_shows_completion_time(self, tmp_path: Path):
        """Completion time is shown even for tasks with improve tasks."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Root task",
        )

        # Override completion time to a specific date
        task.completed_at = datetime(2026, 2, 12, 10, 30, tzinfo=UTC)
        store.update(task)

        improve = store.add("Improve root task", task_type="improve")
        improve.status = "completed"
        improve.completed_at = datetime.now(UTC)
        improve.branch = "feature/test"
        improve.based_on = task.id
        improve.merge_status = "unmerged"
        store.update(improve)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        # Completion date should appear
        assert "2026-02-12" in result.stdout


class TestUnmergedUnifiedQueryOutput:
    @pytest.fixture(autouse=True)
    def _stub_github(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(query_cli, "GitHub", _UnavailableGitHub)

    def test_unmerged_default_text_is_slim_and_scannable(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        store, task, _git = _setup_unmerged_env_fast(
            tmp_path,
            task_prompt=(
                "Ship slimmer unmerged view\n\n"
                "Context:\n- this should not render verbatim\n\n"
                "Acceptance:\n- one line only"
            ),
        )

        review = store.add("Review slim output", task_type="review")
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.depends_on = task.id
        review.output_content = "Verdict: APPROVED"
        store.update(review)

        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=5,
            json=False,
            fields=None,
        )

        result = query_cli.cmd_unmerged(args, git=_FastUnmergedGit())

        captured = capsys.readouterr()
        assert result == 0
        header_line = next(line for line in captured.out.splitlines() if line.startswith("⚡ "))
        assert "Ship slimmer unmerged view" in header_line
        assert "Context:" not in header_line
        assert "Acceptance:" not in header_line
        assert "lineage:" in captured.out
        assert "branch:" in captured.out
        assert "review: reviewed [✓ approved]" in " ".join(captured.out.split())

    def test_unmerged_default_text_surfaces_conflicts_on_own_line(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        _store, task, _git = _setup_unmerged_env_fast(
            tmp_path,
            task_prompt="Conflicting unmerged branch",
            branch="feature/conflicting-unmerged-branch",
        )
        fake_git = _FastUnmergedGit()
        fake_git._can_merge[("feature/conflicting-unmerged-branch", "main")] = False

        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=5,
            json=False,
            fields=None,
        )

        result = query_cli.cmd_unmerged(args, git=fake_git)

        captured = capsys.readouterr()
        assert result == 0
        assert f"⚡ {task.id}" in captured.out
        assert "branch: feature/conflicting-unmerged-branch" in captured.out
        assert "has conflicts" in captured.out
        assert "merge: has conflicts" in captured.out

    def test_unmerged_json_empty_results_returns_empty_array_without_human_message(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        _setup_unmerged_env_fast(
            tmp_path,
            task_prompt="No-commit task",
            branch="feature/no-commit-task",
            has_commits=False,
            merge_status=None,
        )

        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=5,
            json=True,
            fields=None,
        )

        result = query_cli.cmd_unmerged(args, git=_FastUnmergedGit())

        captured = capsys.readouterr()
        assert result == 0
        assert json.loads(captured.out) == []
        assert "No unmerged tasks" not in captured.out

    def test_unmerged_json_migration_message_goes_to_stderr_and_stdout_stays_json(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        setup_config(tmp_path)
        make_store(tmp_path)
        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=5,
            json=True,
            fields=None,
        )

        with patch("gza.db.needs_merge_status_migration", return_value=True):
            result = query_cli.cmd_unmerged(args, git=_FastUnmergedGit())

        captured = capsys.readouterr()
        assert result == 0
        assert json.loads(captured.out) == []
        assert "Migrating merge status" not in captured.out
        assert "Migrating merge status" in captured.err

    def test_unmerged_text_fields_skip_lineage_rendering_work(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        _setup_unmerged_env_fast(tmp_path, task_prompt="Text output task")
        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=5,
            json=False,
            fields="id,prompt",
        )

        with patch("gza.cli.query._format_lineage", side_effect=AssertionError("lineage render should be skipped")):
            result = query_cli.cmd_unmerged(args, git=_FastUnmergedGit())

        captured = capsys.readouterr()
        assert result == 0
        assert "id:" in captured.out
        assert "prompt: Text output task" in captured.out

    def test_unmerged_json_fields_support_narrow_projection_and_keep_progress_on_stderr(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        _setup_unmerged_env_fast(tmp_path, task_prompt="JSON output task")

        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=5,
            json=True,
            fields="id,prompt",
        )

        result = query_cli.cmd_unmerged(args, git=_FastUnmergedGit())

        captured = capsys.readouterr()
        assert result == 0
        payload = json.loads(captured.out)
        assert payload == [{"id": payload[0]["id"], "prompt": "JSON output task"}]
        assert "Progress:" in captured.err
        assert "On branch" not in captured.out

    def test_unmerged_json_fields_expose_merge_unit_metadata(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        _setup_unmerged_env_fast(tmp_path, task_prompt="Unit metadata task")

        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=5,
            json=True,
            fields="id,merge_unit_id,merge_unit_state,source_branch,target_branch",
        )

        result = query_cli.cmd_unmerged(args, git=_FastUnmergedGit())

        captured = capsys.readouterr()
        assert result == 0
        payload = json.loads(captured.out)
        assert payload[0]["merge_unit_id"].startswith("testproject-mu-")
        assert payload[0]["merge_unit_state"] == "unmerged"
        assert payload[0]["source_branch"] == "feature/test"
        assert payload[0]["target_branch"] == "main"

    def test_unmerged_json_unions_unit_backed_and_legacy_candidates_during_default_refresh(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        unit_task = store.add("Unit-backed task", task_type="implement")
        unit_task.status = "completed"
        unit_task.completed_at = datetime.now(UTC)
        unit_task.branch = "feature/unit-backed"
        unit_task.has_commits = True
        unit_task.merge_status = "unmerged"
        store.update(unit_task)
        assert unit_task.id is not None
        existing_unit = store.get_or_create_merge_unit_for_task(unit_task)
        assert existing_unit is not None

        legacy_task = store.add("Legacy task pending backfill", task_type="implement")
        legacy_task.status = "completed"
        legacy_task.completed_at = datetime.now(UTC)
        legacy_task.branch = "feature/legacy-backfill"
        legacy_task.has_commits = True
        legacy_task.merge_status = "unmerged"
        store.update(legacy_task)
        assert legacy_task.id is not None
        assert store.resolve_merge_unit_for_task(legacy_task.id) is None

        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=0,
            json=True,
            fields="id,prompt,merge_unit_id",
        )

        result = query_cli.cmd_unmerged(args, git=_FastUnmergedGit())

        captured = capsys.readouterr()
        assert result == 0
        payload = json.loads(captured.out)
        assert [row["prompt"] for row in payload] == [
            "Legacy task pending backfill",
            "Unit-backed task",
        ]
        assert all(row["merge_unit_id"] for row in payload)
        backfilled_unit = store.resolve_merge_unit_for_task(legacy_task.id)
        assert backfilled_unit is not None
        assert backfilled_unit.state == "unmerged"

    def test_unmerged_default_refresh_ignores_historical_merged_unit_when_branch_is_reused(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        historical = store.add("Historical merged task", task_type="implement")
        historical.status = "completed"
        historical.completed_at = datetime.now(UTC)
        historical.branch = "feature/reused"
        historical.has_commits = True
        historical.merge_status = "unmerged"
        store.update(historical)
        assert historical.id is not None
        historical_unit = store.get_or_create_merge_unit_for_task(historical)
        assert historical_unit is not None
        store.set_merge_unit_state(historical_unit.id, "merged")

        unrelated = store.add("Current reused branch task", task_type="implement")
        unrelated.status = "completed"
        unrelated.completed_at = datetime.now(UTC)
        unrelated.branch = "feature/reused"
        unrelated.has_commits = True
        unrelated.merge_status = "unmerged"
        store.update(unrelated)
        assert unrelated.id is not None
        unrelated_unit = store.get_or_create_merge_unit_for_task(unrelated)
        assert unrelated_unit is not None
        assert unrelated_unit.id != historical_unit.id

        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=0,
            json=True,
            fields="id,prompt,merge_unit_id",
        )

        result = query_cli.cmd_unmerged(args, git=_FastUnmergedGit())

        captured = capsys.readouterr()
        assert result == 0
        assert json.loads(captured.out) == [
            {
                "id": unrelated.id,
                "prompt": "Current reused branch task",
                "merge_unit_id": unrelated_unit.id,
            }
        ]

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

    def test_unmerged_live_target_json_fields_use_live_state_not_default_target_unit(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        store, task, git = _setup_unmerged_env_fast(tmp_path, task_prompt="Live target unit metadata task")
        assert task.id is not None
        unit = store.get_or_create_merge_unit_for_task(task)
        assert unit is not None
        store.set_merge_unit_state(unit.id, "merged")

        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target="release",
            fetch=False,
            limit=5,
            json=True,
            fields="id,merge_unit_id,merge_unit_state,target_branch",
        )

        result = query_cli.cmd_unmerged(args, git=git)

        captured = capsys.readouterr()
        assert result == 0
        payload = json.loads(captured.out)
        assert payload[0]["target_branch"] == "release"
        assert payload[0]["merge_unit_state"] == "unmerged"
        assert payload[0]["merge_unit_id"] is None

    def test_unmerged_live_target_text_fields_use_live_state_not_default_target_unit(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        store, task, git = _setup_unmerged_env_fast(tmp_path, task_prompt="Live target text metadata task")
        assert task.id is not None
        unit = store.get_or_create_merge_unit_for_task(task)
        assert unit is not None
        store.set_merge_unit_state(unit.id, "merged")

        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target="release",
            fetch=False,
            limit=5,
            json=False,
            fields="id,merge_unit_state,target_branch",
        )

        result = query_cli.cmd_unmerged(args, git=git)

        captured = capsys.readouterr()
        assert result == 0
        assert "target_branch: release" in captured.out
        assert "merge_unit_state: unmerged" in captured.out

    def test_unmerged_text_fields_support_single_and_multi_projection(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        _store, task, _git = _setup_unmerged_env_fast(tmp_path, task_prompt="Projected text row")

        multi_args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=5,
            json=False,
            fields="id,prompt",
        )
        multi_result = query_cli.cmd_unmerged(multi_args, git=_FastUnmergedGit())
        multi = capsys.readouterr()
        assert multi_result == 0
        assert "On branch main" in multi.out
        assert f"id: {task.id}" in multi.out
        assert "prompt: Projected text row" in multi.out

        single_args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=5,
            json=False,
            fields="id",
        )
        single_result = query_cli.cmd_unmerged(single_args, git=_FastUnmergedGit())
        single = capsys.readouterr()
        assert single_result == 0
        value_lines = [line for line in single.out.splitlines() if line and not line.startswith("On branch ")]
        assert value_lines[-1] == task.id

    def test_merged_json_filters_by_source_and_last_days(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        manual = store.add("Manual merged", task_type="implement")
        store.mark_completed(manual, has_commits=True, branch="feature/manual-merged")
        assert manual.id is not None
        manual_unit = store.resolve_merge_unit_for_task(manual.id)
        assert manual_unit is not None
        store.set_merge_unit_state(
            manual_unit.id,
            "merged",
            merge_source="manual",
            merged_at=datetime.now(UTC) - timedelta(days=2),
        )

        advance = store.add("Advance merged", task_type="implement")
        store.mark_completed(advance, has_commits=True, branch="feature/advance-merged")
        assert advance.id is not None
        advance_unit = store.resolve_merge_unit_for_task(advance.id)
        assert advance_unit is not None
        store.set_merge_unit_state(
            advance_unit.id,
            "merged",
            merge_source="advance",
            merged_at=datetime.now(UTC) - timedelta(days=10),
        )

        args = argparse.Namespace(
            project_dir=tmp_path,
            source="manual",
            last_days=7,
            since=None,
            fields="merge_unit_id,merge_source,branch",
            list_fields=False,
            json=True,
        )

        result = query_cli.cmd_merged(args)
        captured = capsys.readouterr()

        assert result == 0
        assert json.loads(captured.out) == [
            {
                "merge_unit_id": manual_unit.id,
                "merge_source": "manual",
                "branch": "feature/manual-merged",
            }
        ]

    def test_unmerged_text_fields_unknown_field_errors_clearly(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        _setup_unmerged_env_fast(tmp_path, task_prompt="Unknown field row")

        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=5,
            json=False,
            fields="id,nope",
        )

        result = query_cli.cmd_unmerged(args, git=_FastUnmergedGit())

        captured = capsys.readouterr()
        assert result == 2
        assert "unknown field for gza unmerged: nope" in captured.err
        assert "valid fields:" in captured.err
        assert "Run uv run gza unmerged --list-fields to list valid fields." in captured.err

    def test_unmerged_list_fields_prints_valid_projection_choices(self, tmp_path: Path) -> None:
        setup_config(tmp_path)

        result = run_gza("unmerged", "--list-fields", "--project", str(tmp_path))

        assert result.returncode == 0
        assert result.stdout.strip() == _projection_list_stdout("unmerged")
        assert result.stderr == ""

    @pytest.mark.parametrize("flag_args", [
        ("--view", "flat"),
        ("--preset", "json_minimal"),
        ("--commits-only",),
        ("--all",),
        ("--update",),
    ])
    def test_unmerged_removed_compatibility_flags_error(self, tmp_path: Path, flag_args: tuple[str, ...]) -> None:
        result = run_gza("unmerged", *flag_args, "--project", str(tmp_path))

        assert result.returncode != 0
        assert "unrecognized arguments" in result.stderr

    def test_unmerged_json_limit_footer_stays_off_stdout(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        store, first_task, _git = _setup_unmerged_env_fast(
            tmp_path,
            task_prompt="JSON row 0",
            branch="feature/json-0",
        )
        first_task.completed_at = datetime(2026, 2, 12, 10, 0, tzinfo=UTC)
        store.update(first_task)

        for index in range(1, 6):
            task = store.add(f"JSON row {index}", task_type="implement")
            task.status = "completed"
            task.completed_at = datetime(2026, 2, 12, 10 + index, 0, tzinfo=UTC)
            task.branch = f"feature/json-{index}"
            task.has_commits = True
            task.merge_status = "unmerged"
            store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=5,
            json=True,
            fields=None,
        )

        result = query_cli.cmd_unmerged(args, git=_FastUnmergedGit())

        captured = capsys.readouterr()
        assert result == 0
        payload = json.loads(captured.out)
        assert len(payload) == 5
        assert [row["prompt"] for row in payload] == [
            "JSON row 5",
            "JSON row 4",
            "JSON row 3",
            "JSON row 2",
            "JSON row 1",
        ]
        assert "Showing 5 of 6" not in captured.out
        assert "Showing 5 of 6" in captured.err

    def test_unmerged_same_branch_pending_descendant_does_not_replace_completed_representative(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        store, root_task, _git = _setup_unmerged_env_fast(
            tmp_path,
            task_prompt="Completed unmerged implementation",
            branch="feature/shared-branch",
        )
        root_task.completed_at = datetime(2026, 2, 12, 10, 0, tzinfo=UTC)
        root_task.has_commits = True
        root_task.merge_status = "unmerged"
        store.update(root_task)

        pending_descendant = store.add("Pending same-branch follow-up", task_type="implement")
        pending_descendant.status = "pending"
        pending_descendant.created_at = datetime(2026, 2, 12, 11, 0, tzinfo=UTC)
        pending_descendant.based_on = root_task.id
        pending_descendant.branch = "feature/shared-branch"
        pending_descendant.same_branch = True
        pending_descendant.has_commits = False
        store.update(pending_descendant)

        rich_args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=5,
            json=False,
            fields=None,
        )
        block_args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=5,
            json=False,
            fields="id,prompt,status",
        )
        json_args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=5,
            json=True,
            fields="id,prompt,status",
        )

        rich_result = query_cli.cmd_unmerged(rich_args, git=_FastUnmergedGit())
        rich_output = capsys.readouterr()
        assert rich_result == 0
        rich_block = _unmerged_branch_block(rich_output.out, "feature/shared-branch")
        rich_header = next(line for line in rich_block.splitlines() if line.startswith("⚡ "))
        assert rich_header.endswith("Completed unmerged implementation")
        assert root_task.id in rich_header

        block_result = query_cli.cmd_unmerged(block_args, git=_FastUnmergedGit())
        block_output = capsys.readouterr()
        assert block_result == 0
        assert f"id: {root_task.id}" in block_output.out
        assert "prompt: Completed unmerged implementation" in block_output.out
        assert "Pending same-branch follow-up" not in block_output.out

        json_result = query_cli.cmd_unmerged(json_args, git=_FastUnmergedGit())
        json_output = capsys.readouterr()
        assert json_result == 0
        assert json.loads(json_output.out) == [
            {
                "id": root_task.id,
                "prompt": "Completed unmerged implementation",
                "status": "completed",
            }
        ]

    def test_unmerged_json_rebase_representative_with_branchless_plan_returns_branch_owner_row(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Branchless plan root", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime(2026, 2, 12, 9, 0, tzinfo=UTC)
        store.update(plan)
        assert plan.id is not None

        implement = store.add("Actionable branch owner", task_type="implement", depends_on=plan.id)
        implement.status = "completed"
        implement.completed_at = datetime(2026, 2, 12, 10, 0, tzinfo=UTC)
        implement.branch = "feature/rebase-representative"
        implement.has_commits = True
        store.update(implement)
        assert implement.id is not None

        rebase = store.add("Selected representative rebase", task_type="rebase", based_on=implement.id)
        rebase.status = "completed"
        rebase.completed_at = datetime(2026, 2, 12, 11, 0, tzinfo=UTC)
        rebase.branch = "feature/rebase-representative"
        rebase.has_commits = True
        store.update(rebase)
        assert rebase.id is not None

        unit = store.create_merge_unit(
            source_branch="feature/rebase-representative",
            target_branch="main",
            owner_task_id=plan.id,
            state="unmerged",
        )
        store.attach_task_to_merge_unit(implement.id, unit.id, "owner")
        store.attach_task_to_merge_unit(rebase.id, unit.id, "rebase")

        selected = store.resolve_merge_unit_representative_task(unit, require_actionable=True)
        assert selected is not None
        assert selected.id == rebase.id

        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=5,
            json=True,
            fields="id,prompt,branch",
        )

        result = query_cli.cmd_unmerged(args, git=_FastUnmergedGit())

        captured = capsys.readouterr()
        assert result == 0
        assert json.loads(captured.out) == [
            {
                "id": implement.id,
                "prompt": "Actionable branch owner",
                "branch": "feature/rebase-representative",
            }
        ]

    def test_unmerged_json_id_prompt_projection_skips_lineage_rendering_work(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        _store, task, _git = _setup_unmerged_env_fast(tmp_path, task_prompt="JSON output task")
        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=5,
            json=True,
            fields="id,prompt",
        )

        with patch("gza.cli.query._format_lineage", side_effect=AssertionError("lineage render should be skipped")):
            result = query_cli.cmd_unmerged(args, git=_FastUnmergedGit())

        captured = capsys.readouterr()
        assert result == 0
        assert json.loads(captured.out) == [{"id": task.id, "prompt": "JSON output task"}]
        assert "Progress:" in captured.err

    def test_unmerged_default_text_still_computes_descendants_only_lineage_text(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        _setup_unmerged_env_fast(tmp_path, task_prompt="Rich output task")
        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=5,
            json=False,
            fields=None,
        )
        original_format_lineage = query_cli._format_lineage

        with patch("gza.cli.query.GitHub", _UnavailableGitHub):
            with patch("gza.cli.query._format_lineage", wraps=original_format_lineage) as format_lineage:
                result = query_cli.cmd_unmerged(args, git=_FastUnmergedGit())

        captured = capsys.readouterr()
        assert result == 0
        assert format_lineage.call_count > 0
        assert "lineage:" in captured.out
        assert "Rich output task" in captured.out

    def test_unmerged_default_text_attaches_review_verdict_color_via_theme(self, tmp_path: Path) -> None:
        import gza.colors as current_colors

        store, task, _git = _setup_unmerged_env_fast(tmp_path, task_prompt="Color verdict task")
        review = store.add("Review color verdict", task_type="review")
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.depends_on = task.id
        review.output_content = "Verdict: APPROVED"
        store.update(review)

        config = query_cli.Config.load(tmp_path)
        store = query_cli.get_store(config, open_mode="readwrite")
        service = query_cli._TaskQueryService(store)
        query = query_cli._TaskQueryPresets.unmerged(
            branch_owner_ids=(task.id,),
            task_ids=(task.id,),
            limit=5,
            mode="rich",
            projection=query_cli._TaskProjectionSpec(preset=query_cli._TaskProjectionPreset.UNMERGED_DEFAULT),
        )
        result = service.run(query)
        enriched = query_cli._enrich_unmerged_result(
            result,
            store=store,
            config=config,
            git_client=_FastUnmergedGit(),
            target_branch="main",
            default_branch="main",
        )

        rendered = enriched.render("rich")
        approved_color = current_colors.UNMERGED_COLORS.review_approved
        assert f"[{approved_color}]✓ approved[/{approved_color}]" in rendered

    @pytest.mark.parametrize(
        ("verdict_text", "score", "badge_text", "expected_color_attr"),
        [
            ("APPROVED", 91, "✓ approved", "review_approved"),
            (
                "APPROVED_WITH_FOLLOWUPS",
                72,
                "↺ approved with follow-ups",
                "review_followups",
            ),
            (
                "CHANGES_REQUESTED",
                34,
                "⚠ changes requested",
                "review_changes",
            ),
        ],
    )
    def test_unmerged_scored_review_badge_keeps_single_score_and_verdict_theme(
        self,
        tmp_path: Path,
        verdict_text: str,
        score: int,
        badge_text: str,
        expected_color_attr: str,
    ) -> None:
        import gza.colors as current_colors

        store, task, _git = _setup_unmerged_env_fast(tmp_path, task_prompt="Scored verdict task")
        review = store.add("Review scored verdict", task_type="review")
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.depends_on = task.id
        review.output_content = f"Verdict: {verdict_text}"
        review.review_score = score
        store.update(review)

        config = query_cli.Config.load(tmp_path)
        store = query_cli.get_store(config, open_mode="readwrite")
        service = query_cli._TaskQueryService(store)
        query = query_cli._TaskQueryPresets.unmerged(
            branch_owner_ids=(task.id,),
            task_ids=(task.id,),
            limit=5,
            mode="rich",
            projection=query_cli._TaskProjectionSpec(preset=query_cli._TaskProjectionPreset.UNMERGED_DEFAULT),
        )
        result = service.run(query)
        enriched = query_cli._enrich_unmerged_result(
            result,
            store=store,
            config=config,
            git_client=_FastUnmergedGit(),
            target_branch="main",
            default_branch="main",
        )

        rendered = enriched.render("rich")
        expected_color = getattr(current_colors.UNMERGED_COLORS, expected_color_attr)
        assert rendered.count(f"({score})") == 1
        assert f"[{expected_color}]{badge_text} ({score})[/{expected_color}]" in rendered

    def test_unmerged_progress_logs_counts_for_refresh_query_and_render(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        _setup_unmerged_env_fast(tmp_path, task_prompt="Progress task")

        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=5,
            json=False,
            fields=None,
        )

        result = query_cli.cmd_unmerged(args, git=_FastUnmergedGit())

        captured = capsys.readouterr()
        assert result == 0
        assert "Progress:" not in captured.out
        assert re.search(r"Progress: refreshing canonical merge truth for \d+ candidate tasks", captured.err)
        assert re.search(r"Progress: running unmerged query over \d+ task rows for \d+ selected branches", captured.err)
        assert re.search(r"Progress: rendering \d+ row\(s\) from \d+ filtered result\(s\) as rich", captured.err)

    def test_unmerged_descendants_only_lineage_excludes_depends_on_only_child(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        store, impl, _git = _setup_unmerged_env_fast(tmp_path, task_prompt="Owner implementation")
        impl.completed_at = datetime(2026, 2, 12, 10, 0, tzinfo=UTC)
        store.update(impl)

        review = store.add("Review owner", task_type="review")
        review.status = "completed"
        review.completed_at = datetime(2026, 2, 12, 11, 0, tzinfo=UTC)
        review.depends_on = impl.id
        review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(review)

        improve = store.add("Improve owner", task_type="improve")
        improve.status = "completed"
        improve.completed_at = datetime(2026, 2, 12, 12, 0, tzinfo=UTC)
        improve.based_on = impl.id
        improve.depends_on = review.id
        improve.branch = "feature/test"
        improve.same_branch = True
        store.update(improve)

        depends_only_impl = store.add("Depends-only implement noise", task_type="implement")
        depends_only_impl.status = "completed"
        depends_only_impl.completed_at = datetime(2026, 2, 12, 13, 0, tzinfo=UTC)
        depends_only_impl.depends_on = impl.id
        depends_only_impl.branch = "feature/test"
        depends_only_impl.same_branch = True
        store.update(depends_only_impl)

        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=5,
            json=False,
            fields=None,
        )

        result = query_cli.cmd_unmerged(args, git=_FastUnmergedGit())

        captured = capsys.readouterr()
        assert result == 0
        normalized = " ".join(captured.out.split())
        assert impl.id in normalized
        assert review.id in normalized
        assert improve.id in normalized
        assert "Depends-only implement noise" not in normalized
        assert depends_only_impl.id not in normalized

    def test_descendants_only_unmerged_lineage_tree_dedupes_direct_review_children(self) -> None:
        store = MagicMock()
        owner = Task(
            id="gza-1",
            prompt="Owner implementation",
            status="completed",
            task_type="implement",
            created_at=datetime(2026, 2, 12, 10, 0, tzinfo=UTC),
        )
        review_a = Task(
            id="gza-2",
            prompt="Review A",
            status="completed",
            task_type="review",
            based_on=owner.id,
            depends_on=owner.id,
            created_at=datetime(2026, 2, 12, 11, 0, tzinfo=UTC),
        )
        review_b = Task(
            id="gza-3",
            prompt="Review B",
            status="completed",
            task_type="review",
            based_on=owner.id,
            depends_on=owner.id,
            created_at=datetime(2026, 2, 12, 12, 0, tzinfo=UTC),
        )

        store.get_based_on_children.side_effect = lambda task_id: (
            [review_a, review_b] if task_id == owner.id else []
        )
        store.get_reviews_for_task.side_effect = lambda task_id: (
            [review_a, review_b] if task_id == owner.id else []
        )

        tree = query_cli._descendants_only_unmerged_lineage_tree(  # noqa: SLF001
            store,
            owner_task=owner,
        )

        assert tree is not None
        assert [child.task.id for child in tree.children].count(review_a.id) == 1
        assert [child.task.id for child in tree.children].count(review_b.id) == 1

    def test_unmerged_descendants_only_lineage_renders_each_review_once(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        store, impl, _git = _setup_unmerged_env_fast(tmp_path, task_prompt="Owner implementation")
        impl.completed_at = datetime(2026, 2, 12, 10, 0, tzinfo=UTC)
        store.update(impl)

        review_a = store.add("Review one", task_type="review", based_on=impl.id, depends_on=impl.id)
        review_a.status = "completed"
        review_a.completed_at = datetime(2026, 2, 12, 11, 0, tzinfo=UTC)
        review_a.output_content = "Verdict: APPROVED"
        store.update(review_a)
        assert review_a.id is not None

        review_b = store.add("Review two", task_type="review", based_on=impl.id, depends_on=impl.id)
        review_b.status = "completed"
        review_b.completed_at = datetime(2026, 2, 12, 12, 0, tzinfo=UTC)
        review_b.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(review_b)
        assert review_b.id is not None

        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=5,
            json=False,
            fields=None,
        )

        result = query_cli.cmd_unmerged(args, git=_FastUnmergedGit())

        captured = capsys.readouterr()
        assert result == 0
        normalized = " ".join(captured.out.split())
        assert normalized.count(str(review_a.id)) == 1
        assert normalized.count(str(review_b.id)) == 1

    def test_unmerged_shows_descendants_only_lineage_without_ancestors(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        store, root, _git = _setup_unmerged_env_fast(
            tmp_path,
            task_prompt="Merged branch A root",
            branch="feature/a-root",
            merge_status="merged",
        )
        root.completed_at = datetime(2026, 2, 12, 9, 0, tzinfo=UTC)
        store.update(root)
        assert root.id is not None

        branch_b = store.add("Unmerged branch B task", task_type="implement")
        branch_b.status = "completed"
        branch_b.completed_at = datetime(2026, 2, 12, 10, 0, tzinfo=UTC)
        branch_b.based_on = root.id
        branch_b.branch = "feature/b-task"
        branch_b.has_commits = True
        branch_b.merge_status = "unmerged"
        store.update(branch_b)
        assert branch_b.id is not None

        merged_sibling = store.add("Merged branch A sibling", task_type="implement")
        merged_sibling.status = "completed"
        merged_sibling.completed_at = datetime(2026, 2, 12, 11, 0, tzinfo=UTC)
        merged_sibling.based_on = root.id
        merged_sibling.branch = "feature/a-root"
        merged_sibling.same_branch = True
        merged_sibling.has_commits = True
        merged_sibling.merge_status = "merged"
        store.update(merged_sibling)
        assert merged_sibling.id is not None

        merged_sibling_child = store.add("Merged branch A child", task_type="review")
        merged_sibling_child.status = "completed"
        merged_sibling_child.completed_at = datetime(2026, 2, 12, 12, 0, tzinfo=UTC)
        merged_sibling_child.depends_on = merged_sibling.id
        merged_sibling_child.output_content = "Verdict: APPROVED"
        store.update(merged_sibling_child)

        fast_git = _FastUnmergedGit()
        fast_git._merged[("feature/a-root", "main")] = True

        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=5,
            json=False,
            fields=None,
        )

        result = query_cli.cmd_unmerged(args, git=fast_git)

        captured = capsys.readouterr()
        assert result == 0
        assert "Unmerged branch B task" in captured.out
        assert branch_b.id in captured.out
        assert "Merged branch A root" not in captured.out
        assert root.id not in captured.out
        assert "Merged branch A sibling" not in captured.out
        assert merged_sibling.id not in captured.out
        assert "Merged branch A child" not in captured.out


class TestFailureReasonField:
    """Tests for the failure_reason field on Task."""

    def test_failure_reason_persisted(self, tmp_path: Path):
        """failure_reason field is saved and loaded from the database."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task")
        task.status = "failed"
        task.failure_reason = "Claude returned exit code 1"
        store.update(task)

        loaded = store.get(task.id)
        assert loaded is not None
        assert loaded.failure_reason == "Claude returned exit code 1"

    def test_failure_reason_defaults_to_none(self, tmp_path: Path):
        """failure_reason defaults to None for new tasks."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task")
        assert task.failure_reason is None

        loaded = store.get(task.id)
        assert loaded is not None
        assert loaded.failure_reason is None


class TestNextCommandWithDependencies:
    """Tests for 'gza next' command with dependencies."""

    def test_next_skips_blocked_tasks(self, tmp_path: Path):
        """Next command skips tasks blocked by dependencies."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create task chain
        task1 = store.add("First task")
        store.add("Blocked task", depends_on=task1.id)
        store.add("Independent task")

        result = run_gza("next", "--project", str(tmp_path))

        assert result.returncode == 0
        # Should show task1 or task3, but not task2
        assert "Blocked task" not in result.stdout or "blocked" in result.stdout.lower()

    def test_next_all_shows_blocked_tasks(self, tmp_path: Path):
        """Next --all command shows blocked tasks."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create task chain
        task1 = store.add("First task")
        store.add("Blocked task", depends_on=task1.id)

        result = run_gza("next", "--all", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "First task" in result.stdout
        assert "Blocked task" in result.stdout

    def test_next_all_marks_dropped_dependency_blockers_explicitly(self, tmp_path: Path):
        """Next --all carries blocker status so dropped dependencies are visible."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        prereq = store.add("Dropped prereq")
        prereq.status = "dropped"
        prereq.completed_at = datetime.now(UTC)
        store.update(prereq)
        assert prereq.id is not None

        dependent = store.add("Blocked by dropped prereq", depends_on=prereq.id)
        assert dependent.id is not None

        result = run_gza("next", "--all", "--project", str(tmp_path))

        assert result.returncode == 0
        normalized = " ".join(result.stdout.split())
        assert dependent.id in normalized
        assert f"blocked-by-dropped {prereq.id}" in normalized

    def test_next_shows_blocked_count(self, tmp_path: Path):
        """Next command shows count of blocked tasks."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create blocked tasks
        task1 = store.add("First task")
        store.add("Blocked task 1", depends_on=task1.id)
        store.add("Blocked task 2", depends_on=task1.id)
        store.add("Independent task")

        result = run_gza("next", "--project", str(tmp_path))

        assert result.returncode == 0
        # Should mention 2 blocked tasks
        assert "2" in result.stdout and "blocked" in result.stdout.lower()

    def test_next_picks_up_task_after_clearing_dependency(self, tmp_path: Path):
        """A blocked pending task becomes runnable after its dependency is cleared."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        blocker = store.add("Dependency blocker")
        downstream = store.add("Blocked task", depends_on=blocker.id)
        assert downstream.id is not None
        assert store.is_task_blocked(downstream) == (True, blocker.id, "pending")

        before = run_gza("next", "--project", str(tmp_path))
        assert before.returncode == 0
        assert "Blocked task" not in before.stdout
        assert "blocked by dependencies" in before.stdout

        edit_result = run_gza("edit", downstream.id, "--clear-depends-on", "--project", str(tmp_path))
        assert edit_result.returncode == 0

        refreshed = store.get(downstream.id)
        assert refreshed is not None
        assert refreshed.depends_on is None
        assert store.is_task_blocked(refreshed) == (False, None, None)

        after = run_gza("next", "--project", str(tmp_path))
        assert after.returncode == 0
        assert "Blocked task" in after.stdout
        assert "blocked by dependencies" not in after.stdout

    def test_next_excludes_internal_and_only_shows_blocked_via_blocked_path(self, tmp_path: Path):
        """Internal pending tasks should not appear in runnable or blocked output."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        blocker = store.add("Dependency blocker")
        store.add("Blocked task", depends_on=blocker.id)
        store.add("Internal pending", task_type="internal")
        store.add("Runnable task")

        result = run_gza("next", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Runnable task" in result.stdout
        assert "Internal pending" not in result.stdout
        assert "Blocked task" not in result.stdout
        assert "blocked by dependencies" in result.stdout

        result_all = run_gza("next", "--all", "--project", str(tmp_path))
        assert result_all.returncode == 0
        assert "Runnable task" in result_all.stdout
        assert "Blocked task" in result_all.stdout
        assert "Internal pending" not in result_all.stdout


class TestKillCommand:
    """Tests for the 'gza kill' command."""

    def test_kill_refuses_non_in_progress_task(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """kill must reject tasks that are not in_progress."""
        from gza.cli.query import cmd_kill
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        store = SqliteTaskStore(tmp_path / ".gza" / "gza.db")
        task = store.add("Completed task")
        assert task.id is not None

        args = argparse.Namespace(project_dir=tmp_path, task_id=task.id, all=False, force=False)
        with patch("gza.cli.query.os.kill") as mock_kill:
            rc = cmd_kill(args)

        captured = capsys.readouterr()
        assert rc == 1
        assert "not running" in captured.out
        mock_kill.assert_not_called()

    def test_kill_task_not_found(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """kill must report an error for unknown task IDs."""
        from gza.cli.query import cmd_kill

        setup_config(tmp_path)

        args = argparse.Namespace(project_dir=tmp_path, task_id="testproject-99999", all=False, force=False)
        rc = cmd_kill(args)

        captured = capsys.readouterr()
        assert rc == 1
        assert "not found" in captured.out

    def test_kill_signals_via_worker_record(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """kill sends SIGTERM to the worker PID found in the registry."""
        from gza.cli.query import cmd_kill
        from gza.db import SqliteTaskStore
        from gza.failure_reasons import mark_task_failed_from_cause
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = SqliteTaskStore(tmp_path / ".gza" / "gza.db")
        task = store.add("Running task")
        assert task.id is not None
        task.status = "in_progress"
        task.running_pid = 12345
        store.update(task)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        registry.register(WorkerMetadata(worker_id="w-kill-1", task_id=task.id, pid=12345, status="running"))

        args = argparse.Namespace(project_dir=tmp_path, task_id=task.id, all=False, force=False)
        # Patch os.kill so SIGTERM appears sent and process dies immediately (OSError on signal 0)
        def fake_kill(pid: int, sig: int) -> None:
            if sig == 0:
                raise OSError("no such process")
        with patch("gza.cli.query.os.kill", side_effect=fake_kill) as mock_kill:
            with patch("gza.cli.query.time.sleep"), \
                 patch("gza.cli.query.mark_task_failed_from_cause", wraps=mark_task_failed_from_cause) as mock_mark_failed:
                rc = cmd_kill(args)

        captured = capsys.readouterr()
        assert rc == 0
        assert "Task " in captured.out and "killed" in captured.out
        # Confirm SIGTERM was sent
        import signal
        mock_kill.assert_any_call(12345, signal.SIGTERM)
        # Task status must be KILLED
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "KILLED"
        assert mock_mark_failed.call_count == 1
        assert mock_mark_failed.call_args.kwargs["explicit_reason"] == "KILLED"
        request = registry.consume_interrupt_request(12345)
        assert request is not None
        assert request["signal"] == "SIGTERM"
        assert request["source"] == "gza_kill"

    def test_kill_escalates_to_sigkill_when_process_survives_sigterm(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        """If the process survives SIGTERM for 3 seconds, kill escalates to SIGKILL."""

        from gza.cli.query import cmd_kill
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = SqliteTaskStore(tmp_path / ".gza" / "gza.db")
        task = store.add("Stubborn task")
        assert task.id is not None
        task.status = "in_progress"
        task.running_pid = 22222
        store.update(task)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        registry.register(WorkerMetadata(worker_id="w-kill-2", task_id=task.id, pid=22222, status="running"))

        args = argparse.Namespace(project_dir=tmp_path, task_id=task.id, all=False, force=False)
        # signal 0 succeeds (process still alive after SIGTERM), others succeed silently
        def fake_kill(pid: int, sig: int) -> None:
            pass  # process "survives" every signal check
        with patch("gza.cli.query.os.kill", side_effect=fake_kill):
            with patch("gza.cli.query.time.sleep"):
                rc = cmd_kill(args)

        captured = capsys.readouterr()
        assert rc == 0
        assert "SIGKILL" in captured.out or "escalated" in captured.out.lower()

    def test_kill_force_sends_sigkill_immediately(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """--force skips SIGTERM and sends SIGKILL immediately."""
        import signal as _signal

        from gza.cli.query import cmd_kill
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = SqliteTaskStore(tmp_path / ".gza" / "gza.db")
        task = store.add("Force kill task")
        assert task.id is not None
        task.status = "in_progress"
        task.running_pid = 33333
        store.update(task)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        registry.register(WorkerMetadata(worker_id="w-kill-3", task_id=task.id, pid=33333, status="running"))

        args = argparse.Namespace(project_dir=tmp_path, task_id=task.id, all=False, force=True)
        with patch("gza.cli.query.os.kill") as mock_kill:
            rc = cmd_kill(args)

        assert rc == 0
        mock_kill.assert_called_once_with(33333, _signal.SIGKILL)

    def test_kill_uses_running_pid_when_no_worker_record(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """tmux bug case: no worker record, kill falls back to task.running_pid."""
        import signal as _signal

        from gza.cli.query import cmd_kill
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        store = SqliteTaskStore(tmp_path / ".gza" / "gza.db")
        task = store.add("Orphaned task")
        assert task.id is not None
        task.status = "in_progress"
        task.running_pid = 44444
        store.update(task)

        args = argparse.Namespace(project_dir=tmp_path, task_id=task.id, all=False, force=True)
        with patch("gza.cli.query.os.kill") as mock_kill:
            rc = cmd_kill(args)

        capsys.readouterr()
        assert rc == 0
        mock_kill.assert_called_once_with(44444, _signal.SIGKILL)
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.failure_reason == "KILLED"

    def test_kill_all_kills_all_in_progress_tasks(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """--all kills every in-progress task."""
        import signal as _signal

        from gza.cli.query import cmd_kill
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = SqliteTaskStore(tmp_path / ".gza" / "gza.db")

        task1 = store.add("Task A")
        assert task1.id is not None
        task1.status = "in_progress"
        task1.running_pid = 55555
        store.update(task1)

        task2 = store.add("Task B")
        assert task2.id is not None
        task2.status = "in_progress"
        task2.running_pid = 66666
        store.update(task2)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        registry.register(WorkerMetadata(worker_id="w-all-1", task_id=task1.id, pid=55555, status="running"))
        registry.register(WorkerMetadata(worker_id="w-all-2", task_id=task2.id, pid=66666, status="running"))

        args = argparse.Namespace(project_dir=tmp_path, task_id=None, all=True, force=True)
        with patch("gza.cli.query.os.kill") as mock_kill:
            rc = cmd_kill(args)

        assert rc == 0
        killed_pids = {c.args[0] for c in mock_kill.call_args_list if c.args[1] == _signal.SIGKILL}
        assert {55555, 66666} == killed_pids

    def test_kill_all_no_running_tasks(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """--all exits cleanly with a message when no tasks are running."""
        from gza.cli.query import cmd_kill

        setup_config(tmp_path)

        args = argparse.Namespace(project_dir=tmp_path, task_id=None, all=True, force=False)
        rc = cmd_kill(args)

        captured = capsys.readouterr()
        assert rc == 0
        assert "No running tasks" in captured.out

    def test_kill_all_returns_nonzero_when_some_kills_fail(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        """--all returns 1 if any task could not be killed (e.g. no PID)."""
        from gza.cli.query import cmd_kill
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = SqliteTaskStore(tmp_path / ".gza" / "gza.db")

        # Task with a valid PID — kill will succeed.
        task_ok = store.add("Task with PID")
        assert task_ok.id is not None
        task_ok.status = "in_progress"
        task_ok.running_pid = 55555
        store.update(task_ok)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        registry.register(
            WorkerMetadata(worker_id="w-ok-1", task_id=task_ok.id, pid=55555, status="running")
        )

        # Task with no PID and no worker record — kill will fail.
        task_no_pid = store.add("Task without PID")
        assert task_no_pid.id is not None
        task_no_pid.status = "in_progress"
        task_no_pid.running_pid = None
        store.update(task_no_pid)

        args = argparse.Namespace(project_dir=tmp_path, task_id=None, all=True, force=True)
        with patch("gza.cli.query.os.kill"):
            rc = cmd_kill(args)

        assert rc == 1


class TestLineageCommand:
    """Tests for 'gza lineage <task-id>' command."""

    def test_lineage_single_root_task(self, tmp_path: Path):
        """Lineage command shows a single root task with no children."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Design auth system", "status": "completed", "task_type": "plan"},
        ])

        result = run_gza("lineage", "testproject-1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Design auth system" in result.stdout
        assert "plan" in result.stdout
        assert "completed" in result.stdout

    def test_lineage_includes_node_timestamp(self, tmp_path: Path):
        """Lineage command renders node timestamp using completed/started/created fallback."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        created_at = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
        started_at = datetime(2026, 1, 2, 4, 5, 6, tzinfo=UTC)
        completed_at = datetime(2026, 1, 2, 5, 6, 7, tzinfo=UTC)

        root = store.add("Root task", task_type="implement")
        root.status = "pending"
        root.started_at = None
        root.completed_at = None
        store.update(root)
        with store._connect() as conn:
            conn.execute(
                "UPDATE tasks SET created_at = ? WHERE id = ?",
                (created_at.isoformat(), root.id),
            )

        child = store.add("Started child", task_type="task", based_on=root.id)
        child.status = "in_progress"
        child.created_at = created_at
        child.started_at = started_at
        child.completed_at = None
        store.update(child)

        grandchild = store.add("Completed grandchild", task_type="review", depends_on=child.id)
        grandchild.status = "completed"
        grandchild.created_at = created_at
        grandchild.started_at = started_at
        grandchild.completed_at = completed_at
        store.update(grandchild)

        result = run_gza("lineage", str(root.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "2026-01-02 03:04:05 UTC" in result.stdout
        assert "2026-01-02 04:05:06 UTC" in result.stdout
        assert "2026-01-02 05:06:07 UTC" in result.stdout

    def test_lineage_task_not_found(self, tmp_path: Path):
        """Lineage command returns error for missing task ID."""
        setup_db_with_tasks(tmp_path, [])

        result = run_gza("lineage", "testproject-999999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout.lower() or "not found" in result.stderr.lower()

    def test_lineage_highlights_target_task(self, tmp_path: Path):
        """Lineage command highlights the requested task with an arrow marker."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        now = datetime.now(UTC)

        root = store.add("Design auth system", task_type="plan")
        root.status = "completed"
        root.completed_at = now
        store.update(root)

        child = store.add("Implement auth per plan", task_type="implement", based_on=root.id)
        child.status = "completed"
        child.completed_at = now
        store.update(child)

        result = run_gza("lineage", str(child.id), "--project", str(tmp_path))

        assert result.returncode == 0
        # Arrow marker for the target task
        assert "→" in result.stdout
        # Both tasks shown — collapse whitespace since Rich may wrap long lines
        normalized = " ".join(result.stdout.split())
        assert "Design auth system" in normalized
        assert "Implement auth per plan" in normalized

    def test_lineage_shows_failed_task_with_reason(self, tmp_path: Path):
        """Lineage command shows failure_reason for failed tasks."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        now = datetime.now(UTC)

        task = store.add("Implement feature", task_type="implement")
        task.status = "failed"
        task.failure_reason = "MAX_STEPS"
        task.completed_at = now
        store.update(task)

        result = run_gza("lineage", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "MAX_STEPS" in result.stdout
        normalized = " ".join(result.stdout.split())
        assert "Implement feature" in normalized

    def test_lineage_shows_completed_task_with_completion_reason(self, tmp_path: Path):
        """Lineage command shows completion_reason for completed tasks."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Extraction already merged", task_type="implement")
        task.status = "completed"
        task.completion_reason = "EXTRACTION_ALREADY_MERGED"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = run_gza("lineage", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "EXTRACTION_ALREADY_MERGED" in result.stdout
        normalized = " ".join(result.stdout.split())
        assert "Extraction already merged" in normalized

    def test_lineage_keeps_lineage_status_colors(self, tmp_path: Path) -> None:
        """Standalone lineage command should keep using lineage status colors."""
        from gza.cli.query import cmd_lineage

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Failed lineage task", task_type="implement")
        assert task.id is not None
        task.status = "failed"
        task.failure_reason = "TIMEOUT"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        console = Console(record=True, force_terminal=True, color_system="standard", width=300)
        show_colors = dict(query_cli.SHOW_COLORS_DICT)
        show_colors["status_failed"] = "red"

        with (
            patch.object(query_cli, "console", console),
            patch.object(query_cli, "SHOW_COLORS_DICT", show_colors),
            patch.object(query_cli, "get_task_status_color", return_value="yellow"),
        ):
            exit_code = cmd_lineage(argparse.Namespace(project_dir=tmp_path, task_id=str(task.id)))

        rendered = console.export_text(styles=True, clear=False)
        plain = console.export_text(clear=False)
        assert exit_code == 0
        assert "failed (TIMEOUT)" in plain
        assert "\x1b[33mfailed \x1b[0m" in rendered
        assert "\x1b[31mfailed \x1b[0m" not in rendered

    def test_lineage_full_tree(self, tmp_path: Path):
        """Lineage command renders a multi-level tree with parent and children."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        now = datetime.now(UTC)

        root = store.add("Design auth system", task_type="plan")
        root.status = "completed"
        root.completed_at = now
        store.update(root)

        impl = store.add("Implement auth per plan", task_type="implement", based_on=root.id)
        impl.status = "completed"
        impl.completed_at = now
        store.update(impl)

        review = store.add("Review implementation", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.completed_at = now
        store.update(review)

        result = run_gza("lineage", str(root.id), "--project", str(tmp_path))

        assert result.returncode == 0
        normalized = " ".join(result.stdout.split())
        assert "Design auth system" in normalized
        assert "Implement auth per plan" in normalized
        assert "review" in normalized

    def test_lineage_default_notes_immediate_resume_parent(self, tmp_path: Path) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        parent = store.add("Recover original attempt", task_type="implement")
        assert parent.id is not None
        parent.status = "failed"
        parent.session_id = "sess-1"
        parent.branch = "feature/recover"
        parent.completed_at = datetime.now(UTC)
        store.update(parent)

        child = store.add(
            "Recover original attempt",
            task_type="implement",
            based_on=parent.id,
            recovery_origin="resume",
        )
        assert child.id is not None
        child.status = "completed"
        child.session_id = parent.session_id
        child.branch = parent.branch
        child.completed_at = datetime.now(UTC)
        store.update(child)

        result = run_gza("lineage", str(child.id), "--project", str(tmp_path))

        assert result.returncode == 0
        normalized = " ".join(result.stdout.split())
        assert f"Parents: {parent.id} [resume]. Use --full or --parents-only to inspect ancestors." in normalized
        assert child.id in normalized

    def test_lineage_default_uses_persisted_resume_parent_label_without_matching_session(self, tmp_path: Path) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        parent = store.add("Recover original attempt", task_type="implement")
        assert parent.id is not None
        parent.status = "failed"
        parent.session_id = "sess-parent"
        parent.branch = "feature/recover"
        parent.completed_at = datetime.now(UTC)
        store.update(parent)

        child = store.add(
            "Recover original attempt",
            task_type="implement",
            based_on=parent.id,
            recovery_origin="resume",
        )
        assert child.id is not None
        child.status = "completed"
        child.session_id = "sess-child"
        child.branch = parent.branch
        child.completed_at = datetime.now(UTC)
        store.update(child)

        result = run_gza("lineage", str(child.id), "--project", str(tmp_path))

        assert result.returncode == 0
        normalized = " ".join(result.stdout.split())
        assert f"Parents: {parent.id} [resume]. Use --full or --parents-only to inspect ancestors." in normalized
        assert f"Parents: {parent.id} [retry]." not in normalized

    def test_lineage_default_uses_persisted_retry_parent_label_even_with_matching_session(self, tmp_path: Path) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        parent = store.add("Retry original attempt", task_type="implement")
        assert parent.id is not None
        parent.status = "failed"
        parent.session_id = "sess-parent"
        parent.branch = "feature/retry"
        parent.completed_at = datetime.now(UTC)
        store.update(parent)

        child = store.add(
            "Retry original attempt",
            task_type="implement",
            based_on=parent.id,
            recovery_origin="retry",
        )
        assert child.id is not None
        child.status = "completed"
        child.session_id = parent.session_id
        child.branch = "feature/retry-2"
        child.completed_at = datetime.now(UTC)
        store.update(child)

        result = run_gza("lineage", str(child.id), "--project", str(tmp_path))

        assert result.returncode == 0
        normalized = " ".join(result.stdout.split())
        assert f"Parents: {parent.id} [retry]. Use --full or --parents-only to inspect ancestors." in normalized
        assert f"Parents: {parent.id} [resume]." not in normalized

    def test_lineage_children_only_omits_parent_hint(self, tmp_path: Path) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        parent = store.add("Initial attempt", task_type="implement")
        assert parent.id is not None
        parent.status = "failed"
        parent.completed_at = datetime.now(UTC)
        store.update(parent)

        parent.session_id = "sess-2"
        parent.branch = "feature/retry"
        store.update(parent)

        child = store.add(
            "Retry attempt",
            task_type="implement",
            based_on=parent.id,
            recovery_origin="resume",
        )
        assert child.id is not None
        child.status = "completed"
        child.session_id = parent.session_id
        child.branch = parent.branch
        child.completed_at = datetime.now(UTC)
        store.update(child)

        review = store.add("Review retry", task_type="review", depends_on=child.id)
        assert review.id is not None
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        result = run_gza("lineage", str(child.id), "--children-only", "--project", str(tmp_path))

        assert result.returncode == 0
        normalized = " ".join(result.stdout.split())
        assert "Parents:" not in normalized
        assert child.id in normalized
        assert review.id in normalized
        assert parent.id not in normalized

    def test_lineage_parents_only_shows_ancestor_chain_without_children(self, tmp_path: Path) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Design auth flow", task_type="plan")
        assert plan.id is not None
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        impl = store.add("Implement auth flow", task_type="implement", based_on=plan.id)
        assert impl.id is not None
        impl.status = "completed"
        impl.completed_at = datetime.now(UTC)
        store.update(impl)

        review = store.add("Review auth flow", task_type="review", depends_on=impl.id)
        assert review.id is not None
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        result = run_gza("lineage", str(review.id), "--parents-only", "--project", str(tmp_path))

        assert result.returncode == 0
        normalized = " ".join(result.stdout.split())
        assert "Parents:" not in normalized
        assert plan.id in normalized
        assert impl.id in normalized
        assert review.id in normalized
        assert "children" not in normalized.lower()

    def test_lineage_parent_modes_deduplicate_shared_parent_edge(self, tmp_path: Path) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        parent = store.add("Shared parent", task_type="implement")
        assert parent.id is not None
        parent.status = "completed"
        parent.completed_at = datetime.now(UTC)
        store.update(parent)

        child = store.add(
            "Shared-edge child",
            task_type="implement",
            based_on=parent.id,
            depends_on=parent.id,
        )
        assert child.id is not None
        child.status = "completed"
        child.completed_at = datetime.now(UTC)
        store.update(child)

        parents_only = run_gza("lineage", str(child.id), "--parents-only", "--project", str(tmp_path))

        assert parents_only.returncode == 0
        assert parents_only.stdout.count(child.id) == 1

        full = run_gza("lineage", str(child.id), "--full", "--project", str(tmp_path))

        assert full.returncode == 0
        parents_section = full.stdout.split("Children:", 1)[0]
        assert parents_section.count(child.id) == 1

    def test_lineage_full_shows_parents_and_children_sections(self, tmp_path: Path) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan rollout", task_type="plan")
        assert plan.id is not None
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        impl = store.add("Implement rollout", task_type="implement", based_on=plan.id)
        assert impl.id is not None
        impl.status = "completed"
        impl.completed_at = datetime.now(UTC)
        store.update(impl)

        review = store.add("Review rollout", task_type="review", depends_on=impl.id)
        assert review.id is not None
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        result = run_gza("lineage", str(impl.id), "--full", "--project", str(tmp_path))

        assert result.returncode == 0
        normalized = " ".join(result.stdout.split())
        assert "Parents:" in normalized
        assert "Children:" in normalized
        assert plan.id in normalized
        assert impl.id in normalized
        assert review.id in normalized

    def test_lineage_default_without_parents_keeps_descendant_view(self, tmp_path: Path) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("Standalone implementation", task_type="implement")
        assert root.id is not None
        root.status = "completed"
        root.completed_at = datetime.now(UTC)
        store.update(root)

        review = store.add("Review standalone implementation", task_type="review", depends_on=root.id)
        assert review.id is not None
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        result = run_gza("lineage", str(root.id), "--project", str(tmp_path))

        assert result.returncode == 0
        normalized = " ".join(result.stdout.split())
        assert "Parents:" not in normalized
        assert root.id in normalized
        assert review.id in normalized

    def test_lineage_mode_flags_are_mutually_exclusive(self, tmp_path: Path) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Exclusive modes", task_type="implement")
        assert task.id is not None

        result = run_gza(
            "lineage",
            str(task.id),
            "--full",
            "--parents-only",
            "--project",
            str(tmp_path),
        )

        assert result.returncode != 0
        assert "not allowed with argument" in result.stderr

    def test_lineage_omits_runtime_steps_cost_trailer(self, tmp_path: Path):
        """Lineage command omits runtime/steps/cost stats trailers."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        now = datetime.now(UTC)

        task = store.add("Implement feature", task_type="implement")
        task.status = "completed"
        task.completed_at = now
        task.started_at = datetime(2026, 2, 12, 9, 0, tzinfo=UTC)
        task.duration_seconds = 120.0  # 2 minutes
        task.cost_usd = 0.1234
        task.num_steps_reported = 5
        store.update(task)

        result = run_gza("lineage", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "UTC" in result.stdout
        assert "2m0s" not in result.stdout
        assert "steps" not in result.stdout
        assert "$0.1234" not in result.stdout

    def test_lineage_child_shows_relationship_label(self, tmp_path: Path):
        """Relationship label is shown only when it differs from the task type.

        A review child of type 'review' has rel='review' == type_str='review', so
        the bracket label is suppressed (redundant).  A 'task'-typed child with
        depends_on has rel='depends' != type_str='task', so [depends] IS shown.
        """
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        now = datetime.now(UTC)

        impl = store.add("Implement feature", task_type="implement")
        impl.status = "completed"
        impl.completed_at = now
        store.update(impl)

        # rel="review" == type_str="review" → label suppressed
        review = store.add("Review feature impl", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.completed_at = now
        store.update(review)

        # rel="depends" != type_str="task" → label shown
        dep = store.add("Dependent task", task_type="task", depends_on=impl.id)
        dep.status = "pending"
        store.update(dep)

        result = run_gza("lineage", str(impl.id), "--project", str(tmp_path))

        assert result.returncode == 0
        normalized = " ".join(result.stdout.split())
        assert "[review]" not in normalized
        assert "[depends]" in normalized

    def test_lineage_rebase_child_does_not_render_retry_relationship_label(self, tmp_path: Path):
        """Rebase children should classify as rebase, not retry."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        now = datetime.now(UTC)

        root = store.add("Initial rebase", task_type="rebase")
        root.status = "completed"
        root.completed_at = now
        store.update(root)
        assert root.id is not None

        child = store.add("Follow-up rebase", task_type="rebase", based_on=root.id)
        child.status = "completed"
        child.completed_at = now
        store.update(child)

        result = run_gza("lineage", str(root.id), "--project", str(tmp_path))

        assert result.returncode == 0
        normalized = " ".join(result.stdout.split())
        assert "rebase" in normalized
        assert "[retry]" not in normalized

    def test_lineage_rel_label_brackets_are_rendered_literally(self, tmp_path: Path):
        """Relationship labels render as [rel] text, not as Rich markup tags."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        now = datetime.now(UTC)

        root = store.add("Root task", task_type="implement")
        root.status = "completed"
        root.completed_at = now
        store.update(root)

        # task_type="task" with depends_on → _classify_child_relationship returns "depends"
        # → _LINEAGE_REL_LABELS maps to "depends" → rendered as [depends]
        child = store.add("Dependent task", task_type="task", depends_on=root.id)
        child.status = "pending"
        store.update(child)

        result = run_gza("lineage", str(root.id), "--project", str(tmp_path))

        # returncode == 0 catches MarkupError crashes
        assert result.returncode == 0
        # "[depends]" in stdout catches silent text loss of the relationship label
        assert "[depends]" in result.stdout

    def test_cli_lineage_uses_shared_relationship_label_map(self):
        from gza import query as query_module
        from gza.cli import query as cli_query

        cli_query_path = Path(cli_query.__file__)

        assert cli_query._LINEAGE_REL_LABELS is query_module._LINEAGE_REL_LABELS
        assert "_LINEAGE_REL_LABELS:" not in cli_query_path.read_text()

    def test_lineage_merge_labels_render_only_for_merge_owners(self, tmp_path: Path):
        """Shared-branch improve rows must not render independent merge labels."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        now = datetime(2026, 2, 12, 10, 0, tzinfo=UTC)

        plan = store.add("Plan migration", task_type="plan")
        plan.status = "completed"
        plan.completed_at = now
        store.update(plan)

        impl_merged = store.add("Implement merged", task_type="implement", based_on=plan.id)
        impl_merged.status = "completed"
        impl_merged.merge_status = "merged"
        impl_merged.completed_at = now
        store.update(impl_merged)

        impl_none = store.add("Implement no merge status", task_type="implement", based_on=plan.id)
        impl_none.status = "completed"
        impl_none.merge_status = None
        impl_none.completed_at = now
        store.update(impl_none)

        improve_unmerged = store.add("Improve unmerged", task_type="improve", based_on=impl_none.id)
        improve_unmerged.status = "completed"
        improve_unmerged.merge_status = "unmerged"
        improve_unmerged.completed_at = now
        store.update(improve_unmerged)

        review = store.add("Review row", task_type="review", depends_on=impl_none.id)
        review.status = "completed"
        review.completed_at = now
        store.update(review)

        result = run_gza("lineage", str(plan.id), "--project", str(tmp_path))
        assert result.returncode == 0
        output = result.stdout

        assert "[merged]" in output
        assert "Implement no merge status [merged]" not in output
        assert "Implement no merge status [unmerged]" not in output
        assert "Improve unmerged [unmerged]" not in output
        assert "Improve unmerged [merged]" not in output
        assert "review completed [merged]" not in output
        assert "review completed [unmerged]" not in output

    def test_lineage_omits_prompt_for_non_plan_non_implement_rows(self, tmp_path: Path):
        """Prompt/slug text is shown only on plan and implement rows."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        now = datetime(2026, 2, 12, 10, 0, tzinfo=UTC)

        plan = store.add("Plan visible prompt", task_type="plan")
        plan.status = "completed"
        plan.completed_at = now
        store.update(plan)

        impl = store.add("Implement visible prompt", task_type="implement", based_on=plan.id)
        impl.status = "completed"
        impl.completed_at = now
        store.update(impl)

        review = store.add("Review hidden prompt", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.completed_at = now
        store.update(review)

        improve = store.add("Improve hidden prompt", task_type="improve", based_on=impl.id)
        improve.status = "completed"
        improve.completed_at = now
        store.update(improve)

        fix = store.add("Fix hidden prompt", task_type="fix", depends_on=review.id)
        fix.status = "completed"
        fix.completed_at = now
        store.update(fix)

        rebase = store.add("Rebase hidden prompt", task_type="rebase", based_on=impl.id)
        rebase.status = "completed"
        rebase.completed_at = now
        store.update(rebase)

        result = run_gza("lineage", str(plan.id), "--project", str(tmp_path))
        assert result.returncode == 0
        normalized = " ".join(result.stdout.split())

        assert "Plan visible prompt" in normalized
        assert "Implement visible prompt" in normalized
        assert "Review hidden prompt" not in normalized
        assert "Improve hidden prompt" not in normalized
        assert "Fix hidden prompt" not in normalized
        assert "Rebase hidden prompt" not in normalized

    def test_lineage_strips_yyyymmdd_prefix_from_slug(self, tmp_path: Path):
        """Lineage prompt column strips YYYYMMDD- prefix from slugs."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        now = datetime(2026, 2, 12, 10, 0, tzinfo=UTC)
        impl = store.add("Prompt fallback", task_type="implement")
        impl.status = "completed"
        impl.completed_at = now
        impl.slug = "20260212-feature-improve-lineage"
        store.update(impl)

        result = run_gza("lineage", str(impl.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "feature-improve-lineage" in result.stdout
        assert "20260212-feature-improve-lineage" not in result.stdout

    def test_lineage_columns_align_after_tree_prefix(self, tmp_path: Path):
        """Task id/timestamp/type/status/prompt columns align across varying depths."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        now = datetime(2026, 2, 12, 10, 0, tzinfo=UTC)

        plan = store.add("Plan aligned prompt", task_type="plan")
        plan.status = "completed"
        plan.completed_at = now
        store.update(plan)

        impl_merged = store.add("Implement merged prompt", task_type="implement", based_on=plan.id)
        impl_merged.status = "completed"
        impl_merged.merge_status = "merged"
        impl_merged.completed_at = now
        store.update(impl_merged)

        impl_unmerged = store.add("Implement unmerged prompt", task_type="implement", based_on=plan.id)
        impl_unmerged.status = "completed"
        impl_unmerged.merge_status = "unmerged"
        impl_unmerged.completed_at = now
        store.update(impl_unmerged)

        review = store.add("Review hidden", task_type="review", depends_on=impl_unmerged.id)
        review.status = "pending"
        review.completed_at = now
        store.update(review)

        result = run_gza("lineage", str(plan.id), "--project", str(tmp_path))
        assert result.returncode == 0

        lines = [line for line in result.stdout.splitlines() if line.strip()]
        assert len(lines) >= 4

        def _first_index(text: str, options: tuple[str, ...]) -> int:
            for option in options:
                idx = text.find(option)
                if idx != -1:
                    return idx
            raise AssertionError(f"None of {options} found in line: {text}")

        id_positions = {re.search(r"testproject-\d+", line).start() for line in lines if re.search(r"testproject-\d+", line)}
        ts_positions = {line.index("2026-02-12 10:00:00 UTC") for line in lines if "2026-02-12 10:00:00 UTC" in line}
        type_positions = {
            _first_index(line, ("plan", "implement", "review"))
            for line in lines
            if any(token in line for token in ("plan", "implement", "review"))
        }
        status_positions = {
            _first_index(line, ("completed", "pending"))
            for line in lines
            if "completed" in line or "pending" in line
        }
        prompt_positions = {
            line.index("Plan aligned prompt")
            for line in lines
            if "Plan aligned prompt" in line
        } | {
            line.index("Implement merged prompt")
            for line in lines
            if "Implement merged prompt" in line
        } | {
            line.index("Implement unmerged prompt")
            for line in lines
            if "Implement unmerged prompt" in line
        }

        assert len(id_positions) == 1
        assert len(ts_positions) == 1
        assert len(type_positions) == 1
        assert len(status_positions) == 1
        assert len(prompt_positions) == 1

    def test_lineage_prefix_depth_increments_one_column_per_level(self, tmp_path: Path):
        """Ancestor indentation grows by exactly one column per depth level."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        now = datetime(2026, 2, 12, 10, 0, tzinfo=UTC)

        root = store.add("Root implement", task_type="implement")
        root.status = "completed"
        root.completed_at = now
        store.update(root)

        child = store.add("Child improve", task_type="improve", based_on=root.id)
        child.status = "completed"
        child.completed_at = now
        store.update(child)

        grandchild = store.add("Grandchild improve", task_type="improve", based_on=child.id)
        grandchild.status = "completed"
        grandchild.completed_at = now
        store.update(grandchild)

        great_grandchild = store.add("Great grandchild improve", task_type="improve", based_on=grandchild.id)
        great_grandchild.status = "completed"
        great_grandchild.completed_at = now
        store.update(great_grandchild)

        result = run_gza("lineage", str(root.id), "--project", str(tmp_path))
        assert result.returncode == 0

        by_id = {
            match.group(1): line
            for line in result.stdout.splitlines()
            for match in [re.search(r"(testproject-\d+)", line)]
            if match
        }
        child_line = by_id[child.id]
        grandchild_line = by_id[grandchild.id]
        great_grandchild_line = by_id[great_grandchild.id]

        child_connector_col = child_line.index("└── ")
        grandchild_connector_col = grandchild_line.index("└── ")
        great_grandchild_connector_col = great_grandchild_line.index("└── ")

        assert grandchild_connector_col - child_connector_col == 1
        assert great_grandchild_connector_col - grandchild_connector_col == 1


class TestPsSortKey:
    """Tests for _ps_sort_key sort-key function."""

    def _make_row(
        self,
        task_id: str | int | None = None,
        status: str = "running",
        sort_timestamp: str = "2026-01-01T00:00:00",
        worker_id: str = "w-001",
    ) -> dict:
        return {
            "task_id": task_id,
            "status": status,
            "sort_timestamp": sort_timestamp,
            "worker_id": worker_id,
        }

    def test_string_task_ids_sort_in_numeric_order(self):
        """String task IDs sort numerically by decimal suffix."""
        from gza.cli.query import _ps_sort_key

        rows = [
            self._make_row(task_id="gza-100"),
            self._make_row(task_id="gza-10"),
            self._make_row(task_id="gza-2"),  # 2
            self._make_row(task_id="gza-1"),  # 1
        ]
        sorted_rows = sorted(rows, key=_ps_sort_key)
        assert [r["task_id"] for r in sorted_rows] == [
            "gza-1",
            "gza-2",
            "gza-10",
            "gza-100",
        ]

    def test_none_task_id_sorts_last(self):
        """Worker-only rows (task_id=None) must sort after all tasks."""
        import sys

        from gza.cli.query import _ps_sort_key

        row_with_task = self._make_row(task_id="gza-1")
        row_no_task = self._make_row(task_id=None)

        key_with_task = _ps_sort_key(row_with_task)
        key_no_task = _ps_sort_key(row_no_task)

        # task_id component (index 3) of the no-task row should be sys.maxsize
        assert key_no_task[3] == sys.maxsize
        assert key_with_task[3] < sys.maxsize

    def test_status_group_ordering(self):
        """in_progress sorts before failed, failed before completed."""
        from gza.cli.query import _ps_sort_key

        in_progress_row = self._make_row(task_id="gza-1", status="in_progress")
        failed_row = self._make_row(task_id="gza-2", status="failed")
        completed_row = self._make_row(task_id="gza-3", status="completed")

        assert _ps_sort_key(in_progress_row)[0] < _ps_sort_key(failed_row)[0]
        assert _ps_sort_key(failed_row)[0] < _ps_sort_key(completed_row)[0]

    def test_integer_task_id_backward_compat(self):
        """Integer task_id values sort numerically (backward compat)."""
        from gza.cli.query import _ps_sort_key

        row_int = self._make_row(task_id=5)
        key = _ps_sort_key(row_int)
        assert key[3] == 5

    def test_completed_sort_by_start_time_descending(self):
        """Completed tasks sort most-recently-started first."""
        from gza.cli.query import _ps_sort_key

        early = self._make_row(task_id="gza-1", status="completed", sort_timestamp="2026-01-01T00:00:00")
        late = self._make_row(task_id="gza-2", status="completed", sort_timestamp="2026-01-02T00:00:00")

        sorted_rows = sorted([early, late], key=_ps_sort_key)
        assert [r["task_id"] for r in sorted_rows] == ["gza-2", "gza-1"]

    def test_in_progress_sort_by_start_time_ascending(self):
        """In-progress tasks sort longest-running (earliest start) first."""
        from gza.cli.query import _ps_sort_key

        early = self._make_row(task_id="gza-1", status="in_progress", sort_timestamp="2026-01-01T00:00:00")
        late = self._make_row(task_id="gza-2", status="in_progress", sort_timestamp="2026-01-02T00:00:00")

        sorted_rows = sorted([early, late], key=_ps_sort_key)
        assert [r["task_id"] for r in sorted_rows] == ["gza-1", "gza-2"]


class TestIncompleteCommand:
    """Tests for the incomplete projection surface."""

    @staticmethod
    def _incomplete_args(
        tmp_path: Path,
        *,
        fields: str | None,
        json: bool = False,
        tree: bool = False,
    ) -> argparse.Namespace:
        return argparse.Namespace(
            project_dir=tmp_path,
            last=5,
            blocked_by_dropped=False,
            tree=tree,
            type=None,
            days=None,
            date_field="effective",
            json=json,
            verbose=False,
            fields=fields,
        )

    @staticmethod
    def _one_line_row_id(output: str) -> str:
        return next(line.split(":", 1)[0] for line in output.splitlines() if line.strip())

    @staticmethod
    def _tree_root_id(output: str) -> str:
        first_line = next(line for line in output.splitlines() if line.strip() and not set(line.strip()) == {"-"})
        return first_line.split()[1]

    @staticmethod
    def _setup_merged_owner_with_live_descendant_fixture(tmp_path: Path) -> tuple[Task, Task]:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("merged owner plan", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)
        assert plan.id is not None

        impl = store.add("merged implementation owner", task_type="implement", based_on=plan.id)
        store.mark_completed(impl, has_commits=True, branch="feature/cli-merged-owner-live-descendant")
        assert impl.id is not None

        unit = store.resolve_merge_unit_for_task(impl.id)
        assert unit is not None
        store.set_merge_unit_state(unit.id, "merged")

        followup = store.add(
            "live unresolved improve descendant",
            task_type="improve",
            based_on=impl.id,
        )
        followup.status = "completed"
        followup.completed_at = datetime.now(UTC)
        followup.has_commits = True
        followup.branch = "feature/cli-merged-owner-live-descendant-followup"
        followup.merge_status = "unmerged"
        store.update(followup)
        assert followup.id is not None

        return impl, followup

    def test_incomplete_text_fields_multi_field_uses_generic_blocks(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("needs follow-up", task_type="implement")
        task.status = "failed"
        task.completed_at = datetime.now(UTC)
        task.failure_reason = "TEST_FAILURE"
        store.update(task)

        result = query_cli.cmd_incomplete(self._incomplete_args(tmp_path, fields="id,prompt"))

        captured = capsys.readouterr()
        assert result == 0
        assert f"id: {task.id}" in captured.out
        assert "prompt: needs follow-up" in captured.out

    def test_incomplete_text_fields_single_field_prints_bare_values(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("bare incomplete value", task_type="implement")
        task.status = "failed"
        task.completed_at = datetime.now(UTC)
        task.failure_reason = "TEST_FAILURE"
        store.update(task)

        result = query_cli.cmd_incomplete(self._incomplete_args(tmp_path, fields="id"))

        captured = capsys.readouterr()
        assert result == 0
        assert captured.out.strip() == task.id

    def test_incomplete_json_fields_override_limits_projection(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("json incomplete value", task_type="implement")
        task.status = "failed"
        task.completed_at = datetime.now(UTC)
        task.failure_reason = "TEST_FAILURE"
        store.update(task)

        result = query_cli.cmd_incomplete(self._incomplete_args(tmp_path, fields="id,status", json=True))

        captured = capsys.readouterr()
        assert result == 0
        assert json.loads(captured.out) == [{"id": task.id, "status": "failed"}]

    def test_incomplete_unknown_fields_list_valid_choices(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        setup_config(tmp_path)
        make_store(tmp_path).add("unknown incomplete field", task_type="implement")

        result = query_cli.cmd_incomplete(self._incomplete_args(tmp_path, fields="id,nope"))

        captured = capsys.readouterr()
        assert result == 2
        assert "unknown field for gza incomplete: nope" in captured.err
        assert "valid fields:" in captured.err
        assert "id" in captured.err
        assert "Run uv run gza incomplete --list-fields to list valid fields." in captured.err

    def test_incomplete_cli_text_single_field_prints_bare_values(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("cli bare incomplete value", task_type="implement")
        task.status = "failed"
        task.completed_at = datetime.now(UTC)
        task.failure_reason = "TEST_FAILURE"
        store.update(task)

        result = run_gza("incomplete", "--fields", "id", "--project", str(tmp_path))

        assert result.returncode == 0
        assert result.stdout.strip() == task.id
        assert result.stderr == ""

    def test_incomplete_cli_text_multi_field_uses_blocks(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("cli block incomplete value", task_type="implement")
        task.status = "failed"
        task.completed_at = datetime.now(UTC)
        task.failure_reason = "TEST_FAILURE"
        store.update(task)

        result = run_gza("incomplete", "--fields", "id,prompt", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"id: {task.id}" in result.stdout
        assert "prompt: cli block incomplete value" in result.stdout
        assert result.stderr == ""

    def test_incomplete_cli_json_fields_emit_projection_objects(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("cli json incomplete value", task_type="implement")
        task.status = "failed"
        task.completed_at = datetime.now(UTC)
        task.failure_reason = "TEST_FAILURE"
        store.update(task)

        result = run_gza("incomplete", "--json", "--fields", "id,status", "--project", str(tmp_path))

        assert result.returncode == 0
        assert json.loads(result.stdout) == [{"id": task.id, "status": "failed"}]
        assert result.stderr == ""

    def test_incomplete_cli_json_fields_can_project_trigger_source(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("cli json trigger source", task_type="implement", trigger_source="watch")
        task.status = "failed"
        task.completed_at = datetime.now(UTC)
        task.failure_reason = "TEST_FAILURE"
        store.update(task)

        result = run_gza(
            "incomplete",
            "--json",
            "--fields",
            "id,trigger_source",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert json.loads(result.stdout) == [{"id": task.id, "trigger_source": "watch"}]
        assert result.stderr == ""

    def test_incomplete_cli_json_hides_merged_owner_with_orphan_same_branch_descendant(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("merged implement owner", task_type="implement")
        store.mark_completed(root, has_commits=True, branch="feature/cli-merged-owner")
        assert root.id is not None

        unit = store.resolve_merge_unit_for_task(root.id)
        assert unit is not None
        store.set_merge_unit_state(unit.id, "merged")

        orphan = store.add(
            "orphan same-branch descendant on forked branch",
            task_type="improve",
            based_on=root.id,
            same_branch=True,
        )
        orphan.status = "completed"
        orphan.completed_at = datetime.now(UTC)
        orphan.has_commits = True
        orphan.branch = "feature/cli-merged-owner-as-28"
        orphan.merge_status = "unmerged"
        store.update(orphan)

        result = run_gza("incomplete", "--json", "--last", "0", "--project", str(tmp_path))

        assert result.returncode == 0
        assert json.loads(result.stdout) == []
        assert result.stderr == ""

    def test_incomplete_normalization_keeps_merged_owner_with_live_unresolved_descendant(
        self,
        tmp_path: Path,
    ) -> None:
        impl, followup = self._setup_merged_owner_with_live_descendant_fixture(tmp_path)

        config = query_cli.Config.load(tmp_path)
        store = query_cli.get_store(config, open_mode="readwrite")
        service = query_cli._TaskQueryService(store)
        result = service.run(
            query_cli._TaskQueryPresets.incomplete(limit=None),
            config=config,
            git=None,
            target_branch="main",
        )

        normalized = query_cli._normalize_incomplete_result_rows(  # noqa: SLF001
            result,
            service=service,
            store=store,
            config=config,
            git=None,
            target_branch="main",
        )

        assert len(normalized.rows) == 1
        row = normalized.rows[0]
        assert row.owner_task.id == impl.id
        assert {task.id for task in row.unresolved_tasks} == {followup.id}

    def test_incomplete_json_keeps_merged_owner_with_live_unresolved_descendant(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        impl, followup = self._setup_merged_owner_with_live_descendant_fixture(tmp_path)

        result = query_cli.cmd_incomplete(
            self._incomplete_args(tmp_path, fields="id,unresolved_ids", json=True)
        )

        captured = capsys.readouterr()
        assert result == 0
        assert json.loads(captured.out) == [
            {
                "id": impl.id,
                "unresolved_ids": [followup.id],
            }
        ]

    def test_incomplete_cli_json_uses_real_next_action_when_git_context_is_available(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        plan = store.add("cli completed plan", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        with patch("gza.cli.query.Git", return_value=_mock_unmerged_git()):
            result = run_gza(
                "incomplete",
                "--json",
                "--fields",
                "id,next_action,next_action_reason",
                "--project",
                str(tmp_path),
            )

        assert result.returncode == 0
        assert json.loads(result.stdout) == [
            {
                "id": plan.id,
                "next_action": "create_plan_review",
                "next_action_reason": "Create and start plan review task",
            }
        ]
        assert result.stderr == ""

    def test_incomplete_keeps_failed_owner_visible_until_completed_recovery_code_is_merged(
        self,
        tmp_path: Path,
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed = store.add("Failed recovery owner", task_type="implement")
        assert failed.id is not None
        failed.status = "failed"
        failed.failure_reason = "MAX_TURNS"
        failed.completed_at = datetime(2026, 5, 11, 10, 0, tzinfo=UTC)
        failed.branch = "feature/recovery-owner"
        failed.has_commits = True
        store.update(failed)

        completed_retry = store.add(
            "Completed retry with unmerged code",
            task_type="implement",
            based_on=failed.id,
            recovery_origin="retry",
        )
        assert completed_retry.id is not None
        completed_retry.status = "completed"
        completed_retry.completed_at = datetime(2026, 5, 11, 11, 0, tzinfo=UTC)
        completed_retry.branch = "feature/recovery-owner-retry"
        completed_retry.has_commits = True
        completed_retry.merge_status = "unmerged"
        store.update(completed_retry)

        config = query_cli.Config.load(tmp_path)
        service = query_cli._TaskQueryService(store)
        result = service.run(
            query_cli._TaskQueryPresets.incomplete(limit=None),
            config=config,
            git=_mock_unmerged_git(),
            target_branch="main",
        )

        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.owner_task.id == completed_retry.id
        assert row.lifecycle_action_task is not None
        assert row.lifecycle_action_task.id == completed_retry.id
        assert row.recovery_action_task is None
        assert {task.id for task in row.unresolved_tasks} == {completed_retry.id}

    def test_incomplete_hides_failed_owner_after_completed_recovery_code_is_merged(
        self,
        tmp_path: Path,
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed = store.add("Failed merged recovery owner", task_type="implement")
        assert failed.id is not None
        failed.status = "failed"
        failed.failure_reason = "MAX_TURNS"
        failed.completed_at = datetime(2026, 5, 11, 10, 0, tzinfo=UTC)
        failed.branch = "feature/recovery-owner-merged"
        failed.has_commits = True
        store.update(failed)

        completed_retry = store.add(
            "Completed retry with merged code",
            task_type="implement",
            based_on=failed.id,
            recovery_origin="retry",
        )
        assert completed_retry.id is not None
        completed_retry.status = "completed"
        completed_retry.completed_at = datetime(2026, 5, 11, 11, 0, tzinfo=UTC)
        completed_retry.branch = "feature/recovery-owner-merged"
        completed_retry.has_commits = True
        completed_retry.merge_status = "unmerged"
        store.update(completed_retry)

        unit = store.create_merge_unit(
            source_branch=completed_retry.branch,
            target_branch="main",
            owner_task_id=completed_retry.id,
            state="unmerged",
        )
        store.attach_task_to_merge_unit(completed_retry.id, unit.id, "owner")
        store.set_merge_unit_state(unit.id, "merged")

        config = query_cli.Config.load(tmp_path)
        service = query_cli._TaskQueryService(store)
        result = service.run(
            query_cli._TaskQueryPresets.incomplete(limit=None),
            config=config,
            git=_mock_unmerged_git(),
            target_branch="main",
        )

        assert result.rows == ()

    def test_incomplete_cli_json_reports_held_plan_as_awaiting_human(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        plan = store.add("cli completed held plan", task_type="plan", auto_implement=False)
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        with patch("gza.cli.query.Git", return_value=_mock_unmerged_git()):
            result = run_gza(
                "incomplete",
                "--json",
                "--fields",
                "id,next_action,next_action_reason",
                "--project",
                str(tmp_path),
            )

        assert result.returncode == 0
        assert json.loads(result.stdout) == [
            {
                "id": plan.id,
                "next_action": "awaiting_human",
                "next_action_reason": (
                    f"Awaiting human review: review the plan, then run 'uv run gza implement {plan.id}' "
                    "to create implementation, or drop it if you decided not to implement."
                ),
            }
        ]
        assert result.stderr == ""

    def test_incomplete_cli_json_reports_explore_followup_decision_without_legacy_no_branch_text(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        explore = store.add("cli completed explore", task_type="explore")
        explore.status = "completed"
        explore.completed_at = datetime.now(UTC)
        store.update(explore)

        with patch("gza.cli.query.Git", return_value=_mock_unmerged_git()):
            result = run_gza(
                "incomplete",
                "--json",
                "--fields",
                "id,next_action,next_action_reason",
                "--project",
                str(tmp_path),
            )

        assert result.returncode == 0
        assert json.loads(result.stdout) == [
            {
                "id": explore.id,
                "next_action": "needs_discussion",
                "next_action_reason": (
                    "SKIP: completed explore has no plan or implement follow-up; "
                    "decide whether to drop it or spawn follow-up work"
                ),
            }
        ]
        assert "task has no branch (no commits)" not in result.stdout
        assert result.stderr == ""

    def test_incomplete_cli_unknown_fields_list_valid_choices(self, tmp_path: Path):
        setup_config(tmp_path)
        make_store(tmp_path).add("cli unknown incomplete field", task_type="implement")

        result = run_gza("incomplete", "--fields", "id,nope", "--project", str(tmp_path))

        assert result.returncode == 2
        assert "unknown field for gza incomplete: nope" in result.stderr
        assert "valid fields:" in result.stderr
        assert "id" in result.stderr
        assert "Run uv run gza incomplete --list-fields to list valid fields." in result.stderr

    def test_incomplete_cli_list_fields_prints_unresolved_projection_choices(self, tmp_path: Path):
        setup_config(tmp_path)

        result = run_gza("incomplete", "--list-fields", "--project", str(tmp_path))

        assert result.returncode == 0
        assert result.stdout.strip() == _projection_list_stdout("incomplete")
        assert "group" not in result.stdout.split()
        assert result.stderr == ""

    def test_incomplete_cli_blocked_by_dropped_list_fields_prints_blocked_projection_choices(self, tmp_path: Path):
        setup_config(tmp_path)

        result = run_gza("incomplete", "--blocked-by-dropped", "--list-fields", "--project", str(tmp_path))

        assert result.returncode == 0
        assert result.stdout.strip() == _projection_list_stdout("incomplete", blocked_by_dropped=True)
        assert result.stderr == ""

    def test_incomplete_cli_blocked_by_dropped_unknown_fields_use_blocked_mode_hint(self, tmp_path: Path):
        setup_config(tmp_path)
        make_store(tmp_path).add("cli blocked-by-dropped unknown incomplete field", task_type="implement")

        result = run_gza(
            "incomplete",
            "--blocked-by-dropped",
            "--fields",
            "nope",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 2
        assert "unknown field for gza incomplete: nope" in result.stderr
        assert (
            f"valid fields: {_projection_list_stdout('incomplete', blocked_by_dropped=True)}"
            in result.stderr
        )
        assert "Run uv run gza incomplete --blocked-by-dropped --list-fields to list valid fields." in result.stderr

    def test_incomplete_cli_default_one_line_is_compact_per_lineage(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("Root context prompt that should stay hidden", task_type="plan")
        root.status = "completed"
        root.completed_at = datetime.now(UTC)
        store.update(root)

        first_line = "Follow-up first line " + ("x" * 120)
        failed = store.add(
            f"\n\n{first_line}\nFull prompt body that should not render",
            task_type="implement",
            based_on=root.id,
        )
        failed.status = "failed"
        failed.completed_at = datetime.now(UTC)
        failed.failure_reason = "PREREQUISITE_UNMERGED"
        store.update(failed)

        result = run_gza("incomplete", "-n", "0", "--project", str(tmp_path))

        assert result.returncode == 0
        lines = [line for line in result.stdout.splitlines() if line.strip()]
        assert len(lines) == 1
        assert truncate(first_line, 100) in lines[0]
        assert "Full prompt body that should not render" not in lines[0]
        assert root.prompt not in lines[0]
        assert "| context:" not in lines[0]
        assert "| unresolved:" not in lines[0]
        assert result.stderr == ""

    def test_incomplete_roots_plan_impl_review_improve_lineage_at_impl_in_both_views(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan background job retries", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
        store.update(plan)
        assert plan.id is not None

        impl = store.add("Implement background job retries", task_type="implement", based_on=plan.id)
        impl.status = "completed"
        impl.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
        impl.branch = "feature/retries"
        impl.has_commits = True
        impl.merge_status = "unmerged"
        store.update(impl)
        assert impl.id is not None

        unit = store.create_merge_unit(
            source_branch=impl.branch,
            target_branch="main",
            owner_task_id=impl.id,
            state="unmerged",
        )
        store.attach_task_to_merge_unit(impl.id, unit.id, "owner")

        review = store.add(
            "Review background job retries",
            task_type="review",
            based_on=impl.id,
            depends_on=impl.id,
        )
        review.status = "completed"
        review.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        store.update(review)
        assert review.id is not None
        store.attach_task_to_merge_unit(review.id, unit.id, "review")

        improve = store.add(
            "Improve background job retries",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
        )
        improve.status = "in_progress"
        improve.branch = impl.branch
        improve.has_commits = True
        store.update(improve)
        assert improve.id is not None
        store.attach_task_to_merge_unit(improve.id, unit.id, "improve")

        git = _mock_unmerged_git()
        with patch("gza.cli.query.Git", return_value=git):
            result = query_cli.cmd_incomplete(self._incomplete_args(tmp_path, fields=None))
        captured = capsys.readouterr()

        assert result == 0
        one_line_output = captured.out
        assert self._one_line_row_id(one_line_output) == impl.id
        assert f"SKIP: improve task {improve.id} is in_progress" in one_line_output
        assert plan.prompt not in one_line_output

        with patch("gza.cli.query.Git", return_value=git):
            result = query_cli.cmd_incomplete(self._incomplete_args(tmp_path, fields=None, tree=True))
        captured = capsys.readouterr()

        assert result == 0
        tree_output = captured.out
        assert self._tree_root_id(tree_output) == impl.id
        assert plan.id not in tree_output
        assert plan.prompt not in tree_output
        assert review.id in tree_output
        assert improve.id in tree_output
        assert self._one_line_row_id(one_line_output) == self._tree_root_id(tree_output)

    def test_incomplete_roots_failed_rebase_lineage_at_impl_and_reports_blocker(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement startup diagnostics", task_type="implement")
        impl.status = "completed"
        impl.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
        impl.branch = "feature/startup-diagnostics"
        impl.has_commits = True
        impl.merge_status = "unmerged"
        store.update(impl)
        assert impl.id is not None

        unit = store.create_merge_unit(
            source_branch=impl.branch,
            target_branch="main",
            owner_task_id=impl.id,
            state="unmerged",
        )
        store.attach_task_to_merge_unit(impl.id, unit.id, "owner")

        rebase = store.add(
            "Rebase startup diagnostics",
            task_type="rebase",
            based_on=impl.id,
            same_branch=True,
        )
        rebase.status = "failed"
        rebase.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
        rebase.branch = impl.branch
        rebase.has_commits = True
        rebase.failure_reason = "MERGE_CONFLICT"
        store.update(rebase)
        assert rebase.id is not None
        store.attach_task_to_merge_unit(rebase.id, unit.id, "rebase")

        git = _mock_unmerged_git()
        git.can_merge.return_value = False

        with patch("gza.cli.query.Git", return_value=git):
            result = query_cli.cmd_incomplete(self._incomplete_args(tmp_path, fields=None))
        captured = capsys.readouterr()

        assert result == 0
        one_line_output = captured.out
        assert self._one_line_row_id(one_line_output) == impl.id
        assert f"rebase {rebase.id} failed, needs manual resolution" in one_line_output

        with patch("gza.cli.query.Git", return_value=git):
            result = query_cli.cmd_incomplete(self._incomplete_args(tmp_path, fields=None, tree=True))
        captured = capsys.readouterr()

        assert result == 0
        tree_output = captured.out
        assert self._tree_root_id(tree_output) == impl.id
        assert self._one_line_row_id(one_line_output) == self._tree_root_id(tree_output)

    def test_incomplete_empty_prereq_is_unresolved_by_default(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        dep = store.add("Empty prerequisite", task_type="implement")
        store.mark_completed(dep, has_commits=True, branch="feature/incomplete-empty-prereq")
        assert dep.id is not None
        unit = store.resolve_merge_unit_for_task(dep.id)
        assert unit is not None
        store.set_merge_unit_state(unit.id, "empty")

        downstream = store.add("Held downstream", task_type="implement", depends_on=dep.id)
        assert downstream.id is not None

        with patch("gza.cli.query.Git", return_value=_mock_unmerged_git()):
            result = query_cli.cmd_incomplete(self._incomplete_args(tmp_path, fields=None))
        captured = capsys.readouterr()

        assert result == 0
        one_line_output = captured.out
        assert self._one_line_row_id(one_line_output) == downstream.id
        assert "empty prerequisite" in one_line_output
        assert "gza-4072" in one_line_output
        assert "gza edit --clear-depends-on" in one_line_output

    def test_queue_blocked_dependent_surfaces_awaiting_plan_review_release_guidance(
        self,
        tmp_path: Path,
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Held plan", task_type="plan", auto_implement=False)
        assert plan.id is not None
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        dependent = store.add("Blocked downstream", task_type="implement", depends_on=plan.id)
        assert dependent.id is not None

        result = run_gza("queue", "--all", "--project", str(tmp_path))

        assert result.returncode == 0
        normalized = " ".join(result.stdout.split())
        assert dependent.id in normalized
        assert f"blocked: awaiting plan review for {plan.id}" in normalized
        assert f"release with uv run gza implement {plan.id}" in normalized
        assert f"or uv run gza edit {plan.id} --no-hold-for-review" in normalized

    def test_incomplete_surfaces_blocked_dependent_for_completed_held_plan_until_release(
        self,
        tmp_path: Path,
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Held plan", task_type="plan", auto_implement=False)
        assert plan.id is not None
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        dependent = store.add("Blocked downstream", task_type="implement", depends_on=plan.id)
        assert dependent.id is not None

        blocked_result = run_gza("incomplete", "--project", str(tmp_path))

        assert blocked_result.returncode == 0
        blocked_output = " ".join(blocked_result.stdout.split())
        assert f"Blocked dependents:" in blocked_result.stdout
        assert dependent.id in blocked_output
        assert f"blocked: awaiting plan review for {plan.id}" in blocked_output
        assert f"release with uv run gza implement {plan.id}" in blocked_output
        assert f"or uv run gza edit {plan.id} --no-hold-for-review" in blocked_output

        plan = store.get(plan.id)
        assert plan is not None
        plan.auto_implement = True
        store.update(plan)

        released_result = run_gza("incomplete", "--project", str(tmp_path))

        assert released_result.returncode == 0
        assert "No unresolved task lineages" in released_result.stdout
        assert dependent.id not in released_result.stdout

    def test_incomplete_warns_when_mergeable_rows_are_blocked_by_dirty_default_checkout(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement dirty-checkout warning", task_type="implement")
        assert impl.id is not None
        store.mark_completed(impl, has_commits=True, branch="feature/incomplete-dirty-warning")
        unit = store.resolve_merge_unit_for_task(impl.id)
        assert unit is not None
        store.set_merge_unit_state(unit.id, "unmerged")
        review = store.add("Review dirty-checkout warning", task_type="review", depends_on=impl.id, based_on=impl.id)
        review.status = "completed"
        review.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
        review.output_content = "**Verdict: APPROVED**"
        store.update(review)
        assert review.id is not None
        store.attach_task_to_merge_unit(review.id, unit.id, "review")

        git = _mock_unmerged_git()
        git.has_changes.return_value = True

        with patch("gza.cli.query.Git", return_value=git):
            result = query_cli.cmd_incomplete(self._incomplete_args(tmp_path, fields=None))
        captured = capsys.readouterr()

        assert result == 0
        assert "merges blocked: main checkout has uncommitted changes - commit or stash them first" in captured.out

    def test_incomplete_empty_prereq_policy_toggle_hides_release_valve_guidance(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        dep = store.add("Empty prerequisite", task_type="implement")
        store.mark_completed(dep, has_commits=True, branch="feature/incomplete-empty-toggle")
        assert dep.id is not None
        unit = store.resolve_merge_unit_for_task(dep.id)
        assert unit is not None
        store.set_merge_unit_state(unit.id, "empty")

        downstream = store.add("Held downstream", task_type="implement", depends_on=dep.id)
        assert downstream.id is not None

        monkeypatch.setattr(
            dependency_preconditions_module,
            "empty_prereq_satisfies_dependency",
            lambda _store, _prereq, _dependent: True,
        )

        with patch("gza.cli.query.Git", return_value=_mock_unmerged_git()):
            result = query_cli.cmd_incomplete(self._incomplete_args(tmp_path, fields=None))
        captured = capsys.readouterr()

        assert result == 0
        assert "No unresolved task lineages" in captured.out

    def test_incomplete_surfaces_strict_scope_unverified_needs_attention(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement scope verification", task_type="implement")
        impl.status = "completed"
        impl.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
        impl.branch = "feature/scope-verification"
        impl.has_commits = True
        impl.merge_status = "unmerged"
        store.update(impl)
        assert impl.id is not None

        unit = store.create_merge_unit(
            source_branch=impl.branch,
            target_branch="main",
            owner_task_id=impl.id,
            state="unmerged",
        )
        store.attach_task_to_merge_unit(impl.id, unit.id, "owner")

        class _ScopeInspectionFailingGit(_FastUnmergedGit):
            def get_diff_name_status(
                self,
                revision_range: str,
                paths: tuple[str, ...] | list[str] = (),
                *,
                check: bool = False,
            ) -> str:
                raise GitError("git diff --name-status main...feature/scope-verification failed:\nfatal: bad revision")

        git = _ScopeInspectionFailingGit()

        with patch("gza.cli.query.Git", return_value=git):
            result = query_cli.cmd_incomplete(self._incomplete_args(tmp_path, fields=None))
        captured = capsys.readouterr()

        assert result == 0
        one_line_output = captured.out
        assert self._one_line_row_id(one_line_output) == impl.id
        assert "strict project scope could not be verified" in one_line_output
        assert "fatal: bad revision" in one_line_output


class TestLineageOwnerParity:
    @staticmethod
    def _incomplete_args(tmp_path: Path, *, fields: str | None, json: bool = False, tree: bool = False):
        return argparse.Namespace(
            project_dir=tmp_path,
            last=0,
            list_fields=False,
            blocked_by_dropped=False,
            tree=tree,
            type=None,
            days=None,
            date_field="effective",
            json=json,
            verbose=False,
            fields=fields,
        )

    @staticmethod
    def _one_line_row_id(output: str) -> str:
        return next(line.split(":", 1)[0] for line in output.splitlines() if line.strip())

    @staticmethod
    def _tree_root_id(output: str) -> str:
        first_line = next(line for line in output.splitlines() if line.strip() and not set(line.strip()) == {"-"})
        return first_line.split()[1]

    def test_unmerged_keeps_impl_owner_identity_for_mixed_followup_lineage(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        tasks = _setup_lineage_owner_parity_fixture(tmp_path)
        impl = tasks["impl"]

        args = argparse.Namespace(
            project_dir=tmp_path,
            into_current=False,
            target=None,
            fetch=False,
            limit=5,
            json=True,
            fields="id,prompt",
        )

        result = query_cli.cmd_unmerged(args, git=_FastUnmergedGit())
        captured = capsys.readouterr()

        assert result == 0
        assert json.loads(captured.out) == [{"id": impl.id, "prompt": impl.prompt}]

    @pytest.mark.parametrize("focus_key", ["review", "improve", "dropped_two"])
    def test_lineage_re_roots_branch_owned_descendants_at_implement_owner(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        focus_key: str,
    ) -> None:
        tasks = _setup_lineage_owner_parity_fixture(tmp_path)
        impl = tasks["impl"]
        focus_task = tasks[focus_key]

        result = query_cli.cmd_lineage(
            argparse.Namespace(project_dir=tmp_path, task_id=str(focus_task.id))
        )
        captured = capsys.readouterr()

        assert result == 0
        assert _lineage_root_id(captured.out) == impl.id
        assert tasks["plan"].id not in captured.out

    def test_branchless_rebase_owner_identity_parity_across_lineage_history_and_search(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        tasks = _setup_branchless_rebase_owner_fixture(tmp_path)
        impl = tasks["impl"]
        rebase = tasks["rebase"]

        lineage_result = query_cli.cmd_lineage(
            argparse.Namespace(project_dir=tmp_path, task_id=str(rebase.id))
        )
        lineage_captured = capsys.readouterr()
        assert lineage_result == 0
        assert _lineage_root_id(lineage_captured.out) == impl.id

        history_result = run_gza(
            "history",
            "--status",
            "completed",
            "--json",
            "--fields",
            "id,branch_owner_id",
            "--project",
            str(tmp_path),
        )
        assert history_result.returncode == 0
        history_rows = {row["id"]: row["branch_owner_id"] for row in json.loads(history_result.stdout)}
        assert history_rows[rebase.id] == impl.id

        search_result = run_gza(
            "search",
            "Branchless rebase",
            "--json",
            "--fields",
            "id,branch_owner_id",
            "--project",
            str(tmp_path),
        )
        assert search_result.returncode == 0
        search_rows = {row["id"]: row["branch_owner_id"] for row in json.loads(search_result.stdout)}
        assert search_rows[rebase.id] == impl.id

    def test_unattached_retry_review_owner_identity_parity_across_lineage_history_and_search(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        tasks = _setup_retry_review_owner_fixture(tmp_path)
        root = tasks["root"]
        review = tasks["review"]

        lineage_result = query_cli.cmd_lineage(
            argparse.Namespace(project_dir=tmp_path, task_id=str(review.id))
        )
        lineage_captured = capsys.readouterr()
        assert lineage_result == 0
        assert _lineage_root_id(lineage_captured.out) == root.id

        history_result = run_gza(
            "history",
            "--status",
            "completed",
            "--json",
            "--fields",
            "id,branch_owner_id",
            "--project",
            str(tmp_path),
        )
        assert history_result.returncode == 0
        history_rows = {row["id"]: row["branch_owner_id"] for row in json.loads(history_result.stdout)}
        assert history_rows[review.id] == root.id

        search_result = run_gza(
            "search",
            "same-branch retry",
            "--json",
            "--fields",
            "id,branch_owner_id",
            "--project",
            str(tmp_path),
        )
        assert search_result.returncode == 0
        search_rows = {row["id"]: row["branch_owner_id"] for row in json.loads(search_result.stdout)}
        assert search_rows[review.id] == root.id

    def test_lineage_keeps_requested_plan_root_for_fanout_implementations(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        tasks = _setup_plan_fanout_fixture(tmp_path)
        plan = tasks["plan"]
        impl_a = tasks["impl_a"]
        impl_b = tasks["impl_b"]

        lineage_result = query_cli.cmd_lineage(
            argparse.Namespace(project_dir=tmp_path, task_id=str(plan.id))
        )
        captured = capsys.readouterr()

        assert lineage_result == 0
        assert _lineage_root_id(captured.out) == plan.id
        assert impl_a.id in captured.out
        assert impl_b.id in captured.out

    def test_plan_fanout_history_and_search_keep_task_scoped_branch_owner_id(
        self,
        tmp_path: Path,
    ) -> None:
        tasks = _setup_plan_fanout_fixture(tmp_path)
        plan = tasks["plan"]
        impl_a = tasks["impl_a"]
        impl_b = tasks["impl_b"]

        history_result = run_gza(
            "history",
            "--status",
            "completed",
            "--json",
            "--fields",
            "id,branch_owner_id",
            "--project",
            str(tmp_path),
        )
        assert history_result.returncode == 0
        history_rows = {
            row["id"]: row["branch_owner_id"]
            for row in json.loads(history_result.stdout)
            if row["id"] in {plan.id, impl_a.id, impl_b.id}
        }
        assert history_rows == {
            plan.id: plan.id,
            impl_a.id: impl_a.id,
            impl_b.id: impl_b.id,
        }

        search_result = run_gza(
            "search",
            "Fanout",
            "--json",
            "--fields",
            "id,branch_owner_id",
            "--project",
            str(tmp_path),
        )
        assert search_result.returncode == 0
        search_rows = {
            row["id"]: row["branch_owner_id"]
            for row in json.loads(search_result.stdout)
            if row["id"] in {plan.id, impl_a.id, impl_b.id}
        }
        assert search_rows == {
            plan.id: plan.id,
            impl_a.id: impl_a.id,
            impl_b.id: impl_b.id,
        }

    def test_owner_identity_parity_across_incomplete_unmerged_lineage_history_and_search(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        tasks = _setup_lineage_owner_parity_fixture(tmp_path)
        plan = tasks["plan"]
        impl = tasks["impl"]
        review = tasks["review"]
        improve = tasks["improve"]
        dropped_one = tasks["dropped_one"]
        dropped_two = tasks["dropped_two"]

        git = _mock_unmerged_git()
        with patch("gza.cli.query.Git", return_value=git):
            incomplete_result = query_cli.cmd_incomplete(
                argparse.Namespace(
                    project_dir=tmp_path,
                    last=0,
                    list_fields=False,
                    blocked_by_dropped=False,
                    tree=False,
                    type=None,
                    days=None,
                    date_field="effective",
                    json=True,
                    verbose=False,
                    fields="id",
                )
            )
        incomplete_captured = capsys.readouterr()
        assert incomplete_result == 0
        assert json.loads(incomplete_captured.out) == [{"id": impl.id}]

        unmerged_result = query_cli.cmd_unmerged(
            argparse.Namespace(
                project_dir=tmp_path,
                into_current=False,
                target=None,
                fetch=False,
                limit=5,
                json=True,
                fields="id",
            ),
            git=_FastUnmergedGit(),
        )
        unmerged_captured = capsys.readouterr()
        assert unmerged_result == 0
        assert json.loads(unmerged_captured.out) == [{"id": impl.id}]

        lineage_result = query_cli.cmd_lineage(
            argparse.Namespace(project_dir=tmp_path, task_id=str(dropped_two.id))
        )
        lineage_captured = capsys.readouterr()
        assert lineage_result == 0
        assert _lineage_root_id(lineage_captured.out) == impl.id

        history_result = run_gza(
            "history",
            "--json",
            "--fields",
            "id,branch_owner_id",
            "--project",
            str(tmp_path),
        )
        assert history_result.returncode == 0
        history_rows = {
            row["id"]: row["branch_owner_id"]
            for row in json.loads(history_result.stdout)
            if row["id"] in {plan.id, review.id, dropped_one.id, dropped_two.id}
        }
        assert history_rows == {
            plan.id: plan.id,
            review.id: impl.id,
            dropped_one.id: impl.id,
            dropped_two.id: impl.id,
        }

        search_result = run_gza(
            "search",
            "Parity",
            "--json",
            "--fields",
            "id,branch_owner_id",
            "--project",
            str(tmp_path),
        )
        assert search_result.returncode == 0
        search_rows = {
            row["id"]: row["branch_owner_id"]
            for row in json.loads(search_result.stdout)
            if row["id"] in {plan.id, review.id, improve.id, dropped_one.id, dropped_two.id}
        }
        assert search_rows == {
            plan.id: plan.id,
            review.id: impl.id,
            improve.id: impl.id,
            dropped_one.id: impl.id,
            dropped_two.id: impl.id,
        }

    def test_ps_remains_task_scoped_in_a_lineage_owner_fixture(self, tmp_path: Path) -> None:
        tasks = _setup_lineage_owner_parity_fixture(tmp_path)
        improve = tasks["improve"]

        result = run_gza("ps", "--json", "--project", str(tmp_path))

        assert result.returncode == 0
        rows = json.loads(result.stdout)
        assert len(rows) == 1
        assert rows[0]["task_id"] == improve.id

    def test_incomplete_hides_dropped_rebase_chain_in_both_views(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement background mode startup errors", task_type="implement")
        impl.status = "completed"
        impl.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
        impl.branch = "feature/background-errors"
        impl.has_commits = True
        impl.merge_status = "unmerged"
        store.update(impl)
        assert impl.id is not None

        unit = store.create_merge_unit(
            source_branch=impl.branch,
            target_branch="main",
            owner_task_id=impl.id,
            state="unmerged",
        )
        store.attach_task_to_merge_unit(impl.id, unit.id, "owner")

        rebase_resolved = store.add(
            "Resolved rebase",
            task_type="rebase",
            based_on=impl.id,
            same_branch=True,
        )
        rebase_resolved.status = "completed"
        rebase_resolved.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
        rebase_resolved.branch = impl.branch
        rebase_resolved.has_commits = True
        rebase_resolved.merge_status = "unmerged"
        store.update(rebase_resolved)
        assert rebase_resolved.id is not None
        store.attach_task_to_merge_unit(rebase_resolved.id, unit.id, "rebase")

        dropped_one = store.add(
            "Dropped rebase one",
            task_type="rebase",
            based_on=rebase_resolved.id,
            same_branch=True,
        )
        dropped_one.status = "dropped"
        dropped_one.completed_at = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
        dropped_one.branch = impl.branch
        dropped_one.has_commits = True
        store.update(dropped_one)
        assert dropped_one.id is not None
        store.attach_task_to_merge_unit(dropped_one.id, unit.id, "rebase")

        dropped_two = store.add(
            "Dropped rebase two",
            task_type="rebase",
            based_on=dropped_one.id,
            same_branch=True,
        )
        dropped_two.status = "dropped"
        dropped_two.completed_at = datetime(2026, 5, 10, 13, 0, tzinfo=UTC)
        dropped_two.branch = impl.branch
        dropped_two.has_commits = True
        store.update(dropped_two)
        assert dropped_two.id is not None
        store.attach_task_to_merge_unit(dropped_two.id, unit.id, "rebase")

        git = _mock_unmerged_git()
        git.can_merge.return_value = False

        with patch("gza.cli.query.Git", return_value=git):
            result = query_cli.cmd_incomplete(self._incomplete_args(tmp_path, fields=None))
        captured = capsys.readouterr()

        assert result == 0
        one_line_output = captured.out
        assert self._one_line_row_id(one_line_output) == impl.id
        assert "rebase --resolve (conflicts detected)" in one_line_output
        assert f"{dropped_one.id} (dropped)" not in one_line_output
        assert f"{dropped_two.id} (dropped)" not in one_line_output

        with patch("gza.cli.query.Git", return_value=git):
            result = query_cli.cmd_incomplete(self._incomplete_args(tmp_path, fields=None, tree=True))
        captured = capsys.readouterr()

        assert result == 0
        tree_output = captured.out
        assert self._tree_root_id(tree_output) == impl.id
        assert self._one_line_row_id(one_line_output) == self._tree_root_id(tree_output)
        assert f"{dropped_one.id} (dropped)" not in tree_output
        assert f"{dropped_two.id} (dropped)" not in tree_output
