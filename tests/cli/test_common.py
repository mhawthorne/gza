"""Tests for shared CLI utility functions in gza.cli._common."""

from gza.cli._common import _looks_like_task_id, format_stats, run_with_resume
from gza.config import Config
from gza.db import SqliteTaskStore
import pytest


class TestLooksLikeTaskId:
    """Unit tests for _looks_like_task_id() — the heuristic that disambiguates task IDs
    from branch names in cmd_checkout and cmd_diff.

    The function accepts only full prefixed IDs.
    """

    @pytest.mark.parametrize(
        "arg, expected",
        [
            # Full prefixed task IDs — alphanumeric prefix + hyphen + decimal suffix
            ("gza-1", True),
            ("gza-000001", True),
            ("myapp-42", True),
            # Non-prefixed forms are rejected
            ("42", False),
            ("3f", False),
            ("a1", False),
            ("1a2b", False),
            ("abc", False),
            ("gza-3f", False),
            ("myapp-abc1", False),
            ("feature", False),
            # Branch names are rejected
            ("feature-add-logging", False),
            ("fix-some-bug", False),
            ("feature-123-thing", False),
        ],
    )
    def test_looks_like_task_id(self, arg: str, expected: bool) -> None:
        assert _looks_like_task_id(arg) == expected, (
            f"_looks_like_task_id({arg!r}) expected {expected}"
        )


class TestFormatStats:
    """Unit tests for compact task stats formatting."""

    def test_format_stats_includes_attach_counts_and_seconds(self, tmp_path):
        """Attach count and sub-minute attach duration are rendered in seconds."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Task with attach stats")
        task.attach_count = 2
        task.attach_duration_seconds = 45.0
        store.update(task)

        stats = format_stats(task)
        assert "2 attaches (45s)" in stats

    def test_format_stats_includes_attach_duration_minutes_seconds(self, tmp_path):
        """Attach duration >= 60s is rendered as XmYs."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Task with long attach stats")
        task.attach_count = 1
        task.attach_duration_seconds = 125.0
        store.update(task)

        stats = format_stats(task)
        assert "1 attach (2m5s)" in stats


class TestRunWithResume:
    """Unit tests for shared run_with_resume execution helper."""

    def test_resumes_on_max_steps_and_max_turns(self, tmp_path):
        (tmp_path / "gza.yaml").write_text("project_name: test-project\n")
        config = Config.load(tmp_path)
        store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)

        task = store.add("Implement feature", task_type="implement")
        task.session_id = "sess-123"
        store.update(task)

        outcomes = ["MAX_STEPS", "MAX_TURNS", None]
        seen_resume_flags: list[bool] = []

        def _run_task(run_task, resume: bool) -> int:
            seen_resume_flags.append(resume)
            outcome = outcomes.pop(0)
            if outcome is None:
                run_task.status = "completed"
                store.update(run_task)
                return 0
            run_task.status = "failed"
            run_task.failure_reason = outcome
            run_task.session_id = "sess-123"
            store.update(run_task)
            return 1

        final_task, rc = run_with_resume(
            config,
            store,
            task,
            run_task=_run_task,
            max_resume_attempts=3,
        )

        assert rc == 0
        assert final_task.status == "completed"
        assert seen_resume_flags == [False, True, True]
        assert len(store.get_all()) == 3  # original + 2 resume children

    def test_stops_after_max_resume_attempts(self, tmp_path):
        (tmp_path / "gza.yaml").write_text("project_name: test-project\n")
        config = Config.load(tmp_path)
        store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)

        task = store.add("Implement feature", task_type="implement")
        task.session_id = "sess-123"
        store.update(task)

        def _run_task(run_task, resume: bool) -> int:
            run_task.status = "failed"
            run_task.failure_reason = "MAX_STEPS"
            run_task.session_id = "sess-123"
            store.update(run_task)
            return 1

        final_task, rc = run_with_resume(
            config,
            store,
            task,
            run_task=_run_task,
            max_resume_attempts=2,
        )

        assert rc == 1
        assert final_task.status == "failed"
        assert len(store.get_all()) == 3  # original + 2 resume children

    def test_non_resumable_failure_passthrough(self, tmp_path):
        (tmp_path / "gza.yaml").write_text("project_name: test-project\n")
        config = Config.load(tmp_path)
        store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)

        task = store.add("Implement feature", task_type="implement")
        task.session_id = "sess-123"
        store.update(task)

        def _run_task(run_task, resume: bool) -> int:
            run_task.status = "failed"
            run_task.failure_reason = "TEST_FAILURE"
            run_task.session_id = "sess-123"
            store.update(run_task)
            return 1

        final_task, rc = run_with_resume(
            config,
            store,
            task,
            run_task=_run_task,
            max_resume_attempts=3,
        )

        assert rc == 1
        assert final_task.status == "failed"
        assert final_task.failure_reason == "TEST_FAILURE"
        assert len(store.get_all()) == 1
