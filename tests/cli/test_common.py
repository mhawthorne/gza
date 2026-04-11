"""Tests for shared CLI utility functions in gza.cli._common."""

from gza.cli._common import _looks_like_task_id, format_stats
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
