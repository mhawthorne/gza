"""Tests for shared CLI utility functions in gza.cli._common."""

import pytest

from gza.cli._common import _looks_like_task_id


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
