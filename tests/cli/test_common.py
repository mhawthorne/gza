"""Tests for shared CLI utility functions in gza.cli._common."""

import pytest

from gza.cli._common import _looks_like_task_id


class TestLooksLikeTaskId:
    """Unit tests for _looks_like_task_id() — the heuristic that disambiguates task IDs
    from branch names in cmd_checkout and cmd_diff.

    The function must accept bare integers, bare base36 suffixes (containing at least
    one digit), and full prefixed IDs, while rejecting branch names and all-alpha strings.
    """

    @pytest.mark.parametrize(
        "arg, expected",
        [
            # Bare decimal integers — legacy backward-compat form
            ("42", True),
            ("0", True),
            ("1", True),
            ("999", True),
            # Full prefixed task IDs — alphanumeric prefix + hyphen + base36 suffix
            ("gza-3f", True),
            ("gza-1", True),
            ("myapp-abc1", True),
            # Bare base36 suffix containing at least one digit — treated as task ID
            ("3f", True),
            ("a1", True),
            ("1a2b", True),
            # All-alpha strings — NOT treated as task IDs (rejected by isalpha() check)
            ("abc", False),
            ("feature", False),
            # Branch names with hyphens that don't match the prefixed-ID pattern
            # (prefix would be too long or segment has wrong form)
            ("feature-add-logging", False),
            ("fix-some-bug", False),
            # Branch name with a digit — but multi-segment, not matching _TASK_ID_RE
            ("feature-123-thing", False),
            # Short branch-like strings that DO match _TASK_ID_RE — the heuristic is
            # intentionally permissive here.  Callers (cmd_checkout, cmd_diff) handle
            # false positives by falling back to branch-name behaviour when the task
            # lookup returns None, so these True results are expected and correct.
            ("fix-123", True),
            ("dev-1", True),
        ],
    )
    def test_looks_like_task_id(self, arg: str, expected: bool) -> None:
        assert _looks_like_task_id(arg) == expected, (
            f"_looks_like_task_id({arg!r}) expected {expected}"
        )
