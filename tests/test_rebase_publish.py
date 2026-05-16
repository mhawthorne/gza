"""Tests for shared post-rebase publication helpers."""

from unittest.mock import Mock

import pytest

from gza.git import Git, GitError
from gza.rebase_diff import RebaseDiffBaseline
from gza.rebase_publish import publish_rebased_branch


def test_publish_rebased_branch_wraps_unexpected_remote_ref_lookup_failure_as_git_error() -> None:
    git = Mock(spec=Git)
    git.rev_parse.return_value = "local-sha"
    git.rev_parse_if_exists.side_effect = RuntimeError("boom")
    logger = Mock()

    with pytest.raises(GitError, match="Failed to resolve rebased branch publication refs for feature/rebased: boom"):
        publish_rebased_branch(
            git,
            branch="feature/rebased",
            baseline=RebaseDiffBaseline(
                old_tip="old-sha",
                target_at_start="target-sha",
                merge_base_at_start="merge-base",
            ),
            logger=logger,
        )

    git.push_force_with_lease.assert_not_called()
    logger.command.assert_not_called()
    logger.error.assert_called_once()


def test_publish_rebased_branch_pushes_when_remote_ref_is_stale() -> None:
    git = Mock(spec=Git)
    git.rev_parse.return_value = "local-sha"
    git.rev_parse_if_exists.return_value = "remote-old"

    result = publish_rebased_branch(
        git,
        branch="feature/rebased",
        baseline=RebaseDiffBaseline(
            old_tip="old-sha",
            target_at_start="target-sha",
            merge_base_at_start="merge-base",
        ),
    )

    assert result.pushed is True
    git.push_force_with_lease.assert_called_once_with("feature/rebased", remote="origin")


def test_publish_rebased_branch_raises_when_rebase_did_not_advance_known_baseline() -> None:
    git = Mock(spec=Git)
    git.rev_parse.return_value = "same-sha"
    git.rev_parse_if_exists.return_value = "same-sha"
    logger = Mock()

    with pytest.raises(GitError, match="Rebase did not advance feature/rebased"):
        publish_rebased_branch(
            git,
            branch="feature/rebased",
            baseline=RebaseDiffBaseline(
                old_tip="same-sha",
                target_at_start="target-sha",
                merge_base_at_start="merge-base",
            ),
            logger=logger,
        )

    git.push_force_with_lease.assert_not_called()
    logger.error.assert_called_once()
