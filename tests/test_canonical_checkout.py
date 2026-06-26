"""Tests for canonical checkout invariant checks."""

import json
from pathlib import Path
from unittest.mock import Mock, patch

from gza.canonical_checkout import (
    CANONICAL_CHECKOUT_ATTENTION_REASON,
    check_canonical_checkout_invariant,
)
from gza.config import Config
from gza.git import GitError


def _config(tmp_path: Path) -> Config:
    return Config(
        project_dir=tmp_path,
        project_name="test-project",
        project_id="default",
    )


def test_canonical_checkout_restores_expected_branch_when_clean(tmp_path: Path) -> None:
    git = Mock()
    git.current_branch.return_value = "feature/hijacked"
    git.status_porcelain.return_value = set()
    ops_log = tmp_path / "logs" / "task.ops.jsonl"

    with patch("gza.canonical_checkout.Git", return_value=git):
        status = check_canonical_checkout_invariant(
            _config(tmp_path),
            expected_branch="main",
            action="provider task gza-1",
            task_id="gza-1",
            ops_log_file=ops_log,
        )

    assert status.state == "restored"
    assert status.restored is True
    git.checkout.assert_called_once_with("main")
    event = json.loads(ops_log.read_text().strip())
    assert event["subtype"] == CANONICAL_CHECKOUT_ATTENTION_REASON
    assert event["state"] == "restored"
    assert event["restored"] is True


def test_canonical_checkout_leaves_dirty_hijack_for_attention(tmp_path: Path) -> None:
    git = Mock()
    git.current_branch.return_value = "feature/hijacked"
    git.status_porcelain.return_value = {
        ("M", "src/app.py"),
        ("??", "scratch.txt"),
    }
    ops_log = tmp_path / "logs" / "task.ops.jsonl"

    with patch("gza.canonical_checkout.Git", return_value=git):
        status = check_canonical_checkout_invariant(
            _config(tmp_path),
            expected_branch="main",
            action="watch-pass-end",
            ops_log_file=ops_log,
        )

    assert status.needs_attention is True
    assert status.dirty_tracked_paths == ("src/app.py",)
    git.checkout.assert_not_called()
    event = json.loads(ops_log.read_text().strip())
    assert event["subtype"] == CANONICAL_CHECKOUT_ATTENTION_REASON
    assert event["state"] == "needs_attention"
    assert event["restoration_attempted"] is False
    assert event["dirty_tracked_paths"] == ["src/app.py"]


def test_canonical_checkout_dirty_probe_failure_needs_attention_without_restore(tmp_path: Path) -> None:
    git = Mock()
    git.current_branch.return_value = "feature/hijacked"
    git.status_porcelain.side_effect = GitError("status failed")
    ops_log = tmp_path / "logs" / "task.ops.jsonl"

    with patch("gza.canonical_checkout.Git", return_value=git):
        status = check_canonical_checkout_invariant(
            _config(tmp_path),
            expected_branch="main",
            action="provider task gza-1",
            task_id="gza-1",
            ops_log_file=ops_log,
        )

    assert status.needs_attention is True
    assert status.current_branch == "feature/hijacked"
    assert status.message == "status failed"
    git.checkout.assert_not_called()
    event = json.loads(ops_log.read_text().strip())
    assert event["subtype"] == CANONICAL_CHECKOUT_ATTENTION_REASON
    assert event["state"] == "needs_attention"
    assert event["restoration_attempted"] is False
    assert event["dirty_inspection_error"] == "status failed"
