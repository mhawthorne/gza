from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from gza.git import GitError
from gza.git_health import (
    GIT_HEALTH_REASON,
    GIT_HEALTH_PROMPT,
    GIT_HEALTH_TAG,
    check_git_health,
    current_git_health_alert,
    ensure_git_health_task,
    load_git_health_state,
)
from tests.cli.conftest import make_store, setup_config


def test_check_git_health_pass_without_existing_alert_keeps_state_ephemeral(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    git = MagicMock()
    git.worktree_list.return_value = [{"path": str(tmp_path)}]

    with patch("gza.git_health.datetime") as mocked_datetime:
        mocked_datetime.now.return_value = datetime(2026, 6, 26, tzinfo=UTC)
        mocked_datetime.fromisoformat.side_effect = datetime.fromisoformat
        check = check_git_health(store, git)

    assert check.dispatch_halted is False
    assert check.state.task is None
    assert check.state.reason == GIT_HEALTH_REASON
    assert check.state.dispatch_halted is False
    assert check.state.compact_failure is None
    assert check.state.raw_failure_text is None
    assert check.state.alert_message is None
    assert check.state.probe_command == "git worktree list --porcelain"
    assert load_git_health_state(store) is None
    assert current_git_health_alert(store) is None


def test_check_git_health_failure_persists_payload_and_alert(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    git = MagicMock()
    git.worktree_list.side_effect = GitError(
        "fatal: invalid commondir /gza-git/common\n"
        "fatal: not a git repository: /workspace/.git/worktrees/broken"
    )

    with patch("gza.git_health.datetime") as mocked_datetime:
        mocked_datetime.now.return_value = datetime(2026, 6, 26, tzinfo=UTC)
        mocked_datetime.fromisoformat.side_effect = datetime.fromisoformat
        check = check_git_health(store, git)

    assert check.dispatch_halted is True
    state = check.state
    assert state.reason == GIT_HEALTH_REASON
    assert state.dispatch_halted is True
    assert (
        state.compact_failure
        == "fatal: invalid commondir /gza-git/common fatal: not a git repository: /workspace/.git/worktrees/broken"
    )
    assert state.raw_failure_text == (
        "fatal: invalid commondir /gza-git/common\n"
        "fatal: not a git repository: /workspace/.git/worktrees/broken"
    )
    assert state.alert_message is not None
    assert "\n" not in state.alert_message
    assert "`git worktree list` failed:" in state.alert_message
    assert "Inspect `.git/worktrees/*/commondir`" in state.alert_message
    assert "No tasks were started or marked failed by this halt." in state.alert_message
    assert current_git_health_alert(store) == state

    persisted_task = ensure_git_health_task(store)
    payload = json.loads(persisted_task.output_content or "{}")
    assert payload == {
        "alert_message": state.alert_message,
        "captured_at": "2026-06-26T00:00:00+00:00",
        "compact_failure": state.compact_failure,
        "dispatch_halted": True,
        "probe_command": "git worktree list --porcelain",
        "raw_failure_text": state.raw_failure_text,
        "reason": GIT_HEALTH_REASON,
    }
    assert persisted_task.prompt == GIT_HEALTH_PROMPT
    assert persisted_task.skip_learnings is True
    assert GIT_HEALTH_TAG in persisted_task.tags


def test_check_git_health_pass_after_failure_clears_active_alert(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    git = MagicMock()
    git.worktree_list.side_effect = [
        GitError("fatal: broken commondir /gza-git/common"),
        [{"path": str(tmp_path)}],
    ]

    with patch("gza.git_health.datetime") as mocked_datetime:
        mocked_datetime.now.side_effect = [
            datetime(2026, 6, 26, 0, 0, tzinfo=UTC),
            datetime(2026, 6, 26, 0, 0, tzinfo=UTC),
            datetime(2026, 6, 26, 0, 1, tzinfo=UTC),
        ]
        mocked_datetime.fromisoformat.side_effect = datetime.fromisoformat
        first = check_git_health(store, git)
        second = check_git_health(store, git)

    assert first.dispatch_halted is True
    assert second.dispatch_halted is False
    assert current_git_health_alert(store) is None

    state = load_git_health_state(store)
    assert state is not None
    assert state.reason == GIT_HEALTH_REASON
    assert state.dispatch_halted is False
    assert state.compact_failure is None
    assert state.raw_failure_text is None
    assert state.alert_message is None
    assert state.captured_at == datetime(2026, 6, 26, 0, 1, tzinfo=UTC)


def test_check_git_health_compacts_multiline_failures_to_single_line_alert(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    git = MagicMock()
    git.worktree_list.side_effect = OSError("fatal: bad state\n\n  second line with extra   spaces")

    check = check_git_health(store, git)

    assert check.dispatch_halted is True
    assert check.state.compact_failure == "fatal: bad state second line with extra spaces"
    assert check.state.alert_message is not None
    assert "\n" not in check.state.alert_message


def test_load_git_health_state_defaults_reason_for_legacy_payload(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = ensure_git_health_task(store)
    task.output_content = json.dumps(
        {
            "alert_message": "git worktree health RED - dispatch halted",
            "captured_at": "2026-06-26T00:00:00+00:00",
            "compact_failure": "fatal: broken commondir",
            "dispatch_halted": True,
            "probe_command": "git worktree list --porcelain",
            "raw_failure_text": "fatal: broken commondir",
        },
        sort_keys=True,
    )
    store.update(task)

    state = load_git_health_state(store)

    assert state is not None
    assert state.reason == GIT_HEALTH_REASON
    assert state.dispatch_halted is True
