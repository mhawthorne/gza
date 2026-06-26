from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from gza.git import GitError
from gza.git import GitWorktreeHealthProbe, WorktreeAdminMetadataIssue, WorktreeAdminMetadataValidation
from gza.git_health import (
    GIT_WORKTREE_HEALTH_PROMPT,
    GIT_WORKTREE_HEALTH_REASON,
    GIT_WORKTREE_HEALTH_TAG,
    check_git_health,
    check_git_worktree_health,
    current_git_health_alert,
    ensure_git_worktree_health_task,
    load_git_worktree_health_state,
)
from tests.cli.conftest import make_store, setup_config


def _validation(
    repo_dir: Path,
    *issues: WorktreeAdminMetadataIssue,
) -> WorktreeAdminMetadataValidation:
    return WorktreeAdminMetadataValidation(common_dir=repo_dir / ".git", issues=issues)


def test_check_git_worktree_health_pass_without_existing_alert_keeps_state_ephemeral(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    git = SimpleNamespace(repo_dir=tmp_path)
    probe = GitWorktreeHealthProbe(
        command="git worktree list --porcelain",
        returncode=0,
        stdout="",
        stderr="",
    )

    with (
        patch("gza.git_health.datetime") as mocked_datetime,
        patch("gza.git_health._probe_git_worktree_health", return_value=probe),
        patch("gza.git_health.validate_host_worktree_admin_metadata", return_value=_validation(tmp_path)),
    ):
        mocked_datetime.now.return_value = datetime(2026, 6, 26, tzinfo=UTC)
        mocked_datetime.fromisoformat.side_effect = datetime.fromisoformat
        check = check_git_worktree_health(None, store, git)

    assert check.dispatch_halted is False
    assert check.state.task is None
    assert check.state.reason == GIT_WORKTREE_HEALTH_REASON
    assert check.state.dispatch_halted is False
    assert check.state.probe_command == "git worktree list --porcelain"
    assert check.state.probe_returncode == 0
    assert check.state.metadata_findings == ()
    assert check.state.metadata_scan_error is None
    assert check.state.alert_message is None
    assert load_git_worktree_health_state(store) is None
    assert current_git_health_alert(store) is None


def test_check_git_worktree_health_failure_persists_payload_and_alert(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    git = SimpleNamespace(repo_dir=tmp_path)
    probe = GitWorktreeHealthProbe(
        command="git worktree list --porcelain",
        returncode=128,
        stdout="",
        stderr=(
            "fatal: invalid commondir /gza-git/common\n"
            "fatal: not a git repository: /workspace/.git/worktrees/broken"
        ),
    )
    issue = WorktreeAdminMetadataIssue(
        registration_name="broken",
        admin_file="commondir",
        admin_path=tmp_path / ".git" / "worktrees" / "broken" / "commondir",
        value="/gza-git/common",
        problem="containerized-commondir",
        details="container path leak",
        expected_value="../..",
        suspected_container_path_marker="/gza-git",
    )

    with (
        patch("gza.git_health.datetime") as mocked_datetime,
        patch("gza.git_health._probe_git_worktree_health", return_value=probe),
        patch("gza.git_health.validate_host_worktree_admin_metadata", return_value=_validation(tmp_path, issue)),
    ):
        mocked_datetime.now.return_value = datetime(2026, 6, 26, tzinfo=UTC)
        mocked_datetime.fromisoformat.side_effect = datetime.fromisoformat
        check = check_git_worktree_health(None, store, git)

    assert check.dispatch_halted is True
    state = check.state
    assert state.reason == GIT_WORKTREE_HEALTH_REASON
    assert state.dispatch_halted is True
    assert state.compact_failure == (
        "fatal: invalid commondir /gza-git/common fatal: not a git repository: /workspace/.git/worktrees/broken"
    )
    assert state.probe_returncode == 128
    assert state.probe_stderr == probe.stderr
    assert state.suspected_container_path_marker == "/gza-git"
    assert len(state.metadata_findings) == 1
    assert state.metadata_findings[0].admin_file == "commondir"
    assert state.metadata_scan_error is None
    assert state.remediation_message is not None
    assert "Inspect `.git/worktrees/*/commondir`" in state.remediation_message
    assert ".git/worktrees/broken/commondir" in state.remediation_message
    assert "restore `../..`" in state.remediation_message
    assert state.alert_message is not None
    assert "`git worktree list` failed (exit 128)" in state.alert_message
    assert "No tasks were started or marked failed by this halt." in state.alert_message
    assert current_git_health_alert(store) == state

    persisted_task = ensure_git_worktree_health_task(store)
    payload = json.loads(persisted_task.output_content or "{}")
    assert payload == {
        "alert_message": state.alert_message,
        "captured_at": "2026-06-26T00:00:00+00:00",
        "compact_failure": state.compact_failure,
        "dispatch_halted": True,
        "metadata_findings": [
            {
                "admin_file": "commondir",
                "admin_path": str(issue.admin_path),
                "details": "container path leak",
                "expected_value": "../..",
                "problem": "containerized-commondir",
                "registration_name": "broken",
                "suspected_container_path_marker": "/gza-git",
                "value": "/gza-git/common",
            }
        ],
        "metadata_scan_error": None,
        "probe_command": "git worktree list --porcelain",
        "probe_returncode": 128,
        "probe_stderr": probe.stderr,
        "probe_stdout": None,
        "raw_failure_text": probe.stderr,
        "reason": GIT_WORKTREE_HEALTH_REASON,
        "remediation_message": state.remediation_message,
        "suspected_container_path_marker": "/gza-git",
    }
    assert persisted_task.prompt == GIT_WORKTREE_HEALTH_PROMPT
    assert persisted_task.skip_learnings is True
    assert GIT_WORKTREE_HEALTH_TAG in persisted_task.tags


def test_repeated_git_worktree_health_failures_reuse_one_internal_task(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    git = SimpleNamespace(repo_dir=tmp_path)
    probe = GitWorktreeHealthProbe(
        command="git worktree list --porcelain",
        returncode=128,
        stdout="",
        stderr="fatal: broken commondir /gza-git/common",
    )
    issue = WorktreeAdminMetadataIssue(
        registration_name="broken",
        admin_file="gitdir",
        admin_path=tmp_path / ".git" / "worktrees" / "broken" / "gitdir",
        value="/gza-git/repo/.git/worktrees/broken",
        problem="containerized-gitdir",
        details="container gitdir leak",
        suspected_container_path_marker="/gza-git",
    )

    with (
        patch("gza.git_health.datetime") as mocked_datetime,
        patch("gza.git_health._probe_git_worktree_health", return_value=probe),
        patch("gza.git_health.validate_host_worktree_admin_metadata", return_value=_validation(tmp_path, issue)),
    ):
        mocked_datetime.now.side_effect = [
            datetime(2026, 6, 26, 0, 0, tzinfo=UTC),
            datetime(2026, 6, 26, 0, 0, tzinfo=UTC),
            datetime(2026, 6, 26, 0, 1, tzinfo=UTC),
        ]
        mocked_datetime.fromisoformat.side_effect = datetime.fromisoformat
        first = check_git_health(store, git)
        second = check_git_health(store, git)

    assert first.dispatch_halted is True
    assert second.dispatch_halted is True
    first_task = ensure_git_worktree_health_task(store)
    second_task = ensure_git_worktree_health_task(store)
    assert first_task.id == second_task.id
    matching = [task for task in store.get_all() if task.prompt == GIT_WORKTREE_HEALTH_PROMPT]
    assert len(matching) == 1
    state = load_git_worktree_health_state(store)
    assert state is not None
    assert state.captured_at == datetime(2026, 6, 26, 0, 1, tzinfo=UTC)


def test_load_git_worktree_health_state_defaults_reason_for_legacy_payload(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = ensure_git_worktree_health_task(store)
    task.output_content = json.dumps(
        {
            "alert_message": "git worktree health RED - dispatch halted",
            "captured_at": "2026-06-26T00:00:00+00:00",
            "compact_failure": "fatal: broken commondir",
            "dispatch_halted": True,
            "probe_command": "git worktree list --porcelain",
            "probe_returncode": 128,
            "raw_failure_text": "fatal: broken commondir",
        },
        sort_keys=True,
    )
    store.update(task)

    state = load_git_worktree_health_state(store)

    assert state is not None
    assert state.reason == GIT_WORKTREE_HEALTH_REASON
    assert state.dispatch_halted is True
    assert state.probe_returncode == 128
    assert state.metadata_findings == ()


def test_check_git_worktree_health_metadata_scanner_failure_is_durable_alert(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    git = SimpleNamespace(repo_dir=tmp_path)
    probe = GitWorktreeHealthProbe(
        command="git worktree list --porcelain",
        returncode=0,
        stdout="",
        stderr="",
    )

    with (
        patch("gza.git_health.datetime") as mocked_datetime,
        patch("gza.git_health._probe_git_worktree_health", return_value=probe),
        patch(
            "gza.git_health.validate_host_worktree_admin_metadata",
            side_effect=OSError("permission denied reading .git/worktrees"),
        ),
    ):
        mocked_datetime.now.return_value = datetime(2026, 6, 26, tzinfo=UTC)
        mocked_datetime.fromisoformat.side_effect = datetime.fromisoformat
        check = check_git_worktree_health(None, store, git)

    assert check.dispatch_halted is True
    state = check.state
    assert state.probe_returncode == 0
    assert state.metadata_findings == ()
    assert state.metadata_scan_error == "OSError: permission denied reading .git/worktrees"
    assert state.raw_failure_text == state.metadata_scan_error
    assert state.alert_message is not None
    assert "host worktree metadata scan failed" in state.alert_message
    assert state.remediation_message is not None
    assert "scanner failed before it could complete" in state.remediation_message
    assert current_git_health_alert(store) == state

    persisted_task = ensure_git_worktree_health_task(store)
    payload = json.loads(persisted_task.output_content or "{}")
    assert payload["metadata_findings"] == []
    assert payload["metadata_scan_error"] == "OSError: permission denied reading .git/worktrees"
    assert payload["dispatch_halted"] is True


@pytest.mark.parametrize("control_flow_error", [KeyboardInterrupt(), SystemExit(2)])
def test_probe_worktree_list_control_flow_exceptions_propagate(tmp_path, control_flow_error) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    def worktree_list():
        raise control_flow_error

    git = SimpleNamespace(repo_dir=tmp_path, worktree_list=worktree_list)

    with pytest.raises(type(control_flow_error)):
        check_git_worktree_health(None, store, git)

    assert load_git_worktree_health_state(store) is None


def test_probe_worktree_list_git_error_becomes_red_probe(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    def worktree_list():
        raise GitError("fatal: invalid commondir /gza-git/common")

    git = SimpleNamespace(repo_dir=tmp_path, worktree_list=worktree_list)

    with patch("gza.git_health.validate_host_worktree_admin_metadata", return_value=_validation(tmp_path)):
        check = check_git_worktree_health(None, store, git)

    assert check.dispatch_halted is True
    assert check.probe.returncode == 1
    assert check.probe.stderr == "fatal: invalid commondir /gza-git/common"
