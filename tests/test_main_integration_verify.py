from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from gza.config import Config
from gza.db import SqliteTaskStore
from gza.main_integration_verify import (
    MAIN_INTEGRATION_VERIFY_FRESHNESS_UNAVAILABLE_EXIT_STATUS,
    check_main_integration_verify,
    current_main_integration_verify_alert,
)
from gza.runner import _make_review_verify_result
from tests.cli.conftest import make_store, setup_config


def _seed_main_verify_task(store: SqliteTaskStore, *, verify_status: str, verify_exit_status: str, failure: str, alert_message: str) -> str:
    task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.review_verify_command = "./bin/tests"
    task.review_verify_status = verify_status
    task.review_verify_exit_status = verify_exit_status
    task.review_verify_failure = failure
    task.review_verify_head_sha = "abc123"
    task.output_content = (
        '{"alert_message":"'
        + alert_message
        + '","captured_at":"2026-06-23T00:00:00+00:00","failing_phase":"unit","gate_enabled":true,'
        '"head_sha":"abc123","tree_fingerprint":"fp-verified","verify_command":"./bin/tests",'
        '"verify_timeout_grace_seconds":5.0,"verify_timeout_seconds":120}'
    )
    store.update(task)
    return task.id


def test_check_main_integration_verify_reruns_and_halts_when_current_fingerprint_is_unavailable(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _seed_main_verify_task(
        store,
        verify_status="passed",
        verify_exit_status="0",
        failure="",
        alert_message="",
    )

    config = MagicMock(spec=Config)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    git = MagicMock()
    git.repo_dir = tmp_path
    git.current_branch.return_value = "main"
    git.rev_parse_if_exists.return_value = "abc123"

    verify_result = _make_review_verify_result(
        "./bin/tests",
        status="passed",
        exit_status="0",
        captured_at=datetime(2026, 6, 23, tzinfo=UTC),
        reviewed_branch="main",
        reviewed_head_sha="abc123",
        working_directory=str(tmp_path),
        output="all good",
    )

    def capture_verify_result(_config, _store, task, result, **_kwargs) -> None:
        task.review_verify_command = result.command
        task.review_verify_status = result.status
        task.review_verify_exit_status = result.exit_status
        task.review_verify_failure = result.failure
        task.review_verify_head_sha = result.reviewed_head_sha
        task.review_verify_branch = result.reviewed_branch
        task.review_verify_captured_at = result.captured_at
        store.update(task)

    with (
        patch("gza.main_integration_verify._compute_tree_fingerprint", side_effect=[None, None]),
        patch("gza.main_integration_verify._run_review_verify_command", return_value=verify_result),
        patch("gza.main_integration_verify._capture_review_verify_result", side_effect=capture_verify_result),
    ):
        check = check_main_integration_verify(
            config,
            store,
            git,
            reason="unit-test",
        )

    assert check.performed_verify is True
    assert check.merges_halted is True
    assert check.state.verify_status == "unavailable"
    assert check.state.verify_exit_status == MAIN_INTEGRATION_VERIFY_FRESHNESS_UNAVAILABLE_EXIT_STATUS
    assert check.state.failure == (
        "could not prove exact local target tree freshness because the tree fingerprint is unavailable"
    )
    assert check.state.alert_message == (
        "main verify freshness unproven at `abc123` - merges halted; exact tree fingerprint unavailable"
    )


def test_current_main_integration_verify_alert_surfaces_unproven_freshness_when_default_branch_probe_fails(
    tmp_path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _seed_main_verify_task(
        store,
        verify_status="failed",
        verify_exit_status="1",
        failure="verify_command failed",
        alert_message="main verify RED at `abc123` - merges halted; phase `unit` failing",
    )

    config = MagicMock(spec=Config)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    git = MagicMock()
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "main"
    git.rev_parse_if_exists.return_value = "abc123"

    with patch("gza.main_integration_verify._compute_tree_fingerprint", return_value=None):
        alert = current_main_integration_verify_alert(store, git, config)

    assert alert is not None
    assert alert.verify_status == "unavailable"
    assert alert.verify_exit_status == MAIN_INTEGRATION_VERIFY_FRESHNESS_UNAVAILABLE_EXIT_STATUS
    assert alert.alert_message == (
        "main verify freshness unproven at `abc123` - merges halted; exact tree fingerprint unavailable"
    )
