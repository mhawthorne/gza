from __future__ import annotations

import json
import platform
import sys
from datetime import UTC, datetime
from typing import Literal
from unittest.mock import MagicMock, patch

from gza.config import Config
from gza.db import SqliteTaskStore
from gza.main_integration_verify import (
    MAIN_INTEGRATION_VERIFY_FRESHNESS_UNAVAILABLE_EXIT_STATUS,
    MainIntegrationVerifyEnvironmentIdentity,
    check_candidate_integration_verify,
    check_main_integration_verify,
    current_main_integration_verify_alert,
    load_main_integration_verify_state,
    persist_main_integration_verify_alert_message,
)
from gza.runner import _make_review_verify_result
from tests.cli.conftest import make_store, setup_config


def _linux_container_identity() -> MainIntegrationVerifyEnvironmentIdentity:
    return MainIntegrationVerifyEnvironmentIdentity(
        runner_class="container",
        platform_system="Linux",
        platform_machine="x86_64",
        python_executable="/usr/bin/python3",
        python_version="3.12",
    )


def _current_host_identity() -> MainIntegrationVerifyEnvironmentIdentity:
    return _current_identity(runner_class="host")


def _current_identity(
    *,
    runner_class: Literal["host", "container"],
) -> MainIntegrationVerifyEnvironmentIdentity:
    return MainIntegrationVerifyEnvironmentIdentity(
        runner_class=runner_class,
        platform_system=platform.system(),
        platform_machine=platform.machine(),
        python_executable=sys.executable,
        python_version=f"{sys.version_info.major}.{sys.version_info.minor}",
    )


def _seed_main_verify_task(
    store: SqliteTaskStore,
    *,
    verify_status: str,
    verify_exit_status: str,
    failure: str,
    alert_message: str,
    environment_identity: MainIntegrationVerifyEnvironmentIdentity | None = _current_host_identity(),
) -> str:
    task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.review_verify_command = "./bin/tests"
    task.review_verify_status = verify_status
    task.review_verify_exit_status = verify_exit_status
    task.review_verify_failure = failure
    task.review_verify_head_sha = "abc123"
    payload = {
        "alert_message": alert_message,
        "captured_at": "2026-06-23T00:00:00+00:00",
        "environment_identity": environment_identity.to_payload() if environment_identity is not None else None,
        "failing_phase": "unit",
        "gate_enabled": True,
        "head_sha": "abc123",
        "tree_fingerprint": "fp-verified",
        "verify_command": "./bin/tests",
        "verify_timeout_grace_seconds": 5.0,
        "verify_timeout_seconds": 120,
    }
    task.output_content = json.dumps(payload, sort_keys=True)
    store.update(task)
    return task.id


def test_load_main_integration_verify_state_round_trips_environment_identity(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    identity = _linux_container_identity()
    _seed_main_verify_task(
        store,
        verify_status="passed",
        verify_exit_status="0",
        failure="",
        alert_message="",
        environment_identity=identity,
    )

    state = load_main_integration_verify_state(store)

    assert state is not None
    assert state.environment_identity == identity


def test_check_main_integration_verify_treats_missing_environment_identity_as_stale(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _seed_main_verify_task(
        store,
        verify_status="passed",
        verify_exit_status="0",
        failure="",
        alert_message="",
        environment_identity=None,
    )

    config = MagicMock(spec=Config)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0
    config.main_integration_verify_red_ttl_minutes = 30

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
        patch("gza.main_integration_verify._compute_tree_fingerprint", side_effect=["fp-verified", "fp-verified"]),
        patch("gza.main_integration_verify._run_review_verify_command", return_value=verify_result) as run_verify,
        patch("gza.main_integration_verify._capture_review_verify_result", side_effect=capture_verify_result),
    ):
        check = check_main_integration_verify(
            config,
            store,
            git,
            reason="unit-test-missing-environment-identity",
        )

    run_verify.assert_called_once()
    assert check.performed_verify is True
    assert check.state.environment_identity == _current_host_identity()


def test_check_main_integration_verify_treats_environment_identity_mismatch_as_stale(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _seed_main_verify_task(
        store,
        verify_status="passed",
        verify_exit_status="0",
        failure="",
        alert_message="",
        environment_identity=_current_identity(runner_class="container"),
    )

    config = MagicMock(spec=Config)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0
    config.main_integration_verify_red_ttl_minutes = 30

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
        patch("gza.main_integration_verify._compute_tree_fingerprint", side_effect=["fp-verified", "fp-verified"]),
        patch("gza.main_integration_verify._run_review_verify_command", return_value=verify_result) as run_verify,
        patch("gza.main_integration_verify._capture_review_verify_result", side_effect=capture_verify_result),
    ):
        check = check_main_integration_verify(
            config,
            store,
            git,
            reason="unit-test-environment-identity-mismatch",
        )

    run_verify.assert_called_once()
    assert check.performed_verify is True
    assert check.state.environment_identity == _current_host_identity()


def test_check_main_integration_verify_persists_container_runner_class(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    config = MagicMock(spec=Config)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0
    config.main_integration_verify_red_ttl_minutes = 30

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
        patch("gza.main_integration_verify._compute_tree_fingerprint", side_effect=["fp-verified", "fp-verified"]),
        patch("gza.main_integration_verify._run_review_verify_command", return_value=verify_result),
        patch("gza.main_integration_verify._capture_review_verify_result", side_effect=capture_verify_result),
    ):
        check = check_main_integration_verify(
            config,
            store,
            git,
            reason="unit-test-container-runner-class",
            runner_class="container",
        )

    persisted = load_main_integration_verify_state(store)

    assert check.performed_verify is True
    assert check.state.environment_identity == _current_identity(runner_class="container")
    assert persisted is not None
    assert persisted.environment_identity == _current_identity(runner_class="container")


def test_check_candidate_integration_verify_pass_returns_structured_evidence_without_persisting_main_state(
    tmp_path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    config = MagicMock(spec=Config)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    git = MagicMock()
    git.repo_dir = tmp_path
    git.current_branch.return_value = "candidate-main"
    git.rev_parse_if_exists.return_value = "def456"

    fingerprint = "a" * 64
    verify_result = _make_review_verify_result(
        "./bin/tests",
        status="passed",
        exit_status="0",
        captured_at=datetime(2026, 6, 29, 12, 0, tzinfo=UTC),
        reviewed_branch="candidate-main",
        reviewed_head_sha="def456",
        working_directory=str(tmp_path),
        output=f"gza-verify phase=passed name=unit duration_seconds=3.25 tree_fingerprint={fingerprint}",
    )

    with (
        patch("gza.main_integration_verify._compute_tree_fingerprint", return_value=fingerprint) as compute_fingerprint,
        patch("gza.main_integration_verify._run_review_verify_command", return_value=verify_result) as run_verify,
    ):
        check = check_candidate_integration_verify(
            config,
            git,
            reason="candidate-pass",
        )

    run_verify.assert_called_once_with(
        "./bin/tests",
        cwd=tmp_path,
        reviewed_branch="candidate-main",
        reviewed_head_sha="def456",
        timeout_seconds=120,
        timeout_grace_seconds=5.0,
    )
    compute_fingerprint.assert_not_called()
    assert check.classification == "pass"
    assert check.verify_runs == 1
    assert check.merges_halted is False
    assert check.remediation is None
    assert check.evidence.environment_identity == _current_host_identity()
    assert check.evidence.tree_fingerprint == fingerprint
    assert check.evidence.head_sha == "def456"
    assert check.evidence.reviewed_branch == "candidate-main"
    assert check.evidence.working_directory == str(tmp_path)
    assert check.evidence.verify_status == "passed"
    assert check.evidence.failing_phase is None
    assert load_main_integration_verify_state(store) is None


def test_check_candidate_integration_verify_returns_container_runner_class(tmp_path) -> None:
    setup_config(tmp_path)

    config = MagicMock(spec=Config)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    git = MagicMock()
    git.repo_dir = tmp_path
    git.current_branch.return_value = "candidate-main"
    git.rev_parse_if_exists.return_value = "def456"

    fingerprint = "b" * 64
    verify_result = _make_review_verify_result(
        "./bin/tests",
        status="passed",
        exit_status="0",
        captured_at=datetime(2026, 6, 29, 12, 0, tzinfo=UTC),
        reviewed_branch="candidate-main",
        reviewed_head_sha="def456",
        working_directory=str(tmp_path),
        output=f"gza-verify phase=passed name=unit duration_seconds=3.25 tree_fingerprint={fingerprint}",
    )

    with (
        patch("gza.main_integration_verify._compute_tree_fingerprint", return_value=fingerprint),
        patch("gza.main_integration_verify._run_review_verify_command", return_value=verify_result),
    ):
        check = check_candidate_integration_verify(
            config,
            git,
            reason="candidate-container",
            runner_class="container",
        )

    assert check.classification == "pass"
    assert check.evidence.environment_identity == _current_identity(runner_class="container")


def test_check_candidate_integration_verify_red_rerun_classifies_flake(tmp_path) -> None:
    setup_config(tmp_path)

    config = MagicMock(spec=Config)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    git = MagicMock()
    git.repo_dir = tmp_path
    git.current_branch.return_value = "candidate-main"
    git.rev_parse_if_exists.return_value = "def456"

    red_result = _make_review_verify_result(
        "./bin/tests",
        status="failed",
        exit_status="1",
        captured_at=datetime(2026, 6, 29, 12, 0, tzinfo=UTC),
        reviewed_branch="candidate-main",
        reviewed_head_sha="def456",
        working_directory=str(tmp_path),
        failure="verify_command failed",
        output="gza-verify phase=failed name=functional duration_seconds=4.0",
    )
    green_result = _make_review_verify_result(
        "./bin/tests",
        status="passed",
        exit_status="0",
        captured_at=datetime(2026, 6, 29, 12, 1, tzinfo=UTC),
        reviewed_branch="candidate-main",
        reviewed_head_sha="def456",
        working_directory=str(tmp_path),
        output="all good",
    )

    with (
        patch("gza.main_integration_verify._compute_tree_fingerprint", return_value="fp-candidate"),
        patch("gza.main_integration_verify._run_review_verify_command", side_effect=[red_result, green_result]) as run_verify,
    ):
        check = check_candidate_integration_verify(
            config,
            git,
            reason="candidate-flake",
            red_reruns=1,
        )

    assert run_verify.call_count == 2
    assert check.classification == "flake"
    assert check.verify_runs == 2
    assert check.merges_halted is False
    assert check.evidence.verify_status == "passed"
    assert check.remediation is not None
    assert check.remediation.kind == "deflake"
    assert check.remediation.signature == "phase:functional"
    assert check.remediation.tree_fingerprint == "fp-candidate"
    assert check.remediation.failing_phase == "functional"
    assert check.remediation.failure == "verify_command failed"


def test_check_candidate_integration_verify_single_red_without_rerun_stays_unconfirmed(tmp_path) -> None:
    setup_config(tmp_path)

    config = MagicMock(spec=Config)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    git = MagicMock()
    git.repo_dir = tmp_path
    git.current_branch.return_value = "candidate-main"
    git.rev_parse_if_exists.return_value = "def456"

    red_result = _make_review_verify_result(
        "./bin/tests",
        status="failed",
        exit_status="1",
        captured_at=datetime(2026, 6, 29, 12, 0, tzinfo=UTC),
        reviewed_branch="candidate-main",
        reviewed_head_sha="def456",
        working_directory=str(tmp_path),
        failure="verify_command failed",
        output="gza-verify phase=failed name=functional duration_seconds=4.0",
    )

    with (
        patch("gza.main_integration_verify._compute_tree_fingerprint", return_value="fp-candidate"),
        patch("gza.main_integration_verify._run_review_verify_command", return_value=red_result) as run_verify,
    ):
        check = check_candidate_integration_verify(
            config,
            git,
            reason="candidate-unconfirmed-red",
            red_reruns=0,
        )

    run_verify.assert_called_once()
    assert check.classification == "red"
    assert check.classification != "deterministic_red"
    assert check.verify_runs == 1
    assert check.merges_halted is True
    assert check.evidence.verify_status == "failed"
    assert check.evidence.failing_phase == "functional"
    assert check.remediation is None


def test_check_candidate_integration_verify_red_rerun_classifies_deterministic_red(tmp_path) -> None:
    setup_config(tmp_path)

    config = MagicMock(spec=Config)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    git = MagicMock()
    git.repo_dir = tmp_path
    git.current_branch.return_value = "candidate-main"
    git.rev_parse_if_exists.return_value = "def456"

    first_red = _make_review_verify_result(
        "./bin/tests",
        status="failed",
        exit_status="1",
        captured_at=datetime(2026, 6, 29, 12, 0, tzinfo=UTC),
        reviewed_branch="candidate-main",
        reviewed_head_sha="def456",
        working_directory=str(tmp_path),
        failure="verify_command failed",
        output="gza-verify phase=failed name=functional duration_seconds=4.0",
    )
    second_red = _make_review_verify_result(
        "./bin/tests",
        status="failed",
        exit_status="1",
        captured_at=datetime(2026, 6, 29, 12, 1, tzinfo=UTC),
        reviewed_branch="candidate-main",
        reviewed_head_sha="def456",
        working_directory=str(tmp_path),
        failure="verify_command failed again",
        output="gza-verify phase=failed name=functional duration_seconds=4.1",
    )

    with (
        patch("gza.main_integration_verify._compute_tree_fingerprint", return_value="fp-candidate"),
        patch("gza.main_integration_verify._run_review_verify_command", side_effect=[first_red, second_red]) as run_verify,
    ):
        check = check_candidate_integration_verify(
            config,
            git,
            reason="candidate-red",
            red_reruns=1,
        )

    assert run_verify.call_count == 2
    assert check.classification == "deterministic_red"
    assert check.verify_runs == 2
    assert check.merges_halted is True
    assert check.evidence.verify_status == "failed"
    assert check.evidence.failing_phase == "functional"
    assert check.remediation is not None
    assert check.remediation.kind == "fix"
    assert check.remediation.signature == "phase:functional"
    assert check.remediation.tree_fingerprint == "fp-candidate"
    assert check.remediation.failure == "verify_command failed again"


def test_check_candidate_integration_verify_treats_missing_fingerprint_as_unavailable(tmp_path) -> None:
    setup_config(tmp_path)

    config = MagicMock(spec=Config)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    git = MagicMock()
    git.repo_dir = tmp_path
    git.current_branch.return_value = "candidate-main"
    git.rev_parse_if_exists.return_value = "def456"

    verify_result = _make_review_verify_result(
        "./bin/tests",
        status="passed",
        exit_status="0",
        captured_at=datetime(2026, 6, 29, 12, 0, tzinfo=UTC),
        reviewed_branch="candidate-main",
        reviewed_head_sha="def456",
        working_directory=str(tmp_path),
        output="all good",
    )

    with (
        patch("gza.main_integration_verify._compute_tree_fingerprint", return_value=None) as compute_fingerprint,
        patch("gza.main_integration_verify._run_review_verify_command", return_value=verify_result) as run_verify,
    ):
        check = check_candidate_integration_verify(
            config,
            git,
            reason="candidate-unavailable",
        )

    run_verify.assert_called_once()
    compute_fingerprint.assert_called_once_with(git)
    assert check.classification == "unavailable"
    assert check.verify_runs == 1
    assert check.merges_halted is True
    assert check.remediation is None
    assert check.evidence.tree_fingerprint is None
    assert check.evidence.verify_status == "unavailable"
    assert check.evidence.verify_exit_status == MAIN_INTEGRATION_VERIFY_FRESHNESS_UNAVAILABLE_EXIT_STATUS
    assert check.evidence.failure == (
        "could not prove exact local target tree freshness because the tree fingerprint is unavailable"
    )


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
    config.main_integration_verify_red_ttl_minutes = 30

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
    config.main_integration_verify_red_ttl_minutes = 30

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


def test_persist_main_integration_verify_alert_message_preserves_existing_identity_fields(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task_id = _seed_main_verify_task(
        store,
        verify_status="failed",
        verify_exit_status="1",
        failure="verify_command failed",
        alert_message="main verify RED at `abc123` - merges halted; phase `unit` failing",
    )
    task = store.get(task_id)
    assert task is not None
    state = load_main_integration_verify_state(store)
    assert state is not None

    updated = persist_main_integration_verify_alert_message(
        store,
        state=state,
        alert_message=(
            "main verify RED at `abc123` - merges halted; phase `unit` failing; "
            "automatic remediation exhausted after 2/2 attempts for unit on fp-verified; "
            "human intervention required"
        ),
    )

    assert updated.task.id == task_id
    assert updated.verify_command == "./bin/tests"
    assert updated.verify_timeout_seconds == 120
    assert updated.verify_timeout_grace_seconds == 5.0
    assert updated.tree_fingerprint == "fp-verified"
    assert updated.head_sha == "abc123"
    assert updated.failing_phase == "unit"
    assert "automatic remediation exhausted after 2/2 attempts" in (updated.alert_message or "")
    reloaded = load_main_integration_verify_state(store)
    assert reloaded is not None
    assert reloaded.alert_message == updated.alert_message
    assert reloaded.tree_fingerprint == "fp-verified"
    assert reloaded.head_sha == "abc123"


def test_check_main_integration_verify_reuses_same_tree_green_checkpoint_without_rerun(tmp_path) -> None:
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
    config.main_integration_verify_red_ttl_minutes = 30

    git = MagicMock()
    git.repo_dir = tmp_path
    git.current_branch.return_value = "main"
    git.rev_parse_if_exists.return_value = "abc123"

    with (
        patch("gza.main_integration_verify._compute_tree_fingerprint", return_value="fp-verified"),
        patch("gza.main_integration_verify._run_review_verify_command") as run_verify,
    ):
        check = check_main_integration_verify(
            config,
            store,
            git,
            reason="unit-test",
        )

    run_verify.assert_not_called()
    assert check.performed_verify is False
    assert check.is_current is True
    assert check.merges_halted is False
    assert check.state.verify_status == "passed"


def test_check_main_integration_verify_reuses_fresh_same_tree_red_checkpoint_before_ttl(tmp_path) -> None:
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
    config.main_integration_verify_red_ttl_minutes = 30

    git = MagicMock()
    git.repo_dir = tmp_path
    git.current_branch.return_value = "main"
    git.rev_parse_if_exists.return_value = "abc123"

    frozen_now = datetime(2026, 6, 23, 0, 29, tzinfo=UTC)

    with (
        patch("gza.main_integration_verify._compute_tree_fingerprint", return_value="fp-verified"),
        patch("gza.main_integration_verify._run_review_verify_command") as run_verify,
        patch("gza.main_integration_verify.datetime") as mocked_datetime,
    ):
        mocked_datetime.now.return_value = frozen_now
        mocked_datetime.fromisoformat.side_effect = datetime.fromisoformat
        check = check_main_integration_verify(
            config,
            store,
            git,
            reason="unit-test-red-fresh",
        )

    run_verify.assert_not_called()
    assert check.performed_verify is False
    assert check.is_current is True
    assert check.merges_halted is True
    assert check.state.verify_status == "failed"


def test_check_main_integration_verify_reruns_expired_same_tree_red_checkpoint(tmp_path) -> None:
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
    config.main_integration_verify_red_ttl_minutes = 30

    git = MagicMock()
    git.repo_dir = tmp_path
    git.current_branch.return_value = "main"
    git.rev_parse_if_exists.return_value = "abc123"

    verify_result = _make_review_verify_result(
        "./bin/tests",
        status="passed",
        exit_status="0",
        captured_at=datetime(2026, 6, 23, 1, 0, tzinfo=UTC),
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

    frozen_now = datetime(2026, 6, 23, 1, 31, tzinfo=UTC)

    with (
        patch("gza.main_integration_verify._compute_tree_fingerprint", side_effect=["fp-verified", "fp-verified"]),
        patch("gza.main_integration_verify._run_review_verify_command", return_value=verify_result) as run_verify,
        patch("gza.main_integration_verify._capture_review_verify_result", side_effect=capture_verify_result),
        patch("gza.main_integration_verify.datetime") as mocked_datetime,
    ):
        mocked_datetime.now.return_value = frozen_now
        mocked_datetime.fromisoformat.side_effect = datetime.fromisoformat
        check = check_main_integration_verify(
            config,
            store,
            git,
            reason="unit-test-red-ttl",
        )

    run_verify.assert_called_once()
    assert check.performed_verify is True
    assert check.merges_halted is False
    assert check.state.verify_status == "passed"


def test_check_main_integration_verify_watch_red_rerun_classifies_flake_without_halting(tmp_path) -> None:
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
    config.main_integration_verify_red_ttl_minutes = 30

    git = MagicMock()
    git.repo_dir = tmp_path
    git.current_branch.return_value = "main"
    git.rev_parse_if_exists.return_value = "abc123"

    green_result = _make_review_verify_result(
        "./bin/tests",
        status="passed",
        exit_status="0",
        captured_at=datetime(2026, 6, 23, 0, 35, tzinfo=UTC),
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
        patch("gza.main_integration_verify._compute_tree_fingerprint", side_effect=["fp-verified", "fp-verified"]),
        patch("gza.main_integration_verify._run_review_verify_command", return_value=green_result) as run_verify,
        patch("gza.main_integration_verify._capture_review_verify_result", side_effect=capture_verify_result),
        patch("gza.main_integration_verify.datetime") as mocked_datetime,
    ):
        mocked_datetime.now.return_value = datetime(2026, 6, 23, 0, 29, tzinfo=UTC)
        mocked_datetime.fromisoformat.side_effect = datetime.fromisoformat
        check = check_main_integration_verify(
            config,
            store,
            git,
            reason="watch-main-verify",
            red_reruns=2,
        )

    run_verify.assert_called_once()
    assert check.performed_verify is True
    assert check.verify_runs == 1
    assert check.merges_halted is False
    assert check.state.verify_status == "passed"
    assert check.remediation is not None
    assert check.remediation.kind == "deflake"
    assert check.remediation.signature == "phase:unit"
    assert check.remediation.tree_fingerprint == "fp-verified"
    assert check.remediation.failing_phase == "unit"


def test_check_main_integration_verify_watch_red_rerun_retries_fresh_red_and_classifies_flake(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    config = MagicMock(spec=Config)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0
    config.main_integration_verify_red_ttl_minutes = 30

    git = MagicMock()
    git.repo_dir = tmp_path
    git.current_branch.return_value = "main"
    git.rev_parse_if_exists.return_value = "abc123"

    red_result = _make_review_verify_result(
        "./bin/tests",
        status="failed",
        exit_status="1",
        captured_at=datetime(2026, 6, 23, 0, 0, tzinfo=UTC),
        reviewed_branch="main",
        reviewed_head_sha="abc123",
        working_directory=str(tmp_path),
        failure="verify_command failed",
        output="gza-verify phase=failed name=functional duration_seconds=3.25",
    )
    green_result = _make_review_verify_result(
        "./bin/tests",
        status="passed",
        exit_status="0",
        captured_at=datetime(2026, 6, 23, 0, 1, tzinfo=UTC),
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
        patch("gza.main_integration_verify._compute_tree_fingerprint", side_effect=["fp-live", "fp-live", "fp-live"]),
        patch("gza.main_integration_verify._run_review_verify_command", side_effect=[red_result, green_result]) as run_verify,
        patch("gza.main_integration_verify._capture_review_verify_result", side_effect=capture_verify_result),
    ):
        check = check_main_integration_verify(
            config,
            store,
            git,
            reason="watch-main-verify",
            red_reruns=2,
        )

    assert run_verify.call_count == 2
    assert check.performed_verify is True
    assert check.verify_runs == 2
    assert check.merges_halted is False
    assert check.state.verify_status == "passed"
    assert check.remediation is not None
    assert check.remediation.kind == "deflake"
    assert check.remediation.signature == "phase:functional"
    assert check.remediation.tree_fingerprint == "fp-live"
    assert check.remediation.failing_phase == "functional"


def test_check_main_integration_verify_watch_red_rerun_classifies_deterministic_red(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    config = MagicMock(spec=Config)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0
    config.main_integration_verify_red_ttl_minutes = 30

    git = MagicMock()
    git.repo_dir = tmp_path
    git.current_branch.return_value = "main"
    git.rev_parse_if_exists.return_value = "abc123"

    first_red = _make_review_verify_result(
        "./bin/tests",
        status="failed",
        exit_status="1",
        captured_at=datetime(2026, 6, 23, 0, 0, tzinfo=UTC),
        reviewed_branch="main",
        reviewed_head_sha="abc123",
        working_directory=str(tmp_path),
        failure="verify_command failed",
        output="gza-verify phase=failed name=functional duration_seconds=3.25",
    )
    second_red = _make_review_verify_result(
        "./bin/tests",
        status="failed",
        exit_status="1",
        captured_at=datetime(2026, 6, 23, 0, 1, tzinfo=UTC),
        reviewed_branch="main",
        reviewed_head_sha="abc123",
        working_directory=str(tmp_path),
        failure="verify_command failed again",
        output="gza-verify phase=failed name=functional duration_seconds=3.10",
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
        patch("gza.main_integration_verify._compute_tree_fingerprint", side_effect=["fp-live", "fp-live", "fp-live", "fp-live"]),
        patch("gza.main_integration_verify._run_review_verify_command", side_effect=[first_red, second_red]) as run_verify,
        patch("gza.main_integration_verify._capture_review_verify_result", side_effect=capture_verify_result),
    ):
        check = check_main_integration_verify(
            config,
            store,
            git,
            reason="watch-main-verify",
            red_reruns=1,
        )

    assert run_verify.call_count == 2
    assert check.performed_verify is True
    assert check.verify_runs == 2
    assert check.merges_halted is True
    assert check.state.verify_status == "failed"
    assert check.remediation is not None
    assert check.remediation.kind == "fix"
    assert check.remediation.signature == "phase:functional"
    assert check.remediation.tree_fingerprint == "fp-live"
    assert check.remediation.failing_phase == "functional"


def test_check_main_integration_verify_deterministic_red_uses_confirmed_current_failure_metadata(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _seed_main_verify_task(
        store,
        verify_status="failed",
        verify_exit_status="1",
        failure="cached verify_command failed",
        alert_message="main verify RED at `abc123` - merges halted; phase `unit` failing",
    )

    config = MagicMock(spec=Config)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0
    config.main_integration_verify_red_ttl_minutes = 30

    git = MagicMock()
    git.repo_dir = tmp_path
    git.current_branch.return_value = "main"
    git.rev_parse_if_exists.return_value = "abc123"

    first_red = _make_review_verify_result(
        "./bin/tests",
        status="failed",
        exit_status="1",
        captured_at=datetime(2026, 6, 23, 0, 0, tzinfo=UTC),
        reviewed_branch="main",
        reviewed_head_sha="abc123",
        working_directory=str(tmp_path),
        failure="fresh verify_command failed",
        output="gza-verify phase=failed name=functional duration_seconds=3.25",
    )
    second_red = _make_review_verify_result(
        "./bin/tests",
        status="failed",
        exit_status="1",
        captured_at=datetime(2026, 6, 23, 0, 1, tzinfo=UTC),
        reviewed_branch="main",
        reviewed_head_sha="abc123",
        working_directory=str(tmp_path),
        failure="fresh verify_command failed again",
        output="gza-verify phase=failed name=functional duration_seconds=3.10",
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
        patch("gza.main_integration_verify._compute_tree_fingerprint", side_effect=["fp-verified", "fp-verified", "fp-verified"]),
        patch("gza.main_integration_verify._run_review_verify_command", side_effect=[first_red, second_red]) as run_verify,
        patch("gza.main_integration_verify._capture_review_verify_result", side_effect=capture_verify_result),
        patch("gza.main_integration_verify.datetime") as mocked_datetime,
    ):
        mocked_datetime.now.return_value = datetime(2026, 6, 23, 0, 29, tzinfo=UTC)
        mocked_datetime.fromisoformat.side_effect = datetime.fromisoformat
        check = check_main_integration_verify(
            config,
            store,
            git,
            reason="watch-main-verify",
            red_reruns=1,
        )

    assert run_verify.call_count == 2
    assert check.performed_verify is True
    assert check.verify_runs == 2
    assert check.merges_halted is True
    assert check.remediation is not None
    assert check.remediation.kind == "fix"
    assert check.remediation.signature == "phase:functional"
    assert check.remediation.tree_fingerprint == "fp-verified"
    assert check.remediation.failing_phase == "functional"
    assert check.remediation.failure == "fresh verify_command failed again"
