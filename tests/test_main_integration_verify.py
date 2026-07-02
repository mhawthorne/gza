from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from gza.artifacts import store_command_output_artifact
from gza.cli.watch import _main_verify_remediation_prompt
from gza.config import Config
from gza.db import SqliteTaskStore
from gza.main_integration_verify import (
    MAIN_INTEGRATION_VERIFY_FRESHNESS_UNAVAILABLE_EXIT_STATUS,
    _build_main_integration_verify_remediation,
    check_main_integration_verify,
    current_main_integration_verify_alert,
    load_main_integration_verify_state,
    persist_main_integration_verify_alert_message,
    run_main_integration_verify,
)
from gza.runner import _make_review_verify_result
from tests.cli.conftest import make_store, setup_config


def _seed_main_verify_task(
    store: SqliteTaskStore,
    *,
    verify_status: str,
    verify_exit_status: str,
    failure: str,
    alert_message: str,
    failing_phase: str = "unit",
    failure_signature: str | None = None,
    pending_retirement_signatures: tuple[str, ...] = (),
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
    task.output_content = json.dumps(
        {
            "alert_message": alert_message,
            "captured_at": "2026-06-23T00:00:00+00:00",
            "failure_signature": failure_signature,
            "failing_phase": failing_phase,
            "gate_enabled": True,
            "head_sha": "abc123",
            "pending_retirement_signatures": list(pending_retirement_signatures),
            "tree_fingerprint": "fp-verified",
            "verify_command": "./bin/tests",
            "verify_timeout_grace_seconds": 5.0,
            "verify_timeout_seconds": 120,
        },
        sort_keys=True,
    )
    store.update(task)
    return task.id


def test_build_main_integration_verify_remediation_uses_preferred_verify_artifact_and_bounded_excerpt(tmp_path) -> None:
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
    config = Config.load(tmp_path)

    older = store_command_output_artifact(
        store,
        task,
        config,
        kind="verify_command_output",
        producer="main_verify_test",
        label="verify older",
        output="\n".join(
            [
                *(f"noise line {index}" for index in range(20)),
                "WORKER_DIED subprocess boundary failure",
                "=========================== short test summary info ============================",
                "FAILED tests/test_alpha.py::test_one - AssertionError: boom",
                "FAILED tests/test_beta.py::test_two - RuntimeError: kaboom",
                "============================== 2 failed in 0.20s ==============================",
            ]
        ),
        created_at=datetime(2026, 6, 23, 0, 0, tzinfo=UTC),
    )
    newer = store_command_output_artifact(
        store,
        task,
        config,
        kind="verify_command_output",
        producer="main_verify_test",
        label="verify newer",
        output="FAILED tests/test_newer.py::test_latest - AssertionError: newer",
        created_at=datetime(2026, 6, 23, 0, 1, tzinfo=UTC),
    )
    task.review_verify_artifact_file = older.path
    store.update(task)

    state = load_main_integration_verify_state(store)
    assert state is not None
    remediation = _build_main_integration_verify_remediation(
        kind="fix",
        config=config,
        store=store,
        state=state,
    )

    assert remediation.artifact_path == older.path
    assert remediation.artifact_path != newer.path
    assert remediation.failing_test_ids == (
        "tests/test_alpha.py::test_one",
        "tests/test_beta.py::test_two",
    )
    assert remediation.verify_excerpt is not None
    assert "WORKER_DIED subprocess boundary failure" in remediation.verify_excerpt
    assert "FAILED tests/test_alpha.py::test_one - AssertionError: boom" in remediation.verify_excerpt
    assert "noise line 0" not in remediation.verify_excerpt
    assert len(remediation.verify_excerpt.splitlines()) <= 24


def test_build_main_integration_verify_remediation_falls_back_to_newest_verify_artifact(tmp_path) -> None:
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
    config = Config.load(tmp_path)

    older = store_command_output_artifact(
        store,
        task,
        config,
        kind="verify_command_output",
        producer="main_verify_test",
        label="verify older",
        output="older failure output",
        created_at=datetime(2026, 6, 23, 0, 0, tzinfo=UTC),
    )
    newer = store_command_output_artifact(
        store,
        task,
        config,
        kind="verify_command_output",
        producer="main_verify_test",
        label="verify newer",
        output="newest failure output",
        created_at=datetime(2026, 6, 23, 0, 1, tzinfo=UTC),
    )
    task.review_verify_artifact_file = older.path + ".missing"
    store.update(task)

    state = load_main_integration_verify_state(store)
    assert state is not None
    remediation = _build_main_integration_verify_remediation(
        kind="fix",
        config=config,
        store=store,
        state=state,
    )

    assert remediation.artifact_path == newer.path
    assert remediation.failing_test_ids == ()
    assert remediation.verify_excerpt == "newest failure output"


def test_build_main_integration_verify_remediation_skips_unreadable_preferred_artifact_for_newer_readable_evidence(
    tmp_path,
) -> None:
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
    config = Config.load(tmp_path)

    preferred = store_command_output_artifact(
        store,
        task,
        config,
        kind="verify_command_output",
        producer="main_verify_test",
        label="verify preferred",
        output="FAILED tests/test_old.py::test_preferred - AssertionError: old",
        created_at=datetime(2026, 6, 23, 0, 0, tzinfo=UTC),
    )
    newer = store_command_output_artifact(
        store,
        task,
        config,
        kind="verify_command_output",
        producer="main_verify_test",
        label="verify newer",
        output="\n".join(
            [
                "WORKER_DIED subprocess boundary failure",
                "=========================== short test summary info ============================",
                "FAILED tests/test_newer.py::test_latest - AssertionError: newer",
                "============================== 1 failed in 0.20s ==============================",
            ]
        ),
        created_at=datetime(2026, 6, 23, 0, 1, tzinfo=UTC),
    )
    (tmp_path / preferred.path).unlink()
    task.review_verify_artifact_file = preferred.path
    store.update(task)

    state = load_main_integration_verify_state(store)
    assert state is not None
    remediation = _build_main_integration_verify_remediation(
        kind="fix",
        config=config,
        store=store,
        state=state,
    )

    assert remediation.artifact_path == newer.path
    assert remediation.failing_test_ids == ("tests/test_newer.py::test_latest",)
    assert remediation.verify_excerpt is not None
    assert "WORKER_DIED subprocess boundary failure" in remediation.verify_excerpt


def test_build_main_integration_verify_remediation_omits_missing_artifact_evidence_and_prompt_line(
    tmp_path,
) -> None:
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
    config = Config.load(tmp_path)

    older = store_command_output_artifact(
        store,
        task,
        config,
        kind="verify_command_output",
        producer="main_verify_test",
        label="verify older",
        output="FAILED tests/test_old.py::test_old - AssertionError: old",
        created_at=datetime(2026, 6, 23, 0, 0, tzinfo=UTC),
    )
    newer = store_command_output_artifact(
        store,
        task,
        config,
        kind="verify_command_output",
        producer="main_verify_test",
        label="verify newer",
        output="FAILED tests/test_newer.py::test_latest - AssertionError: newer",
        created_at=datetime(2026, 6, 23, 0, 1, tzinfo=UTC),
    )
    (tmp_path / older.path).unlink()
    (tmp_path / newer.path).unlink()
    task.review_verify_artifact_file = older.path
    store.update(task)

    state = load_main_integration_verify_state(store)
    assert state is not None
    remediation = _build_main_integration_verify_remediation(
        kind="fix",
        config=config,
        store=store,
        state=state,
    )

    assert remediation.artifact_path is None
    assert remediation.failing_test_ids == ()
    assert remediation.verify_excerpt is None
    prompt = _main_verify_remediation_prompt(remediation, head_sha=state.head_sha)
    assert "Verify artifact:" not in prompt


def test_build_main_integration_verify_remediation_preserves_ruff_failure_excerpt_without_pytest_ids(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task_id = _seed_main_verify_task(
        store,
        verify_status="failed",
        verify_exit_status="1",
        failure="verify_command failed",
        alert_message="main verify RED at `abc123` - merges halted; phase `ruff` failing",
        failing_phase="ruff",
    )
    task = store.get(task_id)
    assert task is not None
    config = Config.load(tmp_path)

    artifact = store_command_output_artifact(
        store,
        task,
        config,
        kind="verify_command_output",
        producer="main_verify_test",
        label="verify ruff",
        output="\n".join(
            [
                "gza-verify phase=start name=ruff",
                "src/gza/main_integration_verify.py:19:1: F401 [*] imported but unused",
                "Found 1 error.",
                "gza-verify phase=failed name=ruff duration_seconds=0.42 tree_fingerprint=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            ]
        ),
        created_at=datetime(2026, 6, 23, 0, 1, tzinfo=UTC),
    )
    task.review_verify_artifact_file = artifact.path
    store.update(task)

    state = load_main_integration_verify_state(store)
    assert state is not None
    remediation = _build_main_integration_verify_remediation(
        kind="fix",
        config=config,
        store=store,
        state=state,
    )

    assert remediation.signature == "phase:ruff"
    assert remediation.failing_phase == "ruff"
    assert remediation.artifact_path == artifact.path
    assert remediation.failing_test_ids == ()
    assert remediation.verify_excerpt is not None
    assert "src/gza/main_integration_verify.py:19:1: F401 [*] imported but unused" in remediation.verify_excerpt
    assert "gza-verify phase=failed name=ruff duration_seconds=0.42" in remediation.verify_excerpt


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
        pending_retirement_signatures=("phase:functional",),
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
            "automatic remediation exhausted after 2/2 attempts for phase:unit on fp-verified; "
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
    assert updated.failure_signature == "phase:unit"
    assert updated.pending_retirement_signatures == ("phase:functional",)
    assert "automatic remediation exhausted after 2/2 attempts" in (updated.alert_message or "")
    reloaded = load_main_integration_verify_state(store)
    assert reloaded is not None
    assert reloaded.alert_message == updated.alert_message
    assert reloaded.tree_fingerprint == "fp-verified"
    assert reloaded.head_sha == "abc123"
    assert reloaded.failure_signature == "phase:unit"
    assert reloaded.pending_retirement_signatures == ("phase:functional",)


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
    assert check.resolved_red_signature == "phase:unit"
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
    assert check.resolved_red_signature == "phase:functional"
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


def test_check_main_integration_verify_watch_red_rerun_classifies_deterministic_ruff_red(tmp_path) -> None:
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
        output=(
            "gza-verify phase=start name=ruff\n"
            "src/gza/main_integration_verify.py:19:1: F401 [*] imported but unused\n"
            "gza-verify phase=failed name=ruff duration_seconds=0.25"
        ),
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
        output=(
            "gza-verify phase=start name=ruff\n"
            "src/gza/main_integration_verify.py:19:1: F401 [*] imported but unused\n"
            "gza-verify phase=failed name=ruff duration_seconds=0.20"
        ),
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
    assert check.remediation.signature == "phase:ruff"
    assert check.remediation.tree_fingerprint == "fp-live"
    assert check.remediation.failing_phase == "ruff"
    assert check.remediation.failure == "verify_command failed again"


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


def test_run_main_integration_verify_sets_red_since_on_first_red(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    config = MagicMock(spec=Config)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    git = MagicMock()
    git.repo_dir = tmp_path
    git.current_branch.return_value = "main"
    git.rev_parse_if_exists.return_value = "abc123"

    red_result = _make_review_verify_result(
        "./bin/tests",
        status="failed",
        exit_status="1",
        captured_at=datetime(2026, 6, 23, 0, 5, tzinfo=UTC),
        reviewed_branch="main",
        reviewed_head_sha="abc123",
        working_directory=str(tmp_path),
        failure="verify_command failed",
        output="gza-verify phase=failed name=unit duration_seconds=3.25",
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
        patch("gza.main_integration_verify._compute_tree_fingerprint", return_value="fp-verified"),
        patch("gza.main_integration_verify._run_review_verify_command", return_value=red_result),
        patch("gza.main_integration_verify._capture_review_verify_result", side_effect=capture_verify_result),
    ):
        state = run_main_integration_verify(config, store, git, reason="unit-test-first-red")

    assert state.red_since == red_result.captured_at
    persisted = load_main_integration_verify_state(store)
    assert persisted is not None
    assert persisted.red_since == red_result.captured_at


def test_run_main_integration_verify_preserves_red_since_across_consecutive_red_reruns(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    config = MagicMock(spec=Config)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    git = MagicMock()
    git.repo_dir = tmp_path
    git.current_branch.return_value = "main"
    git.rev_parse_if_exists.return_value = "abc123"

    first_red = _make_review_verify_result(
        "./bin/tests",
        status="failed",
        exit_status="1",
        captured_at=datetime(2026, 6, 23, 0, 5, tzinfo=UTC),
        reviewed_branch="main",
        reviewed_head_sha="abc123",
        working_directory=str(tmp_path),
        failure="verify_command failed",
        output="gza-verify phase=failed name=unit duration_seconds=3.25",
    )
    second_red = _make_review_verify_result(
        "./bin/tests",
        status="failed",
        exit_status="1",
        captured_at=datetime(2026, 6, 23, 0, 12, tzinfo=UTC),
        reviewed_branch="main",
        reviewed_head_sha="abc123",
        working_directory=str(tmp_path),
        failure="verify_command failed again",
        output="gza-verify phase=failed name=unit duration_seconds=3.10",
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
        patch("gza.main_integration_verify._compute_tree_fingerprint", return_value="fp-verified"),
        patch("gza.main_integration_verify._run_review_verify_command", side_effect=[first_red, second_red]),
        patch("gza.main_integration_verify._capture_review_verify_result", side_effect=capture_verify_result),
    ):
        first_state = run_main_integration_verify(config, store, git, reason="unit-test-first-red")
        second_state = run_main_integration_verify(config, store, git, reason="unit-test-second-red")

    assert first_state.red_since == first_red.captured_at
    assert second_state.red_since == first_red.captured_at
    assert second_state.captured_at == second_red.captured_at


def test_run_main_integration_verify_resets_red_since_on_green_and_rearms_on_next_red(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    config = MagicMock(spec=Config)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    git = MagicMock()
    git.repo_dir = tmp_path
    git.current_branch.return_value = "main"
    git.rev_parse_if_exists.return_value = "abc123"

    first_red = _make_review_verify_result(
        "./bin/tests",
        status="failed",
        exit_status="1",
        captured_at=datetime(2026, 6, 23, 0, 5, tzinfo=UTC),
        reviewed_branch="main",
        reviewed_head_sha="abc123",
        working_directory=str(tmp_path),
        failure="verify_command failed",
        output="gza-verify phase=failed name=unit duration_seconds=3.25",
    )
    green = _make_review_verify_result(
        "./bin/tests",
        status="passed",
        exit_status="0",
        captured_at=datetime(2026, 6, 23, 0, 10, tzinfo=UTC),
        reviewed_branch="main",
        reviewed_head_sha="abc123",
        working_directory=str(tmp_path),
        output="all good",
    )
    second_red = _make_review_verify_result(
        "./bin/tests",
        status="failed",
        exit_status="1",
        captured_at=datetime(2026, 6, 23, 0, 20, tzinfo=UTC),
        reviewed_branch="main",
        reviewed_head_sha="abc123",
        working_directory=str(tmp_path),
        failure="verify_command failed again",
        output="gza-verify phase=failed name=unit duration_seconds=3.10",
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
        patch("gza.main_integration_verify._compute_tree_fingerprint", return_value="fp-verified"),
        patch("gza.main_integration_verify._run_review_verify_command", side_effect=[first_red, green, second_red]),
        patch("gza.main_integration_verify._capture_review_verify_result", side_effect=capture_verify_result),
    ):
        first_state = run_main_integration_verify(config, store, git, reason="unit-test-first-red")
        green_state = run_main_integration_verify(config, store, git, reason="unit-test-green")
        second_state = run_main_integration_verify(config, store, git, reason="unit-test-second-red")

    assert first_state.red_since == first_red.captured_at
    assert green_state.red_since is None
    assert second_state.red_since == second_red.captured_at
