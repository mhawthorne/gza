"""Tests for transient recovery cooldown helpers."""

from pathlib import Path
from types import SimpleNamespace

from gza.config import Config
from gza.recovery_transients import classify_transient_recovery_terminal, compute_transient_recovery_backoff_seconds


def _task(**overrides: object) -> SimpleNamespace:
    base = {
        "status": "failed",
        "failure_reason": "PROVIDER_UNAVAILABLE",
        "task_type": "implement",
        "has_commits": False,
        "changed_diff": None,
        "diff_files_changed": None,
        "diff_lines_added": None,
        "diff_lines_removed": None,
        "num_steps_reported": None,
        "num_steps_computed": None,
        "num_turns_reported": None,
        "num_turns_computed": None,
        "output_tokens": None,
        "output_content": None,
        "log_file": None,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_compute_transient_recovery_backoff_seconds_follows_bounded_schedule(tmp_path: Path) -> None:
    (tmp_path / "gza.yaml").write_text("project_name: demo\n", encoding="utf-8")
    config = Config.load(tmp_path)

    assert compute_transient_recovery_backoff_seconds(config, 0) == 0
    assert compute_transient_recovery_backoff_seconds(config, 1) == 60
    assert compute_transient_recovery_backoff_seconds(config, 2) == 120
    assert compute_transient_recovery_backoff_seconds(config, 3) == 300
    assert compute_transient_recovery_backoff_seconds(config, 4) == 600
    assert compute_transient_recovery_backoff_seconds(config, 5) == 1200
    assert compute_transient_recovery_backoff_seconds(config, 6) == 1800
    assert compute_transient_recovery_backoff_seconds(config, 7) == 1800


def test_compute_transient_recovery_backoff_seconds_scales_from_initial_and_caps_at_max(tmp_path: Path) -> None:
    (tmp_path / "gza.yaml").write_text(
        "project_name: demo\n"
        "watch:\n"
        "  failure_backoff_initial: 30\n"
        "  transient_recovery_backoff_max: 500\n",
        encoding="utf-8",
    )
    config = Config.load(tmp_path)

    assert compute_transient_recovery_backoff_seconds(config, 1) == 30
    assert compute_transient_recovery_backoff_seconds(config, 2) == 60
    assert compute_transient_recovery_backoff_seconds(config, 3) == 150
    assert compute_transient_recovery_backoff_seconds(config, 4) == 300
    assert compute_transient_recovery_backoff_seconds(config, 5) == 500


def test_classify_transient_recovery_terminal_provider_unavailable_without_execution_is_transient() -> None:
    transient = classify_transient_recovery_terminal(_task(failure_reason="PROVIDER_UNAVAILABLE"))

    assert transient is not None
    assert transient.failure_reason == "PROVIDER_UNAVAILABLE"


def test_classify_transient_recovery_terminal_provider_unavailable_with_execution_and_no_capacity_proof_is_not_transient() -> None:
    transient = classify_transient_recovery_terminal(
        _task(failure_reason="PROVIDER_UNAVAILABLE", num_steps_computed=1, output_tokens=12)
    )

    assert transient is None


def test_classify_transient_recovery_terminal_retryable_provider_error_with_turn_evidence_and_no_capacity_proof_is_not_transient() -> None:
    transient = classify_transient_recovery_terminal(
        _task(failure_reason="RETRYABLE_PROVIDER_ERROR", num_turns_reported=1, output_tokens=4)
    )

    assert transient is None


def test_classify_transient_recovery_terminal_provider_unavailable_with_execution_and_explicit_capacity_proof_is_transient() -> None:
    transient = classify_transient_recovery_terminal(
        _task(
            failure_reason="PROVIDER_UNAVAILABLE",
            num_turns_reported=1,
            output_tokens=32,
            output_content="Selected model is at capacity. Try again shortly.",
        )
    )

    assert transient is not None
    assert transient.failure_reason == "PROVIDER_UNAVAILABLE"


def test_classify_transient_recovery_terminal_timeout_before_execution_is_transient() -> None:
    transient = classify_transient_recovery_terminal(_task(failure_reason="TIMEOUT"))

    assert transient is not None
    assert transient.code == "timeout-before-execution"


def test_classify_transient_recovery_terminal_timeout_after_execution_is_not_transient() -> None:
    transient = classify_transient_recovery_terminal(_task(failure_reason="TIMEOUT", num_turns_reported=1))

    assert transient is None


def test_classify_transient_recovery_terminal_worker_died_before_commits_is_transient() -> None:
    transient = classify_transient_recovery_terminal(_task(failure_reason="WORKER_DIED"))

    assert transient is not None
    assert transient.failure_reason == "WORKER_DIED"


def test_classify_transient_recovery_terminal_workspace_not_populated_is_transient() -> None:
    transient = classify_transient_recovery_terminal(_task(failure_reason="WORKSPACE_NOT_POPULATED"))

    assert transient is not None
    assert transient.failure_reason == "WORKSPACE_NOT_POPULATED"


def test_classify_transient_recovery_terminal_completed_noop_improve_is_not_transient() -> None:
    transient = classify_transient_recovery_terminal(
        _task(status="completed", task_type="improve", failure_reason=None, changed_diff=False)
    )

    assert transient is None


def test_classify_transient_recovery_terminal_manual_and_published_failures_are_not_transient() -> None:
    for failure_reason in ("MANUAL_REVIEW_REQUIRED", "CONFIG_ERROR", "GIT_ERROR", "TEST_FAILURE"):
        assert classify_transient_recovery_terminal(_task(failure_reason=failure_reason)) is None
