from __future__ import annotations

import json
import platform
import sys
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Literal
from unittest.mock import MagicMock, call, patch

import pytest

from gza.artifacts import store_command_output_artifact
from gza.cli.watch import (
    _candidate_rework_identity,
    _main_verify_remediation_prompt,
    _queue_candidate_rework_task,
    _queue_main_verify_remediation_task,
)
from gza.config import Config, ConfigError
from gza.db import SqliteTaskStore
from gza.git import GitError
from gza.main_integration_verify import (
    MAIN_INTEGRATION_VERIFY_FRESHNESS_UNAVAILABLE_EXIT_STATUS,
    MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS,
    CandidateIntegrationVerifyCheck,
    CandidateIntegrationVerifyEvidence,
    MainIntegrationVerifyEnvironmentIdentity,
    MainIntegrationVerifyRemediation,
    MainIntegrationVerifyTargetProof,
    _build_main_integration_verify_remediation,
    check_candidate_integration_verify,
    check_main_integration_verify,
    current_main_integration_verify_alert,
    format_main_integration_verify_attention_message,
    load_main_integration_verify_state,
    main_integration_verify_state_halts_merges,
    main_integration_verify_state_has_exhausted_remediation_attention,
    main_integration_verify_state_is_red_verdict,
    persist_main_integration_verify_alert_message,
    resolve_main_integration_verify_target_proof,
    run_main_integration_verify,
)
from gza.runner import _compute_tree_fingerprint, _make_review_verify_result
from tests.cli.conftest import make_store, setup_config


def _setup_plan_only_model_config(tmp_path) -> Config:
    config_path = tmp_path / "gza.yaml"
    worktree_dir = tmp_path / ".gza-test-worktrees"
    db_path = tmp_path / ".gza" / "gza.db"
    config_path.write_text(
        "project_name: test-project\n"
        f"worktree_dir: {worktree_dir}\n"
        f"db_path: {db_path}\n"
        "provider: codex\n"
        "quiet_period_seconds: 0\n"
        "providers:\n"
        "  codex:\n"
        "    task_types:\n"
        "      plan:\n"
        "        model: gpt-5.5\n"
    )
    return Config.load(tmp_path)


def _linux_container_identity() -> MainIntegrationVerifyEnvironmentIdentity:
    return MainIntegrationVerifyEnvironmentIdentity(
        runner_class="container",
        platform_system="Linux",
        platform_machine="x86_64",
        python_implementation="CPython",
        python_version="3.12",
    )


def _current_host_identity() -> MainIntegrationVerifyEnvironmentIdentity:
    return _current_identity(runner_class="host")


def _current_identity(
    *,
    runner_class: Literal["host", "container"],
    python_executable_family: str | None = None,
) -> MainIntegrationVerifyEnvironmentIdentity:
    return MainIntegrationVerifyEnvironmentIdentity(
        runner_class=runner_class,
        platform_system=platform.system(),
        platform_machine=platform.machine(),
        python_implementation=platform.python_implementation(),
        python_version=f"{sys.version_info.major}.{sys.version_info.minor}",
        python_executable_family=python_executable_family,
    )


def test_main_verify_remediation_pending_reuse_requires_resolved_route_before_mutation(tmp_path) -> None:
    config = _setup_plan_only_model_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Existing main verify remediation", task_type="implement")
    assert task.id is not None
    store.update(task)
    original = task.__dict__.copy()
    remediation = MainIntegrationVerifyRemediation(
        kind="fix",
        signature="phase:unit",
        tree_fingerprint="fp-verified",
        failing_phase="unit",
        failure="unit failed",
        observed_environment_identity=None,
        artifact_path=None,
        failing_test_ids=(),
        verify_excerpt=None,
    )

    with pytest.raises(ConfigError, match="'model' is required for task type 'implement'"):
        _queue_main_verify_remediation_task(
            config=config,
            store=store,
            task=task,
            remediation=remediation,
            head_sha="abc123",
            desired_tags=("system", "main-verify"),
            tags=None,
            any_tag=False,
        )

    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.__dict__ == original
    assert len(store.get_all()) == 1
    assert (
        store.get_main_verify_remediation_attempt_state(
            signature="phase:unit",
            tree_fingerprint="fp-verified",
        )
        is None
    )


def test_main_verify_remediation_completed_unmerged_consumes_attempt_and_requeues(tmp_path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    remediation = MainIntegrationVerifyRemediation(
        kind="fix",
        signature="phase:ruff",
        tree_fingerprint=None,
        failing_phase="ruff",
        failure="ruff failed",
        observed_environment_identity=None,
        artifact_path=None,
        failing_test_ids=(),
        verify_excerpt=None,
    )
    task = store.add(
        _main_verify_remediation_prompt(
            remediation,
            head_sha="deadbeefcafe",
            attempts_spent=0,
            attempt_limit=config.watch.main_verify_remediation_max_attempts,
        ),
        task_type="implement",
        trigger_source="watch-main-integration-verify-remediation",
    )
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime(2026, 8, 22, 1, 0, tzinfo=UTC)
    task.merge_status = "unmerged"
    task.branch = "20260822-fix-ruff"
    task.has_commits = True
    store.update(task)
    store.record_main_verify_remediation_active_task(
        signature="phase:ruff",
        tree_fingerprint=None,
        task_id=task.id,
        last_observed_head_sha="deadbeefcafe",
        last_observed_failure="ruff failed",
    )

    outcome = _queue_main_verify_remediation_task(
        config=config,
        store=store,
        task=task,
        remediation=remediation,
        head_sha="feedfacecafe",
        desired_tags=("system", "system-main-verify"),
        tags=None,
        any_tag=False,
    )

    assert outcome == "queued"
    updated = store.get(task.id)
    assert updated is not None
    assert updated.status == "pending"
    assert updated.completed_at is None
    assert updated.urgent is True
    assert updated.queue_position == 1
    assert "Remediation attempts spent: 1/2" in updated.prompt
    attempt_state = store.get_main_verify_remediation_attempt_state(
        signature="phase:ruff",
        tree_fingerprint=None,
    )
    assert attempt_state is not None
    assert attempt_state.consumed_attempt_count == 1
    assert attempt_state.active_task_id is None
    assert attempt_state.last_consumed_task_id == task.id


def test_candidate_rework_reuse_requires_resolved_existing_route_before_pending_mutation(tmp_path) -> None:
    config = _setup_plan_only_model_config(tmp_path)
    store = make_store(tmp_path)
    owner = store.add("Owner implementation", task_type="implement")
    assert owner.id is not None
    owner.branch = "feature/owner"
    store.update(owner)
    remediation = MainIntegrationVerifyRemediation(
        kind="fix",
        signature="phase:functional",
        tree_fingerprint="fp-candidate",
        failing_phase="functional",
        failure="functional failed",
        observed_environment_identity=None,
        artifact_path=None,
        failing_test_ids=(),
        verify_excerpt=None,
    )
    evidence = CandidateIntegrationVerifyEvidence(
        gate_enabled=True,
        verify_command="./bin/tests",
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        environment_identity=None,
        tree_fingerprint="fp-candidate",
        head_sha="abc123",
        verify_status="failed",
        verify_exit_status="1",
        failure="functional failed",
        failing_phase="functional",
        reviewed_branch="feature/owner",
        working_directory=str(tmp_path),
        captured_at=datetime(2026, 8, 18, 1, 5, tzinfo=UTC),
    )
    check = CandidateIntegrationVerifyCheck(
        evidence=evidence,
        classification="red",
        merges_halted=True,
        remediation=remediation,
        verify_runs=1,
    )
    identity = _candidate_rework_identity(owner, check)
    existing = store.add(
        f"Existing candidate rework\n\nIdentity: {identity}\n",
        task_type="fix",
        based_on=owner.id,
        trigger_source="watch-pre-merge-integration-verify-rework",
    )
    existing.status = "failed"
    existing.failure_reason = "MAX_TURNS"
    existing.completed_at = datetime(2026, 8, 18, 1, 10, tzinfo=UTC)
    store.update(existing)
    original_existing = existing.__dict__.copy()

    with pytest.raises(ConfigError, match="'model' is required for task type 'fix'"):
        _queue_candidate_rework_task(
            config=config,
            store=store,
            owner_task=owner,
            check=check,
            tags=None,
            any_tag=False,
        )

    refreshed = store.get(existing.id)
    assert refreshed is not None
    assert refreshed.__dict__ == original_existing
    assert len(store.get_all()) == 2


def _seed_main_verify_task(
    store: SqliteTaskStore,
    *,
    verify_status: str,
    verify_exit_status: str,
    failure: str,
    alert_message: str,
    failing_phase: str = "unit",
    environment_identity: MainIntegrationVerifyEnvironmentIdentity | None = _current_host_identity(),
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
            "environment_identity": environment_identity.to_payload() if environment_identity is not None else None,
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


def test_format_main_integration_verify_attention_adds_sha_only_with_current_target_proof() -> None:
    state = SimpleNamespace(
        head_sha="abc123deadbeef",
        failing_phase="unit",
        verify_status="failed",
        verify_exit_status="1",
        alert_message="main verify RED - merges halted; phase `unit` failing",
        red_since=None,
    )

    rendered = format_main_integration_verify_attention_message(
        state,
        target_proof=MainIntegrationVerifyTargetProof("current"),
    )

    assert rendered == "main verify RED at `abc123deadbe` - merges halted; phase `unit` failing"


def test_format_main_integration_verify_attention_keeps_unproven_target_non_sha_non_halt() -> None:
    state = SimpleNamespace(
        head_sha="abc123deadbeef",
        failing_phase="unit",
        verify_status="failed",
        verify_exit_status="1",
        alert_message="main verify RED - merges halted; phase `unit` failing",
        red_since=None,
    )

    rendered = format_main_integration_verify_attention_message(
        state,
        target_proof=MainIntegrationVerifyTargetProof("unproven"),
    )

    assert rendered == "main verify red evidence unproven at current HEAD; current HEAD identity unavailable"
    assert "abc123" not in rendered
    assert "merges halted" not in rendered


def test_format_main_integration_verify_attention_keeps_stale_target_non_sha_non_halt() -> None:
    state = SimpleNamespace(
        head_sha="abc123deadbeef",
        failing_phase="unit",
        verify_status="failed",
        verify_exit_status="1",
        alert_message="main verify RED - merges halted; phase `unit` failing",
        red_since=None,
    )

    rendered = format_main_integration_verify_attention_message(
        state,
        target_proof=MainIntegrationVerifyTargetProof("stale"),
    )

    assert rendered == "main verify red evidence stale at current HEAD; recorded target SHA no longer current"
    assert "abc123" not in rendered
    assert "merges halted" not in rendered


def test_format_main_integration_verify_attention_renders_freshness_sha_only_with_current_proof() -> None:
    state = SimpleNamespace(
        head_sha="abc123deadbeef",
        failing_phase=None,
        verify_status="unavailable",
        verify_exit_status=MAIN_INTEGRATION_VERIFY_FRESHNESS_UNAVAILABLE_EXIT_STATUS,
        alert_message="main verify freshness unproven; exact tree fingerprint unavailable",
        red_since=None,
    )

    current = format_main_integration_verify_attention_message(
        state,
        target_proof=MainIntegrationVerifyTargetProof("current"),
    )
    unproven = format_main_integration_verify_attention_message(
        state,
        target_proof=MainIntegrationVerifyTargetProof("unproven"),
    )

    assert current == (
        "main verify freshness unproven at `abc123deadbe` - merges halted; "
        "exact tree fingerprint unavailable"
    )
    assert unproven == "main verify freshness unproven at current HEAD; exact tree fingerprint unavailable"
    assert "abc123" not in unproven
    assert "merges halted" not in unproven


def test_format_main_integration_verify_attention_renders_current_unknown_status_as_unknown_evidence() -> None:
    state = SimpleNamespace(
        gate_enabled=True,
        head_sha="abc123deadbeef",
        failing_phase="unit",
        verify_status="mystery",
        verify_exit_status="42",
        alert_message="main verify RED at `abc123deadbe` - merges halted; phase `unit` failing",
        red_since=datetime(2026, 6, 24, 12, 5, tzinfo=UTC),
    )

    rendered = format_main_integration_verify_attention_message(
        state,
        target_proof=MainIntegrationVerifyTargetProof("current"),
        now=datetime(2026, 6, 24, 12, 13, tzinfo=UTC),
    )

    assert rendered == "main verify evidence unknown for current HEAD; unrecognized verify status `mystery`"
    assert "abc123" not in rendered
    assert "main verify RED" not in rendered
    assert "RED" not in rendered
    assert "merges halted" not in rendered
    assert "red for" not in rendered


@pytest.mark.parametrize(
    ("state_kwargs", "expected"),
    [
        (
            {"gate_enabled": True, "verify_status": "passed", "verify_exit_status": "0"},
            "main verify passed; merges allowed",
        ),
        (
            {"gate_enabled": False, "verify_status": "unavailable", "verify_exit_status": "not configured"},
            "main verify disabled; merges allowed",
        ),
    ],
)
def test_format_main_integration_verify_attention_suppresses_legacy_red_and_exhaustion_for_non_attention_states(
    state_kwargs: dict[str, object],
    expected: str,
) -> None:
    state = SimpleNamespace(
        head_sha="abc123deadbeef",
        failing_phase="unit",
        failure_signature="phase:unit",
        alert_message=(
            "main verify RED at `abc123deadbe` - merges halted; phase `unit` failing; "
            "automatic remediation exhausted after 2/2 attempts for phase:unit on fp-verified; "
            "human intervention required"
        ),
        red_since=None,
        **state_kwargs,
    )

    rendered = format_main_integration_verify_attention_message(
        state,
        target_proof=MainIntegrationVerifyTargetProof("current"),
    )

    assert rendered == expected
    assert "abc123" not in rendered
    assert "main verify RED" not in rendered
    assert "merges halted" not in rendered
    assert "human intervention" not in rendered


@pytest.mark.parametrize(
    "target_status",
    ["current", "stale", "unproven"],
)
@pytest.mark.parametrize(
    ("verify_exit_status", "expected"),
    [
        (
            MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS,
            "main verify misconfigured - verify command launch failed; fix the environment, not the code",
        ),
        (
            MAIN_INTEGRATION_VERIFY_FRESHNESS_UNAVAILABLE_EXIT_STATUS,
            {
                "current": (
                    "main verify freshness unproven at `abc123deadbe` - merges halted; "
                    "exact tree fingerprint unavailable"
                ),
                "stale": "main verify freshness unproven at current HEAD; exact tree fingerprint unavailable",
                "unproven": "main verify freshness unproven at current HEAD; exact tree fingerprint unavailable",
            },
        ),
    ],
)
def test_format_main_integration_verify_attention_prefers_structured_special_status_over_legacy_red(
    target_status: Literal["current", "stale", "unproven"],
    verify_exit_status: str,
    expected: str | dict[str, str],
) -> None:
    state = SimpleNamespace(
        head_sha="abc123deadbeef",
        failing_phase="unit",
        verify_status="unavailable",
        verify_exit_status=verify_exit_status,
        alert_message="main verify RED at `abc123deadbe` - merges halted; phase `unit` failing",
        red_since=None,
    )

    rendered = format_main_integration_verify_attention_message(
        state,
        target_proof=MainIntegrationVerifyTargetProof(target_status),
    )

    expected_text = expected[target_status] if isinstance(expected, dict) else expected
    assert rendered == expected_text
    assert "main verify RED" not in rendered
    if verify_exit_status == MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS:
        assert "abc123" not in rendered
        assert "merges halted" not in rendered
    elif target_status != "current":
        assert "abc123" not in rendered
        assert "merges halted" not in rendered


@pytest.mark.parametrize(
    ("verify_exit_status", "expected"),
    [
        (
            MAIN_INTEGRATION_VERIFY_FRESHNESS_UNAVAILABLE_EXIT_STATUS,
            (
                "main verify freshness unproven at `abc123deadbe` - merges halted; "
                "exact tree fingerprint unavailable"
            ),
        ),
        (
            MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS,
            "main verify misconfigured - verify command launch failed; fix the environment, not the code",
        ),
    ],
)
def test_format_main_integration_verify_attention_prefers_structured_special_status_over_legacy_exhaustion(
    verify_exit_status: str,
    expected: str,
) -> None:
    state = SimpleNamespace(
        gate_enabled=True,
        head_sha="abc123deadbeef",
        failing_phase="unit",
        failure_signature="phase:unit",
        verify_status="unavailable",
        verify_exit_status=verify_exit_status,
        alert_message=(
            "main verify RED at `abc123deadbe` - merges halted; phase `unit` failing; "
            "automatic remediation exhausted after 2/2 attempts for phase:unit on fp-verified; "
            "human intervention required"
        ),
        red_since=None,
    )

    rendered = format_main_integration_verify_attention_message(
        state,
        target_proof=MainIntegrationVerifyTargetProof("current"),
    )

    assert rendered == expected
    assert "human intervention" not in rendered
    if verify_exit_status == MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS:
        assert "abc123" not in rendered
        assert "main verify RED" not in rendered
        assert "merges halted" not in rendered


@pytest.mark.parametrize("target_status", ["stale", "unproven"])
def test_format_main_integration_verify_attention_sanitizes_legacy_launch_failure_sha(
    target_status: Literal["stale", "unproven"],
) -> None:
    state = SimpleNamespace(
        head_sha="abc123deadbeef",
        failing_phase="unit",
        verify_status="unavailable",
        verify_exit_status=MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS,
        alert_message=(
            "main verify misconfigured at `abc123deadbe` - could not launch `ruff` "
            "for phase `unit` (not on PATH); fix the environment, not the code"
        ),
        red_since=None,
    )

    rendered = format_main_integration_verify_attention_message(
        state,
        target_proof=MainIntegrationVerifyTargetProof(target_status),
    )

    assert rendered == "main verify misconfigured - verify command launch failed; fix the environment, not the code"
    assert "abc123" not in rendered
    assert "merges halted" not in rendered
    assert "ruff" not in rendered


def test_format_main_integration_verify_attention_parses_canonical_launch_failure() -> None:
    state = SimpleNamespace(
        head_sha="abc123deadbeef",
        failing_phase=None,
        failure=(
            "verify_command environment error: could not launch `ruff` "
            "for phase `ruff` (not on PATH)"
        ),
        verify_status="unavailable",
        verify_exit_status=MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS,
        alert_message=(
            "main verify RED at `abc123deadbe` - merges halted; "
            "could not launch verify tooling "
            "(verify_command environment error: could not launch `ruff` for phase `ruff` (not on PATH))"
        ),
        red_since=None,
    )

    rendered = format_main_integration_verify_attention_message(
        state,
        target_proof=MainIntegrationVerifyTargetProof("current"),
    )

    assert rendered == (
        "main verify misconfigured - could not launch `ruff` "
        "for phase `ruff` (not on PATH); fix the environment, not the code"
    )
    assert "verify_command environment error" not in rendered
    assert "main verify RED" not in rendered
    assert "merges halted" not in rendered


@pytest.mark.parametrize("target_status", ["stale", "unproven"])
@pytest.mark.parametrize(
    ("state_kwargs", "expected_fragment"),
    [
        ({"alert_message": None}, "verify status unavailable"),
        (
            {
                "verify_status": 7,
                "alert_message": None,
            },
            "invalid verify status evidence",
        ),
        (
            {
                "alert_message": "legacy alert at `abc123deadbe` says merges halted",
            },
            "verify status unavailable",
        ),
        (
            {
                "verify_status": 7,
                "alert_message": "legacy alert at `abc123deadbe` says merges halted",
            },
            "invalid verify status evidence",
        ),
    ],
)
def test_format_main_integration_verify_attention_fails_closed_for_unclassified_state(
    target_status: Literal["stale", "unproven"],
    state_kwargs: dict[str, object],
    expected_fragment: str,
) -> None:
    state = SimpleNamespace(
        head_sha="abc123deadbeef",
        failing_phase=None,
        verify_exit_status="1",
        red_since=None,
        **state_kwargs,
    )

    rendered = format_main_integration_verify_attention_message(
        state,
        target_proof=MainIntegrationVerifyTargetProof(target_status),
    )

    assert f"main verify evidence {target_status} for current HEAD" in rendered
    assert expected_fragment in rendered
    assert "abc123" not in rendered
    assert "merges halted" not in rendered
    assert "legacy alert" not in rendered


@pytest.mark.parametrize("target_status", ["current", "stale", "unproven"])
@pytest.mark.parametrize("verify_status", [7, ""])
@pytest.mark.parametrize(
    "alert_message",
    [
        "main verify RED at `abc123deadbe` - merges halted; phase `unit` failing",
        (
            "main verify RED at `abc123deadbe` - merges halted; phase `unit` failing; "
            "automatic remediation exhausted after 2/2 attempts for phase:unit on fp-verified; "
            "human intervention required"
        ),
    ],
)
def test_format_main_integration_verify_attention_rejects_malformed_status_legacy_fallback(
    target_status: Literal["current", "stale", "unproven"],
    verify_status: object,
    alert_message: str,
) -> None:
    state = SimpleNamespace(
        gate_enabled=True,
        head_sha="abc123deadbeef",
        failing_phase="unit",
        failure_signature="phase:unit",
        verify_status=verify_status,
        verify_exit_status="1",
        alert_message=alert_message,
        red_since=None,
    )

    rendered = format_main_integration_verify_attention_message(
        state,
        target_proof=MainIntegrationVerifyTargetProof(target_status),
    )

    assert main_integration_verify_state_halts_merges(state) is True
    assert main_integration_verify_state_is_red_verdict(state) is False
    assert main_integration_verify_state_has_exhausted_remediation_attention(state) is False
    assert "invalid verify status evidence" in rendered
    assert "abc123" not in rendered
    assert "main verify RED" not in rendered
    assert "RED" not in rendered
    assert "merges halted" not in rendered
    assert "human intervention required" not in rendered


def test_format_main_integration_verify_attention_keeps_configured_missing_evidence_visible() -> None:
    state = SimpleNamespace(
        gate_enabled=True,
        head_sha="abc123deadbeef",
        failing_phase=None,
        verify_status=None,
        verify_exit_status="1",
        alert_message=None,
        red_since=None,
    )

    rendered = format_main_integration_verify_attention_message(
        state,
        target_proof=MainIntegrationVerifyTargetProof("current"),
    )

    assert main_integration_verify_state_halts_merges(state) is True
    assert main_integration_verify_state_is_red_verdict(state) is False
    assert main_integration_verify_state_has_exhausted_remediation_attention(state) is False
    assert rendered == "main verify evidence unknown for current HEAD; verify status unavailable"
    assert "abc123" not in rendered
    assert "RED" not in rendered
    assert "merges halted" not in rendered
    assert "human intervention required" not in rendered


def test_format_main_integration_verify_attention_keeps_status_absent_legacy_red_compatible() -> None:
    state = SimpleNamespace(
        gate_enabled=True,
        head_sha="abc123deadbeef",
        failing_phase="unit",
        verify_exit_status="1",
        alert_message="main verify RED at `abc123deadbe` - merges halted; phase `unit` failing",
        red_since=None,
    )

    rendered = format_main_integration_verify_attention_message(
        state,
        target_proof=MainIntegrationVerifyTargetProof("current"),
    )

    assert main_integration_verify_state_halts_merges(state) is True
    assert main_integration_verify_state_is_red_verdict(state) is True
    assert rendered == "main verify RED at `abc123deadbe` - merges halted; phase `unit` failing"


def test_format_main_integration_verify_attention_keeps_status_absent_legacy_exhaustion_compatible() -> None:
    state = SimpleNamespace(
        gate_enabled=True,
        head_sha="abc123deadbeef",
        failing_phase="unit",
        failure_signature="phase:unit",
        verify_exit_status="1",
        alert_message=(
            "main verify RED at `abc123deadbe` - merges halted; phase `unit` failing; "
            "automatic remediation exhausted after 2/2 attempts for phase:unit on fp-verified; "
            "human intervention required"
        ),
        red_since=None,
    )

    rendered = format_main_integration_verify_attention_message(
        state,
        target_proof=MainIntegrationVerifyTargetProof("current"),
    )

    assert main_integration_verify_state_halts_merges(state) is True
    assert main_integration_verify_state_has_exhausted_remediation_attention(state) is True
    assert rendered == (
        "main verify remediation exhausted for phase:unit after 2/2 attempts; "
        "human intervention required"
    )


def test_resolve_main_integration_verify_target_proof_returns_unproven_for_git_errors() -> None:
    state = SimpleNamespace(head_sha="abc123")
    default_branch_git = MagicMock()
    default_branch_git.default_branch.side_effect = GitError("default branch lookup failed")
    rev_parse_git = MagicMock()
    rev_parse_git.default_branch.return_value = "main"
    rev_parse_git.rev_parse_if_exists.side_effect = GitError("ref lookup failed")

    assert resolve_main_integration_verify_target_proof(state, default_branch_git).status == "unproven"
    assert resolve_main_integration_verify_target_proof(state, rev_parse_git).status == "unproven"


def test_resolve_main_integration_verify_target_proof_propagates_assertion_from_default_branch() -> None:
    state = SimpleNamespace(head_sha="abc123")
    git = MagicMock()
    git.default_branch.side_effect = AssertionError("programming contract failed")

    with pytest.raises(AssertionError, match="programming contract failed"):
        resolve_main_integration_verify_target_proof(state, git)


def test_resolve_main_integration_verify_target_proof_propagates_assertion_from_rev_parse() -> None:
    state = SimpleNamespace(head_sha="abc123")
    git = MagicMock()
    git.default_branch.return_value = "main"
    git.rev_parse_if_exists.side_effect = AssertionError("programming contract failed")

    with pytest.raises(AssertionError, match="programming contract failed"):
        resolve_main_integration_verify_target_proof(state, git)


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


def test_build_main_integration_verify_remediation_skips_whitespace_only_preferred_artifact_for_newer_readable_evidence(
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
        output="  \n\t  \n",
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


def test_build_main_integration_verify_remediation_omits_whitespace_only_artifact_evidence_and_prompt_line(
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
        output="  \n",
        created_at=datetime(2026, 6, 23, 0, 0, tzinfo=UTC),
    )
    store_command_output_artifact(
        store,
        task,
        config,
        kind="verify_command_output",
        producer="main_verify_test",
        label="verify newer",
        output="",
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


def test_build_main_integration_verify_remediation_carries_observed_environment_identity(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    identity = MainIntegrationVerifyEnvironmentIdentity(
        runner_class="host",
        platform_system="Darwin",
        platform_machine="arm64",
        python_implementation="CPython",
        python_version="3.12",
    )
    task_id = _seed_main_verify_task(
        store,
        verify_status="failed",
        verify_exit_status="1",
        failure="verify_command failed",
        alert_message="main verify RED at `abc123` - merges halted; phase `unit` failing",
        environment_identity=identity,
    )
    task = store.get(task_id)
    assert task is not None
    config = Config.load(tmp_path)
    state = load_main_integration_verify_state(store)
    assert state is not None

    remediation = _build_main_integration_verify_remediation(
        kind="fix",
        config=config,
        store=store,
        state=state,
    )

    assert remediation.observed_environment_identity == identity


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


def test_load_main_integration_verify_state_accepts_legacy_python_executable_payload(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    python_executable_family = f"python{python_version}"
    task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.review_verify_command = "./bin/tests"
    task.review_verify_status = "passed"
    task.review_verify_exit_status = "0"
    task.review_verify_head_sha = "abc123"
    task.output_content = json.dumps(
        {
            "captured_at": "2026-06-23T00:00:00+00:00",
            "environment_identity": {
                "runner_class": "host",
                "platform_system": platform.system(),
                "platform_machine": platform.machine(),
                "python_executable": f"/tmp/worktree/.venv/bin/{python_executable_family}",
                "python_version": python_version,
            },
            "gate_enabled": True,
            "head_sha": "abc123",
            "tree_fingerprint": "fp-verified",
            "verify_command": "./bin/tests",
            "verify_timeout_grace_seconds": 5.0,
            "verify_timeout_seconds": 120,
        },
        sort_keys=True,
    )
    store.update(task)

    state = load_main_integration_verify_state(store)

    assert state is not None
    assert state.environment_identity == MainIntegrationVerifyEnvironmentIdentity(
        runner_class="host",
        platform_system=platform.system(),
        platform_machine=platform.machine(),
        python_implementation=None,
        python_version=python_version,
        python_executable_family=python_executable_family,
    )


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


def test_check_main_integration_verify_emits_initial_start_before_stale_verify_body(tmp_path) -> None:
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

    events: list[tuple[str, int | None, int | None]] = []
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

    def run_verify_body(*_args, **_kwargs):
        events.append(("body", None, None))
        return verify_result

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
        patch("gza.main_integration_verify._compute_tree_fingerprint", side_effect=["fp-live", "fp-live"]),
        patch("gza.main_integration_verify._run_review_verify_command", side_effect=run_verify_body),
        patch("gza.main_integration_verify._capture_review_verify_result", side_effect=capture_verify_result),
    ):
        check = check_main_integration_verify(
            config,
            store,
            git,
            reason="watch-main-verify",
            red_reruns=2,
            on_initial_run_start=lambda attempt, total: events.append(("start", attempt, total)),
        )

    assert check.performed_verify is True
    assert events == [("start", 1, 3), ("body", None, None)]


def test_check_main_integration_verify_does_not_emit_initial_start_for_disabled_gate(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    git = MagicMock()
    git.repo_dir = tmp_path
    git.current_branch.return_value = "main"
    git.rev_parse_if_exists.return_value = "abc123"

    starts: list[tuple[int, int]] = []
    with (
        patch("gza.main_integration_verify._compute_tree_fingerprint", return_value="fp-live"),
        patch("gza.main_integration_verify._run_review_verify_command") as run_verify,
    ):
        check = check_main_integration_verify(
            config,
            store,
            git,
            reason="watch-main-verify",
            red_reruns=2,
            on_initial_run_start=lambda attempt, total: starts.append((attempt, total)),
        )

    run_verify.assert_not_called()
    assert check.performed_verify is True
    assert check.verify_runs == 0
    assert check.state.gate_enabled is False
    assert check.state.verify_status == "unavailable"
    assert starts == []


def test_check_main_integration_verify_does_not_emit_initial_start_for_cached_checkpoint(tmp_path) -> None:
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

    starts: list[tuple[int, int]] = []
    with (
        patch("gza.main_integration_verify._compute_tree_fingerprint", return_value="fp-verified"),
        patch("gza.main_integration_verify._run_review_verify_command") as run_verify,
    ):
        check = check_main_integration_verify(
            config,
            store,
            git,
            reason="watch-main-verify",
            red_reruns=2,
            on_initial_run_start=lambda attempt, total: starts.append((attempt, total)),
        )

    run_verify.assert_not_called()
    assert check.performed_verify is False
    assert starts == []


def test_compute_tree_fingerprint_explicit_missing_head_is_not_reusable_for_clean_target(tmp_path) -> None:
    git = MagicMock()
    git.repo_dir = tmp_path
    git._run.return_value = SimpleNamespace(returncode=0, stdout="", stderr="")

    assert _compute_tree_fingerprint(git, head_sha=None) is None
    git._run.assert_not_called()

    concrete = _compute_tree_fingerprint(git, head_sha="abc123")

    assert isinstance(concrete, str)
    assert len(concrete) == 64


def test_check_main_integration_verify_unknown_head_does_not_cache_green_across_clean_targets(tmp_path) -> None:
    setup_config(tmp_path)
    (tmp_path / "gza.yaml").write_text((tmp_path / "gza.yaml").read_text() + "verify_command: ./bin/tests\n")
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    git = MagicMock()
    git.repo_dir = tmp_path
    git.current_branch.return_value = "main"
    git.rev_parse_if_exists.return_value = None
    git._run.return_value = SimpleNamespace(returncode=0, stdout="", stderr="")

    first_result = _make_review_verify_result(
        "./bin/tests",
        status="passed",
        exit_status="0",
        captured_at=datetime(2026, 6, 23, 12, 0, tzinfo=UTC),
        reviewed_branch="main",
        reviewed_head_sha=None,
        working_directory=str(tmp_path),
        output="first clean target passed",
    )
    second_result = _make_review_verify_result(
        "./bin/tests",
        status="passed",
        exit_status="0",
        captured_at=datetime(2026, 6, 23, 12, 5, tzinfo=UTC),
        reviewed_branch="main",
        reviewed_head_sha=None,
        working_directory=str(tmp_path),
        output="second clean target passed",
    )

    with patch(
        "gza.main_integration_verify._run_review_verify_command",
        side_effect=[first_result, second_result],
    ) as run_verify:
        first = check_main_integration_verify(
            config,
            store,
            git,
            reason="unknown-head-first-clean-target",
            resolved_head_sha=None,
        )
        second = check_main_integration_verify(
            config,
            store,
            git,
            reason="unknown-head-second-clean-target",
            resolved_head_sha=None,
        )

    assert run_verify.call_count == 2
    assert first.performed_verify is True
    assert first.current_tree_fingerprint is None
    assert first.state.tree_fingerprint is None
    assert first.state.verify_status == "unavailable"
    assert first.state.verify_exit_status == MAIN_INTEGRATION_VERIFY_FRESHNESS_UNAVAILABLE_EXIT_STATUS
    assert first.merges_halted is True
    assert second.performed_verify is True
    assert second.current_tree_fingerprint is None
    assert second.state.tree_fingerprint is None
    assert second.state.verify_status == "unavailable"
    assert second.state.verify_exit_status == MAIN_INTEGRATION_VERIFY_FRESHNESS_UNAVAILABLE_EXIT_STATUS
    assert second.merges_halted is True


def test_check_main_integration_verify_structured_fingerprint_establishes_freshness_when_head_unknown(
    tmp_path,
) -> None:
    setup_config(tmp_path)
    (tmp_path / "gza.yaml").write_text((tmp_path / "gza.yaml").read_text() + "verify_command: ./bin/tests\n")
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    git = MagicMock()
    git.repo_dir = tmp_path
    git.current_branch.return_value = "main"
    git.rev_parse_if_exists.return_value = None
    git._run.return_value = SimpleNamespace(returncode=0, stdout="", stderr="")

    fingerprint = "a" * 64
    verify_result = _make_review_verify_result(
        "./bin/tests",
        status="passed",
        exit_status="0",
        captured_at=datetime(2026, 6, 23, 12, 0, tzinfo=UTC),
        reviewed_branch="main",
        reviewed_head_sha=None,
        working_directory=str(tmp_path),
        output=f"gza-verify phase=passed name=unit duration_seconds=1.0 tree_fingerprint={fingerprint}",
    )

    with patch("gza.main_integration_verify._run_review_verify_command", return_value=verify_result) as run_verify:
        check = check_main_integration_verify(
            config,
            store,
            git,
            reason="unknown-head-structured-fingerprint",
            resolved_head_sha=None,
        )

    run_verify.assert_called_once()
    assert check.performed_verify is True
    assert check.current_tree_fingerprint is None
    assert check.state.tree_fingerprint == fingerprint
    assert check.state.verify_status == "passed"
    assert check.merges_halted is False


def test_check_main_integration_verify_reuses_checkpoint_when_only_python_path_differs(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    legacy_same_runtime_identity = MainIntegrationVerifyEnvironmentIdentity(
        runner_class="host",
        platform_system=platform.system(),
        platform_machine=platform.machine(),
        python_implementation=None,
        python_version=python_version,
        python_executable_family=f"python{python_version}",
    )
    _seed_main_verify_task(
        store,
        verify_status="passed",
        verify_exit_status="0",
        failure="",
        alert_message="",
        environment_identity=legacy_same_runtime_identity,
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
            reason="unit-test-same-runtime-different-python-path",
        )

    run_verify.assert_not_called()
    assert check.performed_verify is False
    assert check.is_current is True
    assert check.state.environment_identity == legacy_same_runtime_identity


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
    payload = json.loads(persisted.task.output_content or "{}")
    assert payload["environment_identity"] == {
        "runner_class": "container",
        "platform_system": platform.system(),
        "platform_machine": platform.machine(),
        "python_implementation": platform.python_implementation(),
        "python_version": f"{sys.version_info.major}.{sys.version_info.minor}",
    }
    assert "python_executable" not in payload["environment_identity"]


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


def test_check_main_integration_verify_classifies_tool_launch_failure_as_attention_without_red_or_remediation(
    tmp_path,
) -> None:
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

    launch_failure = _make_review_verify_result(
        "./bin/tests",
        status="failed",
        exit_status="127",
        captured_at=datetime(2026, 6, 29, 12, 0, tzinfo=UTC),
        reviewed_branch="main",
        reviewed_head_sha="abc123",
        working_directory=str(tmp_path),
        output=(
            "gza-verify phase=start name=ruff\n"
            "verify_phase: failed to launch command ['ruff', 'check', 'src/gza/']: "
            "[Errno 2] No such file or directory: 'ruff'\n"
            "gza-verify phase=failed name=ruff duration_seconds=0.25"
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
        patch("gza.main_integration_verify._compute_tree_fingerprint", return_value="fp-live"),
        patch("gza.main_integration_verify._run_review_verify_command", return_value=launch_failure) as run_verify,
        patch("gza.main_integration_verify._capture_review_verify_result", side_effect=capture_verify_result),
    ):
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
    assert check.needs_attention is True
    assert check.remediation is None
    assert check.state.verify_status == "unavailable"
    assert check.state.verify_exit_status == MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS
    assert check.state.failing_phase == "ruff"
    assert check.state.alert_message is not None
    assert "could not launch `ruff`" in check.state.alert_message
    assert "fix the environment, not the code" in check.state.alert_message
    assert "abc123" not in check.state.alert_message

    alert_git = MagicMock()
    alert_git.default_branch.return_value = "main"
    alert_git.current_branch.return_value = "topic"
    alert_git.rev_parse_if_exists.side_effect = lambda ref: "abc123" if ref == "refs/heads/main" else "topic-sha"
    alert = current_main_integration_verify_alert(store, alert_git, config)
    assert alert is not None
    assert alert.verify_exit_status == MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS
    assert main_integration_verify_state_halts_merges(alert) is False
    rendered = format_main_integration_verify_attention_message(
        alert,
        target_proof=MainIntegrationVerifyTargetProof("current"),
    )
    assert rendered == (
        "main verify misconfigured - could not launch `ruff` "
        "for phase `ruff` (not on PATH); fix the environment, not the code"
    )
    assert "verify_command environment error" not in rendered


def test_check_main_integration_verify_green_does_not_resolve_launch_failure_signature(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _seed_main_verify_task(
        store,
        verify_status="unavailable",
        verify_exit_status=MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS,
        failure="verify tool launch failed",
        alert_message="main verify misconfigured - could not launch `ruff` (missing); fix the environment, not the code",
        failing_phase="ruff",
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
    ):
        check = check_main_integration_verify(
            config,
            store,
            git,
            reason="watch-main-verify-after-launch-failure",
        )

    run_verify.assert_called_once()
    assert check.performed_verify is True
    assert check.merges_halted is False
    assert check.remediation is None
    assert check.resolved_red_signature is None
    assert check.state.verify_status == "passed"
    assert check.state.failure_signature is None


def test_check_main_integration_verify_extracts_tool_name_from_top_level_launch_failed_oserror(
    tmp_path,
) -> None:
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

    launch_failure = _make_review_verify_result(
        "./bin/tests",
        status="unavailable",
        exit_status=MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS,
        captured_at=datetime(2026, 7, 2, 12, 0, tzinfo=UTC),
        reviewed_branch="main",
        reviewed_head_sha="abc123",
        working_directory=str(tmp_path),
        failure="failed to launch verify_command: [Errno 2] No such file or directory: 'ruff'",
        output="failed to launch verify_command: [Errno 2] No such file or directory: 'ruff'",
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
        patch("gza.main_integration_verify._compute_tree_fingerprint", return_value="fp-live"),
        patch("gza.main_integration_verify._run_review_verify_command", return_value=launch_failure) as run_verify,
        patch("gza.main_integration_verify._capture_review_verify_result", side_effect=capture_verify_result),
    ):
        check = check_main_integration_verify(
            config,
            store,
            git,
            reason="watch-main-verify",
            red_reruns=2,
        )

    run_verify.assert_called_once()
    assert check.merges_halted is False
    assert check.needs_attention is True
    assert check.remediation is None
    assert check.state.verify_status == "unavailable"
    assert check.state.verify_exit_status == MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS
    assert check.state.alert_message is not None
    assert "could not launch `ruff`" in check.state.alert_message
    assert "fix the environment, not the code" in check.state.alert_message
    assert "abc123" not in check.state.alert_message


def test_check_main_integration_verify_classifies_shell_not_found_phase_failure_as_attention(
    tmp_path,
) -> None:
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

    launch_failure = _make_review_verify_result(
        "./bin/tests",
        status="failed",
        exit_status="127",
        captured_at=datetime(2026, 7, 2, 12, 5, tzinfo=UTC),
        reviewed_branch="main",
        reviewed_head_sha="abc123",
        working_directory=str(tmp_path),
        output=(
            "gza-verify phase=start name=ruff\n"
            "sh: 1: ruff: not found\n"
            "gza-verify phase=failed name=ruff duration_seconds=0.25"
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
        patch("gza.main_integration_verify._compute_tree_fingerprint", return_value="fp-live"),
        patch("gza.main_integration_verify._run_review_verify_command", return_value=launch_failure) as run_verify,
        patch("gza.main_integration_verify._capture_review_verify_result", side_effect=capture_verify_result),
    ):
        check = check_main_integration_verify(
            config,
            store,
            git,
            reason="watch-main-verify",
            red_reruns=2,
        )

    run_verify.assert_called_once()
    assert check.merges_halted is False
    assert check.needs_attention is True
    assert check.remediation is None
    assert check.state.verify_status == "unavailable"
    assert check.state.verify_exit_status == MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS
    assert check.state.failing_phase == "ruff"
    assert check.state.alert_message is not None
    assert "could not launch `ruff`" in check.state.alert_message
    assert "fix the environment, not the code" in check.state.alert_message
    assert "abc123" not in check.state.alert_message


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
    assert check.state.alert_message == "main verify freshness unproven; exact tree fingerprint unavailable"
    assert "abc123" not in check.state.alert_message


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
    assert alert.alert_message == "main verify freshness unproven; exact tree fingerprint unavailable"
    assert "abc123" not in alert.alert_message


def test_current_main_integration_verify_alert_ignores_ambiguous_short_target_ref(tmp_path) -> None:
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
    git.current_branch.return_value = "topic"
    git.rev_parse_if_exists.side_effect = lambda ref: "abc123" if ref == "main" else None

    alert = current_main_integration_verify_alert(store, git, config)

    assert alert is None
    assert git.rev_parse_if_exists.call_args_list == [call("refs/heads/main")]


def test_current_main_integration_verify_alert_omits_red_checkpoint_missing_environment_identity(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _seed_main_verify_task(
        store,
        verify_status="failed",
        verify_exit_status="1",
        failure="verify_command failed",
        alert_message="main verify RED at `abc123` - merges halted; phase `unit` failing",
        environment_identity=None,
    )

    config = MagicMock(spec=Config)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0
    config.main_integration_verify_red_ttl_minutes = 30

    git = MagicMock()
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "topic"
    git.rev_parse_if_exists.side_effect = lambda ref: "abc123" if ref == "refs/heads/main" else "topic-sha"

    alert = current_main_integration_verify_alert(store, git, config)

    assert alert is None


def test_current_main_integration_verify_alert_omits_red_checkpoint_with_mismatched_environment_identity(
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
        environment_identity=_linux_container_identity(),
    )

    config = MagicMock(spec=Config)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0
    config.main_integration_verify_red_ttl_minutes = 30

    git = MagicMock()
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "topic"
    git.rev_parse_if_exists.side_effect = lambda ref: "abc123" if ref == "refs/heads/main" else "topic-sha"

    alert = current_main_integration_verify_alert(store, git, config)

    assert alert is None


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


def test_check_main_integration_verify_preserves_pending_retirements_across_fresh_green_rerun(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _seed_main_verify_task(
        store,
        verify_status="passed",
        verify_exit_status="0",
        failure="",
        alert_message="",
        pending_retirement_signatures=("phase:functional",),
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

    with (
        patch(
            "gza.main_integration_verify._compute_tree_fingerprint",
            side_effect=["fp-refreshed", "fp-refreshed"],
        ),
        patch("gza.main_integration_verify._run_review_verify_command", return_value=green_result) as run_verify,
        patch("gza.main_integration_verify._capture_review_verify_result", side_effect=capture_verify_result),
    ):
        check = check_main_integration_verify(
            config,
            store,
            git,
            reason="unit-test-green-refresh",
        )

    run_verify.assert_called_once()
    assert check.performed_verify is True
    assert check.merges_halted is False
    assert check.resolved_red_signature is None
    assert check.state.verify_status == "passed"
    assert check.state.pending_retirement_signatures == ("phase:functional",)
    reloaded = load_main_integration_verify_state(store)
    assert reloaded is not None
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
    assert check.resolved_signature == "phase:unit"


def test_check_main_integration_verify_force_green_carries_resolved_signature_without_remediation(
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
            reason="operator-main-verify",
            force=True,
            red_reruns=0,
        )

    run_verify.assert_called_once()
    assert check.performed_verify is True
    assert check.merges_halted is False
    assert check.remediation is None
    assert check.resolved_signature == "phase:unit"


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


def test_check_main_integration_verify_watch_red_rerun_preserves_red_artifact_evidence_for_deflake(
    tmp_path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0
    config.main_integration_verify_red_ttl_minutes = 30

    git = MagicMock()
    git.repo_dir = tmp_path
    git.current_branch.return_value = "main"
    git.rev_parse_if_exists.return_value = "abc123"
    tree_fingerprint = "f" * 64

    red_output = "\n".join(
        [
            "gza-verify phase=start name=unit",
            "WORKER_DIED subprocess boundary failure",
            "=========================== short test summary info ============================",
            "FAILED tests/test_red.py::test_first - AssertionError: red one",
            "FAILED tests/test_red.py::test_second - RuntimeError: red two",
            "============================== 2 failed in 0.20s ==============================",
            f"gza-verify phase=failed name=unit duration_seconds=3.25 tree_fingerprint={tree_fingerprint}",
        ]
    )
    green_output = "\n".join(
        [
            "gza-verify phase=start name=unit",
            "GREEN RERUN MARKER",
            "============================== 120 passed in 0.40s ==============================",
            f"gza-verify phase=passed name=unit duration_seconds=0.40 tree_fingerprint={tree_fingerprint}",
        ]
    )
    red_result = _make_review_verify_result(
        "./bin/tests",
        status="failed",
        exit_status="1",
        captured_at=datetime(2026, 6, 23, 0, 0, tzinfo=UTC),
        reviewed_branch="main",
        reviewed_head_sha="abc123",
        working_directory=str(tmp_path),
        failure="verify_command failed",
        output=red_output,
    )
    green_result = _make_review_verify_result(
        "./bin/tests",
        status="passed",
        exit_status="0",
        captured_at=datetime(2026, 6, 23, 0, 1, tzinfo=UTC),
        reviewed_branch="main",
        reviewed_head_sha="abc123",
        working_directory=str(tmp_path),
        output=green_output,
    )

    with (
        patch("gza.main_integration_verify._compute_tree_fingerprint", return_value=tree_fingerprint),
        patch("gza.main_integration_verify._run_review_verify_command", side_effect=[red_result, green_result]) as run_verify,
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
    assert check.merges_halted is False
    assert check.state.verify_status == "passed"
    assert check.remediation is not None
    assert check.remediation.kind == "deflake"
    assert check.remediation.signature == "phase:unit"
    assert check.remediation.tree_fingerprint == tree_fingerprint
    assert check.remediation.failing_phase == "unit"
    assert check.remediation.failure == "verify_command failed"
    assert check.remediation.failing_test_ids == (
        "tests/test_red.py::test_first",
        "tests/test_red.py::test_second",
    )
    assert check.remediation.verify_excerpt is not None
    assert "WORKER_DIED subprocess boundary failure" in check.remediation.verify_excerpt
    assert "FAILED tests/test_red.py::test_first - AssertionError: red one" in check.remediation.verify_excerpt
    assert "GREEN RERUN MARKER" not in check.remediation.verify_excerpt

    artifacts = store.list_artifacts(check.state.task.id, kind="verify_command_output")
    artifact_paths = [artifact.path for artifact in artifacts if artifact.path]
    assert len(artifact_paths) == 2

    red_artifact_path = next(
        path for path in artifact_paths if "GREEN RERUN MARKER" not in (tmp_path / path).read_text()
    )
    green_artifact_path = next(
        path for path in artifact_paths if "GREEN RERUN MARKER" in (tmp_path / path).read_text()
    )
    assert check.remediation.artifact_path == red_artifact_path
    assert check.remediation.artifact_path != green_artifact_path
    assert check.state.task.review_verify_artifact_file == green_artifact_path


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


def test_run_main_integration_verify_does_not_persist_failure_signature_for_launch_failure(tmp_path) -> None:
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

    launch_failure = _make_review_verify_result(
        "./bin/tests",
        status="unavailable",
        exit_status=MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS,
        captured_at=datetime(2026, 6, 23, 0, 5, tzinfo=UTC),
        reviewed_branch="main",
        reviewed_head_sha="abc123",
        working_directory=str(tmp_path),
        failure="verify tool launch failed",
        output="gza-verify phase=error name=ruff detail=missing executable",
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
        patch("gza.main_integration_verify._run_review_verify_command", return_value=launch_failure),
        patch("gza.main_integration_verify._capture_review_verify_result", side_effect=capture_verify_result),
    ):
        state = run_main_integration_verify(config, store, git, reason="unit-test-launch-failure")

    assert state.verify_status == "unavailable"
    assert state.verify_exit_status == MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS
    assert state.failure_signature is None
    persisted = load_main_integration_verify_state(store)
    assert persisted is not None
    assert persisted.failure_signature is None
