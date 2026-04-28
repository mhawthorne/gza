from datetime import UTC, datetime
from pathlib import Path

from gza.recovery_engine import decide_failed_task_recovery

from tests.cli.conftest import make_store, setup_config


def _failed_task(tmp_path: Path, *, task_type: str = "implement", reason: str = "MAX_TURNS", session_id: str | None = "sess-1"):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Failed task", task_type=task_type)
    assert task.id is not None
    task.status = "failed"
    task.failure_reason = reason
    task.session_id = session_id
    task.completed_at = datetime.now(UTC)
    store.update(task)
    return store, task


def test_recovery_engine_resumable_with_session_chooses_resume(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, reason="MAX_TURNS", session_id="sess-1")
    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)
    assert decision.action == "resume"
    assert decision.launch_mode == "iterate"


def test_recovery_engine_infra_failure_chooses_retry(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, task_type="plan", reason="INFRASTRUCTURE_ERROR", session_id=None)
    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)
    assert decision.action == "retry"
    assert decision.launch_mode == "worker"


def test_recovery_engine_resumable_without_session_chooses_retry(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, reason="MAX_STEPS", session_id=None)
    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)
    assert decision.action == "retry"


def test_recovery_engine_manual_reason_skips(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, reason="TEST_FAILURE")
    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "manual_failure_reason"


def test_recovery_engine_scope_exclusions_skip(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, task_type="review", reason="MAX_TURNS")
    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "task_type_out_of_scope"


def test_recovery_engine_existing_children_skip(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, reason="INFRASTRUCTURE_ERROR", session_id=None)
    child = store.add("Retry child", task_type=task.task_type, based_on=task.id)
    assert child.id is not None
    child.status = "pending"
    store.update(child)

    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "recovery_already_pending"


def test_recovery_engine_attempt_cap_reached_skips(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, reason="INFRASTRUCTURE_ERROR", session_id=None)
    attempt = store.add("Attempt", task_type=task.task_type, based_on=task.id)
    assert attempt.id is not None
    attempt.status = "failed"
    attempt.failure_reason = "INFRASTRUCTURE_ERROR"
    attempt.completed_at = datetime.now(UTC)
    store.update(attempt)

    decision = decide_failed_task_recovery(store, attempt, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "attempt_cap_reached"
