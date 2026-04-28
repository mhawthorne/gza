from datetime import UTC, datetime
from pathlib import Path

import pytest

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
    assert decision.action == "retry"
    assert decision.recovery_task_id == child.id
    assert decision.reuse_existing is True


def test_recovery_engine_existing_pending_resume_child_reuses_resume_semantics(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, reason="MAX_TURNS", session_id="sess-1")
    child = store.add("Resume child", task_type=task.task_type, based_on=task.id)
    assert child.id is not None
    child.status = "pending"
    child.session_id = task.session_id
    store.update(child)

    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)
    assert decision.action == "resume"
    assert decision.recovery_task_id == child.id
    assert decision.reuse_existing is True


@pytest.mark.parametrize(
    ("descendant_status", "expected_reason"),
    [
        ("pending", "recovery_already_pending"),
        ("in_progress", "recovery_already_running"),
        ("completed", "recovery_already_completed"),
    ],
)
def test_recovery_engine_skips_failed_ancestor_when_deeper_descendant_supersedes_chain(
    tmp_path: Path,
    descendant_status: str,
    expected_reason: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    root = store.add("Failed root", task_type="implement")
    assert root.id is not None
    root.status = "failed"
    root.failure_reason = "MAX_TURNS"
    root.session_id = "sess-root"
    root.completed_at = datetime.now(UTC)
    store.update(root)

    retry_child = store.add("Failed retry child", task_type="implement", based_on=root.id)
    assert retry_child.id is not None
    retry_child.status = "failed"
    retry_child.failure_reason = "MAX_TURNS"
    retry_child.session_id = root.session_id
    retry_child.completed_at = datetime.now(UTC)
    store.update(retry_child)

    grandchild = store.add("Recovery grandchild", task_type="implement", based_on=retry_child.id)
    assert grandchild.id is not None
    grandchild.status = descendant_status
    grandchild.session_id = root.session_id
    if descendant_status == "completed":
        grandchild.completed_at = datetime.now(UTC)
    store.update(grandchild)

    decision = decide_failed_task_recovery(store, root, max_recovery_attempts=3)
    assert decision.action == "skip"
    assert decision.reason_code == expected_reason
    assert decision.recovery_task_id is None
    assert decision.reuse_existing is False


def test_recovery_engine_only_terminal_failed_node_remains_actionable(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    root = store.add("Failed root", task_type="plan")
    assert root.id is not None
    root.status = "failed"
    root.failure_reason = "INFRASTRUCTURE_ERROR"
    root.completed_at = datetime.now(UTC)
    store.update(root)

    retry_child = store.add("Failed retry child", task_type="plan", based_on=root.id)
    assert retry_child.id is not None
    retry_child.status = "failed"
    retry_child.failure_reason = "INFRASTRUCTURE_ERROR"
    retry_child.completed_at = datetime.now(UTC)
    store.update(retry_child)

    root_decision = decide_failed_task_recovery(store, root, max_recovery_attempts=3)
    assert root_decision.action == "skip"
    assert root_decision.reason_code == "recovery_has_newer_failed_descendant"

    child_decision = decide_failed_task_recovery(store, retry_child, max_recovery_attempts=3)
    assert child_decision.action == "retry"


def test_recovery_engine_blocked_failed_task_with_pending_child_skips_until_dependency_ready(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    dependency = store.add("Dependency", task_type="plan")
    failed = store.add("Blocked failed task", task_type="implement", depends_on=dependency.id)
    assert dependency.id is not None
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-1"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    child = store.add("Pending recovery child", task_type=failed.task_type, based_on=failed.id, depends_on=dependency.id)
    assert child.id is not None
    child.status = "pending"
    child.session_id = failed.session_id
    store.update(child)

    blocked_decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert blocked_decision.action == "skip"
    assert blocked_decision.reason_code == "dependency_not_ready"
    assert blocked_decision.recovery_task_id is None
    assert blocked_decision.reuse_existing is False

    dependency.status = "completed"
    dependency.completed_at = datetime.now(UTC)
    store.update(dependency)

    ready_decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert ready_decision.action == "resume"
    assert ready_decision.recovery_task_id == child.id
    assert ready_decision.reuse_existing is True


def test_recovery_engine_manual_reason_with_pending_child_still_skips(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, task_type="plan", reason="TEST_FAILURE", session_id=None)
    child = store.add("Pending retry child", task_type=task.task_type, based_on=task.id)
    assert child.id is not None
    child.status = "pending"
    store.update(child)

    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "manual_failure_reason"
    assert decision.recovery_task_id is None
    assert decision.reuse_existing is False


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
