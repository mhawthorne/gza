from datetime import UTC, datetime
from pathlib import Path

import pytest

from gza.recovery_engine import (
    decide_failed_task_recovery,
    get_completed_recovery_descendant,
    get_failed_recovery_needs_attention_reason,
    get_recovery_chain_root_task_id,
    get_recovery_chain_state,
    is_chain_resolved_by_recovery,
    is_resolved_by_merged_target,
    list_failed_tasks_for_recovery,
)
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


def _completed_impl(store, *, merge_status: str):
    task = store.add("Implementation", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.has_commits = True
    task.merge_status = merge_status
    task.completed_at = datetime.now(UTC)
    store.update(task)
    return task


def _failed_sidequest(store, *, task_type: str, impl_id: str, reason: str):
    depends_on = None
    based_on = None
    if task_type == "review":
        depends_on = impl_id
        based_on = impl_id
    elif task_type == "improve":
        review = store.add("Review", task_type="review", depends_on=impl_id, based_on=impl_id)
        assert review.id is not None
        depends_on = review.id
        based_on = impl_id
    elif task_type == "rebase":
        based_on = impl_id
    else:
        raise AssertionError(f"unsupported task_type: {task_type}")

    task = store.add(
        f"Failed {task_type}",
        task_type=task_type,
        depends_on=depends_on,
        based_on=based_on,
    )
    assert task.id is not None
    task.status = "failed"
    task.failure_reason = reason
    task.completed_at = datetime.now(UTC)
    store.update(task)
    return task


@pytest.mark.parametrize(
    ("task_type", "reason"),
    [
        ("review", "MISSING_REPORT_ARTIFACT"),
        ("review", "needs-improvement"),
        ("improve", "GIT_ERROR"),
        ("improve", "WORKER_DIED"),
        ("rebase", "INTERRUPTED"),
    ],
)
def test_recovery_engine_suppresses_failed_sidequests_when_target_impl_is_merged(
    tmp_path: Path,
    task_type: str,
    reason: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _completed_impl(store, merge_status="merged")
    assert impl.id is not None

    failed = _failed_sidequest(store, task_type=task_type, impl_id=impl.id, reason=reason)

    assert is_resolved_by_merged_target(store, failed) is True

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "resolved_by_merged_target"
    assert decision.reason_text == "target implementation already merged"
    assert get_failed_recovery_needs_attention_reason(store, failed, decision=decision, max_recovery_attempts=1) is None
    assert list_failed_tasks_for_recovery(store) == []


@pytest.mark.parametrize("task_type", ["review", "improve", "rebase"])
def test_recovery_engine_keeps_failed_sidequests_visible_when_target_impl_is_not_merged(
    tmp_path: Path,
    task_type: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _completed_impl(store, merge_status="unmerged")
    assert impl.id is not None

    failed = _failed_sidequest(store, task_type=task_type, impl_id=impl.id, reason="MISSING_REPORT_ARTIFACT")

    assert is_resolved_by_merged_target(store, failed) is False

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert decision.reason_code != "resolved_by_merged_target"
    assert [task.id for task in list_failed_tasks_for_recovery(store)] == [failed.id]


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


def test_recovery_engine_timeout_without_session_requires_manual_review(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, reason="MAX_STEPS", session_id=None)
    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "manual_review_required"


def test_recovery_engine_manual_reason_skips(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, reason="TEST_FAILURE")
    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "manual_failure_reason"


def test_recovery_engine_review_timeout_chooses_resume(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, task_type="review", reason="MAX_TURNS")
    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)
    assert decision.action == "resume"
    assert decision.launch_mode == "worker"


def test_recovery_engine_existing_children_skip(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, reason="INFRASTRUCTURE_ERROR", session_id=None)
    child = store.add(task.prompt, task_type=task.task_type, based_on=task.id, depends_on=task.depends_on)
    assert child.id is not None
    child.status = "pending"
    store.update(child)

    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)
    assert decision.action == "retry"
    assert decision.recovery_task_id == child.id
    assert decision.reuse_existing is True


def test_recovery_engine_existing_pending_resume_child_reuses_resume_semantics(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, reason="MAX_TURNS", session_id="sess-1")
    child = store.add(task.prompt, task_type=task.task_type, based_on=task.id, depends_on=task.depends_on)
    assert child.id is not None
    child.status = "pending"
    child.session_id = task.session_id
    child.spec = task.spec
    child.branch = task.branch
    store.update(child)

    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)
    assert decision.action == "resume"
    assert decision.recovery_task_id == child.id
    assert decision.reuse_existing is True


def test_recovery_engine_pending_match_does_not_override_running_sibling(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, reason="INFRASTRUCTURE_ERROR", session_id=None)

    reusable_pending = store.add(task.prompt, task_type=task.task_type, based_on=task.id, depends_on=task.depends_on)
    assert reusable_pending.id is not None
    reusable_pending.status = "pending"
    store.update(reusable_pending)

    running_sibling = store.add(task.prompt, task_type=task.task_type, based_on=task.id, depends_on=task.depends_on)
    assert running_sibling.id is not None
    running_sibling.status = "in_progress"
    store.update(running_sibling)

    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "recovery_already_running"
    assert decision.recovery_task_id is None
    assert decision.reuse_existing is False


def test_recovery_engine_pending_match_does_not_override_completed_sibling(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, reason="INFRASTRUCTURE_ERROR", session_id=None)

    reusable_pending = store.add(task.prompt, task_type=task.task_type, based_on=task.id, depends_on=task.depends_on)
    assert reusable_pending.id is not None
    reusable_pending.status = "pending"
    store.update(reusable_pending)

    completed_sibling = store.add(task.prompt, task_type=task.task_type, based_on=task.id, depends_on=task.depends_on)
    assert completed_sibling.id is not None
    completed_sibling.status = "completed"
    completed_sibling.completed_at = datetime.now(UTC)
    store.update(completed_sibling)

    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "recovery_already_completed"
    assert decision.recovery_task_id is None
    assert decision.reuse_existing is False


def test_recovery_engine_marks_multi_step_resume_chain_as_resolved(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    root = store.add("Failed root", task_type="implement")
    assert root.id is not None
    root.status = "failed"
    root.failure_reason = "MAX_TURNS"
    root.session_id = "sess-root"
    root.branch = "feature/root"
    root.completed_at = datetime.now(UTC)
    store.update(root)

    failed_resume = store.add(root.prompt, task_type=root.task_type, based_on=root.id, depends_on=root.depends_on)
    assert failed_resume.id is not None
    failed_resume.status = "failed"
    failed_resume.failure_reason = "MAX_TURNS"
    failed_resume.session_id = root.session_id
    failed_resume.branch = root.branch
    failed_resume.completed_at = datetime.now(UTC)
    store.update(failed_resume)

    completed_resume = store.add(
        failed_resume.prompt,
        task_type=failed_resume.task_type,
        based_on=failed_resume.id,
        depends_on=failed_resume.depends_on,
    )
    assert completed_resume.id is not None
    completed_resume.status = "completed"
    completed_resume.session_id = failed_resume.session_id
    completed_resume.branch = failed_resume.branch
    completed_resume.completed_at = datetime.now(UTC)
    store.update(completed_resume)

    assert is_chain_resolved_by_recovery(store, root) is True
    assert is_chain_resolved_by_recovery(store, failed_resume) is True
    assert get_completed_recovery_descendant(store, root).id == completed_resume.id
    assert get_completed_recovery_descendant(store, failed_resume).id == completed_resume.id
    assert get_recovery_chain_root_task_id(store, completed_resume) == root.id


def test_recovery_engine_non_recovery_based_on_descendant_does_not_resolve_parent(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-failed"
    failed.branch = "feature/failed"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    manual_follow_up = store.add("Fresh follow-up implement", task_type="implement", based_on=failed.id)
    assert manual_follow_up.id is not None
    manual_follow_up.status = "completed"
    manual_follow_up.session_id = "sess-manual"
    manual_follow_up.branch = "feature/manual"
    manual_follow_up.completed_at = datetime.now(UTC)
    store.update(manual_follow_up)

    assert is_chain_resolved_by_recovery(store, failed) is False
    assert get_completed_recovery_descendant(store, failed) is None
    assert get_recovery_chain_root_task_id(store, manual_follow_up) == manual_follow_up.id


def test_list_failed_tasks_for_recovery_hides_fully_recovered_ancestors(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    root = store.add("Failed root", task_type="implement")
    assert root.id is not None
    root.status = "failed"
    root.failure_reason = "MAX_TURNS"
    root.session_id = "sess-root"
    root.branch = "feature/root"
    root.completed_at = datetime.now(UTC)
    store.update(root)

    failed_resume = store.add(root.prompt, task_type=root.task_type, based_on=root.id, depends_on=root.depends_on)
    assert failed_resume.id is not None
    failed_resume.status = "failed"
    failed_resume.failure_reason = "MAX_TURNS"
    failed_resume.session_id = root.session_id
    failed_resume.branch = root.branch
    failed_resume.completed_at = datetime.now(UTC)
    store.update(failed_resume)

    completed_resume = store.add(
        failed_resume.prompt,
        task_type=failed_resume.task_type,
        based_on=failed_resume.id,
        depends_on=failed_resume.depends_on,
    )
    assert completed_resume.id is not None
    completed_resume.status = "completed"
    completed_resume.session_id = failed_resume.session_id
    completed_resume.branch = failed_resume.branch
    completed_resume.completed_at = datetime.now(UTC)
    store.update(completed_resume)

    unrelated = store.add("Still failed", task_type="plan")
    assert unrelated.id is not None
    unrelated.status = "failed"
    unrelated.failure_reason = "INFRASTRUCTURE_ERROR"
    unrelated.completed_at = datetime.now(UTC)
    store.update(unrelated)

    failed = list_failed_tasks_for_recovery(store)
    assert [task.id for task in failed] == [unrelated.id]


def test_recovery_engine_pending_match_stays_suppressed_by_newer_failed_descendant(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    root = store.add("Failed root", task_type="plan")
    assert root.id is not None
    root.status = "failed"
    root.failure_reason = "INFRASTRUCTURE_ERROR"
    root.completed_at = datetime.now(UTC)
    store.update(root)

    reusable_pending = store.add(root.prompt, task_type=root.task_type, based_on=root.id, depends_on=root.depends_on)
    assert reusable_pending.id is not None
    reusable_pending.status = "pending"
    store.update(reusable_pending)

    failed_sibling = store.add(root.prompt, task_type=root.task_type, based_on=root.id, depends_on=root.depends_on)
    assert failed_sibling.id is not None
    failed_sibling.status = "failed"
    failed_sibling.failure_reason = "INFRASTRUCTURE_ERROR"
    failed_sibling.completed_at = datetime.now(UTC)
    store.update(failed_sibling)

    failed_grandchild = store.add(
        failed_sibling.prompt,
        task_type=root.task_type,
        based_on=failed_sibling.id,
        depends_on=failed_sibling.depends_on,
    )
    assert failed_grandchild.id is not None
    failed_grandchild.status = "failed"
    failed_grandchild.failure_reason = "MAX_TURNS"
    failed_grandchild.session_id = "sess-grandchild"
    failed_grandchild.completed_at = datetime.now(UTC)
    store.update(failed_grandchild)

    decision = decide_failed_task_recovery(store, root, max_recovery_attempts=3)
    assert decision.action == "skip"
    assert decision.reason_code == "recovery_has_newer_unresolved_descendant"
    assert decision.recovery_task_id is None
    assert decision.reuse_existing is False


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

    retry_child = store.add(root.prompt, task_type="implement", based_on=root.id, depends_on=root.depends_on)
    assert retry_child.id is not None
    retry_child.status = "failed"
    retry_child.failure_reason = "MAX_TURNS"
    retry_child.session_id = root.session_id
    retry_child.completed_at = datetime.now(UTC)
    store.update(retry_child)

    grandchild = store.add(
        retry_child.prompt,
        task_type="implement",
        based_on=retry_child.id,
        depends_on=retry_child.depends_on,
    )
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

    retry_child = store.add(root.prompt, task_type="plan", based_on=root.id, depends_on=root.depends_on)
    assert retry_child.id is not None
    retry_child.status = "failed"
    retry_child.failure_reason = "INFRASTRUCTURE_ERROR"
    retry_child.completed_at = datetime.now(UTC)
    store.update(retry_child)

    root_decision = decide_failed_task_recovery(store, root, max_recovery_attempts=3)
    assert root_decision.action == "skip"
    assert root_decision.reason_code == "recovery_has_newer_unresolved_descendant"

    child_decision = decide_failed_task_recovery(store, retry_child, max_recovery_attempts=3)
    assert child_decision.action == "skip"
    assert child_decision.reason_code == "manual_review_required"


def test_recovery_engine_dropped_recovery_child_requires_shared_attention(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    root = store.add("Failed root", task_type="implement")
    assert root.id is not None
    root.status = "failed"
    root.failure_reason = "MAX_TURNS"
    root.session_id = "sess-root"
    root.branch = "feature/root"
    root.completed_at = datetime.now(UTC)
    store.update(root)

    dropped_resume = store.add(root.prompt, task_type=root.task_type, based_on=root.id, depends_on=root.depends_on)
    assert dropped_resume.id is not None
    dropped_resume.status = "dropped"
    dropped_resume.session_id = root.session_id
    dropped_resume.branch = root.branch
    dropped_resume.completed_at = datetime.now(UTC)
    store.update(dropped_resume)

    decision = decide_failed_task_recovery(store, root, max_recovery_attempts=3)
    assert decision.action == "skip"
    assert decision.reason_code == "recovery_has_newer_unresolved_descendant"
    assert decision.recovery_task_id is None
    assert decision.reuse_existing is False
    assert get_failed_recovery_needs_attention_reason(store, root, decision=decision, max_recovery_attempts=3) == (
        "newer-recovery-descendant-needs-attention"
    )
    assert is_chain_resolved_by_recovery(store, root) is False


def test_recovery_engine_failed_then_dropped_recovery_descendant_requires_shared_attention(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    root = store.add("Failed root", task_type="implement")
    assert root.id is not None
    root.status = "failed"
    root.failure_reason = "MAX_TURNS"
    root.session_id = "sess-root"
    root.branch = "feature/root"
    root.completed_at = datetime.now(UTC)
    store.update(root)

    failed_resume = store.add(root.prompt, task_type=root.task_type, based_on=root.id, depends_on=root.depends_on)
    assert failed_resume.id is not None
    failed_resume.status = "failed"
    failed_resume.failure_reason = "MAX_TURNS"
    failed_resume.session_id = root.session_id
    failed_resume.branch = root.branch
    failed_resume.completed_at = datetime.now(UTC)
    store.update(failed_resume)

    dropped_grandchild = store.add(
        failed_resume.prompt,
        task_type=failed_resume.task_type,
        based_on=failed_resume.id,
        depends_on=failed_resume.depends_on,
    )
    assert dropped_grandchild.id is not None
    dropped_grandchild.status = "dropped"
    dropped_grandchild.session_id = failed_resume.session_id
    dropped_grandchild.branch = failed_resume.branch
    dropped_grandchild.completed_at = datetime.now(UTC)
    store.update(dropped_grandchild)

    root_decision = decide_failed_task_recovery(store, root, max_recovery_attempts=3)
    assert root_decision.action == "skip"
    assert root_decision.reason_code == "recovery_has_newer_unresolved_descendant"
    assert (
        get_failed_recovery_needs_attention_reason(store, root, decision=root_decision, max_recovery_attempts=3)
        == "newer-recovery-descendant-needs-attention"
    )

    child_decision = decide_failed_task_recovery(store, failed_resume, max_recovery_attempts=3)
    assert child_decision.action == "skip"
    assert child_decision.reason_code == "manual_review_required"
    assert is_chain_resolved_by_recovery(store, root) is False
    assert is_chain_resolved_by_recovery(store, failed_resume) is False


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
    child.prompt = failed.prompt
    child.session_id = failed.session_id
    child.spec = failed.spec
    child.branch = failed.branch
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
    child = store.add(task.prompt, task_type=task.task_type, based_on=task.id, depends_on=task.depends_on)
    assert child.id is not None
    child.status = "pending"
    store.update(child)

    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "manual_failure_reason"
    assert decision.recovery_task_id is None
    assert decision.reuse_existing is False


def test_recovery_engine_prerequisite_unmerged_retries_after_dependency_merge(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    dependency = store.add("Dependency", task_type="implement")
    assert dependency.id is not None
    dependency.status = "completed"
    dependency.merge_status = "unmerged"
    dependency.completed_at = datetime.now(UTC)
    store.update(dependency)

    failed = store.add("Failed downstream", task_type="implement", depends_on=dependency.id)
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PREREQUISITE_UNMERGED"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    blocked_decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert blocked_decision.action == "skip"
    assert blocked_decision.reason_code == "dependency_not_ready"

    store.set_merge_status(dependency.id, "merged")

    ready_decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert ready_decision.action == "retry"
    assert ready_decision.reason_code == "PREREQUISITE_UNMERGED"


def test_recovery_engine_attempt_cap_reached_skips(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, reason="INFRASTRUCTURE_ERROR", session_id=None)
    attempt = store.add(task.prompt, task_type=task.task_type, based_on=task.id, depends_on=task.depends_on)
    assert attempt.id is not None
    attempt.status = "failed"
    attempt.failure_reason = "INFRASTRUCTURE_ERROR"
    attempt.completed_at = datetime.now(UTC)
    store.update(attempt)

    decision = decide_failed_task_recovery(store, attempt, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "manual_review_required"


def test_recovery_engine_resume_child_failure_stops(tmp_path: Path) -> None:
    store, root = _failed_task(tmp_path, reason="MAX_TURNS", session_id="sess-1")
    child = store.add(root.prompt, task_type=root.task_type, based_on=root.id, depends_on=root.depends_on)
    assert child.id is not None
    child.status = "failed"
    child.failure_reason = "INFRASTRUCTURE_ERROR"
    child.session_id = root.session_id
    child.spec = root.spec
    child.branch = root.branch
    child.completed_at = datetime.now(UTC)
    store.update(child)

    decision = decide_failed_task_recovery(store, child, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "manual_review_required"


def test_recovery_engine_retry_child_timeout_gets_one_resume(tmp_path: Path) -> None:
    store, root = _failed_task(tmp_path, task_type="plan", reason="INFRASTRUCTURE_ERROR", session_id=None)
    retry_child = store.add(root.prompt, task_type=root.task_type, based_on=root.id, depends_on=root.depends_on)
    assert retry_child.id is not None
    retry_child.status = "failed"
    retry_child.failure_reason = "MAX_TURNS"
    retry_child.session_id = "sess-retry"
    retry_child.completed_at = datetime.now(UTC)
    store.update(retry_child)

    decision = decide_failed_task_recovery(store, retry_child, max_recovery_attempts=1)
    assert decision.action == "resume"
    assert decision.launch_mode == "worker"


def test_recovery_engine_retry_resume_child_failure_stops(tmp_path: Path) -> None:
    store, root = _failed_task(tmp_path, task_type="plan", reason="INFRASTRUCTURE_ERROR", session_id=None)
    retry_child = store.add(root.prompt, task_type=root.task_type, based_on=root.id, depends_on=root.depends_on)
    assert retry_child.id is not None
    retry_child.status = "failed"
    retry_child.failure_reason = "MAX_TURNS"
    retry_child.session_id = "sess-retry"
    retry_child.completed_at = datetime.now(UTC)
    store.update(retry_child)

    resumed_retry = store.add(
        retry_child.prompt,
        task_type=retry_child.task_type,
        based_on=retry_child.id,
        depends_on=retry_child.depends_on,
    )
    assert resumed_retry.id is not None
    resumed_retry.status = "failed"
    resumed_retry.failure_reason = "TIMEOUT"
    resumed_retry.session_id = retry_child.session_id
    resumed_retry.spec = retry_child.spec
    resumed_retry.branch = retry_child.branch
    resumed_retry.completed_at = datetime.now(UTC)
    store.update(resumed_retry)

    decision = decide_failed_task_recovery(store, resumed_retry, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "manual_review_required"


def test_recovery_engine_retry_resume_failure_saturates_attempt_counter(tmp_path: Path) -> None:
    store, root = _failed_task(tmp_path, task_type="plan", reason="INFRASTRUCTURE_ERROR", session_id=None)
    retry_child = store.add(root.prompt, task_type=root.task_type, based_on=root.id, depends_on=root.depends_on)
    assert retry_child.id is not None
    retry_child.status = "failed"
    retry_child.failure_reason = "MAX_TURNS"
    retry_child.session_id = "sess-retry"
    retry_child.completed_at = datetime.now(UTC)
    store.update(retry_child)

    resumed_retry = store.add(
        retry_child.prompt,
        task_type=retry_child.task_type,
        based_on=retry_child.id,
        depends_on=retry_child.depends_on,
    )
    assert resumed_retry.id is not None
    resumed_retry.status = "failed"
    resumed_retry.failure_reason = "TIMEOUT"
    resumed_retry.session_id = retry_child.session_id
    resumed_retry.spec = retry_child.spec
    resumed_retry.branch = retry_child.branch
    resumed_retry.completed_at = datetime.now(UTC)
    store.update(resumed_retry)

    decision = decide_failed_task_recovery(store, resumed_retry, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "manual_review_required"
    assert (decision.attempt_index, decision.attempt_limit) == (2, 2)


def test_recovery_engine_counts_only_based_on_chain_not_dependency_ancestry(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    upstream = store.add("Upstream plan", task_type="plan")
    assert upstream.id is not None
    upstream.status = "completed"
    upstream.completed_at = datetime.now(UTC)
    store.update(upstream)

    upstream_retry = store.add(upstream.prompt, task_type="plan", based_on=upstream.id)
    assert upstream_retry.id is not None
    upstream_retry.status = "failed"
    upstream_retry.failure_reason = "MAX_TURNS"
    upstream_retry.session_id = "sess-upstream"
    upstream_retry.completed_at = datetime.now(UTC)
    store.update(upstream_retry)

    downstream = store.add("Downstream plan", task_type="plan", depends_on=upstream.id)
    assert downstream.id is not None
    downstream.status = "failed"
    downstream.failure_reason = "INFRASTRUCTURE_ERROR"
    downstream.completed_at = datetime.now(UTC)
    store.update(downstream)

    decision = decide_failed_task_recovery(store, downstream, max_recovery_attempts=1)
    assert decision.action == "retry"


def test_recovery_engine_multiple_pending_children_require_manual_review(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, reason="INFRASTRUCTURE_ERROR", session_id=None)

    first_pending = store.add(task.prompt, task_type=task.task_type, based_on=task.id, depends_on=task.depends_on)
    second_pending = store.add(task.prompt, task_type=task.task_type, based_on=task.id, depends_on=task.depends_on)
    assert first_pending.id is not None
    assert second_pending.id is not None
    first_pending.status = "pending"
    second_pending.status = "pending"
    store.update(first_pending)
    store.update(second_pending)

    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "manual_review_required"
    assert decision.reason_text == "multiple pending recovery children require manual review"
    assert decision.recovery_task_id is None


def test_recovery_engine_pending_manual_follow_up_does_not_suppress_failed_parent(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-failed"
    failed.branch = "feature/failed"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    manual_follow_up = store.add("Fresh follow-up implement", task_type="implement", based_on=failed.id)
    assert manual_follow_up.id is not None
    manual_follow_up.status = "pending"
    manual_follow_up.session_id = "sess-manual"
    manual_follow_up.branch = "feature/manual"
    store.update(manual_follow_up)

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert decision.action == "resume"
    assert decision.reason_code == "MAX_TURNS"
    assert decision.reason_text == "MAX_TURNS with preserved session"
    assert decision.recovery_task_id is None
    assert decision.reuse_existing is False


def test_recovery_engine_failed_manual_follow_up_does_not_supersede_failed_parent(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed plan", task_type="plan")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    manual_follow_up = store.add("Fresh follow-up plan", task_type="plan", based_on=failed.id)
    assert manual_follow_up.id is not None
    manual_follow_up.status = "failed"
    manual_follow_up.failure_reason = "MAX_TURNS"
    manual_follow_up.session_id = "sess-manual"
    manual_follow_up.branch = "feature/manual"
    manual_follow_up.completed_at = datetime.now(UTC)
    store.update(manual_follow_up)

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert decision.action == "retry"
    assert decision.reason_code == "INFRASTRUCTURE_ERROR"
    assert decision.reason_text == "INFRASTRUCTURE_ERROR restart with fresh attempt"
    assert decision.recovery_task_id is None
    assert decision.reuse_existing is False


def test_recovery_engine_completed_same_payload_manual_follow_up_different_session_branch_stays_non_recovery(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    dependency = store.add("Dependency", task_type="plan")
    assert dependency.id is not None
    dependency.status = "completed"
    dependency.completed_at = datetime.now(UTC)
    store.update(dependency)

    failed = store.add("Failed implement", task_type="implement", depends_on=dependency.id)
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-failed"
    failed.branch = "feature/failed"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    manual_follow_up = store.add(
        failed.prompt,
        task_type="implement",
        based_on=failed.id,
        depends_on=failed.depends_on,
        recovery_origin="manual",
    )
    assert manual_follow_up.id is not None
    manual_follow_up.status = "completed"
    manual_follow_up.session_id = "sess-manual"
    manual_follow_up.branch = "feature/manual"
    manual_follow_up.completed_at = datetime.now(UTC)
    store.update(manual_follow_up)

    chain = get_recovery_chain_state(store, failed)
    assert chain.role == "original"
    assert chain.steps == ()
    assert chain.root_task_id == failed.id
    assert chain.resolved_task_id is None
    assert is_chain_resolved_by_recovery(store, failed) is False
    assert get_completed_recovery_descendant(store, failed) is None

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert decision.action == "resume"
    assert decision.reason_code == "MAX_TURNS"
    assert decision.recovery_task_id is None
    assert decision.reuse_existing is False


def test_recovery_engine_completed_same_payload_legacy_manual_follow_up_different_session_branch_stays_non_recovery(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    dependency = store.add("Dependency", task_type="plan")
    assert dependency.id is not None
    dependency.status = "completed"
    dependency.completed_at = datetime.now(UTC)
    store.update(dependency)

    failed = store.add("Failed implement", task_type="implement", depends_on=dependency.id)
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-failed"
    failed.branch = "feature/failed"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    manual_follow_up = store.add(
        failed.prompt,
        task_type="implement",
        based_on=failed.id,
        depends_on=failed.depends_on,
    )
    assert manual_follow_up.id is not None
    manual_follow_up.status = "completed"
    manual_follow_up.session_id = "sess-manual"
    manual_follow_up.branch = "feature/manual"
    manual_follow_up.completed_at = datetime.now(UTC)
    store.update(manual_follow_up)

    chain = get_recovery_chain_state(store, failed)
    assert chain.role == "original"
    assert chain.steps == ()
    assert chain.root_task_id == failed.id
    assert chain.resolved_task_id is None
    assert is_chain_resolved_by_recovery(store, failed) is False
    assert get_completed_recovery_descendant(store, failed) is None

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert decision.action == "resume"
    assert decision.reason_code == "MAX_TURNS"
    assert decision.recovery_task_id is None
    assert decision.reuse_existing is False


def test_recovery_engine_non_recovery_break_blocks_deeper_recovery_resolution_for_root(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed_root = store.add("Failed implement", task_type="implement")
    assert failed_root.id is not None
    failed_root.status = "failed"
    failed_root.failure_reason = "MAX_TURNS"
    failed_root.session_id = "sess-root"
    failed_root.branch = "feature/root"
    failed_root.completed_at = datetime.now(UTC)
    store.update(failed_root)

    manual_follow_up = store.add(
        failed_root.prompt,
        task_type="implement",
        based_on=failed_root.id,
        recovery_origin="manual",
    )
    assert manual_follow_up.id is not None
    manual_follow_up.status = "failed"
    manual_follow_up.failure_reason = "MAX_TURNS"
    manual_follow_up.session_id = "sess-manual"
    manual_follow_up.branch = "feature/manual"
    manual_follow_up.completed_at = datetime.now(UTC)
    store.update(manual_follow_up)

    completed_resume = store.add(
        manual_follow_up.prompt,
        task_type="implement",
        based_on=manual_follow_up.id,
        depends_on=manual_follow_up.depends_on,
    )
    assert completed_resume.id is not None
    completed_resume.status = "completed"
    completed_resume.session_id = manual_follow_up.session_id
    completed_resume.branch = manual_follow_up.branch
    completed_resume.completed_at = datetime.now(UTC)
    store.update(completed_resume)

    root_chain = get_recovery_chain_state(store, failed_root)
    assert root_chain.role == "original"
    assert root_chain.steps == ()
    assert root_chain.root_task_id == failed_root.id
    assert root_chain.resolved_task_id is None
    assert is_chain_resolved_by_recovery(store, failed_root) is False
    assert get_completed_recovery_descendant(store, failed_root) is None

    manual_chain = get_recovery_chain_state(store, manual_follow_up)
    assert manual_chain.role == "original"
    assert manual_chain.root_task_id == manual_follow_up.id
    assert manual_chain.resolved_task_id == completed_resume.id
    assert is_chain_resolved_by_recovery(store, manual_follow_up) is True


def test_list_failed_tasks_for_recovery_sorts_oldest_created_first(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    legacy = store.add("Legacy failed task", task_type="plan")
    assert legacy.id is not None
    legacy.status = "failed"
    legacy.failure_reason = "INFRASTRUCTURE_ERROR"
    legacy.completed_at = datetime(2026, 4, 28, 10, 0, 0)
    store.update(legacy)

    current = store.add("Current failed task", task_type="plan")
    assert current.id is not None
    current.status = "failed"
    current.failure_reason = "INFRASTRUCTURE_ERROR"
    current.completed_at = datetime(2026, 4, 28, 11, 0, 0, tzinfo=UTC)
    store.update(current)

    failed = list_failed_tasks_for_recovery(store)
    assert [task.id for task in failed] == [legacy.id, current.id]
