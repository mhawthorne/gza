import logging
from datetime import UTC, datetime
from pathlib import Path

import pytest

import gza.recovery_engine as recovery_engine
from gza.branch_publication import BranchPublicationState, persist_branch_publication_state
from gza.config import Config
from gza.config import ConfigError
from gza.db import MergeTargetResolutionError
from gza.git import Git, GitError
from gza.lineage_query import _load_indexes
from gza.operator_state import MOOT_EMPTY_LIFECYCLE_DETAIL, MOOT_REDUNDANT_LIFECYCLE_DETAIL
from gza.recovery_read_context import RecoveryReadContext
from gza.recovery_engine import (
    _MergeContext,
    _build_recovery_chain_snapshot,
    _is_resolved_by_landed_lineage,
    FailedRecoveryDecision,
    classify_failure_reason,
    decide_failed_task_recovery,
    empty_task_requires_recovery,
    get_completed_recovery_descendant,
    get_completed_sibling_recovery,
    get_failed_recovery_needs_attention_reason,
    get_recovery_chain_root_task_id,
    get_recovery_chain_state,
    is_chain_resolved_by_recovery,
    is_resolved_by_merged_target,
    list_failed_tasks_for_recovery,
    resolve_pending_recovery_execution_mode,
    resolve_recovery_planning_task,
    should_hide_failed_recovery_decision,
)
from tests.cli.conftest import make_store, setup_config


def _read_context_for_store(store) -> RecoveryReadContext:
    indexes = _load_indexes(store)
    return RecoveryReadContext(
        tasks=indexes.tasks,
        task_by_id=indexes.task_by_id,
        based_on_children=indexes.based_on_children,
        depends_on_children=indexes.depends_on_children,
        root_by_task_id=indexes.root_by_task_id,
        merge_units_by_task_id=indexes.merge_units_by_task_id,
        allow_reconcile_mutation=False,
    )


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


def _attach_empty_merge_unit(store, task) -> None:
    assert task.id is not None
    unit = store.create_merge_unit(
        source_branch=task.branch or f"feature/{task.id}",
        target_branch="main",
        owner_task_id=task.id,
        state="empty",
    )
    store.attach_task_to_merge_unit(task.id, unit.id, "owner")


def _attach_redundant_merge_unit(store, task) -> None:
    assert task.id is not None
    unit = store.create_merge_unit(
        source_branch=task.branch or f"feature/{task.id}",
        target_branch="main",
        owner_task_id=task.id,
        state="redundant",
    )
    store.attach_task_to_merge_unit(task.id, unit.id, "owner")


class _StubMergeGit:
    def __init__(
        self,
        *,
        merged_side_branches: set[str] | None = None,
        empty_merged_branches: set[str] | None = None,
        default_branch: str = "main",
    ) -> None:
        self.merged_side_branches = merged_side_branches or set()
        self.empty_merged_branches = empty_merged_branches or set()
        self.merged_branches = self.merged_side_branches | self.empty_merged_branches
        self.default_branch = default_branch

    def resolve_fresh_merge_source(self, branch: str):
        from gza.git import ResolvedMergeSourceRef

        return ResolvedMergeSourceRef(branch)

    def rev_parse_if_exists(self, ref: str) -> str | None:
        if ref == self.default_branch:
            return "target-tip"
        if ref in self.merged_side_branches:
            return f"{ref}-merged-tip"
        if ref in self.empty_merged_branches:
            return f"{ref}-empty-tip"
        return f"{ref}-tip" if ref else None

    def branch_exists(self, branch: str) -> bool:
        return bool(branch)

    def is_merged(self, branch: str, into: str) -> bool:
        return into == self.default_branch and branch in self.merged_branches

    def count_commits_ahead_checked(self, branch: str, base: str) -> int | None:
        if base != self.default_branch:
            return None
        if branch in self.merged_branches:
            return 0
        return 1

    def is_on_first_parent_history(self, commit: str, target: str) -> bool:
        return target == self.default_branch and commit in self.empty_merged_branches


class _StubEmptyBranchGit:
    def __init__(self, *, target_branch: str = "main") -> None:
        self.target_branch = target_branch

    def resolve_fresh_merge_source(self, branch: str):
        from gza.git import ResolvedMergeSourceRef

        return ResolvedMergeSourceRef(branch)

    def rev_parse_if_exists(self, ref: str) -> str | None:
        if ref in {self.target_branch, "feature/prereq-empty", "feature/prereq-empty-retry"}:
            return "abc123"
        return None

    def branch_exists(self, branch: str) -> bool:
        return bool(branch)

    def is_merged(self, branch: str, into: str) -> bool:
        return False


class _StubNonEmptyBranchGit:
    def __init__(self, *, target_branch: str = "main") -> None:
        self.target_branch = target_branch

    def resolve_fresh_merge_source(self, branch: str):
        from gza.git import ResolvedMergeSourceRef

        return ResolvedMergeSourceRef(branch)

    def rev_parse_if_exists(self, ref: str) -> str | None:
        if ref == self.target_branch:
            return "target123"
        if ref in {"feature/prereq-live-work", "feature/prereq-live-work-watch"}:
            return "source456"
        return None

    def branch_exists(self, branch: str) -> bool:
        return bool(branch)

    def is_merged(self, branch: str, into: str) -> bool:
        return False

    def count_commits_ahead(self, source_ref: str, base_ref: str) -> int:
        return 1


def _stub_merge_context(
    monkeypatch: pytest.MonkeyPatch,
    *,
    merged_side_branches: set[str] | None = None,
    empty_merged_branches: set[str] | None = None,
    default_branch: str = "main",
) -> None:
    git = _StubMergeGit(
        merged_side_branches=merged_side_branches,
        empty_merged_branches=empty_merged_branches,
        default_branch=default_branch,
    )
    monkeypatch.setattr(
        recovery_engine,
        "_load_merge_context",
        lambda _project_dir=None: _MergeContext(git=git, default_branch=default_branch),
    )


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


def test_list_failed_tasks_for_recovery_filters_failed_impl_when_landed_work_is_already_on_main(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _stub_merge_context(monkeypatch, empty_merged_branches={"feature/landed-work"})

    failed = store.add("Failed implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.branch = "feature/landed-work"
    failed.has_commits = True
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    unrelated = store.add("Still failed", task_type="plan")
    assert unrelated.id is not None
    unrelated.status = "failed"
    unrelated.failure_reason = "INFRASTRUCTURE_ERROR"
    unrelated.completed_at = datetime.now(UTC)
    store.update(unrelated)

    assert is_chain_resolved_by_recovery(store, failed) is False
    assert [task.id for task in list_failed_tasks_for_recovery(store)] == [unrelated.id]


def test_list_failed_tasks_for_recovery_keeps_empty_failed_worker_died_branch_for_shared_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _stub_merge_context(
        monkeypatch,
        empty_merged_branches={"feature/empty-worker-died"},
    )

    failed = store.add("Failed implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "WORKER_DIED"
    failed.branch = "feature/empty-worker-died"
    failed.session_id = "sess-empty-worker-died"
    failed.num_steps_computed = 1
    failed.completed_at = datetime.now(UTC)
    store.update(failed)
    _attach_empty_merge_unit(store, failed)

    assert [task.id for task in list_failed_tasks_for_recovery(store)] == [failed.id]

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert decision.action == "retry"
    assert decision.reason_code == "WORKER_DIED"


def test_list_failed_tasks_for_recovery_keeps_failed_task_without_landed_lineage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _stub_merge_context(monkeypatch)

    failed = store.add("Failed implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-failed"
    failed.branch = "feature/unmerged-work"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    failed_retry = store.add(failed.prompt, task_type="implement", based_on=failed.id)
    assert failed_retry.id is not None
    failed_retry.status = "failed"
    failed_retry.failure_reason = "MAX_TURNS"
    failed_retry.session_id = "sess-retry"
    failed_retry.branch = "feature/unmerged-work-retry"
    failed_retry.completed_at = datetime.now(UTC)
    store.update(failed_retry)

    assert is_chain_resolved_by_recovery(store, failed) is False
    assert [task.id for task in list_failed_tasks_for_recovery(store)] == [failed.id, failed_retry.id]


def test_empty_task_requires_recovery_for_session_backed_failed_empty_branch(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-empty"
    failed.branch = "feature/resume-empty"
    failed.num_steps_computed = 2
    failed.completed_at = datetime.now(UTC)
    store.update(failed)
    _attach_empty_merge_unit(store, failed)

    assert empty_task_requires_recovery(store, failed) is True

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert decision.action == "resume"
    assert [task.id for task in list_failed_tasks_for_recovery(store)] == [failed.id]


def test_empty_task_requires_recovery_fail_closed_when_session_metrics_are_missing(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-missing"
    failed.branch = "feature/resume-empty-missing"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)
    _attach_empty_merge_unit(store, failed)

    assert empty_task_requires_recovery(store, failed) is True
    assert decide_failed_task_recovery(store, failed, max_recovery_attempts=1).action == "resume"


@pytest.mark.parametrize(
    ("recovery_origin", "session_id", "expected"),
    [
        ("resume", "sess-pending", "resume"),
        ("resume", None, "retry"),
        ("retry", None, "retry"),
        (None, "sess-pending", None),
    ],
)
def test_resolve_pending_recovery_execution_mode(
    tmp_path: Path,
    recovery_origin: str | None,
    session_id: str | None,
    expected: str | None,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Pending recovery row", task_type="implement", recovery_origin=recovery_origin)
    assert task.id is not None
    task.status = "pending"
    task.session_id = session_id
    store.update(task)

    assert resolve_pending_recovery_execution_mode(task) == expected


def test_empty_task_requires_recovery_false_when_landed_representative_exists(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    root = store.add("Implementation root", task_type="implement")
    assert root.id is not None
    root.status = "completed"
    root.branch = "feature/root"
    root.has_commits = True
    root.completed_at = datetime.now(UTC)
    store.update(root)

    failed = store.add("Failed manual follow-up", task_type="implement", based_on=root.id, recovery_origin="manual")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-empty-landed"
    failed.branch = "feature/independent-landed"
    failed.num_steps_computed = 2
    failed.completed_at = datetime.now(UTC)
    store.update(failed)
    _attach_empty_merge_unit(store, failed)

    landed = store.add("Merged sibling representative", task_type="implement", based_on=root.id, recovery_origin="manual")
    assert landed.id is not None
    landed.status = "completed"
    landed.branch = failed.branch
    landed.has_commits = True
    landed.merge_status = "merged"
    landed.completed_at = datetime.now(UTC)
    store.update(landed)

    assert empty_task_requires_recovery(store, failed) is False

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "terminal_no_work_recovery_already_resolved"
    assert decision.reason_text == "terminal no-work failed task already resolved by landed lineage or completed recovery work"
    assert list_failed_tasks_for_recovery(store) == []


def test_list_failed_tasks_for_recovery_filters_moot_failed_empty_branch_without_execution(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Never-ran implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-empty-zero"
    failed.branch = "feature/moot-empty"
    failed.num_steps_computed = 0
    failed.num_steps_reported = 0
    failed.output_tokens = 0
    failed.completed_at = datetime.now(UTC)
    store.update(failed)
    _attach_empty_merge_unit(store, failed)

    assert empty_task_requires_recovery(store, failed) is False

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "merge_unit_empty"
    assert decision.reason_text == MOOT_EMPTY_LIFECYCLE_DETAIL
    assert decision.reason_text != MOOT_REDUNDANT_LIFECYCLE_DETAIL
    assert list_failed_tasks_for_recovery(store) == []


def test_failed_redundant_branch_without_execution_uses_distinct_moot_reason(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Never-ran redundant implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-redundant-zero"
    failed.branch = "feature/moot-redundant"
    failed.has_commits = True
    failed.num_steps_computed = 0
    failed.num_steps_reported = 0
    failed.output_tokens = 0
    failed.completed_at = datetime.now(UTC)
    store.update(failed)
    _attach_redundant_merge_unit(store, failed)

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "merge_unit_redundant"
    assert decision.reason_text == MOOT_REDUNDANT_LIFECYCLE_DETAIL
    assert decision.reason_text != MOOT_EMPTY_LIFECYCLE_DETAIL
    assert should_hide_failed_recovery_decision(decision) is True
    assert list_failed_tasks_for_recovery(store) == []


def test_failed_redundant_branch_with_provider_execution_remains_recoverable(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Executed redundant implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-redundant-executed"
    failed.branch = "feature/recoverable-redundant"
    failed.has_commits = True
    failed.num_steps_computed = 2
    failed.completed_at = datetime.now(UTC)
    store.update(failed)
    _attach_redundant_merge_unit(store, failed)

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert decision.action != "skip"
    assert decision.reason_code != "merge_unit_redundant"
    assert [task.id for task in list_failed_tasks_for_recovery(store)] == [failed.id]


def test_failed_redundant_branch_with_completed_recovery_descendant_is_already_resolved(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Executed redundant implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-redundant-resolved"
    failed.branch = "feature/resolved-redundant"
    failed.has_commits = True
    failed.num_steps_computed = 2
    failed.completed_at = datetime.now(UTC)
    store.update(failed)
    _attach_redundant_merge_unit(store, failed)

    recovered = store.add(
        "Completed recovery descendant",
        task_type="implement",
        based_on=failed.id,
        recovery_origin="retry",
    )
    assert recovered.id is not None
    recovered.status = "completed"
    recovered.branch = failed.branch
    recovered.has_commits = True
    recovered.completed_at = datetime.now(UTC)
    store.update(recovered)
    _attach_redundant_merge_unit(store, recovered)

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code in {
        "recovery_already_completed",
        "terminal_no_work_recovery_already_resolved",
    }
    assert list_failed_tasks_for_recovery(store) == []


def test_decide_failed_task_recovery_live_resolution_for_unitless_redundant_branch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unit-less task whose live branch resolves redundant is moot for both decide and list.

    No git instance is passed manually — the function auto-loads merge context so that
    every production caller (advance, watch, iterate) agrees with list_failed_tasks_for_recovery.
    """
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Unitless redundant implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.branch = "feature/unitless-redundant"
    failed.has_commits = True
    failed.num_steps_computed = 0
    failed.num_steps_reported = 0
    failed.output_tokens = 0
    failed.completed_at = datetime.now(UTC)
    store.update(failed)
    # No merge unit attached — unit-less task

    monkeypatch.setattr(
        recovery_engine,
        "resolve_task_merge_state_for_target",
        lambda **kwargs: "redundant",
    )
    # _stub_merge_context patches _load_merge_context so the auto-load path inside
    # decide_failed_task_recovery returns a working stub — no git injected by hand.
    _stub_merge_context(monkeypatch)

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "merge_unit_redundant"
    assert decision.reason_text == MOOT_REDUNDANT_LIFECYCLE_DETAIL
    assert list_failed_tasks_for_recovery(store) == []


def test_decide_failed_task_recovery_live_resolution_for_unitless_empty_branch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unit-less task whose live branch resolves empty uses MOOT_EMPTY_LIFECYCLE_DETAIL.

    Verifies that the live-moot empty path sources its reason_text from the centralized
    constant rather than a hardcoded string, so future edits to the constant propagate here.
    """
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Unitless empty implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.branch = "feature/unitless-empty"
    failed.has_commits = False
    failed.num_steps_computed = 0
    failed.num_steps_reported = 0
    failed.output_tokens = 0
    failed.completed_at = datetime.now(UTC)
    store.update(failed)
    # No merge unit attached — live probe determines state

    monkeypatch.setattr(
        recovery_engine,
        "resolve_task_merge_state_for_target",
        lambda **kwargs: "empty",
    )
    _stub_merge_context(monkeypatch)

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "merge_unit_empty"
    assert decision.reason_text == MOOT_EMPTY_LIFECYCLE_DETAIL
    assert list_failed_tasks_for_recovery(store) == []


def test_decide_failed_task_recovery_live_probe_failure_logs_warning_and_does_not_silently_moot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """When the live merge-state probe raises, a warning is logged and the task is NOT silently mooted.

    Mirrors test_list_failed_tasks_for_recovery_emits_one_warning_when_branch_reachability_probe_fails
    for the decide path: probe failure must be observable, not a silent recovery-action change.
    """
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Unitless implementation with probe failure", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.branch = "feature/probe-failure"
    failed.has_commits = True
    failed.session_id = "sess-probe-failure"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)
    # No merge unit attached — unit-less task

    def _raise_git_error(**kwargs: object) -> None:
        raise GitError("simulated probe failure")

    monkeypatch.setattr(recovery_engine, "resolve_task_merge_state_for_target", _raise_git_error)
    _stub_merge_context(monkeypatch)

    with caplog.at_level(logging.WARNING, logger="gza.recovery_engine"):
        decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)

    warning_messages = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("live merge-state probe failed" in m for m in warning_messages), (
        f"Expected probe-failure warning in: {warning_messages}"
    )
    assert any("feature/probe-failure" in m for m in warning_messages), (
        f"Expected branch name in warning: {warning_messages}"
    )

    # Decision must not be silently mooted — task has session so should resume, not skip
    assert decision.action == "resume", (
        f"Expected resume after probe failure, got action={decision.action} reason={decision.reason_code}"
    )


def test_list_failed_tasks_for_recovery_emits_one_warning_when_branch_reachability_probe_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    class _BrokenMergeGit:
        def branch_exists(self, branch: str) -> bool:
            return bool(branch)

        def is_merged(self, branch: str, into: str) -> bool:
            raise GitError("simulated reachability failure")

    monkeypatch.setattr(
        recovery_engine,
        "_load_merge_context",
        lambda _project_dir=None: _MergeContext(git=_BrokenMergeGit(), default_branch="main"),
    )

    first = store.add("Failed implementation A", task_type="implement")
    assert first.id is not None
    first.status = "failed"
    first.failure_reason = "INFRASTRUCTURE_ERROR"
    first.branch = "feature/a"
    first.completed_at = datetime.now(UTC)
    store.update(first)

    second = store.add("Failed implementation B", task_type="implement")
    assert second.id is not None
    second.status = "failed"
    second.failure_reason = "INFRASTRUCTURE_ERROR"
    second.branch = "feature/b"
    second.completed_at = datetime.now(UTC)
    store.update(second)

    warnings: list[str] = []
    failed = list_failed_tasks_for_recovery(store, warnings=warnings)

    assert {task.id for task in failed} == {first.id, second.id}
    assert len(warnings) == 1
    assert warnings[0].startswith(
        "Failed-task recovery could not inspect repository branch reachability; "
        "git branch reachability suppression is unavailable for this run, but "
        "metadata-based same-lineage merged-task suppression may still apply: "
        "failed to check whether branch 'feature/"
    )
    assert "reached default branch 'main': simulated reachability failure" in warnings[0]


def test_list_failed_tasks_for_recovery_warns_once_when_local_branch_batch_probe_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    class _BranchListFailureGit:
        def __init__(self, project_dir: Path) -> None:
            self.project_dir = project_dir

        def default_branch(self) -> str:
            return "main"

        def local_branch_names(self) -> frozenset[str]:
            raise GitError("simulated branch list failure")

        def branch_exists(self, branch: str) -> bool:
            return False

        def is_merged(self, branch: str, into: str) -> bool:
            raise AssertionError("is_merged should not run when branch_exists returns False")

    monkeypatch.setattr(recovery_engine, "Git", _BranchListFailureGit)

    first = store.add("Failed implementation A", task_type="implement")
    assert first.id is not None
    first.status = "failed"
    first.failure_reason = "INFRASTRUCTURE_ERROR"
    first.branch = "feature/a"
    first.completed_at = datetime.now(UTC)
    store.update(first)

    second = store.add("Failed implementation B", task_type="implement")
    assert second.id is not None
    second.status = "failed"
    second.failure_reason = "INFRASTRUCTURE_ERROR"
    second.branch = "feature/b"
    second.completed_at = datetime.now(UTC)
    store.update(second)

    warnings: list[str] = []
    failed = list_failed_tasks_for_recovery(store, warnings=warnings)

    assert [task.id for task in failed] == [first.id, second.id]
    assert warnings == [
        "Failed-task recovery could not inspect repository branch reachability; "
        "git branch reachability suppression is unavailable for this run, but "
        "metadata-based same-lineage merged-task suppression may still apply: "
        "failed to list local branches for recovery-lane batch inspection: simulated branch list failure"
    ]


def test_load_merge_context_warning_says_metadata_based_lineage_suppression_may_still_apply(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        recovery_engine.Config,
        "load",
        staticmethod(lambda *_args, **_kwargs: (_ for _ in ()).throw(ConfigError("simulated config failure"))),
    )

    merge_context = recovery_engine._load_merge_context(tmp_path)

    assert merge_context.git is None
    assert merge_context.default_branch is None
    assert merge_context.repository_inspection_warnings == [
        "Failed-task recovery could not inspect repository branch reachability; "
        "git branch reachability suppression is unavailable for this run, but "
        "metadata-based same-lineage merged-task suppression may still apply: "
        "failed to load repository default-branch context: simulated config failure"
    ]


def test_list_failed_tasks_for_recovery_raises_when_project_default_merge_target_cannot_be_resolved(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.branch = "feature/unresolved-default"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    monkeypatch.setattr(
        recovery_engine,
        "_load_merge_context",
        lambda _project_dir=None: _MergeContext(
            git=None,
            default_branch=None,
            resolution_error="simulated default-branch failure",
        ),
    )

    with pytest.raises(MergeTargetResolutionError, match="Could not determine default merge target"):
        list_failed_tasks_for_recovery(store)


def test_list_failed_tasks_for_recovery_filters_failed_descendant_when_merged_ancestor_shares_branch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _stub_merge_context(monkeypatch)

    merged_ancestor = _completed_impl(store, merge_status="merged")
    assert merged_ancestor.id is not None
    merged_ancestor.branch = "feature/shared-lineage"
    store.update(merged_ancestor)

    failed_descendant = store.add("Failed retry", task_type="implement", based_on=merged_ancestor.id)
    assert failed_descendant.id is not None
    failed_descendant.status = "failed"
    failed_descendant.failure_reason = "MAX_TURNS"
    failed_descendant.session_id = "sess-descendant"
    failed_descendant.branch = merged_ancestor.branch
    failed_descendant.completed_at = datetime.now(UTC)
    store.update(failed_descendant)

    assert is_chain_resolved_by_recovery(store, failed_descendant) is False
    assert list_failed_tasks_for_recovery(store) == []


def test_list_failed_tasks_for_recovery_filters_fast_forward_landed_descendant_via_merged_merge_unit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _stub_merge_context(monkeypatch)

    merged_ancestor = store.add("Fast-forward landed implementation", task_type="implement")
    assert merged_ancestor.id is not None
    merged_ancestor.status = "completed"
    merged_ancestor.branch = "feature/fast-forward-landed"
    merged_ancestor.has_commits = True
    merged_ancestor.completed_at = datetime.now(UTC)
    store.update(merged_ancestor)
    unit = store.create_merge_unit(
        source_branch=merged_ancestor.branch,
        target_branch="main",
        owner_task_id=merged_ancestor.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(merged_ancestor.id, unit.id, "owner")

    failed_descendant = store.add("Failed retry", task_type="implement", based_on=merged_ancestor.id)
    assert failed_descendant.id is not None
    failed_descendant.status = "failed"
    failed_descendant.failure_reason = "WORKER_DIED"
    failed_descendant.session_id = "sess-fast-forward-descendant"
    failed_descendant.branch = merged_ancestor.branch
    failed_descendant.completed_at = datetime.now(UTC)
    store.update(failed_descendant)

    assert _is_resolved_by_landed_lineage(
        store,
        failed_descendant,
        merge_context=recovery_engine._load_merge_context(tmp_path),
    ) is True
    assert list_failed_tasks_for_recovery(store) == []


def test_list_failed_tasks_for_recovery_keeps_failed_descendant_under_merged_manual_follow_up_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _stub_merge_context(monkeypatch)

    root = _completed_impl(store, merge_status="unmerged")
    assert root.id is not None
    root.branch = "feature/root"
    store.update(root)

    manual_follow_up = store.add("Manual follow-up implement", task_type="implement", based_on=root.id)
    assert manual_follow_up.id is not None
    manual_follow_up.status = "completed"
    manual_follow_up.session_id = "sess-manual"
    manual_follow_up.branch = "feature/manual"
    manual_follow_up.merge_status = "merged"
    manual_follow_up.completed_at = datetime.now(UTC)
    store.update(manual_follow_up)

    failed_descendant = store.add(manual_follow_up.prompt, task_type="implement", based_on=manual_follow_up.id)
    assert failed_descendant.id is not None
    failed_descendant.status = "failed"
    failed_descendant.failure_reason = "MAX_TURNS"
    failed_descendant.session_id = manual_follow_up.session_id
    failed_descendant.branch = manual_follow_up.branch
    failed_descendant.completed_at = datetime.now(UTC)
    store.update(failed_descendant)

    assert is_chain_resolved_by_recovery(store, failed_descendant) is False
    assert [task.id for task in list_failed_tasks_for_recovery(store)] == [failed_descendant.id]


def test_recovery_engine_timeout_implement_with_stale_merged_metadata_still_chooses_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _stub_merge_context(monkeypatch, merged_side_branches={"feature/stale-timeout"})

    failed = store.add("Failed timeout implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "TIMEOUT"
    failed.session_id = "sess-timeout"
    failed.branch = "feature/stale-timeout"
    failed.completed_at = datetime.now(UTC)
    failed.merge_status = "merged"
    failed.has_commits = True
    store.update(failed)

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)

    assert decision.action == "resume"
    assert decision.reason_code == "TIMEOUT"
    assert [task.id for task in list_failed_tasks_for_recovery(store)] == [failed.id]


def test_list_failed_tasks_for_recovery_keeps_failed_fix_under_merged_cross_type_follow_up_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _stub_merge_context(monkeypatch)

    root = _completed_impl(store, merge_status="unmerged")
    assert root.id is not None
    root.branch = "feature/root"
    store.update(root)

    completed_fix = store.add("Completed fix", task_type="fix", based_on=root.id, same_branch=True)
    assert completed_fix.id is not None
    completed_fix.status = "completed"
    completed_fix.session_id = "sess-fix"
    completed_fix.branch = root.branch
    completed_fix.merge_status = "merged"
    completed_fix.completed_at = datetime.now(UTC)
    store.update(completed_fix)

    failed_fix = store.add(completed_fix.prompt, task_type="fix", based_on=completed_fix.id, same_branch=True)
    assert failed_fix.id is not None
    failed_fix.status = "failed"
    failed_fix.failure_reason = "MAX_TURNS"
    failed_fix.session_id = completed_fix.session_id
    failed_fix.branch = completed_fix.branch
    failed_fix.completed_at = datetime.now(UTC)
    store.update(failed_fix)

    assert is_chain_resolved_by_recovery(store, failed_fix) is False
    assert [task.id for task in list_failed_tasks_for_recovery(store)] == [failed_fix.id]


def test_list_failed_tasks_for_recovery_filters_same_branch_failed_improve_under_merged_impl(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _stub_merge_context(monkeypatch)

    impl = _completed_impl(store, merge_status="unmerged")
    assert impl.id is not None
    impl.branch = "feature/shared-branch"
    store.update(impl)

    merged_sibling = store.add("Merged sibling", task_type="implement", based_on=impl.id)
    assert merged_sibling.id is not None
    merged_sibling.status = "completed"
    merged_sibling.session_id = "sess-sibling"
    merged_sibling.branch = impl.branch
    merged_sibling.merge_status = "merged"
    merged_sibling.completed_at = datetime.now(UTC)
    store.update(merged_sibling)

    review = store.add("Review", task_type="review", depends_on=impl.id, based_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    store.update(review)

    failed_improve = store.add(
        "Failed improve",
        task_type="improve",
        depends_on=review.id,
        based_on=impl.id,
        same_branch=True,
    )
    assert failed_improve.id is not None
    failed_improve.status = "failed"
    failed_improve.failure_reason = "GIT_ERROR"
    failed_improve.branch = impl.branch
    failed_improve.completed_at = datetime.now(UTC)
    store.update(failed_improve)

    assert is_resolved_by_merged_target(store, failed_improve) is False
    merge_context = recovery_engine._load_merge_context(tmp_path)
    assert _is_resolved_by_landed_lineage(store, failed_improve, merge_context=merge_context) is True
    assert list_failed_tasks_for_recovery(store) == []


def test_is_resolved_by_landed_lineage_uses_existing_branch_set_and_branch_resolution_cache(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.branch = "feature/cached-landed"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    class _CachedBranchGit:
        def __init__(self) -> None:
            self.branch_exists_calls = 0
            self.is_merged_calls = 0
            self.count_commits_ahead_checked_calls = 0

        def resolve_fresh_merge_source(self, branch: str):
            from gza.git import ResolvedMergeSourceRef

            return ResolvedMergeSourceRef(branch)

        def rev_parse_if_exists(self, ref: str) -> str | None:
            if ref == "main":
                return "target-tip"
            return f"{ref}-tip"

        def branch_exists(self, branch: str) -> bool:
            self.branch_exists_calls += 1
            return True

        def is_merged(self, branch: str, into: str) -> bool:
            self.is_merged_calls += 1
            return True

        def count_commits_ahead_checked(self, branch: str, base: str) -> int | None:
            self.count_commits_ahead_checked_calls += 1
            return 0

        def is_on_first_parent_history(self, commit: str, target: str) -> bool:
            return False

    git = _CachedBranchGit()
    merge_context = _MergeContext(
        git=git,
        default_branch="main",
        existing_branches=frozenset({failed.branch}),
    )

    assert _is_resolved_by_landed_lineage(store, failed, merge_context=merge_context) is True
    assert _is_resolved_by_landed_lineage(store, failed, merge_context=merge_context) is True
    assert git.branch_exists_calls == 0
    assert git.is_merged_calls == 1
    assert git.count_commits_ahead_checked_calls == 1


def test_is_resolved_by_landed_lineage_does_not_treat_redundant_branch_as_landed(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.branch = "feature/merged-zero-ahead"
    failed.has_commits = True
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    git = _StubMergeGit(empty_merged_branches={failed.branch})
    merge_context = _MergeContext(
        git=git,
        default_branch="main",
        existing_branches=frozenset({failed.branch}),
    )

    assert _is_resolved_by_landed_lineage(store, failed, merge_context=merge_context) is False


def test_is_resolved_by_landed_lineage_does_not_treat_reachable_empty_branch_as_landed(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "WORKER_DIED"
    failed.branch = "feature/reachable-empty"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    git = _StubMergeGit(empty_merged_branches={failed.branch})
    merge_context = _MergeContext(
        git=git,
        default_branch="main",
        existing_branches=frozenset({failed.branch}),
    )

    assert _is_resolved_by_landed_lineage(store, failed, merge_context=merge_context) is False


def test_empty_task_with_provider_output_remains_visible_when_branch_has_no_unique_commits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    branch = "feature/recoverable-empty-provider-output"
    _stub_merge_context(monkeypatch, empty_merged_branches={branch})

    failed = store.add("Recoverable empty failed work", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "WORKER_DIED"
    failed.branch = branch
    failed.session_id = "sess-recoverable-empty-provider-output"
    failed.output_content = "provider output proves the task executed"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)
    _attach_empty_merge_unit(store, failed)

    merge_context = recovery_engine._load_merge_context(tmp_path)

    assert _is_resolved_by_landed_lineage(store, failed, merge_context=merge_context) is False
    assert empty_task_requires_recovery(store, failed, merge_state="empty", merge_context=merge_context) is True
    assert [task.id for task in list_failed_tasks_for_recovery(store)] == [failed.id]


@pytest.mark.parametrize("landed_created_first", [True, False])
def test_list_failed_tasks_for_recovery_recomputes_reachable_empty_branch_resolution_per_task(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    landed_created_first: bool,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    branch = "feature/reachable-empty-shared"
    _stub_merge_context(monkeypatch, empty_merged_branches={branch})

    def _landed_failed_task():
        task = store.add("Failed landed work", task_type="implement")
        assert task.id is not None
        task.status = "failed"
        task.failure_reason = "INFRASTRUCTURE_ERROR"
        task.branch = branch
        task.has_commits = True
        task.completed_at = datetime.now(UTC)
        store.update(task)
        return task

    def _recoverable_empty_task():
        task = store.add("Recoverable empty failed work", task_type="implement")
        assert task.id is not None
        task.status = "failed"
        task.failure_reason = "WORKER_DIED"
        task.branch = branch
        task.session_id = "sess-recoverable-empty"
        task.num_steps_computed = 1
        task.completed_at = datetime.now(UTC)
        store.update(task)
        return task

    if landed_created_first:
        landed = _landed_failed_task()
        recoverable_empty = _recoverable_empty_task()
    else:
        recoverable_empty = _recoverable_empty_task()
        landed = _landed_failed_task()

    failed = list_failed_tasks_for_recovery(store)

    assert [task.id for task in failed] == [recoverable_empty.id]
    assert landed not in failed


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


def test_classify_failure_reason_branch_unpushable_is_reconcile() -> None:
    assert classify_failure_reason("BRANCH_UNPUSHABLE") == "reconcile"


def test_classify_failure_reason_pr_required_is_reconcile_compatibility() -> None:
    assert classify_failure_reason("PR_REQUIRED") == "reconcile"


def test_recovery_engine_branch_unpushable_chooses_reconcile(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, reason="BRANCH_UNPUSHABLE", session_id=None)
    task.branch = "feature/branch-unpushable"
    store.update(task)
    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)
    assert decision.action == "reconcile"
    assert decision.launch_mode == "none"
    assert decision.reason_code == "BRANCH_UNPUSHABLE"


def test_recovery_engine_branch_unpushable_direct_reconcile_attempt_reaches_retry_limit(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, reason="BRANCH_UNPUSHABLE", session_id=None)
    task.branch = "feature/branch-unpushable"
    store.update(task)
    persist_branch_publication_state(
        store=store,
        task=task,
        config=Config.load(tmp_path),
        state=BranchPublicationState(reconcile_attempts_consumed=1),
        status="BRANCH_UNPUSHABLE",
        exit_status="reconcile_retry_failed",
    )

    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)

    assert decision.action == "skip"
    assert decision.reason_code == "retry_limit_reached"
    assert (decision.attempt_index, decision.attempt_limit) == (2, 2)


def test_recovery_engine_legacy_pr_required_chooses_reconcile(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, reason="PR_REQUIRED", session_id=None)
    task.branch = "feature/legacy-pr-required"
    store.update(task)
    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)
    assert decision.action == "reconcile"
    assert decision.launch_mode == "none"
    assert decision.reason_code == "PR_REQUIRED"


def test_recovery_engine_branchless_branch_unpushable_parks_for_manual_repair(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, reason="BRANCH_UNPUSHABLE", session_id=None)

    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)

    assert decision.action == "skip"
    assert decision.reason_code == "reconcile_branch_missing"
    assert "no branch to reconcile" in decision.reason_text
    assert (
        get_failed_recovery_needs_attention_reason(
            store,
            task,
            decision=decision,
            max_recovery_attempts=1,
        )
        == "branch-publication-needs-manual-repair"
    )


def test_recovery_engine_branchless_pr_required_parks_for_manual_repair(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, reason="PR_REQUIRED", session_id=None)

    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)

    assert decision.action == "skip"
    assert decision.reason_code == "reconcile_branch_missing"
    assert "no branch to reconcile" in decision.reason_text
    assert (
        get_failed_recovery_needs_attention_reason(
            store,
            task,
            decision=decision,
            max_recovery_attempts=1,
        )
        == "branch-publication-needs-manual-repair"
    )


def test_classify_failure_reason_config_error_is_manual() -> None:
    assert classify_failure_reason("CONFIG_ERROR") == "manual"


def test_recovery_engine_config_error_skips(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, reason="CONFIG_ERROR")
    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "manual_failure_reason"


def test_recovery_engine_provider_empty_turn_is_retryable(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, task_type="plan", reason="PROVIDER_EMPTY_TURN", session_id=None)

    assert classify_failure_reason("PROVIDER_EMPTY_TURN") == "retryable"

    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)
    assert decision.action == "retry"
    assert decision.launch_mode == "worker"
    assert decision.reason_code == "PROVIDER_EMPTY_TURN"


def test_recovery_engine_retryable_provider_error_chooses_fresh_retry_even_with_session(tmp_path: Path) -> None:
    store, task = _failed_task(
        tmp_path,
        task_type="plan",
        reason="RETRYABLE_PROVIDER_ERROR",
        session_id="thread_codex_123",
    )

    assert classify_failure_reason("RETRYABLE_PROVIDER_ERROR") == "retryable"

    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)
    assert decision.action == "retry"
    assert decision.launch_mode == "worker"
    assert decision.reason_code == "RETRYABLE_PROVIDER_ERROR"


def test_recovery_engine_retryable_provider_error_parks_after_one_retry(tmp_path: Path) -> None:
    store, root = _failed_task(
        tmp_path,
        task_type="plan",
        reason="RETRYABLE_PROVIDER_ERROR",
        session_id="thread_codex_root",
    )
    retry_child = store.add(
        root.prompt,
        task_type=root.task_type,
        based_on=root.id,
        depends_on=root.depends_on,
        recovery_origin="retry",
    )
    assert retry_child.id is not None
    retry_child.status = "failed"
    retry_child.failure_reason = "RETRYABLE_PROVIDER_ERROR"
    retry_child.session_id = "thread_codex_retry"
    retry_child.completed_at = datetime.now(UTC)
    store.update(retry_child)

    decision = decide_failed_task_recovery(store, retry_child, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "retryable_provider_error"
    assert decision.reason_text == "fresh retry already consumed; retryable provider error now requires manual review"
    assert get_failed_recovery_needs_attention_reason(
        store,
        retry_child,
        decision=decision,
        max_recovery_attempts=1,
    ) == "retryable-provider-error"


def test_recovery_engine_timeout_without_session_requires_manual_review(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, reason="MAX_STEPS", session_id=None)
    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "retry_limit_reached"
    assert get_failed_recovery_needs_attention_reason(store, task, decision=decision, max_recovery_attempts=1) == (
        "retry-limit-reached"
    )


def test_get_failed_recovery_needs_attention_reason_keeps_manual_review_required_alias(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, reason="MAX_STEPS", session_id=None)
    decision = FailedRecoveryDecision(
        task_id=task.id,
        action="skip",
        reason_code="manual_review_required",
        reason_text="legacy manual review required",
        launch_mode="none",
        attempt_index=2,
        attempt_limit=2,
    )

    assert get_failed_recovery_needs_attention_reason(store, task, decision=decision, max_recovery_attempts=1) == (
        "retry-limit-reached"
    )


def test_recovery_engine_manual_reason_skips(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, reason="TEST_FAILURE")
    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "manual_failure_reason"


def test_recovery_engine_provider_empty_turn_stops_at_retry_limit(tmp_path: Path) -> None:
    store, root = _failed_task(tmp_path, reason="PROVIDER_EMPTY_TURN", session_id=None)
    retry_child = store.add(root.prompt, task_type=root.task_type, based_on=root.id, depends_on=root.depends_on)
    assert retry_child.id is not None
    retry_child.status = "failed"
    retry_child.failure_reason = "PROVIDER_EMPTY_TURN"
    retry_child.completed_at = datetime.now(UTC)
    store.update(retry_child)

    decision = decide_failed_task_recovery(store, retry_child, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "retry_limit_reached"
    assert (decision.attempt_index, decision.attempt_limit) == (2, 2)


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
    assert decision.recovery_task_id == running_sibling.id
    assert decision.reuse_existing is False


def test_recovery_engine_single_pending_nonmatching_child_still_reports_recovery_task_id(tmp_path: Path) -> None:
    store, task = _failed_task(tmp_path, reason="MAX_TURNS", session_id="sess-pending-skip")

    pending_retry = store.add(
        task.prompt,
        task_type=task.task_type,
        based_on=task.id,
        depends_on=task.depends_on,
        recovery_origin="retry",
    )
    assert pending_retry.id is not None
    pending_retry.status = "pending"
    pending_retry.session_id = task.session_id
    store.update(pending_retry)

    decision = decide_failed_task_recovery(store, task, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "recovery_already_pending"
    assert decision.recovery_task_id == pending_retry.id
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
    assert decision.recovery_task_id == completed_sibling.id
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


def test_recovery_engine_marks_explicit_forked_resume_chain_as_resolved(tmp_path: Path) -> None:
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

    failed_resume = store.add(root.prompt, task_type=root.task_type, based_on=root.id, recovery_origin="resume")
    assert failed_resume.id is not None
    failed_resume.status = "failed"
    failed_resume.failure_reason = "MAX_TURNS"
    failed_resume.session_id = "sess-forked"
    failed_resume.branch = "feature/root-2"
    failed_resume.completed_at = datetime.now(UTC)
    store.update(failed_resume)

    completed_resume = store.add(
        failed_resume.prompt,
        task_type=failed_resume.task_type,
        based_on=failed_resume.id,
        recovery_origin="resume",
    )
    assert completed_resume.id is not None
    completed_resume.status = "completed"
    completed_resume.session_id = failed_resume.session_id
    completed_resume.branch = failed_resume.branch
    completed_resume.completed_at = datetime.now(UTC)
    store.update(completed_resume)

    assert is_chain_resolved_by_recovery(store, root) is True
    assert get_completed_recovery_descendant(store, root).id == completed_resume.id
    assert [task.id for task in list_failed_tasks_for_recovery(store)] == []


def test_get_completed_sibling_recovery_returns_completed_resume_sibling(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    root = store.add("Failed root", task_type="plan")
    assert root.id is not None
    root.status = "failed"
    root.failure_reason = "NO_ACTIVITY"
    root.session_id = "sess-root"
    root.branch = "feature/root"
    root.completed_at = datetime.now(UTC)
    store.update(root)

    failed_resume = store.add(root.prompt, task_type="plan", based_on=root.id, recovery_origin="resume")
    assert failed_resume.id is not None
    failed_resume.status = "failed"
    failed_resume.failure_reason = "INFRASTRUCTURE_ERROR"
    failed_resume.session_id = root.session_id
    failed_resume.branch = root.branch
    failed_resume.completed_at = datetime.now(UTC)
    store.update(failed_resume)

    completed_resume = store.add(root.prompt, task_type="plan", based_on=root.id, recovery_origin="resume")
    assert completed_resume.id is not None
    completed_resume.status = "completed"
    completed_resume.session_id = root.session_id
    completed_resume.branch = root.branch
    completed_resume.completed_at = datetime.now(UTC)
    store.update(completed_resume)

    assert get_completed_sibling_recovery(store, failed_resume).id == completed_resume.id


def test_get_completed_sibling_recovery_returns_completed_descendant_of_failed_sibling(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    root = store.add("Failed root", task_type="plan")
    assert root.id is not None
    root.status = "failed"
    root.failure_reason = "NO_ACTIVITY"
    root.session_id = "sess-root"
    root.branch = "feature/root"
    root.completed_at = datetime.now(UTC)
    store.update(root)

    failed_resume = store.add(root.prompt, task_type="plan", based_on=root.id, recovery_origin="resume")
    assert failed_resume.id is not None
    failed_resume.status = "failed"
    failed_resume.failure_reason = "INFRASTRUCTURE_ERROR"
    failed_resume.session_id = root.session_id
    failed_resume.branch = root.branch
    failed_resume.completed_at = datetime.now(UTC)
    store.update(failed_resume)

    sibling_resume = store.add(root.prompt, task_type="plan", based_on=root.id, recovery_origin="resume")
    assert sibling_resume.id is not None
    sibling_resume.status = "failed"
    sibling_resume.failure_reason = "INFRASTRUCTURE_ERROR"
    sibling_resume.session_id = root.session_id
    sibling_resume.branch = root.branch
    sibling_resume.completed_at = datetime.now(UTC)
    store.update(sibling_resume)

    completed_grandchild = store.add(
        sibling_resume.prompt,
        task_type="plan",
        based_on=sibling_resume.id,
        recovery_origin="resume",
    )
    assert completed_grandchild.id is not None
    completed_grandchild.status = "completed"
    completed_grandchild.session_id = sibling_resume.session_id
    completed_grandchild.branch = sibling_resume.branch
    completed_grandchild.completed_at = datetime.now(UTC)
    store.update(completed_grandchild)

    assert get_completed_sibling_recovery(store, failed_resume).id == completed_grandchild.id


def test_recovery_snapshot_helpers_match_indexed_context_for_descendants_and_siblings(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    root = store.add("Failed root", task_type="plan")
    assert root.id is not None
    root.status = "failed"
    root.failure_reason = "NO_ACTIVITY"
    root.session_id = "sess-root"
    root.branch = "feature/root"
    root.completed_at = datetime.now(UTC)
    store.update(root)

    failed_resume = store.add(root.prompt, task_type="plan", based_on=root.id, recovery_origin="resume")
    assert failed_resume.id is not None
    failed_resume.status = "failed"
    failed_resume.failure_reason = "INFRASTRUCTURE_ERROR"
    failed_resume.session_id = root.session_id
    failed_resume.branch = root.branch
    failed_resume.completed_at = datetime.now(UTC)
    store.update(failed_resume)

    sibling_resume = store.add(root.prompt, task_type="plan", based_on=root.id, recovery_origin="resume")
    assert sibling_resume.id is not None
    sibling_resume.status = "failed"
    sibling_resume.failure_reason = "INFRASTRUCTURE_ERROR"
    sibling_resume.session_id = root.session_id
    sibling_resume.branch = root.branch
    sibling_resume.completed_at = datetime.now(UTC)
    store.update(sibling_resume)

    completed_grandchild = store.add(
        sibling_resume.prompt,
        task_type="plan",
        based_on=sibling_resume.id,
        recovery_origin="resume",
    )
    assert completed_grandchild.id is not None
    completed_grandchild.status = "completed"
    completed_grandchild.session_id = sibling_resume.session_id
    completed_grandchild.branch = sibling_resume.branch
    completed_grandchild.completed_at = datetime.now(UTC)
    store.update(completed_grandchild)

    read_context = _read_context_for_store(store)
    assert get_recovery_chain_state(store, root) == get_recovery_chain_state(store, root, read_context=read_context)
    assert get_completed_recovery_descendant(store, root, read_context=read_context) is None
    assert get_completed_sibling_recovery(store, failed_resume) == get_completed_sibling_recovery(
        store,
        failed_resume,
        read_context=read_context,
    )


def test_completed_recovery_descendant_requires_merged_code_outcome(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-root"
    failed.branch = "feature/root"
    failed.has_commits = True
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    completed_retry = store.add(
        failed.prompt,
        task_type="implement",
        based_on=failed.id,
        recovery_origin="resume",
    )
    assert completed_retry.id is not None
    completed_retry.status = "completed"
    completed_retry.session_id = failed.session_id
    completed_retry.branch = failed.branch
    completed_retry.has_commits = True
    completed_retry.merge_status = "unmerged"
    completed_retry.completed_at = datetime.now(UTC)
    store.update(completed_retry)

    assert is_chain_resolved_by_recovery(store, failed) is False
    assert get_completed_recovery_descendant(store, failed) is None
    assert resolve_recovery_planning_task(store, failed).id == completed_retry.id

    completed_retry.merge_status = "merged"
    store.update(completed_retry)

    assert is_chain_resolved_by_recovery(store, failed) is True
    assert get_completed_recovery_descendant(store, failed).id == completed_retry.id


def test_get_completed_sibling_recovery_ignores_unresolved_sibling_chain(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    root = store.add("Failed root", task_type="plan")
    assert root.id is not None
    root.status = "failed"
    root.failure_reason = "NO_ACTIVITY"
    root.session_id = "sess-root"
    root.branch = "feature/root"
    root.completed_at = datetime.now(UTC)
    store.update(root)

    failed_resume = store.add(root.prompt, task_type="plan", based_on=root.id, recovery_origin="resume")
    assert failed_resume.id is not None
    failed_resume.status = "failed"
    failed_resume.failure_reason = "INFRASTRUCTURE_ERROR"
    failed_resume.session_id = root.session_id
    failed_resume.branch = root.branch
    failed_resume.completed_at = datetime.now(UTC)
    store.update(failed_resume)

    sibling_resume = store.add(root.prompt, task_type="plan", based_on=root.id, recovery_origin="resume")
    assert sibling_resume.id is not None
    sibling_resume.status = "failed"
    sibling_resume.failure_reason = "INFRASTRUCTURE_ERROR"
    sibling_resume.session_id = root.session_id
    sibling_resume.branch = root.branch
    sibling_resume.completed_at = datetime.now(UTC)
    store.update(sibling_resume)

    assert get_completed_sibling_recovery(store, failed_resume) is None


def test_get_completed_sibling_recovery_requires_merged_code_outcome(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    root = store.add("Failed root", task_type="implement")
    assert root.id is not None
    root.status = "failed"
    root.failure_reason = "NO_ACTIVITY"
    root.session_id = "sess-root"
    root.branch = "feature/root"
    root.has_commits = True
    root.completed_at = datetime.now(UTC)
    store.update(root)

    failed_resume = store.add(root.prompt, task_type="implement", based_on=root.id, recovery_origin="resume")
    assert failed_resume.id is not None
    failed_resume.status = "failed"
    failed_resume.failure_reason = "INFRASTRUCTURE_ERROR"
    failed_resume.session_id = root.session_id
    failed_resume.branch = root.branch
    failed_resume.has_commits = True
    failed_resume.completed_at = datetime.now(UTC)
    store.update(failed_resume)

    completed_resume = store.add(root.prompt, task_type="implement", based_on=root.id, recovery_origin="resume")
    assert completed_resume.id is not None
    completed_resume.status = "completed"
    completed_resume.session_id = root.session_id
    completed_resume.branch = root.branch
    completed_resume.has_commits = True
    completed_resume.merge_status = "unmerged"
    completed_resume.completed_at = datetime.now(UTC)
    store.update(completed_resume)

    assert get_completed_sibling_recovery(store, failed_resume) is None

    completed_resume.merge_status = "merged"
    store.update(completed_resume)

    assert get_completed_sibling_recovery(store, failed_resume).id == completed_resume.id


def test_get_completed_sibling_recovery_ignores_manual_sibling(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    root = store.add("Failed root", task_type="plan")
    assert root.id is not None
    root.status = "failed"
    root.failure_reason = "NO_ACTIVITY"
    root.session_id = "sess-root"
    root.branch = "feature/root"
    root.completed_at = datetime.now(UTC)
    store.update(root)

    failed_resume = store.add(root.prompt, task_type="plan", based_on=root.id, recovery_origin="resume")
    assert failed_resume.id is not None
    failed_resume.status = "failed"
    failed_resume.failure_reason = "INFRASTRUCTURE_ERROR"
    failed_resume.session_id = root.session_id
    failed_resume.branch = root.branch
    failed_resume.completed_at = datetime.now(UTC)
    store.update(failed_resume)

    manual_follow_up = store.add(root.prompt, task_type="plan", based_on=root.id, recovery_origin="manual")
    assert manual_follow_up.id is not None
    manual_follow_up.status = "completed"
    manual_follow_up.session_id = "sess-manual"
    manual_follow_up.branch = "feature/manual"
    manual_follow_up.completed_at = datetime.now(UTC)
    store.update(manual_follow_up)

    assert get_completed_sibling_recovery(store, failed_resume) is None


def test_get_completed_sibling_recovery_ignores_cross_type_sibling(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    root = store.add("Failed root", task_type="plan")
    assert root.id is not None
    root.status = "failed"
    root.failure_reason = "NO_ACTIVITY"
    root.session_id = "sess-root"
    root.branch = "feature/root"
    root.completed_at = datetime.now(UTC)
    store.update(root)

    failed_resume = store.add(root.prompt, task_type="plan", based_on=root.id, recovery_origin="resume")
    assert failed_resume.id is not None
    failed_resume.status = "failed"
    failed_resume.failure_reason = "INFRASTRUCTURE_ERROR"
    failed_resume.session_id = root.session_id
    failed_resume.branch = root.branch
    failed_resume.completed_at = datetime.now(UTC)
    store.update(failed_resume)

    completed_implement = store.add("Completed implement", task_type="implement", based_on=root.id)
    assert completed_implement.id is not None
    completed_implement.status = "completed"
    completed_implement.completed_at = datetime.now(UTC)
    store.update(completed_implement)

    assert get_completed_sibling_recovery(store, failed_resume) is None


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


def test_recovery_engine_pending_match_reuses_retry_child_after_one_consumed_same_action_attempt(
    tmp_path: Path,
) -> None:
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
    assert decision.action == "retry"
    assert decision.reason_code == "INFRASTRUCTURE_ERROR"
    assert decision.reason_text == f"reusing pending retry child {reusable_pending.id}"
    assert decision.recovery_task_id == reusable_pending.id
    assert decision.reuse_existing is True
    assert (decision.attempt_index, decision.attempt_limit) == (2, 2)


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
    assert decision.recovery_task_id == grandchild.id
    assert decision.reuse_existing is False


def test_recovery_engine_single_same_action_failed_recovery_descendant_gets_final_bounded_attempt(
    tmp_path: Path,
) -> None:
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
    assert root_decision.action == "retry"
    assert root_decision.reason_code == "INFRASTRUCTURE_ERROR"
    assert root_decision.reason_text == "INFRASTRUCTURE_ERROR restart with fresh attempt"
    assert (root_decision.attempt_index, root_decision.attempt_limit) == (2, 2)
    assert root_decision.recovery_task_id is None
    assert root_decision.reuse_existing is False

    child_decision = decide_failed_task_recovery(store, retry_child, max_recovery_attempts=3)
    assert child_decision.action == "skip"
    assert child_decision.reason_code == "retry_limit_reached"


def test_recovery_engine_two_same_action_failed_recovery_descendants_exhaust_budget(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    root = store.add("Failed root", task_type="plan")
    assert root.id is not None
    root.status = "failed"
    root.failure_reason = "INFRASTRUCTURE_ERROR"
    root.completed_at = datetime.now(UTC)
    store.update(root)

    first_retry_child = store.add(root.prompt, task_type="plan", based_on=root.id, depends_on=root.depends_on)
    assert first_retry_child.id is not None
    first_retry_child.status = "failed"
    first_retry_child.failure_reason = "INFRASTRUCTURE_ERROR"
    first_retry_child.completed_at = datetime.now(UTC)
    store.update(first_retry_child)

    second_retry_child = store.add(root.prompt, task_type="plan", based_on=root.id, depends_on=root.depends_on)
    assert second_retry_child.id is not None
    second_retry_child.status = "failed"
    second_retry_child.failure_reason = "INFRASTRUCTURE_ERROR"
    second_retry_child.completed_at = datetime.now(UTC)
    store.update(second_retry_child)

    root_decision = decide_failed_task_recovery(store, root, max_recovery_attempts=3)
    assert root_decision.action == "skip"
    assert root_decision.reason_code == "retry_limit_reached"
    assert (root_decision.attempt_index, root_decision.attempt_limit) == (2, 2)


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
    assert child_decision.reason_code == "retry_limit_reached"
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


def test_recovery_engine_prerequisite_unmerged_reconciles_no_output_row_to_empty_after_dependency_merge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    failed.branch = "feature/prereq-empty"
    failed.has_commits = False
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    monkeypatch.setattr(
        recovery_engine,
        "_load_merge_context",
        lambda _project_dir=None: _MergeContext(git=_StubEmptyBranchGit(), default_branch="main"),
    )

    blocked_decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert blocked_decision.action == "skip"
    assert blocked_decision.reason_code == "dependency_not_ready"

    store.set_merge_status(dependency.id, "merged")

    ready_decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert ready_decision.action == "skip"
    assert ready_decision.reason_code == "merge_unit_empty"

    unit = store.resolve_merge_unit_for_task(failed.id)
    assert unit is not None
    assert unit.state == "empty"


def test_recovery_engine_prerequisite_unmerged_branchless_no_output_row_is_moot_after_dependency_merge(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    dependency = store.add("Dependency", task_type="implement")
    assert dependency.id is not None
    dependency.status = "completed"
    dependency.merge_status = "merged"
    dependency.completed_at = datetime.now(UTC)
    store.update(dependency)

    failed = store.add("Failed downstream", task_type="implement", depends_on=dependency.id)
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PREREQUISITE_UNMERGED"
    failed.has_commits = False
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "merge_unit_empty"
    assert decision.reason_text == "moot (no unique commits vs target)"
    assert store.resolve_merge_unit_for_task(failed.id) is None
    assert list_failed_tasks_for_recovery(store) == []


def test_recovery_engine_prerequisite_unmerged_stored_redundant_row_is_moot_after_dependency_merge(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    dependency = store.add("Dependency", task_type="implement")
    assert dependency.id is not None
    dependency.status = "completed"
    dependency.merge_status = "merged"
    dependency.completed_at = datetime.now(UTC)
    store.update(dependency)

    failed = store.add("Failed downstream", task_type="implement", depends_on=dependency.id)
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PREREQUISITE_UNMERGED"
    failed.branch = "feature/prereq-redundant"
    failed.has_commits = True
    failed.completed_at = datetime.now(UTC)
    store.update(failed)
    _attach_redundant_merge_unit(store, failed)

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "merge_unit_redundant"
    assert decision.reason_text == MOOT_REDUNDANT_LIFECYCLE_DETAIL
    assert decision.reason_text != MOOT_EMPTY_LIFECYCLE_DETAIL
    assert list_failed_tasks_for_recovery(store) == []

    unit = store.resolve_merge_unit_for_task(failed.id)
    assert unit is not None
    assert unit.state == "redundant"


def test_recovery_engine_legacy_empty_with_task_commits_uses_redundant_reason_text(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Legacy empty row with task commits", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.branch = "feature/legacy-empty-redundant"
    failed.has_commits = True
    failed.session_id = "sess-legacy-empty"
    failed.num_steps_computed = 0
    failed.num_steps_reported = 0
    failed.output_tokens = 0
    failed.completed_at = datetime.now(UTC)
    store.update(failed)
    _attach_empty_merge_unit(store, failed)

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "merge_unit_redundant"
    assert decision.reason_text == MOOT_REDUNDANT_LIFECYCLE_DETAIL


def test_recovery_engine_prerequisite_unmerged_legacy_empty_with_task_commits_reconciles_to_moot_redundant(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    dependency = store.add("Dependency", task_type="implement")
    assert dependency.id is not None
    dependency.status = "completed"
    dependency.merge_status = "merged"
    dependency.completed_at = datetime.now(UTC)
    store.update(dependency)

    failed = store.add("Legacy prereq empty row with task commits", task_type="implement", depends_on=dependency.id)
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PREREQUISITE_UNMERGED"
    failed.branch = "feature/prereq-legacy-empty-redundant"
    failed.has_commits = True
    failed.session_id = "sess-prereq-legacy-empty"
    failed.num_steps_computed = 0
    failed.num_steps_reported = 0
    failed.output_tokens = 0
    failed.completed_at = datetime.now(UTC)
    store.update(failed)
    _attach_empty_merge_unit(store, failed)

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "merge_unit_redundant"
    assert decision.reason_text == MOOT_REDUNDANT_LIFECYCLE_DETAIL

    unit = store.resolve_merge_unit_for_task(failed.id)
    assert unit is not None
    assert unit.state == "empty"


def test_recovery_engine_prerequisite_unmerged_stays_dependency_not_ready_when_dependency_is_empty(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    dependency = store.add("Dependency", task_type="implement")
    assert dependency.id is not None
    dependency.status = "completed"
    dependency.branch = "feature/dependency-empty"
    dependency.has_commits = True
    dependency.completed_at = datetime.now(UTC)
    store.update(dependency)
    unit = store.create_merge_unit(
        source_branch=dependency.branch,
        target_branch="main",
        owner_task_id=dependency.id,
        state="empty",
    )
    store.attach_task_to_merge_unit(dependency.id, unit.id, "owner")

    failed = store.add("Failed downstream", task_type="implement", depends_on=dependency.id)
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PREREQUISITE_UNMERGED"
    failed.branch = "feature/blocked-downstream"
    failed.has_commits = False
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "dependency_not_ready"
    assert decision.reason_text == "dependency precondition not satisfied"
    assert list_failed_tasks_for_recovery(store) == []


def test_recovery_engine_prerequisite_unmerged_empty_session_backed_failure_stays_visible_after_dependency_merge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    dependency = store.add("Dependency", task_type="implement")
    assert dependency.id is not None
    dependency.status = "completed"
    dependency.merge_status = "merged"
    dependency.completed_at = datetime.now(UTC)
    store.update(dependency)

    failed = store.add("Failed downstream", task_type="implement", depends_on=dependency.id)
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PREREQUISITE_UNMERGED"
    failed.session_id = "sess-prereq-empty"
    failed.branch = "feature/prereq-empty"
    failed.has_commits = False
    failed.num_steps_computed = 2
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    monkeypatch.setattr(
        recovery_engine,
        "_load_merge_context",
        lambda _project_dir=None: _MergeContext(git=_StubEmptyBranchGit(), default_branch="main"),
    )

    assert empty_task_requires_recovery(store, failed, merge_state="empty") is True

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "legacy_prerequisite_unmerged_parked"
    assert decision.reason_code != "merge_unit_empty"
    assert [task.id for task in list_failed_tasks_for_recovery(store)] == [failed.id]

    unit = store.resolve_merge_unit_for_task(failed.id)
    assert unit is not None
    assert unit.state == "empty"


def test_recovery_engine_prerequisite_unmerged_live_redundant_branch_persists_redundant_after_dependency_merge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    dependency = store.add("Dependency", task_type="implement")
    assert dependency.id is not None
    dependency.status = "completed"
    dependency.merge_status = "merged"
    dependency.completed_at = datetime.now(UTC)
    store.update(dependency)

    failed = store.add("Failed downstream", task_type="implement", depends_on=dependency.id)
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PREREQUISITE_UNMERGED"
    failed.branch = "feature/prereq-redundant"
    failed.has_commits = True
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    _stub_merge_context(monkeypatch, empty_merged_branches={"feature/prereq-redundant"})

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "merge_unit_redundant"
    assert decision.reason_text == MOOT_REDUNDANT_LIFECYCLE_DETAIL
    assert decision.reason_text != MOOT_EMPTY_LIFECYCLE_DETAIL

    unit = store.resolve_merge_unit_for_task(failed.id)
    assert unit is not None
    assert unit.state == "redundant"


def test_recovery_engine_prerequisite_unmerged_with_commits_retries_after_dependency_merge(
    tmp_path: Path,
) -> None:
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
    failed.branch = "feature/prereq-empty-retry"
    failed.has_commits = True
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    store.set_merge_status(dependency.id, "merged")

    ready_decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert ready_decision.action == "retry"
    assert ready_decision.reason_code == "PREREQUISITE_UNMERGED"
    assert ready_decision.reason_text == "dependency merge prerequisite now satisfied"
    assert [task.id for task in list_failed_tasks_for_recovery(store)] == [failed.id]


def test_recovery_engine_prerequisite_unmerged_with_live_non_empty_branch_retries_after_dependency_merge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    dependency = store.add("Dependency", task_type="implement")
    assert dependency.id is not None
    dependency.status = "completed"
    dependency.merge_status = "merged"
    dependency.completed_at = datetime.now(UTC)
    store.update(dependency)

    failed = store.add("Failed downstream", task_type="implement", depends_on=dependency.id)
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PREREQUISITE_UNMERGED"
    failed.branch = "feature/prereq-live-work"
    failed.has_commits = False
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    monkeypatch.setattr(
        recovery_engine,
        "_load_merge_context",
        lambda _project_dir=None: _MergeContext(git=_StubNonEmptyBranchGit(), default_branch="main"),
    )

    ready_decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)

    assert ready_decision.action == "retry"
    assert ready_decision.reason_code == "PREREQUISITE_UNMERGED"
    assert ready_decision.reason_text == "dependency merge prerequisite now satisfied"
    assert [task.id for task in list_failed_tasks_for_recovery(store)] == [failed.id]
    unit = store.resolve_merge_unit_for_task(failed.id)
    assert unit is None or unit.state != "empty"


def test_recovery_engine_prerequisite_unmerged_redundant_session_backed_failure_stays_visible_after_dependency_merge(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    dependency = store.add("Dependency", task_type="implement")
    assert dependency.id is not None
    dependency.status = "completed"
    dependency.merge_status = "merged"
    dependency.completed_at = datetime.now(UTC)
    store.update(dependency)

    failed = store.add("Failed downstream", task_type="implement", depends_on=dependency.id)
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PREREQUISITE_UNMERGED"
    failed.session_id = "sess-prereq-redundant"
    failed.branch = "feature/prereq-redundant"
    failed.has_commits = True
    failed.num_steps_computed = 1
    failed.completed_at = datetime.now(UTC)
    store.update(failed)
    _attach_redundant_merge_unit(store, failed)

    assert empty_task_requires_recovery(store, failed, merge_state="redundant") is True

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "legacy_prerequisite_unmerged_parked"
    assert "redundant merge unit is recoverable" in decision.reason_text
    assert decision.reason_code != "merge_unit_redundant"
    assert [task.id for task in list_failed_tasks_for_recovery(store)] == [failed.id]

    unit = store.resolve_merge_unit_for_task(failed.id)
    assert unit is not None
    assert unit.state == "redundant"


def test_recovery_engine_prerequisite_unmerged_with_provider_output_and_session_resumes_after_dependency_merge(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    dependency = store.add("Dependency", task_type="implement")
    assert dependency.id is not None
    dependency.status = "completed"
    dependency.merge_status = "merged"
    dependency.completed_at = datetime.now(UTC)
    store.update(dependency)

    failed = store.add("Failed downstream", task_type="implement", depends_on=dependency.id)
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PREREQUISITE_UNMERGED"
    failed.session_id = "sess-prereq-real-work"
    failed.output_content = "provider produced output before the legacy failure was recorded"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    ready_decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert ready_decision.action == "resume"
    assert ready_decision.reason_code == "PREREQUISITE_UNMERGED"
    assert ready_decision.reason_text == "dependency merge prerequisite now satisfied"
    assert [task.id for task in list_failed_tasks_for_recovery(store)] == [failed.id]


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
    assert decision.reason_code == "retry_limit_reached"


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
    assert decision.reason_code == "retry_limit_reached"


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
    assert decision.reason_code == "retry_limit_reached"


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
    assert decision.reason_code == "retry_limit_reached"
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
    assert decision.reason_code == "recovery_ambiguous"
    assert decision.reason_text == "multiple pending recovery children require manual review"
    assert decision.recovery_task_id is None
    assert get_failed_recovery_needs_attention_reason(store, task, decision=decision, max_recovery_attempts=1) == (
        "recovery-ambiguous"
    )


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
    assert [task.id for task in list_failed_tasks_for_recovery(store)] == [failed.id]


def test_recovery_engine_explicit_retry_provenance_is_authoritative_for_resolution(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    root = store.add("Failed implement", task_type="implement")
    assert root.id is not None
    root.status = "failed"
    root.failure_reason = "INFRASTRUCTURE_ERROR"
    root.session_id = "sess-root"
    root.branch = "feature/root"
    root.completed_at = datetime.now(UTC)
    store.update(root)

    failed_retry = store.add(root.prompt, task_type="implement", based_on=root.id, recovery_origin="retry")
    assert failed_retry.id is not None
    failed_retry.status = "failed"
    failed_retry.failure_reason = "MAX_TURNS"
    failed_retry.session_id = "sess-forked"
    failed_retry.branch = "feature/forked"
    failed_retry.completed_at = datetime.now(UTC)
    store.update(failed_retry)

    completed_retry = store.add(
        failed_retry.prompt,
        task_type="implement",
        based_on=failed_retry.id,
        recovery_origin="retry",
    )
    assert completed_retry.id is not None
    completed_retry.status = "completed"
    completed_retry.session_id = "sess-forked-2"
    completed_retry.branch = "feature/forked-2"
    completed_retry.completed_at = datetime.now(UTC)
    store.update(completed_retry)

    root_chain = get_recovery_chain_state(store, root)
    assert root_chain.role == "original"
    assert root_chain.steps == ()
    assert root_chain.root_task_id == root.id
    assert root_chain.resolved_task_id == completed_retry.id
    assert is_chain_resolved_by_recovery(store, root) is True
    assert get_completed_recovery_descendant(store, root).id == completed_retry.id
    assert get_recovery_chain_root_task_id(store, completed_retry) == root.id
    assert [task.id for task in list_failed_tasks_for_recovery(store)] == []


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


def test_recovery_engine_reuses_same_pending_child_with_indexed_context(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "RETRYABLE_PROVIDER_ERROR"
    failed.completed_at = datetime(2026, 5, 16, 8, 0, tzinfo=UTC)
    store.update(failed)

    pending_retry = store.add(failed.prompt, task_type="implement", based_on=failed.id, recovery_origin="retry")
    assert pending_retry.id is not None
    pending_retry.status = "pending"
    store.update(pending_retry)

    manual_follow_up = store.add(failed.prompt, task_type="implement", based_on=failed.id, recovery_origin="manual")
    assert manual_follow_up.id is not None
    manual_follow_up.status = "pending"
    store.update(manual_follow_up)

    read_context = _read_context_for_store(store)
    store_decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    indexed_decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1, read_context=read_context)

    assert store_decision.reuse_existing is True
    assert indexed_decision.reuse_existing is True
    assert store_decision.recovery_task_id == pending_retry.id
    assert indexed_decision.recovery_task_id == store_decision.recovery_task_id


def test_recovery_snapshot_descendant_order_matches_indexed_context(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    root = store.add("Failed plan root", task_type="plan")
    assert root.id is not None
    root.status = "failed"
    root.failure_reason = "INFRASTRUCTURE_ERROR"
    root.completed_at = datetime(2026, 5, 16, 8, 30, tzinfo=UTC)
    store.update(root)

    older_child = store.add(root.prompt, task_type="plan", based_on=root.id, recovery_origin="retry")
    assert older_child.id is not None
    older_child.status = "pending"
    store.update(older_child)

    newer_child = store.add(root.prompt, task_type="plan", based_on=root.id, recovery_origin="retry")
    assert newer_child.id is not None
    newer_child.status = "pending"
    store.update(newer_child)

    grandchild = store.add(older_child.prompt, task_type="plan", based_on=older_child.id, recovery_origin="retry")
    assert grandchild.id is not None
    grandchild.status = "pending"
    store.update(grandchild)

    read_context = _read_context_for_store(store)
    store_snapshot = _build_recovery_chain_snapshot(store, root)
    indexed_snapshot = _build_recovery_chain_snapshot(store, root, read_context=read_context)

    assert [task.id for task in store_snapshot.direct_children] == [older_child.id, newer_child.id]
    assert [task.id for task in indexed_snapshot.direct_children] == [task.id for task in store_snapshot.direct_children]
    assert [task.id for task in indexed_snapshot.descendants] == [task.id for task in store_snapshot.descendants]


def test_recovery_snapshot_and_sibling_resolution_match_indexed_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    root = store.add("Failed implementation", task_type="implement")
    assert root.id is not None
    root.status = "failed"
    root.failure_reason = "MAX_TURNS"
    root.session_id = "sess-root"
    root.branch = "feature/recovery-root"
    root.completed_at = datetime(2026, 5, 16, 8, 0, tzinfo=UTC)
    store.update(root)

    failed_resume = store.add(root.prompt, task_type="implement", based_on=root.id, recovery_origin="resume")
    assert failed_resume.id is not None
    failed_resume.status = "failed"
    failed_resume.failure_reason = "MAX_TURNS"
    failed_resume.session_id = root.session_id
    failed_resume.branch = root.branch
    failed_resume.completed_at = datetime(2026, 5, 16, 8, 30, tzinfo=UTC)
    store.update(failed_resume)

    retry_child = store.add(root.prompt, task_type="implement", based_on=root.id, recovery_origin="retry")
    assert retry_child.id is not None
    retry_child.status = "failed"
    retry_child.failure_reason = "MAX_TURNS"
    retry_child.session_id = "sess-retry"
    retry_child.branch = "feature/recovery-retry"
    retry_child.completed_at = datetime(2026, 5, 16, 9, 0, tzinfo=UTC)
    store.update(retry_child)

    resumed_retry = store.add(retry_child.prompt, task_type="implement", based_on=retry_child.id, recovery_origin="resume")
    assert resumed_retry.id is not None
    resumed_retry.status = "completed"
    resumed_retry.session_id = retry_child.session_id
    resumed_retry.branch = retry_child.branch
    resumed_retry.has_commits = False
    resumed_retry.completed_at = datetime(2026, 5, 16, 9, 30, tzinfo=UTC)
    store.update(resumed_retry)

    read_context = _read_context_for_store(store)
    store_snapshot = _build_recovery_chain_snapshot(store, retry_child)
    store_sibling = get_completed_sibling_recovery(store, failed_resume)

    def _unexpected_store_lookup(*_args, **_kwargs):
        raise AssertionError("indexed recovery traversal should not hit the store")

    monkeypatch.setattr(store, "get", _unexpected_store_lookup)
    monkeypatch.setattr(store, "get_based_on_children_by_type", _unexpected_store_lookup)

    indexed_snapshot = _build_recovery_chain_snapshot(store, retry_child, read_context=read_context)
    indexed_sibling = get_completed_sibling_recovery(store, failed_resume, read_context=read_context)

    assert indexed_snapshot.root_task.id == store_snapshot.root_task.id
    assert indexed_snapshot.ancestor_ids == store_snapshot.ancestor_ids
    assert indexed_snapshot.steps == store_snapshot.steps
    assert [task.id for task in indexed_snapshot.descendants] == [task.id for task in store_snapshot.descendants]
    assert [task.id for task in indexed_snapshot.direct_children] == [task.id for task in store_snapshot.direct_children]
    assert [task.id for task in indexed_snapshot.terminal_descendants] == [task.id for task in store_snapshot.terminal_descendants]
    assert indexed_snapshot.completed_terminal_descendant is not None
    assert store_snapshot.completed_terminal_descendant is not None
    assert indexed_snapshot.completed_terminal_descendant.id == store_snapshot.completed_terminal_descendant.id
    assert store_sibling is not None
    assert indexed_sibling is not None
    assert indexed_sibling.id == store_sibling.id == resumed_retry.id


def test_list_failed_tasks_for_recovery_uses_indexed_lineage_without_store_walks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.branch = "feature/landed-lineage"
    failed.completed_at = datetime(2026, 5, 16, 8, 0, tzinfo=UTC)
    store.update(failed)

    landed_follow_up = store.add(
        "Merged same-branch follow-up",
        task_type="implement",
        based_on=failed.id,
        recovery_origin="manual",
    )
    assert landed_follow_up.id is not None
    landed_follow_up.status = "completed"
    landed_follow_up.branch = failed.branch
    landed_follow_up.has_commits = True
    landed_follow_up.merge_status = "merged"
    landed_follow_up.completed_at = datetime(2026, 5, 16, 9, 0, tzinfo=UTC)
    store.update(landed_follow_up)

    assert list_failed_tasks_for_recovery(store) == []

    read_context = _read_context_for_store(store)

    def _unexpected_store_lineage_read(*_args, **_kwargs):
        raise AssertionError("store lineage read should not run when RecoveryReadContext is available")

    monkeypatch.setattr(store, "get", _unexpected_store_lineage_read)
    monkeypatch.setattr(store, "get_lineage_children", _unexpected_store_lineage_read)

    assert list_failed_tasks_for_recovery(store, read_context=read_context) == []


def test_decide_failed_task_recovery_uses_injected_merge_context_not_load(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When merge_context is passed explicitly, _load_merge_context must not be called.

    Guards the performance/nondeterminism fix: batch callers thread the already-loaded
    context so each task in the loop does not re-run Config.load + Git + branch listing.
    """
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Unitless implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.branch = "feature/mc-injection-test"
    failed.has_commits = True
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    # _load_merge_context raises: if it's called despite the injected context, the test fails.
    def _must_not_be_called(_project_dir=None) -> _MergeContext:
        raise AssertionError("_load_merge_context must not be called when merge_context is injected")

    monkeypatch.setattr(recovery_engine, "_load_merge_context", _must_not_be_called)
    monkeypatch.setattr(
        recovery_engine,
        "resolve_task_merge_state_for_target",
        lambda **kwargs: "redundant",
    )

    git = _StubMergeGit(default_branch="main")
    injected_mc = _MergeContext(git=git, default_branch="main")

    # Must not raise (i.e., _load_merge_context is never called)
    decision = decide_failed_task_recovery(
        store, failed, max_recovery_attempts=1, merge_context=injected_mc
    )
    assert decision.action == "skip"
    assert decision.reason_code == "merge_unit_redundant"


def test_decide_failed_task_recovery_uses_read_context_merge_context_not_load(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When read_context.merge_context is pre-populated, _load_merge_context must not be called.

    Guards the batch-sharing fix: the first decide call in a loop caches the context in
    read_context.merge_context; subsequent calls reuse it without re-running the ambient load.
    """
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Unitless implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.branch = "feature/rc-mc-injection-test"
    failed.has_commits = True
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    def _must_not_be_called(_project_dir=None) -> _MergeContext:
        raise AssertionError("_load_merge_context must not be called when read_context.merge_context is set")

    monkeypatch.setattr(recovery_engine, "_load_merge_context", _must_not_be_called)
    monkeypatch.setattr(
        recovery_engine,
        "resolve_task_merge_state_for_target",
        lambda **kwargs: "redundant",
    )

    git = _StubMergeGit(default_branch="main")
    pre_loaded_mc = _MergeContext(git=git, default_branch="main")

    indexes = _load_indexes(store)
    read_context = RecoveryReadContext(
        tasks=indexes.tasks,
        task_by_id=indexes.task_by_id,
        based_on_children=indexes.based_on_children,
        depends_on_children=indexes.depends_on_children,
        root_by_task_id=indexes.root_by_task_id,
        merge_units_by_task_id=indexes.merge_units_by_task_id,
        allow_reconcile_mutation=False,
    )
    read_context.merge_context = pre_loaded_mc  # pre-populate as a batch caller would

    # Must not raise (i.e., _load_merge_context is never called)
    decision = decide_failed_task_recovery(
        store, failed, max_recovery_attempts=1, read_context=read_context
    )
    assert decision.action == "skip"
    assert decision.reason_code == "merge_unit_redundant"


class _MinimalRecoveryGit(Git):
    """Git subclass satisfying isinstance checks for list_failed_tasks_for_recovery tests.

    Overrides subprocess-backed methods so tests run in-process.
    """

    def __init__(self, *, branches: frozenset[str] = frozenset()) -> None:
        self.repo_dir = Path("/dev/null")
        self._cache = None
        self._branches = branches

    def local_branch_names(self) -> frozenset[str]:  # type: ignore[override]
        return self._branches

    def is_merged(self, branch: str, into: str | None = None, use_cherry: bool = False) -> bool:  # type: ignore[override]
        return False

    def branch_exists(self, branch: str) -> bool:  # type: ignore[override]
        return branch in self._branches

    def resolve_fresh_merge_source(self, branch: str, **_kwargs: object):  # type: ignore[override]
        from gza.git import ResolvedMergeSourceRef
        return ResolvedMergeSourceRef(None)


def test_list_failed_tasks_for_recovery_uses_seeded_git_not_load(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When git+target_branch are supplied, _load_merge_context must not be called.

    Guards the helper contract: when a caller supplies live git+target_branch kwargs,
    list_failed_tasks_for_recovery must use them and must not fall back to the ambient
    Config.load(discover=True) + Git() path.  The advance-path wiring (cmd_advance
    constructing the Git instance and threading it through) is covered separately in
    tests/cli/test_advance_squash_threshold.py.
    """
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Advance-path implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.branch = "feature/advance-preseed-test"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    def _must_not_be_called(_project_dir: object = None) -> _MergeContext:
        raise AssertionError(
            "_load_merge_context was called despite holding a live git+target_branch; "
            "the advance path did not eliminate the ambient discover=True load"
        )

    monkeypatch.setattr(recovery_engine, "_load_merge_context", _must_not_be_called)

    git = _MinimalRecoveryGit(branches=frozenset({failed.branch}))
    warnings: list[str] = []

    # Must not raise (i.e., _load_merge_context is never called)
    result = list_failed_tasks_for_recovery(store, warnings=warnings, git=git, target_branch="main")
    assert result is not None
