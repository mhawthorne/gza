from __future__ import annotations

import importlib
import json
import platform
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import ANY, MagicMock, call, patch

import pytest

import gza.recovery_engine as recovery_engine
from gza import dependency_preconditions as dependency_preconditions_module
from gza.cli._recovery_lane import collect_recovery_lane_entries
from gza.config import Config
from gza.db import MergeUnit, SqliteTaskStore, Task as DbTask
from gza.dispatch_preview import DispatchPreview
from gza.git import Git, GitError, ResolvedMergeSourceRef
from gza.lineage_query import (
    LineageOwnerQuery,
    LineageOwnerRow,
    _failed_leaf_has_unique_unmerged_work_under_terminal_owner,
    _load_indexes,
    _query_lineage_owner_rows_with_context,
    collect_stale_unmerged_sweep_candidates,
    query_lineage_owner_rows,
)
from gza.main_integration_verify import (
    MAIN_INTEGRATION_VERIFY_FRESHNESS_UNAVAILABLE_EXIT_STATUS,
    MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS,
    MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_REASON,
    MAIN_INTEGRATION_VERIFY_REASON,
    MAIN_INTEGRATION_VERIFY_REMEDIATION_TRIGGER_SOURCE,
    MAIN_INTEGRATION_VERIFY_TAG,
    load_main_integration_verify_state,
)
from gza.main_verify_format import main_verify_state_halts_merges
from gza.merge_state import BranchMergeClassification
from gza.operator_state import blocked_by_empty_prereq_label
from gza.recovery_engine import list_failed_tasks_for_recovery
from gza.recovery_read_context import RecoveryReadContext
from gza.review_verify_state import persist_verify_gate_artifact
from gza.review_scope import build_spec_coherence_review_scope
from tests.cli.conftest import make_store, setup_config


@pytest.fixture(autouse=True)
def _stub_ambient_merge_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep unit coverage in-process when a test does not seed a live Git context."""

    monkeypatch.setattr(
        recovery_engine,
        "_load_merge_context",
        lambda _project_dir=None: recovery_engine._MergeContext(
            git=None,
            default_branch="main",
            existing_branches=frozenset(),
        ),
    )


def _set_completed(task, *, when: datetime, branch: str | None, has_commits: bool) -> None:
    task.status = "completed"
    task.completed_at = when
    task.branch = branch
    task.has_commits = has_commits


def _main_verify_environment_identity_payload() -> dict[str, str]:
    return {
        "runner_class": "host",
        "platform_system": platform.system(),
        "platform_machine": platform.machine(),
        "python_executable": sys.executable,
        "python_version": f"{sys.version_info.major}.{sys.version_info.minor}",
    }


def _set_dropped(task, *, when: datetime, branch: str | None, has_commits: bool) -> None:
    task.status = "dropped"
    task.completed_at = when
    task.branch = branch
    task.has_commits = has_commits


def _read_context_for_store(store: SqliteTaskStore) -> RecoveryReadContext:
    indexes = _load_indexes(store)
    return RecoveryReadContext(
        tasks=indexes.tasks,
        task_by_id=indexes.task_by_id,
        based_on_children=indexes.based_on_children,
        depends_on_children=indexes.depends_on_children,
        root_by_task_id=indexes.root_by_task_id,
        merge_units_by_task_id=indexes.merge_units_by_task_id,
        historical_merge_units_by_task_id=indexes.historical_merge_units_by_task_id,
        allow_reconcile_mutation=False,
    )


def _persist_current_green_verify(
    store: SqliteTaskStore,
    config: Config,
    *,
    owner_task,
    source_task,
    head_sha: str,
    base_sha: str = "target-sha",
) -> None:
    persist_verify_gate_artifact(
        store,
        config,
        owner_task=owner_task,
        source_task=source_task,
        result=type(
            "VerifyResult",
            (),
            {
                "command": "./bin/tests",
                "status": "passed",
                "exit_status": "0",
                "captured_at": datetime(2026, 5, 10, 13, 30, tzinfo=UTC),
                "reviewed_branch": owner_task.branch,
                "reviewed_head_sha": head_sha,
                "reviewed_base_sha": base_sha,
                "working_directory": str(config.project_dir),
                "failure": None,
            },
        )(),
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="test",
    )


def _completed_main_verify_remediation(store: SqliteTaskStore, *, branch: str) -> DbTask:
    remediation = store.add(
        "Fix local main integration verify phase `unit`\n\n"
        "Remediation kind: fix\n"
        "Failure signature: phases:unit\n"
        "Tree fingerprint: fp-unit-a\n",
        task_type="implement",
        tags=("system", MAIN_INTEGRATION_VERIFY_TAG),
        trigger_source=MAIN_INTEGRATION_VERIFY_REMEDIATION_TRIGGER_SOURCE,
    )
    assert remediation.id is not None
    _set_completed(
        remediation,
        when=datetime(2026, 5, 10, 9, 0, tzinfo=UTC),
        branch=branch,
        has_commits=True,
    )
    remediation.merge_status = "unmerged"
    store.update(remediation)
    return remediation


def _main_verify_remediation_git(branch: str, *, diff_name_status: str = "") -> MagicMock:
    git = MagicMock(spec=Git)
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "topic"
    git.can_merge.return_value = True
    git.count_commits_behind.return_value = 0
    git.branch_exists.return_value = True
    git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(branch)
    git.rev_parse_if_exists.side_effect = lambda ref: (
        "same-head" if ref == branch else "base-head" if ref == "main" else None
    )
    git.get_diff_name_status.return_value = diff_name_status
    return git


def _query_single_owner_action(
    store: SqliteTaskStore,
    config: Config,
    git: MagicMock,
    owner_id: str,
) -> dict[str, Any]:
    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True),
        config=config,
        git=git,
        target_branch="main",
    )
    row = next(r for r in rows if r.owner_task.id == owner_id)
    assert row.next_action is not None
    return row.next_action


class _ExplodingLineageGit:
    def __init__(self) -> None:
        self.is_ancestor_calls = 0

    def is_ancestor(self, _ancestor: str, _descendant: str) -> bool:
        self.is_ancestor_calls += 1
        raise AssertionError("terminal merged failed leaf should not hit git ancestry probes")


def test_failed_leaf_with_terminal_merged_unit_skips_live_git_proof(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")

    owner = store.add("Merged owner", task_type="implement")
    owner.status = "completed"
    owner.completed_at = datetime(2026, 5, 20, 9, 0, tzinfo=UTC)
    owner.branch = "feature/owner"
    owner.has_commits = True
    store.update(owner)
    assert owner.id is not None

    failed = store.add("Failed leaf", task_type="implement", based_on=owner.id, recovery_origin="manual")
    failed.status = "failed"
    failed.completed_at = datetime(2026, 5, 21, 9, 0, tzinfo=UTC)
    failed.branch = "feature/failed-leaf"
    failed.has_commits = True
    store.update(failed)
    assert failed.id is not None

    owner_merge_unit: MergeUnit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="merged",
    )
    leaf_merge_unit: MergeUnit = store.create_merge_unit(
        source_branch=failed.branch,
        target_branch="main",
        owner_task_id=failed.id,
        state="merged",
    )
    store.refresh_merge_unit_head(leaf_merge_unit.id, head_sha="missing-recorded-head")

    git = _ExplodingLineageGit()
    with caplog.at_level("DEBUG"):
        visible = _failed_leaf_has_unique_unmerged_work_under_terminal_owner(
            store=store,
            failed_task=failed,
            completed_owner=owner,
            owner_merge_unit=owner_merge_unit,
            leaf_merge_unit=store.get_merge_unit(leaf_merge_unit.id),
            git=git,  # type: ignore[arg-type]
        )

    assert visible is False
    assert git.is_ancestor_calls == 0
    assert caplog.text == ""


class _LineageMergeStateGit:
    def __init__(
        self,
        *,
        source_ref: str,
        source_sha: str,
        target_sha: str,
        ahead_count: int | None,
        merged: bool = False,
        net_diff: bool | None = None,
    ) -> None:
        self.source_ref = source_ref
        self.source_sha = source_sha
        self.target_sha = target_sha
        self.ahead_count = ahead_count
        self.merged = merged
        self.net_diff = net_diff
        self.probes: list[tuple[str, str]] = []

    def resolve_fresh_merge_source(self, branch: str):
        self.probes.append(("resolve_fresh_merge_source", branch))
        return ResolvedMergeSourceRef(self.source_ref)

    def rev_parse_if_exists(self, ref: str) -> str | None:
        self.probes.append(("rev_parse_if_exists", ref))
        if ref == self.source_ref:
            return self.source_sha
        if ref == "main":
            return self.target_sha
        return None

    def count_commits_ahead_checked(self, source_ref: str, target_ref: str) -> int | None:
        self.probes.append(("count_commits_ahead_checked", f"{source_ref}->{target_ref}"))
        return self.ahead_count

    def is_merged(self, branch: str, into: str) -> bool:
        self.probes.append(("is_merged", f"{branch}->{into}"))
        return self.merged

    def has_non_empty_source_diff_against_target(self, source_ref: str, target: str) -> bool | None:
        self.probes.append(("has_non_empty_source_diff_against_target", f"{source_ref}->{target}"))
        return self.net_diff


class _RecordedHeadProofGit:
    def __init__(self, *, ancestor_error: Exception, source_ref: str = "feature/failed-leaf") -> None:
        self.ancestor_error = ancestor_error
        self.source_ref = source_ref
        self.is_ancestor_calls = 0
        self.patch_present_calls = 0
        self.ancestor_probes: list[tuple[str, str]] = []
        self.patch_present_probes: list[tuple[str, str]] = []

    def resolve_fresh_merge_source(self, branch: str) -> ResolvedMergeSourceRef:
        return ResolvedMergeSourceRef(self.source_ref if branch == "feature/failed-leaf" else branch)

    def rev_parse_if_exists(self, ref: str) -> str | None:
        if ref == self.source_ref:
            return "source-sha"
        if ref == "main":
            return "target-sha"
        return None

    def count_commits_ahead_checked(self, _source_ref: str, _target_ref: str) -> int | None:
        return 0

    def is_merged(self, _branch: str, _target: str) -> bool:
        return False

    def has_non_empty_source_diff_against_target(self, _source_ref: str, _target: str) -> bool | None:
        return False

    def is_ancestor(self, _ancestor: str, _descendant: str) -> bool:
        self.is_ancestor_calls += 1
        self.ancestor_probes.append((_ancestor, _descendant))
        raise self.ancestor_error

    def is_patch_equivalent_commit_present_on_target(self, _commit: str, _target: str) -> bool:
        self.patch_present_calls += 1
        self.patch_present_probes.append((_commit, _target))
        return False


def _terminal_owner_failed_leaf_case(tmp_path: Path) -> tuple[SqliteTaskStore, DbTask, DbTask, MergeUnit, MergeUnit]:
    store = SqliteTaskStore(tmp_path / "test.db")
    owner = store.add("Merged owner", task_type="implement")
    owner.status = "completed"
    owner.completed_at = datetime(2026, 8, 21, 8, 0, tzinfo=UTC)
    owner.branch = "feature/owner"
    owner.has_commits = True
    owner.merge_status = "merged"
    store.update(owner)
    assert owner.id is not None

    failed = store.add("Failed leaf", task_type="implement", based_on=owner.id, recovery_origin="manual")
    failed.status = "failed"
    failed.completed_at = datetime(2026, 8, 21, 9, 0, tzinfo=UTC)
    failed.branch = "feature/failed-leaf"
    failed.has_commits = True
    store.update(failed)
    assert failed.id is not None

    owner_unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="merged",
        head_sha="owner-head",
    )
    store.attach_task_to_merge_unit(owner.id, owner_unit.id, "owner")
    leaf_unit = store.create_merge_unit(
        source_branch=failed.branch,
        target_branch="main",
        owner_task_id=failed.id,
        state="unmerged",
        head_sha="deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
    )
    store.attach_task_to_merge_unit(failed.id, leaf_unit.id, "owner")
    return store, owner, failed, owner_unit, leaf_unit


def test_failed_leaf_dead_recorded_head_uses_fallback_without_warning_and_reuses_cache(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _store, owner, failed, owner_unit, leaf_unit = _terminal_owner_failed_leaf_case(tmp_path)
    git = _RecordedHeadProofGit(
        ancestor_error=GitError(
            "git merge-base --is-ancestor deadbeefdeadbeefdeadbeefdeadbeefdeadbeef "
            "feature/failed-leaf failed: fatal: Not a valid commit name "
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
        )
    )
    cache = {}

    with caplog.at_level("WARNING"):
        first = _failed_leaf_has_unique_unmerged_work_under_terminal_owner(
            failed_task=failed,
            completed_owner=owner,
            owner_merge_unit=owner_unit,
            leaf_merge_unit=leaf_unit,
            git=git,  # type: ignore[arg-type]
            classification_cache=cache,
        )
        second = _failed_leaf_has_unique_unmerged_work_under_terminal_owner(
            failed_task=failed,
            completed_owner=owner,
            owner_merge_unit=owner_unit,
            leaf_merge_unit=leaf_unit,
            git=git,  # type: ignore[arg-type]
            classification_cache=cache,
        )

    assert first is True
    assert second is True
    assert git.is_ancestor_calls == 1
    assert git.patch_present_calls == 1
    # The dead-ref path and the genuine-error path share identical message text
    # (merge_state.py) and are only distinguished by severity: dead-ref logs at
    # DEBUG (benign, expected), genuine errors escalate to WARNING. Capturing at
    # WARNING here is the actual behavior under test, not incidental coupling.
    assert caplog.records == []


def test_failed_leaf_recorded_head_genuine_git_error_still_warns(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _store, owner, failed, owner_unit, leaf_unit = _terminal_owner_failed_leaf_case(tmp_path)
    git = _RecordedHeadProofGit(
        ancestor_error=GitError(
            "git merge-base --is-ancestor deadbeefdeadbeefdeadbeefdeadbeefdeadbeef "
            "feature/failed-leaf failed: fatal: unable to read repository"
        )
    )

    with caplog.at_level("DEBUG", logger="gza.lineage_query"):
        result = _failed_leaf_has_unique_unmerged_work_under_terminal_owner(
            failed_task=failed,
            completed_owner=owner,
            owner_merge_unit=owner_unit,
            leaf_merge_unit=leaf_unit,
            git=git,  # type: ignore[arg-type]
        )

    assert result is True
    assert git.is_ancestor_calls == 1
    assert git.patch_present_calls == 1
    assert any("Could not verify whether" in record.getMessage() for record in caplog.records)


def test_failed_leaf_recorded_head_bad_source_ref_still_warns_and_uses_fallback(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _store, owner, failed, owner_unit, leaf_unit = _terminal_owner_failed_leaf_case(tmp_path)
    git = _RecordedHeadProofGit(
        ancestor_error=GitError(
            "git merge-base --is-ancestor deadbeefdeadbeefdeadbeefdeadbeefdeadbeef "
            "refs/heads/feature/failed-leaf failed:\n"
            "fatal: Not a valid commit name refs/heads/feature/failed-leaf"
        ),
        source_ref="refs/heads/feature/failed-leaf",
    )

    with caplog.at_level("DEBUG", logger="gza.lineage_query"):
        result = _failed_leaf_has_unique_unmerged_work_under_terminal_owner(
            failed_task=failed,
            completed_owner=owner,
            owner_merge_unit=owner_unit,
            leaf_merge_unit=leaf_unit,
            git=git,  # type: ignore[arg-type]
        )

    assert result is True
    assert git.ancestor_probes == [("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef", "refs/heads/feature/failed-leaf")]
    assert git.patch_present_probes == [("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef", "main")]
    assert any("Could not verify whether" in record.getMessage() for record in caplog.records)


def test_query_lineage_owner_rows_reuses_failed_leaf_cache_for_terminal_rerooting(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    owner = store.add("Merged owner", task_type="implement")
    owner.status = "completed"
    owner.completed_at = datetime(2026, 8, 21, 8, 0, tzinfo=UTC)
    owner.branch = "feature/owner"
    owner.has_commits = True
    owner.merge_status = "merged"
    store.update(owner)
    assert owner.id is not None

    failed = store.add("Failed leaf", task_type="implement", based_on=owner.id, recovery_origin="manual")
    failed.status = "failed"
    failed.completed_at = datetime(2026, 8, 21, 9, 0, tzinfo=UTC)
    failed.branch = "feature/failed-leaf"
    failed.has_commits = True
    store.update(failed)
    assert failed.id is not None

    owner_unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="merged",
        head_sha="owner-head",
    )
    store.attach_task_to_merge_unit(owner.id, owner_unit.id, "owner")
    leaf_unit = store.create_merge_unit(
        source_branch=failed.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="unmerged",
        head_sha="deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
    )
    store.attach_task_to_merge_unit(failed.id, leaf_unit.id, "contributor")
    git = _RecordedHeadProofGit(
        ancestor_error=GitError(
            "git merge-base --is-ancestor deadbeefdeadbeefdeadbeefdeadbeefdeadbeef "
            "refs/heads/feature/failed-leaf failed: fatal: Not a valid commit name "
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
        ),
        source_ref="refs/heads/feature/failed-leaf",
    )

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        git=git,  # type: ignore[arg-type]
        target_branch="main",
        persist_post_merge_rebase_state=False,
        persist_review_clearance=False,
    )

    assert [row.owner_task.id for row in rows] == [failed.id]
    assert rows[0].unresolved_tasks == (failed,)
    assert rows[0].recovery_leaf_task == failed
    assert git.ancestor_probes == [("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef", "refs/heads/feature/failed-leaf")]
    assert git.patch_present_probes == [("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef", "main")]


def _build_tag_filtered_merge_unit_case(tmp_path: Path) -> tuple[SqliteTaskStore, str, str, str]:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    branch = "feature/tag-filtered-merge-unit"
    tag = "v0.5.0"

    owner = store.add("Failed implement owner", task_type="implement", tags=(tag,))
    owner.status = "failed"
    owner.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    owner.branch = branch
    owner.has_commits = False
    store.update(owner)
    assert owner.id is not None

    implement = store.add(
        "Completed implement sibling",
        task_type="implement",
        based_on=owner.id,
        tags=(tag,),
    )
    _set_completed(
        implement,
        when=datetime(2026, 5, 10, 10, 0, tzinfo=UTC),
        branch=branch,
        has_commits=True,
    )
    store.update(implement)
    assert implement.id is not None

    improve = store.add(
        "Completed improve sibling",
        task_type="improve",
        based_on=implement.id,
        same_branch=True,
        tags=(tag,),
    )
    _set_completed(
        improve,
        when=datetime(2026, 5, 10, 11, 0, tzinfo=UTC),
        branch=branch,
        has_commits=True,
    )
    store.update(improve)
    assert improve.id is not None

    rebase = store.add(
        "Completed rebase sibling",
        task_type="rebase",
        based_on=improve.id,
        same_branch=True,
    )
    _set_completed(
        rebase,
        when=datetime(2026, 5, 10, 12, 0, tzinfo=UTC),
        branch=branch,
        has_commits=True,
    )
    rebase.changed_diff = False
    store.update(rebase)
    assert rebase.id is not None

    review = store.add(
        "Completed branchless review",
        task_type="review",
        depends_on=rebase.id,
        tags=(tag,),
    )
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 10, 13, 0, tzinfo=UTC)
    review.output_content = "**Verdict: APPROVED**"
    store.update(review)
    assert review.id is not None

    unit = store.create_merge_unit(
        source_branch=branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(owner.id, unit.id, "owner")
    store.attach_task_to_merge_unit(implement.id, unit.id, "implement")
    store.attach_task_to_merge_unit(improve.id, unit.id, "improve")
    store.attach_task_to_merge_unit(rebase.id, unit.id, "rebase")
    store.attach_task_to_merge_unit(review.id, unit.id, "review")

    return store, tag, owner.id, rebase.id


def _set_merge_unit_timestamps(store: SqliteTaskStore, merge_unit_id: str, *, when: datetime) -> None:
    timestamp = when.strftime("%Y-%m-%d %H:%M:%S")
    with store._connect() as conn:
        conn.execute(
            "UPDATE merge_units SET created_at = ?, updated_at = ? WHERE id = ?",
            (timestamp, timestamp, merge_unit_id),
        )


def _set_task_created_at(store: SqliteTaskStore, task_id: str, *, when: datetime) -> None:
    timestamp = when.strftime("%Y-%m-%d %H:%M:%S")
    with store._connect() as conn:
        conn.execute(
            "UPDATE tasks SET created_at = ? WHERE id = ?",
            (timestamp, task_id),
        )


def _plan_review_slice_prompt(*, plan_id: str, review_id: str, slice_id: str, title: str = "Slice title") -> str:
    return "\n".join(
        (
            f"Implement approved plan-review slice {slice_id}: {title}",
            "",
            "Provenance:",
            f"- Plan source: {plan_id}",
            f"- Plan review: {review_id}",
            f"- Slice: {slice_id} ({title})",
            "",
            "Slice prompt:",
            "Implement the slice.",
            "",
            "Scope:",
            "- Do the slice work.",
        )
    )


def test_query_lineage_owner_rows_tag_filter_keeps_merge_unit_representative(tmp_path: Path) -> None:
    store, tag, owner_id, rebase_id = _build_tag_filtered_merge_unit_case(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0
    git = MagicMock()
    git.branch_exists.return_value = True
    git.can_merge.return_value = True
    git.count_commits_behind.return_value = 0
    git.rev_parse_if_exists.side_effect = lambda ref: (
        "same-head" if ref == "feature/tag-filtered-merge-unit" else "target-sha" if ref == "main" else None
    )

    rebase = store.get(rebase_id)
    assert rebase is not None
    improve = store.get(rebase.based_on) if rebase.based_on is not None else None
    assert improve is not None
    impl = store.get(improve.based_on) if improve.based_on is not None else None
    assert impl is not None
    _persist_current_green_verify(store, config, owner_task=impl, source_task=rebase, head_sha="same-head")

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(
            limit=None,
            tags=(tag,),
            include_skipped=True,
            max_recovery_attempts=1,
        ),
        config=config,
        git=git,
        target_branch="main",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.owner_task.id == owner_id
    assert row.lifecycle_action_task is not None
    assert row.lifecycle_action_task.id == rebase_id
    assert row.next_action is not None
    assert row.next_action["type"] in {"merge", "merge_with_followups"}
    assert "no branch" not in str(row.next_action.get("description", "")).lower()


def test_query_lineage_owner_rows_tag_filter_excludes_owner_when_only_member_matches(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    owner = store.add("Beta owner", task_type="implement", tags=("beta",))
    owner.status = "failed"
    owner.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    owner.failure_reason = "TEST_FAILURE"
    store.update(owner)
    assert owner.id is not None

    member = store.add(
        "Alpha descendant",
        task_type="improve",
        based_on=owner.id,
        tags=("alpha",),
    )
    member.status = "failed"
    member.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    member.failure_reason = "TEST_FAILURE"
    store.update(member)

    git = MagicMock()
    with (
        patch("gza.lineage_query.prime_advance_planning_refs") as preload,
        patch("gza.cli.advance_engine.determine_next_action") as determine,
    ):
        rows = query_lineage_owner_rows(
            store,
            LineageOwnerQuery(limit=None, tags=("alpha",), include_skipped=True, max_recovery_attempts=1),
            config=config,
            git=git,
            target_branch="main",
        )

    assert not rows
    preload.assert_called_once_with(
        git,
        branch_names=ANY,
        target_branch="main",
        warning_logger=ANY,
    )
    assert tuple(preload.call_args.kwargs["branch_names"]) == ()
    determine.assert_not_called()


def test_query_lineage_owner_rows_profile_classified_counter_counts_filtered_failed_candidates(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("GZA_PROFILE", "1")
    metrics_module = importlib.import_module("gza.metrics")
    with metrics_module._STATE.lock:  # noqa: SLF001
        metrics_module._STATE.counters.clear()  # noqa: SLF001
        metrics_module._STATE.latencies.clear()  # noqa: SLF001

    setup_config(tmp_path)
    store = make_store(tmp_path)

    matching = store.add("Alpha failed candidate", task_type="implement", tags=("alpha",))
    matching.status = "failed"
    matching.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    matching.failure_reason = "TEST_FAILURE"
    store.update(matching)

    resolved = store.add("Alpha resolved failed candidate", task_type="implement", tags=("alpha",))
    resolved.status = "failed"
    resolved.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    resolved.failure_reason = "TEST_FAILURE"
    store.update(resolved)
    resolved_recovery = store.add(
        "Alpha resolved failed candidate",
        task_type="implement",
        based_on=resolved.id,
        tags=("alpha",),
    )
    resolved_recovery.status = "completed"
    resolved_recovery.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    resolved_recovery.recovery_origin = "retry"
    store.update(resolved_recovery)

    unrelated = store.add("Beta failed candidate", task_type="implement", tags=("beta",))
    unrelated.status = "failed"
    unrelated.completed_at = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    unrelated.failure_reason = "TEST_FAILURE"
    store.update(unrelated)

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, tags=("alpha",), include_skipped=True, max_recovery_attempts=1),
    )
    snapshot = metrics_module.snapshot()

    assert [row.owner_task.id for row in rows] == [matching.id]
    assert snapshot.counters[
        metrics_module.MetricKey("gza_lineage_owner_failed_tasks_seen_total")
    ] == 3
    assert snapshot.counters[
        metrics_module.MetricKey("gza_lineage_owner_failed_tasks_classified_total")
    ] == 1


def _add_unmerged_tag_pruning_owner(
    store: SqliteTaskStore,
    *,
    prompt: str,
    tags: tuple[str, ...],
    branch: str,
    when: datetime,
) -> DbTask:
    owner = store.add(prompt, task_type="implement", tags=tags)
    assert owner.id is not None
    _set_completed(owner, when=when, branch=branch, has_commits=True)
    owner.merge_status = "unmerged"
    store.update(owner)
    unit = store.create_merge_unit(
        source_branch=branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(owner.id, unit.id, "owner")
    return owner


@pytest.mark.parametrize(
    ("query_kwargs", "expected_prompts"),
    [
        ({"tags": ("alpha", "gamma"), "any_tag": True}, {"Alpha owner", "Alpha beta owner"}),
        ({"tags": ("alpha", "beta"), "any_tag": False}, {"Alpha beta owner"}),
        ({"exclude_tags": ("beta",)}, {"Alpha owner", "Untagged owner"}),
        ({"untagged_only": True}, {"Untagged owner"}),
    ],
)
def test_query_lineage_owner_rows_tag_pruning_preserves_visible_rows_and_skips_resolution(
    tmp_path: Path,
    query_kwargs: dict[str, Any],
    expected_prompts: set[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    owners = [
        _add_unmerged_tag_pruning_owner(
            store,
            prompt="Alpha owner",
            tags=("alpha",),
            branch="feature/tag-prune-alpha",
            when=datetime(2026, 5, 10, 9, 0, tzinfo=UTC),
        ),
        _add_unmerged_tag_pruning_owner(
            store,
            prompt="Alpha beta owner",
            tags=("alpha", "beta"),
            branch="feature/tag-prune-alpha-beta",
            when=datetime(2026, 5, 10, 10, 0, tzinfo=UTC),
        ),
        _add_unmerged_tag_pruning_owner(
            store,
            prompt="Beta owner",
            tags=("beta",),
            branch="feature/tag-prune-beta",
            when=datetime(2026, 5, 10, 11, 0, tzinfo=UTC),
        ),
        _add_unmerged_tag_pruning_owner(
            store,
            prompt="Untagged owner",
            tags=(),
            branch="feature/tag-prune-untagged",
            when=datetime(2026, 5, 10, 12, 0, tzinfo=UTC),
        ),
    ]
    expected_owner_ids = {owner.id for owner in owners if owner.prompt in expected_prompts}
    expected_branches = {owner.branch for owner in owners if owner.prompt in expected_prompts}

    git = MagicMock()
    seen_planning_task_ids: list[str] = []

    def _determine_next_action(_config, _store, _git, planning_task, *_args, **_kwargs):
        assert planning_task.id is not None
        seen_planning_task_ids.append(planning_task.id)
        return {"type": "merge", "description": "merge candidate"}

    with (
        patch("gza.lineage_query.prime_advance_planning_refs") as preload,
        patch("gza.cli.advance_engine.determine_next_action", side_effect=_determine_next_action) as determine,
    ):
        rows = query_lineage_owner_rows(
            store,
            LineageOwnerQuery(
                limit=None,
                include_skipped=True,
                max_recovery_attempts=1,
                **query_kwargs,
            ),
            config=config,
            git=git,
            target_branch="main",
        )

    assert {row.owner_task.id for row in rows} == expected_owner_ids
    assert set(seen_planning_task_ids) == expected_owner_ids
    assert determine.call_count == len(expected_owner_ids)
    preload.assert_called_once_with(
        git,
        branch_names=ANY,
        target_branch="main",
        warning_logger=ANY,
    )
    assert set(preload.call_args.kwargs["branch_names"]) == expected_branches














def test_query_lineage_owner_rows_mixed_owner_and_task_filters_exclude_mismatched_owner_lineage(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    owner_a = store.add("Owner A", task_type="implement")
    assert owner_a.id is not None
    _set_completed(
        owner_a,
        when=datetime(2026, 5, 10, 9, 0, tzinfo=UTC),
        branch="feature/owner-a",
        has_commits=True,
    )
    store.update(owner_a)

    member_a = store.add(
        "Member A",
        task_type="improve",
        based_on=owner_a.id,
        same_branch=True,
    )
    assert member_a.id is not None
    _set_completed(
        member_a,
        when=datetime(2026, 5, 10, 10, 0, tzinfo=UTC),
        branch="feature/owner-a",
        has_commits=True,
    )
    store.update(member_a)

    owner_b = store.add("Owner B", task_type="implement")
    assert owner_b.id is not None
    _set_completed(
        owner_b,
        when=datetime(2026, 5, 10, 11, 0, tzinfo=UTC),
        branch="feature/owner-b",
        has_commits=True,
    )
    store.update(owner_b)

    member_b = store.add(
        "Member B",
        task_type="improve",
        based_on=owner_b.id,
        same_branch=True,
    )
    assert member_b.id is not None
    _set_completed(
        member_b,
        when=datetime(2026, 5, 10, 12, 0, tzinfo=UTC),
        branch="feature/owner-b",
        has_commits=True,
    )
    store.update(member_b)

    git = MagicMock()
    git.can_merge.return_value = False

    with patch("gza.lineage_query.prime_advance_planning_refs") as preload:
        rows = query_lineage_owner_rows(
            store,
            LineageOwnerQuery(
                limit=None,
                include_skipped=True,
                max_recovery_attempts=1,
                owner_task_ids=(owner_a.id,),
                task_ids=(member_b.id,),
            ),
            config=config,
            git=git,
            target_branch="main",
        )

    assert rows == ()
    preload.assert_called_once_with(
        git,
        branch_names=ANY,
        target_branch="main",
        warning_logger=ANY,
    )
    assert tuple(preload.call_args.kwargs["branch_names"]) == ()




def test_query_lineage_owner_rows_hides_failed_resume_resolved_by_completed_sibling_resume(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    root = store.add("Failed plan root", task_type="plan")
    assert root.id is not None
    root.status = "failed"
    root.failure_reason = "NO_ACTIVITY"
    root.session_id = "sess-root"
    root.branch = "feature/root"
    root.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    store.update(root)

    failed_resume = store.add(root.prompt, task_type="plan", based_on=root.id, recovery_origin="resume")
    assert failed_resume.id is not None
    failed_resume.status = "failed"
    failed_resume.failure_reason = "INFRASTRUCTURE_ERROR"
    failed_resume.session_id = root.session_id
    failed_resume.branch = root.branch
    failed_resume.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    store.update(failed_resume)

    completed_resume = store.add(root.prompt, task_type="plan", based_on=root.id, recovery_origin="resume")
    assert completed_resume.id is not None
    completed_resume.status = "completed"
    completed_resume.session_id = root.session_id
    completed_resume.branch = root.branch
    completed_resume.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    store.update(completed_resume)

    implement = store.add("Completed implement", task_type="implement", based_on=completed_resume.id)
    assert implement.id is not None
    implement.status = "completed"
    implement.branch = "feature/impl"
    implement.has_commits = True
    implement.completed_at = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    store.update(implement)

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(
            limit=None,
            include_skipped=True,
            max_recovery_attempts=1,
        ),
        config=config,
        git=MagicMock(),
    )

    assert rows
    unresolved_ids = {task.id for row in rows for task in row.unresolved_tasks if task.id is not None}
    failed_leaf_ids = {
        row.recovery_leaf_task.id
        for row in rows
        if row.recovery_leaf_task is not None and row.recovery_leaf_task.id is not None
    }
    assert failed_resume.id not in unresolved_ids
    assert failed_resume.id not in failed_leaf_ids


def test_query_lineage_owner_rows_completed_explore_without_followup_needs_attention(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    explore = store.add("Explore auth provider options", task_type="explore")
    assert explore.id is not None
    _set_completed(
        explore,
        when=datetime(2026, 5, 10, 9, 0, tzinfo=UTC),
        branch=None,
        has_commits=False,
    )
    store.update(explore)

    git = MagicMock()
    git.branch_exists.return_value = True
    git.can_merge.return_value = True
    git.count_commits_behind.return_value = 0

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=git,
        target_branch="main",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.owner_task.id == explore.id
    assert row.next_action is not None
    assert row.next_action["type"] == "needs_discussion"
    assert row.next_action["needs_attention_reason"] == "explore-needs-follow-up-decision"




def test_query_lineage_owner_rows_keeps_completed_explore_with_only_dropped_plan_descendant(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    explore = store.add("Explore auth provider options", task_type="explore")
    assert explore.id is not None
    _set_completed(
        explore,
        when=datetime(2026, 5, 10, 9, 0, tzinfo=UTC),
        branch=None,
        has_commits=False,
    )
    store.update(explore)

    dropped_plan = store.add("Plan auth provider options", task_type="plan", based_on=explore.id)
    assert dropped_plan.id is not None
    dropped_plan.status = "dropped"
    dropped_plan.created_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    store.update(dropped_plan)

    git = MagicMock()
    git.can_merge.return_value = True
    git.count_commits_behind.return_value = 0

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(
            limit=None,
            include_skipped=True,
            exclude_dropped_from_planning=True,
            max_recovery_attempts=1,
        ),
        config=config,
        git=git,
        target_branch="main",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.owner_task.id == explore.id
    assert row.next_action is not None
    assert row.next_action["type"] == "needs_discussion"
    assert row.next_action["needs_attention_reason"] == "explore-needs-follow-up-decision"
    assert {task.id for task in row.unresolved_tasks if task.id is not None} == {explore.id}


def test_query_lineage_owner_rows_suppresses_completed_explore_with_pending_plan_even_when_tag_matches_only_root(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    tag = "v0.5.0"

    explore = store.add("Explore scheduler rewrite", task_type="explore", tags=(tag,))
    assert explore.id is not None
    _set_completed(
        explore,
        when=datetime(2026, 5, 10, 9, 0, tzinfo=UTC),
        branch=None,
        has_commits=False,
    )
    store.update(explore)

    plan = store.add("Plan scheduler rewrite", task_type="plan", based_on=explore.id)
    assert plan.id is not None
    plan.status = "pending"
    store.update(plan)

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, tags=(tag,), include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=MagicMock(),
        target_branch="main",
    )

    assert not rows


def test_query_lineage_owner_rows_suppresses_completed_explore_with_pending_implement_descendant(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    explore = store.add("Explore cache invalidation", task_type="explore")
    assert explore.id is not None
    _set_completed(
        explore,
        when=datetime(2026, 5, 10, 9, 0, tzinfo=UTC),
        branch=None,
        has_commits=False,
    )
    store.update(explore)

    plan = store.add("Plan cache invalidation", task_type="plan", based_on=explore.id)
    assert plan.id is not None
    _set_completed(
        plan,
        when=datetime(2026, 5, 10, 10, 0, tzinfo=UTC),
        branch=None,
        has_commits=False,
    )
    store.update(plan)

    implement = store.add("Implement cache invalidation", task_type="implement", based_on=plan.id)
    assert implement.id is not None
    implement.status = "pending"
    store.update(implement)

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=MagicMock(),
        target_branch="main",
    )

    assert not rows




def test_query_lineage_owner_rows_surfaces_held_completed_plan_as_awaiting_human(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan background jobs", task_type="plan", auto_implement=False)
    assert plan.id is not None
    _set_completed(
        plan,
        when=datetime(2026, 5, 10, 10, 0, tzinfo=UTC),
        branch=None,
        has_commits=False,
    )
    store.update(plan)

    git = MagicMock()
    git.can_merge.return_value = True
    git.count_commits_behind.return_value = 0

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=git,
        target_branch="main",
    )

    assert len(rows) == 1


def test_query_lineage_owner_rows_uses_completed_revised_plan_as_action_frontier(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan background jobs", task_type="plan")
    assert plan.id is not None
    _set_completed(
        plan,
        when=datetime(2026, 5, 10, 10, 0, tzinfo=UTC),
        branch=None,
        has_commits=False,
    )
    store.update(plan)

    initial_review = store.add("Review the original plan", task_type="plan_review", depends_on=plan.id)
    assert initial_review.id is not None
    initial_review.status = "completed"
    initial_review.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    initial_review.output_content = "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    store.update(initial_review)

    revised_plan = store.add(
        "Revise the plan",
        task_type="plan_improve",
        based_on=plan.id,
        depends_on=initial_review.id,
    )
    assert revised_plan.id is not None
    _set_completed(
        revised_plan,
        when=datetime(2026, 5, 10, 12, 0, tzinfo=UTC),
        branch=None,
        has_commits=False,
    )
    store.update(revised_plan)

    git = MagicMock()
    git.can_merge.return_value = True
    git.count_commits_behind.return_value = 0

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=git,
        target_branch="main",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.owner_task.id == revised_plan.id
    assert row.lifecycle_action_task is not None
    assert row.lifecycle_action_task.id == revised_plan.id
    assert row.next_action is not None
    assert row.next_action["type"] == "create_plan_review"
    assert [task.id for task in row.unresolved_tasks if task.id is not None] == [revised_plan.id]


def test_query_lineage_owner_rows_completed_empty_prereq_is_not_awaiting_human(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    dep = store.add("Empty prerequisite", task_type="implement")
    store.mark_completed(dep, has_commits=False, branch="feature/lineage-empty-toggle")
    assert dep.id is not None
    unit = store.create_merge_unit(
        source_branch=dep.branch,
        target_branch=store.default_merge_target(),
        owner_task_id=dep.id,
        state="empty",
    )
    store.attach_task_to_merge_unit(dep.id, unit.id, "owner")

    downstream = store.add("Held downstream", task_type="implement", depends_on=dep.id)
    assert downstream.id is not None

    git = MagicMock()
    git.can_merge.return_value = True
    git.count_commits_behind.return_value = 0

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=git,
        target_branch="main",
    )

    assert rows == ()




def test_query_lineage_owner_rows_failed_empty_prereq_surfaces_release_valve_by_default(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    dep = store.add("Failed empty prerequisite", task_type="implement")
    dep.status = "failed"
    dep.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    dep.failure_reason = "PREREQUISITE_UNMERGED"
    dep.branch = "feature/lineage-failed-empty-prereq"
    dep.has_commits = False
    store.update(dep)
    assert dep.id is not None

    unit = store.create_merge_unit(
        source_branch=dep.branch,
        target_branch="main",
        owner_task_id=dep.id,
        state="empty",
    )
    store.attach_task_to_merge_unit(dep.id, unit.id, "owner")

    downstream = store.add("Held downstream", task_type="implement", depends_on=dep.id)
    assert downstream.id is not None

    git = MagicMock()
    git.can_merge.return_value = True
    git.count_commits_behind.return_value = 0

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=git,
        target_branch="main",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.owner_task.id == downstream.id
    assert row.next_action is not None
    assert row.next_action["type"] == "awaiting_human"
    assert "empty prerequisite" in row.next_action["description"]
    assert "requires recovery or manual resolution" in row.next_action["description"]


def test_blocked_by_empty_prereq_label_uses_completed_retry_descendant_from_read_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    dependency = store.add("Failed dependency", task_type="implement")
    assert dependency.id is not None
    dependency.status = "failed"
    dependency.failure_reason = "PREREQUISITE_UNMERGED"
    dependency.completed_at = datetime(2026, 5, 16, 8, 0, tzinfo=UTC)
    store.update(dependency)

    retry = store.add("Completed retry", task_type="implement", based_on=dependency.id, recovery_origin="retry")
    assert retry.id is not None
    retry.status = "completed"
    retry.branch = "feature/retry-empty-prereq"
    retry.has_commits = False
    retry.completed_at = datetime(2026, 5, 16, 9, 0, tzinfo=UTC)
    store.update(retry)

    retry_unit = store.create_merge_unit(
        source_branch=retry.branch,
        target_branch="main",
        owner_task_id=retry.id,
        state="empty",
    )
    store.attach_task_to_merge_unit(retry.id, retry_unit.id, "owner")

    downstream = store.add("Held downstream", task_type="implement", depends_on=dependency.id)
    assert downstream.id is not None

    read_context = _read_context_for_store(store)
    store_label = blocked_by_empty_prereq_label(store, downstream)
    assert store_label is None

    def _unexpected_store_lookup(*_args, **_kwargs):
        raise AssertionError("indexed retry-descendant prerequisite resolution should not hit the store")

    monkeypatch.setattr(store, "get", _unexpected_store_lookup)
    monkeypatch.setattr(store, "resolve_merge_unit_for_task", _unexpected_store_lookup)
    monkeypatch.setattr(store, "resolve_dependency_completion", _unexpected_store_lookup)

    assert blocked_by_empty_prereq_label(store, downstream, read_context=read_context) == store_label


def test_query_lineage_owner_rows_failed_empty_prereq_policy_toggle_suppresses_release_valve(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    dep = store.add("Failed empty prerequisite", task_type="implement")
    dep.status = "failed"
    dep.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    dep.failure_reason = "PREREQUISITE_UNMERGED"
    dep.branch = "feature/lineage-failed-empty-toggle"
    dep.has_commits = False
    store.update(dep)
    assert dep.id is not None

    unit = store.create_merge_unit(
        source_branch=dep.branch,
        target_branch="main",
        owner_task_id=dep.id,
        state="empty",
    )
    store.attach_task_to_merge_unit(dep.id, unit.id, "owner")

    downstream = store.add("Held downstream", task_type="implement", depends_on=dep.id)
    assert downstream.id is not None

    monkeypatch.setattr(
        dependency_preconditions_module,
        "empty_prereq_satisfies_dependency",
        lambda _store, _prereq, _dependent: True,
    )

    git = MagicMock()
    git.can_merge.return_value = True
    git.count_commits_behind.return_value = 0

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=git,
        target_branch="main",
    )

    assert not rows


def test_query_lineage_owner_rows_surfaces_manual_review_creation_attention(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "advance_create_reviews: false\n")
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement background jobs", task_type="implement")
    assert impl.id is not None
    _set_completed(
        impl,
        when=datetime(2026, 5, 10, 10, 0, tzinfo=UTC),
        branch="feature/manual-review-creation",
        has_commits=True,
    )
    impl.merge_status = "unmerged"
    store.update(impl)

    git = MagicMock()
    git.can_merge.return_value = True
    git.count_commits_behind.return_value = 0

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=git,
        target_branch="main",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.owner_task.id == impl.id
    assert row.next_action is not None
    assert row.next_action["type"] == "needs_discussion"
    assert row.next_action["needs_attention_reason"] == "review-needs-manual-creation"
    assert row.next_action["subject_task_id"] == impl.id
    assert "run gza review manually" in row.next_action["description"]




def test_query_lineage_owner_rows_marks_orphan_only_impl_lineage_for_manual_resolution(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "in_progress"
    impl.branch = "feature/canonical"
    impl.has_commits = True
    store.update(impl)

    orphan = store.add("Completed orphan rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    assert orphan.id is not None
    _set_completed(
        orphan,
        when=datetime(2026, 5, 12, 11, 0, tzinfo=UTC),
        branch="feature/orphan",
        has_commits=True,
    )
    orphan.merge_status = "unmerged"
    store.update(orphan)
    orphan_unit = store.create_merge_unit(
        source_branch=orphan.branch,
        target_branch="main",
        owner_task_id=orphan.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(orphan.id, orphan_unit.id, "owner")

    git = MagicMock()
    git.can_merge.return_value = True
    git.count_commits_behind.return_value = 0

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=git,
        target_branch="main",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.owner_task.id == impl.id
    assert row.lifecycle_action_task is None
    assert row.next_action is not None
    assert row.next_action["type"] == "needs_discussion"
    assert row.next_action["needs_attention_reason"] == "no-descendant-on-the-impl-branch"
    assert row.next_action["subject_task_id"] == impl.id
    assert "no descendant on the impl branch" in row.next_action["description"]
    assert {task.id for task in row.unresolved_tasks if task.id is not None} == {orphan.id}






def test_query_lineage_owner_rows_planning_skips_dropped_owner_lineage(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Dropped implement owner", task_type="implement")
    assert impl.id is not None
    _set_dropped(
        impl,
        when=datetime(2026, 5, 12, 9, 0, tzinfo=UTC),
        branch="feature/dropped-owner",
        has_commits=True,
    )
    impl.merge_status = "unmerged"
    store.update(impl)

    descendant = store.add("Completed descendant rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    assert descendant.id is not None
    _set_completed(
        descendant,
        when=datetime(2026, 5, 12, 10, 0, tzinfo=UTC),
        branch="feature/dropped-owner",
        has_commits=True,
    )
    descendant.merge_status = "unmerged"
    store.update(descendant)

    git = MagicMock()
    git.can_merge.return_value = True
    git.count_commits_behind.return_value = 0

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(
            limit=None,
            include_skipped=True,
            exclude_dropped_from_planning=True,
            max_recovery_attempts=1,
        ),
        config=config,
        git=git,
        target_branch="main",
    )

    assert rows == ()


def test_query_lineage_owner_rows_planning_skips_dropped_only_descendants(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _set_completed(
        impl,
        when=datetime(2026, 5, 12, 9, 0, tzinfo=UTC),
        branch="feature/dropped-only-descendants",
        has_commits=True,
    )
    impl.merge_status = "merged"
    store.update(impl)

    dropped_rebase = store.add("Dropped rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    assert dropped_rebase.id is not None
    _set_dropped(
        dropped_rebase,
        when=datetime(2026, 5, 12, 10, 0, tzinfo=UTC),
        branch=impl.branch,
        has_commits=True,
    )
    dropped_rebase.merge_status = "unmerged"
    store.update(dropped_rebase)

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(
            limit=None,
            include_skipped=True,
            exclude_dropped_from_planning=True,
            max_recovery_attempts=1,
        ),
        config=config,
        git=MagicMock(),
        target_branch="main",
    )

    assert rows == ()




def test_query_lineage_owner_rows_hides_failed_root_resolved_by_completed_recovery_descendant(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-root"
    failed.branch = "feature/recovered-lineage"
    failed.completed_at = datetime(2026, 5, 12, 9, 0, tzinfo=UTC)
    store.update(failed)

    resumed = store.add(failed.prompt, task_type="implement", based_on=failed.id)
    assert resumed.id is not None
    _set_completed(
        resumed,
        when=datetime(2026, 5, 12, 10, 0, tzinfo=UTC),
        branch=failed.branch,
        has_commits=True,
    )
    resumed.merge_status = "merged"
    resumed.session_id = failed.session_id
    store.update(resumed)

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=MagicMock(),
        target_branch="main",
    )

    assert rows == ()


def test_query_lineage_owner_rows_tag_scope_keeps_untagged_recovery_descendant_resolution(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    tag = "v0.5.0"

    failed = store.add("Failed implement", task_type="implement", tags=(tag,))
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-root"
    failed.branch = "feature/recovered-lineage-tagged"
    failed.completed_at = datetime(2026, 5, 12, 9, 0, tzinfo=UTC)
    store.update(failed)

    resumed = store.add(failed.prompt, task_type="implement", based_on=failed.id)
    assert resumed.id is not None
    _set_completed(
        resumed,
        when=datetime(2026, 5, 12, 10, 0, tzinfo=UTC),
        branch=failed.branch,
        has_commits=True,
    )
    resumed.merge_status = "merged"
    resumed.session_id = failed.session_id
    store.update(resumed)

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, tags=(tag,), include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=MagicMock(),
        target_branch="main",
    )

    assert rows == ()




def test_query_lineage_owner_rows_reroots_same_unit_failed_leaf_when_proof_unavailable(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    owner = store.add("Merged owner without live proof", task_type="implement")
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 5, 17, 9, 0, tzinfo=UTC),
        branch="feature/unavailable-terminal-proof",
        has_commits=True,
    )
    owner.merge_status = "merged"
    owner.merged_at = datetime(2026, 5, 17, 9, 30, tzinfo=UTC)
    store.update(owner)

    owner_unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(owner.id, owner_unit.id, "owner")

    failed_leaf = store.add(
        "Failed implement leaf with unavailable proof",
        task_type="implement",
        based_on=owner.id,
        recovery_origin="manual",
        same_branch=True,
    )
    assert failed_leaf.id is not None
    failed_leaf.status = "failed"
    failed_leaf.failure_reason = "WORKER_DIED"
    failed_leaf.branch = owner.branch
    failed_leaf.has_commits = True
    failed_leaf.completed_at = datetime(2026, 5, 17, 10, 0, tzinfo=UTC)
    store.update(failed_leaf)
    store.attach_task_to_merge_unit(failed_leaf.id, owner_unit.id, "implement")

    assert owner.branch is not None
    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=None,
        target_branch="main",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.owner_task.id == failed_leaf.id
    assert row.recovery_action_task is not None
    assert row.recovery_action_task.id == failed_leaf.id
    assert row.recovery_leaf_task is not None
    assert row.recovery_leaf_task.id == failed_leaf.id
    assert row.unresolved_tasks == (failed_leaf,)


def test_query_lineage_owner_rows_reroots_branchless_failed_review_leaf_under_merged_owner(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    owner = store.add("Merged owner with dead review leaf", task_type="implement")
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 5, 17, 9, 0, tzinfo=UTC),
        branch="feature/merged-owner-dead-review",
        has_commits=True,
    )
    owner.merge_status = "merged"
    owner.merged_at = datetime(2026, 5, 17, 9, 30, tzinfo=UTC)
    store.update(owner)

    owner_unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(owner.id, owner_unit.id, "owner")

    failed_review = store.add(
        "Failed review from an earlier lap",
        task_type="review",
        based_on=owner.id,
        depends_on=owner.id,
    )
    assert failed_review.id is not None
    failed_review.status = "failed"
    failed_review.failure_reason = "CONFIG_ERROR"
    failed_review.completed_at = datetime(2026, 5, 17, 8, 0, tzinfo=UTC)
    store.update(failed_review)
    store.attach_task_to_merge_unit(failed_review.id, owner_unit.id, "review")

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=None,
        target_branch="main",
    )

    assert len(rows) == 1
    assert rows[0].owner_task.id == failed_review.id
    assert rows[0].recovery_leaf_task is not None
    assert rows[0].recovery_leaf_task.id == failed_review.id


def test_query_lineage_owner_rows_keeps_unmerged_owner_with_failed_review_leaf(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    owner = store.add("Unmerged owner with dead review leaf", task_type="implement")
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 5, 17, 9, 0, tzinfo=UTC),
        branch="feature/unmerged-owner-dead-review",
        has_commits=True,
    )
    owner.merge_status = "unmerged"
    store.update(owner)

    owner_unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(owner.id, owner_unit.id, "owner")

    failed_review = store.add(
        "Failed review still blocks unmerged owner",
        task_type="review",
        based_on=owner.id,
        depends_on=owner.id,
    )
    assert failed_review.id is not None
    failed_review.status = "failed"
    failed_review.failure_reason = "CONFIG_ERROR"
    failed_review.completed_at = datetime(2026, 5, 17, 10, 0, tzinfo=UTC)
    store.update(failed_review)
    store.attach_task_to_merge_unit(failed_review.id, owner_unit.id, "review")

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=None,
        target_branch="main",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.owner_task.id == owner.id
    assert row.recovery_leaf_task is not None
    assert row.recovery_leaf_task.id == failed_review.id


def test_query_lineage_owner_rows_keeps_legitimate_impl_branch_rebase_descendant_actionable(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _set_completed(
        impl,
        when=datetime(2026, 5, 12, 9, 0, tzinfo=UTC),
        branch="feature/canonical",
        has_commits=True,
    )
    impl.merge_status = "unmerged"
    store.update(impl)

    review = store.add("Approved review", task_type="review", depends_on=impl.id, based_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 12, 10, 0, tzinfo=UTC)
    review.output_content = "**Verdict: APPROVED**"
    store.update(review)

    descendant = store.add("Completed descendant rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    assert descendant.id is not None
    _set_completed(
        descendant,
        when=datetime(2026, 5, 12, 11, 0, tzinfo=UTC),
        branch="feature/canonical",
        has_commits=True,
    )
    descendant.changed_diff = False
    descendant.merge_status = "unmerged"
    store.update(descendant)

    git = MagicMock()
    git.can_merge.return_value = True
    git.count_commits_behind.return_value = 0
    git.branch_exists.return_value = True
    git.rev_parse_if_exists.side_effect = lambda ref: (
        "same-head" if ref == "feature/canonical" else "base-head" if ref == "main" else None
    )
    _persist_current_green_verify(store, config, owner_task=impl, source_task=review, head_sha="same-head")

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=git,
        target_branch="main",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.owner_task.id == impl.id
    assert row.lifecycle_action_task is not None
    assert row.lifecycle_action_task.id == descendant.id
    assert row.next_action is not None
    assert row.next_action["type"] == "merge"








def test_query_lineage_owner_rows_mergeable_behind_branch_projects_rebase(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    impl = store.add("Implement stale lineage", task_type="implement")
    assert impl.id is not None
    _set_completed(
        impl,
        when=datetime(2026, 5, 15, 9, 0, tzinfo=UTC),
        branch="feature/stale-lineage",
        has_commits=True,
    )
    impl.merge_status = "unmerged"
    store.update(impl)

    git = MagicMock()
    git.can_merge.return_value = True
    git.count_commits_behind.return_value = 0
    git.branch_exists.return_value = True
    git.rev_parse_if_exists.side_effect = lambda ref: (
        "same-head" if ref == "feature/stale-lineage" else "base-head" if ref == "main" else None
    )
    git.resolve_fresh_merge_source.return_value = ("origin/feature/stale-lineage", None)
    git.count_commits_behind.return_value = 1
    _persist_current_green_verify(store, config, owner_task=impl, source_task=impl, head_sha="same-head")

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=git,
        target_branch="main",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.lifecycle_action_task is not None
    assert row.lifecycle_action_task.id == impl.id
    assert row.next_action is not None
    assert row.next_action["type"] == "needs_rebase"
    assert row.next_action["reason"] == "pre-dispatch-rebase"
    assert row.lineage_status == "actionable"


def test_query_lineage_owner_rows_projects_merge_for_approved_clean_behind_branch(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    impl = store.add("Implement approved stale lineage", task_type="implement")
    assert impl.id is not None
    _set_completed(
        impl,
        when=datetime(2026, 5, 15, 9, 0, tzinfo=UTC),
        branch="feature/approved-stale-lineage",
        has_commits=True,
    )
    impl.merge_status = "unmerged"
    store.update(impl)

    review = store.add(
        "Approved review",
        task_type="review",
        depends_on=impl.id,
        based_on=impl.id,
    )
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 15, 10, 0, tzinfo=UTC)
    review.output_content = "**Verdict: APPROVED**"
    store.update(review)

    git = MagicMock()
    git.can_merge.return_value = True
    git.count_commits_behind.return_value = 0
    git.branch_exists.return_value = True
    git.rev_parse_if_exists.side_effect = lambda ref: (
        "same-head" if ref == "feature/approved-stale-lineage" else "base-head" if ref == "main" else None
    )
    git.resolve_fresh_merge_source.return_value = (
        "origin/feature/approved-stale-lineage",
        None,
    )
    git.count_commits_behind.return_value = 1
    _persist_current_green_verify(store, config, owner_task=impl, source_task=review, head_sha="same-head")

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=git,
        target_branch="main",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.lifecycle_action_task is not None
    assert row.lifecycle_action_task.id == impl.id
    assert row.next_action is not None
    assert row.next_action["type"] == "merge"
    assert row.lineage_status == "actionable"


def test_query_lineage_owner_rows_needs_merge_excludes_empty_merge_units(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    empty_task = store.add("Completed empty implement", task_type="implement")
    store.mark_completed(empty_task, has_commits=True, branch="feature/empty-needs-merge")
    assert empty_task.id is not None

    empty_unit = store.resolve_merge_unit_for_task(empty_task.id)
    assert empty_unit is not None
    store.set_merge_unit_state(empty_unit.id, "empty")

    merge_task = store.add("Completed real implement", task_type="implement")
    store.mark_completed(merge_task, has_commits=True, branch="feature/real-needs-merge")
    assert merge_task.id is not None

    merge_unit = store.resolve_merge_unit_for_task(merge_task.id)
    assert merge_unit is not None
    store.set_merge_unit_state(merge_unit.id, "unmerged")

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(
            limit=None,
            statuses=("completed",),
            merge_chain_state=("needs_merge",),
            include_skipped=True,
            max_recovery_attempts=1,
        ),
        config=config,
        git=MagicMock(),
        target_branch="main",
    )

    owner_ids = {row.owner_task.id for row in rows if row.owner_task.id is not None}
    assert empty_task.id not in owner_ids
    assert merge_task.id in owner_ids








def test_collect_recovery_lane_entries_uses_one_read_session_connection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement owner", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-reuse"
    failed.branch = "feature/reuse"
    failed.num_steps_computed = 3
    failed.completed_at = datetime(2026, 5, 16, 9, 0, tzinfo=UTC)
    store.update(failed)

    unit = store.create_merge_unit(
        source_branch=failed.branch,
        target_branch="main",
        owner_task_id=failed.id,
        state="empty",
    )
    store.attach_task_to_merge_unit(failed.id, unit.id, "owner")

    opened_connections: list[tuple[bool, object]] = []
    original_open_connection = store._open_connection

    def _tracking_open_connection(*, close_on_exit: bool):
        conn = original_open_connection(close_on_exit=close_on_exit)
        opened_connections.append((close_on_exit, conn))
        return conn

    monkeypatch.setattr(store, "_open_connection", _tracking_open_connection)

    entries = collect_recovery_lane_entries(
        store,
        tags=None,
        any_tag=False,
        max_recovery_attempts=1,
    )

    assert [entry.task.id for entry in entries] == [failed.id]
    assert len([conn for close_on_exit, conn in opened_connections if close_on_exit is False]) == 1


def test_query_lineage_owner_rows_builds_owner_trees_without_store_lineage_child_queries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    owner = store.add("Failed implement owner", task_type="implement")
    assert owner.id is not None
    owner.status = "failed"
    owner.failure_reason = "TEST_FAILURE"
    owner.session_id = "tree-session"
    owner.branch = "feature/tree-owner"
    owner.completed_at = datetime(2026, 5, 16, 8, 0, tzinfo=UTC)
    store.update(owner)

    child = store.add("Pending improve child", task_type="improve", based_on=owner.id, same_branch=True)
    assert child.id is not None

    grandchild = store.add("Pending review grandchild", task_type="review", depends_on=child.id)
    assert grandchild.id is not None

    lineage_child_calls: list[str] = []
    original_get_lineage_children = store.get_lineage_children

    def _counting_get_lineage_children(task_id: str):
        lineage_child_calls.append(task_id)
        return original_get_lineage_children(task_id)

    monkeypatch.setattr(store, "get_lineage_children", _counting_get_lineage_children)

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True),
    )

    assert [row.owner_task.id for row in rows] == [owner.id]
    assert rows[0].tree is not None
    assert rows[0].tree.task.id == owner.id
    assert [task.id for task in rows[0].members] == [owner.id]
    assert lineage_child_calls == []






def test_blocked_by_empty_prereq_label_matches_indexed_context_for_direct_empty_dependency(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    dependency = store.add("Empty dependency", task_type="implement")
    assert dependency.id is not None
    dependency.status = "completed"
    dependency.branch = "feature/direct-empty"
    dependency.completed_at = datetime(2026, 5, 16, 8, 0, tzinfo=UTC)
    store.update(dependency)

    unit = store.create_merge_unit(
        source_branch=dependency.branch,
        target_branch="main",
        owner_task_id=dependency.id,
        state="empty",
    )
    store.attach_task_to_merge_unit(dependency.id, unit.id, "owner")

    dependent = store.add("Dependent task", task_type="implement", depends_on=dependency.id)
    assert dependent.id is not None

    read_context = _read_context_for_store(store)
    assert blocked_by_empty_prereq_label(store, dependent) == blocked_by_empty_prereq_label(
        store,
        dependent,
        read_context=read_context,
    )


def test_blocked_by_empty_prereq_label_matches_indexed_context_for_completed_retry_descendant(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    dependency = store.add("Failed dependency", task_type="implement")
    assert dependency.id is not None
    dependency.status = "failed"
    dependency.failure_reason = "INFRASTRUCTURE_ERROR"
    dependency.completed_at = datetime(2026, 5, 16, 8, 0, tzinfo=UTC)
    store.update(dependency)

    completed_retry = store.add("Completed retry", task_type="implement", based_on=dependency.id)
    assert completed_retry.id is not None
    completed_retry.status = "completed"
    completed_retry.branch = "feature/retry-empty"
    completed_retry.completed_at = datetime(2026, 5, 16, 9, 0, tzinfo=UTC)
    store.update(completed_retry)

    unit = store.create_merge_unit(
        source_branch=completed_retry.branch,
        target_branch="main",
        owner_task_id=completed_retry.id,
        state="empty",
    )
    store.attach_task_to_merge_unit(completed_retry.id, unit.id, "owner")

    dependent = store.add("Dependent task", task_type="implement", depends_on=dependency.id)
    assert dependent.id is not None

    read_context = _read_context_for_store(store)
    assert blocked_by_empty_prereq_label(store, dependent) == blocked_by_empty_prereq_label(
        store,
        dependent,
        read_context=read_context,
    )


def test_blocked_by_empty_prereq_label_ignores_failed_parent_empty_when_retry_descendant_is_unmerged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    dependency = store.add("Failed dependency", task_type="implement")
    assert dependency.id is not None
    dependency.status = "failed"
    dependency.failure_reason = "UNKNOWN"
    dependency.branch = "feature/parent-empty-only"
    dependency.has_commits = False
    dependency.completed_at = datetime(2026, 5, 16, 8, 0, tzinfo=UTC)
    store.update(dependency)

    parent_unit = store.create_merge_unit(
        source_branch=dependency.branch,
        target_branch="main",
        owner_task_id=dependency.id,
        state="empty",
    )
    store.attach_task_to_merge_unit(dependency.id, parent_unit.id, "owner")

    retry = store.add("Completed retry", task_type="implement", based_on=dependency.id, recovery_origin="retry")
    assert retry.id is not None
    retry.status = "completed"
    retry.branch = "feature/retry-still-unmerged"
    retry.has_commits = True
    retry.completed_at = datetime(2026, 5, 16, 9, 0, tzinfo=UTC)
    retry.merge_status = "unmerged"
    store.update(retry)

    retry_unit = store.create_merge_unit(
        source_branch=retry.branch,
        target_branch="main",
        owner_task_id=retry.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(retry.id, retry_unit.id, "owner")

    downstream = store.add("Held downstream", task_type="implement", depends_on=dependency.id)
    assert downstream.id is not None

    read_context = _read_context_for_store(store)
    store_label = blocked_by_empty_prereq_label(store, downstream)
    assert store_label is None

    def _unexpected_store_lookup(*_args, **_kwargs):
        raise AssertionError("indexed prerequisite label resolution should not hit the store")

    monkeypatch.setattr(store, "get", _unexpected_store_lookup)
    monkeypatch.setattr(store, "resolve_merge_unit_for_task", _unexpected_store_lookup)
    monkeypatch.setattr(store, "resolve_dependency_completion", _unexpected_store_lookup)

    assert blocked_by_empty_prereq_label(store, downstream, read_context=read_context) == store_label


def test_read_context_preserves_store_based_on_child_order(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Failed implementation", task_type="implement")
    assert parent.id is not None
    parent.status = "failed"
    parent.failure_reason = "INFRASTRUCTURE_ERROR"
    store.update(parent)

    older = store.add(parent.prompt, task_type="implement", based_on=parent.id, recovery_origin="retry")
    assert older.id is not None
    older.status = "pending"
    store.update(older)

    newer = store.add(parent.prompt, task_type="implement", based_on=parent.id, recovery_origin="retry")
    assert newer.id is not None
    newer.status = "pending"
    store.update(newer)

    read_context = _read_context_for_store(store)
    assert [task.id for task in store.get_based_on_children(parent.id)] == [
        task.id for task in read_context.get_based_on_children(parent.id)
    ]
    assert [task.id for task in store.get_based_on_children_by_type(parent.id, "implement")] == [
        task.id for task in read_context.get_based_on_children_by_type(parent.id, "implement")
    ]


def test_read_context_dependency_completion_matches_store_oldest_completed_retry_descendant(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    dependency = store.add("Failed dependency", task_type="implement")
    assert dependency.id is not None
    dependency.status = "failed"
    dependency.failure_reason = "INFRASTRUCTURE_ERROR"
    dependency.completed_at = datetime(2026, 5, 16, 8, 30, tzinfo=UTC)
    store.update(dependency)

    older_retry = store.add(
        "Completed retry older", task_type="implement", based_on=dependency.id, recovery_origin="retry"
    )
    assert older_retry.id is not None
    older_retry.status = "completed"
    older_retry.completed_at = datetime(2026, 5, 16, 9, 30, tzinfo=UTC)
    store.update(older_retry)

    newer_retry = store.add(
        "Completed retry newer", task_type="implement", based_on=dependency.id, recovery_origin="retry"
    )
    assert newer_retry.id is not None
    newer_retry.status = "completed"
    newer_retry.completed_at = datetime(2026, 5, 16, 10, 30, tzinfo=UTC)
    store.update(newer_retry)

    dependent = store.add("Dependent task", task_type="implement", depends_on=dependency.id)
    assert dependent.id is not None

    read_context = _read_context_for_store(store)
    store_resolved = store.resolve_dependency_completion(dependent)
    indexed_resolved = read_context.resolve_dependency_completion(dependent)

    assert store_resolved is not None
    assert indexed_resolved is not None
    assert store_resolved.id == older_retry.id
    assert indexed_resolved.id == store_resolved.id


def test_load_indexes_prefers_latest_active_merge_unit_per_task(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Implementation", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.branch = "feature/tie-break"
    task.completed_at = datetime(2026, 5, 16, 8, 0, tzinfo=UTC)
    store.update(task)

    older = store.create_merge_unit(
        source_branch=task.branch,
        target_branch="main",
        owner_task_id=task.id,
        state="unmerged",
    )
    newer = store.create_merge_unit(
        source_branch=task.branch,
        target_branch="main",
        owner_task_id=task.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(task.id, older.id, "owner")
    store.attach_task_to_merge_unit(task.id, newer.id, "owner")

    indexes = _load_indexes(store)
    assert indexes.merge_units_by_task_id[task.id].id == newer.id


def test_load_indexes_matches_merge_unit_updated_at_then_id_tie_break(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Implementation", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.branch = "feature/merge-unit-tie-break"
    task.completed_at = datetime(2026, 5, 16, 8, 0, tzinfo=UTC)
    store.update(task)

    first = store.create_merge_unit(
        source_branch=task.branch,
        target_branch="main",
        owner_task_id=task.id,
        state="unmerged",
    )
    second = store.create_merge_unit(
        source_branch=task.branch,
        target_branch="main",
        owner_task_id=task.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(task.id, first.id, "owner")
    store.attach_task_to_merge_unit(task.id, second.id, "owner")

    with store._connect() as conn:
        conn.execute(
            "UPDATE merge_units SET updated_at = ? WHERE id IN (?, ?)",
            ("2026-05-16 09:00:00", first.id, second.id),
        )

    resolved = store.resolve_merge_unit_for_task(task.id)
    indexes = _load_indexes(store)

    assert resolved is not None
    assert indexes.merge_units_by_task_id[task.id].id == resolved.id
    assert resolved.id == max(first.id, second.id)




def test_query_lineage_owner_rows_hides_branchless_moot_prerequisite_unmerged_failed_owner_from_recovery_lane(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    dependency = store.add("Merged dependency", task_type="implement")
    assert dependency.id is not None
    dependency.status = "completed"
    dependency.merge_status = "merged"
    dependency.completed_at = datetime(2026, 5, 16, 8, 0, tzinfo=UTC)
    store.update(dependency)

    failed = store.add("Historical blocked implementation", task_type="implement", depends_on=dependency.id)
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PREREQUISITE_UNMERGED"
    failed.has_commits = False
    failed.completed_at = datetime(2026, 5, 16, 9, 0, tzinfo=UTC)
    store.update(failed)

    assert list_failed_tasks_for_recovery(store) == []

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=MagicMock(),
        target_branch="main",
    )

    failed_leaf_ids = {
        row.recovery_leaf_task.id
        for row in rows
        if row.recovery_leaf_task is not None and row.recovery_leaf_task.id is not None
    }
    assert failed.id not in failed_leaf_ids

    entries = collect_recovery_lane_entries(
        store,
        tags=None,
        any_tag=False,
        max_recovery_attempts=1,
    )

    assert [entry.task.id for entry in entries] == []


def test_query_lineage_owner_rows_tag_scope_keeps_untagged_dependency_resolution(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    tag = "v0.5.0"

    dependency = store.add("Merged dependency", task_type="implement")
    assert dependency.id is not None
    dependency.status = "completed"
    dependency.merge_status = "merged"
    dependency.completed_at = datetime(2026, 5, 16, 8, 0, tzinfo=UTC)
    store.update(dependency)

    failed = store.add(
        "Historical blocked implementation", task_type="implement", depends_on=dependency.id, tags=(tag,)
    )
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PREREQUISITE_UNMERGED"
    failed.has_commits = False
    failed.completed_at = datetime(2026, 5, 16, 9, 0, tzinfo=UTC)
    store.update(failed)

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, tags=(tag,), include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=MagicMock(),
        target_branch="main",
    )

    failed_leaf_ids = {
        row.recovery_leaf_task.id
        for row in rows
        if row.recovery_leaf_task is not None and row.recovery_leaf_task.id is not None
    }
    assert failed.id not in failed_leaf_ids


def test_query_lineage_owner_rows_keeps_terminal_owner_visible_for_failed_leaf_with_unique_unmerged_work(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    owner = store.add("Merged owner", task_type="implement")
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 5, 18, 8, 0, tzinfo=UTC),
        branch="feature/merged-owner-visible",
        has_commits=True,
    )
    owner.merge_status = "merged"
    store.update(owner)

    owner_unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(owner.id, owner_unit.id, "owner")

    failed_leaf = store.add(
        "Failed implement leaf with distinct unmerged work",
        task_type="implement",
        based_on=owner.id,
        recovery_origin="manual",
    )
    assert failed_leaf.id is not None
    failed_leaf.status = "failed"
    failed_leaf.failure_reason = "WORKER_DIED"
    failed_leaf.branch = "feature/merged-owner-visible-followup"
    failed_leaf.has_commits = True
    failed_leaf.completed_at = datetime(2026, 5, 18, 9, 0, tzinfo=UTC)
    store.update(failed_leaf)

    failed_leaf_unit = store.create_merge_unit(
        source_branch=failed_leaf.branch,
        target_branch="main",
        owner_task_id=failed_leaf.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(failed_leaf.id, failed_leaf_unit.id, "implement")

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=None,
        target_branch="main",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.owner_task.id == failed_leaf.id
    assert row.recovery_leaf_task is not None
    assert row.recovery_leaf_task.id == failed_leaf.id
    assert failed_leaf in row.unresolved_tasks


def test_query_lineage_owner_rows_tag_scope_keeps_matching_rerooted_failed_leaf_under_nonmatching_terminal_owner(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    tag = "selected-reroot"

    owner = store.add("Terminal owner outside tag scope", task_type="implement", tags=("other",))
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 7, 8, 8, 0, tzinfo=UTC),
        branch="feature/nonmatching-terminal-owner",
        has_commits=True,
    )
    owner.merge_status = "merged"
    store.update(owner)
    owner_unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(owner.id, owner_unit.id, "owner")

    failed_leaf = store.add(
        "Matching failed leaf with distinct unmerged work",
        task_type="improve",
        based_on=owner.id,
        tags=(tag,),
    )
    assert failed_leaf.id is not None
    failed_leaf.status = "failed"
    failed_leaf.failure_reason = "WORKER_DIED"
    failed_leaf.branch = "feature/nonmatching-terminal-owner-matching-leaf"
    failed_leaf.has_commits = True
    failed_leaf.completed_at = datetime(2026, 7, 8, 9, 0, tzinfo=UTC)
    store.update(failed_leaf)
    failed_leaf_unit = store.create_merge_unit(
        source_branch=failed_leaf.branch,
        target_branch="main",
        owner_task_id=failed_leaf.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(failed_leaf.id, failed_leaf_unit.id, "owner")

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, tags=(tag,), include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=None,
        target_branch="main",
    )

    assert [row.owner_task.id for row in rows] == [failed_leaf.id]
    row = rows[0]
    assert row.recovery_action_task is not None
    assert row.recovery_action_task.id == failed_leaf.id
    assert row.recovery_leaf_task is not None
    assert row.recovery_leaf_task.id == failed_leaf.id
    assert [task.id for task in row.unresolved_tasks] == [failed_leaf.id]


def test_query_lineage_owner_rows_tag_scope_keeps_branchless_unknown_failed_leaf_under_nonmatching_merged_owner(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    tag = "selected-branchless-reroot"

    owner = store.add("Merged owner outside branchless tag scope", task_type="implement", tags=("other",))
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 7, 9, 8, 0, tzinfo=UTC),
        branch="feature/nonmatching-branchless-owner",
        has_commits=True,
    )
    owner.merge_status = "merged"
    store.update(owner)
    owner_unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(owner.id, owner_unit.id, "owner")

    failed_leaf = store.add(
        "Matching branchless failed review leaf",
        task_type="review",
        based_on=owner.id,
        depends_on=owner.id,
        tags=(tag,),
    )
    assert failed_leaf.id is not None
    failed_leaf.status = "failed"
    failed_leaf.failure_reason = "CONFIG_ERROR"
    failed_leaf.completed_at = datetime(2026, 7, 9, 9, 0, tzinfo=UTC)
    store.update(failed_leaf)
    store.attach_task_to_merge_unit(failed_leaf.id, owner_unit.id, "review")

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, tags=(tag,), include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=None,
        target_branch="main",
    )

    assert [row.owner_task.id for row in rows] == [failed_leaf.id]
    row = rows[0]
    assert row.recovery_leaf_task is not None
    assert row.recovery_leaf_task.id == failed_leaf.id
    assert [task.id for task in row.unresolved_tasks] == [failed_leaf.id]


@pytest.mark.parametrize("terminal_state", ["empty", "redundant"])
def test_query_lineage_owner_rows_tag_scope_keeps_failed_leaf_under_nonmatching_no_work_terminal_owner(
    tmp_path: Path,
    terminal_state: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    tag = f"selected-{terminal_state}-reroot"

    owner = store.add("No-work terminal owner outside tag scope", task_type="implement", tags=("other",))
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 7, 10, 8, 0, tzinfo=UTC),
        branch=f"feature/nonmatching-{terminal_state}-owner",
        has_commits=False,
    )
    owner.merge_status = terminal_state
    store.update(owner)
    owner_unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state=terminal_state,
    )
    store.attach_task_to_merge_unit(owner.id, owner_unit.id, "owner")

    failed_leaf = store.add(
        "Matching failed leaf under no-work owner",
        task_type="improve",
        based_on=owner.id,
        tags=(tag,),
    )
    assert failed_leaf.id is not None
    failed_leaf.status = "failed"
    failed_leaf.failure_reason = "WORKER_DIED"
    failed_leaf.branch = owner.branch
    failed_leaf.session_id = f"sess-{terminal_state}-reroot"
    failed_leaf.num_steps_computed = 1
    failed_leaf.completed_at = datetime(2026, 7, 10, 9, 0, tzinfo=UTC)
    store.update(failed_leaf)
    store.attach_task_to_merge_unit(failed_leaf.id, owner_unit.id, "improve")

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, tags=(tag,), include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=None,
        target_branch="main",
    )

    assert [row.owner_task.id for row in rows] == [failed_leaf.id]
    row = rows[0]
    assert row.recovery_leaf_task is not None
    assert row.recovery_leaf_task.id == failed_leaf.id
    assert [task.id for task in row.unresolved_tasks] == [failed_leaf.id]


def test_query_lineage_owner_rows_tag_scope_keeps_no_commit_failed_leaf_with_resumable_evidence(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    tag = "selected-resumable-reroot"

    owner = store.add("Merged owner outside resumable tag scope", task_type="implement", tags=("other",))
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 7, 11, 8, 0, tzinfo=UTC),
        branch="feature/nonmatching-resumable-owner",
        has_commits=True,
    )
    owner.merge_status = "merged"
    store.update(owner)
    owner_unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(owner.id, owner_unit.id, "owner")

    failed_leaf = store.add(
        "Matching no-commit failed leaf with resumable evidence",
        task_type="improve",
        based_on=owner.id,
        recovery_origin="manual",
        same_branch=True,
        tags=(tag,),
    )
    assert failed_leaf.id is not None
    failed_leaf.status = "failed"
    failed_leaf.failure_reason = "WORKER_DIED"
    failed_leaf.branch = owner.branch
    failed_leaf.has_commits = False
    failed_leaf.session_id = "sess-resumable-reroot"
    failed_leaf.num_steps_computed = 1
    failed_leaf.completed_at = datetime(2026, 7, 11, 9, 0, tzinfo=UTC)
    store.update(failed_leaf)
    store.attach_task_to_merge_unit(failed_leaf.id, owner_unit.id, "improve")

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, tags=(tag,), include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=None,
        target_branch="main",
    )

    assert [row.owner_task.id for row in rows] == [failed_leaf.id]
    row = rows[0]
    assert row.recovery_action_task is not None
    assert row.recovery_action_task.id == failed_leaf.id
    assert row.recovery_leaf_task is not None
    assert row.recovery_leaf_task.id == failed_leaf.id
    assert [task.id for task in row.unresolved_tasks] == [failed_leaf.id]


def test_query_lineage_owner_rows_tag_scope_skips_nonmatching_rerooted_failed_leaf_resolution(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    tag = "selected-owner"

    owner = store.add("Matching terminal owner", task_type="implement", tags=(tag,))
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 7, 8, 8, 0, tzinfo=UTC),
        branch="feature/matching-terminal-owner",
        has_commits=True,
    )
    owner.merge_status = "merged"
    store.update(owner)
    owner_unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(owner.id, owner_unit.id, "owner")

    failed_leaf = store.add(
        "Out-of-scope failed leaf with distinct unmerged work",
        task_type="improve",
        based_on=owner.id,
        tags=("other",),
    )
    assert failed_leaf.id is not None
    failed_leaf.status = "failed"
    failed_leaf.failure_reason = "WORKER_DIED"
    failed_leaf.branch = "feature/matching-terminal-owner-other-leaf"
    failed_leaf.has_commits = True
    failed_leaf.completed_at = datetime(2026, 7, 8, 9, 0, tzinfo=UTC)
    store.update(failed_leaf)
    failed_leaf_unit = store.create_merge_unit(
        source_branch=failed_leaf.branch,
        target_branch="main",
        owner_task_id=failed_leaf.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(failed_leaf.id, failed_leaf_unit.id, "owner")

    with (
        patch("gza.recovery_engine.decide_failed_task_recovery") as decide,
        patch("gza.cli.advance_engine.determine_next_action") as determine,
    ):
        rows = query_lineage_owner_rows(
            store,
            LineageOwnerQuery(limit=None, tags=(tag,), include_skipped=True, max_recovery_attempts=1),
            config=config,
            git=MagicMock(),
            target_branch="main",
        )

    assert rows == ()
    decide.assert_not_called()
    determine.assert_not_called()


def test_query_lineage_owner_rows_reroots_failed_based_on_leaf_when_merged_owner_landed(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    owner = store.add("Merged owner", task_type="implement")
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 7, 8, 8, 0, tzinfo=UTC),
        branch="feature/merged-owner-distinct-leaf",
        has_commits=True,
    )
    owner.merge_status = "merged"
    store.update(owner)
    owner_unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(owner.id, owner_unit.id, "owner")

    failed_leaf = store.add("Failed distinct improve", task_type="improve", based_on=owner.id)
    assert failed_leaf.id is not None
    failed_leaf.status = "failed"
    failed_leaf.failure_reason = "WORKER_DIED"
    failed_leaf.branch = "feature/merged-owner-distinct-improve"
    failed_leaf.has_commits = True
    failed_leaf.completed_at = datetime(2026, 7, 8, 9, 0, tzinfo=UTC)
    store.update(failed_leaf)

    git = _LineageMergeStateGit(
        source_ref=failed_leaf.branch,
        source_sha="leaf-sha",
        target_sha="target-sha",
        ahead_count=1,
        merged=False,
        net_diff=True,
    )

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=None,
        git=git,
        target_branch="main",
    )

    assert [row.owner_task.id for row in rows] == [failed_leaf.id]
    row = rows[0]
    assert row.recovery_leaf_task is not None
    assert row.recovery_leaf_task.id == failed_leaf.id
    assert [task.id for task in row.unresolved_tasks] == [failed_leaf.id]
    assert owner.id not in {row.owner_task.id for row in rows}
    assert ("count_commits_ahead_checked", f"{failed_leaf.branch}->main") in git.probes


def test_query_lineage_owner_rows_reroots_failed_based_on_leaf_when_merge_proof_unavailable(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    owner = store.add("Merged owner", task_type="implement")
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 7, 8, 8, 0, tzinfo=UTC),
        branch="feature/merged-owner-unprovable-leaf",
        has_commits=True,
    )
    owner.merge_status = "merged"
    store.update(owner)
    owner_unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(owner.id, owner_unit.id, "owner")

    failed_leaf = store.add("Failed unprovable rebase", task_type="rebase", based_on=owner.id)
    assert failed_leaf.id is not None
    failed_leaf.status = "failed"
    failed_leaf.failure_reason = "GIT_ERROR"
    failed_leaf.branch = "feature/merged-owner-unprovable-rebase"
    failed_leaf.has_commits = True
    failed_leaf.completed_at = datetime(2026, 7, 8, 9, 0, tzinfo=UTC)
    store.update(failed_leaf)

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=None,
        target_branch="main",
    )

    assert [row.owner_task.id for row in rows] == [failed_leaf.id]
    row = rows[0]
    assert row.recovery_leaf_task is not None
    assert row.recovery_leaf_task.id == failed_leaf.id
    assert [task.id for task in row.unresolved_tasks] == [failed_leaf.id]
    assert owner.id not in {row.owner_task.id for row in rows}


def test_query_lineage_owner_rows_suppresses_stale_authoritative_merged_owner_with_same_unit_failed_leaf(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    owner = store.add("Stale merged owner", task_type="implement")
    assert owner.id is not None
    owner.status = "failed"
    owner.failure_reason = "STALE_ROW"
    owner.branch = "feature/stale-authoritative-merged-owner"
    owner.has_commits = True
    owner.completed_at = datetime(2026, 7, 8, 8, 0, tzinfo=UTC)
    owner.merge_status = "unmerged"
    store.update(owner)
    owner_unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(owner.id, owner_unit.id, "owner")

    failed_leaf = store.add("Historical same-unit failed review", task_type="review", based_on=owner.id)
    assert failed_leaf.id is not None
    failed_leaf.status = "failed"
    failed_leaf.failure_reason = "WORKER_DIED"
    failed_leaf.branch = owner.branch
    failed_leaf.has_commits = False
    failed_leaf.completed_at = datetime(2026, 7, 8, 9, 0, tzinfo=UTC)
    store.update(failed_leaf)
    store.attach_task_to_merge_unit(failed_leaf.id, owner_unit.id, "review")

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=None,
        target_branch="main",
    )

    assert rows == ()


@pytest.mark.parametrize("terminal_state", ("empty", "redundant"))
def test_query_lineage_owner_rows_suppresses_stale_authoritative_no_work_owner_until_distinct_work_surfaces(
    tmp_path: Path,
    terminal_state: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    owner = store.add(f"Stale {terminal_state} owner", task_type="implement")
    assert owner.id is not None
    owner.status = "failed"
    owner.failure_reason = "STALE_COMPATIBILITY_ROW"
    owner.branch = f"feature/stale-{terminal_state}-owner"
    owner.has_commits = terminal_state == "redundant"
    owner.completed_at = datetime(2026, 7, 8, 8, 0, tzinfo=UTC)
    owner.merge_status = "unmerged"
    store.update(owner)
    owner_unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state=terminal_state,
    )
    store.attach_task_to_merge_unit(owner.id, owner_unit.id, "owner")

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=None,
        target_branch="main",
    )

    assert rows == ()

    failed_leaf = store.add(f"Distinct failed leaf after stale {terminal_state}", task_type="rebase", based_on=owner.id)
    assert failed_leaf.id is not None
    failed_leaf.status = "failed"
    failed_leaf.failure_reason = "GIT_ERROR"
    failed_leaf.branch = f"feature/stale-{terminal_state}-distinct-rebase"
    failed_leaf.has_commits = True
    failed_leaf.completed_at = datetime(2026, 7, 8, 9, 0, tzinfo=UTC)
    store.update(failed_leaf)
    leaf_unit = store.create_merge_unit(
        source_branch=failed_leaf.branch,
        target_branch="main",
        owner_task_id=failed_leaf.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(failed_leaf.id, leaf_unit.id, "owner")

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=None,
        target_branch="main",
    )

    assert [row.owner_task.id for row in rows] == [failed_leaf.id]
    assert [task.id for task in rows[0].unresolved_tasks] == [failed_leaf.id]
    assert owner.id not in {row.owner_task.id for row in rows}


@pytest.mark.parametrize("terminal_state", ("empty", "redundant"))
def test_query_lineage_owner_rows_reroots_failed_leaf_under_terminal_no_work_owner(
    tmp_path: Path,
    terminal_state: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    owner = store.add(f"{terminal_state.title()} owner", task_type="implement")
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 7, 8, 8, 0, tzinfo=UTC),
        branch=f"feature/{terminal_state}-owner",
        has_commits=False,
    )
    store.update(owner)
    owner_unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state=terminal_state,
    )
    store.attach_task_to_merge_unit(owner.id, owner_unit.id, "owner")

    failed_leaf = store.add(f"Distinct failed leaf after {terminal_state}", task_type="rebase", based_on=owner.id)
    assert failed_leaf.id is not None
    failed_leaf.status = "failed"
    failed_leaf.failure_reason = "GIT_ERROR"
    failed_leaf.branch = f"feature/{terminal_state}-owner-distinct-rebase"
    failed_leaf.has_commits = True
    failed_leaf.completed_at = datetime(2026, 7, 8, 9, 0, tzinfo=UTC)
    store.update(failed_leaf)

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=None,
        target_branch="main",
    )

    assert [row.owner_task.id for row in rows] == [failed_leaf.id]
    row = rows[0]
    assert row.recovery_action_task is not None
    assert row.recovery_action_task.id == failed_leaf.id
    assert row.recovery_leaf_task is not None
    assert row.recovery_leaf_task.id == failed_leaf.id
    assert [task.id for task in row.unresolved_tasks] == [failed_leaf.id]
    assert owner.id not in {row.owner_task.id for row in rows}


def test_query_lineage_owner_rows_reroots_only_task_type_matching_failed_leaf(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    owner = store.add("Merged owner with mixed failed leaves", task_type="implement")
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 7, 8, 8, 0, tzinfo=UTC),
        branch="feature/mixed-leaves-owner",
        has_commits=True,
    )
    store.update(owner)
    owner_unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(owner.id, owner_unit.id, "owner")

    review_leaf = store.add("Older matching failed review", task_type="review", based_on=owner.id)
    assert review_leaf.id is not None
    review_leaf.status = "failed"
    review_leaf.failure_reason = "REVIEW_FAILED"
    review_leaf.branch = "feature/mixed-leaves-review"
    review_leaf.has_commits = True
    review_leaf.completed_at = datetime(2026, 7, 8, 9, 0, tzinfo=UTC)
    store.update(review_leaf)

    improve_leaf = store.add("Newer excluded failed improve", task_type="improve", based_on=owner.id)
    assert improve_leaf.id is not None
    improve_leaf.status = "failed"
    improve_leaf.failure_reason = "IMPROVE_FAILED"
    improve_leaf.branch = "feature/mixed-leaves-improve"
    improve_leaf.has_commits = True
    improve_leaf.completed_at = datetime(2026, 7, 8, 10, 0, tzinfo=UTC)
    store.update(improve_leaf)

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(
            limit=None,
            task_types=("review",),
            include_skipped=True,
            max_recovery_attempts=1,
        ),
        config=config,
        git=None,
        target_branch="main",
    )

    assert [row.owner_task.id for row in rows] == [review_leaf.id]
    row = rows[0]
    assert row.recovery_action_task is not None
    assert row.recovery_action_task.id == review_leaf.id
    assert row.recovery_leaf_task is not None
    assert row.recovery_leaf_task.id == review_leaf.id
    assert [task.id for task in row.unresolved_tasks] == [review_leaf.id]
    assert improve_leaf.id not in {task.id for task in row.members}


def test_query_lineage_owner_rows_reroots_only_task_id_matching_failed_leaf(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    owner = store.add("Merged owner with selectable failed leaves", task_type="implement")
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 7, 8, 8, 0, tzinfo=UTC),
        branch="feature/selectable-leaves-owner",
        has_commits=True,
    )
    store.update(owner)
    owner_unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(owner.id, owner_unit.id, "owner")

    selected_leaf = store.add("Selected failed rebase", task_type="rebase", based_on=owner.id)
    assert selected_leaf.id is not None
    selected_leaf.status = "failed"
    selected_leaf.failure_reason = "SELECTED"
    selected_leaf.branch = "feature/selectable-leaves-selected"
    selected_leaf.has_commits = True
    selected_leaf.completed_at = datetime(2026, 7, 8, 9, 0, tzinfo=UTC)
    store.update(selected_leaf)

    excluded_leaf = store.add("Excluded failed rebase", task_type="rebase", based_on=owner.id)
    assert excluded_leaf.id is not None
    excluded_leaf.status = "failed"
    excluded_leaf.failure_reason = "EXCLUDED"
    excluded_leaf.branch = "feature/selectable-leaves-excluded"
    excluded_leaf.has_commits = True
    excluded_leaf.completed_at = datetime(2026, 7, 8, 10, 0, tzinfo=UTC)
    store.update(excluded_leaf)

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(
            limit=None,
            task_ids=(selected_leaf.id,),
            include_skipped=True,
            max_recovery_attempts=1,
        ),
        config=config,
        git=None,
        target_branch="main",
    )

    assert [row.owner_task.id for row in rows] == [selected_leaf.id]
    row = rows[0]
    assert row.recovery_action_task is not None
    assert row.recovery_action_task.id == selected_leaf.id
    assert row.recovery_leaf_task is not None
    assert row.recovery_leaf_task.id == selected_leaf.id
    assert [task.id for task in row.unresolved_tasks] == [selected_leaf.id]
    assert excluded_leaf.id not in {task.id for task in row.members}


def test_query_lineage_owner_rows_keeps_failed_timeout_owner_with_only_legacy_merged_metadata(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    failed = store.add("Failed timeout implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "TIMEOUT"
    failed.session_id = "sess-timeout"
    failed.branch = "feature/stale-timeout-lineage"
    failed.completed_at = datetime(2026, 7, 8, 8, 0, tzinfo=UTC)
    failed.merge_status = "merged"
    failed.has_commits = True
    store.update(failed)

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=None,
        target_branch="main",
    )

    assert [row.owner_task.id for row in rows] == [failed.id]
    row = rows[0]
    assert row.recovery_leaf_task is not None
    assert row.recovery_leaf_task.id == failed.id


def test_query_lineage_owner_rows_suppresses_completed_legacy_merged_owner_with_historical_failed_leaf(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    owner = store.add("Legacy merged owner", task_type="implement")
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 7, 8, 8, 0, tzinfo=UTC),
        branch="feature/legacy-merged-owner",
        has_commits=True,
    )
    owner.merge_status = "merged"
    store.update(owner)

    failed_leaf = store.add("Historical failed improve", task_type="improve", based_on=owner.id)
    assert failed_leaf.id is not None
    failed_leaf.status = "failed"
    failed_leaf.failure_reason = "WORKER_DIED"
    failed_leaf.branch = owner.branch
    failed_leaf.has_commits = False
    failed_leaf.completed_at = datetime(2026, 7, 8, 9, 0, tzinfo=UTC)
    store.update(failed_leaf)

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=None,
        target_branch="main",
    )

    assert rows == ()


@pytest.mark.parametrize(
    ("has_commits", "expect_visible"),
    ((None, True), (False, False)),
)
def test_query_lineage_owner_rows_keeps_unprovable_distinct_failed_leaf_under_merged_owner(
    tmp_path: Path,
    has_commits: bool | None,
    expect_visible: bool,
) -> None:
    """Unknown ``has_commits`` on a distinct branch is not proof of no work (spec P6).

    Without live Git the query cannot disprove unique work on a branch the merged owner
    never landed, so visibility fails closed. An affirmative ``False`` still suppresses.
    """
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    owner = store.add("Merged owner with distinct failed contributor", task_type="implement")
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 7, 10, 8, 0, tzinfo=UTC),
        branch="feature/merged-owner-unknown-contributor",
        has_commits=True,
    )
    owner.merge_status = "merged"
    owner.merged_at = datetime(2026, 7, 10, 8, 30, tzinfo=UTC)
    store.update(owner)

    owner_unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(owner.id, owner_unit.id, "owner")

    failed_leaf = store.add("Failed improve on its own branch", task_type="improve", based_on=owner.id)
    assert failed_leaf.id is not None
    failed_leaf.status = "failed"
    failed_leaf.failure_reason = "WORKER_DIED"
    failed_leaf.branch = "feature/merged-owner-unknown-contributor-improve"
    failed_leaf.has_commits = has_commits
    failed_leaf.completed_at = datetime(2026, 7, 10, 9, 0, tzinfo=UTC)
    store.update(failed_leaf)
    store.attach_task_to_merge_unit(failed_leaf.id, owner_unit.id, "improve")

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=None,
        target_branch="main",
    )
    failed_for_recovery = list_failed_tasks_for_recovery(store)

    owner_ids = {row.owner_task.id for row in rows}
    assert owner.id not in owner_ids
    if expect_visible:
        assert [task.id for task in failed_for_recovery] == [failed_leaf.id]
        assert [row.owner_task.id for row in rows] == [failed_leaf.id]
    else:
        assert failed_for_recovery == []
        assert rows == ()


@pytest.mark.parametrize(("has_commits", "expect_visible"), ((None, True), (False, False)))
def test_query_lineage_owner_rows_keeps_unknown_branchless_legacy_leaf_under_merged_owner(
    tmp_path: Path,
    has_commits: bool | None,
    expect_visible: bool,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    owner = store.add("Branchless legacy merged owner", task_type="implement")
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 7, 9, 8, 0, tzinfo=UTC),
        branch=None,
        has_commits=True,
    )
    owner.merge_status = "merged"
    store.update(owner)

    failed_leaf = store.add("Branchless failed legacy improve", task_type="improve", based_on=owner.id)
    assert failed_leaf.id is not None
    failed_leaf.status = "failed"
    failed_leaf.failure_reason = "WORKER_DIED"
    failed_leaf.branch = None
    failed_leaf.has_commits = has_commits
    failed_leaf.completed_at = datetime(2026, 7, 9, 9, 0, tzinfo=UTC)
    store.update(failed_leaf)

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=None,
        target_branch="main",
    )

    if expect_visible:
        assert [row.owner_task.id for row in rows] == [failed_leaf.id]
        row = rows[0]
        assert row.recovery_leaf_task is not None
        assert row.recovery_leaf_task.id == failed_leaf.id
        assert [task.id for task in row.unresolved_tasks] == [failed_leaf.id]
    else:
        assert rows == ()


def test_query_lineage_owner_rows_reroots_legacy_merged_owner_distinct_failed_leaf_without_merge_unit(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    owner = store.add("Legacy merged owner", task_type="implement")
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 7, 9, 8, 0, tzinfo=UTC),
        branch="feature/legacy-merged-owner-without-unit",
        has_commits=True,
    )
    owner.merge_status = "merged"
    store.update(owner)

    failed_leaf = store.add("Distinct failed legacy improve", task_type="improve", based_on=owner.id)
    assert failed_leaf.id is not None
    failed_leaf.status = "failed"
    failed_leaf.failure_reason = "WORKER_DIED"
    failed_leaf.branch = "feature/legacy-merged-owner-distinct-improve"
    failed_leaf.has_commits = True
    failed_leaf.completed_at = datetime(2026, 7, 9, 9, 0, tzinfo=UTC)
    store.update(failed_leaf)

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=None,
        target_branch="main",
    )

    assert [row.owner_task.id for row in rows] == [failed_leaf.id]
    row = rows[0]
    assert row.recovery_action_task is not None
    assert row.recovery_action_task.id == failed_leaf.id
    assert row.recovery_leaf_task is not None
    assert row.recovery_leaf_task.id == failed_leaf.id
    assert [task.id for task in row.unresolved_tasks] == [failed_leaf.id]
    assert owner.id not in {row.owner_task.id for row in rows}


@pytest.mark.parametrize("terminal_state", ("merged", "empty", "redundant"))
def test_query_lineage_owner_rows_suppresses_legacy_merged_owner_failed_leaf_when_git_proves_terminal(
    tmp_path: Path,
    terminal_state: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    owner = store.add("Legacy merged owner", task_type="implement")
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 7, 9, 8, 0, tzinfo=UTC),
        branch="feature/legacy-merged-owner-git-proof",
        has_commits=True,
    )
    owner.merge_status = "merged"
    store.update(owner)

    failed_leaf = store.add(f"Git-proven {terminal_state} legacy improve", task_type="improve", based_on=owner.id)
    assert failed_leaf.id is not None
    failed_leaf.status = "failed"
    failed_leaf.failure_reason = "WORKER_DIED"
    failed_leaf.branch = f"feature/git-proven-{terminal_state}-legacy-improve"
    failed_leaf.has_commits = terminal_state != "empty"
    failed_leaf.completed_at = datetime(2026, 7, 9, 9, 0, tzinfo=UTC)
    store.update(failed_leaf)

    if terminal_state == "merged":
        git = _LineageMergeStateGit(
            source_ref=failed_leaf.branch,
            source_sha="leaf-sha",
            target_sha="target-sha",
            ahead_count=1,
            merged=True,
            net_diff=True,
        )
    else:
        git = _LineageMergeStateGit(
            source_ref=failed_leaf.branch,
            source_sha="target-sha",
            target_sha="target-sha",
            ahead_count=0,
            merged=False,
            net_diff=False,
        )

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=git,  # type: ignore[arg-type]
        target_branch="main",
    )

    assert rows == ()
    assert ("count_commits_ahead_checked", f"{failed_leaf.branch}->main") in git.probes


def test_query_lineage_owner_rows_reroots_legacy_merged_owner_failed_leaf_when_owner_branch_missing(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    owner = store.add("Branchless legacy merged owner", task_type="implement")
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 7, 9, 8, 0, tzinfo=UTC),
        branch=None,
        has_commits=True,
    )
    owner.merge_status = "merged"
    store.update(owner)

    failed_leaf = store.add("Unprovable failed legacy improve", task_type="improve", based_on=owner.id)
    assert failed_leaf.id is not None
    failed_leaf.status = "failed"
    failed_leaf.failure_reason = "WORKER_DIED"
    failed_leaf.branch = "feature/unprovable-legacy-improve"
    failed_leaf.has_commits = True
    failed_leaf.completed_at = datetime(2026, 7, 9, 9, 0, tzinfo=UTC)
    store.update(failed_leaf)

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=None,
        target_branch="main",
    )

    assert [row.owner_task.id for row in rows] == [failed_leaf.id]
    row = rows[0]
    assert row.recovery_leaf_task is not None
    assert row.recovery_leaf_task.id == failed_leaf.id
    assert [task.id for task in row.unresolved_tasks] == [failed_leaf.id]
    assert owner.id not in {row.owner_task.id for row in rows}


def test_query_lineage_owner_rows_suppresses_proven_same_unit_failed_leaf_under_merged_unit(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    owner = store.add("Merged owner with proven same-unit leaf", task_type="implement")
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 7, 9, 8, 0, tzinfo=UTC),
        branch="feature/proven-same-unit-owner",
        has_commits=True,
    )
    store.update(owner)
    owner_unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(owner.id, owner_unit.id, "owner")

    failed_leaf = store.add("Proven same-unit failed review", task_type="review", based_on=owner.id)
    assert failed_leaf.id is not None
    failed_leaf.status = "failed"
    failed_leaf.failure_reason = "WORKER_DIED"
    failed_leaf.branch = owner.branch
    failed_leaf.has_commits = False
    failed_leaf.completed_at = datetime(2026, 7, 9, 9, 0, tzinfo=UTC)
    store.update(failed_leaf)
    store.attach_task_to_merge_unit(failed_leaf.id, owner_unit.id, "review")

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=None,
        target_branch="main",
    )

    assert rows == ()




def test_query_lineage_owner_rows_hides_terminal_owner_for_self_owned_failed_leaf_without_unique_work(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    owner = store.add("Merged owner", task_type="implement")
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 5, 18, 8, 0, tzinfo=UTC),
        branch="feature/merged-owner-hidden",
        has_commits=True,
    )
    owner.merge_status = "merged"
    store.update(owner)

    owner_unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(owner.id, owner_unit.id, "owner")

    failed_leaf = store.add(
        "Failed implement leaf with self-owned empty merge unit",
        task_type="implement",
        based_on=owner.id,
        recovery_origin="manual",
    )
    assert failed_leaf.id is not None
    failed_leaf.status = "failed"
    failed_leaf.failure_reason = "WORKER_DIED"
    failed_leaf.branch = "feature/merged-owner-hidden-followup"
    failed_leaf.has_commits = False
    failed_leaf.completed_at = datetime(2026, 5, 18, 9, 0, tzinfo=UTC)
    store.update(failed_leaf)

    failed_leaf_unit = store.create_merge_unit(
        source_branch=failed_leaf.branch,
        target_branch="main",
        owner_task_id=failed_leaf.id,
        state="empty",
    )
    store.attach_task_to_merge_unit(failed_leaf.id, failed_leaf_unit.id, "implement")

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=None,
        target_branch="main",
    )

    assert not rows


def test_query_lineage_owner_rows_hides_failed_same_slice_leaf_resolved_by_landed_sibling(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan source", task_type="plan")
    review = store.add("Plan review", task_type="plan_review", depends_on=plan.id)
    assert plan.id is not None
    assert review.id is not None

    failed = store.add(
        _plan_review_slice_prompt(plan_id=plan.id, review_id=review.id, slice_id="S1"),
        task_type="implement",
        based_on=plan.id,
        review_scope="Review only the parser slice.",
    )
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.completed_at = datetime(2026, 6, 28, 8, 0, tzinfo=UTC)
    store.update(failed)

    landed = store.add(
        _plan_review_slice_prompt(plan_id=plan.id, review_id=review.id, slice_id="S1"),
        task_type="implement",
        based_on=plan.id,
        review_scope="Review only the parser slice.",
    )
    assert landed.id is not None
    landed.status = "completed"
    landed.branch = "feature/lineage-same-slice-landed"
    landed.has_commits = True
    landed.completed_at = datetime(2026, 6, 28, 9, 0, tzinfo=UTC)
    store.update(landed)
    landed_unit = store.create_merge_unit(
        source_branch=landed.branch,
        target_branch="main",
        owner_task_id=landed.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(landed.id, landed_unit.id, "owner")

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=None,
        target_branch="main",
    )

    failed_leaf_ids = {
        row.recovery_leaf_task.id
        for row in rows
        if row.recovery_leaf_task is not None and row.recovery_leaf_task.id is not None
    }
    unresolved_ids = {task.id for row in rows for task in row.unresolved_tasks if task.id is not None}
    assert failed.id not in failed_leaf_ids
    assert failed.id not in unresolved_ids


def test_query_lineage_owner_rows_tag_scope_keeps_untagged_merged_lineage_resolution(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    tag = "v0.5.0"

    plan = store.add("Plan source", task_type="plan")
    review = store.add("Plan review", task_type="plan_review", depends_on=plan.id)
    assert plan.id is not None
    assert review.id is not None

    failed = store.add(
        _plan_review_slice_prompt(plan_id=plan.id, review_id=review.id, slice_id="S1"),
        task_type="implement",
        based_on=plan.id,
        review_scope="Review only the parser slice.",
        tags=(tag,),
    )
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.completed_at = datetime(2026, 6, 28, 8, 0, tzinfo=UTC)
    store.update(failed)

    landed = store.add(
        _plan_review_slice_prompt(plan_id=plan.id, review_id=review.id, slice_id="S1"),
        task_type="implement",
        based_on=plan.id,
        review_scope="Review only the parser slice.",
    )
    assert landed.id is not None
    landed.status = "completed"
    landed.branch = "feature/lineage-same-slice-landed-tagged"
    landed.has_commits = True
    landed.completed_at = datetime(2026, 6, 28, 9, 0, tzinfo=UTC)
    store.update(landed)
    landed_unit = store.create_merge_unit(
        source_branch=landed.branch,
        target_branch="main",
        owner_task_id=landed.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(landed.id, landed_unit.id, "owner")

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, tags=(tag,), include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=None,
        target_branch="main",
    )

    failed_leaf_ids = {
        row.recovery_leaf_task.id
        for row in rows
        if row.recovery_leaf_task is not None and row.recovery_leaf_task.id is not None
    }
    unresolved_ids = {task.id for row in rows for task in row.unresolved_tasks if task.id is not None}
    assert failed.id not in failed_leaf_ids
    assert failed.id not in unresolved_ids


def test_query_lineage_owner_rows_tag_scope_does_not_pull_unrelated_failed_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    tag = "v0.5.0"

    tagged_failed = store.add("Tagged failed implementation", task_type="implement", tags=(tag,))
    assert tagged_failed.id is not None
    tagged_failed.status = "failed"
    tagged_failed.failure_reason = "INFRASTRUCTURE_ERROR"
    tagged_failed.completed_at = datetime(2026, 6, 28, 8, 0, tzinfo=UTC)
    store.update(tagged_failed)

    unrelated_tagged = store.add("Tagged pending task", task_type="implement", tags=(tag,))
    assert unrelated_tagged.id is not None

    unrelated_failed = store.add("Unrelated failed implementation", task_type="implement")
    assert unrelated_failed.id is not None
    unrelated_failed.status = "failed"
    unrelated_failed.failure_reason = "INFRASTRUCTURE_ERROR"
    unrelated_failed.completed_at = datetime(2026, 6, 28, 9, 0, tzinfo=UTC)
    store.update(unrelated_failed)

    touched_failed_ids: list[str] = []
    original = recovery_engine.is_chain_resolved_by_recovery

    def _record_touched(store_arg, task, *args, **kwargs):
        if task.id is not None:
            touched_failed_ids.append(task.id)
        return original(store_arg, task, *args, **kwargs)

    monkeypatch.setattr(recovery_engine, "is_chain_resolved_by_recovery", _record_touched)

    query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, tags=(tag,), include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=MagicMock(),
        target_branch="main",
    )

    assert tagged_failed.id in touched_failed_ids
    assert unrelated_failed.id not in touched_failed_ids


@pytest.mark.parametrize("sibling_has_commits", (False, None))
def test_query_lineage_owner_rows_keeps_failed_same_slice_leaf_visible_without_terminal_merge_proof(
    tmp_path: Path,
    sibling_has_commits: bool | None,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan source", task_type="plan")
    review = store.add("Plan review", task_type="plan_review", depends_on=plan.id)
    assert plan.id is not None
    assert review.id is not None

    failed = store.add(
        _plan_review_slice_prompt(plan_id=plan.id, review_id=review.id, slice_id="S1"),
        task_type="implement",
        based_on=plan.id,
        review_scope="Review only the parser slice.",
    )
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.completed_at = datetime(2026, 6, 28, 8, 0, tzinfo=UTC)
    store.update(failed)

    insufficient_proof = store.add(
        _plan_review_slice_prompt(plan_id=plan.id, review_id=review.id, slice_id="S1"),
        task_type="implement",
        based_on=plan.id,
        review_scope="Review only the parser slice.",
    )
    assert insufficient_proof.id is not None
    insufficient_proof.status = "completed"
    insufficient_proof.has_commits = sibling_has_commits
    insufficient_proof.completed_at = datetime(2026, 6, 28, 9, 0, tzinfo=UTC)
    store.update(insufficient_proof)

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=None,
        target_branch="main",
    )

    failed_leaf_ids = {
        row.recovery_leaf_task.id
        for row in rows
        if row.recovery_leaf_task is not None and row.recovery_leaf_task.id is not None
    }
    unresolved_ids = {task.id for row in rows for task in row.unresolved_tasks if task.id is not None}
    assert failed.id in failed_leaf_ids
    assert failed.id in unresolved_ids


def test_query_lineage_owner_rows_keeps_failed_same_slice_leaf_with_active_unmerged_unit_visible(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    plan = store.add("Plan source", task_type="plan")
    review = store.add("Plan review", task_type="plan_review", depends_on=plan.id)
    assert plan.id is not None
    assert review.id is not None

    failed = store.add(
        _plan_review_slice_prompt(plan_id=plan.id, review_id=review.id, slice_id="S1"),
        task_type="implement",
        based_on=plan.id,
        review_scope="Review only the parser slice.",
    )
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.branch = "feature/lineage-same-slice-live"
    failed.has_commits = True
    failed.completed_at = datetime(2026, 6, 28, 8, 0, tzinfo=UTC)
    store.update(failed)
    live_unit = store.create_merge_unit(
        source_branch=failed.branch,
        target_branch="main",
        owner_task_id=failed.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(failed.id, live_unit.id, "owner")

    landed = store.add(
        _plan_review_slice_prompt(plan_id=plan.id, review_id=review.id, slice_id="S1"),
        task_type="implement",
        based_on=plan.id,
        review_scope="Review only the parser slice.",
    )
    assert landed.id is not None
    landed.status = "completed"
    landed.branch = "feature/lineage-same-slice-landed-live"
    landed.has_commits = True
    landed.completed_at = datetime(2026, 6, 28, 9, 0, tzinfo=UTC)
    store.update(landed)
    landed_unit = store.create_merge_unit(
        source_branch=landed.branch,
        target_branch="main",
        owner_task_id=landed.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(landed.id, landed_unit.id, "owner")

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=None,
        target_branch="main",
    )

    failed_leaf_ids = {
        row.recovery_leaf_task.id
        for row in rows
        if row.recovery_leaf_task is not None and row.recovery_leaf_task.id is not None
    }
    unresolved_ids = {task.id for row in rows for task in row.unresolved_tasks if task.id is not None}
    assert failed.id in failed_leaf_ids
    assert failed.id in unresolved_ids




@pytest.mark.parametrize(
    ("leaf_state", "has_commits"),
    [("merged", True), ("empty", False), ("redundant", True)],
)
def test_failed_leaf_unique_unmerged_work_short_circuits_terminal_leaf_even_with_live_git(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    leaf_state: str,
    has_commits: bool,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    owner = store.add("Merged owner", task_type="implement")
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 5, 19, 8, 0, tzinfo=UTC),
        branch="feature/terminal-owner-short-circuit",
        has_commits=True,
    )
    owner.merge_status = "merged"
    store.update(owner)

    owner_unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(owner.id, owner_unit.id, "owner")

    failed_leaf = store.add(
        "Failed implement leaf with merged own unit",
        task_type="implement",
        based_on=owner.id,
        recovery_origin="manual",
    )
    assert failed_leaf.id is not None
    failed_leaf.status = "failed"
    failed_leaf.failure_reason = "WORKER_DIED"
    failed_leaf.branch = "feature/terminal-owner-short-circuit-followup"
    failed_leaf.has_commits = has_commits
    failed_leaf.completed_at = datetime(2026, 5, 19, 9, 0, tzinfo=UTC)
    store.update(failed_leaf)

    failed_leaf_unit = store.create_merge_unit(
        source_branch=failed_leaf.branch,
        target_branch="main",
        owner_task_id=failed_leaf.id,
        state=leaf_state,
        head_sha="deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
    )
    store.attach_task_to_merge_unit(failed_leaf.id, failed_leaf_unit.id, "owner")

    git = MagicMock(spec=Git)
    git.is_ancestor.side_effect = AssertionError("terminal leaf should short-circuit before git proof")

    caplog.clear()
    with (
        caplog.at_level("DEBUG", logger="gza.lineage_query"),
        patch(
            "gza.recovery_engine.classify_branch_merge_state_for_target",
            side_effect=AssertionError("terminal leaf should short-circuit before classify"),
        ) as classify,
    ):
        result = _failed_leaf_has_unique_unmerged_work_under_terminal_owner(
            store=store,
            failed_task=failed_leaf,
            completed_owner=owner,
            owner_merge_unit=owner_unit,
            leaf_merge_unit=failed_leaf_unit,
            git=git,
        )

    assert result is False
    classify.assert_not_called()
    git.is_ancestor.assert_not_called()
    assert caplog.records == []

    entries = collect_recovery_lane_entries(
        store,
        tags=None,
        any_tag=False,
        max_recovery_attempts=1,
    )
    assert [entry.decision.task_id for entry in entries] == []


def test_failed_rebase_contributor_under_terminal_owner_suppresses_after_live_terminal_proof(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    owner = store.add("Merged owner", task_type="implement")
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 7, 6, 8, 0, tzinfo=UTC),
        branch="feature/terminal-owner-rebase-contributor",
        has_commits=True,
    )
    owner.merge_status = "merged"
    store.update(owner)

    owner_unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="merged",
        head_sha="deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
    )
    store.attach_task_to_merge_unit(owner.id, owner_unit.id, "owner")

    failed_rebase = store.add(
        "Failed rebase contributor",
        task_type="rebase",
        based_on=owner.id,
        recovery_origin="manual",
    )
    assert failed_rebase.id is not None
    failed_rebase.status = "failed"
    failed_rebase.failure_reason = "GIT_ERROR"
    failed_rebase.branch = owner.branch
    failed_rebase.has_commits = True
    failed_rebase.completed_at = datetime(2026, 7, 6, 9, 0, tzinfo=UTC)
    store.update(failed_rebase)
    store.attach_task_to_merge_unit(failed_rebase.id, owner_unit.id, "contributor")

    git = MagicMock(spec=Git)
    git.is_ancestor.side_effect = AssertionError("terminal contributor should short-circuit before git proof")

    caplog.clear()
    with (
        caplog.at_level("DEBUG", logger="gza.lineage_query"),
        patch(
            "gza.recovery_engine.classify_branch_merge_state_for_target",
            return_value=BranchMergeClassification(
                state="merged",
                reason="content-equivalent-with-commits",
                source_ref=str(failed_rebase.branch),
                target_ref="main",
                source_sha="source-sha",
                target_sha="target-sha",
            ),
        ) as classify,
    ):
        result = _failed_leaf_has_unique_unmerged_work_under_terminal_owner(
            store=store,
            failed_task=failed_rebase,
            completed_owner=owner,
            owner_merge_unit=owner_unit,
            leaf_merge_unit=owner_unit,
            git=git,
        )

    assert result is False
    classify.assert_called_once()
    git.is_ancestor.assert_not_called()
    assert caplog.records == []


class _MinimalGit(Git):
    """Minimal Git subclass that satisfies isinstance checks and build_merge_context_from_git.

    All methods that would run subprocesses are overridden to return safe in-memory answers.
    """

    def __init__(
        self,
        *,
        branches: frozenset[str] = frozenset(),
        can_merge_result: bool = False,
        diff_name_status: str = "",
        count_commits_behind_result: int = 0,
        resolved_merge_source_ref: str | None = None,
    ) -> None:
        self.repo_dir = Path("/dev/null")
        self._cache = None
        self._branches = branches
        self._can_merge_result = can_merge_result
        self._diff_name_status = diff_name_status
        self._count_commits_behind_result = count_commits_behind_result
        self._resolved_merge_source_ref = resolved_merge_source_ref

    def local_branch_names(self) -> frozenset[str]:  # type: ignore[override]
        return self._branches

    def default_branch(self) -> str:
        return "main"

    def _run(  # type: ignore[override]
        self,
        *args: str,
        check: bool = True,
        stdin: bytes | None = None,
    ) -> subprocess.CompletedProcess[str]:
        del stdin
        command = args[0] if args else ""
        returncode = 0
        stdout = ""

        if command == "merge-tree":
            returncode = 0 if self._can_merge_result else 1
        elif command == "merge-base":
            stdout = "main-sha\n"
        elif command == "rev-parse":
            resolved = self.rev_parse_if_exists(args[-1]) if len(args) >= 2 else None
            if resolved is None:
                returncode = 1
            else:
                stdout = f"{resolved}\n"
        elif command == "symbolic-ref":
            stdout = "refs/remotes/origin/main\n"

        result = subprocess.CompletedProcess(
            args=["git", *args],
            returncode=returncode,
            stdout=stdout,
            stderr="",
        )
        if check and returncode != 0:
            raise GitError(f"git {' '.join(args)} failed")
        return result

    def branches_exist(self, branches: tuple[str, ...]) -> dict[str, bool]:
        return {branch: branch in self._branches for branch in branches}

    def resolve_refs(
        self,
        refs: tuple[str, ...] | list[str],
        *,
        peel: str = "commit",
    ) -> dict[str, str | None]:
        del peel
        return {ref: self.rev_parse_if_exists(ref) for ref in refs}

    def rev_parse_if_exists(self, ref: str) -> str | None:
        if ref == "main":
            return "main-sha"
        if ref == "main^{commit}":
            return "main-sha"
        if ref == "main^{tree}":
            return "main-tree"
        if ref.startswith("origin/") and ref.removeprefix("origin/") in self._branches:
            return f"{ref.removeprefix('origin/')}-sha"
        if ref in self._branches:
            return f"{ref}-sha"
        return None

    def is_merged(self, branch: str, into: str | None = None, use_cherry: bool = False) -> bool:
        return False

    def branch_exists(self, branch: str) -> bool:
        return branch in self._branches

    def can_merge(self, source_branch: str, target_branch: str) -> bool:
        del source_branch, target_branch
        return self._can_merge_result

    def is_ancestor(self, ancestor: str, descendant: str) -> bool:
        del ancestor, descendant
        return False

    def count_commits_behind(self, source_ref: str, target_branch: str) -> int:
        del source_ref, target_branch
        return self._count_commits_behind_result

    def get_diff_name_status(self, base_ref: str, tip_ref: str) -> str:
        del base_ref, tip_ref
        return self._diff_name_status

    def resolve_fresh_merge_source(self, branch: str, **_kwargs: object) -> ResolvedMergeSourceRef:
        # No remote; the branch itself is the source (avoids subprocess calls).
        if self._resolved_merge_source_ref is not None:
            return ResolvedMergeSourceRef(self._resolved_merge_source_ref)
        return ResolvedMergeSourceRef(branch if branch in self._branches else None)


class _NoWorkLineageGit(_MinimalGit):
    def __init__(self, *, branch: str) -> None:
        super().__init__(branches=frozenset({branch}))
        self.ahead_probes: list[tuple[str, str]] = []
        self.diff_probes: list[tuple[str, str]] = []

    def count_commits_ahead_checked(self, source_ref: str, target_ref: str) -> int | None:  # type: ignore[override]
        self.ahead_probes.append((source_ref, target_ref))
        return 0

    def count_commits_ahead(self, source_ref: str, target_ref: str) -> int | None:  # type: ignore[override]
        self.ahead_probes.append((source_ref, target_ref))
        return 0

    def has_non_empty_source_diff_against_target(self, source_ref: str, target: str) -> bool | None:
        self.diff_probes.append((source_ref, target))
        return False


def _terminal_owner_self_owned_unmerged_no_work_case(
    tmp_path: Path,
    *,
    merge_state: str,
    with_execution_evidence: bool,
) -> tuple[SqliteTaskStore, DbTask, _NoWorkLineageGit]:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    owner = store.add("Merged terminal owner", task_type="implement")
    assert owner.id is not None
    owner.status = "completed"
    owner.completed_at = datetime(2026, 8, 22, 8, 0, tzinfo=UTC)
    owner.branch = "feature/terminal-owner-live-no-work"
    owner.has_commits = True
    owner.merge_status = "merged"
    store.update(owner)
    owner_unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="merged",
        head_sha=f"{owner.branch}-sha",
    )
    store.attach_task_to_merge_unit(owner.id, owner_unit.id, "owner")

    failed = store.add(
        f"Failed leaf live {merge_state}",
        task_type="implement",
        based_on=owner.id,
        recovery_origin="manual",
    )
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "TERMINAL_NO_WORK"
    failed.session_id = f"sess-live-{merge_state}"
    failed.branch = f"feature/failed-leaf-live-{merge_state}"
    failed.has_commits = merge_state == "redundant"
    if with_execution_evidence:
        failed.num_steps_computed = 2
    else:
        failed.num_steps_computed = 0
        failed.num_steps_reported = 0
        failed.output_tokens = 0
    failed.completed_at = datetime(2026, 8, 22, 9, 0, tzinfo=UTC)
    store.update(failed)
    leaf_unit = store.create_merge_unit(
        source_branch=failed.branch,
        target_branch="main",
        owner_task_id=failed.id,
        state="unmerged",
        head_sha=f"{failed.branch}-sha",
    )
    store.attach_task_to_merge_unit(failed.id, leaf_unit.id, "owner")
    return store, failed, _NoWorkLineageGit(branch=failed.branch)


@pytest.mark.parametrize("merge_state", ["empty", "redundant"])
def test_terminal_owner_self_owned_unmerged_live_no_work_keeps_executed_failed_leaf_visible(
    tmp_path: Path,
    merge_state: str,
) -> None:
    store, failed, git = _terminal_owner_self_owned_unmerged_no_work_case(
        tmp_path,
        merge_state=merge_state,
        with_execution_evidence=True,
    )

    assert [task.id for task in list_failed_tasks_for_recovery(store, git=git, target_branch="main")] == [failed.id]

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        git=git,
        target_branch="main",
        persist_post_merge_rebase_state=False,
        persist_review_clearance=False,
    )

    assert [row.owner_task.id for row in rows] == [failed.id]
    assert rows[0].recovery_leaf_task == failed
    assert rows[0].unresolved_tasks == (failed,)


@pytest.mark.parametrize("merge_state", ["empty", "redundant"])
def test_terminal_owner_self_owned_unmerged_live_no_work_keeps_never_executed_failed_leaf_visible(
    tmp_path: Path,
    merge_state: str,
) -> None:
    store, failed, git = _terminal_owner_self_owned_unmerged_no_work_case(
        tmp_path,
        merge_state=merge_state,
        with_execution_evidence=False,
    )

    assert [task.id for task in list_failed_tasks_for_recovery(store, git=git, target_branch="main")] == [failed.id]

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        git=git,
        target_branch="main",
        persist_post_merge_rebase_state=False,
        persist_review_clearance=False,
    )

    assert [row.owner_task.id for row in rows] == [failed.id]
    assert rows[0].recovery_leaf_task == failed
    assert rows[0].unresolved_tasks == (failed,)


@pytest.mark.parametrize("merge_state", ["empty", "redundant"])
def test_failed_leaf_store_backed_self_owned_unmerged_short_circuits_live_no_work_classification(
    tmp_path: Path,
    merge_state: str,
) -> None:
    store, failed, git = _terminal_owner_self_owned_unmerged_no_work_case(
        tmp_path,
        merge_state=merge_state,
        with_execution_evidence=True,
    )
    owner = store.get(failed.based_on)
    assert owner is not None
    owner_unit = store.resolve_merge_unit_for_task(owner.id)
    leaf_unit = store.resolve_merge_unit_for_task(failed.id)
    assert owner_unit is not None
    assert leaf_unit is not None
    cache = {}

    with patch(
        "gza.recovery_engine.classify_branch_merge_state_for_target",
        wraps=recovery_engine.classify_branch_merge_state_for_target,
    ) as classify:
        first = _failed_leaf_has_unique_unmerged_work_under_terminal_owner(
            store=store,
            failed_task=failed,
            completed_owner=owner,
            owner_merge_unit=owner_unit,
            leaf_merge_unit=leaf_unit,
            git=git,
            target_branch="main",
            classification_cache=cache,
        )
        second = _failed_leaf_has_unique_unmerged_work_under_terminal_owner(
            store=store,
            failed_task=failed,
            completed_owner=owner,
            owner_merge_unit=owner_unit,
            leaf_merge_unit=leaf_unit,
            git=git,
            target_branch="main",
            classification_cache=cache,
        )

    assert first is True
    assert second is True
    assert classify.call_count == 0


def test_query_lineage_owner_rows_does_not_call_load_merge_context_when_git_provided(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When git is passed to _query_lineage_owner_rows_with_context, _load_merge_context must
    not be invoked — the caller's git/target_branch seed the merge context instead."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Test implementation", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 6, 15, 10, 0, tzinfo=UTC)
    impl.branch = "feature/test-preseed"
    impl.has_commits = True
    store.update(impl)

    def _raise_if_called(_project_dir=None):
        raise AssertionError(
            "_load_merge_context was called even though git was pre-seeded; "
            "the ambient discover=True path was not eliminated"
        )

    monkeypatch.setattr(recovery_engine, "_load_merge_context", _raise_if_called)

    git = _MinimalGit(branches=frozenset({impl.branch}))
    rows, _read_context = _query_lineage_owner_rows_with_context(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        git=git,
        target_branch="main",
    )
    assert rows is not None


def test_query_lineage_owner_rows_calls_load_merge_context_when_git_not_provided(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without git, _load_merge_context IS invoked as the fallback ambient path."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    call_count: list[int] = []

    original_load = recovery_engine._load_merge_context

    def _record_load(_project_dir=None):
        call_count.append(1)
        return original_load(_project_dir)

    monkeypatch.setattr(recovery_engine, "_load_merge_context", _record_load)

    rows, _read_context = _query_lineage_owner_rows_with_context(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        git=None,
        target_branch=None,
    )
    assert rows is not None
    assert call_count, "_load_merge_context should have been called when git is None"


def test_query_lineage_owner_rows_calls_load_merge_context_when_target_branch_not_provided(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without target_branch, _load_merge_context is still invoked even when git is provided,
    because we cannot seed a deterministic default_branch without it."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    call_count: list[int] = []

    original_load = recovery_engine._load_merge_context

    def _record_load(_project_dir=None):
        call_count.append(1)
        return original_load(_project_dir)

    monkeypatch.setattr(recovery_engine, "_load_merge_context", _record_load)

    git = _MinimalGit(branches=frozenset())
    rows, _read_context = _query_lineage_owner_rows_with_context(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        git=git,
        target_branch=None,
    )
    assert rows is not None
    assert call_count, "_load_merge_context should have been called when target_branch is None"




def test_query_lineage_owner_rows_short_circuits_attached_member_when_owner_unit_is_merged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    base_time = datetime(2026, 6, 22, 12, 0, tzinfo=UTC)

    merged_owner = store.add("Merged owner", task_type="implement")
    assert merged_owner.id is not None
    _set_completed(
        merged_owner,
        when=base_time,
        branch="feature/merged-owner",
        has_commits=True,
    )
    store.update(merged_owner)
    merged_owner_unit = store.create_merge_unit(
        source_branch=merged_owner.branch,
        target_branch="main",
        owner_task_id=merged_owner.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(merged_owner.id, merged_owner_unit.id, "owner")

    attached_failed = store.add("Failed attached review", task_type="review", based_on=merged_owner.id)
    assert attached_failed.id is not None
    attached_failed.status = "failed"
    attached_failed.failure_reason = "INFRASTRUCTURE_ERROR"
    attached_failed.completed_at = base_time.replace(minute=1)
    store.update(attached_failed)
    # The attached member resolves to its own active unmerged unit, but that unit still
    # points at an owner task whose resolved unit is already merged.
    attached_unit = store.create_merge_unit(
        source_branch="feature/merged-owner-review",
        target_branch="main",
        owner_task_id=merged_owner.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(attached_failed.id, attached_unit.id, "review")

    live_failed = store.add("Live unresolved failed owner", task_type="implement")
    assert live_failed.id is not None
    live_failed.status = "failed"
    live_failed.failure_reason = "INFRASTRUCTURE_ERROR"
    live_failed.branch = "feature/live-owner"
    live_failed.completed_at = base_time.replace(hour=13)
    store.update(live_failed)
    live_unit = store.create_merge_unit(
        source_branch=live_failed.branch,
        target_branch="main",
        owner_task_id=live_failed.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(live_failed.id, live_unit.id, "owner")

    read_context_calls: list[str] = []
    store_calls: list[str] = []

    original_read_context_get_lineage_children = RecoveryReadContext.get_lineage_children
    original_store_get_lineage_children = store.get_lineage_children

    def _count_read_context_get_lineage_children(
        self: RecoveryReadContext,
        task_id: str,
        *,
        parent=None,
    ):
        read_context_calls.append(task_id)
        return original_read_context_get_lineage_children(self, task_id, parent=parent)

    def _count_store_get_lineage_children(task_id: str):
        store_calls.append(task_id)
        return original_store_get_lineage_children(task_id)

    monkeypatch.setattr(
        RecoveryReadContext,
        "get_lineage_children",
        _count_read_context_get_lineage_children,
    )
    monkeypatch.setattr(store, "get_lineage_children", _count_store_get_lineage_children)

    rows, _read_context = _query_lineage_owner_rows_with_context(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
    )

    assert {row.owner_task.id for row in rows if row.owner_task.id is not None} == {
        attached_failed.id,
        live_failed.id,
    }
    assert read_context_calls == []
    assert store_calls == []
    assert attached_failed.id not in read_context_calls
    assert attached_failed.id not in store_calls




def test_query_lineage_owner_rows_exclude_tags_scope_does_not_classify_excluded_sibling_or_dependent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    shared_plan = store.add("Shared plan", task_type="plan")
    assert shared_plan.id is not None
    shared_plan.status = "completed"
    shared_plan.completed_at = datetime(2026, 8, 26, 8, 0, tzinfo=UTC)
    store.update(shared_plan)

    owner = store.add("Included owner", task_type="implement", tags=("included-scope",))
    assert owner.id is not None
    owner.status = "completed"
    owner.completed_at = datetime(2026, 8, 26, 9, 0, tzinfo=UTC)
    owner.branch = "feature/included-owner"
    owner.has_commits = True
    owner.merge_status = "unmerged"
    owner.depends_on = shared_plan.id
    store.update(owner)
    owner_unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(owner.id, owner_unit.id, "owner")

    selected_failed = store.add("Included failed review", task_type="review", based_on=owner.id, depends_on=owner.id)
    assert selected_failed.id is not None
    selected_failed.status = "failed"
    selected_failed.failure_reason = "INFRASTRUCTURE_ERROR"
    selected_failed.completed_at = datetime(2026, 8, 26, 9, 1, tzinfo=UTC)
    store.update(selected_failed)
    store.attach_task_to_merge_unit(selected_failed.id, owner_unit.id, "review")

    excluded_sibling = store.add(
        "Excluded shared-plan sibling",
        task_type="implement",
        depends_on=shared_plan.id,
        tags=("excluded-scope",),
    )
    assert excluded_sibling.id is not None
    excluded_sibling.status = "failed"
    excluded_sibling.failure_reason = "INFRASTRUCTURE_ERROR"
    excluded_sibling.completed_at = datetime(2026, 8, 26, 10, 0, tzinfo=UTC)
    excluded_sibling.branch = "feature/excluded-sibling"
    store.update(excluded_sibling)
    excluded_sibling_unit = store.create_merge_unit(
        source_branch=excluded_sibling.branch,
        target_branch="main",
        owner_task_id=excluded_sibling.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(excluded_sibling.id, excluded_sibling_unit.id, "owner")

    excluded_dependent = store.add(
        "Excluded downstream dependent",
        task_type="implement",
        depends_on=owner.id,
        tags=("excluded-scope",),
    )
    assert excluded_dependent.id is not None
    excluded_dependent.status = "failed"
    excluded_dependent.failure_reason = "INFRASTRUCTURE_ERROR"
    excluded_dependent.completed_at = datetime(2026, 8, 26, 10, 1, tzinfo=UTC)
    excluded_dependent.branch = "feature/excluded-dependent"
    store.update(excluded_dependent)
    excluded_dependent_unit = store.create_merge_unit(
        source_branch=excluded_dependent.branch,
        target_branch="main",
        owner_task_id=excluded_dependent.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(excluded_dependent.id, excluded_dependent_unit.id, "owner")

    chain_resolved_calls: list[str] = []
    inactive_merge_unit_calls: list[str] = []
    merged_target_calls: list[str] = []

    def _record_chain_resolved(store_arg, task, *, read_context=None):
        assert task.id is not None
        chain_resolved_calls.append(task.id)
        return False

    def _record_inactive_merge_unit(store_arg, task, *, read_context=None):
        assert task.id is not None
        inactive_merge_unit_calls.append(task.id)
        return False

    def _record_merged_target(store_arg, task, *, merge_context, read_context=None):
        assert task.id is not None
        merged_target_calls.append(task.id)
        return False

    monkeypatch.setattr(recovery_engine, "is_chain_resolved_by_recovery", _record_chain_resolved)
    monkeypatch.setattr(
        recovery_engine,
        "is_recovery_suppressed_by_inactive_merge_unit",
        _record_inactive_merge_unit,
    )
    monkeypatch.setattr(recovery_engine, "is_resolved_by_merged_target", _record_merged_target)

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(
            limit=None,
            include_skipped=True,
            exclude_dropped_from_planning=True,
            exclude_tags=("excluded-scope",),
            max_recovery_attempts=1,
        ),
        config=Config.load(tmp_path),
        target_branch="main",
    )

    assert {row.owner_task.id for row in rows if row.owner_task.id is not None} == {owner.id}
    assert set(chain_resolved_calls) == {selected_failed.id}
    assert set(inactive_merge_unit_calls) == {selected_failed.id}
    assert set(merged_target_calls) == {selected_failed.id}
    excluded_failed_ids = {excluded_sibling.id, excluded_dependent.id}
    assert not (excluded_failed_ids & set(chain_resolved_calls))
    assert not (excluded_failed_ids & set(inactive_merge_unit_calls))
    assert not (excluded_failed_ids & set(merged_target_calls))


@pytest.mark.parametrize("use_matching_tag", (False, True))
def test_query_lineage_owner_rows_orphan_task_id_scope_classifies_canonical_root_failed_members(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    use_matching_tag: bool,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    tag = "selected-lineage"

    owner = store.add(
        "Implementation root",
        task_type="implement",
        tags=(tag,) if use_matching_tag else (),
    )
    assert owner.id is not None
    owner.status = "in_progress"
    owner.branch = "feature/canonical-orphan-owner"
    owner.has_commits = True
    store.update(owner)

    selected_failed_ids: list[str] = []
    for prompt, task_type in (
        ("Failed root review", "review"),
        ("Failed root improve", "improve"),
    ):
        failed = store.add(prompt, task_type=task_type, based_on=owner.id, depends_on=owner.id)
        assert failed.id is not None
        failed.status = "failed"
        failed.failure_reason = "INFRASTRUCTURE_ERROR"
        failed.completed_at = datetime(2026, 8, 25, 9, len(selected_failed_ids), tzinfo=UTC)
        store.update(failed)
        selected_failed_ids.append(failed.id)

    orphan = store.add(
        "Completed orphan rebase",
        task_type="rebase",
        based_on=owner.id,
        same_branch=True,
    )
    assert orphan.id is not None
    _set_completed(
        orphan,
        when=datetime(2026, 8, 25, 10, 0, tzinfo=UTC),
        branch="feature/orphan-rebase",
        has_commits=True,
    )
    orphan.merge_status = "unmerged"
    if use_matching_tag:
        orphan.tags = (tag,)
    store.update(orphan)
    orphan_unit = store.create_merge_unit(
        source_branch=orphan.branch,
        target_branch="main",
        owner_task_id=orphan.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(orphan.id, orphan_unit.id, "owner")

    unrelated_failed = store.add("Unrelated failed implementation", task_type="implement")
    assert unrelated_failed.id is not None
    unrelated_failed.status = "failed"
    unrelated_failed.failure_reason = "INFRASTRUCTURE_ERROR"
    unrelated_failed.completed_at = datetime(2026, 8, 25, 11, 0, tzinfo=UTC)
    store.update(unrelated_failed)

    downstream_dependent = store.add("Failed downstream dependent", task_type="implement")
    assert downstream_dependent.id is not None
    downstream_dependent.status = "failed"
    downstream_dependent.failure_reason = "INFRASTRUCTURE_ERROR"
    downstream_dependent.completed_at = datetime(2026, 8, 25, 11, 1, tzinfo=UTC)
    downstream_dependent.depends_on = owner.id
    store.update(downstream_dependent)

    query_kwargs = {"tags": (tag,)} if use_matching_tag else {}
    git = MagicMock()
    git.branch_exists.return_value = True
    git.can_merge.return_value = True
    git.count_commits_behind.return_value = 0

    global_rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(
            limit=None,
            include_skipped=True,
            max_recovery_attempts=1,
            **query_kwargs,
        ),
        config=config,
        git=git,
        target_branch="main",
    )
    baseline_row = next(row for row in global_rows if row.owner_task.id == owner.id)
    assert baseline_row.recovery_action_task is not None
    assert baseline_row.recovery_action_task.id in selected_failed_ids

    chain_resolved_calls: list[str] = []
    inactive_merge_unit_calls: list[str] = []
    merged_target_calls: list[str] = []

    def _record_chain_resolved(store_arg, task, *, read_context=None):
        assert task.id is not None
        chain_resolved_calls.append(task.id)
        return False

    def _record_inactive_merge_unit(store_arg, task, *, read_context=None):
        assert task.id is not None
        inactive_merge_unit_calls.append(task.id)
        return False

    def _record_merged_target(store_arg, task, *, merge_context, read_context=None):
        assert task.id is not None
        merged_target_calls.append(task.id)
        return False

    monkeypatch.setattr(recovery_engine, "is_chain_resolved_by_recovery", _record_chain_resolved)
    monkeypatch.setattr(
        recovery_engine,
        "is_recovery_suppressed_by_inactive_merge_unit",
        _record_inactive_merge_unit,
    )
    monkeypatch.setattr(recovery_engine, "is_resolved_by_merged_target", _record_merged_target)

    explicit_rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(
            limit=None,
            include_skipped=True,
            max_recovery_attempts=1,
            task_ids=(orphan.id,),
            **query_kwargs,
        ),
        config=config,
        git=git,
        target_branch="main",
    )

    assert len(explicit_rows) == 1
    explicit_row = explicit_rows[0]
    assert explicit_row.owner_task.id == owner.id
    assert explicit_row.next_action == baseline_row.next_action
    assert explicit_row.recovery_action_task is not None
    assert explicit_row.recovery_action_task.id == baseline_row.recovery_action_task.id
    assert set(chain_resolved_calls) == set(selected_failed_ids)
    assert set(inactive_merge_unit_calls) == set(selected_failed_ids)
    assert set(merged_target_calls) == set(selected_failed_ids)
    external_failed_ids = {unrelated_failed.id, downstream_dependent.id}
    assert not (external_failed_ids & set(chain_resolved_calls))
    assert not (external_failed_ids & set(inactive_merge_unit_calls))
    assert not (external_failed_ids & set(merged_target_calls))


def test_query_lineage_owner_rows_skipped_task_id_scope_honors_task_type_for_failed_members(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    owner = store.add("Implementation root", task_type="implement")
    assert owner.id is not None
    owner.status = "in_progress"
    owner.branch = "feature/skipped-member-type-filter"
    owner.has_commits = True
    store.update(owner)

    failed_review = store.add("Failed root review", task_type="review", based_on=owner.id, depends_on=owner.id)
    assert failed_review.id is not None
    failed_review.status = "failed"
    failed_review.failure_reason = "INFRASTRUCTURE_ERROR"
    failed_review.completed_at = datetime(2026, 8, 25, 9, 0, tzinfo=UTC)
    store.update(failed_review)

    failed_impl = store.add("Failed implementation retry", task_type="implement", based_on=owner.id, same_branch=True)
    assert failed_impl.id is not None
    failed_impl.status = "failed"
    failed_impl.failure_reason = "INFRASTRUCTURE_ERROR"
    failed_impl.completed_at = datetime(2026, 8, 25, 9, 1, tzinfo=UTC)
    store.update(failed_impl)

    orphan = store.add(
        "Completed orphan rebase",
        task_type="rebase",
        based_on=owner.id,
        same_branch=True,
    )
    assert orphan.id is not None
    _set_completed(
        orphan,
        when=datetime(2026, 8, 25, 10, 0, tzinfo=UTC),
        branch="feature/skipped-orphan",
        has_commits=True,
    )
    orphan.merge_status = "unmerged"
    store.update(orphan)
    orphan_unit = store.create_merge_unit(
        source_branch=orphan.branch,
        target_branch="main",
        owner_task_id=orphan.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(orphan.id, orphan_unit.id, "owner")

    git = MagicMock()
    git.branch_exists.return_value = True
    git.can_merge.return_value = True
    git.count_commits_behind.return_value = 0

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(
            limit=None,
            include_skipped=True,
            max_recovery_attempts=1,
            task_ids=(orphan.id,),
            task_types=("implement",),
        ),
        config=config,
        git=git,
        target_branch="main",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.owner_task.id == owner.id
    assert row.recovery_action_task is not None
    assert row.recovery_action_task.id == failed_impl.id
    assert row.lifecycle_action_task is None
    assert {task.id for task in row.unresolved_tasks if task.id is not None} == {failed_impl.id}
    assert failed_review.id not in {summary.task_id for summary in row.unresolved_leaf_summary}


def test_query_lineage_owner_rows_seeded_git_drives_same_suppression_as_non_seeded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed owner resolved by a landed sibling is suppressed identically whether git is
    seeded or not.  In the seeded variant _load_merge_context must never be called — the
    seeded _MergeContext built from build_merge_context_from_git is what drives the recovery
    decision (via the lineage scan fallback when git.is_merged returns False)."""
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    root = store.add("Implementation root", task_type="implement")
    assert root.id is not None
    _set_completed(root, when=datetime(2026, 5, 16, 8, 0, tzinfo=UTC), branch="feature/root", has_commits=True)
    store.update(root)

    failed = store.add("Failed manual follow-up", task_type="implement", based_on=root.id, recovery_origin="manual")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-seeded-landed"
    failed.branch = "feature/seeded-landed"
    failed.num_steps_computed = 3
    failed.completed_at = datetime(2026, 5, 16, 9, 0, tzinfo=UTC)
    store.update(failed)

    unit = store.create_merge_unit(
        source_branch=failed.branch,
        target_branch="main",
        owner_task_id=failed.id,
        state="empty",
    )
    store.attach_task_to_merge_unit(failed.id, unit.id, "owner")

    landed = store.add(
        "Merged sibling representative", task_type="implement", based_on=root.id, recovery_origin="manual"
    )
    assert landed.id is not None
    _set_completed(landed, when=datetime(2026, 5, 16, 10, 0, tzinfo=UTC), branch=failed.branch, has_commits=True)
    landed.merge_status = "merged"
    store.update(landed)

    # Baseline (non-seeded): _load_merge_context is invoked via the ambient path.
    rows_no_git = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=None,
        target_branch=None,
    )
    failed_ids_no_git = {
        row.recovery_leaf_task.id
        for row in rows_no_git
        if row.recovery_leaf_task is not None and row.recovery_leaf_task.id is not None
    }
    assert failed.id not in failed_ids_no_git, "baseline: failed owner should be suppressed"

    # Seeded variant: _load_merge_context must not be called.
    # _MinimalGit includes the failed branch so the git-branch-check path is exercised;
    # is_merged returns False, so suppression falls through to the lineage scan.
    def _raise_if_called(_project_dir=None):
        raise AssertionError("_load_merge_context was called even though git + target_branch were pre-seeded")

    monkeypatch.setattr(recovery_engine, "_load_merge_context", _raise_if_called)

    git = _MinimalGit(branches=frozenset({failed.branch}))
    rows_with_git = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=git,
        target_branch="main",
    )
    failed_ids_with_git = {
        row.recovery_leaf_task.id
        for row in rows_with_git
        if row.recovery_leaf_task is not None and row.recovery_leaf_task.id is not None
    }

    assert failed.id not in failed_ids_with_git, (
        "seeded context failed to suppress failed owner resolved by landed sibling"
    )
    assert failed_ids_no_git == failed_ids_with_git, (
        "seeded vs non-seeded paths produced different owner-row visibility"
    )


def test_query_lineage_owner_rows_seeded_git_reachability_only_merged_keeps_failed_owner_visible(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Seeded git reachability alone must not suppress a failed owner.

    The seeded path still exercises build_merge_context_from_git's existing_branches/is_merged
    wiring, but rev/commit/diff provenance is unavailable. R5 requires this to stay
    visible because reachability is not affirmative landed proof by itself."""
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    root = store.add("Implementation root", task_type="implement")
    assert root.id is not None
    _set_completed(
        root, when=datetime(2026, 5, 16, 8, 0, tzinfo=UTC), branch="feature/root-git-proven", has_commits=True
    )
    store.update(root)

    failed = store.add("Failed follow-up", task_type="implement", based_on=root.id, recovery_origin="manual")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    # session_id intentionally left None so _is_resumable_timeout_implementation returns
    # False — otherwise the git-driven path in _is_resolved_by_landed_lineage is skipped.
    failed.branch = "feature/git-proven-merged"
    failed.has_commits = True
    failed.num_steps_computed = 3
    failed.completed_at = datetime(2026, 5, 16, 9, 0, tzinfo=UTC)
    store.update(failed)

    unit = store.create_merge_unit(
        source_branch=failed.branch,
        target_branch="main",
        owner_task_id=failed.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(failed.id, unit.id, "owner")

    # No landed-sibling DB row: the only possible evidence is reachability from seeded git.

    class _MergedBranchGit(_MinimalGit):
        """Variant that reports the task branch as merged and returns it as its own source ref.

        resolve_fresh_merge_source returns the branch itself (not ResolvedMergeSourceRef(None))
        so that classify_branch_merge_state_for_target has a non-None source_ref and reaches
            the merged_proof path. rev_parse_if_exists and count_commits_ahead_checked return None
            so the seeded path has reachability evidence but no landed contribution proof."""

        def is_merged(self, branch: str, into: str | None = None, use_cherry: bool = False) -> bool:
            return branch == failed.branch

        def resolve_fresh_merge_source(self, branch: str, **_kwargs: object) -> ResolvedMergeSourceRef:
            return ResolvedMergeSourceRef(branch)

        def rev_parse_if_exists(self, ref: str) -> str | None:  # type: ignore[override]
            return None

        def ref_exists(self, ref: str) -> bool:  # type: ignore[override]
            return ref in self._branches

        def count_commits_ahead_checked(self, source_ref: str, target_ref: str) -> int | None:  # type: ignore[override]
            return None

    # Baseline: branch absent from existing_branches so the git path cannot confirm the
    # merge.  The lineage scan runs but finds no landed sibling → task must appear.
    git_no_branch = _MergedBranchGit(branches=frozenset())
    rows_baseline = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=git_no_branch,
        target_branch="main",
    )
    failed_ids_baseline = {
        row.recovery_leaf_task.id
        for row in rows_baseline
        if row.recovery_leaf_task is not None and row.recovery_leaf_task.id is not None
    }
    assert failed.id in failed_ids_baseline, (
        "baseline: failed owner should appear in results when the task branch is absent "
        "from existing_branches (git cannot confirm merge, no landed sibling in DB)"
    )

    # Seeded reachability path: patch _load_merge_context to raise, proving the seeded
    # context is the sole source of merge truth.
    def _raise_if_called(_project_dir=None):
        raise AssertionError("_load_merge_context was called even though git + target_branch were pre-seeded")

    monkeypatch.setattr(recovery_engine, "_load_merge_context", _raise_if_called)

    git_merged = _MergedBranchGit(branches=frozenset({failed.branch}))
    rows_git_proven = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=git_merged,
        target_branch="main",
    )
    failed_ids_git_proven = {
        row.recovery_leaf_task.id
        for row in rows_git_proven
        if row.recovery_leaf_task is not None and row.recovery_leaf_task.id is not None
    }

    assert failed.id in failed_ids_git_proven, (
        "seeded git path: failed owner should remain visible when is_merged is the only "
        "available evidence and no landed-sibling DB row exists"
    )


def test_query_lineage_owner_rows_includes_current_main_verify_red_attention(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"

    main_verify_task = store.add(
        "System alert: local main integration verify", task_type="internal", skip_learnings=True
    )
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_command = "./bin/tests"
    main_verify_task.review_verify_status = "failed"
    main_verify_task.review_verify_exit_status = "1"
    main_verify_task.review_verify_failure = "verify_command failed"
    main_verify_task.review_verify_head_sha = "abc123"
    main_verify_task.output_content = json.dumps(
        {
            "alert_message": "main verify RED - merges halted; phase unit failing",
            "captured_at": "2026-06-23T00:00:00+00:00",
            "environment_identity": _main_verify_environment_identity_payload(),
            "failing_phases": ["unit"], "phase_results": [],
            "gate_enabled": True,
            "head_sha": "abc123",
            "tree_fingerprint": "fp",
            "verify_command": "./bin/tests",
            "verify_timeout_grace_seconds": 5.0,
            "verify_timeout_seconds": 120,
        },
        sort_keys=True,
    )
    store.update(main_verify_task)

    git = MagicMock(spec=Git)
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "topic"
    git.rev_parse_if_exists.side_effect = lambda ref: "abc123" if ref == "refs/heads/main" else "topic-sha"

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True),
        config=config,
        git=git,
        target_branch="main",
    )

    assert rows
    row = rows[0]
    assert row.owner_task.id == main_verify_task.id
    assert row.next_action is not None
    assert row.next_action["needs_attention_reason"] == MAIN_INTEGRATION_VERIFY_REASON
    assert "main verify RED at `abc123` - merges halted; phase unit failing" in row.next_action["description"]


@pytest.mark.parametrize(
    ("verify_status", "verify_exit_status", "alert_message", "failing_phase"),
    [
        ("failed", "1", "main verify RED at `abc123` - merges halted; phase unit failing", "unit"),
        (
            "unavailable",
            "tree fingerprint unavailable",
            "main verify freshness unproven at `abc123` - merges halted; exact tree fingerprint unavailable",
            None,
        ),
    ],
)
def test_query_lineage_owner_rows_ignores_ambiguous_short_main_verify_ref(
    tmp_path: Path,
    verify_status: str,
    verify_exit_status: str,
    alert_message: str,
    failing_phase: str | None,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"

    main_verify_task = store.add(
        "System alert: local main integration verify", task_type="internal", skip_learnings=True
    )
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_command = "./bin/tests"
    main_verify_task.review_verify_status = verify_status
    main_verify_task.review_verify_exit_status = verify_exit_status
    main_verify_task.review_verify_failure = "verify_command failed"
    main_verify_task.review_verify_head_sha = "abc123"
    main_verify_task.output_content = json.dumps(
        {
            "alert_message": alert_message,
            "captured_at": "2026-06-23T00:00:00+00:00",
            "environment_identity": _main_verify_environment_identity_payload(),
            "failing_phases": [failing_phase] if failing_phase else [],
            "phase_results": [{"name": failing_phase, "status": "failed"}] if failing_phase else [],
            "gate_enabled": True,
            "head_sha": "abc123",
            "tree_fingerprint": "fp",
            "verify_command": "./bin/tests",
            "verify_timeout_grace_seconds": 5.0,
            "verify_timeout_seconds": 120,
        },
        sort_keys=True,
    )
    store.update(main_verify_task)

    git = MagicMock(spec=Git)
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "topic"
    git.rev_parse_if_exists.side_effect = lambda ref: "abc123" if ref == "main" else None

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True),
        config=config,
        git=git,
        target_branch="main",
    )

    descriptions = "\n".join(
        row.next_action["description"]
        for row in rows
        if row.next_action is not None and "description" in row.next_action
    )
    assert not any(row.owner_task.id == main_verify_task.id for row in rows)
    assert "abc123" not in descriptions
    assert "merges halted" not in descriptions
    assert call("main") not in git.rev_parse_if_exists.call_args_list


def test_query_lineage_owner_rows_sanitizes_current_malformed_main_verify_legacy_red(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"

    main_verify_task = store.add(
        "System alert: local main integration verify", task_type="internal", skip_learnings=True
    )
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_command = "./bin/tests"
    main_verify_task.review_verify_status = ""
    main_verify_task.review_verify_exit_status = "1"
    main_verify_task.review_verify_failure = "verify_command failed"
    main_verify_task.review_verify_head_sha = "abc123"
    main_verify_task.output_content = json.dumps(
        {
            "alert_message": "main verify RED at `abc123` - merges halted; phase unit failing",
            "captured_at": "2026-06-23T00:00:00+00:00",
            "environment_identity": _main_verify_environment_identity_payload(),
            "failing_phases": ["unit"], "phase_results": [],
            "gate_enabled": True,
            "head_sha": "abc123",
            "tree_fingerprint": "fp",
            "verify_command": "./bin/tests",
            "verify_timeout_grace_seconds": 5.0,
            "verify_timeout_seconds": 120,
        },
        sort_keys=True,
    )
    store.update(main_verify_task)

    git = MagicMock(spec=Git)
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "topic"
    git.rev_parse_if_exists.side_effect = lambda ref: "abc123" if ref == "refs/heads/main" else "topic-sha"

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True),
        config=config,
        git=git,
        target_branch="main",
    )

    assert rows
    row = rows[0]
    assert row.owner_task.id == main_verify_task.id
    assert row.next_action is not None
    assert row.next_action["needs_attention_reason"] == "main-integration-verify-red"
    assert (
        "main verify evidence unknown for current HEAD; invalid verify status evidence"
        in row.next_action["description"]
    )
    assert "abc123" not in row.next_action["description"]
    assert "RED" not in row.next_action["description"]
    assert "merges halted" not in row.next_action["description"]


def test_query_lineage_owner_rows_keeps_current_missing_main_verify_evidence_visible(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"

    main_verify_task = store.add(
        "System alert: local main integration verify", task_type="internal", skip_learnings=True
    )
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_command = "./bin/tests"
    main_verify_task.review_verify_status = None
    main_verify_task.review_verify_exit_status = "1"
    main_verify_task.review_verify_failure = "verify_command status missing"
    main_verify_task.review_verify_head_sha = "abc123"
    main_verify_task.output_content = json.dumps(
        {
            "alert_message": None,
            "captured_at": "2026-06-23T00:00:00+00:00",
            "environment_identity": _main_verify_environment_identity_payload(),
            "failing_phases": [], "phase_results": [],
            "gate_enabled": True,
            "head_sha": "abc123",
            "tree_fingerprint": "fp",
            "verify_command": "./bin/tests",
            "verify_timeout_grace_seconds": 5.0,
            "verify_timeout_seconds": 120,
        },
        sort_keys=True,
    )
    store.update(main_verify_task)

    git = MagicMock(spec=Git)
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "topic"
    git.rev_parse_if_exists.side_effect = lambda ref: "abc123" if ref == "refs/heads/main" else "topic-sha"

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True),
        config=config,
        git=git,
        target_branch="main",
    )

    assert rows
    row = rows[0]
    assert row.owner_task.id == main_verify_task.id
    assert row.next_action is not None
    assert row.next_action["needs_attention_reason"] == "main-integration-verify-red"
    assert "main verify evidence unknown for current HEAD; verify status unavailable" in row.next_action["description"]
    assert "abc123" not in row.next_action["description"]
    assert "RED" not in row.next_action["description"]
    assert "merges halted" not in row.next_action["description"]


@pytest.mark.parametrize(
    ("live_main_sha", "expected"),
    [
        ("abc123", "main verify evidence unknown for current HEAD; verify status unavailable"),
        ("def456", "main verify evidence stale for current HEAD; verify status unavailable"),
        (None, "main verify evidence unproven for current HEAD; verify status unavailable"),
    ],
)
def test_query_lineage_owner_rows_renders_wholly_missing_main_verify_evidence_as_proof_aware_unknown(
    tmp_path: Path,
    live_main_sha: str | None,
    expected: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"

    main_verify_task = store.add(
        "System alert: local main integration verify", task_type="internal", skip_learnings=True
    )
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_command = "./bin/tests"
    main_verify_task.review_verify_status = None
    main_verify_task.review_verify_exit_status = None
    main_verify_task.review_verify_failure = None
    main_verify_task.review_verify_head_sha = "abc123"
    main_verify_task.output_content = json.dumps(
        {
            "alert_message": None,
            "captured_at": "2026-06-23T00:00:00+00:00",
            "environment_identity": _main_verify_environment_identity_payload(),
            "failing_phases": [], "phase_results": [],
            "gate_enabled": True,
            "head_sha": "abc123",
            "tree_fingerprint": "fp",
            "verify_command": "./bin/tests",
            "verify_timeout_grace_seconds": 5.0,
            "verify_timeout_seconds": 120,
        },
        sort_keys=True,
    )
    store.update(main_verify_task)

    git = MagicMock(spec=Git)
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "main"
    git.rev_parse_if_exists.side_effect = lambda ref: live_main_sha if ref == "refs/heads/main" else "topic-sha"

    with patch("gza.main_integration_verify._compute_tree_fingerprint", return_value="fp"):
        rows = query_lineage_owner_rows(
            store,
            LineageOwnerQuery(limit=None, include_skipped=True),
            config=config,
            git=git,
            target_branch="main",
        )

    assert rows
    row = rows[0]
    assert row.owner_task.id == main_verify_task.id
    assert row.next_action is not None
    assert expected in row.next_action["description"]
    assert "abc123" not in row.next_action["description"]
    assert "RED" not in row.next_action["description"]
    assert "merges halted" not in row.next_action["description"]
    assert "red for" not in row.next_action["description"]
    assert "remediation exhausted" not in row.next_action["description"]


def test_query_lineage_owner_rows_uses_exact_main_ref_for_current_main_verify_proof(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"

    main_verify_task = store.add(
        "System alert: local main integration verify", task_type="internal", skip_learnings=True
    )
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_command = "./bin/tests"
    main_verify_task.review_verify_status = "failed"
    main_verify_task.review_verify_exit_status = "1"
    main_verify_task.review_verify_failure = "verify_command failed"
    main_verify_task.review_verify_head_sha = "aaaaaaaaaaaa1111"
    main_verify_task.output_content = json.dumps(
        {
            "alert_message": "main verify RED at `aaaaaaaaaaaa` - merges halted; phase unit failing",
            "captured_at": "2026-06-23T00:00:00+00:00",
            "environment_identity": _main_verify_environment_identity_payload(),
            "failing_phases": ["unit"], "phase_results": [],
            "gate_enabled": True,
            "head_sha": "aaaaaaaaaaaa1111",
            "tree_fingerprint": "fp",
            "verify_command": "./bin/tests",
            "verify_timeout_grace_seconds": 5.0,
            "verify_timeout_seconds": 120,
        },
        sort_keys=True,
    )
    store.update(main_verify_task)

    git = MagicMock(spec=Git)
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "topic"
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "main": "aaaaaaaaaaaa1111",
        "refs/heads/main": "bbbbbbbbbbbb2222",
    }.get(ref)

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True),
        config=config,
        git=git,
        target_branch="main",
    )

    assert not any(row.owner_task.id == main_verify_task.id for row in rows)
    assert "main" not in [call.args[0] for call in git.rev_parse_if_exists.call_args_list]
    git.rev_parse_if_exists.assert_any_call("refs/heads/main")


def test_query_lineage_owner_rows_weakens_matching_fingerprint_red_when_exact_target_ref_differs(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"

    main_verify_task = store.add(
        "System alert: local main integration verify", task_type="internal", skip_learnings=True
    )
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_command = "./bin/tests"
    main_verify_task.review_verify_status = "failed"
    main_verify_task.review_verify_exit_status = "1"
    main_verify_task.review_verify_failure = "verify_command failed"
    main_verify_task.review_verify_head_sha = "aaaaaaaaaaaa1111"
    main_verify_task.output_content = json.dumps(
        {
            "alert_message": "main verify RED at `aaaaaaaaaaaa` - merges halted; phase unit failing",
            "captured_at": "2026-06-23T00:00:00+00:00",
            "environment_identity": _main_verify_environment_identity_payload(),
            "failing_phases": ["unit"], "phase_results": [],
            "gate_enabled": True,
            "head_sha": "aaaaaaaaaaaa1111",
            "tree_fingerprint": "fp",
            "verify_command": "./bin/tests",
            "verify_timeout_grace_seconds": 5.0,
            "verify_timeout_seconds": 120,
        },
        sort_keys=True,
    )
    store.update(main_verify_task)

    git = MagicMock(spec=Git)
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "main"
    git.rev_parse_if_exists.side_effect = lambda ref: "bbbbbbbbbbbb2222" if ref == "refs/heads/main" else None

    with patch("gza.main_integration_verify._compute_tree_fingerprint", return_value="fp"):
        rows = query_lineage_owner_rows(
            store,
            LineageOwnerQuery(limit=None, include_skipped=True),
            config=config,
            git=git,
            target_branch="main",
        )

    assert rows
    row = rows[0]
    assert row.owner_task.id == main_verify_task.id
    assert row.next_action is not None
    assert row.next_action["needs_attention_reason"] == "main-integration-verify-red"
    assert "main verify red evidence stale at current HEAD" in row.next_action["description"]
    assert "aaaaaaaaaaaa" not in row.next_action["description"]
    assert "merges halted" not in row.next_action["description"]
    git.rev_parse_if_exists.assert_any_call("refs/heads/main")




def test_query_lineage_owner_rows_merges_verified_main_verify_remediation_without_review(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    assert config.require_review_before_merge is True
    assert config.advance_create_reviews is True
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    remediation = store.add(
        "Fix local main integration verify phase `unit`\n\n"
        "Remediation kind: fix\n"
        "Failure signature: phases:unit\n"
        "Tree fingerprint: fp-unit-a\n",
        task_type="implement",
        tags=("system", MAIN_INTEGRATION_VERIFY_TAG),
        trigger_source=MAIN_INTEGRATION_VERIFY_REMEDIATION_TRIGGER_SOURCE,
    )
    assert remediation.id is not None
    _set_completed(
        remediation,
        when=datetime(2026, 5, 10, 9, 0, tzinfo=UTC),
        branch="feature/main-verify-remediation",
        has_commits=True,
    )
    remediation.merge_status = "unmerged"
    store.update(remediation)

    git = MagicMock(spec=Git)
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "topic"
    git.can_merge.return_value = True
    git.count_commits_behind.return_value = 0
    git.branch_exists.return_value = True
    git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(remediation.branch)
    git.rev_parse_if_exists.side_effect = lambda ref: (
        "same-head" if ref == remediation.branch else "base-head" if ref == "main" else None
    )
    _persist_current_green_verify(
        store,
        config,
        owner_task=remediation,
        source_task=remediation,
        head_sha="same-head",
    )

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True),
        config=config,
        git=git,
        target_branch="main",
    )

    row = next(r for r in rows if r.owner_task.id == remediation.id)
    assert row.next_action is not None
    assert row.next_action["type"] == "merge"
    assert row.next_action["description"] == "Merge main-verify remediation after green verify"
    assert store.get_reviews_for_task(remediation.id) == []


def test_query_lineage_owner_rows_merges_main_verify_remediation_with_changes_requested_review(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    remediation = _completed_main_verify_remediation(store, branch="feature/main-verify-with-cr-review")
    review = store.add("Review round 1", task_type="review", depends_on=remediation.id, based_on=remediation.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    review.output_content = "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    review.review_verify_head_sha = "same-head"
    store.update(review)

    git = _main_verify_remediation_git(remediation.branch or "")
    _persist_current_green_verify(store, config, owner_task=remediation, source_task=remediation, head_sha="same-head")

    action = _query_single_owner_action(store, config, git, remediation.id)

    assert action["type"] == "merge"
    assert action["description"] == "Merge main-verify remediation after green verify"
    assert action.get("review_task") is None


def test_query_lineage_owner_rows_merges_main_verify_remediation_with_needs_discussion_review(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    remediation = _completed_main_verify_remediation(store, branch="feature/main-verify-with-needs-discussion-review")
    review = store.add("Review round 1", task_type="review", depends_on=remediation.id, based_on=remediation.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    review.output_content = "## Verdict\n\nVerdict: NEEDS_DISCUSSION\n"
    review.review_verify_head_sha = "same-head"
    store.update(review)

    git = _main_verify_remediation_git(remediation.branch or "")
    _persist_current_green_verify(store, config, owner_task=remediation, source_task=remediation, head_sha="same-head")

    action = _query_single_owner_action(store, config, git, remediation.id)

    assert action["type"] == "merge"
    assert action["description"] == "Merge main-verify remediation after green verify"
    assert action.get("needs_attention_reason") is None


def test_query_lineage_owner_rows_main_verify_remediation_preserves_spec_coherence_before_merge(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0
    config.spec_coherence.enabled = True
    config.spec_coherence.paths = ("specs/behavior/**",)

    remediation = _completed_main_verify_remediation(store, branch="feature/main-verify-with-spec-coherence")
    ordinary_review = store.add(
        "Pending normal review",
        task_type="review",
        depends_on=remediation.id,
        based_on=remediation.id,
    )
    assert ordinary_review.id is not None
    ordinary_review.status = "pending"
    ordinary_review.review_verify_head_sha = "same-head"
    store.update(ordinary_review)

    git = _main_verify_remediation_git(
        remediation.branch or "",
        diff_name_status="M\tspecs/behavior/lifecycle-engine.md\n",
    )
    _persist_current_green_verify(store, config, owner_task=remediation, source_task=remediation, head_sha="same-head")

    action = _query_single_owner_action(store, config, git, remediation.id)

    assert action["type"] == "create_review"
    assert action["review_mode"] == "spec_coherence"
    assert action["description"] == "Create behavior-spec coherence review"

    ordinary_review.status = "in_progress"
    store.update(ordinary_review)
    spec_review = store.add(
        "Spec coherence review",
        task_type="review",
        depends_on=remediation.id,
        based_on=remediation.id,
    )
    assert spec_review.id is not None
    spec_review.status = "completed"
    spec_review.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    spec_review.output_content = "## Verdict\n\nVerdict: APPROVED\n"
    spec_review.review_scope = build_spec_coherence_review_scope(
        implementation_task_id=remediation.id,
        reviewed_head_sha="same-head",
        changed_paths=("specs/behavior/lifecycle-engine.md",),
    )
    spec_review.review_verify_head_sha = "same-head"
    store.update(spec_review)

    action = _query_single_owner_action(store, config, git, remediation.id)

    assert action["type"] == "merge"
    assert action["description"] == "Merge main-verify remediation after green verify"


def test_query_lineage_owner_rows_main_verify_remediation_bypasses_fresh_feedback_improve(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    remediation = _completed_main_verify_remediation(store, branch="feature/main-verify-with-fresh-feedback")
    review = store.add("Review round 1", task_type="review", depends_on=remediation.id, based_on=remediation.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    review.output_content = "## Verdict\n\nVerdict: APPROVED\n"
    review.review_verify_head_sha = "same-head"
    store.update(review)
    improve = store.add("Pending feedback improve", task_type="improve", depends_on=review.id, based_on=remediation.id)
    assert improve.id is not None
    improve.status = "in_progress"
    improve.created_at = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    store.update(improve)
    store.add_comment(remediation.id, "Fresh feedback after the review.")

    git = _main_verify_remediation_git(remediation.branch or "")
    _persist_current_green_verify(store, config, owner_task=remediation, source_task=remediation, head_sha="same-head")

    action = _query_single_owner_action(store, config, git, remediation.id)

    assert action["type"] == "merge"
    assert action["description"] == "Merge main-verify remediation after green verify"
    assert "improve" not in action["type"]




def test_query_lineage_owner_rows_omits_stale_current_main_verify_red_attention_when_gate_removed(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = None

    main_verify_task = store.add(
        "System alert: local main integration verify", task_type="internal", skip_learnings=True
    )
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_command = "./bin/tests"
    main_verify_task.review_verify_status = "failed"
    main_verify_task.review_verify_exit_status = "1"
    main_verify_task.review_verify_failure = "verify_command failed"
    main_verify_task.review_verify_head_sha = "abc123"
    main_verify_task.output_content = json.dumps(
        {
            "alert_message": "main verify RED at `abc123` - merges halted; phase unit failing",
            "captured_at": "2026-06-23T00:00:00+00:00",
            "environment_identity": _main_verify_environment_identity_payload(),
            "failing_phases": ["unit"], "phase_results": [],
            "gate_enabled": True,
            "head_sha": "abc123",
            "tree_fingerprint": "fp",
            "verify_command": "./bin/tests",
            "verify_timeout_grace_seconds": 5.0,
            "verify_timeout_seconds": 120,
        },
        sort_keys=True,
    )
    store.update(main_verify_task)

    git = MagicMock(spec=Git)
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "topic"
    git.rev_parse_if_exists.side_effect = lambda ref: "abc123" if ref == "refs/heads/main" else "topic-sha"

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True),
        config=config,
        git=git,
        target_branch="main",
    )

    assert not any(row.owner_task.id == main_verify_task.id for row in rows)


def test_query_lineage_owner_rows_omits_stale_current_main_verify_red_attention_when_gate_identity_changes(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"

    main_verify_task = store.add(
        "System alert: local main integration verify", task_type="internal", skip_learnings=True
    )
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_command = "./bin/old-verify"
    main_verify_task.review_verify_status = "failed"
    main_verify_task.review_verify_exit_status = "1"
    main_verify_task.review_verify_failure = "verify_command failed"
    main_verify_task.review_verify_head_sha = "abc123"
    main_verify_task.output_content = (
        '{"alert_message":"main verify RED at `abc123` - merges halted; phase unit failing",'
        '"captured_at":"2026-06-23T00:00:00+00:00",'
        '"failing_phases":["unit"],"phase_results":[],'
        '"gate_enabled":true,'
        '"head_sha":"abc123",'
        '"tree_fingerprint":"fp",'
        '"verify_command":"./bin/old-verify",'
        '"verify_timeout_grace_seconds":5.0,'
        '"verify_timeout_seconds":120}'
    )
    store.update(main_verify_task)

    git = MagicMock(spec=Git)
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "topic"
    git.rev_parse_if_exists.side_effect = lambda ref: "abc123" if ref == "refs/heads/main" else "topic-sha"

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True),
        config=config,
        git=git,
        target_branch="main",
    )

    assert not any(row.owner_task.id == main_verify_task.id for row in rows)


def test_query_lineage_owner_rows_omits_stale_current_main_verify_red_attention_when_environment_identity_mismatches(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"

    main_verify_task = store.add(
        "System alert: local main integration verify", task_type="internal", skip_learnings=True
    )
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_command = "./bin/tests"
    main_verify_task.review_verify_status = "failed"
    main_verify_task.review_verify_exit_status = "1"
    main_verify_task.review_verify_failure = "verify_command failed"
    main_verify_task.review_verify_head_sha = "abc123"
    main_verify_task.output_content = json.dumps(
        {
            "alert_message": "main verify RED at `abc123` - merges halted; phase unit failing",
            "captured_at": "2026-06-23T00:00:00+00:00",
            "environment_identity": {
                "runner_class": "container",
                "platform_system": "Linux",
                "platform_machine": "x86_64",
                "python_version": "3.12",
            },
            "failing_phases": ["unit"], "phase_results": [],
            "gate_enabled": True,
            "head_sha": "abc123",
            "tree_fingerprint": "fp",
            "verify_command": "./bin/tests",
            "verify_timeout_grace_seconds": 5.0,
            "verify_timeout_seconds": 120,
        },
        sort_keys=True,
    )
    store.update(main_verify_task)

    git = MagicMock(spec=Git)
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "topic"
    git.rev_parse_if_exists.side_effect = lambda ref: "abc123" if ref == "refs/heads/main" else "topic-sha"

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True),
        config=config,
        git=git,
        target_branch="main",
    )

    assert not any(
        row.next_action is not None and row.next_action.get("needs_attention_reason") == "main-integration-verify-red"
        for row in rows
    )


def test_query_lineage_owner_rows_keeps_visible_main_verify_attention_when_default_branch_fingerprint_probe_fails(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"

    main_verify_task = store.add(
        "System alert: local main integration verify", task_type="internal", skip_learnings=True
    )
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_command = "./bin/tests"
    main_verify_task.review_verify_status = "failed"
    main_verify_task.review_verify_exit_status = "1"
    main_verify_task.review_verify_failure = "verify_command failed"
    main_verify_task.review_verify_head_sha = "abc123"
    main_verify_task.output_content = json.dumps(
        {
            "alert_message": "main verify RED at `abc123` - merges halted; phase unit failing",
            "captured_at": "2026-06-23T00:00:00+00:00",
            "environment_identity": _main_verify_environment_identity_payload(),
            "failing_phases": ["unit"], "phase_results": [],
            "gate_enabled": True,
            "head_sha": "abc123",
            "tree_fingerprint": "fp",
            "verify_command": "./bin/tests",
            "verify_timeout_grace_seconds": 5.0,
            "verify_timeout_seconds": 120,
        },
        sort_keys=True,
    )
    store.update(main_verify_task)

    git = MagicMock(spec=Git)
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "main"
    git.rev_parse_if_exists.side_effect = lambda ref: "abc123" if ref == "refs/heads/main" else "topic-sha"

    with patch("gza.main_integration_verify._compute_tree_fingerprint", return_value=None):
        rows = query_lineage_owner_rows(
            store,
            LineageOwnerQuery(limit=None, include_skipped=True),
            config=config,
            git=git,
            target_branch="main",
        )

    assert rows
    row = rows[0]
    assert row.owner_task.id == main_verify_task.id
    assert row.next_action is not None
    assert row.next_action["needs_attention_reason"] == "main-integration-verify-red"
    assert (
        "main verify freshness unproven at `abc123` - merges halted; exact tree fingerprint unavailable"
        in row.next_action["description"]
    )


@pytest.mark.parametrize("live_main_sha", ["def456", None])
def test_query_lineage_owner_rows_weakens_default_branch_fingerprint_unavailable_when_head_unproven(
    tmp_path: Path,
    live_main_sha: str | None,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"

    main_verify_task = store.add(
        "System alert: local main integration verify", task_type="internal", skip_learnings=True
    )
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_command = "./bin/tests"
    main_verify_task.review_verify_status = "failed"
    main_verify_task.review_verify_exit_status = "1"
    main_verify_task.review_verify_failure = "verify_command failed"
    main_verify_task.review_verify_head_sha = "abc123"
    main_verify_task.output_content = json.dumps(
        {
            "alert_message": "main verify RED at `abc123` - merges halted; phase unit failing",
            "captured_at": "2026-06-23T00:00:00+00:00",
            "environment_identity": _main_verify_environment_identity_payload(),
            "failing_phases": ["unit"], "phase_results": [],
            "gate_enabled": True,
            "head_sha": "abc123",
            "tree_fingerprint": "fp",
            "verify_command": "./bin/tests",
            "verify_timeout_grace_seconds": 5.0,
            "verify_timeout_seconds": 120,
        },
        sort_keys=True,
    )
    store.update(main_verify_task)

    git = MagicMock(spec=Git)
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "main"
    git.rev_parse_if_exists.side_effect = lambda ref: live_main_sha if ref == "refs/heads/main" else "topic-sha"

    with patch("gza.main_integration_verify._compute_tree_fingerprint", return_value=None):
        rows = query_lineage_owner_rows(
            store,
            LineageOwnerQuery(limit=None, include_skipped=True),
            config=config,
            git=git,
            target_branch="main",
        )

    assert rows
    row = rows[0]
    assert row.owner_task.id == main_verify_task.id
    assert row.next_action is not None
    assert row.next_action["needs_attention_reason"] == "main-integration-verify-red"
    assert "main verify freshness unproven at current HEAD" in row.next_action["description"]
    assert "exact tree fingerprint unavailable" in row.next_action["description"]
    assert "abc123" not in row.next_action["description"]
    assert "merges halted" not in row.next_action["description"]




@pytest.mark.parametrize(
    ("live_main_sha", "expected"),
    [
        ("def456", "main verify evidence stale for current HEAD; verify status unavailable"),
        (None, "main verify evidence unproven for current HEAD; verify status unavailable"),
    ],
)
def test_query_lineage_owner_rows_fails_closed_for_malformed_unclassified_main_verify_attention(
    tmp_path: Path,
    live_main_sha: str | None,
    expected: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"

    main_verify_task = store.add(
        "System alert: local main integration verify", task_type="internal", skip_learnings=True
    )
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_command = "./bin/tests"
    main_verify_task.review_verify_status = None
    main_verify_task.review_verify_exit_status = "1"
    main_verify_task.review_verify_failure = "verify_command failed"
    main_verify_task.review_verify_head_sha = "abc123"
    main_verify_task.output_content = json.dumps(
        {
            "alert_message": "legacy alert at `abc123` says merges halted",
            "captured_at": "2026-06-23T00:00:00+00:00",
            "environment_identity": _main_verify_environment_identity_payload(),
            "failing_phases": [], "phase_results": [],
            "gate_enabled": True,
            "head_sha": "abc123",
            "tree_fingerprint": "fp",
            "verify_command": "./bin/tests",
            "verify_timeout_grace_seconds": 5.0,
            "verify_timeout_seconds": 120,
        },
        sort_keys=True,
    )
    store.update(main_verify_task)

    git = MagicMock(spec=Git)
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "main"
    git.rev_parse_if_exists.side_effect = lambda ref: live_main_sha if ref == "refs/heads/main" else "topic-sha"

    with patch("gza.main_integration_verify._compute_tree_fingerprint", return_value="fp"):
        rows = query_lineage_owner_rows(
            store,
            LineageOwnerQuery(limit=None, include_skipped=True),
            config=config,
            git=git,
            target_branch="main",
        )

    assert rows
    row = rows[0]
    assert row.owner_task.id == main_verify_task.id
    assert row.next_action is not None
    assert row.next_action["needs_attention_reason"] == "main-integration-verify-red"
    assert expected in row.next_action["description"]
    assert "abc123" not in row.next_action["description"]
    assert "merges halted" not in row.next_action["description"]
    assert "legacy alert" not in row.next_action["description"]


def test_query_lineage_owner_rows_renders_current_unknown_main_verify_status_as_unknown_evidence(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"

    main_verify_task = store.add(
        "System alert: local main integration verify", task_type="internal", skip_learnings=True
    )
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_command = "./bin/tests"
    main_verify_task.review_verify_status = "mystery"
    main_verify_task.review_verify_exit_status = "42"
    main_verify_task.review_verify_failure = "unexpected verify status"
    main_verify_task.review_verify_head_sha = "abc123"
    main_verify_task.output_content = json.dumps(
        {
            "alert_message": "main verify RED at `abc123` - merges halted; phase unit failing",
            "captured_at": "2026-06-23T00:00:00+00:00",
            "environment_identity": _main_verify_environment_identity_payload(),
            "failing_phases": ["unit"], "phase_results": [],
            "gate_enabled": True,
            "head_sha": "abc123",
            "tree_fingerprint": "fp",
            "verify_command": "./bin/tests",
            "verify_timeout_grace_seconds": 5.0,
            "verify_timeout_seconds": 120,
        },
        sort_keys=True,
    )
    store.update(main_verify_task)

    git = MagicMock(spec=Git)
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "topic"
    git.rev_parse_if_exists.side_effect = lambda ref: "abc123" if ref == "refs/heads/main" else "topic-sha"

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True),
        config=config,
        git=git,
        target_branch="main",
    )

    assert rows
    row = rows[0]
    assert row.owner_task.id == main_verify_task.id
    assert row.next_action is not None
    assert (
        "main verify evidence unknown for current HEAD; unrecognized verify status `mystery`"
        in row.next_action["description"]
    )
    assert "abc123" not in row.next_action["description"]
    assert "main verify RED" not in row.next_action["description"]
    assert "merges halted" not in row.next_action["description"]


@pytest.mark.parametrize(
    ("verify_status", "verify_exit_status", "gate_enabled", "config_verify_command"),
    [
        ("passed", "0", True, "./bin/tests"),
        ("unavailable", "not configured", False, None),
    ],
)
def test_query_lineage_owner_rows_suppresses_incompatible_legacy_main_verify_attention(
    tmp_path: Path,
    verify_status: str,
    verify_exit_status: str,
    gate_enabled: bool,
    config_verify_command: str | None,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = config_verify_command

    main_verify_task = store.add(
        "System alert: local main integration verify", task_type="internal", skip_learnings=True
    )
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_command = "./bin/tests" if gate_enabled else "(verify_command unavailable)"
    main_verify_task.review_verify_status = verify_status
    main_verify_task.review_verify_exit_status = verify_exit_status
    main_verify_task.review_verify_failure = None
    main_verify_task.review_verify_head_sha = "abc123"
    main_verify_task.output_content = json.dumps(
        {
            "alert_message": (
                "main verify RED at `abc123` - merges halted; phase unit failing; "
                "automatic remediation exhausted after 2/2 attempts for phases:unit on fp; "
                "human intervention required"
            ),
            "captured_at": "2026-06-23T00:00:00+00:00",
            "environment_identity": _main_verify_environment_identity_payload() if gate_enabled else None,
            "failure_signature": "phases:unit",
            "failing_phases": ["unit"], "phase_results": [],
            "gate_enabled": gate_enabled,
            "head_sha": "abc123",
            "tree_fingerprint": "fp",
            "verify_command": "./bin/tests" if gate_enabled else None,
            "verify_timeout_grace_seconds": 5.0 if gate_enabled else None,
            "verify_timeout_seconds": 120 if gate_enabled else None,
        },
        sort_keys=True,
    )
    store.update(main_verify_task)

    git = MagicMock(spec=Git)
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "topic"
    git.rev_parse_if_exists.return_value = "abc123"

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True),
        config=config,
        git=git,
        target_branch="main",
    )

    assert not any(row.owner_task.id == main_verify_task.id for row in rows)


@pytest.mark.parametrize(
    ("verify_exit_status", "expected"),
    [
        (
            MAIN_INTEGRATION_VERIFY_FRESHNESS_UNAVAILABLE_EXIT_STATUS,
            ("main verify freshness unproven at `abc123` - merges halted; exact tree fingerprint unavailable"),
        ),
        (
            MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS,
            "main verify misconfigured - verify command launch failed; fix the environment, not the code",
        ),
    ],
)
def test_query_lineage_owner_rows_prefers_structured_special_status_over_legacy_exhaustion(
    tmp_path: Path,
    verify_exit_status: str,
    expected: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"

    main_verify_task = store.add(
        "System alert: local main integration verify", task_type="internal", skip_learnings=True
    )
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_command = "./bin/tests"
    main_verify_task.review_verify_status = "unavailable"
    main_verify_task.review_verify_exit_status = verify_exit_status
    main_verify_task.review_verify_failure = "verify unavailable"
    main_verify_task.review_verify_head_sha = "abc123"
    main_verify_task.output_content = json.dumps(
        {
            "alert_message": (
                "main verify RED at `abc123` - merges halted; phase unit failing; "
                "automatic remediation exhausted after 2/2 attempts for phases:unit on fp; "
                "human intervention required"
            ),
            "captured_at": "2026-06-23T00:00:00+00:00",
            "environment_identity": _main_verify_environment_identity_payload(),
            "failure_signature": "phases:unit",
            "failing_phases": ["unit"], "phase_results": [],
            "gate_enabled": True,
            "head_sha": "abc123",
            "tree_fingerprint": "fp",
            "verify_command": "./bin/tests",
            "verify_timeout_grace_seconds": 5.0,
            "verify_timeout_seconds": 120,
        },
        sort_keys=True,
    )
    store.update(main_verify_task)

    git = MagicMock(spec=Git)
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "topic"
    git.rev_parse_if_exists.side_effect = lambda ref: "abc123" if ref == "refs/heads/main" else "topic-sha"

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True),
        config=config,
        git=git,
        target_branch="main",
    )

    assert rows
    row = rows[0]
    assert row.owner_task.id == main_verify_task.id
    assert row.next_action is not None
    expected_reason = (
        MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_REASON
        if verify_exit_status == MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS
        else MAIN_INTEGRATION_VERIFY_REASON
    )
    assert row.next_action["needs_attention_reason"] == expected_reason
    assert expected in row.next_action["description"]
    assert "human intervention" not in row.next_action["description"]
    if verify_exit_status == MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS:
        assert row.next_action["needs_attention_reason"] != MAIN_INTEGRATION_VERIFY_REASON
        assert "main verify RED" not in row.next_action["description"]
        assert "merges halted" not in row.next_action["description"]


def test_query_lineage_owner_rows_surfaces_canonical_launch_failure_concisely(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"

    main_verify_task = store.add(
        "System alert: local main integration verify", task_type="internal", skip_learnings=True
    )
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_command = "./bin/tests"
    main_verify_task.review_verify_status = "unavailable"
    main_verify_task.review_verify_exit_status = MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS
    main_verify_task.review_verify_failure = (
        "verify_command environment error: could not launch `ruff` for phase `ruff` (not on PATH)"
    )
    main_verify_task.review_verify_head_sha = "abc123"
    main_verify_task.output_content = json.dumps(
        {
            "alert_message": (
                "main verify misconfigured - could not launch `ruff` "
                "for phase `ruff` (not on PATH); fix the environment, not the code"
            ),
            "captured_at": "2026-06-23T00:00:00+00:00",
            "environment_identity": _main_verify_environment_identity_payload(),
            "failure_signature": None,
            "failing_phases": ["ruff"], "phase_results": [],
            "gate_enabled": True,
            "head_sha": "abc123",
            "tree_fingerprint": "fp",
            "verify_command": "./bin/tests",
            "verify_timeout_grace_seconds": 5.0,
            "verify_timeout_seconds": 120,
        },
        sort_keys=True,
    )
    store.update(main_verify_task)

    git = MagicMock(spec=Git)
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "topic"
    git.rev_parse_if_exists.side_effect = lambda ref: "abc123" if ref == "refs/heads/main" else "topic-sha"

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True),
        config=config,
        git=git,
        target_branch="main",
    )

    assert rows
    row = rows[0]
    assert row.owner_task.id == main_verify_task.id
    assert row.next_action is not None
    assert row.next_action["needs_attention_reason"] == MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_REASON
    assert row.next_action["needs_attention_reason"] != MAIN_INTEGRATION_VERIFY_REASON
    assert row.next_action["description"] == (
        "SKIP: main verify misconfigured - could not launch `ruff` "
        "for phase `ruff` (not on PATH); fix the environment, not the code"
    )
    assert row.next_action["description"].count("could not launch `ruff`") == 1
    assert "verify_command environment error" not in row.next_action["description"]
    assert "main verify RED" not in row.next_action["description"]
    assert "merges halted" not in row.next_action["description"]
    loaded_state = load_main_integration_verify_state(store)
    assert loaded_state is not None
    assert main_verify_state_halts_merges(loaded_state) is False
    assert not any(
        task.trigger_source == MAIN_INTEGRATION_VERIFY_REMEDIATION_TRIGGER_SOURCE for task in store.get_all()
    )


@pytest.mark.parametrize("verify_status", ["mystery", ""])
def test_query_lineage_owner_rows_rejects_legacy_exhaustion_for_unknown_invalid_or_empty_status(
    tmp_path: Path,
    verify_status: object,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"

    main_verify_task = store.add(
        "System alert: local main integration verify", task_type="internal", skip_learnings=True
    )
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_command = "./bin/tests"
    main_verify_task.review_verify_status = verify_status if isinstance(verify_status, str) else None
    main_verify_task.review_verify_exit_status = "42"
    main_verify_task.review_verify_failure = "unexpected verify status"
    main_verify_task.review_verify_head_sha = "abc123"
    main_verify_task.output_content = json.dumps(
        {
            "alert_message": (
                "main verify RED at `abc123` - merges halted; "
                "automatic remediation exhausted after 2/2 attempts for phases:unit on fp; "
                "human intervention required"
            ),
            "captured_at": "2026-06-23T00:00:00+00:00",
            "environment_identity": _main_verify_environment_identity_payload(),
            "failure_signature": "phases:unit",
            "failing_phases": ["unit"], "phase_results": [],
            "gate_enabled": True,
            "head_sha": "abc123",
            "tree_fingerprint": "fp",
            "verify_command": "./bin/tests",
            "verify_status": verify_status,
            "verify_timeout_grace_seconds": 5.0,
            "verify_timeout_seconds": 120,
        },
        sort_keys=True,
    )
    store.update(main_verify_task)

    git = MagicMock(spec=Git)
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "topic"
    git.rev_parse_if_exists.side_effect = lambda ref: "abc123" if ref == "refs/heads/main" else "topic-sha"

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True),
        config=config,
        git=git,
        target_branch="main",
    )

    assert rows
    row = rows[0]
    assert row.owner_task.id == main_verify_task.id
    assert row.next_action is not None
    assert "main verify evidence unknown for current HEAD" in row.next_action["description"]
    assert (
        "unrecognized verify status `mystery`" if verify_status == "mystery" else "invalid verify status evidence"
    ) in row.next_action["description"]
    assert "abc123" not in row.next_action["description"]
    assert "RED" not in row.next_action["description"]
    assert "merges halted" not in row.next_action["description"]
    assert "red for" not in row.next_action["description"]
    assert "remediation exhausted" not in row.next_action["description"]


def test_query_lineage_owner_rows_rejects_legacy_exhaustion_for_invalid_non_string_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    state = SimpleNamespace(
        task=SimpleNamespace(id="gza-main", status="completed", task_type="internal"),
        gate_enabled=True,
        head_sha="abc123",
        verify_status=7,
        verify_exit_status="42",
        failing_phases=("unit",),
        phase_results=(),
        failure_signature="phases:unit",
        failure="unexpected verify status",
        alert_message=(
            "main verify RED at `abc123` - merges halted; "
            "automatic remediation exhausted after 2/2 attempts for phases:unit on fp; "
            "human intervention required"
        ),
        red_since=datetime(2026, 6, 24, 12, 5, tzinfo=UTC),
    )
    monkeypatch.setattr(
        "gza.lineage_query.current_main_integration_verify_alert",
        lambda *_args, **_kwargs: SimpleNamespace(
            state=state,
            target_proof=SimpleNamespace(status="current"),
        ),
    )
    git = MagicMock(spec=Git)
    git.default_branch.return_value = "main"

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True),
        config=config,
        git=git,
        target_branch="main",
    )

    assert rows
    row = rows[0]
    assert row.owner_task.id == "gza-main"
    assert row.next_action is not None
    assert (
        "main verify evidence unknown for current HEAD; invalid verify status evidence"
        in row.next_action["description"]
    )
    assert "abc123" not in row.next_action["description"]
    assert "RED" not in row.next_action["description"]
    assert "merges halted" not in row.next_action["description"]
    assert "red for" not in row.next_action["description"]
    assert "remediation exhausted" not in row.next_action["description"]


def test_build_merge_context_from_git_records_warning_and_clears_existing_branches_on_git_error(
    tmp_path: Path,
) -> None:
    """build_merge_context_from_git must set existing_branches=None and record the
    local-branch-list inspection warning when local_branch_names() raises GitError,
    locking the intended narrow (GitError, OSError, ValueError) contract."""

    class _RaisingGit(_MinimalGit):
        def local_branch_names(self) -> frozenset[str]:  # type: ignore[override]
            raise GitError("simulated git failure")

    git = _RaisingGit()
    merge_context = recovery_engine.build_merge_context_from_git(git, "main")

    assert merge_context.existing_branches is None, (
        "existing_branches should be None when local_branch_names() raises GitError"
    )
    assert "local-branch-list" in merge_context._warning_keys, "local-branch-list warning key should be recorded"
    assert merge_context.repository_inspection_warnings, "at least one inspection warning should be recorded"
    assert "failed to list local branches" in merge_context.repository_inspection_warnings[0], (
        "warning text should mention the failed branch listing"
    )


def test_collect_recovery_lane_entries_does_not_call_load_merge_context_when_git_provided(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """collect_recovery_lane_entries must not invoke _load_merge_context when a live
    git/target_branch are threaded through.

    Mirrors the watch-loop test: when git is passed, _query_lineage_owner_rows_with_context
    seeds read_context.merge_context via build_merge_context_from_git before
    list_failed_tasks_for_recovery runs, so the ambient discover=True load is never needed.
    """
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.branch = "feature/recovery-lane"
    failed.completed_at = datetime(2026, 6, 16, 9, 0, tzinfo=UTC)
    store.update(failed)

    def _must_not_be_called(_project_dir: object = None) -> object:
        raise AssertionError(
            "_load_merge_context was called despite holding a live git; "
            "the ambient discover=True load was not eliminated by the pre-seeded merge context"
        )

    monkeypatch.setattr(recovery_engine, "_load_merge_context", _must_not_be_called)
    monkeypatch.setattr(
        "gza.cli.advance_engine.determine_next_action",
        lambda *args, **kwargs: {"type": "noop"},
    )

    git = _MinimalGit(branches=frozenset({failed.branch}))
    entries = collect_recovery_lane_entries(
        store,
        tags=None,
        any_tag=False,
        max_recovery_attempts=1,
        git=git,
        target_branch="main",
    )

    # Must not raise — _load_merge_context was not called.
    assert isinstance(entries, list)


def test_collect_recovery_lane_entries_reuses_supplied_owner_rows_and_read_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    owner = store.add("Failed implement", task_type="implement")
    assert owner.id is not None
    owner.status = "failed"
    owner.failure_reason = "MAX_TURNS"
    owner.branch = "feature/recovery-lane"
    owner.completed_at = datetime(2026, 6, 16, 9, 0, tzinfo=UTC)
    store.update(owner)

    owner_row = LineageOwnerRow(
        owner_task=owner,
        members=(owner,),
        tree=None,
        lineage_status="actionable",
        next_action=None,
        next_action_reason="",
        unresolved_tasks=(owner,),
        unresolved_leaf_summary=(),
        recovery_action_task=owner,
        recovery_leaf_task=owner,
    )
    read_context = RecoveryReadContext()

    def _fake_build_dispatch_preview(*args, owner_rows=None, read_context=None, **kwargs):
        assert owner_rows == (owner_row,)
        assert read_context is supplied_read_context
        return DispatchPreview(entries=(), owner_rows=(owner_row,), read_context=read_context)

    supplied_read_context = read_context
    monkeypatch.setattr("gza.cli._recovery_lane.build_dispatch_preview", _fake_build_dispatch_preview)

    entries = collect_recovery_lane_entries(
        store,
        tags=None,
        any_tag=False,
        max_recovery_attempts=1,
        owner_rows=(owner_row,),
        read_context=read_context,
    )

    assert entries == []
