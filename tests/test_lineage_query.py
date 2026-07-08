from __future__ import annotations

import json
import platform
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import ANY, MagicMock, patch

import pytest

import gza.recovery_engine as recovery_engine
from gza import dependency_preconditions as dependency_preconditions_module
from gza.cli._recovery_lane import collect_recovery_lane_entries
from gza.config import Config
from gza.db import MergeUnit, SqliteTaskStore
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
from gza.operator_state import blocked_by_empty_prereq_label
from gza.recovery_engine import list_failed_tasks_for_recovery
from gza.recovery_read_context import RecoveryReadContext
from gza.review_verify_state import persist_verify_gate_artifact
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
    with caplog.at_level("WARNING"):
        visible = _failed_leaf_has_unique_unmerged_work_under_terminal_owner(
            failed_task=failed,
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
    git.rev_parse_if_exists.side_effect = (
        lambda ref: "same-head" if ref == "feature/tag-filtered-merge-unit" else "target-sha" if ref == "main" else None
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

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, tags=("alpha",), include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=MagicMock(),
        target_branch="main",
    )

    assert not rows


def test_collect_stale_unmerged_sweep_candidates_selects_only_old_unlinked_units(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    now = datetime(2026, 6, 20, 12, 0, tzinfo=UTC)

    stale_owner = store.add("Old abandoned implement", task_type="implement")
    assert stale_owner.id is not None
    _set_completed(
        stale_owner,
        when=datetime(2026, 4, 1, 9, 0, tzinfo=UTC),
        branch="feature/stale",
        has_commits=True,
    )
    store.update(stale_owner)
    _set_task_created_at(store, stale_owner.id, when=datetime(2026, 4, 1, 8, 0, tzinfo=UTC))
    stale_review = store.add("Old review", task_type="review", depends_on=stale_owner.id)
    assert stale_review.id is not None
    stale_review.status = "completed"
    stale_review.completed_at = datetime(2026, 4, 2, 9, 0, tzinfo=UTC)
    store.update(stale_review)
    _set_task_created_at(store, stale_review.id, when=datetime(2026, 4, 2, 8, 0, tzinfo=UTC))
    stale_unit = store.create_merge_unit(
        source_branch="feature/stale",
        target_branch="main",
        owner_task_id=stale_owner.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(stale_owner.id, stale_unit.id, "owner")
    store.attach_task_to_merge_unit(stale_review.id, stale_unit.id, "review")
    _set_merge_unit_timestamps(store, stale_unit.id, when=datetime(2026, 4, 2, 9, 0, tzinfo=UTC))

    recent_owner = store.add("Recent implement", task_type="implement")
    assert recent_owner.id is not None
    _set_completed(
        recent_owner,
        when=datetime(2026, 6, 15, 9, 0, tzinfo=UTC),
        branch="feature/recent",
        has_commits=True,
    )
    store.update(recent_owner)
    recent_unit = store.create_merge_unit(
        source_branch="feature/recent",
        target_branch="main",
        owner_task_id=recent_owner.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(recent_owner.id, recent_unit.id, "owner")
    _set_merge_unit_timestamps(store, recent_unit.id, when=datetime(2026, 6, 15, 9, 0, tzinfo=UTC))

    blocked_owner = store.add("Blocked stale implement", task_type="implement")
    assert blocked_owner.id is not None
    _set_completed(
        blocked_owner,
        when=datetime(2026, 4, 3, 9, 0, tzinfo=UTC),
        branch="feature/blocked",
        has_commits=True,
    )
    store.update(blocked_owner)
    _set_task_created_at(store, blocked_owner.id, when=datetime(2026, 4, 3, 8, 0, tzinfo=UTC))
    blocked_unit = store.create_merge_unit(
        source_branch="feature/blocked",
        target_branch="main",
        owner_task_id=blocked_owner.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(blocked_owner.id, blocked_unit.id, "owner")
    _set_merge_unit_timestamps(store, blocked_unit.id, when=datetime(2026, 4, 3, 9, 0, tzinfo=UTC))

    dependent = store.add("Live dependent", task_type="implement", depends_on=blocked_owner.id)
    assert dependent.id is not None
    dependent.status = "pending"
    store.update(dependent)

    candidates = collect_stale_unmerged_sweep_candidates(
        store,
        threshold_days=45,
        now=now,
    )

    assert [candidate.owner_task.id for candidate in candidates] == [stale_owner.id]
    assert candidates[0].drop_task_ids == (stale_owner.id, stale_review.id)


def test_collect_stale_unmerged_sweep_candidates_ignore_resolved_external_dependency_edges(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    now = datetime(2026, 6, 20, 12, 0, tzinfo=UTC)

    merged_upstream = store.add("Merged upstream", task_type="implement")
    assert merged_upstream.id is not None
    _set_completed(
        merged_upstream,
        when=datetime(2026, 4, 1, 9, 0, tzinfo=UTC),
        branch="feature/upstream",
        has_commits=True,
    )
    store.update(merged_upstream)
    _set_task_created_at(store, merged_upstream.id, when=datetime(2026, 4, 1, 8, 0, tzinfo=UTC))
    merged_upstream_unit = store.create_merge_unit(
        source_branch="feature/upstream",
        target_branch="main",
        owner_task_id=merged_upstream.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(merged_upstream.id, merged_upstream_unit.id, "owner")
    _set_merge_unit_timestamps(store, merged_upstream_unit.id, when=datetime(2026, 4, 1, 9, 0, tzinfo=UTC))

    depends_on_resolved = store.add(
        "Stale implement with merged prerequisite",
        task_type="implement",
        depends_on=merged_upstream.id,
    )
    assert depends_on_resolved.id is not None
    _set_completed(
        depends_on_resolved,
        when=datetime(2026, 4, 2, 9, 0, tzinfo=UTC),
        branch="feature/stale-outgoing",
        has_commits=True,
    )
    store.update(depends_on_resolved)
    _set_task_created_at(store, depends_on_resolved.id, when=datetime(2026, 4, 2, 8, 0, tzinfo=UTC))
    depends_on_resolved_unit = store.create_merge_unit(
        source_branch="feature/stale-outgoing",
        target_branch="main",
        owner_task_id=depends_on_resolved.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(depends_on_resolved.id, depends_on_resolved_unit.id, "owner")
    _set_merge_unit_timestamps(store, depends_on_resolved_unit.id, when=datetime(2026, 4, 2, 9, 0, tzinfo=UTC))

    incoming_resolved = store.add("Stale implement with merged dependent", task_type="implement")
    assert incoming_resolved.id is not None
    _set_completed(
        incoming_resolved,
        when=datetime(2026, 4, 3, 9, 0, tzinfo=UTC),
        branch="feature/stale-incoming",
        has_commits=True,
    )
    store.update(incoming_resolved)
    _set_task_created_at(store, incoming_resolved.id, when=datetime(2026, 4, 3, 8, 0, tzinfo=UTC))
    incoming_resolved_unit = store.create_merge_unit(
        source_branch="feature/stale-incoming",
        target_branch="main",
        owner_task_id=incoming_resolved.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(incoming_resolved.id, incoming_resolved_unit.id, "owner")
    _set_merge_unit_timestamps(store, incoming_resolved_unit.id, when=datetime(2026, 4, 3, 9, 0, tzinfo=UTC))

    merged_dependent = store.add(
        "Merged downstream",
        task_type="implement",
        depends_on=incoming_resolved.id,
    )
    assert merged_dependent.id is not None
    _set_completed(
        merged_dependent,
        when=datetime(2026, 4, 4, 9, 0, tzinfo=UTC),
        branch="feature/downstream",
        has_commits=True,
    )
    store.update(merged_dependent)
    _set_task_created_at(store, merged_dependent.id, when=datetime(2026, 4, 4, 8, 0, tzinfo=UTC))
    merged_dependent_unit = store.create_merge_unit(
        source_branch="feature/downstream",
        target_branch="main",
        owner_task_id=merged_dependent.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(merged_dependent.id, merged_dependent_unit.id, "owner")
    _set_merge_unit_timestamps(store, merged_dependent_unit.id, when=datetime(2026, 4, 4, 9, 0, tzinfo=UTC))

    blocked_by_pending = store.add("Stale implement with live dependent", task_type="implement")
    assert blocked_by_pending.id is not None
    _set_completed(
        blocked_by_pending,
        when=datetime(2026, 4, 5, 9, 0, tzinfo=UTC),
        branch="feature/stale-blocked",
        has_commits=True,
    )
    store.update(blocked_by_pending)
    _set_task_created_at(store, blocked_by_pending.id, when=datetime(2026, 4, 5, 8, 0, tzinfo=UTC))
    blocked_by_pending_unit = store.create_merge_unit(
        source_branch="feature/stale-blocked",
        target_branch="main",
        owner_task_id=blocked_by_pending.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(blocked_by_pending.id, blocked_by_pending_unit.id, "owner")
    _set_merge_unit_timestamps(store, blocked_by_pending_unit.id, when=datetime(2026, 4, 5, 9, 0, tzinfo=UTC))

    live_dependent = store.add(
        "Pending downstream",
        task_type="implement",
        depends_on=blocked_by_pending.id,
    )
    assert live_dependent.id is not None
    live_dependent.status = "pending"
    store.update(live_dependent)

    candidates = collect_stale_unmerged_sweep_candidates(
        store,
        threshold_days=45,
        now=now,
    )

    assert {candidate.owner_task.id for candidate in candidates} == {
        depends_on_resolved.id,
        incoming_resolved.id,
    }


def test_query_lineage_owner_rows_without_tag_filter_keeps_merge_unit_representative(tmp_path: Path) -> None:
    store, _tag, owner_id, rebase_id = _build_tag_filtered_merge_unit_case(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0
    git = MagicMock()
    git.branch_exists.return_value = True
    git.can_merge.return_value = True
    git.rev_parse_if_exists.side_effect = (
        lambda ref: "same-head" if ref == "feature/tag-filtered-merge-unit" else "target-sha" if ref == "main" else None
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


def test_query_lineage_owner_rows_uses_shared_ref_preload_helper(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    task = store.add("Completed implement owner", task_type="implement")
    assert task.id is not None
    _set_completed(
        task,
        when=datetime(2026, 5, 10, 9, 0, tzinfo=UTC),
        branch="feature/shared-preload",
        has_commits=True,
    )
    store.update(task)

    unrelated = store.add("Completed unrelated owner", task_type="implement")
    assert unrelated.id is not None
    _set_completed(
        unrelated,
        when=datetime(2026, 5, 10, 10, 0, tzinfo=UTC),
        branch="feature/unrelated-preload",
        has_commits=True,
    )
    store.update(unrelated)

    git = MagicMock()
    git.can_merge.return_value = False

    with patch("gza.lineage_query.prime_advance_planning_refs") as preload:
        rows = query_lineage_owner_rows(
            store,
            LineageOwnerQuery(
                limit=None,
                include_skipped=True,
                max_recovery_attempts=1,
                owner_task_ids=(task.id,),
            ),
            config=config,
            git=git,
            target_branch="main",
        )

    assert len(rows) == 1
    preload.assert_called_once_with(
        git,
        branch_names=ANY,
        target_branch="main",
        warning_logger=ANY,
    )
    assert tuple(preload.call_args.kwargs["branch_names"]) == ("feature/shared-preload",)


def test_query_lineage_owner_rows_task_id_filter_limits_shared_ref_preload_to_matching_owner_lineage(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    owner = store.add("Requested owner", task_type="implement")
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 5, 10, 9, 0, tzinfo=UTC),
        branch="feature/requested-owner",
        has_commits=True,
    )
    store.update(owner)

    member = store.add(
        "Requested member",
        task_type="improve",
        based_on=owner.id,
        same_branch=True,
    )
    assert member.id is not None
    _set_completed(
        member,
        when=datetime(2026, 5, 10, 10, 0, tzinfo=UTC),
        branch="feature/requested-owner",
        has_commits=True,
    )
    store.update(member)

    unrelated_owner = store.add("Unrelated owner", task_type="implement")
    assert unrelated_owner.id is not None
    _set_completed(
        unrelated_owner,
        when=datetime(2026, 5, 10, 11, 0, tzinfo=UTC),
        branch="feature/unrelated-owner",
        has_commits=True,
    )
    store.update(unrelated_owner)

    git = MagicMock()
    git.can_merge.return_value = False

    with patch("gza.lineage_query.prime_advance_planning_refs") as preload:
        rows = query_lineage_owner_rows(
            store,
            LineageOwnerQuery(
                limit=None,
                include_skipped=True,
                max_recovery_attempts=1,
                task_ids=(member.id,),
            ),
            config=config,
            git=git,
            target_branch="main",
        )

    assert len(rows) == 1
    assert rows[0].owner_task.id == owner.id
    preload.assert_called_once_with(
        git,
        branch_names=ANY,
        target_branch="main",
        warning_logger=ANY,
    )
    assert set(preload.call_args.kwargs["branch_names"]) == {"feature/requested-owner"}


def test_query_lineage_owner_rows_task_id_filter_keeps_skipped_same_branch_manual_resolution(
    tmp_path: Path,
) -> None:
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
        when=datetime(2026, 5, 10, 10, 0, tzinfo=UTC),
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

    unrelated_owner = store.add("Unrelated owner", task_type="implement")
    assert unrelated_owner.id is not None
    _set_completed(
        unrelated_owner,
        when=datetime(2026, 5, 10, 11, 0, tzinfo=UTC),
        branch="feature/unrelated-owner",
        has_commits=True,
    )
    unrelated_owner.merge_status = "unmerged"
    store.update(unrelated_owner)

    git = MagicMock()
    git.branch_exists.return_value = True
    git.can_merge.return_value = True

    unfiltered_rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=git,
        target_branch="main",
    )

    with patch("gza.lineage_query.prime_advance_planning_refs") as preload:
        filtered_rows = query_lineage_owner_rows(
            store,
            LineageOwnerQuery(
                limit=None,
                include_skipped=True,
                max_recovery_attempts=1,
                task_ids=(orphan.id,),
            ),
            config=config,
            git=git,
            target_branch="main",
        )

    assert len(unfiltered_rows) == 2
    baseline_row = next(row for row in unfiltered_rows if row.owner_task.id == impl.id)
    assert baseline_row.next_action is not None
    assert baseline_row.next_action["type"] == "needs_discussion"
    assert baseline_row.next_action["needs_attention_reason"] == "no-descendant-on-the-impl-branch"

    assert len(filtered_rows) == 1
    filtered_row = filtered_rows[0]
    assert filtered_row.owner_task.id == impl.id
    assert filtered_row.lifecycle_action_task is None
    assert filtered_row.next_action == baseline_row.next_action
    assert {task.id for task in filtered_row.unresolved_tasks if task.id is not None} == {orphan.id}

    preload.assert_called_once_with(
        git,
        branch_names=ANY,
        target_branch="main",
        warning_logger=ANY,
    )
    assert tuple(preload.call_args.kwargs["branch_names"]) == ("feature/canonical",)


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


def test_query_lineage_owner_rows_mixed_owner_and_task_filters_keep_matching_owner_lineage(
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
                task_ids=(member_a.id,),
            ),
            config=config,
            git=git,
            target_branch="main",
        )

    assert len(rows) == 1
    assert rows[0].owner_task.id == owner_a.id
    preload.assert_called_once_with(
        git,
        branch_names=ANY,
        target_branch="main",
        warning_logger=ANY,
    )
    assert tuple(preload.call_args.kwargs["branch_names"]) == ("feature/owner-a", "feature/owner-a")


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


def test_query_lineage_owner_rows_surfaces_strict_scope_violation_paths(tmp_path: Path) -> None:
    from gza.runner import ProjectBoundary

    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.project_dir = tmp_path / "services" / "foo"
    config.project_dir.mkdir(parents=True, exist_ok=True)
    config.enforce_project_scope = True
    setattr(
        config,
        "_project_boundary_cache",
        ProjectBoundary(
            repo_root=tmp_path,
            scope_root=Path("services/foo"),
            local_dependencies=(),
        ),
    )

    impl = store.add("Scoped implement", task_type="implement")
    assert impl.id is not None
    _set_completed(
        impl,
        when=datetime(2026, 5, 10, 9, 0, tzinfo=UTC),
        branch="feature/scoped-violation",
        has_commits=True,
    )
    impl.merge_status = "unmerged"
    store.update(impl)

    git = MagicMock()
    git.branch_exists.return_value = True
    git.can_merge.return_value = True
    git.get_diff_name_status.return_value = "M\tservices/foo/app.py\nM\tdre/web/src/app.tsx\n"

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=git,
        target_branch="main",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.next_action is not None
    assert row.next_action["type"] == "needs_discussion"
    assert row.next_action["needs_attention_reason"] == "project-scope-violation"
    assert row.next_action["out_of_scope_paths"] == ("dre/web/src/app.tsx",)
    assert "Tag `cross-project` and re-advance if intended, or fix the branch." in row.next_action["description"]


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


def test_query_lineage_owner_rows_promotes_completed_plan_descendant_over_explore_root(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    explore = store.add("Explore background jobs", task_type="explore")
    assert explore.id is not None
    _set_completed(
        explore,
        when=datetime(2026, 5, 10, 9, 0, tzinfo=UTC),
        branch=None,
        has_commits=False,
    )
    store.update(explore)

    plan = store.add("Plan background jobs", task_type="plan", based_on=explore.id)
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

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=git,
        target_branch="main",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.owner_task.id == plan.id
    assert row.lifecycle_action_task is not None
    assert row.lifecycle_action_task.id == plan.id
    assert row.next_action is not None
    assert row.next_action["type"] == "create_plan_review"


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

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=git,
        target_branch="main",
    )

    assert rows == ()


def test_blocked_by_empty_prereq_label_uses_direct_empty_dependency_from_read_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    dep = store.add("Empty prerequisite", task_type="implement")
    store.mark_completed(dep, has_commits=True, branch="feature/direct-empty-prereq")
    assert dep.id is not None
    unit = store.resolve_merge_unit_for_task(dep.id)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "empty")

    downstream = store.add("Held downstream", task_type="implement", depends_on=dep.id)
    assert downstream.id is not None

    read_context = _read_context_for_store(store)
    store_label = blocked_by_empty_prereq_label(store, downstream)
    assert store_label is None

    def _unexpected_store_lookup(*_args, **_kwargs):
        raise AssertionError("indexed empty-prerequisite resolution should not hit the store")

    monkeypatch.setattr(store, "get", _unexpected_store_lookup)
    monkeypatch.setattr(store, "resolve_merge_unit_for_task", _unexpected_store_lookup)
    monkeypatch.setattr(store, "resolve_dependency_completion", _unexpected_store_lookup)

    assert blocked_by_empty_prereq_label(store, downstream, read_context=read_context) == store_label


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


def test_query_lineage_owner_rows_prefers_impl_branch_over_orphan_rebase_owner(tmp_path: Path) -> None:
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

    impl_unit = store.create_merge_unit(
        source_branch=impl.branch,
        target_branch="main",
        owner_task_id=impl.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(impl.id, impl_unit.id, "owner")

    review = store.add("Approved review", task_type="review", depends_on=impl.id, based_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 12, 10, 0, tzinfo=UTC)
    review.output_content = "**Verdict: APPROVED**"
    store.update(review)
    store.attach_task_to_merge_unit(review.id, impl_unit.id, "review")

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
    git.branch_exists.return_value = True
    git.rev_parse_if_exists.side_effect = (
        lambda ref: "same-head" if ref == "feature/canonical" else "base-head" if ref == "main" else None
    )
    _persist_current_green_verify(store, config, owner_task=impl, source_task=review, head_sha="same-head")

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=git,
        target_branch="main",
    )

    assert [row.owner_task.id for row in rows] == [impl.id]
    row = rows[0]
    assert row.lifecycle_action_task is not None
    assert row.lifecycle_action_task.id == impl.id
    assert row.next_action is not None
    assert row.next_action["type"] == "merge"
    assert row.next_action["description"] == "Merge (review APPROVED)"


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


def test_query_lineage_owner_rows_excludes_orphan_rebase_descendant_from_actionable_plan(tmp_path: Path) -> None:
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

    orphan = store.add("Completed orphan rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    assert orphan.id is not None
    _set_completed(
        orphan,
        when=datetime(2026, 5, 12, 10, 0, tzinfo=UTC),
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
    git.can_merge.return_value = False
    git.branch_exists.return_value = True
    git.rev_parse_if_exists.side_effect = (
        lambda ref: "same-head" if ref == "feature/canonical" else "base-head" if ref == "main" else None
    )
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
    assert row.owner_task.id == impl.id
    assert row.lifecycle_action_task is not None
    assert row.lifecycle_action_task.id == impl.id
    assert row.next_action is not None
    assert row.next_action["type"] == "create_review"
    unresolved_ids = {task.id for task in row.unresolved_tasks if task.id is not None}
    assert orphan.id not in unresolved_ids


def test_query_lineage_owner_rows_planning_excludes_dropped_descendant_rebase(tmp_path: Path) -> None:
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

    review = store.add("Approved review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 12, 10, 0, tzinfo=UTC)
    review.output_content = "**Verdict: APPROVED**"
    store.update(review)

    dropped_rebase = store.add("Dropped orphan rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    assert dropped_rebase.id is not None
    _set_dropped(
        dropped_rebase,
        when=datetime(2026, 5, 12, 11, 0, tzinfo=UTC),
        branch="feature/orphan",
        has_commits=True,
    )
    dropped_rebase.merge_status = "unmerged"
    store.update(dropped_rebase)

    git = MagicMock()
    git.can_merge.return_value = True
    git.branch_exists.return_value = True
    git.rev_parse_if_exists.side_effect = (
        lambda ref: "same-head" if ref == "feature/canonical" else "base-head" if ref == "main" else None
    )
    _persist_current_green_verify(store, config, owner_task=impl, source_task=impl, head_sha="same-head")

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
    assert row.owner_task.id == impl.id
    assert row.lifecycle_action_task is not None
    assert row.lifecycle_action_task.id == impl.id
    assert row.next_action is not None
    assert row.next_action["type"] == "merge"
    unresolved_ids = {task.id for task in row.unresolved_tasks if task.id is not None}
    assert dropped_rebase.id not in unresolved_ids


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


def test_query_lineage_owner_rows_repeated_planning_omits_superseded_loser_root_and_keeps_winner_rows(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    loser = store.add("Superseded loser", task_type="implement")
    winner = store.add("Winning implement", task_type="implement", based_on=loser.id)
    unrelated = store.add("Unrelated actionable implement", task_type="implement")
    loser_review = store.add("Loser review", task_type="review", based_on=loser.id, depends_on=loser.id)
    assert loser.id is not None
    assert winner.id is not None
    assert unrelated.id is not None
    assert loser_review.id is not None

    loser.branch = "feature/superseded-loser"
    loser.status = "failed"
    loser.failure_reason = "INFRASTRUCTURE_ERROR"
    loser.has_commits = True
    loser.completed_at = datetime(2026, 6, 27, 9, 0, tzinfo=UTC)
    store.update(loser)

    winner.branch = "feature/superseded-winner"
    winner.status = "completed"
    winner.has_commits = True
    winner.merge_status = "unmerged"
    winner.completed_at = datetime(2026, 6, 27, 9, 5, tzinfo=UTC)
    store.update(winner)

    unrelated.branch = "feature/unrelated-root"
    unrelated.status = "completed"
    unrelated.has_commits = True
    unrelated.merge_status = "unmerged"
    unrelated.completed_at = datetime(2026, 6, 27, 9, 10, tzinfo=UTC)
    store.update(unrelated)

    loser_review.status = "pending"
    store.update(loser_review)

    loser_unit = store.create_merge_unit(
        source_branch=loser.branch,
        target_branch="main",
        owner_task_id=loser.id,
        state="unmerged",
    )
    winner_unit = store.create_merge_unit(
        source_branch=winner.branch,
        target_branch="main",
        owner_task_id=winner.id,
        state="unmerged",
    )
    unrelated_unit = store.create_merge_unit(
        source_branch=unrelated.branch,
        target_branch="main",
        owner_task_id=unrelated.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(loser.id, loser_unit.id, "owner")
    store.attach_task_to_merge_unit(loser_review.id, loser_unit.id, "review")
    store.attach_task_to_merge_unit(winner.id, winner_unit.id, "owner")
    store.attach_task_to_merge_unit(unrelated.id, unrelated_unit.id, "owner")

    store.supersede_merge_unit(loser_unit.id, superseded_by_unit_id=winner_unit.id)

    git = MagicMock()
    git.can_merge.return_value = True

    first_rows = query_lineage_owner_rows(
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
    second_rows = query_lineage_owner_rows(
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

    for rows in (first_rows, second_rows):
        rows_by_owner = {row.owner_task.id: row for row in rows if row.owner_task.id is not None}
        assert set(rows_by_owner) == {winner.id, unrelated.id}
        assert rows_by_owner[winner.id].tree is not None
        assert rows_by_owner[winner.id].tree.task.id == winner.id
        rendered_ids = {task.id for task in rows_by_owner[winner.id].members if task.id is not None}
        assert loser.id not in rendered_ids
        assert loser_review.id not in rendered_ids


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


def test_query_lineage_owner_rows_keeps_terminal_owner_visible_when_same_unit_leaf_is_live_unmerged(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    owner = store.add("Merged owner with stale terminal unit", task_type="implement")
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 5, 17, 9, 0, tzinfo=UTC),
        branch="feature/stale-terminal-owner",
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
        "Failed implement leaf advanced owner branch",
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
    git = _LineageMergeStateGit(
        source_ref=owner.branch,
        source_sha="advanced-owner-sha",
        target_sha="target-sha",
        ahead_count=1,
        merged=False,
        net_diff=True,
    )

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=git,
        target_branch="main",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.owner_task.id == owner.id
    assert row.recovery_leaf_task is not None
    assert row.recovery_leaf_task.id == failed_leaf.id
    assert ("count_commits_ahead_checked", f"{owner.branch}->main") in git.probes


def test_query_lineage_owner_rows_keeps_terminal_owner_visible_when_same_unit_leaf_probe_unavailable(
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
    assert row.owner_task.id == owner.id
    assert row.recovery_leaf_task is not None
    assert row.recovery_leaf_task.id == failed_leaf.id


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
    git.branch_exists.return_value = True
    git.rev_parse_if_exists.side_effect = (
        lambda ref: "same-head" if ref == "feature/canonical" else "base-head" if ref == "main" else None
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


def test_query_lineage_owner_rows_planning_keeps_completed_and_failed_live_tasks(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    completed_impl = store.add("Completed implement", task_type="implement")
    assert completed_impl.id is not None
    _set_completed(
        completed_impl,
        when=datetime(2026, 5, 12, 9, 0, tzinfo=UTC),
        branch="feature/completed-live",
        has_commits=True,
    )
    completed_impl.merge_status = "unmerged"
    store.update(completed_impl)

    failed_impl = store.add("Failed implement", task_type="implement")
    assert failed_impl.id is not None
    failed_impl.status = "failed"
    failed_impl.failure_reason = "MAX_STEPS"
    failed_impl.session_id = "sess-failed"
    failed_impl.completed_at = datetime(2026, 5, 12, 10, 0, tzinfo=UTC)
    failed_impl.branch = "feature/failed-live"
    failed_impl.has_commits = True
    store.update(failed_impl)

    git = MagicMock()
    git.can_merge.return_value = True
    git.branch_exists.return_value = True
    git.rev_parse_if_exists.side_effect = (
        lambda ref: "same-head" if ref == "feature/completed-live" else "base-head" if ref == "main" else None
    )
    _persist_current_green_verify(store, config, owner_task=completed_impl, source_task=completed_impl, head_sha="same-head")

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

    rows_by_owner = {row.owner_task.id: row for row in rows if row.owner_task.id is not None}
    assert completed_impl.id in rows_by_owner
    assert failed_impl.id in rows_by_owner
    assert rows_by_owner[completed_impl.id].lifecycle_action_task is not None
    assert rows_by_owner[completed_impl.id].lifecycle_action_task.id == completed_impl.id
    assert rows_by_owner[completed_impl.id].next_action is not None
    assert rows_by_owner[completed_impl.id].next_action["type"] == "create_review"
    assert rows_by_owner[failed_impl.id].recovery_action_task is not None
    assert rows_by_owner[failed_impl.id].recovery_action_task.id == failed_impl.id
    assert rows_by_owner[failed_impl.id].next_action is not None
    assert rows_by_owner[failed_impl.id].next_action["type"] == "needs_rebase"
    assert rows_by_owner[failed_impl.id].next_action["reason"] == "recovery-preflight-rebase"
    assert rows_by_owner[failed_impl.id].next_action["recovery_preflight"]["failed_task_id"] == failed_impl.id


def test_query_lineage_owner_rows_excludes_completed_empty_implementation(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    task = store.add("Completed empty implement", task_type="implement")
    assert task.id is not None
    store.mark_completed(task, has_commits=True, branch="feature/completed-empty-owner")
    unit = store.resolve_merge_unit_for_task(task.id)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "empty")

    git = MagicMock()
    git.can_merge.return_value = True

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

    owner_ids = {row.owner_task.id for row in rows if row.owner_task.id is not None}
    assert task.id not in owner_ids


def test_query_lineage_owner_rows_failed_timeout_no_review_prefers_resume_over_merge(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.require_review_before_merge = False

    failed_impl = store.add("Failed timeout implement", task_type="implement")
    assert failed_impl.id is not None
    failed_impl.status = "failed"
    failed_impl.failure_reason = "TIMEOUT"
    failed_impl.session_id = "sess-timeout"
    failed_impl.completed_at = datetime(2026, 5, 12, 10, 0, tzinfo=UTC)
    failed_impl.branch = "feature/failed-timeout-no-review"
    failed_impl.has_commits = True
    failed_impl.merge_status = "unmerged"
    store.update(failed_impl)

    git = MagicMock()
    git.can_merge.return_value = True
    git.resolve_fresh_merge_source.return_value = ("origin/feature/failed-timeout-no-review", None)

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

    rows_by_owner = {row.owner_task.id: row for row in rows if row.owner_task.id is not None}
    row = rows_by_owner[failed_impl.id]
    assert row.recovery_action_task is not None
    assert row.recovery_action_task.id == failed_impl.id
    assert row.next_action is not None
    assert row.next_action["type"] == "needs_rebase"


def test_query_lineage_owner_rows_mergeable_behind_branch_projects_normal_action(tmp_path: Path) -> None:
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
    git.branch_exists.return_value = True
    git.rev_parse_if_exists.side_effect = (
        lambda ref: "same-head" if ref == "feature/stale-lineage" else "base-head" if ref == "main" else None
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
    assert row.next_action["type"] == "create_review"
    assert row.lineage_status == "actionable"


def test_query_lineage_owner_rows_projects_merge_for_approved_behind_branch(tmp_path: Path) -> None:
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
    git.branch_exists.return_value = True
    git.rev_parse_if_exists.side_effect = (
        lambda ref: "same-head" if ref == "feature/approved-stale-lineage" else "base-head" if ref == "main" else None
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


def test_query_lineage_owner_rows_hides_empty_owner_with_failed_same_branch_descendants(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Completed empty implement", task_type="implement")
    assert impl.id is not None
    _set_completed(
        impl,
        when=datetime(2026, 5, 16, 9, 0, tzinfo=UTC),
        branch="feature/empty-owner-descendants",
        has_commits=True,
    )
    store.update(impl)

    impl_unit = store.get_or_create_merge_unit_for_task(impl)
    assert impl_unit is not None
    store.set_merge_unit_state(impl_unit.id, "empty")

    review = store.add("Failed review", task_type="review", depends_on=impl.id, based_on=impl.id)
    assert review.id is not None
    review.status = "failed"
    review.completed_at = datetime(2026, 5, 16, 10, 0, tzinfo=UTC)
    review.branch = impl.branch
    review.failure_reason = "REVIEW_CHANGES_REQUESTED"
    store.update(review)
    store.attach_task_to_merge_unit(review.id, impl_unit.id, "review")

    improve = store.add("Failed improve", task_type="improve", based_on=review.id, same_branch=True)
    assert improve.id is not None
    improve.status = "failed"
    improve.completed_at = datetime(2026, 5, 16, 11, 0, tzinfo=UTC)
    improve.branch = impl.branch
    improve.has_commits = True
    improve.failure_reason = "MAX_TURNS"
    store.update(improve)
    store.attach_task_to_merge_unit(improve.id, impl_unit.id, "improve")

    assert impl.branch is not None
    git = _LineageMergeStateGit(
        source_ref=impl.branch,
        source_sha="impl-sha",
        target_sha="target-sha",
        ahead_count=0,
        merged=False,
        net_diff=False,
    )

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=git,
        target_branch="main",
    )

    assert rows == ()
    assert git.probes == []


def test_query_lineage_owner_rows_hides_terminal_owner_with_moot_failed_implement_leaf(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    owner = store.add("Completed redundant implement", task_type="implement")
    assert owner.id is not None
    _set_completed(
        owner,
        when=datetime(2026, 5, 17, 9, 0, tzinfo=UTC),
        branch="feature/redundant-owner",
        has_commits=True,
    )
    store.update(owner)

    owner_unit = store.get_or_create_merge_unit_for_task(owner)
    assert owner_unit is not None
    store.set_merge_unit_state(owner_unit.id, "redundant")

    failed_leaf = store.add(
        "Failed implement follow-up on landed branch",
        task_type="implement",
        based_on=owner.id,
        recovery_origin="manual",
    )
    assert failed_leaf.id is not None
    failed_leaf.status = "failed"
    failed_leaf.failure_reason = "NO_ACTIVITY"
    failed_leaf.branch = owner.branch
    failed_leaf.has_commits = True
    failed_leaf.completed_at = datetime(2026, 5, 17, 10, 0, tzinfo=UTC)
    store.update(failed_leaf)
    store.attach_task_to_merge_unit(failed_leaf.id, owner_unit.id, "implement")

    assert owner.branch is not None
    git = _LineageMergeStateGit(
        source_ref=owner.branch,
        source_sha="owner-sha",
        target_sha="target-sha",
        ahead_count=0,
        merged=False,
        net_diff=False,
    )

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=git,
        target_branch="main",
    )

    assert rows == ()
    assert ("count_commits_ahead_checked", f"{owner.branch}->main") in git.probes
    assert ("has_non_empty_source_diff_against_target", f"{owner.branch}->main") in git.probes


def test_query_lineage_owner_rows_keeps_empty_failed_owner_visible_for_recovery_lane(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    failed = store.add("Failed implement owner", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-empty-owner"
    failed.branch = "feature/empty-failed-owner"
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

    assert [task.id for task in list_failed_tasks_for_recovery(store)] == [failed.id]

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=MagicMock(),
        target_branch="main",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.owner_task.id == failed.id
    assert row.recovery_action_task is not None
    assert row.recovery_action_task.id == failed.id
    assert row.recovery_leaf_task is not None
    assert row.recovery_leaf_task.id == failed.id

    entries = collect_recovery_lane_entries(
        store,
        tags=None,
        any_tag=False,
        max_recovery_attempts=1,
    )

    assert len(entries) == 1
    assert entries[0].owner_task.id == failed.id
    assert entries[0].task.id == failed.id
    assert entries[0].decision.action == "resume"


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


def test_collect_recovery_lane_entries_performs_prerequisite_reconciliation_writes_only_after_read_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

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
    failed.branch = "feature/prereq-empty-lane"
    failed.completed_at = datetime(2026, 5, 16, 9, 0, tzinfo=UTC)
    store.update(failed)

    class _EmptyBranchGit:
        def resolve_fresh_merge_source(self, branch: str):
            from gza.git import ResolvedMergeSourceRef

            return ResolvedMergeSourceRef(branch)

        def rev_parse_if_exists(self, ref: str) -> str | None:
            if ref in {"main", failed.branch}:
                return "same-sha"
            return None

        def branch_exists(self, branch: str) -> bool:
            return bool(branch)

        def is_merged(self, branch: str, into: str) -> bool:
            return False

    monkeypatch.setattr(
        recovery_engine,
        "_load_merge_context",
        lambda _project_dir=None: recovery_engine._MergeContext(
            git=_EmptyBranchGit(),
            default_branch="main",
            existing_branches=frozenset({failed.branch}),
        ),
    )

    depths: list[tuple[str, int]] = []
    original_get_or_create = store.get_or_create_merge_unit_for_task
    original_set_state = store.set_merge_unit_state

    def _record_get_or_create(task):
        depths.append(("get_or_create", store._read_session_depth))
        return original_get_or_create(task)

    def _record_set_state(unit_id: str, state: str) -> None:
        depths.append(("set_merge_unit_state", store._read_session_depth))
        original_set_state(unit_id, state)

    monkeypatch.setattr(store, "get_or_create_merge_unit_for_task", _record_get_or_create)
    monkeypatch.setattr(store, "set_merge_unit_state", _record_set_state)

    entries = collect_recovery_lane_entries(
        store,
        tags=None,
        any_tag=False,
        max_recovery_attempts=1,
    )

    assert [entry.task.id for entry in entries] == []
    assert depths
    assert all(depth == 0 for _name, depth in depths)
    merge_unit = store.resolve_merge_unit_for_task(failed.id)
    assert merge_unit is not None
    assert merge_unit.state == "empty"


def test_query_lineage_owner_rows_reconciles_historical_prerequisite_unmerged_empty_branch_outside_read_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

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
    failed.branch = "feature/prereq-empty-watch"
    failed.completed_at = datetime(2026, 5, 16, 9, 0, tzinfo=UTC)
    store.update(failed)

    class _EmptyBranchGit:
        def resolve_fresh_merge_source(self, branch: str):
            from gza.git import ResolvedMergeSourceRef

            return ResolvedMergeSourceRef(branch)

        def rev_parse_if_exists(self, ref: str) -> str | None:
            if ref in {"main", failed.branch}:
                return "same-sha"
            return None

        def branch_exists(self, branch: str) -> bool:
            return bool(branch)

        def is_merged(self, branch: str, into: str) -> bool:
            return False

    monkeypatch.setattr(
        recovery_engine,
        "_load_merge_context",
        lambda _project_dir=None: recovery_engine._MergeContext(
            git=_EmptyBranchGit(),
            default_branch="main",
            existing_branches=frozenset({failed.branch}),
        ),
    )

    depths: list[tuple[str, int]] = []
    original_get_or_create = store.get_or_create_merge_unit_for_task
    original_set_state = store.set_merge_unit_state

    def _record_get_or_create(task):
        depths.append(("get_or_create", store._read_session_depth))
        return original_get_or_create(task)

    def _record_set_state(unit_id: str, state: str) -> None:
        depths.append(("set_merge_unit_state", store._read_session_depth))
        original_set_state(unit_id, state)

    monkeypatch.setattr(store, "get_or_create_merge_unit_for_task", _record_get_or_create)
    monkeypatch.setattr(store, "set_merge_unit_state", _record_set_state)

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
    )

    assert rows == ()
    assert depths
    assert all(depth == 0 for _name, depth in depths)
    merge_unit = store.resolve_merge_unit_for_task(failed.id)
    assert merge_unit is not None
    assert merge_unit.state == "empty"


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
    assert [task.id for task in store.get_based_on_children(parent.id)] == [task.id for task in read_context.get_based_on_children(parent.id)]
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

    older_retry = store.add("Completed retry older", task_type="implement", based_on=dependency.id, recovery_origin="retry")
    assert older_retry.id is not None
    older_retry.status = "completed"
    older_retry.completed_at = datetime(2026, 5, 16, 9, 30, tzinfo=UTC)
    store.update(older_retry)

    newer_retry = store.add("Completed retry newer", task_type="implement", based_on=dependency.id, recovery_origin="retry")
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


def test_load_indexes_excludes_inactive_manual_tombstone_merge_units(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    dropped = store.add("Dropped implementation", task_type="implement")
    superseded = store.add("Superseded implementation", task_type="implement")
    winner = store.add("Winning implementation", task_type="implement")
    assert dropped.id is not None
    assert superseded.id is not None
    assert winner.id is not None

    store.mark_completed(dropped, has_commits=True, branch="feature/indexes-dropped")
    store.mark_completed(superseded, has_commits=True, branch="feature/indexes-superseded")
    store.mark_completed(winner, has_commits=True, branch="feature/indexes-winner")

    dropped_unit = store.resolve_merge_unit_for_task(dropped.id)
    superseded_unit = store.resolve_merge_unit_for_task(superseded.id)
    winner_unit = store.resolve_merge_unit_for_task(winner.id)
    assert dropped_unit is not None
    assert superseded_unit is not None
    assert winner_unit is not None

    store.set_merge_unit_state(dropped_unit.id, "dropped")
    with store._connect() as conn:
        conn.execute(
            """
            UPDATE merge_units
            SET superseded_by_unit_id = ?, updated_at = ?
            WHERE project_id = ? AND id = ?
            """,
            (winner_unit.id, "2026-06-27 12:00:00", store._project_id, superseded_unit.id),
        )
    store.dual_write_legacy_merge_status(superseded_unit.id)

    indexes = _load_indexes(store)

    assert dropped.id not in indexes.merge_units_by_task_id
    assert superseded.id not in indexes.merge_units_by_task_id
    assert indexes.merge_units_by_task_id[winner.id].id == winner_unit.id


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

    failed = store.add("Historical blocked implementation", task_type="implement", depends_on=dependency.id, tags=(tag,))
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
    unresolved_ids = {
        task.id
        for row in rows
        for task in row.unresolved_tasks
        if task.id is not None
    }
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
    unresolved_ids = {
        task.id
        for row in rows
        for task in row.unresolved_tasks
        if task.id is not None
    }
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
    unresolved_ids = {
        task.id
        for row in rows
        for task in row.unresolved_tasks
        if task.id is not None
    }
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
    unresolved_ids = {
        task.id
        for row in rows
        for task in row.unresolved_tasks
        if task.id is not None
    }
    assert failed.id in failed_leaf_ids
    assert failed.id in unresolved_ids


def test_query_lineage_owner_rows_hides_empty_failed_owner_resolved_by_landed_sibling(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    root = store.add("Implementation root", task_type="implement")
    assert root.id is not None
    _set_completed(
        root,
        when=datetime(2026, 5, 16, 8, 0, tzinfo=UTC),
        branch="feature/root",
        has_commits=True,
    )
    store.update(root)

    failed = store.add("Failed manual follow-up", task_type="implement", based_on=root.id, recovery_origin="manual")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-empty-landed"
    failed.branch = "feature/independent-landed"
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

    landed = store.add("Merged sibling representative", task_type="implement", based_on=root.id, recovery_origin="manual")
    assert landed.id is not None
    _set_completed(
        landed,
        when=datetime(2026, 5, 16, 10, 0, tzinfo=UTC),
        branch=failed.branch,
        has_commits=True,
    )
    landed.merge_status = "merged"
    store.update(landed)

    assert list_failed_tasks_for_recovery(store) == []

    assert failed.branch is not None
    git = _LineageMergeStateGit(
        source_ref=failed.branch,
        source_sha="failed-sha",
        target_sha="target-sha",
        ahead_count=0,
        merged=False,
        net_diff=False,
    )

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=git,
        target_branch="main",
    )

    failed_leaf_ids = {
        row.recovery_leaf_task.id
        for row in rows
        if row.recovery_leaf_task is not None and row.recovery_leaf_task.id is not None
    }
    assert failed.id not in failed_leaf_ids
    assert not any(f"{failed.branch}->main" in probe for _kind, probe in git.probes)


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
        caplog.at_level("WARNING", logger="gza.lineage_query"),
        patch(
            "gza.lineage_query.classify_branch_merge_state_for_target",
            side_effect=AssertionError("terminal leaf should short-circuit before classify"),
        ) as classify,
    ):
        result = _failed_leaf_has_unique_unmerged_work_under_terminal_owner(
            failed_task=failed_leaf,
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


def test_failed_rebase_contributor_under_terminal_owner_short_circuits_before_git_proof(
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
        caplog.at_level("WARNING", logger="gza.lineage_query"),
        patch(
            "gza.lineage_query.classify_branch_merge_state_for_target",
            side_effect=AssertionError("terminal contributor should short-circuit before classify"),
        ) as classify,
    ):
        result = _failed_leaf_has_unique_unmerged_work_under_terminal_owner(
            failed_task=failed_rebase,
            owner_merge_unit=owner_unit,
            leaf_merge_unit=owner_unit,
            git=git,
        )

    assert result is False
    classify.assert_not_called()
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


def test_query_lineage_owner_rows_short_circuits_merged_history_before_lineage_walk(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    merged_owner_ids: list[str] = []
    live_owner_ids: list[str] = []
    base_time = datetime(2026, 6, 22, 12, 0, tzinfo=UTC)

    for index in range(5):
        failed = store.add(f"Merged historical failed {index}", task_type="implement")
        assert failed.id is not None
        failed.status = "failed"
        failed.failure_reason = "INFRASTRUCTURE_ERROR"
        failed.branch = f"feature/merged-history-{index}"
        failed.completed_at = base_time.replace(minute=index)
        store.update(failed)
        unit = store.create_merge_unit(
            source_branch=failed.branch,
            target_branch="main",
            owner_task_id=failed.id,
            state="merged",
        )
        store.attach_task_to_merge_unit(failed.id, unit.id, "owner")
        merged_owner_ids.append(failed.id)

    for index in range(2):
        failed = store.add(f"Live unresolved failed {index}", task_type="implement")
        assert failed.id is not None
        failed.status = "failed"
        failed.failure_reason = "INFRASTRUCTURE_ERROR"
        failed.branch = f"feature/live-history-{index}"
        failed.completed_at = base_time.replace(hour=13, minute=index)
        store.update(failed)
        unit = store.create_merge_unit(
            source_branch=failed.branch,
            target_branch="main",
            owner_task_id=failed.id,
            state="unmerged",
        )
        store.attach_task_to_merge_unit(failed.id, unit.id, "owner")
        live_owner_ids.append(failed.id)

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

    assert {row.owner_task.id for row in rows if row.owner_task.id is not None} == set(live_owner_ids)
    assert read_context_calls == []
    assert store_calls == []
    assert not (set(read_context_calls) & set(merged_owner_ids))
    assert not (set(store_calls) & set(merged_owner_ids))


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

    assert {row.owner_task.id for row in rows if row.owner_task.id is not None} == {live_failed.id}
    assert read_context_calls == []
    assert store_calls == []
    assert attached_failed.id not in read_context_calls
    assert attached_failed.id not in store_calls


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

    landed = store.add("Merged sibling representative", task_type="implement", based_on=root.id, recovery_origin="manual")
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
        raise AssertionError(
            "_load_merge_context was called even though git + target_branch were pre-seeded"
        )

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


def test_query_lineage_owner_rows_seeded_git_proven_merged_suppresses_without_landed_sibling(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Seeded git that proves a branch merged suppresses the failed owner even with no
    landed-sibling DB row, directly exercising build_merge_context_from_git's
    existing_branches/is_merged wiring.

    This is the complement of test_query_lineage_owner_rows_seeded_git_drives_same_suppression_as_non_seeded:
    that test covers the lineage-scan fallback (is_merged→False); this one covers the
    git-proven-merged path (is_merged→True) so a regression in that specific wiring
    (e.g. existing_branches populated incorrectly or wrong default_branch) cannot be
    masked by an incidental landed-sibling lineage-scan hit.

    _load_merge_context is patched to raise to confirm the seeded merge context is the
    sole source of merge truth.  A baseline pass with the branch absent from
    existing_branches proves the task does appear in results when the git path cannot
    confirm the merge, so the suppression assertion is non-trivial."""
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    root = store.add("Implementation root", task_type="implement")
    assert root.id is not None
    _set_completed(root, when=datetime(2026, 5, 16, 8, 0, tzinfo=UTC), branch="feature/root-git-proven", has_commits=True)
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

    # No landed-sibling DB row: the only suppression evidence must be the seeded git path.

    class _MergedBranchGit(_MinimalGit):
        """Variant that reports the task branch as merged and returns it as its own source ref.

        resolve_fresh_merge_source returns the branch itself (not ResolvedMergeSourceRef(None))
        so that classify_branch_merge_state_for_target has a non-None source_ref and reaches
        the merged_proof path.  rev_parse_if_exists and count_commits_ahead_checked return None
        so no subprocess is invoked — classify_proven_merged_state then defaults to
        state="merged", which is what drives suppression in _is_resolved_by_landed_lineage."""

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

    # Seeded git-proven path: patch _load_merge_context to raise, proving the seeded
    # context is the sole source of merge truth.
    def _raise_if_called(_project_dir=None):
        raise AssertionError(
            "_load_merge_context was called even though git + target_branch were pre-seeded"
        )

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

    assert failed.id not in failed_ids_git_proven, (
        "seeded git path: failed owner should be suppressed when is_merged proves the "
        "task branch merged into main, even with no landed-sibling DB row"
    )


def test_query_lineage_owner_rows_includes_current_main_verify_red_attention(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"

    main_verify_task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
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
            "alert_message": "main verify RED at `abc123` - merges halted; phase `unit` failing",
            "captured_at": "2026-06-23T00:00:00+00:00",
            "environment_identity": _main_verify_environment_identity_payload(),
            "failing_phase": "unit",
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
    git.rev_parse_if_exists.side_effect = lambda ref: "abc123" if ref == "main" else "topic-sha"

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
    assert "main verify RED at `abc123` - merges halted; phase `unit` failing" in row.next_action["description"]


def test_query_lineage_owner_rows_keeps_auto_refreshable_stale_review_out_of_attention(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.max_review_cycles = 1

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    impl.branch = "feature/owner-row-stale-review-refresh"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review = store.add("Review round 1", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    review.output_content = "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    review.review_verify_head_sha = "reviewed-sha"
    store.update(review)

    improve = store.add("Improve round 1", task_type="improve", based_on=impl.id, depends_on=review.id)
    improve.status = "completed"
    improve.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    improve.branch = impl.branch
    improve.has_commits = True
    improve.changed_diff = True
    store.update(improve)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    git = MagicMock(spec=Git)
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "topic"
    git.can_merge.return_value = True
    git.branch_exists.return_value = True
    git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(impl.branch)
    git.rev_parse_if_exists.side_effect = lambda ref: "current-sha" if ref == impl.branch else None
    _persist_current_green_verify(store, config, owner_task=impl, source_task=improve, head_sha="current-sha")

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True),
        config=config,
        git=git,
        target_branch="main",
    )

    row = next(r for r in rows if r.owner_task.id == impl.id)
    assert row.next_action is not None
    assert row.next_action["type"] == "create_review"
    assert row.next_action.get("needs_attention_reason") is None


def test_query_lineage_owner_rows_surfaces_cleared_review_probe_failure_attention(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from gza import advance_engine as advance_engine_module
    from gza.review_verdict import ParsedReviewReport

    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    impl.branch = "feature/owner-row-cleared-review-probe-failure"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review = store.add("Review round 1", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    review.output_content = "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    review.review_verify_head_sha = "reviewed-sha"
    store.update(review)

    improve = store.add("Improve round 1", task_type="improve", based_on=impl.id, depends_on=review.id)
    improve.status = "completed"
    improve.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    improve.branch = impl.branch
    improve.has_commits = True
    improve.changed_diff = True
    store.update(improve)

    impl.review_cleared_at = improve.completed_at
    store.update(impl)

    unit = store.get_or_create_merge_unit_for_task(impl)
    store.refresh_merge_unit_head(unit.id, head_sha="reviewed-sha")

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review: ParsedReviewReport(
            verdict="CHANGES_REQUESTED",
            findings=(),
            format_version="legacy",
        ),
    )

    git = MagicMock(spec=Git)
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "topic"
    git.can_merge.return_value = True
    git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(impl.branch)
    git.rev_parse_if_exists.side_effect = GitError("probe blew up")

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True),
        config=config,
        git=git,
        target_branch="main",
    )

    row = next(r for r in rows if r.owner_task.id == impl.id)
    assert row.next_action is not None
    assert row.next_action["type"] == "needs_discussion"
    assert row.next_action["needs_attention_reason"] == "review-freshness-unverified"
    assert "branch-head probe failed" in row.next_action["description"]


def test_query_lineage_owner_rows_omits_stale_current_main_verify_red_attention_when_gate_removed(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = None

    main_verify_task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
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
            "alert_message": "main verify RED at `abc123` - merges halted; phase `unit` failing",
            "captured_at": "2026-06-23T00:00:00+00:00",
            "environment_identity": _main_verify_environment_identity_payload(),
            "failing_phase": "unit",
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
    git.rev_parse_if_exists.side_effect = lambda ref: "abc123" if ref == "main" else "topic-sha"

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

    main_verify_task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_command = "./bin/old-verify"
    main_verify_task.review_verify_status = "failed"
    main_verify_task.review_verify_exit_status = "1"
    main_verify_task.review_verify_failure = "verify_command failed"
    main_verify_task.review_verify_head_sha = "abc123"
    main_verify_task.output_content = (
        '{"alert_message":"main verify RED at `abc123` - merges halted; phase `unit` failing",'
        '"captured_at":"2026-06-23T00:00:00+00:00",'
        '"failing_phase":"unit",'
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
    git.rev_parse_if_exists.side_effect = lambda ref: "abc123" if ref == "main" else "topic-sha"

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

    main_verify_task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
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
            "alert_message": "main verify RED at `abc123` - merges halted; phase `unit` failing",
            "captured_at": "2026-06-23T00:00:00+00:00",
            "environment_identity": {
                "runner_class": "container",
                "platform_system": "Linux",
                "platform_machine": "x86_64",
                "python_version": "3.12",
            },
            "failing_phase": "unit",
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
    git.rev_parse_if_exists.side_effect = lambda ref: "abc123" if ref == "main" else "topic-sha"

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True),
        config=config,
        git=git,
        target_branch="main",
    )

    assert not any(
        row.next_action is not None
        and row.next_action.get("needs_attention_reason") == "main-integration-verify-red"
        for row in rows
    )


def test_query_lineage_owner_rows_keeps_visible_main_verify_attention_when_default_branch_fingerprint_probe_fails(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"

    main_verify_task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
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
            "alert_message": "main verify RED at `abc123` - merges halted; phase `unit` failing",
            "captured_at": "2026-06-23T00:00:00+00:00",
            "environment_identity": _main_verify_environment_identity_payload(),
            "failing_phase": "unit",
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
    git.rev_parse_if_exists.side_effect = lambda ref: "abc123" if ref == "main" else "topic-sha"

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
    assert "local-branch-list" in merge_context._warning_keys, (
        "local-branch-list warning key should be recorded"
    )
    assert merge_context.repository_inspection_warnings, (
        "at least one inspection warning should be recorded"
    )
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
