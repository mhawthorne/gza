from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import subprocess
from unittest.mock import ANY, MagicMock, patch

import pytest

from gza import dependency_preconditions as dependency_preconditions_module
import gza.recovery_engine as recovery_engine
from gza.cli._recovery_lane import collect_recovery_lane_entries
from gza.config import Config
from gza.db import SqliteTaskStore
from gza.git import Git, GitError, ResolvedMergeSourceRef
from gza.lineage_query import (
    LineageOwnerQuery,
    _load_indexes,
    _query_lineage_owner_rows_with_context,
    collect_stale_unmerged_sweep_candidates,
    query_lineage_owner_rows,
)
from gza.operator_state import blocked_by_empty_prereq_label
from gza.recovery_read_context import RecoveryReadContext
from gza.recovery_engine import list_failed_tasks_for_recovery
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
        allow_reconcile_mutation=False,
    )


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


def test_query_lineage_owner_rows_tag_filter_keeps_merge_unit_representative(tmp_path: Path) -> None:
    store, tag, owner_id, rebase_id = _build_tag_filtered_merge_unit_case(tmp_path)
    config = Config.load(tmp_path)
    git = MagicMock()
    git.can_merge.return_value = True

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
    git = MagicMock()
    git.can_merge.return_value = True

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
    row = rows[0]
    assert row.owner_task.id == plan.id
    assert row.next_action is not None
    assert row.next_action["type"] == "awaiting_human"
    assert row.next_action["needs_attention_reason"] == "awaiting-human-review"
    assert row.next_action["subject_task_id"] == plan.id
    assert f"uv run gza implement {plan.id}" in row.next_action["description"]


def test_query_lineage_owner_rows_empty_prereq_surfaces_release_valve_by_default(tmp_path: Path) -> None:
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

    assert len(rows) == 1
    row = rows[0]
    assert row.owner_task.id == downstream.id
    assert row.next_action is not None
    assert row.next_action["type"] == "awaiting_human"
    assert "empty prerequisite" in row.next_action["description"]
    assert "gza-4072" in row.next_action["description"]


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
    assert "gza-4072" in row.next_action["description"]


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
    assert store_label == f"blocked by {retry.id} (empty prerequisite; manual release tracked by gza-4072 / `gza edit --clear-depends-on`)"

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
    assert row.next_action["type"] == "needs_rebase"
    unresolved_ids = {task.id for task in row.unresolved_tasks if task.id is not None}
    assert orphan.id not in unresolved_ids


def test_query_lineage_owner_rows_planning_excludes_dropped_descendant_rebase(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

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


def test_query_lineage_owner_rows_keeps_legitimate_impl_branch_rebase_descendant_actionable(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

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
    assert rows_by_owner[failed_impl.id].next_action["type"] == "resume"


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
    assert row.next_action["type"] == "resume"


def test_query_lineage_owner_rows_mergeable_behind_branch_projects_normal_action(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

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
    git.resolve_fresh_merge_source.return_value = ("origin/feature/stale-lineage", None)
    git.count_commits_behind.return_value = 1

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
    git.resolve_fresh_merge_source.return_value = (
        "origin/feature/approved-stale-lineage",
        None,
    )
    git.count_commits_behind.return_value = 1

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

    rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=MagicMock(),
        target_branch="main",
    )

    assert rows == ()


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
    assert [entry.decision.task_id for entry in entries] == []


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
    assert len(read_context_calls) == len(live_owner_ids)
    assert len(store_calls) == len(live_owner_ids)
    assert set(read_context_calls) == set(live_owner_ids)
    assert set(store_calls) == set(live_owner_ids)
    assert not (set(read_context_calls) & set(merged_owner_ids))
    assert not (set(store_calls) & set(merged_owner_ids))


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
