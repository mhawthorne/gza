from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

from gza.config import Config
from gza.db import SqliteTaskStore
from gza.lineage_query import LineageOwnerQuery, query_lineage_owner_rows
from tests.cli.conftest import make_store, setup_config


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
    assert row.next_action["type"] == "create_implement"


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
    assert f"uv run gza implement {plan.id}" in row.next_action["description"]


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


def test_query_lineage_owner_rows_projects_recommend_rebase_action(tmp_path: Path) -> None:
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
    assert row.next_action["type"] == "recommend_rebase"
    assert row.lineage_status == "needs_attention"
