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
