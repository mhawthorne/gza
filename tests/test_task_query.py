"""Tests for unified task query service."""

import inspect
from dataclasses import fields, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

import gza.recovery_engine as recovery_engine
from gza.cli.advance_engine import determine_next_action
from gza.config import Config
from gza.db import SqliteTaskStore
from gza.lineage_query import LineageOwnerQuery, query_lineage_owner_rows
from gza.query import TaskLineageNode
from gza.task_query import (
    DateFilter,
    PresentationSpec,
    ProjectionSpec,
    ScopedTagScopeGap,
    TaskProjectionPreset,
    TaskQuery,
    TaskQueryPresets,
    TaskQueryService,
    collect_scoped_tag_scope_gaps,
    task_matches_tag_filters,
)


def _store(tmp_path: Path) -> SqliteTaskStore:
    return SqliteTaskStore(tmp_path / "test.db")


def test_search_default_matches_pending_and_internal(tmp_path: Path) -> None:
    store = _store(tmp_path)
    pending = store.add("needle pending", task_type="implement")
    internal = store.add("needle internal", task_type="internal")

    service = TaskQueryService(store)
    result = service.run(TaskQueryPresets.search("needle", limit=None))

    prompts = [row.task.prompt for row in result.rows if hasattr(row, "task")]
    assert pending.prompt in prompts
    assert internal.prompt in prompts


def test_date_filter_completed_excludes_rows_without_completed_at(tmp_path: Path) -> None:
    store = _store(tmp_path)
    pending = store.add("needle pending", task_type="implement")
    pending.created_at = datetime.now(UTC)
    store.update(pending)

    completed = store.add("needle completed", task_type="implement")
    completed.status = "completed"
    completed.created_at = datetime.now(UTC) - timedelta(days=7)
    completed.completed_at = datetime.now(UTC)
    store.update(completed)

    service = TaskQueryService(store)
    query = TaskQueryPresets.search(
        "needle",
        limit=None,
        date_filter=DateFilter(field="completed", start=date.today()),
    )
    result = service.run(query)

    prompts = [row.task.prompt for row in result.rows if hasattr(row, "task")]
    assert "needle completed" in prompts
    assert "needle pending" not in prompts


def test_incomplete_preset_projects_next_action_fields(tmp_path: Path) -> None:
    store = _store(tmp_path)
    failed = store.add("failed impl", task_type="implement")
    failed.status = "failed"
    failed.completed_at = datetime.now(UTC)
    failed.failure_reason = "TEST_FAILURE"
    store.update(failed)

    service = TaskQueryService(store)
    result = service.run(TaskQueryPresets.incomplete(limit=None))

    assert len(result.rows) == 1
    row = result.rows[0]
    assert hasattr(row, "owner_task")
    assert row.values["next_action"] == "unknown"
    assert "missing config/git context" in str(row.values["next_action_reason"])


def test_incomplete_preset_projects_real_next_action_when_context_available(tmp_path: Path) -> None:
    store = _store(tmp_path)
    plan = store.add("completed plan", task_type="plan")
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    service = TaskQueryService(store)
    result = service.run(
        TaskQueryPresets.incomplete(limit=None),
        config=SimpleNamespace(
            max_resume_attempts=1,
            require_review_before_merge=True,
            advance_create_reviews=True,
            max_review_cycles=3,
        ),
        git=SimpleNamespace(),
        target_branch="main",
    )

    assert len(result.rows) == 1
    row = result.rows[0]
    assert hasattr(row, "owner_task")
    assert row.values["next_action"] == "create_plan_review"
    assert row.values["next_action_reason"] == "Create and start plan review task"


def test_incomplete_query_uses_one_read_session_connection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = _store(tmp_path)
    failed = store.add("failed impl", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.completed_at = datetime.now(UTC)
    failed.failure_reason = "TEST_FAILURE"
    store.update(failed)

    opened_connections: list[tuple[bool, object]] = []
    original_open_connection = store._open_connection

    def _tracking_open_connection(*, close_on_exit: bool):
        conn = original_open_connection(close_on_exit=close_on_exit)
        opened_connections.append((close_on_exit, conn))
        return conn

    monkeypatch.setattr(store, "_open_connection", _tracking_open_connection)

    service = TaskQueryService(store)
    result = service.run(TaskQueryPresets.incomplete(limit=None))

    assert len(result.rows) == 1
    assert len([conn for close_on_exit, conn in opened_connections if close_on_exit is False]) == 1


def test_incomplete_preset_flushes_prerequisite_reconciliation_after_read_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)

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
    failed.branch = "feature/task-query-prereq-empty"
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

    service = TaskQueryService(store)
    result = service.run(TaskQueryPresets.incomplete(limit=None))

    assert result.rows == ()
    assert depths
    assert all(depth == 0 for _name, depth in depths)
    merge_unit = store.resolve_merge_unit_for_task(failed.id)
    assert merge_unit is not None
    assert merge_unit.state == "empty"


def test_incomplete_preset_projects_held_plan_as_awaiting_human_when_context_available(tmp_path: Path) -> None:
    store = _store(tmp_path)
    plan = store.add("completed held plan", task_type="plan", auto_implement=False)
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    service = TaskQueryService(store)
    result = service.run(
        TaskQueryPresets.incomplete(limit=None),
        config=SimpleNamespace(
            max_resume_attempts=1,
            require_review_before_merge=True,
            advance_create_reviews=True,
            max_review_cycles=3,
        ),
        git=SimpleNamespace(),
        target_branch="main",
    )

    assert len(result.rows) == 1
    row = result.rows[0]
    assert hasattr(row, "owner_task")
    assert row.values["next_action"] == "awaiting_human"
    assert row.values["next_action_owner_id"] == plan.id
    assert f"uv run gza implement {plan.id}" in str(row.values["next_action_reason"])


def test_collect_scoped_tag_scope_gaps_reports_out_of_scope_blocking_child(tmp_path: Path) -> None:
    store = _store(tmp_path)
    plan = store.add(
        "Scoped owner",
        task_type="plan",
        tags=("202606-recovery", "v0.5.0"),
        auto_implement=False,
    )
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
    store.update(plan)

    child = store.add(
        "Out of scope child",
        task_type="implement",
        based_on=plan.id,
        tags=("v0.5.0",),
    )
    assert child.id is not None

    gaps = collect_scoped_tag_scope_gaps(
        store,
        tag_filters=("202606-recovery",),
        any_tag=False,
    )

    assert gaps == [
        ScopedTagScopeGap(
            owner_id=plan.id,
            blocking_child_id=child.id,
            child_task_type="implement",
            child_status="pending",
            child_tags=("v0.5.0",),
            missing_filter_tags=("202606-recovery",),
            suggested_next_command=f"uv run gza edit {child.id} --add-tag 202606-recovery",
            blocking_state="runnable",
        )
    ]


def test_collect_scoped_tag_scope_gaps_uses_one_read_session_connection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    plan = store.add(
        "Scoped owner",
        task_type="plan",
        tags=("202606-recovery", "v0.5.0"),
        auto_implement=False,
    )
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
    store.update(plan)

    child = store.add(
        "Out of scope child",
        task_type="implement",
        based_on=plan.id,
        tags=("v0.5.0",),
    )
    assert child.id is not None

    opened_connections: list[tuple[bool, object]] = []
    original_open_connection = store._open_connection

    def _tracking_open_connection(*, close_on_exit: bool):
        conn = original_open_connection(close_on_exit=close_on_exit)
        opened_connections.append((close_on_exit, conn))
        return conn

    monkeypatch.setattr(store, "_open_connection", _tracking_open_connection)

    gaps = collect_scoped_tag_scope_gaps(
        store,
        tag_filters=("202606-recovery",),
        any_tag=False,
    )

    assert len(gaps) == 1
    assert len([conn for close_on_exit, conn in opened_connections if close_on_exit is False]) == 1


def test_collect_scoped_tag_scope_gaps_any_tag_suggests_single_matching_tag(tmp_path: Path) -> None:
    store = _store(tmp_path)
    plan = store.add(
        "Scoped owner",
        task_type="plan",
        tags=("202606-recovery", "ops"),
        auto_implement=False,
    )
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
    store.update(plan)

    child = store.add(
        "Out of scope child",
        task_type="implement",
        based_on=plan.id,
        tags=("v0.5.0",),
    )
    assert child.id is not None

    gaps = collect_scoped_tag_scope_gaps(
        store,
        tag_filters=("202606-recovery", "ops"),
        any_tag=True,
    )

    assert len(gaps) == 1
    assert gaps[0].missing_filter_tags == ("202606-recovery", "ops")
    assert gaps[0].suggested_next_command == f"uv run gza edit {child.id} --add-tag 202606-recovery"


def test_collect_scoped_tag_scope_gaps_reports_owner_missing_from_lineage_projection(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    unrelated = store.add("Unrelated scoped failed", task_type="implement", tags=("202606-recovery",))
    assert unrelated.id is not None
    store.mark_failed(unrelated, "verify failed")

    plan = store.add(
        "Scoped owner hidden from owner rows",
        task_type="plan",
        tags=("202606-recovery", "v0.5.0"),
        auto_implement=False,
    )
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
    store.update(plan)

    child = store.add(
        "Out of scope child",
        task_type="implement",
        based_on=plan.id,
        tags=("v0.5.0",),
    )
    assert child.id is not None

    owner_rows = list(
        query_lineage_owner_rows(
            store,
            LineageOwnerQuery(
                limit=None,
                tags=("202606-recovery",),
                any_tag=False,
                include_skipped=True,
                exclude_dropped_from_planning=True,
            ),
        )
    )
    assert [row.owner_task.id for row in owner_rows] == [unrelated.id]

    gaps = collect_scoped_tag_scope_gaps(
        store,
        tag_filters=("202606-recovery",),
        any_tag=False,
    )

    assert ScopedTagScopeGap(
        owner_id=plan.id,
        blocking_child_id=child.id,
        child_task_type="implement",
        child_status="pending",
        child_tags=("v0.5.0",),
        missing_filter_tags=("202606-recovery",),
        suggested_next_command=f"uv run gza edit {child.id} --add-tag 202606-recovery",
        blocking_state="runnable",
    ) in gaps


def test_collect_scoped_tag_scope_gaps_reports_independent_out_of_scope_child_even_with_runnable_in_scope_sibling(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    plan = store.add(
        "Scoped owner",
        task_type="plan",
        tags=("202606-recovery", "v0.5.0"),
        auto_implement=False,
    )
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
    store.update(plan)

    in_scope_child = store.add(
        "Runnable in-scope child",
        task_type="implement",
        based_on=plan.id,
        tags=("202606-recovery", "v0.5.0"),
    )
    assert in_scope_child.id is not None

    out_of_scope_child = store.add(
        "Independent out-of-scope child",
        task_type="review",
        based_on=plan.id,
        tags=("v0.5.0",),
    )
    assert out_of_scope_child.id is not None

    gaps = collect_scoped_tag_scope_gaps(
        store,
        tag_filters=("202606-recovery",),
        any_tag=False,
    )

    assert ScopedTagScopeGap(
        owner_id=plan.id,
        blocking_child_id=out_of_scope_child.id,
        child_task_type="review",
        child_status="pending",
        child_tags=("v0.5.0",),
        missing_filter_tags=("202606-recovery",),
        suggested_next_command=f"uv run gza edit {out_of_scope_child.id} --add-tag 202606-recovery",
        blocking_state="runnable",
    ) in gaps
    assert all(gap.blocking_child_id != in_scope_child.id for gap in gaps)


def test_incomplete_preset_keeps_held_plan_visible_when_pending_dependent_awaits_review(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    plan = store.add("completed held plan", task_type="plan", auto_implement=False)
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    dependent = store.add("blocked dependent", task_type="implement", depends_on=plan.id)
    assert dependent.id is not None

    service = TaskQueryService(store)
    result = service.run(
        TaskQueryPresets.incomplete(limit=None),
        config=SimpleNamespace(
            max_resume_attempts=1,
            require_review_before_merge=True,
            advance_create_reviews=True,
            max_review_cycles=3,
        ),
        git=SimpleNamespace(),
        target_branch="main",
    )

    assert len(result.rows) == 1
    row = result.rows[0]
    assert hasattr(row, "owner_task")
    assert row.owner_task.id == plan.id
    assert row.values["next_action"] == "skip"
    assert row.values["next_action_owner_id"] == plan.id
    assert row.values["next_action_reason"] == "SKIP: implement task already exists for this plan"
    assert row.values["unresolved_ids"] == [plan.id]


def test_incomplete_preset_falls_back_to_owner_for_unknown_subject_task_id(tmp_path: Path) -> None:
    store = _store(tmp_path)
    config = SimpleNamespace(
        max_resume_attempts=1,
        require_review_before_merge=True,
        advance_create_reviews=True,
        max_review_cycles=3,
    )
    git = SimpleNamespace(
        can_merge=lambda source, target: False,
        is_merged=lambda source, target: False,
        resolve_fresh_merge_source=lambda branch: (f"origin/{branch}", None),
        count_commits_behind=lambda source, target: 0,
        get_diff_name_status=lambda revision_range, paths=(), check=False: "",
    )
    impl = store.add("completed implementation", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/unknown-subject"
    impl.has_commits = True
    impl.merge_status = "unmerged"
    store.update(impl)

    review = store.add("failed review", task_type="review", based_on=impl.id, depends_on=impl.id)
    assert review.id is not None
    review.status = "failed"
    review.completed_at = datetime.now(UTC)
    review.failure_reason = "REVIEW_CHANGES_REQUESTED"
    store.update(review)

    service = TaskQueryService(store)
    row = service._collect_lineages_unlimited(  # noqa: SLF001
        TaskQueryPresets.incomplete(limit=None),
        config=config,
        git=git,
        target_branch="main",
    )[0]
    row = replace(
        row,
        next_action_data={
            "type": "needs_discussion",
            "description": "SKIP: manual intervention required",
            "needs_attention_reason": "retry-limit-reached",
            "subject_task_id": "gza-999999",
        },
    )

    projected = service._project_lineage_row(  # noqa: SLF001
        row,
        TaskQueryPresets.incomplete(limit=None),
        config=config,
        git=git,
        target_branch="main",
    )

    assert projected.values["next_action_owner_id"] == impl.id


def test_incomplete_preset_warns_and_falls_back_to_owner_for_missing_subject_task_id(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = _store(tmp_path)
    config = SimpleNamespace(
        max_resume_attempts=1,
        require_review_before_merge=True,
        advance_create_reviews=True,
        max_review_cycles=3,
    )
    git = SimpleNamespace(
        can_merge=lambda source, target: False,
        is_merged=lambda source, target: False,
        resolve_fresh_merge_source=lambda branch: (f"origin/{branch}", None),
        count_commits_behind=lambda source, target: 0,
        get_diff_name_status=lambda revision_range, paths=(), check=False: "",
    )
    impl = store.add("completed implementation", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/missing-subject"
    impl.has_commits = True
    impl.merge_status = "unmerged"
    store.update(impl)

    review = store.add("failed review", task_type="review", based_on=impl.id, depends_on=impl.id)
    assert review.id is not None
    review.status = "failed"
    review.completed_at = datetime.now(UTC)
    review.failure_reason = "REVIEW_CHANGES_REQUESTED"
    store.update(review)

    service = TaskQueryService(store)
    row = service._collect_lineages_unlimited(  # noqa: SLF001
        TaskQueryPresets.incomplete(limit=None),
        config=config,
        git=git,
        target_branch="main",
    )[0]
    row = replace(
        row,
        next_action_data={
            "type": "needs_discussion",
            "description": "SKIP: manual intervention required",
            "needs_attention_reason": "retry-limit-reached",
        },
    )

    with caplog.at_level("WARNING", logger="gza.advance_engine"):
        projected = service._project_lineage_row(  # noqa: SLF001
            row,
            TaskQueryPresets.incomplete(limit=None),
            config=config,
            git=git,
            target_branch="main",
        )

    assert projected.values["next_action_owner_id"] == impl.id
    assert "without subject_task_id" in caplog.text


def test_attention_subject_agrees_across_show_incomplete_and_watch_for_held_plan_lineage(
    tmp_path: Path,
) -> None:
    (tmp_path / "gza.yaml").write_text("project_name: test-project\n")
    store = _store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Merged implement", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 18, 9, 0, tzinfo=UTC)
    impl.branch = "feature/merged-parent"
    impl.has_commits = True
    impl.merge_status = "merged"
    store.update(impl)

    plan = store.add("Held plan", task_type="plan", based_on=impl.id, auto_implement=False)
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime(2026, 5, 18, 10, 0, tzinfo=UTC)
    store.update(plan)

    git = SimpleNamespace(
        can_merge=lambda source, target: True,
        is_merged=lambda source, target: False,
        resolve_fresh_merge_source=lambda branch: (f"origin/{branch}", None),
        count_commits_behind=lambda source, target: 0,
        get_diff_name_status=lambda revision_range, paths=(), check=False: "",
    )

    show_action = determine_next_action(config, store, git, plan, "main")
    assert show_action["subject_task_id"] == plan.id

    service = TaskQueryService(store)
    incomplete_result = service.run(
        TaskQueryPresets.incomplete(limit=None),
        config=config,
        git=git,
        target_branch="main",
    )
    assert len(incomplete_result.rows) == 1
    incomplete_row = incomplete_result.rows[0]
    assert incomplete_row.values["next_action_owner_id"] == plan.id

    from gza.cli.watch import _resolve_watch_attention_display_task, _watch_needs_attention_message

    watch_rows = query_lineage_owner_rows(
        store,
        LineageOwnerQuery(limit=None, include_skipped=True, max_recovery_attempts=1),
        config=config,
        git=git,
        target_branch="main",
    )
    assert len(watch_rows) == 1
    watch_row = watch_rows[0]
    subject_task = _resolve_watch_attention_display_task(store, watch_row)
    assert subject_task.id == plan.id
    message = _watch_needs_attention_message(subject_task, watch_row.next_action or {})
    assert plan.id in message
    assert impl.id not in message


def test_lifecycle_incomplete_prefers_merged_unit_state_over_stale_task_row(tmp_path: Path) -> None:
    store = _store(tmp_path)
    task = store.add("stale task-row merge status", task_type="implement")
    store.mark_completed(task, has_commits=True, branch="feature/stale-task-row-status")
    assert task.id is not None

    unit = store.resolve_merge_unit_for_task(task.id)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "merged")

    task = store.get(task.id)
    assert task is not None
    task.merge_status = "unmerged"
    store.update(task)

    service = TaskQueryService(store)
    result = service.run(
        TaskQuery(
            statuses=("completed",),
            lifecycle_state=("incomplete",),
            limit=None,
        )
    )

    assert result.rows == ()


def test_lifecycle_incomplete_excludes_completed_empty_implementation(tmp_path: Path) -> None:
    store = _store(tmp_path)
    task = store.add("completed empty implementation", task_type="implement")
    store.mark_completed(task, has_commits=True, branch="feature/completed-empty-implementation")
    assert task.id is not None

    unit = store.resolve_merge_unit_for_task(task.id)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "empty")

    service = TaskQueryService(store)
    result = service.run(
        TaskQuery(
            statuses=("completed",),
            lifecycle_state=("incomplete",),
            limit=None,
        )
    )

    assert result.rows == ()


def test_lifecycle_complete_excludes_pending_review_attached_to_merged_unit(tmp_path: Path) -> None:
    store = _store(tmp_path)
    root = store.add("merged implement", task_type="implement")
    store.mark_completed(root, has_commits=True, branch="feature/merged-pending-review")
    assert root.id is not None

    review = store.add("pending review", task_type="review", based_on=root.id, depends_on=root.id)
    assert review.id is not None

    unit = store.resolve_merge_unit_for_task(root.id)
    assert unit is not None
    attached_unit = store.get_or_create_merge_unit_for_task(review)
    assert attached_unit is not None
    assert attached_unit.id == unit.id
    store.set_merge_unit_state(unit.id, "merged")

    service = TaskQueryService(store)
    result = service.run(
        TaskQuery(
            lifecycle_state=("complete",),
            limit=None,
        )
    )

    prompts = [row.task.prompt for row in result.rows if hasattr(row, "task")]
    assert "merged implement" in prompts
    assert "pending review" not in prompts


def test_incomplete_projection_uses_review_flow_for_mergeable_behind_branch(tmp_path: Path) -> None:
    from tests.cli.conftest import make_store, setup_config

    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement stale projection", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/stale-projection"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    service = TaskQueryService(store)
    result = service.run(
        TaskQueryPresets.incomplete(limit=None),
        config=Config.load(tmp_path),
        git=SimpleNamespace(
            can_merge=lambda source, target: True,
            is_merged=lambda source, target: False,
            resolve_fresh_merge_source=lambda branch: ("origin/feature/stale-projection", None),
            count_commits_behind=lambda source, target: 1,
            get_diff_name_status=lambda revision_range, paths=(), check=False: "",
        ),
        target_branch="main",
    )

    assert len(result.rows) == 1
    row = result.rows[0]
    assert row.values["next_action"] == "create_review"
    assert "stale" not in str(row.values["next_action_reason"]).lower()


def test_incomplete_projection_uses_merge_flow_for_approved_behind_branch(tmp_path: Path) -> None:
    from tests.cli.conftest import make_store, setup_config

    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement approved stale projection", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/approved-stale-projection"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review = store.add(
        "Approved review",
        task_type="review",
        depends_on=impl.id,
        based_on=impl.id,
    )
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: APPROVED**"
    store.update(review)

    service = TaskQueryService(store)
    result = service.run(
        TaskQueryPresets.incomplete(limit=None),
        config=Config.load(tmp_path),
        git=SimpleNamespace(
            can_merge=lambda source, target: True,
            is_merged=lambda source, target: False,
            resolve_fresh_merge_source=lambda branch: (
                "origin/feature/approved-stale-projection",
                None,
            ),
            count_commits_behind=lambda source, target: 1,
            get_diff_name_status=lambda revision_range, paths=(), check=False: "",
        ),
        target_branch="main",
    )

    assert len(result.rows) == 1
    row = result.rows[0]
    assert row.values["next_action"] == "merge"
    assert "stale" not in str(row.values["next_action_reason"]).lower()


def test_merge_chain_unmerged_matches_legacy_unmerged_status(tmp_path: Path) -> None:
    store = _store(tmp_path)
    legacy = store.add("legacy unmerged", task_type="implement")
    legacy.status = "unmerged"
    legacy.completed_at = datetime.now(UTC)
    legacy.has_commits = True
    store.update(legacy)

    service = TaskQueryService(store)
    query = TaskQuery(
        statuses=("completed", "unmerged"),
        merge_chain_state=("unmerged",),
        lifecycle_state=("terminal",),
        limit=None,
    )
    result = service.run(query)

    prompts = [row.task.prompt for row in result.rows if hasattr(row, "task")]
    assert "legacy unmerged" in prompts


def test_merge_chain_unmerged_hides_legacy_unmerged_status_when_unit_is_merged(tmp_path: Path) -> None:
    store = _store(tmp_path)
    task = store.add("legacy unmerged status with merged unit", task_type="implement")
    store.mark_completed(task, has_commits=True, branch="feature/legacy-unmerged-merged-unit")
    assert task.id is not None

    unit = store.resolve_merge_unit_for_task(task.id)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "merged")

    task = store.get(task.id)
    assert task is not None
    task.status = "unmerged"
    task.merge_status = None
    store.update(task)

    service = TaskQueryService(store)
    query = TaskQuery(
        statuses=("completed", "unmerged"),
        merge_chain_state=("unmerged",),
        lifecycle_state=("terminal",),
        limit=None,
    )
    result = service.run(query)

    prompts = [row.task.prompt for row in result.rows if hasattr(row, "task")]
    assert "legacy unmerged status with merged unit" not in prompts


def test_branch_merge_state_projects_empty_for_moot_merge_unit(tmp_path: Path) -> None:
    store = _store(tmp_path)
    task = store.add("empty merge unit", task_type="implement")
    store.mark_completed(task, has_commits=False, branch="feature/empty-projection")
    assert task.id is not None
    unit = store.create_merge_unit(
        source_branch=task.branch,
        target_branch=store.default_merge_target(),
        owner_task_id=task.id,
        state="empty",
    )
    store.attach_task_to_merge_unit(task.id, unit.id, "owner")

    service = TaskQueryService(store)
    result = service.run(TaskQueryPresets.search("empty merge unit", limit=None))

    assert len(result.rows) == 1
    assert result.rows[0].values["branch_merge_state"] == "empty"


def test_projection_fields_override_applies_to_task_and_lineage_json(tmp_path: Path) -> None:
    store = _store(tmp_path)
    task = store.add("needle task", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    store.update(task)

    failed = store.add("needle failed", task_type="implement")
    failed.status = "failed"
    failed.completed_at = datetime.now(UTC)
    failed.failure_reason = "TEST_FAILURE"
    store.update(failed)

    service = TaskQueryService(store)

    task_query = replace(
        TaskQueryPresets.search("needle", limit=None),
        projection=ProjectionSpec(fields=("id", "status")),
    )
    task_json = service.run(task_query).to_json()
    assert task_json
    assert set(task_json[0].keys()) == {"id", "status"}

    lineage_query = replace(
        TaskQueryPresets.incomplete(limit=None),
        projection=ProjectionSpec(fields=("id", "next_action")),
    )
    lineage_json = service.run(lineage_query).to_json()
    assert lineage_json
    assert set(lineage_json[0].keys()) == {"id", "next_action"}


def test_projection_preset_override_changes_output_shape(tmp_path: Path) -> None:
    store = _store(tmp_path)
    task = store.add("needle", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    store.update(task)

    service = TaskQueryService(store)
    default_json = service.run(TaskQueryPresets.search("needle", limit=None)).to_json()

    minimal_query = replace(
        TaskQueryPresets.search("needle", limit=None),
        projection=ProjectionSpec(preset=TaskProjectionPreset.JSON_MINIMAL),
    )
    minimal_json = service.run(minimal_query).to_json()

    assert default_json and minimal_json
    assert set(default_json[0].keys()) != set(minimal_json[0].keys())
    assert set(minimal_json[0].keys()) == {"id", "prompt", "status", "task_type"}


def test_unmerged_preset_uses_lineage_scope_and_unmerged_default_projection() -> None:
    query = TaskQueryPresets.unmerged(branch_owner_ids=("gza-1",), limit=7, mode="flat")

    assert query.scope == "lineages"
    assert query.limit == 7
    assert query.branch_owner_ids == ("gza-1",)
    assert query.branch_owner_mode == "unmerged_same_branch"
    assert query.projection.preset == TaskProjectionPreset.UNMERGED_DEFAULT
    assert query.presentation.mode == "flat"


def test_unmerged_branch_owner_filter_uses_same_branch_owner_for_representative_descendant(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    plan = store.add("Branchless plan", task_type="plan")
    plan.status = "completed"
    plan.completed_at = datetime(2026, 2, 12, 9, 0, tzinfo=UTC)
    store.update(plan)
    assert plan.id is not None

    implement = store.add("Branch owner implementation", task_type="implement", depends_on=plan.id)
    implement.status = "completed"
    implement.completed_at = datetime(2026, 2, 12, 10, 0, tzinfo=UTC)
    implement.branch = "feature/representative-descendant"
    implement.has_commits = True
    store.update(implement)
    assert implement.id is not None

    rebase = store.add("Representative rebase", task_type="rebase", based_on=implement.id)
    rebase.status = "completed"
    rebase.completed_at = datetime(2026, 2, 12, 11, 0, tzinfo=UTC)
    rebase.branch = "feature/representative-descendant"
    rebase.has_commits = True
    store.update(rebase)
    assert rebase.id is not None

    unit = store.create_merge_unit(
        source_branch="feature/representative-descendant",
        target_branch="main",
        owner_task_id=plan.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(implement.id, unit.id, "owner")
    store.attach_task_to_merge_unit(rebase.id, unit.id, "rebase")

    service = TaskQueryService(store)
    result = service.run(
        TaskQueryPresets.unmerged(
            branch_owner_ids=(implement.id,),
            merge_unit_ids=(unit.id,),
            task_ids=(rebase.id,),
            limit=None,
            mode="json",
        )
    )

    assert len(result.rows) == 1
    row = result.rows[0]
    assert hasattr(row, "owner_task")
    assert row.owner_task.id == implement.id
    assert row.values["branch_owner_id"] == implement.id


def test_incomplete_date_field_created_vs_effective_affects_lineage_selection(tmp_path: Path) -> None:
    store = _store(tmp_path)

    stale_failed = store.add("stale failed", task_type="implement")
    stale_failed.status = "failed"
    stale_failed.created_at = datetime.now(UTC)
    stale_failed.completed_at = datetime.now(UTC) - timedelta(days=5)
    stale_failed.failure_reason = "TEST_FAILURE"
    store.update(stale_failed)

    service = TaskQueryService(store)
    created_result = service.run(
        TaskQueryPresets.incomplete(
            limit=None,
            date_filter=DateFilter(field="created", days=1),
        )
    )
    effective_result = service.run(
        TaskQueryPresets.incomplete(
            limit=None,
            date_filter=DateFilter(field="effective", days=1),
        )
    )

    assert len(created_result.rows) == 1
    assert len(effective_result.rows) == 0


def test_task_row_plan_fanout_keeps_branch_owner_task_scoped(tmp_path: Path) -> None:
    store = _store(tmp_path)

    plan = store.add("fanout plan", task_type="plan")
    plan.status = "completed"
    plan.completed_at = datetime(2026, 5, 14, 9, 0, tzinfo=UTC)
    store.update(plan)
    assert plan.id is not None

    implement_a = store.add("fanout implement a", task_type="implement", based_on=plan.id)
    implement_a.status = "completed"
    implement_a.completed_at = datetime(2026, 5, 14, 10, 0, tzinfo=UTC)
    implement_a.branch = "feature/fanout-a"
    implement_a.has_commits = True
    implement_a.merge_status = "unmerged"
    store.update(implement_a)
    assert implement_a.id is not None

    implement_b = store.add("fanout implement b", task_type="implement", based_on=plan.id)
    implement_b.status = "completed"
    implement_b.completed_at = datetime(2026, 5, 14, 11, 0, tzinfo=UTC)
    implement_b.branch = "feature/fanout-b"
    implement_b.has_commits = True
    implement_b.merge_status = "unmerged"
    store.update(implement_b)
    assert implement_b.id is not None

    service = TaskQueryService(store)
    result = service.run(
        TaskQueryPresets.search(
            "fanout",
            limit=None,
        )
    )

    rows = {row.task.id: row.values["branch_owner_id"] for row in result.rows if hasattr(row, "task")}
    assert rows[plan.id] == plan.id
    assert rows[implement_a.id] == implement_a.id
    assert rows[implement_b.id] == implement_b.id


def test_incomplete_date_field_completed_excludes_missing_completed_at(tmp_path: Path) -> None:
    store = _store(tmp_path)

    failed_no_completed = store.add("failed unresolved", task_type="implement")
    failed_no_completed.status = "failed"
    failed_no_completed.created_at = datetime.now(UTC)
    failed_no_completed.completed_at = None
    failed_no_completed.failure_reason = "TEST_FAILURE"
    store.update(failed_no_completed)

    service = TaskQueryService(store)
    created_result = service.run(
        TaskQueryPresets.incomplete(
            limit=None,
            date_filter=DateFilter(field="created", days=1),
        )
    )
    completed_result = service.run(
        TaskQueryPresets.incomplete(
            limit=None,
            date_filter=DateFilter(field="completed", days=1),
        )
    )

    assert len(created_result.rows) == 1
    assert len(completed_result.rows) == 0


def test_lineages_incomplete_rejects_multi_task_type_filter(tmp_path: Path) -> None:
    store = _store(tmp_path)
    failed = store.add("failed unresolved", task_type="implement")
    failed.status = "failed"
    failed.completed_at = datetime.now(UTC)
    failed.failure_reason = "TEST_FAILURE"
    store.update(failed)

    service = TaskQueryService(store)
    query = TaskQuery(
        scope="lineages",
        lifecycle_state=("incomplete",),
        task_types=("implement", "review"),
        limit=None,
    )

    with pytest.raises(
        ValueError,
        match="lineages scope with lifecycle_state=incomplete supports at most one task type",
    ):
        service.run(query)


def test_incomplete_limit_applies_once_at_owner_row_level(tmp_path: Path) -> None:
    store = _store(tmp_path)

    root_a = store.add("Root A owner old", task_type="implement")
    root_a.status = "completed"
    root_a.completed_at = datetime.now(UTC) - timedelta(days=30)
    root_a.has_commits = True
    root_a.merge_status = "merged"
    store.update(root_a)
    assert root_a.id is not None

    recent_failed_descendant = store.add(
        "Recent unresolved descendant on A",
        task_type="improve",
        based_on=root_a.id,
        same_branch=True,
    )
    recent_failed_descendant.status = "failed"
    recent_failed_descendant.completed_at = datetime.now(UTC) - timedelta(hours=1)
    recent_failed_descendant.failure_reason = "TEST_FAILURE"
    store.update(recent_failed_descendant)

    root_b = store.add("Root B owner newer", task_type="implement")
    root_b.status = "failed"
    root_b.completed_at = datetime.now(UTC) - timedelta(hours=2)
    root_b.failure_reason = "TEST_FAILURE"
    store.update(root_b)
    assert root_b.id is not None

    service = TaskQueryService(store)
    result = service.run(TaskQueryPresets.incomplete(limit=1))

    assert len(result.rows) == 1


def test_queue_preset_matches_runnable_pickup_order(tmp_path: Path) -> None:
    store = _store(tmp_path)
    first = store.add("First runnable")
    blocked_parent = store.add("Blocking task")
    blocked = store.add("Blocked task", depends_on=blocked_parent.id)
    internal = store.add("Internal task", task_type="internal")
    bumped = store.add("Bumped task")
    assert first.id is not None
    assert blocked.id is not None
    assert internal.id is not None
    assert bumped.id is not None
    store.set_urgent(bumped.id, True)

    service = TaskQueryService(store)
    result = service.run(TaskQueryPresets.queue(limit=None))

    prompts = [row.task.prompt for row in result.rows if hasattr(row, "task")]
    assert prompts == ["Bumped task", "First runnable", "Blocking task"]


def test_queue_preset_filters_to_tags(tmp_path: Path) -> None:
    store = _store(tmp_path)
    release = store.add("Release runnable", tags=("release",))
    backlog = store.add("Backlog runnable", tags=("backlog",))
    assert release.id is not None
    assert backlog.id is not None

    service = TaskQueryService(store)
    result = service.run(TaskQueryPresets.queue(limit=None, tags=("release",)))

    prompts = [row.task.prompt for row in result.rows if hasattr(row, "task")]
    assert prompts == ["Release runnable"]


def test_queue_listing_preset_includes_blocked_tasks_after_runnable_rows(tmp_path: Path) -> None:
    store = _store(tmp_path)
    runnable = store.add("Runnable task")
    blocker = store.add("Blocking task")
    blocked = store.add("Blocked task", depends_on=blocker.id)
    internal = store.add("Internal task", task_type="internal")
    assert runnable.id is not None
    assert blocker.id is not None
    assert blocked.id is not None
    assert internal.id is not None

    service = TaskQueryService(store)
    result = service.run(TaskQueryPresets.queue_listing())

    prompts = [row.task.prompt for row in result.rows if hasattr(row, "task")]
    blocked_flags = [bool(row.values.get("blocked")) for row in result.rows if hasattr(row, "task")]

    assert prompts == ["Runnable task", "Blocking task", "Blocked task"]
    assert blocked_flags == [False, False, True]


def test_task_matches_tag_filters_use_or_by_default_and_and_with_all_tags() -> None:
    assert task_matches_tag_filters(task_tags=("release",), tag_filters=("release", "ops")) is True
    assert task_matches_tag_filters(task_tags=("release",), tag_filters=("release", "ops"), any_tag=False) is False


def test_task_query_tag_filter_matches_any_of_selected_tags_by_default(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.add("Release task", tags=("release",))
    store.add("Backlog task", tags=("backlog",))
    store.add("Ops task", tags=("ops",))

    service = TaskQueryService(store)
    result = service.run(
        TaskQuery(
            scope="tasks",
            tag_filters=("release", "ops"),
            limit=None,
        )
    )

    prompts = [row.task.prompt for row in result.rows if hasattr(row, "task")]
    assert "Release task" in prompts
    assert "Ops task" in prompts
    assert "Backlog task" not in prompts


def test_lineage_scope_tag_filter_prunes_tree_to_matching_members_and_ancestors(tmp_path: Path) -> None:
    store = _store(tmp_path)
    root = store.add("Shared root owner", task_type="implement")
    assert root.id is not None
    store.add("Release child", task_type="implement", tags=("release",), based_on=root.id, same_branch=True)
    store.add("Backlog sibling", task_type="implement", tags=("backlog",), based_on=root.id, same_branch=True)

    service = TaskQueryService(store)
    result = service.run(
        TaskQuery(
            scope="lineages",
            tag_filters=("release",),
            limit=None,
        )
    )

    assert len(result.rows) == 1
    row = result.rows[0]
    assert hasattr(row, "tree")
    assert row.tree is not None
    member_prompts = [task.prompt for task in row.members]
    assert "Shared root owner" in member_prompts
    assert "Release child" in member_prompts
    assert "Backlog sibling" not in member_prompts

    tree_prompts: list[str] = []

    def _collect_prompts(node: TaskLineageNode) -> None:
        tree_prompts.append(node.task.prompt)
        for child in node.children:
            _collect_prompts(child)

    _collect_prompts(row.tree)
    assert "Shared root owner" in tree_prompts
    assert "Release child" in tree_prompts
    assert "Backlog sibling" not in tree_prompts


def test_lineages_incomplete_tag_filter_excludes_unrelated_owners(tmp_path: Path) -> None:
    store = _store(tmp_path)

    release_failed = store.add("Release failed", task_type="implement", tags=("release",))
    release_failed.status = "failed"
    release_failed.completed_at = datetime.now(UTC)
    release_failed.failure_reason = "TEST_FAILURE"
    store.update(release_failed)

    backlog_failed = store.add("Backlog failed", task_type="implement", tags=("backlog",))
    backlog_failed.status = "failed"
    backlog_failed.completed_at = datetime.now(UTC)
    backlog_failed.failure_reason = "TEST_FAILURE"
    store.update(backlog_failed)

    service = TaskQueryService(store)
    result = service.run(
        TaskQuery(
            scope="lineages",
            lifecycle_state=("incomplete",),
            tag_filters=("release",),
            limit=None,
        )
    )

    owners = [row.owner_task.prompt for row in result.rows if hasattr(row, "owner_task")]
    assert owners == ["Release failed"]
def test_lineage_presentation_mode_renders_tree_layout(tmp_path: Path) -> None:
    store = _store(tmp_path)
    root = store.add("Lineage root owner", task_type="implement")
    assert root.id is not None
    store.add("Tagged release child", task_type="implement", tags=("release",), based_on=root.id, same_branch=True)

    service = TaskQueryService(store)
    result = service.run(
        TaskQuery(
            scope="lineages",
            tag_filters=("release",),
            limit=None,
            presentation=PresentationSpec(mode="lineage"),
        )
    )

    rendered = result.render()
    assert "Lineage root owner" in rendered
    assert "Tagged release child" in rendered
    assert "└──" in rendered or "├──" in rendered


def test_default_projection_uses_tags_without_group_field(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.add("Release task", tags=("release",))

    service = TaskQueryService(store)
    rows = service.run(TaskQueryPresets.search("Release", limit=None)).to_json()

    assert rows
    assert "group" not in rows[0]
    assert rows[0]["tags"] == ["release"]


def test_query_api_surfaces_do_not_expose_group_filters() -> None:
    assert "groups" not in {field.name for field in fields(TaskQuery)}
    assert "group" not in inspect.signature(TaskQueryPresets.queue).parameters


def test_explicit_group_projection_returns_no_group_key_for_tasks(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.add("Release task", group="legacy-group", tags=("release",))

    service = TaskQueryService(store)
    rows = service.run(
        TaskQuery(
            scope="tasks",
            limit=None,
            projection=ProjectionSpec(fields=("group",)),
        )
    ).to_json()

    assert rows == [{}]


def test_explicit_group_projection_returns_no_group_key_for_lineages(tmp_path: Path) -> None:
    store = _store(tmp_path)
    owner = store.add("Release owner", group="legacy-group", tags=("release",))
    owner.status = "failed"
    owner.completed_at = datetime.now(UTC)
    owner.failure_reason = "TEST_FAILURE"
    store.update(owner)

    service = TaskQueryService(store)
    rows = service.run(
        TaskQuery(
            scope="lineages",
            lifecycle_state=("incomplete",),
            limit=None,
            projection=ProjectionSpec(fields=("group",)),
        )
    ).to_json()

    assert rows == [{}]


def test_dependency_state_blocked_by_dropped_dep_filters_pending_only(tmp_path: Path) -> None:
    store = _store(tmp_path)

    dropped_dep = store.add("Dropped dependency", task_type="implement")
    dropped_dep.status = "dropped"
    dropped_dep.completed_at = datetime.now(UTC)
    store.update(dropped_dep)
    assert dropped_dep.id is not None

    blocked_pending = store.add("Blocked pending", task_type="implement", depends_on=dropped_dep.id)
    blocked_pending_dropped = store.add("Blocked dropped", task_type="implement", depends_on=dropped_dep.id)
    blocked_pending_dropped.status = "dropped"
    blocked_pending_dropped.completed_at = datetime.now(UTC)
    store.update(blocked_pending_dropped)

    resolved_dep = store.add("Dropped with retry", task_type="plan")
    resolved_dep.status = "dropped"
    resolved_dep.completed_at = datetime.now(UTC) - timedelta(hours=2)
    store.update(resolved_dep)
    assert resolved_dep.id is not None
    retry = store.add("Resolved retry", task_type="plan", based_on=resolved_dep.id)
    retry.status = "completed"
    retry.completed_at = datetime.now(UTC) - timedelta(hours=1)
    store.update(retry)
    blocked_resolved = store.add("Blocked but resolved", task_type="implement", depends_on=resolved_dep.id)

    service = TaskQueryService(store)
    result = service.run(
        TaskQuery(
            scope="tasks",
            statuses=("pending",),
            dependency_state=("blocked_by_dropped_dep",),
            limit=None,
        )
    )

    ids = [row.task.id for row in result.rows if hasattr(row, "task")]
    assert blocked_pending.id in ids
    assert blocked_pending_dropped.id not in ids
    assert blocked_resolved.id not in ids


def test_dependency_state_completed_empty_prereq_is_unblocked(tmp_path: Path) -> None:
    store = _store(tmp_path)

    dep = store.add("Empty dependency", task_type="implement")
    store.mark_completed(dep, has_commits=True, branch="feature/query-empty-default")
    assert dep.id is not None
    unit = store.resolve_merge_unit_for_task(dep.id)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "empty")

    blocked = store.add("Blocked downstream", task_type="implement", depends_on=dep.id)
    ready = store.add("Ready task", task_type="task")

    service = TaskQueryService(store)
    blocked_result = service.run(
        TaskQuery(scope="tasks", statuses=("pending",), dependency_state=("blocked",), limit=None)
    )
    unblocked_result = service.run(
        TaskQuery(scope="tasks", statuses=("pending",), dependency_state=("unblocked",), limit=None)
    )

    blocked_ids = [row.task.id for row in blocked_result.rows if hasattr(row, "task")]
    unblocked_ids = [row.task.id for row in unblocked_result.rows if hasattr(row, "task")]

    assert blocked.id not in blocked_ids
    assert blocked.id in unblocked_ids
    assert ready.id in unblocked_ids


def test_dependency_state_failed_empty_prereq_stays_blocked(tmp_path: Path) -> None:
    store = _store(tmp_path)

    dep = store.add("Empty dependency", task_type="implement")
    store.mark_completed(dep, has_commits=True, branch="feature/query-empty-toggle")
    assert dep.id is not None
    unit = store.resolve_merge_unit_for_task(dep.id)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "empty")
    dep = store.get(dep.id)
    assert dep is not None
    store.mark_failed(dep, failure_reason="UNKNOWN")

    downstream = store.add("Downstream", task_type="implement", depends_on=dep.id)

    service = TaskQueryService(store)
    blocked_result = service.run(
        TaskQuery(scope="tasks", statuses=("pending",), dependency_state=("blocked",), limit=None)
    )
    unblocked_result = service.run(
        TaskQuery(scope="tasks", statuses=("pending",), dependency_state=("unblocked",), limit=None)
    )

    blocked_ids = [row.task.id for row in blocked_result.rows if hasattr(row, "task")]
    unblocked_ids = [row.task.id for row in unblocked_result.rows if hasattr(row, "task")]

    assert downstream.id in blocked_ids
    assert downstream.id not in unblocked_ids


def test_incomplete_preset_failed_empty_prereq_sets_awaiting_human(tmp_path: Path) -> None:
    store = _store(tmp_path)

    dep = store.add("Empty prerequisite", task_type="implement")
    store.mark_completed(dep, has_commits=True, branch="feature/incomplete-empty-toggle")
    assert dep.id is not None
    unit = store.resolve_merge_unit_for_task(dep.id)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "empty")
    dep = store.get(dep.id)
    assert dep is not None
    store.mark_failed(dep, failure_reason="PREREQUISITE_UNMERGED")

    downstream = store.add("Held downstream", task_type="implement", depends_on=dep.id)
    assert downstream.id is not None

    service = TaskQueryService(store)
    result = service.run(
        TaskQueryPresets.incomplete(limit=None),
        config=SimpleNamespace(
            max_resume_attempts=1,
            require_review_before_merge=True,
            advance_create_reviews=True,
            max_review_cycles=3,
        ),
        git=SimpleNamespace(),
        target_branch="main",
    )

    assert len(result.rows) == 1
    row = result.rows[0]
    assert row.owner_task.id == downstream.id
    assert row.values["next_action"] == "awaiting_human"


def test_search_negative_scalar_filters_apply_after_positive_filters(tmp_path: Path) -> None:
    store = _store(tmp_path)

    keep = store.add("needle keep", task_type="implement", tags=("release",))
    keep.status = "completed"
    keep.completed_at = datetime.now(UTC)
    store.update(keep)

    excluded_status = store.add("needle excluded status", task_type="implement", tags=("release",))
    excluded_status.status = "failed"
    excluded_status.completed_at = datetime.now(UTC)
    store.update(excluded_status)

    excluded_type = store.add("needle excluded type", task_type="plan", tags=("release",))
    excluded_type.status = "completed"
    excluded_type.completed_at = datetime.now(UTC)
    store.update(excluded_type)

    excluded_tag = store.add("needle excluded tag", task_type="implement", tags=("release", "blocked"))
    excluded_tag.status = "completed"
    excluded_tag.completed_at = datetime.now(UTC)
    store.update(excluded_tag)

    service = TaskQueryService(store)
    result = service.run(
        TaskQueryPresets.search(
            "needle",
            limit=None,
            statuses=("completed", "failed"),
            exclude_statuses=("failed",),
            task_types=("implement", "plan"),
            exclude_task_types=("plan",),
        )
    )
    filtered = service.run(
        replace(
            result.query,
            tag_filters=("release",),
            exclude_tag_filters=("blocked",),
        )
    )

    prompts = [row.task.prompt for row in filtered.rows if hasattr(row, "task")]
    assert prompts == ["needle keep"]


def test_search_negative_lineage_filters_exclude_matching_roots(tmp_path: Path) -> None:
    store = _store(tmp_path)

    root_a = store.add("needle root a", task_type="implement")
    root_a.status = "completed"
    root_a.completed_at = datetime.now(UTC)
    store.update(root_a)
    assert root_a.id is not None

    child_a = store.add("needle child a", task_type="review", based_on=root_a.id, same_branch=True)
    child_a.status = "completed"
    child_a.completed_at = datetime.now(UTC)
    store.update(child_a)
    assert child_a.id is not None

    root_b = store.add("needle root b", task_type="implement")
    root_b.status = "completed"
    root_b.completed_at = datetime.now(UTC)
    store.update(root_b)
    assert root_b.id is not None

    service = TaskQueryService(store)

    lineage_filtered = service.run(
        TaskQueryPresets.search(
            "needle",
            limit=None,
            exclude_lineage_of=child_a.id,
        )
    )
    lineage_prompts = [row.task.prompt for row in lineage_filtered.rows if hasattr(row, "task")]
    assert lineage_prompts == ["needle root b"]


def test_lineages_incomplete_exclude_task_types_filters_shared_rollup_path(tmp_path: Path) -> None:
    store = _store(tmp_path)

    review_failed = store.add("Review failed", task_type="review")
    review_failed.status = "failed"
    review_failed.completed_at = datetime.now(UTC)
    review_failed.failure_reason = "TEST_FAILURE"
    store.update(review_failed)

    implement_failed = store.add("Implement failed", task_type="implement")
    implement_failed.status = "failed"
    implement_failed.completed_at = datetime.now(UTC)
    implement_failed.failure_reason = "TEST_FAILURE"
    store.update(implement_failed)

    service = TaskQueryService(store)

    excluded = service.run(
        TaskQuery(
            scope="lineages",
            lifecycle_state=("incomplete",),
            exclude_task_types=("review",),
            limit=None,
        )
    )
    excluded_owners = [row.owner_task.prompt for row in excluded.rows if hasattr(row, "owner_task")]
    assert excluded_owners == ["Implement failed"]

    positive = service.run(
        TaskQuery(
            scope="lineages",
            lifecycle_state=("incomplete",),
            task_types=("implement",),
            limit=None,
        )
    )
    positive_owners = [row.owner_task.prompt for row in positive.rows if hasattr(row, "owner_task")]
    assert positive_owners == ["Implement failed"]
