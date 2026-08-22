from __future__ import annotations

import importlib
import sys
from contextlib import ExitStack
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

import gza.cli.watch as watch_module
from gza.cli.advance_executor import (
    AdvanceActionExecutionContext,
    AdvanceActionExecutionResult,
    BranchDivergenceReconcileResult,
    execute_advance_action,
)
from gza.concurrency import ConcurrencySnapshot, MaxConcurrentTasksError
from gza.config import Config
from gza.db import DuplicateActiveChildError
from gza.dispatch_preview import DispatchPreview, DispatchPreviewEntry
from gza.git import Git
from gza.lineage_query import LineageOwnerRow, RecoveryReadContext
from gza.recovery_engine import FailedRecoveryDecision, _MergeContext, decide_failed_task_recovery
from gza.unstick import UnstickOutcome
from tests.cli.conftest import invoke_gza, make_store, setup_config


class _UnstickGitDouble(Git):
    def __init__(self, _project_dir=None) -> None:
        self._cache = None

    def default_branch(self) -> str:
        return "main"

    def current_branch(self) -> str:
        return "main"

    def branch_exists(self, branch: str) -> bool:
        return not branch.startswith("missing/")

    def branches_exist(self, branches: tuple[str, ...]) -> dict[str, bool]:
        return {branch: self.branch_exists(branch) for branch in branches}

    def ref_exists(self, ref: str) -> bool:
        return False

    def resolve_refs(self, refs, peel: str = "commit") -> dict[str, str | None]:
        del peel
        return {str(ref): self.rev_parse_if_exists(str(ref)) for ref in refs}

    def is_merged(self, branch: str, into: str | None = None, use_cherry: bool = False) -> bool:
        del into, use_cherry
        return branch.startswith("merged/")

    def can_merge(self, branch: str, into: str | None = None) -> bool:
        del branch, into
        return True

    def get_diff_numstat(self, revision_range: str) -> str:
        del revision_range
        return "1\t0\tfeature.txt\n"

    def count_commits_ahead_checked(self, branch: str, target: str) -> int | None:
        del target
        if branch.startswith("empty/"):
            return 0
        return 1

    def rev_parse_if_exists(self, ref: str) -> str | None:
        return f"sha-{ref}"

    def local_branch_names(self) -> tuple[str, ...]:
        return ()


def test_unstick_requires_explicit_selector(tmp_path):
    setup_config(tmp_path)

    result = invoke_gza("unstick", "--project", str(tmp_path))

    assert result.returncode == 2
    assert "requires at least one selector" in result.stdout


def test_unstick_dispatches_through_live_parser(tmp_path, monkeypatch):
    setup_config(tmp_path)
    cli_main_module = importlib.import_module("gza.cli.main")
    captured = {}

    def fake_cmd(args):
        captured["command"] = args.command
        captured["task_ids"] = tuple(args.task_ids)
        captured["tags"] = list(args.tags or [])
        captured["all_tags"] = args.all_tags
        captured["reasons"] = list(args.reasons or [])
        captured["all"] = args.all
        captured["run"] = args.run
        captured["limit"] = args.limit
        captured["project_dir"] = args.project_dir
        return 0

    monkeypatch.setattr(cli_main_module, "cmd_unstick", fake_cmd)

    with patch.object(
        sys,
        "argv",
        [
            "gza",
            "unstick",
            "testproject-1",
            "testproject-2",
            "--tag",
            "ops",
            "--tag",
            "critical",
            "--all-tags",
            "--reason",
            "backstop",
            "--reason",
            "retry-limit",
            "--reason",
            "reconcile",
            "--all",
            "--run",
            "--limit",
            "2",
            "--project",
            str(tmp_path),
        ],
    ):
        result = cli_main_module.main()

    assert result == 0
    assert captured == {
        "command": "unstick",
        "task_ids": ("testproject-1", "testproject-2"),
        "tags": ["ops", "critical"],
        "all_tags": True,
        "reasons": ["backstop", "retry-limit", "reconcile"],
        "all": True,
        "run": True,
        "limit": 2,
        "project_dir": tmp_path.resolve(),
    }


def test_unstick_help_mentions_reason_and_all_tags(tmp_path):
    setup_config(tmp_path)

    result = invoke_gza("unstick", "--help", "--project", str(tmp_path))

    assert result.returncode == 0
    assert "--reason {backstop,retry-limit,reconcile,verify-fix-failed}" in result.stdout
    assert "--all-tags" in result.stdout
    assert "--all" in result.stdout
    assert "--run" in result.stdout
    assert "--limit N" in result.stdout


def test_unstick_run_reports_started_cleared_only_and_capacity_blocked(tmp_path, monkeypatch):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    first = store.add("Started owner", task_type="implement")
    second = store.add("Direct owner", task_type="implement")
    third = store.add("Blocked owner", task_type="implement")
    assert first.id is not None
    assert second.id is not None
    assert third.id is not None

    monkeypatch.setattr("gza.cli.unstick.Git", _UnstickGitDouble)

    outcomes = (
        UnstickOutcome(
            owner_task=first, reason_class="retry-limit", status="rearmed", detail="cleared retry-limit-reached"
        ),
        UnstickOutcome(owner_task=second, reason_class="reconcile", status="rearmed", detail="cleared reconcile"),
        UnstickOutcome(
            owner_task=third, reason_class="backstop", status="rearmed", detail="cleared watch-no-progress-backstop"
        ),
    )

    with (
        patch(
            "gza.cli.unstick.select_and_clear_parked_tasks",
            return_value=SimpleNamespace(
                selected=(object(), object(), object()), outcomes=outcomes, stale_backstop_cleared=0
            ),
        ),
        patch(
            "gza.cli.unstick._dispatch_rearmed_owners",
            return_value=SimpleNamespace(
                started_owner_ids=frozenset({str(first.id)}),
                capacity_blocked_owner_ids=frozenset({str(third.id)}),
                direct_owner_outcomes={
                    str(second.id): SimpleNamespace(action_type="reconcile_branch_divergence", status="success")
                },
                launch_blocked_owner_outcomes={},
            ),
        ),
    ):
        result = invoke_gza(
            "unstick",
            str(first.id),
            "--run",
            "--project",
            str(tmp_path),
        )

    assert result.returncode == 0
    assert (
        "Run summary: 1 started, 1 direct, 0 direct-blocked, 0 launch-blocked, 0 cleared-only, 1 capacity-blocked"
        in result.stdout
    )
    assert "Started:" in result.stdout
    assert f"{first.id} [retry-limit] Started owner" in result.stdout
    assert "Direct:" in result.stdout
    assert f"{second.id} [reconcile] reconcile_branch_divergence success: Direct owner" in result.stdout
    assert "Cleared Only:" not in result.stdout
    assert "Capacity Blocked:" in result.stdout
    assert f"{third.id} [backstop] Blocked owner" in result.stdout


def test_dispatch_rearmed_owners_treats_limit_as_new_start_cap(tmp_path):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    owner_ids = ("gza-1", "gza-2")

    with (
        patch(
            "gza.cli.unstick.get_concurrency_snapshot",
            return_value=ConcurrencySnapshot(
                limit=4,
                running=2,
                available=2,
                live_pids=frozenset({101, 202}),
                running_task_ids=("gza-900", "gza-901"),
                anonymous_worker_count=0,
                current_pid_counted=False,
            ),
        ),
        patch("gza.cli.unstick._build_watch_cycle_plan", return_value="plan-token") as build_plan,
        patch("gza.cli.unstick._dispatch_scoped_watch_once", return_value=SimpleNamespace()) as dispatch_once,
    ):
        from gza.cli.unstick import _dispatch_rearmed_owners

        _dispatch_rearmed_owners(config=config, store=store, owner_ids=owner_ids, limit=2)

    assert build_plan.call_args.kwargs["batch"] == 4
    assert build_plan.call_args.kwargs["recovery_slots"] == 2
    assert build_plan.call_args.kwargs["scoped_owner_ids"] == owner_ids
    assert dispatch_once.call_args.kwargs["batch"] == 4
    assert dispatch_once.call_args.kwargs["recovery_slots"] == 2
    assert dispatch_once.call_args.kwargs["precomputed_plan"] == "plan-token"
    assert dispatch_once.call_args.kwargs["scoped_owner_ids"] == owner_ids
    assert dispatch_once.call_args.kwargs["new_worker_start_cap"] == 2


def test_dispatch_rearmed_owners_collects_started_and_capacity_blocked(tmp_path):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    def fake_dispatch(**kwargs):
        observer = kwargs["dispatch_observer"]
        observer("gza-1", "started", "retry")
        observer("gza-2", "capacity_blocked", "create_review")
        return SimpleNamespace()

    with (
        patch("gza.cli.unstick._build_watch_cycle_plan", return_value="plan-token"),
        patch("gza.cli.unstick._dispatch_scoped_watch_once", side_effect=fake_dispatch),
    ):
        from gza.cli.unstick import _dispatch_rearmed_owners

        summary = _dispatch_rearmed_owners(config=config, store=store, owner_ids=("gza-1", "gza-2"), limit=1)

    assert summary.started_owner_ids == frozenset({"gza-1"})
    assert summary.capacity_blocked_owner_ids == frozenset({"gza-2"})
    assert summary.direct_owner_outcomes == {}
    assert summary.launch_blocked_owner_outcomes == {}


def test_dispatch_rearmed_owners_collects_launch_blocked_detail(tmp_path):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    def fake_dispatch(**kwargs):
        observer = kwargs["dispatch_observer"]
        observer("gza-1", "launch_blocked", "needs_rebase", "active rebase child already exists")
        return SimpleNamespace()

    with (
        patch("gza.cli.unstick._build_watch_cycle_plan", return_value="plan-token"),
        patch("gza.cli.unstick._dispatch_scoped_watch_once", side_effect=fake_dispatch),
    ):
        from gza.cli.unstick import _dispatch_rearmed_owners

        summary = _dispatch_rearmed_owners(config=config, store=store, owner_ids=("gza-1",), limit=1)

    assert summary.started_owner_ids == frozenset()
    assert summary.capacity_blocked_owner_ids == frozenset()
    assert summary.direct_owner_outcomes == {}
    assert summary.launch_blocked_owner_outcomes["gza-1"].action_type == "needs_rebase"
    assert summary.launch_blocked_owner_outcomes["gza-1"].detail == "active rebase child already exists"


def test_dispatch_rearmed_owners_allows_limit_starts_when_live_workers_leave_capacity(tmp_path):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    observed_slots: list[int] = []

    def fake_dispatch(**kwargs):
        observed_slots.append(kwargs["precomputed_plan"].slots)
        observer = kwargs["dispatch_observer"]
        observer("gza-1", "started", "retry")
        observer("gza-2", "started", "retry")
        return SimpleNamespace()

    with (
        patch(
            "gza.cli.unstick.get_concurrency_snapshot",
            return_value=ConcurrencySnapshot(
                limit=4,
                running=2,
                available=2,
                live_pids=frozenset({101, 202}),
                running_task_ids=("gza-900", "gza-901"),
                anonymous_worker_count=0,
                current_pid_counted=False,
            ),
        ),
        patch(
            "gza.cli.watch.get_concurrency_snapshot",
            return_value=ConcurrencySnapshot(
                limit=4,
                running=2,
                available=2,
                live_pids=frozenset({101, 202}),
                running_task_ids=("gza-900", "gza-901"),
                anonymous_worker_count=0,
                current_pid_counted=False,
            ),
        ),
        patch("gza.cli.watch._analyze_watch_cycle", return_value=SimpleNamespace()),
        patch("gza.cli.unstick._dispatch_scoped_watch_once", side_effect=fake_dispatch),
    ):
        from gza.cli.unstick import _dispatch_rearmed_owners

        summary = _dispatch_rearmed_owners(config=config, store=store, owner_ids=("gza-1", "gza-2"), limit=2)

    assert observed_slots == [2]
    assert summary.started_owner_ids == frozenset({"gza-1", "gza-2"})
    assert summary.capacity_blocked_owner_ids == frozenset()
    assert summary.launch_blocked_owner_outcomes == {}


def test_dispatch_rearmed_owners_only_starts_available_slots_then_marks_capacity_blocked(tmp_path):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    observed_slots: list[int] = []

    def fake_dispatch(**kwargs):
        observed_slots.append(kwargs["precomputed_plan"].slots)
        observer = kwargs["dispatch_observer"]
        observer("gza-1", "started", "retry")
        observer("gza-2", "capacity_blocked", "retry")
        return SimpleNamespace()

    with (
        patch(
            "gza.cli.unstick.get_concurrency_snapshot",
            return_value=ConcurrencySnapshot(
                limit=4,
                running=3,
                available=1,
                live_pids=frozenset({101, 202, 303}),
                running_task_ids=("gza-900", "gza-901", "gza-902"),
                anonymous_worker_count=0,
                current_pid_counted=False,
            ),
        ),
        patch(
            "gza.cli.watch.get_concurrency_snapshot",
            return_value=ConcurrencySnapshot(
                limit=4,
                running=3,
                available=1,
                live_pids=frozenset({101, 202, 303}),
                running_task_ids=("gza-900", "gza-901", "gza-902"),
                anonymous_worker_count=0,
                current_pid_counted=False,
            ),
        ),
        patch("gza.cli.watch._analyze_watch_cycle", return_value=SimpleNamespace()),
        patch("gza.cli.unstick._dispatch_scoped_watch_once", side_effect=fake_dispatch),
    ):
        from gza.cli.unstick import _dispatch_rearmed_owners

        summary = _dispatch_rearmed_owners(config=config, store=store, owner_ids=("gza-1", "gza-2"), limit=2)

    assert observed_slots == [1]
    assert summary.started_owner_ids == frozenset({"gza-1"})
    assert summary.capacity_blocked_owner_ids == frozenset({"gza-2"})
    assert summary.launch_blocked_owner_outcomes == {}


def test_dispatch_rearmed_owners_marks_zero_slot_worker_recovery_as_capacity_blocked(tmp_path):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    owner = store.add("Blocked retry owner", task_type="implement")
    assert owner.id is not None
    plan = SimpleNamespace(
        slots=0,
        analysis=SimpleNamespace(
            actionable_failed=(
                (
                    SimpleNamespace(owner_task=owner),
                    owner,
                    SimpleNamespace(action="retry"),
                    {"type": "retry"},
                    True,
                    None,
                ),
            )
        ),
    )

    with (
        patch(
            "gza.cli.unstick.get_concurrency_snapshot",
            return_value=ConcurrencySnapshot(
                limit=1,
                running=1,
                available=0,
                live_pids=frozenset({101}),
                running_task_ids=("gza-900",),
                anonymous_worker_count=0,
                current_pid_counted=False,
            ),
        ),
        patch("gza.cli.unstick._build_watch_cycle_plan", return_value=plan),
        patch("gza.cli.unstick._dispatch_scoped_watch_once", return_value=SimpleNamespace()),
    ):
        from gza.cli.unstick import _dispatch_rearmed_owners

        summary = _dispatch_rearmed_owners(config=config, store=store, owner_ids=(str(owner.id),), limit=1)

    assert summary.started_owner_ids == frozenset()
    assert summary.capacity_blocked_owner_ids == frozenset({str(owner.id)})


def test_dispatch_rearmed_owners_caps_lifecycle_worker_starts_after_stale_capacity_snapshot(tmp_path):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    first_owner = store.add("First owner", task_type="implement")
    second_owner = store.add("Second owner", task_type="implement")
    first_review = store.add("First review", task_type="review")
    second_review = store.add("Second review", task_type="review")
    assert first_owner.id is not None
    assert second_owner.id is not None
    assert first_review.id is not None
    assert second_review.id is not None

    first_row = SimpleNamespace(owner_task=first_owner)
    second_row = SimpleNamespace(owner_task=second_owner)
    plan = SimpleNamespace(
        running_task_ids=(),
        anonymous_worker_count=0,
        pending_count=0,
        blocked_pending_count=0,
        running=0,
        slots=4,
        analysis=SimpleNamespace(
            target_branch="main",
            scope_gaps=(),
            owner_rows=(),
            watch_read_context=RecoveryReadContext(),
            lifecycle_rows=(first_row, second_row),
            recovery_rows=(),
            recovery_lane_entry_by_failed_id={},
            action_plan=(
                (first_row, first_owner, {"type": "create_review", "description": "Create review"}),
                (second_row, second_owner, {"type": "create_review", "description": "Create review"}),
            ),
            recovery_attention_rows=(),
            recovery_visible_skips=(),
            active_recovery_subject_ids=frozenset(),
            actionable_failed=(),
            pending_recovery_task_ids=frozenset(),
        ),
    )
    review_tasks = iter((first_review, second_review))

    def fake_execute_advance_action(*_args, **_kwargs):
        review_task = next(review_tasks)
        return AdvanceActionExecutionResult(
            action_type="create_review",
            status="success",
            worker_consuming=True,
            attempted_spawn=True,
            worker_started=True,
            work_done=True,
            handled_task_id=str(review_task.id),
            created_task=review_task,
            worker_label="iterate",
        )

    with ExitStack() as stack:
        stack.enter_context(
            patch(
                "gza.cli.unstick.get_concurrency_snapshot",
                return_value=ConcurrencySnapshot(
                    limit=4,
                    running=3,
                    available=1,
                    live_pids=frozenset({101, 202, 303}),
                    running_task_ids=("gza-900", "gza-901", "gza-902"),
                    anonymous_worker_count=0,
                    current_pid_counted=False,
                ),
            )
        )
        stack.enter_context(patch("gza.cli.unstick._build_watch_cycle_plan", return_value=plan))
        stack.enter_context(patch("gza.cli.watch.Git", return_value=_UnstickGitDouble()))
        stack.enter_context(patch("gza.cli._common.reconcile_in_progress_tasks"))
        stack.enter_context(patch("gza.cli._common.prune_terminal_dead_workers"))
        stack.enter_context(patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"))
        stack.enter_context(patch("gza.cli.watch.reconcile_stale_watch_no_progress_parks"))
        stack.enter_context(patch("gza.cli.watch._warn_if_installed_gza_changed"))
        stack.enter_context(
            patch(
                "gza.cli.watch.check_canonical_checkout_invariant",
                return_value=SimpleNamespace(
                    restored=False,
                    needs_attention=False,
                    dirty_tracked_paths=[],
                    current_branch="main",
                    expected_branch="main",
                ),
            )
        )
        stack.enter_context(
            patch(
                "gza.cli.watch.check_main_integration_verify",
                return_value=SimpleNamespace(
                    merges_halted=False,
                    state=SimpleNamespace(task=SimpleNamespace(id=None), alert_message=None),
                ),
            )
        )
        stack.enter_context(patch("gza.cli.watch._maybe_file_main_verify_remediation"))
        stack.enter_context(
            patch(
                "gza.cli.watch.build_dispatch_preview",
                return_value=SimpleNamespace(runnable_entries=(), recovery_entries=()),
            )
        )
        stack.enter_context(
            patch(
                "gza.cli.watch.plan_watch_dispatch_entries",
                return_value=SimpleNamespace(recovery_worker_slots=0, pending_slots=0),
            )
        )
        execute_action = stack.enter_context(
            patch("gza.cli.watch.execute_advance_action", side_effect=fake_execute_advance_action)
        )
        stack.enter_context(patch("gza.cli.watch._snapshot_watch_dispatch_task", return_value=None))
        stack.enter_context(
            patch(
                "gza.cli.watch._settle_watch_dispatch_starts",
                side_effect=lambda *, pending_starts, **_kwargs: [
                    watch_module._WatchDispatchSettleResult(
                        entry=pending_starts[0],
                        status=watch_module._DispatchSettleStatus.LIVE,
                        reason=f"task {first_review.id} reached running state",
                        task=first_review,
                    )
                ],
            )
        )
        stack.enter_context(patch("gza.cli.watch._maybe_emit_active_watch_recovery_backoff", return_value=False))
        stack.enter_context(patch("gza.cli.watch._maybe_park_watch_no_progress", return_value=None))
        stack.enter_context(
            patch("gza.cli.watch._watch_no_progress_result_deferred_for_transient_backoff", return_value=False)
        )
        stack.enter_context(
            patch("gza.cli.watch._maybe_finalize_watch_no_progress_for_background_action", return_value=None)
        )
        stack.enter_context(patch("gza.cli.watch._finalize_watch_no_progress_after_execution", return_value=None))
        stack.enter_context(patch("gza.cli.watch._emit_cycle_attention_summary"))
        stack.enter_context(patch("gza.cli.watch._count_live_workers", return_value=0))
        stack.enter_context(patch("gza.cli.watch._scoped_watch_active_count", return_value=0))
        from gza.cli.unstick import _dispatch_rearmed_owners

        summary = _dispatch_rearmed_owners(
            config=config,
            store=store,
            owner_ids=(str(first_owner.id), str(second_owner.id)),
            limit=1,
        )

    assert execute_action.call_count == 1
    assert summary.started_owner_ids == frozenset({str(first_owner.id)})
    assert summary.capacity_blocked_owner_ids == frozenset({str(second_owner.id)})


def test_unstick_run_reports_zero_slot_retry_owner_as_capacity_blocked(tmp_path, monkeypatch):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    owner = store.add("Blocked retry owner", task_type="implement")
    assert owner.id is not None
    monkeypatch.setattr("gza.cli.unstick.Git", _UnstickGitDouble)
    plan = SimpleNamespace(
        slots=0,
        analysis=SimpleNamespace(
            actionable_failed=(
                (
                    SimpleNamespace(owner_task=owner),
                    owner,
                    SimpleNamespace(action="retry"),
                    {"type": "retry"},
                    True,
                    None,
                ),
            )
        ),
    )
    outcomes = (
        UnstickOutcome(
            owner_task=owner, reason_class="retry-limit", status="rearmed", detail="cleared retry-limit-reached"
        ),
    )

    with (
        patch(
            "gza.cli.unstick.select_and_clear_parked_tasks",
            return_value=SimpleNamespace(selected=(object(),), outcomes=outcomes, stale_backstop_cleared=0),
        ),
        patch(
            "gza.cli.unstick.get_concurrency_snapshot",
            return_value=ConcurrencySnapshot(
                limit=1,
                running=1,
                available=0,
                live_pids=frozenset({101}),
                running_task_ids=("gza-900",),
                anonymous_worker_count=0,
                current_pid_counted=False,
            ),
        ),
        patch("gza.cli.unstick._build_watch_cycle_plan", return_value=plan),
        patch("gza.cli.unstick._dispatch_scoped_watch_once", return_value=SimpleNamespace()),
    ):
        result = invoke_gza(
            "unstick",
            str(owner.id),
            "--reason",
            "retry-limit",
            "--run",
            "--project",
            str(tmp_path),
        )

    assert result.returncode == 0
    assert (
        "Run summary: 0 started, 0 direct, 0 direct-blocked, 0 launch-blocked, 0 cleared-only, 1 capacity-blocked"
        in result.stdout
    )
    assert "Capacity Blocked:" in result.stdout
    assert f"{owner.id} [retry-limit] Blocked retry owner" in result.stdout
    assert "Cleared Only:" not in result.stdout


def _invoke_unstick_run_for_lifecycle_action(
    tmp_path,
    *,
    action_type: str,
    exec_result: AdvanceActionExecutionResult | None,
):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    owner = store.add("Lifecycle recovery owner", task_type="implement")
    assert owner.id is not None
    owner.status = "completed"
    owner.completed_at = datetime.now(UTC)
    owner.branch = "feature/lifecycle-owner"
    owner.has_commits = True
    store.update(owner)
    review_task = store.add("Lifecycle review worker", task_type="review", depends_on=owner.id, based_on=owner.id)
    assert review_task.id is not None

    lifecycle_action = {
        "type": action_type,
        "description": "Run worker-oriented lifecycle action",
    }
    if action_type == "run_review":
        lifecycle_action["review_task"] = review_task
    execution_probe = None
    if exec_result is None:
        spawn_worker = Mock(return_value=1)
        context = AdvanceActionExecutionContext(
            store=store,
            trigger_source="watch",
            dry_run=False,
            max_resume_attempts=1,
            use_iterate_for_create_implement=True,
            use_iterate_for_needs_rebase=False,
            prepare_task_for_background_start=lambda task, _rollback_on_failure: task,
            prepare_create_review=lambda _task: SimpleNamespace(status="skip", review_task=None, message="unused"),
            create_resume_task=lambda task: task,
            create_rebase_task=lambda task: task,
            create_implement_task=lambda task: task,
            spawn_worker=spawn_worker,
            spawn_resume_worker=spawn_worker,
            spawn_iterate_worker=spawn_worker,
            can_spawn_worker=lambda _kind: True,
        )
        exec_result = execute_advance_action(task=owner, action=lifecycle_action, context=context)
        execution_probe = spawn_worker
    owner_row = LineageOwnerRow(
        owner_task=owner,
        members=(owner,),
        tree=None,
        lineage_status="actionable",
        next_action=lifecycle_action,
        next_action_reason="backstop",
        unresolved_tasks=(owner,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=owner,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )
    preview_entry = DispatchPreviewEntry(
        lane="pending",
        task=owner,
        owner_task=owner,
        runnable=True,
        worker_consuming=True,
        advance_action=lifecycle_action,
        lineage_row=owner_row,
    )
    plan = SimpleNamespace(
        running_task_ids=(),
        anonymous_worker_count=0,
        pending_count=1,
        blocked_pending_count=0,
        running=0,
        slots=1,
        analysis=SimpleNamespace(
            target_branch="main",
            scope_gaps=(),
            owner_rows=(owner_row,),
            watch_read_context=RecoveryReadContext(),
            lifecycle_rows=(owner_row,),
            recovery_rows=(),
            recovery_lane_entry_by_failed_id={},
            action_plan=((owner_row, owner, lifecycle_action),),
            recovery_attention_rows=(),
            recovery_visible_skips=(),
            recovery_undispatched_rows=(),
            active_recovery_subject_ids=frozenset(),
            actionable_failed=(),
            pending_recovery_task_ids=frozenset(),
            dispatch_preview=DispatchPreview(
                entries=(preview_entry,),
                owner_rows=(owner_row,),
                read_context=RecoveryReadContext(),
            ),
        ),
    )
    outcomes = (
        UnstickOutcome(
            owner_task=owner, reason_class="backstop", status="rearmed", detail="cleared watch-no-progress-backstop"
        ),
    )

    with ExitStack() as stack:
        stack.enter_context(patch("gza.git.Git.default_branch", return_value="main"))
        stack.enter_context(patch("gza.cli.unstick.Git", return_value=_UnstickGitDouble()))
        stack.enter_context(
            patch(
                "gza.cli.unstick.select_and_clear_parked_tasks",
                return_value=SimpleNamespace(selected=(object(),), outcomes=outcomes, stale_backstop_cleared=0),
            )
        )
        stack.enter_context(
            patch(
                "gza.cli.unstick.get_concurrency_snapshot",
                return_value=ConcurrencySnapshot(
                    limit=1,
                    running=0,
                    available=1,
                    live_pids=frozenset(),
                    running_task_ids=(),
                    anonymous_worker_count=0,
                    current_pid_counted=False,
                ),
            )
        )
        stack.enter_context(patch("gza.cli.unstick._build_watch_cycle_plan", return_value=plan))
        stack.enter_context(patch("gza.cli.watch.Git", return_value=_UnstickGitDouble()))
        stack.enter_context(patch("gza.cli._common.reconcile_in_progress_tasks"))
        stack.enter_context(patch("gza.cli._common.prune_terminal_dead_workers"))
        stack.enter_context(patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"))
        stack.enter_context(patch("gza.cli.watch.reconcile_stale_watch_no_progress_parks"))
        stack.enter_context(patch("gza.cli.watch._warn_if_installed_gza_changed"))
        stack.enter_context(
            patch(
                "gza.cli.watch.check_canonical_checkout_invariant",
                return_value=SimpleNamespace(
                    restored=False,
                    needs_attention=False,
                    dirty_tracked_paths=[],
                    current_branch="main",
                    expected_branch="main",
                ),
            )
        )
        stack.enter_context(
            patch(
                "gza.cli.watch.check_main_integration_verify",
                return_value=SimpleNamespace(
                    merges_halted=False,
                    state=SimpleNamespace(task=SimpleNamespace(id=None), alert_message=None),
                ),
            )
        )
        stack.enter_context(
            patch(
                "gza.cli.watch.build_dispatch_preview",
                return_value=DispatchPreview(
                    entries=(preview_entry,),
                    owner_rows=(owner_row,),
                    read_context=RecoveryReadContext(),
                ),
            )
        )
        execute_action = stack.enter_context(patch("gza.cli.watch.execute_advance_action", return_value=exec_result))
        stack.enter_context(patch("gza.cli.watch._maybe_emit_active_watch_recovery_backoff", return_value=False))
        stack.enter_context(patch("gza.cli.watch._maybe_park_watch_no_progress", return_value=None))
        stack.enter_context(
            patch(
                "gza.cli.watch._watch_no_progress_result_deferred_for_transient_backoff",
                return_value=False,
            )
        )
        stack.enter_context(
            patch("gza.cli.watch._observe_selected_watch_no_progress_without_dispatch", return_value=None)
        )
        stack.enter_context(patch("gza.cli.watch._finalize_watch_no_progress_after_execution", return_value=None))
        stack.enter_context(patch("gza.cli.watch._emit_cycle_attention_summary"))
        stack.enter_context(patch("gza.cli.watch._count_live_workers", return_value=0))
        stack.enter_context(patch("gza.cli.watch._scoped_watch_active_count", return_value=0))
        result = invoke_gza(
            "unstick",
            str(owner.id),
            "--reason",
            "backstop",
            "--run",
            "--project",
            str(tmp_path),
        )

    return result, owner, execution_probe or execute_action


@pytest.mark.parametrize(
    "action_type,worker_label",
    (
        ("create_verify_fix", "verify_fix"),
        ("run_verify_fix", "verify_fix"),
        ("create_review_adjudication", "review_adjudication"),
        ("run_review_adjudication", "review_adjudication"),
    ),
)
def test_unstick_run_reports_worker_lifecycle_launch_permit_capacity_race_as_capacity_blocked(
    tmp_path,
    action_type,
    worker_label,
):
    result, owner, execute_action = _invoke_unstick_run_for_lifecycle_action(
        tmp_path,
        action_type=action_type,
        exec_result=AdvanceActionExecutionResult(
            action_type=action_type,
            status="skip",
            execution_phase="worker_launch",
            message="SKIP: already at max concurrent tasks: 1 running, limit is 1",
            worker_consuming=False,
            worker_label=worker_label,
        ),
    )

    assert result.returncode == 0
    assert execute_action.call_count == 1
    assert (
        "Run summary: 0 started, 0 direct, 0 direct-blocked, 0 launch-blocked, 0 cleared-only, 1 capacity-blocked"
        in result.stdout
    )
    assert "Capacity Blocked:" in result.stdout
    assert f"{owner.id} [backstop]" in result.stdout
    assert "Lifecycle recovery owner" in result.stdout
    assert "Direct Blocked:" not in result.stdout
    assert "Cleared Only:" not in result.stdout
    assert (
        "already at max concurrent tasks: 1 running, limit is 1" in (tmp_path / ".gza" / "unstick-run.log").read_text()
    )


@pytest.mark.parametrize(
    "action_type,worker_label",
    (
        ("create_verify_fix", "verify_fix"),
        ("run_verify_fix", "verify_fix"),
        ("create_review_adjudication", "review_adjudication"),
        ("run_review_adjudication", "review_adjudication"),
    ),
)
def test_unstick_run_does_not_report_worker_lifecycle_startup_error_as_direct_error(
    tmp_path,
    action_type,
    worker_label,
):
    result, owner, execute_action = _invoke_unstick_run_for_lifecycle_action(
        tmp_path,
        action_type=action_type,
        exec_result=AdvanceActionExecutionResult(
            action_type=action_type,
            status="error",
            execution_phase="worker_launch",
            message="startup preparation failed for task gza-999",
            error_message=f"Failed to start {worker_label} worker for task gza-999",
            worker_consuming=False,
            worker_label=worker_label,
        ),
    )

    assert result.returncode == 0
    assert execute_action.call_count == 1
    assert (
        "Run summary: 0 started, 0 direct, 0 direct-blocked, 1 launch-blocked, 0 cleared-only, 0 capacity-blocked"
        in result.stdout
    )
    assert "Launch Blocked:" in result.stdout
    assert "Direct Blocked:" not in result.stdout
    assert "Cleared Only:" not in result.stdout
    assert "Capacity Blocked:" not in result.stdout
    assert f"{owner.id} [backstop]" in result.stdout
    assert "Lifecycle recovery owner" in result.stdout
    assert "startup preparation failed for task gza-999" in result.stdout


def test_unstick_run_reports_real_lifecycle_spawn_failure_diagnostic_as_launch_blocked(tmp_path):
    result, owner, spawn_worker = _invoke_unstick_run_for_lifecycle_action(
        tmp_path,
        action_type="run_review",
        exec_result=None,
    )

    assert result.returncode == 0
    assert spawn_worker.call_count == 1
    assert (
        "Run summary: 0 started, 0 direct, 0 direct-blocked, 1 launch-blocked, 0 cleared-only, 0 capacity-blocked"
        in result.stdout
    )
    assert "Launch Blocked:" in result.stdout
    assert "Direct Blocked:" not in result.stdout
    assert "Cleared Only:" not in result.stdout
    assert "Capacity Blocked:" not in result.stdout
    assert f"{owner.id} [backstop] run_review: Lifecycle recovery owner" in result.stdout
    spawned_task = spawn_worker.call_args.args[0]
    assert f"Failed to start review worker for task {spawned_task.id}" in result.stdout


def test_unstick_run_does_not_report_worker_duplicate_child_skip_as_direct_blocked(tmp_path):
    result, owner, execute_action = _invoke_unstick_run_for_lifecycle_action(
        tmp_path,
        action_type="needs_rebase",
        exec_result=AdvanceActionExecutionResult(
            action_type="needs_rebase",
            status="skip",
            message="SKIP: active rebase child already exists for this owner",
            worker_consuming=False,
            worker_label="rebase",
        ),
    )

    assert result.returncode == 0
    assert execute_action.call_count == 1
    assert (
        "Run summary: 0 started, 0 direct, 0 direct-blocked, 1 launch-blocked, 0 cleared-only, 0 capacity-blocked"
        in result.stdout
    )
    assert "Launch Blocked:" in result.stdout
    assert "Cleared Only:" not in result.stdout
    assert f"{owner.id} [backstop]" in result.stdout
    assert "Lifecycle recovery owner" in result.stdout
    assert "Direct Blocked:" not in result.stdout
    assert "Capacity Blocked:" not in result.stdout
    assert "active rebase child already exists for this owner" in result.stdout


def _invoke_unstick_run_for_inline_recovery_launch(
    tmp_path,
    *,
    action_type: str,
    launch_mode: str,
    failure_kind: str,
):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    owner = store.add("Inline recovery owner", task_type="implement")
    failed = store.add("Inline recovery failed leaf", task_type="implement", depends_on=owner.id)
    assert owner.id is not None
    assert failed.id is not None
    owner.status = "completed"
    owner.completed_at = datetime.now(UTC)
    owner.branch = "feature/inline-recovery-owner"
    owner.has_commits = True
    store.update(owner)
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-inline-recovery"
    failed.completed_at = datetime.now(UTC)
    failed.branch = owner.branch
    store.update(failed)

    child = store.add(f"Prepared {action_type} child", task_type="implement", based_on=failed.id, depends_on=owner.id)
    assert child.id is not None
    active_child = store.add(
        f"Duplicate {action_type} child", task_type="implement", based_on=failed.id, depends_on=owner.id
    )
    assert active_child.id is not None

    recovery_action = {"type": action_type, "description": f"{action_type.title()} failed task"}
    decision = FailedRecoveryDecision(
        task_id=str(failed.id),
        action=action_type,
        reason_code="retryable_failure",
        reason_text=f"{action_type.title()} failed task",
        launch_mode=launch_mode,
        attempt_index=1,
        attempt_limit=2,
        recovery_task_id=None,
        reuse_existing=False,
    )
    owner_row = LineageOwnerRow(
        owner_task=owner,
        members=(owner, failed),
        tree=None,
        lineage_status="failed",
        next_action={"type": "skip", "description": "failed recovery"},
        next_action_reason="recovery",
        unresolved_tasks=(failed,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=owner,
        recovery_action_task=failed,
        recovery_leaf_task=failed,
    )
    preview_entry = DispatchPreviewEntry(
        lane="recovery",
        task=failed,
        owner_task=owner,
        runnable=True,
        worker_consuming=True,
        decision=decision,
        advance_action=recovery_action,
        lineage_row=owner_row,
    )
    plan = SimpleNamespace(
        running_task_ids=(),
        anonymous_worker_count=0,
        pending_count=0,
        blocked_pending_count=0,
        running=0,
        slots=1,
        analysis=SimpleNamespace(
            target_branch="main",
            scope_gaps=(),
            owner_rows=(owner_row,),
            watch_read_context=RecoveryReadContext(),
            lifecycle_rows=(),
            recovery_rows=(owner_row,),
            recovery_lane_entry_by_failed_id={str(failed.id): preview_entry},
            action_plan=(),
            recovery_attention_rows=(),
            recovery_visible_skips=(),
            recovery_undispatched_rows=(),
            active_recovery_subject_ids=frozenset(),
            actionable_failed=((owner_row, failed, decision, recovery_action, True, failed),),
            pending_recovery_task_ids=frozenset(),
        ),
    )
    outcomes = (
        UnstickOutcome(
            owner_task=owner, reason_class="retry-limit", status="rearmed", detail="cleared retry-limit-reached"
        ),
    )

    create_target = f"gza.cli.watch._create_{action_type}_task"
    spawn_target = (
        "gza.cli.watch._spawn_background_resume_worker"
        if action_type == "resume" and launch_mode == "worker"
        else "gza.cli.watch._spawn_background_worker"
        if action_type == "retry" and launch_mode == "worker"
        else "gza.cli.watch._spawn_background_iterate"
    )
    create_side_effect = DuplicateActiveChildError(active_child) if failure_kind == "duplicate" else child
    spawn_return = 1 if failure_kind == "spawn" else 0

    with ExitStack() as stack:
        stack.enter_context(patch("gza.cli.unstick.Git", return_value=_UnstickGitDouble()))
        stack.enter_context(
            patch(
                "gza.cli.unstick.select_and_clear_parked_tasks",
                return_value=SimpleNamespace(selected=(object(),), outcomes=outcomes, stale_backstop_cleared=0),
            )
        )
        stack.enter_context(
            patch(
                "gza.cli.unstick.get_concurrency_snapshot",
                return_value=ConcurrencySnapshot(
                    limit=1,
                    running=0,
                    available=1,
                    live_pids=frozenset(),
                    running_task_ids=(),
                    anonymous_worker_count=0,
                    current_pid_counted=False,
                ),
            )
        )
        stack.enter_context(patch("gza.cli.unstick._build_watch_cycle_plan", return_value=plan))
        stack.enter_context(patch("gza.cli.watch.Git", return_value=_UnstickGitDouble()))
        stack.enter_context(patch("gza.cli._common.reconcile_in_progress_tasks"))
        stack.enter_context(patch("gza.cli._common.prune_terminal_dead_workers"))
        stack.enter_context(patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"))
        stack.enter_context(patch("gza.cli.watch.reconcile_stale_watch_no_progress_parks"))
        stack.enter_context(patch("gza.cli.watch._warn_if_installed_gza_changed"))
        stack.enter_context(
            patch(
                "gza.cli.watch.check_canonical_checkout_invariant",
                return_value=SimpleNamespace(
                    restored=False,
                    needs_attention=False,
                    dirty_tracked_paths=[],
                    current_branch="main",
                    expected_branch="main",
                ),
            )
        )
        stack.enter_context(
            patch(
                "gza.cli.watch.check_main_integration_verify",
                return_value=SimpleNamespace(
                    merges_halted=False,
                    state=SimpleNamespace(task=SimpleNamespace(id=None), alert_message=None),
                ),
            )
        )
        stack.enter_context(
            patch(
                "gza.cli.watch.build_dispatch_preview",
                return_value=DispatchPreview(
                    entries=(preview_entry,),
                    owner_rows=(owner_row,),
                    read_context=RecoveryReadContext(),
                ),
            )
        )
        create_task = stack.enter_context(
            patch(create_target, side_effect=create_side_effect if failure_kind == "duplicate" else None)
        )
        if failure_kind != "duplicate":
            create_task.return_value = child
        if failure_kind == "preparation":
            stack.enter_context(patch("gza.cli.watch._prepare_task_for_immediate_execution", return_value=None))
        else:
            stack.enter_context(
                patch(
                    "gza.cli.watch._prepare_task_for_immediate_execution",
                    side_effect=lambda _config, task, **_kwargs: task,
                )
            )
        spawn_worker = stack.enter_context(patch(spawn_target, return_value=spawn_return))
        for other_spawn_target in {
            "gza.cli.watch._spawn_background_resume_worker",
            "gza.cli.watch._spawn_background_worker",
            "gza.cli.watch._spawn_background_iterate",
        } - {spawn_target}:
            stack.enter_context(patch(other_spawn_target, side_effect=AssertionError("unexpected worker launch")))
        stack.enter_context(patch("gza.cli.watch._maybe_emit_active_watch_recovery_backoff", return_value=False))
        stack.enter_context(patch("gza.cli.watch._maybe_park_watch_no_progress", return_value=None))
        stack.enter_context(
            patch("gza.cli.watch._watch_no_progress_result_deferred_for_transient_backoff", return_value=False)
        )
        stack.enter_context(
            patch("gza.cli.watch._observe_selected_watch_no_progress_without_dispatch", return_value=None)
        )
        stack.enter_context(patch("gza.cli.watch._emit_cycle_attention_summary"))
        stack.enter_context(patch("gza.cli.watch._count_live_workers", return_value=0))
        stack.enter_context(patch("gza.cli.watch._scoped_watch_active_count", return_value=0))
        result = invoke_gza(
            "unstick",
            str(owner.id),
            "--reason",
            "retry-limit",
            "--run",
            "--project",
            str(tmp_path),
        )

    return result, owner, failed, child, active_child, create_task, spawn_worker


@pytest.mark.parametrize("action_type", ("resume", "retry"))
@pytest.mark.parametrize("launch_mode", ("worker", "iterate"))
def test_unstick_run_reports_inline_recovery_duplicate_child_as_launch_blocked(tmp_path, action_type, launch_mode):
    result, owner, failed, _child, active_child, create_task, spawn_worker = (
        _invoke_unstick_run_for_inline_recovery_launch(
            tmp_path,
            action_type=action_type,
            launch_mode=launch_mode,
            failure_kind="duplicate",
        )
    )

    assert result.returncode == 0
    assert create_task.call_count == 1
    assert spawn_worker.call_count == 0
    assert (
        "Run summary: 0 started, 0 direct, 0 direct-blocked, 1 launch-blocked, 0 cleared-only, 0 capacity-blocked"
        in result.stdout
    )
    assert "Launch Blocked:" in result.stdout
    assert "Direct Blocked:" not in result.stdout
    assert "Cleared Only:" not in result.stdout
    assert "Capacity Blocked:" not in result.stdout
    assert f"{owner.id} [retry-limit] {action_type}: Inline recovery owner" in result.stdout
    assert f"implement already pending/in progress for {failed.id}: {active_child.id}" in result.stdout


@pytest.mark.parametrize("action_type", ("resume", "retry"))
@pytest.mark.parametrize("launch_mode", ("worker", "iterate"))
def test_unstick_run_reports_inline_recovery_preparation_failure_as_launch_blocked(tmp_path, action_type, launch_mode):
    result, owner, _failed, child, _active_child, create_task, spawn_worker = (
        _invoke_unstick_run_for_inline_recovery_launch(
            tmp_path,
            action_type=action_type,
            launch_mode=launch_mode,
            failure_kind="preparation",
        )
    )

    assert result.returncode == 0
    assert create_task.call_count == 1
    assert spawn_worker.call_count == 0
    assert (
        "Run summary: 0 started, 0 direct, 0 direct-blocked, 1 launch-blocked, 0 cleared-only, 0 capacity-blocked"
        in result.stdout
    )
    assert "Launch Blocked:" in result.stdout
    assert "Direct Blocked:" not in result.stdout
    assert "Cleared Only:" not in result.stdout
    assert "Capacity Blocked:" not in result.stdout
    assert f"{owner.id} [retry-limit] {action_type}: Inline recovery owner" in result.stdout
    assert f"failed to prepare {action_type} task {child.id}" in result.stdout


@pytest.mark.parametrize("action_type", ("resume", "retry"))
@pytest.mark.parametrize("launch_mode", ("worker", "iterate"))
def test_unstick_run_reports_inline_recovery_spawn_failure_as_launch_blocked(tmp_path, action_type, launch_mode):
    result, owner, failed, child, _active_child, create_task, spawn_worker = (
        _invoke_unstick_run_for_inline_recovery_launch(
            tmp_path,
            action_type=action_type,
            launch_mode=launch_mode,
            failure_kind="spawn",
        )
    )

    assert result.returncode == 0
    assert create_task.call_count == 1
    assert spawn_worker.call_count == 1
    assert (
        "Run summary: 0 started, 0 direct, 0 direct-blocked, 1 launch-blocked, 0 cleared-only, 0 capacity-blocked"
        in result.stdout
    )
    assert "Launch Blocked:" in result.stdout
    assert "Direct Blocked:" not in result.stdout
    assert "Cleared Only:" not in result.stdout
    assert "Capacity Blocked:" not in result.stdout
    assert f"{owner.id} [retry-limit] {action_type}: Inline recovery owner" in result.stdout
    expected_detail = (
        f"{failed.id} -> {child.id}: resume worker spawn failed"
        if action_type == "resume" and launch_mode == "worker"
        else f"{failed.id} -> {child.id}: worker spawn failed"
        if action_type == "retry" and launch_mode == "worker"
        else f"{failed.id} -> {child.id}: iterate worker spawn failed"
    )
    assert expected_detail in result.stdout


def _invoke_unstick_run_for_reconcile_recovery(
    tmp_path,
    *,
    exec_result: AdvanceActionExecutionResult | None,
    settle_status=None,
):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    owner = store.add("Reconcile recovery owner", task_type="implement")
    failed = store.add("Failed publication recovery", task_type="implement")
    assert owner.id is not None
    assert failed.id is not None
    owner.status = "completed"
    owner.completed_at = datetime.now(UTC)
    owner.branch = "feature/reconcile-owner"
    owner.has_commits = True
    store.update(owner)
    failed.status = "failed"
    failed.branch = owner.branch
    failed.depends_on = owner.id
    store.update(failed)
    if exec_result is None or (
        exec_result.status == "success"
        and exec_result.execution_phase == "worker_launch"
        and exec_result.handled_task_id is None
    ):
        rebase_child = store.add("Spawned reconcile rebase", task_type="rebase", based_on=failed.id)
        assert rebase_child.id is not None
        rebase_child.status = "pending"
        store.update(rebase_child)
        if exec_result is not None:
            exec_result.handled_task_id = rebase_child.id
            exec_result.created_task = rebase_child
    owner_row = LineageOwnerRow(
        owner_task=owner,
        members=(owner, failed),
        tree=None,
        lineage_status="failed",
        next_action={"type": "skip", "description": "failed recovery"},
        next_action_reason="recovery",
        unresolved_tasks=(failed,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=owner,
        recovery_action_task=failed,
        recovery_leaf_task=failed,
    )
    recovery_action = {
        "type": "reconcile_branch_divergence",
        "description": "Reconcile branch publication",
    }
    execution_probe = None
    if exec_result is None:
        spawn_worker = Mock(return_value=1)
        context = AdvanceActionExecutionContext(
            store=store,
            trigger_source="watch",
            dry_run=False,
            max_resume_attempts=1,
            use_iterate_for_create_implement=True,
            use_iterate_for_needs_rebase=False,
            prepare_task_for_background_start=lambda task, _rollback_on_failure: task,
            prepare_create_review=lambda _task: SimpleNamespace(status="skip", review_task=None, message="unused"),
            create_resume_task=lambda task: task,
            create_rebase_task=lambda task: task,
            create_implement_task=lambda task: task,
            spawn_worker=spawn_worker,
            spawn_resume_worker=spawn_worker,
            spawn_iterate_worker=spawn_worker,
            can_spawn_worker=lambda _kind: True,
            create_targeted_rebase_task=lambda _task, _target: rebase_child,
            reconcile_diverged_branch=lambda _task: BranchDivergenceReconcileResult(
                status="needs_rebase",
                message="branch reconciliation needs rebase",
                rebase_target="origin/main",
            ),
        )
        exec_result = execute_advance_action(task=failed, action=recovery_action, context=context)
        execution_probe = spawn_worker
    decision = FailedRecoveryDecision(
        task_id=str(failed.id),
        action="reconcile",
        reason_code="branch_unpushable",
        reason_text="branch publication needs reconciliation",
        launch_mode="none",
        attempt_index=1,
        attempt_limit=1,
    )
    preview_entry = DispatchPreviewEntry(
        lane="recovery",
        task=failed,
        owner_task=owner,
        runnable=True,
        worker_consuming=False,
        decision=decision,
        advance_action=recovery_action,
        lineage_row=owner_row,
    )
    plan = SimpleNamespace(
        running_task_ids=(),
        anonymous_worker_count=0,
        pending_count=0,
        blocked_pending_count=0,
        running=0,
        slots=1,
        analysis=SimpleNamespace(
            target_branch="main",
            scope_gaps=(),
            owner_rows=(owner_row,),
            watch_read_context=RecoveryReadContext(),
            lifecycle_rows=(),
            recovery_rows=(),
            recovery_lane_entry_by_failed_id={str(failed.id): preview_entry},
            action_plan=(),
            recovery_attention_rows=(),
            recovery_visible_skips=(),
            recovery_undispatched_rows=(),
            active_recovery_subject_ids=frozenset(),
            actionable_failed=((owner_row, failed, decision, recovery_action, False, failed),),
            pending_recovery_task_ids=frozenset(),
        ),
    )
    outcomes = (
        UnstickOutcome(owner_task=owner, reason_class="reconcile", status="rearmed", detail="cleared reconcile"),
    )

    with ExitStack() as stack:
        stack.enter_context(patch("gza.git.Git.default_branch", return_value="main"))
        stack.enter_context(patch("gza.cli.unstick.Git", return_value=_UnstickGitDouble()))
        stack.enter_context(
            patch(
                "gza.cli.unstick.select_and_clear_parked_tasks",
                return_value=SimpleNamespace(selected=(object(),), outcomes=outcomes, stale_backstop_cleared=0),
            )
        )
        stack.enter_context(
            patch(
                "gza.cli.unstick.get_concurrency_snapshot",
                return_value=ConcurrencySnapshot(
                    limit=1,
                    running=0,
                    available=1,
                    live_pids=frozenset(),
                    running_task_ids=(),
                    anonymous_worker_count=0,
                    current_pid_counted=False,
                ),
            )
        )
        stack.enter_context(patch("gza.cli.unstick._build_watch_cycle_plan", return_value=plan))
        stack.enter_context(patch("gza.cli.watch.Git", return_value=_UnstickGitDouble()))
        stack.enter_context(patch("gza.cli._common.reconcile_in_progress_tasks"))
        stack.enter_context(patch("gza.cli._common.prune_terminal_dead_workers"))
        stack.enter_context(patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"))
        stack.enter_context(patch("gza.cli.watch.reconcile_stale_watch_no_progress_parks"))
        stack.enter_context(patch("gza.cli.watch._warn_if_installed_gza_changed"))
        stack.enter_context(
            patch(
                "gza.cli.watch.check_canonical_checkout_invariant",
                return_value=SimpleNamespace(
                    restored=False,
                    needs_attention=False,
                    dirty_tracked_paths=[],
                    current_branch="main",
                    expected_branch="main",
                ),
            )
        )
        stack.enter_context(
            patch(
                "gza.cli.watch.check_main_integration_verify",
                return_value=SimpleNamespace(
                    merges_halted=False,
                    state=SimpleNamespace(task=SimpleNamespace(id=None), alert_message=None),
                ),
            )
        )
        stack.enter_context(patch("gza.cli.watch._maybe_file_main_verify_remediation"))
        stack.enter_context(
            patch(
                "gza.cli.watch.build_dispatch_preview",
                return_value=DispatchPreview(
                    entries=(preview_entry,),
                    owner_rows=(owner_row,),
                    read_context=RecoveryReadContext(),
                ),
            )
        )
        execute_action = stack.enter_context(patch("gza.cli.watch.execute_advance_action", return_value=exec_result))
        if exec_result is not None and settle_status is not None:
            stack.enter_context(
                patch(
                    "gza.cli.watch._settle_watch_dispatch_starts",
                    side_effect=lambda *, pending_starts, **_kwargs: [
                        watch_module._WatchDispatchSettleResult(
                            entry=entry,
                            status=settle_status,
                            reason=(
                                f"task {entry.task_id} reached running state"
                                if settle_status is watch_module._DispatchSettleStatus.LIVE
                                else f"task {entry.task_id} remains pending with no live worker"
                            ),
                            task=store.get(entry.task_id),
                        )
                        for entry in pending_starts
                    ],
                )
            )
        stack.enter_context(patch("gza.cli.watch._maybe_emit_active_watch_recovery_backoff", return_value=False))
        stack.enter_context(patch("gza.cli.watch._maybe_park_watch_no_progress", return_value=None))
        stack.enter_context(
            patch("gza.cli.watch._watch_no_progress_result_deferred_for_transient_backoff", return_value=False)
        )
        stack.enter_context(
            patch("gza.cli.watch._observe_selected_watch_no_progress_without_dispatch", return_value=None)
        )
        stack.enter_context(patch("gza.cli.watch._finalize_watch_no_progress_after_execution", return_value=None))
        stack.enter_context(patch("gza.cli.watch._emit_cycle_attention_summary"))
        stack.enter_context(patch("gza.cli.watch._count_live_workers", return_value=0))
        stack.enter_context(patch("gza.cli.watch._scoped_watch_active_count", return_value=0))
        result = invoke_gza(
            "unstick",
            str(owner.id),
            "--reason",
            "reconcile",
            "--run",
            "--project",
            str(tmp_path),
        )

    return result, owner, execution_probe or execute_action


def _invoke_unstick_run_for_needs_rebase_recovery(tmp_path):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    owner = store.add("Needs rebase recovery owner", task_type="implement")
    failed = store.add("Needs rebase failed leaf", task_type="implement", depends_on=owner.id)
    assert owner.id is not None
    assert failed.id is not None
    owner.status = "completed"
    owner.completed_at = datetime.now(UTC)
    owner.branch = "feature/needs-rebase-owner"
    owner.has_commits = True
    store.update(owner)
    failed.status = "failed"
    failed.branch = owner.branch
    store.update(failed)
    rebase_child = store.add("Needs rebase child", task_type="rebase", based_on=failed.id, branch=failed.branch)
    assert rebase_child.id is not None

    owner_row = LineageOwnerRow(
        owner_task=owner,
        members=(owner, failed),
        tree=None,
        lineage_status="failed",
        next_action={"type": "skip", "description": "failed recovery"},
        next_action_reason="recovery",
        unresolved_tasks=(failed,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=owner,
        recovery_action_task=failed,
        recovery_leaf_task=failed,
    )
    recovery_action = {
        "type": "needs_rebase",
        "description": "Rebase failed task",
        "reason": "recovery-preflight-rebase",
    }
    spawn_worker = Mock(return_value=1)
    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="watch",
        dry_run=False,
        max_resume_attempts=1,
        use_iterate_for_create_implement=True,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback_on_failure: task,
        prepare_create_review=lambda _task: SimpleNamespace(status="skip", review_task=None, message="unused"),
        create_resume_task=lambda task: task,
        create_rebase_task=lambda _task: rebase_child,
        create_implement_task=lambda task: task,
        spawn_worker=spawn_worker,
        spawn_resume_worker=spawn_worker,
        spawn_iterate_worker=spawn_worker,
        can_spawn_worker=lambda _kind: True,
    )
    exec_result = execute_advance_action(task=failed, action=recovery_action, context=context)
    decision = FailedRecoveryDecision(
        task_id=str(failed.id),
        action="needs_rebase",
        reason_code="branch_needs_rebase",
        reason_text="failed task needs rebase",
        launch_mode="worker",
        attempt_index=1,
        attempt_limit=1,
    )
    preview_entry = DispatchPreviewEntry(
        lane="recovery",
        task=failed,
        owner_task=owner,
        runnable=True,
        worker_consuming=True,
        decision=decision,
        advance_action=recovery_action,
        lineage_row=owner_row,
    )
    plan = SimpleNamespace(
        running_task_ids=(),
        anonymous_worker_count=0,
        pending_count=0,
        blocked_pending_count=0,
        running=0,
        slots=1,
        analysis=SimpleNamespace(
            target_branch="main",
            scope_gaps=(),
            owner_rows=(owner_row,),
            watch_read_context=RecoveryReadContext(),
            lifecycle_rows=(),
            recovery_rows=(),
            recovery_lane_entry_by_failed_id={str(failed.id): preview_entry},
            action_plan=(),
            recovery_attention_rows=(),
            recovery_visible_skips=(),
            recovery_undispatched_rows=(),
            active_recovery_subject_ids=frozenset(),
            actionable_failed=((owner_row, failed, decision, recovery_action, False, failed),),
            pending_recovery_task_ids=frozenset(),
        ),
    )
    outcomes = (
        UnstickOutcome(owner_task=owner, reason_class="retry-limit", status="rearmed", detail="cleared retry-limit"),
    )

    with ExitStack() as stack:
        stack.enter_context(patch("gza.git.Git.default_branch", return_value="main"))
        stack.enter_context(patch("gza.cli.unstick.Git", return_value=_UnstickGitDouble()))
        stack.enter_context(
            patch(
                "gza.cli.unstick.select_and_clear_parked_tasks",
                return_value=SimpleNamespace(selected=(object(),), outcomes=outcomes, stale_backstop_cleared=0),
            )
        )
        stack.enter_context(
            patch(
                "gza.cli.unstick.get_concurrency_snapshot",
                return_value=ConcurrencySnapshot(
                    limit=1,
                    running=0,
                    available=1,
                    live_pids=frozenset(),
                    running_task_ids=(),
                    anonymous_worker_count=0,
                    current_pid_counted=False,
                ),
            )
        )
        stack.enter_context(patch("gza.cli.unstick._build_watch_cycle_plan", return_value=plan))
        stack.enter_context(patch("gza.cli.watch.Git", return_value=_UnstickGitDouble()))
        stack.enter_context(patch("gza.cli._common.reconcile_in_progress_tasks"))
        stack.enter_context(patch("gza.cli._common.prune_terminal_dead_workers"))
        stack.enter_context(patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"))
        stack.enter_context(patch("gza.cli.watch.reconcile_stale_watch_no_progress_parks"))
        stack.enter_context(patch("gza.cli.watch._warn_if_installed_gza_changed"))
        stack.enter_context(
            patch(
                "gza.cli.watch.check_canonical_checkout_invariant",
                return_value=SimpleNamespace(
                    restored=False,
                    needs_attention=False,
                    dirty_tracked_paths=[],
                    current_branch="main",
                    expected_branch="main",
                ),
            )
        )
        stack.enter_context(
            patch(
                "gza.cli.watch.check_main_integration_verify",
                return_value=SimpleNamespace(
                    merges_halted=False,
                    state=SimpleNamespace(task=SimpleNamespace(id=None), alert_message=None),
                ),
            )
        )
        stack.enter_context(
            patch(
                "gza.cli.watch.build_dispatch_preview",
                return_value=DispatchPreview(
                    entries=(preview_entry,),
                    owner_rows=(owner_row,),
                    read_context=RecoveryReadContext(),
                ),
            )
        )
        stack.enter_context(patch("gza.cli.watch.execute_advance_action", return_value=exec_result))
        stack.enter_context(patch("gza.cli.watch._maybe_emit_active_watch_recovery_backoff", return_value=False))
        stack.enter_context(patch("gza.cli.watch._maybe_park_watch_no_progress", return_value=None))
        stack.enter_context(
            patch("gza.cli.watch._watch_no_progress_result_deferred_for_transient_backoff", return_value=False)
        )
        stack.enter_context(
            patch("gza.cli.watch._observe_selected_watch_no_progress_without_dispatch", return_value=None)
        )
        stack.enter_context(patch("gza.cli.watch._emit_cycle_attention_summary"))
        stack.enter_context(patch("gza.cli.watch._count_live_workers", return_value=0))
        stack.enter_context(patch("gza.cli.watch._scoped_watch_active_count", return_value=0))
        result = invoke_gza(
            "unstick",
            str(owner.id),
            "--reason",
            "retry-limit",
            "--run",
            "--project",
            str(tmp_path),
        )

    return result, owner, spawn_worker


def test_unstick_run_reports_confirmed_reconcile_rebase_launch_as_started(tmp_path):
    result, owner, execute_action = _invoke_unstick_run_for_reconcile_recovery(
        tmp_path,
        exec_result=AdvanceActionExecutionResult(
            action_type="reconcile_branch_divergence",
            status="success",
            execution_phase="worker_launch",
            message="Created rebase task",
            success_message="Created rebase task",
            worker_consuming=True,
            attempted_spawn=True,
            worker_started=True,
            work_done=True,
            worker_label="rebase",
        ),
        settle_status=watch_module._DispatchSettleStatus.LIVE,
    )

    assert result.returncode == 0
    assert execute_action.call_count == 1
    assert (
        "Run summary: 1 started, 0 direct, 0 direct-blocked, 0 launch-blocked, 0 cleared-only, 0 capacity-blocked"
        in result.stdout
    )
    assert "Started:" in result.stdout
    assert f"{owner.id} [reconcile]" in result.stdout
    assert "Reconcile recovery owner" in result.stdout
    assert "Direct:" not in result.stdout
    assert "Direct Blocked:" not in result.stdout


def test_unstick_run_does_not_report_undispatched_reconcile_rebase_launch_as_direct_or_started(tmp_path):
    result, owner, execute_action = _invoke_unstick_run_for_reconcile_recovery(
        tmp_path,
        exec_result=AdvanceActionExecutionResult(
            action_type="reconcile_branch_divergence",
            status="success",
            execution_phase="worker_launch",
            message="Created rebase task",
            success_message="Created rebase task",
            worker_consuming=True,
            attempted_spawn=True,
            worker_started=True,
            work_done=True,
            worker_label="rebase",
        ),
        settle_status=watch_module._DispatchSettleStatus.NO_LIVE_PROOF,
    )

    assert result.returncode == 0
    assert execute_action.call_count == 1
    assert (
        "Run summary: 0 started, 0 direct, 0 direct-blocked, 1 launch-blocked, 0 cleared-only, 0 capacity-blocked"
        in result.stdout
    )
    assert "Started:" not in result.stdout
    assert "Direct:" not in result.stdout
    assert "Direct Blocked:" not in result.stdout
    assert "Launch Blocked:" in result.stdout
    assert "Cleared Only:" not in result.stdout
    assert f"{owner.id} [reconcile]" in result.stdout
    assert "Reconcile recovery owner" in result.stdout
    assert "dispatch did not reach live slot occupancy" in result.stdout


def test_unstick_run_reports_terminal_before_running_reconcile_rebase_launch_as_launch_blocked(tmp_path):
    result, owner, execute_action = _invoke_unstick_run_for_reconcile_recovery(
        tmp_path,
        exec_result=AdvanceActionExecutionResult(
            action_type="reconcile_branch_divergence",
            status="success",
            execution_phase="worker_launch",
            message="Created rebase task",
            success_message="Created rebase task",
            worker_consuming=True,
            attempted_spawn=True,
            worker_started=True,
            work_done=True,
            worker_label="rebase",
        ),
        settle_status=watch_module._DispatchSettleStatus.TERMINAL_BEFORE_RUNNING,
    )

    assert result.returncode == 0
    assert execute_action.call_count == 1
    assert (
        "Run summary: 0 started, 0 direct, 0 direct-blocked, 1 launch-blocked, 0 cleared-only, 0 capacity-blocked"
        in result.stdout
    )
    assert "Started:" not in result.stdout
    assert "Direct:" not in result.stdout
    assert "Direct Blocked:" not in result.stdout
    assert "Launch Blocked:" in result.stdout
    assert "Cleared Only:" not in result.stdout
    assert f"{owner.id} [reconcile]" in result.stdout
    assert "Reconcile recovery owner" in result.stdout
    assert "dispatch exited before occupying a live slot" in result.stdout


def test_unstick_run_reports_reconcile_direct_skip_as_direct_blocked(tmp_path):
    result, owner, execute_action = _invoke_unstick_run_for_reconcile_recovery(
        tmp_path,
        exec_result=AdvanceActionExecutionResult(
            action_type="reconcile_branch_divergence",
            status="skip",
            message="SKIP: reconcile needs manual resolution",
            worker_consuming=False,
        ),
    )

    assert result.returncode == 0
    assert execute_action.call_count == 1
    assert (
        "Run summary: 0 started, 0 direct, 1 direct-blocked, 0 launch-blocked, 0 cleared-only, 0 capacity-blocked"
        in result.stdout
    )
    assert "Direct Blocked:" in result.stdout
    assert f"{owner.id} [reconcile] reconcile_branch_divergence blocked: Reconcile recovery owner" in result.stdout
    assert "Cleared Only:" not in result.stdout


def test_unstick_run_reports_reconcile_rebase_capacity_race_as_capacity_blocked(tmp_path):
    result, owner, execute_action = _invoke_unstick_run_for_reconcile_recovery(
        tmp_path,
        exec_result=AdvanceActionExecutionResult(
            action_type="reconcile_branch_divergence",
            status="skip",
            execution_phase="worker_launch",
            message="SKIP: already at max concurrent tasks: 1 running, limit is 1",
            worker_consuming=False,
            worker_label="rebase",
        ),
    )

    assert result.returncode == 0
    assert execute_action.call_count == 1
    assert (
        "Run summary: 0 started, 0 direct, 0 direct-blocked, 0 launch-blocked, 0 cleared-only, 1 capacity-blocked"
        in result.stdout
    )
    assert "Capacity Blocked:" in result.stdout
    assert f"{owner.id} [reconcile]" in result.stdout
    assert "Reconcile recovery owner" in result.stdout
    assert "Direct Blocked:" not in result.stdout
    assert (
        "already at max concurrent tasks: 1 running, limit is 1" in (tmp_path / ".gza" / "unstick-run.log").read_text()
    )


def test_unstick_run_does_not_report_reconcile_rebase_duplicate_child_as_direct_blocked(tmp_path):
    result, owner, execute_action = _invoke_unstick_run_for_reconcile_recovery(
        tmp_path,
        exec_result=AdvanceActionExecutionResult(
            action_type="reconcile_branch_divergence",
            status="skip",
            execution_phase="worker_launch",
            message="SKIP: active rebase child already exists for this owner",
            worker_consuming=False,
            worker_label="rebase",
        ),
    )

    assert result.returncode == 0
    assert execute_action.call_count == 1
    assert (
        "Run summary: 0 started, 0 direct, 0 direct-blocked, 1 launch-blocked, 0 cleared-only, 0 capacity-blocked"
        in result.stdout
    )
    assert "Launch Blocked:" in result.stdout
    assert "Direct Blocked:" not in result.stdout
    assert "Cleared Only:" not in result.stdout
    assert "Capacity Blocked:" not in result.stdout
    assert f"{owner.id} [reconcile]" in result.stdout
    assert "Reconcile recovery owner" in result.stdout
    assert "active rebase child already exists for this owner" in result.stdout


def test_unstick_run_does_not_report_reconcile_rebase_startup_error_as_direct_error(tmp_path):
    result, owner, execute_action = _invoke_unstick_run_for_reconcile_recovery(
        tmp_path,
        exec_result=AdvanceActionExecutionResult(
            action_type="reconcile_branch_divergence",
            status="error",
            execution_phase="worker_launch",
            message="startup preparation failed for task gza-999",
            error_message="Failed to start rebase worker for task gza-999",
            worker_consuming=False,
            worker_label="rebase",
        ),
    )

    assert result.returncode == 0
    assert execute_action.call_count == 1
    assert (
        "Run summary: 0 started, 0 direct, 0 direct-blocked, 1 launch-blocked, 0 cleared-only, 0 capacity-blocked"
        in result.stdout
    )
    assert "Launch Blocked:" in result.stdout
    assert "Direct Blocked:" not in result.stdout
    assert "Cleared Only:" not in result.stdout
    assert "Capacity Blocked:" not in result.stdout
    assert f"{owner.id} [reconcile]" in result.stdout
    assert "Reconcile recovery owner" in result.stdout
    assert "startup preparation failed for task gza-999" in result.stdout


def test_unstick_run_reports_real_reconcile_rebase_spawn_failure_diagnostic_as_launch_blocked(tmp_path):
    result, owner, spawn_worker = _invoke_unstick_run_for_reconcile_recovery(
        tmp_path,
        exec_result=None,
    )

    assert result.returncode == 0
    assert spawn_worker.call_count == 1
    spawned_task = spawn_worker.call_args.args[0]
    assert (
        "Run summary: 0 started, 0 direct, 0 direct-blocked, 1 launch-blocked, 0 cleared-only, 0 capacity-blocked"
        in result.stdout
    )
    assert "Launch Blocked:" in result.stdout
    assert "Direct Blocked:" not in result.stdout
    assert "Cleared Only:" not in result.stdout
    assert "Capacity Blocked:" not in result.stdout
    assert f"{owner.id} [reconcile] reconcile_branch_divergence: Reconcile recovery owner" in result.stdout
    assert f"Failed to start rebase worker for task {spawned_task.id}" in result.stdout


def test_unstick_run_reports_real_recovery_needs_rebase_spawn_failure_diagnostic_as_launch_blocked(tmp_path):
    result, owner, spawn_worker = _invoke_unstick_run_for_needs_rebase_recovery(tmp_path)

    assert result.returncode == 0
    assert spawn_worker.call_count == 1
    spawned_task = spawn_worker.call_args.args[0]
    assert (
        "Run summary: 0 started, 0 direct, 0 direct-blocked, 1 launch-blocked, 0 cleared-only, 0 capacity-blocked"
        in result.stdout
    )
    assert "Launch Blocked:" in result.stdout
    assert "Direct Blocked:" not in result.stdout
    assert "Cleared Only:" not in result.stdout
    assert "Capacity Blocked:" not in result.stdout
    assert f"{owner.id} [retry-limit] needs_rebase: Needs rebase recovery owner" in result.stdout
    assert f"Failed to start rebase worker for task {spawned_task.id}" in result.stdout


def test_unstick_run_reports_reconcile_direct_error_as_direct_blocked_error(tmp_path):
    result, owner, execute_action = _invoke_unstick_run_for_reconcile_recovery(
        tmp_path,
        exec_result=AdvanceActionExecutionResult(
            action_type="reconcile_branch_divergence",
            status="error",
            message="publication reconcile failed",
            worker_consuming=False,
        ),
    )

    assert result.returncode == 0
    assert execute_action.call_count == 1
    assert (
        "Run summary: 0 started, 0 direct, 1 direct-blocked, 0 launch-blocked, 0 cleared-only, 0 capacity-blocked"
        in result.stdout
    )
    assert "Direct Blocked:" in result.stdout
    assert f"{owner.id} [reconcile] reconcile_branch_divergence error: Reconcile recovery owner" in result.stdout
    assert "Cleared Only:" not in result.stdout


def _invoke_unstick_run_for_isolated_merge_conflict_rebase(
    tmp_path,
    *,
    failure_kind: str,
):
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\n")
    store = make_store(tmp_path)
    owner = store.add("Merge conflict owner", task_type="implement")
    assert owner.id is not None
    owner.status = "completed"
    owner.completed_at = datetime.now(UTC)
    owner.branch = "feature/merge-conflict-owner"
    owner.has_commits = True
    store.update(owner)
    store.set_merge_status(owner.id, "unmerged")

    rebase_task = store.add("Merge conflict rebase", task_type="rebase", based_on=owner.id, branch=owner.branch)
    assert rebase_task.id is not None
    active_rebase = store.add(
        "Active merge conflict rebase", task_type="rebase", based_on=owner.id, branch=owner.branch
    )
    assert active_rebase.id is not None

    action = {"type": "merge", "description": "Merge completed task"}
    owner_row = LineageOwnerRow(
        owner_task=owner,
        members=(owner,),
        tree=None,
        lineage_status="actionable",
        next_action=action,
        next_action_reason="backstop",
        unresolved_tasks=(owner,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=owner,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )
    preview_entry = DispatchPreviewEntry(
        lane="pending",
        task=owner,
        owner_task=owner,
        runnable=True,
        worker_consuming=False,
        advance_action=action,
        lineage_row=owner_row,
    )
    plan_slots = 0 if failure_kind == "no_free_slot" else 1
    plan = SimpleNamespace(
        running_task_ids=(),
        anonymous_worker_count=0,
        pending_count=1,
        blocked_pending_count=0,
        running=0,
        slots=plan_slots,
        analysis=SimpleNamespace(
            target_branch="main",
            scope_gaps=(),
            owner_rows=(owner_row,),
            watch_read_context=RecoveryReadContext(),
            lifecycle_rows=(owner_row,),
            recovery_rows=(),
            recovery_lane_entry_by_failed_id={},
            action_plan=((owner_row, owner, action),),
            recovery_attention_rows=(),
            recovery_visible_skips=(),
            recovery_undispatched_rows=(),
            active_recovery_subject_ids=frozenset(),
            actionable_failed=(),
            pending_recovery_task_ids=frozenset(),
            dispatch_preview=DispatchPreview(
                entries=(preview_entry,),
                owner_rows=(owner_row,),
                read_context=RecoveryReadContext(),
            ),
        ),
    )
    outcomes = (
        UnstickOutcome(
            owner_task=owner, reason_class="backstop", status="rearmed", detail="cleared watch-no-progress-backstop"
        ),
    )
    create_rebase_side_effect = (
        DuplicateActiveChildError(active_rebase)
        if failure_kind == "duplicate"
        else RuntimeError("create rebase exploded")
        if failure_kind == "create_error"
        else rebase_task
    )

    with ExitStack() as stack:
        stack.enter_context(patch("gza.cli.unstick.Git", return_value=_UnstickGitDouble()))
        stack.enter_context(
            patch(
                "gza.cli.unstick.select_and_clear_parked_tasks",
                return_value=SimpleNamespace(selected=(object(),), outcomes=outcomes, stale_backstop_cleared=0),
            )
        )
        stack.enter_context(
            patch(
                "gza.cli.unstick.get_concurrency_snapshot",
                return_value=ConcurrencySnapshot(
                    limit=1,
                    running=0,
                    available=1,
                    live_pids=frozenset(),
                    running_task_ids=(),
                    anonymous_worker_count=0,
                    current_pid_counted=False,
                ),
            )
        )
        stack.enter_context(patch("gza.cli.unstick._build_watch_cycle_plan", return_value=plan))
        stack.enter_context(patch("gza.cli.watch.Git", return_value=_UnstickGitDouble()))
        stack.enter_context(patch("gza.cli._common.reconcile_in_progress_tasks"))
        stack.enter_context(patch("gza.cli._common.prune_terminal_dead_workers"))
        stack.enter_context(patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"))
        stack.enter_context(patch("gza.cli.watch.reconcile_stale_watch_no_progress_parks"))
        stack.enter_context(patch("gza.cli.watch._warn_if_installed_gza_changed"))
        stack.enter_context(patch("gza.cli.watch.verify_gate_enabled", return_value=False))
        stack.enter_context(patch("gza.cli.watch.ensure_watch_main_checkout", return_value=_UnstickGitDouble()))
        stack.enter_context(
            patch(
                "gza.cli.watch.check_canonical_checkout_invariant",
                return_value=SimpleNamespace(
                    restored=False,
                    needs_attention=False,
                    dirty_tracked_paths=[],
                    current_branch="main",
                    expected_branch="main",
                ),
            )
        )
        stack.enter_context(
            patch(
                "gza.cli.watch.check_main_integration_verify",
                return_value=SimpleNamespace(
                    merges_halted=False,
                    state=SimpleNamespace(task=SimpleNamespace(id=None), alert_message=None),
                ),
            )
        )
        stack.enter_context(
            patch(
                "gza.cli.watch.build_dispatch_preview",
                return_value=DispatchPreview(
                    entries=(preview_entry,),
                    owner_rows=(owner_row,),
                    read_context=RecoveryReadContext(),
                ),
            )
        )
        stack.enter_context(patch("gza.cli.watch.determine_next_action", return_value=action))
        stack.enter_context(
            patch(
                "gza.cli.watch._execute_merge_action",
                return_value=SimpleNamespace(rc=1, created_followups=[], reused_followups=[]),
            )
        )
        stack.enter_context(
            patch(
                "gza.cli.watch._assess_isolated_merge_failure",
                return_value=watch_module._IsolatedMergeFailureAssessment(True),
            )
        )
        stack.enter_context(patch("gza.cli.watch.cleanup_failed_merge_checkout"))
        if failure_kind == "permit":
            stack.enter_context(
                patch(
                    "gza.cli.watch.launch_permit",
                    side_effect=MaxConcurrentTasksError("already at max concurrent tasks: 1 running, limit is 1"),
                )
            )
        if failure_kind in {"duplicate", "create_error"}:
            create_rebase = stack.enter_context(
                patch("gza.cli.watch._create_rebase_task", side_effect=create_rebase_side_effect)
            )
        else:
            create_rebase = stack.enter_context(patch("gza.cli.watch._create_rebase_task", return_value=rebase_task))
        if failure_kind == "preparation":
            stack.enter_context(patch("gza.cli.watch._prepare_task_for_immediate_execution", return_value=None))
        else:
            stack.enter_context(
                patch(
                    "gza.cli.watch._prepare_task_for_immediate_execution",
                    side_effect=lambda _config, task, **_kwargs: task,
                )
            )
        spawn_worker = stack.enter_context(
            patch("gza.cli.watch._spawn_background_worker", return_value=1 if failure_kind == "spawn" else 0)
        )
        stack.enter_context(patch("gza.cli.watch._maybe_emit_active_watch_recovery_backoff", return_value=False))
        stack.enter_context(patch("gza.cli.watch._maybe_park_watch_no_progress", return_value=None))
        stack.enter_context(
            patch("gza.cli.watch._watch_no_progress_result_deferred_for_transient_backoff", return_value=False)
        )
        stack.enter_context(
            patch("gza.cli.watch._observe_selected_watch_no_progress_without_dispatch", return_value=None)
        )
        stack.enter_context(patch("gza.cli.watch._emit_cycle_attention_summary"))
        stack.enter_context(patch("gza.cli.watch._count_live_workers", return_value=0))
        stack.enter_context(patch("gza.cli.watch._scoped_watch_active_count", return_value=0))
        result = invoke_gza(
            "unstick",
            str(owner.id),
            "--reason",
            "backstop",
            "--run",
            "--project",
            str(tmp_path),
        )

    return result, owner, rebase_task, active_rebase, create_rebase, spawn_worker


@pytest.mark.parametrize("failure_kind", ("permit", "no_free_slot"))
def test_unstick_run_reports_merge_conflict_rebase_capacity_races_as_capacity_blocked(tmp_path, failure_kind):
    result, owner, rebase_task, _active_rebase, create_rebase, spawn_worker = (
        _invoke_unstick_run_for_isolated_merge_conflict_rebase(
            tmp_path,
            failure_kind=failure_kind,
        )
    )

    assert result.returncode == 0
    assert (
        "Run summary: 0 started, 0 direct, 0 direct-blocked, 0 launch-blocked, 0 cleared-only, 1 capacity-blocked"
        in result.stdout
    )
    assert "Capacity Blocked:" in result.stdout
    assert f"{owner.id} [backstop] Merge conflict owner" in result.stdout
    assert "Launch Blocked:" not in result.stdout
    assert "Direct Blocked:" not in result.stdout
    assert "Cleared Only:" not in result.stdout
    if failure_kind == "permit":
        assert create_rebase.call_count == 1
        assert spawn_worker.call_count == 0
        assert (
            "already at max concurrent tasks: 1 running, limit is 1"
            in (tmp_path / ".gza" / "unstick-run.log").read_text()
        )
        assert (
            f"merge conflict queued rebase {rebase_task.id} (no launch capacity)"
            in (tmp_path / ".gza" / "unstick-run.log").read_text()
        )
    else:
        assert create_rebase.call_count == 1
        assert spawn_worker.call_count == 0
        assert (
            f"merge conflict queued rebase {rebase_task.id} (no free slots)"
            in (tmp_path / ".gza" / "unstick-run.log").read_text()
        )


@pytest.mark.parametrize("failure_kind", ("duplicate", "create_error", "preparation", "spawn"))
def test_unstick_run_reports_merge_conflict_rebase_launch_failures_as_launch_blocked(tmp_path, failure_kind):
    result, owner, rebase_task, active_rebase, create_rebase, spawn_worker = (
        _invoke_unstick_run_for_isolated_merge_conflict_rebase(
            tmp_path,
            failure_kind=failure_kind,
        )
    )

    assert result.returncode == 0
    assert create_rebase.call_count == 1
    assert (
        "Run summary: 0 started, 0 direct, 0 direct-blocked, 1 launch-blocked, 0 cleared-only, 0 capacity-blocked"
        in result.stdout
    )
    assert "Launch Blocked:" in result.stdout
    assert "Capacity Blocked:" not in result.stdout
    assert "Direct Blocked:" not in result.stdout
    assert "Cleared Only:" not in result.stdout
    assert f"{owner.id} [backstop] needs_rebase: Merge conflict owner" in result.stdout
    expected_detail = {
        "duplicate": f"rebase already pending/in progress for branch {owner.branch}: {active_rebase.id}",
        "create_error": "failed to create rebase task (create rebase exploded)",
        "preparation": f"failed to prepare merge-conflict rebase task {rebase_task.id}",
        "spawn": "merge conflict rebase worker spawn failed",
    }[failure_kind]
    assert expected_detail in result.stdout
    if failure_kind == "spawn":
        assert spawn_worker.call_count == 1
    else:
        assert spawn_worker.call_count == 0


def test_unstick_run_reports_zero_slot_lifecycle_owner_as_capacity_blocked(tmp_path, monkeypatch):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    owner = store.add("Blocked lifecycle owner", task_type="implement")
    assert owner.id is not None
    owner.status = "completed"
    owner.completed_at = datetime.now(UTC)
    owner.branch = "feature/blocked-lifecycle"
    owner.has_commits = True
    store.update(owner)
    monkeypatch.setattr("gza.cli.unstick.Git", _UnstickGitDouble)
    owner_row = LineageOwnerRow(
        owner_task=owner,
        members=(owner,),
        tree=None,
        lineage_status="actionable",
        next_action={"type": "create_review", "description": "Create review before merge"},
        next_action_reason="review",
        unresolved_tasks=(owner,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=owner,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )
    outcomes = (
        UnstickOutcome(
            owner_task=owner, reason_class="backstop", status="rearmed", detail="cleared watch-no-progress-backstop"
        ),
    )

    with (
        patch(
            "gza.cli.unstick.select_and_clear_parked_tasks",
            return_value=SimpleNamespace(selected=(object(),), outcomes=outcomes, stale_backstop_cleared=0),
        ),
        patch(
            "gza.cli.unstick.get_concurrency_snapshot",
            return_value=ConcurrencySnapshot(
                limit=1,
                running=1,
                available=0,
                live_pids=frozenset({101}),
                running_task_ids=("gza-900",),
                anonymous_worker_count=0,
                current_pid_counted=False,
            ),
        ),
        patch(
            "gza.cli.watch.get_concurrency_snapshot",
            return_value=ConcurrencySnapshot(
                limit=1,
                running=1,
                available=0,
                live_pids=frozenset({101}),
                running_task_ids=("gza-900",),
                anonymous_worker_count=0,
                current_pid_counted=False,
            ),
        ),
        patch("gza.cli.watch.Git", return_value=_UnstickGitDouble()),
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch.reconcile_stale_watch_no_progress_parks"),
        patch(
            "gza.cli.watch.check_canonical_checkout_invariant",
            return_value=SimpleNamespace(
                restored=False,
                needs_attention=False,
                dirty_tracked_paths=[],
                current_branch="main",
                expected_branch="main",
            ),
        ),
        patch(
            "gza.cli.watch.check_main_integration_verify",
            return_value=SimpleNamespace(
                merges_halted=False,
                state=SimpleNamespace(task=SimpleNamespace(id=None), alert_message=None),
            ),
        ),
        patch("gza.cli.watch.collect_scoped_tag_scope_gaps", return_value=[]),
        patch(
            "gza.cli.watch._query_owner_rows_with_context",
            return_value=((owner_row,), RecoveryReadContext()),
        ),
        patch("gza.cli.watch.collect_recovery_lane_entries", return_value=[]),
        patch(
            "gza.cli.watch.determine_next_action",
            return_value={"type": "create_review", "description": "Create review before merge"},
        ),
        patch(
            "gza.cli.watch.execute_advance_action",
            side_effect=AssertionError("lifecycle capacity gate should skip before execution"),
        ),
    ):
        result = invoke_gza(
            "unstick",
            str(owner.id),
            "--reason",
            "backstop",
            "--run",
            "--project",
            str(tmp_path),
        )

    assert result.returncode == 0
    assert (
        "Run summary: 0 started, 0 direct, 0 direct-blocked, 0 launch-blocked, 0 cleared-only, 1 capacity-blocked"
        in result.stdout
    )
    assert "Capacity Blocked:" in result.stdout
    assert f"{owner.id} [backstop] Blocked lifecycle owner" in result.stdout
    assert "Cleared Only:" not in result.stdout


def test_unstick_cli_rearms_real_retry_limit_failed_owner_by_retry_id(tmp_path, monkeypatch):
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "max_resume_attempts: 1\n")
    store = make_store(tmp_path)

    impl = store.add("CLI retry limit owner", task_type="implement")
    assert impl.id is not None
    impl.status = "failed"
    impl.failure_reason = "MAX_TURNS"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/cli-retry-limit"
    impl.session_id = "sess-cli-retry-limit"
    impl.has_commits = False
    store.update(impl)

    first_retry = store.add(impl.prompt, task_type="implement", based_on=impl.id, depends_on=impl.depends_on)
    assert first_retry.id is not None
    first_retry.status = "failed"
    first_retry.failure_reason = "MAX_TURNS"
    first_retry.completed_at = datetime.now(UTC)
    first_retry.branch = impl.branch
    first_retry.session_id = impl.session_id
    first_retry.has_commits = False
    store.update(first_retry)

    exhausted_retry = store.add(impl.prompt, task_type="implement", based_on=impl.id, depends_on=impl.depends_on)
    assert exhausted_retry.id is not None
    exhausted_retry.status = "failed"
    exhausted_retry.failure_reason = "MAX_TURNS"
    exhausted_retry.completed_at = datetime.now(UTC)
    exhausted_retry.branch = impl.branch
    exhausted_retry.session_id = impl.session_id
    exhausted_retry.has_commits = False
    store.update(exhausted_retry)

    with patch(
        "gza.recovery_engine._load_merge_context",
        return_value=_MergeContext(git=_UnstickGitDouble(), default_branch="main"),
    ):
        decision = decide_failed_task_recovery(store, impl, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "retry_limit_reached"

    monkeypatch.setattr("gza.cli.unstick.Git", _UnstickGitDouble)

    with patch(
        "gza.recovery_engine._load_merge_context",
        return_value=_MergeContext(git=_UnstickGitDouble(), default_branch="main"),
    ):
        result = invoke_gza(
            "unstick",
            exhausted_retry.id,
            "--reason",
            "retry-limit",
            "--project",
            str(tmp_path),
        )

    assert result.returncode == 0
    assert "No parked owners matched" not in result.stdout
    assert "Selected 1 parked owner(s)" in result.stdout
    assert f"{impl.id} [retry-limit] CLI retry limit owner" in result.stdout

    rearm = store.get_parked_task_rearm(
        subject_kind="task",
        subject_id=impl.id,
        attention_reason="retry-limit-reached",
    )
    assert rearm is not None
    assert rearm.manual_rearm_epoch == 1
