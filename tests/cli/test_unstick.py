from __future__ import annotations

import importlib
import sys
from contextlib import ExitStack
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import patch

from gza.cli.advance_executor import AdvanceActionExecutionResult
from gza.concurrency import ConcurrencySnapshot
from gza.config import Config
from gza.git import Git
from gza.lineage_query import LineageOwnerRow, RecoveryReadContext
from gza.recovery_engine import _MergeContext, decide_failed_task_recovery
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
    assert "--reason {backstop,retry-limit,reconcile}" in result.stdout
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
        UnstickOutcome(owner_task=first, reason_class="retry-limit", status="rearmed", detail="cleared retry-limit-reached"),
        UnstickOutcome(owner_task=second, reason_class="reconcile", status="rearmed", detail="cleared reconcile"),
        UnstickOutcome(owner_task=third, reason_class="backstop", status="rearmed", detail="cleared watch-no-progress-backstop"),
    )

    with (
        patch(
            "gza.cli.unstick.select_and_clear_parked_tasks",
            return_value=SimpleNamespace(selected=(object(), object(), object()), outcomes=outcomes, stale_backstop_cleared=0),
        ),
        patch(
            "gza.cli.unstick._dispatch_rearmed_owners",
            return_value=SimpleNamespace(
                started_owner_ids=frozenset({str(first.id)}),
                capacity_blocked_owner_ids=frozenset({str(third.id)}),
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
    assert "Run summary: 1 started, 1 cleared-only, 1 capacity-blocked" in result.stdout
    assert "Started:" in result.stdout
    assert f"{first.id} [retry-limit] Started owner" in result.stdout
    assert "Cleared Only:" in result.stdout
    assert f"{second.id} [reconcile] Direct owner" in result.stdout
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
        stack.enter_context(patch("gza.cli.watch._confirm_watch_dispatch_start", return_value=(True, first_review)))
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
        UnstickOutcome(owner_task=owner, reason_class="retry-limit", status="rearmed", detail="cleared retry-limit-reached"),
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
    assert "Run summary: 0 started, 0 cleared-only, 1 capacity-blocked" in result.stdout
    assert "Capacity Blocked:" in result.stdout
    assert f"{owner.id} [retry-limit] Blocked retry owner" in result.stdout
    assert "Cleared Only:" not in result.stdout


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
        UnstickOutcome(owner_task=owner, reason_class="backstop", status="rearmed", detail="cleared watch-no-progress-backstop"),
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
        patch("gza.cli.watch.determine_next_action", return_value={"type": "create_review", "description": "Create review before merge"}),
        patch("gza.cli.watch.execute_advance_action", side_effect=AssertionError("lifecycle capacity gate should skip before execution")),
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
    assert "Run summary: 0 started, 0 cleared-only, 1 capacity-blocked" in result.stdout
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

    with patch("gza.recovery_engine._load_merge_context", return_value=_MergeContext(git=_UnstickGitDouble(), default_branch="main")):
        decision = decide_failed_task_recovery(store, impl, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "retry_limit_reached"

    monkeypatch.setattr("gza.cli.unstick.Git", _UnstickGitDouble)

    with patch("gza.recovery_engine._load_merge_context", return_value=_MergeContext(git=_UnstickGitDouble(), default_branch="main")):
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
