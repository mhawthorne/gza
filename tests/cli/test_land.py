from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal
from unittest.mock import MagicMock, patch

import pytest

from gza.db import MergeUnit, SqliteTaskStore
from gza.landing import (
    LandBlocked,
    LandingCollaborators,
    LandingCoordinator,
    LandingStore,
    LandRequest,
    LandTerminalResult,
)
from tests.cli.conftest import invoke_gza, make_store, setup_config

TERMINAL_STATES = ("merged", "empty", "redundant")
NO_WORK_STATES = ("empty", "redundant")
PROHIBITED_COLLABORATORS = (
    "run_rebase",
    "run_provider",
    "run_source_verify",
    "run_post_merge_verify",
    "run_spec_review",
    "run_code_review",
    "run_judgment",
    "create_followup_or_deferred_task",
    "materialize_artifact",
    "mark_merged",
    "git_merge",
)


class CountingStore:
    def __init__(self, store: SqliteTaskStore) -> None:
        self._store = store
        self.merge_unit_state_writes: list[tuple[str, str]] = []

    def __getattr__(self, name: str) -> object:
        return getattr(self._store, name)

    def resolve_merge_unit_subject(self, subject_id: str) -> MergeUnit | None:
        return self._store.resolve_merge_unit_subject(subject_id)

    def get_merge_unit(self, unit_id: str) -> MergeUnit | None:
        return self._store.get_merge_unit(unit_id)

    def set_merge_unit_state(self, unit_id: str, state: str, **kwargs: Any) -> bool:
        updated = self._store.set_merge_unit_state(unit_id, state, **kwargs)
        if updated:
            self.merge_unit_state_writes.append((unit_id, state))
        return updated


class TerminalProofGit:
    def __init__(
        self,
        *,
        merged: bool = False,
        source_sha: str = "a" * 40,
        target_sha: str = "a" * 40,
        commits_ahead: int = 0,
        net_diff: bool | None = False,
        on_first_parent: bool = True,
    ) -> None:
        self.calls: list[tuple[str, str]] = []
        self.merged = merged
        self.source_sha = source_sha
        self.target_sha = target_sha
        self.commits_ahead = commits_ahead
        self.net_diff = net_diff
        self.on_first_parent = on_first_parent

    def branch_exists(self, branch: str) -> bool:
        self.calls.append(("branch_exists", branch))
        return True

    def is_merged(self, branch: str, into: str | None = None) -> bool:
        self.calls.append(("is_merged", f"{branch}->{into}"))
        return self.merged

    def rev_parse_if_exists(self, ref: str) -> str | None:
        self.calls.append(("rev_parse_if_exists", ref))
        if ref.startswith("feature/"):
            return self.source_sha
        return self.target_sha

    def merge_base(self, ref1: str, ref2: str) -> str:
        self.calls.append(("merge_base", f"{ref1}->{ref2}"))
        return ref2

    def count_commits_ahead(self, branch: str, base: str) -> int:
        self.calls.append(("count_commits_ahead", f"{branch}->{base}"))
        return self.commits_ahead

    def has_non_empty_source_diff_against_target(self, source_ref: str, target_ref: str) -> bool | None:
        self.calls.append(("has_non_empty_source_diff_against_target", f"{source_ref}->{target_ref}"))
        return self.net_diff

    def is_on_first_parent_history(self, source_ref: str, target_ref: str) -> bool:
        self.calls.append(("is_on_first_parent_history", f"{source_ref}->{target_ref}"))
        return self.on_first_parent


def _task_with_unit(
    tmp_path: Path,
    *,
    state: str,
    has_commits: bool = False,
) -> tuple[SqliteTaskStore, str, str]:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("land terminal branch", task_type="implement")
    task.status = "completed"
    task.branch = f"feature/{state}-{task.id}"
    task.has_commits = has_commits
    task.merge_status = "unmerged"
    store.update(task)
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    if unit.state != state:
        if state == "merged":
            store.set_merge_unit_state(unit.id, state, merged_by_task_id=task.id, merge_source="manual")
        else:
            store.set_merge_unit_state(unit.id, state)
    return store, task.id, unit.id


def _collaborators(
    reconcile_terminal_state: Callable[
        [LandingStore, MergeUnit], Literal["merged", "empty", "redundant"] | None
    ]
    | None = None,
) -> LandingCollaborators:
    values = {name: MagicMock(name=name) for name in PROHIBITED_COLLABORATORS}
    values["reconcile_terminal_state"] = (
        MagicMock(name="reconcile_terminal_state", side_effect=reconcile_terminal_state)
        if reconcile_terminal_state is not None
        else MagicMock(name="reconcile_terminal_state", return_value=None)
    )
    return LandingCollaborators(**values)


def _assert_zero_downstream_activity(collaborators: LandingCollaborators) -> None:
    for name in PROHIBITED_COLLABORATORS:
        getattr(collaborators, name).assert_not_called()


@pytest.mark.parametrize("state", TERMINAL_STATES)
@pytest.mark.parametrize("dry_run", [False, True])
def test_land_initial_terminal_states_return_known_outcome_without_activity(
    tmp_path: Path,
    *,
    state: Literal["merged", "empty", "redundant"],
    dry_run: bool,
) -> None:
    store, task_id, unit_id = _task_with_unit(tmp_path, state=state)
    counting_store = CountingStore(store)
    collaborators = _collaborators()

    result = LandingCoordinator(counting_store, collaborators=collaborators).land(
        LandRequest(task_id=task_id, dry_run=dry_run)
    )

    assert isinstance(result, LandTerminalResult)
    assert result.outcome == state
    assert result.dry_run is dry_run
    assert result.reconciled is False
    assert counting_store.merge_unit_state_writes == []
    assert store.get_merge_unit(unit_id).state == state  # type: ignore[union-attr]
    collaborators.reconcile_terminal_state.assert_not_called()
    _assert_zero_downstream_activity(collaborators)


@pytest.mark.parametrize("state", NO_WORK_STATES)
@pytest.mark.parametrize("dry_run", [False, True])
def test_land_reconciles_unmerged_to_terminal_no_work_with_only_allowed_write(
    tmp_path: Path,
    *,
    state: Literal["empty", "redundant"],
    dry_run: bool,
) -> None:
    store, task_id, unit_id = _task_with_unit(tmp_path, state="unmerged")
    counting_store = CountingStore(store)
    collaborators = _collaborators(lambda _store, _unit: state)

    result = LandingCoordinator(counting_store, collaborators=collaborators).land(
        LandRequest(task_id=task_id, dry_run=dry_run)
    )

    assert isinstance(result, LandTerminalResult)
    assert result.outcome == state
    assert result.dry_run is dry_run
    assert result.reconciled is True
    expected_writes = [] if dry_run else [(unit_id, state)]
    assert counting_store.merge_unit_state_writes == expected_writes
    expected_durable_state = "unmerged" if dry_run else state
    assert store.get_merge_unit(unit_id).state == expected_durable_state  # type: ignore[union-attr]
    collaborators.reconcile_terminal_state.assert_called_once()
    _assert_zero_downstream_activity(collaborators)


@pytest.mark.parametrize("dry_run", [False, True])
def test_land_reconciles_unmerged_to_merged_with_only_allowed_write(
    tmp_path: Path,
    *,
    dry_run: bool,
) -> None:
    store, task_id, unit_id = _task_with_unit(tmp_path, state="unmerged", has_commits=True)
    counting_store = CountingStore(store)
    collaborators = _collaborators(lambda _store, _unit: "merged")

    result = LandingCoordinator(counting_store, collaborators=collaborators).land(
        LandRequest(task_id=task_id, dry_run=dry_run)
    )

    assert isinstance(result, LandTerminalResult)
    assert result.outcome == "merged"
    assert result.dry_run is dry_run
    assert result.reconciled is True
    expected_writes = [] if dry_run else [(unit_id, "merged")]
    assert counting_store.merge_unit_state_writes == expected_writes
    expected_durable_state = "unmerged" if dry_run else "merged"
    assert store.get_merge_unit(unit_id).state == expected_durable_state  # type: ignore[union-attr]
    collaborators.reconcile_terminal_state.assert_called_once()
    _assert_zero_downstream_activity(collaborators)


@pytest.mark.parametrize("concurrent_state", TERMINAL_STATES)
def test_land_reconciliation_does_not_clobber_concurrent_terminal_state(
    tmp_path: Path,
    *,
    concurrent_state: Literal["merged", "empty", "redundant"],
) -> None:
    store, task_id, unit_id = _task_with_unit(tmp_path, state="unmerged")
    counting_store = CountingStore(store)

    def _reconcile(_store: LandingStore, _unit: MergeUnit) -> Literal["empty"]:
        if concurrent_state == "merged":
            store.set_merge_unit_state(unit_id, concurrent_state, merged_by_task_id=task_id, merge_source="manual")
        else:
            store.set_merge_unit_state(unit_id, concurrent_state)
        return "empty"

    collaborators = _collaborators(_reconcile)

    result = LandingCoordinator(counting_store, collaborators=collaborators).land(LandRequest(task_id=task_id))

    assert isinstance(result, LandTerminalResult)
    assert result.outcome == concurrent_state
    assert result.reconciled is False
    assert counting_store.merge_unit_state_writes == []
    assert store.get_merge_unit(unit_id).state == concurrent_state  # type: ignore[union-attr]
    collaborators.reconcile_terminal_state.assert_called_once()
    _assert_zero_downstream_activity(collaborators)


def test_land_reconciliation_fails_closed_for_concurrent_nonterminal_state(tmp_path: Path) -> None:
    store, task_id, unit_id = _task_with_unit(tmp_path, state="unmerged")
    counting_store = CountingStore(store)

    def _reconcile(_store: LandingStore, _unit: MergeUnit) -> Literal["redundant"]:
        store.set_merge_unit_state(unit_id, "blocked")
        return "redundant"

    collaborators = _collaborators(_reconcile)

    result = LandingCoordinator(counting_store, collaborators=collaborators).land(LandRequest(task_id=task_id))

    assert isinstance(result, LandBlocked)
    assert result.reason_code == "merge-state-changed"
    assert "changed from unmerged to blocked" in result.fact
    assert counting_store.merge_unit_state_writes == []
    assert store.get_merge_unit(unit_id).state == "blocked"  # type: ignore[union-attr]
    collaborators.reconcile_terminal_state.assert_called_once()
    _assert_zero_downstream_activity(collaborators)


@pytest.mark.parametrize("state", TERMINAL_STATES)
@pytest.mark.parametrize("dry_run", [False, True])
def test_land_cli_reports_initial_terminal_result(
    tmp_path: Path,
    *,
    state: str,
    dry_run: bool,
) -> None:
    store, task_id, unit_id = _task_with_unit(tmp_path, state=state)
    unit = store.get_merge_unit(unit_id)
    assert unit is not None

    args = ["land", task_id, "--project", str(tmp_path)]
    if dry_run:
        args.append("--dry-run")
    result = invoke_gza(*args, cwd=tmp_path)

    assert result.returncode == 0
    if dry_run:
        assert result.stdout.startswith("Dry run: ")
    assert f"owner {task_id}" in result.stdout
    assert f"source {unit.source_branch}" in result.stdout
    assert f"target {unit.target_branch}" in result.stdout
    assert f"known outcome {state}" in result.stdout
    if state == "merged":
        assert "already merged" in result.stdout
    else:
        assert f"terminal no-work state {state}" in result.stdout
        assert "already merged" not in result.stdout


@pytest.mark.parametrize(
    ("state", "has_commits"),
    [("empty", False), ("redundant", True), ("merged", True)],
)
@pytest.mark.parametrize("dry_run", [False, True])
def test_land_cli_reconciles_unmerged_terminal_state_from_canonical_git_proof(
    tmp_path: Path,
    *,
    state: Literal["merged", "empty", "redundant"],
    has_commits: bool,
    dry_run: bool,
) -> None:
    store, task_id, unit_id = _task_with_unit(tmp_path, state="unmerged", has_commits=has_commits)
    proof_git = (
        TerminalProofGit(
            merged=True,
            source_sha="b" * 40,
            target_sha="a" * 40,
            commits_ahead=0,
            net_diff=False,
            on_first_parent=False,
        )
        if state == "merged"
        else TerminalProofGit()
    )
    unit = store.get_merge_unit(unit_id)
    assert unit is not None
    state_writes: list[tuple[str, str, str | None]] = []
    original_set_merge_unit_state = SqliteTaskStore.set_merge_unit_state

    def _record_set_merge_unit_state(
        store_self: SqliteTaskStore,
        unit_id_arg: str,
        state_arg: str,
        **kwargs: Any,
    ) -> bool:
        state_writes.append((unit_id_arg, state_arg, kwargs.get("expected_state")))
        return original_set_merge_unit_state(store_self, unit_id_arg, state_arg, **kwargs)

    args = ["land", task_id, "--project", str(tmp_path)]
    if dry_run:
        args.append("--dry-run")
    with (
        patch("gza.cli.land.Git", return_value=proof_git),
        patch.object(SqliteTaskStore, "set_merge_unit_state", autospec=True, side_effect=_record_set_merge_unit_state),
    ):
        result = invoke_gza(*args, cwd=tmp_path)

    assert result.returncode == 0
    assert f"owner {task_id}" in result.stdout
    assert f"source {unit.source_branch}" in result.stdout
    assert f"target {unit.target_branch}" in result.stdout
    assert f"known outcome {state}" in result.stdout
    if state == "merged":
        assert "already merged" in result.stdout
        assert "terminal no-work state" not in result.stdout
    else:
        assert f"terminal no-work state {state}" in result.stdout
        assert "already merged" not in result.stdout
    if dry_run:
        assert result.stdout.startswith("Dry run: ")
        assert "would reconcile" in result.stdout
    else:
        assert "reconciled" in result.stdout
    expected_durable_state = "unmerged" if dry_run else state
    expected_writes = [] if dry_run else [(unit_id, state, "unmerged")]
    assert state_writes == expected_writes
    assert store.get_merge_unit(unit_id).state == expected_durable_state  # type: ignore[union-attr]
    assert ("branch_exists", store.get_merge_unit(unit_id).source_branch) in proof_git.calls  # type: ignore[union-attr]
    assert any(call[0] == "count_commits_ahead" for call in proof_git.calls)
    if state == "merged":
        assert any(call[0] == "is_on_first_parent_history" for call in proof_git.calls)
