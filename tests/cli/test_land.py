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
    MergeUnitProofIdentity,
    TerminalProof,
    reconcile_terminal_merge_truth,
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

    def set_merge_unit_state_if_identity(
        self,
        unit_id: str,
        state: str,
        *,
        expected_identity: MergeUnitProofIdentity,
    ) -> bool:
        updated = self._store.set_merge_unit_state_if_identity(
            unit_id,
            state,
            expected_identity=expected_identity,
        )
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
        patch_present: bool | None = True,
    ) -> None:
        self.calls: list[tuple[str, str]] = []
        self.merged = merged
        self.source_sha = source_sha
        self.target_sha = target_sha
        self.commits_ahead = commits_ahead
        self.net_diff = net_diff
        self.on_first_parent = on_first_parent
        self.patch_present = patch_present

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

    def is_patch_equivalent_commit_present_on_target(self, recorded_head_sha: str, target_branch: str) -> bool | None:
        self.calls.append(("is_patch_equivalent_commit_present_on_target", f"{recorded_head_sha}->{target_branch}"))
        return self.patch_present


def _task_with_unit(
    tmp_path: Path,
    *,
    state: str,
    has_commits: bool = False,
    head_sha: str | None = None,
    base_sha: str | None = None,
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
    if head_sha is not None or base_sha is not None:
        store.refresh_merge_unit_head(unit.id, head_sha=head_sha, base_sha=base_sha)
    return store, task.id, unit.id


def _collaborators(
    reconcile_terminal_state: Callable[
        [LandingStore, MergeUnit], TerminalProof | LandBlocked | Literal["merged", "empty", "redundant"] | None
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


def _terminal_proof_for_unit(
    unit: MergeUnit,
    state: Literal["merged", "empty", "redundant"],
    *,
    source_sha: str | None = "a" * 40,
    target_sha: str | None = "b" * 40,
) -> TerminalProof:
    return TerminalProof(
        state=state,
        identity=MergeUnitProofIdentity(
            source_branch=unit.source_branch,
            target_branch=unit.target_branch,
            state=unit.state,
            owner_task_id=unit.owner_task_id,
            head_sha=unit.head_sha,
            base_sha=unit.base_sha,
        ),
        source_sha=source_sha,
        target_sha=target_sha,
    )


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
def test_land_initial_recorded_head_no_work_returns_terminal_when_patch_represented(
    tmp_path: Path,
    *,
    state: Literal["empty", "redundant"],
    dry_run: bool,
) -> None:
    recorded_head = "c" * 40
    store, task_id, unit_id = _task_with_unit(tmp_path, state=state, has_commits=True, head_sha=recorded_head)
    counting_store = CountingStore(store)
    collaborators = _collaborators(reconcile_terminal_merge_truth(TerminalProofGit(patch_present=True)))

    result = LandingCoordinator(counting_store, collaborators=collaborators).land(
        LandRequest(task_id=task_id, dry_run=dry_run)
    )

    assert isinstance(result, LandTerminalResult)
    assert result.outcome == state
    assert result.reconciled is False
    assert result.dry_run is dry_run
    assert counting_store.merge_unit_state_writes == []
    assert store.get_merge_unit(unit_id).state == state  # type: ignore[union-attr]
    collaborators.reconcile_terminal_state.assert_called_once()
    _assert_zero_downstream_activity(collaborators)


@pytest.mark.parametrize("state", NO_WORK_STATES)
@pytest.mark.parametrize("dry_run", [False, True])
def test_land_initial_recorded_head_no_work_missing_patch_repairs_only_writable(
    tmp_path: Path,
    *,
    state: Literal["empty", "redundant"],
    dry_run: bool,
) -> None:
    store, task_id, unit_id = _task_with_unit(
        tmp_path,
        state=state,
        has_commits=True,
        head_sha="d" * 40,
        base_sha="e" * 40,
    )
    counting_store = CountingStore(store)
    collaborators = _collaborators(reconcile_terminal_merge_truth(TerminalProofGit(patch_present=False)))

    result = LandingCoordinator(counting_store, collaborators=collaborators).land(
        LandRequest(task_id=task_id, dry_run=dry_run)
    )

    assert isinstance(result, LandBlocked)
    assert result.reason_code == ("recorded-head-repair-needed" if dry_run else "required-review-unavailable")
    expected_state = state if dry_run else "unmerged"
    assert store.get_merge_unit(unit_id).state == expected_state  # type: ignore[union-attr]
    assert counting_store.merge_unit_state_writes == ([] if dry_run else [(unit_id, "unmerged")])
    collaborators.reconcile_terminal_state.assert_called_once()
    _assert_zero_downstream_activity(collaborators)


@pytest.mark.parametrize("state", NO_WORK_STATES)
@pytest.mark.parametrize("dry_run", [False, True])
def test_land_initial_recorded_head_no_work_unavailable_proof_fails_closed_terminal(
    tmp_path: Path,
    *,
    state: Literal["empty", "redundant"],
    dry_run: bool,
) -> None:
    store, task_id, unit_id = _task_with_unit(tmp_path, state=state, has_commits=True, head_sha="f" * 40)
    counting_store = CountingStore(store)
    collaborators = _collaborators(reconcile_terminal_merge_truth(TerminalProofGit(patch_present=None)))

    result = LandingCoordinator(counting_store, collaborators=collaborators).land(
        LandRequest(task_id=task_id, dry_run=dry_run)
    )

    assert isinstance(result, LandTerminalResult)
    assert result.outcome == state
    assert result.reconciled is False
    assert result.dry_run is dry_run
    assert store.get_merge_unit(unit_id).state == state  # type: ignore[union-attr]
    assert counting_store.merge_unit_state_writes == []
    collaborators.reconcile_terminal_state.assert_called_once()
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
    assert collaborators.reconcile_terminal_state.call_count == (1 if dry_run else 2)
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
    assert collaborators.reconcile_terminal_state.call_count == (1 if dry_run else 2)
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
    assert collaborators.reconcile_terminal_state.call_count == 1
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
    assert collaborators.reconcile_terminal_state.call_count == 1
    _assert_zero_downstream_activity(collaborators)


def test_land_terminal_write_rejects_head_base_change_after_proof(tmp_path: Path) -> None:
    store, task_id, unit_id = _task_with_unit(
        tmp_path,
        state="unmerged",
        head_sha="1" * 40,
        base_sha="2" * 40,
    )
    counting_store = CountingStore(store)
    calls = 0

    def _reconcile(_store: LandingStore, unit: MergeUnit) -> TerminalProof:
        nonlocal calls
        calls += 1
        proof = _terminal_proof_for_unit(unit, "empty")
        if calls == 1:
            store.refresh_merge_unit_head(unit_id, head_sha="3" * 40, base_sha="4" * 40)
        return proof

    collaborators = _collaborators(_reconcile)

    result = LandingCoordinator(counting_store, collaborators=collaborators).land(LandRequest(task_id=task_id))

    assert isinstance(result, LandBlocked)
    assert result.reason_code == "merge-proof-changed"
    assert store.get_merge_unit(unit_id).state == "unmerged"  # type: ignore[union-attr]
    assert counting_store.merge_unit_state_writes == []
    assert collaborators.reconcile_terminal_state.call_count == 2
    _assert_zero_downstream_activity(collaborators)


def test_land_terminal_write_rejects_owner_identity_change_after_proof(tmp_path: Path) -> None:
    store, task_id, unit_id = _task_with_unit(tmp_path, state="unmerged", head_sha="5" * 40)
    unit = store.get_merge_unit(unit_id)
    assert unit is not None
    new_owner = store.add("new owner", task_type="implement")
    new_owner.status = "completed"
    new_owner.branch = unit.source_branch
    store.update(new_owner)
    assert new_owner.id is not None
    store.attach_task_to_merge_unit(new_owner.id, unit_id, "contributor")
    counting_store = CountingStore(store)
    calls = 0

    def _reconcile(_store: LandingStore, current_unit: MergeUnit) -> TerminalProof:
        nonlocal calls
        calls += 1
        proof = _terminal_proof_for_unit(current_unit, "redundant")
        if calls == 1:
            store.set_merge_unit_owner_task_id(unit_id, new_owner.id)
        return proof

    collaborators = _collaborators(_reconcile)

    result = LandingCoordinator(counting_store, collaborators=collaborators).land(LandRequest(task_id=task_id))

    assert isinstance(result, LandBlocked)
    assert result.reason_code == "merge-proof-changed"
    refreshed = store.get_merge_unit(unit_id)
    assert refreshed is not None
    assert refreshed.state == "unmerged"
    assert refreshed.owner_task_id == new_owner.id
    assert counting_store.merge_unit_state_writes == []
    assert collaborators.reconcile_terminal_state.call_count == 2
    _assert_zero_downstream_activity(collaborators)


@pytest.mark.parametrize(
    ("first_source_sha", "second_source_sha", "first_target_sha", "second_target_sha"),
    [("a" * 40, "b" * 40, "c" * 40, "c" * 40), ("a" * 40, "a" * 40, "c" * 40, "d" * 40)],
)
def test_land_terminal_write_rejects_live_ref_change_after_proof(
    tmp_path: Path,
    *,
    first_source_sha: str,
    second_source_sha: str,
    first_target_sha: str,
    second_target_sha: str,
) -> None:
    store, task_id, unit_id = _task_with_unit(tmp_path, state="unmerged", head_sha="6" * 40)
    counting_store = CountingStore(store)
    calls = 0

    def _reconcile(_store: LandingStore, unit: MergeUnit) -> TerminalProof:
        nonlocal calls
        calls += 1
        return _terminal_proof_for_unit(
            unit,
            "empty",
            source_sha=first_source_sha if calls == 1 else second_source_sha,
            target_sha=first_target_sha if calls == 1 else second_target_sha,
        )

    collaborators = _collaborators(_reconcile)

    result = LandingCoordinator(counting_store, collaborators=collaborators).land(LandRequest(task_id=task_id))

    assert isinstance(result, LandBlocked)
    assert result.reason_code == "merge-proof-changed"
    assert store.get_merge_unit(unit_id).state == "unmerged"  # type: ignore[union-attr]
    assert counting_store.merge_unit_state_writes == []
    assert collaborators.reconcile_terminal_state.call_count == 2
    _assert_zero_downstream_activity(collaborators)


@pytest.mark.parametrize("concurrent_state", TERMINAL_STATES)
def test_land_returns_authoritative_terminal_state_seen_after_write(
    tmp_path: Path,
    *,
    concurrent_state: Literal["merged", "empty", "redundant"],
) -> None:
    store, task_id, unit_id = _task_with_unit(tmp_path, state="unmerged")
    counting_store = CountingStore(store)
    original_set_if_identity = counting_store.set_merge_unit_state_if_identity

    def _set_then_terminal(
        unit_id_arg: str,
        state_arg: str,
        *,
        expected_identity: MergeUnitProofIdentity,
    ) -> bool:
        updated = original_set_if_identity(unit_id_arg, state_arg, expected_identity=expected_identity)
        if updated:
            if concurrent_state == "merged":
                store.set_merge_unit_state(unit_id, concurrent_state, merged_by_task_id=task_id, merge_source="manual")
            else:
                store.set_merge_unit_state(unit_id, concurrent_state)
        return updated

    counting_store.set_merge_unit_state_if_identity = _set_then_terminal  # type: ignore[method-assign]
    collaborators = _collaborators(lambda _store, unit: _terminal_proof_for_unit(unit, "empty"))

    result = LandingCoordinator(counting_store, collaborators=collaborators).land(LandRequest(task_id=task_id))

    assert isinstance(result, LandTerminalResult)
    assert result.outcome == concurrent_state
    assert result.reconciled is True
    assert store.get_merge_unit(unit_id).state == concurrent_state  # type: ignore[union-attr]
    assert counting_store.merge_unit_state_writes == [(unit_id, "empty")]
    assert collaborators.reconcile_terminal_state.call_count == 2
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
    original_set_merge_unit_state_if_identity = SqliteTaskStore.set_merge_unit_state_if_identity

    def _record_set_merge_unit_state_if_identity(
        store_self: SqliteTaskStore,
        unit_id_arg: str,
        state_arg: str,
        **kwargs: Any,
    ) -> bool:
        expected_identity = kwargs.get("expected_identity")
        state_writes.append((unit_id_arg, state_arg, expected_identity.state))
        return original_set_merge_unit_state_if_identity(store_self, unit_id_arg, state_arg, **kwargs)

    args = ["land", task_id, "--project", str(tmp_path)]
    if dry_run:
        args.append("--dry-run")
    with (
        patch("gza.cli.land.Git", return_value=proof_git),
        patch.object(
            SqliteTaskStore,
            "set_merge_unit_state_if_identity",
            autospec=True,
            side_effect=_record_set_merge_unit_state_if_identity,
        ),
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
