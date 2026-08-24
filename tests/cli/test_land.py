from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Literal
from unittest.mock import MagicMock

import pytest

from gza.db import MergeUnit, SqliteTaskStore
from gza.landing import (
    LandRequest,
    LandTerminalResult,
    LandingCollaborators,
    LandingCoordinator,
    LandingStore,
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

    def set_merge_unit_state(self, unit_id: str, state: str) -> None:
        self.merge_unit_state_writes.append((unit_id, state))
        self._store.set_merge_unit_state(unit_id, state)


def _task_with_unit(
    tmp_path: Path,
    *,
    state: str,
) -> tuple[SqliteTaskStore, str, str]:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("land terminal branch", task_type="implement")
    task.status = "completed"
    task.branch = f"feature/{state}-{task.id}"
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
    reconcile_terminal_state: Callable[[LandingStore, MergeUnit], Literal["empty", "redundant"] | None] | None = None,
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


@pytest.mark.parametrize("state", TERMINAL_STATES)
@pytest.mark.parametrize("dry_run", [False, True])
def test_land_cli_reports_initial_terminal_result(
    tmp_path: Path,
    *,
    state: str,
    dry_run: bool,
) -> None:
    _store, task_id, _unit_id = _task_with_unit(tmp_path, state=state)

    args = ["land", task_id, "--project", str(tmp_path)]
    if dry_run:
        args.append("--dry-run")
    result = invoke_gza(*args, cwd=tmp_path)

    assert result.returncode == 0
    if dry_run:
        assert result.stdout.startswith("Dry run: ")
    if state == "merged":
        assert "already merged" in result.stdout
    else:
        assert f"terminal no-work state {state}" in result.stdout
        assert "already merged" not in result.stdout
