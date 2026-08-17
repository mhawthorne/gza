from __future__ import annotations

import json
from pathlib import Path

import pytest

from gza.config import Config
from gza.db import SqliteTaskStore
from gza.verify_fix_outcome import (
    effective_verify_fix_completion_outcome,
    inspect_verify_fix_completion_outcome,
    inspect_legacy_review_scope_completion_outcome,
    persist_verify_fix_completion_outcome,
)


def _make_store(tmp_path: Path) -> SqliteTaskStore:
    (tmp_path / "gza.yaml").write_text("project_name: test-project\n")
    config = Config.load(tmp_path)
    db_path = tmp_path / ".gza" / "gza.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return SqliteTaskStore(db_path, prefix=config.project_prefix)


def test_verify_fix_completion_outcome_preserves_inherited_review_scope(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    inherited_scope = "Review only the timeout recovery persistence slice.\nKeep query paths read-only."
    impl = store.add("Implement timeout recovery", task_type="implement", review_scope=inherited_scope)
    assert impl.id is not None
    verify_fix = store.add(
        "Fix timeout recovery verify",
        task_type="verify_fix",
        based_on=impl.id,
        same_branch=True,
        review_scope=inherited_scope,
    )
    assert verify_fix.id is not None

    store.mark_completed(
        verify_fix,
        branch="feature/timeout-recovery",
        has_commits=False,
        changed_diff=False,
        head_sha="head-1",
        base_sha="base-1",
    )
    completed = store.get(verify_fix.id)
    assert completed is not None
    assert completed.review_scope == inherited_scope
    completed_outcome = effective_verify_fix_completion_outcome(completed)
    assert completed_outcome is not None
    assert completed_outcome.no_source_changes is True
    assert completed_outcome.completion_head_sha == "head-1"
    assert completed_outcome.recovery_rerun_attempted is False

    persist_verify_fix_completion_outcome(
        store,
        completed,
        no_source_changes=True,
        completion_head_sha="head-1",
        recovery_rerun_attempted=True,
    )
    refreshed = store.get(verify_fix.id)
    assert refreshed is not None
    assert refreshed.review_scope == inherited_scope
    refreshed_outcome = effective_verify_fix_completion_outcome(refreshed)
    assert refreshed_outcome is not None
    assert refreshed_outcome.no_source_changes is True
    assert refreshed_outcome.completion_head_sha == "head-1"
    assert refreshed_outcome.recovery_rerun_attempted is True
    assert refreshed.verify_fix_completion_outcome_json is not None


@pytest.mark.parametrize(
    "raw_outcome",
    [
        "{not-json",
        '{"kind":"wrong","schema_version":1,"no_source_changes":true}',
        '{"kind":"verify_fix_completion_outcome","schema_version":999,"no_source_changes":true}',
    ],
)
def test_invalid_canonical_verify_fix_completion_outcome_does_not_use_legacy_fallback(
    tmp_path: Path,
    raw_outcome: str,
) -> None:
    store = _make_store(tmp_path)
    verify_fix = store.add("Fix timeout recovery verify", task_type="verify_fix")
    assert verify_fix.id is not None
    verify_fix.changed_diff = False
    verify_fix.review_verify_head_sha = "legacy-head"
    verify_fix.verify_fix_completion_outcome_json = raw_outcome
    store.update(verify_fix)

    refreshed = store.get(verify_fix.id)
    assert refreshed is not None
    inspection = inspect_verify_fix_completion_outcome(refreshed)
    assert inspection.state == "invalid"
    assert inspection.invalid_reason
    assert effective_verify_fix_completion_outcome(refreshed) is None


def _completion_outcome_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "kind": "verify_fix_completion_outcome",
        "schema_version": 1,
        "no_source_changes": True,
        "completion_head_sha": "head-1",
        "recovery_rerun_attempted": False,
    }
    payload.update(overrides)
    return payload


@pytest.mark.parametrize("field_name", ["no_source_changes", "completion_head_sha", "recovery_rerun_attempted"])
@pytest.mark.parametrize("storage", ["canonical", "legacy_review_scope"])
def test_structured_verify_fix_completion_outcome_rejects_missing_required_fields(
    tmp_path: Path,
    storage: str,
    field_name: str,
) -> None:
    store = _make_store(tmp_path)
    verify_fix = store.add("Fix timeout recovery verify", task_type="verify_fix")
    assert verify_fix.id is not None
    payload = _completion_outcome_payload()
    del payload[field_name]
    if storage == "canonical":
        verify_fix.verify_fix_completion_outcome_json = json.dumps(payload)
    else:
        verify_fix.review_scope = json.dumps(payload)
        verify_fix.changed_diff = False
        verify_fix.review_verify_head_sha = "legacy-head"
    store.update(verify_fix)

    refreshed = store.get(verify_fix.id)
    assert refreshed is not None
    inspection = (
        inspect_verify_fix_completion_outcome(refreshed)
        if storage == "canonical"
        else inspect_legacy_review_scope_completion_outcome(refreshed.review_scope)
    )
    assert inspection.state == "invalid"
    assert effective_verify_fix_completion_outcome(refreshed) is None


@pytest.mark.parametrize(
    "field_name,invalid_value",
    [
        ("no_source_changes", 1),
        ("no_source_changes", "true"),
        ("no_source_changes", None),
        ("recovery_rerun_attempted", 0),
        ("recovery_rerun_attempted", "false"),
        ("recovery_rerun_attempted", None),
        ("completion_head_sha", None),
        ("completion_head_sha", ""),
        ("completion_head_sha", 123),
    ],
)
@pytest.mark.parametrize("storage", ["canonical", "legacy_review_scope"])
def test_structured_verify_fix_completion_outcome_rejects_invalid_required_field_types(
    tmp_path: Path,
    storage: str,
    field_name: str,
    invalid_value: object,
) -> None:
    store = _make_store(tmp_path)
    verify_fix = store.add("Fix timeout recovery verify", task_type="verify_fix")
    assert verify_fix.id is not None
    payload = _completion_outcome_payload(**{field_name: invalid_value})
    if storage == "canonical":
        verify_fix.verify_fix_completion_outcome_json = json.dumps(payload)
    else:
        verify_fix.review_scope = json.dumps(payload)
        verify_fix.changed_diff = False
        verify_fix.review_verify_head_sha = "legacy-head"
    store.update(verify_fix)

    refreshed = store.get(verify_fix.id)
    assert refreshed is not None
    inspection = (
        inspect_verify_fix_completion_outcome(refreshed)
        if storage == "canonical"
        else inspect_legacy_review_scope_completion_outcome(refreshed.review_scope)
    )
    assert inspection.state == "invalid"
    assert effective_verify_fix_completion_outcome(refreshed) is None
