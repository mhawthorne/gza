"""Tests for the unattended stuck-task reviver script."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest


def _load_revive_stuck() -> ModuleType:
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "revive_stuck.py"
    spec = importlib.util.spec_from_file_location("revive_stuck_for_tests", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_classified_rows_reads_incomplete_json_envelope_rows(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    revive_stuck = _load_revive_stuck()
    payload = {
        "summary": {"deferred_blockers_outstanding": 1},
        "rows": [
            {
                "id": "gza-1",
                "status": "failed",
                "tags": ["system"],
                "next_action": "resume",
                "next_action_reason": "MAX_TURNS",
            },
            {
                "id": "gza-2",
                "status": "dropped",
                "next_action": "resume",
            },
        ],
    }

    monkeypatch.setattr(
        revive_stuck,
        "_gza",
        lambda _args, _project: (0, json.dumps(payload), ""),
    )

    rows, total = revive_stuck._classified_rows(tmp_path, {"system"})

    assert rows == [("gza-1", ["advance", "gza-1", "-y"], "advance (resume)", "MAX_TURNS")]
    assert total == 1


def test_classified_rows_allows_empty_rows_with_deferred_summary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    revive_stuck = _load_revive_stuck()
    payload = {"summary": {"deferred_blockers_outstanding": 3}, "rows": []}
    monkeypatch.setattr(
        revive_stuck,
        "_gza",
        lambda _args, _project: (0, json.dumps(payload), ""),
    )

    rows, total = revive_stuck._classified_rows(tmp_path, None)

    assert rows == []
    assert total == 0


def test_classified_rows_rejects_malformed_incomplete_json_envelope(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    revive_stuck = _load_revive_stuck()
    monkeypatch.setattr(
        revive_stuck,
        "_gza",
        lambda _args, _project: (0, json.dumps({"summary": {}, "rows": {}}), ""),
    )

    rows, total = revive_stuck._classified_rows(tmp_path, None)

    assert rows == []
    assert total == 0
    assert "expected gza incomplete --json envelope to contain a rows list" in capsys.readouterr().err
