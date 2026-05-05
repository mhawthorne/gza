"""Tests for shared failure-reason resolution and ownership."""

import ast
from pathlib import Path

from gza.db import TaskStats
from gza.failure_reasons import resolve_failure_reason


def test_resolve_failure_reason_uses_reported_turns_when_computed_is_below_limit() -> None:
    reason = resolve_failure_reason(
        error_type="max_turns",
        exit_code=0,
        log_file=None,
        stats=TaskStats(num_turns_computed=49, num_turns_reported=60),
        turn_limit=50,
    )

    assert reason == "MAX_TURNS"


def test_resolve_failure_reason_uses_reported_steps_when_computed_is_below_limit() -> None:
    reason = resolve_failure_reason(
        error_type="max_steps",
        exit_code=0,
        log_file=None,
        stats=TaskStats(num_steps_computed=49, num_steps_reported=60),
        step_limit=50,
    )

    assert reason == "MAX_STEPS"


def test_production_failure_reason_persistence_uses_shared_helper() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source_root = repo_root / "src" / "gza"
    violations: list[str] = []

    for path in source_root.rglob("*.py"):
        if path.name == "failure_reasons.py":
            continue
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not isinstance(func, ast.Attribute) or func.attr != "mark_failed":
                continue
            if any(keyword.arg == "failure_reason" for keyword in node.keywords):
                violations.append(f"{path.relative_to(repo_root)}:{node.lineno}")

    assert violations == []
