"""Fixture-based guardrails for step-boundary detection.

Step counting has regressed multiple times (codex's ``turn.started`` fires
only once per session; only ``agent_message`` items mark logical steps).
These tests feed real-shape JSONL fixtures through every site that counts
steps and assert the totals match. If a provider changes event shape, the
fixture has to be updated and the change is visible in review.
"""
from __future__ import annotations

import json
from pathlib import Path

from gza.cli.log import _LiveLogPrinter
from gza.cli.tv import _scan_log
from gza.log_events import is_new_step

FIXTURES = Path(__file__).parent / "fixtures" / "logs"


def _count_steps_via_predicate(path: Path) -> int:
    seen: set[str] = set()
    n = 0
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("---"):
            continue
        entry = json.loads(line)
        if is_new_step(entry, seen):
            n += 1
    return n


def _count_steps_via_log_printer(path: Path) -> int:
    printer = _LiveLogPrinter(live=False)
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("---"):
            continue
        printer.process(json.loads(line))
    return printer._step_count


def _count_steps_via_tv_scan(path: Path) -> int:
    _lines, stats = _scan_log(path, 1000)
    return stats.step_count


def test_codex_multi_step_fixture_has_expected_step_count():
    # 3 non-empty agent_message items → 3 steps. turn.started is NOT a step.
    path = FIXTURES / "codex_multi_step.jsonl"
    assert _count_steps_via_predicate(path) == 3
    assert _count_steps_via_log_printer(path) == 3
    assert _count_steps_via_tv_scan(path) == 3


def test_claude_multi_step_fixture_has_expected_step_count():
    # 3 unique assistant msg_ids → 3 steps (msg_1 repeats across deltas).
    path = FIXTURES / "claude_multi_step.jsonl"
    assert _count_steps_via_predicate(path) == 3
    assert _count_steps_via_log_printer(path) == 3
    assert _count_steps_via_tv_scan(path) == 3


def test_legacy_codex_fixture_single_step():
    # Single agent_message → 1 step, regardless of the lone turn.started.
    path = FIXTURES / "legacy_turn_only_codex.jsonl"
    assert _count_steps_via_predicate(path) == 1
    assert _count_steps_via_log_printer(path) == 1
    assert _count_steps_via_tv_scan(path) == 1


def test_all_three_call_sites_agree_on_fixtures():
    # Guard against drift: any fixture must produce the same count everywhere.
    for fx in FIXTURES.glob("*.jsonl"):
        a = _count_steps_via_predicate(fx)
        b = _count_steps_via_log_printer(fx)
        c = _count_steps_via_tv_scan(fx)
        assert a == b == c, f"{fx.name}: predicate={a}, log={b}, tv={c}"
