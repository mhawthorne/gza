"""Fixture-based guardrails for provider-owned step-boundary detection."""
from __future__ import annotations

import json
from pathlib import Path

from gza.cli.log import _LiveLogPrinter
from gza.cli.tv import _scan_log
from gza.providers.log_renderers import get_log_renderer

FIXTURES = Path(__file__).parent / "fixtures" / "logs"


def _iter_fixture_entries(path: Path) -> list[dict]:
    entries: list[dict] = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("---"):
            continue
        entries.append(json.loads(line))
    return entries


def _count_steps_via_renderer(path: Path) -> int:
    renderer = get_log_renderer(None)
    for entry in _iter_fixture_entries(path):
        renderer.handle_log(entry, live=False)
    return renderer.stats.step_count


def _count_steps_via_log_printer(path: Path) -> int:
    printer = _LiveLogPrinter(live=False, provider=None)
    for entry in _iter_fixture_entries(path):
        printer.process(entry)
    return printer.renderer.stats.step_count


def _count_steps_via_tv_scan(path: Path) -> int:
    _lines, stats = _scan_log(path, 1000, None)
    return stats.step_count


def test_codex_multi_step_fixture_has_expected_step_count():
    # 3 non-empty agent_message items → 3 steps. turn.started is NOT a step.
    path = FIXTURES / "codex_multi_step.jsonl"
    assert _count_steps_via_renderer(path) == 3
    assert _count_steps_via_log_printer(path) == 3
    assert _count_steps_via_tv_scan(path) == 3


def test_claude_multi_step_fixture_has_expected_step_count():
    # 3 unique assistant msg_ids → 3 steps (msg_1 repeats across deltas).
    path = FIXTURES / "claude_multi_step.jsonl"
    assert _count_steps_via_renderer(path) == 3
    assert _count_steps_via_log_printer(path) == 3
    assert _count_steps_via_tv_scan(path) == 3


def test_legacy_codex_fixture_single_step():
    # Single agent_message → 1 step, regardless of the lone turn.started.
    path = FIXTURES / "legacy_turn_only_codex.jsonl"
    assert _count_steps_via_renderer(path) == 1
    assert _count_steps_via_log_printer(path) == 1
    assert _count_steps_via_tv_scan(path) == 1


def test_renderer_log_and_tv_call_sites_agree_on_fixtures():
    # Guard against drift across renderer-driven call sites.
    for fx in FIXTURES.glob("*.jsonl"):
        a = _count_steps_via_renderer(fx)
        b = _count_steps_via_log_printer(fx)
        c = _count_steps_via_tv_scan(fx)
        assert a == b == c, f"{fx.name}: renderer={a}, log={b}, tv={c}"


def test_tv_scan_ignores_non_object_json_lines(tmp_path: Path):
    path = tmp_path / "mixed.jsonl"
    path.write_text(
        '\n'.join(
            [
                '"plain string line"',
                '{"type":"item.completed","item":{"type":"agent_message","text":"first step"}}',
                "123",
                '{"type":"assistant","message":{"id":"msg_1","content":"hello"}}',
            ]
        )
        + "\n"
    )

    lines, stats = _scan_log(path, 1000, None)

    assert stats.step_count == 1
    assert "plain string line" in lines
