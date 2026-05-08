from __future__ import annotations

import json
from pathlib import Path

from gza.cli.log import _LiveLogPrinter
from gza.cli.tv import _scan_log
from gza.providers.log_renderers import get_log_renderer

from tests.cli.conftest import make_store, setup_config
from tests.helpers.cli import run_gza

FIXTURES = Path(__file__).parent / "fixtures" / "log_renderer"


def _load_entries(name: str) -> list[dict]:
    path = FIXTURES / name
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def test_claude_renderer_renders_known_events_and_counts_suppression() -> None:
    renderer = get_log_renderer("claude")
    lines: list[str] = []
    starts = 0
    for entry in _load_entries("claude.jsonl"):
        rendered = renderer.handle_log(entry, live=False)
        lines.extend(rendered.log_lines)
        starts += int(rendered.starts_step)

    joined = "\n".join(lines)
    assert starts == 2
    assert renderer.stats.step_count == 2
    assert renderer.stats.input_tokens == 15
    assert renderer.stats.output_tokens == 5
    assert renderer.suppressed_count == 2
    assert "Session initialized" in joined
    assert "Investigating failure." in joined
    assert "FAILED tests/test_cli.py::test_case - AssertionError" in joined
    assert "[event:mystery]" in joined


def test_codex_renderer_accumulates_tv_tokens_from_usage_fixture() -> None:
    fixture = FIXTURES / "codex.jsonl"
    lines, stats = _scan_log(fixture, 100, "codex")
    joined = "\n".join(lines)

    assert stats.step_count == 1
    assert stats.input_tokens == 24
    assert stats.output_tokens == 8
    assert "Session started (thread: thread_123)" in joined
    assert "I found the root cause." in joined
    assert "event:weird.codex" in joined


def test_codex_renderer_counts_distinct_usage_events_with_identical_token_values() -> None:
    renderer = get_log_renderer("codex", configured_model="gpt-5.3-codex")

    renderer.handle_log(
        {
            "type": "turn.completed",
            "event_id": "evt-1",
            "usage": {"input_tokens": 20, "output_tokens": 8, "cached_input_tokens": 4},
        },
        live=False,
    )
    renderer.handle_log(
        {
            "type": "turn.error",
            "event_id": "evt-2",
            "usage": {"input_tokens": 20, "output_tokens": 8, "cached_input_tokens": 4},
        },
        live=False,
    )

    assert renderer.stats.input_tokens == 48
    assert renderer.stats.output_tokens == 16


def test_gemini_renderer_suppresses_routine_events_and_keeps_unknowns_visible() -> None:
    renderer = get_log_renderer("gemini")
    tv_lines: list[str] = []
    for entry in _load_entries("gemini.jsonl"):
        tv_lines.extend(renderer.handle_tv(entry).tv_lines)

    joined = "\n".join(tv_lines)
    assert renderer.stats.step_count == 1
    assert renderer.stats.input_tokens == 30
    assert renderer.stats.output_tokens == 12
    assert renderer.suppressed_count == 2
    assert "Planning changes now." in joined
    assert "event:oddity message=unknown event" in joined


def test_unknown_event_verbose_mode_expands_json_payload() -> None:
    renderer = get_log_renderer("codex", verbose=True)
    rendered = renderer.handle_log(
        {"type": "mystery", "message": "hello", "nested": {"x": 1}},
        live=False,
    )

    assert rendered.log_lines[0].startswith("[event:mystery]")
    assert any('"nested": {' in line for line in rendered.log_lines[1:])


def test_live_log_printer_uses_provider_renderer_step_count() -> None:
    printer = _LiveLogPrinter(live=False, provider="codex")
    seen = False
    for entry in _load_entries("codex.jsonl"):
        seen = printer.process(entry) or seen

    assert seen is True
    assert printer.renderer.stats.step_count == 1
    assert printer.renderer.suppressed_count == 4


def test_gza_log_prints_suppressed_footer_and_verbose_unknown_payload(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Renderer footer test")
    task.status = "completed"
    task.provider = "claude"
    task.provider_is_explicit = True
    task.log_file = ".gza/logs/claude.log"
    store.update(task)

    log_path = tmp_path / ".gza" / "logs" / "claude.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text((FIXTURES / "claude.jsonl").read_text() + "\n")

    result = run_gza("log", str(task.id), "--verbose", "--project", str(tmp_path))

    assert result.returncode == 0
    assert "routine events suppressed" in result.stdout
    assert '"alpha": 1' in result.stdout
