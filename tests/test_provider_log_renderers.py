from __future__ import annotations

import json
from pathlib import Path

from gza.cli.log import _LiveLogPrinter
from gza.cli.tv import _scan_log
from gza.providers.log_renderers import get_log_renderer
from gza.providers.log_rendering import RenderStats

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
    assert starts == 1
    assert renderer.stats.step_count == 1
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
    assert '"type": "weird.codex"' in joined


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
    assert '"type": "oddity"' in joined
    assert '"message": "unknown event"' in joined


def test_gemini_renderer_keeps_user_content_visible_and_only_suppresses_empty_boundaries() -> None:
    renderer = get_log_renderer("gemini")

    visible_entry = {"type": "message", "role": "user", "content": "important prompt/context"}
    log_rendered = renderer.handle_log(visible_entry, live=False)
    tv_rendered = renderer.handle_tv(visible_entry)

    assert log_rendered.log_lines == ["user: important prompt/context"]
    assert tv_rendered.tv_lines == ["user: important prompt/context"]
    assert renderer.suppressed_count == 0

    renderer.handle_log({"type": "message", "role": "user", "content": ""}, live=False)

    assert renderer.suppressed_count == 1


def test_codex_renderer_shows_error_message_in_tv_mode() -> None:
    renderer = get_log_renderer("codex")
    entry = {"type": "error", "message": "FAILED tests/test_cli.py::test_case - AssertionError"}

    tv_rendered = renderer.handle_tv(entry)

    assert any("FAILED tests/test_cli.py::test_case - AssertionError" in line for line in tv_rendered.tv_lines)
    assert all("event:error" not in line for line in tv_rendered.tv_lines)


def test_claude_renderer_shows_error_message_in_tv_mode() -> None:
    renderer = get_log_renderer("claude")
    entry = {"type": "error", "message": '{"error":{"message":"provider crashed"}}'}

    tv_rendered = renderer.handle_tv(entry)

    assert any("provider crashed" in line for line in tv_rendered.tv_lines)
    assert all("event:error" not in line for line in tv_rendered.tv_lines)


def test_gemini_renderer_shows_error_message_in_tv_mode() -> None:
    renderer = get_log_renderer("gemini")
    entry = {"type": "error", "message": "permission denied"}

    tv_rendered = renderer.handle_tv(entry)

    assert any("permission denied" in line for line in tv_rendered.tv_lines)
    assert all("event:error" not in line for line in tv_rendered.tv_lines)


def test_claude_renderer_falls_back_for_unknown_assistant_blocks() -> None:
    renderer = get_log_renderer("claude")
    entry = {
        "type": "assistant",
        "message": {
            "id": "msg_unknown",
            "content": [{"type": "new_block", "payload": "visible"}],
        },
    }

    log_rendered = renderer.handle_log(entry, live=False)
    tv_rendered = renderer.handle_tv(entry)

    assert log_rendered.starts_step is True
    assert tv_rendered.starts_step is False
    assert log_rendered.log_lines[0].startswith("[event:assistant]")
    assert "new_block" in log_rendered.log_lines[0]
    assert tv_rendered.tv_lines[0].startswith("event:assistant")
    assert tv_rendered.tv_lines[1] == "{"
    assert any('"new_block"' in line for line in tv_rendered.tv_lines[1:])
    assert tv_rendered.tv_lines[-1].startswith("... (+")
    assert renderer.suppressed_count == 0


def test_claude_renderer_keeps_user_text_content_visible() -> None:
    renderer = get_log_renderer("claude")
    entry = {
        "type": "user",
        "message": {"content": "please continue"},
    }

    log_rendered = renderer.handle_log(entry, live=False)
    tv_rendered = renderer.handle_tv(entry)

    assert log_rendered.log_lines == ["user: please continue"]
    assert tv_rendered.tv_lines == ["user: please continue"]
    assert renderer.suppressed_count == 0


def test_claude_renderer_falls_back_for_unknown_user_blocks() -> None:
    renderer = get_log_renderer("claude")
    entry = {
        "type": "user",
        "message": {
            "content": [{"type": "new_block", "payload": "visible"}],
        },
    }

    log_rendered = renderer.handle_log(entry, live=False)
    tv_rendered = renderer.handle_tv(entry)

    assert log_rendered.log_lines[0].startswith("[event:user]")
    assert "new_block" in log_rendered.log_lines[0]
    assert tv_rendered.tv_lines[0] == "{"
    assert any('"new_block"' in line for line in tv_rendered.tv_lines)
    assert tv_rendered.tv_lines[-1].startswith("... (+")
    assert renderer.suppressed_count == 0


def test_claude_renderer_keeps_unknown_blocks_when_mixed_with_text() -> None:
    renderer = get_log_renderer("claude")
    entry = {
        "type": "assistant",
        "message": {
            "id": "msg_mixed",
            "content": [
                {"type": "text", "text": "Known text"},
                {"type": "new_block", "payload": "SHOULD_SHOW"},
            ],
        },
    }

    log_rendered = renderer.handle_log(entry, live=False)
    tv_rendered = renderer.handle_tv(entry)

    assert log_rendered.starts_step is True
    assert log_rendered.log_lines[0] == "Known text"
    assert any(line.startswith("[event:assistant]") for line in log_rendered.log_lines[1:])
    assert any("new_block" in line for line in log_rendered.log_lines[1:])
    assert tv_rendered.tv_lines[0] == "Known text"
    assert any(line == "event:assistant block=new_block" for line in tv_rendered.tv_lines[1:])
    assert tv_rendered.tv_lines[2] == "{"
    assert tv_rendered.tv_lines[-1].startswith("... (+")


def test_claude_renderer_keeps_non_init_system_errors_visible() -> None:
    renderer = get_log_renderer("claude")
    entry = {
        "type": "system",
        "subtype": "session",
        "message": "error from provider metadata sync",
        "error": {"message": "visible failure"},
    }

    log_rendered = renderer.handle_log(entry, live=False)
    tv_rendered = renderer.handle_tv(entry)

    assert "[event:system]" in log_rendered.log_lines[0]
    assert "subtype=session" in log_rendered.log_lines[0]
    assert "error={\"message\": \"visible failure\"}" in log_rendered.log_lines[0]
    assert tv_rendered.tv_lines[0] == "{"
    assert any('"type": "system"' in line for line in tv_rendered.tv_lines)
    assert any('"message": "error from provider metadata sync"' in line for line in tv_rendered.tv_lines)
    assert renderer.suppressed_count == 0


def test_claude_renderer_suppresses_non_init_system_session_events() -> None:
    renderer = get_log_renderer("claude")
    entry = {
        "type": "system",
        "subtype": "session",
        "session_id": "sess_123",
        "message": "routine session metadata",
    }

    log_rendered = renderer.handle_log(entry, live=False)
    tv_rendered = renderer.handle_tv(entry)

    assert log_rendered.log_lines == []
    assert tv_rendered.tv_lines == []
    assert renderer.suppressed_count == 2


def test_claude_renderer_routine_system_metadata_does_not_clear_provider_model() -> None:
    renderer = get_log_renderer("claude")

    renderer.handle_log(
        {"type": "gza", "subtype": "info", "message": "Provider: Claude, Model: claude-opus-4-6"},
        live=False,
    )
    init_rendered = renderer.handle_log({"type": "system", "subtype": "init", "model": "claude-opus-4-6"}, live=False)
    renderer.handle_log(
        {"type": "system", "subtype": "session", "session_id": "sess_123", "message": "routine metadata"},
        live=False,
    )
    rendered = renderer.handle_log({"type": "gza", "subtype": "info", "message": "Still running"}, live=False)

    assert renderer._provider_model == "claude-opus-4-6"
    assert any(
        "Model parity: configured=claude-opus-4-6; provider=claude-opus-4-6" in line
        for line in init_rendered.log_lines
    )
    assert not any("Model parity:" in line for line in rendered.log_lines)
    assert not any("provider=(not echoed by provider)" in line for line in rendered.log_lines)
    assert not any("Warning: provider did not echo model" in line for line in rendered.log_lines)


def test_unknown_event_log_mode_always_expands_json_payload() -> None:
    renderer = get_log_renderer("codex", verbose=False)
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


def test_codex_renderer_falls_back_for_empty_agent_message() -> None:
    log_renderer = get_log_renderer("codex")
    tv_renderer = get_log_renderer("codex")
    entry = {"type": "item.completed", "item": {"type": "agent_message", "id": "a1", "text": ""}}

    log_rendered = log_renderer.handle_log(entry, live=False)
    tv_rendered = tv_renderer.handle_tv(entry)

    assert log_rendered.starts_step is False
    assert log_rendered.log_lines[0].startswith("[event:item.completed]")
    assert "item.type=agent_message" in log_rendered.log_lines[0]
    assert tv_rendered.starts_step is False
    assert tv_rendered.tv_lines[0] == "{"
    assert any('"type": "item.completed"' in line for line in tv_rendered.tv_lines)
    assert any('"item": {' in line for line in tv_rendered.tv_lines)
    assert log_renderer.stats.step_count == 0
    assert tv_renderer.stats.step_count == 0
    assert log_renderer.suppressed_count == 0
    assert tv_renderer.suppressed_count == 0


def test_codex_renderer_falls_back_for_command_execution_without_command_or_output() -> None:
    log_renderer = get_log_renderer("codex")
    tv_renderer = get_log_renderer("codex")
    entry = {
        "type": "item.completed",
        "item": {"type": "command_execution", "id": "cmd1", "status": "failed", "error": "boom"},
    }

    log_rendered = log_renderer.handle_log(entry, live=False)
    tv_rendered = tv_renderer.handle_tv(entry)

    assert log_rendered.log_lines[0].startswith("[event:item.completed]")
    assert "item.type=command_execution" in log_rendered.log_lines[0]
    assert "item.id=cmd1" in log_rendered.log_lines[0]
    assert tv_rendered.tv_lines[0] == "{"
    assert any('"command_execution"' in line for line in tv_rendered.tv_lines)
    assert tv_rendered.tv_lines[-1].startswith("... (+")
    assert log_renderer.suppressed_count == 0
    assert tv_renderer.suppressed_count == 0


def test_unknown_event_tv_mode_renders_pretty_json() -> None:
    renderer = get_log_renderer("codex")

    rendered = renderer.handle_tv({"type": "mystery", "foo": "bar"})

    assert rendered.tv_lines == [
        "{",
        '  "foo": "bar",',
        '  "type": "mystery"',
        "}",
    ]


def test_unknown_event_tv_mode_truncates_long_json_payload() -> None:
    renderer = get_log_renderer("codex")
    entry = {
        "type": "mystery",
        "outer": {
            "alpha": 1,
            "beta": 2,
            "gamma": 3,
            "delta": 4,
            "epsilon": 5,
            "zeta": 6,
        },
    }

    rendered = renderer.handle_tv(entry)

    assert rendered.tv_lines == [
        "{",
        '  "outer": {',
        '    "alpha": 1,',
        '    "beta": 2,',
        '    "delta": 4,',
        '    "epsilon": 5,',
        '    "gamma": 3,',
        "... (+4 lines)",
    ]


def test_live_log_printer_does_not_emit_blank_header_for_suppressed_empty_claude_assistant() -> None:
    printer = _LiveLogPrinter(live=False, provider="claude")
    empty_entry = {
        "type": "assistant",
        "message": {
            "id": "msg_empty",
            "content": [],
            "usage": {
                "input_tokens": 10,
                "cache_creation_input_tokens": 2,
                "cache_read_input_tokens": 3,
                "output_tokens": 5,
            },
        },
    }

    with printer._console.capture() as capture:
        seen = printer.process(empty_entry)

    assert capture.get() == ""
    assert seen is False
    assert printer.renderer.stats.step_count == 0
    assert printer.renderer.stats.input_tokens == 15
    assert printer.renderer.stats.output_tokens == 5
    assert printer.renderer.suppressed_count == 1

    visible_entry = {
        "type": "assistant",
        "message": {"id": "msg_visible", "content": [{"type": "text", "text": "Visible step"}]},
    }
    with printer._console.capture() as capture:
        seen = printer.process(visible_entry)

    assert "| Step 1 |" in capture.get()
    assert seen is True
    assert printer.renderer.stats.step_count == 1


def test_claude_renderer_counts_usage_for_suppressed_empty_assistant_on_log_and_tv() -> None:
    entry = {
        "type": "assistant",
        "message": {
            "id": "msg_usage_only",
            "content": [],
            "usage": {
                "input_tokens": 10,
                "cache_creation_input_tokens": 2,
                "cache_read_input_tokens": 3,
                "output_tokens": 5,
            },
        },
    }

    log_renderer = get_log_renderer("claude", configured_model="claude-sonnet-4")
    tv_renderer = get_log_renderer("claude", configured_model="claude-sonnet-4")

    log_rendered = log_renderer.handle_log(entry, live=False)
    tv_rendered = tv_renderer.handle_tv(entry)

    assert log_rendered.log_lines == []
    assert log_rendered.starts_step is False
    assert log_renderer.stats.step_count == 0
    assert log_renderer.stats.input_tokens == 15
    assert log_renderer.stats.output_tokens == 5
    assert log_renderer.suppressed_count == 1

    assert tv_rendered.tv_lines == []
    assert tv_rendered.starts_step is False
    assert tv_renderer.stats.step_count == 0
    assert tv_renderer.stats.input_tokens == 15
    assert tv_renderer.stats.output_tokens == 5
    assert tv_renderer.suppressed_count == 1


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


def test_gza_log_unknown_provider_returns_clear_cli_error(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Unknown provider log task")
    task.status = "completed"
    task.provider = "mistral"
    task.provider_is_explicit = True
    task.log_file = ".gza/logs/unknown-provider.log"
    store.update(task)

    log_path = tmp_path / ".gza" / "logs" / "unknown-provider.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text('{"type":"assistant","message":{"id":"msg_1","content":"hello"}}\n')

    result = run_gza("log", str(task.id), "--project", str(tmp_path))

    assert result.returncode == 1
    assert "Error: unknown provider for log rendering: mistral" in result.stdout
    assert "Traceback" not in result.stdout
    assert "Traceback" not in result.stderr


def test_tv_scan_unknown_provider_returns_visible_error_line(tmp_path: Path) -> None:
    log_path = tmp_path / "unknown-provider.jsonl"
    log_path.write_text('{"type":"assistant","message":{"id":"msg_1","content":"hello"}}\n')

    lines, stats = _scan_log(log_path, 10, "mistral")

    assert lines == ["Error: unknown provider for log rendering: mistral"]
    assert stats == RenderStats()
