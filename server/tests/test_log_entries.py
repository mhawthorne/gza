from gza_server.log_entries import (
    ERROR,
    OPS,
    RESULT,
    TEXT,
    THINKING,
    TOOL_RESULT,
    TOOL_USE,
    UNKNOWN,
    events_for,
)


def test_assistant_text_and_thinking_become_separate_events() -> None:
    record = {
        "type": "assistant",
        "message": {
            "content": [
                {"type": "thinking", "thinking": "weighing options"},
                {"type": "text", "text": "Here is the plan."},
            ]
        },
    }

    events = events_for(0, record, "")

    assert [event.kind for event in events] == [THINKING, TEXT]
    assert events[0].body == "weighing options"
    assert events[1].body == "Here is the plan."
    assert events[1].role == "assistant"


def test_bash_tool_use_keeps_the_full_command_in_the_body() -> None:
    record = {
        "type": "assistant",
        "message": {
            "content": [
                {
                    "type": "tool_use",
                    "name": "Bash",
                    "input": {"command": '/bin/bash -lc "pytest tests/ -q"'},
                }
            ]
        },
    }

    (event,) = events_for(12, record, "")

    assert event.kind == TOOL_USE
    assert event.tool_name == "Bash"
    assert event.body == "pytest tests/ -q"
    assert event.offset == 12


def test_failed_tool_result_is_flagged_as_an_error() -> None:
    record = {
        "type": "user",
        "message": {
            "content": [
                {"type": "tool_result", "is_error": True, "content": [{"type": "text", "text": "boom"}]}
            ]
        },
    }

    (event,) = events_for(0, record, "")

    assert event.kind == TOOL_RESULT
    assert event.is_error is True
    assert event.body == "boom"


def test_result_event_summarizes_steps_duration_and_cost() -> None:
    record = {"type": "result", "num_steps": 7, "duration_ms": 2500, "total_cost_usd": 0.1234}

    (event,) = events_for(0, record, "")

    assert event.kind == RESULT
    assert "7 steps" in event.body
    assert "2.5s" in event.body
    assert "$0.1234" in event.body


def test_gza_ops_entries_carry_their_subtype_and_error_flag() -> None:
    (info,) = events_for(0, {"type": "gza", "subtype": "phase", "message": "verify"}, "")
    (failure,) = events_for(0, {"type": "gza", "subtype": "error", "message": "verify failed"}, "")

    assert info.kind == OPS and info.title == "phase" and info.is_error is False
    assert failure.is_error is True


def test_provider_error_payload_is_unwrapped_without_terminal_prefixes() -> None:
    (event,) = events_for(0, {"type": "error", "message": "rate limited"}, "")

    assert event.kind == ERROR
    assert event.is_error is True
    assert "[error]" not in event.body
    assert "rate limited" in event.body


def test_codex_and_gemini_dialects_map_to_the_same_event_kinds() -> None:
    (codex,) = events_for(
        0, {"type": "item.completed", "item": {"type": "agent_message", "text": "done"}}, ""
    )
    (gemini,) = events_for(0, {"type": "message", "role": "assistant", "content": "done"}, "")

    assert codex.kind == gemini.kind == TEXT
    assert codex.body == gemini.body == "done"


def test_unparsed_and_unrecognized_records_are_shown_rather_than_dropped() -> None:
    (unparsed,) = events_for(0, None, "garbage line")
    (unrecognized,) = events_for(0, {"type": "brand_new_event"}, '{"type": "brand_new_event"}')

    assert unparsed.kind == UNKNOWN and unparsed.body == "garbage line"
    assert unrecognized.kind == UNKNOWN and unrecognized.title == "brand_new_event"
