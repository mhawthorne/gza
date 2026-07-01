from __future__ import annotations

import json
from pathlib import Path

from gza.providers.claude import (
    CLAUDE_ASSISTANT_BLOCK_REGISTRY,
    CLAUDE_EVENT_REGISTRY,
    CLAUDE_LIVE_ASSISTANT_BLOCK_HANDLERS,
    CLAUDE_LIVE_EVENT_HANDLERS,
    CLAUDE_LIVE_KNOWN_ASSISTANT_BLOCK_TYPES,
    CLAUDE_LIVE_KNOWN_EVENT_TYPES,
    CLAUDE_LIVE_KNOWN_USER_BLOCK_TYPES,
    CLAUDE_LIVE_USER_BLOCK_HANDLERS,
    CLAUDE_RENDER_ASSISTANT_BLOCK_HANDLERS,
    CLAUDE_RENDER_KNOWN_ASSISTANT_BLOCK_TYPES,
    CLAUDE_RENDER_KNOWN_EVENT_TYPES,
    CLAUDE_RENDER_KNOWN_USER_BLOCK_TYPES,
    CLAUDE_RENDER_USER_BLOCK_HANDLERS,
    CLAUDE_USER_BLOCK_REGISTRY,
    ClaudeLogRenderer,
    ClaudeProvider,
)
from gza.providers.codex import (
    CODEX_EVENT_REGISTRY,
    CODEX_ITEM_REGISTRY,
    CODEX_LIVE_EVENT_HANDLERS,
    CODEX_LIVE_ITEM_HANDLERS,
    CODEX_LIVE_KNOWN_EVENT_TYPES,
    CODEX_LIVE_KNOWN_ITEM_TYPES,
    CODEX_RENDER_ITEM_HANDLERS,
    CODEX_RENDER_KNOWN_EVENT_TYPES,
    CODEX_RENDER_KNOWN_ITEM_TYPES,
    CodexLogRenderer,
    CodexProvider,
)

FIXTURES = Path(__file__).parent / "fixtures" / "log_renderer"


def _load_entries(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _classified_live_event_types(registry: dict[str, dict[str, object]]) -> tuple[set[str], set[str]]:
    live_event_types: set[str] = set()
    non_live_event_types: set[str] = set()
    for event_type, metadata in registry.items():
        live_dispatch = metadata.get("live")
        if isinstance(live_dispatch, str):
            live_event_types.add(event_type)
        elif live_dispatch is False:
            non_live_event_types.add(event_type)
    return live_event_types, non_live_event_types


def test_claude_log_renderer_fixture_types_are_registered() -> None:
    unknown_event_types: set[str] = set()
    unknown_assistant_block_types: set[str] = set()
    unknown_user_block_types: set[str] = set()

    for entry in _load_entries(FIXTURES / "claude.jsonl"):
        event_type = entry.get("type")
        if isinstance(event_type, str) and event_type not in CLAUDE_RENDER_KNOWN_EVENT_TYPES:
            unknown_event_types.add(event_type)
        if event_type not in {"assistant", "user"}:
            continue
        message = entry.get("message", {})
        if not isinstance(message, dict):
            continue
        content = message.get("content", [])
        if not isinstance(content, list):
            continue
        for block in content:
            if not isinstance(block, dict):
                continue
            block_type = block.get("type")
            if not isinstance(block_type, str):
                continue
            if event_type == "assistant" and block_type not in CLAUDE_RENDER_KNOWN_ASSISTANT_BLOCK_TYPES:
                unknown_assistant_block_types.add(block_type)
            if event_type == "user" and block_type not in CLAUDE_RENDER_KNOWN_USER_BLOCK_TYPES:
                unknown_user_block_types.add(block_type)

    assert not unknown_event_types, f"Claude fixture event types missing registry entries: {sorted(unknown_event_types)}"
    assert not unknown_assistant_block_types, (
        "Claude fixture assistant block types missing registry entries: "
        f"{sorted(unknown_assistant_block_types)}"
    )
    assert not unknown_user_block_types, (
        "Claude fixture user block types missing registry entries: "
        f"{sorted(unknown_user_block_types)}"
    )


def test_claude_live_fixture_types_are_registered() -> None:
    unknown_event_types: set[str] = set()
    unknown_assistant_block_types: set[str] = set()
    unknown_user_block_types: set[str] = set()
    live_event_types, non_live_event_types = _classified_live_event_types(CLAUDE_EVENT_REGISTRY)

    for entry in _load_entries(FIXTURES / "claude.jsonl"):
        event_type = entry.get("type")
        if isinstance(event_type, str) and event_type not in live_event_types and event_type not in non_live_event_types:
            unknown_event_types.add(event_type)
        if event_type not in {"assistant", "user"}:
            continue
        message = entry.get("message", {})
        if not isinstance(message, dict):
            continue
        content = message.get("content", [])
        if not isinstance(content, list):
            continue
        for block in content:
            if not isinstance(block, dict):
                continue
            block_type = block.get("type")
            if not isinstance(block_type, str):
                continue
            if event_type == "assistant" and block_type not in CLAUDE_LIVE_KNOWN_ASSISTANT_BLOCK_TYPES:
                unknown_assistant_block_types.add(block_type)
            if event_type == "user" and block_type not in CLAUDE_LIVE_KNOWN_USER_BLOCK_TYPES:
                unknown_user_block_types.add(block_type)

    assert not unknown_event_types, f"Claude live fixture event types missing registry entries: {sorted(unknown_event_types)}"
    fixture_event_types = {
        entry["type"]
        for entry in _load_entries(FIXTURES / "claude.jsonl")
        if isinstance(entry.get("type"), str)
    }
    assert fixture_event_types & non_live_event_types == set()
    assert not unknown_assistant_block_types, (
        "Claude live fixture assistant block types missing registry entries: "
        f"{sorted(unknown_assistant_block_types)}"
    )
    assert not unknown_user_block_types, (
        "Claude live fixture user block types missing registry entries: "
        f"{sorted(unknown_user_block_types)}"
    )


def test_claude_known_type_sets_are_derived_from_authoritative_registries() -> None:
    assert CLAUDE_RENDER_KNOWN_EVENT_TYPES == frozenset(
        event_type
        for event_type, metadata in CLAUDE_EVENT_REGISTRY.items()
        if metadata.get("render") is not None
    )
    assert CLAUDE_LIVE_EVENT_HANDLERS == {
        event_type: metadata["live"]
        for event_type, metadata in CLAUDE_EVENT_REGISTRY.items()
        if isinstance(metadata.get("live"), str)
    }
    assert CLAUDE_LIVE_KNOWN_EVENT_TYPES == frozenset(
        event_type
        for event_type, metadata in CLAUDE_EVENT_REGISTRY.items()
        if isinstance(metadata.get("live"), str)
    )
    for method_name in CLAUDE_LIVE_EVENT_HANDLERS.values():
        assert hasattr(ClaudeProvider, method_name)
    assert CLAUDE_RENDER_KNOWN_ASSISTANT_BLOCK_TYPES == frozenset(
        block_type
        for block_type, metadata in CLAUDE_ASSISTANT_BLOCK_REGISTRY.items()
        if isinstance(metadata.get("render"), str)
    )
    assert CLAUDE_LIVE_KNOWN_ASSISTANT_BLOCK_TYPES == frozenset(
        block_type
        for block_type, metadata in CLAUDE_ASSISTANT_BLOCK_REGISTRY.items()
        if isinstance(metadata.get("live"), str)
    )
    assert CLAUDE_RENDER_KNOWN_USER_BLOCK_TYPES == frozenset(
        block_type
        for block_type, metadata in CLAUDE_USER_BLOCK_REGISTRY.items()
        if isinstance(metadata.get("render"), str)
    )
    assert CLAUDE_LIVE_KNOWN_USER_BLOCK_TYPES == frozenset(
        block_type
        for block_type, metadata in CLAUDE_USER_BLOCK_REGISTRY.items()
        if isinstance(metadata.get("live"), str)
    )


def test_claude_assistant_block_registry_handlers_cover_render_and_live_dispatch() -> None:
    assert CLAUDE_RENDER_ASSISTANT_BLOCK_HANDLERS == {
        block_type: metadata["render"]
        for block_type, metadata in CLAUDE_ASSISTANT_BLOCK_REGISTRY.items()
        if isinstance(metadata.get("render"), str)
    }
    assert CLAUDE_LIVE_ASSISTANT_BLOCK_HANDLERS == {
        block_type: metadata["live"]
        for block_type, metadata in CLAUDE_ASSISTANT_BLOCK_REGISTRY.items()
        if isinstance(metadata.get("live"), str)
    }
    for method_name in CLAUDE_RENDER_ASSISTANT_BLOCK_HANDLERS.values():
        assert hasattr(ClaudeLogRenderer, method_name)
    for method_name in CLAUDE_LIVE_ASSISTANT_BLOCK_HANDLERS.values():
        assert hasattr(ClaudeProvider, method_name)


def test_claude_user_block_registry_handlers_cover_render_and_live_dispatch() -> None:
    assert CLAUDE_RENDER_USER_BLOCK_HANDLERS == {
        block_type: metadata["render"]
        for block_type, metadata in CLAUDE_USER_BLOCK_REGISTRY.items()
        if isinstance(metadata.get("render"), str)
    }
    assert CLAUDE_LIVE_USER_BLOCK_HANDLERS == {
        block_type: metadata["live"]
        for block_type, metadata in CLAUDE_USER_BLOCK_REGISTRY.items()
        if isinstance(metadata.get("live"), str)
    }
    for method_name in CLAUDE_RENDER_USER_BLOCK_HANDLERS.values():
        assert hasattr(ClaudeLogRenderer, method_name)
    for method_name in CLAUDE_LIVE_USER_BLOCK_HANDLERS.values():
        assert hasattr(ClaudeProvider, method_name)


def test_codex_log_renderer_fixture_types_are_registered() -> None:
    unknown_event_types: set[str] = set()
    unknown_item_types: set[str] = set()

    for entry in _load_entries(FIXTURES / "codex.jsonl"):
        event_type = entry.get("type")
        if isinstance(event_type, str) and event_type not in CODEX_RENDER_KNOWN_EVENT_TYPES:
            unknown_event_types.add(event_type)
        if event_type != "item.completed":
            continue
        item = entry.get("item", {})
        if not isinstance(item, dict):
            continue
        item_type = item.get("type")
        if isinstance(item_type, str) and item_type not in CODEX_RENDER_KNOWN_ITEM_TYPES:
            unknown_item_types.add(item_type)

    assert not unknown_event_types, f"Codex fixture event types missing registry entries: {sorted(unknown_event_types)}"
    assert not unknown_item_types, f"Codex fixture item types missing registry entries: {sorted(unknown_item_types)}"


def test_codex_live_fixture_types_are_registered() -> None:
    unknown_event_types: set[str] = set()
    unknown_item_types: set[str] = set()
    live_event_types, non_live_event_types = _classified_live_event_types(CODEX_EVENT_REGISTRY)

    for entry in _load_entries(FIXTURES / "codex.jsonl"):
        event_type = entry.get("type")
        if isinstance(event_type, str) and event_type not in live_event_types and event_type not in non_live_event_types:
            unknown_event_types.add(event_type)
        if event_type != "item.completed":
            continue
        item = entry.get("item", {})
        if not isinstance(item, dict):
            continue
        item_type = item.get("type")
        if isinstance(item_type, str) and item_type not in CODEX_LIVE_KNOWN_ITEM_TYPES:
            unknown_item_types.add(item_type)

    assert not unknown_event_types, f"Codex live fixture event types missing registry entries: {sorted(unknown_event_types)}"
    fixture_event_types = {
        entry["type"]
        for entry in _load_entries(FIXTURES / "codex.jsonl")
        if isinstance(entry.get("type"), str)
    }
    assert "error" in fixture_event_types
    assert "error" in live_event_types
    assert not unknown_item_types, f"Codex live fixture item types missing registry entries: {sorted(unknown_item_types)}"


def test_codex_known_type_sets_are_derived_from_authoritative_registries() -> None:
    assert CODEX_RENDER_KNOWN_EVENT_TYPES == frozenset(
        event_type
        for event_type, metadata in CODEX_EVENT_REGISTRY.items()
        if metadata.get("render") is not None
    )
    assert CODEX_LIVE_EVENT_HANDLERS == {
        event_type: metadata["live"]
        for event_type, metadata in CODEX_EVENT_REGISTRY.items()
        if isinstance(metadata.get("live"), str)
    }
    assert CODEX_LIVE_KNOWN_EVENT_TYPES == frozenset(
        event_type
        for event_type, metadata in CODEX_EVENT_REGISTRY.items()
        if isinstance(metadata.get("live"), str)
    )
    for method_name in CODEX_LIVE_EVENT_HANDLERS.values():
        assert hasattr(CodexProvider, method_name)
    assert CODEX_RENDER_KNOWN_ITEM_TYPES == frozenset(
        item_type
        for item_type, metadata in CODEX_ITEM_REGISTRY.items()
        if isinstance(metadata.get("render"), str)
    )
    assert CODEX_LIVE_KNOWN_ITEM_TYPES == frozenset(
        item_type
        for item_type, metadata in CODEX_ITEM_REGISTRY.items()
        if isinstance(metadata.get("live"), str)
    )


def test_codex_item_registry_handlers_cover_render_and_live_dispatch() -> None:
    assert CODEX_RENDER_ITEM_HANDLERS == {
        item_type: metadata["render"]
        for item_type, metadata in CODEX_ITEM_REGISTRY.items()
        if isinstance(metadata.get("render"), str)
    }
    assert CODEX_LIVE_ITEM_HANDLERS == {
        item_type: metadata["live"]
        for item_type, metadata in CODEX_ITEM_REGISTRY.items()
        if isinstance(metadata.get("live"), str)
    }
    for method_name in CODEX_RENDER_ITEM_HANDLERS.values():
        assert hasattr(CodexLogRenderer, method_name)
    for method_name in CODEX_LIVE_ITEM_HANDLERS.values():
        assert hasattr(CodexProvider, method_name)
