"""Shared classification of provider log events.

Step-counting logic has regressed multiple times because it was duplicated
across the codex/claude provider live printers, ``gza log`` replay, and
``gza tv``'s scan. When a provider changed its event shape, other sites
silently drifted. Keep the predicate here and call it from every site.
"""
from __future__ import annotations


def is_new_step(entry: dict, seen_msg_ids: set[str]) -> bool:
    """Return True iff *entry* marks the start of a new logical step.

    Mutates ``seen_msg_ids`` to deduplicate Claude's streaming ``assistant``
    messages (same id across deltas).

    Providers:
    - Claude: each ``assistant`` entry with a new ``message.id``.
    - Codex: each ``item.completed`` entry whose item is a non-empty
      ``agent_message``. ``turn.started`` fires once per session and is
      NOT a step boundary.
    """
    etype = entry.get("type")
    if etype == "assistant":
        message = entry.get("message") or {}
        msg_id = message.get("id")
        if msg_id and msg_id not in seen_msg_ids:
            seen_msg_ids.add(msg_id)
            return True
        return False
    if etype == "item.completed":
        item = entry.get("item") or {}
        if isinstance(item, dict) and item.get("type") == "agent_message":
            text = item.get("text", "")
            return isinstance(text, str) and bool(text.strip())
    return False
