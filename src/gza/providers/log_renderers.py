"""Provider log renderer resolver."""

from __future__ import annotations

from typing import Any

from .claude import ClaudeLogRenderer
from .codex import CodexLogRenderer
from .gemini import GeminiLogRenderer
from .log_rendering import ProviderLogRenderer


class MixedLegacyLogRenderer:
    """Fallback renderer for worker-only/startup logs without task metadata."""

    def __init__(self, *, configured_model: str | None = None, verbose: bool = False) -> None:
        self._configured_model = configured_model
        self._verbose = verbose
        self._delegate: ProviderLogRenderer | None = None
        self._generic = ClaudeLogRenderer(configured_model=configured_model, verbose=verbose)
        self.stats = self._generic.stats
        self.suppressed_count = self._generic.suppressed_count

    def _ensure_delegate(self, entry: dict[str, Any]) -> ProviderLogRenderer:
        if self._delegate is not None:
            return self._delegate
        event_type = entry.get("type")
        if event_type in {"system", "assistant", "user"}:
            self._delegate = ClaudeLogRenderer(configured_model=self._configured_model, verbose=self._verbose)
        elif event_type in {"thread.started", "turn.started", "item.started", "item.completed", "turn.completed"}:
            self._delegate = CodexLogRenderer(configured_model=self._configured_model, verbose=self._verbose)
        elif event_type in {"init", "message", "tool_use", "tool_output", "tool_error", "tool_retry"}:
            self._delegate = GeminiLogRenderer(configured_model=self._configured_model, verbose=self._verbose)
        else:
            return self._generic
        self.stats = self._delegate.stats
        self.suppressed_count = self._delegate.suppressed_count
        return self._delegate

    def handle_log(self, entry: dict[str, Any], *, live: bool):
        renderer = self._ensure_delegate(entry)
        rendered = renderer.handle_log(entry, live=live)
        self.stats = renderer.stats
        self.suppressed_count = renderer.suppressed_count
        return rendered

    def handle_tv(self, entry: dict[str, Any]):
        renderer = self._ensure_delegate(entry)
        rendered = renderer.handle_tv(entry)
        self.stats = renderer.stats
        self.suppressed_count = renderer.suppressed_count
        return rendered


def get_log_renderer(
    provider: str | None,
    *,
    configured_model: str | None = None,
    verbose: bool = False,
) -> ProviderLogRenderer:
    """Resolve a provider-owned log renderer."""
    if provider is None:
        return MixedLegacyLogRenderer(configured_model=configured_model, verbose=verbose)
    normalized = provider.strip().lower()
    if normalized == "claude":
        return ClaudeLogRenderer(configured_model=configured_model, verbose=verbose)
    if normalized == "codex":
        return CodexLogRenderer(configured_model=configured_model, verbose=verbose)
    if normalized == "gemini":
        return GeminiLogRenderer(configured_model=configured_model, verbose=verbose)
    raise ValueError(f"Unknown provider for log rendering: {provider}")
