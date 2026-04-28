"""Provider-facing prompt sanitization for high-risk wording patterns."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class _SanitizationRule:
    trigger: re.Pattern[str]
    context: re.Pattern[str]
    replacement: str


_FENCED_BLOCK_RE = re.compile(r"(```[\s\S]*?```)", re.MULTILINE)
_CONTEXT_WINDOW_CHARS = 160

_RULES: tuple[_SanitizationRule, ...] = (
    _SanitizationRule(
        trigger=re.compile(r"\bbypass(?:ing|ed)?\b", re.IGNORECASE),
        context=re.compile(r"\b(sandbox|guardrail|policy|safety|restriction|constraint)s?\b", re.IGNORECASE),
        replacement="work within",
    ),
    _SanitizationRule(
        trigger=re.compile(r"\bkill(?:ed|ing)?\b", re.IGNORECASE),
        context=re.compile(r"\b(process|task|run|session|job|agent)s?\b", re.IGNORECASE),
        replacement="terminate",
    ),
    _SanitizationRule(
        trigger=re.compile(r"\binterrupted\b", re.IGNORECASE),
        context=re.compile(r"\b(task|run|session|execution|agent|job)s?\b", re.IGNORECASE),
        replacement="paused",
    ),
    _SanitizationRule(
        trigger=re.compile(r"\boverride(?:n)?\b", re.IGNORECASE),
        context=re.compile(r"\b(rule|policy|instruction|constraint|guardrail|safety|sandbox)s?\b", re.IGNORECASE),
        replacement="adjust",
    ),
)


def _match_has_nearby_context(
    text: str,
    *,
    trigger_start: int,
    trigger_end: int,
    context_pattern: re.Pattern[str],
    window_chars: int = _CONTEXT_WINDOW_CHARS,
) -> bool:
    """Return True when a context term appears near a trigger match."""
    window_start = max(0, trigger_start - window_chars)
    window_end = min(len(text), trigger_end + window_chars)
    nearby_text = text[window_start:window_end]
    return context_pattern.search(nearby_text) is not None


def _sanitize_segment(text: str) -> str:
    result = text
    for rule in _RULES:
        if not rule.trigger.search(result):
            continue
        replacements: list[tuple[int, int]] = []
        for match in rule.trigger.finditer(result):
            if _match_has_nearby_context(
                result,
                trigger_start=match.start(),
                trigger_end=match.end(),
                context_pattern=rule.context,
            ):
                replacements.append((match.start(), match.end()))
        if not replacements:
            continue
        rebuilt: list[str] = []
        cursor = 0
        for start, end in replacements:
            rebuilt.append(result[cursor:start])
            rebuilt.append(rule.replacement)
            cursor = end
        rebuilt.append(result[cursor:])
        result = "".join(rebuilt)
    return result


def sanitize_provider_prompt(prompt: str, *, task_type: str) -> str:
    """Sanitize provider-facing prompt text for selected task types only."""
    if task_type not in {"review", "improve"}:
        return prompt
    if not prompt:
        return prompt

    # Preserve fenced code blocks verbatim to reduce accidental replacements.
    parts = _FENCED_BLOCK_RE.split(prompt)
    if len(parts) == 1:
        return _sanitize_segment(prompt)

    sanitized: list[str] = []
    for idx, part in enumerate(parts):
        if idx % 2 == 1 and part.startswith("```"):
            sanitized.append(part)
        else:
            sanitized.append(_sanitize_segment(part))
    return "".join(sanitized)
