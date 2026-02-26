"""Tests for console output helpers."""

from io import StringIO

from rich.console import Console

from gza.console import stats_line
from gza.db import TaskStats


def test_stats_line_shows_estimated_markers(monkeypatch):
    """Stats line should mark estimated token/cost values."""
    output = StringIO()
    test_console = Console(file=output, force_terminal=False)
    monkeypatch.setattr("gza.console.console", test_console)

    stats = TaskStats(
        duration_seconds=12.0,
        input_tokens=1234,
        output_tokens=56,
        cost_usd=0.1234,
        tokens_estimated=True,
        cost_estimated=True,
    )
    stats_line(stats)

    rendered = output.getvalue()
    assert "Tokens:" in rendered
    assert "in 1,234 / out 56" in rendered
    assert "Cost:" in rendered
    assert "$0.1234" in rendered
    assert "(estimated)" in rendered
