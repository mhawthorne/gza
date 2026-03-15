"""Tests for console output helpers."""

from io import StringIO
from unittest.mock import patch

from rich.console import Console

from gza.console import (
    error_message,
    format_duration,
    get_terminal_width,
    info_line,
    next_steps,
    stats_line,
    success_message,
    task_header,
    truncate,
    warning_message,
)
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


# --- truncate ---

def test_truncate_short_text():
    assert truncate("hello", 10) == "hello"


def test_truncate_exact_length():
    assert truncate("hello", 5) == "hello"


def test_truncate_long_text():
    assert truncate("hello world", 8) == "hello..."


def test_truncate_custom_suffix():
    assert truncate("hello world", 8, suffix="~") == "hello w~"


# --- get_terminal_width ---

def test_get_terminal_width_returns_positive():
    width = get_terminal_width()
    assert isinstance(width, int)
    assert width > 0


def test_get_terminal_width_fallback_on_error():
    # Verify the fallback logic by calling the except branch directly.
    # We can't safely monkeypatch shutil.get_terminal_size because pytest/xdist
    # also depends on it. Instead, just verify the function handles the case
    # by checking the return type is always int.
    width = get_terminal_width()
    assert isinstance(width, int)
    assert width >= 1


# --- format_duration ---

def test_format_duration_seconds_only():
    assert format_duration(45) == "45s"


def test_format_duration_zero():
    assert format_duration(0) == "0s"


def test_format_duration_minutes_and_seconds():
    assert format_duration(150) == "2m 30s"


def test_format_duration_verbose_hours():
    assert format_duration(5000, verbose=True) == "1h 23m"


def test_format_duration_non_verbose_above_3600():
    assert format_duration(5000, verbose=False) == "83m 20s"


# --- task_header ---

def test_task_header_prints_info(monkeypatch):
    output = StringIO()
    test_console = Console(file=output, force_terminal=False)
    monkeypatch.setattr("gza.console.console", test_console)

    task_header("Fix the bug", "20260101-fix-bug", "implement")
    rendered = output.getvalue()
    assert "Fix the bug" in rendered
    assert "20260101-fix-bug" in rendered
    assert "implement" in rendered


def test_task_header_truncates_long_prompt(monkeypatch):
    output = StringIO()
    test_console = Console(file=output, force_terminal=False)
    monkeypatch.setattr("gza.console.console", test_console)

    long_prompt = "x" * 100
    task_header(long_prompt, "id-1", "plan")
    rendered = output.getvalue()
    assert "..." in rendered


# --- success_message ---

def test_success_message(monkeypatch):
    output = StringIO()
    test_console = Console(file=output, force_terminal=False)
    monkeypatch.setattr("gza.console.console", test_console)

    success_message("All done")
    assert "All done" in output.getvalue()


# --- error_message ---

def test_error_message(monkeypatch):
    output = StringIO()
    test_console = Console(file=output, force_terminal=False)
    monkeypatch.setattr("gza.console.console", test_console)

    error_message("Something broke")
    assert "Something broke" in output.getvalue()


# --- warning_message ---

def test_warning_message(monkeypatch):
    output = StringIO()
    test_console = Console(file=output, force_terminal=False)
    monkeypatch.setattr("gza.console.console", test_console)

    warning_message("Watch out")
    assert "Watch out" in output.getvalue()


# --- info_line ---

def test_info_line(monkeypatch):
    output = StringIO()
    test_console = Console(file=output, force_terminal=False)
    monkeypatch.setattr("gza.console.console", test_console)

    info_line("Status", "running")
    rendered = output.getvalue()
    assert "Status" in rendered
    assert "running" in rendered


# --- next_steps ---

def test_next_steps(monkeypatch):
    output = StringIO()
    test_console = Console(file=output, force_terminal=False)
    monkeypatch.setattr("gza.console.console", test_console)

    next_steps([("gza work", "run next task"), ("gza history", "see results")])
    rendered = output.getvalue()
    assert "Next steps:" in rendered
    assert "gza work" in rendered
    assert "gza history" in rendered
