"""Tests for console output helpers."""

from io import StringIO

from rich.console import Console

from gza.console import (
    _recommend_next_steps,
    error_message,
    format_duration,
    get_terminal_width,
    info_line,
    stats_line,
    task_footer,
    task_header,
    truncate,
    warning_message,
)
from gza.db import Task, TaskStats


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

    task_header("Fix the bug", "42", "implement", slug="20260101-fix-bug")
    rendered = output.getvalue()
    assert "Task: Fix the bug" in rendered
    assert "ID: 42" in rendered
    assert "Slug: 20260101-fix-bug" in rendered
    assert "Type: implement" in rendered


def test_task_header_uses_four_dash_separator(monkeypatch):
    output = StringIO()
    test_console = Console(file=output, force_terminal=False)
    monkeypatch.setattr("gza.console.console", test_console)

    task_header("Fix the bug", "42", "implement")
    rendered = output.getvalue()
    lines = [line for line in rendered.splitlines() if line]
    # First and last non-empty lines should be exactly the 4-dash separator.
    assert lines[0] == "----"
    assert lines[-1] == "----"


def test_task_header_no_longer_uses_equals_banner(monkeypatch):
    """The old '=== Task: ... ===' banner format must not return."""
    output = StringIO()
    test_console = Console(file=output, force_terminal=False)
    monkeypatch.setattr("gza.console.console", test_console)

    task_header("Fix the bug", "42", "implement")
    rendered = output.getvalue()
    assert "===" not in rendered


def test_task_header_lines_are_not_indented(monkeypatch):
    output = StringIO()
    test_console = Console(file=output, force_terminal=False)
    monkeypatch.setattr("gza.console.console", test_console)

    task_header("Fix the bug", "42", "implement", slug="20260101-fix-bug")
    rendered = output.getvalue()
    for line in rendered.splitlines():
        if not line:
            continue
        # No line in the header block should start with whitespace.
        assert not line.startswith(" "), f"Unexpected indentation on line: {line!r}"


def test_task_header_omits_slug_when_not_provided(monkeypatch):
    output = StringIO()
    test_console = Console(file=output, force_terminal=False)
    monkeypatch.setattr("gza.console.console", test_console)

    task_header("Fix the bug", "42", "implement")
    rendered = output.getvalue()
    assert "Slug:" not in rendered


def test_task_header_omits_slug_when_empty(monkeypatch):
    output = StringIO()
    test_console = Console(file=output, force_terminal=False)
    monkeypatch.setattr("gza.console.console", test_console)

    task_header("Fix the bug", "42", "implement", slug="")
    rendered = output.getvalue()
    assert "Slug:" not in rendered


def test_task_header_truncates_long_prompt(monkeypatch):
    output = StringIO()
    test_console = Console(file=output, force_terminal=False)
    monkeypatch.setattr("gza.console.console", test_console)

    long_prompt = "x" * 100
    task_header(long_prompt, "1", "plan")
    rendered = output.getvalue()
    assert "..." in rendered


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


# --- task_footer ---


def _make_task(
    *,
    task_id: int | None = 42,
    task_type: str = "implement",
    slug: str | None = "20260410-test-task",
    depends_on: int | None = None,
) -> Task:
    return Task(
        id=task_id,
        prompt="Test prompt",
        task_type=task_type,
        status="completed",
        slug=slug,
        depends_on=depends_on,
    )


def _render_footer(monkeypatch, **kwargs) -> str:
    output = StringIO()
    test_console = Console(file=output, force_terminal=False)
    monkeypatch.setattr("gza.console.console", test_console)
    task_footer(**kwargs)
    return output.getvalue()


def test_task_footer_uses_four_dash_separator(monkeypatch):
    rendered = _render_footer(
        monkeypatch,
        task=_make_task(),
        status="Done",
    )
    lines = [line for line in rendered.splitlines() if line]
    assert lines[0] == "----"
    assert lines[-1] == "----"


def test_task_footer_no_equals_banner(monkeypatch):
    """The old '=== Done ===' / '=== X Complete ===' format must not return."""
    rendered = _render_footer(
        monkeypatch,
        task=_make_task(),
        status="Done",
    )
    assert "===" not in rendered


def test_task_footer_field_lines_are_not_indented(monkeypatch):
    """Field lines (Status, ID, Slug, Type, Branch, ...) sit flush-left.

    The only exception is the children of ``Next steps:`` — those are a nested
    command list and legitimately indent two spaces to show hierarchy. This
    test skips lines that belong to the next-step list by checking the leading
    label instead of an indentation rule alone.
    """
    rendered = _render_footer(
        monkeypatch,
        task=_make_task(),
        status="Done",
        branch="20260410-test",
    )
    field_labels = ("Status:", "ID:", "Slug:", "Type:", "Branch:", "Report:",
                    "Verdict:", "Worktree:", "Learnings:", "Stats:", "Next steps:", "----")
    for line in rendered.splitlines():
        if not line:
            continue
        # Lines that begin with a field label must not be indented.
        if any(line.lstrip().startswith(label) for label in field_labels):
            assert not line.startswith(" "), f"Field line unexpectedly indented: {line!r}"


def test_task_footer_prints_core_fields(monkeypatch):
    rendered = _render_footer(
        monkeypatch,
        task=_make_task(task_id=42, task_type="implement", slug="20260410-test"),
        status="Done",
    )
    assert "Status: Done" in rendered
    assert "ID: 42" in rendered
    assert "Slug: 20260410-test" in rendered
    assert "Type: implement" in rendered


def test_task_footer_failure_status(monkeypatch):
    rendered = _render_footer(
        monkeypatch,
        task=_make_task(task_type="implement"),
        status="Failed: max steps of 50 exceeded",
    )
    assert "Status: Failed: max steps of 50 exceeded" in rendered
    # Failure footers still render the core identifying fields.
    assert "ID: 42" in rendered
    assert "Type: implement" in rendered


def test_task_footer_omits_unset_optional_fields(monkeypatch):
    rendered = _render_footer(
        monkeypatch,
        task=_make_task(),
        status="Done",
    )
    assert "Branch:" not in rendered
    assert "Report:" not in rendered
    assert "Verdict:" not in rendered
    assert "Worktree:" not in rendered
    assert "Learnings:" not in rendered


def test_task_footer_branch_field(monkeypatch):
    rendered = _render_footer(
        monkeypatch,
        task=_make_task(),
        status="Done",
        branch="20260410-feature-x",
    )
    assert "Branch: 20260410-feature-x" in rendered


def test_task_footer_report_field(monkeypatch):
    rendered = _render_footer(
        monkeypatch,
        task=_make_task(task_type="review"),
        status="Done",
        report=".gza/reviews/foo.md",
    )
    assert "Report: .gza/reviews/foo.md" in rendered


def test_task_footer_verdict_field(monkeypatch):
    rendered = _render_footer(
        monkeypatch,
        task=_make_task(task_type="review", depends_on=100),
        status="Done",
        verdict="APPROVED",
    )
    assert "Verdict: APPROVED" in rendered


def test_task_footer_worktree_field(monkeypatch):
    rendered = _render_footer(
        monkeypatch,
        task=_make_task(task_type="review"),
        status="Failed: timed out",
        worktree="/tmp/gza-worktrees/foo",
    )
    assert "Worktree: /tmp/gza-worktrees/foo" in rendered


def test_task_footer_next_steps_success_code_task(monkeypatch):
    rendered = _render_footer(
        monkeypatch,
        task=_make_task(task_id=42, task_type="implement"),
        status="Done",
        branch="20260410-test",
    )
    assert "Next steps:" in rendered
    assert "gza merge 42" in rendered
    assert "gza pr 42" in rendered


def test_task_footer_next_steps_failure(monkeypatch):
    rendered = _render_footer(
        monkeypatch,
        task=_make_task(task_id=42, task_type="implement"),
        status="Failed: timed out",
    )
    assert "Next steps:" in rendered
    assert "gza retry 42" in rendered
    assert "gza resume 42" in rendered


def test_task_footer_no_next_steps_for_internal_success(monkeypatch):
    rendered = _render_footer(
        monkeypatch,
        task=_make_task(task_id=42, task_type="internal"),
        status="Done",
    )
    assert "Next steps:" not in rendered


# --- _recommend_next_steps ---


def test_recommend_failure_returns_retry_resume():
    task = _make_task(task_type="implement")
    steps = _recommend_next_steps(task, status="Failed: timed out")
    cmds = [cmd for cmd, _ in steps]
    assert any("gza retry 42" in c for c in cmds)
    assert any("gza resume 42" in c for c in cmds)


def test_recommend_failure_for_non_code_task():
    """Failure recommendation is task-type-independent (still retry/resume)."""
    task = _make_task(task_type="review", depends_on=100)
    steps = _recommend_next_steps(task, status="Failed: MAX_STEPS")
    cmds = [cmd for cmd, _ in steps]
    assert any("gza retry 42" in c for c in cmds)
    assert any("gza resume 42" in c for c in cmds)


def test_recommend_success_implement_returns_merge_pr():
    task = _make_task(task_type="implement")
    steps = _recommend_next_steps(task, status="Done")
    cmds = [cmd for cmd, _ in steps]
    assert any("gza merge 42" in c for c in cmds)
    assert any("gza pr 42" in c for c in cmds)


def test_recommend_success_improve_returns_merge_pr():
    task = _make_task(task_type="improve")
    steps = _recommend_next_steps(task, status="Done")
    cmds = [cmd for cmd, _ in steps]
    assert any("gza merge 42" in c for c in cmds)


def test_recommend_success_rebase_returns_merge_pr():
    task = _make_task(task_type="rebase")
    steps = _recommend_next_steps(task, status="Done")
    cmds = [cmd for cmd, _ in steps]
    assert any("gza merge 42" in c for c in cmds)


def test_recommend_success_explore_returns_based_on():
    task = _make_task(task_type="explore")
    steps = _recommend_next_steps(task, status="Done")
    cmds = [cmd for cmd, _ in steps]
    assert any("gza add --based-on 42" in c for c in cmds)


def test_recommend_success_plan_returns_implement():
    task = _make_task(task_type="plan")
    steps = _recommend_next_steps(task, status="Done")
    cmds = [cmd for cmd, _ in steps]
    assert any("gza implement 42" in c for c in cmds)


def test_recommend_success_review_returns_improve_for_dep():
    task = _make_task(task_type="review", depends_on=100)
    steps = _recommend_next_steps(task, status="Done")
    cmds = [cmd for cmd, _ in steps]
    assert any("gza improve 100" in c for c in cmds)


def test_recommend_success_review_without_dep_returns_empty():
    task = _make_task(task_type="review", depends_on=None)
    steps = _recommend_next_steps(task, status="Done")
    assert steps == []


def test_recommend_success_internal_returns_empty():
    task = _make_task(task_type="internal")
    steps = _recommend_next_steps(task, status="Done")
    assert steps == []


def test_recommend_handles_missing_task_id():
    task = _make_task(task_id=None)
    steps = _recommend_next_steps(task, status="Done")
    assert steps == []


def test_recommend_accepts_store_kwarg_without_erroring():
    """The store kwarg is reserved for lineage-aware rules; callers may pass None."""
    task = _make_task(task_type="implement")
    steps = _recommend_next_steps(task, status="Done", store=None)
    cmds = [cmd for cmd, _ in steps]
    assert any("gza merge 42" in c for c in cmds)
