"""Tests for gza.colors — the centralized color definitions module."""

import re

import pytest


# ---------------------------------------------------------------------------
# Import smoke tests — ensures all public exports are importable
# ---------------------------------------------------------------------------


def test_import_singleton_instances() -> None:
    from gza.colors import (  # noqa: F401
        TASK_COLORS,
        STATUS_COLORS,
        WORK_OUTPUT_COLORS,
        SHOW_COLORS,
        UNMERGED_COLORS,
        NEXT_COLORS,
    )


def test_import_dict_variants() -> None:
    from gza.colors import (  # noqa: F401
        TASK_COLORS_DICT,
        STATUS_COLORS_DICT,
        WORK_OUTPUT_COLORS_DICT,
        SHOW_COLORS_DICT,
        UNMERGED_COLORS_DICT,
        NEXT_COLORS_DICT,
        LINEAGE_STATUS_COLORS,
        PS_STATUS_COLORS,
        LOG_TASK_STATUS_COLORS,
        LOG_WORKER_STATUS_COLORS,
        REVIEW_VERDICT_COLORS,
        CYCLE_STATUS_COLORS,
    )


def test_import_base_palette() -> None:
    from gza.colors import (  # noqa: F401
        pink_prompt,
        gray_secondary,
        blue_step,
        cyan_header,
        green_success,
        yellow_warning,
        red_error,
        magenta_tool,
        bold_heading,
        dim_secondary,
        bold_cyan_heading,
        bold_red_error,
        dim_yellow_note,
    )


# ---------------------------------------------------------------------------
# Dataclass key-set regression tests
# ---------------------------------------------------------------------------


def test_task_colors_dict_keys() -> None:
    from gza.colors import TASK_COLORS_DICT

    expected_keys = {"task_id", "prompt", "branch", "stats", "success", "failure",
                     "unmerged", "orphaned", "lineage", "header", "label", "value"}
    assert set(TASK_COLORS_DICT.keys()) == expected_keys


def test_status_colors_dict_keys() -> None:
    from gza.colors import STATUS_COLORS_DICT

    expected_keys = {"completed", "failed", "pending", "in_progress", "unmerged",
                     "dropped", "stale", "unknown", "running"}
    assert set(STATUS_COLORS_DICT.keys()) == expected_keys


def test_work_output_colors_dict_keys() -> None:
    from gza.colors import WORK_OUTPUT_COLORS_DICT

    expected_keys = {"step_header", "assistant_text", "tool_use", "error",
                     "todo_pending", "todo_in_progress", "todo_completed"}
    assert set(WORK_OUTPUT_COLORS_DICT.keys()) == expected_keys


def test_show_colors_dict_keys() -> None:
    from gza.colors import SHOW_COLORS_DICT

    expected_keys = {"heading", "section", "label", "value", "task_id", "prompt",
                     "branch", "stats", "status_pending", "status_running",
                     "status_completed", "status_failed", "status_default"}
    assert set(SHOW_COLORS_DICT.keys()) == expected_keys


def test_unmerged_colors_dict_keys() -> None:
    from gza.colors import UNMERGED_COLORS_DICT

    expected_keys = {"task_id", "prompt", "stats", "branch",
                     "review_approved", "review_changes", "review_discussion", "review_none"}
    assert set(UNMERGED_COLORS_DICT.keys()) == expected_keys


def test_next_colors_dict_keys() -> None:
    from gza.colors import NEXT_COLORS_DICT

    expected_keys = {"task_id", "prompt", "type", "blocked", "index"}
    assert set(NEXT_COLORS_DICT.keys()) == expected_keys


def test_lineage_status_colors_keys() -> None:
    from gza.colors import LINEAGE_STATUS_COLORS

    expected_keys = {"completed", "failed", "pending", "in_progress", "unmerged", "dropped"}
    assert set(LINEAGE_STATUS_COLORS.keys()) == expected_keys


def test_ps_status_colors_keys() -> None:
    from gza.colors import PS_STATUS_COLORS

    expected_keys = {"running", "in_progress", "completed", "failed", "failed(startup)",
                     "stale", "unknown"}
    assert set(PS_STATUS_COLORS.keys()) == expected_keys


def test_log_task_status_colors_keys() -> None:
    from gza.colors import LOG_TASK_STATUS_COLORS

    expected_keys = {"completed", "unmerged", "failed", "dropped", "in_progress"}
    assert set(LOG_TASK_STATUS_COLORS.keys()) == expected_keys


def test_log_worker_status_colors_keys() -> None:
    from gza.colors import LOG_WORKER_STATUS_COLORS

    expected_keys = {"running", "in_progress", "completed", "failed", "stale"}
    assert set(LOG_WORKER_STATUS_COLORS.keys()) == expected_keys


def test_review_verdict_colors_keys() -> None:
    from gza.colors import REVIEW_VERDICT_COLORS

    expected_keys = {"APPROVED", "CHANGES_REQUESTED", "NEEDS_DISCUSSION"}
    assert set(REVIEW_VERDICT_COLORS.keys()) == expected_keys


def test_cycle_status_colors_keys() -> None:
    from gza.colors import CYCLE_STATUS_COLORS

    expected_keys = {"active", "approved", "maxed_out", "blocked"}
    assert set(CYCLE_STATUS_COLORS.keys()) == expected_keys


# ---------------------------------------------------------------------------
# Specific value regression tests (guard against silent color drift)
# ---------------------------------------------------------------------------


def test_task_colors_prompt_is_pink() -> None:
    from gza.colors import TASK_COLORS, pink_prompt

    assert TASK_COLORS.prompt == pink_prompt
    assert TASK_COLORS.prompt == "#ff99cc"


def test_next_colors_prompt_is_pink() -> None:
    from gza.colors import NEXT_COLORS, pink_prompt

    assert NEXT_COLORS.prompt == pink_prompt


def test_review_verdict_approved_is_green() -> None:
    from gza.colors import REVIEW_VERDICT_COLORS, green_success

    assert REVIEW_VERDICT_COLORS["APPROVED"] == green_success


def test_review_verdict_changes_requested_is_yellow() -> None:
    from gza.colors import REVIEW_VERDICT_COLORS, yellow_warning

    assert REVIEW_VERDICT_COLORS["CHANGES_REQUESTED"] == yellow_warning


def test_log_task_status_unmerged_is_green() -> None:
    """unmerged maps to green in log display (treated as successfully merged)."""
    from gza.colors import LOG_TASK_STATUS_COLORS, green_success

    assert LOG_TASK_STATUS_COLORS["unmerged"] == green_success


# ---------------------------------------------------------------------------
# OutputStyles inheritance test
# ---------------------------------------------------------------------------


def test_output_styles_inherits_work_output_colors() -> None:
    from gza.providers.output_formatter import OutputStyles
    from gza.colors import WorkOutputColors

    assert issubclass(OutputStyles, WorkOutputColors)


def test_output_styles_instantiation() -> None:
    from gza.providers.output_formatter import OutputStyles

    styles = OutputStyles()
    # Verify inherited fields are accessible
    assert styles.step_header
    assert styles.assistant_text
    assert styles.tool_use
    assert styles.error


# ---------------------------------------------------------------------------
# No hex color literals outside colors.py (regression guard)
# ---------------------------------------------------------------------------


def test_no_hex_colors_outside_colors_module() -> None:
    """Assert no #rrggbb hex color literals appear outside gza/colors.py.

    Matches hex colors in any context: standalone strings (``"#ff99cc"``),
    Rich markup tags (``[#ff99cc]``, ``[/#ff99cc]``), and f-string interpolations.
    """
    from pathlib import Path

    # Match any bare #rrggbb sequence — covers both quoted strings and Rich markup tags.
    hex_pattern = re.compile(r'#[0-9a-fA-F]{6}')
    src_root = Path(__file__).parent.parent / "src" / "gza"

    violations: list[str] = []
    for py_file in src_root.rglob("*.py"):
        if py_file.name == "colors.py":
            continue
        text = py_file.read_text()
        for match in hex_pattern.finditer(text):
            line_no = text[: match.start()].count("\n") + 1
            violations.append(f"{py_file.relative_to(src_root)}:{line_no}: {match.group()}")

    assert violations == [], (
        "Hex color literals found outside gza/colors.py — add them to colors.py instead:\n"
        + "\n".join(violations)
    )
