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
        TASK_STREAM_COLORS,
        SHOW_COLORS,
        UNMERGED_COLORS,
        NEXT_COLORS,
        ADVANCE_COLORS,
    )


def test_import_dict_variants() -> None:
    from gza.colors import (  # noqa: F401
        TASK_COLORS_DICT,
        STATUS_COLORS_DICT,
        TASK_STREAM_COLORS_DICT,
        SHOW_COLORS_DICT,
        UNMERGED_COLORS_DICT,
        NEXT_COLORS_DICT,
        ADVANCE_COLORS_DICT,
        LINEAGE_STATUS_COLORS,
        PS_STATUS_COLORS,
    )


def test_import_base_palette() -> None:
    from gza.colors import (  # noqa: F401
        pink,
        gray_secondary,
        cyan,
        green_success,
        yellow_warning,
        red_error,
        magenta_tool,
        bold_heading,
        bold_cyan_heading,
        bold_red_error,
        dim_yellow_note,
    )


def test_import_theme_types() -> None:
    from gza.colors import BaseColors, Theme, BUILT_IN_THEMES, set_theme  # noqa: F401


# ---------------------------------------------------------------------------
# Dataclass key-set regression tests
# ---------------------------------------------------------------------------


def test_task_colors_dict_keys() -> None:
    from gza.colors import TASK_COLORS_DICT

    expected_keys = {"task_id", "prompt", "branch", "stats", "success", "failure",
                     "unmerged", "orphaned", "lineage", "date", "file", "header", "label", "value"}
    assert set(TASK_COLORS_DICT.keys()) == expected_keys


def test_status_colors_dict_keys() -> None:
    from gza.colors import STATUS_COLORS_DICT

    expected_keys = {"completed", "failed", "pending", "in_progress", "unmerged",
                     "dropped", "stale", "unknown", "running"}
    assert set(STATUS_COLORS_DICT.keys()) == expected_keys


def test_task_stream_colors_dict_keys() -> None:
    from gza.colors import TASK_STREAM_COLORS_DICT

    expected_keys = {"step_header", "assistant_text", "tool_use", "error",
                     "todo_pending", "todo_in_progress", "todo_completed"}
    assert set(TASK_STREAM_COLORS_DICT.keys()) == expected_keys


def test_show_colors_dict_keys() -> None:
    from gza.colors import SHOW_COLORS_DICT

    expected_keys = {"heading", "section", "label", "value", "task_id", "prompt",
                     "branch", "stats", "status_pending", "status_running",
                     "status_completed", "status_failed", "status_default"}
    assert set(SHOW_COLORS_DICT.keys()) == expected_keys


def test_unmerged_colors_dict_keys() -> None:
    from gza.colors import UNMERGED_COLORS_DICT

    expected_keys = {"task_id", "prompt", "stats", "branch", "date",
                     "review_approved", "review_changes", "review_discussion", "review_none"}
    assert set(UNMERGED_COLORS_DICT.keys()) == expected_keys


def test_next_colors_dict_keys() -> None:
    from gza.colors import NEXT_COLORS_DICT

    expected_keys = {"task_id", "prompt", "type", "blocked", "index"}
    assert set(NEXT_COLORS_DICT.keys()) == expected_keys


def test_advance_colors_dict_keys() -> None:
    from gza.colors import ADVANCE_COLORS_DICT

    expected_keys = {"merge", "error", "waiting", "default"}
    assert set(ADVANCE_COLORS_DICT.keys()) == expected_keys


def test_lineage_status_colors_keys() -> None:
    from gza.colors import LINEAGE_STATUS_COLORS

    expected_keys = {"completed", "failed", "pending", "in_progress", "unmerged", "dropped"}
    assert set(LINEAGE_STATUS_COLORS.keys()) == expected_keys


def test_ps_status_colors_keys() -> None:
    from gza.colors import PS_STATUS_COLORS

    expected_keys = {"running", "in_progress", "completed", "failed", "failed(startup)",
                     "stale", "unknown"}
    assert set(PS_STATUS_COLORS.keys()) == expected_keys




# ---------------------------------------------------------------------------
# StreamOutputFormatter default styles
# ---------------------------------------------------------------------------


def test_stream_formatter_uses_themed_singleton() -> None:
    import gza.colors as c
    from gza.providers.output_formatter import StreamOutputFormatter

    try:
        c.set_theme("minimal")
        formatter = StreamOutputFormatter()
        assert formatter.styles is c.TASK_STREAM_COLORS
        assert formatter.styles.step_header == c.TASK_STREAM_COLORS.step_header
    finally:
        c.set_theme(None)


def test_build_rich_theme_returns_none_when_empty() -> None:
    import gza.colors as c

    try:
        c.set_theme(None)
        assert c.build_rich_theme() is None
    finally:
        c.set_theme(None)


def test_build_rich_theme_populated_under_minimal() -> None:
    import gza.colors as c
    from rich.theme import Theme as RichTheme

    try:
        c.set_theme("minimal")
        rich_theme = c.build_rich_theme()
        assert isinstance(rich_theme, RichTheme)
        assert "repr.number" in c.RICH_STYLES_DICT
        assert "repr.path" in c.RICH_STYLES_DICT
    finally:
        c.set_theme(None)


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


# ---------------------------------------------------------------------------
# Theme system tests
# ---------------------------------------------------------------------------


def _reset_theme() -> None:
    """Reset colors module to default (no theme) state."""
    import gza.colors as c
    c.set_theme(None, None)


class TestBaseColors:
    """BaseColors dataclass has the right fields and defaults."""

    def test_base_colors_has_shared_fields(self) -> None:
        from gza.colors import BaseColors
        import dataclasses
        field_names = {f.name for f in dataclasses.fields(BaseColors)}
        assert field_names == {"task_id", "prompt", "stats", "branch", "label", "value", "heading"}

    def test_base_colors_defaults(self) -> None:
        from gza.colors import BaseColors, default_color
        bc = BaseColors()
        for f_name in ("task_id", "prompt", "stats", "branch", "label", "value", "heading"):
            assert getattr(bc, f_name) == default_color


class TestBuiltInThemes:
    """BUILT_IN_THEMES contains all three required themes."""

    def test_all_themes_present(self) -> None:
        from gza.colors import BUILT_IN_THEMES
        assert "default_dark" in BUILT_IN_THEMES
        assert "minimal" in BUILT_IN_THEMES
        assert "selective_neon" in BUILT_IN_THEMES
        assert "blue" in BUILT_IN_THEMES

    def test_theme_names_match_keys(self) -> None:
        from gza.colors import BUILT_IN_THEMES
        for key, theme in BUILT_IN_THEMES.items():
            assert theme.name == key


class TestSetTheme:
    """set_theme() correctly applies themes and resets to defaults."""

    def setup_method(self) -> None:
        _reset_theme()

    def teardown_method(self) -> None:
        _reset_theme()

    def test_default_theme_uses_dataclass_defaults(self) -> None:
        from gza.colors import TaskColors, ShowColors
        import gza.colors as c
        c.set_theme(None)
        assert c.TASK_COLORS == TaskColors()
        assert c.SHOW_COLORS == ShowColors()

    def test_set_theme_none_restores_defaults(self) -> None:
        import gza.colors as c
        from gza.colors import TaskColors
        c.set_theme("blue")
        c.set_theme(None)
        assert c.TASK_COLORS == TaskColors()

    def test_dict_variants_stay_in_sync(self) -> None:
        import gza.colors as c
        import dataclasses
        c.set_theme("blue")
        assert c.TASK_COLORS_DICT == dataclasses.asdict(c.TASK_COLORS)
        assert c.STATUS_COLORS_DICT == dataclasses.asdict(c.STATUS_COLORS)
        assert c.SHOW_COLORS_DICT == dataclasses.asdict(c.SHOW_COLORS)

    def test_lineage_status_colors_stays_in_sync(self) -> None:
        import gza.colors as c
        c.set_theme("default_dark")
        assert c.LINEAGE_STATUS_COLORS["completed"] == c.STATUS_COLORS.completed
        assert c.LINEAGE_STATUS_COLORS["failed"] == c.STATUS_COLORS.failed

    def test_ps_status_colors_stays_in_sync(self) -> None:
        import gza.colors as c
        c.set_theme("default_dark")
        assert c.PS_STATUS_COLORS["completed"] == c.STATUS_COLORS.completed
        assert c.PS_STATUS_COLORS["failed"] == c.STATUS_COLORS.failed


class TestBaseColorsPropagation:
    """Theme base overrides propagate to all domain classes with the same field."""

    def setup_method(self) -> None:
        _reset_theme()

    def teardown_method(self) -> None:
        _reset_theme()

    def test_base_task_id_applies_to_all_classes(self) -> None:
        import gza.colors as c
        from gza.colors import Theme, BUILT_IN_THEMES

        # Apply a custom theme with only a base override for task_id
        custom_theme = Theme(name="_test_base_propagate", base={"task_id": "#ff0000"})
        # Temporarily register and use it
        BUILT_IN_THEMES["_test_base_propagate"] = custom_theme
        try:
            c.set_theme("_test_base_propagate")
            # All domain classes with task_id should have been overridden
            assert c.TASK_COLORS.task_id == "#ff0000"
            assert c.SHOW_COLORS.task_id == "#ff0000"
            assert c.UNMERGED_COLORS.task_id == "#ff0000"
            assert c.LINEAGE_COLORS.task_id == "#ff0000"
            assert c.NEXT_COLORS.task_id == "#ff0000"
        finally:
            del BUILT_IN_THEMES["_test_base_propagate"]

    def test_base_prompt_applies_to_all_classes(self) -> None:
        import gza.colors as c
        from gza.colors import Theme, BUILT_IN_THEMES

        custom_theme = Theme(name="_test_base_prompt", base={"prompt": "#00ff00"})
        BUILT_IN_THEMES["_test_base_prompt"] = custom_theme
        try:
            c.set_theme("_test_base_prompt")
            assert c.TASK_COLORS.prompt == "#00ff00"
            assert c.SHOW_COLORS.prompt == "#00ff00"
            assert c.UNMERGED_COLORS.prompt == "#00ff00"
            assert c.LINEAGE_COLORS.prompt == "#00ff00"
            assert c.NEXT_COLORS.prompt == "#00ff00"
        finally:
            del BUILT_IN_THEMES["_test_base_prompt"]

    def test_base_non_shared_fields_ignored(self) -> None:
        """Fields not in BaseColors are not affected by base overrides."""
        import gza.colors as c
        from gza.colors import Theme, BUILT_IN_THEMES, StatusColors

        custom_theme = Theme(name="_test_base_ignore", base={"task_id": "#ff0000"})
        BUILT_IN_THEMES["_test_base_ignore"] = custom_theme
        try:
            c.set_theme("_test_base_ignore")
            # StatusColors has no task_id, so it's unaffected by the base override
            assert c.STATUS_COLORS == StatusColors()
        finally:
            del BUILT_IN_THEMES["_test_base_ignore"]


class TestDomainOverridePrecedence:
    """Domain-specific theme overrides take precedence over base overrides."""

    def setup_method(self) -> None:
        _reset_theme()

    def teardown_method(self) -> None:
        _reset_theme()

    def test_domain_overrides_base(self) -> None:
        import gza.colors as c
        from gza.colors import Theme, BUILT_IN_THEMES

        custom_theme = Theme(
            name="_test_domain_prio",
            base={"task_id": "#aaaaaa"},
            task={"task_id": "#ff0000"},  # domain-specific overrides base
        )
        BUILT_IN_THEMES["_test_domain_prio"] = custom_theme
        try:
            c.set_theme("_test_domain_prio")
            # TaskColors gets domain-specific value
            assert c.TASK_COLORS.task_id == "#ff0000"
            # ShowColors gets base value (no domain override)
            assert c.SHOW_COLORS.task_id == "#aaaaaa"
        finally:
            del BUILT_IN_THEMES["_test_domain_prio"]

    def test_domain_override_does_not_affect_other_classes(self) -> None:
        import gza.colors as c
        from gza.colors import Theme, BUILT_IN_THEMES

        custom_theme = Theme(
            name="_test_domain_isolated",
            task={"task_id": "#112233"},
        )
        BUILT_IN_THEMES["_test_domain_isolated"] = custom_theme
        try:
            c.set_theme("_test_domain_isolated")
            assert c.TASK_COLORS.task_id == "#112233"
            # Other classes not overridden — use their own defaults
            from gza.colors import ShowColors, UnmergedColors
            assert c.SHOW_COLORS.task_id == ShowColors().task_id
            assert c.UNMERGED_COLORS.task_id == UnmergedColors().task_id
        finally:
            del BUILT_IN_THEMES["_test_domain_isolated"]


class TestAdHocColorOverrides:
    """Ad-hoc color_overrides layer on top of a theme (highest priority)."""

    def setup_method(self) -> None:
        _reset_theme()

    def teardown_method(self) -> None:
        _reset_theme()

    def test_override_applies_to_all_classes_with_field(self) -> None:
        import gza.colors as c
        c.set_theme(None, {"task_id": "#ffffff"})
        assert c.TASK_COLORS.task_id == "#ffffff"
        assert c.SHOW_COLORS.task_id == "#ffffff"
        assert c.UNMERGED_COLORS.task_id == "#ffffff"

    def test_override_beats_theme_domain(self) -> None:
        import gza.colors as c
        from gza.colors import Theme, BUILT_IN_THEMES

        custom_theme = Theme(
            name="_test_override_prio",
            base={"task_id": "#aaaaaa"},
            task={"task_id": "#bbbbbb"},
        )
        BUILT_IN_THEMES["_test_override_prio"] = custom_theme
        try:
            c.set_theme("_test_override_prio", {"task_id": "#cccccc"})
            assert c.TASK_COLORS.task_id == "#cccccc"  # override beats domain
            assert c.SHOW_COLORS.task_id == "#cccccc"  # override beats base
        finally:
            del BUILT_IN_THEMES["_test_override_prio"]

    def test_override_only_affects_named_fields(self) -> None:
        import gza.colors as c
        from gza.colors import TaskColors
        c.set_theme(None, {"task_id": "#ffffff"})
        # Unrelated fields retain their defaults
        assert c.TASK_COLORS.prompt == TaskColors().prompt
        assert c.TASK_COLORS.branch == TaskColors().branch

    def test_override_with_no_theme(self) -> None:
        import gza.colors as c
        c.set_theme(None, {"prompt": "#123456"})
        assert c.TASK_COLORS.prompt == "#123456"
        assert c.SHOW_COLORS.prompt == "#123456"


class TestThemeUniform:
    """Theme.uniform() creates a theme with every field set to one color."""

    def test_uniform_sets_all_base_fields(self) -> None:
        from gza.colors import Theme, BaseColors
        import dataclasses
        t = Theme.uniform("test", "#ff00ff")
        for f in dataclasses.fields(BaseColors):
            assert t.base[f.name] == "#ff00ff"

    def test_uniform_sets_all_domain_fields(self) -> None:
        from gza.colors import (
            Theme, TaskColors, StatusColors, TaskStreamColors,
            ShowColors, UnmergedColors, LineageColors, NextColors,
        )
        import dataclasses
        t = Theme.uniform("test", "#abcdef")
        for cls, attr in [
            (TaskColors, "task"), (StatusColors, "status"),
            (TaskStreamColors, "task_stream"), (ShowColors, "show"),
            (UnmergedColors, "unmerged"), (LineageColors, "lineage"),
            (NextColors, "next_colors"),
        ]:
            domain_dict = getattr(t, attr)
            for f in dataclasses.fields(cls):
                assert domain_dict[f.name] == "#abcdef", f"{attr}.{f.name}"

    def test_uniform_applied_via_set_theme(self) -> None:
        import gza.colors as c
        from gza.colors import Theme, BUILT_IN_THEMES
        t = Theme.uniform("_test_uniform", "#ff00ff")
        BUILT_IN_THEMES["_test_uniform"] = t
        try:
            c.set_theme("_test_uniform")
            assert c.TASK_COLORS.task_id == "#ff00ff"
            assert c.TASK_COLORS.prompt == "#ff00ff"
            assert c.STATUS_COLORS.completed == "#ff00ff"
            assert c.SHOW_COLORS.heading == "#ff00ff"
            assert c.LINEAGE_COLORS.connector == "#ff00ff"
            assert c.NEXT_COLORS.index == "#ff00ff"
        finally:
            del BUILT_IN_THEMES["_test_uniform"]
            c.set_theme(None)


class TestBuiltInThemeDefaultDark:
    """default_dark theme sets colors to light gray / white."""

    def setup_method(self) -> None:
        _reset_theme()

    def teardown_method(self) -> None:
        _reset_theme()

    def test_task_id_becomes_light_gray(self) -> None:
        import gza.colors as c
        from gza.colors import gray_light1
        c.set_theme("default_dark")
        assert c.TASK_COLORS.task_id == gray_light1

    def test_prompt_becomes_light_gray(self) -> None:
        import gza.colors as c
        from gza.colors import gray_light1
        c.set_theme("default_dark")
        assert c.TASK_COLORS.prompt == gray_light1
        assert c.SHOW_COLORS.prompt == gray_light1

    def test_unoverridden_fields_still_present(self) -> None:
        """set_theme does not add unexpected fields."""
        import gza.colors as c
        import dataclasses
        from gza.colors import TaskColors
        c.set_theme("default_dark")
        themed_keys = {f.name for f in dataclasses.fields(c.TASK_COLORS)}
        default_keys = {f.name for f in dataclasses.fields(TaskColors())}
        assert themed_keys == default_keys


class TestBuiltInThemeSelectiveNeon:
    """selective_neon theme changes only a few fields to bright neon colors."""

    def setup_method(self) -> None:
        _reset_theme()

    def teardown_method(self) -> None:
        _reset_theme()

    def test_task_id_becomes_neon_blue(self) -> None:
        import gza.colors as c
        from gza.colors import blue_neon
        c.set_theme("selective_neon")
        assert c.TASK_COLORS.task_id == blue_neon
        assert c.SHOW_COLORS.task_id == blue_neon

    def test_non_overridden_fields_keep_defaults(self) -> None:
        import gza.colors as c
        from gza.colors import TaskColors
        c.set_theme("selective_neon")
        # stats and branch are not touched by selective_neon
        assert c.TASK_COLORS.stats == TaskColors().stats
        assert c.TASK_COLORS.branch == TaskColors().branch
        assert c.TASK_COLORS.success == TaskColors().success


class TestBuiltInThemeBlue:
    """blue theme applies 3–4 shades of blue to a handful of fields."""

    def setup_method(self) -> None:
        _reset_theme()

    def teardown_method(self) -> None:
        _reset_theme()

    def test_task_id_is_blue(self) -> None:
        import gza.colors as c
        from gza.colors import blue_bright
        c.set_theme("blue")
        assert c.TASK_COLORS.task_id == blue_bright

    def test_branch_is_blue(self) -> None:
        import gza.colors as c
        from gza.colors import blue_neon
        c.set_theme("blue")
        assert c.TASK_COLORS.branch == blue_neon

    def test_non_overridden_fields_keep_defaults(self) -> None:
        import gza.colors as c
        from gza.colors import TaskColors
        c.set_theme("blue")
        assert c.TASK_COLORS.success == TaskColors().success
        assert c.TASK_COLORS.failure == TaskColors().failure
        assert c.TASK_COLORS.prompt == TaskColors().prompt


class TestNoModuleLevelConfigRead:
    """Importing gza.colors must not read gza.yaml from CWD."""

    def test_import_does_not_read_cwd_gza_yaml(self, tmp_path: "pytest.TempdirFactory") -> None:  # type: ignore[name-defined]
        """Module-level _load_theme_from_config side-effect must not exist."""
        import os
        import importlib
        import sys

        # Write a gza.yaml with theme: blue in tmp_path
        gza_yaml = tmp_path / "gza.yaml"
        gza_yaml.write_text("project_name: test\ntheme: blue\n")

        orig_dir = os.getcwd()
        try:
            os.chdir(tmp_path)
            # Force a fresh import by removing cached module
            sys.modules.pop("gza.colors", None)
            import gza.colors as c
            importlib.reload(c)
            # Default colors must be intact — blue theme NOT auto-applied
            from gza.colors import TaskColors
            assert c.TASK_COLORS == TaskColors(), (
                "gza.colors auto-applied theme from CWD gza.yaml — "
                "module-level config read must be removed"
            )
        finally:
            os.chdir(orig_dir)
            sys.modules.pop("gza.colors", None)


class TestConfigThemeIntegration:
    """Config.load() parses theme and colors, and calls set_theme()."""

    def setup_method(self) -> None:
        _reset_theme()

    def teardown_method(self) -> None:
        _reset_theme()

    def test_config_with_no_theme_uses_minimal(self, tmp_path: "pytest.TempdirFactory") -> None:  # type: ignore[name-defined]
        import gza.colors as c
        from gza.config import Config
        from gza.colors import gray_secondary

        cfg_file = tmp_path / "gza.yaml"
        cfg_file.write_text("project_name: test\n")
        Config.load(tmp_path)
        # minimal theme sets task_id to blue_bright via base
        from gza.colors import blue_bright
        assert c.TASK_COLORS.task_id == blue_bright

    def test_config_with_valid_theme_applies_it(self, tmp_path: "pytest.TempdirFactory") -> None:  # type: ignore[name-defined]
        import gza.colors as c
        from gza.config import Config
        from gza.colors import blue_bright

        cfg_file = tmp_path / "gza.yaml"
        cfg_file.write_text("project_name: test\ntheme: blue\n")
        Config.load(tmp_path)
        assert c.TASK_COLORS.task_id == blue_bright

    def test_config_with_invalid_theme_raises(self, tmp_path: "pytest.TempdirFactory") -> None:  # type: ignore[name-defined]
        from gza.config import Config, ConfigError

        cfg_file = tmp_path / "gza.yaml"
        cfg_file.write_text("project_name: test\ntheme: not_a_real_theme\n")
        with pytest.raises(ConfigError, match="theme"):
            Config.load(tmp_path)

    def test_config_with_adhoc_colors_applies_them(self, tmp_path: "pytest.TempdirFactory") -> None:  # type: ignore[name-defined]
        import gza.colors as c
        from gza.config import Config

        cfg_file = tmp_path / "gza.yaml"
        cfg_file.write_text("project_name: test\ncolors:\n  task_id: '#ff0000'\n  prompt: '#00ff00'\n")
        Config.load(tmp_path)
        assert c.TASK_COLORS.task_id == "#ff0000"
        assert c.TASK_COLORS.prompt == "#00ff00"

    def test_config_theme_stored_on_config_object(self, tmp_path: "pytest.TempdirFactory") -> None:  # type: ignore[name-defined]
        from gza.config import Config

        cfg_file = tmp_path / "gza.yaml"
        cfg_file.write_text("project_name: test\ntheme: blue\n")
        cfg = Config.load(tmp_path)
        assert cfg.theme == "blue"

    def test_config_colors_stored_on_config_object(self, tmp_path: "pytest.TempdirFactory") -> None:  # type: ignore[name-defined]
        from gza.config import Config

        cfg_file = tmp_path / "gza.yaml"
        cfg_file.write_text("project_name: test\ncolors:\n  task_id: '#aabbcc'\n")
        cfg = Config.load(tmp_path)
        assert cfg.colors == {"task_id": "#aabbcc"}

    def test_config_theme_and_colors_combined(self, tmp_path: "pytest.TempdirFactory") -> None:  # type: ignore[name-defined]
        """Ad-hoc colors override theme values when both are present."""
        import gza.colors as c
        from gza.config import Config
        from gza.colors import blue_bright

        cfg_file = tmp_path / "gza.yaml"
        # blue theme sets task_id to blue_bright; the colors override wins
        cfg_file.write_text(
            "project_name: test\ntheme: blue\ncolors:\n  task_id: '#deadbe'\n"
        )
        Config.load(tmp_path)
        assert c.TASK_COLORS.task_id == "#deadbe"
