"""Subprocess CLI pager-flag regression tests."""

from pathlib import Path

from tests.helpers.cli import run_gza_subprocess


def _setup_config(tmp_path: Path) -> None:
    (tmp_path / "gza.yaml").write_text("project_name: test-project\n")


class TestCliPageFlag:
    """Ensure --page flag is accepted by gza show and gza log."""

    def test_show_page_flag_is_recognized(self, tmp_path: Path) -> None:
        """gza show --page should not error with 'unrecognized argument'."""
        _setup_config(tmp_path)
        result = run_gza_subprocess("show", "testproject-99999", "--page", "--project", str(tmp_path))
        assert "unrecognized" not in result.stderr.lower()
        assert "not found" in (result.stdout + result.stderr).lower()

    def test_show_page_not_active_without_flag(self, tmp_path: Path) -> None:
        """gza show without --page should not produce argparse errors."""
        _setup_config(tmp_path)
        result = run_gza_subprocess("show", "testproject-99999", "--project", str(tmp_path))
        assert "unrecognized" not in result.stderr.lower()

    def test_log_page_flag_is_recognized(self, tmp_path: Path) -> None:
        """gza log --page should not error with 'unrecognized argument'."""
        _setup_config(tmp_path)
        result = run_gza_subprocess("log", "testproject-99999", "--page", "--project", str(tmp_path))
        assert "unrecognized" not in result.stderr.lower()
        assert "not found" in (result.stdout + result.stderr).lower()

    def test_log_page_not_active_without_flag(self, tmp_path: Path) -> None:
        """gza log without --page should not produce argparse errors."""
        _setup_config(tmp_path)
        result = run_gza_subprocess("log", "testproject-99999", "--project", str(tmp_path))
        assert "unrecognized" not in result.stderr.lower()

    def test_show_help_mentions_page(self, tmp_path: Path) -> None:
        """gza show --help should mention the --page flag."""
        result = run_gza_subprocess("show", "--help")
        assert result.returncode == 0
        assert "--page" in result.stdout

    def test_log_help_mentions_page(self, tmp_path: Path) -> None:
        """gza log --help should mention the --page flag."""
        result = run_gza_subprocess("log", "--help")
        assert result.returncode == 0
        assert "--page" in result.stdout
