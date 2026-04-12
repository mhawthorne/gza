"""Tests for CLI parser and help output."""


import subprocess
import sys
from unittest.mock import patch

import pytest

from .conftest import run_gza, setup_config


class TestHelpOutput:
    """Tests for CLI help output."""

    def test_commands_displayed_alphabetically(self):
        """Help output should display commands in alphabetical order."""
        result = subprocess.run(
            ["uv", "run", "gza", "--help"],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0

        # Extract the commands section from help output
        help_text = result.stdout

        # Find where the commands list starts (after "positional arguments:" or "{")
        # Commands are typically shown as "{command1,command2,...}"
        import re

        # Look for the commands in the help output
        # They appear in a format like: {add,delete,edit,...}
        commands_match = re.search(r'\{([^}]+)\}', help_text)
        if not commands_match:
            # Alternative: commands listed line by line
            # Extract command names from lines that look like "  command_name  description"
            command_lines = []
            in_commands_section = False
            for line in help_text.split('\n'):
                if 'positional arguments:' in line or '{' in line:
                    in_commands_section = True
                    continue
                if in_commands_section and line.strip() and not line.startswith(' ' * 10):
                    # Extract command name (first word after leading spaces)
                    parts = line.strip().split()
                    if parts and not parts[0].startswith('-'):
                        command_lines.append(parts[0])
                if in_commands_section and line and not line.startswith(' '):
                    # End of commands section
                    break

            # Check if commands are sorted
            if command_lines:
                sorted_commands = sorted(command_lines)
                assert command_lines == sorted_commands, f"Commands not in alphabetical order. Got: {command_lines}, Expected: {sorted_commands}"
        else:
            # Commands are in {cmd1,cmd2,...} format
            commands_str = commands_match.group(1)
            commands = [cmd.strip() for cmd in commands_str.split(',')]

            # Verify commands are in alphabetical order
            sorted_commands = sorted(commands)
            assert commands == sorted_commands, f"Commands not in alphabetical order. Got: {commands}, Expected: {sorted_commands}"

    def test_history_lineage_depth_help_mentions_root_deduplicated_trees(self, tmp_path):
        """history --help should describe tree/root lineage semantics."""
        setup_config(tmp_path)

        result = run_gza("history", "--help", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Render root-deduplicated lineage trees up to N levels" in result.stdout
        assert "from each resolved root" in result.stdout
        assert "Expand lineage N levels for each matching task" not in result.stdout

    def test_advance_help_shows_unimplemented_and_hides_plans_alias(self):
        """advance --help should show --unimplemented and keep --plans hidden."""
        result = subprocess.run(
            ["uv", "run", "gza", "advance", "--help"],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert "--unimplemented" in result.stdout
        assert "--plans" not in result.stdout

class TestReconciliationWarnings:
    """Tests for reconciliation failure visibility during CLI dispatch."""

    def test_main_warns_and_continues_when_reconciliation_raises(self, tmp_path, capsys: pytest.CaptureFixture[str]):
        """Dispatch should continue even if reconciliation fails unexpectedly."""
        from gza.cli.main import main

        setup_config(tmp_path)

        with (
            patch.object(sys, "argv", ["gza", "ps", "--project", str(tmp_path)]),
            patch("gza.cli.main.reconcile_in_progress_tasks", side_effect=RuntimeError("boom")),
            patch("gza.cli.main.cmd_ps", return_value=0),
        ):
            rc = main()

        captured = capsys.readouterr()
        assert rc == 0
        assert "Warning: In-progress reconciliation failed: boom" in captured.err


class TestCommandAliases:
    """Tests for CLI command alias dispatch behavior."""

    def test_cycle_alias_dispatches_to_cmd_iterate(self, tmp_path):
        """Legacy `cycle` command should route to cmd_iterate."""
        from gza.cli.main import main

        setup_config(tmp_path)

        with (
            patch.object(sys, "argv", ["gza", "cycle", "testproject-1", "--dry-run", "--project", str(tmp_path)]),
            patch("gza.cli.main.cmd_iterate", return_value=0) as cmd_iterate,
        ):
            rc = main()

        assert rc == 0
        cmd_iterate.assert_called_once()
