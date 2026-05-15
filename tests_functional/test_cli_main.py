"""Subprocess CLI help regression tests."""

from tests_functional.helpers.cli import run_gza_subprocess


class TestHelpOutputSubprocess:
    def test_commands_displayed_alphabetically(self):
        """Help output should display commands in alphabetical order."""
        result = run_gza_subprocess("--help")

        assert result.returncode == 0
        help_text = result.stdout

        import re

        commands_match = re.search(r"\{([^}]+)\}", help_text)
        if not commands_match:
            command_lines = []
            in_commands_section = False
            for line in help_text.split("\n"):
                if "positional arguments:" in line or "{" in line:
                    in_commands_section = True
                    continue
                if in_commands_section and line.strip() and not line.startswith(" " * 10):
                    parts = line.strip().split()
                    if parts and not parts[0].startswith("-"):
                        command_lines.append(parts[0])
                if in_commands_section and line and not line.startswith(" "):
                    break

            if command_lines:
                sorted_commands = sorted(command_lines)
                assert command_lines == sorted_commands, (
                    f"Commands not in alphabetical order. Got: {command_lines}, "
                    f"Expected: {sorted_commands}"
                )
        else:
            commands = [cmd.strip() for cmd in commands_match.group(1).split(",")]
            sorted_commands = sorted(commands)
            assert commands == sorted_commands, (
                f"Commands not in alphabetical order. Got: {commands}, Expected: {sorted_commands}"
            )

    def test_advance_help_shows_unimplemented_and_hides_plans_alias(self):
        """advance --help should show --unimplemented/--force and keep --plans hidden."""
        result = run_gza_subprocess("advance", "--help")
        normalized_output = " ".join(result.stdout.split())

        assert result.returncode == 0
        assert "--unimplemented" in result.stdout
        assert "--force" in result.stdout
        assert (
            "List completed plan/explore source rows that still need an implementation path"
            in normalized_output
        )
        assert "preferring newer descendants per branch" not in normalized_output
        assert "With --unimplemented: queue implement tasks for the listed source rows" in normalized_output
        assert "--plans" not in result.stdout

    def test_module_entrypoint_propagates_main_return_codes(self, tmp_path):
        """python -m gza should preserve nonzero CLI statuses from main()."""
        result = run_gza_subprocess("next", "--project", str(tmp_path / "missing-project"))

        assert result.returncode == 1
        assert "is not a directory" in result.stdout
