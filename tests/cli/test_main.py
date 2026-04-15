"""Tests for CLI parser and help output."""


import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from gza.config import Config
from gza.db import SqliteTaskStore

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
        """advance --help should show --unimplemented/--force and keep --plans hidden."""
        result = subprocess.run(
            ["uv", "run", "gza", "advance", "--help"],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert "--unimplemented" in result.stdout
        assert "--force" in result.stdout
        assert "--plans" not in result.stdout

    def test_iterate_help_uses_lifecycle_wording_and_config_default(self, tmp_path):
        """iterate --help should keep lifecycle wording and describe config-backed max-iterations default."""
        setup_config(tmp_path)

        result = run_gza("iterate", "--help", "--project", str(tmp_path))
        normalized_output = " ".join(result.stdout.split())

        assert result.returncode == 0
        assert "implementation lifecycle loop" in normalized_output
        assert "for an implementation task" in normalized_output
        assert "for a task" not in normalized_output
        assert "Maximum iterate actions (default: iterate_max_iterations or 3)" in normalized_output
        assert "review/improve loop" not in normalized_output
        assert "default: 5" not in normalized_output

    def test_attach_help_and_docs_describe_provider_specific_attach(self, tmp_path):
        """Attach help/docs should reflect Claude interactive + Codex/Gemini observe-only semantics."""
        setup_config(tmp_path)

        result = run_gza("--help", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "interactive for Claude" in result.stdout
        assert "observe-only for Codex/Gemini" in result.stdout

        tmux_docs = Path("docs/tmux.md").read_text()
        config_docs = Path("docs/configuration.md").read_text()
        assert "GZA_ENABLE_TMUX_PROXY=1" in tmux_docs
        assert "Normal interactive Claude exit also auto-resumes in background." in tmux_docs
        assert "Attach to a running task." in config_docs

    def test_stats_help_no_longer_claims_reviews_only(self, tmp_path):
        """Help output should not imply `stats` only supports `reviews`."""
        setup_config(tmp_path)

        result = run_gza("--help", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Review and iteration analytics" in result.stdout
        assert "Review analytics (use 'gza stats reviews')" not in result.stdout

    def test_add_next_help_and_docs_describe_front_of_urgent_lane(self, tmp_path):
        """`add --next` contract should explicitly mention bump-to-front urgent-lane behavior."""
        setup_config(tmp_path)

        help_result = run_gza("add", "--help", "--project", str(tmp_path))
        assert help_result.returncode == 0
        normalized_help = " ".join(help_result.stdout.split())
        assert "front of the urgent lane" in normalized_help
        assert "picked up before normal queue items" not in normalized_help

        docs_text = " ".join(Path("docs/configuration.md").read_text().split())
        assert "front of the urgent lane" in docs_text
        assert "picked up before normal queue items" not in docs_text

    def test_work_pr_help_and_docs_are_aligned(self, tmp_path):
        """`work --pr` should be documented in CLI help and canonical configuration docs."""
        setup_config(tmp_path)

        help_result = run_gza("work", "--help", "--project", str(tmp_path))
        assert help_result.returncode == 0
        normalized_help = " ".join(help_result.stdout.split())
        assert "--pr" in help_result.stdout
        assert "Create/reuse a GitHub PR after successful code-task completion" in normalized_help

        docs_text = " ".join(Path("docs/configuration.md").read_text().split())
        assert "--pr" in docs_text
        assert "Create/reuse a GitHub PR for completed code tasks before auto-created review runs" in docs_text

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

    def test_watch_dispatches_to_cmd_watch(self, tmp_path):
        """`watch` command should parse args and dispatch to cmd_watch."""
        from gza.cli.main import main

        setup_config(tmp_path)

        with (
            patch.object(sys, "argv", ["gza", "watch", "--batch", "2", "--project", str(tmp_path)]),
            patch("gza.cli.main.cmd_watch", return_value=0) as cmd_watch,
        ):
            rc = main()

        assert rc == 0
        cmd_watch.assert_called_once()
        args = cmd_watch.call_args.args[0]
        assert args.command == "watch"
        assert args.batch == 2

    @pytest.mark.parametrize("queue_action", ["bump", "unbump"])
    def test_queue_subcommands_dispatch_to_cmd_queue(self, tmp_path, queue_action):
        """`queue bump|unbump` should parse subcommand shape and route to cmd_queue."""
        from gza.cli.main import main

        setup_config(tmp_path)

        with (
            patch.object(
                sys,
                "argv",
                ["gza", "queue", queue_action, "test-project-1", "--project", str(tmp_path)],
            ),
            patch("gza.cli.main.cmd_queue", return_value=0) as cmd_queue,
        ):
            rc = main()

        assert rc == 0
        cmd_queue.assert_called_once()
        args = cmd_queue.call_args.args[0]
        assert args.command == "queue"
        assert args.queue_action == queue_action
        assert args.task_id == "test-project-1"


class TestWorkForceBackgroundDispatch:
    """Command-level regression tests for work --force dispatch and propagation."""

    def test_work_force_background_propagates_to_worker_command(self, tmp_path):
        """`gza work --force --background` should propagate --force to worker subprocess args."""
        from gza.cli.main import main

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Pending task for background force run")
        assert task.id is not None

        captured_cmd: list[str] | None = None
        mock_proc = MagicMock()
        mock_proc.pid = 4242

        def capture_popen(cmd, **_kwargs):
            nonlocal captured_cmd
            captured_cmd = cmd
            return mock_proc

        with (
            patch.object(
                sys,
                "argv",
                [
                    "gza",
                    "work",
                    str(task.id),
                    "--background",
                    "--force",
                    "--no-docker",
                    "--project",
                    str(tmp_path),
                ],
            ),
            patch("gza.cli._common.subprocess.Popen", side_effect=capture_popen),
        ):
            rc = main()

        assert rc == 0
        assert captured_cmd is not None
        assert "--worker-mode" in captured_cmd
        assert "--force" in captured_cmd


class TestDirectExecutionForceDispatch:
    """Parser/dispatch coverage for --force on direct execution commands."""

    @pytest.mark.parametrize(
        ("argv", "command_patch"),
        [
            (
                ["gza", "implement", "testproject-1", "--force"],
                "gza.cli.main.cmd_implement",
            ),
            (
                ["gza", "retry", "testproject-1", "--force"],
                "gza.cli.main.cmd_retry",
            ),
            (
                ["gza", "resume", "testproject-1", "--force"],
                "gza.cli.main.cmd_resume",
            ),
            (
                ["gza", "improve", "testproject-1", "--force"],
                "gza.cli.main.cmd_improve",
            ),
            (
                ["gza", "iterate", "testproject-1", "--force"],
                "gza.cli.main.cmd_iterate",
            ),
        ],
    )
    def test_direct_execution_force_reaches_command_handler(self, tmp_path, argv, command_patch):
        """CLI should parse --force and pass it through args to the selected direct execution handler."""
        from gza.cli.main import main

        setup_config(tmp_path)

        with (
            patch.object(sys, "argv", [*argv, "--project", str(tmp_path)]),
            patch(command_patch, return_value=0) as cmd_handler,
        ):
            rc = main()

        assert rc == 0
        cmd_handler.assert_called_once()
        parsed_args = cmd_handler.call_args[0][0]
        assert parsed_args.force is True


class TestIterateBackgroundForceDispatch:
    """Command-level regression tests for iterate --background force propagation."""

    def test_iterate_force_background_propagates_to_worker_command(self, tmp_path):
        """`gza iterate --background --force` should retain --force in the detached iterate command."""
        from gza.cli.main import main

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Pending implement for iterate background", task_type="implement")
        assert task.id is not None

        captured_cmd: list[str] | None = None
        mock_proc = MagicMock()
        mock_proc.pid = 5252

        def capture_popen(cmd, **_kwargs):
            nonlocal captured_cmd
            captured_cmd = cmd
            return mock_proc

        with (
            patch.object(
                sys,
                "argv",
                [
                    "gza",
                    "iterate",
                    str(task.id),
                    "--background",
                    "--force",
                    "--no-docker",
                    "--project",
                    str(tmp_path),
                ],
            ),
            patch("gza.cli._common.subprocess.Popen", side_effect=capture_popen),
        ):
            rc = main()

        assert rc == 0
        assert captured_cmd is not None
        assert "--force" in captured_cmd

    def test_iterate_background_propagates_explicit_max_iterations(self, tmp_path):
        """`gza iterate --background --max-iterations N` should pass N unchanged to detached worker."""
        from gza.cli.main import main

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Pending implement for iterate background max-iterations", task_type="implement")
        assert task.id is not None

        captured_cmd: list[str] | None = None
        mock_proc = MagicMock()
        mock_proc.pid = 5353

        def capture_popen(cmd, **_kwargs):
            nonlocal captured_cmd
            captured_cmd = cmd
            return mock_proc

        with (
            patch.object(
                sys,
                "argv",
                [
                    "gza",
                    "iterate",
                    str(task.id),
                    "--background",
                    "--max-iterations",
                    "7",
                    "--no-docker",
                    "--project",
                    str(tmp_path),
                ],
            ),
            patch("gza.cli._common.subprocess.Popen", side_effect=capture_popen),
        ):
            rc = main()

        assert rc == 0
        assert captured_cmd is not None
        idx = captured_cmd.index("--max-iterations")
        assert captured_cmd[idx + 1] == "7"

    def test_iterate_background_uses_config_max_iterations_when_flag_omitted(self, tmp_path):
        """`gza iterate --background` should use iterate_max_iterations from config when -i is omitted."""
        from gza.cli.main import main

        (tmp_path / "gza.yaml").write_text("project_name: test-project\niterate_max_iterations: 6\n")
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Pending implement for iterate background config max-iterations", task_type="implement")
        assert task.id is not None

        captured_cmd: list[str] | None = None
        mock_proc = MagicMock()
        mock_proc.pid = 5454

        def capture_popen(cmd, **_kwargs):
            nonlocal captured_cmd
            captured_cmd = cmd
            return mock_proc

        with (
            patch.object(
                sys,
                "argv",
                [
                    "gza",
                    "iterate",
                    str(task.id),
                    "--background",
                    "--no-docker",
                    "--project",
                    str(tmp_path),
                ],
            ),
            patch("gza.cli._common.subprocess.Popen", side_effect=capture_popen),
        ):
            rc = main()

        assert rc == 0
        assert captured_cmd is not None
        idx = captured_cmd.index("--max-iterations")
        assert captured_cmd[idx + 1] == "6"

    def test_iterate_background_rejects_zero_max_iterations_before_spawn(self, tmp_path):
        """`gza iterate --background --max-iterations 0` should fail before detached worker spawn."""
        from gza.cli.main import main

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Pending implement for iterate background invalid max", task_type="implement")
        assert task.id is not None

        with (
            patch.object(
                sys,
                "argv",
                [
                    "gza",
                    "iterate",
                    str(task.id),
                    "--background",
                    "--max-iterations",
                    "0",
                    "--no-docker",
                    "--project",
                    str(tmp_path),
                ],
            ),
            patch("gza.cli._common.subprocess.Popen") as popen_mock,
        ):
            rc = main()

        assert rc == 1
        popen_mock.assert_not_called()

    @pytest.mark.parametrize("restart_flag", ["--resume", "--retry"])
    def test_iterate_restart_background_keeps_force_in_worker_command(self, tmp_path, restart_flag):
        """Restarting iterate in background should preserve --force alongside --resume/--retry."""
        from gza.cli.main import main

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Failed implement for iterate restart", task_type="implement")
        task.status = "failed"
        store.update(task)
        assert task.id is not None

        captured_cmd: list[str] | None = None
        mock_proc = MagicMock()
        mock_proc.pid = 6262

        def capture_popen(cmd, **_kwargs):
            nonlocal captured_cmd
            captured_cmd = cmd
            return mock_proc

        with (
            patch.object(
                sys,
                "argv",
                [
                    "gza",
                    "iterate",
                    str(task.id),
                    "--background",
                    restart_flag,
                    "--force",
                    "--no-docker",
                    "--project",
                    str(tmp_path),
                ],
            ),
            patch("gza.cli._common.subprocess.Popen", side_effect=capture_popen),
        ):
            rc = main()

        assert rc == 0
        assert captured_cmd is not None
        assert "--force" in captured_cmd
        assert restart_flag in captured_cmd


class TestIterateMaxIterationsValidation:
    """Command-level regression tests for iterate max-iterations bounds."""

    @pytest.mark.parametrize("value", ["0", "-1"])
    def test_iterate_rejects_non_positive_max_iterations(self, tmp_path, value):
        setup_config(tmp_path)
        result = run_gza("iterate", "testproject-1", "--max-iterations", value, "--project", str(tmp_path))

        assert result.returncode == 1
        assert "--max-iterations must be a positive integer" in result.stdout
