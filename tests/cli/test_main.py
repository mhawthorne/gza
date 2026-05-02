"""Tests for CLI parser and help output."""


import importlib
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

    def test_top_level_help_hides_incomplete_command(self, tmp_path):
        """Top-level help should stop advertising `gza incomplete`."""
        setup_config(tmp_path)

        result = run_gza("--help", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "incomplete" not in result.stdout

    def test_incomplete_command_returns_deprecation_guidance(self, tmp_path):
        """Legacy `gza incomplete` should print directed replacements and fail closed."""
        setup_config(tmp_path)

        result = run_gza("incomplete", "--help", "--project", str(tmp_path))

        assert result.returncode == 2
        output = result.stdout + result.stderr
        assert "invalid choice" not in output
        assert "deprecated and no longer supported" in output
        assert "uv run gza unmerged" in output
        assert "uv run gza advance --unimplemented" in output
        assert "uv run gza history --status failed" in output
        assert "factual failed-task history" in output
        assert "uv run gza watch --restart-failed --dry-run" in output
        assert "uv run gza next --all" in output
        assert "/gza-summary" in output

    def test_incomplete_command_dispatches_through_hidden_parser(self, tmp_path, monkeypatch):
        """`gza incomplete` should dispatch through parsed `args.command`, not a raw argv trapdoor."""
        setup_config(tmp_path)

        cli_main_module = importlib.import_module("gza.cli.main")

        captured = {}

        def fake_cmd(args):
            captured["command"] = args.command
            captured["legacy_help"] = args.legacy_help
            captured["project_dir"] = args.project_dir
            return 2

        monkeypatch.setattr(cli_main_module, "cmd_incomplete_deprecated", fake_cmd)

        with patch.object(sys, "argv", ["gza", "incomplete", "--help", "--project", str(tmp_path)]):
            result = cli_main_module.main()

        assert result == 2
        assert captured == {
            "command": "incomplete",
            "legacy_help": True,
            "project_dir": tmp_path.resolve(),
        }

    def test_advance_help_shows_unimplemented_and_hides_plans_alias(self):
        """advance --help should show --unimplemented/--force and keep --plans hidden."""
        result = subprocess.run(
            ["uv", "run", "gza", "advance", "--help"],
            capture_output=True,
            text=True,
        )
        normalized_output = " ".join(result.stdout.split())

        assert result.returncode == 0
        assert "--unimplemented" in result.stdout
        assert "--force" in result.stdout
        assert (
            "List plan/explore source rows without implementation, preferring newer descendants per branch"
            in normalized_output
        )
        assert "With --unimplemented: queue implement tasks for the listed source rows" in normalized_output
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
        assert (
            "Maximum iterate iterations (each is a code-change task [implement/improve] plus its review)"
            in normalized_output
        )

    def test_iterate_accepts_internal_worker_id_flag(self, tmp_path):
        """Background iterate workers pass --worker-id; parser must accept it."""
        setup_config(tmp_path)

        result = run_gza("iterate", "gza-999999", "--worker-id", "w-test", "--project", str(tmp_path))

        assert result.returncode == 1
        output = result.stdout + (result.stderr or "")
        assert "unrecognized arguments" not in output
        assert "not found" in output.lower()

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

    def test_ps_docs_describe_worker_prune_without_task_reconciliation(self, tmp_path):
        """Operator docs for ps should match the non-reconciling startup path."""
        setup_config(tmp_path)

        docs_text = " ".join(Path("docs/configuration.md").read_text().split())

        assert "`gza ps`/`gza status` only prune dead worker metadata" in docs_text
        assert "On CLI startup, `in_progress` tasks are reconciled and auto-failed" not in docs_text

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

    def test_edit_help_and_docs_describe_non_pending_tag_only_restriction(self, tmp_path):
        """`edit --help` and docs should both explain the non-pending tag-only contract."""
        setup_config(tmp_path)

        help_result = run_gza("edit", "--help", "--project", str(tmp_path))

        assert help_result.returncode == 0
        normalized_help = " ".join(help_result.stdout.split())
        docs_text = " ".join(Path("docs/configuration.md").read_text().split())

        assert "Non-pending tasks may only use tag mutation flags" in normalized_help
        assert "Non-pending tasks may only use tag mutation flags" in docs_text
        assert "remain pending-only" in normalized_help
        assert "remain pending-only" in docs_text

        for flag in ("--add-tag", "--remove-tag", "--clear-tags", "--set-tags", "--group"):
            assert flag in normalized_help
            assert flag in docs_text

        for flag in ("--based-on", "--depends-on", "--review", "--pr", "--prompt-file", "--model"):
            assert flag in normalized_help
            assert flag in docs_text

    def test_sync_help_and_docs_describe_explicit_branch_and_pr_reconciliation(self, tmp_path):
        """`sync --help` and docs should keep sync as the broader explicit maintenance surface."""
        setup_config(tmp_path)

        sync_help = run_gza("sync", "--help", "--project", str(tmp_path))
        assert sync_help.returncode == 0

        help_text = " ".join(sync_help.stdout.split())
        docs_text = " ".join(Path("docs/configuration.md").read_text().split())

        assert "--git-only" in sync_help.stdout
        assert "--pr-only" in sync_help.stdout
        assert "--no-fetch" in sync_help.stdout
        assert "Skip `git fetch origin`; stale-PR auto-close is disabled without a fresh fetch" in help_text

        assert "### sync" in docs_text
        assert "gza sync [task_id ...] [options]" in docs_text
        assert "Use `gza unmerged` for the daily \"what still needs to be merged?\" check." in docs_text
        assert "`gza sync` remains the broader explicit branch and PR reconciliation command." in docs_text
        assert "The only GitHub-side exception outside `gza sync` is improve completion with `--review`" in docs_text
        assert "Run `gza sync` after those merges" in docs_text

    def test_improve_help_and_docs_describe_narrow_pr_sync_before_auto_review(self, tmp_path):
        """`improve --help` and docs should explain the same-branch push-before-review exception."""
        setup_config(tmp_path)

        improve_help = run_gza("improve", "--help", "--project", str(tmp_path))
        assert improve_help.returncode == 0

        help_text = " ".join(improve_help.stdout.split())
        docs_text = " ".join(Path("docs/configuration.md").read_text().split())

        expected = "if the branch already has an open PR, push same-branch improve commits first"
        assert expected in help_text
        assert expected in docs_text
        assert "If GitHub is unavailable, lookup fails, or no live PR exists, improve preserves the normal auto-review flow." in docs_text

    def test_watch_and_queue_tag_help_point_to_same_scoped_pickup_preview(self, tmp_path):
        """Help/docs should make `queue --tag` the preview for `watch --tag`."""
        setup_config(tmp_path)

        watch_help = run_gza("watch", "--help", "--project", str(tmp_path))
        queue_help = run_gza("queue", "--help", "--project", str(tmp_path))
        assert watch_help.returncode == 0
        assert queue_help.returncode == 0

        watch_text = " ".join(watch_help.stdout.split())
        queue_text = " ".join(queue_help.stdout.split())
        docs_text = " ".join(Path("docs/configuration.md").read_text().split())

        assert "use 'gza queue --tag TAG' to preview scoped pickup order" in watch_text
        assert "same scoped pickup order used by 'gza watch --tag TAG'" in queue_text
        assert "use `gza queue --tag TAG` to preview the same scoped pickup order" in docs_text
        assert "canonical preview for what `gza watch --tag release-1.2` will consider and in what order" in docs_text

    def test_watch_help_mentions_restart_failed_flags(self, tmp_path):
        """watch --help should advertise failed-recovery mode flags."""
        setup_config(tmp_path)
        result = run_gza("watch", "--help", "--project", str(tmp_path))
        assert result.returncode == 0
        text = " ".join(result.stdout.split())
        assert "--restart-failed" in text
        assert "--restart-failed-batch" in text
        assert "--max-resume-attempts" in text
        assert "--show-skipped" in text

    def test_watch_help_and_docs_describe_recovery_dry_run_and_attempt_scope(self, tmp_path):
        """watch help/docs should document the recovery dry-run surface and true attempt-cap scope."""
        setup_config(tmp_path)
        help_result = run_gza("watch", "--help", "--project", str(tmp_path))
        assert help_result.returncode == 0

        help_text = " ".join(help_result.stdout.split())
        docs_text = " ".join(Path("docs/configuration.md").read_text().split())
        failed_tasks_docs = " ".join(Path("docs/examples/failed-tasks.md").read_text().split())

        assert "with --restart-failed, print the failed-recovery report and exit" in help_text
        assert "auto-resume and --restart-failed recovery decisions" in help_text
        assert "include skipped failed tasks in the dry-run recovery report and live watch logs" in help_text

        assert "with `--restart-failed`, print the full failed-recovery report and exit" in docs_text
        assert "applies to plain-watch auto-resume and to `--restart-failed` resume/retry decisions" in docs_text
        assert "`gza watch --restart-failed --dry-run` is the recovery inspection surface" in docs_text
        assert "oldest-created failed task first" in docs_text
        assert "Skipped tasks are hidden by default" in docs_text
        assert "`--show-skipped` to include them" in docs_text
        assert "live watch logs" in docs_text

        assert "`gza watch --restart-failed --dry-run`" in failed_tasks_docs
        assert "Print the recovery decision report and exit" in failed_tasks_docs
        assert "--show-skipped" in failed_tasks_docs
        assert "`--max-resume-attempts` applies both to plain-watch auto-resume and to `--restart-failed` recovery decisions." in failed_tasks_docs

    def test_internal_advance_workflow_docs_describe_watch_failed_recovery(self, tmp_path):
        """Internal workflow docs should stay aligned with watch failed-task recovery behavior."""
        setup_config(tmp_path)

        docs_text = " ".join(Path("docs/internal/advance-workflow.md").read_text().split())

        assert "`--restart-failed`" in docs_text
        assert "drains actionable failed-task recovery before pending queue work" in docs_text
        assert "advance-driven improve recovery" in docs_text

    def test_queue_help_and_docs_describe_default_limit_and_all_overrides(self, tmp_path):
        """`queue --help` and docs should describe capped default output and all-task overrides."""
        setup_config(tmp_path)

        queue_help = run_gza("queue", "--help", "--project", str(tmp_path))
        assert queue_help.returncode == 0

        help_text = " ".join(queue_help.stdout.split())
        docs_text = " ".join(Path("docs/configuration.md").read_text().split())

        assert "Show first N runnable tasks (default: 10; use 0, -1, or --all for all)" in help_text
        assert "Show all runnable tasks" in help_text
        assert "Show first N runnable tasks (default: 10; use `0`, `-1`, or `--all` for all)" in docs_text
        assert "By default, `gza queue` shows the first 10 runnable tasks." in docs_text

    def test_queue_ordering_language_is_consistent_between_help_docs_and_tag_scope_behavior(self, tmp_path):
        """Queue docs/help should consistently describe tag-scoped explicit ordering semantics."""
        setup_config(tmp_path)

        queue_help = run_gza("queue", "--help", "--project", str(tmp_path))
        assert queue_help.returncode == 0

        queue_help_text = " ".join(queue_help.stdout.split())
        docs_text = " ".join(Path("docs/configuration.md").read_text().split())
        assert "Assign an explicit queue position (with --tag scope shared across matching tasks; fails if target does not match scope)" in queue_help_text
        assert "Move a pending task to explicit queue position 1 (with --tag scope shared across matching tasks; fails if target does not match scope)" in queue_help_text
        assert "When `queue move`, `queue next`, or `queue clear` include `--tag` filters, explicit ordering is shared across all tasks matching that tag scope" in docs_text
        assert "Those commands fail closed when the target task does not match the provided tag scope" in docs_text
        assert "within each task's current tag-set bucket" not in docs_text

    def test_unmerged_help_and_docs_describe_daily_refresh_and_update_deprecation(self, tmp_path):
        """unmerged help/docs should expose the daily mutating default and `--update` deprecation."""
        setup_config(tmp_path)

        unmerged_help = run_gza("unmerged", "--help", "--project", str(tmp_path))
        assert unmerged_help.returncode == 0

        help_text = " ".join(unmerged_help.stdout.split())
        docs_text = " ".join(Path("docs/configuration.md").read_text().split())

        assert "--update" in unmerged_help.stdout
        assert "--into-current" in unmerged_help.stdout
        assert "--target BRANCH" in unmerged_help.stdout
        assert help_text.count("Retained compatibility no-op; has no effect on unmerged output") == 2
        assert "Show last N unmerged tasks (default: 5, 0 for all)" in help_text
        assert "Deprecated compatibility alias for the default default-branch refresh" in help_text
        assert "plain `gza unmerged` already persists canonical merge truth before listing" in help_text
        assert "Has no effect with `--into-current` or `--target`" in help_text

        assert "`gza unmerged` is the daily merge-truth command" in docs_text
        assert "`--commits-only` | Backwards-compatible no-op retained in the CLI surface" in docs_text
        assert "`--all` | Backwards-compatible no-op retained in the CLI surface" in docs_text
        assert "opens the task store read/write" in docs_text
        assert "This is the deliberate narrow exception to the usual read-only query convention" in docs_text
        assert "If the canonical default-branch refresh cannot persist because the database is read-only" in docs_text
        assert "`--update` is deprecated because plain `gza unmerged` now does the canonical refresh automatically" in docs_text
        assert "With `--into-current` or `--target`, `gza unmerged` always does ad hoc live git comparisons and leaves the database unchanged" in docs_text

    def test_show_help_and_docs_describe_prompt_as_plain_text(self, tmp_path):
        """`show --prompt` should be documented as plain prompt-text output, not JSON."""
        setup_config(tmp_path)

        show_help = run_gza("show", "--help", "--project", str(tmp_path))
        assert show_help.returncode == 0

        help_text = " ".join(show_help.stdout.split())
        docs_text = " ".join(Path("docs/configuration.md").read_text().split())

        assert "Print only the fully built prompt text for this task and exit" in help_text
        assert "as JSON" not in help_text
        assert "| `--prompt` | Print only the fully built prompt text for this task and exit |" in docs_text

    def test_groups_alias_docs_and_dispatch_remain_aligned(self, tmp_path):
        """`gza groups` docs invocation should match runtime deprecation-alias behavior."""
        setup_config(tmp_path)
        store = SqliteTaskStore(tmp_path / ".gza" / "gza.db")
        store.add("Grouped task", tags=("release",))

        docs_text = " ".join(Path("docs/configuration.md").read_text().split())
        assert "### groups" in docs_text
        assert "gza groups" in docs_text

        result = run_gza("groups", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Warning: 'gza groups' is deprecated; use 'gza groups list'." in result.stdout
        assert "release" in result.stdout
        assert "usage:" not in result.stdout

    def test_group_help_and_docs_describe_view_modes(self, tmp_path):
        """group --help and docs should advertise query presentation modes."""
        setup_config(tmp_path)

        group_help = run_gza("group", "--help", "--project", str(tmp_path))
        assert group_help.returncode == 0

        help_text = " ".join(group_help.stdout.split())
        docs_text = " ".join(Path("docs/configuration.md").read_text().split())

        assert "--view" in help_text
        assert "{flat,lineage,tree,json}" in help_text
        assert "Presentation mode (default: flat)" in help_text
        assert "| `--view MODE` | Presentation mode: `flat`, `lineage`, `tree`, or `json` (default: `flat`) |" in docs_text

    def test_search_command_help_mentions_prompt_substring_scope(self, tmp_path):
        """`search --help` should describe prompt-only substring matching."""
        setup_config(tmp_path)

        result = run_gza("search", "--help", "--project", str(tmp_path))
        normalized_output = " ".join(result.stdout.split())

        assert result.returncode == 0
        assert "Substring to match in task prompt text" in normalized_output
        assert "Show last N matching tasks" in normalized_output

    def test_improve_help_mentions_fix_ids(self, tmp_path):
        """`improve --help` should advertise fix-task ID resolution support."""
        setup_config(tmp_path)

        result = run_gza("improve", "--help", "--project", str(tmp_path))
        normalized_output = " ".join(result.stdout.split())

        assert result.returncode == 0
        assert "implement, improve, review, or fix" in normalized_output

    def test_review_help_mentions_fix_ids(self, tmp_path):
        """`review --help` should advertise fix-task ID resolution support."""
        setup_config(tmp_path)

        result = run_gza("review", "--help", "--project", str(tmp_path))
        normalized_output = " ".join(result.stdout.split())

        assert result.returncode == 0
        assert "implement, improve, review, or fix" in normalized_output

    def test_implement_help_does_not_expose_depends_on_flag(self, tmp_path):
        """`implement --help` should match parser behavior and omit removed --depends-on."""
        setup_config(tmp_path)

        result = run_gza("implement", "--help", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "--depends-on" not in result.stdout

class TestReconciliationWarnings:
    """Tests for reconciliation failure visibility during CLI dispatch."""

    def test_main_warns_and_continues_when_reconciliation_raises(self, tmp_path, capsys: pytest.CaptureFixture[str]):
        """Dispatch should continue even if reconciliation fails unexpectedly."""
        from gza.cli.main import main

        setup_config(tmp_path)

        with (
            patch.object(sys, "argv", ["gza", "work", "--project", str(tmp_path)]),
            patch("gza.cli.main.reconcile_in_progress_tasks", side_effect=RuntimeError("boom")),
            patch("gza.cli.main.cmd_run", return_value=0),
        ):
            rc = main()

        captured = capsys.readouterr()
        assert rc == 0
        assert "Warning: In-progress reconciliation failed: boom" in captured.err

    @pytest.mark.parametrize(
        ("command", "patched_command"),
        [
            ("ps", "cmd_ps"),
            ("status", "cmd_status"),
        ],
    )
    def test_main_skips_task_reconciliation_for_query_worker_views(
        self,
        tmp_path,
        command: str,
        patched_command: str,
    ) -> None:
        """ps/status should prune worker metadata without reconciling DB task state."""
        from gza.cli.main import main

        setup_config(tmp_path)

        with (
            patch.object(sys, "argv", ["gza", command, "--project", str(tmp_path)]),
            patch("gza.cli.main.reconcile_in_progress_tasks") as reconcile,
            patch("gza.cli.main.prune_terminal_dead_workers") as prune,
            patch(f"gza.cli.main.{patched_command}", return_value=0),
        ):
            rc = main()

        assert rc == 0
        reconcile.assert_not_called()
        prune.assert_called_once()

    def test_main_reconciles_for_work_commands(self, tmp_path) -> None:
        """Mutating lifecycle commands should still reconcile stale in-progress tasks on startup."""
        from gza.cli.main import main

        setup_config(tmp_path)

        with (
            patch.object(sys, "argv", ["gza", "work", "--project", str(tmp_path)]),
            patch("gza.cli.main.reconcile_in_progress_tasks") as reconcile,
            patch("gza.cli.main.prune_terminal_dead_workers") as prune,
            patch("gza.cli.main.cmd_run", return_value=0),
        ):
            rc = main()

        assert rc == 0
        reconcile.assert_called_once()
        prune.assert_not_called()


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

    @pytest.mark.parametrize(
        ("queue_action", "argv_tail"),
        [
            ("bump", ["test-project-1"]),
            ("unbump", ["test-project-1"]),
            ("next", ["test-project-1"]),
            ("clear", ["test-project-1"]),
            ("move", ["test-project-1", "2"]),
        ],
    )
    def test_queue_subcommands_dispatch_to_cmd_queue(self, tmp_path, queue_action, argv_tail):
        """`queue` subcommands should parse subcommand shape and route to cmd_queue."""
        from gza.cli.main import main

        setup_config(tmp_path)

        with (
            patch.object(
                sys,
                "argv",
                ["gza", "queue", queue_action, *argv_tail, "--project", str(tmp_path)],
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
        if queue_action == "move":
            assert args.position == 2


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
                ["gza", "extract", "--branch", "feature/source", "src/file.py", "--force"],
                "gza.cli.main.cmd_extract",
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
                ["gza", "run-inline", "testproject-1", "--force"],
                "gza.cli.main.cmd_run_inline",
            ),
            (
                ["gza", "improve", "testproject-1", "--force"],
                "gza.cli.main.cmd_improve",
            ),
            (
                ["gza", "fix", "testproject-1", "--force"],
                "gza.cli.main.cmd_fix",
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

        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\n"
            "project_id: default\n"
            "db_path: .gza/gza.db\n"
            "iterate_max_iterations: 6\n"
        )
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
        if restart_flag == "--resume":
            task.session_id = "resume-session"
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
