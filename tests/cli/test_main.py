"""Tests for CLI parser and help output."""


import importlib
import os
import re
import signal
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from gza.config import Config
from gza.db import SqliteTaskStore

from .conftest import run_gza, setup_config


def _normalized_markdown_section(path: Path, heading: str) -> str:
    """Return a whitespace-normalized level-3 markdown section by heading."""
    pattern = rf"^### {re.escape(heading)}\n(?P<section>.*?)(?=^### |\Z)"
    match = re.search(pattern, path.read_text(), re.MULTILINE | re.DOTALL)
    assert match is not None, f"Missing docs section: ### {heading}"
    return " ".join(match.group(0).split())


class TestHelpOutput:
    """Tests for CLI help output."""

    def test_migrate_import_local_db_dry_run_bootstraps_missing_shared_project_id_from_user_config(
        self, tmp_path: Path
    ) -> None:
        """Dry-run import should bootstrap legacy project_id when shared DB comes from user config."""
        home_dir = Path(os.environ["HOME"])
        shared_db = home_dir / ".gza" / "shared.db"
        user_config = home_dir / ".gza" / "config.yaml"
        user_config.parent.mkdir(parents=True, exist_ok=True)
        user_config.write_text(f"db_path: {shared_db}\n", encoding="utf-8")

        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: demo\n", encoding="utf-8")
        legacy_store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix="demo")
        legacy_store.add("legacy task")

        result = run_gza("migrate", "--import-local-db", "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "would persist missing project_id in gza.yaml" in result.stdout
        assert "Dry-run: legacy local DB import preview" in result.stdout
        assert "project_id: p" in result.stdout
        assert "project_id:" not in config_path.read_text(encoding="utf-8")

    def test_migrate_import_local_db_yes_bootstraps_missing_shared_project_id_from_user_config(
        self, tmp_path: Path
    ) -> None:
        """Real import should persist legacy project_id before importing when shared DB comes from user config."""
        home_dir = Path(os.environ["HOME"])
        shared_db = home_dir / ".gza" / "shared.db"
        user_config = home_dir / ".gza" / "config.yaml"
        user_config.parent.mkdir(parents=True, exist_ok=True)
        user_config.write_text(f"db_path: {shared_db}\n", encoding="utf-8")

        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: demo\n", encoding="utf-8")
        legacy_store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix="demo")
        legacy_store.add("legacy task")

        result = run_gza("migrate", "--import-local-db", "--yes", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Persisted project_id" in result.stdout
        assert "Imported legacy local DB into shared DB." in result.stdout
        assert "tasks_imported: 1" in result.stdout

        config = Config.load(tmp_path)
        assert f"project_id: {config.project_id}" in config_path.read_text(encoding="utf-8")
        shared_store = SqliteTaskStore(shared_db, prefix=config.project_prefix, project_id=config.project_id)
        assert [task.prompt for task in shared_store.get_all()] == ["legacy task"]

    def test_migrate_import_local_db_yes_reports_persist_os_error_without_traceback(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Import should surface project_id persistence failures as a controlled CLI error."""
        shared_db = tmp_path / "shared" / "gza.db"
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: demo\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )
        legacy_store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix="demo")
        legacy_store.add("legacy task")

        def _raise_persist_error(*_args, **_kwargs):
            raise OSError("disk full")

        cli_main_module = importlib.import_module("gza.cli.main")
        monkeypatch.setattr(cli_main_module, "bootstrap_missing_shared_project_id", _raise_persist_error)

        result = run_gza("migrate", "--import-local-db", "--yes", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "project_id could not be persisted to" in result.stderr
        assert str(config_path) in result.stderr
        assert "disk full" in result.stderr
        assert "Traceback" not in result.stderr
        assert "Persisted project_id" not in result.stdout
        assert "project_id:" not in config_path.read_text(encoding="utf-8")

    def test_migrate_import_local_db_dry_run_bootstraps_missing_shared_project_id(self, tmp_path: Path) -> None:
        """migrate --import-local-db should keep working for legacy shared configs without project_id."""
        shared_db = tmp_path / "shared" / "gza.db"
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: demo\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )
        legacy_store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix="demo")
        legacy_store.add("legacy task")

        result = run_gza("migrate", "--import-local-db", "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "would persist missing project_id in gza.yaml" in result.stdout
        assert "Dry-run: legacy local DB import preview" in result.stdout
        assert "project_id: p" in result.stdout
        assert "project_id:" not in config_path.read_text(encoding="utf-8")

    def test_history_lineage_depth_help_mentions_root_deduplicated_trees(self, tmp_path):
        """history --help should describe tree/root lineage semantics."""
        setup_config(tmp_path)

        result = run_gza("history", "--help", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Render root-deduplicated lineage trees up to N levels" in result.stdout
        assert "from each resolved root" in result.stdout
        assert "Expand lineage N levels for each matching task" not in result.stdout

    def test_queue_and_lifecycle_help_make_command_scope_explicit(self, tmp_path: Path) -> None:
        setup_config(tmp_path)

        next_help = run_gza("next", "--help", "--project", str(tmp_path))
        queue_help = run_gza("queue", "--help", "--project", str(tmp_path))
        work_help = run_gza("work", "--help", "--project", str(tmp_path))
        advance_help = run_gza("advance", "--help", "--project", str(tmp_path))
        watch_help = run_gza("watch", "--help", "--project", str(tmp_path))

        assert next_help.returncode == 0
        assert "recovery lane and pending lane separately" in next_help.stdout
        assert queue_help.returncode == 0
        assert "Preview recovery vs pending lanes separately" in queue_help.stdout
        assert work_help.returncode == 0
        assert "does not run recovery or review/merge lifecycle work" in work_help.stdout
        assert advance_help.returncode == 0
        assert "use --new to also start pending tasks" in advance_help.stdout
        assert watch_help.returncode == 0
        assert "run recovery, lifecycle, and pending pickup" in watch_help.stdout

    def test_history_and_search_help_list_negative_query_filters(self, tmp_path):
        """Query help should advertise the explicit negative filter flags."""
        setup_config(tmp_path)

        history_help = run_gza("history", "--help", "--project", str(tmp_path))
        search_help = run_gza("search", "--help", "--project", str(tmp_path))
        docs_text = Path("docs/configuration.md").read_text()

        assert history_help.returncode == 0
        assert "--status-not" in history_help.stdout
        assert "--type-not" in history_help.stdout
        assert "--tag-not" in history_help.stdout
        assert "--preset" not in history_help.stdout
        assert "works in text or JSON mode" in history_help.stdout
        assert "--list-fields" in history_help.stdout
        assert "--" + "incomplete" not in history_help.stdout
        assert "--tag/--tag-not values" in history_help.stdout
        assert search_help.returncode == 0
        assert "--status-not" in search_help.stdout
        assert "--type-not" in search_help.stdout
        assert "--tag-not" in search_help.stdout
        assert "--preset" not in search_help.stdout
        assert "works in text or JSON mode" in search_help.stdout
        assert "--list-fields" in search_help.stdout
        assert "--related-to-not" in search_help.stdout
        assert "Deprecated alias for --lineage-of" in search_help.stdout
        assert "--lineage-of-not" in search_help.stdout
        assert "--root-not" in search_help.stdout
        assert docs_text.count("| `--list-fields` | List valid `--fields` values for this command and exit |") >= 4

    def test_history_and_search_reject_removed_preset_flag(self, tmp_path):
        setup_config(tmp_path)

        history_result = run_gza("history", "--preset", "json_minimal", "--project", str(tmp_path))
        search_result = run_gza("search", "needle", "--preset", "json_minimal", "--project", str(tmp_path))

        assert history_result.returncode == 2
        assert "unrecognized arguments: --preset json_minimal" in history_result.stderr
        assert search_result.returncode == 2
        assert "unrecognized arguments: --preset json_minimal" in search_result.stderr

    def test_history_rejects_removed_legacy_flag(self, tmp_path):
        """history should reject the removed legacy flag."""
        setup_config(tmp_path)
        legacy_flag = "--" + "incomplete"

        result = run_gza("history", legacy_flag, "--project", str(tmp_path))

        assert result.returncode == 2
        assert f"unrecognized arguments: {legacy_flag}" in result.stderr

    @pytest.mark.parametrize(
        ("argv", "expected_value"),
        [
            (("history", "--status", "bogus"), "bogus"),
            (("history", "--status", "bad-value"), "bad-value"),
            (("add", "--type", "bogus"), "bogus"),
            (("add", "--type", "bad-value"), "bad-value"),
        ],
    )
    def test_valid_choice_flags_keep_argparse_invalid_choice_errors(
        self, tmp_path: Path, argv: tuple[str, ...], expected_value: str
    ) -> None:
        """Valid flags with invalid values should keep argparse's native invalid-choice wording."""
        setup_config(tmp_path)

        result = run_gza(*argv, "--project", str(tmp_path))

        assert result.returncode == 2
        assert f"invalid choice: '{expected_value}'" in result.stderr
        assert "unrecognized arguments" not in result.stderr
        assert "is not a gza command" not in result.stderr

    def test_top_level_help_shows_incomplete_command(self, tmp_path):
        """Top-level help should advertise `gza incomplete`."""
        setup_config(tmp_path)

        result = run_gza("--help", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "incomplete" in result.stdout

    def test_hidden_attach_command_is_absent_from_help_but_still_dispatches(
        self, tmp_path, monkeypatch, capsys
    ):
        """Hidden subcommands should disappear from top-level help while remaining callable."""
        setup_config(tmp_path)

        cli_main_module = importlib.import_module("gza.cli.main")

        with patch.object(sys, "argv", ["gza", "--help", "--project", str(tmp_path)]):
            with pytest.raises(SystemExit) as excinfo:
                cli_main_module.main()

        assert excinfo.value.code == 0
        help_output = capsys.readouterr().out
        command_block = re.search(r"\{([^}]+)\}", help_output)
        assert command_block is not None
        assert "attach" not in command_block.group(1).split(",")
        assert "\n  attach " not in help_output
        assert "attach                 ==SUPPRESS==" not in help_output

        captured = {}

        def fake_cmd_attach(args):
            captured["command"] = args.command
            captured["worker_id"] = args.worker_id
            captured["project_dir"] = args.project_dir
            return 0

        monkeypatch.setattr(cli_main_module, "cmd_attach", fake_cmd_attach)

        with patch.object(
            sys,
            "argv",
            ["gza", "attach", "w-20260301-1", "--project", str(tmp_path)],
        ):
            result = cli_main_module.main()

        assert result == 0
        assert captured == {
            "command": "attach",
            "worker_id": "w-20260301-1",
            "project_dir": tmp_path.resolve(),
        }

    def test_incomplete_command_help_describes_live_projection_surface(self, tmp_path):
        """`gza incomplete --help` should show the live projection options."""
        setup_config(tmp_path)

        result = run_gza("incomplete", "--help", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Show unresolved task lineages that still need attention" in result.stdout
        assert "--fields" in result.stdout
        assert "--list-fields" in result.stdout
        assert "--json" in result.stdout
        assert "--blocked-by-dropped" in result.stdout
        assert "live shared lifecycle planner" in result.stdout
        assert "deprecated and no longer supported" not in result.stdout
        assert "incomplete --blocked-by-dropped --list-fields" in Path("docs/configuration.md").read_text()
        assert result.stderr == ""

    def test_incomplete_command_dispatches_through_live_parser(self, tmp_path, monkeypatch):
        """`gza incomplete` should dispatch through parsed `args.command` to the live handler."""
        setup_config(tmp_path)

        cli_main_module = importlib.import_module("gza.cli.main")

        captured = {}

        def fake_cmd(args):
            captured["command"] = args.command
            captured["fields"] = args.fields
            captured["project_dir"] = args.project_dir
            return 0

        monkeypatch.setattr(cli_main_module, "cmd_incomplete", fake_cmd)

        with patch.object(sys, "argv", ["gza", "incomplete", "--fields", "id", "--project", str(tmp_path)]):
            result = cli_main_module.main()

        assert result == 0
        assert captured == {
            "command": "incomplete",
            "fields": "id",
            "project_dir": tmp_path.resolve(),
        }

    def test_advance_help_and_docs_describe_shared_failed_task_recovery_scope(self, tmp_path):
        """advance help/docs/config-key surfaces should describe shared failed-task recovery, not resume-only."""
        import json

        setup_config(tmp_path)

        help_result = run_gza("advance", "--help", "--project", str(tmp_path))
        assert help_result.returncode == 0
        help_text = " ".join(help_result.stdout.split())
        docs_text = " ".join(Path("docs/configuration.md").read_text().split())

        assert "Skip automatic failed-task recovery decisions (resume/retry/manual-review)" in help_text
        assert "Override max_resume_attempts (0 disables automatic failed-task recovery; any positive value enables the fixed bounded shared recovery policy)" in help_text
        assert "Skip auto-resume of resumable failed tasks" not in help_text

        assert "shared automatic failed-task recovery (resume/retry)" in docs_text
        assert "Skip automatic failed-task recovery decisions (resume/retry/manual-review)" in docs_text
        assert "Override `max_resume_attempts`: `0` disables automatic failed-task recovery; any positive value enables the fixed bounded shared resume/retry policy" in docs_text
        assert "Skip auto-resume of failed tasks" not in docs_text

        config_keys = run_gza("config", "keys", "--json", "--project", str(tmp_path))
        assert config_keys.returncode == 0
        payload = json.loads(config_keys.stdout)
        keyed_entries = {entry["key"]: entry for entry in payload["keys"]}
        assert (
            keyed_entries["max_resume_attempts"]["description"]
            == "Shared automatic failed-task recovery toggle: 0 disables; any positive value enables the fixed bounded resume/retry policy used by advance, iterate improve recovery, and watch."
        )

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
        assert "new attempt with a fresh conversation" in normalized_output
        assert "implement retries may fork fresh" in normalized_output
        assert "same-branch follow-ups stay on the shared branch" in normalized_output
        assert "starts fresh" not in normalized_output

    def test_iterate_accepts_internal_worker_flags(self, tmp_path):
        """Background iterate workers pass hidden flags; parser must accept them."""
        setup_config(tmp_path)

        result = run_gza(
            "iterate",
            "gza-999999",
            "--worker-id",
            "w-test",
            "--auto-iterate",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 1
        output = result.stdout + (result.stderr or "")
        assert "unrecognized arguments" not in output
        assert "not found" in output.lower()

    def test_attach_help_and_docs_describe_provider_specific_attach(self, tmp_path):
        """Attach help/docs should reflect Claude interactive + Codex/Gemini observe-only semantics."""
        setup_config(tmp_path)

        result = run_gza("attach", "--help", "--project", str(tmp_path))
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
        retired = "gza sta" "tus"

        assert "`gza ps` only prune dead worker metadata" in docs_text
        assert f"`{retired}`" not in docs_text
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

    def test_public_pr_help_and_docs_use_completion_time_request_wording(self, tmp_path):
        """Public `--pr` help/docs should use the same deferred request wording."""
        setup_config(tmp_path)

        expected = (
            "Request auto-create/reuse of a GitHub PR after successful code-task completion; "
            "evaluated at completion time and skipped without failing when PRs are unavailable"
        )

        for command, section in (
            ("work", "work"),
            ("add", "add"),
            ("edit", "edit"),
            ("improve", "improve"),
            ("implement", "implement"),
            ("extract", "extract"),
        ):
            help_result = run_gza(command, "--help", "--project", str(tmp_path))
            assert help_result.returncode == 0
            normalized_help = " ".join(help_result.stdout.split())
            docs_text = _normalized_markdown_section(Path("docs/configuration.md"), section)

            assert "--pr" in help_result.stdout
            assert expected in normalized_help
            assert expected in docs_text

        docs_text = " ".join(Path("docs/configuration.md").read_text().split())
        assert (
            "implement --based-on <plan_id> --review --pr` - Build per plan, auto-create review, and request PR "
            "creation/reuse at successful completion for later review comments when PRs are available"
        ) in docs_text
        assert "ensure a PR exists for later review comments" not in docs_text

    def test_edit_help_and_docs_describe_non_pending_tag_only_restriction(self, tmp_path):
        """`edit --help` and docs should both explain the non-pending tag-only contract."""
        setup_config(tmp_path)

        help_result = run_gza("edit", "--help", "--project", str(tmp_path))

        assert help_result.returncode == 0
        normalized_help = " ".join(help_result.stdout.split())
        docs_text = _normalized_markdown_section(Path("docs/configuration.md"), "edit")

        assert "Non-pending tasks may only use tag mutation flags" in normalized_help
        assert "Non-pending tasks may only use tag mutation flags" in docs_text
        assert "remain pending-only" in normalized_help
        assert "remain pending-only" in docs_text
        assert "--hold-for-review" in normalized_help
        assert "--no-hold-for-review" in normalized_help
        assert "--auto-implement" in normalized_help
        assert "--hold-for-review" in docs_text
        assert "--no-hold-for-review" in docs_text
        assert "--auto-implement" in docs_text
        assert "Compatibility alias for `--no-hold-for-review`" in normalized_help
        assert "--no-hold-for-review` (preferred) or `--auto-implement` (compatibility alias)" in docs_text

        for flag in ("--add-tag", "--remove-tag", "--clear-tags", "--set-tags"):
            assert flag in normalized_help
            assert flag in docs_text

        for flag in (
            "--based-on",
            "--depends-on",
            "--review",
            "--pr",
            "--prompt-file",
            "--model",
            "--provider",
            "--no-learnings",
        ):
            assert flag in normalized_help
            assert flag in docs_text

    def test_review_scope_help_and_docs_are_aligned_for_add_and_implement(self, tmp_path):
        """`--review-scope` should be documented anywhere CLI help exposes it."""
        setup_config(tmp_path)

        expected = "authoritative gradeable review"

        for command, section in (("add", "add"), ("implement", "implement")):
            help_result = run_gza(command, "--help", "--project", str(tmp_path))
            assert help_result.returncode == 0

            normalized_help = " ".join(help_result.stdout.split()).lower()
            docs_text = _normalized_markdown_section(Path("docs/configuration.md"), section).lower()

            assert "--review-scope" in help_result.stdout
            assert "--review-scope" in docs_text
            assert expected in normalized_help
            assert expected in docs_text

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
        assert "uv run gza sync [task_id ...] [options]" in docs_text
        assert "Use `uv run gza unmerged` for the daily \"what still needs to be merged?\" check." in docs_text
        assert "`uv run gza sync` remains the broader explicit branch and PR reconciliation command." in docs_text
        assert "When `pr_integration: true`, it also performs project-level `gh`-backed PR discovery/comment/create flows." in docs_text
        assert "Set `pr_integration: false` to disable those PR operations." in docs_text
        assert "The only GitHub-side exceptions outside `uv run gza sync` are improve and fix completion with `--review`" in docs_text
        assert "With `pr_integration: false`, those same branch-scoped PR checks are skipped." in docs_text
        assert "Run `uv run gza sync` after those merges" in docs_text

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

    def test_fix_help_and_docs_describe_narrow_pr_sync_before_auto_review(self, tmp_path):
        """`fix --help` and docs should explain the same-branch push-before-review exception."""
        setup_config(tmp_path)

        fix_help = run_gza("fix", "--help", "--project", str(tmp_path))
        assert fix_help.returncode == 0

        help_text = " ".join(fix_help.stdout.split())
        docs_text = " ".join(Path("docs/configuration.md").read_text().split())

        expected = "if the branch already has an open PR, push same-branch fix commits first"
        assert expected in help_text
        assert expected in docs_text
        assert "If GitHub is unavailable, lookup fails, or no live PR exists, fix preserves the normal auto-review flow." in docs_text

    def test_watch_and_queue_tag_help_point_to_same_scoped_pickup_preview(self, tmp_path):
        """Help/docs should describe queue tag scope as two lanes, with pending as watch's pickup order."""
        setup_config(tmp_path)

        watch_help = run_gza("watch", "--help", "--project", str(tmp_path))
        queue_help = run_gza("queue", "--help", "--project", str(tmp_path))
        assert watch_help.returncode == 0
        assert queue_help.returncode == 0

        watch_text = " ".join(watch_help.stdout.split())
        queue_text = " ".join(queue_help.stdout.split())
        docs_text = " ".join(Path("docs/configuration.md").read_text().split())

        assert "use 'uv run gza queue --tag TAG' to preview matching recovery candidates plus pending pickup order" in watch_text
        assert "pending lane uses the same scoped pickup order as 'uv run gza watch --tag TAG'" in queue_text
        assert "use 'gza queue --tag TAG' to preview scoped pickup order" not in watch_text
        assert "same scoped pickup order used by 'gza watch --tag TAG'" not in queue_text
        assert "Only list recovery and pending lanes matching tag filters" in docs_text
        assert "use `uv run gza queue --tag TAG` to preview matching recovery candidates plus the pending pickup order" in docs_text
        assert "canonical preview for what `uv run gza watch --tag release-1.2` will consider and in what order" in docs_text

    def test_watch_help_mentions_recovery_lane_flags(self, tmp_path):
        """watch --help should advertise the recovery lane controls."""
        setup_config(tmp_path)
        result = run_gza("watch", "--help", "--project", str(tmp_path))
        assert result.returncode == 0
        text = " ".join(result.stdout.split())
        assert "default: watch.batch or 2" in text
        assert "--recovery-slots" in text
        assert "--recovery-only" in text
        assert "--pending-only" in text
        assert "--restart-failed" not in text
        assert "--restart-failed-batch" not in text
        assert "--max-resume-attempts" in text
        assert "--show-skipped" in text
        assert "--resumed-reexec" not in text

    def test_watch_help_and_docs_describe_recovery_dry_run_and_attempt_scope(self, tmp_path):
        """watch help/docs should document the recovery dry-run surface and true attempt-cap scope."""
        setup_config(tmp_path)
        help_result = run_gza("watch", "--help", "--project", str(tmp_path))
        assert help_result.returncode == 0

        help_text = " ".join(help_result.stdout.split())
        docs_text = " ".join(Path("docs/configuration.md").read_text().split())
        failed_tasks_docs = " ".join(Path("docs/examples/failed-tasks.md").read_text().split())

        assert "with --recovery-only, print the failed-recovery report and exit" in help_text
        assert "0 disables automatic failed-task recovery; any positive value enables the fixed bounded shared recovery policy" in help_text
        assert "include skipped failed tasks in the dry-run recovery report and live watch logs" in help_text

        assert "with `--recovery-only`, print the full failed-recovery report and exit" in docs_text
        assert "Override `max_resume_attempts` for this watch run: `0` disables automatic failed-task recovery; any positive value enables the fixed bounded shared policy used by both plain watch and the recovery lane" in docs_text
        assert "`uv run gza watch --recovery-only --dry-run` is the recovery inspection surface" in docs_text
        assert "`watch.recovery_slots`" in docs_text
        assert "`--pending-only`" in docs_text
        assert "live watch logs" in docs_text

        assert "`uv run gza watch --recovery-only --dry-run`" in failed_tasks_docs
        assert "Print the recovery decision report and exit" in failed_tasks_docs
        assert "--show-skipped" in failed_tasks_docs
        assert "`--max-resume-attempts` controls that shared policy as a toggle" in failed_tasks_docs

    def test_watch_docs_distinguish_watch_batch_from_global_max_concurrent_fallback(self, tmp_path):
        """watch docs should explain that only an explicit watch.batch changes the global fallback cap."""
        setup_config(tmp_path)
        docs_text = " ".join(Path("docs/configuration.md").read_text().split())

        assert "when `max_concurrent` is unset, an explicitly configured `watch.batch` also becomes the global cap" in docs_text
        assert "otherwise the fallback global cap remains `5`" in docs_text

    def test_watch_help_and_docs_describe_next_pass_drift_restart(self, tmp_path):
        """watch help/docs should describe next-pass drift restart without drain gating."""
        setup_config(tmp_path)
        help_result = run_gza("watch", "--help", "--project", str(tmp_path))
        assert help_result.returncode == 0

        help_text = " ".join(help_result.stdout.split())
        docs_text = " ".join(Path("docs/configuration.md").read_text().split())
        internal_docs_text = " ".join(Path("docs/internal/advance-workflow.md").read_text().split())

        assert "Re-exec watch on the next pass boundary when the installed gza code changes" in help_text
        assert "drained batch boundary" not in help_text

        assert "re-exec at the next watch-pass boundary to load the new code without waiting for running or pending work to drain" in docs_text
        assert "Detached workers keep running, and the replacement watch process reconciles them after it auto-resumes" in docs_text
        assert "current batch drains and no worker remains running" not in docs_text

        assert "re-exec itself on the next watch-pass boundary without waiting for running or pending work to drain" in internal_docs_text
        assert "detached workers stay alive and the replacement process reconciles them after it auto-resumes" in internal_docs_text
        assert "current batch drains and no worker remains running" not in internal_docs_text

    def test_watch_help_and_docs_distinguish_max_idle_from_no_activity_timeout(self, tmp_path):
        """watch help/docs should distinguish loop idle exit from silent-worker reconciliation."""
        setup_config(tmp_path)

        help_result = run_gza("watch", "--help", "--project", str(tmp_path))
        assert help_result.returncode == 0

        help_text = " ".join(help_result.stdout.split())
        docs_text = " ".join(Path("docs/configuration.md").read_text().split())
        example_text = " ".join(Path("src/gza/gza.yaml.example").read_text().split())

        assert "Exit after SECS of consecutive idle watch cycles" in help_text
        assert "`watch.no_activity_timeout` controls when watch reconciliation marks a still-running worker `NO_ACTIVITY`" in docs_text
        assert "`watch.max_idle` keeps its existing meaning: it exits the `gza watch` loop itself after consecutive idle cycles." in docs_text
        assert "no_activity_timeout: 60" in example_text

    def test_internal_advance_workflow_docs_describe_watch_failed_recovery(self, tmp_path):
        """Internal workflow docs should stay aligned with watch failed-task recovery behavior."""
        setup_config(tmp_path)

        docs_text = " ".join(Path("docs/internal/advance-workflow.md").read_text().split())

        assert "`--recovery-only`" in docs_text
        assert "`watch.recovery_slots`" in docs_text
        assert "plain watch, failed-task recovery, and advance-driven improve recovery" in docs_text

    def test_watch_help_and_docs_lock_queue_priority_contract(self, tmp_path):
        """Help/docs should keep the two-lane recovery split wording aligned."""
        setup_config(tmp_path)

        help_result = run_gza("watch", "--help", "--project", str(tmp_path))
        assert help_result.returncode == 0

        help_text = " ".join(help_result.stdout.split())
        docs_text = " ".join(Path("docs/configuration.md").read_text().split())
        failed_tasks_docs = " ".join(Path("docs/examples/failed-tasks.md").read_text().split())
        internal_docs = " ".join(Path("docs/internal/advance-workflow.md").read_text().split())

        assert "reserved for failed-task recovery before pending pickup" in help_text
        assert "default `watch.recovery_slots = 1`" in docs_text
        assert "`uv run gza watch --recovery-only`" in failed_tasks_docs
        assert "`watch.recovery_slots`" in internal_docs

    def test_queue_help_and_docs_describe_default_limit_and_all_overrides(self, tmp_path):
        """`queue --help` and docs should describe capped default output and all-task overrides."""
        setup_config(tmp_path)

        queue_help = run_gza("queue", "--help", "--project", str(tmp_path))
        assert queue_help.returncode == 0

        help_text = " ".join(queue_help.stdout.split())
        docs_text = " ".join(Path("docs/configuration.md").read_text().split())

        assert "Show first N runnable tasks (default: 10; blocked tasks are always shown; use 0, -1, or --all for all runnable tasks)" in help_text
        assert "Show all runnable tasks (blocked tasks are always shown)" in help_text
        assert "Show first N runnable tasks (default: 10; blocked tasks are always shown; use `0`, `-1`, or `--all` for all runnable tasks)" in docs_text
        assert "By default, `gza queue` shows the first 10 runnable tasks plus all blocked tasks." in docs_text

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

    def test_unmerged_help_and_docs_describe_fetch_opt_in_and_fields_projection(self, tmp_path):
        """unmerged help/docs should expose the no-fetch default, opt-in fetch, and `--fields` projection."""
        setup_config(tmp_path)

        unmerged_help = run_gza("unmerged", "--help", "--project", str(tmp_path))
        assert unmerged_help.returncode == 0

        help_text = " ".join(unmerged_help.stdout.split())
        docs_raw = Path("docs/configuration.md").read_text()
        docs_text = " ".join(docs_raw.split())

        assert "--fetch" in unmerged_help.stdout
        assert "--into-current" in unmerged_help.stdout
        assert "--target BRANCH" in unmerged_help.stdout
        assert "--json" in unmerged_help.stdout
        assert "--fields CSV" in unmerged_help.stdout
        assert "--list-fields" in unmerged_help.stdout
        assert "Show last N unmerged tasks (default: 5, 0 for all)" in help_text
        assert "Fetch `origin` before the canonical default-branch refresh" in help_text
        assert "Has no effect with `--into-current` or `--target`" in help_text
        assert "Projection fields override" in help_text
        assert "works in text or JSON mode" in help_text
        assert "Output JSON rows from the unified query API" in help_text

        assert "uv run gza unmerged [options]" in docs_text
        assert "\ngza unmerged [options]\n" not in docs_raw
        assert "`uv run gza unmerged` is the daily merge-truth command" in docs_text
        assert "`--fetch` | Fetch `origin` before the canonical default-branch refresh" in docs_text
        assert "| `--json` | Output JSON rows from the unified query API |" in docs_text
        assert "| `--fields CSV` | Projection field override" in docs_text
        assert "| `--list-fields` | List valid `--fields` values for this command and exit |" in docs_text
        assert "opens the task store read/write" in docs_text
        assert "By default, plain `uv run gza unmerged` does not initiate network I/O" in docs_text
        assert "This is the deliberate narrow exception to the usual read-only query convention" in docs_text
        assert "If the canonical default-branch refresh cannot persist because the database is read-only" in docs_text
        assert "With `--into-current` or `--target`, `uv run gza unmerged` always does ad hoc live git comparisons and leaves the database unchanged" in docs_text
        assert "builds an unmerged-specific query preset and then renders that result through the shared query projection/presentation path" in docs_text
        assert "logs concise progress for the refresh, query, and render phases" in docs_text
        assert "showing only the selected branch-owner task and its descendants" in docs_text

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

    def test_artifact_and_clean_help_docs_stay_aligned_on_artifact_operator_surfaces(self, tmp_path: Path) -> None:
        """Artifact retrieval and cleanup docs should match parser help for artifact-aware behavior."""
        setup_config(tmp_path)

        artifact_help = run_gza("artifact", "--help", "--project", str(tmp_path))
        clean_help = run_gza("clean", "--help", "--project", str(tmp_path))
        docs_text = " ".join(Path("docs/configuration.md").read_text().split())

        assert artifact_help.returncode == 0
        assert "Print the latest stored task artifact content or path" in artifact_help.stdout
        assert "--kind KIND" in artifact_help.stdout
        assert "--latest" in artifact_help.stdout
        assert "--path" in artifact_help.stdout
        assert "### artifact" in docs_text
        assert "gza artifact <task_id> [options]" in docs_text
        assert "| `--kind KIND` | Filter artifacts by kind (for example `verify_command_output`) |" in docs_text
        assert "| `--latest` | Select the latest matching artifact (default behavior) |" in docs_text
        assert "Print only the resolved absolute artifact path when the latest row has a content file" in artifact_help.stdout
        assert (
            "| `--path` | Print only the resolved absolute artifact path when the latest row has a content file |"
            in docs_text
        )
        assert "both default content retrieval and `--path` fail clearly" in docs_text

        assert clean_help.returncode == 0
        assert "task artifacts" in clean_help.stdout
        assert "archived log, artifact, and worker files" in clean_help.stdout
        assert "Only clean up old log files and live task artifacts" in clean_help.stdout
        assert (
            "| `--logs` | Only clean up old log files (conversation `.log` and paired `.ops.jsonl` siblings together) and live task artifact files; archived artifacts are left for `--purge` |"
            in docs_text
        )
        assert "| `--keep-unmerged` | Keep logs and task artifacts for tasks that are still unmerged |" in docs_text
        assert "Archive old log, live task artifact, and worker files instead of deleting" in clean_help.stdout
        assert (
            "| `--archive` | Archive old log, live task artifact, and worker files instead of deleting; already archived artifacts are skipped |"
            in docs_text
        )
        assert "| `--purge` | Delete previously archived log, artifact, and worker files (default: older than 365 days) |" in docs_text

    def test_removed_group_commands_are_absent_from_docs_and_rejected(self, tmp_path):
        """Retired group command spellings should be undocumented and fail in argparse."""
        setup_config(tmp_path)

        docs_text = " ".join(Path("docs/configuration.md").read_text().split())
        assert "### groups" not in docs_text
        groups_cmd = "gza " + "groups"
        group_cmd = "gza " + "group"
        assert groups_cmd not in docs_text
        assert group_cmd not in docs_text

        for argv in (
            ("groups",),
            ("groups", "list"),
            ("groups", "rename", "old", "new"),
            ("group", "release"),
        ):
            result = run_gza(*argv, "--project", str(tmp_path))
            assert result.returncode == 2
            assert "invalid choice" in result.stderr
            assert "is not a gza command" not in result.stderr

    def test_retired_tag_alias_flag_is_rejected_on_all_former_cli_surfaces(self, tmp_path):
        """All retired group-flag spellings should fail in argparse."""
        setup_config(tmp_path)

        legacy_flag = "--" + "group"
        cases = (
            ("work", legacy_flag, "release"),
            ("next", legacy_flag, "release"),
            ("watch", legacy_flag, "release"),
            ("queue", legacy_flag, "release"),
            ("queue", "bump", "gza-1", legacy_flag, "release"),
            ("add", "Prompt", legacy_flag, "release"),
            ("edit", "gza-1", legacy_flag, "release"),
            ("implement", "gza-1", legacy_flag, "release"),
            ("extract", "gza-1", legacy_flag, "release"),
        )

        for argv in cases:
            result = run_gza(*argv, "--project", str(tmp_path))
            assert result.returncode == 2
            assert f"unrecognized arguments: {legacy_flag}" in result.stderr
            assert "invalid choice" not in result.stderr

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
        assert "approved slice manifest" in " ".join(result.stdout.split())

    def test_plan_review_and_plan_improve_help_cover_direct_commands(self, tmp_path):
        """Direct plan-review commands should be documented in parser help."""
        setup_config(tmp_path)

        review_help = run_gza("plan-review", "--help", "--project", str(tmp_path))
        improve_help = run_gza("plan-improve", "--help", "--project", str(tmp_path))

        assert review_help.returncode == 0
        assert improve_help.returncode == 0
        assert "--rerun" in review_help.stdout
        assert "--edit-slices" in review_help.stdout
        assert "--materialize" in review_help.stdout
        normalized_review_help = " ".join(review_help.stdout.split())
        assert "completed plan source" in normalized_review_help
        assert "completed plan_review" in normalized_review_help
        normalized_improve_help = " ".join(improve_help.stdout.split())
        assert "completed CHANGES_REQUESTED plan_review task ID to revise" in normalized_improve_help

    def test_extract_help_and_docs_describe_commit_mode(self, tmp_path):
        """`extract --help` and docs should document commit-based extraction and background ordering semantics."""
        setup_config(tmp_path)

        help_result = run_gza("extract", "--help", "--project", str(tmp_path))

        assert help_result.returncode == 0
        help_text = " ".join(help_result.stdout.split())
        docs_text = " ".join(Path("docs/configuration.md").read_text().split())

        assert "--commit REV" in help_result.stdout
        assert "--per-commit" in help_result.stdout
        assert "Committed git revision to extract from" in help_text
        assert "applied in the order provided" in help_text
        assert "create one extracted task per selected commit" in help_text
        assert "with --background, workers still start in parallel" in help_text

        assert "--commit REV" in docs_text
        assert "--per-commit" in docs_text
        assert "applied in the order provided" in docs_text
        assert "one extracted task per selected commit" in docs_text
        assert "execution starts in parallel rather than as a serialized commit-by-commit run" in docs_text

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
        "command",
        [
            "ps",
        ],
    )
    def test_main_skips_task_reconciliation_for_query_worker_views(
        self,
        tmp_path,
        command: str,
    ) -> None:
        """ps should prune worker metadata without reconciling DB task state."""
        from gza.cli.main import main

        setup_config(tmp_path)

        with (
            patch.object(sys, "argv", ["gza", command, "--project", str(tmp_path)]),
            patch("gza.cli.main.reconcile_in_progress_tasks") as reconcile,
            patch("gza.cli.main.prune_terminal_dead_workers") as prune,
            patch("gza.cli.main.cmd_ps", return_value=0),
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
    """Tests for CLI command dispatch behavior."""

    def test_status_command_is_rejected(self, tmp_path):
        """Removed `status` alias should fail at parser validation."""
        setup_config(tmp_path)
        retired = "sta" "tus"

        result = run_gza(retired, "--all", "--project", str(tmp_path))

        assert result.returncode == 2
        assert f"invalid choice: '{retired}'" in result.stderr
        assert "ps" in result.stderr

    def test_cycle_command_is_rejected(self, tmp_path):
        """Removed `cycle` command should now fail at parser validation."""
        setup_config(tmp_path)

        result = run_gza("cycle", "testproject-1", "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 2
        assert "invalid choice: 'cycle'" in result.stderr
        assert "iterate" in result.stderr

    def test_import_command_is_rejected(self, tmp_path):
        """Removed `import` command should now fail at parser validation."""
        setup_config(tmp_path)

        result = run_gza("import", "tasks.yaml", "--project", str(tmp_path))

        assert result.returncode == 2
        assert "invalid choice: 'import'" in result.stderr
        assert "add" in result.stderr

    def test_refresh_command_is_rejected(self, tmp_path):
        """Removed `refresh` command should now fail at parser validation."""
        setup_config(tmp_path)

        result = run_gza("refresh", "--project", str(tmp_path))

        assert result.returncode == 2
        assert "invalid choice: 'refresh'" in result.stderr
        assert "sync" in result.stderr

    def test_iterate_dispatches_to_cmd_iterate(self, tmp_path):
        """`iterate` command should parse args and dispatch to cmd_iterate."""
        from gza.cli.main import main

        setup_config(tmp_path)

        with (
            patch.object(sys, "argv", ["gza", "iterate", "testproject-1", "--dry-run", "--project", str(tmp_path)]),
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

    def test_watch_hidden_resumed_reexec_flag_still_parses(self, tmp_path):
        """Hidden internal watch flags should stay callable without appearing in help."""
        from gza.cli.main import main

        setup_config(tmp_path)

        with (
            patch.object(
                sys,
                "argv",
                ["gza", "watch", "--resumed-reexec", "--project", str(tmp_path)],
            ),
            patch("gza.cli.main.cmd_watch", return_value=0) as cmd_watch,
        ):
            rc = main()

        assert rc == 0
        cmd_watch.assert_called_once()
        args = cmd_watch.call_args.args[0]
        assert args.command == "watch"
        assert args.resumed_reexec is True

    @pytest.mark.parametrize(
        ("command", "argv_tail", "patch_target"),
        [
            ("watch", [], "gza.cli.main.cmd_watch"),
            ("iterate", ["testproject-1"], "gza.cli.main.cmd_iterate"),
        ],
    )
    def test_keyboard_interrupt_returns_130_without_traceback(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        command: str,
        argv_tail: list[str],
        patch_target: str,
    ) -> None:
        """Top-level dispatch should convert KeyboardInterrupt into a clean SIGINT exit."""
        from gza.cli.main import main

        setup_config(tmp_path)

        with (
            patch.object(sys, "argv", ["gza", command, *argv_tail, "--project", str(tmp_path)]),
            patch(patch_target, side_effect=KeyboardInterrupt),
        ):
            rc = main()

        captured = capsys.readouterr()
        assert rc == 130
        assert captured.out == ""
        assert captured.err == "stopping due to ctrl+c\n"
        assert "Traceback" not in captured.err

    def test_keyboard_interrupt_during_project_discovery_returns_130(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Project-dir discovery should use the same clean SIGINT exit path."""
        from gza.cli.main import main

        with (
            patch.object(sys, "argv", ["gza", "watch"]),
            patch("gza.cli.main.discover_project_dir", side_effect=KeyboardInterrupt),
        ):
            rc = main()

        captured = capsys.readouterr()
        assert rc == 130
        assert captured.out == ""
        assert captured.err == "stopping due to ctrl+c\n"
        assert "Traceback" not in captured.err

    def test_watch_second_sigint_escapes_to_top_level_clean_shutdown(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """A second Ctrl-C during watch should unwind promptly to the top-level 130 exit."""
        from gza.cli.main import main
        from gza.cli.watch import _CycleResult

        setup_config(tmp_path)
        handlers: dict[int, object] = {}

        def fake_signal(sig, handler):
            previous = handlers.get(sig, signal.SIG_DFL)
            handlers[sig] = handler
            return previous

        def fake_run_cycle(**_kwargs):
            handler = handlers[signal.SIGINT]
            assert callable(handler)
            handler(signal.SIGINT, None)
            handler(signal.SIGINT, None)
            return _CycleResult(True, 0, 0)

        with (
            patch.object(
                sys,
                "argv",
                ["gza", "watch", "--project", str(tmp_path), "--yes", "--quiet"],
            ),
            patch("gza.cli.watch.signal.signal", side_effect=fake_signal),
            patch("gza.cli.watch._run_cycle", side_effect=fake_run_cycle),
        ):
            rc = main()

        captured = capsys.readouterr()
        assert rc == 130
        assert captured.out == ""
        assert captured.err == "shutting down (workers left running)\nstopping due to ctrl+c\n"
        assert "Traceback" not in captured.err

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

        def capture_spawn(cmd, _config, worker_id):
            nonlocal captured_cmd
            captured_cmd = cmd
            return mock_proc, f".gza/workers/{worker_id}-startup.log"

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
            patch("gza.cli.execution._prepare_task_for_immediate_execution", side_effect=lambda _c, prepared_task, **_k: prepared_task),
            patch("gza.cli._spawn_detached_worker_process", side_effect=capture_spawn),
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

        def capture_spawn(cmd, _config, worker_id):
            nonlocal captured_cmd
            captured_cmd = cmd
            return mock_proc, f".gza/workers/{worker_id}-startup.log"

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
            patch(
                "gza.cli.execution._prepare_task_for_immediate_execution",
                side_effect=lambda _c, prepared_task, **_k: prepared_task,
            ),
            patch("gza.cli._common._spawn_detached_worker_process", side_effect=capture_spawn),
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

        def capture_spawn(cmd, _config, worker_id):
            nonlocal captured_cmd
            captured_cmd = cmd
            return mock_proc, f".gza/workers/{worker_id}-startup.log"

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
            patch(
                "gza.cli.execution._prepare_task_for_immediate_execution",
                side_effect=lambda _c, prepared_task, **_k: prepared_task,
            ),
            patch("gza.cli._common._spawn_detached_worker_process", side_effect=capture_spawn),
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

        def capture_spawn(cmd, _config, worker_id):
            nonlocal captured_cmd
            captured_cmd = cmd
            return mock_proc, f".gza/workers/{worker_id}-startup.log"

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
            patch(
                "gza.cli.execution._prepare_task_for_immediate_execution",
                side_effect=lambda _c, prepared_task, **_k: prepared_task,
            ),
            patch("gza.cli._common._spawn_detached_worker_process", side_effect=capture_spawn),
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
        task.failure_reason = "MAX_TURNS"
        if restart_flag == "--resume":
            task.session_id = "resume-session"
        store.update(task)
        assert task.id is not None

        captured_cmd: list[str] | None = None
        mock_proc = MagicMock()
        mock_proc.pid = 6262
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

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
            patch(
                "gza.cli.execution._prepare_task_for_immediate_execution",
                side_effect=lambda _c, prepared_task, **_k: prepared_task,
            ),
            patch("gza.cli.execution.Git", return_value=mock_git),
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
