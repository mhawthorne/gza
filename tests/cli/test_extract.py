"""Fast unit tests for `gza extract` command wiring."""

from argparse import Namespace
from pathlib import Path
from unittest.mock import MagicMock, patch

from gza.db import SqliteTaskStore
from gza.extractions import ExtractionDraft, ExtractionError, FileDiffSummary, SourceSelection
from gza.git import Git

from .conftest import get_latest_task, make_store, setup_config


def _args(project_dir: Path, **overrides: object) -> Namespace:
    values: dict[str, object] = {
        "project_dir": project_dir,
        "no_docker": True,
        "max_turns": None,
        "source": None,
        "branch": None,
        "paths": (),
        "files_from": None,
        "prompt": None,
        "dry_run": False,
        "review": False,
        "create_pr": False,
        "branch_type": None,
        "model": None,
        "provider": None,
        "skip_learnings": False,
        "background": False,
        "queue": False,
        "force": False,
        "base_branch": None,
        "tag": None,
        "tags": None,
        "no_tag": None,
        "any_tag": False,
    }
    values.update(overrides)
    return Namespace(**values)


def _draft(*, source: SourceSelection) -> ExtractionDraft:
    return ExtractionDraft(
        source=source,
        selected_paths=("src/extracted.py",),
        touched_paths=("src/extracted.py",),
        file_summaries=(
            FileDiffSummary(
                status="A",
                selected_path="src/extracted.py",
                old_path=None,
                new_path="src/extracted.py",
                additions=1,
                deletions=0,
                binary=False,
            ),
        ),
        patch=(
            "diff --git a/src/extracted.py b/src/extracted.py\n"
            "index e69de29..8c7e5a6 100644\n"
            "--- a/src/extracted.py\n"
            "+++ b/src/extracted.py\n"
            "@@ -0,0 +1 @@\n"
            "+print('seeded')\n"
        ),
        prompt="Carry over: Add extracted source module\n",
    )


def test_extract_branch_mode_treats_nonexistent_task_id_like_source_as_path(tmp_path: Path) -> None:
    from gza.cli.execution import cmd_extract

    setup_config(tmp_path)
    source = SourceSelection(
        source_task_id=None,
        source_branch="feature/source",
        source_base_ref="main",
    )
    draft = _draft(source=source)
    git = MagicMock(spec=Git)
    git.branch_exists.return_value = False
    bundle_dir = tmp_path / ".gza" / "extractions" / "20260427-target"

    with (
        patch("gza.cli.execution.Git", return_value=git),
        patch("gza.cli.execution.normalize_selected_paths", return_value=("testproject-9999",)) as mock_normalize,
        patch("gza.cli.execution.resolve_source_selection", return_value=source) as mock_resolve_source,
        patch("gza.cli.execution.plan_extraction", return_value=draft),
        patch("gza.cli.execution.generate_slug", return_value="20260427-target"),
        patch("gza.cli.execution.write_extraction_bundle", return_value=bundle_dir),
        patch("gza.cli.execution._print_extraction_plan_summary"),
    ):
        rc = cmd_extract(
            _args(
                tmp_path,
                source="testproject-9999",
                branch="feature/source",
                queue=True,
            )
        )

    assert rc == 0
    mock_normalize.assert_called_once_with(["testproject-9999"])
    mock_resolve_source.assert_called_once()
    assert mock_resolve_source.call_args.kwargs["source_task_id"] is None
    assert mock_resolve_source.call_args.kwargs["source_branch"] == "feature/source"


def test_extract_dry_run_uses_current_branch_when_no_source_selector(tmp_path: Path) -> None:
    from gza.cli.execution import cmd_extract

    setup_config(tmp_path)
    store = make_store(tmp_path)
    source = SourceSelection(
        source_task_id=None,
        source_branch="feature/current",
        source_base_ref="main",
    )
    draft = _draft(source=source)
    git = MagicMock(spec=Git)
    git.current_branch.return_value = "feature/current"

    with (
        patch("gza.cli.execution.Git", return_value=git),
        patch("gza.cli.execution.resolve_source_selection", return_value=source),
        patch("gza.cli.execution.infer_selected_paths", return_value=("src/extracted.py",)),
        patch("gza.cli.execution.plan_extraction", return_value=draft),
        patch("gza.cli.execution._print_extraction_plan_summary") as mock_print,
    ):
        rc = cmd_extract(_args(tmp_path, dry_run=True))

    assert rc == 0
    assert store.get_all() == []
    mock_print.assert_called_once()
    assert mock_print.call_args.kwargs["source_label"] == "branch feature/current"
    assert mock_print.call_args.kwargs["dry_run"] is True


def test_extract_bundle_write_failure_marks_failed_task_when_delete_fails(tmp_path: Path) -> None:
    from gza.cli.execution import cmd_extract

    setup_config(tmp_path)
    store = make_store(tmp_path)
    source = SourceSelection(
        source_task_id=None,
        source_branch="feature/source",
        source_base_ref="main",
    )
    draft = _draft(source=source)
    git = MagicMock(spec=Git)
    git.branch_exists.return_value = False

    def _refuse_delete(self: SqliteTaskStore, task_id: str) -> bool:
        del task_id
        return False

    with (
        patch("gza.cli.execution.Git", return_value=git),
        patch("gza.cli.execution.resolve_source_selection", return_value=source),
        patch("gza.cli.execution.normalize_selected_paths", return_value=("src/extracted.py",)),
        patch("gza.cli.execution.plan_extraction", return_value=draft),
        patch("gza.cli.execution.generate_slug", return_value="20260427-target"),
        patch(
            "gza.cli.execution.write_extraction_bundle",
            side_effect=ExtractionError("bundle write failed"),
        ),
        patch.object(SqliteTaskStore, "delete", _refuse_delete),
    ):
        rc = cmd_extract(
            _args(
                tmp_path,
                branch="feature/source",
                paths=("src/extracted.py",),
                queue=True,
            )
        )

    assert rc == 1
    failed_task = get_latest_task(store, task_type="implement")
    assert failed_task is not None
    assert failed_task.status == "failed"
    assert failed_task.failure_reason == "EXTRACTION_BUNDLE_WRITE_FAILED"
