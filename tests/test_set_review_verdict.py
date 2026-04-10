"""Tests for bin/set-review-verdict.py."""

import json
import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Import the script's functions directly.
sys.path.insert(0, str(Path(__file__).parent.parent / "bin"))
import importlib.util

_spec = importlib.util.spec_from_file_location(
    "set_review_verdict",
    Path(__file__).parent.parent / "bin" / "set-review-verdict.py",
)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)  # type: ignore[attr-defined]

main = _mod.main
extract_verdict = _mod.extract_verdict


def _make_db(tmp_path: Path) -> Path:
    """Create a minimal tasks database with schema initialised by SqliteTaskStore."""
    db_path = tmp_path / "gza.db"
    from gza.db import SqliteTaskStore

    # Constructing the store runs schema migrations and sets up all tables.
    SqliteTaskStore(db_path, prefix="testproject")
    return db_path


def _insert_review_task(
    db_path: Path,
    *,
    output_content: str | None = None,
    log_file: str | None = None,
    cycle_id: int | None = None,
) -> str:
    """Insert a review task via SqliteTaskStore and patch optional fields."""
    from gza.db import SqliteTaskStore

    store = SqliteTaskStore(db_path, prefix="testproject")
    task = store.add(prompt="Test review", task_type="review")
    # Patch optional fields directly with raw SQL.
    conn = sqlite3.connect(db_path)
    conn.execute(
        "UPDATE tasks SET output_content = ?, log_file = ?, cycle_id = ? WHERE id = ?",
        (output_content, log_file, cycle_id, task.id),
    )
    conn.commit()
    conn.close()
    return task.id


def _get_output_content(db_path: Path, task_id: str) -> str | None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT output_content FROM tasks WHERE id = ?", (task_id,)).fetchone()
    conn.close()
    return row["output_content"] if row else None


class TestFromLog:
    def test_from_log_extracts_and_backfills(self, tmp_path: Path) -> None:
        """--from-log extracts review content from log and backfills output_content."""
        db_path = _make_db(tmp_path)

        # Create a log file with a result entry containing a verdict.
        log_file = tmp_path / "review.jsonl"
        review_text = "# Review\n\n**Verdict: APPROVED**\n\nLooks great."
        log_file.write_text(json.dumps({"type": "result", "subtype": "success", "result": review_text}) + "\n")

        task_id = _insert_review_task(db_path, log_file=str(log_file))

        with patch.object(_mod, "get_db_path", return_value=db_path):
            with patch("sys.argv", ["set-review-verdict.py", str(task_id), "--from-log"]):
                exit_code = main()

        assert exit_code == 0
        assert _get_output_content(db_path, task_id) == review_text

    def test_from_log_returns_error_when_no_result_entry(self, tmp_path: Path) -> None:
        """--from-log returns non-zero when the log has no 'result' entry."""
        db_path = _make_db(tmp_path)

        log_file = tmp_path / "review.jsonl"
        # Log has events but no result entry.
        log_file.write_text(json.dumps({"type": "system", "subtype": "init"}) + "\n")

        task_id = _insert_review_task(db_path, log_file=str(log_file))

        with patch.object(_mod, "get_db_path", return_value=db_path):
            with patch("sys.argv", ["set-review-verdict.py", str(task_id), "--from-log"]):
                exit_code = main()

        assert exit_code != 0
        # output_content should remain NULL.
        assert _get_output_content(db_path, task_id) is None

    def test_from_log_returns_last_result_entry(self, tmp_path: Path) -> None:
        """--from-log returns the last result entry when multiple are present."""
        db_path = _make_db(tmp_path)

        log_file = tmp_path / "review.jsonl"
        first_text = "# Intermediate result\n\n**Verdict: CHANGES_REQUESTED**"
        final_text = "# Final result\n\n**Verdict: APPROVED**\n\nAll issues resolved."
        with open(log_file, "w") as f:
            f.write(json.dumps({"type": "result", "result": first_text}) + "\n")
            f.write(json.dumps({"type": "result", "result": final_text}) + "\n")

        task_id = _insert_review_task(db_path, log_file=str(log_file))

        with patch.object(_mod, "get_db_path", return_value=db_path):
            with patch("sys.argv", ["set-review-verdict.py", str(task_id), "--from-log"]):
                exit_code = main()

        assert exit_code == 0
        assert _get_output_content(db_path, task_id) == final_text


class TestVerdict:
    def test_verdict_backfills_output_content_when_null(self, tmp_path: Path) -> None:
        """--verdict backfills output_content with a synthetic verdict block when it is NULL."""
        db_path = _make_db(tmp_path)
        task_id = _insert_review_task(db_path, output_content=None)

        with patch.object(_mod, "get_db_path", return_value=db_path):
            with patch("sys.argv", ["set-review-verdict.py", str(task_id), "--verdict", "APPROVED"]):
                exit_code = main()

        assert exit_code == 0
        content = _get_output_content(db_path, task_id)
        assert content is not None
        assert "APPROVED" in content

    def test_verdict_does_not_overwrite_existing_output_content(self, tmp_path: Path) -> None:
        """--verdict must NOT overwrite output_content when it already has a value."""
        db_path = _make_db(tmp_path)
        existing_content = "# Existing review\n\n**Verdict: CHANGES_REQUESTED**\n\nFix these issues."
        task_id = _insert_review_task(db_path, output_content=existing_content)

        with patch.object(_mod, "get_db_path", return_value=db_path):
            with patch("sys.argv", ["set-review-verdict.py", str(task_id), "--verdict", "APPROVED"]):
                exit_code = main()

        assert exit_code == 0
        # output_content must remain unchanged.
        assert _get_output_content(db_path, task_id) == existing_content


class TestMutualExclusivity:
    def test_verdict_and_from_log_are_mutually_exclusive(self, tmp_path: Path) -> None:
        """--verdict and --from-log together must return an error."""
        db_path = _make_db(tmp_path)
        task_id = _insert_review_task(db_path)

        with patch.object(_mod, "get_db_path", return_value=db_path):
            with patch("sys.argv", ["set-review-verdict.py", str(task_id), "--verdict", "APPROVED", "--from-log"]):
                exit_code = main()

        assert exit_code != 0
