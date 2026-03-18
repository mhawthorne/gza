"""Tests for log display CLI commands."""


import json
import subprocess
import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

from gza.cli import _format_log_entry, _build_step_timeline
from gza.db import SqliteTaskStore

from .conftest import run_gza, setup_config, setup_db_with_tasks, LOG_FIXTURES_DIR


class TestLogCommand:
    """Tests for 'gza log' command."""

    def test_log_by_task_id_single_json_format(self, tmp_path: Path):
        """Log command by task ID parses single JSON format with successful result."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)

        # Create a task with a log file
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Test task for log")
        task.status = "completed"
        task.log_file = ".gza/logs/test.log"
        store.update(task)

        # Create a single JSON log file (old format)
        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "test.log"
        log_data = {
            "type": "result",
            "subtype": "success",
            "result": "## Summary\n\nTask completed successfully!",
            "duration_ms": 60000,
            "num_turns": 10,
            "total_cost_usd": 0.5,
        }
        log_file.write_text(json.dumps(log_data))

        result = run_gza("log", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Task completed successfully!" in result.stdout
        assert "Duration:" in result.stdout
        assert "Steps: 10" in result.stdout
        assert "Legacy turns: 10" in result.stdout
        assert "Cost: $0.5000" in result.stdout

    def test_log_by_task_id_jsonl_format(self, tmp_path: Path):
        """Log command by task ID parses step-first JSONL format with successful result."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)

        # Create a task with a log file
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Test task for JSONL log")
        task.status = "completed"
        task.log_file = ".gza/logs/test.log"
        store.update(task)

        # Create a JSONL log file (step-first format fixture)
        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "test.log"
        log_file.write_text((LOG_FIXTURES_DIR / "step_schema_v2_like.jsonl").read_text())

        result = run_gza("log", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Finished." in result.stdout
        assert "Duration:" in result.stdout
        assert "Steps: 2" in result.stdout
        assert "Cost: $0.1234" in result.stdout

    def test_log_by_task_id_error_max_turns(self, tmp_path: Path):
        """Log command by task ID handles JSONL format with error_max_turns result."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)

        # Create a task with a log file
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Test task that hit max turns")
        task.status = "completed"
        task.log_file = ".gza/logs/test.log"
        store.update(task)

        # Create a JSONL log file with error_max_turns (no result field)
        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "test.log"
        lines = [
            {"type": "system", "subtype": "init", "session_id": "abc123"},
            {"type": "assistant", "message": {"role": "assistant", "content": "Working..."}},
            {
                "type": "result",
                "subtype": "error_max_turns",
                "duration_ms": 300000,
                "num_turns": 60,
                "total_cost_usd": 1.5,
                "errors": [],
            },
        ]
        log_file.write_text("\n".join(json.dumps(line) for line in lines))

        result = run_gza("log", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "error_max_turns" in result.stdout
        assert "Steps: 60" in result.stdout
        assert "Legacy turns: 60" in result.stdout
        assert "Cost: $1.5000" in result.stdout

    def test_log_by_task_id_missing_log_file(self, tmp_path: Path):
        """Log command by task ID handles missing log file."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)

        # Create a task with a log file path that doesn't exist
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Test task with missing log")
        task.status = "completed"
        task.log_file = ".gza/logs/nonexistent.log"
        store.update(task)

        result = run_gza("log", "1", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Log file not found" in result.stdout

    def test_log_by_task_id_no_result_entry(self, tmp_path: Path):
        """Log command by task ID shows compact step timeline when no result entry exists."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)

        # Create a task with a log file
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Test task with incomplete log")
        task.status = "completed"
        task.log_file = ".gza/logs/test.log"
        store.update(task)

        # Create a JSONL log file with no result entry
        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "test.log"
        lines = [
            {"type": "system", "subtype": "init", "session_id": "abc123", "model": "test-model"},
            {"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "Working..."}]}},
        ]
        log_file.write_text("\n".join(json.dumps(line) for line in lines))

        result = run_gza("log", "1", "--project", str(tmp_path))

        # Should show formatted entries instead of failing
        assert result.returncode == 0
        assert "Working..." in result.stdout

    def test_log_by_task_id_falls_back_to_inferred_log_path(self, tmp_path: Path):
        """Task lookup should render entries from inferred slug log when task.log_file is stale."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Fallback log path task")
        task.status = "completed"
        task.task_id = "20260227-fallback-log"
        task.log_file = ".gza/logs/missing.log"
        store.update(task)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        inferred_log = log_dir / "20260227-fallback-log.log"
        lines = [
            {"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "Recovered via inferred path"}]}},
            {"type": "result", "subtype": "success", "result": "ok", "num_steps": 1, "duration_ms": 1000, "total_cost_usd": 0.01},
        ]
        inferred_log.write_text("\n".join(json.dumps(line) for line in lines))

        result = run_gza("log", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Recovered via inferred path" in result.stdout

    def test_log_by_task_id_resolves_to_latest_retry_resume_attempt(self, tmp_path: Path):
        """Task lookup resolves deterministically to latest same-type based_on attempt."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        original = store.add("Original failed task")
        assert original.id is not None
        original.status = "failed"
        original.log_file = ".gza/logs/original.log"
        store.update(original)

        retry = store.add("Retry task", based_on=original.id, task_type=original.task_type)
        assert retry.id is not None
        retry.status = "completed"
        retry.log_file = ".gza/logs/retry.log"
        retry.started_at = datetime.now(timezone.utc)
        store.update(retry)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "original.log").write_text(json.dumps({"type": "result", "result": "old run"}))
        (log_dir / "retry.log").write_text("\n".join([
            json.dumps({"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "Latest attempt output"}]}}),
            json.dumps({"type": "result", "subtype": "success", "result": "done", "num_steps": 1, "duration_ms": 1000, "total_cost_usd": 0.01}),
        ]))

        result = run_gza("log", str(original.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Resolved to latest run attempt" in result.stdout
        assert "Latest attempt output" in result.stdout

    def test_log_by_task_id_non_root_query_stays_within_its_retry_chain(self, tmp_path: Path):
        """Querying a retry should not jump to a newer same-type sibling chain."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        root = store.add("Original failed task")
        assert root.id is not None
        root.status = "failed"
        root.log_file = ".gza/logs/root.log"
        root.started_at = datetime(2026, 2, 25, 12, 0, tzinfo=timezone.utc)
        store.update(root)

        retry_a = store.add("Retry A", based_on=root.id, task_type=root.task_type)
        assert retry_a.id is not None
        retry_a.status = "failed"
        retry_a.log_file = ".gza/logs/retry_a.log"
        retry_a.started_at = datetime(2026, 2, 26, 12, 0, tzinfo=timezone.utc)
        store.update(retry_a)

        retry_b = store.add("Retry B", based_on=root.id, task_type=root.task_type)
        assert retry_b.id is not None
        retry_b.status = "completed"
        retry_b.log_file = ".gza/logs/retry_b.log"
        retry_b.started_at = datetime(2026, 2, 26, 12, 30, tzinfo=timezone.utc)
        store.update(retry_b)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "root.log").write_text(json.dumps({"type": "result", "result": "root run"}))
        (log_dir / "retry_a.log").write_text(json.dumps({"type": "result", "result": "retry A run"}))
        (log_dir / "retry_b.log").write_text("\n".join([
            json.dumps({"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "Newest sibling output"}]}}),
            json.dumps({"type": "result", "subtype": "success", "result": "done", "num_steps": 1, "duration_ms": 1000, "total_cost_usd": 0.01}),
        ]))

        result = run_gza("log", str(retry_a.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Resolved to latest run attempt" not in result.stdout
        assert "retry A run" in result.stdout
        assert "Newest sibling output" not in result.stdout

    def test_log_by_slug_non_root_query_stays_within_its_retry_chain(self, tmp_path: Path):
        """Slug lookup should not jump to a newer same-type sibling chain."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        root = store.add("Original failed task")
        assert root.id is not None
        root.task_id = "20260227-chain-root"
        root.status = "failed"
        root.log_file = ".gza/logs/root.log"
        root.started_at = datetime(2026, 2, 25, 12, 0, tzinfo=timezone.utc)
        store.update(root)

        retry_a = store.add("Retry A", based_on=root.id, task_type=root.task_type)
        assert retry_a.id is not None
        retry_a.task_id = "20260227-chain-a"
        retry_a.status = "failed"
        retry_a.log_file = ".gza/logs/retry_a.log"
        retry_a.started_at = datetime(2026, 2, 26, 12, 0, tzinfo=timezone.utc)
        store.update(retry_a)

        retry_b = store.add("Retry B", based_on=root.id, task_type=root.task_type)
        assert retry_b.id is not None
        retry_b.task_id = "20260227-chain-b"
        retry_b.status = "completed"
        retry_b.log_file = ".gza/logs/retry_b.log"
        retry_b.started_at = datetime(2026, 2, 26, 12, 30, tzinfo=timezone.utc)
        store.update(retry_b)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "root.log").write_text(json.dumps({"type": "result", "result": "root run"}))
        (log_dir / "retry_a.log").write_text(json.dumps({"type": "result", "result": "retry A run"}))
        (log_dir / "retry_b.log").write_text("\n".join([
            json.dumps({"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "Newest sibling output"}]}}),
            json.dumps({"type": "result", "subtype": "success", "result": "done", "num_steps": 1, "duration_ms": 1000, "total_cost_usd": 0.01}),
        ]))

        result = run_gza("log", "--slug", "20260227-chain-a", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Resolved to latest run attempt" not in result.stdout
        assert "retry A run" in result.stdout
        assert "Newest sibling output" not in result.stdout

    def test_log_by_task_id_latest_attempt_ignores_mixed_type_lineage_nodes(self, tmp_path: Path):
        """Latest-attempt resolution should only consider same-type retry/resume attempts."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        root = store.add("Original task", task_type="implement")
        assert root.id is not None
        root.status = "failed"
        root.log_file = ".gza/logs/root.log"
        root.started_at = datetime(2026, 2, 25, 12, 0, tzinfo=timezone.utc)
        store.update(root)

        retry = store.add("Retry task", based_on=root.id, task_type="implement")
        assert retry.id is not None
        retry.status = "completed"
        retry.log_file = ".gza/logs/retry.log"
        retry.started_at = datetime(2026, 2, 26, 12, 0, tzinfo=timezone.utc)
        store.update(retry)

        review = store.add("Review task", based_on=root.id, task_type="review")
        assert review.id is not None
        review.status = "completed"
        review.log_file = ".gza/logs/review.log"
        review.started_at = datetime(2026, 2, 26, 12, 30, tzinfo=timezone.utc)
        store.update(review)

        improve = store.add("Improve task", based_on=review.id, task_type="improve")
        assert improve.id is not None
        improve.status = "completed"
        improve.log_file = ".gza/logs/improve.log"
        improve.started_at = datetime(2026, 2, 26, 13, 0, tzinfo=timezone.utc)
        store.update(improve)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "root.log").write_text(json.dumps({"type": "result", "result": "root run"}))
        (log_dir / "retry.log").write_text(json.dumps({"type": "result", "result": "retry chain output"}))
        (log_dir / "review.log").write_text(json.dumps({"type": "result", "result": "review output"}))
        (log_dir / "improve.log").write_text(json.dumps({"type": "result", "result": "improve output"}))

        result = run_gza("log", str(root.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Resolved to latest run attempt" in result.stdout
        assert f"task #{retry.id}" in result.stdout
        assert "retry chain output" in result.stdout
        assert "review output" not in result.stdout
        assert "improve output" not in result.stdout

    def test_resolve_latest_attempt_for_task_excludes_parallel_same_type_sibling_chains(self, tmp_path: Path):
        """Resolver should stay within the queried task's direct same-type chain."""
        from gza.cli import _resolve_latest_attempt_for_task
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        root = store.add("Root task", task_type="implement")
        assert root.id is not None
        root.started_at = datetime(2026, 2, 25, 12, 0, tzinfo=timezone.utc)
        root.status = "failed"
        store.update(root)

        chain_a = store.add("Chain A", based_on=root.id, task_type="implement")
        assert chain_a.id is not None
        chain_a.started_at = datetime(2026, 2, 26, 12, 0, tzinfo=timezone.utc)
        chain_a.status = "failed"
        store.update(chain_a)

        chain_a_resume = store.add("Chain A Resume", based_on=chain_a.id, task_type="implement")
        assert chain_a_resume.id is not None
        chain_a_resume.started_at = datetime(2026, 2, 26, 12, 10, tzinfo=timezone.utc)
        chain_a_resume.status = "completed"
        store.update(chain_a_resume)

        sibling_chain = store.add("Sibling Chain", based_on=root.id, task_type="implement")
        assert sibling_chain.id is not None
        sibling_chain.started_at = datetime(2026, 2, 26, 12, 30, tzinfo=timezone.utc)
        sibling_chain.status = "completed"
        store.update(sibling_chain)

        selected, attempts = _resolve_latest_attempt_for_task(store, chain_a)

        attempt_ids = [attempt.id for attempt in attempts]
        assert selected.id == chain_a_resume.id
        assert sibling_chain.id not in attempt_ids

    def test_log_default_mode_renders_entries_when_result_exists(self, tmp_path: Path):
        """Default formatted output should include entry rendering, not metadata-only output."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Default render parity task")
        task.status = "completed"
        task.log_file = ".gza/logs/test.log"
        store.update(task)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        lines = [
            {"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "Entry text should render"}]}},
            {"type": "result", "subtype": "success", "result": "summary", "num_steps": 1, "duration_ms": 1000, "total_cost_usd": 0.01},
        ]
        (log_dir / "test.log").write_text("\n".join(json.dumps(line) for line in lines))

        result = run_gza("log", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Entry text should render" in result.stdout

    def test_log_follow_by_task_uses_running_worker_when_available(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        """-t -f should follow via live worker when a task is actively running."""
        import argparse
        import json
        from gza.cli import cmd_log
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Running task for follow")
        assert task.id is not None
        task.status = "in_progress"
        task.log_file = ".gza/logs/follow.log"
        store.update(task)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        worker = WorkerMetadata(
            worker_id="w-20260227-010101",
            pid=os.getpid(),
            task_id=task.id,
            task_slug=task.task_id,
            started_at="2026-02-27T01:01:01+00:00",
            status="running",
            log_file=task.log_file,
            worktree=None,
            is_background=False,
        )
        registry.register(worker)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "follow.log").write_text("\n".join([
            json.dumps({"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "live"}]}})
        ]))

        captured: dict[str, str | None] = {}

        def fake_tail(log_path, args, reg, worker_id, task_id, store_obj):
            captured["worker_id"] = worker_id
            captured["task_id"] = str(task_id) if task_id is not None else None
            return 0

        monkeypatch.setattr("gza.cli._tail_log_file", fake_tail)

        args = argparse.Namespace(
            identifier=str(task.id),
            slug=False,
            worker=False,
            follow=True,
            raw=False,
            timeline_mode=None,
            tail=None,
            project_dir=tmp_path,
        )

        rc = cmd_log(args)
        assert rc == 0
        assert captured["worker_id"] == "w-20260227-010101"
        assert captured["task_id"] == str(task.id)

    def test_log_follow_by_task_not_running_falls_back_to_static_output(self, tmp_path: Path):
        """-t -f should print persisted logs and exit when task is not actively running."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Completed task with persisted log")
        task.status = "completed"
        task.log_file = ".gza/logs/static.log"
        store.update(task)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "static.log").write_text("\n".join([
            json.dumps({"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "persisted output"}]}}),
            json.dumps({"type": "result", "subtype": "success", "result": "done", "num_steps": 1, "duration_ms": 1000, "total_cost_usd": 0.01}),
        ]))

        result = subprocess.run(
            ["uv", "run", "gza", "log", "1", "--follow", "--project", str(tmp_path)],
            capture_output=True,
            text=True,
            timeout=5,
        )

        assert result.returncode == 0
        assert "persisted output" in result.stdout

    def test_log_steps_compact_renders_step_labels(self, tmp_path: Path):
        """--steps renders compact step-first timeline anchors."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Step timeline task")
        task.status = "completed"
        task.log_file = ".gza/logs/test.log"
        store.update(task)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "test.log").write_text((LOG_FIXTURES_DIR / "step_schema_v2_like.jsonl").read_text())

        result = run_gza("log", "1", "--steps", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "[Step S1]" in result.stdout
        assert "[Step S2]" in result.stdout
        assert "[S1.1]" not in result.stdout

    def test_log_steps_verbose_renders_substep_labels(self, tmp_path: Path):
        """--steps-verbose renders S<n>.<m> substep labels."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Verbose step timeline task")
        task.status = "completed"
        task.log_file = ".gza/logs/test.log"
        store.update(task)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "test.log").write_text((LOG_FIXTURES_DIR / "step_schema_v2_like.jsonl").read_text())

        result = run_gza("log", "1", "--steps-verbose", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "[Step S1]" in result.stdout
        assert "[S1.1]" in result.stdout
        assert "tool_call rg -n" in result.stdout

    def test_log_turns_alias_reads_legacy_turn_only_logs(self, tmp_path: Path):
        """Deprecated --turns alias still renders verbose step timeline for turn-only logs."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Legacy log task")
        task.status = "completed"
        task.log_file = ".gza/logs/test.log"
        store.update(task)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "test.log").write_text((LOG_FIXTURES_DIR / "legacy_turn_only_codex.jsonl").read_text())

        result = run_gza("log", "1", "--turns", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "[Step S1] Pre-message tool activity" in result.stdout
        assert "[S1.1] tool_call Bash ls -la" in result.stdout
        assert "[Step S2] Listed files." in result.stdout

    def test_log_by_task_id_not_found(self, tmp_path: Path):
        """Log command by task ID handles nonexistent task."""
        setup_config(tmp_path)

        # Create empty database
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        from gza.db import SqliteTaskStore
        SqliteTaskStore(db_path)

        result = run_gza("log", "999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Task 999 not found" in result.stdout

    def test_log_by_task_id_invalid_id(self, tmp_path: Path):
        """Log command by task ID rejects non-numeric ID."""
        setup_config(tmp_path)

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        from gza.db import SqliteTaskStore
        SqliteTaskStore(db_path)

        result = run_gza("log", "not-a-number", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not a valid task ID" in result.stdout

    def test_log_by_slug_exact_match(self, tmp_path: Path):
        """Log command with --slug finds task by exact slug."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Test task for slug lookup")
        task.task_id = "20260108-test-slug"
        task.status = "completed"
        task.log_file = ".gza/logs/test.log"
        store.update(task)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "test.log"
        log_data = {"type": "result", "result": "Slug lookup works!", "duration_ms": 1000, "num_turns": 1, "total_cost_usd": 0.01}
        log_file.write_text(json.dumps(log_data))

        result = run_gza("log", "--slug", "20260108-test-slug", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Slug lookup works!" in result.stdout

    def test_log_by_slug_partial_match(self, tmp_path: Path):
        """Log command with --slug finds task by partial slug match."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Test task for partial slug")
        task.task_id = "20260108-partial-slug-test"
        task.status = "completed"
        task.log_file = ".gza/logs/test.log"
        store.update(task)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "test.log"
        log_data = {"type": "result", "result": "Partial match works!", "duration_ms": 1000, "num_turns": 1, "total_cost_usd": 0.01}
        log_file.write_text(json.dumps(log_data))

        result = run_gza("log", "--slug", "partial-slug", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Partial match works!" in result.stdout

    def test_log_by_slug_not_found(self, tmp_path: Path):
        """Log command with --slug handles nonexistent slug."""
        setup_config(tmp_path)

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        from gza.db import SqliteTaskStore
        SqliteTaskStore(db_path)

        result = run_gza("log", "--slug", "nonexistent-slug", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "No task found matching slug" in result.stdout

    def test_log_by_worker_success(self, tmp_path: Path):
        """Log command with --worker finds log via worker registry."""
        import json
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)

        # Create task
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Test task for worker lookup")
        task.status = "completed"
        task.log_file = ".gza/logs/test.log"
        store.update(task)

        # Create worker registry entry
        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        worker_id = registry.generate_worker_id()
        worker = WorkerMetadata(
            worker_id=worker_id,
            pid=12345,
            task_id=task.id,
            task_slug=task.task_id,
            started_at="2026-01-08T00:00:00Z",
            status="completed",
            log_file=".gza/logs/test.log",
            worktree=None,
        )
        registry.register(worker)

        # Create log file
        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "test.log"
        log_data = {"type": "result", "result": "Worker lookup works!", "duration_ms": 1000, "num_turns": 1, "total_cost_usd": 0.01}
        log_file.write_text(json.dumps(log_data))

        result = run_gza("log", "--worker", worker_id, "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Worker lookup works!" in result.stdout

    def test_log_by_worker_not_found(self, tmp_path: Path):
        """Log command with --worker handles nonexistent worker."""
        setup_config(tmp_path)

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        from gza.db import SqliteTaskStore
        SqliteTaskStore(db_path)

        # Create empty workers directory
        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)

        result = run_gza("log", "--worker", "w-nonexistent", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Worker 'w-nonexistent' not found" in result.stdout

    def test_log_by_worker_falls_back_to_startup_log_when_main_missing(self, tmp_path: Path):
        """Worker lookup uses startup log when no main task log exists."""
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Task that failed before runner log setup")
        task.status = "failed"
        store.update(task)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        worker = WorkerMetadata(
            worker_id="w-test-startup-failure",
            pid=12345,
            task_id=task.id,
            task_slug=task.task_id,
            started_at="2026-01-08T00:00:00Z",
            status="failed",
            log_file=None,
            worktree=None,
            startup_log_file=".gza/workers/w-test-startup-failure-startup.log",
        )
        registry.register(worker)

        startup_log = tmp_path / ".gza" / "workers" / "w-test-startup-failure-startup.log"
        startup_log.write_text("Docker daemon is not running")

        result = run_gza("log", "--worker", worker.worker_id, "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Using startup log (main task log not available)." in result.stdout
        assert "Docker daemon is not running" in result.stdout

    def test_log_by_worker_shows_failed_status_for_startup_failure_without_task(self, tmp_path: Path):
        """Worker-only view should show failed status when worker metadata is failed."""
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        SqliteTaskStore(db_path)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        worker = WorkerMetadata(
            worker_id="w-test-worker-only-failed",
            pid=12345,
            task_id=None,
            task_slug="startup-failed-worker-only",
            started_at="2026-01-08T00:00:00Z",
            status="failed",
            log_file=None,
            worktree=None,
            startup_log_file=".gza/workers/w-test-worker-only-failed-startup.log",
        )
        registry.register(worker)

        startup_log = tmp_path / ".gza" / "workers" / "w-test-worker-only-failed-startup.log"
        startup_log.write_text(json.dumps({"type": "result", "result": "startup-log-result"}))

        result = run_gza("log", "--worker", worker.worker_id, "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Status: failed" in result.stdout
        assert "Status: completed" not in result.stdout
        assert "startup-log-result" in result.stdout

    def test_log_by_worker_prefers_main_log_over_startup_log(self, tmp_path: Path):
        """Worker log resolution should prefer main task log when both exist."""
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Task with both main and startup logs")
        task.status = "failed"
        task.log_file = ".gza/logs/test-main.log"
        store.update(task)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        worker = WorkerMetadata(
            worker_id="w-test-main-wins",
            pid=12345,
            task_id=task.id,
            task_slug=task.task_id,
            started_at="2026-01-08T00:00:00Z",
            status="failed",
            log_file=None,
            worktree=None,
            startup_log_file=".gza/workers/w-test-main-wins-startup.log",
        )
        registry.register(worker)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "test-main.log").write_text(json.dumps({"type": "result", "result": "main-log-output"}))

        startup_log = tmp_path / ".gza" / "workers" / "w-test-main-wins-startup.log"
        startup_log.write_text("startup-log-output")

        result = run_gza("log", "--worker", worker.worker_id, "--project", str(tmp_path))

        assert result.returncode == 0
        assert "main-log-output" in result.stdout
        assert "startup-log-output" not in result.stdout
        assert "Using startup log (main task log not available)." not in result.stdout

    @pytest.mark.parametrize("query_mode", ["task_id", "slug"])
    def test_log_task_lookup_falls_back_to_startup_log_when_main_missing(self, tmp_path: Path, query_mode: str):
        """Task lookup should use latest worker startup log when task log is missing."""
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Task lookup startup fallback")
        assert task.id is not None
        task.task_id = "20260318-task-startup-fallback"
        task.status = "failed"
        task.log_file = ".gza/logs/missing-main.log"
        store.update(task)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        worker = WorkerMetadata(
            worker_id="w-test-task-startup-fallback",
            pid=12345,
            task_id=task.id,
            task_slug=task.task_id,
            started_at="2026-03-18T00:00:00Z",
            status="failed",
            log_file=None,
            worktree=None,
            startup_log_file=".gza/workers/w-test-task-startup-fallback-startup.log",
        )
        registry.register(worker)

        startup_log = tmp_path / ".gza" / "workers" / "w-test-task-startup-fallback-startup.log"
        startup_log.write_text("startup-output-from-worker")

        if query_mode == "task_id":
            result = run_gza("log", str(task.id), "--project", str(tmp_path))
        else:
            result = run_gza("log", "--slug", task.task_id, "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Using startup log (main task log not available)." in result.stdout
        assert "startup-output-from-worker" in result.stdout

    @pytest.mark.parametrize("query_mode", ["task_id", "slug"])
    def test_log_task_lookup_prefers_main_log_over_startup_log(self, tmp_path: Path, query_mode: str):
        """Task lookup should prefer task log when both task and startup logs are present."""
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Task lookup main log precedence")
        assert task.id is not None
        task.task_id = "20260318-task-main-precedence"
        task.status = "failed"
        task.log_file = ".gza/logs/task-main.log"
        store.update(task)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        worker = WorkerMetadata(
            worker_id="w-test-task-main-precedence",
            pid=12345,
            task_id=task.id,
            task_slug=task.task_id,
            started_at="2026-03-18T00:00:00Z",
            status="failed",
            log_file=None,
            worktree=None,
            startup_log_file=".gza/workers/w-test-task-main-precedence-startup.log",
        )
        registry.register(worker)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "task-main.log").write_text(json.dumps({"type": "result", "result": "task-main-output"}))

        startup_log = tmp_path / ".gza" / "workers" / "w-test-task-main-precedence-startup.log"
        startup_log.write_text("startup-output-should-not-be-used")

        if query_mode == "task_id":
            result = run_gza("log", str(task.id), "--project", str(tmp_path))
        else:
            result = run_gza("log", "--slug", task.task_id, "--project", str(tmp_path))

        assert result.returncode == 0
        assert "task-main-output" in result.stdout
        assert "startup-output-should-not-be-used" not in result.stdout
        assert "Using startup log (main task log not available)." not in result.stdout

    @pytest.mark.parametrize("query_mode", ["task_id", "slug"])
    def test_log_task_lookup_reports_clear_error_when_no_task_or_startup_log(self, tmp_path: Path, query_mode: str):
        """Task lookup should return a clear missing-log error when no logs exist."""
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Task lookup with no available logs")
        assert task.id is not None
        task.task_id = "20260318-task-no-logs"
        task.status = "failed"
        task.log_file = ".gza/logs/task-missing.log"
        store.update(task)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        worker = WorkerMetadata(
            worker_id="w-test-task-no-logs",
            pid=12345,
            task_id=task.id,
            task_slug=task.task_id,
            started_at="2026-03-18T00:00:00Z",
            status="failed",
            log_file=None,
            worktree=None,
            startup_log_file=".gza/workers/w-test-task-no-logs-startup.log",
        )
        registry.register(worker)

        if query_mode == "task_id":
            result = run_gza("log", str(task.id), "--project", str(tmp_path))
        else:
            result = run_gza("log", "--slug", task.task_id, "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Error: Log file not found at" in result.stdout

    def test_log_by_task_id_startup_failure(self, tmp_path: Path):
        """Log command shows startup error when log contains non-JSON content."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)

        # Create a task with a log file
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Test task with startup failure")
        task.status = "failed"
        task.log_file = ".gza/logs/test-startup-error.log"
        store.update(task)

        # Create a log file with raw error text (simulating Docker startup failure)
        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "test-startup-error.log"
        log_file.write_text("exec /usr/local/bin/docker-entrypoint.sh: argument list too long")

        result = run_gza("log", "1", "--project", str(tmp_path))

        # Should detect startup failure and display the error
        assert result.returncode == 1
        assert "Task failed during startup (no Claude session):" in result.stdout
        assert "exec /usr/local/bin/docker-entrypoint.sh: argument list too long" in result.stdout
        # The error should be indented
        assert "  exec /usr/local/bin/docker-entrypoint.sh" in result.stdout

    def test_log_default_uses_task_id(self, tmp_path: Path):
        """Log command defaults to task ID lookup without any flag."""
        setup_config(tmp_path)

        result = run_gza("log", "123", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Task 123 not found" in result.stdout


class TestFormatLogEntry:
    """Tests for _format_log_entry with gza entry type."""

    def test_gza_entry_with_subtype_renders_correctly(self) -> None:
        """gza entry with subtype is formatted as [gza:subtype] message."""
        entry = {"type": "gza", "subtype": "branch", "message": "Branch: feat/foo", "branch": "feat/foo"}
        result = _format_log_entry(entry)
        assert result == r"[cyan]\[gza:branch][/cyan] Branch: feat/foo"

    def test_gza_entry_without_subtype_renders_correctly(self) -> None:
        """gza entry without subtype is formatted as [gza] message."""
        entry = {"type": "gza", "message": "Some info"}
        result = _format_log_entry(entry)
        assert result == r"[cyan]\[gza][/cyan] Some info"

    def test_gza_entry_with_empty_message_returns_none(self) -> None:
        """gza entry with empty message returns None (should be skipped)."""
        entry = {"type": "gza", "subtype": "info", "message": ""}
        result = _format_log_entry(entry)
        assert result is None

    def test_gza_entry_outcome_renders_with_subtype(self) -> None:
        """gza outcome entry renders with subtype label."""
        entry = {"type": "gza", "subtype": "outcome", "message": "Outcome: completed", "exit_code": 0}
        result = _format_log_entry(entry)
        assert result == r"[cyan]\[gza:outcome][/cyan] Outcome: completed"

    def test_gza_entry_stats_renders_with_subtype(self) -> None:
        """gza stats entry renders with subtype label."""
        entry = {"type": "gza", "subtype": "stats", "message": "Stats: 5 steps, 12.3s, $0.0042", "duration_seconds": 12.3, "cost_usd": 0.0042, "num_steps": 5}
        result = _format_log_entry(entry)
        assert result == r"[cyan]\[gza:stats][/cyan] Stats: 5 steps, 12.3s, $0.0042"

    def test_gza_log_entry_renders_in_gza_log_output(self, tmp_path: Path) -> None:
        """Integration: gza log renders gza entries from a JSONL log file."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Orchestration logging integration test")
        task.status = "completed"
        task.log_file = ".gza/logs/test.log"
        store.update(task)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        lines = [
            {"type": "gza", "subtype": "info", "message": "Task: #1 20260101-test-task"},
            {"type": "gza", "subtype": "branch", "message": "Branch: feat/test", "branch": "feat/test"},
            {"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "Working on it"}]}},
            {"type": "gza", "subtype": "outcome", "message": "Outcome: completed", "exit_code": 0},
            {"type": "result", "subtype": "success", "result": "", "num_steps": 2, "duration_ms": 5000, "total_cost_usd": 0.005},
        ]
        (log_dir / "test.log").write_text("\n".join(json.dumps(line) for line in lines))

        result = run_gza("log", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Branch: feat/test" in result.stdout
        assert "Outcome: completed" in result.stdout


class TestBuildStepTimeline:
    """Tests for _build_step_timeline with gza entries."""

    def test_gza_metadata_subtypes_do_not_produce_timeline_steps(self) -> None:
        """branch, stats, and outcome entries are not added as timeline steps."""
        entries = [
            {"type": "gza", "subtype": "branch", "message": "Branch: feat/foo", "branch": "feat/foo"},
            {"type": "gza", "subtype": "stats", "message": "Stats: 3 steps, 10.0s, $0.001", "num_steps": 3},
            {"type": "gza", "subtype": "outcome", "message": "Outcome: completed", "exit_code": 0},
        ]
        steps = _build_step_timeline(entries)
        assert steps == []

    def test_gza_info_subtype_produces_timeline_step(self) -> None:
        """info entries are added as timeline steps."""
        entries = [
            {"type": "gza", "subtype": "info", "message": "Task: #1 20260101-test-task"},
        ]
        steps = _build_step_timeline(entries)
        assert len(steps) == 1
        assert steps[0]["message_text"] == "[gza:info] Task: #1 20260101-test-task"

    def test_gza_metadata_mixed_with_info_only_info_appears(self) -> None:
        """Only info entries appear in timeline when mixed with metadata entries."""
        entries = [
            {"type": "gza", "subtype": "branch", "message": "Branch: feat/foo"},
            {"type": "gza", "subtype": "info", "message": "Task: #1 slug"},
            {"type": "gza", "subtype": "stats", "message": "Stats: 1 step, 5.0s, $0.0001"},
            {"type": "gza", "subtype": "outcome", "message": "Outcome: completed"},
        ]
        steps = _build_step_timeline(entries)
        assert len(steps) == 1
        assert steps[0]["message_text"] == "[gza:info] Task: #1 slug"
