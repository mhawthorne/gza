"""Subprocess CLI log-follow regression tests."""

from pathlib import Path

from tests.cli.conftest import make_store, setup_config
from tests.helpers.cli import run_gza_subprocess


class TestLogFollowSubprocess:
    def test_log_follow_by_task_not_running_falls_back_to_static_output(self, tmp_path: Path):
        """-t -f should print persisted logs and exit when task is not actively running."""
        import json

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Completed task with persisted log")
        task.status = "completed"
        task.log_file = ".gza/logs/static.log"
        store.update(task)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "static.log").write_text(
            "\n".join(
                [
                    json.dumps(
                        {
                            "type": "assistant",
                            "message": {
                                "role": "assistant",
                                "content": [{"type": "text", "text": "persisted output"}],
                            },
                        }
                    ),
                    json.dumps(
                        {
                            "type": "result",
                            "subtype": "success",
                            "result": "done",
                            "num_steps": 1,
                            "duration_ms": 1000,
                            "total_cost_usd": 0.01,
                        }
                    ),
                ]
            )
        )

        result = run_gza_subprocess(
            "log",
            str(task.id),
            "--follow",
            "--project",
            str(tmp_path),
            timeout=2,
        )

        assert result.returncode == 0
        assert "persisted output" in result.stdout
        assert "Task: Completed task with persisted log" in result.stdout
        assert "ID:" in result.stdout

    def test_log_follow_raw_by_task_not_running_falls_back_to_static_output(self, tmp_path: Path):
        """-t -f --raw should print persisted logs and exit when task is not actively running."""
        import json

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Completed task with persisted raw log")
        task.status = "completed"
        task.log_file = ".gza/logs/static-raw.log"
        store.update(task)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "static-raw.log").write_text(
            "\n".join(
                [
                    json.dumps(
                        {
                            "type": "assistant",
                            "message": {
                                "role": "assistant",
                                "content": [{"type": "text", "text": "persisted raw output"}],
                            },
                        }
                    ),
                    json.dumps(
                        {
                            "type": "result",
                            "subtype": "success",
                            "result": "done",
                            "num_steps": 1,
                            "duration_ms": 1000,
                            "total_cost_usd": 0.01,
                        }
                    ),
                ]
            )
        )

        result = run_gza_subprocess(
            "log",
            str(task.id),
            "--follow",
            "--raw",
            "--project",
            str(tmp_path),
            timeout=2,
        )

        assert result.returncode == 0
        assert "persisted raw output" in result.stdout
        assert "Task: Completed task with persisted raw log" not in result.stdout
