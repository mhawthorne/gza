from datetime import UTC, datetime

import pytest

from gza.cli._common import (
    _extract_last_agent_message_for_failure,
    _looks_like_task_id,
    format_stats,
    run_with_resume,
)
from gza.config import Config
from gza.db import SqliteTaskStore


class TestLooksLikeTaskId:
    """Unit tests for _looks_like_task_id() — the heuristic that disambiguates task IDs
    from branch names in cmd_checkout and cmd_diff.

    The function accepts only full prefixed IDs.
    """

    @pytest.mark.parametrize(
        "arg, expected",
        [
            # Full prefixed task IDs — alphanumeric prefix + hyphen + decimal suffix
            ("gza-1", True),
            ("gza-000001", True),
            ("myapp-42", True),
            # Non-prefixed forms are rejected
            ("42", False),
            ("3f", False),
            ("a1", False),
            ("1a2b", False),
            ("abc", False),
            ("gza-3f", False),
            ("myapp-abc1", False),
            ("feature", False),
            # Branch names are rejected
            ("feature-add-logging", False),
            ("fix-some-bug", False),
            ("feature-123-thing", False),
        ],
    )
    def test_looks_like_task_id(self, arg: str, expected: bool) -> None:
        assert _looks_like_task_id(arg) == expected, (
            f"_looks_like_task_id({arg!r}) expected {expected}"
        )


class TestFormatStats:
    """Unit tests for compact task stats formatting."""

    def test_format_stats_includes_started_date_and_omits_steps_and_cost(self, tmp_path):
        """Stats include started date while excluding step and cost fields."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Task with stats")
        task.started_at = datetime(2026, 4, 25, 8, 30, tzinfo=UTC)
        task.duration_seconds = 120.0
        task.num_steps_reported = 7
        task.cost_usd = 0.4321
        store.update(task)

        stats = format_stats(task)
        assert "2m0s" in stats
        assert "2026-04-25" in stats
        assert "steps" not in stats
        assert "$" not in stats

    def test_format_stats_includes_attach_counts_and_seconds(self, tmp_path):
        """Attach count and sub-minute attach duration are rendered in seconds."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Task with attach stats")
        task.attach_count = 2
        task.attach_duration_seconds = 45.0
        store.update(task)

        stats = format_stats(task)
        assert "2 attaches (45s)" in stats

    def test_format_stats_includes_attach_duration_minutes_seconds(self, tmp_path):
        """Attach duration >= 60s is rendered as XmYs."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Task with long attach stats")
        task.attach_count = 1
        task.attach_duration_seconds = 125.0
        store.update(task)

        stats = format_stats(task)
        assert "1 attach (2m5s)" in stats


class TestRunWithResume:
    """Unit tests for shared run_with_resume execution helper."""

    def test_resumes_on_handled_timeout_failure_with_zero_exit(self, tmp_path):
        (tmp_path / "gza.yaml").write_text("project_name: test-project\n")
        config = Config.load(tmp_path)
        store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)

        task = store.add("Implement feature", task_type="implement")
        task.session_id = "sess-123"
        store.update(task)

        outcomes = ["TIMEOUT", None]
        seen_resume_flags: list[bool] = []

        def _run_task(run_task, resume: bool) -> int:
            seen_resume_flags.append(resume)
            outcome = outcomes.pop(0)
            if outcome is None:
                run_task.status = "completed"
                store.update(run_task)
                return 0
            run_task.status = "failed"
            run_task.failure_reason = outcome
            run_task.session_id = "sess-123"
            store.update(run_task)
            return 0

        final_task, rc = run_with_resume(
            config,
            store,
            task,
            run_task=_run_task,
            max_resume_attempts=1,
        )

        assert rc == 0
        assert final_task.status == "completed"
        assert seen_resume_flags == [False, True]
        assert len(store.get_all()) == 2  # original + 1 resume child

    def test_resumes_on_max_steps_max_turns_and_terminated(self, tmp_path):
        (tmp_path / "gza.yaml").write_text("project_name: test-project\n")
        config = Config.load(tmp_path)
        store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)

        task = store.add("Implement feature", task_type="implement")
        task.session_id = "sess-123"
        store.update(task)

        outcomes = ["MAX_STEPS", "MAX_TURNS", "TERMINATED", None]
        seen_resume_flags: list[bool] = []

        def _run_task(run_task, resume: bool) -> int:
            seen_resume_flags.append(resume)
            outcome = outcomes.pop(0)
            if outcome is None:
                run_task.status = "completed"
                store.update(run_task)
                return 0
            run_task.status = "failed"
            run_task.failure_reason = outcome
            run_task.session_id = "sess-123"
            store.update(run_task)
            return 1

        final_task, rc = run_with_resume(
            config,
            store,
            task,
            run_task=_run_task,
            max_resume_attempts=3,
        )

        assert rc == 0
        assert final_task.status == "completed"
        assert seen_resume_flags == [False, True, True, True]
        assert len(store.get_all()) == 4  # original + 3 resume children

    def test_stops_after_max_resume_attempts(self, tmp_path):
        (tmp_path / "gza.yaml").write_text("project_name: test-project\n")
        config = Config.load(tmp_path)
        store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)

        task = store.add("Implement feature", task_type="implement")
        task.session_id = "sess-123"
        store.update(task)

        def _run_task(run_task, resume: bool) -> int:
            run_task.status = "failed"
            run_task.failure_reason = "MAX_STEPS"
            run_task.session_id = "sess-123"
            store.update(run_task)
            return 1

        final_task, rc = run_with_resume(
            config,
            store,
            task,
            run_task=_run_task,
            max_resume_attempts=2,
        )

        assert rc == 1
        assert final_task.status == "failed"
        assert len(store.get_all()) == 3  # original + 2 resume children

    def test_does_not_resume_on_test_failure(self, tmp_path):
        (tmp_path / "gza.yaml").write_text("project_name: test-project\n")
        config = Config.load(tmp_path)
        store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)

        task = store.add("Implement feature", task_type="implement")
        task.session_id = "sess-123"
        store.update(task)

        seen_resume_flags: list[bool] = []

        def _run_task(run_task, resume: bool) -> int:
            seen_resume_flags.append(resume)
            run_task.status = "failed"
            run_task.failure_reason = "TEST_FAILURE"
            run_task.session_id = "sess-123"
            store.update(run_task)
            return 1

        final_task, rc = run_with_resume(
            config,
            store,
            task,
            run_task=_run_task,
            max_resume_attempts=3,
        )

        assert rc == 1
        assert final_task.status == "failed"
        assert seen_resume_flags == [False]
        assert len(store.get_all()) == 1

    def test_returns_nonzero_for_handled_failed_outcome_with_zero_exit(self, tmp_path):
        (tmp_path / "gza.yaml").write_text("project_name: test-project\n")
        config = Config.load(tmp_path)
        store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)

        task = store.add("Implement feature", task_type="implement")
        task.session_id = "sess-123"
        store.update(task)

        def _run_task(run_task, resume: bool) -> int:
            del resume
            run_task.status = "failed"
            run_task.failure_reason = "TEST_FAILURE"
            run_task.session_id = "sess-123"
            store.update(run_task)
            return 0

        final_task, rc = run_with_resume(
            config,
            store,
            task,
            run_task=_run_task,
            max_resume_attempts=3,
        )

        assert rc == 1
        assert final_task.status == "failed"
        assert len(store.get_all()) == 1


class TestExtractLastAgentMessageForFailure:
    """Unit tests for extracting final agent explanation from JSONL logs."""

    def test_returns_last_agent_message_and_strips_failure_marker(self, tmp_path):
        import json

        log_path = tmp_path / "run.log"
        log_path.write_text(
            "\n".join(
                [
                    json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "First explanation"}}),
                    json.dumps(
                        {
                            "type": "item.completed",
                            "item": {
                                "type": "agent_message",
                                "text": "[GZA_FAILURE:MAX_STEPS]\nSecond explanation line 1\nline 2",
                            },
                        }
                    ),
                ]
            )
        )

        explanation = _extract_last_agent_message_for_failure(log_path)
        assert explanation == "Second explanation line 1\nline 2"

    def test_returns_none_when_no_agent_messages(self, tmp_path):
        import json

        log_path = tmp_path / "run.log"
        log_path.write_text(
            "\n".join(
                [
                    json.dumps({"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "hello"}]}}),
                    json.dumps({"type": "result", "subtype": "error_max_steps"}),
                ]
            )
        )

        assert _extract_last_agent_message_for_failure(log_path) is None

    def test_tolerates_malformed_json_lines(self, tmp_path):
        import json

        log_path = tmp_path / "run.log"
        log_path.write_text(
            "\n".join(
                [
                    "{bad-json-line",
                    json.dumps(
                        {
                            "type": "item.completed",
                            "item": {
                                "type": "agent_message",
                                "text": "[GZA_FAILURE:UNKNOWN]\nUsable explanation",
                            },
                        }
                    ),
                ]
            )
        )

        assert _extract_last_agent_message_for_failure(log_path) == "Usable explanation"
