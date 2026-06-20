"""Tests for interactive attach wrapper behavior."""

import json
import signal
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

from gza.attach_wrapper import main
from gza.config import Config
from gza.db import SqliteTaskStore
from gza.log_paths import ops_log_path_for
from gza.recovery_engine import FailedRecoveryDecision, decide_failed_task_recovery


def _setup_task_with_log(project_dir: Path, *, task_type: str = "implement") -> tuple[str, Path]:
    (project_dir / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "use_docker: false\n"
    )
    (project_dir / ".gza" / "logs").mkdir(parents=True, exist_ok=True)
    config = Config.load(project_dir)
    store = SqliteTaskStore(project_dir / ".gza" / "gza.db", prefix=config.project_prefix)
    task = store.add("Test attach wrapper", task_type=task_type)
    task.log_file = ".gza/logs/task.log"
    store.update(task)
    assert task.id is not None
    return task.id, project_dir / task.log_file


def _read_log_events(log_path: Path) -> list[dict]:
    ops_path = ops_log_path_for(log_path)
    return [json.loads(line) for line in ops_path.read_text().splitlines() if line.strip()]


def test_attach_wrapper_normal_exit_auto_resumes_when_task_incomplete(tmp_path: Path) -> None:
    """Normal interactive exit should resume in background when task is incomplete."""
    task_id, log_path = _setup_task_with_log(tmp_path)

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-123",
            "--project", str(tmp_path),
        ]),
        patch("gza.attach_wrapper._run_interactive_claude", return_value=0),
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0) as mock_spawn,
    ):
        rc = main()

    assert rc == 0
    mock_spawn.assert_called_once()

    events = _read_log_events(log_path)
    names = [event["event"] for event in events if event.get("subtype") == "worker_lifecycle"]
    assert "attach" in names
    detach_events = [event for event in events if event.get("event") == "detach"]
    assert detach_events
    assert detach_events[-1]["reason"] == "exited_ok"
    assert "resume" in names


def test_attach_wrapper_sets_foreground_attach_resume_execution_mode(tmp_path: Path) -> None:
    """Interactive attach/resume should stamp foreground_attach_resume provenance on the task."""
    task_id, _ = _setup_task_with_log(tmp_path)

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-123",
            "--project", str(tmp_path),
        ]),
        patch("gza.attach_wrapper._run_interactive_claude", return_value=0),
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0),
    ):
        rc = main()

    assert rc == 0
    config = Config.load(tmp_path)
    store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)
    refreshed = store.get(task_id)
    assert refreshed is not None
    assert refreshed.execution_mode == "foreground_attach_resume"


def test_attach_wrapper_sigterm_detach_auto_resumes(tmp_path: Path) -> None:
    """SIGTERM detach path should auto-resume in background and emit lifecycle events."""
    task_id, log_path = _setup_task_with_log(tmp_path)
    handlers: dict[int, object] = {}

    def fake_signal(sig, handler):
        handlers[sig] = handler
        return None

    def fake_run_interactive(_config, _session_id, *, max_turns=None, task=None, no_docker=False):
        del max_turns, task, no_docker
        assert signal.SIGTERM in handlers
        handlers[signal.SIGTERM](signal.SIGTERM, None)
        return 0

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-123",
            "--project", str(tmp_path),
        ]),
        patch("gza.attach_wrapper.signal.signal", side_effect=fake_signal),
        patch("gza.attach_wrapper._run_interactive_claude", side_effect=fake_run_interactive),
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0) as mock_spawn,
    ):
        rc = main()

    assert rc == 128 + signal.SIGTERM
    mock_spawn.assert_called_once()

    events = _read_log_events(log_path)
    names = [event["event"] for event in events if event.get("subtype") == "worker_lifecycle"]
    assert "attach" in names
    assert "detach" in names
    assert "resume" in names
    detach_events = [event for event in events if event.get("event") == "detach"]
    assert detach_events[-1]["reason"] == "detached"


def test_attach_wrapper_resume_failure_keeps_task_pending(tmp_path: Path) -> None:
    """Failed detach/exit handoff should not emit resume success or rewrite task status."""
    task_id, log_path = _setup_task_with_log(tmp_path)

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-123",
            "--project", str(tmp_path),
        ]),
        patch("gza.attach_wrapper._run_interactive_claude", return_value=0),
        patch("gza.attach_wrapper._spawn_background_worker", return_value=7),
    ):
        rc = main()

    assert rc == 0
    events = _read_log_events(log_path)
    lifecycle_events = [event for event in events if event.get("subtype") == "worker_lifecycle"]
    event_names = [event["event"] for event in lifecycle_events]
    assert "resume" not in event_names
    assert "resume_failed" in event_names
    failure_event = [event for event in lifecycle_events if event["event"] == "resume_failed"][-1]
    assert failure_event["handoff_exit_code"] == 7

    config = Config.load(tmp_path)
    store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)
    refreshed = store.get(task_id)
    assert refreshed is not None
    assert refreshed.status == "pending"
    assert refreshed.failure_reason is None


def test_attach_wrapper_failed_resume_descendant_does_not_auto_resume(tmp_path: Path) -> None:
    """Failed resume descendants should not bypass shared recovery policy via attach handoff."""
    task_id, _ = _setup_task_with_log(tmp_path)
    config = Config.load(tmp_path)
    store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)

    original = store.get(task_id)
    assert original is not None
    assert original.id is not None
    original.status = "failed"
    original.failure_reason = "MAX_TURNS"
    original.session_id = "sess-123"
    store.update(original)

    failed_resume_descendant = store.add(
        original.prompt,
        task_type=original.task_type,
        based_on=original.id,
    )
    assert failed_resume_descendant.id is not None
    failed_resume_descendant.status = "failed"
    failed_resume_descendant.failure_reason = "INFRASTRUCTURE_ERROR"
    failed_resume_descendant.session_id = original.session_id
    store.update(failed_resume_descendant)

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", failed_resume_descendant.id,
            "--session-id", original.session_id,
            "--project", str(tmp_path),
        ]),
        patch("gza.attach_wrapper._run_interactive_claude", return_value=0),
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0) as mock_spawn,
    ):
        rc = main()

    assert rc == 0
    mock_spawn.assert_not_called()


def test_attach_wrapper_manual_review_failed_task_does_not_auto_resume(tmp_path: Path) -> None:
    """Manual-review-only failed reasons should not auto-resume after interactive attach exit."""
    task_id, _ = _setup_task_with_log(tmp_path)
    config = Config.load(tmp_path)
    store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)

    failed = store.get(task_id)
    assert failed is not None
    failed.status = "failed"
    failed.failure_reason = "TEST_FAILURE"
    failed.session_id = "sess-123"
    store.update(failed)

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-123",
            "--project", str(tmp_path),
        ]),
        patch("gza.attach_wrapper._run_interactive_claude", return_value=0),
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0) as mock_spawn,
    ):
        rc = main()

    assert rc == 0
    mock_spawn.assert_not_called()


def test_attach_wrapper_timeout_failed_implement_handoff_launches_iterate_resume(tmp_path: Path) -> None:
    """Timeout failed implement handoff should relaunch through iterate resume, not a plain worker.

    The Phase-1 boundary requires the parent to create and prepare a resume
    recovery task before detachment, and hand its identity to the iterate spawn
    as prepared metadata.
    """
    task_id, _ = _setup_task_with_log(tmp_path)
    config = Config.load(tmp_path)
    store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)

    failed = store.get(task_id)
    assert failed is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-123"
    store.update(failed)

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-123",
            "--project", str(tmp_path),
        ]),
        patch("gza.attach_wrapper._run_interactive_claude", return_value=0),
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0) as mock_spawn_worker,
        patch("gza.attach_wrapper._spawn_background_iterate", return_value=0) as mock_spawn_iterate,
        patch(
            "gza.attach_wrapper._prepare_task_for_immediate_execution",
            side_effect=lambda _config, task, **_kwargs: task,
        ),
    ):
        rc = main()

    assert rc == 0
    mock_spawn_worker.assert_not_called()
    mock_spawn_iterate.assert_called_once()
    spawned_args = mock_spawn_iterate.call_args.args[0]
    spawned_task = mock_spawn_iterate.call_args.args[2]
    assert spawned_args.resume is False
    assert spawned_args.retry is False
    # Iterate targets the prepared recovery child directly; the parent only
    # contributes the recovery decision and prepared metadata.
    resume_children = store.get_based_on_children(task_id)
    assert len(resume_children) == 1
    resume_child = resume_children[0]
    assert spawned_task.id == resume_child.id
    assert resume_child.based_on == task_id
    assert resume_child.recovery_origin == "resume"
    spawned_kwargs = mock_spawn_iterate.call_args.kwargs
    assert spawned_kwargs.get("prepared_task_id") == resume_child.id
    assert spawned_kwargs.get("prepared_resume") is True
    assert spawned_kwargs.get("prepared_phase") == "preloop"


def test_attach_wrapper_retryable_failed_implement_handoff_launches_iterate_retry(tmp_path: Path) -> None:
    """Retryable failed implement handoff should relaunch a retry child via iterate."""
    task_id, log_path = _setup_task_with_log(tmp_path)
    config = Config.load(tmp_path)
    store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)

    failed = store.get(task_id)
    assert failed is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    store.update(failed)

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-123",
            "--project", str(tmp_path),
        ]),
        patch("gza.attach_wrapper._run_interactive_claude", return_value=0),
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0) as mock_spawn_worker,
        patch("gza.attach_wrapper._spawn_background_iterate", return_value=0) as mock_spawn_iterate,
        patch(
            "gza.attach_wrapper._prepare_task_for_immediate_execution",
            side_effect=lambda _config, task, **_kwargs: task,
        ),
    ):
        rc = main()

    assert rc == 0
    mock_spawn_worker.assert_not_called()
    mock_spawn_iterate.assert_called_once()
    spawned_args = mock_spawn_iterate.call_args.args[0]
    retry_child = mock_spawn_iterate.call_args.args[2]
    assert spawned_args.resume is False
    assert spawned_args.retry is False
    assert retry_child.id is not None
    assert retry_child.id != task_id
    assert retry_child.based_on == task_id
    events = _read_log_events(log_path)
    lifecycle_events = [event for event in events if event.get("subtype") == "worker_lifecycle"]
    event_names = [event["event"] for event in lifecycle_events]
    assert "retry" in event_names
    assert "resume" not in event_names


def test_attach_wrapper_retry_handoff_failure_logs_retry_failed(tmp_path: Path) -> None:
    """Retry handoff failures should emit retry_failed rather than resume_failed events."""
    task_id, log_path = _setup_task_with_log(tmp_path)
    config = Config.load(tmp_path)
    store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)

    failed = store.get(task_id)
    assert failed is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    store.update(failed)

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-123",
            "--project", str(tmp_path),
        ]),
        patch("gza.attach_wrapper._run_interactive_claude", return_value=0),
        patch("gza.attach_wrapper._spawn_background_iterate", return_value=9),
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0),
        patch(
            "gza.attach_wrapper._prepare_task_for_immediate_execution",
            side_effect=lambda _config, task, **_kwargs: task,
        ),
    ):
        rc = main()

    assert rc == 0
    events = _read_log_events(log_path)
    lifecycle_events = [event for event in events if event.get("subtype") == "worker_lifecycle"]
    event_names = [event["event"] for event in lifecycle_events]
    assert "retry_failed" in event_names
    assert "resume_failed" not in event_names
    failure_event = [event for event in lifecycle_events if event["event"] == "retry_failed"][-1]
    assert failure_event["handoff_exit_code"] == 9
    retry_children = store.get_based_on_children(task_id)
    assert len(retry_children) == 1
    retry_child = retry_children[0]
    assert retry_child.status == "pending"
    refreshed_failed = store.get(task_id)
    assert refreshed_failed is not None
    decision = decide_failed_task_recovery(store, refreshed_failed, max_recovery_attempts=config.max_resume_attempts)
    assert decision.action == "retry"
    assert decision.reuse_existing is True
    assert decision.recovery_task_id == retry_child.id


def test_attach_wrapper_timeout_handoff_spawn_failure_keeps_resume_child_pending_for_shared_policy(tmp_path: Path) -> None:
    """Timeout handoff spawn failures must keep resume descendants pending for shared recovery evaluation."""
    task_id, log_path = _setup_task_with_log(tmp_path, task_type="plan")
    config = Config.load(tmp_path)
    store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)

    failed = store.get(task_id)
    assert failed is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-123"
    store.update(failed)

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-123",
            "--project", str(tmp_path),
        ]),
        patch("gza.attach_wrapper._run_interactive_claude", return_value=0),
        patch("gza.attach_wrapper._spawn_background_worker", return_value=7),
    ):
        rc = main()

    assert rc == 0
    events = _read_log_events(log_path)
    lifecycle_events = [event for event in events if event.get("subtype") == "worker_lifecycle"]
    assert "resume_failed" in [event["event"] for event in lifecycle_events]

    resume_children = store.get_based_on_children(task_id)
    assert len(resume_children) == 1
    resume_child = resume_children[0]
    assert resume_child.status == "pending"

    refreshed_failed = store.get(task_id)
    assert refreshed_failed is not None
    decision = decide_failed_task_recovery(store, refreshed_failed, max_recovery_attempts=config.max_resume_attempts)
    assert decision.action == "resume"
    assert decision.reuse_existing is True
    assert decision.recovery_task_id == resume_child.id


def test_attach_wrapper_retryable_failed_non_implement_handoff_uses_worker_path(tmp_path: Path) -> None:
    """Failed non-implement handoff should keep using plain worker execution."""
    task_id, _ = _setup_task_with_log(tmp_path, task_type="plan")
    config = Config.load(tmp_path)
    store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)

    failed = store.get(task_id)
    assert failed is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    store.update(failed)

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-123",
            "--project", str(tmp_path),
        ]),
        patch("gza.attach_wrapper._run_interactive_claude", return_value=0),
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0) as mock_spawn_worker,
        patch("gza.attach_wrapper._spawn_background_iterate", return_value=0) as mock_spawn_iterate,
        patch(
            "gza.attach_wrapper._prepare_task_for_immediate_execution",
            side_effect=lambda _config, task, **_kwargs: task,
        ),
    ):
        rc = main()

    assert rc == 0
    mock_spawn_iterate.assert_not_called()
    mock_spawn_worker.assert_called_once()
    spawned_task_id = mock_spawn_worker.call_args.kwargs["task_id"]
    spawned_task = store.get(spawned_task_id)
    assert spawned_task is not None
    assert spawned_task.based_on == task_id


def test_attach_wrapper_failed_resume_descendant_does_not_auto_recover_further(tmp_path: Path) -> None:
    """Failed resume descendants should not get another automatic handoff recovery run."""
    task_id, _ = _setup_task_with_log(tmp_path)
    config = Config.load(tmp_path)
    store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)

    original = store.get(task_id)
    assert original is not None
    assert original.id is not None
    original.status = "failed"
    original.failure_reason = "MAX_TURNS"
    original.session_id = "sess-123"
    store.update(original)

    failed_resume_descendant = store.add(
        original.prompt,
        task_type=original.task_type,
        based_on=original.id,
    )
    assert failed_resume_descendant.id is not None
    failed_resume_descendant.status = "failed"
    failed_resume_descendant.failure_reason = "INFRASTRUCTURE_ERROR"
    failed_resume_descendant.session_id = original.session_id
    store.update(failed_resume_descendant)

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", failed_resume_descendant.id,
            "--session-id", original.session_id,
            "--project", str(tmp_path),
        ]),
        patch("gza.attach_wrapper._run_interactive_claude", return_value=0),
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0) as mock_spawn,
    ):
        rc = main()

    assert rc == 0
    mock_spawn.assert_not_called()


def test_attach_wrapper_timeout_parent_with_failed_resume_descendant_stops_at_manual_review(
    tmp_path: Path,
) -> None:
    """Repeated handoff after timeout budget consumption should not relaunch original task."""
    task_id, _ = _setup_task_with_log(tmp_path)
    config = Config.load(tmp_path)
    store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)

    original = store.get(task_id)
    assert original is not None
    assert original.id is not None
    original.status = "failed"
    original.failure_reason = "MAX_TURNS"
    original.session_id = "sess-123"
    store.update(original)

    failed_resume_descendant = store.add(
        original.prompt,
        task_type=original.task_type,
        based_on=original.id,
    )
    assert failed_resume_descendant.id is not None
    failed_resume_descendant.status = "failed"
    failed_resume_descendant.failure_reason = "MAX_TURNS"
    failed_resume_descendant.session_id = original.session_id
    store.update(failed_resume_descendant)

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", original.session_id,
            "--project", str(tmp_path),
        ]),
        patch(
            "gza.attach_wrapper.decide_failed_task_recovery",
            return_value=FailedRecoveryDecision(
                task_id=task_id,
                action="skip",
                reason_code="retry_limit_reached",
                reason_text="automatic recovery stops here; retry limit reached",
                launch_mode="none",
                attempt_index=1,
                attempt_limit=config.max_resume_attempts,
            ),
        ),
        patch("gza.attach_wrapper._run_interactive_claude", return_value=0),
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0) as mock_spawn,
    ):
        rc = main()

    assert rc == 0
    mock_spawn.assert_not_called()


def test_attach_wrapper_passes_resume_overrides_to_background_worker(tmp_path: Path) -> None:
    """Wrapper should preserve no-docker/max-turns/force overrides when respawning."""
    task_id, _ = _setup_task_with_log(tmp_path)

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-123",
            "--project", str(tmp_path),
            "--no-docker",
            "--max-turns", "66",
            "--force",
        ]),
        patch("gza.attach_wrapper._run_interactive_claude", return_value=0) as mock_interactive,
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0) as mock_spawn,
    ):
        rc = main()

    assert rc == 0
    mock_interactive.assert_called_once()
    assert mock_interactive.call_args.kwargs["max_turns"] == 66
    assert mock_spawn.call_count == 1
    worker_args = mock_spawn.call_args[0][0]
    assert worker_args.no_docker is True
    assert worker_args.max_turns == 66
    assert worker_args.force is True


def test_attach_wrapper_sigint_during_interactive_forwarded_to_child_without_detach_state(tmp_path: Path) -> None:
    """Ctrl-C during interactive attach should be forwarded to Claude, not treated as wrapper detach."""
    task_id, log_path = _setup_task_with_log(tmp_path)
    handlers: dict[int, object] = {}

    def fake_signal(sig, handler):
        previous = handlers.get(sig, signal.SIG_DFL)
        handlers[sig] = handler
        return previous

    fake_proc = MagicMock()
    fake_proc.pid = 4242

    def fake_wait():
        assert signal.SIGINT in handlers
        handlers[signal.SIGINT](signal.SIGINT, None)
        return 0

    fake_proc.wait.side_effect = fake_wait

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-123",
            "--project", str(tmp_path),
        ]),
        patch("gza.attach_wrapper.signal.signal", side_effect=fake_signal),
        patch("gza.attach_wrapper.subprocess.Popen", return_value=fake_proc),
        patch("gza.attach_wrapper.os.kill") as mock_kill,
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0) as mock_spawn,
    ):
        rc = main()

    assert rc == 0
    mock_kill.assert_called_once_with(4242, signal.SIGINT)
    mock_spawn.assert_called_once()

    events = _read_log_events(log_path)
    detach_events = [event for event in events if event.get("event") == "detach"]
    assert detach_events
    assert detach_events[-1]["reason"] == "exited_ok"


def test_attach_wrapper_calls_load_dotenv_before_interactive_claude(tmp_path: Path) -> None:
    """Attach wrapper must load .env files so API keys are available during interactive session."""
    task_id, _ = _setup_task_with_log(tmp_path)

    call_order: list[str] = []

    def track_dotenv(project_dir):
        call_order.append("load_dotenv")

    def track_interactive(*args, **kwargs):
        call_order.append("interactive_claude")
        return 0

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-123",
            "--project", str(tmp_path),
        ]),
        patch("gza.attach_wrapper.load_dotenv", side_effect=track_dotenv) as mock_dotenv,
        patch("gza.attach_wrapper._run_interactive_claude", side_effect=track_interactive),
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0),
    ):
        rc = main()

    assert rc == 0
    mock_dotenv.assert_called_once_with(tmp_path)
    assert call_order.index("load_dotenv") < call_order.index("interactive_claude"), \
        "load_dotenv must be called before _run_interactive_claude"


def _setup_docker_task(project_dir: Path) -> tuple[str, Path]:
    (project_dir / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "use_docker: true\n"
        "docker_image: test-project-gza\n"
    )
    (project_dir / ".gza" / "logs").mkdir(parents=True, exist_ok=True)
    config = Config.load(project_dir)
    assert config.use_docker is True
    store = SqliteTaskStore(project_dir / ".gza" / "gza.db", prefix=config.project_prefix)
    task = store.add("Test docker attach")
    task.log_file = ".gza/logs/task.log"
    store.update(task)
    assert task.id is not None
    return task.id, project_dir / task.log_file


def test_attach_wrapper_docker_task_launches_via_docker(tmp_path: Path) -> None:
    """Docker-backed Claude task must route the interactive resume through Docker, not host claude."""
    task_id, _ = _setup_docker_task(tmp_path)

    fake_proc = MagicMock()
    fake_proc.pid = 5555
    fake_proc.wait.return_value = 0

    docker_cmd_stub = [
        "timeout", "60m",
        "docker", "run", "--rm", "-it",
        "-v", f"{tmp_path}:/workspace",
        "-w", "/workspace",
        "test-project-gza-claude",
        "claude", "--resume", "sess-docker", "--max-turns", "200",
    ]

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-docker",
            "--project", str(tmp_path),
        ]),
        patch(
            "gza.attach_wrapper._build_docker_interactive_cmd",
            return_value=docker_cmd_stub,
        ) as mock_build,
        patch("gza.attach_wrapper.subprocess.Popen", return_value=fake_proc) as mock_popen,
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0),
    ):
        rc = main()

    assert rc == 0
    mock_build.assert_called_once()
    _, build_kwargs = mock_build.call_args
    assert build_kwargs["max_turns"] is None
    # First positional: config; second: task; third: session id.
    build_args = mock_build.call_args[0]
    assert build_args[2] == "sess-docker"

    mock_popen.assert_called_once()
    cmd = mock_popen.call_args[0][0]
    assert cmd is docker_cmd_stub
    assert mock_popen.call_args.kwargs.get("cwd") is None


def test_build_docker_interactive_cmd_uses_it_and_claude_resume(tmp_path: Path) -> None:
    """Unit test: _build_docker_interactive_cmd builds a -it docker run + claude --resume command."""
    from gza.attach_wrapper import _build_docker_interactive_cmd

    task_id, _ = _setup_docker_task(tmp_path)
    config = Config.load(tmp_path)
    store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)
    task = store.get(task_id)
    assert task is not None

    with (
        patch("gza.attach_wrapper.ensure_docker_image", return_value=True),
        patch("gza.attach_wrapper.sync_keychain_credentials", return_value=True),
        patch("gza.providers.base.subprocess.run", return_value=MagicMock(returncode=1, stdout="")),
    ):
        cmd = _build_docker_interactive_cmd(config, task, "sess-docker", max_turns=42)

    assert "docker" in cmd
    assert "run" in cmd
    assert "-it" in cmd, f"expected -it for interactive attach, got {cmd}"
    assert "-i" not in cmd
    claude_idx = cmd.index("claude")
    assert cmd[claude_idx + 1 : claude_idx + 3] == ["--resume", "sess-docker"]
    assert "--max-turns" in cmd[claude_idx:]
    max_turns_idx = cmd.index("--max-turns", claude_idx)
    assert cmd[max_turns_idx + 1] == "42"


def test_build_docker_interactive_cmd_tolerates_unwritable_provider_json_mirror(tmp_path: Path) -> None:
    """Docker command construction should fail soft when the provider JSON mirror cannot be written."""
    from gza.attach_wrapper import _build_docker_interactive_cmd

    task_id, _ = _setup_docker_task(tmp_path)
    config = Config.load(tmp_path)
    store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)
    task = store.get(task_id)
    assert task is not None

    with (
        patch("gza.attach_wrapper.ensure_docker_image", return_value=True),
        patch("gza.attach_wrapper.sync_keychain_credentials", return_value=True),
        patch("gza.providers.base.shutil.copy2", side_effect=PermissionError("denied")),
        patch("gza.providers.base.subprocess.run", return_value=MagicMock(returncode=1, stdout="")),
    ):
        cmd = _build_docker_interactive_cmd(config, task, "sess-docker", max_turns=42)

    assert "docker" in cmd
    assert "run" in cmd


def test_attach_wrapper_docker_task_with_no_docker_flag_uses_host(tmp_path: Path) -> None:
    """Even when config.use_docker is True, --no-docker should preserve host execution."""
    task_id, _ = _setup_docker_task(tmp_path)

    fake_proc = MagicMock()
    fake_proc.pid = 5555
    fake_proc.wait.return_value = 0

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-host",
            "--project", str(tmp_path),
            "--no-docker",
        ]),
        patch("gza.attach_wrapper.subprocess.Popen", return_value=fake_proc) as mock_popen,
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0),
    ):
        rc = main()

    assert rc == 0
    mock_popen.assert_called_once()
    cmd = mock_popen.call_args[0][0]
    assert cmd[0] == "claude"
    assert "docker" not in cmd
    assert mock_popen.call_args.kwargs.get("cwd") == tmp_path


def test_attach_wrapper_non_docker_task_uses_host(tmp_path: Path) -> None:
    """Tasks without use_docker must continue to resume via host claude."""
    task_id, _ = _setup_task_with_log(tmp_path)

    fake_proc = MagicMock()
    fake_proc.pid = 5555
    fake_proc.wait.return_value = 0

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-plain",
            "--project", str(tmp_path),
        ]),
        patch("gza.attach_wrapper.subprocess.Popen", return_value=fake_proc) as mock_popen,
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0),
    ):
        rc = main()

    assert rc == 0
    mock_popen.assert_called_once()
    cmd = mock_popen.call_args[0][0]
    assert cmd[0] == "claude"
    assert "docker" not in cmd


def test_attach_wrapper_resume_iterate_prepare_failure_rolls_back_recovery(tmp_path: Path) -> None:
    """When resume+iterate parent-side preparation fails for a freshly-created
    recovery task, the iterate worker must not spawn, the recovery row must be
    rolled back, and a resume_failed lifecycle event must be logged so the
    failure is visible to the caller."""
    task_id, log_path = _setup_task_with_log(tmp_path)
    config = Config.load(tmp_path)
    store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)

    failed = store.get(task_id)
    assert failed is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-123"
    store.update(failed)

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-123",
            "--project", str(tmp_path),
        ]),
        patch("gza.attach_wrapper._run_interactive_claude", return_value=0),
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0) as mock_spawn_worker,
        patch("gza.attach_wrapper._spawn_background_iterate", return_value=0) as mock_spawn_iterate,
        patch(
            "gza.attach_wrapper._prepare_task_for_immediate_execution",
            return_value=None,
        ) as mock_prepare,
    ):
        rc = main()

    assert rc == 0  # the attach session exit code; the handoff failure is logged.
    mock_spawn_worker.assert_not_called()
    mock_spawn_iterate.assert_not_called()
    # Prepare was invoked with rollback_on_failure=True for the freshly-created
    # resume child.
    mock_prepare.assert_called_once()
    prep_kwargs = mock_prepare.call_args.kwargs
    assert prep_kwargs.get("rollback_on_failure") is True
    # The patched prepare returns None, which simulates either a) a real failure
    # in prepare_task_startup_phase that ran rollback internally, or b) a stub
    # that returns None without performing rollback. The contract this test
    # asserts is that the spawn does not happen on prepare failure. Real
    # rollback of the recovery row lives in _prepare_task_for_immediate_execution
    # and is covered by its own tests.
    events = _read_log_events(log_path)
    lifecycle_events = [event for event in events if event.get("subtype") == "worker_lifecycle"]
    assert "resume_failed" in [event["event"] for event in lifecycle_events]


def test_attach_wrapper_retry_iterate_prepare_failure_rolls_back_recovery(tmp_path: Path) -> None:
    """Same Phase-1 contract for retry+iterate: prepare failure on a freshly
    created retry child must not spawn iterate, and must surface as a
    retry_failed lifecycle event."""
    task_id, log_path = _setup_task_with_log(tmp_path)
    config = Config.load(tmp_path)
    store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)

    failed = store.get(task_id)
    assert failed is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    store.update(failed)

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-123",
            "--project", str(tmp_path),
        ]),
        patch("gza.attach_wrapper._run_interactive_claude", return_value=0),
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0) as mock_spawn_worker,
        patch("gza.attach_wrapper._spawn_background_iterate", return_value=0) as mock_spawn_iterate,
        patch(
            "gza.attach_wrapper._prepare_task_for_immediate_execution",
            return_value=None,
        ) as mock_prepare,
    ):
        rc = main()

    assert rc == 0
    mock_spawn_worker.assert_not_called()
    mock_spawn_iterate.assert_not_called()
    mock_prepare.assert_called_once()
    prep_kwargs = mock_prepare.call_args.kwargs
    assert prep_kwargs.get("rollback_on_failure") is True
    events = _read_log_events(log_path)
    lifecycle_events = [event for event in events if event.get("subtype") == "worker_lifecycle"]
    assert "retry_failed" in [event["event"] for event in lifecycle_events]


def test_attach_wrapper_retry_iterate_success_passes_prepared_metadata(tmp_path: Path) -> None:
    """Retry+iterate handoff must hand prepared recovery task id, resume flag,
    and prepared_phase=preloop to _spawn_background_iterate so the detached
    worker inherits the parent-prepared identity."""
    task_id, _ = _setup_task_with_log(tmp_path)
    config = Config.load(tmp_path)
    store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)

    failed = store.get(task_id)
    assert failed is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    store.update(failed)

    retry_child = store.add(failed.prompt, task_type=failed.task_type, based_on=task_id)
    assert retry_child.id is not None
    retry_child.recovery_origin = "retry"
    store.update(retry_child)

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-123",
            "--project", str(tmp_path),
        ]),
        patch(
            "gza.attach_wrapper.decide_failed_task_recovery",
            return_value=FailedRecoveryDecision(
                task_id=task_id,
                action="retry",
                reason_code="INFRASTRUCTURE_ERROR",
                reason_text="INFRASTRUCTURE_ERROR restart with fresh attempt",
                launch_mode="iterate",
                attempt_index=0,
                attempt_limit=config.max_resume_attempts,
                recovery_task_id=retry_child.id,
                reuse_existing=True,
            ),
        ),
        patch("gza.attach_wrapper._run_interactive_claude", return_value=0),
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0) as mock_spawn_worker,
        patch("gza.attach_wrapper._spawn_background_iterate", return_value=0) as mock_spawn_iterate,
        patch(
            "gza.attach_wrapper._prepare_task_for_immediate_execution",
            side_effect=lambda _config, task, **_kwargs: task,
        ),
    ):
        rc = main()

    assert rc == 0
    mock_spawn_worker.assert_not_called()
    mock_spawn_iterate.assert_called_once()
    retry_children = [
        t for t in store.get_based_on_children(task_id) if t.recovery_origin == "retry"
    ]
    assert len(retry_children) == 1
    retry_child = retry_children[0]
    assert mock_spawn_iterate.call_args.args[2].id == retry_child.id
    kwargs = mock_spawn_iterate.call_args.kwargs
    assert kwargs.get("prepared_task_id") == retry_child.id
    assert kwargs.get("prepared_resume") is False
    assert kwargs.get("prepared_phase") == "preloop"
