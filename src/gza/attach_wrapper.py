"""Interactive attach wrapper for kill/resume tmux sessions."""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from .cli._common import (
    _create_resume_task,
    _create_retry_task,
    _prepare_task_for_immediate_execution,
    _spawn_background_worker,
    get_store,
)
from .cli.execution import _spawn_background_iterate
from .config import Config
from .providers.base import build_docker_cmd, ensure_docker_image
from .providers.claude import _get_docker_config, sync_keychain_credentials
from .recovery_engine import decide_failed_task_recovery
from .runner import load_dotenv, write_log_entry


def _task_log_path(config: Config, task) -> Path | None:
    if not task.log_file:
        return None
    return config.project_dir / task.log_file


def _build_interactive_claude_args(config: Config, session_id: str, *, max_turns: int | None) -> list[str]:
    args = ["--resume", session_id]
    if config.model:
        args.extend(["--model", config.model])
    args.extend(config.claude.args)
    effective_max_turns = max_turns if max_turns is not None else config.max_steps
    args.extend(["--max-turns", str(effective_max_turns)])
    return args


def _resolve_work_dir(config: Config, task) -> Path:
    """Prefer the task's worktree (if it exists on disk) so Docker mounts match the background worker's environment."""
    slug = getattr(task, "slug", None)
    if slug:
        worktree = config.worktree_path / slug
        if worktree.exists():
            return worktree
    return config.project_dir


def _build_docker_interactive_cmd(
    config: Config,
    task,
    session_id: str,
    *,
    max_turns: int | None,
) -> list[str]:
    """Build a Docker-backed interactive claude --resume command.

    Mirrors the direct-mode args used by ``ClaudeProvider._run_docker`` but with
    a TTY-allocated container and the interactive (non-pipe) argument list.
    """
    if config.claude.fetch_auth_token_from_keychain:
        sync_keychain_credentials()
    docker_config = _get_docker_config(
        f"{config.docker_image}-claude",
        docker_startup_timeout=config.docker_startup_timeout,
    )
    work_dir = _resolve_work_dir(config, task)
    if not ensure_docker_image(docker_config, config.project_dir):
        raise RuntimeError("failed to build Docker image for interactive attach")
    cmd = build_docker_cmd(
        docker_config,
        work_dir,
        config.timeout_minutes,
        config.docker_volumes,
        config.docker_setup_command,
        getattr(config, "docker_env", None),
        interactive=True,
    )
    cmd.append("claude")
    cmd.extend(_build_interactive_claude_args(config, session_id, max_turns=max_turns))
    return cmd


def _run_interactive_claude(
    config: Config,
    session_id: str,
    *,
    max_turns: int | None = None,
    task=None,
    no_docker: bool = False,
) -> int:
    use_docker = bool(config.use_docker) and not no_docker and task is not None
    if use_docker:
        try:
            cmd = _build_docker_interactive_cmd(config, task, session_id, max_turns=max_turns)
        except RuntimeError as exc:
            print(f"Error: {exc}")
            return 1
        cwd: Path | None = None
    else:
        cmd = ["claude", *_build_interactive_claude_args(config, session_id, max_turns=max_turns)]
        cwd = config.project_dir

    child = subprocess.Popen(cmd, cwd=cwd)
    original_sigint = signal.getsignal(signal.SIGINT)

    def _forward_sigint(_signum, _frame):
        try:
            os.kill(child.pid, signal.SIGINT)
        except OSError:
            # Child already exited; nothing to forward.
            return

    signal.signal(signal.SIGINT, _forward_sigint)
    try:
        return child.wait()
    finally:
        signal.signal(signal.SIGINT, original_sigint)


@dataclass(frozen=True)
class _AttachHandoffTarget:
    task_id: str
    resume_mode: bool
    launch_mode: Literal["worker", "iterate"]
    iterate_task_id: str | None = None
    # For iterate handoffs, the recovery (resume/retry) task whose identity is
    # prepared in the parent before detachment, so the detached iterate worker
    # inherits durable slug/log paths and can surface its startup failures.
    # Always set when launch_mode == "iterate".
    recovery_task_id: str | None = None
    # True when this attach run freshly created the recovery task row. The
    # spawn block uses this to decide whether to roll back the row on
    # preparation failure (fresh = roll back; reused = leave intact).
    recovery_task_freshly_created: bool = False


def _resolve_handoff_target(config: Config, store, task) -> _AttachHandoffTarget | None:
    """Resolve the task/flags to relaunch after interactive attach exits."""
    if task.id is None:
        return None
    if task.status in {"pending", "in_progress"}:
        return _AttachHandoffTarget(
            task_id=str(task.id),
            resume_mode=True,
            launch_mode="worker",
        )
    if task.status != "failed":
        return None

    decision = decide_failed_task_recovery(
        store,
        task,
        max_recovery_attempts=config.max_resume_attempts,
    )
    if decision.action == "skip":
        return None

    if decision.action == "resume":
        if decision.launch_mode == "iterate":
            if decision.reuse_existing and decision.recovery_task_id is not None:
                resume_task_id = decision.recovery_task_id
                freshly_created = False
            else:
                resume_task = _create_resume_task(store, task, trigger_source="auto-recovery")
                assert resume_task.id is not None
                resume_task_id = str(resume_task.id)
                freshly_created = True
            return _AttachHandoffTarget(
                task_id=resume_task_id,
                resume_mode=True,
                launch_mode="iterate",
                iterate_task_id=str(task.id),
                recovery_task_id=resume_task_id,
                recovery_task_freshly_created=freshly_created,
            )
        if decision.reuse_existing and decision.recovery_task_id is not None:
            return _AttachHandoffTarget(
                task_id=decision.recovery_task_id,
                resume_mode=True,
                launch_mode="worker",
            )
        resume_task = _create_resume_task(store, task, trigger_source="auto-recovery")
        assert resume_task.id is not None
        return _AttachHandoffTarget(
            task_id=str(resume_task.id),
            resume_mode=True,
            launch_mode="worker",
        )
    if decision.action == "reconcile":
        return None

    if decision.launch_mode == "iterate":
        if decision.reuse_existing and decision.recovery_task_id is not None:
            retry_task_id = decision.recovery_task_id
            freshly_created = False
        else:
            retry_task = _create_retry_task(
                store,
                task,
                trigger_source="auto-recovery",
                automatic_recovery=True,
            )
            assert retry_task.id is not None
            retry_task_id = str(retry_task.id)
            freshly_created = True
        return _AttachHandoffTarget(
            task_id=retry_task_id,
            resume_mode=False,
            launch_mode="iterate",
            iterate_task_id=retry_task_id,
            recovery_task_id=retry_task_id,
            recovery_task_freshly_created=freshly_created,
        )

    if decision.reuse_existing and decision.recovery_task_id is not None:
        return _AttachHandoffTarget(
            task_id=decision.recovery_task_id,
            resume_mode=False,
            launch_mode="worker",
        )
    retry_task = _create_retry_task(
        store,
        task,
        trigger_source="auto-recovery",
        automatic_recovery=True,
    )
    assert retry_task.id is not None
    return _AttachHandoffTarget(
        task_id=str(retry_task.id),
        resume_mode=False,
        launch_mode="worker",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="gza interactive attach wrapper")
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--no-docker", action="store_true")
    parser.add_argument("--max-turns", type=int)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    config = Config.load(Path(args.project))
    load_dotenv(config.project_dir)
    store = get_store(config)
    task = store.get(args.task_id)
    if task is None:
        return 1

    if task.id is not None and task.execution_mode != "foreground_attach_resume":
        task.execution_mode = "foreground_attach_resume"
        store.set_execution_mode(task.id, task.execution_mode)

    log_file = _task_log_path(config, task)
    started = time.monotonic()
    if log_file is not None:
        write_log_entry(
            log_file,
            {
                "type": "gza",
                "execution_mode": "foreground_attach_resume",
                "subtype": "worker_lifecycle",
                "event": "attach",
                "message": f"Interactive session started (session: {args.session_id[:12]}...)",
                "session_id": args.session_id,
            },
        )

    detach_signal: int | None = None

    def _handle_signal(signum, _frame):
        nonlocal detach_signal
        detach_signal = signum
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGHUP, _handle_signal)

    exit_code = 1
    try:
        exit_code = _run_interactive_claude(
            config,
            args.session_id,
            max_turns=args.max_turns,
            task=task,
            no_docker=args.no_docker,
        )
    except SystemExit as exc:
        exit_code = int(exc.code) if isinstance(exc.code, int) else 1
    finally:
        duration = max(0.0, time.monotonic() - started)
        refreshed = store.get(args.task_id)
        if refreshed is not None:
            store.record_attach_session(refreshed, duration)

            if log_file is not None:
                is_detached = detach_signal in (signal.SIGTERM, signal.SIGHUP)
                if is_detached:
                    end_reason = "detached"
                elif exit_code == 0:
                    end_reason = "exited_ok"
                else:
                    end_reason = "exited_error"
                write_log_entry(
                    log_file,
                    {
                        "type": "gza",
                        "subtype": "worker_lifecycle",
                        "event": "detach",
                        "message": f"Interactive session ended ({end_reason})",
                        "reason": end_reason,
                        "duration_seconds": duration,
                        "exit_code": exit_code,
                    },
                )

            should_handoff = detach_signal in (signal.SIGTERM, signal.SIGHUP) or exit_code == 0
            handoff = _resolve_handoff_target(config, store, refreshed) if should_handoff else None
            if handoff is not None:
                handoff_task_id = handoff.task_id
                handoff_resume_mode = handoff.resume_mode
                if handoff.launch_mode == "worker":
                    worker_args = argparse.Namespace(
                        no_docker=args.no_docker,
                        max_turns=args.max_turns,
                        force=args.force,
                        resume=handoff_resume_mode,
                    )
                    spawn_rc = _spawn_background_worker(
                        worker_args,
                        config,
                        task_id=handoff_task_id,
                        quiet=True,
                    )
                else:
                    assert handoff.iterate_task_id is not None
                    assert handoff.recovery_task_id is not None
                    iterate_task = store.get(handoff.iterate_task_id)
                    recovery_task = store.get(handoff.recovery_task_id)
                    if iterate_task is None or recovery_task is None:
                        spawn_rc = 1
                    else:
                        # Phase 1: run the parent-side preparation on the
                        # recovery row so slug/log identity is durable before
                        # detachment. Roll back the row only if this attach run
                        # created it; reused pending rows must survive.
                        prepared_recovery = _prepare_task_for_immediate_execution(
                            config,
                            recovery_task,
                            rollback_on_failure=handoff.recovery_task_freshly_created,
                        )
                        if prepared_recovery is None or prepared_recovery.id is None:
                            spawn_rc = 1
                        else:
                            iterate_args = argparse.Namespace(
                                no_docker=args.no_docker,
                                force=args.force,
                                max_iterations=config.iterate_max_iterations,
                                resume=False,
                                retry=False,
                                auto_iterate=True,
                            )
                            spawn_rc = _spawn_background_iterate(
                                iterate_args,
                                config,
                                prepared_recovery,
                                prepared_task_id=str(prepared_recovery.id),
                                prepared_resume=handoff_resume_mode,
                                prepared_phase="preloop",
                            )
                if log_file is not None:
                    handoff_event = "resume" if handoff_resume_mode else "retry"
                    handoff_failed_event = "resume_failed" if handoff_resume_mode else "retry_failed"
                    handoff_action_label = "resume" if handoff_resume_mode else "retry"
                    if spawn_rc == 0:
                        write_log_entry(
                            log_file,
                            {
                                "type": "gza",
                                "subtype": "worker_lifecycle",
                                "event": handoff_event,
                                "message": (
                                    f"Background worker {handoff_action_label} launched in "
                                    f"{handoff.launch_mode} mode"
                                ),
                                "handoff_task_id": handoff_task_id,
                            },
                        )
                    else:
                        write_log_entry(
                            log_file,
                            {
                                "type": "gza",
                                "subtype": "worker_lifecycle",
                                "event": handoff_failed_event,
                                "message": (
                                    f"Background worker {handoff_action_label} failed "
                                    "after interactive session"
                                ),
                                "handoff_exit_code": spawn_rc,
                                "handoff_task_id": handoff_task_id,
                            },
                        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
