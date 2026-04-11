"""Interactive attach wrapper for kill/resume tmux sessions."""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import time
from pathlib import Path

from .cli._common import _spawn_background_worker, get_store
from .config import Config
from .runner import write_log_entry


def _task_log_path(config: Config, task) -> Path | None:
    if not task.log_file:
        return None
    return config.project_dir / task.log_file


def _run_interactive_claude(config: Config, session_id: str, *, max_turns: int | None = None) -> int:
    cmd = ["claude", "--resume", session_id]
    if config.model:
        cmd.extend(["--model", config.model])
    cmd.extend(config.claude.args)
    effective_max_turns = max_turns if max_turns is not None else config.max_steps
    cmd.extend(["--max-turns", str(effective_max_turns)])

    child = subprocess.Popen(cmd, cwd=config.project_dir)
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


def _should_auto_resume(task) -> bool:
    return task.status in {"pending", "in_progress", "failed"}


def main() -> int:
    parser = argparse.ArgumentParser(description="gza interactive attach wrapper")
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--no-docker", action="store_true")
    parser.add_argument("--max-turns", type=int)
    args = parser.parse_args()

    config = Config.load(Path(args.project))
    store = get_store(config)
    task = store.get(args.task_id)
    if task is None:
        return 1

    log_file = _task_log_path(config, task)
    started = time.monotonic()
    if log_file is not None:
        write_log_entry(
            log_file,
            {
                "type": "gza",
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
        exit_code = _run_interactive_claude(config, args.session_id, max_turns=args.max_turns)
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

            should_resume = (
                _should_auto_resume(refreshed)
                and (detach_signal in (signal.SIGTERM, signal.SIGHUP) or exit_code == 0)
            )
            if should_resume:
                worker_args = argparse.Namespace(
                    no_docker=args.no_docker,
                    max_turns=args.max_turns,
                    resume=True,
                )
                spawn_rc = _spawn_background_worker(
                    worker_args,
                    config,
                    task_id=args.task_id,
                    quiet=True,
                )
                if log_file is not None:
                    if spawn_rc == 0:
                        write_log_entry(
                            log_file,
                            {
                                "type": "gza",
                                "subtype": "worker_lifecycle",
                                "event": "resume",
                                "message": "Background worker resumed in pipe mode",
                            },
                        )
                    else:
                        write_log_entry(
                            log_file,
                            {
                                "type": "gza",
                                "subtype": "worker_lifecycle",
                                "event": "resume_failed",
                                "message": "Background worker resume failed after interactive session",
                                "handoff_exit_code": spawn_rc,
                            },
                        )
                if spawn_rc != 0:
                    store.mark_failed(
                        refreshed,
                        log_file=refreshed.log_file,
                        branch=refreshed.branch,
                        has_commits=bool(refreshed.has_commits),
                        failure_reason="WORKER_DIED",
                    )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
