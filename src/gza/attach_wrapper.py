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
from .providers.base import build_docker_cmd, ensure_docker_image
from .providers.claude import _get_docker_config, sync_keychain_credentials
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
    docker_config = _get_docker_config(f"{config.docker_image}-claude")
    work_dir = _resolve_work_dir(config, task)
    if not ensure_docker_image(docker_config, config.project_dir):
        raise RuntimeError("failed to build Docker image for interactive attach")
    cmd = build_docker_cmd(
        docker_config,
        work_dir,
        config.timeout_minutes,
        config.docker_volumes,
        config.docker_setup_command,
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


def _should_auto_resume(task) -> bool:
    return task.status in {"pending", "in_progress", "failed"}


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

            should_resume = (
                _should_auto_resume(refreshed)
                and (detach_signal in (signal.SIGTERM, signal.SIGHUP) or exit_code == 0)
            )
            if should_resume:
                worker_args = argparse.Namespace(
                    no_docker=args.no_docker,
                    max_turns=args.max_turns,
                    force=args.force,
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
