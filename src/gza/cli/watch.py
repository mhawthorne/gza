"""Continuous watch loop and queue management commands."""

import argparse
import contextlib
import io
import os
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TypeVar

from ..config import Config
from ..console import truncate
from ..db import SqliteTaskStore, Task as DbTask
from ..failure_policy import is_resumable_failure_reason
from ..git import Git
from ..pickup import get_runnable_pending_tasks, is_worker_consuming_advance_action
from ..workers import WorkerRegistry
from ._common import (
    _create_improve_task,
    _create_rebase_task,
    _create_resume_task,
    _spawn_background_resume_worker,
    _spawn_background_worker,
    get_review_verdict,
    get_store,
    resolve_id,
    resolve_improve_action,
    set_task_urgency,
)
from .execution import _spawn_background_iterate
from .git_ops import (
    _build_auto_merge_args,
    _collect_advance_completed_tasks,
    _determine_advance_action,
    _merge_single_task,
    _prepare_create_review_action,
    _require_default_branch,
    _unimplemented_implement_prompt,
)

_WATCH_ADVANCE_ACTION_ORDER: dict[str, int] = {"merge": 0}
T = TypeVar("T")


def _watch_skip_message(task: DbTask, action: dict) -> str:
    """Build a stable skip message for non-executed advance actions."""
    action_type = str(action.get("type", "skip"))
    description = str(action.get("description", "")).strip()
    if description.startswith("SKIP: "):
        description = description[len("SKIP: ") :]
    if not description:
        description = action_type.replace("_", " ")
    return f"{task.id}: {description}"


def _short_prompt(prompt: str) -> str:
    return truncate(prompt.replace("\n", " "), 56)


def _format_hms() -> str:
    return datetime.now(UTC).strftime("%H:%M:%S")


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _format_elapsed(started_at: str | None, completed_at: str | None) -> str | None:
    start_dt = _parse_dt(started_at)
    end_dt = _parse_dt(completed_at)
    if start_dt is None or end_dt is None:
        return None
    elapsed = max(0.0, (end_dt - start_dt).total_seconds())
    mins = int(elapsed // 60)
    secs = int(elapsed % 60)
    if mins > 0:
        return f"{mins}m{secs:02d}s"
    return f"{secs}s"


def _sleep_interruptibly(seconds: int, stop_requested: Callable[[], bool], *, quantum: float = 1.0) -> None:
    """Sleep for up to `seconds`, exiting early if stop was requested."""
    remaining = float(seconds)
    while remaining > 0:
        if stop_requested():
            return
        step = min(quantum, remaining)
        time.sleep(step)
        remaining -= step


def _pid_alive(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _task_snapshot(store: SqliteTaskStore) -> dict[str, dict[str, str | None]]:
    snap: dict[str, dict[str, str | None]] = {}
    with store._connect() as conn:  # noqa: SLF001 - CLI internal polling helper
        cur = conn.execute(
            """
            SELECT id, status, task_type, started_at, completed_at, failure_reason, depends_on
            FROM tasks
            """
        )
        for row in cur.fetchall():
            task_id = str(row["id"])
            snap[task_id] = {
                "status": row["status"],
                "task_type": row["task_type"],
                "started_at": row["started_at"],
                "completed_at": row["completed_at"],
                "failure_reason": row["failure_reason"],
                "depends_on": row["depends_on"],
            }
    return snap


class _WatchLog:
    def __init__(self, path: Path, *, quiet: bool = False) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.quiet = quiet
        self._skip_keys_prev_cycle: set[str] = set()
        self._skip_keys_this_cycle: set[str] = set()

    def begin_cycle(self) -> None:
        self._skip_keys_this_cycle.clear()

    def end_cycle(self) -> None:
        self._skip_keys_prev_cycle = set(self._skip_keys_this_cycle)

    def emit(self, event: str, message: str, *, dedupe_key: str | None = None) -> None:
        if event == "SKIP" and dedupe_key is not None:
            self._skip_keys_this_cycle.add(dedupe_key)
            if dedupe_key in self._skip_keys_prev_cycle:
                return
        line = f"{_format_hms()} {event:<6} {message}".rstrip()
        with open(self.path, "a") as f:
            f.write(line + "\n")
        if not self.quiet:
            print(line)


def _emit_transition_events(
    old: dict[str, dict[str, str | None]],
    new: dict[str, dict[str, str | None]],
    *,
    store: SqliteTaskStore,
    config: Config,
    log: _WatchLog,
) -> None:
    for task_id in sorted(new.keys()):
        old_status = (old.get(task_id) or {}).get("status")
        new_row = new[task_id]
        new_status = new_row.get("status")
        if old_status == new_status:
            continue

        task_type = new_row.get("task_type") or "implement"
        elapsed = _format_elapsed(new_row.get("started_at"), new_row.get("completed_at"))
        elapsed_suffix = f" ({elapsed})" if elapsed else ""
        if new_status == "completed":
            if task_type == "review":
                task = store.get(task_id)
                impl_id = new_row.get("depends_on") or "unknown"
                verdict = get_review_verdict(config, task) if task is not None else None
                log.emit("REVIEW", f"{task_id} for {impl_id}: {verdict or 'UNKNOWN'}")
            else:
                log.emit("DONE", f"{task_id} {task_type}{elapsed_suffix}")
        elif new_status == "failed":
            reason = new_row.get("failure_reason") or "UNKNOWN"
            log.emit("FAIL", f"{task_id} {task_type}: {reason}{elapsed_suffix}")


def _count_live_workers(config: Config, store: SqliteTaskStore) -> int:
    registry = WorkerRegistry(config.workers_path)
    live_pids: set[int] = set()

    for worker in registry.list_all(include_completed=False):
        if worker.status != "running":
            continue
        if not registry.is_running(worker.worker_id):
            continue
        if worker.pid > 0:
            live_pids.add(worker.pid)

    for task in store.get_in_progress():
        pid = task.running_pid
        if not _pid_alive(pid):
            continue
        assert pid is not None
        live_pids.add(pid)

    return len(live_pids)


def _pending_runnable_tasks(store: SqliteTaskStore) -> list[DbTask]:
    return get_runnable_pending_tasks(store)


def _run_with_optional_stdout_suppressed(quiet: bool, fn: Callable[[], T]) -> T:
    if not quiet:
        return fn()
    with contextlib.redirect_stdout(io.StringIO()):
        return fn()


def _spawn_worker_with_failure_log(
    *,
    quiet: bool,
    log: _WatchLog,
    failure_message: str,
    spawn_fn: Callable[[], int],
    dedupe_key: str,
) -> int:
    rc = _run_with_optional_stdout_suppressed(quiet, spawn_fn)
    if rc != 0:
        log.emit("START_FAILED", failure_message, dedupe_key=dedupe_key)
    return rc


@dataclass
class _CycleResult:
    work_done: bool
    running: int
    pending: int


def _run_cycle(
    *,
    config: Config,
    store: SqliteTaskStore,
    batch: int,
    max_iterations: int,
    dry_run: bool,
    log: _WatchLog,
    quiet: bool = False,
) -> _CycleResult:
    from ._common import prune_terminal_dead_workers, reconcile_in_progress_tasks

    log.begin_cycle()
    if not dry_run:
        reconcile_in_progress_tasks(config)
        prune_terminal_dead_workers(config)

    running = _count_live_workers(config, store)
    slots = max(0, batch - running)
    work_done = False
    started_task_ids: set[str] = set()
    step1_handled_child_task_ids: set[str] = set()

    log.emit("WAKE", f"checking... ({running} running, {slots} slots)")

    # 1) Execute advance actions for completed tasks (includes completed plans
    # with no implement child, aligned with gza advance).
    # Merges run first; worker-spawning actions consume available slots.
    merge_candidates, impl_based_on_ids = _collect_advance_completed_tasks(store)
    if merge_candidates:
        git = Git(config.project_dir)
        current_branch = git.current_branch()
        target_branch = git.default_branch()
        action_plan: list[tuple[DbTask, dict]] = []
        for task in merge_candidates:
            action_plan.append(
                (
                    task,
                    _determine_advance_action(
                        config,
                        store,
                        git,
                        task,
                        target_branch,
                        impl_based_on_ids=impl_based_on_ids,
                    ),
                )
            )
        action_plan.sort(key=lambda item: _WATCH_ADVANCE_ACTION_ORDER.get(item[1].get("type", ""), 1))
        has_merge_action = any(action.get("type") == "merge" for _, action in action_plan)
        can_merge = True
        if has_merge_action:
            can_merge = _run_with_optional_stdout_suppressed(
                quiet,
                lambda: _require_default_branch(git, current_branch, "merge"),
            )

        for task, action in action_plan:
            action_type = action.get("type")
            if action_type in {
                "skip",
                "wait_review",
                "wait_improve",
                "needs_discussion",
                "max_cycles_reached",
                "max_improve_attempts",
            }:
                log.emit(
                    "SKIP",
                    _watch_skip_message(task, action),
                    dedupe_key=f"advance-skip:{action_type}:{task.id}",
                )
                continue

            if action_type == "merge":
                if not can_merge:
                    log.emit(
                        "SKIP",
                        "merge actions skipped: not on default branch",
                        dedupe_key="merge-not-default-branch",
                    )
                    continue
                if dry_run:
                    log.emit("MERGE", f"{task.id} -> {target_branch} [dry-run]")
                    work_done = True
                    continue
                merge_args = _build_auto_merge_args(config, git, task, target_branch)
                rc = _run_with_optional_stdout_suppressed(
                    quiet,
                    lambda: _merge_single_task(str(task.id), config, store, git, merge_args, current_branch),
                )
                if rc == 0:
                    log.emit("MERGE", f"{task.id} -> {target_branch}")
                    work_done = True
                else:
                    log.emit(
                        "SKIP",
                        f"{task.id}: merge failed",
                        dedupe_key=f"merge-failed:{task.id}",
                    )
                continue

            if not is_worker_consuming_advance_action(str(action_type)) or action_type == "resume":
                continue
            if slots <= 0:
                continue

            if action_type == "create_review":
                if task.id is None:
                    continue
                if dry_run:
                    log.emit("START", f"(new) review for {task.id} [dry-run]")
                    slots -= 1
                    work_done = True
                    continue
                create_result = _prepare_create_review_action(store, task)
                if create_result.status == "skip":
                    log.emit(
                        "SKIP",
                        create_result.message,
                        dedupe_key=f"create-review-skip:{task.id}:{create_result.message}",
                    )
                    continue
                review_task = create_result.review_task
                assert review_task is not None
                step1_handled_child_task_ids.add(str(review_task.id))

                worker_args = argparse.Namespace(no_docker=False, max_turns=None, resume=False)
                rc = _spawn_worker_with_failure_log(
                    quiet=quiet,
                    log=log,
                    failure_message=f"{review_task.id} review: worker spawn failed",
                    dedupe_key=f"spawn-worker-failed:{review_task.id}",
                    spawn_fn=lambda: _spawn_background_worker(
                        worker_args, config, task_id=review_task.id, quiet=quiet
                    ),
                )
                if rc == 0:
                    log.emit("START", f"{review_task.id} review")
                    started_task_ids.add(str(review_task.id))
                    slots -= 1
                    work_done = True
                continue

            if action_type == "run_review":
                review_task_obj = action.get("review_task")
                if not isinstance(review_task_obj, DbTask) or review_task_obj.id is None:
                    continue
                review_task_id = str(review_task_obj.id)

                if dry_run:
                    log.emit("START", f"{review_task_id} review [dry-run]")
                    started_task_ids.add(review_task_id)
                    slots -= 1
                    work_done = True
                    continue

                worker_args = argparse.Namespace(no_docker=False, max_turns=None, resume=False)
                rc = _spawn_worker_with_failure_log(
                    quiet=quiet,
                    log=log,
                    failure_message=f"{review_task_id} review: worker spawn failed",
                    dedupe_key=f"spawn-worker-failed:{review_task_id}",
                    spawn_fn=lambda: _spawn_background_worker(
                        worker_args, config, task_id=review_task_id, quiet=quiet
                    ),
                )
                if rc == 0:
                    log.emit("START", f"{review_task_id} review")
                    started_task_ids.add(review_task_id)
                    slots -= 1
                    work_done = True
                else:
                    log.emit("START_FAILED", f"{review_task_id} review")
                    step1_handled_child_task_ids.add(review_task_id)
                continue

            if action_type == "improve":
                review_task_obj = action.get("review_task")
                if not isinstance(review_task_obj, DbTask) or review_task_obj.id is None or task.id is None:
                    continue

                improve_action, failed_improve = resolve_improve_action(
                    store,
                    task.id,
                    review_task_obj.id,
                    max_resume_attempts=config.max_resume_attempts,
                )
                if improve_action == "give_up" and failed_improve is not None:
                    assert failed_improve.id is not None
                    msg = (
                        f"SKIP: max improve attempts ({config.max_resume_attempts}) reached for "
                        f"{task.id} + {review_task_obj.id}; latest failed improve: {failed_improve.id}. "
                        f"Run uv run gza fix {task.id}"
                    )
                    log.emit(
                        "SKIP",
                        f"{task.id}: {msg}",
                        dedupe_key=f"max-improve-attempts:{task.id}:{review_task_obj.id}",
                    )
                    continue

                if dry_run:
                    if improve_action == "resume" and failed_improve is not None and failed_improve.id is not None:
                        log.emit("START", f"(resume) improve for {failed_improve.id} [dry-run]")
                    elif improve_action == "retry" and failed_improve is not None and failed_improve.id is not None:
                        log.emit("START", f"(retry) improve for {failed_improve.id} [dry-run]")
                    else:
                        log.emit("START", f"(new) improve for {task.id} [dry-run]")
                    slots -= 1
                    work_done = True
                    continue

                if improve_action == "resume" and failed_improve is not None:
                    assert failed_improve.id is not None
                    improve_task = _create_resume_task(store, failed_improve)
                elif improve_action == "retry" and failed_improve is not None:
                    assert failed_improve.id is not None
                    retry_same_branch = failed_improve.same_branch
                    retry_base_branch: str | None = None
                    if failed_improve.same_branch and failed_improve.branch:
                        retry_same_branch = False
                        retry_base_branch = failed_improve.branch
                    improve_task = store.add(
                        prompt=failed_improve.prompt,
                        task_type="improve",
                        depends_on=failed_improve.depends_on,
                        based_on=failed_improve.id,
                        same_branch=retry_same_branch,
                        group=failed_improve.group,
                        base_branch=retry_base_branch,
                    )
                else:
                    try:
                        improve_task = _create_improve_task(store, task, review_task_obj)
                    except ValueError as exc:
                        log.emit(
                            "ERROR",
                            f"{task.id}: unable to create improve task: {exc}",
                            dedupe_key=f"improve-create-error:{task.id}:{review_task_obj.id}",
                        )
                        continue
                assert improve_task.id is not None
                improve_task_id = improve_task.id
                step1_handled_child_task_ids.add(improve_task_id)

                worker_args = argparse.Namespace(no_docker=False, max_turns=None, resume=False)
                is_resume = improve_task.session_id is not None
                if is_resume:
                    rc = _spawn_worker_with_failure_log(
                        quiet=quiet,
                        log=log,
                        failure_message=f"{improve_task_id} improve: resume worker spawn failed",
                        dedupe_key=f"spawn-resume-failed:{improve_task_id}",
                        spawn_fn=lambda: _spawn_background_resume_worker(
                            worker_args, config, new_task_id=improve_task_id, quiet=quiet
                        ),
                    )
                else:
                    rc = _spawn_worker_with_failure_log(
                        quiet=quiet,
                        log=log,
                        failure_message=f"{improve_task_id} improve: worker spawn failed",
                        dedupe_key=f"spawn-worker-failed:{improve_task_id}",
                        spawn_fn=lambda: _spawn_background_worker(
                            worker_args, config, task_id=improve_task_id, quiet=quiet
                        ),
                    )
                if rc == 0:
                    log.emit("START", f"{improve_task_id} improve")
                    started_task_ids.add(improve_task_id)
                    slots -= 1
                    work_done = True
                continue

            if action_type == "run_improve":
                improve_task_obj = action.get("improve_task")
                if not isinstance(improve_task_obj, DbTask) or improve_task_obj.id is None:
                    continue

                if dry_run:
                    log.emit("START", f"{improve_task_obj.id} improve [dry-run]")
                    started_task_ids.add(str(improve_task_obj.id))
                    slots -= 1
                    work_done = True
                    continue

                worker_args = argparse.Namespace(no_docker=False, max_turns=None, resume=False)
                rc = _spawn_worker_with_failure_log(
                    quiet=quiet,
                    log=log,
                    failure_message=f"{improve_task_obj.id} improve: worker spawn failed",
                    dedupe_key=f"spawn-worker-failed:{improve_task_obj.id}",
                    spawn_fn=lambda: _spawn_background_worker(
                        worker_args, config, task_id=improve_task_obj.id, quiet=quiet
                    ),
                )
                if rc == 0:
                    log.emit("START", f"{improve_task_obj.id} improve")
                    started_task_ids.add(str(improve_task_obj.id))
                    slots -= 1
                    work_done = True
                else:
                    log.emit("START_FAILED", f"{improve_task_obj.id} improve")
                    step1_handled_child_task_ids.add(str(improve_task_obj.id))
                continue

            if action_type == "create_implement":
                if task.id is None:
                    continue
                if dry_run:
                    log.emit("START", f"(new) implement for {task.id} [dry-run]")
                    slots -= 1
                    work_done = True
                    continue
                prompt_text = _unimplemented_implement_prompt(task)
                impl_task = store.add(
                    prompt=prompt_text,
                    task_type="implement",
                    based_on=task.id,
                    group=task.group,
                )
                step1_handled_child_task_ids.add(str(impl_task.id))

                iterate_args = argparse.Namespace(
                    max_iterations=max_iterations,
                    no_docker=False,
                    resume=False,
                    retry=False,
                )
                rc = _spawn_worker_with_failure_log(
                    quiet=quiet,
                    log=log,
                    failure_message=f"{impl_task.id} implement: iterate worker spawn failed",
                    dedupe_key=f"spawn-iterate-failed:{impl_task.id}",
                    spawn_fn=lambda: _spawn_background_iterate(iterate_args, config, impl_task),
                )
                if rc == 0:
                    log.emit("START", f"{impl_task.id} implement")
                    started_task_ids.add(str(impl_task.id))
                    slots -= 1
                    work_done = True
                continue

            if action_type == "needs_rebase":
                if task.id is None or not task.branch:
                    continue
                if dry_run:
                    log.emit("START", f"(new) rebase for {task.id} [dry-run]")
                    slots -= 1
                    work_done = True
                    continue
                rebase_task = _create_rebase_task(store, task.id, task.branch, target_branch)
                step1_handled_child_task_ids.add(str(rebase_task.id))

                worker_args = argparse.Namespace(no_docker=False, max_turns=None, resume=False)
                rc = _spawn_worker_with_failure_log(
                    quiet=quiet,
                    log=log,
                    failure_message=f"{rebase_task.id} rebase: worker spawn failed",
                    dedupe_key=f"spawn-worker-failed:{rebase_task.id}",
                    spawn_fn=lambda: _spawn_background_worker(
                        worker_args, config, task_id=rebase_task.id, quiet=quiet
                    ),
                )
                if rc == 0:
                    log.emit("START", f"{rebase_task.id} rebase")
                    started_task_ids.add(str(rebase_task.id))
                    slots -= 1
                    work_done = True

    # 2) Resume failed resumable tasks (consumes slots)
    pending_resume_task_ids: set[str] = set()
    if slots > 0:
        failed_tasks = store.get_resumable_failed_tasks()
        for failed in failed_tasks:
            if slots <= 0:
                break
            if not is_resumable_failure_reason(failed.failure_reason) or not failed.session_id:
                continue
            assert failed.id is not None

            children = store.get_based_on_children(failed.id)
            resume_task: DbTask | None = None
            for child in children:
                if (
                    child.status == "pending"
                    and child.task_type == failed.task_type
                    and child.session_id == failed.session_id
                    and child.id is not None
                ):
                    resume_task = child
                    break
            if resume_task is None and children:
                continue

            depth = store.count_resume_chain_depth(failed.id)
            attempt = depth + 1
            if depth >= config.max_resume_attempts:
                log.emit(
                    "SKIP",
                    f"{failed.id}: max_resume_attempts reached",
                    dedupe_key=f"max-resume:{failed.id}",
                )
                continue
            if dry_run:
                resume_target = str(resume_task.id) if resume_task is not None and resume_task.id is not None else "(new task)"
                log.emit(
                    "RESUME",
                    f"{failed.id} -> {resume_target} (attempt {attempt}/{config.max_resume_attempts}) [dry-run]",
                )
                slots -= 1
                work_done = True
                continue
            if resume_task is None:
                resume_task = _create_resume_task(store, failed)
            assert resume_task.id is not None
            resume_task_id = str(resume_task.id)
            pending_resume_task_ids.add(resume_task_id)
            worker_args = argparse.Namespace(no_docker=False, max_turns=None)
            rc = _spawn_worker_with_failure_log(
                quiet=quiet,
                log=log,
                failure_message=f"{failed.id} -> {resume_task_id}: resume worker spawn failed",
                dedupe_key=f"spawn-resume-failed:{failed.id}:{resume_task_id}",
                spawn_fn=lambda: _spawn_background_resume_worker(worker_args, config, resume_task_id, quiet=quiet),
            )
            if rc != 0:
                continue
            slots -= 1
            work_done = True
            started_task_ids.add(resume_task_id)
            log.emit(
                "RESUME",
                f"{failed.id} -> {resume_task_id} (attempt {attempt}/{config.max_resume_attempts})",
            )

    # 3) Start new queued tasks (consumes slots)
    pending_tasks = _pending_runnable_tasks(store)
    if slots > 0:
        for task in pending_tasks:
            if slots <= 0:
                break
            assert task.id is not None
            if str(task.id) in started_task_ids:
                continue
            if str(task.id) in pending_resume_task_ids:
                continue
            if str(task.id) in step1_handled_child_task_ids:
                continue
            task_type = task.task_type or "implement"
            if task_type == "implement":
                if dry_run:
                    log.emit("START", f"{task.id} {task_type} \"{_short_prompt(task.prompt)}\" [dry-run]")
                    started_task_ids.add(str(task.id))
                    slots -= 1
                    work_done = True
                    continue
                iterate_args = argparse.Namespace(
                    max_iterations=max_iterations,
                    no_docker=False,
                    resume=False,
                    retry=False,
                )
                rc = _spawn_worker_with_failure_log(
                    quiet=quiet,
                    log=log,
                    failure_message=f"{task.id} {task_type}: iterate worker spawn failed",
                    dedupe_key=f"spawn-iterate-failed:{task.id}",
                    spawn_fn=lambda: _spawn_background_iterate(iterate_args, config, task),
                )
                if rc != 0:
                    continue
                slots -= 1
                work_done = True
                started_task_ids.add(str(task.id))
                log.emit("START", f"{task.id} {task_type} \"{_short_prompt(task.prompt)}\"")
                continue

            if dry_run:
                log.emit("START", f"{task.id} {task_type} \"{_short_prompt(task.prompt)}\" [dry-run]")
                started_task_ids.add(str(task.id))
                slots -= 1
                work_done = True
                continue
            worker_args = argparse.Namespace(no_docker=False, max_turns=None, resume=False)
            rc = _spawn_worker_with_failure_log(
                quiet=quiet,
                log=log,
                failure_message=f"{task.id} {task_type}: worker spawn failed",
                dedupe_key=f"spawn-worker-failed:{task.id}",
                spawn_fn=lambda: _spawn_background_worker(worker_args, config, task_id=task.id, quiet=quiet),
            )
            if rc != 0:
                continue
            slots -= 1
            work_done = True
            started_task_ids.add(str(task.id))
            log.emit("START", f"{task.id} {task_type} \"{_short_prompt(task.prompt)}\"")

    pending_count = len(_pending_runnable_tasks(store))
    log.end_cycle()
    return _CycleResult(
        work_done=work_done,
        running=_count_live_workers(config, store),
        pending=pending_count,
    )


def cmd_watch(args: argparse.Namespace) -> int:
    """Run continuous scheduler loop that maintains N concurrent workers."""
    config = Config.load(args.project_dir)
    store = get_store(config)

    batch = args.batch if args.batch is not None else config.watch.batch
    poll = args.poll if args.poll is not None else config.watch.poll
    max_idle = args.max_idle if args.max_idle is not None else config.watch.max_idle
    max_iterations = (
        args.max_iterations if args.max_iterations is not None else config.watch.max_iterations
    )
    dry_run = bool(getattr(args, "dry_run", False))
    quiet = bool(getattr(args, "quiet", False))

    if batch < 1:
        print("Error: --batch must be a positive integer")
        return 1
    if poll < 1:
        print("Error: --poll must be a positive integer")
        return 1
    if max_idle is not None and max_idle < 1:
        print("Error: --max-idle must be a positive integer")
        return 1
    if max_iterations < 1:
        print("Error: --max-iterations must be a positive integer")
        return 1

    log = _WatchLog(config.project_dir / ".gza" / "watch.log", quiet=quiet)
    stop_requested = False

    def _handle_shutdown(_signum: int, _frame: object) -> None:
        nonlocal stop_requested
        stop_requested = True
        log.emit("INFO", "shutting down (workers left running)")

    old_sigint = signal.signal(signal.SIGINT, _handle_shutdown)
    old_sigterm = signal.signal(signal.SIGTERM, _handle_shutdown)

    idle_seconds = 0
    previous_snapshot = _task_snapshot(store)

    # Preview first cycle and ask for confirmation before executing
    skip_confirm = dry_run or bool(getattr(args, "yes", False))
    if not skip_confirm:
        preview_result = _run_cycle(
            config=config,
            store=store,
            batch=batch,
            max_iterations=max_iterations,
            dry_run=True,
            quiet=False,
            log=log,
        )
        if preview_result.work_done:
            try:
                answer = input("\nProceed? [y/N] ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                answer = ""
            if answer not in ("y", "yes"):
                print("Aborted.")
                return 0

    try:
        while True:
            if stop_requested:
                break

            cycle_result = _run_cycle(
                config=config,
                store=store,
                batch=batch,
                max_iterations=max_iterations,
                dry_run=dry_run,
                quiet=quiet,
                log=log,
            )

            current_snapshot = _task_snapshot(store)
            _emit_transition_events(
                previous_snapshot,
                current_snapshot,
                store=store,
                config=config,
                log=log,
            )
            previous_snapshot = current_snapshot

            if cycle_result.work_done:
                idle_seconds = 0
            else:
                idle_seconds += poll
                log.emit(
                    "IDLE",
                    f"sleeping {poll}s ({cycle_result.pending} pending, {cycle_result.running} running)",
                )
                if max_idle is not None and idle_seconds >= max_idle:
                    log.emit("INFO", f"max idle time reached ({max_idle}s), exiting")
                    break

            if stop_requested:
                break
            _sleep_interruptibly(poll, lambda: stop_requested)
    finally:
        signal.signal(signal.SIGINT, old_sigint)
        signal.signal(signal.SIGTERM, old_sigterm)

    return 0


def cmd_queue(args: argparse.Namespace) -> int:
    """Inspect and adjust pending queue urgency."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    action = getattr(args, "queue_action", None)

    if action in {"bump", "unbump"}:
        task_id = resolve_id(config, args.task_id)
        task = store.get(task_id)
        if task is None:
            print(f"Error: Task {task_id} not found")
            return 1
        if task.status != "pending":
            print(f"Error: Task {task_id} is not pending (status: {task.status})")
            return 1
        if task.task_type == "internal":
            print(f"Error: Task {task_id} is internal and not part of the runnable queue")
            return 1

        runnable_pending_ids = {str(pending_task.id) for pending_task in store.get_pending_pickup() if pending_task.id is not None}
        is_currently_runnable = str(task_id) in runnable_pending_ids

        new_urgent = action == "bump"
        set_task_urgency(store, task_id, urgent=new_urgent)
        if new_urgent:
            if is_currently_runnable:
                print(f"✓ Bumped task {task_id} to urgent queue")
            else:
                print(f"✓ Bumped task {task_id} (not currently runnable; urgency will apply once runnable)")
        else:
            if is_currently_runnable:
                print(f"✓ Removed task {task_id} from urgent queue")
            else:
                print(f"✓ Removed urgent flag from task {task_id} (task is not currently runnable)")
        return 0

    pending = store.get_pending_pickup()
    if not pending:
        print("No runnable tasks")
        return 0

    for index, task in enumerate(pending, start=1):
        lane = "urgent" if task.urgent else "normal"
        print(f"{index:>3}  {task.id}  [{lane}] [{task.task_type}] {_short_prompt(task.prompt)}")

    return 0
