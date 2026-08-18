"""Shared rebase source-branch identity rules."""

from __future__ import annotations

from typing import Protocol

AUTHORITATIVE_REBASE_RECOVERY_ORIGINS = frozenset({"retry", "resume"})


class RebaseIdentityTask(Protocol):
    task_type: str
    status: str
    branch: str | None
    based_on: str | None
    recovery_origin: str | None
    trigger_source: str | None


def rebase_persisted_branch_is_authoritative(
    task: RebaseIdentityTask,
    *,
    parent_task_type: str | None,
) -> bool:
    """Return whether a rebase row's stored branch is its singleton/runtime key."""

    if task.task_type != "rebase" or not task.branch:
        return False
    if task.based_on is None:
        return True
    active = task.status in {"pending", "in_progress"}
    if parent_task_type != "rebase":
        return active or task.trigger_source is not None
    return active and task.recovery_origin in AUTHORITATIVE_REBASE_RECOVERY_ORIGINS
