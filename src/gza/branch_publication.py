"""Durable state for branch-publication recovery and continuation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from .artifacts import store_command_output_artifact
from .config import Config
from .db import SqliteTaskStore, Task

BRANCH_PUBLICATION_STATE_ARTIFACT_KIND: Final[str] = "branch_publication_state"
_ARTIFACT_LABEL: Final[str] = "branch publication state"
_ARTIFACT_PRODUCER: Final[str] = "branch_publication"
_SCHEMA_VERSION: Final[int] = 1


@dataclass(frozen=True)
class BranchPublicationState:
    """Durable metadata needed to continue branch publication after reconcile."""

    reconcile_attempts_consumed: int = 0
    fix_commits_ahead_before_run: int | None = None
    fix_default_branch: str | None = None
    fix_was_merged_before_run: bool = False


def load_branch_publication_state(
    store: SqliteTaskStore,
    task_id: str | None,
) -> BranchPublicationState:
    """Return the latest persisted branch-publication state for a task."""
    if not task_id:
        return BranchPublicationState()
    for artifact in store.list_artifacts(task_id, kind=BRANCH_PUBLICATION_STATE_ARTIFACT_KIND):
        metadata = artifact.metadata or {}
        if metadata.get("schema_version") != _SCHEMA_VERSION:
            continue
        return BranchPublicationState(
            reconcile_attempts_consumed=_coerce_nonnegative_int(metadata.get("reconcile_attempts_consumed")),
            fix_commits_ahead_before_run=_coerce_optional_int(metadata.get("fix_commits_ahead_before_run")),
            fix_default_branch=_coerce_optional_str(metadata.get("fix_default_branch")),
            fix_was_merged_before_run=bool(metadata.get("fix_was_merged_before_run", False)),
        )
    return BranchPublicationState()


def persist_branch_publication_state(
    *,
    store: SqliteTaskStore,
    task: Task,
    config: Config,
    state: BranchPublicationState,
    status: str,
    exit_status: str,
    head_sha: str | None = None,
) -> None:
    """Persist branch-publication continuation state as a metadata-only artifact."""
    if task.id is None:
        return
    metadata = {
        "schema_version": _SCHEMA_VERSION,
        "reconcile_attempts_consumed": state.reconcile_attempts_consumed,
        "fix_commits_ahead_before_run": state.fix_commits_ahead_before_run,
        "fix_default_branch": state.fix_default_branch,
        "fix_was_merged_before_run": state.fix_was_merged_before_run,
    }
    store_command_output_artifact(
        store,
        task,
        config,
        kind=BRANCH_PUBLICATION_STATE_ARTIFACT_KIND,
        producer=_ARTIFACT_PRODUCER,
        label=_ARTIFACT_LABEL,
        output="",
        status=status,
        exit_status=exit_status,
        head_sha=head_sha if isinstance(head_sha, str) and head_sha else None,
        metadata=metadata,
    )


def _coerce_nonnegative_int(value: object) -> int:
    if value is None:
        return 0
    try:
        parsed = int(str(value))
    except (TypeError, ValueError):
        return 0
    return max(parsed, 0)


def _coerce_optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _coerce_optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
