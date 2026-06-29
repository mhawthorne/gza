"""Shared helpers for persisted and rendered verify-gate provenance."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from gza.artifacts import store_command_output_artifact
from gza.db import SqliteTaskStore, Task
from gza.git import GitError

if TYPE_CHECKING:
    from gza.config import Config


VERIFY_GATE_ARTIFACT_KIND = "verify_gate_result"
VERIFY_GATE_ARTIFACT_LABEL = "verify_gate_result"
VERIFY_GATE_ARTIFACT_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class VerifyEpoch:
    """Identity for one verify gate evaluation at a specific head."""

    reviewed_branch: str | None
    reviewed_head_sha: str | None
    verify_command: str | None
    verify_timeout_seconds: int | None
    verify_timeout_grace_seconds: float | None


@dataclass(frozen=True)
class VerifyGateResult:
    """Neutral persisted verify result for merge-gating evidence."""

    command: str
    status: str
    exit_status: str
    captured_at: datetime
    reviewed_branch: str | None = None
    reviewed_head_sha: str | None = None
    reviewed_base_sha: str | None = None
    working_directory: str | None = None
    failure: str | None = None
    source_task_id: str | None = None
    source_task_type: str | None = None
    output_artifact_id: int | None = None
    output_artifact_task_id: str | None = None
    output_artifact_path: str | None = None


@dataclass(frozen=True)
class VerifyGateLookup:
    """Resolved latest verify evidence for an epoch."""

    result: VerifyGateResult | None
    source: Literal["owner_artifact", "legacy_review"] | None
    is_current: bool
    has_owner_artifact: bool


@dataclass(frozen=True)
class LatestVerifyEvidence:
    """Latest persisted verify evidence paired with its original epoch identity."""

    result: VerifyGateResult
    epoch: VerifyEpoch
    source: Literal["owner_artifact", "legacy_review"]
    has_owner_artifact: bool


@dataclass(frozen=True)
class VerifyReadModel:
    """Operator-facing verify read model shared across query and inspect paths."""

    result: VerifyGateResult
    source: Literal["owner_artifact", "legacy_review"]
    is_current: bool
    has_owner_artifact: bool
    owner_task_id: str | None
    source_task_id: str | None
    source_task_type: str | None
    legacy_markdown: str | None = None


def normalized_verify_command(command: str | None) -> str | None:
    """Return the stable verify command identity used for freshness matching."""
    if not isinstance(command, str):
        return None
    normalized = command.strip()
    return normalized or None


def make_verify_epoch(
    *,
    reviewed_branch: str | None,
    reviewed_head_sha: str | None,
    verify_command: str | None,
    verify_timeout_seconds: int | None,
    verify_timeout_grace_seconds: float | None,
) -> VerifyEpoch:
    """Build the canonical verify epoch identity for one branch head."""
    return VerifyEpoch(
        reviewed_branch=reviewed_branch,
        reviewed_head_sha=reviewed_head_sha,
        verify_command=normalized_verify_command(verify_command),
        verify_timeout_seconds=verify_timeout_seconds,
        verify_timeout_grace_seconds=verify_timeout_grace_seconds,
    )


def verify_epoch_matches(*, expected: VerifyEpoch, candidate: VerifyEpoch) -> bool:
    """Return whether two verify epoch identities are equivalent."""
    return expected == candidate


def _task_verify_result(task: Task) -> VerifyGateResult | None:
    if (
        not task.review_verify_command
        or not task.review_verify_status
        or not task.review_verify_exit_status
        or task.review_verify_captured_at is None
    ):
        return None
    return VerifyGateResult(
        command=task.review_verify_command,
        status=task.review_verify_status,
        exit_status=task.review_verify_exit_status,
        captured_at=task.review_verify_captured_at,
        reviewed_branch=task.review_verify_branch,
        reviewed_head_sha=task.review_verify_head_sha,
        reviewed_base_sha=task.review_verify_base_sha,
        working_directory=task.review_verify_cwd,
        failure=task.review_verify_failure,
        source_task_id=task.id,
        source_task_type=task.task_type,
        output_artifact_path=task.review_verify_artifact_file,
    )


def _result_epoch(
    result: VerifyGateResult,
    *,
    verify_timeout_seconds: int | None,
    verify_timeout_grace_seconds: float | None,
) -> VerifyEpoch:
    return make_verify_epoch(
        reviewed_branch=result.reviewed_branch,
        reviewed_head_sha=result.reviewed_head_sha,
        verify_command=result.command,
        verify_timeout_seconds=verify_timeout_seconds,
        verify_timeout_grace_seconds=verify_timeout_grace_seconds,
    )


def _legacy_result_epoch(result: VerifyGateResult) -> VerifyEpoch:
    """Build legacy review freshness identity without projecting current config metadata."""
    return _result_epoch(
        result,
        verify_timeout_seconds=None,
        verify_timeout_grace_seconds=None,
    )


def _coerce_optional_str(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _coerce_optional_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _coerce_optional_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _artifact_verify_result(metadata: dict[str, Any]) -> VerifyGateResult | None:
    result_payload = metadata.get("result")
    if not isinstance(result_payload, dict):
        return None
    captured_at_raw = result_payload.get("captured_at")
    if not isinstance(captured_at_raw, str):
        return None
    try:
        captured_at = datetime.fromisoformat(captured_at_raw)
    except ValueError:
        return None
    command = _coerce_optional_str(result_payload.get("command"))
    status = _coerce_optional_str(result_payload.get("status"))
    exit_status = _coerce_optional_str(result_payload.get("exit_status"))
    if command is None or status is None or exit_status is None:
        return None
    return VerifyGateResult(
        command=command,
        status=status,
        exit_status=exit_status,
        captured_at=captured_at,
        reviewed_branch=_coerce_optional_str(result_payload.get("reviewed_branch")),
        reviewed_head_sha=_coerce_optional_str(result_payload.get("reviewed_head_sha")),
        reviewed_base_sha=_coerce_optional_str(result_payload.get("reviewed_base_sha")),
        working_directory=_coerce_optional_str(result_payload.get("working_directory")),
        failure=_coerce_optional_str(result_payload.get("failure")),
        source_task_id=_coerce_optional_str(metadata.get("source_task_id")),
        source_task_type=_coerce_optional_str(metadata.get("source_task_type")),
        output_artifact_id=_coerce_optional_int(metadata.get("output_artifact_id")),
        output_artifact_task_id=_coerce_optional_str(metadata.get("output_artifact_task_id")),
        output_artifact_path=_coerce_optional_str(metadata.get("output_artifact_path")),
    )


def _artifact_verify_epoch(metadata: dict[str, Any]) -> VerifyEpoch | None:
    epoch_payload = metadata.get("verify_epoch")
    if not isinstance(epoch_payload, dict):
        return None
    return make_verify_epoch(
        reviewed_branch=_coerce_optional_str(epoch_payload.get("reviewed_branch")),
        reviewed_head_sha=_coerce_optional_str(epoch_payload.get("reviewed_head_sha")),
        verify_command=_coerce_optional_str(epoch_payload.get("verify_command")),
        verify_timeout_seconds=_coerce_optional_int(epoch_payload.get("verify_timeout_seconds")),
        verify_timeout_grace_seconds=_coerce_optional_float(epoch_payload.get("verify_timeout_grace_seconds")),
    )


def latest_verify_result_for_epoch(
    store: SqliteTaskStore,
    owner_task: Task,
    *,
    current_epoch: VerifyEpoch | None,
) -> VerifyGateLookup:
    """Return the latest verify result for ``current_epoch`` with legacy fallback."""
    if owner_task.id is None:
        return VerifyGateLookup(result=None, source=None, is_current=False, has_owner_artifact=False)

    owner_artifacts = store.list_artifacts(owner_task.id, kind=VERIFY_GATE_ARTIFACT_KIND)
    if owner_artifacts:
        latest_result: VerifyGateResult | None = None
        for artifact in owner_artifacts:
            metadata = artifact.metadata if isinstance(artifact.metadata, dict) else None
            if metadata is None or metadata.get("schema_version") != VERIFY_GATE_ARTIFACT_SCHEMA_VERSION:
                continue
            result = _artifact_verify_result(metadata)
            epoch = _artifact_verify_epoch(metadata)
            if result is None or epoch is None:
                continue
            if latest_result is None:
                latest_result = result
            if current_epoch is not None and verify_epoch_matches(expected=current_epoch, candidate=epoch):
                return VerifyGateLookup(
                    result=result,
                    source="owner_artifact",
                    is_current=True,
                    has_owner_artifact=True,
                )
        return VerifyGateLookup(
            result=latest_result,
            source="owner_artifact" if latest_result is not None else None,
            is_current=False,
            has_owner_artifact=True,
        )

    latest_legacy: VerifyGateResult | None = None
    for review in store.get_reviews_for_task(owner_task.id):
        legacy = _task_verify_result(review)
        if legacy is None:
            continue
        if latest_legacy is None:
            latest_legacy = legacy
        legacy_epoch = _legacy_result_epoch(legacy)
        if current_epoch is not None and verify_epoch_matches(expected=current_epoch, candidate=legacy_epoch):
            return VerifyGateLookup(
                result=legacy,
                source="legacy_review",
                is_current=True,
                has_owner_artifact=False,
            )
    return VerifyGateLookup(
        result=latest_legacy,
        source="legacy_review" if latest_legacy is not None else None,
        is_current=False,
        has_owner_artifact=False,
    )


def latest_verify_evidence_for_owner(
    store: SqliteTaskStore,
    owner_task: Task,
) -> LatestVerifyEvidence | None:
    """Return the latest persisted verify evidence and its original epoch identity."""
    if owner_task.id is None:
        return None

    owner_artifacts = store.list_artifacts(owner_task.id, kind=VERIFY_GATE_ARTIFACT_KIND)
    if owner_artifacts:
        for artifact in owner_artifacts:
            metadata = artifact.metadata if isinstance(artifact.metadata, dict) else None
            if metadata is None or metadata.get("schema_version") != VERIFY_GATE_ARTIFACT_SCHEMA_VERSION:
                continue
            result = _artifact_verify_result(metadata)
            epoch = _artifact_verify_epoch(metadata)
            if result is None or epoch is None:
                continue
            return LatestVerifyEvidence(
                result=result,
                epoch=epoch,
                source="owner_artifact",
                has_owner_artifact=True,
            )
        return None

    latest_review = store.get_latest_review(owner_task.id)
    if latest_review is None:
        return None
    legacy_result = _task_verify_result(latest_review)
    if legacy_result is None:
        return None
    timeout_seconds = getattr(latest_review, "review_verify_timeout_seconds", None)
    timeout_grace_seconds = getattr(latest_review, "review_verify_timeout_grace_seconds", None)
    return LatestVerifyEvidence(
        result=legacy_result,
        epoch=_result_epoch(
            legacy_result,
            verify_timeout_seconds=timeout_seconds if isinstance(timeout_seconds, int) else None,
            verify_timeout_grace_seconds=(
                float(timeout_grace_seconds)
                if isinstance(timeout_grace_seconds, (int, float)) and not isinstance(timeout_grace_seconds, bool)
                else None
            ),
        ),
        source="legacy_review",
        has_owner_artifact=False,
    )


def review_task_verify_epoch(task: Task, config: object | None) -> VerifyEpoch | None:
    """Build a canonical verify epoch from persisted review-era verify fields."""
    if task.task_type != "review":
        return None
    command = normalized_verify_command(task.review_verify_command)
    if command is None:
        return None
    timeout_seconds = getattr(config, "autonomous_verify_timeout_seconds", None)
    timeout_grace_seconds = getattr(config, "review_verify_timeout_grace_seconds", None)
    return make_verify_epoch(
        reviewed_branch=task.review_verify_branch,
        reviewed_head_sha=task.review_verify_head_sha,
        verify_command=command,
        verify_timeout_seconds=timeout_seconds if isinstance(timeout_seconds, int) else None,
        verify_timeout_grace_seconds=(
            float(timeout_grace_seconds)
            if isinstance(timeout_grace_seconds, (int, float)) and not isinstance(timeout_grace_seconds, bool)
            else None
        ),
    )


def owner_task_verify_epoch(task: Task, config: object | None, git: object | None) -> VerifyEpoch | None:
    """Build the current canonical verify epoch for an implementation owner."""
    branch = task.branch
    command = normalized_verify_command(getattr(config, "verify_command", None))
    if not branch or command is None or git is None:
        return None
    rev_parse = getattr(git, "rev_parse_if_exists", None)
    if not callable(rev_parse):
        return None
    try:
        head_sha = rev_parse(branch)
    except (GitError, OSError, RuntimeError, ValueError):
        return None
    if not isinstance(head_sha, str) or not head_sha:
        return None
    timeout_seconds = getattr(config, "autonomous_verify_timeout_seconds", None)
    timeout_grace_seconds = getattr(config, "review_verify_timeout_grace_seconds", None)
    return make_verify_epoch(
        reviewed_branch=branch,
        reviewed_head_sha=head_sha,
        verify_command=command,
        verify_timeout_seconds=timeout_seconds if isinstance(timeout_seconds, int) else None,
        verify_timeout_grace_seconds=(
            float(timeout_grace_seconds)
            if isinstance(timeout_grace_seconds, (int, float)) and not isinstance(timeout_grace_seconds, bool)
            else None
        ),
    )


def resolve_verify_owner_task(store: SqliteTaskStore, task: Task) -> Task:
    """Resolve the canonical owner row for verify evidence attached to ``task``."""
    if task.task_type == "review":
        for related_id in (task.depends_on, task.based_on):
            if isinstance(related_id, str):
                owner = store.get(related_id)
                if owner is not None:
                    return owner
    return task


def resolve_verify_read_model(
    store: SqliteTaskStore,
    task: Task,
    *,
    owner_task: Task | None = None,
    current_epoch: VerifyEpoch | None,
) -> VerifyReadModel | None:
    """Resolve the shared operator-facing verify read model for one task surface."""
    owner = owner_task or resolve_verify_owner_task(store, task)

    lookup = latest_verify_result_for_epoch(store, owner, current_epoch=current_epoch)
    if lookup.result is None or lookup.source is None:
        return None

    legacy_markdown: str | None = None
    if lookup.source == "legacy_review" and lookup.result.source_task_id:
        source_task = store.get(lookup.result.source_task_id)
        if source_task is not None:
            legacy_markdown = source_task.review_verify_markdown

    return VerifyReadModel(
        result=lookup.result,
        source=lookup.source,
        is_current=lookup.is_current,
        has_owner_artifact=lookup.has_owner_artifact,
        owner_task_id=owner.id,
        source_task_id=lookup.result.source_task_id,
        source_task_type=lookup.result.source_task_type,
        legacy_markdown=legacy_markdown,
    )


def verify_output_artifact_path(read_model: VerifyReadModel) -> str | None:
    """Return the canonical captured output artifact path for one verify read model."""
    return read_model.result.output_artifact_path


def read_verify_output_excerpt(
    project_dir: Path,
    read_model: VerifyReadModel,
    *,
    max_lines: int = 20,
    max_chars: int = 4000,
) -> str | None:
    """Read a bounded text excerpt from the canonical verify output artifact, if present."""
    from gza.artifact_paths import InvalidArtifactPathError, resolve_artifact_path

    artifact_path = verify_output_artifact_path(read_model)
    if not artifact_path:
        return None
    try:
        resolved_path = resolve_artifact_path(project_dir, artifact_path)
    except InvalidArtifactPathError:
        return None
    if not resolved_path.exists():
        return None
    content = resolved_path.read_text(encoding="utf-8", errors="replace").strip()
    if not content:
        return None
    lines = content.splitlines()
    excerpt = "\n".join(lines[:max_lines])
    if len(excerpt) > max_chars:
        excerpt = excerpt[:max_chars].rstrip()
    return excerpt


def persist_verify_gate_artifact(
    store: SqliteTaskStore,
    config: Config,
    *,
    owner_task: Task,
    source_task: Task,
    result: Any,
    verify_timeout_seconds: int | None,
    verify_timeout_grace_seconds: float | None,
    output_artifact_id: int | None = None,
    output_artifact_task_id: str | None = None,
    output_artifact_path: str | None = None,
    producer: str,
) -> None:
    """Persist canonical owner-attached verify-gate evidence."""
    if owner_task.id is None:
        return

    epoch = make_verify_epoch(
        reviewed_branch=getattr(result, "reviewed_branch", None),
        reviewed_head_sha=getattr(result, "reviewed_head_sha", None),
        verify_command=getattr(result, "command", None),
        verify_timeout_seconds=verify_timeout_seconds,
        verify_timeout_grace_seconds=verify_timeout_grace_seconds,
    )
    payload = {
        "schema_version": VERIFY_GATE_ARTIFACT_SCHEMA_VERSION,
        "verify_epoch": {
            "reviewed_branch": epoch.reviewed_branch,
            "reviewed_head_sha": epoch.reviewed_head_sha,
            "verify_command": epoch.verify_command,
            "verify_timeout_seconds": epoch.verify_timeout_seconds,
            "verify_timeout_grace_seconds": epoch.verify_timeout_grace_seconds,
        },
        "result": {
            "command": getattr(result, "command", None),
            "status": getattr(result, "status", None),
            "exit_status": getattr(result, "exit_status", None),
            "captured_at": getattr(result, "captured_at").isoformat(),
            "reviewed_branch": getattr(result, "reviewed_branch", None),
            "reviewed_head_sha": getattr(result, "reviewed_head_sha", None),
            "reviewed_base_sha": getattr(result, "reviewed_base_sha", None),
            "working_directory": getattr(result, "working_directory", None),
            "failure": getattr(result, "failure", None),
        },
        "source_task_id": source_task.id,
        "source_task_type": source_task.task_type,
        "output_artifact_id": output_artifact_id,
        "output_artifact_task_id": output_artifact_task_id,
        "output_artifact_path": output_artifact_path,
    }
    store_command_output_artifact(
        store,
        owner_task,
        config,
        kind=VERIFY_GATE_ARTIFACT_KIND,
        producer=producer,
        label=VERIFY_GATE_ARTIFACT_LABEL,
        output=json.dumps(payload, sort_keys=True, indent=2) + "\n",
        command=getattr(result, "command", None),
        status=getattr(result, "status", None),
        exit_status=getattr(result, "exit_status", None),
        head_sha=getattr(result, "reviewed_head_sha", None),
        metadata=payload,
        created_at=getattr(result, "captured_at"),
        content_type="application/json",
    )


def refresh_preserved_rebase_review_verify_heads(
    store: SqliteTaskStore,
    impl_task: Task | None,
    *,
    branch: str | None,
    old_head_sha: str | None,
    new_head_sha: str | None,
) -> int:
    """Retarget preserved verify evidence to a rewritten same-diff branch head.

    When a completed same-branch rebase proves the tracked diff is unchanged, the
    latest completed review remains valid. For verify-only review blockers, the
    latest review's runner-owned verify failure provenance and any no-op improve
    verify provenance for that review must follow the rewritten branch head so the
    persisted mergeable recognition continues to match the current tip.
    """
    if impl_task is None or impl_task.id is None or impl_task.task_type != "implement":
        return 0
    if not branch or not old_head_sha or not new_head_sha or old_head_sha == new_head_sha:
        return 0

    reviews = [
        review
        for review in store.get_reviews_for_task(impl_task.id)
        if review.status == "completed"
    ]
    if not reviews:
        return 0
    latest_review = max(
        reviews,
        key=lambda review: (review.completed_at or review.created_at, review.created_at),
    )
    if latest_review.id is None:
        return 0

    refreshed = 0
    if (
        latest_review.review_verify_branch == branch
        and latest_review.review_verify_head_sha == old_head_sha
    ):
        latest_review.review_verify_head_sha = new_head_sha
        store.update(latest_review)
        refreshed += 1

    for improve in store.get_improve_tasks_for(impl_task.id, latest_review.id):
        if improve.status != "completed" or improve.changed_diff is not False:
            continue
        if improve.review_verify_branch != branch or improve.review_verify_head_sha != old_head_sha:
            continue
        improve.review_verify_head_sha = new_head_sha
        store.update(improve)
        refreshed += 1

    return refreshed
