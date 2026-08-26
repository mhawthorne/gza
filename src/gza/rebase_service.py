"""Reusable task-backed rebase service and outcome artifacts."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Literal, Protocol, cast

from .artifacts import store_command_output_artifact
from .config import Config
from .db import DuplicateActiveChildError, SqliteTaskStore, Task
from .git import Git, GitError
from .runtime_context import RuntimeExecutionContext

REBASE_EXECUTION_OUTCOME_ARTIFACT_KIND = "rebase_execution_outcome"
REBASE_EXECUTION_OUTCOME_SCHEMA_VERSION = 1

RebaseExecutionStatus = Literal[
    "skipped",
    "queued",
    "in_progress",
    "identity_conflict",
    "completed_mechanical",
    "completed_no_op",
    "provider_conflict_resolved",
    "failed",
    "proof_unavailable",
]

COMPLETED_REBASE_EXECUTION_STATUSES: frozenset[RebaseExecutionStatus] = frozenset(
    {
        "skipped",
        "completed_mechanical",
        "completed_no_op",
        "provider_conflict_resolved",
    }
)
REPLAYABLE_REBASE_EXECUTION_STATUSES: frozenset[RebaseExecutionStatus] = frozenset(
    {
        "completed_mechanical",
        "completed_no_op",
        "provider_conflict_resolved",
    }
)


@dataclass(frozen=True)
class RebaseExecutionOutcome:
    """Structured outcome captured at the end of a task-backed rebase execution."""

    status: RebaseExecutionStatus
    source_head_before: str | None
    target_head_before: str | None
    source_head_after: str | None
    target_head_after: str | None
    changed_diff: bool | None
    completion_reason: str | None = None
    provider_conflict_resolved: bool = False
    superseded: bool = False


@dataclass(frozen=True)
class RebaseServiceRequest:
    """Input for creating or running one task-backed rebase."""

    parent_task_id: str
    branch: str
    target_branch: str
    remote: bool = False
    trigger_source: str = "manual"
    run: bool = True
    skip_if_target_contained: bool = True
    reuse_completed: bool = True
    duplicate_as_result: bool = True


@dataclass(frozen=True)
class RebaseServiceResult:
    """Typed synchronous rebase service result for landing and CLI adapters."""

    status: RebaseExecutionStatus
    parent_task_id: str
    branch: str
    target_ref: str
    rebase_task_id: str | None = None
    exit_code: int = 0
    changed_diff: bool | None = None
    artifact_id: int | None = None
    artifact_key: str | None = None
    source_head_before: str | None = None
    target_head_before: str | None = None
    source_head_after: str | None = None
    target_head_after: str | None = None
    fact: str | None = None

    @property
    def completed(self) -> bool:
        return self.status in COMPLETED_REBASE_EXECUTION_STATUSES


class RebaseTaskFactory(Protocol):
    def __call__(
        self,
        store: SqliteTaskStore,
        parent_task_id: str,
        branch: str,
        target_branch: str,
        *,
        config: Config | None = None,
        trigger_source: str,
    ) -> Task: ...


class RebaseExecutor(Protocol):
    def __call__(
        self,
        *,
        config: Config,
        store: SqliteTaskStore,
        rebase_task: Task,
        branch: str,
        target_branch: str,
        remote: bool = False,
        parent_task_id: str | None = None,
        failure_hint_lines: list[str] | None = None,
        runtime_context: RuntimeExecutionContext | None = None,
        outcome_callback: Callable[[RebaseExecutionOutcome], None] | None = None,
    ) -> int: ...


def resolve_rebase_target_ref(target_branch: str, *, remote: bool = False) -> str:
    return f"origin/{target_branch}" if remote else target_branch


def execute_task_backed_rebase_service(
    *,
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    request: RebaseServiceRequest,
    create_rebase_task: RebaseTaskFactory,
    executor: RebaseExecutor | None = None,
    runtime_context: RuntimeExecutionContext | None = None,
) -> RebaseServiceResult:
    """Create/reuse and optionally run one task-backed rebase synchronously."""

    target_ref = resolve_rebase_target_ref(request.target_branch, remote=request.remote)
    source_head_before = _rev_parse_if_available(git, request.branch)
    target_head_before = _rev_parse_if_available(git, target_ref)
    initial_proof_required = request.skip_if_target_contained or request.reuse_completed
    if initial_proof_required and (source_head_before is None or target_head_before is None):
        return RebaseServiceResult(
            status="proof_unavailable",
            parent_task_id=request.parent_task_id,
            branch=request.branch,
            target_ref=target_ref,
            exit_code=1,
            source_head_before=source_head_before,
            target_head_before=target_head_before,
            fact="source or target ref could not be resolved",
        )

    if request.reuse_completed and source_head_before is not None and target_head_before is not None:
        replay = find_reusable_rebase_execution_outcome(
            store=store,
            parent_task_id=request.parent_task_id,
            branch=request.branch,
            target_ref=target_ref,
            current_source_head=source_head_before,
            current_target_head=target_head_before,
        )
        if replay.result is not None:
            return replay.result
        if replay.invalid_fact is not None:
            return RebaseServiceResult(
                status="proof_unavailable",
                parent_task_id=request.parent_task_id,
                branch=request.branch,
                target_ref=target_ref,
                exit_code=1,
                source_head_before=source_head_before,
                target_head_before=target_head_before,
                fact=replay.invalid_fact,
            )

    if request.skip_if_target_contained and source_head_before is not None and target_head_before is not None:
        try:
            if git.is_ancestor(target_head_before, source_head_before):
                artifact = persist_rebase_execution_outcome(
                    store=store,
                    task=store.get(request.parent_task_id) or _synthetic_task(request.parent_task_id),
                    config=config,
                    parent_task_id=request.parent_task_id,
                    branch=request.branch,
                    target_ref=target_ref,
                    rebase_task_id=None,
                    outcome=RebaseExecutionOutcome(
                        status="skipped",
                        source_head_before=source_head_before,
                        target_head_before=target_head_before,
                        source_head_after=source_head_before,
                        target_head_after=target_head_before,
                        changed_diff=False,
                    ),
                )
                return RebaseServiceResult(
                    status="skipped",
                    parent_task_id=request.parent_task_id,
                    branch=request.branch,
                    target_ref=target_ref,
                    changed_diff=False,
                    artifact_id=artifact.id,
                    artifact_key=artifact.key,
                    source_head_before=source_head_before,
                    target_head_before=target_head_before,
                    source_head_after=source_head_before,
                    target_head_after=target_head_before,
                    fact="source already contains target",
                )
        except GitError as exc:
            return RebaseServiceResult(
                status="proof_unavailable",
                parent_task_id=request.parent_task_id,
                branch=request.branch,
                target_ref=target_ref,
                exit_code=1,
                source_head_before=source_head_before,
                target_head_before=target_head_before,
                fact=f"target ancestry could not be proven: {exc}",
            )

    try:
        rebase_task = create_rebase_task(
            store,
            request.parent_task_id,
            request.branch,
            target_ref,
            config=config,
            trigger_source=request.trigger_source,
        )
    except DuplicateActiveChildError as exc:
        if not request.duplicate_as_result:
            raise
        active = exc.active_child
        duplicate_identity_matches = (
            active.based_on == request.parent_task_id
            and active.branch == request.branch
            and active.base_branch == target_ref
        )
        return RebaseServiceResult(
            status="in_progress" if duplicate_identity_matches else "identity_conflict",
            parent_task_id=request.parent_task_id,
            branch=request.branch,
            target_ref=target_ref,
            rebase_task_id=active.id,
            exit_code=1,
            source_head_before=source_head_before,
            target_head_before=target_head_before,
            fact=(
                f"rebase task already active: {active.id}"
                if duplicate_identity_matches
                else (
                    "active rebase task identity does not match requested "
                    f"parent/branch/target: {active.id}"
                )
            ),
        )
    assert rebase_task.id is not None
    rebase_task.branch = request.branch
    store.update(rebase_task)

    if not request.run:
        return RebaseServiceResult(
            status="queued",
            parent_task_id=request.parent_task_id,
            branch=request.branch,
            target_ref=target_ref,
            rebase_task_id=rebase_task.id,
            source_head_before=source_head_before,
            target_head_before=target_head_before,
            fact="rebase task queued",
        )

    if executor is None:
        raise ValueError("executor is required when request.run is true")

    captured: list[RebaseExecutionOutcome] = []
    exit_code = executor(
        config=config,
        store=store,
        rebase_task=rebase_task,
        branch=request.branch,
        target_branch=request.target_branch,
        remote=request.remote,
        parent_task_id=request.parent_task_id,
        runtime_context=runtime_context,
        outcome_callback=captured.append,
    )
    if captured:
        outcome = captured[-1]
    else:
        outcome = RebaseExecutionOutcome(
            status="failed" if exit_code else "proof_unavailable",
            source_head_before=source_head_before,
            target_head_before=target_head_before,
            source_head_after=_rev_parse_if_available(git, request.branch),
            target_head_after=_rev_parse_if_available(git, target_ref),
            changed_diff=None,
        )
    artifact = persist_rebase_execution_outcome(
        store=store,
        task=rebase_task,
        config=config,
        parent_task_id=request.parent_task_id,
        branch=request.branch,
        target_ref=target_ref,
        rebase_task_id=rebase_task.id,
        outcome=outcome,
        exit_status=str(exit_code),
    )
    return RebaseServiceResult(
        status=outcome.status,
        parent_task_id=request.parent_task_id,
        branch=request.branch,
        target_ref=target_ref,
        rebase_task_id=rebase_task.id,
        exit_code=exit_code,
        changed_diff=outcome.changed_diff,
        artifact_id=artifact.id,
        artifact_key=artifact.key,
        source_head_before=outcome.source_head_before,
        target_head_before=outcome.target_head_before,
        source_head_after=outcome.source_head_after,
        target_head_after=outcome.target_head_after,
        fact=None if exit_code == 0 else "task-backed rebase failed",
    )


@dataclass(frozen=True)
class PersistedRebaseOutcome:
    id: int
    key: str


@dataclass(frozen=True)
class RebaseOutcomeReplayLookup:
    result: RebaseServiceResult | None = None
    invalid_fact: str | None = None


def persist_rebase_execution_outcome(
    *,
    store: SqliteTaskStore,
    task: Task,
    config: Config,
    parent_task_id: str,
    branch: str,
    target_ref: str,
    rebase_task_id: str | None,
    outcome: RebaseExecutionOutcome,
    exit_status: str | None = None,
) -> PersistedRebaseOutcome:
    """Persist a generic exact-identity rebase outcome artifact."""

    metadata = _rebase_outcome_metadata(
        parent_task_id=parent_task_id,
        branch=branch,
        target_ref=target_ref,
        rebase_task_id=rebase_task_id,
        outcome=outcome,
    )
    key = _rebase_outcome_key(metadata)
    metadata["key"] = key
    output = json.dumps(metadata, indent=2, sort_keys=True)
    stored = store_command_output_artifact(
        store,
        task,
        config,
        kind=REBASE_EXECUTION_OUTCOME_ARTIFACT_KIND,
        producer="gza.rebase_service",
        label="rebase_execution_outcome",
        output=output,
        status=outcome.status,
        exit_status=exit_status,
        head_sha=outcome.source_head_after,
        metadata=metadata,
    )
    return PersistedRebaseOutcome(id=stored.id, key=key)


def find_reusable_rebase_execution_outcome(
    *,
    store: SqliteTaskStore,
    parent_task_id: str,
    branch: str,
    target_ref: str,
    current_source_head: str,
    current_target_head: str,
) -> RebaseOutcomeReplayLookup:
    """Return the exact completed outcome matching the current durable post-state."""

    matches: list[tuple[Task, Any, dict[str, Any]]] = []
    for child in reversed(store.get_based_on_children_by_type(parent_task_id, "rebase")):
        if child.id is None:
            continue
        for artifact in store.list_artifacts(child.id, kind=REBASE_EXECUTION_OUTCOME_ARTIFACT_KIND):
            metadata = artifact.metadata or {}
            if (
                metadata.get("parent_task_id") != parent_task_id
                or metadata.get("branch") != branch
                or metadata.get("target_ref") != target_ref
            ):
                continue
            status = metadata.get("status")
            if status in {"skipped", "failed", "proof_unavailable"}:
                continue
            if status not in REPLAYABLE_REBASE_EXECUTION_STATUSES:
                return RebaseOutcomeReplayLookup(
                    invalid_fact="stored rebase outcome is malformed: status is not replayable"
                )
            validation_error = _validate_rebase_replay_metadata(metadata)
            if validation_error is not None:
                return RebaseOutcomeReplayLookup(
                    invalid_fact=f"stored rebase outcome is malformed: {validation_error}"
                )
            if (
                metadata["source_head_after"] != current_source_head
                or metadata["target_head_after"] != current_target_head
            ):
                continue
            matches.append((child, artifact, metadata))

    if len(matches) > 1:
        return RebaseOutcomeReplayLookup(invalid_fact="stored rebase outcome is ambiguous")
    if not matches:
        return RebaseOutcomeReplayLookup()

    child, artifact, metadata = matches[0]
    status = cast(RebaseExecutionStatus, metadata["status"])
    return RebaseOutcomeReplayLookup(
        result=RebaseServiceResult(
                status=status,
                parent_task_id=parent_task_id,
                branch=branch,
                target_ref=target_ref,
                rebase_task_id=child.id,
                changed_diff=cast(bool, metadata["changed_diff"]),
                artifact_id=artifact.id,
                artifact_key=cast(str, metadata["key"]),
                source_head_before=cast(str, metadata["source_head_before"]),
                target_head_before=cast(str, metadata["target_head_before"]),
                source_head_after=cast(str, metadata["source_head_after"]),
                target_head_after=cast(str, metadata["target_head_after"]),
                fact="reused exact rebase outcome",
            )
    )


def _validate_rebase_replay_metadata(metadata: dict[str, Any]) -> str | None:
    if metadata.get("schema_version") != REBASE_EXECUTION_OUTCOME_SCHEMA_VERSION:
        return "unsupported schema version"
    expected_key = metadata.get("key")
    if not isinstance(expected_key, str) or not expected_key:
        return "missing key"
    if _rebase_outcome_key(metadata) != expected_key:
        return "key mismatch"
    for field in (
        "parent_task_id",
        "branch",
        "target_ref",
        "source_head_before",
        "target_head_before",
        "source_head_after",
        "target_head_after",
    ):
        value = metadata.get(field)
        if not isinstance(value, str) or not value:
            return f"missing {field}"
    status = metadata.get("status")
    changed_diff = metadata.get("changed_diff")
    if not isinstance(changed_diff, bool):
        return "changed_diff is not boolean"
    provider_conflict_resolved = metadata.get("provider_conflict_resolved")
    if not isinstance(provider_conflict_resolved, bool):
        return "provider_conflict_resolved is not boolean"
    superseded = metadata.get("superseded")
    if not isinstance(superseded, bool):
        return "superseded is not boolean"
    if status == "provider_conflict_resolved" and not provider_conflict_resolved:
        return "provider status missing provider resolution flag"
    if status != "provider_conflict_resolved" and provider_conflict_resolved:
        return "provider resolution flag conflicts with status"
    if status == "completed_no_op" and not superseded:
        return "no-op status missing superseded flag"
    if status != "completed_no_op" and superseded:
        return "superseded flag conflicts with status"
    return None


def _rebase_outcome_metadata(
    *,
    parent_task_id: str,
    branch: str,
    target_ref: str,
    rebase_task_id: str | None,
    outcome: RebaseExecutionOutcome,
) -> dict[str, Any]:
    return {
        "schema_version": REBASE_EXECUTION_OUTCOME_SCHEMA_VERSION,
        "parent_task_id": parent_task_id,
        "rebase_task_id": rebase_task_id,
        "branch": branch,
        "target_ref": target_ref,
        "source_head_before": outcome.source_head_before,
        "target_head_before": outcome.target_head_before,
        "source_head_after": outcome.source_head_after,
        "target_head_after": outcome.target_head_after,
        "status": outcome.status,
        "changed_diff": outcome.changed_diff,
        "provider_conflict_resolved": outcome.provider_conflict_resolved,
        "superseded": outcome.superseded,
        "completion_reason": outcome.completion_reason,
    }


def _rebase_outcome_key(metadata: dict[str, Any]) -> str:
    key_payload = {
        key: value
        for key, value in metadata.items()
        if key not in {"key"}
    }
    encoded = json.dumps(key_payload, sort_keys=True, separators=(",", ":"))
    return sha256(encoded.encode("utf-8")).hexdigest()


def _rev_parse_if_available(git: Git, ref: str) -> str | None:
    rev_parse = getattr(git, "rev_parse_if_exists", None)
    if not callable(rev_parse):
        return None
    return rev_parse(ref)


def _synthetic_task(task_id: str) -> Task:
    return Task(id=task_id, prompt="", status="completed")
