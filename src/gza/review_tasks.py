"""Shared helpers for creating review, follow-up, adjudication, and investigation tasks."""

import json
import re
import sqlite3
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, cast

from .artifacts import prepare_command_output_artifact, store_command_output_artifact
from .config import Config
from .db import NewTaskParams, SqliteTaskStore, Task, TaskArtifact
from .derived_tags import resolve_derived_task_tags
from .prompts import PromptBuilder
from .rebase_diff import parse_rebase_diff_provenance
from .review_scope import (
    build_resolution_review_scope,
    build_spec_coherence_review_scope,
    resolve_review_scope_for_impl,
)
from .review_verdict import ReviewFinding
from .review_verify_state import VerifyEpoch
from .task_slug import (
    extract_task_id_suffix,
    get_base_task_slug,
    strip_derived_implement_prefixes,
)

_FOLLOWUP_PROMPT_PREFIX_RE = re.compile(
    r"^Follow-up\s+(\S+)\s+from review\s+(\S+)\s+for task\s+(\S+):"
)
_DEFERRED_BLOCKER_PROMPT_PREFIX_RE = re.compile(
    r"^Deferred blocker\s+(\S+)\s+from review\s+(\S+)\s+for task\s+(\S+):"
)
_REVIEW_BLOCKER_ADJUDICATION_PROMPT_PREFIX_RE = re.compile(
    r"^Adjudicate blocker\s+(\S+)\s+from review\s+(\S+)\s+for task\s+(\S+):"
)
_REVIEW_BLOCKER_ADJUDICATION_SOURCE_TASK_RE = re.compile(
    r"^Dispute source task:\s*(\S+)\s*$",
    re.MULTILINE,
)
_REVIEW_BLOCKER_ADJUDICATION_HEAD_SHA_RE = re.compile(
    r"^Dispute source head SHA:\s*(\S+)\s*$",
    re.MULTILINE,
)
_REVIEW_BLOCKER_ADJUDICATION_SOURCE_BRANCH_RE = re.compile(
    r"^Dispute source branch:\s*(\S+)\s*$",
    re.MULTILINE,
)
_REVIEW_BLOCKER_ADJUDICATION_REASON_RE = re.compile(r"^Reason:\s*(.+?)\s*$", re.MULTILINE)
_REVIEW_BLOCKER_ADJUDICATION_EVIDENCE_RE = re.compile(r"^Evidence:\s*(.+?)\s*$", re.MULTILINE)
_REVIEW_BLOCKER_ADJUDICATION_CURRENT_STATE_CITATION_RE = re.compile(
    r"^Current-state citation:\s*(.+?)\s*$",
    re.MULTILINE,
)
_DISPUTE_ARTIFACT_ID_RE = re.compile(r"^Dispute artifact id:\s*(\d+)\s*$", re.MULTILINE)
_DISPUTE_SOURCE_TASK_ID_RE = re.compile(r"^Dispute source task:\s*(\S+)\s*$", re.MULTILINE)

OFF_TOPIC_VERIFY_INVESTIGATION_ARTIFACT_KIND = "off_topic_verify_investigation"
OFF_TOPIC_VERIFY_INVESTIGATION_REUSABLE_STATUSES = frozenset({"pending", "in_progress"})
SPEC_COHERENCE_REVIEW_SCOPE = "spec-coherence"


class DuplicateReviewError(ValueError):
    """Raised when attempting to create a duplicate active review task."""

    def __init__(self, active_review: Task) -> None:
        self.active_review = active_review
        super().__init__(
            f"An active review task already exists: {active_review.id} ({active_review.status})"
        )


class OffTopicVerifyPersistenceError(RuntimeError):
    """Raised when audited off-topic investigation or clearance persistence fails closed."""


@dataclass(frozen=True)
class OffTopicVerifyClearancePersistenceResult:
    """Persisted off-topic verify clearance details."""

    created_tasks: tuple[Task, ...]
    reused_tasks: tuple[Task, ...]
    review_cleared_at: datetime


@dataclass(frozen=True)
class ReviewClearancePersistenceResult:
    """Persisted structured review-clearance details."""

    review_cleared_at: datetime


def _known_derived_suffixes_for_review(store: SqliteTaskStore, impl_task: Task) -> set[str]:
    """Collect task-id suffixes from an implementation task lineage.

    Includes the implementation task itself and ancestors reachable via
    ``based_on`` / ``depends_on``. This allows exact derived-prefix stripping
    without over-matching semantic ``*-impl-*`` slug segments.
    """
    known: set[str] = set()
    current = impl_task
    seen: set[str] = set()
    while current:
        suffix = extract_task_id_suffix(current.id)
        if suffix:
            known.add(suffix)
        if current.id is not None:
            current_id = str(current.id)
            if current_id in seen:
                break
            seen.add(current_id)
        parent_id = current.based_on or current.depends_on
        if parent_id is None:
            break
        parent = store.get(parent_id)
        if parent is None:
            break
        current = parent
    return known


def build_auto_review_prompt(
    impl_task: Task,
    project_prefix: str | None = None,
    known_task_id_suffixes: Iterable[str] | None = None,
) -> str:
    """Build prompt text for runner auto-created reviews.

    Preserves the historical slug-first prompt semantics used by runner auto-review.
    When project_prefix is provided, it is stripped from the slug-derived description
    so the prompt contains only the semantic portion (e.g. "review add-feature" rather
    than "review myproj-add-feature").
    """
    if impl_task.slug:
        slug = get_base_task_slug(impl_task.slug) if "-" in impl_task.slug else None
        if slug:
            # Derived implement slugs are "<task_id_suffix>-impl-<semantic-slug>".
            # Normalize first, then optionally strip project_prefix from semantic tail.
            normalized = strip_derived_implement_prefixes(slug, set(known_task_id_suffixes or ()))
            if normalized is None:
                slug = None
            else:
                slug = normalized
        if slug:
            if project_prefix and slug.startswith(f"{project_prefix}-"):
                slug = slug[len(project_prefix) + 1:]
            return f"review {slug}"

    return f"Review task {impl_task.id}"


def build_verify_fix_prompt(impl_task_id: str, verify_epoch: VerifyEpoch) -> str:
    """Build the stable prompt used to key one verify_fix lane per verify epoch."""
    return PromptBuilder().verify_fix_task_prompt(
        impl_task_id,
        reviewed_branch=verify_epoch.reviewed_branch,
        reviewed_head_sha=verify_epoch.reviewed_head_sha,
        verify_command=verify_epoch.verify_command,
        verify_timeout_seconds=verify_epoch.verify_timeout_seconds,
        verify_timeout_grace_seconds=verify_epoch.verify_timeout_grace_seconds,
    )


def find_existing_verify_fix_task(
    store: SqliteTaskStore,
    *,
    impl_task_id: str,
    verify_epoch: VerifyEpoch,
) -> Task | None:
    """Return the latest non-dropped verify_fix task for the given verify epoch."""
    expected_prompt = build_verify_fix_prompt(impl_task_id, verify_epoch)
    candidates = [
        task
        for task in store.get_verify_fix_tasks_by_root(impl_task_id)
        if task.prompt == expected_prompt and task.status != "dropped"
    ]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda task: (
            task.created_at or datetime.min.replace(tzinfo=UTC),
            task.id or "",
        ),
    )


def create_or_reuse_verify_fix_task(
    store: SqliteTaskStore,
    *,
    impl_task: Task,
    based_on_task: Task,
    verify_epoch: VerifyEpoch,
    trigger_source: str,
    model: str | None = None,
    provider: str | None = None,
) -> tuple[Task, bool]:
    """Create or reuse one same-branch verify_fix lane for the given verify epoch."""
    if impl_task.id is None:
        raise ValueError("Cannot create verify_fix for implementation without an ID.")
    if based_on_task.id is None:
        raise ValueError("Cannot create verify_fix without a based_on task ID.")

    existing = find_existing_verify_fix_task(
        store,
        impl_task_id=impl_task.id,
        verify_epoch=verify_epoch,
    )
    if existing is not None:
        return existing, False

    created = store.add(
        prompt=build_verify_fix_prompt(impl_task.id, verify_epoch),
        task_type="verify_fix",
        based_on=based_on_task.id,
        same_branch=True,
        tags=resolve_derived_task_tags(impl_task),
        review_scope=(
            resolved_scope.summary
            if (resolved_scope := resolve_review_scope_for_impl(store, impl_task)) is not None
            else None
        ),
        model=model,
        provider=provider,
        trigger_source=trigger_source,
    )
    return created, True


def create_review_task(
    store: SqliteTaskStore,
    impl_task: Task,
    *,
    trigger_source: str,
    model: str | None = None,
    provider: str | None = None,
    prompt_mode: Literal["cli", "auto"] = "cli",
    project_prefix: str | None = None,
) -> Task:
    """Create a review task for a completed implementation task.

    Validates implementation type/state and prevents duplicate active reviews.
    """
    if impl_task.task_type != "implement":
        raise ValueError(
            f"Task {impl_task.id} is a {impl_task.task_type} task. "
            "Expected an implementation task."
        )
    if impl_task.status != "completed":
        raise ValueError(
            f"Task {impl_task.id} is {impl_task.status}. Can only review completed tasks."
        )
    if impl_task.id is None:
        raise ValueError("Cannot create review for task without an ID.")

    existing_reviews = store.get_reviews_for_task(impl_task.id)
    active_reviews = [r for r in existing_reviews if r.status in ("pending", "in_progress")]
    if active_reviews:
        raise DuplicateReviewError(active_reviews[0])

    if prompt_mode == "auto":
        known_suffixes = _known_derived_suffixes_for_review(store, impl_task)
        review_prompt = build_auto_review_prompt(
            impl_task,
            project_prefix=project_prefix,
            known_task_id_suffixes=known_suffixes,
        )
    else:
        review_prompt = PromptBuilder().review_task_prompt(impl_task.id, impl_task.prompt)
    resolved_scope = resolve_review_scope_for_impl(store, impl_task)
    review_task = store.add(
        prompt=review_prompt,
        task_type="review",
        depends_on=impl_task.id,
        tags=resolve_derived_task_tags(impl_task),
        based_on=impl_task.id,
        enforce_single_active_sibling=True,
        review_scope=resolved_scope.summary if resolved_scope is not None else None,
        model=model,
        provider=provider,
        trigger_source=trigger_source,
    )
    impl_unit = store.resolve_merge_unit_for_task(impl_task.id)
    if impl_unit is not None:
        store.get_or_create_merge_unit_for_task(review_task)
    return review_task


def build_spec_coherence_review_prompt(
    impl_task: Task,
    *,
    changed_paths: Iterable[str],
) -> str:
    """Build the behavior-spec coherence review prompt for one implementation branch."""
    scope_lines = "\n".join(f"- `{path}`" for path in changed_paths)
    return (
        f"Run /gza-spec-coherence for implementation task {impl_task.id}.\n\n"
        "This is the branch-scoped behavior-spec coherence gate.\n"
        "Review only the branch diff under the configured behavior-spec paths against the rest of the behavior-spec set.\n\n"
        "Write the report using the standard review output contract:\n"
        "- `## Summary`\n"
        "- `## Blockers`\n"
        "- `## Follow-Ups`\n"
        "- `## Questions / Assumptions`\n"
        "- `## Verdict`\n"
        "The final verdict must be exactly one of `APPROVED`, `CHANGES_REQUESTED`, or `NEEDS_DISCUSSION`.\n"
        "Do not omit empty sections; write `None.` when a section has no entries.\n\n"
        "Changed behavior-spec paths in scope:\n"
        f"{scope_lines}\n"
    )


def create_spec_coherence_review_task(
    store: SqliteTaskStore,
    impl_task: Task,
    *,
    reviewed_head_sha: str,
    changed_paths: Iterable[str],
    trigger_source: str,
    model: str | None = None,
    provider: str | None = None,
) -> Task:
    """Create the dedicated behavior-spec coherence review task for an implementation."""
    if impl_task.task_type != "implement":
        raise ValueError(
            f"Task {impl_task.id} is a {impl_task.task_type} task. Expected an implementation task."
        )
    if impl_task.status != "completed":
        raise ValueError(
            f"Task {impl_task.id} is {impl_task.status}. Can only review completed tasks."
        )
    if impl_task.id is None:
        raise ValueError("Cannot create review for task without an ID.")

    normalized_head_sha = reviewed_head_sha.strip()
    if not normalized_head_sha:
        raise ValueError("Spec coherence review requires the current reviewed head SHA.")

    changed = tuple(dict.fromkeys(path.strip() for path in changed_paths if str(path).strip()))
    if not changed:
        raise ValueError("Spec coherence review requires at least one changed behavior-spec path.")

    inherited_tags = tuple(resolve_derived_task_tags(impl_task))
    review_tags = inherited_tags + tuple(
        tag for tag in ("spec-coherence", "specs-behavior") if tag not in inherited_tags
    )

    existing_reviews = store.get_reviews_for_task(impl_task.id)
    active_reviews = [r for r in existing_reviews if r.status in ("pending", "in_progress")]
    if active_reviews:
        raise DuplicateReviewError(active_reviews[0])

    review_task = store.add(
        prompt=build_spec_coherence_review_prompt(impl_task, changed_paths=changed),
        task_type="review",
        depends_on=impl_task.id,
        tags=review_tags,
        based_on=impl_task.id,
        enforce_single_active_sibling=True,
        review_scope=build_spec_coherence_review_scope(
            implementation_task_id=impl_task.id,
            reviewed_head_sha=normalized_head_sha,
            changed_paths=changed,
        ),
        model=model,
        provider=provider,
        trigger_source=trigger_source,
    )
    impl_unit = store.resolve_merge_unit_for_task(impl_task.id)
    if impl_unit is not None:
        store.get_or_create_merge_unit_for_task(review_task)
    return review_task


def create_resolution_review_task(
    store: SqliteTaskStore,
    impl_task: Task,
    *,
    rebase_task: Task,
    resolved_head_sha: str,
    resolved_target_sha: str,
    trigger_source: str,
    model: str | None = None,
    provider: str | None = None,
) -> Task:
    """Create a resolution-scoped review task for a changed/unknown rebase."""
    if impl_task.id is None:
        raise ValueError("Cannot create resolution review for task without an ID.")
    if rebase_task.id is None:
        raise ValueError("Cannot create resolution review without a rebase task ID.")
    if not resolved_head_sha or not resolved_target_sha:
        raise ValueError("Resolution review requires resolved head and target SHAs.")
    provenance = parse_rebase_diff_provenance(rebase_task.review_scope)
    if provenance is None or not provenance.resolved_head_sha or not provenance.resolved_target_sha:
        raise ValueError("Resolution review requires persisted rebase provenance with resolved head and target SHAs.")
    if (
        resolved_head_sha != provenance.resolved_head_sha
        or resolved_target_sha != provenance.resolved_target_sha
    ):
        raise ValueError("Resolution review metadata must match the completed rebase provenance.")

    review_task = create_review_task(
        store,
        impl_task,
        trigger_source=trigger_source,
        model=model,
        provider=provider,
        prompt_mode="cli",
    )
    review_task.review_scope = build_resolution_review_scope(
        implementation_task_id=impl_task.id,
        rebase_task_id=rebase_task.id,
        resolved_head_sha=provenance.resolved_head_sha,
        resolved_target_sha=provenance.resolved_target_sha,
        pre_rebase_head_sha=provenance.old_tip,
        pre_rebase_target_sha=provenance.target_at_start,
        pre_rebase_merge_base_sha=provenance.merge_base_at_start,
    )
    store.update(review_task)
    return review_task


def build_followup_prompt_prefix(review_task_id: str, impl_task_id: str, finding_id: str) -> str:
    """Build deterministic prompt prefix for auto-created follow-up tasks."""
    return f"Follow-up {finding_id} from review {review_task_id} for task {impl_task_id}:"


def build_deferred_blocker_prompt_prefix(review_task_id: str, impl_task_id: str, finding_id: str) -> str:
    """Build deterministic prompt prefix for auto-created deferred blocker tasks."""
    return f"Deferred blocker {finding_id} from review {review_task_id} for task {impl_task_id}:"


def build_review_blocker_adjudication_prompt_prefix(
    review_task_id: str,
    impl_task_id: str,
    finding_id: str,
) -> str:
    """Build deterministic prompt prefix for blocker adjudication tasks."""
    return f"Adjudicate blocker {finding_id} from review {review_task_id} for task {impl_task_id}:"


def _normalize_off_topic_investigation_signature(failing_node: Mapping[str, Any]) -> dict[str, str]:
    nodeid = str(failing_node.get("nodeid", "")).strip()
    assertion_signature = str(failing_node.get("assertion_signature", "") or "").strip()
    if not nodeid:
        raise ValueError("failing node evidence must include nodeid")
    return {"nodeid": nodeid, "assertion_signature": assertion_signature}


def _off_topic_investigation_signature_key(signature: Mapping[str, str]) -> str:
    raw = f"{signature['nodeid']}\n{signature['assertion_signature']}"
    return sha256(raw.encode("utf-8")).hexdigest()


def _format_off_topic_investigation_review_scope(
    *,
    signature: Mapping[str, str],
    payload: Mapping[str, Any],
) -> str:
    lines = [
        "Investigate off-topic verify failure",
        f"Node: {signature['nodeid']}",
    ]
    if signature["assertion_signature"]:
        lines.append(f"Signature: {signature['assertion_signature']}")
    lines.extend(
        [
            f"Implementation task: {payload.get('implementation_task_id')}",
            f"Review task: {payload.get('review_task_id')}",
            f"Reviewed head SHA: {payload.get('head_sha')}",
            f"Target branch: {payload.get('target_branch')}",
        ]
    )
    return "\n".join(lines)


def build_off_topic_verify_investigation_prompt(
    *,
    review_task_id: str,
    impl_task_id: str,
    signature: Mapping[str, str],
    payload: Mapping[str, Any],
) -> str:
    heading = (
        f"Investigate off-topic verify failure {signature['nodeid']} "
        f"from review {review_task_id} for task {impl_task_id}: REPRODUCE-OR-RECORD"
    )
    lines = [
        heading,
        "",
        "This task exists because lifecycle cleared a verify-only review blocker through the audited off-topic path.",
        "Contract: REPRODUCE-OR-RECORD.",
        "1. Reproduce the exact failing node under a bounded stress harness and fix only with proof, or",
        "2. Close with a structured inconclusive record that preserves attempts, environment, and observed evidence.",
        "Do not default to blanket sleeps, retries, @flaky, or broad timeout changes.",
        "",
        f"Implementation task: {impl_task_id}",
        f"Review task: {review_task_id}",
        f"Reviewed head SHA: {payload.get('head_sha')}",
        f"Reviewed tree fingerprint: {payload.get('tree_fingerprint')}",
        f"Target branch: {payload.get('target_branch')}",
        f"Target head SHA: {payload.get('target_head_sha')}",
        f"Failing node: {signature['nodeid']}",
    ]
    if signature["assertion_signature"]:
        lines.append(f"Failure signature: {signature['assertion_signature']}")
    lines.extend(
        [
            f"Baseline mode: {payload.get('baseline_mode')}",
            f"Verify command: {payload.get('verify_command')}",
        ]
    )
    return "\n".join(lines)


def create_or_reuse_off_topic_verify_investigations(
    store: SqliteTaskStore,
    *,
    config: Config,
    review_task: Task,
    impl_task: Task,
    payload: Mapping[str, Any],
    trigger_source: str,
) -> tuple[tuple[Task, ...], tuple[Task, ...]]:
    """Create or reuse one investigation task per normalized failing-node signature.

    Returns:
        (created_tasks, reused_tasks)
    """
    if review_task.id is None:
        raise ValueError("Cannot create investigation for review without an ID.")
    if impl_task.id is None:
        raise ValueError("Cannot create investigation for implementation without an ID.")

    failing_nodes_raw = payload.get("failing_nodes")
    if not isinstance(failing_nodes_raw, list) or not failing_nodes_raw:
        raise ValueError("off-topic investigation payload must include failing_nodes")

    tasks_by_id = {task.id: task for task in store.get_all() if task.id is not None}
    created: list[Task] = []
    reused: list[Task] = []

    for failing_node_raw in failing_nodes_raw:
        if not isinstance(failing_node_raw, Mapping):
            raise ValueError("failing node evidence must be a mapping")
        signature = _normalize_off_topic_investigation_signature(failing_node_raw)
        signature_key = _off_topic_investigation_signature_key(signature)

        matching_task_ids: list[str] = []
        for task in tasks_by_id.values():
            assert task.id is not None
            for artifact in store.list_artifacts(task.id, kind=OFF_TOPIC_VERIFY_INVESTIGATION_ARTIFACT_KIND):
                metadata = artifact.metadata or {}
                if metadata.get("signature_key") == signature_key:
                    matching_task_ids.append(task.id)
                    break
        matching_tasks = [tasks_by_id[task_id] for task_id in dict.fromkeys(matching_task_ids)]
        reusable_tasks = [
            task for task in matching_tasks if task.status in OFF_TOPIC_VERIFY_INVESTIGATION_REUSABLE_STATUSES
        ]
        if len(reusable_tasks) > 1:
            raise ValueError(
                f"multiple active off-topic investigation tasks already exist for signature {signature['nodeid']}"
            )
        if reusable_tasks:
            reused.append(reusable_tasks[0])
            continue

        prompt = build_off_topic_verify_investigation_prompt(
            review_task_id=review_task.id,
            impl_task_id=impl_task.id,
            signature=signature,
            payload=payload,
        )
        created_task = store.add(
            prompt=prompt,
            task_type="explore",
            based_on=review_task.id,
            depends_on=impl_task.id,
            same_branch=True,
            tags=resolve_derived_task_tags(impl_task),
            review_scope=_format_off_topic_investigation_review_scope(signature=signature, payload=payload),
            trigger_source=trigger_source,
        )
        investigation_payload = {
            "reason": "off_topic_verify_failure",
            "signature_key": signature_key,
            "nodeid": signature["nodeid"],
            "assertion_signature": signature["assertion_signature"],
            "implementation_task_id": impl_task.id,
            "review_task_id": review_task.id,
            "head_sha": payload.get("head_sha"),
            "tree_fingerprint": payload.get("tree_fingerprint"),
            "target_branch": payload.get("target_branch"),
            "target_head_sha": payload.get("target_head_sha"),
            "target_tree_fingerprint": payload.get("target_tree_fingerprint"),
            "baseline_mode": payload.get("baseline_mode"),
            "verify_command": payload.get("verify_command"),
            "failing_node": dict(failing_node_raw),
        }
        store_command_output_artifact(
            store,
            created_task,
            config,
            kind=OFF_TOPIC_VERIFY_INVESTIGATION_ARTIFACT_KIND,
            producer="advance_off_topic_verify_unblock",
            label="off_topic_verify_investigation",
            output=json.dumps(investigation_payload, indent=2, sort_keys=True),
            status="queued",
            head_sha=payload.get("head_sha") if isinstance(payload.get("head_sha"), str) else None,
            metadata=investigation_payload,
            content_type="application/json; charset=utf-8",
        )
        created.append(created_task)
        assert created_task.id is not None
        tasks_by_id[created_task.id] = created_task

    return tuple(created), tuple(reused)


def persist_off_topic_verify_clearance(
    store: SqliteTaskStore,
    *,
    config: Config,
    review_task: Task,
    impl_task: Task,
    payload: Mapping[str, Any],
    trigger_source: str,
    review_clearance_artifact_kind: str,
    review_clearance_artifact_label: str,
    review_clearance_artifact_producer: str,
) -> OffTopicVerifyClearancePersistenceResult:
    """Persist investigation tasks and review clearance as one fail-closed transaction."""
    if review_task.id is None:
        raise ValueError("Cannot persist off-topic clearance for review without an ID.")
    if impl_task.id is None:
        raise ValueError("Cannot persist off-topic clearance for implementation without an ID.")

    failing_nodes_raw = payload.get("failing_nodes")
    if not isinstance(failing_nodes_raw, list) or not failing_nodes_raw:
        raise ValueError("off-topic investigation payload must include failing_nodes")

    tasks_by_id = {task.id: task for task in store.get_all() if task.id is not None}
    created: list[Task] = []
    reused: list[Task] = []
    prepared_paths: list[Path] = []
    conn = cast(Any, store._connect())
    try:
        conn.execute("BEGIN")
        cleared_at = datetime.now(UTC)
        clearance_payload = dict(payload)

        for failing_node_raw in failing_nodes_raw:
            if not isinstance(failing_node_raw, Mapping):
                raise ValueError("failing node evidence must be a mapping")
            signature = _normalize_off_topic_investigation_signature(failing_node_raw)
            signature_key = _off_topic_investigation_signature_key(signature)

            matching_task_ids: list[str] = []
            for task in tasks_by_id.values():
                assert task.id is not None
                for artifact in store.list_artifacts(task.id, kind=OFF_TOPIC_VERIFY_INVESTIGATION_ARTIFACT_KIND):
                    metadata = artifact.metadata or {}
                    if metadata.get("signature_key") == signature_key:
                        matching_task_ids.append(task.id)
                        break
            matching_tasks = [tasks_by_id[task_id] for task_id in dict.fromkeys(matching_task_ids)]
            reusable_tasks = [
                task
                for task in matching_tasks
                if task.status in OFF_TOPIC_VERIFY_INVESTIGATION_REUSABLE_STATUSES
            ]
            if len(reusable_tasks) > 1:
                raise ValueError(
                    f"multiple active off-topic investigation tasks already exist for signature {signature['nodeid']}"
                )
            if reusable_tasks:
                reused.append(reusable_tasks[0])
                continue

            prompt = build_off_topic_verify_investigation_prompt(
                review_task_id=review_task.id,
                impl_task_id=impl_task.id,
                signature=signature,
                payload=payload,
            )
            created_task_id = store._next_id(conn)
            created_task = store._add_task_conn(
                conn,
                NewTaskParams(
                    prompt=prompt,
                    task_id=created_task_id,
                    task_type="explore",
                    based_on=review_task.id,
                    depends_on=impl_task.id,
                    same_branch=True,
                    tags=resolve_derived_task_tags(impl_task),
                    review_scope=_format_off_topic_investigation_review_scope(
                        signature=signature,
                        payload=payload,
                    ),
                    trigger_source=trigger_source,
                ),
            )
            assert created_task.id is not None
            investigation_payload = {
                "reason": "off_topic_verify_failure",
                "signature_key": signature_key,
                "nodeid": signature["nodeid"],
                "assertion_signature": signature["assertion_signature"],
                "implementation_task_id": impl_task.id,
                "review_task_id": review_task.id,
                "head_sha": payload.get("head_sha"),
                "tree_fingerprint": payload.get("tree_fingerprint"),
                "target_branch": payload.get("target_branch"),
                "target_head_sha": payload.get("target_head_sha"),
                "target_tree_fingerprint": payload.get("target_tree_fingerprint"),
                "baseline_mode": payload.get("baseline_mode"),
                "verify_command": payload.get("verify_command"),
                "failing_node": dict(failing_node_raw),
            }
            prepared_investigation = prepare_command_output_artifact(
                Path(config.project_dir),
                created_task.id,
                label="off_topic_verify_investigation",
                output=json.dumps(investigation_payload, indent=2, sort_keys=True),
                created_at=cleared_at,
            )
            prepared_paths.append(prepared_investigation.absolute_path)
            store._add_artifact_conn(
                conn,
                created_task.id,
                kind=OFF_TOPIC_VERIFY_INVESTIGATION_ARTIFACT_KIND,
                producer=review_clearance_artifact_producer,
                label="off_topic_verify_investigation",
                path=prepared_investigation.path,
                content_type="application/json; charset=utf-8",
                byte_size=prepared_investigation.bytes,
                sha256=prepared_investigation.digest,
                created_at=cleared_at,
                status="queued",
                head_sha=payload.get("head_sha") if isinstance(payload.get("head_sha"), str) else None,
                metadata=investigation_payload,
            )
            created.append(created_task)
            tasks_by_id[created_task.id] = created_task

        clearance_payload["created_investigation_task_ids"] = [task.id for task in created]
        clearance_payload["reused_investigation_task_ids"] = [task.id for task in reused]
        prepared_clearance = prepare_command_output_artifact(
            Path(config.project_dir),
            impl_task.id,
            label=review_clearance_artifact_label,
            output=json.dumps(clearance_payload, indent=2, sort_keys=True),
            created_at=cleared_at,
        )
        prepared_paths.append(prepared_clearance.absolute_path)
        store._set_review_cleared_at_conn(conn, impl_task.id, cleared_at)
        store._add_artifact_conn(
            conn,
            impl_task.id,
            kind=review_clearance_artifact_kind,
            producer=review_clearance_artifact_producer,
            label=review_clearance_artifact_label,
            path=prepared_clearance.path,
            content_type="application/json; charset=utf-8",
            byte_size=prepared_clearance.bytes,
            sha256=prepared_clearance.digest,
            created_at=cleared_at,
            status="cleared",
            head_sha=payload.get("head_sha") if isinstance(payload.get("head_sha"), str) else None,
            metadata={
                "reason": "off_topic_verify_failure",
                "review_task_id": review_task.id,
                "tree_fingerprint": payload.get("tree_fingerprint"),
                "green_task_id": payload.get("green_task_id"),
                "red_task_id": payload.get("red_task_id"),
                "target_branch": payload.get("target_branch"),
                "created_investigation_task_ids": clearance_payload["created_investigation_task_ids"],
                "reused_investigation_task_ids": clearance_payload["reused_investigation_task_ids"],
                "failing_nodes": payload.get("failing_nodes"),
            },
        )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        for prepared_path in reversed(prepared_paths):
            try:
                if prepared_path.exists():
                    prepared_path.unlink()
                parent = prepared_path.parent
                while parent.name and parent.exists() and parent != Path(config.project_dir):
                    if any(parent.iterdir()):
                        break
                    parent.rmdir()
                    parent = parent.parent
            except Exception:
                continue
        raise OffTopicVerifyPersistenceError(
            f"off-topic verify clearance persistence failed: {exc}"
        ) from exc
    finally:
        try:
            conn.close()
        except Exception:
            pass

    impl_task.review_cleared_at = cleared_at
    return OffTopicVerifyClearancePersistenceResult(
        created_tasks=tuple(created),
        reused_tasks=tuple(reused),
        review_cleared_at=cleared_at,
    )


def persist_review_clearance_artifact(
    store: SqliteTaskStore,
    *,
    config: Config,
    impl_task: Task,
    clearance_payload: Mapping[str, Any],
    created_at: datetime,
    review_clearance_artifact_kind: str,
    review_clearance_artifact_label: str,
    review_clearance_artifact_producer: str,
    status: str,
    head_sha: str | None,
    metadata: dict[str, Any] | None,
) -> ReviewClearancePersistenceResult:
    """Persist a structured review-clearance artifact and bind it to the impl row."""
    if impl_task.id is None:
        raise OffTopicVerifyPersistenceError("review clearance persistence requires an implementation task id")

    conn = store._connect()
    prepared_paths: list[Path] = []
    try:
        write_conn = cast(sqlite3.Connection, conn)
        prepared_clearance = prepare_command_output_artifact(
            Path(config.project_dir),
            impl_task.id,
            label=review_clearance_artifact_label,
            output=json.dumps(clearance_payload, indent=2, sort_keys=True),
            created_at=created_at,
        )
        prepared_paths.append(prepared_clearance.absolute_path)
        store._set_review_cleared_at_conn(write_conn, impl_task.id, created_at)
        store._add_artifact_conn(
            write_conn,
            impl_task.id,
            kind=review_clearance_artifact_kind,
            producer=review_clearance_artifact_producer,
            label=review_clearance_artifact_label,
            path=prepared_clearance.path,
            content_type="application/json; charset=utf-8",
            byte_size=prepared_clearance.bytes,
            sha256=prepared_clearance.digest,
            created_at=created_at,
            status=status,
            head_sha=head_sha,
            metadata=metadata,
        )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        for prepared_path in reversed(prepared_paths):
            try:
                if prepared_path.exists():
                    prepared_path.unlink()
                parent = prepared_path.parent
                while parent.name and parent.exists() and parent != Path(config.project_dir):
                    if any(parent.iterdir()):
                        break
                    parent.rmdir()
                    parent = parent.parent
            except Exception:
                continue
        raise OffTopicVerifyPersistenceError(
            f"review clearance persistence failed: {exc}"
        ) from exc
    finally:
        try:
            conn.close()
        except Exception:
            pass

    impl_task.review_cleared_at = created_at
    return ReviewClearancePersistenceResult(review_cleared_at=created_at)


def build_followup_prompt(
    review_task_id: str,
    impl_task_id: str,
    finding: ReviewFinding,
) -> str:
    """Build full prompt for an auto-created follow-up implementation task."""
    prefix = build_followup_prompt_prefix(review_task_id, impl_task_id, finding.id)
    tail = (finding.fix_or_followup or "").strip()
    heading = f"{prefix} {tail}" if tail else prefix
    return f"{heading}\n\n## Follow-up finding to implement:\n\n{format_followup_finding_context(finding)}"


def build_deferred_blocker_prompt(
    review_task_id: str,
    impl_task_id: str,
    finding: ReviewFinding,
) -> str:
    """Build full prompt for an auto-created deferred blocker implementation task."""
    prefix = build_deferred_blocker_prompt_prefix(review_task_id, impl_task_id, finding.id)
    tail = (finding.fix_or_followup or finding.title or "").strip()
    heading = f"{prefix} {tail}" if tail else prefix
    open_state_citation = finding.open_state_citation or "not provided by review"
    canonical_context = format_blocker_finding_context(finding)
    return (
        f"{heading}\n\n"
        "## Deferred blocker to resolve\n\n"
        "This task was created because `gza merge` bypassed a BLOCKER-severity review finding "
        "during a manual merge override attempt.\n\n"
        f"Original implementation: {impl_task_id}\n"
        f"Review: {review_task_id}\n"
        f"Open-state citation: {open_state_citation}\n\n"
        f"{canonical_context}"
    )


def build_review_blocker_adjudication_prompt(
    review_task_id: str,
    impl_task_id: str,
    finding: ReviewFinding,
    dispute_metadata: Mapping[str, Any],
) -> str:
    """Build full prompt for a strict review-blocker adjudication task."""
    prefix = build_review_blocker_adjudication_prompt_prefix(review_task_id, impl_task_id, finding.id)
    tail = (finding.title or finding.id).strip()
    heading = f"{prefix} {tail}" if tail else prefix
    dispute_reason = str(dispute_metadata.get("reason", "")).strip() or "unknown"
    dispute_evidence = str(dispute_metadata.get("evidence", "")).strip() or "not provided"
    current_state_citation = str(dispute_metadata.get("current_state_citation", "")).strip() or "not provided"
    source_task_id = str(dispute_metadata.get("source_task_id", "")).strip() or "unknown"
    source_head_sha = str(dispute_metadata.get("head_sha", "")).strip()
    dispute_artifact_id = _normalize_dispute_artifact_id(
        dispute_metadata.get("disputed_artifact_id")
    )
    scope_citation = str(dispute_metadata.get("scope_citation", "")).strip()
    downstream_task_id = str(dispute_metadata.get("downstream_task_id", "")).strip()
    source_branch = str(dispute_metadata.get("source_branch", "")).strip()

    lines = [
        heading,
        "",
        "Return exactly one non-empty line: VALID, INVALID, or NEEDS_HUMAN.",
        "Do not add explanation, markdown, or code fences.",
        "Do not run tests or propose fixes. Judge only whether the disputed blocker remains a valid current blocker.",
        "",
        "Adjudication question:",
        "- VALID: the review blocker is still current, in scope, and actionable.",
        "- INVALID: the dispute evidence shows the blocker is stale, already satisfied, out of scope, or otherwise not valid.",
        "- NEEDS_HUMAN: the evidence is ambiguous, unsafe, or insufficient.",
        "",
        f"Implementation task: {impl_task_id}",
        f"Review task: {review_task_id}",
        f"Dispute source task: {source_task_id}",
    ]
    if dispute_artifact_id is not None:
        lines.append(f"Dispute artifact id: {dispute_artifact_id}")
    if source_branch:
        lines.append(f"Dispute source branch: {source_branch}")
    if source_head_sha:
        lines.append(f"Dispute source head SHA: {source_head_sha}")
    lines.extend(
        [
            "",
            "## Review blocker under dispute",
            "",
            format_blocker_finding_context(finding),
            "",
            "## Dispute evidence",
            "",
            f"Reason: {dispute_reason}",
            f"Evidence: {dispute_evidence}",
            f"Current-state citation: {current_state_citation}",
        ]
    )
    if scope_citation:
        lines.append(f"Scope citation: {scope_citation}")
    if downstream_task_id:
        lines.append(f"Downstream task: {downstream_task_id}")
    return "\n".join(lines).strip()


def _finding_heading(finding: ReviewFinding) -> str:
    title = f" {finding.title}" if finding.title and finding.title != finding.id else ""
    return f"### {finding.id}{title}"


def _finding_structured_body(
    finding: ReviewFinding,
    *,
    fix_label: str,
    tests_label: str,
) -> str:
    lines: list[str] = []
    if finding.evidence:
        lines.append(f"Evidence: {finding.evidence}")
    if finding.impact:
        lines.append(f"Impact: {finding.impact}")
    if finding.fix_or_followup:
        lines.append(f"{fix_label}: {finding.fix_or_followup}")
    if finding.tests:
        lines.append(f"{tests_label}: {finding.tests}")
    if finding.open_state_citation:
        lines.append(f"Open-state citation: {finding.open_state_citation}")
    return "\n".join(lines)


def format_followup_finding_context(finding: ReviewFinding) -> str:
    """Format canonical finding context for follow-up implementation tasks."""
    if finding.body.strip():
        body = finding.body.strip()
    else:
        body = _finding_structured_body(
            finding,
            fix_label="Recommended follow-up",
            tests_label="Recommended tests",
        )
    return f"{_finding_heading(finding)}\n{body}".strip()


def format_blocker_finding_context(finding: ReviewFinding) -> str:
    """Format canonical finding context for deferred blocker implementation tasks."""
    parts: list[str] = [_finding_heading(finding)]
    if finding.body.strip():
        parts.append(finding.body.strip())
    structured = _finding_structured_body(
        finding,
        fix_label="Required fix",
        tests_label="Required tests",
    )
    if structured:
        parts.append(structured)
    return "\n".join(part for part in parts if part).strip()


def extract_followup_prompt_parts(prompt: str) -> tuple[str, str, str] | None:
    """Return (finding_id, review_task_id, impl_task_id) for follow-up prompts."""
    match = _FOLLOWUP_PROMPT_PREFIX_RE.match(prompt.strip())
    if match is None:
        return None
    return match.group(1), match.group(2), match.group(3)


def extract_deferred_blocker_prompt_parts(prompt: str) -> tuple[str, str, str] | None:
    """Return (finding_id, review_task_id, impl_task_id) for deferred blocker prompts."""
    match = _DEFERRED_BLOCKER_PROMPT_PREFIX_RE.match(prompt.strip())
    if match is None:
        return None
    return match.group(1), match.group(2), match.group(3)


def extract_review_blocker_adjudication_prompt_parts(prompt: str) -> tuple[str, str, str] | None:
    """Return (finding_id, review_task_id, impl_task_id) for adjudication prompts."""
    match = _REVIEW_BLOCKER_ADJUDICATION_PROMPT_PREFIX_RE.match(prompt.strip())
    if match is None:
        return None
    return match.group(1), match.group(2), match.group(3)

def extract_review_blocker_adjudication_dispute_reference(
    prompt: str,
) -> tuple[int | None, str | None, str | None]:
    """Return (artifact_id, source_task_id, head_sha) embedded in an adjudication prompt."""
    artifact_id_match = _DISPUTE_ARTIFACT_ID_RE.search(prompt)
    source_task_match = _REVIEW_BLOCKER_ADJUDICATION_SOURCE_TASK_RE.search(prompt)
    head_sha_match = _REVIEW_BLOCKER_ADJUDICATION_HEAD_SHA_RE.search(prompt)
    artifact_id = int(artifact_id_match.group(1)) if artifact_id_match is not None else None
    source_task_id = source_task_match.group(1) if source_task_match is not None else None
    head_sha = head_sha_match.group(1) if head_sha_match is not None else None
    return artifact_id, source_task_id, head_sha


def extract_review_blocker_adjudication_dispute_identity(
    prompt: str,
) -> tuple[str | None, str | None]:
    """Return (source_task_id, head_sha) embedded in an adjudication prompt."""
    _, source_task_id, head_sha = extract_review_blocker_adjudication_dispute_reference(prompt)
    return source_task_id, head_sha


def extract_review_blocker_adjudication_dispute_metadata(
    prompt: str,
) -> dict[str, Any]:
    """Return structured dispute metadata embedded in an adjudication prompt."""
    artifact_id, source_task_id, head_sha = extract_review_blocker_adjudication_dispute_reference(prompt)
    metadata: dict[str, Any] = {}
    if artifact_id is not None:
        metadata["disputed_artifact_id"] = artifact_id
    if source_task_id is not None:
        metadata["source_task_id"] = source_task_id
        metadata["disputed_source_task_id"] = source_task_id
    if head_sha is not None:
        metadata["head_sha"] = head_sha

    source_branch_match = _REVIEW_BLOCKER_ADJUDICATION_SOURCE_BRANCH_RE.search(prompt)
    if source_branch_match is not None:
        metadata["source_branch"] = source_branch_match.group(1)

    reason_match = _REVIEW_BLOCKER_ADJUDICATION_REASON_RE.search(prompt)
    if reason_match is not None:
        metadata["reason"] = reason_match.group(1).strip()

    evidence_match = _REVIEW_BLOCKER_ADJUDICATION_EVIDENCE_RE.search(prompt)
    if evidence_match is not None:
        metadata["evidence"] = evidence_match.group(1).strip()

    citation_match = _REVIEW_BLOCKER_ADJUDICATION_CURRENT_STATE_CITATION_RE.search(prompt)
    if citation_match is not None:
        metadata["current_state_citation"] = citation_match.group(1).strip()

    return metadata


def _normalize_dispute_artifact_id(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, str) and value.strip().isdigit():
        parsed = int(value.strip())
        return parsed if parsed > 0 else None
    return None


def build_review_blocker_dispute_metadata(dispute_artifact: TaskArtifact) -> dict[str, Any]:
    """Return canonical dispute metadata with a stable reference to the artifact itself."""
    metadata = dict(dispute_artifact.metadata or {})
    metadata["disputed_artifact_id"] = dispute_artifact.id
    source_task_id = metadata.get("source_task_id")
    if isinstance(source_task_id, str) and source_task_id.strip():
        metadata.setdefault("disputed_source_task_id", source_task_id)
    return metadata


def review_blocker_dispute_matches_current(
    *,
    current_dispute_artifact: TaskArtifact | None,
    metadata: Mapping[str, Any] | None = None,
    prompt: str | None = None,
) -> bool:
    """Return whether metadata/prompt references the exact current dispute evidence."""
    if current_dispute_artifact is None:
        return True

    current_artifact_id = current_dispute_artifact.id
    current_source_task_id = None
    current_metadata = current_dispute_artifact.metadata or {}
    source_task_id = current_metadata.get("source_task_id")
    if isinstance(source_task_id, str) and source_task_id.strip():
        current_source_task_id = source_task_id

    candidate_artifact_id = None
    candidate_source_task_id = None
    if metadata is not None:
        candidate_artifact_id = _normalize_dispute_artifact_id(
            metadata.get("disputed_artifact_id")
        )
        disputed_source_task_id = metadata.get("disputed_source_task_id")
        if isinstance(disputed_source_task_id, str) and disputed_source_task_id.strip():
            candidate_source_task_id = disputed_source_task_id
        else:
            candidate_source_task_id = metadata.get("source_task_id")
            if not isinstance(candidate_source_task_id, str) or not candidate_source_task_id.strip():
                candidate_source_task_id = None

    if candidate_artifact_id is None and prompt:
        artifact_match = _DISPUTE_ARTIFACT_ID_RE.search(prompt)
        if artifact_match is not None:
            candidate_artifact_id = int(artifact_match.group(1))
    if candidate_source_task_id is None and prompt:
        source_match = _DISPUTE_SOURCE_TASK_ID_RE.search(prompt)
        if source_match is not None:
            candidate_source_task_id = source_match.group(1)

    if candidate_artifact_id is not None:
        return candidate_artifact_id == current_artifact_id
    if candidate_source_task_id is not None and current_source_task_id is not None:
        return candidate_source_task_id == current_source_task_id
    return False


def review_blocker_dispute_references_match(
    dispute_metadata: Mapping[str, Any],
    *,
    prompt: str,
) -> bool:
    """Return whether prompt metadata references the same dispute as the current metadata."""
    expected_artifact_id = _normalize_dispute_artifact_id(dispute_metadata.get("disputed_artifact_id"))
    expected_source_task_id = dispute_metadata.get("disputed_source_task_id")
    if not isinstance(expected_source_task_id, str) or not expected_source_task_id.strip():
        expected_source_task_id = dispute_metadata.get("source_task_id")
        if not isinstance(expected_source_task_id, str) or not expected_source_task_id.strip():
            expected_source_task_id = None
    expected_head_sha = dispute_metadata.get("head_sha")
    if not isinstance(expected_head_sha, str) or not expected_head_sha.strip():
        expected_head_sha = None

    prompt_artifact_id = None
    artifact_match = _DISPUTE_ARTIFACT_ID_RE.search(prompt)
    if artifact_match is not None:
        prompt_artifact_id = int(artifact_match.group(1))

    prompt_source_task_id = None
    source_match = _DISPUTE_SOURCE_TASK_ID_RE.search(prompt)
    if source_match is not None:
        prompt_source_task_id = source_match.group(1)
    prompt_head_sha = None
    head_sha_match = _REVIEW_BLOCKER_ADJUDICATION_HEAD_SHA_RE.search(prompt)
    if head_sha_match is not None:
        prompt_head_sha = head_sha_match.group(1)

    if expected_artifact_id is not None:
        if prompt_artifact_id != expected_artifact_id:
            return False
        if expected_head_sha is not None:
            return prompt_head_sha == expected_head_sha
        return True
    if expected_source_task_id is not None:
        if prompt_source_task_id != expected_source_task_id:
            return False
        if expected_head_sha is not None:
            return prompt_head_sha == expected_head_sha
        return True
    return False


def find_existing_followup_task(
    store: SqliteTaskStore,
    *,
    review_task_id: str,
    impl_task_id: str,
    finding_id: str,
) -> Task | None:
    """Return an existing auto-created follow-up task for (review, finding), if any."""
    prefix = build_followup_prompt_prefix(review_task_id, impl_task_id, finding_id)
    for child in store.get_based_on_children(review_task_id):
        if child.task_type != "implement":
            continue
        if child.prompt.strip().startswith(prefix):
            return child
    return None


def find_existing_deferred_blocker_task(
    store: SqliteTaskStore,
    *,
    review_task_id: str,
    impl_task_id: str,
    finding_id: str,
) -> Task | None:
    """Return an existing auto-created deferred blocker task for (review, finding), if any."""
    prefix = build_deferred_blocker_prompt_prefix(review_task_id, impl_task_id, finding_id)
    for child in store.get_based_on_children(review_task_id):
        if child.task_type != "implement":
            continue
        if child.prompt.strip().startswith(prefix):
            return child
    return None


def find_existing_review_blocker_adjudication_task(
    store: SqliteTaskStore,
    *,
    review_task_id: str,
    impl_task_id: str,
    finding_id: str,
    dispute_source_task_id: str | None = None,
    dispute_head_sha: str | None = None,
    dispute_metadata: Mapping[str, Any] | None = None,
) -> Task | None:
    """Return an existing adjudication task for (review, finding), if any."""
    prefix = build_review_blocker_adjudication_prompt_prefix(review_task_id, impl_task_id, finding_id)
    matching: list[Task] = []
    for child in store.get_based_on_children(review_task_id):
        if child.task_type != "internal":
            continue
        if child.prompt.strip().startswith(prefix):
            prompt_source_task_id, prompt_head_sha = (
                extract_review_blocker_adjudication_dispute_identity(child.prompt)
            )
            if dispute_source_task_id is not None and prompt_source_task_id != dispute_source_task_id:
                continue
            if dispute_head_sha is not None and prompt_head_sha != dispute_head_sha:
                continue
            matching.append(child)
    if not matching:
        return None
    if dispute_metadata is None:
        return matching[0]
    for child in reversed(matching):
        if _is_reusable_review_blocker_adjudication_task(
            store,
            child,
            dispute_metadata=dispute_metadata,
        ):
            return child
    return None


def _is_reusable_review_blocker_adjudication_task(
    store: SqliteTaskStore,
    existing: Task,
    *,
    dispute_metadata: Mapping[str, Any],
) -> bool:
    """Return whether an existing adjudication task still applies to the current dispute."""

    if existing.status in {"pending", "in_progress"}:
        return review_blocker_dispute_references_match(dispute_metadata, prompt=existing.prompt)
    if existing.status not in {"completed", "failed"}:
        return False

    return review_blocker_dispute_references_match(dispute_metadata, prompt=existing.prompt)


def create_or_reuse_followup_task(
    store: SqliteTaskStore,
    *,
    review_task: Task,
    impl_task: Task,
    finding: ReviewFinding,
    trigger_source: str,
) -> tuple[Task, bool]:
    """Create or reuse an idempotent follow-up task for a parsed FOLLOWUP finding.

    Returns:
        (task, created_now) where created_now is True only when a new row was created.
    """
    if review_task.id is None:
        raise ValueError("Cannot create follow-up for review without an ID.")
    if impl_task.id is None:
        raise ValueError("Cannot create follow-up for implementation without an ID.")

    existing = find_existing_followup_task(
        store,
        review_task_id=review_task.id,
        impl_task_id=impl_task.id,
        finding_id=finding.id,
    )
    if existing is not None:
        return existing, False

    prompt = build_followup_prompt(
        review_task.id,
        impl_task.id,
        finding,
    )
    created = store.add(
        prompt=prompt,
        task_type="implement",
        based_on=review_task.id,
        depends_on=impl_task.id,
        review_scope=format_followup_finding_context(finding),
        tags=resolve_derived_task_tags(impl_task),
        trigger_source=trigger_source,
    )
    return created, True


def create_or_reuse_deferred_blocker_task(
    store: SqliteTaskStore,
    *,
    review_task: Task,
    impl_task: Task,
    finding: ReviewFinding,
    trigger_source: str,
) -> tuple[Task, bool]:
    """Create or reuse an idempotent deferred blocker task for a parsed BLOCKER finding."""
    if review_task.id is None:
        raise ValueError("Cannot create deferred blocker for review without an ID.")
    if impl_task.id is None:
        raise ValueError("Cannot create deferred blocker for implementation without an ID.")

    existing = find_existing_deferred_blocker_task(
        store,
        review_task_id=review_task.id,
        impl_task_id=impl_task.id,
        finding_id=finding.id,
    )
    if existing is not None:
        return existing, False

    prompt = build_deferred_blocker_prompt(
        review_task.id,
        impl_task.id,
        finding,
    )
    created = store.add(
        prompt=prompt,
        task_type="implement",
        based_on=review_task.id,
        depends_on=impl_task.id,
        review_scope=format_blocker_finding_context(finding),
        tags=resolve_derived_task_tags(impl_task),
        trigger_source=trigger_source,
        create_pr=True,
        urgent=True,
    )
    return created, True


def create_or_reuse_review_blocker_adjudication_task(
    store: SqliteTaskStore,
    *,
    review_task: Task,
    impl_task: Task,
    finding: ReviewFinding,
    dispute_metadata: Mapping[str, Any],
    trigger_source: str,
) -> tuple[Task, bool]:
    """Create or reuse an idempotent adjudication task for one disputed blocker."""
    if review_task.id is None:
        raise ValueError("Cannot create adjudication for review without an ID.")
    if impl_task.id is None:
        raise ValueError("Cannot create adjudication for implementation without an ID.")

    existing = find_existing_review_blocker_adjudication_task(
        store,
        review_task_id=review_task.id,
        impl_task_id=impl_task.id,
        finding_id=finding.id,
        dispute_metadata=dispute_metadata,
    )
    if existing is not None:
        return existing, False

    prompt = build_review_blocker_adjudication_prompt(
        review_task.id,
        impl_task.id,
        finding,
        dispute_metadata,
    )
    created = store.add(
        prompt=prompt,
        task_type="internal",
        based_on=review_task.id,
        depends_on=impl_task.id,
        same_branch=True,
        tags=resolve_derived_task_tags(impl_task),
        review_scope=format_blocker_finding_context(finding),
        trigger_source=trigger_source,
        urgent=True,
    )
    return created, True
