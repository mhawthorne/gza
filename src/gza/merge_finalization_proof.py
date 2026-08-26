"""Durable proof for replaying post-promotion merge finalization."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any, Literal

from .db import SqliteTaskStore, Task, TaskArtifact
from .review_verdict import ReviewFinding

MERGE_FINALIZATION_PROOF_ARTIFACT_KIND = "merge_finalization_attempt_proof"
MERGE_FINALIZATION_PREPARED_ARTIFACT_KIND = "merge_finalization_prepared_attempt"
MERGE_FINALIZATION_PROOF_SCHEMA_VERSION = 1
MERGE_FINALIZATION_PROOF_LABEL = "merge_finalization_attempt_proof"
MERGE_FINALIZATION_PREPARED_LABEL = "merge_finalization_prepared_attempt"

MergeFinalizationFamily = Literal["ordinary_followup", "max_cycles_deferred"]
MergeFinalizationPromotionKind = Literal["normal", "squash"]


@dataclass(frozen=True)
class MergeFinalizationProof:
    """Parsed durable proof for one post-promotion finalization attempt."""

    artifact: TaskArtifact
    action_family: MergeFinalizationFamily
    impl_task_id: str
    review_task_id: str
    finding_ids: tuple[str, ...]
    child_task_ids: tuple[str, ...]
    source_branch: str
    source_ref: str
    source_ref_sha: str
    target_branch: str
    previous_target_sha: str
    promoted_target_sha: str
    merge_unit_id: str | None
    promotion_kind: MergeFinalizationPromotionKind = "normal"
    promoted_target_tree_sha: str | None = None


@dataclass(frozen=True)
class MergeFinalizationPreparedAttempt:
    """Parsed durable marker for a prepared merge finalization attempt."""

    artifact: TaskArtifact
    action_family: MergeFinalizationFamily
    impl_task_id: str
    review_task_id: str
    finding_ids: tuple[str, ...]
    child_task_ids: tuple[str, ...]
    source_branch: str
    source_ref: str
    source_ref_sha: str
    target_branch: str
    previous_target_sha: str
    merge_unit_id: str | None
    promotion_observed: bool = False
    promotion_kind: MergeFinalizationPromotionKind = "normal"


def merge_finalization_finding_ids(findings: Iterable[ReviewFinding]) -> tuple[str, ...]:
    return tuple(finding.id for finding in findings)


def merge_finalization_child_task_ids(tasks: Iterable[Task]) -> tuple[str, ...]:
    return tuple(task.id for task in tasks if task.id is not None)


def merge_finalization_attempt_digest(
    *,
    action_family: MergeFinalizationFamily,
    impl_task_id: str,
    review_task_id: str,
    finding_ids: tuple[str, ...],
    child_task_ids: tuple[str, ...],
    source_branch: str,
    source_ref: str,
    source_ref_sha: str,
    target_branch: str,
    previous_target_sha: str,
    promoted_target_sha: str,
    promotion_kind: MergeFinalizationPromotionKind = "normal",
    promoted_target_tree_sha: str | None = None,
    merge_unit_id: str | None,
) -> str:
    payload = {
        "action_family": action_family,
        "child_task_ids": list(child_task_ids),
        "finding_ids": list(finding_ids),
        "impl_task_id": impl_task_id,
        "merge_unit_id": merge_unit_id,
        "previous_target_sha": previous_target_sha,
        "promoted_target_tree_sha": promoted_target_tree_sha,
        "promoted_target_sha": promoted_target_sha,
        "promotion_kind": promotion_kind,
        "review_task_id": review_task_id,
        "schema_version": MERGE_FINALIZATION_PROOF_SCHEMA_VERSION,
        "source_branch": source_branch,
        "source_ref": source_ref,
        "source_ref_sha": source_ref_sha,
        "target_branch": target_branch,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256(encoded).hexdigest()


def _legacy_merge_finalization_attempt_digest(
    *,
    action_family: MergeFinalizationFamily,
    impl_task_id: str,
    review_task_id: str,
    finding_ids: tuple[str, ...],
    child_task_ids: tuple[str, ...],
    source_branch: str,
    source_ref: str,
    source_ref_sha: str,
    target_branch: str,
    previous_target_sha: str,
    promoted_target_sha: str,
    merge_unit_id: str | None,
) -> str:
    payload = {
        "action_family": action_family,
        "child_task_ids": list(child_task_ids),
        "finding_ids": list(finding_ids),
        "impl_task_id": impl_task_id,
        "merge_unit_id": merge_unit_id,
        "previous_target_sha": previous_target_sha,
        "promoted_target_sha": promoted_target_sha,
        "review_task_id": review_task_id,
        "schema_version": MERGE_FINALIZATION_PROOF_SCHEMA_VERSION,
        "source_branch": source_branch,
        "source_ref": source_ref,
        "source_ref_sha": source_ref_sha,
        "target_branch": target_branch,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256(encoded).hexdigest()


def merge_finalization_prepared_attempt_digest(
    *,
    action_family: MergeFinalizationFamily,
    impl_task_id: str,
    review_task_id: str,
    finding_ids: tuple[str, ...],
    child_task_ids: tuple[str, ...],
    source_branch: str,
    source_ref: str,
    source_ref_sha: str,
    target_branch: str,
    previous_target_sha: str,
    merge_unit_id: str | None,
    promotion_observed: bool = False,
    promotion_kind: MergeFinalizationPromotionKind = "normal",
) -> str:
    payload = {
        "action_family": action_family,
        "child_task_ids": list(child_task_ids),
        "finding_ids": list(finding_ids),
        "impl_task_id": impl_task_id,
        "merge_unit_id": merge_unit_id,
        "previous_target_sha": previous_target_sha,
        "promotion_observed": promotion_observed,
        "promotion_kind": promotion_kind,
        "review_task_id": review_task_id,
        "schema_version": MERGE_FINALIZATION_PROOF_SCHEMA_VERSION,
        "source_branch": source_branch,
        "source_ref": source_ref,
        "source_ref_sha": source_ref_sha,
        "target_branch": target_branch,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256(encoded).hexdigest()


def _legacy_merge_finalization_prepared_attempt_digest(
    *,
    action_family: MergeFinalizationFamily,
    impl_task_id: str,
    review_task_id: str,
    finding_ids: tuple[str, ...],
    child_task_ids: tuple[str, ...],
    source_branch: str,
    source_ref: str,
    source_ref_sha: str,
    target_branch: str,
    previous_target_sha: str,
    merge_unit_id: str | None,
    promotion_observed: bool = False,
) -> str:
    payload = {
        "action_family": action_family,
        "child_task_ids": list(child_task_ids),
        "finding_ids": list(finding_ids),
        "impl_task_id": impl_task_id,
        "merge_unit_id": merge_unit_id,
        "previous_target_sha": previous_target_sha,
        "promotion_observed": promotion_observed,
        "review_task_id": review_task_id,
        "schema_version": MERGE_FINALIZATION_PROOF_SCHEMA_VERSION,
        "source_branch": source_branch,
        "source_ref": source_ref,
        "source_ref_sha": source_ref_sha,
        "target_branch": target_branch,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256(encoded).hexdigest()


def _metadata_tuple(metadata: Mapping[str, Any], key: str) -> tuple[str, ...] | None:
    raw = metadata.get(key)
    if not isinstance(raw, list) or not all(isinstance(value, str) and value for value in raw):
        return None
    return tuple(raw)


def _parse_proof_artifact(artifact: TaskArtifact) -> MergeFinalizationProof | None:
    metadata = artifact.metadata
    if not isinstance(metadata, dict):
        return None
    if metadata.get("schema_version") != MERGE_FINALIZATION_PROOF_SCHEMA_VERSION:
        return None
    family = metadata.get("action_family")
    if family not in {"ordinary_followup", "max_cycles_deferred"}:
        return None
    string_fields: dict[str, str] = {}
    for key in (
        "impl_task_id",
        "review_task_id",
        "source_branch",
        "source_ref",
        "source_ref_sha",
        "target_branch",
        "previous_target_sha",
        "promoted_target_sha",
        "attempt_digest",
    ):
        value = metadata.get(key)
        if not isinstance(value, str) or not value:
            return None
        string_fields[key] = value
    finding_ids = _metadata_tuple(metadata, "finding_ids")
    child_task_ids = _metadata_tuple(metadata, "child_task_ids")
    if finding_ids is None or child_task_ids is None:
        return None
    merge_unit_id = metadata.get("merge_unit_id")
    if merge_unit_id is not None and (not isinstance(merge_unit_id, str) or not merge_unit_id):
        return None
    promotion_kind = metadata.get("promotion_kind", "normal")
    if promotion_kind not in {"normal", "squash"}:
        return None
    promoted_target_tree_sha = metadata.get("promoted_target_tree_sha")
    if promoted_target_tree_sha is not None and (
        not isinstance(promoted_target_tree_sha, str) or not promoted_target_tree_sha
    ):
        return None
    if promotion_kind == "squash" and promoted_target_tree_sha is None:
        return None
    expected_digest = merge_finalization_attempt_digest(
        action_family=family,
        impl_task_id=string_fields["impl_task_id"],
        review_task_id=string_fields["review_task_id"],
        finding_ids=finding_ids,
        child_task_ids=child_task_ids,
        source_branch=string_fields["source_branch"],
        source_ref=string_fields["source_ref"],
        source_ref_sha=string_fields["source_ref_sha"],
        target_branch=string_fields["target_branch"],
        previous_target_sha=string_fields["previous_target_sha"],
        promoted_target_sha=string_fields["promoted_target_sha"],
        promotion_kind=promotion_kind,
        promoted_target_tree_sha=promoted_target_tree_sha,
        merge_unit_id=merge_unit_id,
    )
    if string_fields["attempt_digest"] != expected_digest or artifact.sha256 != expected_digest:
        legacy_digest = _legacy_merge_finalization_attempt_digest(
            action_family=family,
            impl_task_id=string_fields["impl_task_id"],
            review_task_id=string_fields["review_task_id"],
            finding_ids=finding_ids,
            child_task_ids=child_task_ids,
            source_branch=string_fields["source_branch"],
            source_ref=string_fields["source_ref"],
            source_ref_sha=string_fields["source_ref_sha"],
            target_branch=string_fields["target_branch"],
            previous_target_sha=string_fields["previous_target_sha"],
            promoted_target_sha=string_fields["promoted_target_sha"],
            merge_unit_id=merge_unit_id,
        )
        if promotion_kind != "normal" or string_fields["attempt_digest"] != legacy_digest or artifact.sha256 != legacy_digest:
            return None
    return MergeFinalizationProof(
        artifact=artifact,
        action_family=family,
        impl_task_id=string_fields["impl_task_id"],
        review_task_id=string_fields["review_task_id"],
        finding_ids=finding_ids,
        child_task_ids=child_task_ids,
        source_branch=string_fields["source_branch"],
        source_ref=string_fields["source_ref"],
        source_ref_sha=string_fields["source_ref_sha"],
        target_branch=string_fields["target_branch"],
        previous_target_sha=string_fields["previous_target_sha"],
        promoted_target_sha=string_fields["promoted_target_sha"],
        promotion_kind=promotion_kind,
        promoted_target_tree_sha=promoted_target_tree_sha,
        merge_unit_id=merge_unit_id,
    )


def _parse_prepared_artifact(artifact: TaskArtifact) -> MergeFinalizationPreparedAttempt | None:
    metadata = artifact.metadata
    if not isinstance(metadata, dict):
        return None
    if metadata.get("schema_version") != MERGE_FINALIZATION_PROOF_SCHEMA_VERSION:
        return None
    family = metadata.get("action_family")
    if family not in {"ordinary_followup", "max_cycles_deferred"}:
        return None
    string_fields: dict[str, str] = {}
    for key in (
        "impl_task_id",
        "review_task_id",
        "source_branch",
        "source_ref",
        "source_ref_sha",
        "target_branch",
        "previous_target_sha",
        "attempt_digest",
    ):
        value = metadata.get(key)
        if not isinstance(value, str) or not value:
            return None
        string_fields[key] = value
    finding_ids = _metadata_tuple(metadata, "finding_ids")
    child_task_ids = _metadata_tuple(metadata, "child_task_ids")
    if finding_ids is None or child_task_ids is None:
        return None
    merge_unit_id = metadata.get("merge_unit_id")
    if merge_unit_id is not None and (not isinstance(merge_unit_id, str) or not merge_unit_id):
        return None
    promotion_kind = metadata.get("promotion_kind", "normal")
    if promotion_kind not in {"normal", "squash"}:
        return None
    expected_digest = merge_finalization_prepared_attempt_digest(
        action_family=family,
        impl_task_id=string_fields["impl_task_id"],
        review_task_id=string_fields["review_task_id"],
        finding_ids=finding_ids,
        child_task_ids=child_task_ids,
        source_branch=string_fields["source_branch"],
        source_ref=string_fields["source_ref"],
        source_ref_sha=string_fields["source_ref_sha"],
        target_branch=string_fields["target_branch"],
        previous_target_sha=string_fields["previous_target_sha"],
        merge_unit_id=merge_unit_id,
        promotion_observed=bool(metadata.get("promotion_observed")),
        promotion_kind=promotion_kind,
    )
    if string_fields["attempt_digest"] != expected_digest or artifact.sha256 != expected_digest:
        legacy_digest = _legacy_merge_finalization_prepared_attempt_digest(
            action_family=family,
            impl_task_id=string_fields["impl_task_id"],
            review_task_id=string_fields["review_task_id"],
            finding_ids=finding_ids,
            child_task_ids=child_task_ids,
            source_branch=string_fields["source_branch"],
            source_ref=string_fields["source_ref"],
            source_ref_sha=string_fields["source_ref_sha"],
            target_branch=string_fields["target_branch"],
            previous_target_sha=string_fields["previous_target_sha"],
            merge_unit_id=merge_unit_id,
            promotion_observed=bool(metadata.get("promotion_observed")),
        )
        if promotion_kind != "normal" or string_fields["attempt_digest"] != legacy_digest or artifact.sha256 != legacy_digest:
            return None
    return MergeFinalizationPreparedAttempt(
        artifact=artifact,
        action_family=family,
        impl_task_id=string_fields["impl_task_id"],
        review_task_id=string_fields["review_task_id"],
        finding_ids=finding_ids,
        child_task_ids=child_task_ids,
        source_branch=string_fields["source_branch"],
        source_ref=string_fields["source_ref"],
        source_ref_sha=string_fields["source_ref_sha"],
        target_branch=string_fields["target_branch"],
        previous_target_sha=string_fields["previous_target_sha"],
        merge_unit_id=merge_unit_id,
        promotion_observed=bool(metadata.get("promotion_observed")),
        promotion_kind=promotion_kind,
    )


def persist_merge_finalization_prepared_attempt(
    store: SqliteTaskStore,
    *,
    action_family: MergeFinalizationFamily,
    impl_task_id: str,
    review_task_id: str,
    finding_ids: tuple[str, ...],
    child_task_ids: tuple[str, ...],
    source_branch: str,
    source_ref: str,
    source_ref_sha: str,
    target_branch: str,
    previous_target_sha: str,
    merge_unit_id: str | None,
    promotion_observed: bool = False,
    promotion_kind: MergeFinalizationPromotionKind = "normal",
) -> TaskArtifact:
    """Persist the prepared child/source/target identity before target promotion."""
    attempt_digest = merge_finalization_prepared_attempt_digest(
        action_family=action_family,
        impl_task_id=impl_task_id,
        review_task_id=review_task_id,
        finding_ids=finding_ids,
        child_task_ids=child_task_ids,
        source_branch=source_branch,
        source_ref=source_ref,
        source_ref_sha=source_ref_sha,
        target_branch=target_branch,
        previous_target_sha=previous_target_sha,
        merge_unit_id=merge_unit_id,
        promotion_observed=promotion_observed,
        promotion_kind=promotion_kind,
    )
    metadata: dict[str, Any] = {
        "action_family": action_family,
        "attempt_digest": attempt_digest,
        "child_task_ids": list(child_task_ids),
        "finding_ids": list(finding_ids),
        "impl_task_id": impl_task_id,
        "merge_unit_id": merge_unit_id,
        "previous_target_sha": previous_target_sha,
        "promotion_observed": promotion_observed,
        "promotion_kind": promotion_kind,
        "review_task_id": review_task_id,
        "schema_version": MERGE_FINALIZATION_PROOF_SCHEMA_VERSION,
        "source_branch": source_branch,
        "source_ref": source_ref,
        "source_ref_sha": source_ref_sha,
        "target_branch": target_branch,
    }
    return store.add_artifact(
        impl_task_id,
        kind=MERGE_FINALIZATION_PREPARED_ARTIFACT_KIND,
        label=MERGE_FINALIZATION_PREPARED_LABEL,
        path=f".gza/artifacts/{impl_task_id}/merge-finalization-prepared-{attempt_digest}.json",
        content_type="application/json",
        byte_size=0,
        sha256=attempt_digest,
        created_at=datetime.now(UTC),
        producer="merge_finalization",
        status="prepared",
        head_sha=previous_target_sha,
        metadata=metadata,
    )


def persist_merge_finalization_attempt_proof(
    store: SqliteTaskStore,
    *,
    action_family: MergeFinalizationFamily,
    impl_task_id: str,
    review_task_id: str,
    finding_ids: tuple[str, ...],
    child_task_ids: tuple[str, ...],
    source_branch: str,
    source_ref: str,
    source_ref_sha: str,
    target_branch: str,
    previous_target_sha: str,
    promoted_target_sha: str,
    merge_unit_id: str | None,
    promotion_kind: MergeFinalizationPromotionKind = "normal",
    promoted_target_tree_sha: str | None = None,
) -> TaskArtifact:
    """Persist the exact promoted transition authorized for merge finalization replay."""
    attempt_digest = merge_finalization_attempt_digest(
        action_family=action_family,
        impl_task_id=impl_task_id,
        review_task_id=review_task_id,
        finding_ids=finding_ids,
        child_task_ids=child_task_ids,
        source_branch=source_branch,
        source_ref=source_ref,
        source_ref_sha=source_ref_sha,
        target_branch=target_branch,
        previous_target_sha=previous_target_sha,
        promoted_target_sha=promoted_target_sha,
        promotion_kind=promotion_kind,
        promoted_target_tree_sha=promoted_target_tree_sha,
        merge_unit_id=merge_unit_id,
    )
    metadata: dict[str, Any] = {
        "action_family": action_family,
        "attempt_digest": attempt_digest,
        "child_task_ids": list(child_task_ids),
        "finding_ids": list(finding_ids),
        "impl_task_id": impl_task_id,
        "merge_unit_id": merge_unit_id,
        "previous_target_sha": previous_target_sha,
        "promoted_target_tree_sha": promoted_target_tree_sha,
        "promoted_target_sha": promoted_target_sha,
        "promotion_kind": promotion_kind,
        "review_task_id": review_task_id,
        "schema_version": MERGE_FINALIZATION_PROOF_SCHEMA_VERSION,
        "source_branch": source_branch,
        "source_ref": source_ref,
        "source_ref_sha": source_ref_sha,
        "target_branch": target_branch,
    }
    return store.add_artifact(
        impl_task_id,
        kind=MERGE_FINALIZATION_PROOF_ARTIFACT_KIND,
        label=MERGE_FINALIZATION_PROOF_LABEL,
        path=f".gza/artifacts/{impl_task_id}/merge-finalization-{attempt_digest}.json",
        content_type="application/json",
        byte_size=0,
        sha256=attempt_digest,
        created_at=datetime.now(UTC),
        producer="merge_finalization",
        status="promoted",
        head_sha=promoted_target_sha,
        metadata=metadata,
    )


def matching_merge_finalization_prepared_attempts(
    store: SqliteTaskStore,
    *,
    action_family: MergeFinalizationFamily,
    impl_task_id: str,
    review_task_id: str,
    finding_ids: tuple[str, ...],
    child_task_ids: tuple[str, ...],
    target_branch: str,
    merge_unit_id: str | None,
) -> tuple[MergeFinalizationPreparedAttempt, ...]:
    """Return prepared attempts exactly matching this replay identity."""
    attempts: list[MergeFinalizationPreparedAttempt] = []
    for artifact in store.list_artifacts(impl_task_id, kind=MERGE_FINALIZATION_PREPARED_ARTIFACT_KIND):
        prepared = _parse_prepared_artifact(artifact)
        if prepared is None:
            continue
        if prepared.action_family != action_family:
            continue
        if prepared.impl_task_id != impl_task_id or prepared.review_task_id != review_task_id:
            continue
        if prepared.finding_ids != finding_ids or prepared.child_task_ids != child_task_ids:
            continue
        if prepared.target_branch != target_branch:
            continue
        if merge_unit_id is not None and prepared.merge_unit_id != merge_unit_id:
            continue
        attempts.append(prepared)
    return tuple(attempts)


def merge_finalization_prepared_attempts_for_review(
    store: SqliteTaskStore,
    *,
    action_family: MergeFinalizationFamily,
    impl_task_id: str,
    review_task_id: str,
    target_branch: str,
    merge_unit_id: str | None,
) -> tuple[MergeFinalizationPreparedAttempt, ...]:
    """Return prepared attempts for this review and target, regardless of child identity."""
    attempts: list[MergeFinalizationPreparedAttempt] = []
    for artifact in store.list_artifacts(impl_task_id, kind=MERGE_FINALIZATION_PREPARED_ARTIFACT_KIND):
        prepared = _parse_prepared_artifact(artifact)
        if prepared is None:
            continue
        if prepared.action_family != action_family:
            continue
        if prepared.impl_task_id != impl_task_id or prepared.review_task_id != review_task_id:
            continue
        if prepared.target_branch != target_branch:
            continue
        if merge_unit_id is not None and prepared.merge_unit_id != merge_unit_id:
            continue
        attempts.append(prepared)
    return tuple(attempts)


def matching_merge_finalization_proofs(
    store: SqliteTaskStore,
    *,
    action_family: MergeFinalizationFamily,
    impl_task_id: str,
    review_task_id: str,
    finding_ids: tuple[str, ...],
    child_task_ids: tuple[str, ...],
    target_branch: str,
    live_target_sha: str | None,
    merge_unit_id: str | None,
) -> tuple[MergeFinalizationProof, ...]:
    """Return durable proofs exactly matching this replay identity and live target."""
    if not isinstance(live_target_sha, str) or not live_target_sha:
        return ()
    proofs: list[MergeFinalizationProof] = []
    for artifact in store.list_artifacts(impl_task_id, kind=MERGE_FINALIZATION_PROOF_ARTIFACT_KIND):
        proof = _parse_proof_artifact(artifact)
        if proof is None:
            continue
        if proof.action_family != action_family:
            continue
        if proof.impl_task_id != impl_task_id or proof.review_task_id != review_task_id:
            continue
        if proof.finding_ids != finding_ids or proof.child_task_ids != child_task_ids:
            continue
        if proof.target_branch != target_branch:
            continue
        if merge_unit_id is not None and proof.merge_unit_id != merge_unit_id:
            continue
        if proof.promoted_target_sha != live_target_sha:
            continue
        proofs.append(proof)
    return tuple(proofs)


def get_merge_finalization_proof(
    store: SqliteTaskStore,
    *,
    artifact_id: int,
    impl_task_id: str,
) -> MergeFinalizationProof | None:
    """Re-read one durable merge-finalization proof by immutable artifact id."""
    artifact = store.get_artifact(artifact_id, task_id=impl_task_id)
    if artifact is None:
        return None
    return _parse_proof_artifact(artifact)


def merge_finalization_proofs_for_live_attempt(
    store: SqliteTaskStore,
    *,
    action_family: MergeFinalizationFamily,
    impl_task_id: str,
    review_task_id: str,
    target_branch: str,
    live_target_sha: str | None,
    merge_unit_id: str | None,
) -> tuple[MergeFinalizationProof, ...]:
    """Return proofs for this review only when they promoted the exact live target."""
    if not isinstance(live_target_sha, str) or not live_target_sha:
        return ()
    proofs: list[MergeFinalizationProof] = []
    for artifact in store.list_artifacts(impl_task_id, kind=MERGE_FINALIZATION_PROOF_ARTIFACT_KIND):
        proof = _parse_proof_artifact(artifact)
        if proof is None:
            continue
        if proof.action_family != action_family:
            continue
        if proof.impl_task_id != impl_task_id or proof.review_task_id != review_task_id:
            continue
        if proof.target_branch != target_branch:
            continue
        if merge_unit_id is not None and proof.merge_unit_id != merge_unit_id:
            continue
        if proof.promoted_target_sha != live_target_sha:
            continue
        proofs.append(proof)
    return tuple(proofs)
