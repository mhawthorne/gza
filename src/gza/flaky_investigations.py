"""Create or reuse non-blocking flaky verify investigation tasks."""

from __future__ import annotations

import importlib.util
import json
import shlex
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .db import SqliteTaskStore, Task, task_id_numeric_key
from .derived_tags import resolve_derived_task_tags
from .off_topic_verify import build_local_target_pytest_command, parse_review_verify_failure_set

if TYPE_CHECKING:
    from collections.abc import Callable

    from .off_topic_verify import FailingNode, PytestPassFailCounts, PytestXdistMetadata
    from .runner import ReviewVerifyResult

FLAKY_VERIFY_INVESTIGATION_ARTIFACT_KIND = "flaky_verify_investigation"
FLAKY_VERIFY_ATTEMPT_ARTIFACT_KIND = "flaky_verify_attempt"
FLAKY_VERIFY_INCONCLUSIVE_ARTIFACT_KIND = "flaky_verify_inconclusive"
FLAKY_VERIFY_INVESTIGATION_SCHEMA_VERSION = 1
FLAKY_VERIFY_ATTEMPT_SCHEMA_VERSION = 1
FLAKY_VERIFY_INCONCLUSIVE_SCHEMA_VERSION = 1
DEFAULT_FLAKY_REPRO_RUNS = 20
MAX_FLAKY_REPRO_RUNS = 20
DEFAULT_RANDOMIZATION_SEED = 1729


@dataclass(frozen=True)
class FlakyInvestigationEvidence:
    """Structured evidence for one off-topic verify failure signature."""

    node: FailingNode
    dedup_key: str
    review_task_id: str
    impl_task_id: str
    merge_unit_id: str | None
    reviewed_head_sha: str
    tree_fingerprint: str
    observed_branch: str
    target_branch: str
    verify_command: str
    targeted_command: str | None
    working_directory: str | None
    branch_pass_fail_counts: PytestPassFailCounts
    xdist: PytestXdistMetadata
    branch_verify_status: str | None
    branch_verify_exit_status: str | None


@dataclass(frozen=True)
class FlakyInvestigationUpsert:
    """Created/reused investigation tasks for one clearance event."""

    created: tuple[Task, ...] = ()
    reused: tuple[Task, ...] = ()


@dataclass(frozen=True)
class FlakyReproductionPlan:
    """Bounded targeted stress harness derived from persisted investigation evidence."""

    task_id: str
    dedup_key: str
    nodeid: str
    assertion_signature: str | None
    command: str
    working_directory: Path
    runs: int
    reviewed_head_sha: str
    tree_fingerprint: str
    randomization_plugin: str | None = None
    randomization_seed_base: int | None = None
    xdist_enabled: bool = False
    xdist_worker_count_raw: str | None = None
    xdist_dist_mode: str | None = None


@dataclass(frozen=True)
class FlakyAttemptRecord:
    """Persisted outcome for one reproduce-harness attempt."""

    artifact_id: int
    matched_signature: bool
    status: str
    exit_status: str


@dataclass(frozen=True)
class FlakyReproductionRun:
    """Aggregate outcome from a flaky reproduce command execution."""

    plan: FlakyReproductionPlan
    attempts: tuple[FlakyAttemptRecord, ...]
    reproduced: bool
    inconclusive_artifact_id: int | None = None


def normalize_flaky_investigation_dedup_key(nodeid: str, assertion_signature: str | None) -> str:
    """Return the stable dedup signature for one failing-node identity."""
    normalized_signature = " ".join((assertion_signature or "").split()).strip() or "unknown"
    return f"{nodeid}::{normalized_signature}"


def derive_flaky_targeted_command(*, verify_command: str, nodeids: tuple[str, ...]) -> str | None:
    """Return a bounded targeted pytest command for flaky reproduction or ``None``."""
    command = build_local_target_pytest_command(verify_command, nodeids=nodeids)
    if command:
        return command
    if _looks_like_bin_tests_wrapper(verify_command):
        return shlex.join(["uv", "run", "pytest", *nodeids, "--maxfail=0"])
    return None


def ensure_flaky_investigation_targeted_command(
    evidence: FlakyInvestigationEvidence,
) -> FlakyInvestigationEvidence:
    """Return evidence with a usable targeted command or raise when derivation is unsafe."""
    targeted_command = (evidence.targeted_command or "").strip()
    if not targeted_command:
        targeted_command = derive_flaky_targeted_command(
            verify_command=evidence.verify_command,
            nodeids=(evidence.node.nodeid,),
        ) or ""
    if not targeted_command:
        raise ValueError(
            "flaky investigation evidence cannot produce a bounded targeted pytest command "
            f"for verify_command={evidence.verify_command!r}"
        )
    return replace(evidence, targeted_command=targeted_command)


def build_flaky_investigation_prompt(
    *,
    review_task_id: str,
    impl_task_id: str,
    evidence: FlakyInvestigationEvidence,
) -> str:
    """Build the operator/runner prompt for one investigation task."""
    heading = (
        f"Investigate flaky verify signature {evidence.dedup_key} "
        f"from review {review_task_id} for task {impl_task_id}"
    )
    lines = [
        heading,
        "",
        "Contract: REPRODUCE-OR-RECORD.",
        "First produce red-under-stress evidence on the bounded targeted harness before fixing, unless you can prove a concrete root cause from current code and current state.",
        "After any fix, rerun the same targeted harness and keep working until that exact harness is green.",
        "If the failure does not reproduce within budget, record a structured inconclusive result with the attempts, environment, and hypotheses instead of making a speculative fix.",
        "Use `uv run gza flaky reproduce <this-task-id>` to drive the bounded targeted harness and persist attempt evidence.",
        "Do not default to sleeps, blanket retries, @flaky, or broad timeout increases.",
        "",
        f"Review task: {review_task_id}",
        f"Implementation task: {impl_task_id}",
        f"Dedup key: {evidence.dedup_key}",
        f"Failing node: {evidence.node.nodeid}",
        f"Assertion/failure signature: {evidence.node.assertion_signature or 'unknown'}",
        f"Reviewed head SHA: {evidence.reviewed_head_sha}",
        f"Tree fingerprint: {evidence.tree_fingerprint}",
        f"Observed branch: {evidence.observed_branch}",
        f"Target branch: {evidence.target_branch}",
        f"Verify command: {evidence.verify_command}",
    ]
    if evidence.targeted_command:
        lines.append(f"Targeted command: {evidence.targeted_command}")
    if evidence.working_directory:
        lines.append(f"Working directory: {evidence.working_directory}")
    lines.extend(
        [
            "",
            "Required evidence:",
            "- Red on the targeted stress harness for the same failing-node signature before fixing, or a current-code root-cause proof that makes the failure mechanism explicit.",
            "- Green on the same harness after the fix.",
            "- If no red appears within budget, a structured inconclusive record instead of a speculative patch.",
        ]
    )
    return "\n".join(lines)


def load_latest_flaky_investigation_metadata(
    store: SqliteTaskStore,
    task_id: str,
) -> dict[str, Any]:
    """Return metadata for the newest persisted flaky investigation artifact."""
    artifacts = store.list_artifacts(task_id, kind=FLAKY_VERIFY_INVESTIGATION_ARTIFACT_KIND)
    if not artifacts:
        raise ValueError(f"Task {task_id} has no flaky investigation artifact")
    metadata = artifacts[0].metadata
    if not isinstance(metadata, dict):
        raise ValueError(f"Task {task_id} has flaky investigation artifact without metadata")
    return metadata


def build_flaky_reproduction_plan(
    store: SqliteTaskStore,
    *,
    project_dir: Path,
    task_id: str,
    runs: int = DEFAULT_FLAKY_REPRO_RUNS,
    enable_xdist: bool = True,
    enable_randomization: bool = True,
    randomization_seed_base: int = DEFAULT_RANDOMIZATION_SEED,
) -> FlakyReproductionPlan:
    """Build a bounded targeted reproduce harness from persisted investigation metadata."""
    if runs < 1 or runs > MAX_FLAKY_REPRO_RUNS:
        raise ValueError(
            f"runs must be between 1 and {MAX_FLAKY_REPRO_RUNS}: {runs}"
        )
    metadata = load_latest_flaky_investigation_metadata(store, task_id)
    dedup_key = str(metadata.get("dedup_key") or "").strip()
    reviewed_head_sha = str(metadata.get("reviewed_head_sha") or "").strip()
    tree_fingerprint = str(metadata.get("tree_fingerprint") or "").strip()
    node = metadata.get("failing_node")
    if not isinstance(node, dict):
        raise ValueError(f"Task {task_id} investigation metadata is missing failing_node")
    nodeid = str(node.get("nodeid") or "").strip()
    if not dedup_key or not nodeid or not reviewed_head_sha or not tree_fingerprint:
        raise ValueError(f"Task {task_id} investigation metadata is incomplete")

    verify_command = str(metadata.get("verify_command") or "").strip()
    targeted_command = str(metadata.get("targeted_command") or "").strip()
    if targeted_command:
        command = targeted_command
    else:
        command = derive_flaky_targeted_command(verify_command=verify_command, nodeids=(nodeid,)) or ""
    if not command:
        raise ValueError(f"Task {task_id} investigation metadata cannot produce a targeted pytest command")

    working_directory = _resolve_flaky_working_directory(
        project_dir=project_dir,
        recorded_working_directory=metadata.get("working_directory"),
    )
    xdist_metadata = metadata.get("xdist")
    worker_count_raw: str | None = None
    dist_mode: str | None = None
    if isinstance(xdist_metadata, dict):
        raw_worker_count = xdist_metadata.get("worker_count_raw")
        worker_count_raw = str(raw_worker_count).strip() if raw_worker_count else None
        raw_dist_mode = xdist_metadata.get("dist_mode")
        dist_mode = str(raw_dist_mode).strip() if raw_dist_mode else None
    if enable_xdist and _pytest_plugin_available("xdist"):
        command = _append_xdist_flags(command, worker_count_raw=worker_count_raw, dist_mode=dist_mode)
    else:
        worker_count_raw = None
        dist_mode = None
    randomization_plugin = None
    if enable_randomization:
        randomization_plugin = _available_randomization_plugin()
        if randomization_plugin is not None:
            command = _append_randomization_flags(
                command,
                plugin=randomization_plugin,
                seed=randomization_seed_base,
            )
    command = _prepend_harness_env(command)
    return FlakyReproductionPlan(
        task_id=task_id,
        dedup_key=dedup_key,
        nodeid=nodeid,
        assertion_signature=_normalize_optional_signature(node.get("assertion_signature")),
        command=command,
        working_directory=working_directory,
        runs=runs,
        reviewed_head_sha=reviewed_head_sha,
        tree_fingerprint=tree_fingerprint,
        randomization_plugin=randomization_plugin,
        randomization_seed_base=randomization_seed_base if randomization_plugin is not None else None,
        xdist_enabled=worker_count_raw is not None,
        xdist_worker_count_raw=worker_count_raw,
        xdist_dist_mode=dist_mode,
    )


def run_flaky_reproduction_plan(
    store: SqliteTaskStore,
    *,
    project_dir: Path,
    task_id: str,
    plan: FlakyReproductionPlan,
    timeout_seconds: int,
    timeout_grace_seconds: float,
    hypotheses: tuple[str, ...] = (),
    run_verify_command: Callable[..., ReviewVerifyResult] | None = None,
) -> FlakyReproductionRun:
    """Execute the bounded harness and persist attempt/inconclusive artifacts."""
    from .runner import _run_review_verify_command

    verify_runner = run_verify_command or _run_review_verify_command
    attempts: list[FlakyAttemptRecord] = []
    for attempt_number in range(1, plan.runs + 1):
        command = _command_for_attempt(plan, attempt_number)
        result = verify_runner(
            command,
            cwd=plan.working_directory,
            reviewed_head_sha=plan.reviewed_head_sha,
            timeout_seconds=timeout_seconds,
            timeout_grace_seconds=timeout_grace_seconds,
        )
        matched_signature = _result_matches_flaky_signature(
            result,
            nodeid=plan.nodeid,
            assertion_signature=plan.assertion_signature,
        )
        artifact = persist_flaky_attempt_artifact(
            store,
            project_dir=project_dir,
            task_id=task_id,
            plan=plan,
            attempt_number=attempt_number,
            command=command,
            result=result,
            matched_signature=matched_signature,
        )
        attempts.append(
            FlakyAttemptRecord(
                artifact_id=artifact.id,
                matched_signature=matched_signature,
                status=artifact.status or "",
                exit_status=artifact.exit_status or "",
            )
        )
        if matched_signature:
            return FlakyReproductionRun(
                plan=plan,
                attempts=tuple(attempts),
                reproduced=True,
            )
    inconclusive = persist_flaky_inconclusive_artifact(
        store,
        project_dir=project_dir,
        task_id=task_id,
        plan=plan,
        attempts=tuple(attempts),
        hypotheses=hypotheses,
    )
    return FlakyReproductionRun(
        plan=plan,
        attempts=tuple(attempts),
        reproduced=False,
        inconclusive_artifact_id=inconclusive.id,
    )


def find_reusable_flaky_investigation_task(
    store: SqliteTaskStore,
    *,
    dedup_key: str,
) -> Task | None:
    """Return an open investigation task whose artifact metadata matches the signature."""
    candidates: list[Task] = []
    for task in store.get_all():
        if task.task_type != "explore" or task.id is None:
            continue
        if task.status not in {"pending", "in_progress"}:
            continue
        for artifact in store.list_artifacts(task.id, kind=FLAKY_VERIFY_INVESTIGATION_ARTIFACT_KIND):
            metadata = artifact.metadata or {}
            if metadata.get("dedup_key") == dedup_key:
                candidates.append(task)
                break
    if not candidates:
        return None
    return min(candidates, key=lambda task: task_id_numeric_key(task.id))


def create_or_reuse_flaky_investigations(
    store: SqliteTaskStore,
    *,
    config: Any,
    review_task: Task,
    impl_task: Task,
    evidences: tuple[FlakyInvestigationEvidence, ...],
    trigger_source: str,
) -> FlakyInvestigationUpsert:
    """Create or reuse exactly one open investigation task per dedup signature."""
    if review_task.id is None:
        raise ValueError("review_task.id is required")
    if impl_task.id is None:
        raise ValueError("impl_task.id is required")

    created: list[Task] = []
    reused: list[Task] = []
    for raw_evidence in evidences:
        evidence = ensure_flaky_investigation_targeted_command(raw_evidence)
        existing = find_reusable_flaky_investigation_task(store, dedup_key=evidence.dedup_key)
        if existing is not None:
            _store_flaky_investigation_artifact(
                store,
                project_dir=Path(config.project_dir),
                task=existing,
                review_task=review_task,
                impl_task=impl_task,
                evidence=evidence,
            )
            reused.append(existing)
            continue

        prompt = build_flaky_investigation_prompt(
            review_task_id=review_task.id,
            impl_task_id=impl_task.id,
            evidence=evidence,
        )
        metadata = _build_flaky_investigation_metadata(
            review_task=review_task,
            impl_task=impl_task,
            evidence=evidence,
        )
        artifact_path, artifact_bytes, artifact_digest = _write_flaky_investigation_artifact_file(
            project_dir=Path(config.project_dir),
            dedup_key=evidence.dedup_key,
            artifact_group="records",
            payload=metadata,
            created_at=datetime.now(UTC),
        )
        task = store.add(
            prompt=prompt,
            task_type="explore",
            based_on=review_task.id,
            depends_on=impl_task.id,
            tags=resolve_derived_task_tags(impl_task),
            review_scope=f"Investigate flaky verify signature {evidence.dedup_key}",
            trigger_source=trigger_source,
        )
        assert task.id is not None
        store.add_artifact(
            task.id,
            kind=FLAKY_VERIFY_INVESTIGATION_ARTIFACT_KIND,
            label="flaky_verify_investigation",
            path=artifact_path,
            content_type="application/json",
            byte_size=artifact_bytes,
            sha256=artifact_digest,
            created_at=datetime.now(UTC),
            producer="off_topic_verify_clearance",
            command=evidence.verify_command,
            status="observed",
            exit_status=evidence.branch_verify_exit_status,
            head_sha=evidence.reviewed_head_sha,
            metadata=metadata,
        )
        created.append(task)
    return FlakyInvestigationUpsert(created=tuple(created), reused=tuple(reused))


def _store_flaky_investigation_artifact(
    store: SqliteTaskStore,
    *,
    project_dir: Path,
    task: Task,
    review_task: Task,
    impl_task: Task,
    evidence: FlakyInvestigationEvidence,
) -> None:
    created_at = datetime.now(UTC)
    metadata = _build_flaky_investigation_metadata(
        review_task=review_task,
        impl_task=impl_task,
        evidence=evidence,
    )
    artifact_path, artifact_bytes, artifact_digest = _write_flaky_investigation_artifact_file(
        project_dir=project_dir,
        dedup_key=evidence.dedup_key,
        artifact_group="records",
        payload=metadata,
        created_at=created_at,
    )
    assert task.id is not None
    store.add_artifact(
        task.id,
        kind=FLAKY_VERIFY_INVESTIGATION_ARTIFACT_KIND,
        label="flaky_verify_investigation",
        path=artifact_path,
        content_type="application/json",
        byte_size=artifact_bytes,
        sha256=artifact_digest,
        created_at=created_at,
        producer="off_topic_verify_clearance",
        command=evidence.verify_command,
        status="observed",
        exit_status=evidence.branch_verify_exit_status,
        head_sha=evidence.reviewed_head_sha,
        metadata=metadata,
    )


def _build_flaky_investigation_metadata(
    *,
    review_task: Task,
    impl_task: Task,
    evidence: FlakyInvestigationEvidence,
) -> dict[str, Any]:
    return {
        "schema_version": FLAKY_VERIFY_INVESTIGATION_SCHEMA_VERSION,
        "dedup_key": evidence.dedup_key,
        "review_task_id": review_task.id,
        "impl_task_id": impl_task.id,
        "merge_unit_id": evidence.merge_unit_id,
        "reviewed_head_sha": evidence.reviewed_head_sha,
        "tree_fingerprint": evidence.tree_fingerprint,
        "observed_branch": evidence.observed_branch,
        "target_branch": evidence.target_branch,
        "verify_command": evidence.verify_command,
        "targeted_command": evidence.targeted_command,
        "working_directory": evidence.working_directory,
        "branch_verify_status": evidence.branch_verify_status,
        "branch_verify_exit_status": evidence.branch_verify_exit_status,
        "branch_pass_fail_counts": {
            "failed": evidence.branch_pass_fail_counts.failed,
            "passed": evidence.branch_pass_fail_counts.passed,
            "errors": evidence.branch_pass_fail_counts.errors,
            "skipped": evidence.branch_pass_fail_counts.skipped,
            "xfailed": evidence.branch_pass_fail_counts.xfailed,
            "xpassed": evidence.branch_pass_fail_counts.xpassed,
            "deselected": evidence.branch_pass_fail_counts.deselected,
            "warnings": evidence.branch_pass_fail_counts.warnings,
            "reruns": evidence.branch_pass_fail_counts.reruns,
        },
        "xdist": {
            "enabled": evidence.xdist.enabled,
            "worker_count": evidence.xdist.worker_count,
            "worker_count_raw": evidence.xdist.worker_count_raw,
            "dist_mode": evidence.xdist.dist_mode,
            "plugin_version": evidence.xdist.plugin_version,
            "worker_ids": list(evidence.xdist.worker_ids),
        },
        "failing_node": {
            "nodeid": evidence.node.nodeid,
            "path": evidence.node.path,
            "outcome": evidence.node.outcome,
            "assertion_signature": evidence.node.assertion_signature,
            "failure_path": evidence.node.failure_path,
            "failure_line": evidence.node.failure_line,
            "traceback_paths": list(evidence.node.traceback_paths),
            "trustworthy_attribution": evidence.node.trustworthy_attribution,
        },
    }


def _write_flaky_investigation_artifact_file(
    *,
    project_dir: Path,
    dedup_key: str,
    artifact_group: str,
    payload: dict[str, Any],
    created_at: datetime,
) -> tuple[str, int, str]:
    normalized_key = sha256(dedup_key.encode("utf-8")).hexdigest()[:16]
    timestamp = created_at.astimezone(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    relative_path = (
        Path(".gza")
        / "artifacts"
        / "flaky-investigations"
        / artifact_group
        / f"{normalized_key}-{timestamp}.json"
    )
    absolute_path = project_dir / relative_path
    absolute_path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
    absolute_path.write_bytes(encoded)
    return relative_path.as_posix(), len(encoded), sha256(encoded).hexdigest()


def persist_flaky_attempt_artifact(
    store: SqliteTaskStore,
    *,
    project_dir: Path,
    task_id: str,
    plan: FlakyReproductionPlan,
    attempt_number: int,
    command: str,
    result: ReviewVerifyResult,
    matched_signature: bool,
) -> Any:
    """Persist one reproduce-harness attempt as a task artifact."""
    created_at = datetime.now(UTC)
    parsed = parse_review_verify_failure_set(result)
    payload = {
        "schema_version": FLAKY_VERIFY_ATTEMPT_SCHEMA_VERSION,
        "task_id": task_id,
        "dedup_key": plan.dedup_key,
        "attempt_number": attempt_number,
        "attempt_budget": plan.runs,
        "nodeid": plan.nodeid,
        "assertion_signature": plan.assertion_signature,
        "command": command,
        "working_directory": str(plan.working_directory),
        "matched_signature": matched_signature,
        "reviewed_head_sha": plan.reviewed_head_sha,
        "tree_fingerprint": plan.tree_fingerprint,
        "randomization_plugin": plan.randomization_plugin,
        "randomization_seed": (
            (plan.randomization_seed_base + attempt_number - 1)
            if plan.randomization_seed_base is not None
            else None
        ),
        "xdist_enabled": plan.xdist_enabled,
        "xdist_worker_count_raw": plan.xdist_worker_count_raw,
        "xdist_dist_mode": plan.xdist_dist_mode,
        "result": {
            "status": result.status,
            "exit_status": result.exit_status,
            "failure": result.failure,
            "reviewed_branch": result.reviewed_branch,
            "reviewed_head_sha": result.reviewed_head_sha,
            "working_directory": result.working_directory,
            "pass_fail_counts": {
                "failed": parsed.pass_fail_counts.failed,
                "passed": parsed.pass_fail_counts.passed,
                "errors": parsed.pass_fail_counts.errors,
                "skipped": parsed.pass_fail_counts.skipped,
                "xfailed": parsed.pass_fail_counts.xfailed,
                "xpassed": parsed.pass_fail_counts.xpassed,
                "deselected": parsed.pass_fail_counts.deselected,
                "warnings": parsed.pass_fail_counts.warnings,
                "reruns": parsed.pass_fail_counts.reruns,
            },
            "failing_nodes": [
                {
                    "nodeid": node.nodeid,
                    "assertion_signature": node.assertion_signature,
                    "failure_path": node.failure_path,
                    "failure_line": node.failure_line,
                }
                for node in parsed.failing_nodes
            ],
        },
    }
    artifact_path, artifact_bytes, artifact_digest = _write_flaky_investigation_artifact_file(
        project_dir=project_dir,
        dedup_key=plan.dedup_key,
        artifact_group="attempts",
        payload=payload,
        created_at=created_at,
    )
    return store.add_artifact(
        task_id,
        kind=FLAKY_VERIFY_ATTEMPT_ARTIFACT_KIND,
        label="flaky_verify_attempt",
        path=artifact_path,
        content_type="application/json",
        byte_size=artifact_bytes,
        sha256=artifact_digest,
        created_at=created_at,
        producer="gza flaky reproduce",
        command=command,
        status=result.status,
        exit_status=result.exit_status,
        head_sha=result.reviewed_head_sha,
        metadata=payload,
    )


def persist_flaky_inconclusive_artifact(
    store: SqliteTaskStore,
    *,
    project_dir: Path,
    task_id: str,
    plan: FlakyReproductionPlan,
    attempts: tuple[FlakyAttemptRecord, ...],
    hypotheses: tuple[str, ...] = (),
) -> Any:
    """Persist a structured inconclusive investigation record."""
    created_at = datetime.now(UTC)
    payload = {
        "schema_version": FLAKY_VERIFY_INCONCLUSIVE_SCHEMA_VERSION,
        "task_id": task_id,
        "dedup_key": plan.dedup_key,
        "nodeid": plan.nodeid,
        "assertion_signature": plan.assertion_signature,
        "attempt_count": len(attempts),
        "attempt_budget": plan.runs,
        "reviewed_head_sha": plan.reviewed_head_sha,
        "tree_fingerprint": plan.tree_fingerprint,
        "working_directory": str(plan.working_directory),
        "base_command": plan.command,
        "randomization_plugin": plan.randomization_plugin,
        "randomization_seed_base": plan.randomization_seed_base,
        "xdist_enabled": plan.xdist_enabled,
        "xdist_worker_count_raw": plan.xdist_worker_count_raw,
        "xdist_dist_mode": plan.xdist_dist_mode,
        "attempt_artifact_ids": [attempt.artifact_id for attempt in attempts],
        "attempt_status_counts": _count_attempt_statuses(attempts),
        "matched_signature_attempts": sum(1 for attempt in attempts if attempt.matched_signature),
        "hypotheses": [hypothesis for hypothesis in hypotheses if hypothesis.strip()],
    }
    artifact_path, artifact_bytes, artifact_digest = _write_flaky_investigation_artifact_file(
        project_dir=project_dir,
        dedup_key=plan.dedup_key,
        artifact_group="inconclusive",
        payload=payload,
        created_at=created_at,
    )
    return store.add_artifact(
        task_id,
        kind=FLAKY_VERIFY_INCONCLUSIVE_ARTIFACT_KIND,
        label="flaky_verify_inconclusive",
        path=artifact_path,
        content_type="application/json",
        byte_size=artifact_bytes,
        sha256=artifact_digest,
        created_at=created_at,
        producer="gza flaky reproduce",
        command=plan.command,
        status="inconclusive",
        exit_status=None,
        head_sha=plan.reviewed_head_sha,
        metadata=payload,
    )


def _prepend_harness_env(command: str) -> str:
    tokens = shlex.split(command)
    return shlex.join(["env", "PYTHONFAULTHANDLER=1", *tokens])


def _resolve_flaky_working_directory(*, project_dir: Path, recorded_working_directory: Any) -> Path:
    if isinstance(recorded_working_directory, str) and recorded_working_directory.strip():
        candidate = Path(recorded_working_directory).expanduser()
        if not candidate.is_absolute():
            candidate = (project_dir / candidate).resolve()
        else:
            candidate = candidate.resolve()
        try:
            candidate.relative_to(project_dir.resolve())
        except ValueError as exc:
            raise ValueError(
                f"recorded flaky investigation cwd escapes project root: {recorded_working_directory}"
            ) from exc
        return candidate
    return project_dir.resolve()


def _looks_like_bin_tests_wrapper(command: str) -> bool:
    try:
        tokens = shlex.split(command)
    except ValueError:
        return False
    command_token: str | None = None
    for token in tokens:
        if "=" in token and not token.startswith("-") and token.split("=", 1)[0].isidentifier():
            continue
        command_token = token
        break
    if command_token is None:
        return False
    normalized = command_token.replace("\\", "/")
    return normalized.endswith("/bin/tests") or normalized == "bin/tests"


def _pytest_plugin_available(plugin_name: str) -> bool:
    module_name = "xdist" if plugin_name == "xdist" else plugin_name
    return importlib.util.find_spec(module_name) is not None


def _available_randomization_plugin() -> str | None:
    if importlib.util.find_spec("pytest_randomly") is not None:
        return "pytest-randomly"
    if importlib.util.find_spec("pytest_random_order") is not None:
        return "pytest-random-order"
    return None


def _append_xdist_flags(command: str, *, worker_count_raw: str | None, dist_mode: str | None) -> str:
    if " -n " in f" {command} " or "--numprocesses" in command:
        return command
    tokens = shlex.split(command)
    worker_count = worker_count_raw or "auto"
    tokens.extend(["-n", worker_count])
    if dist_mode and "--dist" not in command:
        tokens.extend(["--dist", dist_mode])
    return shlex.join(tokens)


def _append_randomization_flags(command: str, *, plugin: str, seed: int) -> str:
    tokens = shlex.split(command)
    if plugin == "pytest-randomly":
        if any(token.startswith("--randomly-seed") for token in tokens):
            return command
        tokens.extend([f"--randomly-seed={seed}"])
        return shlex.join(tokens)
    if any(token.startswith("--random-order-seed") for token in tokens):
        return command
    tokens.extend(["--random-order-bucket=global", f"--random-order-seed={seed}"])
    return shlex.join(tokens)


def _command_for_attempt(plan: FlakyReproductionPlan, attempt_number: int) -> str:
    if plan.randomization_plugin is None or plan.randomization_seed_base is None:
        return plan.command
    seed = plan.randomization_seed_base + attempt_number - 1
    if plan.randomization_plugin == "pytest-randomly":
        return _replace_seed_flag(plan.command, "--randomly-seed", seed)
    return _replace_seed_flag(plan.command, "--random-order-seed", seed)


def _replace_seed_flag(command: str, option: str, seed: int) -> str:
    tokens = shlex.split(command)
    updated: list[str] = []
    replaced = False
    for token in tokens:
        if token.startswith(f"{option}="):
            updated.append(f"{option}={seed}")
            replaced = True
        else:
            updated.append(token)
    if not replaced:
        updated.append(f"{option}={seed}")
    return shlex.join(updated)


def _result_matches_flaky_signature(
    result: ReviewVerifyResult,
    *,
    nodeid: str,
    assertion_signature: str | None,
) -> bool:
    parsed = parse_review_verify_failure_set(result)
    if not parsed.available:
        return False
    for node in parsed.failing_nodes:
        if node.nodeid != nodeid:
            continue
        if _normalize_optional_signature(node.assertion_signature) == _normalize_optional_signature(assertion_signature):
            return True
    return False


def _normalize_optional_signature(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.split()).strip()
    return normalized or None


def _count_attempt_statuses(attempts: tuple[FlakyAttemptRecord, ...]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for attempt in attempts:
        key = f"{attempt.status}:{attempt.exit_status}"
        counts[key] = counts.get(key, 0) + 1
    return counts
