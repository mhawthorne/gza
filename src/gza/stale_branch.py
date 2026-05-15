"""Stale-branch classification shared by lifecycle planning surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import yaml

from gza.config import (
    DEFAULT_RECOMMEND_REBASE_BEHIND_COMMITS,
    DEFAULT_REVIEW_VERIFY_TIMEOUT_SECONDS,
)
from gza.db import SqliteTaskStore, Task as DbTask

StaleBranchReason = Literal["verify_duration", "behind_target", "both"]


@dataclass(frozen=True)
class BranchStaleness:
    """Operator-facing stale-branch recommendation evidence."""

    recommend_rebase: bool
    reason: StaleBranchReason | None
    verify_duration_seconds: float | None
    review_verify_timeout_seconds: int
    behind_count: int | None
    behind_threshold: int
    target_branch: str
    source_ref: str | None
    evidence_task_id: str | None
    warning: str | None = None


@dataclass(frozen=True)
class _FixLedgerEvidence:
    task_id: str | None
    verify_duration_seconds: float | None
    review_verify_timeout_seconds: int
    recommend_rebase: bool
    recommend_rebase_reasons: frozenset[str]
    behind_count: int | None
    behind_threshold: int | None
    target_branch: str | None
    source_ref: str | None


def _read_fix_ledger_text(task: DbTask, *, project_dir: Path) -> str | None:
    if isinstance(task.output_content, str) and task.output_content.strip():
        return task.output_content
    if not isinstance(task.report_file, str) or not task.report_file.strip():
        return None
    path = project_dir / task.report_file
    try:
        return path.read_text()
    except OSError:
        return None


def _strip_origin_comment(text: str) -> str:
    stripped = text.lstrip()
    if stripped.startswith("<!--"):
        end = stripped.find("-->")
        if end != -1:
            return stripped[end + 3 :].lstrip()
    return text


def _coerce_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _coerce_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def parse_fix_ledger_evidence(
    task: DbTask,
    *,
    project_dir: Path,
    current_review_verify_timeout_seconds: int,
) -> _FixLedgerEvidence | None:
    """Return structured stale-branch evidence from a completed fix ledger."""
    raw_text = _read_fix_ledger_text(task, project_dir=project_dir)
    if not raw_text:
        return None

    try:
        parsed = yaml.safe_load(_strip_origin_comment(raw_text))
    except yaml.YAMLError:
        return None
    if not isinstance(parsed, dict):
        return None

    if parsed.get("fix_result") not in {"repaired_pending_review", "diagnosed_no_change"}:
        return None

    verify_block = parsed.get("verify")
    duration_seconds: float | None = None
    timeout_seconds = current_review_verify_timeout_seconds
    if isinstance(verify_block, dict) and verify_block.get("passed") is True:
        duration_seconds = _coerce_float(verify_block.get("duration_seconds"))
        verify_timeout_seconds = _coerce_int(verify_block.get("review_verify_timeout_seconds"))
        if verify_timeout_seconds is not None and verify_timeout_seconds >= 1:
            timeout_seconds = verify_timeout_seconds

    recommend_rebase_block = parsed.get("recommend_rebase")
    recommend_rebase = False
    recommend_rebase_reasons: frozenset[str] = frozenset()
    behind_count: int | None = None
    behind_threshold: int | None = None
    target_branch: str | None = None
    source_ref: str | None = None
    if isinstance(recommend_rebase_block, dict):
        recommend_rebase = recommend_rebase_block.get("recommended") is True
        raw_reasons = recommend_rebase_block.get("reasons")
        if isinstance(raw_reasons, list):
            recommend_rebase_reasons = frozenset(
                reason for reason in raw_reasons if isinstance(reason, str) and reason
            )
        if duration_seconds is None:
            duration_seconds = _coerce_float(recommend_rebase_block.get("verify_duration_seconds"))
        recommend_timeout_seconds = _coerce_int(
            recommend_rebase_block.get("review_verify_timeout_seconds")
        )
        if recommend_timeout_seconds is not None and recommend_timeout_seconds >= 1:
            timeout_seconds = recommend_timeout_seconds
        behind_count = _coerce_int(recommend_rebase_block.get("behind_count"))
        behind_threshold = _coerce_int(recommend_rebase_block.get("behind_threshold"))
        raw_target_branch = recommend_rebase_block.get("target_branch")
        if isinstance(raw_target_branch, str) and raw_target_branch:
            target_branch = raw_target_branch
        raw_source_ref = recommend_rebase_block.get("source_ref")
        if isinstance(raw_source_ref, str) and raw_source_ref:
            source_ref = raw_source_ref

    return _FixLedgerEvidence(
        task_id=task.id,
        verify_duration_seconds=duration_seconds,
        review_verify_timeout_seconds=timeout_seconds,
        recommend_rebase=recommend_rebase,
        recommend_rebase_reasons=recommend_rebase_reasons,
        behind_count=behind_count,
        behind_threshold=behind_threshold,
        target_branch=target_branch,
        source_ref=source_ref,
    )


def latest_fix_ledger_evidence(
    store: SqliteTaskStore,
    implementation_task: DbTask,
    *,
    project_dir: Path,
    current_review_verify_timeout_seconds: int,
) -> _FixLedgerEvidence | None:
    """Return the newest completed fix-ledger evidence for an implementation lineage."""
    if implementation_task.id is None:
        return None

    fix_children = [
        child
        for child in store.get_lineage_children(implementation_task.id)
        if child.task_type == "fix" and child.status == "completed"
    ]
    if implementation_task.branch:
        fix_children = [
            child for child in fix_children if child.branch in {None, "", implementation_task.branch}
        ]
    fix_children.sort(
        key=lambda task: (
            task.completed_at or task.created_at,
            task.id or "",
        ),
        reverse=True,
    )
    if not fix_children:
        return None
    return parse_fix_ledger_evidence(
        fix_children[0],
        project_dir=project_dir,
        current_review_verify_timeout_seconds=current_review_verify_timeout_seconds,
    )


def resolve_same_branch_implementation_anchor(store: SqliteTaskStore, task: DbTask) -> DbTask:
    """Walk same-branch based_on ancestors back to the implementation anchor."""
    current = task
    seen: set[str] = set()
    while current.id is not None and current.id not in seen and current.based_on is not None:
        seen.add(current.id)
        parent = store.get(current.based_on)
        if parent is None:
            break
        if current.task_type not in {"improve", "fix", "rebase"}:
            break
        current = parent
        if current.task_type == "implement":
            break
    return current


def resolve_branch_staleness(
    *,
    config: object,
    store: SqliteTaskStore,
    git: object,
    task: DbTask,
    target_branch: str,
    source_ref: str | None,
) -> BranchStaleness | None:
    """Classify whether the lineage should recommend a manual rebase."""
    review_timeout = int(
        getattr(config, "review_verify_timeout_seconds", DEFAULT_REVIEW_VERIFY_TIMEOUT_SECONDS)
    )
    behind_threshold = int(
        getattr(config, "recommend_rebase_behind_commits", DEFAULT_RECOMMEND_REBASE_BEHIND_COMMITS)
    )

    anchor_task = resolve_same_branch_implementation_anchor(store, task)

    fix_evidence = latest_fix_ledger_evidence(
        store,
        anchor_task,
        project_dir=Path(getattr(config, "project_dir", ".")),
        current_review_verify_timeout_seconds=review_timeout,
    )
    verify_duration_seconds = (
        fix_evidence.verify_duration_seconds if fix_evidence is not None else None
    )
    evidence_task_id = fix_evidence.task_id if fix_evidence is not None else None

    verify_timeout_seconds = (
        fix_evidence.review_verify_timeout_seconds if fix_evidence is not None else review_timeout
    )
    verify_duration_trigger = (
        verify_duration_seconds is not None
        and verify_duration_seconds >= verify_timeout_seconds
    )
    ledger_behind_count = fix_evidence.behind_count if fix_evidence is not None else None
    ledger_behind_threshold = fix_evidence.behind_threshold if fix_evidence is not None else None
    ledger_behind_target_trigger = (
        fix_evidence is not None
        and fix_evidence.recommend_rebase
        and "branch_behind_target" in fix_evidence.recommend_rebase_reasons
        and isinstance(ledger_behind_count, int)
        and not isinstance(ledger_behind_count, bool)
        and isinstance(ledger_behind_threshold, int)
        and not isinstance(ledger_behind_threshold, bool)
        and ledger_behind_threshold >= 0
        and ledger_behind_count >= ledger_behind_threshold
    )

    behind_count: int | None = None
    behind_target_trigger = False
    warning: str | None = None
    count_commits_behind = getattr(git, "count_commits_behind", None)
    if (
        behind_threshold > 0
        and source_ref
        and callable(count_commits_behind)
    ):
        try:
            behind_count = count_commits_behind(source_ref, target_branch)
        except Exception as exc:
            warning = (
                "stale-branch behind count unavailable for "
                f"{source_ref} vs {target_branch}: {exc}"
            )
            behind_count = None
        if not isinstance(behind_count, int) or isinstance(behind_count, bool):
            behind_count = None
        behind_target_trigger = behind_count is not None and behind_count >= behind_threshold
    if behind_count is None and ledger_behind_target_trigger:
        behind_count = ledger_behind_count
        behind_target_trigger = True

    if not verify_duration_trigger and not behind_target_trigger:
        if warning is None:
            return None
        return BranchStaleness(
            recommend_rebase=False,
            reason=None,
            verify_duration_seconds=verify_duration_seconds,
            review_verify_timeout_seconds=verify_timeout_seconds,
            behind_count=None,
            behind_threshold=behind_threshold,
            target_branch=target_branch,
            source_ref=source_ref,
            evidence_task_id=evidence_task_id,
            warning=warning,
        )

    if verify_duration_trigger and behind_target_trigger:
        reason: StaleBranchReason | None = "both"
    elif verify_duration_trigger:
        reason = "verify_duration"
    else:
        reason = "behind_target"

    return BranchStaleness(
        recommend_rebase=True,
        reason=reason,
        verify_duration_seconds=verify_duration_seconds,
        review_verify_timeout_seconds=(
            fix_evidence.review_verify_timeout_seconds if fix_evidence is not None else review_timeout
        ),
        behind_count=behind_count,
        behind_threshold=behind_threshold,
        target_branch=target_branch,
        source_ref=source_ref,
        evidence_task_id=evidence_task_id,
        warning=warning,
    )
