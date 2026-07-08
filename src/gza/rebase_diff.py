"""Helpers for determining whether a completed rebase changed the implementation diff."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .git import Git, GitError


@dataclass(frozen=True)
class RebaseDiffBaseline:
    """Concrete refs captured before a rebase attempt starts."""

    old_tip: str | None
    target_at_start: str | None
    merge_base_at_start: str | None
    recovered: bool = False


@dataclass(frozen=True)
class RebaseDiffResult:
    """Outcome of classifying whether a rebase changed the reviewed diff."""

    changed_diff: bool
    detail: str
    warning: str | None = None


@dataclass(frozen=True)
class RebaseDiffProvenance:
    """Persisted provenance needed to reconstruct a resolution-review delta."""

    old_tip: str | None
    target_at_start: str | None
    merge_base_at_start: str | None
    resolved_head_sha: str | None
    resolved_target_sha: str | None
    recovered: bool = False


@dataclass(frozen=True)
class ResolutionDeltaContext:
    """Focused review context for a changed rebase."""

    available: bool
    summary: str
    before_range: str | None = None
    after_range: str | None = None
    range_diff: str | None = None


@dataclass(frozen=True)
class ReflogEntry:
    """Parsed reflog entry for one ref update."""

    old_oid: str
    new_oid: str
    timestamp: int
    message: str


_REFLOG_LINE_RE = re.compile(
    r"^(?P<old>[0-9a-f]{40}) (?P<new>[0-9a-f]{40}) .* (?P<timestamp>\d+) (?P<tz>[+-]\d{4})\t(?P<message>.*)$"
)
_REFLOG_REBASE_TARGET_RE = re.compile(r"onto (?P<target>[0-9a-f]{40})$")


def resolution_delta_provenance_is_complete(provenance: RebaseDiffProvenance) -> bool:
    """Return whether persisted provenance retains the irrecoverable pre-rebase proof."""
    if provenance.recovered:
        return False
    return all(
        (
            provenance.old_tip,
            provenance.target_at_start,
            provenance.merge_base_at_start,
        )
    )


def rebase_diff_provenance_quality(provenance: RebaseDiffProvenance | None) -> tuple[int, int, int]:
    """Rank persisted rebase provenance so stale writes cannot replace better proof."""
    if provenance is None:
        return (0, 0, 0)
    full_resolution_proof = int(
        resolution_delta_provenance_is_complete(provenance)
        and bool(provenance.resolved_head_sha)
        and bool(provenance.resolved_target_sha)
    )
    irrecoverable_pre_rebase_proof = int(resolution_delta_provenance_is_complete(provenance))
    populated_fields = sum(
        1
        for value in (
            provenance.old_tip,
            provenance.target_at_start,
            provenance.merge_base_at_start,
            provenance.resolved_head_sha,
            provenance.resolved_target_sha,
        )
        if value
    )
    return (full_resolution_proof, irrecoverable_pre_rebase_proof, populated_fields)


def capture_rebase_diff_baseline(
    git: Git,
    *,
    branch: str,
    target: str,
    recovered: bool = False,
) -> RebaseDiffBaseline:
    """Capture the concrete refs needed to compare pre/post rebase patch identity."""
    old_tip = _resolve_ref_quietly(git, branch)
    target_at_start = _resolve_ref_quietly(git, target)
    merge_base_at_start: str | None = None
    if old_tip and target_at_start:
        merge_base_at_start = _merge_base(git, old_tip, target_at_start)
    return RebaseDiffBaseline(
        old_tip=old_tip,
        target_at_start=target_at_start,
        merge_base_at_start=merge_base_at_start,
        recovered=recovered,
    )


def restore_rebase_diff_baseline(
    git: Git,
    *,
    old_tip: str | None,
    target_at_start: str | None,
    recovered: bool = False,
) -> RebaseDiffBaseline:
    """Rebuild a diff baseline from previously persisted refs."""
    merge_base_at_start: str | None = None
    if old_tip and target_at_start:
        merge_base_at_start = _merge_base(git, old_tip, target_at_start)
    return RebaseDiffBaseline(
        old_tip=old_tip,
        target_at_start=target_at_start,
        merge_base_at_start=merge_base_at_start,
        recovered=recovered,
    )


def compute_rebase_changed_diff(
    git: Git,
    *,
    baseline: RebaseDiffBaseline,
    branch: str,
    target: str,
) -> RebaseDiffResult:
    """Compare aggregate patch identity before and after a completed rebase."""
    if baseline.recovered:
        return RebaseDiffResult(
            changed_diff=True,
            detail="yes (review must be refreshed)",
            warning="rebase diff comparison unavailable for recovered/resumed rebase; treating as changed",
        )
    if not baseline.old_tip or not baseline.target_at_start or not baseline.merge_base_at_start:
        return RebaseDiffResult(
            changed_diff=True,
            detail="yes (review must be refreshed)",
            warning="rebase diff comparison missing pre-rebase refs; treating as changed",
        )

    new_tip = git.rev_parse_if_exists(branch)
    target_at_completion = git.rev_parse_if_exists(target)
    if not new_tip or not target_at_completion:
        return RebaseDiffResult(
            changed_diff=True,
            detail="yes (review must be refreshed)",
            warning="rebase diff comparison missing post-rebase refs; treating as changed",
        )

    try:
        pre_patch_id = _aggregate_patch_id(git, baseline.merge_base_at_start, baseline.old_tip)
        post_patch_id = _aggregate_patch_id(git, target_at_completion, new_tip)
    except (GitError, RuntimeError) as exc:
        return RebaseDiffResult(
            changed_diff=True,
            detail="yes (review must be refreshed)",
            warning=f"rebase diff comparison failed: {exc}; treating as changed",
        )

    if pre_patch_id == post_patch_id:
        return RebaseDiffResult(
            changed_diff=False,
            detail="no (review can be preserved)",
        )
    return RebaseDiffResult(
        changed_diff=True,
        detail="yes (review must be refreshed)",
    )


def build_rebase_diff_provenance(
    *,
    baseline: RebaseDiffBaseline,
    resolved_head_sha: str | None,
    resolved_target_sha: str | None,
) -> str:
    """Serialize the refs needed for later resolution-delta review context."""
    return "\n".join(
        (
            "Rebase diff provenance: yes",
            f"Pre-rebase head SHA: {baseline.old_tip or ''}",
            f"Pre-rebase target SHA: {baseline.target_at_start or ''}",
            f"Pre-rebase merge-base SHA: {baseline.merge_base_at_start or ''}",
            f"Resolved head SHA: {resolved_head_sha or ''}",
            f"Resolved target SHA: {resolved_target_sha or ''}",
            f"Recovered baseline: {'yes' if baseline.recovered else 'no'}",
        )
    )


def parse_rebase_diff_provenance(text: str | None) -> RebaseDiffProvenance | None:
    """Parse persisted rebase diff provenance from task metadata text."""
    if text is None or "Rebase diff provenance: yes" not in text:
        return None
    fields: dict[str, str] = {}
    for raw_line in text.splitlines():
        if ":" not in raw_line:
            continue
        key, value = raw_line.split(":", 1)
        fields[key.strip().lower()] = value.strip()
    return RebaseDiffProvenance(
        old_tip=fields.get("pre-rebase head sha") or None,
        target_at_start=fields.get("pre-rebase target sha") or None,
        merge_base_at_start=fields.get("pre-rebase merge-base sha") or None,
        resolved_head_sha=fields.get("resolved head sha") or None,
        resolved_target_sha=fields.get("resolved target sha") or None,
        recovered=(fields.get("recovered baseline") or "").lower() == "yes",
    )


def recover_rebase_diff_provenance(
    git: Git,
    *,
    branch: str,
    target_branch: str,
    completed_at: datetime | None,
    review_scope: str | None = None,
) -> RebaseDiffProvenance | None:
    """Recover missing rebase provenance from local reflogs and persisted hints."""
    existing = parse_rebase_diff_provenance(review_scope)
    repo_dir = getattr(git, "repo_dir", None)
    if not isinstance(repo_dir, Path):
        return existing
    branch_entries = _read_reflog_entries(_reflog_path(git, branch))
    if not branch_entries:
        return existing

    preferred_head = existing.resolved_head_sha if existing is not None else None
    branch_entry = _select_completed_rebase_entry(
        branch_entries,
        completed_at=completed_at,
        preferred_new_oid=preferred_head,
    )
    if branch_entry is None:
        return existing

    target_entries = _read_reflog_entries(_reflog_path(git, target_branch))
    resolved_target_entry = _select_latest_reflog_entry_at_or_before(target_entries, branch_entry.timestamp)
    target_at_start = _parse_rebase_target_from_message(branch_entry.message)
    if target_at_start is None and resolved_target_entry is not None:
        target_at_start = resolved_target_entry.new_oid

    old_tip = existing.old_tip if existing is not None and existing.old_tip else _normalize_oid(branch_entry.old_oid)
    merge_base_at_start = existing.merge_base_at_start if existing is not None else None
    if merge_base_at_start is None and old_tip and target_at_start:
        merge_base_at_start = _merge_base(git, old_tip, target_at_start)

    resolved_head_sha = (
        existing.resolved_head_sha
        if existing is not None and existing.resolved_head_sha
        else _normalize_oid(branch_entry.new_oid)
    )
    resolved_target_sha = (
        existing.resolved_target_sha
        if existing is not None and existing.resolved_target_sha
        else resolved_target_entry.new_oid if resolved_target_entry is not None else None
    )
    recovered = existing.recovered if existing is not None else False

    if not any((old_tip, target_at_start, merge_base_at_start, resolved_head_sha, resolved_target_sha)):
        return existing

    return RebaseDiffProvenance(
        old_tip=old_tip,
        target_at_start=target_at_start,
        merge_base_at_start=merge_base_at_start,
        resolved_head_sha=resolved_head_sha,
        resolved_target_sha=resolved_target_sha,
        recovered=recovered,
    )


def compute_resolution_delta_context(
    git: Git,
    *,
    provenance: RebaseDiffProvenance,
) -> ResolutionDeltaContext:
    """Reconstruct focused resolution-review context from persisted rebase provenance."""
    if provenance.recovered:
        return ResolutionDeltaContext(
            available=False,
            summary=(
                "resolution delta unavailable: recovered or resumed rebase provenance cannot "
                "prove the original pre-rebase delta; fail closed and review manually"
            ),
        )
    if not (
        resolution_delta_provenance_is_complete(provenance)
        and provenance.resolved_head_sha
        and provenance.resolved_target_sha
    ):
        return ResolutionDeltaContext(
            available=False,
            summary=(
                "resolution delta unavailable: missing pre/post rebase provenance refs; "
                "fail closed and review manually"
            ),
        )
    before_range = f"{provenance.merge_base_at_start}..{provenance.old_tip}"
    after_range = f"{provenance.resolved_target_sha}..{provenance.resolved_head_sha}"
    result = git._run("range-diff", "--no-color", before_range, after_range, check=False)  # noqa: SLF001
    if result.returncode != 0:
        error_output = (result.stderr or result.stdout or "").strip()
        detail = (
            "resolution delta unavailable: git range-diff failed while reconstructing the "
            "conflict-resolution delta"
        )
        if error_output:
            detail += f" ({error_output})"
        return ResolutionDeltaContext(
            available=False,
            summary=detail,
            before_range=before_range,
            after_range=after_range,
        )
    range_diff = result.stdout.strip()
    if not range_diff:
        range_diff = "(git range-diff produced no textual delta; inspect the reconstructed ranges directly)"
    return ResolutionDeltaContext(
        available=True,
        summary="resolution delta available",
        before_range=before_range,
        after_range=after_range,
        range_diff=range_diff,
    )


def _merge_base(git: Git, left: str, right: str) -> str | None:
    merge_base = getattr(git, "merge_base", None)
    if callable(merge_base):
        try:
            value = merge_base(left, right)
        except Exception:
            value = None
        if value:
            return value
    result = git._run("merge-base", left, right, check=False)  # noqa: SLF001
    if result.returncode != 0:
        return None
    value = result.stdout.strip()
    return value or None


def _resolve_ref_quietly(git: Git, ref: str) -> str | None:
    try:
        return git.rev_parse_if_exists(ref)
    except Exception:
        return None


def _aggregate_patch_id(git: Git, base_ref: str, tip_ref: str) -> str:
    diff = git._run("diff", "--binary", "--find-renames", base_ref, tip_ref, check=False)  # noqa: SLF001
    if diff.returncode != 0:
        raise GitError(f"git diff --binary --find-renames {base_ref} {tip_ref} failed:\n{diff.stderr or diff.stdout}")
    if not diff.stdout.strip():
        return ""

    patch_id = git._run("patch-id", "--stable", check=False, stdin=diff.stdout.encode())  # noqa: SLF001
    if patch_id.returncode != 0:
        raise GitError(f"git patch-id --stable failed for {base_ref}..{tip_ref}:\n{patch_id.stderr or patch_id.stdout}")
    lines = patch_id.stdout.strip().splitlines()
    if len(lines) != 1:
        raise RuntimeError(f"expected one patch-id line for {base_ref}..{tip_ref}, got {len(lines)}")
    return lines[0].split()[0]


def _normalize_oid(value: str | None) -> str | None:
    if not value or value == "0" * 40:
        return None
    return value


def _reflog_path(git: Git, ref: str) -> Path:
    common_dir = _git_common_dir_from_repo(git.repo_dir)
    return common_dir / "logs" / "refs" / "heads" / ref


def _git_common_dir_from_repo(repo_dir: Path) -> Path:
    git_path = repo_dir / ".git"
    if git_path.is_dir():
        return git_path
    if not git_path.is_file():
        return git_path
    git_dir_text = git_path.read_text().strip()
    if not git_dir_text.startswith("gitdir: "):
        return git_path
    raw = git_dir_text[len("gitdir: "):]
    git_dir = Path(raw) if Path(raw).is_absolute() else (repo_dir / raw).resolve()
    commondir_path = git_dir / "commondir"
    if not commondir_path.exists():
        return git_dir
    commondir_raw = commondir_path.read_text().strip()
    common_dir = Path(commondir_raw) if Path(commondir_raw).is_absolute() else (git_dir / commondir_raw).resolve()
    return common_dir


def _read_reflog_entries(path: Path) -> tuple[ReflogEntry, ...]:
    try:
        text = path.read_text()
    except OSError:
        return ()
    entries: list[ReflogEntry] = []
    for line in text.splitlines():
        match = _REFLOG_LINE_RE.match(line)
        if match is None:
            continue
        entries.append(
            ReflogEntry(
                old_oid=match.group("old"),
                new_oid=match.group("new"),
                timestamp=int(match.group("timestamp")),
                message=match.group("message"),
            )
        )
    return tuple(entries)


def _select_completed_rebase_entry(
    entries: tuple[ReflogEntry, ...],
    *,
    completed_at: datetime | None,
    preferred_new_oid: str | None,
) -> ReflogEntry | None:
    if not entries:
        return None
    if preferred_new_oid:
        for entry in reversed(entries):
            if entry.new_oid == preferred_new_oid and _looks_like_rebase_ref_update(entry.message):
                return entry
    target_ts = int(completed_at.timestamp()) if completed_at is not None else None
    if target_ts is None:
        for entry in reversed(entries):
            if _looks_like_rebase_ref_update(entry.message):
                return entry
        return None

    recent_entries = [
        entry for entry in entries if target_ts - 600 <= entry.timestamp <= target_ts + 5
    ]
    for entry in reversed(recent_entries):
        if _looks_like_rebase_ref_update(entry.message):
            return entry
    return None


def _select_latest_reflog_entry_at_or_before(
    entries: tuple[ReflogEntry, ...],
    timestamp: int,
) -> ReflogEntry | None:
    for entry in reversed(entries):
        if entry.timestamp <= timestamp:
            return entry
    return None


def _looks_like_rebase_ref_update(message: str) -> bool:
    normalized = message.strip().lower()
    return normalized.startswith("rebase (")


def _parse_rebase_target_from_message(message: str) -> str | None:
    match = _REFLOG_REBASE_TARGET_RE.search(message.strip())
    if match is None:
        return None
    return match.group("target")
