"""Helpers for determining whether a completed rebase changed the implementation diff."""

from __future__ import annotations

from dataclasses import dataclass

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


def resolution_delta_provenance_is_complete(provenance: RebaseDiffProvenance) -> bool:
    """Return whether persisted provenance retains the irrecoverable pre-rebase proof."""
    if provenance.recovered:
        return False
    return all(
        (
            provenance.old_tip,
            provenance.merge_base_at_start,
        )
    )


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
