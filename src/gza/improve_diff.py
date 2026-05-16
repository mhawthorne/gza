"""Helpers for determining whether a completed improve changed the reviewable diff."""

from __future__ import annotations

from dataclasses import dataclass

from .git import Git, GitError
from .rebase_diff import _aggregate_patch_id, _resolve_ref_quietly


@dataclass(frozen=True)
class ImproveDiffBaseline:
    """Concrete refs captured before an improve attempt starts."""

    branch_tip_before: str | None
    target_at_start: str | None
    recovered: bool = False


@dataclass(frozen=True)
class ImproveDiffResult:
    """Outcome of classifying whether a completed improve changed the tracked diff."""

    changed_diff: bool
    detail: str
    warning: str | None = None


def capture_improve_diff_baseline(
    git: Git,
    *,
    branch: str,
    target: str,
    recovered: bool = False,
) -> ImproveDiffBaseline:
    """Capture the concrete refs needed to compare pre/post improve patch identity."""
    return ImproveDiffBaseline(
        branch_tip_before=_resolve_ref_quietly(git, branch),
        target_at_start=_resolve_ref_quietly(git, target),
        recovered=recovered,
    )


def compute_improve_changed_diff(
    git: Git,
    *,
    baseline: ImproveDiffBaseline,
    branch: str,
) -> ImproveDiffResult:
    """Compare tracked aggregate patch identity before and after a completed improve."""
    if baseline.recovered:
        return ImproveDiffResult(
            changed_diff=True,
            detail="yes (tracked improve diff changed or comparison unavailable)",
            warning="improve diff comparison unavailable for recovered/resumed improve; treating as changed",
        )
    if not baseline.branch_tip_before or not baseline.target_at_start:
        return ImproveDiffResult(
            changed_diff=True,
            detail="yes (tracked improve diff changed or comparison unavailable)",
            warning="improve diff comparison missing pre-improve refs; treating as changed",
        )

    branch_tip_after = git.rev_parse_if_exists(branch)
    if not branch_tip_after:
        return ImproveDiffResult(
            changed_diff=True,
            detail="yes (tracked improve diff changed or comparison unavailable)",
            warning="improve diff comparison missing post-improve refs; treating as changed",
        )

    try:
        pre_patch_id = _aggregate_patch_id(git, baseline.target_at_start, baseline.branch_tip_before)
        post_patch_id = _aggregate_patch_id(git, baseline.target_at_start, branch_tip_after)
    except (GitError, RuntimeError) as exc:
        return ImproveDiffResult(
            changed_diff=True,
            detail="yes (tracked improve diff changed or comparison unavailable)",
            warning=f"improve diff comparison failed: {exc}; treating as changed",
        )

    if pre_patch_id == post_patch_id:
        return ImproveDiffResult(
            changed_diff=False,
            detail="no (no tracked improve changes)",
        )
    return ImproveDiffResult(
        changed_diff=True,
        detail="yes (tracked improve diff changed or comparison unavailable)",
    )
