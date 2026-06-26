"""Canonical checkout invariant checks."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .config import Config
from .git import Git, GitError
from .providers.base import write_ops_event

CANONICAL_CHECKOUT_ATTENTION_REASON = "canonical-main-checkout-hijacked"


@dataclass(frozen=True)
class CanonicalCheckoutStatus:
    """Result of checking whether the configured project checkout stayed on target."""

    state: str
    expected_branch: str
    current_branch: str | None = None
    dirty_tracked_paths: tuple[str, ...] = ()
    restored: bool = False
    message: str | None = None

    @property
    def needs_attention(self) -> bool:
        return self.state == "needs_attention"


def _tracked_dirty_paths(git: Git) -> tuple[str, ...]:
    paths = sorted(path for status, path in git.status_porcelain() if status != "??")
    return tuple(paths)


def check_canonical_checkout_invariant(
    config: Config,
    *,
    expected_branch: str,
    action: str,
    task_id: str | None = None,
    ops_log_file: Path | None = None,
    canonical_git: Git | None = None,
) -> CanonicalCheckoutStatus:
    """Verify and, when safe, restore the configured canonical checkout branch."""

    if ops_log_file is not None:
        ops_log_file.parent.mkdir(parents=True, exist_ok=True)

    git = canonical_git if canonical_git is not None else Git(config.project_dir)
    try:
        current_branch = git.current_branch()
    except (GitError, OSError, ValueError) as exc:
        status = CanonicalCheckoutStatus(
            state="git_error",
            expected_branch=expected_branch,
            message=str(exc),
        )
        write_ops_event(
            ops_log_file,
            subtype=CANONICAL_CHECKOUT_ATTENTION_REASON,
            message=f"Canonical checkout invariant could not be checked during {action}: {exc}",
            action=action,
            task_id=task_id,
            expected_branch=expected_branch,
            state=status.state,
        )
        return status
    if not isinstance(current_branch, str) or not current_branch:
        status = CanonicalCheckoutStatus(
            state="git_error",
            expected_branch=expected_branch,
            message="unable to determine current canonical checkout branch",
        )
        write_ops_event(
            ops_log_file,
            subtype=CANONICAL_CHECKOUT_ATTENTION_REASON,
            message=(
                "Canonical checkout invariant could not be checked during "
                f"{action}: current branch was unavailable"
            ),
            action=action,
            task_id=task_id,
            expected_branch=expected_branch,
            state=status.state,
        )
        return status

    if current_branch == expected_branch:
        return CanonicalCheckoutStatus(
            state="ok",
            expected_branch=expected_branch,
            current_branch=current_branch,
        )

    try:
        dirty_paths = _tracked_dirty_paths(git)
    except (GitError, OSError, ValueError) as exc:
        status = CanonicalCheckoutStatus(
            state="needs_attention",
            expected_branch=expected_branch,
            current_branch=current_branch,
            message=str(exc),
        )
        write_ops_event(
            ops_log_file,
            subtype=CANONICAL_CHECKOUT_ATTENTION_REASON,
            message=(
                f"Canonical checkout moved from {expected_branch} to {current_branch} "
                f"during {action}; dirty state could not be inspected."
            ),
            action=action,
            task_id=task_id,
            expected_branch=expected_branch,
            current_branch=current_branch,
            state=status.state,
            restoration_attempted=False,
            dirty_inspection_error=str(exc),
        )
        return status

    if dirty_paths:
        status = CanonicalCheckoutStatus(
            state="needs_attention",
            expected_branch=expected_branch,
            current_branch=current_branch,
            dirty_tracked_paths=dirty_paths,
            message="canonical checkout has tracked changes; leaving it untouched",
        )
        write_ops_event(
            ops_log_file,
            subtype=CANONICAL_CHECKOUT_ATTENTION_REASON,
            message=(
                f"Canonical checkout moved from {expected_branch} to {current_branch} "
                f"during {action}; tracked changes require operator attention."
            ),
            action=action,
            task_id=task_id,
            expected_branch=expected_branch,
            current_branch=current_branch,
            state=status.state,
            restoration_attempted=False,
            dirty_tracked_paths=list(dirty_paths),
        )
        return status

    try:
        git.checkout(expected_branch)
    except (GitError, OSError, ValueError) as exc:
        status = CanonicalCheckoutStatus(
            state="needs_attention",
            expected_branch=expected_branch,
            current_branch=current_branch,
            message=str(exc),
        )
        write_ops_event(
            ops_log_file,
            subtype=CANONICAL_CHECKOUT_ATTENTION_REASON,
            message=(
                f"Canonical checkout moved from {expected_branch} to {current_branch} "
                f"during {action}; restoration failed."
            ),
            action=action,
            task_id=task_id,
            expected_branch=expected_branch,
            current_branch=current_branch,
            state=status.state,
            restoration_attempted=True,
            restoration_error=str(exc),
        )
        return status

    status = CanonicalCheckoutStatus(
        state="restored",
        expected_branch=expected_branch,
        current_branch=current_branch,
        restored=True,
        message=f"restored canonical checkout to {expected_branch}",
    )
    write_ops_event(
        ops_log_file,
        subtype=CANONICAL_CHECKOUT_ATTENTION_REASON,
        message=(
            f"Canonical checkout moved from {expected_branch} to {current_branch} "
            f"during {action}; restored to {expected_branch}."
        ),
        action=action,
        task_id=task_id,
        expected_branch=expected_branch,
        current_branch=current_branch,
        state=status.state,
        restoration_attempted=True,
        restored=True,
    )
    return status
