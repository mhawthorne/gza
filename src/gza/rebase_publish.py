"""Shared post-rebase publication helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .git import Git, GitError
from .rebase_diff import RebaseDiffBaseline


class RebasePublishLogger(Protocol):
    """Minimal logger surface needed for rebased-branch publication."""

    def command(self, message: str) -> None: ...

    def info(self, message: str) -> None: ...

    def error(self, message: str) -> None: ...


@dataclass(frozen=True)
class RebasePublishResult:
    """Outcome metadata for publishing a rebased branch."""

    branch_advanced: bool
    pushed: bool
    local_sha: str
    remote_sha_before_push: str | None


def _short_ref(ref: str | None | object) -> str:
    if isinstance(ref, str) and ref:
        return ref[:12]
    return "unknown"


def publish_rebased_branch(
    git: Git,
    *,
    branch: str,
    baseline: RebaseDiffBaseline | None,
    logger: RebasePublishLogger | None = None,
    remote: str = "origin",
) -> RebasePublishResult:
    """Verify and publish a rebased branch to its remote.

    The helper intentionally owns the post-rebase publication flow so every
    rebase-task completion path verifies the rewritten tip the same way and
    reports push failures consistently.
    """

    remote_ref = f"{remote}/{branch}"
    try:
        local_sha = git.rev_parse(branch)
        remote_sha_before_push = git.rev_parse_if_exists(remote_ref)
    except GitError:
        raise
    except Exception as exc:
        message = f"Failed to resolve rebased branch publication refs for {branch}: {exc}"
        if logger is not None:
            logger.error(message)
        raise GitError(message) from exc
    previous_sha = baseline.old_tip if baseline is not None else None
    branch_advanced = previous_sha is not None and previous_sha != local_sha
    pushed = remote_sha_before_push != local_sha

    if logger is not None:
        if branch_advanced:
            logger.info(
                f"Verified rebase advanced {branch}: {_short_ref(previous_sha)} -> {_short_ref(local_sha)}"
            )
        elif previous_sha is not None:
            logger.info(f"Rebase left {branch} at {_short_ref(local_sha)}; verifying remote publication state.")
        else:
            logger.info(f"Verifying rebased tip for {branch} at {_short_ref(local_sha)}.")

    if previous_sha is not None and previous_sha == local_sha:
        message = (
            f"Rebase did not advance {branch}: "
            f"baseline tip and current tip are both {_short_ref(local_sha)}"
        )
        if logger is not None:
            logger.error(message)
        raise GitError(message)

    if not pushed:
        if logger is not None:
            logger.info(f"Origin already points at {branch} ({_short_ref(local_sha)}).")
        return RebasePublishResult(
            branch_advanced=branch_advanced,
            pushed=False,
            local_sha=local_sha,
            remote_sha_before_push=remote_sha_before_push,
        )

    if logger is not None:
        logger.command(f"Pushing {branch} to {remote} with --force-with-lease...")
    try:
        git.push_force_with_lease(branch, remote=remote)
    except GitError as exc:
        if logger is not None:
            logger.error(f"Error pushing rebased branch '{branch}' to {remote}: {exc}")
        raise
    if logger is not None:
        logger.info(f"✓ Pushed {branch}")
    return RebasePublishResult(
        branch_advanced=branch_advanced,
        pushed=True,
        local_sha=local_sha,
        remote_sha_before_push=remote_sha_before_push,
    )
