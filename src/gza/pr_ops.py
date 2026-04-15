"""Shared pull-request ensure/create flow for task branches."""

from dataclasses import dataclass
from typing import Literal

from .db import SqliteTaskStore, Task
from .git import Git, GitError
from .github import GitHub, GitHubError

PrEnsureStatus = Literal[
    "cached",
    "existing",
    "created",
    "merged",
    "gh_unavailable",
    "push_failed",
    "create_failed",
]


@dataclass(frozen=True)
class EnsureTaskPrResult:
    """Result from ensuring a pull request for a task branch."""

    ok: bool
    status: PrEnsureStatus
    pr_url: str | None = None
    pr_number: int | None = None
    error: str | None = None


def ensure_task_pr(
    task: Task,
    store: SqliteTaskStore,
    git: Git,
    *,
    title: str,
    body: str,
    draft: bool = False,
    merged_behavior: Literal["skip", "error"] = "skip",
) -> EnsureTaskPrResult:
    """Ensure a PR exists for a task branch using the shared decision tree."""
    if not task.branch:
        return EnsureTaskPrResult(ok=False, status="create_failed", error="Task has no branch")

    gh = GitHub()
    if not gh.is_available():
        return EnsureTaskPrResult(ok=False, status="gh_unavailable")

    default_branch = git.default_branch()
    if merged_behavior == "error" and git.is_merged(task.branch, default_branch):
        return EnsureTaskPrResult(ok=False, status="merged", error=default_branch)

    try:
        if git.needs_push(task.branch):
            print(f"Pushing branch '{task.branch}' to origin...")
            git.push_branch(task.branch)
    except GitError as e:
        return EnsureTaskPrResult(ok=False, status="push_failed", error=str(e))

    if task.pr_number:
        pr_url = gh.get_pr_url(task.pr_number)
        if pr_url:
            return EnsureTaskPrResult(
                ok=True,
                status="cached",
                pr_url=pr_url,
                pr_number=task.pr_number,
            )
        task.pr_number = None
        store.update(task)

    existing_pr_url = gh.pr_exists(task.branch)
    if existing_pr_url:
        pr_number = gh.get_pr_number(task.branch)
        if pr_number:
            task.pr_number = pr_number
            store.update(task)
        return EnsureTaskPrResult(ok=True, status="existing", pr_url=existing_pr_url, pr_number=pr_number)

    if git.is_merged(task.branch, default_branch):
        if merged_behavior == "error":
            return EnsureTaskPrResult(ok=False, status="merged", error=default_branch)
        return EnsureTaskPrResult(ok=True, status="merged")

    try:
        pr = gh.create_pr(
            head=task.branch,
            base=default_branch,
            title=title,
            body=body,
            draft=draft,
        )
    except GitHubError as e:
        return EnsureTaskPrResult(ok=False, status="create_failed", error=str(e))

    if pr.number:
        task.pr_number = pr.number
        store.update(task)
    return EnsureTaskPrResult(ok=True, status="created", pr_url=pr.url, pr_number=pr.number)
