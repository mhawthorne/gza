"""Shared pull-request ensure/create flow for task branches."""

import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from .config import Config
from .console import MAX_PR_BODY_LENGTH, MAX_PR_TITLE_LENGTH, truncate
from .db import SqliteTaskStore, Task
from .git import Git, GitError
from .github import GitHub, GitHubError
from .prompts import PromptBuilder
from .sync_ops import resolve_branch_pr

PrEnsureStatus = Literal[
    "cached",
    "existing",
    "created",
    "merged",
    "gh_unavailable",
    "lookup_failed",
    "push_failed",
    "create_failed",
]
PrLookupStatus = Literal["cached", "existing", "missing", "gh_unavailable"]


@dataclass(frozen=True)
class EnsureTaskPrResult:
    """Result from ensuring a pull request for a task branch."""

    ok: bool
    status: PrEnsureStatus
    pr_url: str | None = None
    pr_number: int | None = None
    error: str | None = None


def build_task_pr_content(
    task: Task,
    git: Git,
    config: Config,
    store: SqliteTaskStore,
    *,
    title_override: str | None = None,
) -> tuple[str, str]:
    """Build PR title/body for a task branch.

    Both `gza pr` and automatic PR creation should use this shared helper so
    provider-backed generation and deterministic fallback stay on one path.
    """
    if title_override is not None:
        return title_override, f"## Summary\n{truncate(task.prompt, MAX_PR_BODY_LENGTH)}"

    assert task.branch is not None, "Task branch is required to build PR content"
    default_branch = git.default_branch()
    commit_log = git.get_log(f"{default_branch}..{task.branch}")
    diff_stat = git.get_diff_stat(f"{default_branch}...{task.branch}")
    return _generate_pr_content(task, commit_log, diff_stat, config, store)


def _generate_pr_content(
    task: Task,
    commit_log: str,
    diff_stat: str,
    config: Config,
    store: SqliteTaskStore,
) -> tuple[str, str]:
    """Generate PR title and body using an internal task."""
    prompt = PromptBuilder().pr_description_prompt(
        task_prompt=task.prompt,
        commit_log=commit_log,
        diff_stat=diff_stat,
    )

    internal_task = store.add(
        prompt=prompt,
        task_type="internal",
        skip_learnings=True,
    )

    if internal_task.id is None:
        return _fallback_pr_content(task, commit_log, project_prefix=config.project_prefix or None)
    internal_task_id = internal_task.id

    def _mark_internal_task_failed_if_nonterminal() -> None:
        refreshed = store.get(internal_task_id)
        if refreshed is None:
            return
        if refreshed.status in {"pending", "in_progress"}:
            store.mark_failed(refreshed, failure_reason="UNKNOWN")

    try:
        from . import runner as runner_mod

        exit_code = runner_mod.run(config, task_id=internal_task_id)
    except Exception as exc:
        _mark_internal_task_failed_if_nonterminal()
        print(
            f"Warning: PR description internal task {internal_task_id} failed: {exc}",
            file=sys.stderr,
        )
        return _fallback_pr_content(task, commit_log, project_prefix=config.project_prefix or None)

    completed_task = store.get(internal_task_id)
    if exit_code != 0 or completed_task is None or completed_task.status != "completed":
        _mark_internal_task_failed_if_nonterminal()
        print(
            f"Warning: PR description internal task {internal_task_id} did not complete successfully",
            file=sys.stderr,
        )
        return _fallback_pr_content(task, commit_log, project_prefix=config.project_prefix or None)

    response = (completed_task.output_content or "").strip()
    if not response:
        print(
            f"Warning: PR description internal task {internal_task_id} produced no output",
            file=sys.stderr,
        )
        return _fallback_pr_content(task, commit_log, project_prefix=config.project_prefix or None)

    has_title = any(line.startswith("TITLE:") for line in response.splitlines())
    has_body = any(line.strip() == "BODY:" for line in response.splitlines())
    if not (has_title and has_body):
        print(
            f"Warning: PR description internal task {internal_task_id} produced malformed output",
            file=sys.stderr,
        )
        return _fallback_pr_content(task, commit_log, project_prefix=config.project_prefix or None)

    return _parse_pr_response(response, task)


def _parse_pr_response(response: str, task: Task) -> tuple[str, str]:
    """Parse provider output into title/body."""
    lines = response.split("\n")
    title = ""
    body_lines = []
    in_body = False

    for line in lines:
        if line.startswith("TITLE:"):
            title = line[6:].strip()
        elif line.strip() == "BODY:":
            in_body = True
        elif in_body:
            body_lines.append(line)

    if not title:
        title = task.slug or truncate(task.prompt.split("\n")[0], MAX_PR_TITLE_LENGTH)

    body = "\n".join(body_lines).strip()
    if not body:
        body = f"Task: {truncate(task.prompt, MAX_PR_BODY_LENGTH)}"

    return title, body


def _fallback_pr_content(
    task: Task,
    commit_log: str,
    project_prefix: str | None = None,
) -> tuple[str, str]:
    """Generate deterministic PR content when provider generation fails."""
    if task.slug:
        slug_no_date = task.slug.split("-", 1)[1] if "-" in task.slug else task.slug
        if project_prefix and slug_no_date.startswith(f"{project_prefix}-"):
            slug_no_date = slug_no_date[len(project_prefix) + 1:]
        title = slug_no_date.replace("-", " ").capitalize()
    else:
        title = truncate(task.prompt.split("\n")[0], MAX_PR_TITLE_LENGTH)

    body = f"""## Task Prompt

> {truncate(task.prompt, MAX_PR_BODY_LENGTH).replace(chr(10), chr(10) + '> ')}

## Commits
```
{commit_log}
```
"""
    return title, body


@dataclass(frozen=True)
class LookupTaskPrResult:
    """Result from resolving whether a task branch already has an open PR."""

    found: bool
    status: PrLookupStatus
    pr_url: str | None = None
    pr_number: int | None = None


def lookup_task_pr(
    task: Task,
    *,
    store: SqliteTaskStore | None = None,
    gh: GitHub | None = None,
    available: bool | None = None,
    refresh_cache: bool = False,
    include_number: bool = True,
) -> LookupTaskPrResult:
    """Resolve an existing open PR for a task without creating or pushing anything."""
    gh_client = gh or GitHub()
    gh_available = gh_client.is_available() if available is None else available
    if not gh_available:
        return LookupTaskPrResult(found=False, status="gh_unavailable")

    if task.pr_number:
        pr_url = gh_client.get_pr_url(task.pr_number)
        if pr_url:
            return LookupTaskPrResult(
                found=True,
                status="cached",
                pr_url=pr_url,
                pr_number=task.pr_number,
            )
        if refresh_cache and store is not None:
            task.pr_number = None
            store.update(task)

    if not task.branch:
        return LookupTaskPrResult(found=False, status="missing")

    pr_url = gh_client.pr_exists(task.branch)
    if not pr_url:
        return LookupTaskPrResult(found=False, status="missing")

    pr_number = gh_client.get_pr_number(task.branch) if include_number else None
    if refresh_cache and store is not None and pr_number:
        task.pr_number = pr_number
        store.update(task)
    return LookupTaskPrResult(found=True, status="existing", pr_url=pr_url, pr_number=pr_number)


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

    pr_lookup_time = datetime.now(UTC)
    try:
        resolved_pr = resolve_branch_pr(
            gh,
            task.branch,
            cached_pr_numbers=((task.pr_number,) if task.pr_number is not None else ()),
            allow_discovery=True,
        )
    except GitHubError as e:
        return EnsureTaskPrResult(ok=False, status="lookup_failed", error=str(e))
    if resolved_pr.details is not None:
        task.pr_number = resolved_pr.details.number
        task.pr_state = resolved_pr.details.state
        task.pr_last_synced_at = pr_lookup_time
        store.update(task)
        if resolved_pr.details.state == "open":
            return EnsureTaskPrResult(
                ok=True,
                status="cached" if resolved_pr.source == "cached" else "existing",
                pr_url=resolved_pr.details.url,
                pr_number=resolved_pr.details.number,
            )
    elif resolved_pr.clear_cached_number:
        task.pr_number = None
        task.pr_state = None
        task.pr_last_synced_at = pr_lookup_time
        store.update(task)

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
        task.pr_state = "open"
        task.pr_last_synced_at = datetime.now(UTC)
        store.update(task)
    return EnsureTaskPrResult(ok=True, status="created", pr_url=pr.url, pr_number=pr.number)
