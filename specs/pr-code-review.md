# PR Code Review Integration

## Overview

This spec describes how to submit code review results as comments on GitHub Pull Requests, enabling a complete automated workflow from planning through implementation, review, and PR feedback.

## Motivation

The current workflow supports:
1. Planning tasks that produce `.gza/plans/{task_id}.md`
2. Implementation tasks that create branches with code changes
3. Review tasks that produce `.gza/reviews/{task_id}.md` with verdicts
4. PR creation via `gza pr <task_id>`

However, review results exist only as local files. To complete the feedback loop:
- Reviewers should see AI review results directly on the PR
- Review verdicts (APPROVED, CHANGES_REQUESTED, NEEDS_DISCUSSION) should be visible in GitHub
- The workflow should support iterative review cycles

## Target Workflow

```
gza add --type plan "Design feature X"
gza work                                      # Produces plan
# Human reviews/approves plan

gza add --type implement --based-on 1 --review "Implement per plan"
gza work                                      # Implements code
gza work                                      # Runs auto-created review

gza pr <implement_task_id>                    # Creates PR, stores PR number
gza pr-review <review_task_id>                # Submits review to PR as comment

# Iteration (if changes requested):
gza add --type implement --based-on 1 --same-branch "Address review feedback"
gza work
gza add --type review --based-on <new_impl_task_id>
gza work
gza pr-review <new_review_task_id>            # Updates PR with new review
```

## Schema Changes

Add field to track PR association:

```python
@dataclass
class Task:
    # ... existing fields ...
    pr_number: int | None = None    # GitHub PR number (set after PR creation)
```

SQL migration:

```sql
-- Migration v5 → v6
ALTER TABLE tasks ADD COLUMN pr_number INTEGER;
```

## GitHub Wrapper Extensions

New methods in `github.py`:

```python
class GitHub:
    # ... existing methods ...

    def get_pr_number(self, branch: str) -> int | None:
        """Get PR number for a branch, or None if no PR exists."""
        result = subprocess.run(
            ["gh", "pr", "view", branch, "--json", "number", "-q", ".number"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0 and result.stdout.strip():
            return int(result.stdout.strip())
        return None

    def add_pr_comment(self, pr_number: int, body: str) -> None:
        """Add a comment to a PR."""
        subprocess.run(
            ["gh", "pr", "comment", str(pr_number), "--body", body],
            check=True,
        )

    def submit_pr_review(
        self,
        pr_number: int,
        body: str,
        event: str = "COMMENT",  # APPROVE, REQUEST_CHANGES, COMMENT
    ) -> None:
        """Submit a formal PR review."""
        subprocess.run(
            ["gh", "pr", "review", str(pr_number), "--body", body, f"--{event.lower().replace('_', '-')}"],
            check=True,
        )
```

## CLI Changes

### Modified: `gza pr`

Update `cmd_pr()` to store the PR number after creation:

```python
def cmd_pr(task_id: str, draft: bool = False) -> None:
    # ... existing validation ...

    pr_url = github.create_pr(title=title, body=body, head=branch, draft=draft)
    pr_number = github.get_pr_number(branch)

    if pr_number:
        task.pr_number = pr_number
        store.update(task)

    print(f"Created PR: {pr_url}")
```

### New: `gza pr-review`

Submit a review task's output to its associated PR:

```bash
gza pr-review <review_task_id> [--pr <pr_number>]
```

Arguments:
- `<review_task_id>`: ID of a completed review task
- `--pr <pr_number>`: Optional explicit PR number (auto-detected from task chain if omitted)

Implementation:

```python
@app.command(name="pr-review")
def cmd_pr_review(
    review_task_id: str,
    pr_number: int | None = typer.Option(None, "--pr", help="PR number (auto-detected if omitted)"),
) -> None:
    """Submit a review task's output as a PR comment."""
    store = get_store()
    github = GitHub()

    # Validate review task
    task = store.get_by_task_id(review_task_id)
    if not task:
        error(f"Task not found: {review_task_id}")

    if task.task_type != "review":
        error(f"Task {review_task_id} is not a review task (type: {task.task_type})")

    if task.status != "completed":
        error(f"Review task not completed (status: {task.status})")

    # Get review content
    review_content = _get_task_output(task)
    if not review_content:
        error("Review task has no output content")

    # Find PR number
    if pr_number is None:
        pr_number = _find_pr_for_review(task, store)

    if pr_number is None:
        error("Could not determine PR number. Use --pr to specify explicitly.")

    # Parse verdict from review content
    verdict = _parse_review_verdict(review_content)

    # Format comment
    comment_body = _format_pr_review_comment(review_content, task)

    # Submit to GitHub
    if verdict == "APPROVED":
        github.submit_pr_review(pr_number, comment_body, "APPROVE")
    elif verdict == "CHANGES_REQUESTED":
        github.submit_pr_review(pr_number, comment_body, "REQUEST_CHANGES")
    else:
        github.add_pr_comment(pr_number, comment_body)

    print(f"Submitted review to PR #{pr_number}")
```

### Helper Functions

```python
def _find_pr_for_review(review_task: Task, store: TaskStore) -> int | None:
    """Walk task chain to find associated PR number."""
    # Review task -> implement task -> PR
    if review_task.based_on:
        impl_task = store.get(review_task.based_on)
        if impl_task and impl_task.pr_number:
            return impl_task.pr_number

    # Also check depends_on chain
    if review_task.depends_on:
        dep_task = store.get(review_task.depends_on)
        if dep_task and dep_task.pr_number:
            return dep_task.pr_number

    return None


def _parse_review_verdict(content: str) -> str:
    """Extract verdict from review content."""
    content_upper = content.upper()
    if "VERDICT: APPROVED" in content_upper or "**APPROVED**" in content_upper:
        return "APPROVED"
    elif "VERDICT: CHANGES_REQUESTED" in content_upper or "CHANGES REQUESTED" in content_upper:
        return "CHANGES_REQUESTED"
    elif "VERDICT: NEEDS_DISCUSSION" in content_upper or "NEEDS DISCUSSION" in content_upper:
        return "NEEDS_DISCUSSION"
    return "COMMENT"


def _format_pr_review_comment(content: str, task: Task) -> str:
    """Format review content for PR comment."""
    return f"""## Automated Code Review

**Task ID:** {task.task_id}

---

{content}

---
*Generated by gza review task*
"""
```

## Optional: Auto-Submit on PR Creation

Add flag to `gza pr` for automatic review submission:

```bash
gza pr <task_id> --submit-review
```

When `--submit-review` is set:
1. Create the PR as normal
2. Find any completed review tasks linked to this implement task
3. Automatically submit them as PR comments

```python
@app.command()
def cmd_pr(
    task_id: str,
    draft: bool = False,
    submit_review: bool = typer.Option(False, "--submit-review", help="Auto-submit linked reviews"),
) -> None:
    # ... existing PR creation ...

    if submit_review:
        review_tasks = store.get_reviews_for_task(task.id)
        for review_task in review_tasks:
            if review_task.status == "completed":
                _submit_review_to_pr(review_task, pr_number, github, store)
```

## Optional: Plan Commitment to Git

For teams that want plans tracked in version control:

```bash
gza plan-commit <plan_task_id> [--branch <branch>]
```

This would:
1. Create a branch (or use specified branch)
2. Commit the plan file
3. Push to remote

However, this may be over-engineering. Plans in `.gza/plans/` are already accessible, and forcing them into git adds ceremony without clear benefit. Leaving as optional/future.

## Database Query Additions

```python
class TaskStore:
    # ... existing methods ...

    def get_reviews_for_task(self, task_id: int) -> list[Task]:
        """Get all review tasks that reference this task."""
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT * FROM tasks
                WHERE task_type = 'review'
                  AND (based_on = ? OR depends_on = ?)
                ORDER BY created_at DESC
                """,
                (task_id, task_id),
            )
            return [self._row_to_task(row) for row in cur.fetchall()]

    def update_pr_number(self, task: Task, pr_number: int) -> None:
        """Set the PR number for a task."""
        with self._connect() as conn:
            conn.execute(
                "UPDATE tasks SET pr_number = ? WHERE id = ?",
                (pr_number, task.id),
            )
        task.pr_number = pr_number
```

## Error Handling

| Scenario | Behavior |
|----------|----------|
| Review task not completed | Error: "Review task not completed" |
| No review content | Error: "Review task has no output content" |
| PR not found for branch | Error with suggestion to use `--pr` flag |
| gh CLI not authenticated | Error: "GitHub CLI not authenticated" |
| PR already has this review | Allow duplicate (GitHub handles display) |

## Testing

### Unit Tests

```python
def test_pr_review_requires_completed_task():
    """pr-review fails if review task not completed."""

def test_pr_review_requires_review_type():
    """pr-review fails if task is not type=review."""

def test_pr_review_finds_pr_from_chain():
    """pr-review auto-detects PR from based_on task."""

def test_pr_review_uses_explicit_pr():
    """--pr flag overrides auto-detection."""

def test_parse_verdict_approved():
    """Parses APPROVED verdict from review content."""

def test_parse_verdict_changes_requested():
    """Parses CHANGES_REQUESTED verdict from review content."""

def test_submit_review_flag_on_pr():
    """--submit-review on gza pr auto-submits linked reviews."""
```

### Integration Tests

```python
def test_full_workflow_with_pr_review(github_mock):
    """End-to-end: plan -> implement -> review -> pr -> pr-review."""
```

## Implementation Order

1. **Schema migration**: Add `pr_number` column
2. **GitHub wrapper**: Add `get_pr_number()`, `add_pr_comment()`, `submit_pr_review()`
3. **Update `cmd_pr()`**: Store PR number after creation
4. **Add `cmd_pr_review()`**: New command with verdict parsing
5. **Add helper functions**: `_find_pr_for_review()`, `_parse_review_verdict()`, `_format_pr_review_comment()`
6. **Tests**: Unit and integration tests
7. **Optional**: `--submit-review` flag on `gza pr`

## Open Questions

1. **Multiple reviews per PR**: Should we track which reviews have been submitted to avoid duplicates?
   - Suggestion: Allow duplicates. GitHub UI handles multiple comments fine, and it's simpler.

2. **Review updates**: If a review is re-run, should it update the existing comment or add a new one?
   - Suggestion: Add new comment. Preserves history and avoids complexity.

3. **PR review vs PR comment**: GitHub distinguishes between "review" (with approval status) and "comment" (just text). Should `NEEDS_DISCUSSION` use review or comment?
   - Suggestion: Use comment for `NEEDS_DISCUSSION` since it's not actionable.

4. **Storing plan in PR description**: Should plans be included in PR body for visibility?
   - Suggestion: Optional. Could add `--include-plan` flag to `gza pr`.
