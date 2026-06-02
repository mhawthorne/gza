# PR Code Review Integration

**Status**: ✅ Implemented via `gza review` auto-posting (not separate command)

## Overview

This spec describes how code review results are automatically posted as comments on GitHub Pull Requests, enabling a complete automated workflow from planning through implementation, review, and PR feedback.

## Implementation Summary

Reviews automatically post to PRs when:
1. Review task completes successfully (`gza review <task-id> --run`)
2. Implementation task has an associated PR
3. GitHub CLI is available and authenticated
4. `--no-pr` flag is not set

The implementation integrates PR posting directly into `gza review` rather than creating a separate `gza pr-review` command, reducing cognitive overhead and command proliferation.

## Current Workflow

```bash
# Create and implement a plan
gza add --type plan "Design feature X"
gza work                                      # Produces plan

# Implement with auto-review
gza add --type implement --based-on 1 --review "Implement per plan"
gza work                                      # Implements code
gza work                                      # Runs auto-created review

# Create PR (stores PR number for future reviews)
gza pr <implement_task_id>                    # Creates PR, caches PR number

# Review posts automatically to PR
gza review <implement_task_id> --run          # Creates review, runs it, auto-posts to PR

# Iteration (if changes requested):
gza improve <implement_task_id>               # Creates improve task on same branch
gza work                                      # Addresses review feedback (commits include review trailer)
gza review <implement_task_id> --run          # New review auto-posts to PR
```

## Flags

- `gza review <task-id>` - creates review, auto-posts to PR if one exists
- `gza review <task-id> --no-pr` - creates review, never posts to PR
- `gza review <task-id> --pr` - creates review, errors if no PR found (requires PR)

## Schema Changes (v5 → v6)

✅ Implemented

Added field to track PR association:

```python
@dataclass
class Task:
    # ... existing fields ...
    pr_number: int | None = None    # GitHub PR number
```

Migration:

```python
MIGRATION_V5_TO_V6 = """
ALTER TABLE tasks ADD COLUMN pr_number INTEGER;
"""
```

## GitHub Wrapper Extensions

✅ Implemented

Added methods to `github.py`:

```python
class GitHub:
    def get_pr_number(self, branch: str) -> int | None:
        """Get PR number for a branch, or None if no PR exists."""
        result = self._run("pr", "view", branch, "--json", "number", "-q", ".number", check=False)
        if result.returncode == 0 and result.stdout.strip():
            try:
                return int(result.stdout.strip())
            except ValueError:
                return None
        return None

    def add_pr_comment(self, pr_number: int, body: str) -> None:
        """Add a comment to a PR."""
        self._run("pr", "comment", str(pr_number), "--body", body)
```

**Note**: We use `add_pr_comment()` instead of formal PR reviews (`gh pr review`) because:
- Comments are simpler and less invasive
- They don't affect PR merge requirements
- Avoids confusion about whether AI approval counts for PR merging

## CLI Changes

### Modified: `gza pr`

✅ Implemented

Updated `cmd_pr()` to cache PR number after creation:

```python
def cmd_pr(args: argparse.Namespace) -> int:
    # ... existing validation and PR creation ...

    # Cache PR number in task
    if pr.number:
        task.pr_number = pr.number
        store.update(task)
```

### Modified: `gza review`

✅ Implemented - Auto-posting integrated into existing command

Updated `cmd_review()` to auto-post reviews to PRs:

```bash
gza review <task_id> [--run] [--no-pr] [--pr]
```

Arguments:
- `<task_id>`: Implementation task ID to review
- `--run`: Run the review task immediately (existing flag)
- `--no-pr`: Skip PR posting even if PR exists
- `--pr`: Require PR to exist (error if not found)

Implementation:

```python
def cmd_review(args: argparse.Namespace) -> int:
    # ... existing review task creation ...

    # If --run flag is set, run the review task immediately
    if hasattr(args, 'run') and args.run:
        print(f"\nRunning review task #{review_task.id}...")
        from .runner import run
        exit_code = run(config, task_id=review_task.id)

        # After successful review, post to PR if applicable
        if exit_code == 0 and not args.no_pr:
            _post_review_to_pr(review_task, impl_task, store, config.project_dir, required=args.pr)

        return exit_code
```

### Helper Function

```python
def _post_review_to_pr(
    review_task: DbTask,
    impl_task: DbTask,
    store: SqliteTaskStore,
    project_dir: Path,
    required: bool = False,
) -> None:
    """Post a review task's output to its associated PR."""
    gh = GitHub()

    # Check gh is available
    if not gh.is_available():
        if required:
            print("Error: GitHub CLI not available, cannot post review")
        else:
            print("Info: GitHub CLI not available, skipping PR comment")
        return

    # Find PR number (cached or via branch lookup)
    pr_number = None
    if impl_task.pr_number:
        pr_number = impl_task.pr_number
        print(f"Found PR #{pr_number} (cached)")
    elif impl_task.branch:
        pr_number = gh.get_pr_number(impl_task.branch)
        if pr_number:
            print(f"Found PR #{pr_number} for branch {impl_task.branch}")
            # Cache it for future use
            impl_task.pr_number = pr_number
            store.update(impl_task)

    if not pr_number:
        if required:
            print(f"Error: No PR found for task #{impl_task.id}")
        else:
            print(f"Info: No PR found for task #{impl_task.id}, skipping PR comment")
        return

    # Get review content and post
    from .runner import _get_task_output
    review_content = _get_task_output(review_task, project_dir)
    if not review_content:
        print(f"Warning: Review task #{review_task.id} has no output content")
        return

    comment_body = f"""## 🤖 Automated Code Review

**Review Task**: #{review_task.id}
**Implementation Task**: #{impl_task.id}

---

{review_content}

---

*Generated by `gza review` task*
"""

    try:
        gh.add_pr_comment(pr_number, comment_body)
        print(f"✓ Posted review to PR #{pr_number}")
    except GitHubError as e:
        print(f"Warning: Failed to post review to PR: {e}")
```

## Additional Features

### Improve Task Commit Trailers

✅ Implemented

Improve task commits now include `Gza-Review:` trailer for traceability:

```
Improve implementation based on review #30

Address input validation issues identified in code review.

Task ID: 20260211-improve-authentication
Gza-Review: #30
```

This allows searching for commits addressing specific reviews via `git log --grep "Gza-Review: #30"`.

## Database Query Additions

✅ Already implemented

The `get_reviews_for_task()` method already exists in the task store:

```python
def get_reviews_for_task(self, task_id: int) -> list[Task]:
    """Get all review tasks that depend on the given task, ordered by created_at DESC."""
    with self._connect() as conn:
        cur = conn.execute(
            """
            SELECT * FROM tasks
            WHERE task_type = 'review' AND depends_on = ?
            ORDER BY created_at DESC
            """,
            (task_id,),
        )
        return [self._row_to_task(row) for row in cur.fetchall()]
```

PR number updates handled via standard `store.update(task)` method.

## Error Handling

| Scenario | Behavior |
|----------|----------|
| Review task not completed | Review runs first, then posts |
| No review content | Warning: "Review task has no output content" |
| PR not found (default) | Info message, continues without error |
| PR not found (--pr flag) | Error: "No PR found for task #X" |
| gh CLI not available (default) | Info message, continues without error |
| gh CLI not available (--pr flag) | Error: "GitHub CLI not available" |
| PR already has this review | Allow duplicate (GitHub handles display) |
| Multiple reviews per PR | Each review creates separate comment |

## Testing

### Recommended Unit Tests

```python
def test_review_posts_to_pr_when_exists():
    """Review auto-posts to PR when PR number is cached."""

def test_review_discovers_pr_via_branch():
    """Review discovers PR via branch when pr_number not cached."""

def test_review_skips_pr_when_none_exists():
    """Review continues without error when no PR exists."""

def test_review_no_pr_flag():
    """--no-pr flag prevents PR posting."""

def test_review_pr_flag_errors_when_missing():
    """--pr flag errors if no PR found."""

def test_improve_commit_includes_review_trailer():
    """Improve task commits include Gza-Review trailer."""

def test_get_pr_number_success():
    """GitHub.get_pr_number returns PR number when PR exists."""

def test_get_pr_number_no_pr():
    """GitHub.get_pr_number returns None when no PR exists."""

def test_add_pr_comment_success():
    """GitHub.add_pr_comment posts comment successfully."""

def test_gza_pr_caches_pr_number():
    """gza pr command caches PR number after creation."""
```

### Integration Tests

```python
def test_full_workflow_with_auto_review_posting():
    """End-to-end: plan -> implement -> pr -> review (auto-posts)."""
```

## Implementation Status

✅ All core features implemented:

1. ✅ **Schema migration v5 → v6**: Added `pr_number` column
2. ✅ **GitHub wrapper**: Added `get_pr_number()`, `add_pr_comment()`
3. ✅ **Update `cmd_pr()`**: Caches PR number after creation
4. ✅ **Update `cmd_review()`**: Auto-posts review to PR when `--run` is used
5. ✅ **Add helper function**: `_post_review_to_pr()` with PR discovery logic
6. ✅ **Review trailers**: Improve task commits include `Gza-Review: #X`
7. ⏳ **Tests**: Unit and integration tests (recommended, not yet implemented)
8. ⏳ **Documentation**: Update AGENTS.md and examples (in progress)

## Design Decisions

1. **Multiple reviews per PR**: ✅ Allow duplicates
   - Each review iteration creates a separate comment
   - Preserves history of review progression
   - Simple implementation, no duplicate tracking needed

2. **Review updates**: ✅ Add new comment
   - Don't update/replace previous comments
   - Preserves history and avoids complexity
   - Users can see evolution of reviews on PR

3. **PR review vs PR comment**: ✅ Use comments only
   - Don't use formal GitHub reviews (approve/request-changes events)
   - AI reviews shouldn't affect PR merge requirements
   - Simpler implementation, less intrusive
   - Verdict still visible in comment body

4. **Auto-post by default**: ✅ Yes, with opt-out
   - Reviews auto-post to PR when one exists
   - Use `--no-pr` to skip posting
   - Use `--pr` to require PR (error if missing)
   - More intuitive workflow, less friction

## Future Enhancements (Out of Scope)

1. **Formal PR reviews**: Use `gh pr review` with approve/request-changes events
   - Would make AI reviews affect PR merge requirements
   - Can be added as opt-in feature if users request it

2. **Review comment updates**: Update existing PR comment instead of adding new one
   - Complex: requires tracking comment IDs
   - Current approach (multiple comments) is simpler

3. **Line-level PR comments**: Post specific findings as line comments
   - Requires parsing review output for file/line references
   - Much more complex GitHub API interaction

4. **Review summary in PR description**: Include review verdict in PR description
   - Would require PR description updates
   - Current approach (comment) is simpler
