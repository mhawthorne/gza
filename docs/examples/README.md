# Gza Examples

Practical examples showing common workflows with Gza.

## Core Workflows

| Example | Description |
|---------|-------------|
| [Simple Task](simple-task.md) | Quick fix or small feature—no planning, no review |
| [Plan → Implement → Review](plan-implement-review.md) | Multi-phase workflow for larger features |
| [Exploration](exploration.md) | Research and investigation tasks |

## Advanced Topics

| Example | Description |
|---------|-------------|
| [Using Spec Files](using-specs.md) | Provide design docs as context for tasks |
| [Handling Failed Tasks](failed-tasks.md) | Resume vs retry, recovering from failures |
| [Rebasing](rebasing.md) | Rebase branches before merging |

## Batch Operations

| Example | Description |
|---------|-------------|
| [Bulk Import](bulk-import.md) | Import multiple related tasks from YAML |
| [Parallel Workers](parallel-workers.md) | Run multiple tasks concurrently |

## Quick Reference

> **Note:** Commands that take `<task_id>` accept numeric task IDs (e.g., `1`, `42`), not slugs.

| Task | Command |
|------|---------|
| Add simple task | `gza add "prompt"` |
| Add plan task | `gza add --type plan "prompt"` |
| Add implementation | `gza add --type implement --based-on <plan_id> "prompt"` |
| Add with spec | `gza add --spec specs/design.md "prompt"` |
| Run specific task | `gza work <task_id>` |
| Run next task | `gza work` |
| Run in background | `gza work --background` |
| View pending | `gza next` |
| View running workers | `gza ps` |
| Tail worker logs | `gza log -w <worker_id> -f` |
| View task log | `gza log -t <task_id>` |
| Stop a worker | `gza stop <worker_id>` |
| Create review | `gza review <impl_id>` |
| Address feedback | `gza improve <impl_id>` |
| Resume failed task | `gza resume <task_id>` |
| Retry from scratch | `gza retry <task_id>` |
| View unmerged work | `gza unmerged` |
| Rebase a branch | `gza rebase <task_id>` |
| Create PR | `gza pr <task_id>` |
| Merge to main | `gza merge <task_id> --squash` |
| View group status | `gza status <group>` |
| View history | `gza history` |
| View stats | `gza stats` |
