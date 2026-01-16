# Gza Examples

Practical examples showing common workflows with Gza.

| Example | Description |
|---------|-------------|
| [Simple Task](simple-task.md) | Quick fix or small feature—no planning, no review |
| [Plan → Implement → Review](plan-implement-review.md) | Multi-phase workflow for larger features |
| [Bulk Import](bulk-import.md) | Import multiple related tasks from YAML |
| [Parallel Workers](parallel-workers.md) | Run multiple tasks concurrently |
| [Exploration](exploration.md) | Research and investigation tasks |

## Quick Reference

| Task | Command |
|------|---------|
| Add simple task | `gza add "prompt"` |
| Add plan task | `gza add --type plan "prompt"` |
| Add with auto-review | `gza add --review "prompt"` |
| Run next task | `gza work` |
| Run in background | `gza work --background` |
| View pending | `gza next` |
| View running workers | `gza ps` |
| Tail worker logs | `gza log -w <worker_id> -f` |
| View task log | `gza log -t <task_id>` |
| Stop a worker | `gza stop <worker_id>` |
| View unmerged work | `gza unmerged` |
| Create PR | `gza pr <task_id>` |
| View group status | `gza status <group>` |
| View stats | `gza stats` |
