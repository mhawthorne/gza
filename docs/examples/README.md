# Theo Examples

Practical examples showing common workflows with Theo.

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
| Add simple task | `theo add "prompt"` |
| Add plan task | `theo add --type plan "prompt"` |
| Add with auto-review | `theo add --review "prompt"` |
| Run next task | `theo work` |
| Run in background | `theo work --background` |
| View pending | `theo next` |
| View running workers | `theo ps` |
| Tail worker logs | `theo log -w <worker_id> -f` |
| View task log | `theo log -t <task_id>` |
| Stop a worker | `theo stop <worker_id>` |
| View unmerged work | `theo unmerged` |
| Create PR | `theo pr <task_id>` |
| View group status | `theo status <group>` |
| View stats | `theo stats` |
