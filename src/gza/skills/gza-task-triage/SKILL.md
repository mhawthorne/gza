---
name: gza-task-triage
description: Triage `gza incomplete` rows — classify each unresolved merge-unit lineage and recommend the right corrective action (drop moot leaves, escalate to fix, surface manual-resolve rebases, etc.). Never merges, retries, resumes, or deletes branches; never edits code.
allowed-tools: Read, Bash(uv run gza show:*), Bash(uv run gza incomplete:*), Bash(uv run gza lineage:*), Bash(uv run gza log:*), Bash(uv run gza set-status:*), Bash(uv run python -c:*), Bash(git:*), AskUserQuestion
version: 1.0.0
public: true
---

# Gza Task Triage

Walk the `gza incomplete` "needs attention" list (or a single lineage) and decide, row by row, what the correct corrective action is. The output is a per-row classification plus a recommended `gza` command; this skill **asks before doing anything** — including drops.

This skill exists because the same conversation keeps recurring: `gza watch` runs the easy cases and leaves a small pile of merge units in `gza incomplete` that each need a judgment call. The slot below `watch` is the human triage step, and this skill systematizes it.

## What this skill MUST NOT do

These guardrails are load-bearing. Do not relax them.

- **Do not merge** — never run `gza merge` or `git merge`.
- **Do not retry or resume** — never invoke `gza retry` or `gza resume` on your own. If a row looks like it warrants a resume, say so and stop; let the user run it.
- **Do not delete branches** — never `git branch -D` or otherwise mutate refs.
- **Do not edit code** — for review/improve churn that needs a real code fix, hand off to `/gza-task-fix` rather than editing inline.
- **Ask per row** — even for safe-looking drops, ask before executing.

If you find yourself wanting to do any of the above without explicit per-row confirmation, stop and report instead.

## Process

### Step 1: Resolve scope

The skill accepts an optional task ID.

- **With ID:** triage just that lineage. Resolve the merge-unit owner via `gza show <id>` and look at the row that owns it. If the ID is not currently in `gza incomplete`, run `gza show <id>` and report the lifecycle anyway — the user may want to triage a lineage they expect to surface.
- **Without ID:** sweep the whole list. Run `uv run gza incomplete --json --last 0` and process every row.

In both modes, also fetch the structured rows so you can classify by `next_action`:

```bash
uv run gza incomplete --json --last 0
```

The JSON rows include:
- `id` — the merge-unit owner ID (this is the row that surfaces)
- `next_action` — discrete type (see Step 2)
- `next_action_reason` — short reason string (see Step 2)
- `unresolved_ids` — leaves blocking resolution
- `review_verdict` — last review verdict text (when present)
- `lineage_root_id`, `branch_owner_id`, `branch_merge_state`

If the user passed a task ID, filter to the row whose `id` matches the merge-unit owner that contains it (check both `id` and `member_ids`).

### Step 2: Classify each row

Map `next_action` (the `type` field) and `next_action_reason` to a classification. Authoritative values (from `src/gza/advance_engine.py`):

| `next_action` | Typical `next_action_reason` | Classification |
|---|---|---|
| `skip` (with reason `merge-unit-merged`) | merge unit already merged | **moot — propose drop of failed leaves** |
| `needs_rebase` | `rebase --resolve (conflicts detected)` | **manual rebase resolution** |
| `needs_discussion` | `rebase-failed-needs-manual-resolution` | **failed rebase — check if target merged** |
| `needs_discussion` | `merge-source-needs-manual-resolution` | **manual merge-source conflict** |
| `max_cycles_reached` | `review-max-cycles-reached` | **review/improve churn — hand off to `/gza-task-fix`** |
| `needs_discussion` | `retry-limit-reached` / `recovery-ambiguous` / `manual-review-required` | **needs human code review** |
| `resume` | various | **infra failure (timeout, worker died) — recommend `gza resume`** but DO NOT run it |
| `skip` (other) | varies | **unknown — show context and let the user decide** |

Be conservative: if the reason string doesn't match cleanly, classify as "unknown" and surface the raw `next_action_reason` and `review_verdict` to the user.

### Step 3: Verify the classification against current state

A classification is a hypothesis. Before recommending an action, confirm it against the lineage's actual state. The frequent failure mode is a leaf that still surfaces even though its work is already in main.

For each row, also run:

```bash
uv run gza show <merge-unit-owner-id>
```

Read the lineage tree and the lifecycle line carefully. Then for each ID in `unresolved_ids`, run a cheap check:

```bash
uv run gza show <leaf-id>
```

and look at:
- `Lifecycle:` — does it say "target implementation already merged" or "merge-unit-merged"? Those leaves are dead work.
- `Status:` and `Failure Reason:` — TIMEOUT / WORKER_DIED / INFRASTRUCTURE_ERROR signal infra failures (resume-class); semantic failures (failed verify, max turns with real loop) are different.
- `Merge Status:` — `merged` confirms the work shipped.

If every unresolved leaf has lifecycle "target merged" or "merge-unit-merged", the owner is surfacing only because of stale failed leaves. The correct action is to drop those leaves.

### Step 4: Recommend an action per row

For each row, print:

1. The merge-unit owner ID and one-line prompt summary.
2. The classification (from Step 2, possibly refined by Step 3).
3. The unresolved leaves and their lifecycles (short).
4. The recommended action — a single concrete `gza` command, or "manual: …" with a one-line description.
5. A short rationale (one sentence).

Action recipes by classification:

- **moot — propose drop**: recommend `uv run gza set-status <leaf-id> dropped --reason "<short reason>"` for each dead leaf. Multiple drops = multiple commands. Don't drop the owner.
- **manual rebase resolution** (`needs_rebase`): recommend the user run `uv run gza rebase --resolve <task-id>` themselves. Do not run it from this skill.
- **failed rebase** (`needs_discussion / rebase-failed-needs-manual-resolution`): in Step 3 you already checked the target. If target is merged, treat as "moot — propose drop" of the failed rebase. Otherwise recommend manual resolution and surface the conflict details from `gza log` if helpful.
- **review/improve churn** (`max_cycles_reached`): recommend `/gza-task-fix <implementation-task-id>` — the slash command, not a Bash invocation. Surface the latest review's blockers section from `review_verdict` so the user has context.
- **needs human code review**: surface the verdict and stop. Don't recommend a command.
- **infra failure** (`resume`): explicitly say "this looks like infra (TIMEOUT/WORKER_DIED) — `uv run gza resume <id>` is the user-driven path". Do not run it.
- **unknown**: print raw fields and ask the user how to proceed.

### Step 5: Ask before acting

For each row where the recommendation is a `gza set-status … dropped` command (the only mutation this skill is allowed to issue), present an AskUserQuestion with options: `Run it`, `Skip this row`, `Show more context`. Do not bundle drops across rows into a single yes/no — ask per row.

For every other recommendation, do not execute. Print the command and move on.

If the user picks "Show more context" for any row, fetch and surface:
- The full `gza show <id>` for the merge-unit owner.
- The relevant section of the latest review's `review_verdict` (for review/improve churn).
- Recent log tail: `uv run gza log -t <id> --steps-verbose` last ~80 lines (for infra failures).

### Step 6: Final summary

After the sweep, print a compact summary:

- N rows triaged.
- M drops executed (with the dropped leaf IDs).
- K rows handed off to the user (with the recommended commands grouped by classification).

The summary is for the user's audit log — they should be able to read it and know exactly what changed and what's still open.

## Output style

- One block per row, separated by `---`.
- Lead with the merge-unit owner ID and classification.
- Keep prose short. The user has been triaging these all morning and wants signal, not narration.
- When showing `review_verdict` excerpts, quote the `## Blockers` section and skip the rest.

## When to escalate to a different skill

- Real review/improve churn that needs code → `/gza-task-fix`
- Per-task root-cause analysis (loop detection, baseline comparison) → `/gza-task-debug`
- Resuming a specific task → user runs `gza resume <id>` themselves
- Manual rebase conflict resolution → user runs `gza rebase --resolve <id>` themselves

This skill is the dispatch layer, not the worker.
