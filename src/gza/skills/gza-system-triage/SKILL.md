---
name: gza-system-triage
description: Turn the recurring `watch` stuck-task pile into systemic auto-merge fixes. Snapshots watch/incomplete/queue, buckets stuck tasks by failure class, dedups against already-tracked `system` work, ranks fixes by blast radius (cascade-preventer first), and — manual mode presents, auto mode files — `system`-tagged gza tasks. Delegates per-task rescue to /gza-task-fix; never merges, retries, resumes, or edits code.
allowed-tools: Read, Write, AskUserQuestion, Bash(uv run gza incomplete:*), Bash(uv run gza search:*), Bash(uv run gza history:*), Bash(uv run gza next:*), Bash(uv run gza show:*), Bash(uv run gza log:*), Bash(uv run gza add:*), Bash(uv run gza queue:*), Bash(uv run gza ps:*), Bash(uv run python -c:*), Bash(mkdir:*), Bash(date:*)
version: 1.0.0
public: true
---

# Gza System Triage

Every stuck task is a **failed auto-merge**. This skill does not ask "how do I hand-merge this" — hand-merging is debt that grows faster than anyone can service it. It asks, per failure *class*: **"what is the broadest systemic change that would let this and its siblings auto-merge unattended next time?"**

It exists because the same conversation recurs daily: `watch` leaves a pile of `ATTENTION` / `Needs attention` rows, and each session re-derives the landscape from scratch and ships another narrow micro-fix that doesn't compound. This skill closes that loop: snapshot once, bucket by class, **skip classes already being fixed**, rank what's left by blast radius, and file the broadest fix.

The per-task rescue (`/gza-task-fix`) and per-lineage drop/rebase classification (`/gza-task-triage`) already exist. This skill is the **systemic layer above them**, not a replacement.

## What this skill MUST NOT do

These guardrails are load-bearing.

- **Do not merge, retry, resume, or delete branches.** If a row warrants one of those, say so and stop.
- **Do not edit code.** A single unit that needs a code fix is a `/gza-task-fix` handoff, not an inline edit.
- **Do not file untagged tasks.** Every filed task gets `system` plus the active recovery tag — an untagged task is a silent orphan watch never runs.
- **Do not file anything in manual mode without confirmation.**
- **Do not propose another narrow guard for a class that already recurred after a fix landed.** That is the signal the previous fix missed the cause layer — escalate to re-diagnose deeper (see `specs/behavior/systemic-fix-triage.md`), never patch the symptom again.

## Modes

Read the mode from the invocation argument:

- **`manual` (default)** — present the ranked findings and wait. File only after the user confirms.
- **`auto`** — file `system`-tagged tasks with best judgement, then report exactly what was filed.

If the argument is absent or unclear, default to `manual`.

## Process

### Step 1: Snapshot the slow surfaces (and cache)

`watch`/`incomplete`/`queue` are slow to query — capture once and reuse within the session. Write raw outputs to `.gza/system-triage/snapshot-<timestamp>.json` (create the dir if needed). If a snapshot under ~15 minutes old already exists, reuse it unless the caller asks to refresh.

Default to **recent** failures (last 24h) — do not dredge an all-time backlog unless asked.

```bash
mkdir -p .gza/system-triage
uv run gza incomplete --json --last 0                                   # needs-attention rows (next_action_reason)
uv run gza history --status failed --json --days 1 --date-field effective   # recent failed leaves (failure_reason)
uv run gza next --all                                                    # recovery + pending lanes, blocked rows
```

### Step 2: Bucket stuck rows by failure class

Anchor classes on the **reason strings the system already emits** — `next_action_reason` from `incomplete`, `failure_reason` from `history`. The authoritative taxonomy lives in `specs/behavior/systemic-fix-triage.md`; the working set:

| Class | Signal | Default leverage tier |
|---|---|---|
| **environmental-cascade** | `TIMEOUT` / `WORKER_DIED` / `INFRASTRUCTURE_ERROR` (e.g. docker down) clustered across many tasks + their retries in a short window | cascade-preventer |
| **verify-unreproducible** | `improve-no-op` where verify can't be reproduced; subprocess/unit timeouts | auto-merge-enabler |
| **review-no-exit** | `review-max-cycles-reached`, `watch-no-progress-backstop`, reviewer-is-wrong with no loop exit | loop-exit / recovery |
| **rebase/reconcile-manual** | `rebase-failed-needs-manual-resolution`, `merge-source-needs-manual-resolution` | auto-merge-enabler |
| **retry-limit-reached** | `retry-limit-reached` | usually *downstream* of another class — attribute to the real cause |
| **manual-unknown** | `manual-failure-reason UNKNOWN` / `TEST_FAILURE` | per-task diagnosis; cluster only if common |
| **moot** | `merge-unit-merged` / `merge-unit-empty` / `merge-unit-redundant` | not systemic — hand to `/gza-task-triage` to drop |

Where a row has a `/gza-task-fix` ledger, use its `blocker_key` to sharpen clustering (same `blocker_key` across N tasks = one class).

### Step 3: Detect environmental cascades first

If many failures — **including retries** — cluster in a short window on infra reasons, treat them as **one root cause**, not N findings. The right fix is a watch **cycle-level precondition gate** that pauses the whole cycle (recovery lane included) so nothing burns retries while the precondition is broken. Before proposing it, check whether detection/gating already exists (e.g. docker-daemon-crash detection); if it does, point at it rather than re-proposing. A scheduled task cannot fix this — with scarce slots it would deadlock behind a recovery lane that never drains.

### Step 4: Dedup against already-tracked classes

This is the main saving over re-derivation. For each class, check whether **open** `system` work already covers it — the pending and recovery lanes, filtered by tag:

```bash
uv run gza next --all --tag system
```

`gza next` has no `--json`; read the text lanes and run `uv run gza show <id>` on any candidate to confirm which class its prompt covers. If an open `system` task already covers the class, **collapse those rows to "tracked by gza-XXXX — skip"** and move on.

Then check recurrence-after-fix against **landed** `system` fixes:

```bash
uv run gza history --status completed --tag system --json --last 0
```

If a class still recurs *after* its fix landed, flag it for **cause-layer escalation** — do not propose another narrow guard (behavior-spec rule).

(Note: `gza search` is substring-over-prompt and requires a search term, so it is not the right tool for a pure tag-scoped lane query — use `next`/`history` with `--tag` as above.)

### Step 5: Rank the remaining classes by blast radius

For each surviving class, answer the operative question: *"what precondition or rule, if changed, would let this class auto-merge unattended next time?"* Then rank:

1. By **leverage tier**: cascade-preventer > auto-merge-enabler > loop-exit/recovery.
2. Within a tier, by **blast radius** = number of stuck rows the fix clears (impact ≈ frequency × per-task cost, borrowing `/gza-log-insights`' framing).

A fix that itself can't land cheaply (a big general-rule change prone to its own review churn) ranks lower — note the landing risk, because a fix that parks auto-merges nothing.

### Step 6: Present (manual) or file (auto)

For each ranked class produce one block:

- **Class** + **blast radius** (N rows) + affected task IDs.
- **The systemic fix**, stated as a *precondition or rule change*, and its shape: a code change to file, a cycle-level gate, or a `/gza-task-fix` per-task handoff.
- **Dedup result** — "already tracked by gza-XXXX" when applicable, so the user sees what was *not* re-derived.

**Manual mode:** print the ranked blocks, then `AskUserQuestion`: file all / pick a subset / skip.

**Auto mode:** file directly, then report.

Filing recipe (per systemic fix that is a code change):

```bash
# write the multi-line description to a temp file — never use --prompt
uv run gza add --type implement --tag system --tag 202606-recovery --prompt-file <file>
```

- Name the affected stuck task IDs **in the prompt body** as context. Do not auto-link with `--based-on`/`--depends-on` unless there is a real lineage or execution dependency — a systemic fix is usually independent work, and linking would entangle its merge unit with the stuck lineage.
- `system` is the durable cross-skill tag; `202606-recovery` is the active recovery-scope tag so the current `watch` picks it up. Adjust the recovery tag if watch is scoped differently.
- For **cascade-preventers** (top leverage), bump after creating so they lead the pending lane:

```bash
uv run gza queue bump <new-task-id>
```

- A **per-task rescue** (one unit's review/improve churn, not a class) is a `/gza-task-fix <impl-id>` recommendation — do not file a system task for it.

**Report** (both modes): classes filed (new IDs, tags, bumped?), classes skipped as already-tracked, classes escalated for cause-layer re-diagnosis, and per-task handoffs.

## Output style

- One block per class, ranked, separated by `---`. Lead with class + blast radius. Short prose — the user has been reading `watch` output all day and wants signal.
- Always surface the dedup result. "3 classes, 1 already tracked, 1 escalated, 1 new" is the headline that proves the loop is closing instead of re-deriving.

## When to escalate to a different skill

- Per-task rescue (review/improve churn on one unit) → `/gza-task-fix`
- Per-lineage drop / rebase classification of `incomplete` rows → `/gza-task-triage`
- Log-level anti-patterns (bare commands, test loops, cost outliers) → `/gza-log-insights`
- Per-task root-cause / loop analysis → `/gza-task-debug`

This skill is the systemic layer, not the per-task worker.
