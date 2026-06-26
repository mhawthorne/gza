---
name: gza-system-triage
description: Turn the recurring `watch` stuck-task pile into (1) a diagnosis of why each class is stuck, (2) the existing stuck rows actually cleared now, and (3) systemic prevention so it does not recur. Snapshots watch/incomplete/queue, buckets stuck tasks by failure class, dedups against already-tracked `system` work, unsticks each row by its clearing action (drop moot/dead/stale, spawn follow-up, hand review-loop rows to /gza-task-fix), then ranks and files `system`-tagged prevention fixes by blast radius (cascade-preventer first). Never merges, retries, resumes, deletes branches, or edits code.
allowed-tools: Read, Write, AskUserQuestion, Bash(uv run gza incomplete:*), Bash(uv run gza search:*), Bash(uv run gza history:*), Bash(uv run gza next:*), Bash(uv run gza show:*), Bash(uv run gza log:*), Bash(uv run gza add:*), Bash(uv run gza implement:*), Bash(uv run gza set-status:*), Bash(uv run gza queue:*), Bash(uv run gza ps:*), Bash(uv run python -c:*), Bash(mkdir:*), Bash(date:*)
version: 1.5.0
public: false
---

# Gza System Triage

Every stuck task is a **failed auto-merge**. This skill does three things, in order, for the pile `watch` leaves behind:

1. **Diagnose** — bucket the stuck rows by failure *class* and find why each class is stuck.
2. **Unstick now** — clear the *existing* stuck rows by their correct action: drop the moot/dead/stale, spawn the missing follow-up, or hand review/improve-loop rows to `/gza-task-fix`. A stuck row left in place is debt that grows faster than anyone can service it.
3. **Prevent** — for each class, file the broadest `system`-tagged change that would let this and its siblings auto-merge unattended next time, ranked by blast radius.

Diagnosis without unsticking just re-describes the pile; unsticking without prevention means the pile rebuilds tomorrow. Do all three.

It exists because the same conversation recurs daily: `watch` leaves a pile of `ATTENTION` / `Needs attention` rows, and each session re-derives the landscape from scratch and ships another narrow micro-fix that doesn't compound. This skill closes that loop: snapshot once, bucket by class, **skip classes already being fixed**, clear what's clearable now, and file the broadest prevention fix for the rest.

The per-task rescue (`/gza-task-fix`) and per-lineage drop/rebase classification (`/gza-task-triage`) are the workers this skill *drives* — it owns the orchestration (which row gets which action) and executes the safe clearing actions itself, delegating only the heavy per-unit rescue.

## What this skill MUST NOT do

These guardrails are load-bearing.

- **Do not merge, retry, resume, or delete branches.** If a row warrants one of those, say so and stop. (Note: **dropping** a task via `gza set-status <id> dropped` is *not* deleting a branch — the branch stays as history — and is the sanctioned way to clear a moot/dead/stale row. Spawning a follow-up via `gza add --based-on` / `gza implement` and handing a row to `/gza-task-fix` are likewise allowed. These are the unstick actions; merge/retry/resume are not.)
- **Do not edit code.** A single unit that needs a code fix is a `/gza-task-fix` handoff or a filed task, not an inline edit.
- **Confirm before bulk or destructive unstick actions in manual mode.** Drops are reversible (`set-status ... pending`) but bulk drops still need an explicit go-ahead; never mass-drop on a terse instruction without confirming scope.
- **Do not file untagged tasks.** Every filed task gets `system` plus the active recovery tag — an untagged task is a silent orphan watch never runs.
- **Do not file anything in manual mode without confirmation.**
- **Do not propose another narrow guard for a class that already recurred after a fix landed.** That is the signal the previous fix missed the cause layer — escalate to re-diagnose deeper (see `specs/behavior/systemic-fix-triage.md`), never patch the symptom again.
- **Do not rescue a task without accounting for its auto-merge fix, and never re-file a fix that already landed.** Every `/gza-task-fix` rescue is a *failed auto-merge* — run the pairing-with-verification gate (Step 7) before filing any prevention. If the fix that would have auto-merged the task already merged yet the task still got stuck, file a cause-layer escalation, never another copy of that fix. A prevention that clears only the one rescued row is a symptom patch, not a systemic fix.

## Modes

Read the mode from the invocation argument:

- **`manual` (default)** — present the ranked findings and wait. File only after the user confirms.
- **`auto`** — file `system`-tagged tasks with best judgement, then report exactly what was filed.

If the argument is absent or unclear, default to `manual`.

## Process

### Step 1: Snapshot the slow surfaces — in parallel (and cache)

`watch`/`incomplete`/`history` are slow to query — capture once, **in parallel**, and reuse within the session so diagnosis (Step 2+) can start sooner. First, in one quick call, make the dir and **capture a single run timestamp to reuse as the filename _prefix_ for every artifact this run** (the snapshots here and the findings report in Step 7): `TS=$(date +%Y%m%d-%H%M%S)` (timestamp is the filename **prefix**, not a suffix).

```bash
mkdir -p .gza/system-triage
TS=$(date +%Y%m%d-%H%M%S)                                               # reuse as the prefix for the snapshots here AND ${TS}-triage.md in Step 7
```

Then fetch all three surfaces **as parallel Bash tool calls in a single message** — they are independent and read-only, so concurrent runs are safe. Parallel calls do **not** share shell state, so inline the literal `${TS}` value into each command (do not re-derive it) and redirect each to its **own** file (the outputs have different shapes — two are JSON, one is text):

```bash
uv run gza incomplete --json --last 0 > .gza/system-triage/${TS}-incomplete.json                          # needs-attention rows (next_action_reason)
uv run gza history --status failed --json --days 1 --date-field effective > .gza/system-triage/${TS}-history.json   # recent failed leaves (failure_reason)
uv run gza next --all > .gza/system-triage/${TS}-next.txt                                                  # recovery + pending lanes, blocked rows (text; no --json)
```

If a recent (~15 min old) `${TS}-incomplete.json` / `${TS}-history.json` / `${TS}-next.txt` set already exists, reuse it unless the caller asks to refresh.

Default to **recent** failures (last 24h) — do not dredge an all-time backlog unless asked.

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

### Step 6: Unstick the existing rows now

Prevention (Step 7) stops recurrence; it does **not** clear the rows already stuck. Do that here. Assign every stuck row exactly one clearing action, then execute the safe ones:

| Action | When | How |
|---|---|---|
| **DROP** | moot / dead / stale / superseded / never-merged-and-abandoned (incl. months-old backlog and dead `INFRASTRUCTURE_ERROR` leaves once their owner is resolved elsewhere) | `uv run gza set-status <id> dropped` — clears the row now and stops it reappearing; branch is kept |
| **SPAWN follow-up** | completed explore/plan blocked only on a "what next" decision (`explore-needs-follow-up-decision`, plan awaiting implement) | `uv run gza add --based-on <id> ...` (from an explore) or `uv run gza implement <plan-id>` (from a plan; queues by default) — the follow-up *is* the unstick |
| **RESCUE inline** | review/improve-loop on one unit (`review-no-exit`, per-unit `verify-unreproducible`) | `/gza-task-fix <impl-id>` |
| **LEAVE** | genuinely-live pending/in-flight work, or ready advance actions (`materialize_plan_slices`, recovery lane) watch will run | nothing — these are not stuck |

- Before dropping a dead leaf, confirm its owner is resolved elsewhere (e.g. a sibling already produced the verdict / merge) so the drop is pure residue cleanup and watch will not just re-spawn it.
- **Check whether `watch` is running** (`uv run gza ps` / process list) and what tag scope it has — dropping in-scope rows whose owner is still unresolved can trigger an immediate re-spawn before the prevention fix lands.
- **Manual mode:** confirm scope via `AskUserQuestion` before executing drops (especially bulk), then run them. **Auto mode:** execute the safe drops/spawns directly. Either way, hand RESCUE rows to `/gza-task-fix` rather than fixing inline here.
- A row's unstick action and its class's prevention fix are complementary: e.g. an unbounded-respawn cascade → **drop** the dead leaves now (Step 6) **and** file the circuit-breaker (Step 7).

### Step 7: Schedule prevention — present (manual) or file (auto)

**First — the pairing-with-verification gate (run for every row you unstuck by hand in Step 6).**

A manual unstick — above all a `/gza-task-fix` RESCUE — is itself proof that auto-merge failed for that task. The triage is not done until the fix that *would have auto-merged it* is accounted for. But "file a prevention" is the trap: that fix may already exist, and may already have failed. For each rescued / hand-unstuck task, name the fix that would have auto-merged it, then check its status and **branch on it**:

- **Not filed anywhere** (confirm via `gza next --all --tag system` and `gza history --tag system --json --last 0`) → file the broadest version — one that would auto-merge this task *and its siblings*, never a one-row patch.
- **Filed, not yet landed** → already tracked; collapse to "tracked by gza-XXXX", do not duplicate.
- **Landed (merged) but the task still got stuck** → **do NOT file another fix.** The merged fix missed the cause layer. File exactly one *cause-layer escalation* as an **`implement`** task (it investigates-and-fixes in one unit and stays on the auto-merge rails) — or a `plan` if the fix genuinely needs design first. **Never an `explore`:** a completed explore only parks at `explore-needs-follow-up-decision` and auto-merges nothing, so it would just become another stuck row. State the evidence in the prompt: "fix gza-XXXX merged `<date>`, yet this task parked on `<reason>` `<after that date>` — find why the merged path doesn't fire and fix the gap." Re-filing the symptom is forbidden (MUST-NOT recurrence rule). To make this branch real you must look up the candidate fix's **merge state and date** (`gza show <id>` / lineage) and compare against the stuck task's last-activity date — a claim that "X will handle it" without that check is how dud duplicates get filed.

**Breadth test:** reject any prevention that would clear only the one rescued row. The target is the cause that auto-merges multiple stuck siblings; a one-row fix is a symptom patch, not a systemic fix.

Report the pairing outcome per rescue: *filed-broad* / *already-tracked* / *escalated-to-cause-layer*.

**Second — the before/after trace gate (required for every systemic fix before filing).**

A fix you can't trace from the actual failure to merge is a guess. For each proposed systemic fix, pick the **exemplar stuck task** it targets and write two concrete, step-by-step timelines:

- **Actual** — what happened to the exemplar, step by step, into the stuck state, citing real evidence (the `failure_reason`, the *deciding log line*, the lineage/verdict). Trace the real sequence; do not summarize.
- **With the fix in place** — the same sequence holding everything else constant and changing **only** the proposed fix, step by step, to its end state.

The with-fix timeline must either **reach an auto-merge**, or **name the next root cause that still blocks** — and each remaining root becomes its own paired finding (loop it back through the pairing gate above). If you cannot trace a concrete path from the failure toward merge, the fix is not grounded — refine or drop it, do not file it. This catches plausible-but-ineffective fixes and, crucially, surfaces **compounding root causes**: a fix that removes one park but leaves the task blocked on another (e.g. classifying a provider-capacity error as recoverable still won't merge the task while `main`'s verify is red — two roots, two fixes). Read the exemplar's actual log to ground the "Actual" timeline; do not infer it from status fields alone.

Then, for each ranked class produce one block:

- **Class** + **blast radius** (N rows) + affected task IDs.
- **The systemic fix**, stated as a *precondition or rule change*, and its shape: a code change to file, a cycle-level gate, or a `/gza-task-fix` per-task handoff.
- **Before/after trace** — the two timelines from the trace gate (actual → stuck; with-fix → merge or next-root), naming the exemplar task. A fix block without a trace is not ready to file.
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

**Report** (both modes): **rows unstuck** (dropped / follow-ups spawned / handed to `/gza-task-fix`, with counts and IDs), classes filed for prevention (new IDs, tags, bumped?), classes skipped as already-tracked, classes escalated for cause-layer re-diagnosis. The headline proves all three happened — e.g. "87 rows cleared, 1 cascade prevention filed, 1 class already tracked."

**Persist the report to a timestamped file (both modes, always — this is the durable record, not just conversation output).** Write the full report — the class buckets with blast radius, every per-row unstick decision (drop / spawn / rescue / leave), the pairing-gate outcome per rescue (*filed-broad* / *already-tracked* / *escalated-to-cause-layer*), the **before/after trace** for each filed systemic fix, and all filed task IDs — to `.gza/system-triage/${TS}-triage.md`, reusing the **same `${TS}` prefix** as the snapshot from Step 1 (timestamp is the filename **prefix**, not a suffix). This gives an audit trail and lets a later session diff against it instead of re-deriving the landscape. Mirror how `explore`/`review` tasks persist reports to `.gza/explorations/` and `.gza/reviews/`. Write this file even when the user skips filing — the findings still stand.

## Output style

- One block per class, ranked, separated by `---`. Lead with class + blast radius. Short prose — the user has been reading `watch` output all day and wants signal.
- Always surface the dedup result. "3 classes, 1 already tracked, 1 escalated, 1 new" is the headline that proves the loop is closing instead of re-deriving.

## When to escalate to a different skill

- Per-task rescue (review/improve churn on one unit) → `/gza-task-fix`
- Per-lineage drop / rebase classification of `incomplete` rows → `/gza-task-triage`
- Log-level anti-patterns (bare commands, test loops, cost outliers) → `/gza-log-insights`
- Per-task root-cause / loop analysis → `/gza-task-debug`

This skill owns the systemic orchestration — **diagnose, unstick, prevent** — and drives the per-task workers above; it executes the safe clearing actions (drop / spawn) itself but does not do heavy per-unit rescue inline.
