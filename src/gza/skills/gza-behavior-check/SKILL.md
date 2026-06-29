---
name: gza-behavior-check
description: Check the implementation against the behavior specs in specs/behavior/. Reports where the code diverges from intended behavior — each divergence is either a code bug or a spec gap. The behavior spec is the source of truth; this skill never edits code or the spec.
allowed-tools: Read, Glob, Grep, Write, Bash(ls:*), Bash(git log:*), Bash(git blame:*), Bash(git grep:*), Bash(uv run *--help*), Bash(date +%Y%m%d%H%M%S)
version: 1.0.0
public: true
---

# Behavior conformance check

Check gza's **implementation** against the **behavior specs** in `specs/behavior/` and
report every place the code diverges from the intended behavior.

## The one thing that makes this skill different

This is the **inverse** of `gza-spec-review-all`:

| | `gza-spec-review-all` | `gza-behavior-check` (this skill) |
|---|---|---|
| Source of truth | the **code** | the **behavior spec** |
| A mismatch means | the doc drifted → flag the doc | a **finding**: a **code bug** *or* a **spec gap** |
| Reviews | `specs/features/` (proposals) | `specs/behavior/` (requirements) |

**Critical:** the behavior spec is the contract. When code and spec disagree, that is a
*finding to surface*, **not** license to "fix" the spec to match the code. You **MUST NOT
edit the code or the spec** in this skill. You produce a report; a human (or a follow-up
task) decides which side is wrong.

## When to use

- "Does the code conform to the behavior spec?" / "check behavior conformance"
- After changing the engine, recovery, rebase, or merge logic
- Before ratifying a draft behavior spec as agreed contract
- When investigating a lifecycle bug, to see if it's a known divergence

## Inputs (optional scope)

- **No argument** — check every doc in `specs/behavior/`.
- **A doc name** (e.g. `lifecycle-engine.md`) — check only that doc.
- **An assertion-ID prefix** (e.g. `LE-§6`) — check only assertions under that section.

## Process

### Step 1 — Build the assertion list

Read every in-scope doc under `specs/behavior/`. Extract each **normative statement** into
an atomic, checkable assertion. Normative statements are:

- RFC-2119 keywords: **MUST / MUST NOT / SHOULD / SHOULD NOT / MAY**.
- The numbered **invariants** (overview) and **principles** (`P*`, for example `P1`–`P6`).
- Each ordered **rule** (`§1`–`§8`) — guard → action pairs.
- The **parked reason codes** table (the closed set).
- The **policy knobs** table (what is contract: the *existence* of the bound/gate; the
  *value* is not).

Give each assertion a **stable ID** derived from its location, so findings are traceable
and re-runs are diffable. Scheme: `<DOC>-<SECTION>-<SLUG>`, where `DOC` is a stable
short prefix for the source behavior spec, `SECTION` is the local anchor (`INV3`, `P4`,
`§6`, `MV2`, `RC`, `KNOB`), and `SLUG` is a few-word kebab tag.

Current tracked behavior-spec prefixes:

- `LIN` — `lineage.md`
- `OV` — `00-overview.md`
- `LE` — `lifecycle-engine.md`
- `OTV` — `off-topic-verify-failures.md`
- `REC` — `recovery.md`
- `WS` — `watch-supervisor.md`
- `MV` — `main-verify-self-heal.md`

If a new behavior spec is added, assign it a stable doc prefix before reporting findings
from it. Do not reuse or silently invent prefixes mid-report; list the chosen mapping in
the assertion inventory first. Examples:

- `LE-§6-IMPROVE-CHAIN` — "queries MUST follow the review link, not the impl link."
- `LE-P4-LOCAL-TARGET` — "merge-ness MUST be proven against the local target, never origin."
- `OV-INV2-BOUNDED-LOOPS` — "every loop MUST be bounded; hitting the bound escalates."
- `LE-RC-rebase-did-not-unblock-merge` — that reason code exists and is emitted on §4.
- `LIN-P6-TERMINAL-LANDED-NOT-ACTIONABLE` — terminal landed/no-work merge-unit owners stay
  off actionable lineage/recovery surfaces unless unique unmerged work remains visible.
- `MV-MV2-RERUN-BEFORE-REUSE` — red verdicts MUST be re-verified before automation acts.
- `MV-MV4-REMEDIATE-DEDUP-BUMP` — confirmed red verify failures MUST create or reuse
  one remediation task per failure signature and bump it to the front of the runnable
  queue.
- `MV-MV5-NO-LAUNCH-STALL` — red merge freezes MUST NOT hard-park downstream work.
- `MV-MV6-FORCE-REFRESH` — operators MUST have a first-class force-refresh rerun path
  that ignores a cached red checkpoint and leaves behind fresh evidence.
- `WS-S7-BOUNDED-WORK-CREATION` — watch-owned stateful work creation MUST stay bounded
  to deduped supervisor-owned surfaces such as local-target verify remediation.

List the assertions before checking them, so the report can show total coverage.

### Step 2 — Verify each assertion against the code

For each assertion, locate the implementing code (Grep/Glob/Read) and render exactly one
verdict, **with `file:line` evidence**:

- **HOLDS** — the code enforces the assertion. Cite the code that does so.
- **DIVERGES** — the code does something else. Cite **both** the spec statement and the
  conflicting code.
- **UNDETERMINED** — you could not establish it safely (couldn't find the code path, logic
  too indirect to judge by reading). This is a **valid and valuable** verdict — never
  guess a HOLDS or a DIVERGES to avoid it.

Starting points for where behavior lives (these are *hints only* — code moves; confirm by
search, don't trust this list):

- The ordered rule evaluator and action selection (the `advance`/`iterate` engine).
- Recovery policy (resume/retry/give-up classification).
- Rebase publication, conflict detection, and post-rebase diff/review invalidation.
- Merge execution and merge-state/merge-unit resolution.
- The `watch` loop (batching, attention rendering, drift restart).

Do not stop at the first match — confirm the assertion holds on *all* relevant paths, not
just one.

### Step 3 — Classify each divergence (do not resolve it)

For every **DIVERGES**, state the evidence on both sides and recommend one of:

- **Code bug** — the intent is clear and right; the code should change. (Most actionable.)
- **Spec gap** — the spec is wrong, underspecified, or describes intent we no longer hold;
  the *spec* should change. Say what's missing.
- **Ambiguous** — the spec doesn't say enough to judge. Name the missing decision.

Deciding which side is wrong is the human's call — your job is to make the divergence and
its evidence unmissable, with a recommendation.

### Step 4 — Adversarially verify before reporting

For each claimed **DIVERGES**, make one honest attempt to **refute** it: find the code path
that *does* satisfy the assertion (a guard elsewhere, a caller that pre-checks, a later
branch). If you can refute it, downgrade to HOLDS (or UNDETERMINED) and note why. Only
divergences that survive refutation get reported as findings. This is what keeps the report
from crying wolf.

### Step 5 — Write the report

```bash
date +%Y%m%d%H%M%S
```

Write to `reviews/<timestamp>-behavior-check.md`.

## Output format

````markdown
# Behavior conformance check

**Scope:** <all behavior specs | doc | assertion-ID prefix>
**Behavior spec commit:** <git short sha of specs/behavior/ HEAD>

## Scorecard
Assertions checked: N — HOLDS: a · DIVERGES: b · UNDETERMINED: c

## Findings (divergences)

### LE-§6-IMPROVE-CHAIN — DIVERGES — recommend: code bug
**Spec** (lifecycle-engine.md §6): "queries MUST follow the review link, not the impl link."
**Code** (`src/gza/<file>.py:NNN`): filters by the impl link, so retries/resumes are missed.
**Why it matters:** a completed retry isn't counted as addressing the review → review state
stays dirty.
**Refutation attempted:** checked callers for a compensating walk; none found.
**Recommendation:** code bug — switch the query to the review link.

### <next finding…>

## Undetermined (needs a closer look or a human)

| Assertion ID | Why undetermined |
|--------------|------------------|
| LE-§5-CHANGED-DIFF | "normalized patch equivalence" criterion couldn't be traced to a single code path |

## Holds (verified conformant)

| Assertion ID | Evidence |
|--------------|----------|
| LE-P4-LOCAL-TARGET | merge-proof resolves local target at `…:NN`; no origin fallback found |

## Recommendations
1. <highest-priority code bug>
2. <spec gap to resolve with the owner>

## Machine-readable findings

```json
[
  {
    "assertion_id": "LE-§6-IMPROVE-CHAIN",
    "verdict": "DIVERGES",
    "recommendation": "code bug",
    "spec_file": "specs/behavior/lifecycle-engine.md",
    "spec_section": "§6",
    "summary": "Improve chain queries follow the implementation link instead of the review link.",
    "evidence": [
      {
        "path": "src/gza/<file>.py",
        "line": 123,
        "note": "Filters by the implementation link, so review-linked retries are missed."
      }
    ],
    "report_path": "reviews/<timestamp>-behavior-check.md"
  },
  {
    "assertion_id": "LE-P4-LOCAL-TARGET",
    "verdict": "HOLDS",
    "recommendation": null,
    "spec_file": "specs/behavior/lifecycle-engine.md",
    "spec_section": "P4",
    "summary": "Merge-proof resolves the local target only.",
    "evidence": [
      {
        "path": "src/gza/<file>.py",
        "line": 88,
        "note": "Uses the local target ref and does not fall back to origin."
      }
    ],
    "report_path": "reviews/<timestamp>-behavior-check.md"
  }
]
```
````

The human-readable sections above stay exactly as written. The JSON appendix is additive
and MUST appear at the end of every report so automation can parse the run without
scraping prose.

The appendix MUST contain exactly one JSON object per checked assertion, and every object
MUST include `assertion_id`, `verdict`, `recommendation`, `spec_file`, `spec_section`,
`summary`, `evidence`, and `report_path`.

Emit **one JSON object per checked assertion** (HOLDS, DIVERGES, and UNDETERMINED), using
this schema:

- `assertion_id` — stable assertion ID from Step 1.
- `verdict` — `HOLDS`, `DIVERGES`, or `UNDETERMINED`.
- `recommendation` — `code bug`, `spec gap`, or `ambiguous` for `DIVERGES`; use `null` for
  `HOLDS` and `UNDETERMINED`.
- `spec_file` — behavior-spec path under `specs/behavior/`.
- `spec_section` — local section/anchor for the assertion (`INV3`, `P4`, `§6`, `MV2`, `RC`,
  `KNOB`, etc.).
- `summary` — one-sentence result summary for that assertion.
- `evidence` — array of `{path, line, note}` objects. Include every cited implementation
  path here; use an empty array only if `UNDETERMINED` truly has no safe code citation.
- `report_path` — the relative `reviews/<timestamp>-behavior-check.md` path written by this
  run.

## Rules

- **Never edit code or the spec.** Output is a report only. (Filing confirmed code-bug
  findings as gza tasks is a *separate, explicit* step the operator runs — do not do it
  here by default.)
- **Every verdict cites `file:line`.** A verdict with no evidence is not a verdict.
- **UNDETERMINED beats a guess.** Confident-but-wrong findings destroy trust in the check.
- **Respect the contract/policy split.** For a bound (e.g. `max_review_cycles`), check that
  the loop *cannot run unbounded* (contract), not that the code uses a specific number
  (the value is a tunable knob, not contract).
- **Reason codes:** the spec defines a canonical reason-code set; the code may still emit
  shorter strings (noted in the spec's implementation note). A mismatched *string* is a
  real finding, but classify it as the kind of reconciliation the spec already flags, not a
  behavior bug, unless automation actually branches on the wrong value.
- **Aspirational sections are out of scope.** Anything the spec marks *Planned /
  aspirational* (e.g. the automatic plan-review step) is not yet contract — skip it, don't
  report it as a divergence.
- **Draft vs ratified.** Note in the report whether each checked area is ratified contract
  or still draft, so a divergence in a draft rule reads as "confirm intent" rather than
  "definite bug."
- **The JSON appendix is mandatory.** Make it valid JSON, keep it in a fenced `json` code
  block under `## Machine-readable findings`, and ensure every object's `report_path`
  matches the report you wrote.
