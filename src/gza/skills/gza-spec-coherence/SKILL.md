---
name: gza-spec-coherence
description: Check the behavior spec set for coherence, ownership boundaries, and plain-language discipline without editing the spec or the code
allowed-tools: Read, Glob, Grep, Write, Bash(ls:*), Bash(git grep:*), Bash(date +%Y%m%d%H%M%S)
version: 1.0.0
public: true
---

# Behavior spec coherence check

Review the **behavior spec set itself** in `specs/behavior/` for authoring quality:
coherence, ownership boundaries, atomic normative text, and plain-language discipline.

This skill is blind to authorship. The behavior spec is a shared artifact that both humans
and agents edit, so you judge the text only: **never who wrote it**.

## The one thing that makes this skill different

This is neither an implementation conformance check nor a feature-spec freshness review:

| | `gza-spec-review-all` | `gza-behavior-check` | `gza-spec-coherence` (this skill) |
|---|---|---|---|
| Source of truth | the **code** | the **behavior spec** | the **behavior-spec set as a set** |
| A mismatch means | the doc drifted | code diverged or spec is wrong | ownership, wording, or cross-reference discipline drifted |
| Reviews | `specs/features/` | `specs/behavior/` vs code | `specs/behavior/` vs itself |

**Critical:** this skill **MUST NOT edit the spec or the code.** It reports findings only.
Each finding cites file + section and says concretely what to change.

## When to use

- "Check the behavior specs for coherence"
- Before ratifying a behavior-spec refactor
- After moving vocabulary or invariants between behavior-spec files
- When a spec change feels repetitive, contradictory, or over-written
- As an author-side gate on edits under `specs/behavior/**`

## Inputs (optional scope)

- **No argument** — review the full `specs/behavior/` set.
- **One or more changed files** under `specs/behavior/**` — review those files *against the
  rest of the set*.
- **A directory/pattern** under `specs/behavior/` — review matching files, but still load
  the owning files needed to judge overlap and references.

If the caller provides a change list, do **not** limit yourself to those files alone:
coherence findings often live at the boundary between the changed file and its owner.
Primary findings should still be about the changed behavior-spec files unless an owning
file must change to resolve a duplicate-ownership problem.

## What to flag

1. **Overlap** — the same concept is defined authoritatively in two files. The set should
   have one owner and cross-references elsewhere.
2. **Restated shared vocabulary or invariants** — restated shared vocabulary or
   invariants, terms, rules, or system-wide invariants that should point back to the
   owning file, especially `00-overview.md`, instead of re-defining them.
3. **Verbose normative clauses** — RFC-2119 clauses (`MUST`, `MUST NOT`, `SHALL`,
   `SHOULD`, `MAY`) that can be said more plainly. Report the exact clause and give a
   tighter rewrite.
4. **Non-atomic normative clauses** — one sentence bundles multiple requirements, mixes
   contract with rationale, or hides the real requirement inside a long paragraph. Split
   it into plain, atomic requirements.
5. **Broken or missing cross-references** — links, anchors, or ownership references that
   do not resolve or that should exist but do not.
6. **RFC-2119 keyword misuse** — normative weight where the sentence is really context or
   rationale, or soft prose where a real requirement needs an explicit `MUST` / `MUST NOT`.
7. **Open questions presented as contract** — unresolved decisions are written like settled
   behavior instead of being marked explicitly as open questions, assumptions, or future work.
8. **Implementation details in normative text** — code-path, file-layout, or operational
   mechanics appear in contract text instead of a clearly marked `Implementation note`.

## Process

### Step 1 — Build the ownership map

Read `specs/behavior/README.md` and `specs/behavior/00-overview.md` first. Extract:

- Which file owns shared vocabulary
- Which file owns system-wide invariants
- Which file owns subsystem-specific rules
- Which sections are explicitly non-normative (`Implementation note`, status banners,
  planned/aspirational notes, rationale)

Then list every in-scope file and its declared responsibility in one short table before
you start judging overlap.

### Step 2 — Compare each in-scope file against the owning docs

For each file, ask:

- Does it define a concept that another file already owns?
- Does it restate shared vocabulary or invariants instead of linking back to the owner?
- Does it introduce a second "authoritative" explanation of the same decision?
- Does it rely on an implied ownership boundary that the text never states?
- Does it restate behavior that belongs in `00-overview.md` instead of cross-referencing it?

Treat `00-overview.md` as the default owner of shared vocabulary and system-wide
invariants unless another file explicitly owns a narrower concept.

**Known current example:** today `00-overview.md` owns shared vocabulary and core
invariants, while `lifecycle-engine.md` restates both in `## Shared model`, `## Policy
knobs`, and repeated invariant references. On the current tree, this overlap **should be
reported**. After the refactor that replaces restatement with clean cross-references, the
same pair should be reported as resolved.

### Step 3 — Audit cross-reference integrity

Check every ownership reference and every local markdown link that matters to the contract:

- Linked files exist
- Section anchors resolve or are at least textually plausible
- A reader can find the owning definition from the dependent file
- Cross-references point to the owner instead of a duplicate explanation

Report both:

- **Broken references** — the link target is wrong or missing
- **Missing references** — a repeated concept appears with no pointer to its owner

### Step 4 — Audit normative discipline

Walk every normative clause in scope and classify it:

- **Correctly normative** — this is a real contract statement and the RFC-2119 keyword is
  doing useful work
- **Overweighted** — the sentence uses `MUST`/`SHALL`/etc. for commentary, rationale,
  formatting, or obvious restatement
- **Underweighted** — the sentence describes a contract requirement but lacks clear
  normative force
- **Non-atomic** — one clause packs multiple independent requirements or mixes requirement,
  rationale, and implementation detail
- **Verbose** — the requirement is real, but the clause is longer than needed

For every **verbose** clause, quote the clause, then propose a tighter rewrite in plainer
words that preserves the same requirement.

Prefer simple English over legalistic prose. Shorter is better when the requirement stays
equally precise.

### Step 5 — Separate contract, open questions, and implementation notes

For each in-scope section, confirm:

- Open questions are marked explicitly as open questions, assumptions, or future work, not
  implied as settled contract text.
- Implementation details stay inside clearly marked `Implementation note` blocks or
  equivalent non-normative labeling.
- Normative sections describe *what must hold*, not incidental details about current code
  structure, filenames, or execution mechanics.

### Step 6 — Write the report

```bash
date +%Y%m%d%H%M%S
```

Write to `reviews/<timestamp>-spec-coherence.md`.

## Output format

```markdown
# Behavior spec coherence check

**Scope:** <all behavior specs | changed files>
**Owner map reviewed:** <files>

## Summary
Files reviewed: N
Findings: W overlap · X restatement · Y cross-reference · Z normative-discipline · A open-question/implementation-note issues · B plain-language rewrites

## Blockers
### B1
**Finding:** OVERLAP — `specs/behavior/lifecycle-engine.md` §Shared model
**Conflicts with:** `specs/behavior/00-overview.md` §Vocabulary / §Core invariants
**Problem:** Restates shared vocabulary and invariants that `00-overview.md` already owns.
**What to change:** Replace the restated definitions with a brief cross-reference to
`00-overview.md`, and keep only engine-specific material here.

### B2
**Finding:** RFC-2119 — `specs/behavior/<file>.md` §<section>
**Problem:** Uses `MUST` for rationale / omits `MUST` where the sentence is contract.
**What to change:** <concrete rewrite guidance>

## Follow-Ups
- PLAIN-LANGUAGE — `specs/behavior/<file>.md` §<section>
  Quote the clause, explain why it is too wordy, and propose a tighter rewrite.
- CROSS-REFERENCE — `specs/behavior/<file>.md` §<section>
  Name the missing or broken pointer to the owning file/section and the exact fix.
- OPEN-QUESTION — `specs/behavior/<file>.md` §<section>
  Explain why the text reads like an implied contract and how to mark it explicitly as open.
- IMPLEMENTATION-NOTE — `specs/behavior/<file>.md` §<section>
  Identify implementation detail that should move under a clearly marked `Implementation note`.
- Resolved / clean boundaries:
  - `00-overview.md` ↔ `lifecycle-engine.md`: shared vocabulary is owned once and referenced, not restated.

## Questions / Assumptions
- None.

## Verdict
Verdict: CHANGES_REQUESTED
```

The report **must** use this standard review shape exactly:

- `## Summary`
- `## Blockers`
- `## Follow-Ups`
- `## Questions / Assumptions`
- `## Verdict`

The final verdict must be exactly one of:

- `APPROVED`
- `CHANGES_REQUESTED`
- `NEEDS_DISCUSSION`

If a section has nothing to report, write `None.` under that heading instead of omitting it.

## Rules

- **Never edit the spec or the code.** Output is a report only.
- **Judge the text, not the author.** Do not speculate about whether a human or an agent
  wrote a passage.
- **Every finding cites file + section.** "This feels repetitive" is not enough.
- **One owner per concept.** Shared vocabulary and invariants belong in one owning file;
  dependent files should link, not restate.
- **Prefer cross-reference over duplication.** If a concept is shared, move ownership to
  one file and point to it everywhere else.
- **Normative text must be atomic.** One requirement per clause whenever possible; split
  mixed requirement/rationale/implementation prose into separate sentences or notes.
- **Open questions are not contracts.** If the intended behavior is undecided, mark it as
  an open question instead of implying a settled requirement.
- **Implementation detail belongs in clearly marked notes.** Keep implementation mechanics
  out of the contract unless they are the behavior being specified.
- **Plain language wins.** Keep the same normative force with fewer, clearer words.
- **Do not invent contradictions.** Similar wording is only a finding when it creates
  duplicate ownership, conflicting authority, or unnecessary repetition.
