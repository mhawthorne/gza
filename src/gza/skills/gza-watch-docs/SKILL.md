---
name: gza-watch-docs
description: Regenerate the watch-cycle-phases doc and diagram from src/gza/cli/watch.py, since nothing keeps that doc in sync automatically
allowed-tools: Read, Grep, Glob, Write, Bash(git mv:*), Bash(git diff:*)
version: 1.0.0
public: false
---

# Gza Watch Docs

Regenerate `docs/internal/generated/watch-cycle-phases.md` — the description
of what `gza watch`'s main loop does — directly from `src/gza/cli/watch.py`.

This exists because that doc is hand-derived and nothing keeps it in sync
with `watch.py` automatically. Run this skill after changes to watch.py's
phase structure (new phases, removed phases, reordering, or changes to what
a phase touches) instead of writing the doc up from scratch in conversation.

## When to use

- User asks to "regenerate the watch docs" or "update the watch-phases doc"
- Before/after a change that adds, removes, reorders, or changes what a
  `_run_cycle` phase does in `src/gza/cli/watch.py`

## Process

### Step 1: Find the phase list and orchestration order

Read `src/gza/cli/watch.py`'s `_run_cycle` function (search for `def _run_cycle`)
to get the current ordered list of phases it invokes. This is the ground
truth for phase order — do not rely on the previous version of the doc for
ordering.

### Step 2: For each phase, find its implementation

For each phase name found in Step 1, locate the function/block that runs
during that phase (grep for the phase's log-emitted name, e.g. `"cycle-plan"`,
`"lifecycle-preflight"`, `"blind-parked-auto-rearm"`). For each one, determine:

- What DB reads/writes happen (calls into `SqliteTaskStore` / `store.*`)
- What git subprocess calls happen (calls into a `git.*` helper, or direct
  subprocess invocations of `git`)
- What disk/filesystem checks happen outside git (config files, installed
  package version/hash)
- What process spawns happen (worker subprocess launches, `proc`/PID checks)
- Roughly why the phase is fast or slow (one query vs. per-item subprocess
  calls vs. running an external command like the verify/test suite)

Use Explore subagents to parallelize this across phases if there are many —
don't read the whole 15,000+ line file serially in the main context.

### Step 3: Find the cycle-boundary / re-exec behavior

Find where the outer while-loop calls `_run_cycle` and where drift/re-exec
is checked (search for `_should_reexec_watch` or similar). Confirm whether
re-exec still happens only at the cycle boundary (after `_run_cycle` returns)
or whether that's changed.

### Step 4: Write the doc

Overwrite `docs/internal/generated/watch-cycle-phases.md` with:

1. A short preamble stating this is descriptive (derived from source), not
   prescriptive, and that it needs regenerating (via this skill) when
   `watch.py` changes materially.
2. The ordered phase list as a one-line summary.
3. A `## Phases` section: one entry per phase, in order, each with a `watch.py`
   file:line reference and a plain-English description of what it does and
   its rough cost profile.
4. A `## Diagram` section: a mermaid `flowchart TD` of the phase sequence,
   including any loop-backs (e.g. auto-rearm looping back to cycle-plan) and
   the cycle-boundary/re-exec branch. Represent external systems as distinct
   nodes:
   - the sqlite DB as a cylinder: `DB[("sqlite DB")]`
   - git and disk as parallelogram/I-O shapes, not cylinders: `GIT[/"git (repo/worktrees)"/]`, `DISK[/"disk (...)"/]`
   - give all storage nodes (DB, git, disk) one shared `classDef` color,
     distinct from phase-box coloring
   - connect each phase to the storage node(s) it touches with a dotted edge
5. A `## Why cycles take minutes` (or similarly named) section calling out
   which phases dominate wall-clock time and why, if that's still true.
6. A `## Related` section linking `specs/behavior/watch-supervisor.md` and
   `docs/configuration.md` if they describe the same territory.

Match the existing doc's structure and tone if a prior version exists —
this is a regeneration, not a redesign, unless the user asked for a
different structure.

### Step 5: Report what changed

Show a `git diff` of the doc against its previous version and summarize in
1-3 sentences what changed (new phase, removed phase, updated cost
characterization, etc.) — don't just say "regenerated".

## Notes

- This skill is watch.py-specific by design — don't generalize it into a
  generic "document any module" tool until there's a second concrete use
  case for that.
- Do not edit `docs/internal/generated/watch-cycle-phases.md` by hand outside
  this skill; see `docs/internal/generated/README.md`.
- This is manual-invocation only. It is not wired into any CI/merge gate and
  should not be — the project deliberately avoids automated
  spec/doc-coherence enforcement (see `docs/internal/behavior-conformance-vs-spec-coherence.md`).
